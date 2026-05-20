package systemtest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/gdr00/distributed-server-update/internal/network/userpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// globals

var nodes [3]userpb.UpdateServiceClient

func TestMain(m *testing.M) {
	addrs := [3]string{
		envOr("NODE1_ADDR", "localhost:9001"),
		envOr("NODE2_ADDR", "localhost:9002"),
		envOr("NODE3_ADDR", "localhost:9003"),
	}

	conns := make([]*grpc.ClientConn, 3)
	for i, addr := range addrs {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Fprintf(os.Stderr, "connect %s: %v\n", addr, err)
			os.Exit(1)
		}
		conns[i] = conn
		nodes[i] = userpb.NewUpdateServiceClient(conn)
	}

	code := m.Run()
	for _, c := range conns {
		c.Close()
	}
	os.Exit(code)
}

// helpers

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// writeToNode pushes a single entry via Sync so the server's applyRemote fires.
// wallOffset shifts time.Now() — negative for "older", positive for "newer".
func writeToNode(ctx context.Context, c userpb.UpdateServiceClient, key, value, nodeID string, wallOffset int64) error {
	_, err := c.Sync(ctx, &userpb.SyncRequest{
		LocalState: []*userpb.SettingEntry{{
			Key:   key,
			Value: value,
			Clock: &userpb.HLC{
				WallTime: time.Now().UnixNano() + wallOffset,
				Logical:  0,
				NodeId:   nodeID,
			},
		}},
	})
	return err
}

// deleteFromNode pushes a tombstone via Sync.
func deleteFromNode(ctx context.Context, c userpb.UpdateServiceClient, key, nodeID string) error {
	_, err := c.Sync(ctx, &userpb.SyncRequest{
		LocalState: []*userpb.SettingEntry{{
			Key:     key,
			Deleted: true,
			Clock: &userpb.HLC{
				WallTime: time.Now().UnixNano(),
				Logical:  0,
				NodeId:   nodeID,
			},
		}},
	})
	return err
}

// waitKeyValue polls a direct Sync RPC on every node until all report key=wantVal
// or the timeout expires.
func waitKeyValue(t *testing.T, key, wantVal string, timeout time.Duration) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if allNodesHaveValue(ctx, key, wantVal) {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	for i, c := range nodes {
		resp, _ := c.Sync(ctx, &userpb.SyncRequest{})
		var found *userpb.SettingEntry
		for _, e := range resp.NewerEntries {
			if e.Key == key {
				found = e
				break
			}
		}
		switch {
		case found == nil:
			t.Errorf("node%d: key %q absent", i+1, key)
		case found.Deleted:
			t.Errorf("node%d: key %q tombstoned", i+1, key)
		case found.Value != wantVal:
			t.Errorf("node%d: key %q = %q, want %q", i+1, key, found.Value, wantVal)
		}
	}
	t.Fatalf("convergence timeout: key %q never reached %q on all nodes", key, wantVal)
}

func allNodesHaveValue(ctx context.Context, key, wantVal string) bool {
	for _, c := range nodes {
		resp, err := c.Sync(ctx, &userpb.SyncRequest{})
		if err != nil {
			return false
		}
		found := false
		for _, e := range resp.NewerEntries {
			if e.Key == key && !e.Deleted && e.Value == wantVal {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// waitKeyDeleted polls until all nodes report the key absent or tombstoned.
func waitKeyDeleted(t *testing.T, key string, timeout time.Duration) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if allNodesDeleted(ctx, key) {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("convergence timeout: key %q still alive on some node", key)
}

func allNodesDeleted(ctx context.Context, key string) bool {
	for _, c := range nodes {
		resp, err := c.Sync(ctx, &userpb.SyncRequest{})
		if err != nil {
			return false
		}
		for _, e := range resp.NewerEntries {
			if e.Key == key && !e.Deleted {
				return false
			}
		}
	}
	return true
}

// keyPresentOnNode returns true if the node's Sync response contains the key
// (alive or tombstone). False means it was purged from CRDT state entirely.
func keyPresentOnNode(ctx context.Context, c userpb.UpdateServiceClient, key string) bool {
	resp, err := c.Sync(ctx, &userpb.SyncRequest{})
	if err != nil {
		return true // assume present on error to avoid false purge detection
	}
	for _, e := range resp.NewerEntries {
		if e.Key == key {
			return true
		}
	}
	return false
}

// waitKeyPurged blocks until no node returns the key in a Sync response.
func waitKeyPurged(t *testing.T, key string, timeout time.Duration) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		allPurged := true
		for _, c := range nodes {
			if keyPresentOnNode(ctx, c, key) {
				allPurged = false
				break
			}
		}
		if allPurged {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("GC timeout: key %q still present on some node after TTL", key)
}

// tests

// TestInitialStateConverges verifies node1's seed data ("da"="ddd") propagates
// to node2 and node3 via anti-entropy on startup.
func TestInitialStateConverges(t *testing.T) {
	waitKeyValue(t, "da", "ddd", 30*time.Second)
}

// TestWritePropagatesAcrossNodes injects an entry from every node and asserts
// each one converges cluster-wide.
func TestWritePropagatesAcrossNodes(t *testing.T) {
	cases := []struct {
		origin int
		key    string
		value  string
		nodeID string
	}{
		{0, "prop_n1", "from_node1", "node1"},
		{1, "prop_n2", "from_node2", "node2"},
		{2, "prop_n3", "from_node3", "node3"},
	}
	for _, c := range cases {
		t.Run(c.nodeID, func(t *testing.T) {
			if err := writeToNode(context.Background(), nodes[c.origin], c.key, c.value, c.nodeID, 0); err != nil {
				t.Fatalf("write to %s: %v", c.nodeID, err)
			}
			waitKeyValue(t, c.key, c.value, 30*time.Second)
		})
	}
}

// TestTombstonePropagates writes a key, waits for convergence, then deletes it
// from node1 and verifies the tombstone reaches all nodes.
func TestTombstonePropagates(t *testing.T) {
	key := "tombstone_key"

	if err := writeToNode(context.Background(), nodes[0], key, "alive", "node1", 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	waitKeyValue(t, key, "alive", 30*time.Second)

	if err := deleteFromNode(context.Background(), nodes[0], key, "node1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	waitKeyDeleted(t, key, 30*time.Second)
}

// TestHLCConflictResolution writes the same key to two nodes with different HLC
// wall times and verifies every node converges to the entry with the higher clock.
func TestHLCConflictResolution(t *testing.T) {
	key := "hlc_conflict"

	// Loser: wall = now - 2s (lower HLC)
	if err := writeToNode(context.Background(), nodes[0], key, "loser", "test-loser", -int64(2*time.Second)); err != nil {
		t.Fatalf("write loser to node1: %v", err)
	}
	// Winner: wall = now + 500ms (higher HLC)
	if err := writeToNode(context.Background(), nodes[1], key, "winner", "test-winner", int64(500*time.Millisecond)); err != nil {
		t.Fatalf("write winner to node2: %v", err)
	}

	waitKeyValue(t, key, "winner", 30*time.Second)
}

// TestOverwriteConverges writes a key, waits for convergence, then overwrites it
// with a newer clock and verifies all nodes pick up the update.
func TestOverwriteConverges(t *testing.T) {
	key := "overwrite_key"

	if err := writeToNode(context.Background(), nodes[0], key, "v1", "node1", -int64(time.Second)); err != nil {
		t.Fatalf("first write: %v", err)
	}
	waitKeyValue(t, key, "v1", 30*time.Second)

	if err := writeToNode(context.Background(), nodes[0], key, "v2", "node1", 0); err != nil {
		t.Fatalf("second write: %v", err)
	}
	waitKeyValue(t, key, "v2", 30*time.Second)
}

// TestOlderWriteDoesNotOverwrite sends a write with an older HLC to a node that
// already has a newer value and verifies the newer value is preserved everywhere.
func TestOlderWriteDoesNotOverwrite(t *testing.T) {
	key := "stale_write_key"

	if err := writeToNode(context.Background(), nodes[0], key, "current", "node1", 0); err != nil {
		t.Fatalf("write current: %v", err)
	}
	waitKeyValue(t, key, "current", 30*time.Second)

	// Attempt to overwrite with a stale (older) clock — CRDT must reject it.
	if err := writeToNode(context.Background(), nodes[1], key, "stale", "node2", -int64(5*time.Second)); err != nil {
		t.Fatalf("write stale: %v", err)
	}

	// Give time for any incorrect propagation to surface, then confirm value held.
	time.Sleep(8 * time.Second)
	waitKeyValue(t, key, "current", 5*time.Second)
}

// crdt state helpers

type crdtClock struct {
	WallTime int64  `json:"WallTime"`
	Logical  int    `json:"Logical"`
	NodeID   string `json:"NodeID"`
}

type crdtStateEntry struct {
	Key     string    `json:"Key"`
	Value   string    `json:"Value"`
	Clock   crdtClock `json:"Clock"`
	Deleted bool      `json:"Deleted"`
}

// injectCRDTEntry writes an entry directly into a node's crdt_state.json.
func injectCRDTEntry(t *testing.T, crdtDir, key, value string, wallTime int64) {
	t.Helper()
	path := filepath.Join(crdtDir, "crdt_state.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read crdt_state.json: %v", err)
	}
	var state map[string]crdtStateEntry
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatalf("unmarshal crdt_state.json: %v", err)
	}
	state[key] = crdtStateEntry{
		Key:   key,
		Value: value,
		Clock: crdtClock{WallTime: wallTime, NodeID: "test-injected"},
	}
	out, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("marshal crdt_state.json: %v", err)
	}
	if err := os.WriteFile(path, out, 0600); err != nil {
		t.Fatalf("write crdt_state.json: %v", err)
	}
}

// docker control helpers

func stopNode(t *testing.T, container string) {
	t.Helper()
	if out, err := exec.Command("docker", "stop", container).CombinedOutput(); err != nil {
		t.Fatalf("docker stop %s: %v\n%s", container, err, out)
	}
}

func startNode(t *testing.T, container string) {
	t.Helper()
	if out, err := exec.Command("docker", "start", container).CombinedOutput(); err != nil {
		t.Fatalf("docker start %s: %v\n%s", container, err, out)
	}
}

// killNode sends SIGKILL to a container, bypassing graceful shutdown.
// saveShutdownTime defer never runs, simulating a crash.
func killNode(t *testing.T, container string) {
	t.Helper()
	if out, err := exec.Command("docker", "kill", "--signal=KILL", container).CombinedOutput(); err != nil {
		t.Fatalf("docker kill %s: %v\n%s", container, err, out)
	}
}

// waitForNodeUp polls until the node accepts a Sync RPC or the timeout expires.
func waitForNodeUp(t *testing.T, c userpb.UpdateServiceClient, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := c.Sync(context.Background(), &userpb.SyncRequest{}); err == nil {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal("node did not come back up in time")
}

// restart tests

// TestNodeRestartConverges stops node1, writes a key while it is offline, then
// restarts it and verifies it picks up the missed update via anti-entropy.
func TestNodeRestartConverges(t *testing.T) {
	node1Container := envOr("NODE1_CONTAINER", "node1")

	stopNode(t, node1Container)

	// Write while node1 is offline, it must catch this via anti-entropy after restart.
	key := "restart_missed_key"
	if err := writeToNode(context.Background(), nodes[1], key, "missed_while_down", "node2", 0); err != nil {
		t.Fatalf("write: %v", err)
	}

	startNode(t, node1Container)
	waitForNodeUp(t, nodes[0], 15*time.Second)

	waitKeyValue(t, key, "missed_while_down", 30*time.Second)
}

// TestStaleTTLReinit manipulates last_shutdown so node1 appears to have been
// offline longer than tombstoneTTL (10 s in test config). On restart, node1
// must wipe its CRDT state. The wipe is proven by injecting a future-clock
// entry into node1's on-disk state before restart: if reinit fires the entry
// is gone and node2's value wins, if reinit is skipped the injected entry's
// higher HLC beats node2 and spreads everywhere, test fails.
func TestStaleTTLReinit(t *testing.T) {
	node1Container := envOr("NODE1_CONTAINER", "node1")
	node1CRDTDir := envOr("NODE1_CRDT_DIR", "/data/node1_crdt")

	key := "reinit_probe"

	// Write from node2 and wait for full convergence including node1.
	if err := writeToNode(context.Background(), nodes[1], key, "node2_wins", "node2", 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	waitKeyValue(t, key, "node2_wins", 30*time.Second)

	// Stop node1, clean shutdown writes real timestamp to last_shutdown.
	stopNode(t, node1Container)
	time.Sleep(500 * time.Millisecond)

	// Stale both liveness files since max of their value is taken
	staleNS := time.Now().Add(-11 * time.Second).UnixNano()
	staleStr := []byte(strconv.FormatInt(staleNS, 10))
	if err := os.WriteFile(filepath.Join(node1CRDTDir, "last_shutdown"), staleStr, 0600); err != nil {
		t.Fatalf("write stale last_shutdown: %v", err)
	}
	if err := os.WriteFile(filepath.Join(node1CRDTDir, "last_heartbeat"), staleStr, 0600); err != nil {
		t.Fatalf("write stale last_heartbeat: %v", err)
	}

	// Inject a winning entry into node1's on-disk CRDT state. Clock is +100 ms
	// in the future so it would beat node2's entry if loaded. If reinit fires,
	// InitNew wipes crdt_state.json before the server reads it, this entry
	// never makes it into the running CRDT.
	injectCRDTEntry(t, node1CRDTDir, key, "node1_stale", time.Now().Add(100*time.Millisecond).UnixNano())

	startNode(t, node1Container)
	waitForNodeUp(t, nodes[0], 15*time.Second)

	// node2's value must win everywhere, proves node1 wiped its state.
	waitKeyValue(t, key, "node2_wins", 30*time.Second)
}

// TestTombstoneGC verifies that deleted entries are purged from all nodes once
// their tombstone clock age exceeds tombstoneTTL (10 s) and the GC loop fires
// (gcInterval = 3 s). After purge, the key must be absent from Sync responses
// entirely not just marked deleted.
func TestTombstoneGC(t *testing.T) {
	key := "gc_key"

	// Write and wait for full convergence.
	if err := writeToNode(context.Background(), nodes[0], key, "alive", "node1", 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	waitKeyValue(t, key, "alive", 30*time.Second)

	// Delete and wait for tombstone to propagate to all nodes.
	if err := deleteFromNode(context.Background(), nodes[0], key, "node1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	waitKeyDeleted(t, key, 30*time.Second)

	// Wait for tombstone to age past TTL (10 s) and the GC loop to fire (≤3 s).
	// Worst case: 10 s TTL + 3 s GC interval = 13 s.
	waitKeyPurged(t, key, 20*time.Second)
}

// TestTombstoneResurrection writes a key, deletes it, then writes the same key
// again with a newer HLC. Cluster must accept the resurrection (newer write)
// and converge on the new value. Checks if tombstones permanently invalidates a key
func TestTombstoneResurrection(t *testing.T) {
	key := "resurrection_key"

	if err := writeToNode(context.Background(), nodes[0], key, "alive1", "node1", 0); err != nil {
		t.Fatalf("first write: %v", err)
	}
	waitKeyValue(t, key, "alive1", 30*time.Second)

	if err := deleteFromNode(context.Background(), nodes[0], key, "node1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	waitKeyDeleted(t, key, 30*time.Second)

	// Re-write with a +2s wall offset to guarantee the new HLC beats the
	// tombstone's clock, well inside the 10s TTL window.
	if err := writeToNode(context.Background(), nodes[1], key, "alive2", "node2", int64(2*time.Second)); err != nil {
		t.Fatalf("resurrection write: %v", err)
	}
	waitKeyValue(t, key, "alive2", 30*time.Second)
}

// TestFileWatcherPropagates exercises the local file-watcher path end-to-end:
// edit settings.json inside node1's container, fsnotify fires, Logic emits an
// onChange, the CRDT stamps an HLC and broadcasts, peers converge. Bypasses the
// gRPC Sync ingress all other tests use.
func TestFileWatcherPropagates(t *testing.T) {
	node1Container := envOr("NODE1_CONTAINER", "node1")
	key := "fwatcher_key"
	value := "from_file_edit"

	// Read current settings.json so we can preserve existing keys.
	// Overwriting with just our key would tombstone every other entry.
	out, err := exec.Command("docker", "exec", node1Container, "cat", "/data/settings.json").CombinedOutput()
	if err != nil {
		t.Fatalf("read settings.json: %v\n%s", err, out)
	}
	current := make(map[string]string)
	if err := json.Unmarshal(out, &current); err != nil {
		t.Fatalf("parse settings.json: %v\n%s", err, out)
	}
	current[key] = value

	newData, err := json.Marshal(current)
	if err != nil {
		t.Fatalf("marshal settings: %v", err)
	}

	// Pipe new contents into settings.json from outside the controller's
	// write path so only fsnotify can carry the change into the CRDT.
	cmd := exec.Command("docker", "exec", "-i", node1Container, "sh", "-c", "cat > /data/settings.json")
	cmd.Stdin = bytes.NewReader(newData)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("write settings.json: %v\n%s", err, out)
	}

	waitKeyValue(t, key, value, 30*time.Second)
}

// TestSIGKILLRecovery proves that an ungraceful crash leaves crdt_state.json
// intact and that the node loads its prior state on restart. Peers are stopped
// before the kill so they cannot refill node1's state via anti-entropy,
// isolating the test to node1's local persistence. The heartbeat ticker keeps
// last_heartbeat fresh while node1 is running, so even though SIGKILL skips
// the defer that updates last_shutdown, lastAliveTime still resolves to a
// recent timestamp and fixNodeState takes the Init() branch.
func TestSIGKILLRecovery(t *testing.T) {
	node1Container := envOr("NODE1_CONTAINER", "node1")
	node2Container := envOr("NODE2_CONTAINER", "node2")
	node3Container := envOr("NODE3_CONTAINER", "node3")

	key := "sigkill_key"

	if err := writeToNode(context.Background(), nodes[0], key, "survives_kill", "node1", 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	waitKeyValue(t, key, "survives_kill", 30*time.Second)

	// Stop peers — node1's state must come from disk, not from a re-Sync.
	stopNode(t, node2Container)
	stopNode(t, node3Container)
	defer func() {
		startNode(t, node2Container)
		startNode(t, node3Container)
		waitForNodeUp(t, nodes[1], 15*time.Second)
		waitForNodeUp(t, nodes[2], 15*time.Second)
	}()

	killNode(t, node1Container)
	startNode(t, node1Container)
	waitForNodeUp(t, nodes[0], 15*time.Second)

	// Query node1 directly. Peers are down so any returned value must come
	// from node1's reloaded crdt_state.json.
	resp, err := nodes[0].Sync(context.Background(), &userpb.SyncRequest{})
	if err != nil {
		t.Fatalf("sync after restart: %v", err)
	}
	for _, e := range resp.NewerEntries {
		if e.Key == key {
			if e.Deleted {
				t.Fatalf("key %q tombstoned after SIGKILL+restart", key)
			}
			if e.Value != "survives_kill" {
				t.Fatalf("key %q = %q after SIGKILL+restart, want survives_kill", key, e.Value)
			}
			return
		}
	}
	t.Fatalf("key %q absent on node1 after SIGKILL+restart — local persistence broken", key)
}
