package systemtest

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/gdr00/distributed-server-update/internal/network/userpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

// nodeTracker mirrors a node's state locally.
// It bootstraps with one Sync, then refreshes every 4 s to catch anti-entropy
// propagated entries, and also drains the broadcast stream when available.
type nodeTracker struct {
	mu      sync.RWMutex
	entries map[string]*userpb.SettingEntry
	client  userpb.UpdateServiceClient
}

func newNodeTracker(ctx context.Context, c userpb.UpdateServiceClient) *nodeTracker {
	tr := &nodeTracker{
		entries: make(map[string]*userpb.SettingEntry),
		client:  c,
	}
	go tr.run(ctx)
	return tr
}

func (tr *nodeTracker) run(ctx context.Context) {
	tr.refresh(ctx)
	go tr.subscribe(ctx)

	ticker := time.NewTicker(4 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tr.refresh(ctx)
		}
	}
}

// refresh does a single Sync to pull the node's full current state.
func (tr *nodeTracker) refresh(ctx context.Context) {
	resp, err := tr.client.Sync(ctx, &userpb.SyncRequest{})
	if err != nil {
		return
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	for _, e := range resp.NewerEntries {
		tr.entries[e.Key] = e
	}
}

// subscribe drains the broadcast stream for immediate delivery of local-origin changes.
func (tr *nodeTracker) subscribe(ctx context.Context) {
	stream, err := tr.client.SubscribeStateUpdates(ctx, &emptypb.Empty{})
	if err != nil {
		return
	}
	for {
		update, err := stream.Recv()
		if err != nil {
			return
		}
		e := update.Entry
		tr.mu.Lock()
		tr.entries[e.Key] = e
		tr.mu.Unlock()
	}
}

func (tr *nodeTracker) hasValue(key, wantVal string) bool {
	tr.mu.RLock()
	defer tr.mu.RUnlock()
	e, ok := tr.entries[key]
	return ok && !e.Deleted && e.Value == wantVal
}

func (tr *nodeTracker) isDeleted(key string) bool {
	tr.mu.RLock()
	defer tr.mu.RUnlock()
	e, ok := tr.entries[key]
	return !ok || e.Deleted
}

// ── globals ───────────────────────────────────────────────────────────────────

var trackers [3]*nodeTracker

func TestMain(m *testing.M) {
	addrs := [3]string{
		envOr("NODE1_ADDR", "localhost:9001"),
		envOr("NODE2_ADDR", "localhost:9002"),
		envOr("NODE3_ADDR", "localhost:9003"),
	}

	ctx, cancel := context.WithCancel(context.Background())

	conns := make([]*grpc.ClientConn, 3)
	clients := make([]userpb.UpdateServiceClient, 3)
	for i, addr := range addrs {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Fprintf(os.Stderr, "connect %s: %v\n", addr, err)
			cancel()
			os.Exit(1)
		}
		conns[i] = conn
		clients[i] = userpb.NewUpdateServiceClient(conn)
	}

	for i, c := range clients {
		trackers[i] = newNodeTracker(ctx, c)
	}

	code := m.Run()
	cancel()
	for _, c := range conns {
		c.Close()
	}
	os.Exit(code)
}

// ── helpers ───────────────────────────────────────────────────────────────────

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

// waitKeyValue blocks until all trackers report key=wantVal or timeout expires.
// Checks local state only — no RPCs in the wait loop.
func waitKeyValue(t *testing.T, key, wantVal string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if allTrackersHaveValue(key, wantVal) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	// Report actual state from a fresh read on failure.
	for i, tr := range trackers {
		resp, _ := tr.client.Sync(context.Background(), &userpb.SyncRequest{})
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

func allTrackersHaveValue(key, wantVal string) bool {
	for _, tr := range trackers {
		if !tr.hasValue(key, wantVal) {
			return false
		}
	}
	return true
}

// waitKeyDeleted blocks until all trackers report key absent or tombstoned.
func waitKeyDeleted(t *testing.T, key string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if allTrackersDeleted(key) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("convergence timeout: key %q still alive on some node", key)
}

func allTrackersDeleted(key string) bool {
	for _, tr := range trackers {
		if !tr.isDeleted(key) {
			return false
		}
	}
	return true
}

// ── tests ─────────────────────────────────────────────────────────────────────

// TestInitialStateConverges verifies node1's seed data ("da"="ddd") propagates
// to node2 and node3 via anti-entropy on startup.
func TestInitialStateConverges(t *testing.T) {
	waitKeyValue(t, "da", "ddd", 30*time.Second)
}

// TestWritePropagatesFromNode1 injects an entry into node1 and checks all nodes converge.
func TestWritePropagatesFromNode1(t *testing.T) {
	if err := writeToNode(context.Background(), trackers[0].client, "prop_n1", "from_node1", "node1", 0); err != nil {
		t.Fatalf("write to node1: %v", err)
	}
	waitKeyValue(t, "prop_n1", "from_node1", 30*time.Second)
}

// TestWritePropagatesFromNode2 injects an entry into node2 and checks all nodes converge.
func TestWritePropagatesFromNode2(t *testing.T) {
	if err := writeToNode(context.Background(), trackers[1].client, "prop_n2", "from_node2", "node2", 0); err != nil {
		t.Fatalf("write to node2: %v", err)
	}
	waitKeyValue(t, "prop_n2", "from_node2", 30*time.Second)
}

// TestWritePropagatesFromNode3 injects an entry into node3 and checks all nodes converge.
func TestWritePropagatesFromNode3(t *testing.T) {
	if err := writeToNode(context.Background(), trackers[2].client, "prop_n3", "from_node3", "node3", 0); err != nil {
		t.Fatalf("write to node3: %v", err)
	}
	waitKeyValue(t, "prop_n3", "from_node3", 30*time.Second)
}

// TestTombstonePropagates writes a key, waits for convergence, then deletes it
// from node1 and verifies the tombstone reaches all nodes.
func TestTombstonePropagates(t *testing.T) {
	key := "tombstone_key"

	if err := writeToNode(context.Background(), trackers[0].client, key, "alive", "node1", 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	waitKeyValue(t, key, "alive", 30*time.Second)

	if err := deleteFromNode(context.Background(), trackers[0].client, key, "node1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	waitKeyDeleted(t, key, 30*time.Second)
}

// TestHLCConflictResolution writes the same key to two nodes with different HLC
// wall times and verifies every node converges to the entry with the higher clock.
func TestHLCConflictResolution(t *testing.T) {
	key := "hlc_conflict"

	// Loser: wall = now - 2s (lower HLC)
	if err := writeToNode(context.Background(), trackers[0].client, key, "loser", "test-loser", -int64(2*time.Second)); err != nil {
		t.Fatalf("write loser to node1: %v", err)
	}
	// Winner: wall = now + 2s (higher HLC); applyRemote path skips future-clock check
	if err := writeToNode(context.Background(), trackers[1].client, key, "winner", "test-winner", int64(2*time.Second)); err != nil {
		t.Fatalf("write winner to node2: %v", err)
	}

	waitKeyValue(t, key, "winner", 30*time.Second)
}

// TestBidirectionalSync writes a distinct key to node1 and another to node2,
// then verifies all nodes end up with both keys.
func TestBidirectionalSync(t *testing.T) {
	if err := writeToNode(context.Background(), trackers[0].client, "bidir_a", "val_a", "node1", 0); err != nil {
		t.Fatalf("write bidir_a: %v", err)
	}
	if err := writeToNode(context.Background(), trackers[1].client, "bidir_b", "val_b", "node2", 0); err != nil {
		t.Fatalf("write bidir_b: %v", err)
	}

	waitKeyValue(t, "bidir_a", "val_a", 30*time.Second)
	waitKeyValue(t, "bidir_b", "val_b", 30*time.Second)
}

// TestMultipleKeysConverge fans writes across all three nodes and asserts every
// node ends up with every key.
func TestMultipleKeysConverge(t *testing.T) {
	writes := []struct {
		client userpb.UpdateServiceClient
		key    string
		value  string
		nodeID string
	}{
		{trackers[0].client, "multi_1", "v1", "node1"},
		{trackers[1].client, "multi_2", "v2", "node2"},
		{trackers[2].client, "multi_3", "v3", "node3"},
		{trackers[0].client, "multi_4", "v4", "node1"},
		{trackers[1].client, "multi_5", "v5", "node2"},
	}

	for _, w := range writes {
		if err := writeToNode(context.Background(), w.client, w.key, w.value, w.nodeID, 0); err != nil {
			t.Fatalf("write %s: %v", w.key, err)
		}
	}
	for _, w := range writes {
		waitKeyValue(t, w.key, w.value, 30*time.Second)
	}
}

// TestOverwriteConverges writes a key, waits for convergence, then overwrites it
// with a newer clock and verifies all nodes pick up the update.
func TestOverwriteConverges(t *testing.T) {
	key := "overwrite_key"

	if err := writeToNode(context.Background(), trackers[0].client, key, "v1", "node1", -int64(time.Second)); err != nil {
		t.Fatalf("first write: %v", err)
	}
	waitKeyValue(t, key, "v1", 30*time.Second)

	if err := writeToNode(context.Background(), trackers[0].client, key, "v2", "node1", 0); err != nil {
		t.Fatalf("second write: %v", err)
	}
	waitKeyValue(t, key, "v2", 30*time.Second)
}

// TestOlderWriteDoesNotOverwrite sends a write with an older HLC to a node that
// already has a newer value and verifies the newer value is preserved everywhere.
func TestOlderWriteDoesNotOverwrite(t *testing.T) {
	key := "stale_write_key"

	if err := writeToNode(context.Background(), trackers[0].client, key, "current", "node1", 0); err != nil {
		t.Fatalf("write current: %v", err)
	}
	waitKeyValue(t, key, "current", 30*time.Second)

	// Attempt to overwrite with a stale (older) clock — CRDT must reject it.
	if err := writeToNode(context.Background(), trackers[1].client, key, "stale", "node2", -int64(5*time.Second)); err != nil {
		t.Fatalf("write stale: %v", err)
	}

	// Give time for any incorrect propagation to surface, then confirm value held.
	time.Sleep(8 * time.Second)
	waitKeyValue(t, key, "current", 5*time.Second)
}
