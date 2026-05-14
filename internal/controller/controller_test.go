package controller

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gdr00/distributed-server-update/internal/crdt"
	"github.com/gdr00/distributed-server-update/internal/network"
	"github.com/gdr00/distributed-server-update/internal/network/userpb"
	"github.com/gdr00/distributed-server-update/internal/types"
	"google.golang.org/grpc"
)

// helpers

func setupWorkDir(t *testing.T, settings types.Settings) (string, string) {
	t.Helper()
	workDir := t.TempDir()
	settingsDir := t.TempDir()
	settingsPath := filepath.Join(settingsDir, "settings.json")
	cfg := Config{SettingsPath: settingsPath, CRDTWorkdir: workDir}

	// write settings file
	data, _ := json.MarshalIndent(settings, "", "  ")
	if err := os.WriteFile(settingsPath, data, 0600); err != nil {
		t.Fatal(err)
	}

	// init the node
	if err := InitNode(cfg); err != nil {
		t.Fatalf("InitNode failed: %v", err)
	}

	return workDir, settingsPath
}

func newTestController(t *testing.T, workDir string, settingsPath string, port uint16, peers []string) *Controller {
	t.Helper()
	cfg := Config{
		CRDTWorkdir:   workDir,
		SettingsPath:  settingsPath,
		GRPCPort:      port,
		PeerAddresses: peers,
	}
	ctrl, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return ctrl
}

// InitNode tests

func TestInitNode_CreatesNodeIDAndState(t *testing.T) {
	workDir := t.TempDir()
	settingsDir := t.TempDir()
	settingsPath := filepath.Join(settingsDir, "settings.json")
	cfg := Config{SettingsPath: settingsPath, CRDTWorkdir: workDir}

	data, _ := json.MarshalIndent(types.Settings{"theme": "dark"}, "", "  ")
	os.WriteFile(settingsPath, data, 0600)

	if err := InitNode(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, err := os.Stat(filepath.Join(workDir, "node_id")); err != nil {
		t.Fatal("expected node_id to exist")
	}
	if _, err := os.Stat(filepath.Join(workDir, "crdt_state.json")); err != nil {
		t.Fatal("expected crdt_state.json to exist")
	}
}

func TestInitNode_StateContainsSettings(t *testing.T) {
	workDir := t.TempDir()
	settingsDir := t.TempDir()
	settingsPath := filepath.Join(settingsDir, "settings.json")
	cfg := Config{SettingsPath: settingsPath, CRDTWorkdir: workDir}

	data, _ := json.MarshalIndent(types.Settings{"theme": "dark", "lang": "en"}, "", "  ")
	os.WriteFile(settingsPath, data, 0600)
	InitNode(cfg)

	stateData, _ := os.ReadFile(filepath.Join(workDir, "crdt_state.json"))
	var state map[string]types.SettingEntry
	json.Unmarshal(stateData, &state)

	if state["theme"].Value != "dark" {
		t.Fatalf("expected dark, got %s", state["theme"].Value)
	}
	if state["lang"].Value != "en" {
		t.Fatalf("expected en, got %s", state["lang"].Value)
	}
}

func TestInitNode_EntriesHaveNonZeroClock(t *testing.T) {
	workDir := t.TempDir()
	settingsDir := t.TempDir()
	settingsPath := filepath.Join(settingsDir, "settings.json")
	cfg := Config{SettingsPath: settingsPath, CRDTWorkdir: workDir}

	data, _ := json.MarshalIndent(types.Settings{"theme": "dark"}, "", "  ")
	os.WriteFile(settingsPath, data, 0600)
	InitNode(cfg)

	stateData, _ := os.ReadFile(filepath.Join(workDir, "crdt_state.json"))
	var state map[string]types.SettingEntry
	json.Unmarshal(stateData, &state)

	if state["theme"].Clock.WallTime == 0 {
		t.Fatal("expected non-zero clock after init")
	}
	if state["theme"].Clock.NodeID == "" {
		t.Fatal("expected nodeID to be set")
	}
}

func TestInitNode_MissingSettingsFileReturnsError(t *testing.T) {
	workDir := t.TempDir()
	cfg := Config{SettingsPath: "/nonexistent/settings.json", CRDTWorkdir: workDir}
	err := InitNode(cfg)
	if err == nil {
		t.Fatal("expected error for missing settings file")
	}
}

// InitEmptyNode tests

func TestInitEmptyNode_CreatesFiles(t *testing.T) {
	cfg := Config{
		CRDTWorkdir:  t.TempDir(),
		SettingsPath: filepath.Join(t.TempDir(), "settings.json"),
	}
	if err := InitEmptyNode(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, err := os.Stat(filepath.Join(cfg.CRDTWorkdir, "node_id")); err != nil {
		t.Fatal("expected node_id to exist")
	}

	stateData, err := os.ReadFile(filepath.Join(cfg.CRDTWorkdir, "crdt_state.json"))
	if err != nil {
		t.Fatal("expected crdt_state.json to exist")
	}
	if string(stateData) != "{}" {
		t.Fatalf("expected empty state, got %s", stateData)
	}

	if _, err := os.Stat(cfg.SettingsPath); err != nil {
		t.Fatal("expected settings.json to exist")
	}
}

func TestInitEmptyNode_CreatesWorkDir(t *testing.T) {
	cfg := Config{
		CRDTWorkdir:  filepath.Join(t.TempDir(), "nested", "dir"),
		SettingsPath: filepath.Join(t.TempDir(), "nested", "dir", "settings.json"),
	}
	if err := InitEmptyNode(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := os.Stat(cfg.CRDTWorkdir); err != nil {
		t.Fatal("expected workDir to be created")
	}
}

func TestRun_ReconcileDropsStaleSettingsEntries(t *testing.T) {
	workDir, settingsPath := setupWorkDir(t, types.Settings{"theme": "dark"})

	// inject a stale entry into settings.json that is NOT in CRDT state
	data, _ := json.MarshalIndent(types.Settings{"theme": "dark", "stale": "value"}, "", "  ")
	os.WriteFile(settingsPath, data, 0600)

	ctrl := newTestController(t, workDir, settingsPath, 0, nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ctrl.Run(ctx) }()
	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done

	settings := make(map[string]string)
	raw, _ := os.ReadFile(settingsPath)
	json.Unmarshal(raw, &settings)

	if _, exists := settings["stale"]; exists {
		t.Fatal("expected stale entry to be removed by reconcile")
	}
	if settings["theme"] != "dark" {
		t.Fatalf("expected theme=dark, got %v", settings)
	}
}

// Run tests

func TestRun_StartsAndStopsCleanly(t *testing.T) {
	workDir, settingsPath := setupWorkDir(t, types.Settings{"theme": "dark"})
	ctrl := newTestController(t, workDir, settingsPath, 0, nil)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- ctrl.Run(ctx)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after context cancel")
	}
}

func TestRun_LocalFileChangePropagatesToCRDT(t *testing.T) {
	workDir, settingsPath := setupWorkDir(t, types.Settings{"theme": "dark"})
	ctrl := newTestController(t, workDir, settingsPath, 0, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go ctrl.Run(ctx)
	time.Sleep(100 * time.Millisecond) // let watcher start

	// simulate file edit
	data, _ := json.MarshalIndent(types.Settings{"theme": "light"}, "", "  ")
	os.WriteFile(settingsPath, data, 0600)

	// wait for CRDT to pick up the change
	time.Sleep(500 * time.Millisecond)

	result := ctrl.crdt.Get("theme")
	if result.Value != "light" {
		t.Fatalf("expected light, got %s", result.Value)
	}
}

func TestRun_RemoteUpdateWritesToFile(t *testing.T) {
	workDir, settingsPath := setupWorkDir(t, types.Settings{"theme": "dark"})
	ctrl := newTestController(t, workDir, settingsPath, 0, nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { ctrl.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })

	time.Sleep(100 * time.Millisecond)

	ctrl.crdt.NotifyRemote(types.SettingEntry{
		Key:   "theme",
		Value: "light",
		Clock: types.HLC{
			WallTime: time.Now().UnixNano() + int64(time.Second), // guaranteed newer, within 1 min limit
			NodeID:   "remote-node",
		},
	})

	time.Sleep(300 * time.Millisecond)

	data, _ := os.ReadFile(settingsPath)
	var settings types.Settings
	json.Unmarshal(data, &settings)

	if settings["theme"] != "light" {
		t.Fatalf("expected light in settings file, got %s", settings["theme"])
	}
}
func TestRun_InvalidRPCPortReturnsError(t *testing.T) {
	workDir, settingsPath := setupWorkDir(t, types.Settings{})
	cfg := Config{
		CRDTWorkdir:  workDir,
		SettingsPath: settingsPath,
		GRPCPort:     1, // privileged port — should fail without root
	}
	ctrl, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := ctrl.Run(ctx); err == nil {
		t.Fatal("expected error for privileged port")
	}
}

func TestNew_NotInitialized(t *testing.T) {
	cfg := Config{
		CRDTWorkdir:  t.TempDir(), // no node_id — not initialized
		SettingsPath: filepath.Join(t.TempDir(), "settings.json"),
	}
	_, err := New(cfg)
	if err == nil {
		t.Fatal("expected error for uninitialized crdt dir")
	}
}

func TestInitNode_CRDTInitError(t *testing.T) {
	dir := t.TempDir()
	// place a file where the crdt subdir needs to be, blocking MkdirAll
	blocker := filepath.Join(dir, "crdt")
	if err := os.WriteFile(blocker, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	settingsDir := t.TempDir()
	settingsPath := filepath.Join(settingsDir, "settings.json")
	data, _ := json.MarshalIndent(types.Settings{"k": "v"}, "", "  ")
	os.WriteFile(settingsPath, data, 0600)

	cfg := Config{SettingsPath: settingsPath, CRDTWorkdir: filepath.Join(blocker, "sub")}
	if err := InitNode(cfg); err == nil {
		t.Fatal("expected error when crdt dir cannot be created")
	}
}

func TestInitEmptyNode_WorkDirNotCreatable(t *testing.T) {
	dir := t.TempDir()
	blocker := filepath.Join(dir, "blocker")
	os.WriteFile(blocker, []byte("x"), 0600)

	cfg := Config{
		CRDTWorkdir:  filepath.Join(blocker, "sub"),
		SettingsPath: filepath.Join(t.TempDir(), "settings.json"),
	}
	if err := InitEmptyNode(cfg); err == nil {
		t.Fatal("expected error when workdir cannot be created")
	}
}

func TestInitEmptyNode_WriteNodeIDError(t *testing.T) {
	dir := t.TempDir()
	os.Chmod(dir, 0500)
	defer os.Chmod(dir, 0700)

	cfg := Config{
		CRDTWorkdir:  dir,
		SettingsPath: filepath.Join(t.TempDir(), "settings.json"),
	}
	if err := InitEmptyNode(cfg); err == nil {
		t.Fatal("expected error when node_id cannot be written")
	}
}

func TestInitEmptyNode_SettingsWriteError(t *testing.T) {
	dir := t.TempDir()
	settingsDir := t.TempDir()
	os.Chmod(settingsDir, 0500)
	defer os.Chmod(settingsDir, 0700)

	cfg := Config{
		CRDTWorkdir:  dir,
		SettingsPath: filepath.Join(settingsDir, "settings.json"),
	}
	if err := InitEmptyNode(cfg); err == nil {
		t.Fatal("expected error writing settings file")
	}
}

func TestInitEmptyNode_SettingsDirError(t *testing.T) {
	dir := t.TempDir()
	settingsBase := t.TempDir()
	blocker := filepath.Join(settingsBase, "blocker")
	os.WriteFile(blocker, []byte("x"), 0600)

	cfg := Config{
		CRDTWorkdir:  dir,
		SettingsPath: filepath.Join(blocker, "settings.json"),
	}
	if err := InitEmptyNode(cfg); err == nil {
		t.Fatal("expected error when settings dir cannot be created")
	}
}

func TestRun_WithPeerAddress(t *testing.T) {
	workDir, settingsPath := setupWorkDir(t, types.Settings{})
	cfg := Config{
		CRDTWorkdir:   workDir,
		SettingsPath:  settingsPath,
		GRPCPort:      0,
		PeerAddresses: []string{"localhost:19999"}, // lazy dial — no server needed
	}
	ctrl, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- ctrl.Run(ctx) }()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop after context cancel")
	}
}

func TestRun_OnFileSyncWriteError(t *testing.T) {
	workDir, settingsPath := setupWorkDir(t, types.Settings{"theme": "dark"})
	ctrl := newTestController(t, workDir, settingsPath, 0, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go ctrl.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	// make settings file unwritable so OnFileSync's Write call fails
	os.Chmod(settingsPath, 0400)
	defer os.Chmod(settingsPath, 0600)

	ctrl.crdt.NotifyRemote(types.SettingEntry{
		Key:   "theme",
		Value: "light",
		Clock: types.HLC{
			WallTime: time.Now().UnixNano() + int64(time.Second),
			NodeID:   "remote-node",
		},
	})

	time.Sleep(300 * time.Millisecond)
	if ctx.Err() != nil {
		t.Fatal("controller exited unexpectedly")
	}
}

// syncAllPeers tests

func startPeerServer(t *testing.T, snap types.Snapshot) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	srv := network.NewUpdateServer(func() types.Snapshot { return snap }, nil)
	grpcSrv := grpc.NewServer()
	userpb.RegisterUpdateServiceServer(grpcSrv, srv)
	go grpcSrv.Serve(lis)
	t.Cleanup(func() {
		grpcSrv.Stop()
		lis.Close()
	})
	return lis.Addr().String()
}

func TestSyncAllPeers_NoPeers(t *testing.T) {
	workDir, settingsPath := setupWorkDir(t, types.Settings{"theme": "dark"})
	ctrl := newTestController(t, workDir, settingsPath, 0, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ctrl.crdt.Run(ctx)

	ctrl.syncAllPeers(ctx) // no panic, no-op
}

func TestSyncAllPeers_DeliverNewerEntriesToCRDT(t *testing.T) {
	workDir, settingsPath := setupWorkDir(t, types.Settings{"theme": "dark"})

	peerSnap := types.Snapshot{
		Entries: map[string]types.SettingEntry{
			"lang": {
				Key:   "lang",
				Value: "fr",
				Clock: types.HLC{
					WallTime: time.Now().UnixNano() + int64(time.Second),
					NodeID:   "peer-node",
				},
			},
		},
	}
	peerAddr := startPeerServer(t, peerSnap)

	cfg := Config{
		CRDTWorkdir:   workDir,
		SettingsPath:  settingsPath,
		GRPCPort:      0,
		PeerAddresses: []string{peerAddr},
	}
	ctrl, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ctrl.crdt.Run(ctx)

	ctrl.syncAllPeers(ctx)
	time.Sleep(300 * time.Millisecond)

	if result := ctrl.crdt.Get("lang"); result.Value != "fr" {
		t.Fatalf("expected fr, got %q", result.Value)
	}
}

// runAntiEntropy / runTombstoneGC ticker tests

func TestRunAntiEntropy_TickerFiresSyncAllPeers(t *testing.T) {
	workDir, settingsPath := setupWorkDir(t, types.Settings{"theme": "dark"})

	peerSnap := types.Snapshot{
		Entries: map[string]types.SettingEntry{
			"lang": {
				Key:   "lang",
				Value: "fr",
				Clock: types.HLC{WallTime: time.Now().UnixNano() + int64(time.Second), NodeID: "peer"},
			},
		},
	}
	peerAddr := startPeerServer(t, peerSnap)

	cfg := Config{
		CRDTWorkdir:   workDir,
		SettingsPath:  settingsPath,
		GRPCPort:      0,
		PeerAddresses: []string{peerAddr},
	}
	ctrl, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctrl.antiEntropyInterval = 10 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ctrl.crdt.Run(ctx)
	go ctrl.runAntiEntropy(ctx)

	time.Sleep(200 * time.Millisecond)

	if result := ctrl.crdt.Get("lang"); result.Value != "fr" {
		t.Fatalf("expected fr after anti-entropy tick, got %q", result.Value)
	}
}

func TestRunTombstoneGC_TickerFiresPurgeTombstones(t *testing.T) {
	workDir, settingsPath := setupWorkDir(t, types.Settings{})
	ctrl, err := New(Config{CRDTWorkdir: workDir, SettingsPath: settingsPath})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctrl.gcInterval = 10 * time.Millisecond
	ctrl.tombstoneTTL = 0 // purge all tombstones older than now

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ctrl.crdt.Run(ctx)

	// inject a tombstone directly
	ctrl.crdt.NotifyRemote(types.SettingEntry{
		Key:     "dead",
		Deleted: true,
		Clock:   types.HLC{WallTime: 1, NodeID: "n"},
	})
	time.Sleep(50 * time.Millisecond) // let CRDT process it

	go ctrl.runTombstoneGC(ctx)
	time.Sleep(200 * time.Millisecond)

	if entry := ctrl.crdt.Get("dead"); entry.Key != "" {
		t.Fatalf("expected tombstone purged, still present: %+v", entry)
	}
}

// shutdown time tests

func TestSaveAndLoadShutdownTime(t *testing.T) {
	workDir, settingsPath := setupWorkDir(t, types.Settings{})
	ctrl := newTestController(t, workDir, settingsPath, 0, nil)

	before := time.Now().UnixNano()
	ctrl.saveShutdownTime()
	after := time.Now().UnixNano()

	got := ctrl.checkLastShutdown()
	if got < before || got > after {
		t.Fatalf("saved timestamp %d outside [%d, %d]", got, before, after)
	}
}

func TestCheckLastShutdown_MissingFile(t *testing.T) {
	workDir, settingsPath := setupWorkDir(t, types.Settings{})
	ctrl := newTestController(t, workDir, settingsPath, 0, nil)
	// no last_shutdown file written — fresh node
	os.Remove(filepath.Join(workDir, "last_shutdown"))

	if got := ctrl.checkLastShutdown(); got != -1 {
		t.Fatalf("expected -1 for missing file, got %d", got)
	}
}

func TestFixNodeState_RecentShutdown_LoadsExistingState(t *testing.T) {
	workDir, settingsPath := setupWorkDir(t, types.Settings{"k": "v"})
	ctrl := newTestController(t, workDir, settingsPath, 0, nil)

	// re-initialize crdt (simulate restart with recent shutdown)
	ctrl.crdt = crdt.New(workDir)
	recent := time.Now().UnixNano() - int64(time.Hour)
	if err := ctrl.fixNodeState(recent); err != nil {
		t.Fatalf("fixNodeState: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ctrl.crdt.Run(ctx)
	time.Sleep(20 * time.Millisecond)

	if got := ctrl.crdt.Get("k"); got.Value != "v" {
		t.Fatalf("expected existing state loaded, got %q", got.Value)
	}
}

func TestFixNodeState_StaleShutdown_WipesState(t *testing.T) {
	workDir, settingsPath := setupWorkDir(t, types.Settings{"k": "v"})
	ctrl := newTestController(t, workDir, settingsPath, 0, nil)

	// re-initialize crdt (simulate restart with stale shutdown)
	ctrl.crdt = crdt.New(workDir)
	stale := time.Now().UnixNano() - ctrl.tombstoneTTL - int64(time.Hour)
	if err := ctrl.fixNodeState(stale); err != nil {
		t.Fatalf("fixNodeState: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ctrl.crdt.Run(ctx)
	time.Sleep(20 * time.Millisecond)

	if got := ctrl.crdt.Get("k"); got.Key != "" {
		t.Fatalf("expected empty state after stale shutdown, got %+v", got)
	}
}

func TestFixNodeState_MissingFile_LoadsExistingState(t *testing.T) {
	workDir, settingsPath := setupWorkDir(t, types.Settings{"k": "v"})
	ctrl := newTestController(t, workDir, settingsPath, 0, nil)

	ctrl.crdt = crdt.New(workDir)
	if err := ctrl.fixNodeState(-1); err != nil {
		t.Fatalf("fixNodeState with -1: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ctrl.crdt.Run(ctx)
	time.Sleep(20 * time.Millisecond)

	if got := ctrl.crdt.Get("k"); got.Value != "v" {
		t.Fatalf("expected existing state loaded, got %q", got.Value)
	}
}
