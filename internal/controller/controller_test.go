package controller

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gdr00/distributed-server-update/internal/types"
)

// helpers

func setupWorkDir(t *testing.T, settings types.Settings) (string, string) {
	t.Helper()
	workDir := t.TempDir()
	settingsDir := t.TempDir()
	settingsPath := filepath.Join(settingsDir, "settings.json")
	cfg := types.Config{SettingsPath: settingsPath, CRDTWorkdir: workDir}

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

func setupEmptyWorkDir(t *testing.T) string {
	t.Helper()
	workDir := t.TempDir()
	if err := InitEmptyNode(workDir); err != nil {
		t.Fatalf("InitEmptyNode failed: %v", err)
	}
	return workDir
}

func newTestController(t *testing.T, workDir string, settingsPath string, port uint16, peers []string) *Controller {
	t.Helper()
	cfg := types.Config{
		CRDTWorkdir:   workDir,
		SettingsPath:  settingsPath,
		GRPCPort:      port,
		PeerAddresses: peers,
	}
	return New(cfg)
}

// InitNode tests

func TestInitNode_CreatesNodeIDAndState(t *testing.T) {
	workDir := t.TempDir()
	settingsDir := t.TempDir()
	settingsPath := filepath.Join(settingsDir, "settings.json")
	cfg := types.Config{SettingsPath: settingsPath, CRDTWorkdir: workDir}

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
	cfg := types.Config{SettingsPath: settingsPath, CRDTWorkdir: workDir}

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
	cfg := types.Config{SettingsPath: settingsPath, CRDTWorkdir: workDir}

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
	cfg := types.Config{SettingsPath: "/nonexistent/settings.json", CRDTWorkdir: workDir}
	err := InitNode(cfg)
	if err == nil {
		t.Fatal("expected error for missing settings file")
	}
}

// InitEmptyNode tests

func TestInitEmptyNode_CreatesFiles(t *testing.T) {
	workDir := t.TempDir()
	if err := InitEmptyNode(workDir); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, err := os.Stat(filepath.Join(workDir, "node_id")); err != nil {
		t.Fatal("expected node_id to exist")
	}

	stateData, err := os.ReadFile(filepath.Join(workDir, "crdt_state.json"))
	if err != nil {
		t.Fatal("expected crdt_state.json to exist")
	}
	if string(stateData) != "{}" {
		t.Fatalf("expected empty state, got %s", stateData)
	}
}

func TestInitEmptyNode_CreatesWorkDir(t *testing.T) {
	base := t.TempDir()
	workDir := filepath.Join(base, "nested", "dir")
	if err := InitEmptyNode(workDir); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := os.Stat(workDir); err != nil {
		t.Fatal("expected workDir to be created")
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
	defer cancel()

	go ctrl.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	ctrl.crdt.NotifyRemote(types.SettingEntry{
		Key:   "theme",
		Value: "light",
		Clock: types.HLC{
			WallTime: time.Now().UnixNano() + int64(time.Hour), // guaranteed future
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
	cfg := types.Config{
		CRDTWorkdir:  workDir,
		SettingsPath: settingsPath,
		GRPCPort:     1, // privileged port — should fail without root
	}
	ctrl := New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := ctrl.Run(ctx)
	if err == nil {
		t.Fatal("expected error for privileged port")
	}
}
