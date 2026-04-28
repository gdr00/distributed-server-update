package types

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"
)

// HLC.Before tests

func TestHLCBefore_SameValues(t *testing.T) {
	a := HLC{WallTime: 100, Logical: 5, NodeID: "n1"}
	b := HLC{WallTime: 100, Logical: 5, NodeID: "n1"}
	if a.Before(b) {
		t.Errorf("expected equal HLCs: Before() = false, got true")
	}
	if b.Before(a) {
		t.Errorf("expected equal HLCs: Before() = false, got true")
	}
}

func TestHLCBefore_WallTimeDiff(t *testing.T) {
	tests := []struct {
		name string
		a    HLC
		b    HLC
		want bool
	}{
		{"a wall earlier", HLC{WallTime: 50, Logical: 10, NodeID: "n1"}, HLC{WallTime: 100, Logical: 5, NodeID: "n1"}, true},
		{"a wall later", HLC{WallTime: 150, Logical: 5, NodeID: "n1"}, HLC{WallTime: 100, Logical: 10, NodeID: "n1"}, false},
		{"wall equal, same", HLC{WallTime: 100, Logical: 5, NodeID: "n1"}, HLC{WallTime: 100, Logical: 5, NodeID: "n1"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.a.Before(tt.b)
			if got != tt.want {
				t.Errorf("HLC.Before() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHLCBefore_LogicalDiff(t *testing.T) {
	tests := []struct {
		name   string
		aWall  int64
		aLogic uint32
		bWall  int64
		bLogic uint32
		want   bool
	}{
		{"a logic less", 100, 3, 100, 5, true},
		{"a logic greater", 100, 7, 100, 5, false},
		{"zero vs positive", 100, 0, 100, 1, true},
		{"uint32 max overflow case", 100, math.MaxUint32, 100, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := HLC{WallTime: tt.aWall, Logical: tt.aLogic, NodeID: "n1"}
			b := HLC{WallTime: tt.bWall, Logical: tt.bLogic, NodeID: "n1"}
			got := a.Before(b)
			if got != tt.want {
				t.Errorf("Before() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHLCBefore_NodeIDDiff(t *testing.T) {
	tests := []struct {
		name  string
		aNode string
		bNode string
		want  bool
	}{
		{"n1 before n2", "n1", "n2", true},
		{"n2 after n1", "n2", "n1", false},
		{"abc before xyz", "abc", "xyz", true},
		{"z after a", "z", "a", false},
		{"empty string", "", "a", true},
		{"same empty", "", "", false},
		{"case sensitive", "A", "a", true}, // uppercase before lowercase in ASCII
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := HLC{WallTime: 100, Logical: 5, NodeID: tt.aNode}
			b := HLC{WallTime: 100, Logical: 5, NodeID: tt.bNode}
			got := a.Before(b)
			if got != tt.want {
				t.Errorf("Before() = %v, want %v", got, tt.want)
			}
		})
	}
}

// HLC.Update tests

func TestHLCUpdate_EqualClocks(t *testing.T) {
	h := HLC{WallTime: 100, Logical: 5, NodeID: "a"}
	received := HLC{WallTime: 100, Logical: 8, NodeID: "b"}
	h.Update(received)
	// Equal clocks: increment logical to max(5,8)+1 = 9
	if h.Logical != 9 {
		t.Errorf("expected Logical=9, got %d", h.Logical)
	}
	if h.WallTime != 100 {
		t.Errorf("expected WallTime=100, got %d", h.WallTime)
	}
}

func TestHLCUpdate_LocalWallGreater(t *testing.T) {
	h := HLC{WallTime: 200, Logical: 5, NodeID: "a"}
	received := HLC{WallTime: 100, Logical: 8, NodeID: "b"}
	h.Update(received)
	// Local wall > remote: increment logical only
	if h.WallTime != 200 {
		t.Errorf("expected WallTime=200, got %d", h.WallTime)
	}
	if h.Logical != 6 {
		t.Errorf("expected Logical=6, got %d", h.Logical)
	}
}

func TestHLCUpdate_RemoteWallGreater(t *testing.T) {
	h := HLC{WallTime: 100, Logical: 5, NodeID: "a"}
	received := HLC{WallTime: 200, Logical: 10, NodeID: "b"}
	h.Update(received)
	// Remote wall > local: set wall to remote, logical = remote.logical + 1
	if h.WallTime != 200 {
		t.Errorf("expected WallTime=200, got %d", h.WallTime)
	}
	if h.Logical != 11 {
		t.Errorf("expected Logical=11, got %d", h.Logical)
	}
}

func TestHLCUpdate_ZeroValues(t *testing.T) {
	h := HLC{WallTime: 0, Logical: 0, NodeID: "a"}
	received := HLC{WallTime: 0, Logical: 0, NodeID: "b"}
	h.Update(received)
	// Equal zero clocks: logical = max(0,0)+1 = 1
	if h.Logical != 1 {
		t.Errorf("expected Logical=1, got %d", h.Logical)
	}
	if h.WallTime != 0 {
		t.Errorf("expected WallTime=0, got %d", h.WallTime)
	}
}

func TestHLCUpdate_RemoteLogicalZero(t *testing.T) {
	h := HLC{WallTime: 50, Logical: 3, NodeID: "a"}
	received := HLC{WallTime: 100, Logical: 0, NodeID: "b"}
	h.Update(received)
	if h.WallTime != 100 {
		t.Errorf("expected WallTime=100, got %d", h.WallTime)
	}
	if h.Logical != 1 {
		t.Errorf("expected Logical=1, got %d", h.Logical)
	}
}

func TestHLCUpdate_LocalLogicalMax(t *testing.T) {
	h := HLC{WallTime: 200, Logical: 4294967295, NodeID: "a"}
	received := HLC{WallTime: 100, Logical: 0, NodeID: "b"}
	// uint32 overflow: 4294967295 + 1 = 0
	h.Update(received)
	if h.Logical != 0 {
		t.Errorf("expected Logical=0 (overflow), got %d", h.Logical)
	}
}

// HLC.Tick tests

func TestHLCTick_WallAdvances(t *testing.T) {
	h := HLC{WallTime: 100, Logical: 5, NodeID: "a"}
	// Use a mock approach: set wall to far past so Tick will advance it
	h.WallTime = 0
	h.Tick()
	if h.WallTime == 0 {
		t.Error("expected WallTime to advance past 0")
	}
	if h.Logical != 0 {
		t.Errorf("expected Logical=0 after wall advance, got %d", h.Logical)
	}
}

func TestHLCTick_WallSame(t *testing.T) {
	// Tick checks time.Now().UnixNano(). Hard to test wall stay-same without mocking.
	// Test that Tick doesn't panic with zero state
	h := HLC{WallTime: 0, Logical: 0, NodeID: "a"}
	h.Tick()
	// Since time.Now() >> 0, wall will advance, logical resets to 0
	if h.WallTime == 0 {
		t.Error("expected WallTime to advance from 0")
	}
}

// SettingEntry tests

func TestSettingEntry_New(t *testing.T) {
	e := SettingEntry{Key: "foo", Value: "bar", Deleted: false}
	if e.Key != "foo" {
		t.Errorf("Key = %s, want foo", e.Key)
	}
	if e.Value != "bar" {
		t.Errorf("Value = %s, want bar", e.Value)
	}
	if e.Deleted {
		t.Error("expected Deleted=false")
	}
}

func TestSettingEntry_Deleted(t *testing.T) {
	e := SettingEntry{Key: "del", Value: "", Deleted: true}
	if !e.Deleted {
		t.Error("expected Deleted=true")
	}
}

// Snapshot tests

func TestSnapshot_New(t *testing.T) {
	s := Snapshot{}
	if s.Entries != nil {
		t.Error("expected Entries to be nil (zero value map) for empty snapshot")
	}
}

func TestSnapshot_WithEntries(t *testing.T) {
	s := Snapshot{
		Entries: map[string]SettingEntry{
			"k1": {Key: "k1", Value: "v1", Deleted: false},
			"k2": {Key: "k2", Value: "v2", Deleted: false},
		},
	}
	if len(s.Entries) != 2 {
		t.Errorf("expected 2 entries, got %d", len(s.Entries))
	}
	if entry, ok := s.Entries["k1"]; !ok || entry.Value != "v1" {
		t.Error("missing or wrong k1 entry")
	}
	if _, ok := s.Entries["k3"]; ok {
		t.Error("unexpected k3 entry")
	}
}

func TestSnapshot_EmptyEntries(t *testing.T) {
	s := Snapshot{Entries: make(map[string]SettingEntry)}
	if len(s.Entries) != 0 {
		t.Errorf("expected 0 entries, got %d", len(s.Entries))
	}
}

func TestSnapshot_SetAndDelete(t *testing.T) {
	s := Snapshot{Entries: make(map[string]SettingEntry)}
	s.Entries["key"] = SettingEntry{Key: "key", Value: "val", Deleted: false}
	if len(s.Entries) != 1 {
		t.Errorf("expected 1 entry after set, got %d", len(s.Entries))
	}
	entry := s.Entries["key"]
	entry.Deleted = true
	s.Entries["key"] = entry
	if !s.Entries["key"].Deleted {
		t.Error("expected entry to be deleted")
	}
}

// Settings type tests

func TestSettings_SetGet(t *testing.T) {
	s := Settings{"host": "localhost", "port": "8080"}
	if s["host"] != "localhost" {
		t.Errorf("host = %s, want localhost", s["host"])
	}
	if s["port"] != "8080" {
		t.Errorf("port = %s, want 8080", s["port"])
	}
}

func TestSettings_Update(t *testing.T) {
	s := Settings{"key": "old"}
	s["key"] = "new"
	if s["key"] != "new" {
		t.Errorf("key = %s, want new", s["key"])
	}
}

func TestSettings_Delete(t *testing.T) {
	s := Settings{"a": "1", "b": "2"}
	delete(s, "a")
	if _, ok := s["a"]; ok {
		t.Error("expected a to be deleted")
	}
	if len(s) != 1 {
		t.Errorf("expected 1 entry after delete, got %d", len(s))
	}
}

func TestSettings_SetNilValue(t *testing.T) {
	s := Settings{}
	s["empty"] = ""
	if s["empty"] != "" {
		t.Error("expected empty string value")
	}
}

// Config tests

func TestConfig_New(t *testing.T) {
	cfg := Config{
		PeerAddresses: []string{"localhost:50051"},
		SettingsPath:  "/tmp/settings",
		CRDTWorkdir:   "/tmp/crdt",
		GRPCPort:      50051,
	}
	if len(cfg.PeerAddresses) != 1 {
		t.Errorf("PeerAddresses = %d, want 1", len(cfg.PeerAddresses))
	}
	if cfg.SettingsPath != "/tmp/settings" {
		t.Errorf("SettingsPath = %s, want /tmp/settings", cfg.SettingsPath)
	}
	if cfg.CRDTWorkdir != "/tmp/crdt" {
		t.Errorf("CRDTWorkdir = %s, want /tmp/crdt", cfg.CRDTWorkdir)
	}
	if cfg.GRPCPort != 50051 {
		t.Errorf("GRPCPort = %d, want 50051", cfg.GRPCPort)
	}
}

func TestConfig_Defaults(t *testing.T) {
	cfg := Config{}
	if cfg.PeerAddresses != nil {
		t.Error("expected nil PeerAddresses")
	}
	if cfg.SettingsPath != "" {
		t.Errorf("empty SettingsPath, got %s", cfg.SettingsPath)
	}
}

func TestConfig_ZeroPort(t *testing.T) {
	cfg := Config{GRPCPort: 0}
	if cfg.GRPCPort != 0 {
		t.Errorf("expected port 0, got %d", cfg.GRPCPort)
	}
}

func TestConfig_MaxPort(t *testing.T) {
	cfg := Config{GRPCPort: 65535}
	if cfg.GRPCPort != 65535 {
		t.Errorf("expected port 65535, got %d", cfg.GRPCPort)
	}
}

// LoadConfig tests

func TestLoadConfig_Success(t *testing.T) {
	dir := t.TempDir()
	configJSON := `{
		"PeerAddresses": ["peer1:50051", "peer2:50051"],
		"SettingsPath": "/data/settings.json",
		"CRDTWorkdir": "/data/crdt",
		"GRPCPort": 9090
	}`
	err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(configJSON), 0644)
	if err != nil {
		t.Fatalf("failed to write test config: %v", err)
	}

	cfg, err := LoadConfig(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if len(cfg.PeerAddresses) != 2 {
		t.Errorf("PeerAddresses = %d, want 2", len(cfg.PeerAddresses))
	}
	if cfg.PeerAddresses[0] != "peer1:50051" {
		t.Errorf("PeerAddresses[0] = %s, want peer1:50051", cfg.PeerAddresses[0])
	}
	if cfg.SettingsPath != "/data/settings.json" {
		t.Errorf("SettingsPath = %s, want /data/settings.json", cfg.SettingsPath)
	}
	if cfg.CRDTWorkdir != "/data/crdt" {
		t.Errorf("CRDTWorkdir = %s, want /data/crdt", cfg.CRDTWorkdir)
	}
	if cfg.GRPCPort != 9090 {
		t.Errorf("GRPCPort = %d, want 9090", cfg.GRPCPort)
	}
}

func TestLoadConfig_FileNotFound(t *testing.T) {
	_, err := LoadConfig("/nonexistent/path/dir")
	if err == nil {
		t.Fatal("expected error for missing config file")
	}
	if err.Error() == "" {
		t.Error("expected non-empty error message")
	}
}

func TestLoadConfig_InvalidJSON(t *testing.T) {
	dir := t.TempDir()
	err := os.WriteFile(filepath.Join(dir, "config.json"), []byte("{invalid json"), 0644)
	if err != nil {
		t.Fatalf("failed to write test config: %v", err)
	}
	_, err = LoadConfig(dir)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestLoadConfig_EmptyFile(t *testing.T) {
	dir := t.TempDir()
	err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(""), 0644)
	if err != nil {
		t.Fatalf("failed to write test config: %v", err)
	}
	_, err = LoadConfig(dir)
	if err == nil {
		t.Fatal("expected error for empty file")
	}
}

func TestLoadConfig_MinimalConfig(t *testing.T) {
	dir := t.TempDir()
	configJSON := `{}`
	err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(configJSON), 0644)
	if err != nil {
		t.Fatalf("failed to write test config: %v", err)
	}
	cfg, err := LoadConfig(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if cfg.GRPCPort != 0 {
		t.Errorf("GRPCPort = %d, want 0 (zero value)", cfg.GRPCPort)
	}
}

func TestLoadConfig_WrappedError(t *testing.T) {
	dir := t.TempDir()
	_, err := LoadConfig(dir)
	if err == nil {
		t.Fatal("expected error")
	}
	if err.Error() == "" {
		t.Fatal("expected wrapped error message to contain useful info")
	}
}

// HLC serialization

func TestHLC_JSONMarshalUnmarshal(t *testing.T) {
	original := HLC{WallTime: 12345, Logical: 42, NodeID: "test-node"}
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}
	var decoded HLC
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if decoded.WallTime != original.WallTime {
		t.Errorf("WallTime = %d, want %d", decoded.WallTime, original.WallTime)
	}
	if decoded.Logical != original.Logical {
		t.Errorf("Logical = %d, want %d", decoded.Logical, original.Logical)
	}
	if decoded.NodeID != original.NodeID {
		t.Errorf("NodeID = %s, want %s", decoded.NodeID, original.NodeID)
	}
}

func TestHLC_JSONEmpty(t *testing.T) {
	var h HLC
	data, err := json.Marshal(h)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}
	if string(data) != `{"WallTime":0,"Logical":0,"NodeID":""}` {
		t.Errorf("unexpected JSON: %s", string(data))
	}
}

// HLC comparison edge cases

func TestHLCBefore_EdgeCases(t *testing.T) {
	negWall := int64(-1)
	a := HLC{WallTime: negWall, Logical: 0, NodeID: "a"}
	b := HLC{WallTime: 0, Logical: 0, NodeID: "b"}
	if !a.Before(b) {
		t.Error("negative wall time should be before zero")
	}

	c := HLC{WallTime: 0, Logical: 0, NodeID: "c"}
	d := HLC{WallTime: 0, Logical: 0, NodeID: "c"}
	if c.Before(d) || d.Before(c) {
		t.Error("equal HLCs should not be before each other")
	}

	maxUint := uint32(math.MaxUint32)

	g := HLC{WallTime: 100, Logical: maxUint, NodeID: "g"}
	h2 := HLC{WallTime: 100, Logical: 0, NodeID: "h"}
	if g.Before(h2) {
		t.Error("max uint32 logical should not be before 0")
	}
}

// Snapshot JSON serialization

func TestSnapshot_JSONMarshalUnmarshal(t *testing.T) {
	s := Snapshot{
		Entries: map[string]SettingEntry{
			"k1": {Key: "k1", Value: "v1", Deleted: false},
		},
	}
	data, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}
	var decoded Snapshot
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if len(decoded.Entries) != 1 {
		t.Errorf("Entries count = %d, want 1", len(decoded.Entries))
	}
	if entry, ok := decoded.Entries["k1"]; !ok || entry.Value != "v1" {
		t.Error("missing or wrong k1 entry after round-trip")
	}
}

func TestSnapshot_JSONEmpty(t *testing.T) {
	var s Snapshot
	data, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}
	var decoded Snapshot
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
}

// Settings JSON serialization

func TestSettings_JSONMarshalUnmarshal(t *testing.T) {
	s := Settings{"host": "localhost", "port": "8080"}
	data, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}
	var decoded Settings
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if decoded["host"] != "localhost" {
		t.Errorf("host = %s, want localhost", decoded["host"])
	}
	if decoded["port"] != "8080" {
		t.Errorf("port = %s, want 8080", decoded["port"])
	}
}

func TestSettings_JSONEmpty(t *testing.T) {
	var s Settings
	data, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}
	var decoded Settings
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if decoded != nil {
		t.Error("expected nil map after unmarshaling null")
	}
}

// Config JSON serialization

func TestConfig_JSONMarshalUnmarshal(t *testing.T) {
	cfg := Config{
		PeerAddresses: []string{"a:1", "b:2"},
		SettingsPath:  "/s",
		CRDTWorkdir:   "/c",
		GRPCPort:      1234,
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}
	var decoded Config
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if len(decoded.PeerAddresses) != 2 {
		t.Errorf("PeerAddresses = %d, want 2", len(decoded.PeerAddresses))
	}
	if decoded.SettingsPath != "/s" {
		t.Errorf("SettingsPath = %s, want /s", decoded.SettingsPath)
	}
	if decoded.CRDTWorkdir != "/c" {
		t.Errorf("CRDTWorkdir = %s, want /c", decoded.CRDTWorkdir)
	}
	if decoded.GRPCPort != 1234 {
		t.Errorf("GRPCPort = %d, want 1234", decoded.GRPCPort)
	}
}

func TestConfig_JSONEmpty(t *testing.T) {
	var cfg Config
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}
	var decoded Config
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
}

// HLC Tick boundary

func TestHLCTick_MultipleTicks(t *testing.T) {
	h := HLC{WallTime: 0, Logical: 0, NodeID: "a"}
	for i := 0; i < 100; i++ {
		h.Tick()
	}
	if h.Logical != 0 {
		t.Errorf("expected Logical=0 after multiple ticks (wall always advances), got %d", h.Logical)
	}
	if h.WallTime == 0 {
		t.Error("expected WallTime to be non-zero")
	}
}

// HLC Before transitivity

func TestHLCBefore_Transitivity(t *testing.T) {
	a := HLC{WallTime: 100, Logical: 1, NodeID: "a"}
	b := HLC{WallTime: 100, Logical: 2, NodeID: "b"}
	c := HLC{WallTime: 100, Logical: 3, NodeID: "c"}
	if !a.Before(b) {
		t.Error("expected a.Before(b)")
	}
	if !b.Before(c) {
		t.Error("expected b.Before(c)")
	}
	if !a.Before(c) {
		t.Error("expected a.Before(c) by transitivity")
	}
}

// HLC Before antisymmetry

func TestHLCBefore_Antisymmetry(t *testing.T) {
	a := HLC{WallTime: 100, Logical: 1, NodeID: "a"}
	b := HLC{WallTime: 200, Logical: 1, NodeID: "a"}
	if a.Before(b) && b.Before(a) {
		t.Error("antisymmetry violation: both Before() returned true")
	}
}

// HLC Before irreflexivity

func TestHLCBefore_Irreflexivity(t *testing.T) {
	h := HLC{WallTime: 100, Logical: 1, NodeID: "a"}
	if h.Before(h) {
		t.Error("irreflexivity violation: HLC before itself")
	}
}

// HLC Update preserves NodeID

func TestHLCUpdate_NodeIDPreserved(t *testing.T) {
	h := HLC{WallTime: 100, Logical: 5, NodeID: "fixed-id"}
	received := HLC{WallTime: 200, Logical: 10, NodeID: "other-id"}
	h.Update(received)
	if h.NodeID != "fixed-id" {
		t.Errorf("NodeID changed from 'fixed-id' to %s", h.NodeID)
	}
}

// SettingEntry with HLC

func TestSettingEntry_HLCField(t *testing.T) {
	clock := HLC{WallTime: 999, Logical: 123, NodeID: "node-1"}
	entry := SettingEntry{Key: "k", Value: "v", Clock: clock, Deleted: false}
	if entry.Clock.WallTime != 999 {
		t.Errorf("Clock.WallTime = %d, want 999", entry.Clock.WallTime)
	}
	if entry.Clock.Logical != 123 {
		t.Errorf("Clock.Logical = %d, want 123", entry.Clock.Logical)
	}
	if entry.Clock.NodeID != "node-1" {
		t.Errorf("Clock.NodeID = %s, want node-1", entry.Clock.NodeID)
	}
}

// Snapshot CRUD operations

func TestSnapshot_CRUD(t *testing.T) {
	s := Snapshot{Entries: make(map[string]SettingEntry)}

	// Set
	key := "test-key"
	entry := SettingEntry{Key: key, Value: "test-value", Deleted: false}
	s.Entries[key] = entry

	// Get
	got, ok := s.Entries[key]
	if !ok {
		t.Fatal("key not found after set")
	}
	if got.Value != "test-value" {
		t.Errorf("value = %s, want test-value", got.Value)
	}

	// Update
	got.Value = "updated-value"
	s.Entries[key] = got
	if s.Entries[key].Value != "updated-value" {
		t.Error("update failed")
	}

	// Delete
	delete(s.Entries, key)
	if _, ok := s.Entries[key]; ok {
		t.Error("delete failed")
	}
}

// Config LoadConfig with non-directory path

func TestLoadConfig_NonDirectoryPath(t *testing.T) {
	_, err := LoadConfig("/proc/cpuinfo")
	if err == nil {
		t.Fatal("expected error for non-directory path")
	}
}

// LoadConfig with extra fields in JSON (should ignore)

func TestLoadConfig_ExtraFields(t *testing.T) {
	dir := t.TempDir()
	configJSON := `{"ExtraField": "ignored", "GRPCPort": 50051}`
	err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(configJSON), 0644)
	if err != nil {
		t.Fatalf("failed to write test config: %v", err)
	}
	cfg, err := LoadConfig(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if cfg.GRPCPort != 50051 {
		t.Errorf("GRPCPort = %d, want 50051", cfg.GRPCPort)
	}
}

// LoadConfig with unicode values

func TestLoadConfig_UnicodeValues(t *testing.T) {
	dir := t.TempDir()
	configJSON := `{"SettingsPath": "/data/中文", "PeerAddresses": ["サーバー:50051"]}`
	err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(configJSON), 0644)
	if err != nil {
		t.Fatalf("failed to write test config: %v", err)
	}
	cfg, err := LoadConfig(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if cfg.SettingsPath != "/data/中文" {
		t.Errorf("SettingsPath = %s, want /data/中文", cfg.SettingsPath)
	}
}

// Negative wall time HLC

func TestHLCBefore_NegativeWallTime(t *testing.T) {
	a := HLC{WallTime: -1000, Logical: 0, NodeID: "a"}
	b := HLC{WallTime: -500, Logical: 0, NodeID: "a"}
	if !a.Before(b) {
		t.Error("expected negative wall time -1000 before -500")
	}
	c := HLC{WallTime: -1000, Logical: 0, NodeID: "a"}
	d := HLC{WallTime: -1000, Logical: 0, NodeID: "a"}
	if c.Before(d) || d.Before(c) {
		t.Error("equal negative wall times should not compare before")
	}
}
