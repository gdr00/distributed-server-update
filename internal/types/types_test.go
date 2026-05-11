package types

import (
	"encoding/json"
	"math"
	"testing"
	"time"
)

// HLC.Before tests

type MockClock struct {
	Time int64
}

func (m *MockClock) Now() int64 { return m.Time }

func TestHLC_Update(t *testing.T) {

	var timeNow int64 = 1000

	tests := []struct {
		name        string
		local       HLC
		received    HLC
		now         int64
		wantErr     bool
		wantWall    int64
		wantLogical uint32
	}{
		{
			name: "Error: Clock too far in future",
			local: *NewHLC("1", SetHLC(HLC{
				clock:    &MockClock{Time: timeNow},
				WallTime: timeNow,
				Logical:  0,
			})),
			received: *NewHLC("2", SetHLC(HLC{
				clock:    &MockClock{Time: 0},
				WallTime: timeNow + int64(2*time.Minute),
				Logical:  0,
			})),
			now:     timeNow,
			wantErr: true,
		},
		{
			name: "Branches: All Equal",
			local: *NewHLC("1", SetHLC(HLC{
				clock:    &MockClock{Time: timeNow},
				WallTime: timeNow,
				Logical:  5,
			})),
			received: *NewHLC("2", SetHLC(HLC{
				clock:    &MockClock{Time: timeNow},
				WallTime: timeNow,
				Logical:  8,
			})),
			now:         timeNow,
			wantWall:    timeNow,
			wantLogical: 9,
		},
		{
			name: "Branches: Received Wall Ahead",
			local: *NewHLC("1", SetHLC(HLC{
				clock:    &MockClock{Time: timeNow},
				WallTime: timeNow,
				Logical:  10,
			})),
			received: *NewHLC("2", SetHLC(HLC{
				clock:    &MockClock{Time: timeNow},
				WallTime: timeNow + 100,
				Logical:  5,
			})),
			now:         timeNow,
			wantWall:    timeNow + 100,
			wantLogical: 6,
		},
		{
			name: "Branches: Physical Clock Ahead (Reset)",
			local: *NewHLC("1", SetHLC(HLC{
				clock:    &MockClock{Time: timeNow + 1000},
				WallTime: timeNow,
				Logical:  50,
			})),
			received: *NewHLC("2", SetHLC(HLC{
				clock:    &MockClock{Time: timeNow},
				WallTime: timeNow,
				Logical:  50,
			})),
			now:         timeNow + 1000,
			wantWall:    timeNow + 1000,
			wantLogical: 0,
		},
		{
			name: "Edge Case: Logical Overflow",
			local: *NewHLC("1", SetHLC(HLC{
				clock:    &MockClock{Time: timeNow},
				WallTime: timeNow,
				Logical:  4294967295,
			})), // Max uint32
			received: *NewHLC("1", SetHLC(HLC{
				clock:    &MockClock{Time: timeNow},
				WallTime: timeNow,
				Logical:  10,
			})),
			now:         timeNow,
			wantWall:    timeNow + 1,
			wantLogical: 0,
		},
		{
			name: "Branches: Local Wall Ahead",
			local: *NewHLC("1", SetHLC(HLC{
				clock:    &MockClock{Time: timeNow},
				WallTime: timeNow + 100,
				Logical:  5,
			})),
			received: *NewHLC("2", SetHLC(HLC{
				clock:    &MockClock{Time: 0},
				WallTime: timeNow,
				Logical:  8,
			})),
			now:         timeNow,
			wantWall:    timeNow + 100,
			wantLogical: 6,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := tt.local

			err := h.Update(tt.received)

			if (err != nil) != tt.wantErr {
				t.Fatalf("Update() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !tt.wantErr {
				if h.WallTime != tt.wantWall {
					t.Errorf("WallTime = %v, want %v", h.WallTime, tt.wantWall)
				}
				if h.Logical != tt.wantLogical {
					t.Errorf("Logical = %v, want %v", h.Logical, tt.wantLogical)
				}
			}
		})
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

// SystemClock

func TestSystemClock_Now(t *testing.T) {
	before := time.Now().UnixNano()
	got := SystemClock{}.Now()
	after := time.Now().UnixNano()
	if got < before || got > after {
		t.Errorf("SystemClock.Now() = %d, want between %d and %d", got, before, after)
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

func TestHLCTick_AdvancesLogicalWhenWallInFuture(t *testing.T) {
	futureWall := time.Now().UnixNano() + int64(time.Hour)
	h := HLC{WallTime: futureWall, Logical: 5, NodeID: "a"}
	h.Tick()
	if h.WallTime != futureWall {
		t.Error("WallTime should not change when it is already in the future")
	}
	if h.Logical != 6 {
		t.Errorf("expected Logical=6, got %d", h.Logical)
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
	h := *NewHLC("fixed-id", SetHLC(HLC{
		clock:    &MockClock{Time: 100},
		WallTime: 100,
		Logical:  5,
	}))
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
