package types

import (
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
		wantWall    int64
		wantLogical uint32
	}{
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

			h.Update(tt.received)

			if h.WallTime != tt.wantWall {
				t.Errorf("WallTime = %v, want %v", h.WallTime, tt.wantWall)
			}
			if h.Logical != tt.wantLogical {
				t.Errorf("Logical = %v, want %v", h.Logical, tt.wantLogical)
			}
		})
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
	h := NewHLC("a")
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
	h := NewHLC("a", SetHLC(HLC{
		clock:    &MockClock{Time: futureWall - 1},
		WallTime: futureWall,
		Logical:  5,
	}))
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

