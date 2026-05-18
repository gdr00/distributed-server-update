package types

import (
	"fmt"
	"time"
)

// Clock interface for test purposes
type Clock interface {
	Now() int64
}

type SystemClock struct{}

func (s SystemClock) Now() int64 { return time.Now().UnixNano() }

type HLC struct {
	WallTime int64
	Logical  uint32
	NodeID   string
	clock    Clock
}

type Option func(*HLC)

// Inject vars for testing
func SetHLC(c HLC) Option {
	return func(h *HLC) {
		h.WallTime = c.WallTime
		h.Logical = c.Logical
		h.clock = c.clock
	}
}

// Init HLC
func NewHLC(nodeID string, opts ...Option) *HLC {
	h := &HLC{
		NodeID: nodeID,
		clock:  SystemClock{},
	}

	for _, opt := range opts {
		opt(h)
	}
	return h
}

func (a HLC) Before(b HLC) bool {
	if a.WallTime != b.WallTime {
		return a.WallTime < b.WallTime
	}
	if a.Logical != b.Logical {
		return a.Logical < b.Logical
	}
	return a.NodeID < b.NodeID
}

// Update local HLC with a recived HLC
//
// prevents partially bad actors/missconfigured nodes with sys clock in the future discarding updates with delta T > 1min
func (h *HLC) Update(received HLC) error {

	now := h.clock.Now()

	if received.WallTime-now > int64(10*time.Second) {

		return fmt.Errorf("rejecting clock too far in future: %v", received)
	}

	wall := max(h.WallTime, received.WallTime, now)

	oldWall := h.WallTime
	h.WallTime = wall

	if wall == oldWall && wall == received.WallTime {
		// local and remote are equal, phy might be lower
		h.advanceLogical(max(h.Logical, received.Logical))
	} else if wall == received.WallTime {
		// received is ahead
		h.advanceLogical(received.Logical)
	} else if wall == oldWall {
		// local is ahead
		h.advanceLogical(h.Logical)
	} else {
		// phy ahead of both
		h.WallTime = wall
		h.Logical = 0
	}
	return nil
}

// Logical counter update
//
// Handle overflow of the logical counter
func (h *HLC) advanceLogical(base uint32) {
	if base == ^uint32(0) {
		h.WallTime++
		h.Logical = 0
		return
	}
	h.Logical = base + 1
}

// Advance HLC
//
// If NTP steps sys clock backward might overflow Logical (maxUint32 events in the same instant), unlikely
func (h *HLC) Tick() {
	now := h.clock.Now()
	if now > h.WallTime {
		h.WallTime = now
		h.Logical = 0
	} else {
		h.advanceLogical(h.Logical)
	}
}

type Settings map[string]string

type SettingEntry struct {
	Key     string
	Value   string
	Clock   HLC
	Deleted bool
}

type Snapshot struct {
	Entries map[string]SettingEntry
}
