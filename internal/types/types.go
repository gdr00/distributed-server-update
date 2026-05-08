package types

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type HLC struct {
	WallTime int64
	Logical  uint32
	NodeID   string
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

	now := time.Now().UnixNano()

	if received.WallTime-now > int64(time.Minute) {

		return fmt.Errorf("rejecting clock too far in future: %v", received)
	}

	wall := max(h.WallTime, received.WallTime, now)

	if wall == h.WallTime && wall == received.WallTime {
		// local and remote are equal, phy might be lower
		h.advanceLogical(max(h.Logical, received.Logical))
	} else if wall == received.WallTime {
		// received is ahead
		h.advanceLogical(received.Logical)
	} else if wall == h.WallTime {
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
	now := time.Now().UnixNano()
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

type Config struct {
	PeerAddresses []string
	SettingsPath  string
	CRDTWorkdir   string
	GRPCPort      uint16
}

func LoadConfig(configPath string) (Config, error) {
	data, err := os.ReadFile(configPath)

	if err != nil {
		return Config{}, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("failed to parse config file: %w", err)
	}

	return cfg, nil
}
