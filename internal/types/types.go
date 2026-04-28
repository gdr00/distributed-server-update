package types

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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

func (h *HLC) Update(received HLC) {
	wall := max(h.WallTime, received.WallTime)

	if h.WallTime == received.WallTime {
		// Case equal clocks -> increment logical to max +1
		h.Logical = max(h.Logical, received.Logical) + 1
	} else if wall == h.WallTime {
		// Case local clock is greater -> increment LL (local logical)
		h.Logical++
	} else {
		// Case remote clock is greater -> set local clock to remote and increment logical
		h.WallTime = wall
		h.Logical = received.Logical + 1
	}
}

func (h *HLC) Tick() {
	now := time.Now().UnixNano()
	if now > h.WallTime {
		h.WallTime = now
		h.Logical = 0
	} else {
		h.Logical++
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

func LoadConfig(workDir string) (Config, error) {
	path := filepath.Join(workDir, "config.json")

	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("failed to parse config file: %w", err)
	}

	return cfg, nil
}
