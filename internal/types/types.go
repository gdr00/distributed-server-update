package types

import "time"

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
	if wall == h.WallTime && wall == received.WallTime {
		h.Logical = max(h.Logical, received.Logical) + 1
	} else if wall == h.WallTime {
		h.Logical++
	} else {
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
	GRPCPort      uint16
}
