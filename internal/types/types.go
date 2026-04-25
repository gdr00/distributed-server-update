package types

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
