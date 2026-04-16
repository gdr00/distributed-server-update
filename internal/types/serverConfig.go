package types

import "time"

type SettingEntry struct {
	Value     string
	Timestamp time.Time
	ReplicaID string
}

type ServerConfig map[string]SettingEntry
