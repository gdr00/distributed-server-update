package controller

import (
	"encoding/json"
	"fmt"
	"os"
)

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
