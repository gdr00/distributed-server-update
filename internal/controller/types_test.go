package controller

import (
	"os"
	"path/filepath"
	"testing"
)

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