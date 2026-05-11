package controller

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// Config tests

func TestConfig_New(t *testing.T) {
	cfg := Config{
		PeerAddresses: []string{"localhost:50051"},
		SettingsPath:  "/tmp/settings",
		CRDTWorkdir:   "/tmp/crdt",
		GRPCPort:      50051,
	}
	if len(cfg.PeerAddresses) != 1 {
		t.Errorf("PeerAddresses = %d, want 1", len(cfg.PeerAddresses))
	}
	if cfg.SettingsPath != "/tmp/settings" {
		t.Errorf("SettingsPath = %s, want /tmp/settings", cfg.SettingsPath)
	}
	if cfg.CRDTWorkdir != "/tmp/crdt" {
		t.Errorf("CRDTWorkdir = %s, want /tmp/crdt", cfg.CRDTWorkdir)
	}
	if cfg.GRPCPort != 50051 {
		t.Errorf("GRPCPort = %d, want 50051", cfg.GRPCPort)
	}
}

func TestConfig_Defaults(t *testing.T) {
	cfg := Config{}
	if cfg.PeerAddresses != nil {
		t.Error("expected nil PeerAddresses")
	}
	if cfg.SettingsPath != "" {
		t.Errorf("empty SettingsPath, got %s", cfg.SettingsPath)
	}
}

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

func TestLoadConfig_WrappedError(t *testing.T) {
	dir := t.TempDir()
	_, err := LoadConfig(dir)
	if err == nil {
		t.Fatal("expected error")
	}
	if err.Error() == "" {
		t.Fatal("expected wrapped error message to contain useful info")
	}
}

// Config JSON serialization

func TestConfig_JSONMarshalUnmarshal(t *testing.T) {
	cfg := Config{
		PeerAddresses: []string{"a:1", "b:2"},
		SettingsPath:  "/s",
		CRDTWorkdir:   "/c",
		GRPCPort:      1234,
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}
	var decoded Config
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if len(decoded.PeerAddresses) != 2 {
		t.Errorf("PeerAddresses = %d, want 2", len(decoded.PeerAddresses))
	}
	if decoded.SettingsPath != "/s" {
		t.Errorf("SettingsPath = %s, want /s", decoded.SettingsPath)
	}
	if decoded.CRDTWorkdir != "/c" {
		t.Errorf("CRDTWorkdir = %s, want /c", decoded.CRDTWorkdir)
	}
	if decoded.GRPCPort != 1234 {
		t.Errorf("GRPCPort = %d, want 1234", decoded.GRPCPort)
	}
}

func TestConfig_JSONEmpty(t *testing.T) {
	var cfg Config
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}
	var decoded Config
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
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