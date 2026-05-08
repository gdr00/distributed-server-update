package logic

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gdr00/distributed-server-update/internal/types"
)

// helpers

func setupLogic(t *testing.T, initial types.Settings) *Logic {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "settings.json")

	if initial != nil {
		data, err := json.MarshalIndent(initial, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}

	return New(path)
}

// Read tests

func TestRead_ValidFile(t *testing.T) {
	l := setupLogic(t, types.Settings{"theme": "dark", "lang": "en"})

	settings, err := l.Read()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if settings["theme"] != "dark" {
		t.Fatalf("expected dark, got %s", settings["theme"])
	}
	if settings["lang"] != "en" {
		t.Fatalf("expected en, got %s", settings["lang"])
	}
}

func TestRead_EmptyFile(t *testing.T) {
	l := setupLogic(t, types.Settings{})

	settings, err := l.Read()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(settings) != 0 {
		t.Fatalf("expected empty map, got %v", settings)
	}
}

func TestRead_MissingFile(t *testing.T) {
	l := New("/nonexistent/path/settings.json")

	_, err := l.Read()
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestRead_InvalidJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "settings.json")
	os.WriteFile(path, []byte("not json {{{"), 0600)

	l := New(path)
	_, err := l.Read()
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

// Write tests

func TestWrite_NewKey(t *testing.T) {
	l := setupLogic(t, types.Settings{})

	err := l.Write(types.SettingEntry{Key: "theme", Value: "dark"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	settings, _ := l.Read()
	if settings["theme"] != "dark" {
		t.Fatalf("expected dark, got %s", settings["theme"])
	}
}

func TestWrite_UpdateExistingKey(t *testing.T) {
	l := setupLogic(t, types.Settings{"theme": "dark"})

	err := l.Write(types.SettingEntry{Key: "theme", Value: "light"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	settings, _ := l.Read()
	if settings["theme"] != "light" {
		t.Fatalf("expected light, got %s", settings["theme"])
	}
}

func TestWrite_PreservesOtherKeys(t *testing.T) {
	l := setupLogic(t, types.Settings{"theme": "dark", "lang": "en"})

	err := l.Write(types.SettingEntry{Key: "theme", Value: "light"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	settings, _ := l.Read()
	if settings["lang"] != "en" {
		t.Fatalf("expected lang to be preserved, got %s", settings["lang"])
	}
}

func TestWrite_Tombstone_DeletesKey(t *testing.T) {
	l := setupLogic(t, types.Settings{"theme": "dark", "lang": "en"})

	err := l.Write(types.SettingEntry{Key: "theme", Deleted: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	settings, _ := l.Read()
	if _, exists := settings["theme"]; exists {
		t.Fatal("expected theme to be deleted")
	}
	if settings["lang"] != "en" {
		t.Fatal("expected lang to be preserved after deletion of theme")
	}
}

func TestWrite_Tombstone_MissingKeyIsNoop(t *testing.T) {
	l := setupLogic(t, types.Settings{"lang": "en"})

	err := l.Write(types.SettingEntry{Key: "nonexistent", Deleted: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	settings, _ := l.Read()
	if len(settings) != 1 || settings["lang"] != "en" {
		t.Fatalf("expected unchanged settings, got %v", settings)
	}
}

func TestWrite_CreatesFileIfMissing(t *testing.T) {
	dir := t.TempDir()
	l := New(filepath.Join(dir, "settings.json"))

	err := l.Write(types.SettingEntry{Key: "theme", Value: "dark"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	settings, _ := l.Read()
	if settings["theme"] != "dark" {
		t.Fatalf("expected dark, got %s", settings["theme"])
	}
}

// Watch tests

func TestWatch_DetectsFileChange(t *testing.T) {
	l := setupLogic(t, types.Settings{"theme": "dark"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	received := make(chan types.SettingEntry, 10)
	go l.Watch(ctx, func(entry types.SettingEntry) {
		received <- entry
	})

	// give watcher time to start
	time.Sleep(50 * time.Millisecond)

	// simulate external file edit
	newData, _ := json.MarshalIndent(types.Settings{"theme": "light"}, "", "  ")
	os.WriteFile(l.settingsPath, newData, 0600)

	select {
	case entry := <-received:
		if entry.Key != "theme" || entry.Value != "light" {
			t.Fatalf("unexpected entry: %+v", entry)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for file change notification")
	}
}

func TestWatch_StopsOnContextCancel(t *testing.T) {
	l := setupLogic(t, types.Settings{})

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- l.Watch(ctx, func(types.SettingEntry) {})
	}()

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected nil error on cancel, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Watch did not stop after context cancel")
	}
}

// applyChanges error path tests

func TestApplyChanges_ReadError(t *testing.T) {
	l := setupLogic(t, types.Settings{"key": "val"})
	os.Remove(l.settingsPath)
	called := false
	l.applyChanges(func(types.SettingEntry) { called = true })
	if called {
		t.Error("onChange should not be called on read error")
	}
}

func TestApplyChanges_InvalidJSON(t *testing.T) {
	l := setupLogic(t, types.Settings{})
	os.WriteFile(l.settingsPath, []byte("{invalid json"), 0600)
	called := false
	l.applyChanges(func(types.SettingEntry) { called = true })
	if called {
		t.Error("onChange should not be called on parse error")
	}
}

func TestApplyChanges_DeletionDetected(t *testing.T) {
	l := setupLogic(t, types.Settings{"key": "val"})
	// rewrite file without "key" to simulate external deletion
	os.WriteFile(l.settingsPath, []byte("{}"), 0600)
	var changes []types.SettingEntry
	l.applyChanges(func(e types.SettingEntry) { changes = append(changes, e) })
	if len(changes) != 1 || !changes[0].Deleted || changes[0].Key != "key" {
		t.Fatalf("expected deletion change for 'key', got %+v", changes)
	}
}
