package logic

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/fsnotify/fsnotify"
	"github.com/gdr00/distributed-server-update/internal/types"
)

type Logic struct {
	settingsPath string
}

func New(settingsPath string) *Logic {
	return &Logic{settingsPath: settingsPath}
}

// Read parses the settings file into a plain key-value map
func (l *Logic) Read() (types.Settings, error) {
	data, err := os.ReadFile(l.settingsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read settings file: %w", err)
	}

	settings := make(types.Settings)
	if err := json.Unmarshal(data, &settings); err != nil {
		return nil, fmt.Errorf("failed to parse settings file: %w", err)
	}

	return settings, nil
}

// Write entries to file
func (l *Logic) Write(entry types.SettingEntry) error {
	settings, err := l.Read()
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to read settings before write: %w", err)
	}
	if settings == nil {
		settings = make(types.Settings)
	}
	if entry.Deleted {
		delete(settings, entry.Key)
	} else {
		settings[entry.Key] = entry.Value
	}
	data, err := json.MarshalIndent(settings, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal settings: %w", err)
	}
	return os.WriteFile(l.settingsPath, data, 0600)
}

func (l *Logic) Watch(ctx context.Context, onChange func(types.SettingEntry)) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("failed to create watcher: %w", err)
	}
	defer watcher.Close()

	if err := watcher.Add(l.settingsPath); err != nil {
		return fmt.Errorf("failed to watch file: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-watcher.Events:
			if !ok {
				return nil
			}
			if event.Has(fsnotify.Write) {
				entries, err := l.Read()
				if err != nil {
					log.Printf("failed to read settings: %v", err)
					continue
				}
				for k, v := range entries {
					onChange(types.SettingEntry{Key: k, Value: v})
				}
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return nil
			}
			log.Printf("watcher error: %v", err)
		}
	}
}
