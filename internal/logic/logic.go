package logic

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/gdr00/distributed-server-update/internal/types"
)

type Logic struct {
	settingsPath string
	mu           sync.RWMutex
	writing      atomic.Bool
	previous     types.Settings
}

func New(settingsPath string) *Logic {
	l := &Logic{
		settingsPath: settingsPath,
		previous:     make(types.Settings),
	}
	if settings, err := l.Read(); err == nil {
		l.previous = settings
	}
	return l
}

// Read parses the settings file into a plain key-value map
func (l *Logic) Read() (types.Settings, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

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
	l.writing.Store(true)
	defer l.writing.Store(false)

	l.mu.Lock()
	defer l.mu.Unlock()

	// direct read to avoid lock order issues
	data, _ := os.ReadFile(l.settingsPath)
	settings := make(types.Settings)
	if data != nil {
		json.Unmarshal(data, &settings)
	}

	if entry.Deleted {
		// if deleted remove the entry from cache and local read data
		delete(settings, entry.Key)
		delete(l.previous, entry.Key)
	} else {
		// if new add it to cache and settings to write
		settings[entry.Key] = entry.Value
		l.previous[entry.Key] = entry.Value
	}

	out, err := json.MarshalIndent(settings, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal settings: %w", err)
	}
	return os.WriteFile(l.settingsPath, out, 0600)
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

	var debounce *time.Timer

	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-watcher.Events:
			if !ok {
				return nil
			}
			if event.Has(fsnotify.Write) {
				if l.writing.Load() {
					continue // we were writing
				}
				if debounce != nil {
					debounce.Stop()
				}
				debounce = time.AfterFunc(100*time.Millisecond, func() {
					l.applyChanges(onChange)
				})
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return nil
			}
			log.Printf("watcher error: %v", err)
		}
	}
}

func (l *Logic) applyChanges(onChange func(types.SettingEntry)) {
	entries, err := l.Read()
	if err != nil {
		log.Printf("failed to read settings: %v", err)
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	for k := range l.previous {
		if _, exists := entries[k]; !exists {
			onChange(types.SettingEntry{Key: k, Deleted: true})
		}
	}
	for k, v := range entries {
		if l.previous[k] != v {
			onChange(types.SettingEntry{Key: k, Value: v})
		}
	}
	l.previous = entries
}
