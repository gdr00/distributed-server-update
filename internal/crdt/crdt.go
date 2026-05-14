package crdt

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gdr00/distributed-server-update/internal/types"
	"github.com/google/uuid"
)

type queryRequest struct {
	key  string
	resp chan types.SettingEntry // channel for messaging the answer
}
type snapshotRequest struct {
	resp chan types.Snapshot
}

type CRDT struct {
	clock       types.HLC                     // clock to establish an update hierarchy
	state       map[string]types.SettingEntry // private map of up to date settings
	dir         string                        // working directory where all CRTD data is stored
	localCh     chan types.SettingEntry       // channel for local setting updates
	remoteCh    chan types.SettingEntry       // channel for remote setting updates
	queryCh     chan queryRequest             // channel to query for key value pairs in settings
	snapshotCh  chan snapshotRequest          // channel to query for a full snapshot
	gcCh        chan int64                    // channel to trigger tombstone GC with a TTL in nanoseconds
	OnBroadcast func(types.SettingEntry)      // injected callback for outgoing broadcasts
	OnFileSync  func(types.SettingEntry)      // injected callback for file writes
	started     atomic.Bool
}

func New(workDir string) *CRDT {
	c := &CRDT{
		dir:        workDir,
		localCh:    make(chan types.SettingEntry, 10),
		remoteCh:   make(chan types.SettingEntry, 10),
		queryCh:    make(chan queryRequest, 10),
		snapshotCh: make(chan snapshotRequest, 10),
		gcCh:       make(chan int64, 1),
	}
	return c
}

func (c *CRDT) Init() error {

	// GC for orphaned atomicWrites tmp files
	matches, _ := filepath.Glob(filepath.Join(c.dir, ".tmp_*"))
	for _, f := range matches {
		os.Remove(f)
	}

	nodeID, err := loadNodeID(c.dir)
	if err != nil {
		// generate and save node_id
		nodeID = uuid.New().String()
		if err := os.WriteFile(filepath.Join(c.dir, "node_id"), []byte(nodeID), 0600); err != nil {
			return fmt.Errorf("failed to write node_id: %w", err)
		}
	}

	c.clock = *types.NewHLC(nodeID)

	if err := c.loadState(); err != nil {
		return fmt.Errorf("failed to load crdt previous state: %v", err)
	}

	log.Printf("node initialized with ID %s and %d settings", nodeID, len(c.state))
	return nil
}

func (c *CRDT) InitNew(entries types.Settings) error {

	if err := os.MkdirAll(c.dir, 0700); err != nil {
		return fmt.Errorf("failed to create config dir: %w", err)
	}

	nodeID, err := loadNodeID(c.dir)
	if err != nil {
		// generate and save node_id
		nodeID = uuid.New().String()
		if err := os.WriteFile(filepath.Join(c.dir, "node_id"), []byte(nodeID), 0600); err != nil {
			return fmt.Errorf("failed to write node_id: %w", err)
		}
	}

	c.clock = *types.NewHLC(nodeID)

	c.state = make(map[string]types.SettingEntry, len(entries))
	// init newNode state
	if len(entries) != 0 {
		// stamp all entries with new clock
		clock := types.HLC{NodeID: nodeID}
		for key, value := range entries {
			clock.Tick()
			c.state[key] = types.SettingEntry{
				Key:     key,
				Value:   value,
				Clock:   clock,
				Deleted: false,
			}
		}
	}
	// save crdt state
	data, err := json.Marshal(c.state)
	if err != nil {
		return fmt.Errorf("failed to marshal crdt state: %w", err)
	}
	if err := os.WriteFile(filepath.Join(c.dir, "crdt_state.json"), data, 0600); err != nil {
		return fmt.Errorf("failed to write crdt state: %w", err)
	}

	log.Printf("node initialized with ID %s and %d settings", nodeID, len(c.state))
	return nil
}

// for logic package — push a local file change
func (c *CRDT) NotifyLocal(entry types.SettingEntry) {
	c.localCh <- entry
}

// for network package — push a remote update
func (c *CRDT) NotifyRemote(entry types.SettingEntry) {
	c.remoteCh <- entry
}

// PurgeTombstones schedules removal of deleted entries older than ttl nanoseconds.
func (c *CRDT) PurgeTombstones(ttl int64) {
	c.gcCh <- ttl
}

// for any package — query a single key
func (c *CRDT) Get(key string) types.SettingEntry {
	req := queryRequest{key: key, resp: make(chan types.SettingEntry, 1)}
	c.queryCh <- req
	return <-req.resp
}

func (c *CRDT) snapshot() types.Snapshot {
	entries := make(map[string]types.SettingEntry, len(c.state))
	maps.Copy(entries, c.state)
	return types.Snapshot{Entries: entries}
}

func (c *CRDT) Snapshot() types.Snapshot {
	req := snapshotRequest{resp: make(chan types.Snapshot, 1)}
	c.snapshotCh <- req
	return <-req.resp
}

func (c *CRDT) Reconcile(fn func(types.SettingEntry)) {
	if c.started.Load() {
		panic("Reconcile called after Run")
	}
	for _, entry := range c.state {
		fn(entry)
	}
}

// Merge 2 edits with LWW logic
// true when incoming is newer
func (c *CRDT) merge(incoming types.SettingEntry) (changed bool) {
	existing, ok := c.state[incoming.Key]
	if ok && !existing.Clock.Before(incoming.Clock) { // otherwise merges equal clocks
		return false
	}
	c.state[incoming.Key] = incoming
	return true
}

func (c *CRDT) Run(ctx context.Context) {
	c.started.Store(true)
	for {
		select {
		// I have incoming changes from local state
		case entry := <-c.localCh:
			c.clock.Tick()        // on event update clock
			entry.Clock = c.clock // update entry clock to the current system's
			if c.merge(entry) {
				c.saveState()
				if c.OnBroadcast != nil {
					c.OnBroadcast(entry)
				}
			}
		// I have incoming changes from one of the peers I am subscribed to
		case entry := <-c.remoteCh:
			if err := c.clock.Update(entry.Clock); err != nil {
				log.Printf("remote clock anomaly, entry rejected: %v, %v", err, entry)
				continue
			}
			if c.merge(entry) {
				c.saveState()
				if c.OnFileSync != nil {
					c.OnFileSync(entry)
				}
			}
		case req := <-c.queryCh:
			req.resp <- c.state[req.key]
		case req := <-c.snapshotCh:
			req.resp <- c.snapshot()
		case ttl := <-c.gcCh:
			c.purgeTombstones(ttl)
		case <-ctx.Done():
			return
		}
	}
}

func (c *CRDT) purgeTombstones(ttl int64) {
	diff := time.Now().UnixNano() - ttl
	purged := 0
	for key, entry := range c.state {
		if entry.Deleted && entry.Clock.WallTime < diff {
			delete(c.state, key)
			purged++
		}
	}
	if purged > 0 {
		c.saveState()
		log.Printf("tombstone GC: purged %d entries", purged)
	}
}

func loadNodeID(dir string) (string, error) {
	path := filepath.Join(dir, "node_id")
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("node not initialized (run init first): %w", err)
	}
	return strings.TrimSpace(string(data)), nil
}

// Load the previous CRDT state
func (c *CRDT) loadState() error {
	path := filepath.Join(c.dir, "crdt_state.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("crdt state not found (run init first): %w", err)
	}
	if err := json.Unmarshal(data, &c.state); err != nil {
		return fmt.Errorf("failed to parse crdt state: %w", err)
	}
	return nil
}

// Save the current state
func (c *CRDT) saveState() {
	data, err := json.Marshal(c.state)
	if err != nil {
		log.Printf("failed to marshal crdt state: %v", err)
		return
	}
	if err := atomicWriteFile(filepath.Join(c.dir, "crdt_state.json"), data); err != nil {
		log.Printf("failed to save crdt state: %v", err)
	}
}

func atomicWriteFile(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp_*")
	if err != nil {
		return err
	}
	ok := false
	defer func() {
		tmp.Close()
		if !ok {
			os.Remove(tmp.Name())
		}
	}()
	if _, err = tmp.Write(data); err != nil {
		return err
	}
	if err = tmp.Sync(); err != nil {
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmp.Name(), path); err != nil {
		return err
	}
	ok = true
	return nil
}
