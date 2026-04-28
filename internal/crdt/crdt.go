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
	broadcastCh chan types.SettingEntry       // send update to the subscribers
	fileCh      chan types.SettingEntry       // send setting sync to local
	queryCh     chan queryRequest             // channel to query for key value pairs in settings
	snapshotCh  chan snapshotRequest          // channel to query for a full snapshot
}

func New(workDir string) *CRDT {
	return &CRDT{
		clock:       types.HLC{NodeID: loadNodeID(workDir)},
		state:       make(map[string]types.SettingEntry),
		dir:         workDir,
		localCh:     make(chan types.SettingEntry, 10),
		remoteCh:    make(chan types.SettingEntry, 10),
		broadcastCh: make(chan types.SettingEntry, 10),
		fileCh:      make(chan types.SettingEntry, 10),
		queryCh:     make(chan queryRequest, 10),
		snapshotCh:  make(chan snapshotRequest, 10),
	}
}

func Init(entries types.Settings, workDir string) error {
	if err := os.MkdirAll(workDir, 0700); err != nil {
		return fmt.Errorf("failed to create config dir: %w", err)
	}

	// generate and save node_id
	nodeID := uuid.New().String()
	if err := os.WriteFile(filepath.Join(workDir, "node_id"), []byte(nodeID), 0600); err != nil {
		return fmt.Errorf("failed to write node_id: %w", err)
	}

	// stamp each entry with a fresh clock
	clock := types.HLC{NodeID: nodeID}
	state := make(map[string]types.SettingEntry, len(entries))
	for key, value := range entries {
		clock.Tick()
		state[key] = types.SettingEntry{
			Key:     key,
			Value:   value,
			Clock:   clock,
			Deleted: false,
		}
	}

	// save crdt state
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal crdt state: %w", err)
	}
	if err := os.WriteFile(filepath.Join(workDir, "crdt_state.json"), data, 0600); err != nil {
		return fmt.Errorf("failed to write crdt state: %w", err)
	}

	log.Printf("node initialized with ID %s and %d settings", nodeID, len(state))
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

// for network package — read updates to broadcast
func (c *CRDT) Updates() <-chan types.SettingEntry {
	return c.broadcastCh
}

// for logic package — read snapshots to write to file
func (c *CRDT) FileSync() <-chan types.SettingEntry {
	return c.fileCh
}

// for any package — query a single key
func (c *CRDT) Get(key string) types.SettingEntry {
	req := queryRequest{key: key, resp: make(chan types.SettingEntry, 1)}
	c.queryCh <- req
	return <-req.resp
}

// Merge 2 edits with LWW logic
// true when incoming is newer
func (c *CRDT) merge(incoming types.SettingEntry) (changed bool) {
	existing, ok := c.state[incoming.Key]
	if ok && incoming.Clock.Before(existing.Clock) {
		return false
	}
	c.state[incoming.Key] = incoming
	return true
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

func (c *CRDT) Run(ctx context.Context) {
	c.loadState()
	for {
		select {
		// I have incoming changes from local state
		case entry := <-c.localCh:
			c.clock.Tick()        // on event update clock
			entry.Clock = c.clock // update entry clock to the current system's
			if c.merge(entry) {
				c.saveState()
				c.broadcastCh <- entry
			}
		// I have incoming changes from one of the peers I am subscribed to
		case entry := <-c.remoteCh:
			c.clock.Update(entry.Clock)
			if c.merge(entry) {
				c.saveState()
				c.fileCh <- entry
			}
		// I recive a query about current state of an entry
		case req := <-c.queryCh:
			req.resp <- c.state[req.key]
		// I recive a query about state
		case req := <-c.snapshotCh:
			req.resp <- c.snapshot()
		case <-ctx.Done():
			return
		}
	}
}

func loadNodeID(dir string) string {
	path := filepath.Join(dir, "node_id")

	data, err := os.ReadFile(path)

	if err != nil {
		if os.IsNotExist(err) {
			log.Fatalf("nodeId not found, run '%s init' first", os.Args[0])
		}
		log.Fatalf("error occurred while loading node id: %v", err)
	}

	return strings.TrimSpace(string(data))
}

// Load the previous CRTD state
func (c *CRDT) loadState() {
	path := filepath.Join(c.dir, "crdt_state.json")

	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		log.Fatalf("crdt state not found, run '%s init' first", os.Args[0])
	}
	if err != nil {
		log.Fatalf("failed to read crdt state: %v", err)
	}
	if err := json.Unmarshal(data, &c.state); err != nil {
		log.Fatalf("failed to parse crdt state: %v", err)
	}
}

// Save the current state
func (c *CRDT) saveState() {
	path := filepath.Join(c.dir, "crdt_state.json")

	data, err := json.Marshal(c.state)
	if err != nil {
		log.Printf("failed to marshal crdt state: %v", err)
		return
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		log.Printf("failed to save crdt state: %v", err)
	}
}
