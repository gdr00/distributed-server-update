package crdt

import (
	"context"
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

type CRDT struct {
	clock       types.HLC                     // clock to establish an update hierarchy
	state       map[string]types.SettingEntry // private map of up to date settings
	localCh     chan types.SettingEntry       // channel for local setting updates
	remoteCh    chan types.SettingEntry       // channel for remote setting updates
	broadcastCh chan types.SettingEntry       // send update to the subscribers
	fileCh      chan types.SettingEntry       // send setting sync to local
	queryCh     chan queryRequest             // channel to query for key value pairs in settings
}

func New() *CRDT {
	return &CRDT{
		clock:       types.HLC{NodeID: loadOrCreateNodeID()},
		state:       make(map[string]types.SettingEntry),
		localCh:     make(chan types.SettingEntry, 10),
		remoteCh:    make(chan types.SettingEntry, 10),
		broadcastCh: make(chan types.SettingEntry, 10),
		fileCh:      make(chan types.SettingEntry, 10),
		queryCh:     make(chan queryRequest),
	}
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
func (c *CRDT) merge(incoming types.SettingEntry) (changed bool) {
	existing, ok := c.state[incoming.Key]
	if ok && !incoming.Clock.Before(existing.Clock) {
		return false
	}
	c.state[incoming.Key] = incoming
	return true
}

func (c *CRDT) Snapshot() types.Snapshot {
	entries := make(map[string]types.SettingEntry, len(c.state))
	maps.Copy(entries, c.state)
	return types.Snapshot{Entries: entries}
}

func (c *CRDT) Run(ctx context.Context) {
	for {
		select {
		// I have incoming changes from local state
		case entry := <-c.localCh:
			c.clock.Tick()        // on event update clock
			entry.Clock = c.clock // update entry clock to the current system's
			if c.merge(entry) {
				c.broadcastCh <- entry
			}
		// I have incoming changes from one of the peers I am subscribed to
		case entry := <-c.remoteCh:
			c.clock.Update(entry.Clock)
			if c.merge(entry) {
				c.fileCh <- entry
			}
		// I recive a query about current state of an entry
		case req := <-c.queryCh:
			req.resp <- c.state[req.key]
		case <-ctx.Done():
			return
		}
	}
}

func loadOrCreateNodeID() string {
	dir := filepath.Join(os.Getenv("HOME"), ".server-conf")
	path := filepath.Join(dir, "node_id")

	data, err := os.ReadFile(path)
	if err == nil {
		return strings.TrimSpace(string(data))
	}

	if !os.IsNotExist(err) {
		log.Fatalf("failed to read node_id: %v", err)
	}

	// file doesn't exist, generate and persist
	id := uuid.New().String()

	if err := os.MkdirAll(dir, 0700); err != nil {
		log.Fatalf("failed to create config dir: %v", err)
	}
	if err := os.WriteFile(path, []byte(id), 0600); err != nil {
		log.Fatalf("failed to write node_id: %v", err)
	}

	return id
}
