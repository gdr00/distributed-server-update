package crdt

import (
	"context"
	"maps"

	"github.com/gdr00/distributed-server-update/internal/types"
)

type queryRequest struct {
	key  string
	resp chan types.SettingEntry // channel for messaging the answer
}

type CRDT struct {
	state       map[string]types.SettingEntry // private map of up to date settings
	localCh     chan types.SettingEntry       // channel for local setting updates
	remoteCh    chan types.SettingEntry       // channel for remote setting updates
	broadcastCh chan types.SettingEntry       // send update to the subscribers
	fileCh      chan types.Snapshot           // send full setting sync
	queryCh     chan queryRequest             // channel to query for key value pairs in settings
}

func New() *CRDT {
	return &CRDT{
		state:       make(map[string]types.SettingEntry),
		localCh:     make(chan types.SettingEntry, 10),
		remoteCh:    make(chan types.SettingEntry, 10),
		broadcastCh: make(chan types.SettingEntry, 10),
		fileCh:      make(chan types.Snapshot, 10),
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
func (c *CRDT) FileSync() <-chan types.Snapshot {
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
		case entry := <-c.localCh:
			if c.merge(entry) {
				c.broadcastCh <- entry
			}
		case entry := <-c.remoteCh:
			if c.merge(entry) {
				c.fileCh <- c.Snapshot()
			}
		case req := <-c.queryCh:
			req.resp <- c.state[req.key]
		case <-ctx.Done():
			return
		}
	}
}
