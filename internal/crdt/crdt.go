package crdt

import (
	"context"

	"github.com/gdr00/distributed-server-update/internal/types"
)

type CRDT struct {
	state       map[string]types.SettingEntry
	localCh     chan int
	remoteCh    chan int
	queryCh     chan int
	broadcastCh chan int
}

// Merge 2 edits with LWW logic
func (c *CRDT) merge(incoming types.SettingEntry) (changed bool) {
	if incoming.Clock.Before(c.state[incoming.Key].Clock) {
		c.state[incoming.Key] = incoming
		return true
	}
	return false
}

func (c *CRDT) Run(ctx context.Context) {
	for {
		select {
		case entry := <-c.localCh:
			c.merge(entry) // safe, only this goroutine touches state
			c.broadcastCh <- entry
		case entry := <-c.remoteCh:
			if changed := c.merge(entry); changed {
				c.fileCh <- c.snapshot()
			}
		case req := <-c.queryCh:
			req.resp <- c.state[req.key]
		case <-ctx.Done():
			return
		}
	}
}
