package crdt

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/gdr00/distributed-server-update/internal/types"
)

// helpers

func setupCRDT(t *testing.T) (*CRDT, string) {
	t.Helper()
	dir := t.TempDir() // auto cleaned up after test
	// create a minimal valid state file so loadState doesn't fatal
	if err := os.WriteFile(dir+"/crdt_state.json", []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir+"/node_id", []byte("test-node"), 0600); err != nil {
		t.Fatal(err)
	}
	return New(dir), dir
}

func hlc(wall int64, logical uint32, node string) types.HLC {
	return types.HLC{WallTime: wall, Logical: logical, NodeID: node}
}

func entry(key, value string, h types.HLC) types.SettingEntry {
	return types.SettingEntry{Key: key, Value: value, Clock: h, Deleted: false}
}

// merge tests

func TestMerge_NewKeyIsAlwaysAccepted(t *testing.T) {
	c, _ := setupCRDT(t)
	e := entry("theme", "dark", hlc(100, 0, "A"))
	if !c.merge(e) {
		t.Fatal("expected merge to return true for new key")
	}
	if c.state["theme"].Value != "dark" {
		t.Fatalf("expected dark, got %s", c.state["theme"].Value)
	}
}

func TestMerge_NewerWins(t *testing.T) {
	c, _ := setupCRDT(t)
	c.merge(entry("theme", "dark", hlc(100, 0, "A")))
	changed := c.merge(entry("theme", "light", hlc(200, 0, "A")))
	if !changed {
		t.Fatal("expected newer entry to win")
	}
	if c.state["theme"].Value != "light" {
		t.Fatalf("expected light, got %s", c.state["theme"].Value)
	}
}

func TestMerge_OlderDiscarded(t *testing.T) {
	c, _ := setupCRDT(t)
	c.merge(entry("theme", "dark", hlc(200, 0, "A")))
	changed := c.merge(entry("theme", "light", hlc(100, 0, "A")))
	if changed {
		t.Fatal("expected older entry to be discarded")
	}
	if c.state["theme"].Value != "dark" {
		t.Fatalf("expected dark, got %s", c.state["theme"].Value)
	}
}

func TestMerge_SameWallTimeTiebreakByLogical(t *testing.T) {
	c, _ := setupCRDT(t)
	c.merge(entry("theme", "dark", hlc(100, 0, "A")))
	changed := c.merge(entry("theme", "light", hlc(100, 1, "A")))
	if !changed {
		t.Fatal("expected higher logical to win")
	}
	if c.state["theme"].Value != "light" {
		t.Fatalf("expected light, got %s", c.state["theme"].Value)
	}
}

func TestMerge_SameWallAndLogicalTiebreakByNodeID(t *testing.T) {
	c, _ := setupCRDT(t)
	c.merge(entry("theme", "dark", hlc(100, 0, "A")))
	changed := c.merge(entry("theme", "light", hlc(100, 0, "B"))) // "B" > "A"
	if !changed {
		t.Fatal("expected higher nodeID to win")
	}
	if c.state["theme"].Value != "light" {
		t.Fatalf("expected light, got %s", c.state["theme"].Value)
	}
}

func TestMerge_Tombstone(t *testing.T) {
	c, _ := setupCRDT(t)
	c.merge(entry("theme", "dark", hlc(100, 0, "A")))
	deleted := types.SettingEntry{Key: "theme", Clock: hlc(200, 0, "A"), Deleted: true}
	if !c.merge(deleted) {
		t.Fatal("expected tombstone to win over older entry")
	}
	if !c.state["theme"].Deleted {
		t.Fatal("expected entry to be marked deleted")
	}
}

func TestMerge_OlderTombstoneDoesNotDeleteNewerEntry(t *testing.T) {
	c, _ := setupCRDT(t)
	c.merge(entry("theme", "dark", hlc(200, 0, "A")))
	deleted := types.SettingEntry{Key: "theme", Clock: hlc(100, 0, "A"), Deleted: true}
	if c.merge(deleted) {
		t.Fatal("expected older tombstone to be discarded")
	}
	if c.state["theme"].Deleted {
		t.Fatal("expected entry to still be alive")
	}
}

// actor / channel tests

func TestRun_LocalUpdateBroadcast(t *testing.T) {
	c, _ := setupCRDT(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	updates := make(chan types.SettingEntry, 1)
	c.OnBroadcast = func(e types.SettingEntry) { updates <- e }
	go c.Run(ctx)

	c.NotifyLocal(types.SettingEntry{Key: "theme", Value: "dark"})

	select {
	case broadcast := <-updates:
		if broadcast.Key != "theme" || broadcast.Value != "dark" {
			t.Fatalf("unexpected broadcast: %+v", broadcast)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for broadcast")
	}
}

func TestRun_RemoteUpdateWritesToFile(t *testing.T) {
	c, _ := setupCRDT(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fileSync := make(chan types.SettingEntry, 1)
	c.OnFileSync = func(e types.SettingEntry) { fileSync <- e }
	go c.Run(ctx)

	c.NotifyRemote(entry("theme", "dark", hlc(100, 0, "remote-node")))

	select {
	case fileEntry := <-fileSync:
		if fileEntry.Key != "theme" || fileEntry.Value != "dark" {
			t.Fatalf("unexpected file entry: %+v", fileEntry)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for file sync")
	}
}

func TestRun_RemoteOlderThanLocalNotWrittenToFile(t *testing.T) {
	c, _ := setupCRDT(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	updates := make(chan types.SettingEntry, 1)
	c.OnBroadcast = func(e types.SettingEntry) { updates <- e }
	fileSync := make(chan types.SettingEntry, 1)
	c.OnFileSync = func(e types.SettingEntry) { fileSync <- e }
	go c.Run(ctx)

	// push a newer local entry first
	c.NotifyLocal(entry("theme", "dark", hlc(200, 0, "test-node")))
	<-updates // drain broadcast

	// push older remote
	c.NotifyRemote(entry("theme", "light", hlc(100, 0, "remote-node")))

	select {
	case e := <-fileSync:
		t.Fatalf("expected no file sync for stale remote, got: %+v", e)
	case <-time.After(100 * time.Millisecond):
		// correct — nothing written
	}
}

func TestRun_Get(t *testing.T) {
	c, _ := setupCRDT(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sync := make(chan struct{})
	c.OnBroadcast = func(types.SettingEntry) { close(sync) }
	go c.Run(ctx)

	c.NotifyLocal(types.SettingEntry{Key: "theme", Value: "dark"})
	<-sync // wait for processing

	result := c.Get("theme")
	if result.Value != "dark" {
		t.Fatalf("expected dark, got %s", result.Value)
	}
}

func TestRun_GetMissingKey(t *testing.T) {
	c, _ := setupCRDT(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	result := c.Get("nonexistent")
	if result.Key != "" {
		t.Fatalf("expected zero value for missing key, got %+v", result)
	}
}

// clock tests

func TestRun_LocalEntryGetsClockStamped(t *testing.T) {
	c, _ := setupCRDT(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	updates := make(chan types.SettingEntry, 1)
	c.OnBroadcast = func(e types.SettingEntry) { updates <- e }
	go c.Run(ctx)

	c.NotifyLocal(types.SettingEntry{Key: "theme", Value: "dark"}) // no clock set
	broadcast := <-updates

	if broadcast.Clock.WallTime == 0 {
		t.Fatal("expected clock to be stamped on local entry")
	}
	if broadcast.Clock.NodeID != "test-node" {
		t.Fatalf("expected nodeID test-node, got %s", broadcast.Clock.NodeID)
	}
}

func TestRun_ClockMonotonicallyIncreases(t *testing.T) {
	c, _ := setupCRDT(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	updates := make(chan types.SettingEntry, 2)
	c.OnBroadcast = func(e types.SettingEntry) { updates <- e }
	go c.Run(ctx)

	c.NotifyLocal(types.SettingEntry{Key: "a", Value: "1"})
	first := <-updates

	c.NotifyLocal(types.SettingEntry{Key: "b", Value: "2"})
	second := <-updates

	if !first.Clock.Before(second.Clock) {
		t.Fatalf("expected second clock to be after first: %+v >= %+v", first.Clock, second.Clock)
	}
}

// snapshot tests

func TestSnapshot_IsCopy(t *testing.T) {
	c, _ := setupCRDT(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sync := make(chan struct{})
	c.OnBroadcast = func(types.SettingEntry) { close(sync) }
	go c.Run(ctx)

	c.NotifyLocal(types.SettingEntry{Key: "theme", Value: "dark"})
	<-sync

	snap := c.Snapshot()
	snap.Entries["theme"] = types.SettingEntry{Value: "mutated"} // mutate snapshot

	result := c.Get("theme")
	if result.Value == "mutated" {
		t.Fatal("snapshot mutation affected internal state")
	}
}
