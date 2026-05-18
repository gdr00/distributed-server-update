package controller

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gdr00/distributed-server-update/internal/crdt"
	"github.com/gdr00/distributed-server-update/internal/logic"
	"github.com/gdr00/distributed-server-update/internal/network"
	"github.com/gdr00/distributed-server-update/internal/types"
)

type Controller struct {
	cfg                 Config
	crdt                *crdt.CRDT
	network             *network.UpdateServer
	clients             []*network.Client
	logic               *logic.Logic
	tombstoneTTL        int64
	antiEntropyInterval time.Duration
	gcInterval          time.Duration
}

func New(cfg Config) (*Controller, error) {
	clients, err := network.NewClients(cfg.PeerAddresses)
	if err != nil {
		return nil, fmt.Errorf("failed to create peer clients: %w", err)
	}

	antiEntropy := 60 * time.Second
	if cfg.AntiEntropySeconds > 0 {
		antiEntropy = time.Duration(cfg.AntiEntropySeconds) * time.Second
	}

	ctrl := &Controller{
		cfg:                 cfg,
		crdt:                crdt.New(cfg.CRDTWorkdir),
		clients:             clients,
		logic:               logic.New(cfg.SettingsPath),
		tombstoneTTL:        int64(2 * 7 * 24 * time.Hour),
		antiEntropyInterval: antiEntropy,
		gcInterval:          24 * time.Hour,
	}

	if err := ctrl.fixNodeState(ctrl.checkLastShutdown()); err != nil {
		return nil, fmt.Errorf("failed to initialize crdt: %w", err)
	}

	ctrl.network = network.NewUpdateServer(
		func() types.Snapshot {
			return ctrl.crdt.Snapshot()
		},
		func(entry types.SettingEntry) {
			ctrl.crdt.NotifyRemote(entry)
		},
		ctrl.tombstoneTTL)
	return ctrl, nil
}

// Init node with the "master" configuration when new system is initialized
func InitNode(cfg Config) error {
	entries, err := logic.New(cfg.SettingsPath).ReadSettings()
	if err != nil {
		return fmt.Errorf("failed to read settings: %w", err)
	}

	c := crdt.New(cfg.CRDTWorkdir)
	if err := c.InitNew(entries); err != nil {
		return fmt.Errorf("failed to init crdt: %w", err)
	}
	return nil
}

// Init node to sync from others on the network
func InitEmptyNode(cfg Config) error {
	c := crdt.New(cfg.CRDTWorkdir)
	if err := c.InitNew(types.Settings{}); err != nil {
		return fmt.Errorf("failed to initialize new empty node: %w", err)
	}

	settingsDir := filepath.Dir(cfg.SettingsPath)
	if err := os.MkdirAll(settingsDir, 0700); err != nil {
		return fmt.Errorf("failed to create settings dir: %w", err)
	}
	if err := os.WriteFile(cfg.SettingsPath, []byte("{}"), 0600); err != nil {
		return fmt.Errorf("failed to write settings file: %w", err)
	}

	log.Printf("empty node initialized")
	return nil
}

func (ctrl *Controller) Run(ctx context.Context) error {

	defer ctrl.saveShutdownTime()

	// reconcile settings file with CRDT state — full replacement to drop stale entries
	settings := make(types.Settings)
	ctrl.crdt.Reconcile(func(entry types.SettingEntry) {
		if !entry.Deleted {
			settings[entry.Key] = entry.Value
		}
	})
	if err := ctrl.logic.Overwrite(settings); err != nil {
		log.Printf("failed to reconcile settings file: %v", err)
	}

	if err := network.StartRPCServer(ctx, ctrl.network, ctrl.cfg.GRPCPort); err != nil {
		return fmt.Errorf("failed to start RPC server: %w", err)
	}

	// crdt → logic (write file on remote change)
	ctrl.crdt.OnFileSync = func(entry types.SettingEntry) {
		// inside gorutine to avoid locking crdt Run loop on disk write
		go func() {
			if err := ctrl.logic.Write(entry); err != nil {
				log.Printf("failed to write settings: %v", err)
			}
		}()
	}

	// crdt → network (broadcast to peers)
	ctrl.crdt.OnBroadcast = ctrl.network.Broadcast

	go ctrl.crdt.Run(ctx)

	// logic → crdt (local file changes)
	go func() {
		if err := ctrl.logic.Watch(ctx, func(entry types.SettingEntry) {
			ctrl.crdt.NotifyLocal(entry)
		}); err != nil {
			log.Printf("file watcher error: %v", err)
		}
	}()

	// network → crdt (incoming remote updates)
	for _, client := range ctrl.clients {
		go client.Subscribe(ctx, ctrl.crdt.Snapshot, func(entry types.SettingEntry) {
			ctrl.crdt.NotifyRemote(entry)
		})
	}

	go ctrl.runAntiEntropy(ctx)

	go ctrl.runTombstoneGC(ctx)

	<-ctx.Done()
	return nil
}

func (ctrl *Controller) runAntiEntropy(ctx context.Context) {
	ticker := time.NewTicker(ctrl.antiEntropyInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ctrl.syncAllPeers(ctx)
		}
	}
}

func (ctrl *Controller) syncAllPeers(ctx context.Context) {
	snap := ctrl.crdt.Snapshot()
	for _, client := range ctrl.clients {
		go client.Sync(ctx, snap, func(entry types.SettingEntry) {
			ctrl.crdt.NotifyRemote(entry)
		})
	}
}

func (ctrl *Controller) runTombstoneGC(ctx context.Context) {
	ticker := time.NewTicker(ctrl.gcInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ctrl.crdt.PurgeTombstones(ctrl.tombstoneTTL)
		}
	}
}

func (ctrl *Controller) saveShutdownTime() {
	path := filepath.Join(ctrl.cfg.CRDTWorkdir, "last_shutdown")
	data := strconv.FormatInt(time.Now().UnixNano(), 10)
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		log.Printf("failed to save shutdown time: %v", err)
	}
}

// checkLastShutdown returns the last clean shutdown time in nanoseconds,
// or -1 if the file is missing (first startup after InitNew).
func (ctrl *Controller) checkLastShutdown() int64 {
	path := filepath.Join(ctrl.cfg.CRDTWorkdir, "last_shutdown")
	data, err := os.ReadFile(path)
	if err != nil {
		return -1
	}
	t, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return -1
	}
	return t
}

// fixNodeState selects the CRDT init path based on last shutdown time.
// -1 first startup, load normally
// check if node passed past TTL GC period
func (ctrl *Controller) fixNodeState(lastShutdown int64) error {
	if lastShutdown != -1 && time.Now().UnixNano()-lastShutdown > ctrl.tombstoneTTL {
		log.Printf("node offline longer than tombstone TTL, reinitializing from empty state")
		return ctrl.crdt.InitNew(types.Settings{})
	}
	return ctrl.crdt.Init()
}
