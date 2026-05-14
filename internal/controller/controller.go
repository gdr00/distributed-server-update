package controller

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
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
	c := crdt.New(cfg.CRDTWorkdir)
	if err := c.Init(); err != nil {
		return nil, fmt.Errorf("failed to load crdt: %w", err)
	}

	clients, err := network.NewClients(cfg.PeerAddresses)
	if err != nil {
		return nil, fmt.Errorf("failed to create peer clients: %w", err)
	}

	ctrl := &Controller{
		cfg:                 cfg,
		crdt:                c,
		clients:             clients,
		logic:               logic.New(cfg.SettingsPath),
		tombstoneTTL:        int64(2 * 7 * 24 * time.Hour),
		antiEntropyInterval: 60 * time.Second,
		gcInterval:          24 * time.Hour,
	}
	ctrl.network = network.NewUpdateServer(
		func() types.Snapshot {
			return ctrl.crdt.Snapshot()
		},
		func(entry types.SettingEntry) {
			ctrl.crdt.NotifyRemote(entry)
		})
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

	// reconcile settings file with CRDT state
	ctrl.crdt.Reconcile(func(entry types.SettingEntry) {
		ctrl.logic.Write(entry)
	})

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
