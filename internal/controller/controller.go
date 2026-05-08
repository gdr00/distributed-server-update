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
	"github.com/gdr00/distributed-server-update/internal/network/userpb"
	"github.com/gdr00/distributed-server-update/internal/types"
	"github.com/google/uuid"
)

type Controller struct {
	cfg     types.Config
	crdt    *crdt.CRDT
	network *network.UpdateServer
	clients []*network.Client
	logic   *logic.Logic
}

func New(cfg types.Config) (*Controller, error) {
	c, err := crdt.New(cfg.CRDTWorkdir)
	if err != nil {
		return nil, fmt.Errorf("failed to load crdt: %w", err)
	}
	clients, err := network.NewClients(cfg.PeerAddresses)
	if err != nil {
		return nil, fmt.Errorf("failed to create peer clients: %w", err)
	}
	ctrl := &Controller{
		cfg:     cfg,
		crdt:    c,
		clients: clients,
		logic:   logic.New(cfg.SettingsPath),
	}
	ctrl.network = network.NewUpdateServer(func() types.Snapshot {
		return ctrl.crdt.Snapshot()
	})
	return ctrl, nil
}

// Init node with the "master" configuration when new system is initialized
func InitNode(c types.Config) error {
	entries, err := logic.New(c.SettingsPath).Read()
	if err != nil {
		return fmt.Errorf("failed to read settings: %w", err)
	}
	if err := crdt.Init(entries, c.CRDTWorkdir); err != nil {
		return fmt.Errorf("failed to init crdt: %w", err)
	}
	return nil
}

// Init node to sync from others on the network
func InitEmptyNode(cfg types.Config) error {
	if err := os.MkdirAll(cfg.CRDTWorkdir, 0700); err != nil {
		return fmt.Errorf("failed to create config dir: %w", err)
	}

	nodeID := uuid.New().String()
	if err := os.WriteFile(filepath.Join(cfg.CRDTWorkdir, "node_id"), []byte(nodeID), 0600); err != nil {
		return fmt.Errorf("failed to write node_id: %w", err)
	}

	if err := os.WriteFile(filepath.Join(cfg.CRDTWorkdir, "crdt_state.json"), []byte("{}"), 0600); err != nil {
		return fmt.Errorf("failed to write crdt state: %w", err)
	}

	// create empty settings file so watcher can start
	settingsDir := filepath.Dir(cfg.SettingsPath)
	if err := os.MkdirAll(settingsDir, 0700); err != nil {
		return fmt.Errorf("failed to create settings dir: %w", err)
	}
	if err := os.WriteFile(cfg.SettingsPath, []byte("{}"), 0600); err != nil {
		return fmt.Errorf("failed to write settings file: %w", err)
	}

	log.Printf("empty node initialized with ID %s", nodeID)
	return nil
}

func (ctrl *Controller) Run(ctx context.Context) error {

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
	ctrl.crdt.OnBroadcast = func(entry types.SettingEntry) {
		ctrl.network.Broadcast(&userpb.ServerStateUpdate{
			Entry: network.ToProto(entry),
		})
	}

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

	<-ctx.Done()
	return nil
}

func (ctrl *Controller) runAntiEntropy(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Second)
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
