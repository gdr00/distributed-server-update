package controller

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

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

func New(cfg types.Config) *Controller {
	ctrl := &Controller{
		cfg:     cfg,
		crdt:    crdt.New(cfg.CRDTWorkdir),
		clients: network.NewClients(cfg.PeerAddresses),
		logic:   logic.New(cfg.SettingsPath),
	}
	ctrl.network = network.NewUpdateServer(func() types.Snapshot {
		return ctrl.crdt.Snapshot()
	})
	return ctrl
}

// Init node with the "master" configuration when new system is initialized
func InitNode(settingsPath string, workDir string) error {
	entries, err := logic.New(settingsPath).Read()
	if err != nil {
		return fmt.Errorf("failed to read settings: %w", err)
	}
	if err := crdt.Init(entries, workDir); err != nil {
		return fmt.Errorf("failed to init crdt: %w", err)
	}
	return nil
}

// Init node to sync from others on the network
func InitEmptyNode(workDir string) error {
	if err := os.MkdirAll(workDir, 0700); err != nil {
		return fmt.Errorf("failed to create config dir: %w", err)
	}

	nodeID := uuid.New().String()
	if err := os.WriteFile(filepath.Join(workDir, "node_id"), []byte(nodeID), 0600); err != nil {
		return fmt.Errorf("failed to write node_id: %w", err)
	}

	if err := os.WriteFile(filepath.Join(workDir, "crdt_state.json"), []byte("{}"), 0600); err != nil {
		return fmt.Errorf("failed to write crdt state: %w", err)
	}

	log.Printf("empty node initialized with ID %s", nodeID)
	return nil
}

func (ctrl *Controller) Run(ctx context.Context) error {

	if err := network.StartRPCServer(ctx, ctrl.network, ctrl.cfg.GRPCPort); err != nil {
		return fmt.Errorf("failed to start RPC server: %w", err)
	}

	go ctrl.crdt.Run(ctx)

	// logic → crdt
	go ctrl.logic.Watch(ctx, func(entry types.SettingEntry) {
		ctrl.crdt.NotifyLocal(entry)
	})

	// crdt → logic (write file on remote change)
	go func() {
		for entry := range ctrl.crdt.FileSync() {
			ctrl.logic.Write(entry)
		}
	}()

	// network → crdt (incoming remote updates)
	for _, client := range ctrl.clients {
		go client.Subscribe(ctx, network.SnapshotToProto(ctrl.crdt.Snapshot()), func(update *userpb.ServerStateUpdate) {
			ctrl.crdt.NotifyRemote(network.FromProto(update.Entry))
		})
	}

	// crdt → network (broadcast to peers)
	go func() {
		for entry := range ctrl.crdt.Updates() {
			ctrl.network.Broadcast(&userpb.ServerStateUpdate{
				Entry: network.ToProto(entry),
			})
		}
	}()

	<-ctx.Done()
	return nil
}
