package controller

import (
	"context"
	"fmt"

	"github.com/gdr00/distributed-server-update/internal/crdt"
	"github.com/gdr00/distributed-server-update/internal/network"
	"github.com/gdr00/distributed-server-update/internal/network/userpb"
	"github.com/gdr00/distributed-server-update/internal/types"
)

type Controller struct {
	cfg     types.Config
	crdt    *crdt.CRDT
	network *network.UpdateServer
	clients []*network.Client
	//logic   *logic.Logic
}

func New(cfg types.Config) *Controller {
	return &Controller{
		cfg:     cfg,
		crdt:    crdt.New(),
		network: network.NewUpdateServer(),
		clients: network.NewClients(cfg.PeerAddresses),
		//logic:   logic.New(cfg.SettingsPath),
	}
}

func (ctrl *Controller) Run(ctx context.Context) error {

	if err := network.StartRPCServer(ctx, ctrl.network, ctrl.cfg.GRPCPort); err != nil {
		return fmt.Errorf("failed to start RPC server: %w", err)
	}

	go ctrl.crdt.Run(ctx)

	// logic → crdt //TODO
	//go ctrl.logic.Watch(ctx, func(entry types.SettingEntry) {
	//	ctrl.crdt.NotifyLocal(entry)
	//})

	// crdt → logic (write file on remote change) //TODO
	//go func() {
	//	for snapshot := range ctrl.crdt.FileSync() {
	//		ctrl.logic.Write(snapshot)
	//	}
	//}()

	// crdt → network (broadcast to peers)
	go func() {
		for entry := range ctrl.crdt.Updates() {
			ctrl.network.Broadcast(&userpb.ServerStateUpdate{
				Entry: network.ToProto(entry),
			})
		}
	}()

	// network → crdt (incoming remote updates)
	for _, client := range ctrl.clients {
		go client.Subscribe(ctx, network.SnapshotToProto(ctrl.crdt.Snapshot()), func(update *userpb.ServerStateUpdate) {
			ctrl.crdt.NotifyRemote(network.FromProto(update.Entry))
		})
	}

	<-ctx.Done()
	return nil
}
