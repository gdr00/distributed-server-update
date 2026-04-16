package network

import (
	"context"
	"sync"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/gdr00/distributed-server-update/userpb"
)

var (
	subscriberMu sync.Mutex
	subscribers  = make(map[int]chan *userpb.ServerStateUpdate)
	nextSubID    int
)

func addSubscriber(ch chan *userpb.ServerStateUpdate) int {
	subscriberMu.Lock()
	defer subscriberMu.Unlock()
	id := nextSubID
	nextSubID++
	subscribers[id] = ch
	return id
}

func removeSubscriber(id int) {
	subscriberMu.Lock()
	defer subscriberMu.Unlock()
	delete(subscribers, id)
}

func broadcastUpdate(update *userpb.ServerStateUpdate) {
	subscriberMu.Lock()
	defer subscriberMu.Unlock()
	for _, ch := range subscribers {
		select {
		case ch <- update:
		default:
			go func(ch chan *userpb.ServerStateUpdate, u *userpb.ServerStateUpdate) {
				ch <- u
			}(ch, update)
		}
	}
}

// GRPCServer implements the UpdateServer gRPC server.
type GRPCServer struct {
	userpb.UnimplementedUpdateServiceServer
}

func (s *GRPCServer) BroadcastUpdate(ctx context.Context, update *userpb.ServerStateUpdate) (*emptypb.Empty, error) {
	broadcastUpdate(update)
	return &emptypb.Empty{}, nil
}

func (s *GRPCServer) SubscribeStateUpdates(req *emptypb.Empty, stream userpb.UpdateService_SubscribeStateUpdatesServer) error {
	subCh := make(chan *userpb.ServerStateUpdate, 10)
	subscriberID := addSubscriber(subCh)
	defer func() {
		removeSubscriber(subscriberID)
		close(subCh)
	}()

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case _, ok := <-subCh:
			if !ok {
				return nil
			}
		}
	}
}
