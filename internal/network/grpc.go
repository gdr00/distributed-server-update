package network

import (
	"context"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/gdr00/distributed-server-update/internal/network/userpb"
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
			// Skip client update on full buffer
		}
	}
}

type UpdateServer struct {
	userpb.UnimplementedUpdateServiceServer
}

func (s *UpdateServer) BroadcastUpdate(ctx context.Context, update *userpb.ServerStateUpdate) (*emptypb.Empty, error) {
	broadcastUpdate(update)
	return &emptypb.Empty{}, nil
}

func (s *UpdateServer) SubscribeStateUpdates(req *emptypb.Empty, stream userpb.UpdateService_SubscribeStateUpdatesServer) error {
	subCh := make(chan *userpb.ServerStateUpdate, 10)
	subscriberID := addSubscriber(subCh)
	defer func() {
		removeSubscriber(subscriberID)
		close(subCh)
	}()

	//Heartbeat for client connection healthcheck
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-ticker.C:
			if err := stream.Send(&userpb.ServerStateUpdate{}); err != nil {
				return err
			}
		case _, ok := <-subCh:
			if !ok {
				return nil
			}
		}
	}
}
