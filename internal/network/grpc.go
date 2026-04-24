package network

import (
	"sync"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/gdr00/distributed-server-update/internal/network/userpb"
)

type UpdateServer struct {
	userpb.UnimplementedUpdateServiceServer
	mu          sync.RWMutex
	subscribers map[int]chan *userpb.ServerStateUpdate
	nextSubID   int
}

func NewUpdateServer() *UpdateServer {
	return &UpdateServer{
		subscribers: make(map[int]chan *userpb.ServerStateUpdate),
	}
}

func (s *UpdateServer) Broadcast(update *userpb.ServerStateUpdate) {
	s.mu.RLock()
	chs := make([]chan *userpb.ServerStateUpdate, 0, len(s.subscribers))
	for _, ch := range s.subscribers {
		chs = append(chs, ch)
	}
	s.mu.RUnlock()

	for _, ch := range chs {
		select {
		case ch <- update:
		default: // slow client, drop
		}
	}
}

func (s *UpdateServer) SubscribeStateUpdates(req *emptypb.Empty, stream userpb.UpdateService_SubscribeStateUpdatesServer) error {
	ch := make(chan *userpb.ServerStateUpdate, 10)

	s.mu.Lock()
	id := s.nextSubID
	s.nextSubID++
	s.subscribers[id] = ch
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.subscribers, id)
		s.mu.Unlock()
		close(ch)
	}()

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case update, ok := <-ch:
			if !ok {
				return nil
			}
			if err := stream.Send(update); err != nil {
				return err
			}
		}
	}
}
