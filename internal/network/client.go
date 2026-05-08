package network

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/gdr00/distributed-server-update/internal/network/userpb"
	"github.com/gdr00/distributed-server-update/internal/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Client struct {
	conn   *grpc.ClientConn
	client userpb.UpdateServiceClient
}

func NewClients(addrs []string) ([]*Client, error) {
	clients := make([]*Client, 0, len(addrs))
	for _, addr := range addrs {
		host, port, err := net.SplitHostPort(addr)
		if err != nil || host == "" || port == "" {
			return nil, fmt.Errorf("invalid peer address %q: %v", addr, err)
		}
		c, err := NewClient(addr)
		if err != nil {
			return nil, fmt.Errorf("failed to create client for %s: %w", addr, err)
		}
		clients = append(clients, c)
	}
	return clients, nil
}

func NewClient(serverAddress string) (*Client, error) {
	conn, err := grpc.NewClient(serverAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                20 * time.Second,
			Timeout:             5 * time.Second,
			PermitWithoutStream: true,
		}))
	if err != nil {
		return nil, fmt.Errorf("dial failed: %w", err)
	}

	return &Client{conn: conn, client: userpb.NewUpdateServiceClient(conn)}, nil
}

func (c *Client) Close() error { return c.conn.Close() }

func (c *Client) Subscribe(ctx context.Context, getSnapshot func() types.Snapshot, onUpdate func(types.SettingEntry)) {
	for {
		s := getSnapshot()
		if err := c.runStream(ctx, s, onUpdate); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("stream broken, reconnecting: %v", err)
			time.Sleep(3 * time.Second)
		}
	}
}

func (c *Client) Sync(ctx context.Context, localSnapshot types.Snapshot, onUpdate func(types.SettingEntry)) error {
	resp, err := c.client.Sync(ctx, &userpb.SyncRequest{LocalState: SnapshotToProto(localSnapshot)})
	if err != nil {
		return fmt.Errorf("sync failed: %w", err)
	}
	for _, entry := range resp.NewerEntries {
		onUpdate(FromProto(entry))
	}
	return nil
}

func (c *Client) runStream(ctx context.Context, localSnapshot types.Snapshot, onUpdate func(types.SettingEntry)) error {
	if err := c.Sync(ctx, localSnapshot, onUpdate); err != nil {
		return err
	}
	stream, err := c.client.SubscribeStateUpdates(ctx, &emptypb.Empty{})
	if err != nil {
		return err
	}
	for {
		update, err := stream.Recv()
		if err != nil {
			return err
		}
		onUpdate(FromProto(update.Entry))
	}
}
