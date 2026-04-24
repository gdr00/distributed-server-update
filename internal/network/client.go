package network

import (
	"fmt"

	"github.com/gdr00/distributed-server-update/internal/network/userpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func RegisterClient(serverAddress string) (*grpc.ClientConn, userpb.UpdateServiceClient, error) {
	conn, err := grpc.NewClient(serverAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("dial failed: %w", err)
	}

	client := userpb.NewUpdateServiceClient(conn)

	return conn, client, nil
}
