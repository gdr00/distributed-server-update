package network

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/gdr00/distributed-server-update/internal/network/userpb"
	"google.golang.org/grpc"
)

// Server start blackbox
// Auto handles graceful shutdown on os sigint/sigterm,
func StartRPCServer(ctx context.Context, us *UpdateServer, port uint16) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
		return err
	}

	grpcServer := grpc.NewServer()
	userpb.RegisterUpdateServiceServer(grpcServer, us)

	log.Printf("server listening on %s", lis.Addr().String())

	go func() {
		<-ctx.Done()
		grpcServer.Stop()
	}()

	go func() {
		if err := grpcServer.Serve(lis); err != grpc.ErrServerStopped {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	return nil
}
