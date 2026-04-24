package network

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/gdr00/distributed-server-update/internal/network/userpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// Server start blackbox
// Auto handles graceful shutdown on os sigint/sigterm,
func StartRPCServer(ctx context.Context, us *UpdateServer, port uint16) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    20 * time.Second, // ping client if idle for 20s
			Timeout: 5 * time.Second,  // close connection if no response in 5s
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second, // reject client pings faster than this
			PermitWithoutStream: true,             // allow pings even with no active streams
		}),
	)
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
