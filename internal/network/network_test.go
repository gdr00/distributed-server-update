package network

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/gdr00/distributed-server-update/internal/network/userpb"
	"github.com/gdr00/distributed-server-update/internal/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// ── helpers ──────────────────────────────────────────────────────────────────

const bufSize = 1024 * 1024

// startTestServer spins up a real gRPC server over an in-memory bufconn.
// Returns the server, a client connected to it, and a cancel func.
func startTestServer(t *testing.T, snapshot func() types.Snapshot) (*UpdateServer, userpb.UpdateServiceClient, func()) {
	t.Helper()

	if snapshot == nil {
		snapshot = func() types.Snapshot {
			return types.Snapshot{Entries: map[string]types.SettingEntry{}}
		}
	}

	lis := bufconn.Listen(bufSize)
	srv := NewUpdateServer(snapshot)

	grpcSrv := grpc.NewServer()
	userpb.RegisterUpdateServiceServer(grpcSrv, srv)

	go grpcSrv.Serve(lis)

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial bufconn: %v", err)
	}

	client := userpb.NewUpdateServiceClient(conn)

	cleanup := func() {
		conn.Close()
		grpcSrv.Stop()
		lis.Close()
	}

	return srv, client, cleanup
}

func hlc(wall int64, logical uint32, node string) types.HLC {
	return types.HLC{WallTime: wall, Logical: logical, NodeID: node}
}

func entry(key, value string, h types.HLC) types.SettingEntry {
	return types.SettingEntry{Key: key, Value: value, Clock: h}
}

// ── ToProto / FromProto ───────────────────────────────────────────────────────

func TestToProto_RoundTrip(t *testing.T) {
	original := entry("theme", "dark", hlc(100, 1, "nodeA"))
	original.Deleted = false

	proto := ToProto(original)
	restored := FromProto(proto)

	if restored.Key != original.Key {
		t.Errorf("key mismatch: %s != %s", restored.Key, original.Key)
	}
	if restored.Value != original.Value {
		t.Errorf("value mismatch: %s != %s", restored.Value, original.Value)
	}
	if restored.Clock.WallTime != original.Clock.WallTime {
		t.Errorf("walltime mismatch")
	}
	if restored.Clock.Logical != original.Clock.Logical {
		t.Errorf("logical mismatch")
	}
	if restored.Clock.NodeID != original.Clock.NodeID {
		t.Errorf("nodeID mismatch: %s != %s", restored.Clock.NodeID, original.Clock.NodeID)
	}
	if restored.Deleted != original.Deleted {
		t.Errorf("deleted mismatch")
	}
}

func TestToProto_TombstonePreserved(t *testing.T) {
	e := types.SettingEntry{Key: "theme", Deleted: true, Clock: hlc(100, 0, "A")}
	if !ToProto(e).Deleted {
		t.Error("expected Deleted to be preserved in proto")
	}
}

func TestSnapshotToProto_AllEntriesConverted(t *testing.T) {
	snap := types.Snapshot{
		Entries: map[string]types.SettingEntry{
			"a": entry("a", "1", hlc(1, 0, "A")),
			"b": entry("b", "2", hlc(2, 0, "A")),
		},
	}
	result := SnapshotToProto(snap)
	if len(result) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(result))
	}
}

func TestSnapshotToProto_EmptySnapshot(t *testing.T) {
	result := SnapshotToProto(types.Snapshot{Entries: map[string]types.SettingEntry{}})
	if len(result) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(result))
	}
}

// ── Broadcast ────────────────────────────────────────────────────────────────

func TestBroadcast_DeliverToSubscriber(t *testing.T) {
	srv, client, cleanup := startTestServer(t, nil)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := client.SubscribeStateUpdates(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond) // let subscriber register

	update := &userpb.ServerStateUpdate{Entry: ToProto(entry("theme", "dark", hlc(100, 0, "A")))}
	srv.Broadcast(update)

	received := make(chan *userpb.ServerStateUpdate, 1)
	go func() {
		msg, err := stream.Recv()
		if err == nil {
			received <- msg
		}
	}()

	select {
	case msg := <-received:
		if msg.Entry.Key != "theme" {
			t.Fatalf("unexpected key: %s", msg.Entry.Key)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for broadcast")
	}
}

func TestBroadcast_MultipleSubscribers(t *testing.T) {
	srv, client, cleanup := startTestServer(t, nil)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream1, _ := client.SubscribeStateUpdates(ctx, nil)
	stream2, _ := client.SubscribeStateUpdates(ctx, nil)

	time.Sleep(50 * time.Millisecond)

	srv.Broadcast(&userpb.ServerStateUpdate{
		Entry: ToProto(entry("theme", "dark", hlc(100, 0, "A"))),
	})

	for i, stream := range []userpb.UpdateService_SubscribeStateUpdatesClient{stream1, stream2} {
		received := make(chan struct{}, 1)
		s := stream
		go func() {
			s.Recv()
			received <- struct{}{}
		}()
		select {
		case <-received:
		case <-time.After(2 * time.Second):
			t.Fatalf("subscriber %d did not receive broadcast", i+1)
		}
	}
}

func TestBroadcast_NoSubscribersIsNoop(t *testing.T) {
	srv := NewUpdateServer(func() types.Snapshot {
		return types.Snapshot{Entries: map[string]types.SettingEntry{}}
	})
	// should not panic or block
	srv.Broadcast(&userpb.ServerStateUpdate{
		Entry: ToProto(entry("theme", "dark", hlc(100, 0, "A"))),
	})
}

// ── Sync ─────────────────────────────────────────────────────────────────────

func TestSync_ReturnsNewerEntries(t *testing.T) {
	serverState := types.Snapshot{
		Entries: map[string]types.SettingEntry{
			"theme": entry("theme", "dark", hlc(200, 0, "server")),
		},
	}
	_, client, cleanup := startTestServer(t, func() types.Snapshot { return serverState })
	defer cleanup()

	// client has older version
	resp, err := client.Sync(context.Background(), &userpb.SyncRequest{
		LocalState: []*userpb.SettingEntry{
			ToProto(entry("theme", "light", hlc(100, 0, "client"))),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.NewerEntries) != 1 {
		t.Fatalf("expected 1 newer entry, got %d", len(resp.NewerEntries))
	}
	if resp.NewerEntries[0].Value != "dark" {
		t.Fatalf("expected dark, got %s", resp.NewerEntries[0].Value)
	}
}

func TestSync_ClientNewerNotReturned(t *testing.T) {
	serverState := types.Snapshot{
		Entries: map[string]types.SettingEntry{
			"theme": entry("theme", "dark", hlc(100, 0, "server")),
		},
	}
	_, client, cleanup := startTestServer(t, func() types.Snapshot { return serverState })
	defer cleanup()

	// client has newer version
	resp, err := client.Sync(context.Background(), &userpb.SyncRequest{
		LocalState: []*userpb.SettingEntry{
			ToProto(entry("theme", "light", hlc(200, 0, "client"))),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.NewerEntries) != 0 {
		t.Fatalf("expected 0 newer entries, got %d", len(resp.NewerEntries))
	}
}

func TestSync_MissingKeyOnClientIsReturned(t *testing.T) {
	serverState := types.Snapshot{
		Entries: map[string]types.SettingEntry{
			"theme": entry("theme", "dark", hlc(100, 0, "server")),
			"lang":  entry("lang", "en", hlc(100, 0, "server")),
		},
	}
	_, client, cleanup := startTestServer(t, func() types.Snapshot { return serverState })
	defer cleanup()

	// client has newer theme so it won't be returned, but is missing lang
	resp, err := client.Sync(context.Background(), &userpb.SyncRequest{
		LocalState: []*userpb.SettingEntry{
			ToProto(entry("theme", "dark", hlc(200, 0, "client"))), // newer than server
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.NewerEntries) != 1 {
		t.Fatalf("expected 1 entry, got %d: %+v", len(resp.NewerEntries), resp.NewerEntries)
	}
	if resp.NewerEntries[0].Key != "lang" {
		t.Fatalf("expected lang to be returned, got %s", resp.NewerEntries[0].Key)
	}
}

func TestSync_EmptyLocalStateGetsEverything(t *testing.T) {
	serverState := types.Snapshot{
		Entries: map[string]types.SettingEntry{
			"theme": entry("theme", "dark", hlc(100, 0, "server")),
			"lang":  entry("lang", "en", hlc(100, 0, "server")),
		},
	}
	_, client, cleanup := startTestServer(t, func() types.Snapshot { return serverState })
	defer cleanup()

	resp, err := client.Sync(context.Background(), &userpb.SyncRequest{
		LocalState: []*userpb.SettingEntry{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.NewerEntries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(resp.NewerEntries))
	}
}

// ── StartRPCServer ────────────────────────────────────────────────────────────

func TestStartRPCServer_StartsAndStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	srv := NewUpdateServer(func() types.Snapshot {
		return types.Snapshot{Entries: map[string]types.SettingEntry{}}
	})

	if err := StartRPCServer(ctx, srv, 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	cancel()                           // trigger graceful shutdown
	time.Sleep(100 * time.Millisecond) // let it stop
}
