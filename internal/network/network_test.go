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

var testWallTTL = int64(14 * 24 * time.Hour)

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
	srv := NewUpdateServer(snapshot, nil, testWallTTL)

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

func startTestServerWithLis(t *testing.T, snapshot func() types.Snapshot) (*UpdateServer, *bufconn.Listener) {
	t.Helper()
	if snapshot == nil {
		snapshot = func() types.Snapshot {
			return types.Snapshot{Entries: map[string]types.SettingEntry{}}
		}
	}
	lis := bufconn.Listen(bufSize)
	srv := NewUpdateServer(snapshot, nil, testWallTTL)
	grpcSrv := grpc.NewServer()
	userpb.RegisterUpdateServiceServer(grpcSrv, srv)
	go grpcSrv.Serve(lis)
	t.Cleanup(func() {
		grpcSrv.Stop()
		lis.Close()
	})
	return srv, lis
}

func newBufconnClient(t *testing.T, lis *bufconn.Listener) *Client {
	t.Helper()
	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to create bufconn client: %v", err)
	}
	return &Client{conn: conn, client: userpb.NewUpdateServiceClient(conn)}
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

	srv.Broadcast(entry("theme", "dark", hlc(100, 0, "A")))

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

	srv.Broadcast(entry("theme", "dark", hlc(100, 0, "A")))

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
	}, nil, testWallTTL)
	// should not panic or block
	srv.Broadcast(entry("theme", "dark", hlc(100, 0, "A")))
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

func TestSync_ServerAppliesIncomingNewerEntry(t *testing.T) {
	serverState := types.Snapshot{
		Entries: map[string]types.SettingEntry{
			"theme": entry("theme", "dark", hlc(100, 0, "server")),
		},
	}

	applied := make(chan types.SettingEntry, 1)
	lis := bufconn.Listen(bufSize)
	srv := NewUpdateServer(func() types.Snapshot { return serverState }, func(e types.SettingEntry) {
		applied <- e
	}, testWallTTL)
	grpcSrv := grpc.NewServer()
	userpb.RegisterUpdateServiceServer(grpcSrv, srv)
	go grpcSrv.Serve(lis)
	defer grpcSrv.Stop()

	c := newBufconnClient(t, lis)
	defer c.Close()

	// client has newer version
	_, err := c.client.Sync(context.Background(), &userpb.SyncRequest{
		LocalState: []*userpb.SettingEntry{
			ToProto(entry("theme", "light", hlc(200, 0, "client"))),
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case e := <-applied:
		if e.Key != "theme" || e.Value != "light" {
			t.Fatalf("unexpected applied entry: %+v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout: server did not apply client's newer entry")
	}
}

func TestSync_ServerSkipsIncomingOlderEntry(t *testing.T) {
	serverState := types.Snapshot{
		Entries: map[string]types.SettingEntry{
			"theme": entry("theme", "dark", hlc(200, 0, "server")),
		},
	}

	applied := make(chan types.SettingEntry, 1)
	lis := bufconn.Listen(bufSize)
	srv := NewUpdateServer(func() types.Snapshot { return serverState }, func(e types.SettingEntry) {
		applied <- e
	}, testWallTTL)
	grpcSrv := grpc.NewServer()
	userpb.RegisterUpdateServiceServer(grpcSrv, srv)
	go grpcSrv.Serve(lis)
	defer grpcSrv.Stop()

	c := newBufconnClient(t, lis)
	defer c.Close()

	// client has older version
	_, err := c.client.Sync(context.Background(), &userpb.SyncRequest{
		LocalState: []*userpb.SettingEntry{
			ToProto(entry("theme", "light", hlc(100, 0, "client"))),
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case e := <-applied:
		t.Fatalf("server should not apply older client entry, got: %+v", e)
	case <-time.After(100 * time.Millisecond):
		// correct: nothing applied
	}
}

func TestSync_ServerAppliesIncomingMissingKey(t *testing.T) {
	// server has no entries; client has a key server doesn't know about
	applied := make(chan types.SettingEntry, 1)
	lis := bufconn.Listen(bufSize)
	srv := NewUpdateServer(func() types.Snapshot {
		return types.Snapshot{Entries: map[string]types.SettingEntry{}}
	}, func(e types.SettingEntry) {
		applied <- e
	}, testWallTTL)
	grpcSrv := grpc.NewServer()
	userpb.RegisterUpdateServiceServer(grpcSrv, srv)
	go grpcSrv.Serve(lis)
	defer grpcSrv.Stop()

	c := newBufconnClient(t, lis)
	defer c.Close()

	_, err := c.client.Sync(context.Background(), &userpb.SyncRequest{
		LocalState: []*userpb.SettingEntry{
			ToProto(entry("lang", "fr", hlc(100, 0, "client"))),
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case e := <-applied:
		if e.Key != "lang" || e.Value != "fr" {
			t.Fatalf("unexpected applied entry: %+v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout: server did not apply missing key from client")
	}
}

func TestSync_NilApplyRemoteNoPanic(t *testing.T) {
	// nil applyRemote must not panic even when client has newer entries
	serverState := types.Snapshot{
		Entries: map[string]types.SettingEntry{
			"theme": entry("theme", "dark", hlc(100, 0, "server")),
		},
	}
	_, client, cleanup := startTestServer(t, func() types.Snapshot { return serverState })
	defer cleanup()

	// client has newer version — applyRemote is nil in startTestServer
	_, err := client.Sync(context.Background(), &userpb.SyncRequest{
		LocalState: []*userpb.SettingEntry{
			ToProto(entry("theme", "light", hlc(200, 0, "client"))),
		},
	})
	if err != nil {
		t.Fatalf("unexpected error with nil applyRemote: %v", err)
	}
}

func TestSync_BothDirectionsExchanged(t *testing.T) {
	// server has newer theme, client has newer lang — both directions must work in one call
	serverState := types.Snapshot{
		Entries: map[string]types.SettingEntry{
			"theme": entry("theme", "dark", hlc(200, 0, "server")),
			"lang":  entry("lang", "en", hlc(100, 0, "server")),
		},
	}

	applied := make(chan types.SettingEntry, 2)
	lis := bufconn.Listen(bufSize)
	srv := NewUpdateServer(func() types.Snapshot { return serverState }, func(e types.SettingEntry) {
		applied <- e
	}, testWallTTL)
	grpcSrv := grpc.NewServer()
	userpb.RegisterUpdateServiceServer(grpcSrv, srv)
	go grpcSrv.Serve(lis)
	defer grpcSrv.Stop()

	c := newBufconnClient(t, lis)
	defer c.Close()

	resp, err := c.client.Sync(context.Background(), &userpb.SyncRequest{
		LocalState: []*userpb.SettingEntry{
			ToProto(entry("theme", "light", hlc(100, 0, "client"))), // older — server wins
			ToProto(entry("lang", "fr", hlc(200, 0, "client"))),     // newer — client wins
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// server → client: only theme (client's lang is newer)
	if len(resp.NewerEntries) != 1 || resp.NewerEntries[0].Key != "theme" {
		t.Fatalf("expected server to return only theme, got: %+v", resp.NewerEntries)
	}
	if resp.NewerEntries[0].Value != "dark" {
		t.Fatalf("expected dark, got %s", resp.NewerEntries[0].Value)
	}

	// client → server: lang must be applied
	select {
	case e := <-applied:
		if e.Key != "lang" || e.Value != "fr" {
			t.Fatalf("unexpected applied entry: %+v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout: server did not apply client's newer lang entry")
	}

	// only one apply expected
	select {
	case e := <-applied:
		t.Fatalf("unexpected second apply: %+v", e)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestSync_ServerSkipsEqualClockEntry(t *testing.T) {
	// client sends entry with same clock as server — server must not re-apply it
	clock := hlc(100, 0, "node")
	serverState := types.Snapshot{
		Entries: map[string]types.SettingEntry{
			"theme": entry("theme", "dark", clock),
		},
	}

	applied := make(chan types.SettingEntry, 1)
	lis := bufconn.Listen(bufSize)
	srv := NewUpdateServer(func() types.Snapshot { return serverState }, func(e types.SettingEntry) {
		applied <- e
	}, testWallTTL)
	grpcSrv := grpc.NewServer()
	userpb.RegisterUpdateServiceServer(grpcSrv, srv)
	go grpcSrv.Serve(lis)
	defer grpcSrv.Stop()

	c := newBufconnClient(t, lis)
	defer c.Close()

	_, err := c.client.Sync(context.Background(), &userpb.SyncRequest{
		LocalState: []*userpb.SettingEntry{
			ToProto(entry("theme", "dark", clock)), // identical clock
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case e := <-applied:
		t.Fatalf("server should not re-apply entry with equal clock, got: %+v", e)
	case <-time.After(100 * time.Millisecond):
		// correct
	}
}

// ── StartRPCServer ────────────────────────────────────────────────────────────

// ── Client ───────────────────────────────────────────────────────────────────

func TestNewClient_LazyDial(t *testing.T) {
	c, err := NewClient("localhost:19999")
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestNewClients_Empty(t *testing.T) {
	clients, err := NewClients(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(clients) != 0 {
		t.Fatalf("expected 0 clients, got %d", len(clients))
	}
}

func TestNewClients_WithAddress(t *testing.T) {
	clients, err := NewClients([]string{"localhost:19999"})
	if err != nil {
		t.Fatalf("NewClients() error = %v", err)
	}
	if len(clients) != 1 {
		t.Fatalf("expected 1 client, got %d", len(clients))
	}
	clients[0].Close()
}

func TestNewClients_InvalidAddress(t *testing.T) {
	_, err := NewClients([]string{"not-valid::address"})
	if err == nil {
		t.Fatal("expected error for invalid address")
	}
}

func TestClient_Sync(t *testing.T) {
	serverState := types.Snapshot{
		Entries: map[string]types.SettingEntry{
			"theme": entry("theme", "dark", hlc(200, 0, "server")),
		},
	}
	_, lis := startTestServerWithLis(t, func() types.Snapshot { return serverState })
	c := newBufconnClient(t, lis)
	defer c.Close()

	var received []types.SettingEntry
	if err := c.Sync(context.Background(), types.Snapshot{}, func(e types.SettingEntry) {
		received = append(received, e)
	}); err != nil {
		t.Fatalf("sync() error = %v", err)
	}
	if len(received) != 1 || received[0].Value != "dark" {
		t.Fatalf("unexpected sync result: %+v", received)
	}
}

func TestClient_Sync_Error(t *testing.T) {
	_, lis := startTestServerWithLis(t, nil)
	c := newBufconnClient(t, lis)
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := c.Sync(ctx, types.Snapshot{}, func(types.SettingEntry) {})
	if err == nil {
		t.Fatal("expected error with cancelled context")
	}
}

func TestClient_RunStream(t *testing.T) {
	_, lis := startTestServerWithLis(t, nil)
	c := newBufconnClient(t, lis)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	c.runStream(ctx, types.Snapshot{}, func(types.SettingEntry) {})
}

func TestClient_RunStream_ReceivesStreamMessage(t *testing.T) {
	srv, lis := startTestServerWithLis(t, nil)
	c := newBufconnClient(t, lis)
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	received := make(chan types.SettingEntry, 1)
	done := make(chan error, 1)
	go func() {
		done <- c.runStream(ctx, types.Snapshot{}, func(e types.SettingEntry) {
			received <- e
		})
	}()

	time.Sleep(50 * time.Millisecond) // let stream establish

	srv.Broadcast(entry("theme", "dark", hlc(100, 0, "A")))

	select {
	case e := <-received:
		if e.Key != "theme" || e.Value != "dark" {
			t.Fatalf("unexpected entry: %+v", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for stream message")
	}

	cancel()
	<-done
}

func TestClient_RunStream_SyncError(t *testing.T) {
	_, lis := startTestServerWithLis(t, nil)
	c := newBufconnClient(t, lis)
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	c.runStream(ctx, types.Snapshot{}, func(types.SettingEntry) {})
}

func TestClient_Subscribe(t *testing.T) {
	_, lis := startTestServerWithLis(t, nil)
	c := newBufconnClient(t, lis)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		c.Subscribe(ctx, func() types.Snapshot { return types.Snapshot{} }, func(types.SettingEntry) {})
		close(done)
	}()

	<-ctx.Done()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Subscribe did not stop after context cancel")
	}
}

// ── Sync TTL filtering ────────────────────────────────────────────────────────

func TestSync_GCEligibleTombstoneNotSentToClient(t *testing.T) {
	ttl := int64(time.Hour)
	oldClock := hlc(time.Now().UnixNano()-int64(2*time.Hour), 0, "server")
	tombstone := types.SettingEntry{Key: "theme", Deleted: true, Clock: oldClock}

	serverState := types.Snapshot{
		Entries: map[string]types.SettingEntry{"theme": tombstone},
	}

	lis := bufconn.Listen(bufSize)
	srv := NewUpdateServer(func() types.Snapshot { return serverState }, nil, ttl)
	grpcSrv := grpc.NewServer()
	userpb.RegisterUpdateServiceServer(grpcSrv, srv)
	go grpcSrv.Serve(lis)
	defer grpcSrv.Stop()

	c := newBufconnClient(t, lis)
	defer c.Close()

	resp, err := c.client.Sync(context.Background(), &userpb.SyncRequest{LocalState: nil})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.NewerEntries) != 0 {
		t.Fatalf("expected GC-eligible tombstone to be filtered, got %d entries", len(resp.NewerEntries))
	}
}

func TestSync_GCEligibleTombstoneNotAppliedFromClient(t *testing.T) {
	ttl := int64(time.Hour)
	oldClock := hlc(time.Now().UnixNano()-int64(2*time.Hour), 0, "client")

	applied := make(chan types.SettingEntry, 1)
	lis := bufconn.Listen(bufSize)
	srv := NewUpdateServer(func() types.Snapshot {
		return types.Snapshot{Entries: map[string]types.SettingEntry{}}
	}, func(e types.SettingEntry) {
		applied <- e
	}, ttl)
	grpcSrv := grpc.NewServer()
	userpb.RegisterUpdateServiceServer(grpcSrv, srv)
	go grpcSrv.Serve(lis)
	defer grpcSrv.Stop()

	c := newBufconnClient(t, lis)
	defer c.Close()

	tombstone := types.SettingEntry{Key: "theme", Deleted: true, Clock: oldClock}
	_, err := c.client.Sync(context.Background(), &userpb.SyncRequest{
		LocalState: []*userpb.SettingEntry{ToProto(tombstone)},
	})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case e := <-applied:
		t.Fatalf("GC-eligible tombstone should not be applied, got: %+v", e)
	case <-time.After(100 * time.Millisecond):
		// correct: filtered
	}
}

// ── StartRPCServer ────────────────────────────────────────────────────────────

func TestStartRPCServer_StartsAndStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	srv := NewUpdateServer(func() types.Snapshot {
		return types.Snapshot{Entries: map[string]types.SettingEntry{}}
	}, nil, testWallTTL)

	if err := StartRPCServer(ctx, srv, 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	cancel()                           // trigger graceful shutdown
	time.Sleep(100 * time.Millisecond) // let it stop
}
