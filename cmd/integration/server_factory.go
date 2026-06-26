package integration

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/server"
	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/redis/go-redis/v9"
)

// testingTB is an alias for testing.TB
type testingTB = testing.TB

// IsolatedServer wraps a single BoltDB server for integration testing.
// Each server has its own DB, listener, and Redis client — no shared state.
type IsolatedServer struct {
	Addr    string
	Client  *redis.Client
	DB      *store.BotreonStore
	Handler *server.Handler

	listener net.Listener
	cleanup  func()
}

// StartIsolatedServer creates a fresh BoltDB server at a random port on t.TempDir().
// The server is cleaned up via t.Cleanup when the test finishes.
func StartIsolatedServer(t testingTB) *IsolatedServer {
	t.Helper()

	db, err := store.NewBotreonStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewBotreonStore: %v", err)
	}

	h := &server.Handler{
		Db:     db,
		Ctx:    context.Background(),
		PubSub: store.NewPubSubManager(),
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = db.Close()
		t.Fatalf("listen: %v", err)
	}

	go func() { _ = h.ServeTCP(listener) }()

	addr := listener.Addr().String()
	client := redis.NewClient(&redis.Options{
		Addr:        addr,
		DialTimeout: 5 * time.Second,
	})

	pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = listener.Close()
		_ = client.Close()
		_ = db.Close()
		t.Fatalf("server not ready: %v", err)
	}

	srv := &IsolatedServer{
		Addr:     addr,
		Client:   client,
		DB:       db,
		Handler:  h,
		listener: listener,
	}

	srv.cleanup = func() {
		_ = listener.Close()
		h.Shutdown()
		_ = client.Close()
		_ = db.Close()
	}

	t.Cleanup(srv.cleanup)
	return srv
}
