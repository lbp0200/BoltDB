package regressions

import (
	"bufio"
	"context"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/replication"
	"github.com/lbp0200/BoltDB/internal/server"
	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/redis/go-redis/v9"
)

// TestRegressionShutdownRace verifies graceful shutdown under concurrent
// writes, blocking operations, and pubsub — the invariant is zero goroutine
// leak and no DB access after Handler.Shutdown() returns.
//
// Failure doc: docs/failures/shutdown-race.md
// Invariant: goroutine delta ≤ 10, all operations unblock within timeout.
func TestRegressionShutdownRace(t *testing.T) {
	db, err := store.NewBotreonStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	pubsubMgr := store.NewPubSubManager()
	replMgr := replication.NewReplicationManager(db)
	handler := &server.Handler{
		Db:          db,
		PubSub:      pubsubMgr,
		Replication: replMgr,
		Ctx:         ctx,
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	serverDone := make(chan struct{})
	go func() {
		handler.ServeTCP(ln)
		close(serverDone)
	}()

	addr := ln.Addr().String()
	time.Sleep(100 * time.Millisecond)
	baseline := runtime.NumGoroutine()

	var wg sync.WaitGroup

	// Phase 1: concurrent writers
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			client := redis.NewClient(&redis.Options{Addr: addr})
			defer client.Close()
			bg := context.Background()
			for j := 0; j < 50; j++ {
				client.Set(bg, "shutdown:k", "v", 0)
				client.Incr(bg, "shutdown:counter")
				client.LPush(bg, "shutdown:list", j)
			}
		}(i)
	}

	// Phase 2: blocking BLPOP
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, dialErr := net.Dial("tcp", addr)
			if dialErr != nil {
				return
			}
			defer conn.Close()
			req := &proto.Array{Args: [][]byte{[]byte("BLPOP"), []byte("nosuchlist"), []byte("0")}}
			_ = proto.WriteRESP(conn, req)
			conn.SetReadDeadline(time.Now().Add(10 * time.Second))
			buf := make([]byte, 256)
			_, _ = conn.Read(buf)
		}()
	}

	// Phase 3: SUBSCRIBE
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, dialErr := net.Dial("tcp", addr)
			if dialErr != nil {
				return
			}
			defer conn.Close()
			req := &proto.Array{Args: [][]byte{[]byte("SUBSCRIBE"), []byte("shutdown:ch")}}
			_ = proto.WriteRESP(conn, req)
			reader := bufio.NewReader(conn)
			_, _ = proto.ReadRESP(reader)
			conn.SetReadDeadline(time.Now().Add(10 * time.Second))
			buf := make([]byte, 256)
			_, _ = conn.Read(buf)
		}()
	}

	// Phase 4: MONITOR
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, dialErr := net.Dial("tcp", addr)
			if dialErr != nil {
				return
			}
			defer conn.Close()
			req := &proto.Array{Args: [][]byte{[]byte("MONITOR")}}
			_ = proto.WriteRESP(conn, req)
			conn.SetReadDeadline(time.Now().Add(10 * time.Second))
			buf := make([]byte, 256)
			_, _ = conn.Read(buf)
		}()
	}

	time.Sleep(300 * time.Millisecond)

	// Execute shutdown in correct order
	ln.Close()
	<-serverDone
	cancel()
	replMgr.Stop()
	handler.Shutdown()

	wg.Wait()

	time.Sleep(500 * time.Millisecond)
	final := runtime.NumGoroutine()
	leak := final - baseline
	t.Logf("shutdown-race: goroutine delta=%d (baseline=%d, final=%d)", leak, baseline, final)
	if leak > 10 {
		t.Errorf("shutdown-race: goroutine leak: %d (baseline=%d, final=%d)", leak, baseline, final)
	}

	if err := db.CloseWithTimeout(store.CloseTimeout); err != nil {
		t.Errorf("shutdown-race: DB close error: %v", err)
	}
}
