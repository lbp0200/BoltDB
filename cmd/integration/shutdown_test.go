package integration

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
	"github.com/zeebo/assert"
)

// TestGracefulShutdown 优雅关闭验证:
// 高并发写入 + 阻塞操作 + pubsub → shutdown → no leak
func TestGracefulShutdown(t *testing.T) {
	// Must not t.Parallel: leak check uses process-wide NumGoroutine.
	skipHeavyIntegrationInShort(t)
	db, err := store.NewBotreonStore(t.TempDir())
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, db.CloseWithTimeout(store.CloseTimeout))
	}()

	pubsubMgr := store.NewPubSubManager()
	replMgr := replication.NewReplicationManager(db)

	ctx, cancel := context.WithCancel(context.Background())
	handler := &server.Handler{
		Db:          db,
		PubSub:      pubsubMgr,
		Replication: replMgr,
		Ctx:         ctx,
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)

	serverDone := make(chan struct{})
	go func() {
		handler.ServeTCP(ln)
		close(serverDone)
	}()

	addr := ln.Addr().String()
	time.Sleep(50 * time.Millisecond)
	baseline := runtime.NumGoroutine()

	// Phase 1: 并发写入
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := redis.NewClient(&redis.Options{Addr: addr})
			defer client.Close()
			ctx := context.Background()
			for j := 0; j < 20; j++ {
				client.Set(ctx, "shutdown:write", "v", 0)
				client.Incr(ctx, "shutdown:counter")
			}
		}()
	}

	// Phase 2: 阻塞 BLPOP（原始 RESP）
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				return
			}
			defer conn.Close()
			req := &proto.Array{Args: [][]byte{[]byte("BLPOP"), []byte("shutdown:block"), []byte("0")}}
			proto.WriteRESP(conn, req)
			buf := make([]byte, 256)
			conn.SetReadDeadline(time.Now().Add(10 * time.Second))
			conn.Read(buf)
		}()
	}

	// Phase 3: SUBSCRIBE（原始 RESP）
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				return
			}
			defer conn.Close()
			req := &proto.Array{Args: [][]byte{[]byte("SUBSCRIBE"), []byte("shutdown:ch")}}
			proto.WriteRESP(conn, req)
			reader := bufio.NewReader(conn)
			proto.ReadRESP(reader)
			// 保持订阅，等待 shutdown
			conn.SetReadDeadline(time.Now().Add(10 * time.Second))
			buf := make([]byte, 256)
			conn.Read(buf)
		}()
	}

	time.Sleep(200 * time.Millisecond)

	// 执行关闭
	ln.Close()
	<-serverDone
	cancel()
	replMgr.Stop()
	handler.Shutdown()

	// 等待客户端 goroutine 退出（连接关闭后应立刻返回）
	wg.Wait()

	time.Sleep(500 * time.Millisecond)
	final := runtime.NumGoroutine()
	leak := final - baseline
	if leak > 10 {
		t.Errorf("goroutine leak: %d (baseline=%d, final=%d)", leak, baseline, final)
	}
}

// TestShutdownWithReplication 验证有 slave 连接时的关闭
func TestShutdownWithReplication(t *testing.T) {
	t.Parallel()
	skipHeavyIntegrationInShort(t)
	db, err := store.NewBotreonStore(t.TempDir())
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, db.CloseWithTimeout(store.CloseTimeout))
	}()

	pubsubMgr := store.NewPubSubManager()
	replMgr := replication.NewReplicationManager(db)

	ctx, cancel := context.WithCancel(context.Background())
	handler := &server.Handler{
		Db:          db,
		PubSub:      pubsubMgr,
		Replication: replMgr,
		Ctx:         ctx,
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)

	serverDone := make(chan struct{})
	go func() {
		handler.ServeTCP(ln)
		close(serverDone)
	}()
	time.Sleep(50 * time.Millisecond)

	addr := ln.Addr().String()
	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()

	ctxBg := context.Background()
	for i := 0; i < 100; i++ {
		client.Set(ctxBg, "shutdown:repl", "v", 0)
	}

	// 创建一个真实的 slave TCP 连接
	slaveTCP, err := net.Dial("tcp", addr)
	assert.NoError(t, err)
	slaveConn := replication.NewSlaveConnection(slaveTCP)
	slaveConn.SetReady(true)
	handler.Replication.AddSlave(slaveConn)

	handler.Replication.PropagateCommand([][]byte{[]byte("SET"), []byte("k"), []byte("v")})

	ln.Close()
	<-serverDone
	cancel()
	replMgr.Stop()
	handler.Shutdown()
	slaveTCP.Close()
}
