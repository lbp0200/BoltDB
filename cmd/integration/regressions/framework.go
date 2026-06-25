// Package regressions provides replayable regression tests for documented failure modes.
//
// Each regression test:
//   - Creates an isolated BoltDB server (t.TempDir for Badger)
//   - Applies a specific pressure profile that previously caused a known failure
//   - Collects metrics via monitor.PressureMonitor
//   - Asserts system invariants hold after the scenario
//
// Usage:
//
//	go test -race -timeout 120s ./cmd/integration/regressions/ -run TestRegression
package regressions

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/backup"
	"github.com/lbp0200/BoltDB/internal/monitor"
	"github.com/lbp0200/BoltDB/internal/replication"
	"github.com/lbp0200/BoltDB/internal/server"
	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/redis/go-redis/v9"
)

// RegressionServer 持有一次回归测试的完整服务器环境
type RegressionServer struct {
	DB      *store.BotreonStore
	Handler *server.Handler
	Client  *redis.Client
	Addr    string
	T       *testing.T
	Cleanup func()

	replMgr *replication.ReplicationManager
	ownDB   bool // cleanup 时是否关闭 DB
}

// StartRegression 启动一个独立的 BoltDB 服务器用于回归测试
func StartRegression(t *testing.T) *RegressionServer {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping regression test in short mode (see scripts/test-tier-b.sh)")
	}

	dbPath := t.TempDir()
	db, err := store.NewBotreonStore(dbPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	if err := db.NextStartup(); err != nil {
		_ = db.Close()
		t.Fatalf("failed nextStartup: %v", err)
	}

	srv := startRegressionWithDB(t, db, dbPath+"/backup")
	srv.ownDB = true
	// 替换 cleanup 使其关闭 DB
	origCleanup := srv.Cleanup
	srv.Cleanup = func() {
		origCleanup()
		_ = db.Close()
	}
	return srv
}

// StartRegressionWithStore 在已有 store 上启动服务器（用于 RDB load 后验证）
func StartRegressionWithStore(t *testing.T, db *store.BotreonStore) *RegressionServer {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping regression test in short mode (see scripts/test-tier-b.sh)")
	}
	return startRegressionWithDB(t, db, t.TempDir()+"/backup")
}

func startRegressionWithDB(t *testing.T, db *store.BotreonStore, backupDir string) *RegressionServer {
	t.Helper()

	replMgr := replication.NewReplicationManager(db)
	backupMgr := backup.NewBackupManager(db, backupDir)
	pubsubMgr := store.NewPubSubManager()

	h := &server.Handler{
		Db:          db,
		Replication: replMgr,
		Backup:      backupMgr,
		PubSub:      pubsubMgr,
		Ctx:         context.Background(),
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = db.Close()
		t.Fatalf("failed to listen: %v", err)
	}

	go func() {
		_ = h.ServeTCP(listener)
	}()

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

	cleanup := func() {
		replMgr.Stop()
		_ = listener.Close()
		// Shutdown 等待所有 handler goroutine 退出（wg.Wait），
		// 确保没有 handleConnection/handleSlaveReplicationConnection
		// 在 db.Close() 之后还访问存储。
		h.Shutdown()
		backupMgr.Wait()
		_ = client.Close()
		pubsubMgr.Clear()
		// 注意：不关闭 db——由调用方负责
	}

	return &RegressionServer{
		DB:      db,
		Handler: h,
		Client:  client,
		Addr:    addr,
		T:       t,
		Cleanup: cleanup,
		replMgr: replMgr,
	}
}

// NewMonitor 创建指定间隔的 PressureMonitor
func (s *RegressionServer) NewMonitor(interval time.Duration) *monitor.PressureMonitor {
	return monitor.NewPressureMonitor(s.DB, s.replMgr)
}

// WaitForStableL0 等待 L0 score 降到阈值以下，最多等待 timeout
func (s *RegressionServer) WaitForStableL0(ctx context.Context, pm *monitor.PressureMonitor, threshold float64, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return false
		default:
		}
		latest := pm.Latest()
		if latest.LastL0Score <= threshold {
			return true
		}
		time.Sleep(500 * time.Millisecond)
	}
	return false
}

// RunLoad 运行指定数量的写入 goroutine，持续 duration 时间
func (s *RegressionServer) RunLoad(ctx context.Context, writers int, duration time.Duration, errCh chan<- error) {
	loadCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		id := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(id)))
			for loadCtx.Err() == nil {
				key := fmt.Sprintf("regress:key:%d", rng.Intn(500))
				val := fmt.Sprintf("v:%d", rng.Intn(100000))
				if err := s.Client.Set(loadCtx, key, val, 0).Err(); err != nil {
					select {
					case errCh <- fmt.Errorf("writer %d SET %s: %w", id, key, err):
					default:
					}
					return
				}
				time.Sleep(time.Duration(rng.Intn(5)) * time.Millisecond)
			}
		}()
	}
	wg.Wait()
}

// MakeSlave 将此服务器配置为指定 master 的从节点
func (s *RegressionServer) MakeSlave(masterAddr string) error {
	return replication.StartSlaveReplication(s.replMgr, s.DB, masterAddr)
}

// StopSlave 停止从复制（恢复为 master）
func (s *RegressionServer) StopSlave() {
	replication.StopSlaveReplication(s.replMgr)
}

// WaitForReplicaSync 等待 slave 的 offset 追上 master，最多等待 timeout
func (s *RegressionServer) WaitForReplicaSync(ctx context.Context, master, slave *RegressionServer, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return false
		default:
		}
		masterOff := master.replMgr.GetMasterReplOffset()
		slaveOff := slave.replMgr.GetSlaveReplOffset()
		if slaveOff >= masterOff {
			return true
		}
		time.Sleep(500 * time.Millisecond)
	}
	return false
}

// GetReconnectCount 返回重连计数
func (s *RegressionServer) GetReconnectCount() int64 {
	return s.replMgr.GetReconnectCount()
}

// GetMasterOffset 返回 master 复制偏移量
func (s *RegressionServer) GetMasterOffset() int64 {
	return s.replMgr.GetMasterReplOffset()
}

// GetSlaveOffset 返回 slave 复制偏移量
func (s *RegressionServer) GetSlaveOffset() int64 {
	return s.replMgr.GetSlaveReplOffset()
}

// Close closes all resources
func (s *RegressionServer) Close() {
	if s.Cleanup != nil {
		s.Cleanup()
	}
}
