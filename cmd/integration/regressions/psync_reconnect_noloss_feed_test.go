package regressions

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/lbp0200/BoltDB/internal/store"
)

// TestRegressionPsyncReconnectNoLossFeed 验证 feed-mode（S2 backlog 退役双轨——
// master+slave 双侧 EnableFeedLoop——REPLLOG 增量流 + ts 域重连 catch-up）下
// 从侧断连/重连周期后无数据丢失。
//
// 背景：字节模式版本（TestRegressionPsyncReconnectNoLoss）为既有守卫；feed-mode
// 规模验证曾实测 missing=2473/3818（2026-09-04——零对齐后仍丢失）——根因 = 重连
// CatchUpAndEnableSlave 走 backlog 字节 catch-up（按从侧漂移的字节 offset）后跳
// feedSinceTS=curTS+1——字节域与 ts 域 REPLLOG 流坐标不兼容→间隙命令丢失（TODO
// §1c-残留）。修复 = 重连改 ts 域（psync.go ts 域 CONTINUE + handler result.TS>0
// 分流 + CatchUpAndEnableSlaveTS 从 resumeTS+1 起 FeedSlave 补发 gap——2c8ecd0）。
// 本测试为修复后的 feed-mode 复跑验证：missing 应 ≤ 2 容差（期望 0）。
func TestRegressionPsyncReconnectNoLossFeed(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heavy regression in short mode")
	}
	masterDB, err := store.NewBotreonStore(t.TempDir())
	if err != nil {
		t.Fatalf("psync-loss-feed: create store: %v", err)
	}
	master := StartRegressionWithStore(t, masterDB)
	defer master.Close()
	defer masterDB.Close()

	slave := StartRegression(t)
	defer slave.Close()

	// feed-mode 双轨：master+slave 双侧启用（等效 --feed-loop 启动标志）
	master.EnableFeedLoop()
	slave.EnableFeedLoop()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	var writeCounter atomic.Uint64

	t.Log("psync-loss-feed: phase 1 — seed + initial sync")

	// Seed unique keys before slave connects
	seedUniqueTokens(ctx, t, master.Client, 50)

	// Connect slave, initial sync
	if err := slave.MakeSlave(master.Addr); err != nil {
		t.Fatalf("failed to start slave replication: %v", err)
	}
	if !master.WaitForReplicaSync(ctx, master, slave, 5*time.Second) {
		t.Fatal("psync-loss-feed: initial sync failed")
	}
	baseline := runtime.NumGoroutine()
	t.Logf("psync-loss-feed: initial sync ok (go=%d mo=%d so=%d)",
		baseline, master.GetMasterOffset(), slave.GetSlaveOffset())

	// Phase 2: unique writes + partition cycles
	t.Log("psync-loss-feed: phase 2 — writes + partition cycles")

	stop := make(chan struct{})
	errCh := make(chan error, 100)

	var writeWg sync.WaitGroup
	writeWg.Add(3)
	for i := 0; i < 3; i++ {
		id := i
		go writeUniqueToken(ctx, &writeWg, stop, &writeCounter, id, master.Client, errCh)
	}

	// Let writes accumulate before first kill
	time.Sleep(1 * time.Second)

	// 5 partition cycles: kill slave → wait reconnect → CONTINUE/FULLRESYNC
	for i := 0; i < 5; i++ {
		_ = master.Client.Do(ctx, "CLIENT", "KILL", "TYPE", "slave")
		time.Sleep(3 * time.Second) // allow reconnect + sync
		if i%2 == 0 {
			recon := slave.GetReconnectCount()
			mOff := master.GetMasterOffset()
			sOff := slave.GetSlaveOffset()
			t.Logf("psync-loss-feed: cycle %d — recon=%d mo=%d so=%d lag=%d",
				i+1, recon, mOff, sOff, mOff-sOff)
		}
	}

	// Phase 3: stop writes, converge
	t.Log("psync-loss-feed: phase 3 — convergence")
	close(stop)
	writeWg.Wait()

	close(errCh)
	for e := range errCh {
		t.Logf("psync-loss-feed: writer err: %v", e)
	}

	totalTokens := writeCounter.Load()
	_ = totalTokens

	// Wait for slave offset to converge
	converged := false
	mOff := master.GetMasterOffset()
	for i := 0; i < 20; i++ {
		sOff := slave.GetSlaveOffset()
		lag := mOff - sOff
		if lag <= 200 && lag >= -200 {
			t.Logf("psync-loss-feed: converged at iter %d (mo=%d so=%d lag=%d)",
				i, mOff, sOff, lag)
			converged = true
			break
		}
		time.Sleep(1 * time.Second)
	}
	if !converged {
		sOff := slave.GetSlaveOffset()
		if sOff-mOff > 50000 {
			t.Logf("psync-loss-feed: WARN — slave offset ahead by %d (non-critical drift)", sOff-mOff)
		} else {
			t.Fatalf("psync-loss-feed: slave failed to converge: mo=%d so=%d lag=%d",
				mOff, sOff, mOff-sOff)
		}
	}

	time.Sleep(2 * time.Second) // settle in-flight

	t.Logf("psync-loss-feed: convergence ok — tokens=%d recon=%d go=%d",
		writeCounter.Load(), slave.GetReconnectCount(), runtime.NumGoroutine())

	// Phase 4: verify ALL master keys exist on slave
	t.Log("psync-loss-feed: phase 4 — key-set verification")

	mc := redis.NewClient(&redis.Options{Addr: master.Addr, DialTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second})
	defer mc.Close()
	sc := redis.NewClient(&redis.Options{Addr: slave.Addr, DialTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second})
	defer sc.Close()

	verifyUniqueTokenSet(ctx, t, mc, sc, writeCounter.Load())

	// ts 双轨（S2——④）：master 的传播日志键覆盖全部写入（commit 即记日志——日志键
	// 计数 >= 写入数——无丢失的 ts 透镜验证）。
	logEntries, err := masterDB.ReplLogEntries()
	if err != nil {
		t.Fatalf("psync-loss-feed: ReplLogEntries: %v", err)
	}
	if uint64(len(logEntries)) < writeCounter.Load() {
		t.Errorf("psync-loss-feed: repl log entries %d < writes %d (ts lens)", len(logEntries), writeCounter.Load())
	}

	// Goroutine leak check
	delta := runtime.NumGoroutine() - baseline
	if delta > 30 {
		t.Errorf("psync-loss-feed: goroutine delta = %d (possible leak)", delta)
	} else {
		t.Logf("psync-loss-feed: goroutine delta = %d (ok)", delta)
	}
}
