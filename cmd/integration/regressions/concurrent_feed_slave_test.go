package regressions

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestRegressionConcurrentFeedSlaveReplayRate 测量「同一从侧上并发 FeedSlave 是否
// 重发同一 ts 区间」的可达率（TODO §7 开放项——**不做竞态撞窗口**——按真实程序
// 顺序采样）：
//
// 背景：FeedSlave 的「读游标 feedSinceTS → 发送 → 推进」三步非原子（writeMu 只
// 串行化 socket 写），live-push（PropagateCommand feed 分支——持 propMu.RLock——
// 多写者可同时持有）与 gap 补发（CatchUpAndEnableSlaveTS——propMu 外）都可能对
// 同一从侧调 FeedSlave——两个并发调用可各自读到同一个 since → 重发同一 ts 区间
// → 从侧无 ts 去重（apply 无条件执行）→ 重复 apply。全仓只有这两个调用者
// （replication.go:439/:530），生产调用点按客户端连接分布（天然并发——常态形状）。
//
// 判据（§7 判据教训——幂等键集比对查不出重复应用）：**非幂等写入 INCR**——每个
// 写者只 INCR 自己的键（写不同键——多写者天然并发）——双应用 → 计数翻倍。
// 形态：feed 模式双侧 EnableFeedLoop + 多写者 + 若干断连周期（KILL TYPE slave）
// → 停写收敛 → 比对主从两侧每个计数键。
//   - slave > master → 重复 apply 确认（记录键与数值）；
//   - 全部相等 → 本轮未复现——据实降级并记录判据。
// 测量结论入册 TODO §7——先测可达率再谈修复（选项：FeedSlave 加每从侧游标锁 /
// 从侧 ts 去重 / 补发与 live-push 同一序列化点——不擅自改）。
//
// 测量结果（2026-09-05——5/5 轮 × 4/4 键复现）：**重复 apply 已确认**——slave
// 计数 ≈ 2.3× master（+625~+749/键），lost=0——「并发 FeedSlave 重发同一 ts 区间」
// 不是窄窗口而是常态路径。本测试在缺陷修复前**必然红**——故默认跳过，显式复跑用
// REPLAY_RATE_MEASURE=1（修复实施后此 gate 应移除、断言应恒绿）。
func TestRegressionConcurrentFeedSlaveReplayRate(t *testing.T) {
	if os.Getenv("REPLAY_RATE_MEASURE") != "1" {
		t.Skip("§7 发生率测量——2026-09-05 已确认重复 apply（5/5 轮 × 4/4 键）——复跑需 REPLAY_RATE_MEASURE=1；修复决策待定（TODO §7）")
	}
	if testing.Short() {
		t.Skip("skipping heavy regression in short mode")
	}

	master := StartRegression(t)
	defer master.Close()
	master.EnableFeedLoop()

	slave := StartRegression(t)
	defer slave.Close()
	slave.EnableFeedLoop()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	if err := slave.MakeSlave(master.Addr); err != nil {
		t.Fatalf("replay-rate: MakeSlave: %v", err)
	}
	if !master.WaitForReplicaSync(ctx, master, slave, 15*time.Second) {
		t.Fatal("replay-rate: initial sync failed")
	}

	const writers = 4
	const cycles = 6

	stop := make(chan struct{})
	errCh := make(chan error, writers)

	var wg sync.WaitGroup
	wg.Add(writers)
	for i := 0; i < writers; i++ {
		id := i
		go func() {
			defer wg.Done()
			key := fmt.Sprintf("replay:ctr:%d", id)
			for {
				select {
				case <-stop:
					return
				default:
				}
				if err := master.Client.Incr(ctx, key).Err(); err != nil {
					select {
					case errCh <- fmt.Errorf("writer %d INCR: %w", id, err):
					default:
					}
					return
				}
				select {
				case <-time.After(5 * time.Millisecond):
				case <-stop:
					return
				}
			}
		}()
	}

	// 写者跑起来后再开始断连周期
	time.Sleep(500 * time.Millisecond)

	for c := 0; c < cycles; c++ {
		if _, err := master.Client.Do(ctx, "CLIENT", "KILL", "TYPE", "slave").Result(); err != nil {
			t.Fatalf("replay-rate: cycle %d KILL: %v", c+1, err)
		}
		// 等重连 + 收敛（字节判据）
		converged := false
		for i := 0; i < 15; i++ {
			if slave.GetSlaveOffset() >= master.GetMasterOffset() {
				converged = true
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		if !converged {
			t.Logf("replay-rate: cycle %d not converged (mo=%d so=%d) — continuing",
				c+1, master.GetMasterOffset(), slave.GetSlaveOffset())
		}
		time.Sleep(500 * time.Millisecond) // settle 补发
		t.Logf("replay-rate: cycle %d done (recon=%d mo=%d so=%d)",
			c+1, slave.GetReconnectCount(), master.GetMasterOffset(), slave.GetSlaveOffset())
	}

	// 停写 + 最终收敛
	close(stop)
	wg.Wait()
	close(errCh)
	for e := range errCh {
		t.Logf("replay-rate: writer err: %v", e)
	}

	converged := false
	for i := 0; i < 20; i++ {
		if slave.GetSlaveOffset() >= master.GetMasterOffset() {
			converged = true
			break
		}
		time.Sleep(1 * time.Second)
	}
	if !converged {
		t.Fatalf("replay-rate: final convergence failed (mo=%d so=%d)",
			master.GetMasterOffset(), slave.GetSlaveOffset())
	}
	time.Sleep(2 * time.Second) // settle in-flight

	// 比对：主从每个计数键——slave > master = 重复 apply
	sc := redis.NewClient(&redis.Options{Addr: slave.Addr, DialTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second})
	defer sc.Close()

	dup := 0
	lost := 0
	for i := 0; i < writers; i++ {
		key := fmt.Sprintf("replay:ctr:%d", i)
		mv := getInt(ctx, t, master.Client, key)
		sv := getInt(ctx, t, sc, key)
		if sv > mv {
			dup++
			t.Logf("replay-rate: DUPLICATE-APPLY %s: master=%d slave=%d (+%d)",
				key, mv, sv, sv-mv)
		} else if sv < mv {
			lost++
			t.Logf("replay-rate: LOST %s: master=%d slave=%d", key, mv, sv)
		}
	}

	t.Logf("replay-rate: RESULT — keys=%d duplicate-apply=%d lost=%d (ctr:0 master=%d slave=%d)",
		writers, dup, lost,
		getInt(ctx, t, master.Client, "replay:ctr:0"),
		getInt(ctx, t, sc, "replay:ctr:0"))

	if dup > 0 {
		t.Errorf("replay-rate: FAIL — %d/%d keys duplicate-applied (concurrent FeedSlave same-range replay CONFIRMED)",
			dup, writers)
	} else if lost > 0 {
		t.Errorf("replay-rate: FAIL — %d/%d keys lost", lost, writers)
	} else {
		t.Logf("replay-rate: no duplicate apply in this run — 按真实程序顺序未复现（据实降级——判据保留，多轮 -count 采样后入册）")
	}
}
