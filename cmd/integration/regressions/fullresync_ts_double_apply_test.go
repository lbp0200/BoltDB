package regressions

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/lbp0200/BoltDB/internal/replication"
)

// TestRegressionFullresyncTsDoubleApplyGuard 是 FULLRESYNC 线性化点 ts 落点的
// **区分守卫**（TODO §6 ②——5a3fb51 修复的回归守护）：
//
// 被守护缺陷（pre-fix）：FULLRESYNC 响应第 4 字段取 HandlePSync 锁外读的
// currentTS（psync.go:122）——锁外读到快照实际水位（SnapshotMuLock）之间若有
// 新提交，通告 ts 早于快照水位。从侧把该字段直接存为 lastAppliedTS
// （reconnect.go:361）——既是重连续播点。若从侧 FULLRESYNC 后尚未 apply 任何
// feed 增量就断连重连，主侧按过旧 ts 续播 → 重发 (staleTs, snapshotTs] → 该区间
// 命令已在 RDB 内又被执行一遍（从侧**无 ts 去重**——apply 路径无条件执行）→
// 非幂等命令双应用（数据发散）。暴露面：仅 feed 模式。
//
// 注入形态（TODO §6 ② 建议）：replication.HandlePSyncAfterTSRead 钩子在
// HandlePSync 锁外读 currentTS 之后触发——窗口内主节点提交 K 条 INCR；从侧
// FULLRESYNC 后立刻断连重连（不经任何额外 apply）——最终从侧计数应 == K
// （双应用则 == 2K）。pre-fix 上本守卫必须红；post-fix 上绿。
//
// 判据：非幂等写入（INCR）+ 主从计数比对——幂等键集比对（§7 判据教训）查不出
// 重复应用，INCR 计数翻倍是唯一可靠形态。
func TestRegressionFullresyncTsDoubleApplyGuard(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heavy regression in short mode")
	}

	master := StartRegression(t)
	defer master.Close()
	master.EnableFeedLoop()

	slave := StartRegression(t)
	defer slave.Close()
	slave.EnableFeedLoop()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// 预置写入：让 currentTS 起步非零（FULLRESYNC 响应 ts > 0）
	for i := 0; i < 10; i++ {
		if err := master.Client.Set(ctx, fmt.Sprintf("guard:seed:%d", i), "v", 0).Err(); err != nil {
			t.Fatalf("guard: seed SET: %v", err)
		}
	}

	// 窗口注入：锁外读 ts 之后、快照之前提交 K 条 INCR（hook 同步等待响应——
	// INCR 全部提交后才返回——快照必然包含它们——确定性窗口命中）。
	const K = 5
	var hookOnce sync.Once
	replication.HandlePSyncAfterTSRead = func() {
		hookOnce.Do(func() {
			for i := 0; i < K; i++ {
				if err := master.Client.Incr(ctx, "guard:ctr").Err(); err != nil {
					t.Errorf("guard: window INCR %d: %v", i, err)
					return
				}
			}
		})
	}
	defer func() { replication.HandlePSyncAfterTSRead = nil }()

	// 初始同步：FULLRESYNC（hook 触发——K 条 INCR 提交并入 RDB）
	if err := slave.MakeSlave(master.Addr); err != nil {
		t.Fatalf("guard: MakeSlave: %v", err)
	}
	if !master.WaitForReplicaSync(ctx, master, slave, 15*time.Second) {
		t.Fatal("guard: initial FULLRESYNC sync failed")
	}
	mBefore := getInt(ctx, t, master.Client, "guard:ctr")
	t.Logf("guard: initial sync ok — master ctr=%d (want %d)", mBefore, K)

	// 立刻断连（不经任何额外 apply——hook 后主侧无更多写——feed 流为空）→ 重连
	// pre-fix：从侧以过旧 ts 续播 → 重发 (staleTs, snapshotTs] → INCR 双应用。
	if _, err := master.Client.Do(ctx, "CLIENT", "KILL", "TYPE", "slave").Result(); err != nil {
		t.Fatalf("guard: CLIENT KILL: %v", err)
	}

	// 等重连 + 收敛（字节判据——双轨下主从 offset 追平）
	converged := false
	for i := 0; i < 20; i++ {
		if slave.GetSlaveOffset() >= master.GetMasterOffset() {
			converged = true
			break
		}
		time.Sleep(1 * time.Second)
	}
	if !converged {
		t.Fatalf("guard: slave failed to re-converge after kill (mo=%d so=%d)",
			master.GetMasterOffset(), slave.GetSlaveOffset())
	}
	time.Sleep(2 * time.Second) // settle 重连后的补发 apply

	// 最终断言：从侧计数 == K（pre-fix 双应用 → 2K——守卫必须红）
	got := getInt(ctx, t, slave.Client, "guard:ctr")
	mGot := getInt(ctx, t, master.Client, "guard:ctr")
	if got != K {
		t.Fatalf("guard: FAIL — slave ctr=%d, want %d (double-apply => %d; master=%d)",
			got, K, 2*K, mGot)
	}
	t.Logf("guard: OK — slave ctr=%d == K=%d (no double apply; master=%d)", got, K, mGot)
}

// getInt 读取整数值（守卫/测量共用——非幂等判据的读取面）。
func getInt(ctx context.Context, t *testing.T, c *redis.Client, key string) int64 {
	t.Helper()
	v, err := c.Get(ctx, key).Int64()
	if err != nil {
		t.Fatalf("GET %s: %v", key, err)
	}
	return v
}
