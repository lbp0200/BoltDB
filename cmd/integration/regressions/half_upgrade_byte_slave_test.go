package regressions

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestRegressionHalfUpgradeByteSlave 验证阶段 1（a4 §10 附8——offset 水位改 ts
// 源）的**半升级窗口**：feed 模式主侧 + 字节从侧（ts=0 请求——旧客户端/未开
// --feed-loop）混合部署下字节从侧仍能完整同步——阶段 1「双轨并存」的回滚兼容
// 承诺：
//
//   - PSYNC 字节路径（ts==0 → backlog range/boundary 判定——psync.go 字节分支——
//     阶段 1 保留）；
//   - FULLRESYNC 字节 catch-up（CatchUpAndEnableSlave——backlog 字节直读面
//     GetBacklogCurrentOffset——不受 GetMasterReplOffset 的 ts 语义切换影响）；
//   - 断连重连（CLIENT KILL → 字节 CONTINUE/FULLRESYNC 自愈）。
//
// 判据：数据面（从侧读到 seed 全部键——无丢失）+ 断连重连后仍无丢失。seed 全部
// 写入在 MakeSlave 之前（RDB 快照包含全部——FULLRESYNC 后无新写——主侧
// currentTS 静止——字节从侧 lastAppliedTS（=FULLRESYNC 响应 ts）与主侧水位
// 一致——WaitForReplicaSync 的 feed 分支 ts 判据亦成立）。
func TestRegressionHalfUpgradeByteSlave(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heavy regression in short mode")
	}

	master := StartRegression(t)
	defer master.Close()
	master.EnableFeedLoop() // feed 主侧（阶段 1——ts 水位）

	slave := StartRegression(t)
	defer slave.Close()
	// slave 保持字节模式（不 EnableFeedLoop——旧从侧——ts=0 请求）

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// seed 全部写入（MakeSlave 之前——FULLRESYNC 快照包含全部）
	const n = 20
	for i := 0; i < n; i++ {
		if err := master.Client.Set(ctx, fmt.Sprintf("half:k:%d", i), "v", 0).Err(); err != nil {
			t.Fatalf("half: seed: %v", err)
		}
	}

	if err := slave.MakeSlave(master.Addr); err != nil {
		t.Fatalf("half: MakeSlave: %v", err)
	}

	// 数据面同步：从侧读到全部 seed 键（字节路径 apply 确认）
	sc := redis.NewClient(&redis.Options{Addr: slave.Addr, DialTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second})
	defer sc.Close()
	synced := waitKeysOnSlave(ctx, sc, n, "half:k:%d")
	if !synced {
		t.Fatal("half: byte slave never synced all seed keys (FULLRESYNC + 字节 catch-up 失败)")
	}
	t.Logf("half: byte slave initial sync ok (n=%d) — feed master + byte slave 双轨兼容", n)

	// 断连重连（字节从侧重连路径——CLIENT KILL → 字节 CONTINUE/FULLRESYNC）
	if _, err := master.Client.Do(ctx, "CLIENT", "KILL", "TYPE", "slave").Result(); err != nil {
		t.Fatalf("half: CLIENT KILL: %v", err)
	}
	time.Sleep(3 * time.Second) // 允许重连 + catch-up

	// 断连重连后数据面再验证（无丢失）
	reSynced := waitKeysOnSlave(ctx, sc, n, "half:k:%d")
	if !reSynced {
		t.Fatalf("half: byte slave lost keys after reconnect (mo=%d so=%d)",
			master.GetMasterOffset(), slave.GetSlaveOffset())
	}
	t.Logf("half: byte slave reconnect re-sync ok (no loss) — 半升级窗口守卫 ✓")
}

// waitKeysOnSlave 轮询从侧读全 n 个键（数据面同步判据——不依赖字节/ts offset
// 域假设）。
func waitKeysOnSlave(ctx context.Context, sc *redis.Client, n int, format string) bool {
	for i := 0; i < 30; i++ {
		cnt := 0
		for j := 0; j < n; j++ {
			if v, err := sc.Get(ctx, fmt.Sprintf(format, j)).Result(); err == nil && v == "v" {
				cnt++
			}
		}
		if cnt == n {
			return true
		}
		time.Sleep(500 * time.Millisecond)
	}
	return false
}
