package regressions

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestRegressionFeedModeTSSemantics 验证 A4 阶段 1（offset 水位改 ts 源——a4 §10
// 附8）的对外语义（feed 模式——--feed-loop）：
//
//   (i)   INFO master_repl_offset == 主侧 currentTS（log 键最大 ts——ts 水位——
//         非字节 offset——语义注记风险①）；
//   (ii)  ROLE offset 字段 == ts（sentinel 兼容展示随水位切换）；
//   (iii) WAIT 1 在从侧同步后返回 ≥1——WAIT 判据走 ts 域（GetReplAckTS >= master
//         ts——S2 ACK-ts applied 语义——非字节比较——handleWAIT feedTS 分支）。
//
// 回滚面：feedLoop 关（字节模式——现状默认）下 INFO master_repl_offset 仍为字节
// 水位——本守卫仅 feed 模式生效。同步判据用**数据面轮询**（从侧读到预置键——
// 真实 feed 增量 apply 确认——不用 framework.WaitForReplicaSync 的字节/ts 错域
// 比较——feed 模式下 master 侧 offset 已 ts 化而 slave 侧仍字节——见 TODO §1
// 剩余项注记）。
func TestRegressionFeedModeTSSemantics(t *testing.T) {
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

	// 预置写入（currentTS > 0）+ 数据面同步标记键
	for i := 0; i < 5; i++ {
		if err := master.Client.Set(ctx, fmt.Sprintf("sem:k:%d", i), "v", 0).Err(); err != nil {
			t.Fatalf("sem: seed: %v", err)
		}
	}
	if err := master.Client.Set(ctx, "sem:sync", "probe", 0).Err(); err != nil {
		t.Fatalf("sem: sync marker: %v", err)
	}

	if err := slave.MakeSlave(master.Addr); err != nil {
		t.Fatalf("sem: MakeSlave: %v", err)
	}

	// 数据面同步：从侧读到 sem:sync（feed 增量 apply 确认）
	sc := redis.NewClient(&redis.Options{Addr: slave.Addr, DialTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second})
	defer sc.Close()
	synced := false
	for i := 0; i < 30; i++ {
		if v, err := sc.Get(ctx, "sem:sync").Result(); err == nil && v == "probe" {
			synced = true
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !synced {
		t.Fatal("sem: slave never saw sem:sync (feed sync failed)")
	}

	curTS, err := master.DB.ReplLogCurrentTS()
	if err != nil {
		t.Fatalf("sem: ReplLogCurrentTS: %v", err)
	}
	if curTS == 0 {
		t.Fatal("sem: currentTS == 0 (expected > 0 after writes)")
	}

	// (i) INFO master_repl_offset == currentTS（阶段 1 ts 语义）
	info := master.Client.Info(ctx, "replication").Val()
	infoOffset := int64(-1)
	for _, line := range strings.Split(info, "\n") {
		if strings.HasPrefix(line, "master_repl_offset:") {
			infoOffset, _ = strconv.ParseInt(strings.TrimPrefix(line, "master_repl_offset:"), 10, 64)
		}
	}
	if infoOffset != int64(curTS) {
		t.Fatalf("sem: INFO master_repl_offset=%d, want currentTS=%d (阶段 1 ts 语义)",
			infoOffset, curTS)
	}
	t.Logf("sem: INFO master_repl_offset=%d == currentTS=%d ✓", infoOffset, curTS)

	// (ii) ROLE offset == ts（sentinel 兼容展示随水位切换）
	roleRes, err := master.Client.Do(ctx, "ROLE").Result()
	if err != nil {
		t.Fatalf("sem: ROLE: %v", err)
	}
	role, ok := roleRes.([]interface{})
	if !ok || len(role) < 2 {
		t.Fatalf("sem: ROLE shape: %v", roleRes)
	}
	roleOffset, err := strconv.ParseInt(fmt.Sprintf("%v", role[1]), 10, 64)
	if err != nil {
		t.Fatalf("sem: ROLE offset parse %v: %v", role[1], err)
	}
	if roleOffset != int64(curTS) {
		t.Fatalf("sem: ROLE offset=%d, want currentTS=%d (阶段 1 ts 语义)", roleOffset, curTS)
	}
	t.Logf("sem: ROLE offset=%d == currentTS=%d ✓", roleOffset, curTS)

	// (iii) WAIT 1 —— 从侧 ack ts 达标（ts 判据——GetReplAckTS >= master ts）
	// go-redis Wait 的 timeout 是 time.Duration（毫秒级传 3*time.Second——
	// 传裸 3000 会被当作 3000ns 立即超时）。
	n, err := master.Client.Wait(ctx, 1, 3*time.Second).Result()
	if err != nil {
		t.Fatalf("sem: WAIT: %v", err)
	}
	if n < 1 {
		t.Fatalf("sem: WAIT 1 returned %d, want >=1 (ts 判据——ack ts 达标)", n)
	}
	t.Logf("sem: WAIT 1 returned %d ✓ (ts 判据)", n)
}
