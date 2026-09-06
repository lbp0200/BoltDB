package replication

import (
	"fmt"
	"net"
	"testing"
)

// TestFullresyncTsDomainInvariant 验证 ts 域 FULLRESYNC 线性化不变式
// "快照点 ts = catch-up 起点 ts+1"（a4 §10 附8 阶段 1 前置验证）：
//
// 时序（当前实现）：
//
//	[1] HandlePSync 锁外读 currentTS → FULLRESYNC 响应第 4 字段 result.TS
//	[2] （新提交可落——锁外读与快照之间无栅栏）
//	[3] CatchUpAndEnableSlave 字节 catch-up 完成后 feed-mode 激活
//	    feedSinceTS = ReplLogCurrentTS()+1（propMu 内读——激活水位 ≥ 快照水位）
//
// 不变式断言（成立性验证）：
//
//	(i)   result.TS ≤ 激活时刻 FeedSinceTS()-1——FULLRESYNC 响应 ts 不越过
//	      catch-up 起点 ts（从侧 lastAppliedTS 初值不会导致重复补发起点）；
//	(ii)  FeedSinceTS() == 激活时刻 ReplLogCurrentTS()+1——feed 严格从
//	      当前水位+1 起（不重叠不遗漏）；
//	(iii) FeedEntriesFrom(FeedSinceTS()) 返回严格 ≥ 该 ts 的增量（log 键
//	      ts 升序）——补发区间与快照覆盖无交集。
func TestFullresyncTsDomainInvariant(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	rm := NewReplicationManager(s)
	rm.SetRole(RoleMaster)
	rm.SetFeedLoop(true)
	defer rm.Stop()

	const n = 10
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("fsync:k:%d", i)
		if err := s.Set(k, "v"); err != nil {
			t.Fatal(err)
		}
		rm.PropagateCommand([][]byte{[]byte("SET"), []byte(k), []byte("v")})
	}

	// [1] FULLRESYNC 响应（psync.go:122 锁外读 currentTS）
	full, err := HandlePSync(rm, "?", 0, 0)
	if err != nil {
		t.Fatalf("HandlePSync: %v", err)
	}
	if !full.FullResync {
		t.Fatal("expected FULLRESYNC for fresh replId")
	}
	if full.TS == 0 {
		t.Fatal("FULLRESYNC ts (currentTS) must be > 0 after writes")
	}

	// [2] 锁外读与快照之间模拟新提交（证明不变式对并发提交仍成立）
	for i := n; i < n+3; i++ {
		k := fmt.Sprintf("fsync:k:%d", i)
		if err := s.Set(k, "v"); err != nil {
			t.Fatal(err)
		}
		rm.PropagateCommand([][]byte{[]byte("SET"), []byte(k), []byte("v")})
	}

	// [3] 字节 catch-up + feed-mode 激活（CatchUpAndEnableSlave——replication.go:523）
	// 阶段 1（a4 §10 附8）：字节路径起点必须取 backlog 字节水位（GetBacklogCurrentOffset）
	// ——GetMasterReplOffset 在 feed 模式下已返回 ts 域值（喂给字节 catch-up 会错域）。
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	_ = client // 测试仅验证激活水位——不读流帧
	sc := NewSlaveConnection(server)
	rm.AddSlave(sc)
	if err := rm.CatchUpAndEnableSlave(sc, rm.GetBacklogCurrentOffset()); err != nil {
		t.Fatalf("CatchUpAndEnableSlave: %v", err)
	}
	defer rm.RemoveSlave(sc.ID)

	if !sc.FeedIsEnabled() {
		t.Fatal("feed-mode activation expected after byte catch-up")
	}

	// (ii) feedSinceTS = 激活时刻 ReplLogCurrentTS()+1（propMu 内读——不重叠不遗漏）
	wantTS, err := rm.store.ReplLogCurrentTS()
	if err != nil {
		t.Fatalf("ReplLogCurrentTS: %v", err)
	}
	if sc.FeedSinceTS() != wantTS+1 {
		t.Fatalf("FeedSinceTS = %d, want currentTS+1 = %d (snapshot-point invariant)", sc.FeedSinceTS(), wantTS+1)
	}

	// (i) FULLRESYNC 响应 ts ≤ catch-up 起点 ts——从侧 lastAppliedTS 初值不越界
	if full.TS > sc.FeedSinceTS()-1 {
		t.Fatalf("FULLRESYNC ts %d > catch-up start %d (response ts must not exceed feed start)", full.TS, sc.FeedSinceTS()-1)
	}

	// (iii) FeedEntriesFrom(feedSinceTS) 严格从该 ts 起（ts 升序——无交集）
	wire, err := rm.FeedEntriesFrom(sc.FeedSinceTS())
	if err != nil {
		t.Fatalf("FeedEntriesFrom: %v", err)
	}
	for _, args := range wire {
		argBytes := make([][]byte, len(args))
		for j, a := range args {
			argBytes[j] = []byte(a)
		}
		ts, _, perr := feedEntryParse(argBytes)
		if perr != nil {
			t.Fatalf("feedEntryParse: %v", perr)
		}
		if ts < sc.FeedSinceTS() {
			t.Fatalf("feed entry ts %d < feedSinceTS %d (overlap with snapshot-covered range)", ts, sc.FeedSinceTS())
		}
	}
}
