package replication

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/lbp0200/BoltDB/internal/store"
)

// TestRDBSnapshot_NoTearUnderConcurrentWrites 度量 GenerateRDB 产出的 RDB 是否可能
// 读到「半条命令」（撕裂快照）——TODO §6 lost 候选 ④ 的确定性实验。
//
// 机制前提（代码级证明，2026-09-05）：store 以 badger managed 模式打开
// （define.go:499 OpenManaged），而 managed 模式下 db.View() 走
// NewTransactionAt(MaxUint64, false)（badger v4.9.6 txn.go:786-794）——即 RDB 的读取
// **没有 MVCC 快照隔离**，每个键读到的是「迭代器到达该键那一刻」的最新版本。
// GenerateRDB 的一致性因此完全依赖 snapshotMu 写锁（跨 snapshotTS 捕获 → 整个迭代）。
//
// 判据（非幂等 + 跨键不变式，遵 §6「通用判据教训」——不用键集合）：写者反复执行
// MSET torn:a=<n> torn:b=<n>（单条逻辑命令 = 单个 badger 事务）。任何一份自洽快照里
// a 与 b 必然同时存在且相等，故
//   - 一侧缺失（另一侧在）→ 半条命令可见 → 撕裂
//   - 两侧值不等       → 两个时点的键混入同一快照 → 撕裂
//
// 双臂设计（让"绿"也可解释）：
//
//	fenced   —— 写者持 SnapshotMuRLock 跨 commit（与生产 processRequest
//	  handler_core.go:738 同构）→ 度量"有栅栏时是否仍撕裂"；
//	unfenced —— 写者不取栅栏（对照组）→ 证明本探针**对撕裂敏感**。
//	  对照组 0 撕裂 = 探针不敏感 = 本轮结论不成立（据实报 inconclusive，不误判为安全）。
//
// 断言：fenced 臂出现撕裂 → t.Errorf（真缺陷——FULLRESYNC 会交付半条命令）。
// 对照臂无撕裂 → 仅告警，本轮不构成对候选 ④ 的排除。
func TestRDBSnapshot_NoTearUnderConcurrentWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping tearing probe in short mode（自旋写者 + 120 次快照，属定向/nightly）")
	}
	const samples = 120

	fenced := runTearProbe(t, "fenced", true, samples)
	unfenced := runTearProbe(t, "unfenced", false, samples)

	t.Logf("tear probe — fenced=%d/%d  unfenced(control)=%d/%d",
		fenced, samples, unfenced, samples)

	if fenced > 0 {
		t.Errorf("%d/%d FULLRESYNC RDB 样本出现撕裂——snapshotMu 栅栏未能阻止半条命令进入快照"+
			"（对照组撕裂 %d/%d）——TODO §6 候选 ④ 成立", fenced, samples, unfenced, samples)
	}
	if unfenced == 0 {
		t.Logf("INCONCLUSIVE: 对照组（无栅栏）未复现撕裂——本探针敏感度未自证，"+
			"上方 fenced=0 不能用于排除候选 ④（%d 样本）", samples)
	}
}

// runTearProbe 以指定栅栏模式跑 samples 次「并发写 + 快照 + 载入 + 跨键比对」，返回撕裂样本数。
func runTearProbe(t *testing.T, name string, useFence bool, samples int) int {
	t.Helper()

	s := setupTestStore(t)
	defer s.Close()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var writes atomic.Uint64
	var writeErr atomic.Value

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			n := strconv.FormatUint(writes.Add(1), 10)
			if useFence {
				// 与生产 processRequest 同构：栅栏跨越 commit（→ PropagateCommand）。
				s.SnapshotMuRLock()
				err := s.MSet("torn:a", n, "torn:b", n)
				s.SnapshotMuRUnlock()
				if err != nil {
					writeErr.Store(err.Error())
					return
				}
				continue
			}
			if err := s.MSet("torn:a", n, "torn:b", n); err != nil {
				writeErr.Store(err.Error())
				return
			}
		}
	}()

	torn := 0
	for i := 0; i < samples; i++ {
		// 与 handler 的 FULLRESYNC 分支同构：写锁跨整个 RDB 迭代。
		s.SnapshotMuLock()
		rdb, err := GenerateRDBWithSnapshotLock(s)
		s.SnapshotMuUnlock()
		if err != nil {
			t.Fatalf("%s: GenerateRDBWithSnapshotLock: %v", name, err)
		}
		if got := inspectTornPair(t, name, rdb); got {
			torn++
		}
	}

	close(stop)
	wg.Wait()
	if e, ok := writeErr.Load().(string); ok && e != "" {
		t.Fatalf("%s: writer MSet failed: %s", name, e)
	}
	t.Logf("%s: writes=%d torn=%d/%d", name, writes.Load(), torn, samples)
	return torn
}

// inspectTornPair 把 RDB 载入全新 store 并检查 a/b 跨键不变式（一侧缺失或值不等 = 撕裂）。
func inspectTornPair(t *testing.T, name string, rdb []byte) bool {
	t.Helper()

	fresh, err := store.NewBotreonStore(t.TempDir())
	if err != nil {
		t.Fatalf("%s: fresh store: %v", name, err)
	}
	defer fresh.Close()

	if err := LoadRDBWithStore(rdb, fresh); err != nil {
		t.Fatalf("%s: LoadRDBWithStore: %v", name, err)
	}

	a, ea := fresh.Get("torn:a")
	b, eb := fresh.Get("torn:b")
	switch {
	case ea != nil && eb != nil:
		return false // 两侧皆缺失：快照早于首次写——不是撕裂
	case ea != nil || eb != nil:
		t.Logf("%s: TORN(partial) a_err=%v b_err=%v", name, ea, eb)
		return true
	}
	if a != b {
		t.Logf("%s: TORN(value-mismatch) a=%q b=%q", name, a, b)
		return true
	}
	return false
}
