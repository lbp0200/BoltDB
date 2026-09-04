package replication

import (
	"fmt"
	"sync"
	"testing"
)

// TestConversionTableDualTrack 验证换算表（a4 §10 附7——过渡期验证锚）：
// backlog 字节域 ↔ 日志键 ts 域的双向映射——事件对齐构建后：
// (i) Count == 写入数（backlog 条目数 == 日志键数——双轨一致）；
// (ii) 偏移严格递增 + ts 严格递增；
// (iii) 双向换算往返一致（OffsetToTS(TSToOffset(ts)) == ts 起始事件——
//      TSToOffset 取 >= ts 的首条目偏移，OffsetToTS 落回该条目）；
// (iv) AlignCheck == true（建表后无新增写——双轨未分叉）。
func TestConversionTableDualTrack(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	rm := NewReplicationManager(s)
	rm.SetRole(RoleMaster)
	defer rm.Stop()

	const n = 20
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("conv:k:%d", i)
		if err := s.Set(k, "v"); err != nil {
			t.Fatal(err)
		}
		rm.PropagateCommand([][]byte{[]byte("SET"), []byte(k), []byte("v")})
	}

	tbl, err := BuildReplConversionTable(rm.GetBacklog(), s)
	if err != nil {
		t.Fatalf("build conversion table: %v", err)
	}
	if tbl.Count() != n {
		t.Fatalf("table count = %d, want %d", tbl.Count(), n)
	}

	// 偏移/ts 严格递增 + 起始偏移为 0
	if tbl.OffsetAt(0) != 0 {
		t.Fatalf("first offset = %d, want 0", tbl.OffsetAt(0))
	}
	for i := 1; i < n; i++ {
		if tbl.OffsetAt(i) <= tbl.OffsetAt(i-1) {
			t.Fatalf("offsets not strictly increasing at %d: %d <= %d", i, tbl.OffsetAt(i), tbl.OffsetAt(i-1))
		}
		if tbl.TSAt(i) <= tbl.TSAt(i-1) {
			t.Fatalf("tss not strictly increasing at %d: %d <= %d", i, tbl.TSAt(i), tbl.TSAt(i-1))
		}
	}

	// 双向换算往返：事件 i 起始偏移 → OffsetToTS == 事件 i ts；ts → TSToOffset == 事件 i 偏移
	for i := 0; i < n; i++ {
		off := tbl.OffsetAt(i)
		ts := tbl.TSAt(i)
		if got := tbl.OffsetToTS(off); got != ts {
			t.Fatalf("event %d: OffsetToTS(%d) = %d, want ts %d", i, off, got, ts)
		}
		if got := tbl.TSToOffset(ts); got != off {
			t.Fatalf("event %d: TSToOffset(%d) = %d, want offset %d", i, ts, got, off)
		}
	}

	// 对齐核验：建表后无新增写——双轨一致
	if cnt, ok := tbl.AlignCheck(s); !ok || cnt != n {
		t.Fatalf("AlignCheck = (%d, %v), want (%d, true)", cnt, ok, n)
	}

	// 水位记录 = backlog 当前水位
	if tbl.CurrentOffset() != rm.GetBacklog().GetCurrentOffset() {
		t.Fatalf("CurrentOffset = %d, want %d", tbl.CurrentOffset(), rm.GetBacklog().GetCurrentOffset())
	}
}

// TestConversionTableEmpty 验证空表边界：无写入时 Count==0，换算回退安全
// （OffsetToTS/TSToOffset 不 panic 返回 0；AlignCheck 表空返回 false）。
func TestConversionTableEmpty(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	rm := NewReplicationManager(s)
	rm.SetRole(RoleMaster)
	defer rm.Stop()

	tbl, err := BuildReplConversionTable(rm.GetBacklog(), s)
	if err != nil {
		t.Fatalf("build empty conversion table: %v", err)
	}
	if tbl.Count() != 0 {
		t.Fatalf("empty table count = %d, want 0", tbl.Count())
	}
	if tbl.OffsetToTS(0) != 0 {
		t.Fatalf("empty OffsetToTS = %d, want 0", tbl.OffsetToTS(0))
	}
	if tbl.TSToOffset(0) != 0 {
		t.Fatalf("empty TSToOffset = %d, want 0", tbl.TSToOffset(0))
	}
	if _, ok := tbl.AlignCheck(s); !ok {
		t.Fatal("empty table AlignCheck should be consistent (0 events == 0 log keys)")
	}
}

// TestConversionTableDetectsDivergence 验证锚的分叉检测语义（missing=2499 根因
// 家族——并发 commit/append 序错位）：先按序 s.Set（log 键 ts 升序），再以**反转
// 顺序** PropagateCommand（backlog 事件序与 log 键序分叉）——BuildReplConversionTable
// 必须返回对齐错误（锚现形分叉——绝不静默产出错误映射）。
func TestConversionTableDetectsDivergence(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	rm := NewReplicationManager(s)
	rm.SetRole(RoleMaster)
	defer rm.Stop()

	const n = 10
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = fmt.Sprintf("div:k:%d", i)
		if err := s.Set(keys[i], "v"); err != nil {
			t.Fatal(err)
		}
	}
	// backlog 事件序反转（与 log 键序分叉——锚应检测）
	for i := n - 1; i >= 0; i-- {
		rm.PropagateCommand([][]byte{[]byte("SET"), []byte(keys[i]), []byte("v")})
	}

	tbl, err := BuildReplConversionTable(rm.GetBacklog(), s)
	if err == nil {
		// 双轨竟然一致？count 相等但事件序错位——若建表成功则换算必错——兜底断言
		if tbl != nil {
			t.Fatalf("diverged dual-track built without error (count=%d) — anchor failed to detect", tbl.Count())
		}
	}
	// 期望：对齐错误（事件序分叉现形）
	if err == nil {
		t.Fatal("expected alignment error for diverged dual-track, got nil")
	}
	t.Logf("divergence detected as expected: %v", err)
}

// TestConversionTableConcurrentWriters 验证锚在并发写者下的鲁棒性（复刻
// missing=2499 场景结构——8 writers × 25 并发 SET + PropagateCommand）：
// 锚要么建出完全一致的换算表（对齐 + AlignCheck + 抽样往返全通过），要么返回
// 对齐错误（并发 harness 下 commit 序 vs append 序天然分叉——锚检测而非静默
// 产出错误映射）——绝不 panic、绝不静默错表。
func TestConversionTableConcurrentWriters(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	rm := NewReplicationManager(s)
	rm.SetRole(RoleMaster)
	defer rm.Stop()

	const writers = 8
	const perWriter = 25
	var wg sync.WaitGroup
	wg.Add(writers)
	for w := 0; w < writers; w++ {
		go func(writerID int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				k := fmt.Sprintf("conc:w%d:k%d", writerID, i)
				if err := s.Set(k, "v"); err != nil {
					t.Errorf("concurrent SET %s: %v", k, err)
					return
				}
				rm.PropagateCommand([][]byte{[]byte("SET"), []byte(k), []byte("v")})
			}
		}(w)
	}
	wg.Wait()

	tbl, err := BuildReplConversionTable(rm.GetBacklog(), s)
	if err != nil {
		// 允许对齐错误（并发 harness 下分叉是已知现象——锚检测语义）
		t.Logf("concurrent dual-track diverged — anchor detected: %v", err)
		return
	}
	if tbl == nil {
		t.Fatal("nil table with nil error")
	}
	wantCount := writers * perWriter
	if tbl.Count() != wantCount {
		t.Fatalf("concurrent table count = %d, want %d", tbl.Count(), wantCount)
	}
	// 建表一致：AlignCheck + 抽样往返核验
	if cnt, ok := tbl.AlignCheck(s); !ok || cnt != wantCount {
		t.Fatalf("concurrent AlignCheck = (%d, %v), want (%d, true)", cnt, ok, wantCount)
	}
	for i := 0; i < tbl.Count(); i += 7 { // 抽样往返
		off := tbl.OffsetAt(i)
		ts := tbl.TSAt(i)
		if got := tbl.OffsetToTS(off); got != ts {
			t.Fatalf("concurrent event %d: OffsetToTS(%d) = %d, want %d", i, off, got, ts)
		}
	}
}
