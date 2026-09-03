package store

import (
	"math"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

// TestTSSourceMonotonic 验证 ts 源在大量分配下串行唯一、无重复。
func TestTSSourceMonotonic(t *testing.T) {
	t.Parallel()
	src := newTSSource()
	const n = 100000
	seen := make(map[uint64]struct{}, n)
	for i := 0; i < n; i++ {
		v := src.Begin()
		if _, dup := seen[v]; dup {
			t.Fatalf("duplicate ts %d at iteration %d", v, i)
		}
		seen[v] = struct{}{}
		src.End(v)
	}
	if src.SafeDiscard() != n {
		t.Fatalf("ordered watermark did not advance over sequential completes: got %d, want %d",
			src.SafeDiscard(), n)
	}
}

// TestTSSourceOrderedWatermark 验证有序完成水位：并发提交乱序完成时，SafeDiscard
// 只能推进到**连续完成前缀**——不得越过未完成（in-flight）提交的 ts（否则 discard-ts
// 越过 in-flight 提交 → badger `ts >= o.lastCleanupTs` 断言——2026-09-03 全包套件实证）。
func TestTSSourceOrderedWatermark(t *testing.T) {
	t.Parallel()
	src := newTSSource()

	ts1 := src.Begin()
	ts2 := src.Begin()
	ts3 := src.Begin()
	if ts1 != 1 || ts2 != 2 || ts3 != 3 {
		t.Fatalf("unexpected allocation order: %d %d %d", ts1, ts2, ts3)
	}

	// 乱序完成：3 先完成——前缀断裂（1、2 未完成）——水位不得推进。
	src.End(ts3)
	if got := src.SafeDiscard(); got != 0 {
		t.Fatalf("SafeDiscard advanced past in-flight commits: got %d, want 0", got)
	}

	// 1 完成——前缀推进到 1（2 仍 in-flight）。
	src.End(ts1)
	if got := src.SafeDiscard(); got != 1 {
		t.Fatalf("SafeDiscard = %d, want 1", got)
	}

	// 2 完成——前缀推进到 3（1、2、3 全部完成）。
	src.End(ts2)
	if got := src.SafeDiscard(); got != 3 {
		t.Fatalf("SafeDiscard = %d, want 3", got)
	}
}

// TestTSSourceRecoveryFromMaxVersion 验证重启水位恢复：以库内最大已提交版本
// （MaxVersion）+1 为新的起始 ts——managed 写往返 + 重开后 Init 的语义
// （§10 附4——MaxVersion 暴露机制实证；badger 规范模式见 managed_db_test.go：
// NewTransactionAt(MaxUint64, true) + txn.CommitAt(ts, cb)）。
func TestTSSourceRecoveryFromMaxVersion(t *testing.T) {
	dir := t.TempDir()
	opts := badger.DefaultOptions(dir).WithLoggingLevel(badger.ERROR)

	db, err := badger.OpenManaged(opts)
	if err != nil {
		t.Fatal(err)
	}
	last := uint64(7)
	for ts := uint64(1); ts <= last; ts++ {
		ts := ts
		txn := db.NewTransactionAt(math.MaxUint64, true)
		if err := txn.SetEntry(badger.NewEntry([]byte("k"), []byte{byte(ts)})); err != nil {
			t.Fatal(err)
		}
		if err := txn.CommitAt(ts, nil); err != nil {
			t.Fatal(err)
		}
	}
	if got := db.MaxVersion(); got != last {
		t.Fatalf("MaxVersion = %d, want %d", got, last)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// 重开：水位恢复 = MaxVersion()+1
	db2, err := badger.OpenManaged(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	src := newTSSource()
	src.Init(db2)
	if got := src.Begin(); got != last+1 {
		t.Fatalf("recovery watermark: next = %d, want %d", got, last+1)
	}
}
