package store

import (
	"math"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

// TestTSSourceMonotonic 验证 ts 源在大量并发分配下串行唯一、无重复。
func TestTSSourceMonotonic(t *testing.T) {
	t.Parallel()
	src := newTSSource()
	const n = 100000
	seen := make(map[uint64]struct{}, n)
	for i := 0; i < n; i++ {
		v := src.Next()
		if _, dup := seen[v]; dup {
			t.Fatalf("duplicate ts %d at iteration %d", v, i)
		}
		seen[v] = struct{}{}
	}
	if src.Next() != n+1 {
		t.Fatalf("sequence not contiguous: next = %d, want %d", src.Next(), n+1)
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
	if got := src.Next(); got != last+1 {
		t.Fatalf("recovery watermark: next = %d, want %d", got, last+1)
	}
}
