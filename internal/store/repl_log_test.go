package store

import (
	"encoding/binary"
	"fmt"
	"sync"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

// logTS 从日志键中解出 ts（键尾 8 字节大端）。
func logTS(key []byte) uint64 {
	return binary.BigEndian.Uint64(key[len(key)-8:])
}

// replLogEntries 遍历全部传播日志键（前缀扫描）返回 (ts, value) 列表。
func replLogEntries(t *testing.T, s *BotreonStore) [][2]uint64 {
	t.Helper()
	var out [][2]uint64
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(replLogPrefix); it.Valid() && hasPrefix(it.Item().Key(), replLogPrefix); it.Next() {
			v, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			out = append(out, [2]uint64{logTS(it.Item().Key()), uint64(len(v))})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func hasPrefix(b, prefix []byte) bool {
	if len(b) < len(prefix) {
		return false
	}
	for i := range prefix {
		if b[i] != prefix[i] {
			return false
		}
	}
	return true
}

// TestReplLogSameTSBinding 验证 log-in-commit 的核心绑定：SET 的数据条目与传播
// 日志键在同一事务（同一 commit ts）——日志键嵌入 ts == 数据条目 item.Version()。
func TestReplLogSameTSBinding(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	if err := s.Set("binding:key", "value-1"); err != nil {
		t.Fatal(err)
	}

	err := s.db.View(func(txn *badger.Txn) error {
		// 数据条目：STRING 数据键的版本（commit ts）
		item, err := txn.Get([]byte(s.stringKey("binding:key")))
		if err != nil {
			return err
		}
		dataTS := item.Version()

		// 日志条目：REPLLOG_ + ts
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		var logFound bool
		for it.Seek(replLogPrefix); it.Valid() && hasPrefix(it.Item().Key(), replLogPrefix); it.Next() {
			logFound = true
			if lt := logTS(it.Item().Key()); lt != dataTS {
				t.Fatalf("log ts %d != data commit ts %d (binding broken)", lt, dataTS)
			}
			v, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			if string(v) != string(encodePropagateCommand([]byte("SET"), []byte("binding:key"))) {
				t.Fatalf("log value mismatch: %q", v)
			}
		}
		if !logFound {
			t.Fatal("no repl log entry after successful SET")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestReplLogOrdering 验证日志键按 ts 升序（前缀扫描即排水序）。
func TestReplLogOrdering(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	const n = 20
	for i := 0; i < n; i++ {
		if err := s.Set(fmt.Sprintf("order:key:%d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}
	entries := replLogEntries(t, s)
	if len(entries) != n {
		t.Fatalf("log entries = %d, want %d", len(entries), n)
	}
	for i := 1; i < len(entries); i++ {
		if entries[i][0] <= entries[i-1][0] {
			t.Fatalf("log ts not ascending at %d: %d <= %d", i, entries[i][0], entries[i-1][0])
		}
	}
}

// TestReplLogSuccessOnly 验证失败写不入日志：对已存在不同类型键 SET → ErrWrongType
// → 无新日志条目；成功后才有。
func TestReplLogSuccessOnly(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	// 先建 LIST 类型键（冲突类型）
	if _, err := s.LPush("conflict:key", "a"); err != nil {
		t.Fatal(err)
	}
	before := len(replLogEntries(t, s))

	// SET 到 LIST 键 → ErrWrongType（fn 失败——不得写日志）
	if err := s.Set("conflict:key", "v"); err == nil {
		t.Fatal("expected ErrWrongType on SET over LIST key")
	}
	if got := len(replLogEntries(t, s)); got != before {
		t.Fatalf("failed SET wrote a repl log entry: before %d after %d", before, got)
	}

	// 成功 SET → 日志 +1
	if err := s.Set("ok:key", "v"); err != nil {
		t.Fatal(err)
	}
	if got := len(replLogEntries(t, s)); got != before+1 {
		t.Fatalf("successful SET did not write exactly one log entry: before %d after %d", before, got)
	}
}

// TestReplLogCurrentTS 验证当前日志键水位（最大 ts——主侧 currentTS——GETACK 回复
// 的 ts 携带源——S2 feed 协议相位 ③）。
func TestReplLogCurrentTS(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	const n = 15
	for i := 0; i < n; i++ {
		if err := s.Set(fmt.Sprintf("cts:key:%d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}
	entries := replLogEntries(t, s)
	if len(entries) != n {
		t.Fatalf("log entries = %d, want %d", len(entries), n)
	}
	want := entries[len(entries)-1][0]
	got, err := s.ReplLogCurrentTS()
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("current ts = %d, want %d", got, want)
	}
	// 空库 = 0
	empty := setupTestStore(t)
	z, err := empty.ReplLogCurrentTS()
	if err != nil {
		t.Fatal(err)
	}
	if z != 0 {
		t.Fatalf("empty current ts = %d, want 0", z)
	}
}

// TestReplLogTSMonotonicUnderConcurrentWrites 并发写压下日志键 ts 单调实证（ts 单调
// 守卫种子——a4 §10 附7 验证门槛）：多 goroutine 并发 SET（不同键——per-key 锁无串行
// 化）——ts 源串行分配（提交序即 ts 序）不得倒挂——日志键 ts 严格升序 + 无重复。
// 以 -race 运行同时验证 ts 分配路径无共享竞态。
func TestReplLogTSMonotonicUnderConcurrentWrites(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	const workers = 8
	const perWorker = 25
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				k := fmt.Sprintf("conc:%d:%d", w, i)
				if err := s.Set(k, "v"); err != nil {
					t.Errorf("concurrent SET %s: %v", k, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	entries := replLogEntries(t, s)
	if len(entries) != workers*perWorker {
		t.Fatalf("log entries = %d, want %d", len(entries), workers*perWorker)
	}
	for i := 1; i < len(entries); i++ {
		if entries[i][0] <= entries[i-1][0] {
			t.Fatalf("log ts not strictly ascending at %d: %d <= %d (ordering violated under concurrency)", i, entries[i][0], entries[i-1][0])
		}
	}
}

// TestReplLogEntriesFrom 验证增量扫描 API：从指定 ts（含）起返回条目（键序 seek——
// 首个 ts >= since）——log-key 增量流（master 侧按从侧请求 ts 增量发送）的读取基础。
func TestReplLogEntriesFrom(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	const n = 20
	for i := 0; i < n; i++ {
		if err := s.Set(fmt.Sprintf("from:key:%d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}
	all := replLogEntries(t, s)
	if len(all) != n {
		t.Fatalf("log entries = %d, want %d", len(all), n)
	}
	// 从中间 ts 起——应为后半段（含中点的 ts）
	midTS := all[n/2][0]
	from, err := s.ReplLogEntriesFrom(midTS)
	if err != nil {
		t.Fatal(err)
	}
	if len(from) != n-n/2 {
		t.Fatalf("entries from mid ts = %d, want %d", len(from), n-n/2)
	}
	for i := 0; i < len(from); i++ {
		if from[i].TS != all[n/2+i][0] {
			t.Fatalf("entry %d ts = %d, want %d (from-ts scan misaligned)", i, from[i].TS, all[n/2+i][0])
		}
	}
	// 从 0 起 = 全量（与 ReplLogEntries 等价）
	from0, err := s.ReplLogEntriesFrom(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(from0) != n {
		t.Fatalf("entries from 0 = %d, want %d", len(from0), n)
	}
}
