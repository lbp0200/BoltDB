package store

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

// newTestStoreWithVlogSize builds a store whose vlog files are vlogSize
// bytes (default is 1GB, which makes GC tests impractical) and whose
// memtables are small enough that a modest number of keys flushes to the
// LSM. Mirrors the field initialization of NewBotreonStoreWithCompression.
func newTestStoreWithVlogSize(t *testing.T, vlogSize int64) *BotreonStore {
	t.Helper()
	dir := t.TempDir()
	opts := badger.DefaultOptions(dir)
	opts.ValueLogFileSize = vlogSize
	// Small memtables so modest test data actually flushes to the LSM
	// (default 64MB would keep everything in memory, where GC can't see it).
	// Note: values above ValueThreshold live in the vlog, so memtable bytes
	// are key+valuePointer only — many keys are needed to fill a memtable.
	opts.MemTableSize = 1024 * 1024
	opts.ValueThreshold = 512
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	bpConfig := DefaultBackpressureConfig()
	p := newWriteSlot(bpConfig.MaxConcurrentWrites)
	s := &BotreonStore{
		db:                  db,
		compressionType:     CompressionSnappy,
		keyLockMgr:          NewKeyLockManager(runtime.GOMAXPROCS(0) * 16),
		blockingPopChans:    make(map[string][]chan BlockingResult),
		blockingZPopChans:   make(map[string][]chan string),
		streamBlockingChans: make(map[string][]chan StreamReadResult),
		l0Cache:             &l0Cache{},
		zsetRankCaches:      make(map[string]*zsetRankCache),
		closeCh:             make(chan struct{}),
		scanBookmarks:       make(map[uint64][]byte),
	}
	s.backpressure.Store(p)
	cfg := bpConfig
	s.bpConfig.Store(&cfg)
	qbCfg := DefaultQueryBudgetConfig()
	s.queryBudgetConfig.Store(&qbCfg)
	t.Cleanup(func() { _ = s.CloseWithTimeout(CloseTimeout) })
	return s
}

// firstVlogFile returns the path of the first (oldest) .vlog file in dir,
// or "" if none exists. fids are zero-padded, so lexicographic order matches.
func firstVlogFile(dir string) string {
	files, err := filepath.Glob(filepath.Join(dir, "*.vlog"))
	if err != nil || len(files) == 0 {
		return ""
	}
	return files[0]
}

// TestRunValueLogGC verifies that RunValueLogGC rewrites vlog files that
// contain garbage (deleted values) and drops the old files, while keeping
// all live keys readable. Badger never GCs the currently-active vlog file,
// and values above ValueThreshold only occupy ~50B of memtable each (key +
// valuePointer), so the test writes enough keys to spill across several
// memtable flushes and vlog files.
func TestRunValueLogGC(t *testing.T) {
	s := newTestStoreWithVlogSize(t, 1024*1024)

	// 60000 keys × 1KB incompressible values → ~60MB of vlog data spanning
	// many 1MB vlog files; enough keys to force multiple memtable flushes.
	const n = 60000
	rng := rand.New(rand.NewSource(42))
	value := make([]byte, 1024)
	for i := range value {
		value[i] = byte(rng.Intn(256))
	}
	for i := 0; i < n; i++ {
		if err := s.Set(fmt.Sprintf("gc:key:%d", i), string(value)); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}

	dir := s.db.Opts().Dir
	oldVlog := firstVlogFile(dir)
	if oldVlog == "" {
		t.Fatal("expected at least one vlog file after writes")
	}

	// Delete 80% of them → those become vlog garbage.
	for i := 0; i < n-12000; i++ {
		if _, err := s.Del(fmt.Sprintf("gc:key:%d", i)); err != nil {
			t.Fatalf("del %d: %v", i, err)
		}
	}

	// GC picks files based on discard stats, which the LSM only records
	// during compaction. Force a full compaction so the deletions count.
	if err := s.db.Flatten(1); err != nil {
		t.Fatalf("flatten: %v", err)
	}

	rewritten, err := s.RunValueLogGC(0.5)
	if err != nil {
		t.Fatalf("RunValueLogGC: %v", err)
	}
	if rewritten == 0 {
		t.Fatal("expected at least one vlog file to be rewritten")
	}

	// The oldest vlog file must have been dropped by GC.
	if _, err := os.Stat(oldVlog); err == nil {
		t.Fatalf("old vlog file %s still exists after GC", oldVlog)
	}

	// All remaining keys must still be readable.
	for i := n - 12000; i < n; i++ {
		got, err := s.Get(fmt.Sprintf("gc:key:%d", i))
		if err != nil {
			t.Fatalf("get %d after GC: %v", i, err)
		}
		if got != string(value) {
			t.Fatalf("get %d: value mismatch", i)
		}
	}
}

// TestRunValueLogGC_InvalidRatio verifies ratio validation.
func TestRunValueLogGC_InvalidRatio(t *testing.T) {
	s := setupTestStore(t)

	for _, ratio := range []float64{-0.1, 1.5} {
		if _, err := s.RunValueLogGC(ratio); err == nil {
			t.Fatalf("ratio %v: expected error, got nil", ratio)
		}
	}
}
