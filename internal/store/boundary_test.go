package store

import (
	"fmt"
	"strings"
	"testing"
)

// setupBoundaryStore creates a fresh store for large-scale tests
func setupBoundaryStore(t *testing.T) *BotreonStore {
	t.Helper()
	store, err := NewBadgerStore(t.TempDir())
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Logf("close store: %v", err)
		}
	})
	return store
}

func TestBoundary_LargeString1MB(t *testing.T) {
	t.Parallel()
	s := setupBoundaryStore(t)

	data := strings.Repeat("A", 1024*1024) // 1MB
	if err := s.Set("large_str", data); err != nil {
		t.Fatalf("Set 1MB string failed: %v", err)
	}
	got, err := s.Get("large_str")
	if err != nil {
		t.Fatalf("Get 1MB string failed: %v", err)
	}
	if len(got) != len(data) {
		t.Fatalf("length mismatch: got %d, want %d", len(got), len(data))
	}
	if got != data {
		t.Fatal("1MB string content mismatch")
	}
}

func TestBoundary_LargeString10MB(t *testing.T) {
	t.Parallel()
	s := setupBoundaryStore(t)

	data := strings.Repeat("ABCDEFGH", 1024*1024*10/8) // 10MB
	if err := s.Set("large_str", data); err != nil {
		t.Fatalf("Set 10MB string failed: %v", err)
	}
	got, err := s.Get("large_str")
	if err != nil {
		t.Fatalf("Get 10MB string failed: %v", err)
	}
	if len(got) != len(data) {
		t.Fatalf("length mismatch: got %d, want %d", len(got), len(data))
	}
	if got != data {
		t.Fatal("10MB string content mismatch")
	}
}

func TestBoundary_LargeString100MB(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping 100MB string test in short mode")
	}
	s := setupBoundaryStore(t)

	data := strings.Repeat("ABCDEFGH", 1024*1024*100/8) // 100MB
	if err := s.Set("large_str", data); err != nil {
		t.Fatalf("Set 100MB string failed: %v", err)
	}
	got, err := s.Get("large_str")
	if err != nil {
		t.Fatalf("Get 100MB string failed: %v", err)
	}
	if len(got) != len(data) {
		t.Fatalf("length mismatch: got %d, want %d", len(got), len(data))
	}
}

const insertBatchSize = 1000

func batchInsertList(s *BotreonStore, key string, n int) error {
	vals := make([]string, insertBatchSize)
	for i := 0; i < n; i += insertBatchSize {
		end := i + insertBatchSize
		if end > n {
			end = n
		}
		for j := i; j < end; j++ {
			vals[j-i] = fmt.Sprintf("val:%d", j)
		}
		if _, err := s.RPush(key, vals[:end-i]...); err != nil {
			return fmt.Errorf("batch at %d: %w", i, err)
		}
	}
	return nil
}

func batchInsertSet(s *BotreonStore, key string, n int) error {
	members := make([]string, insertBatchSize)
	for i := 0; i < n; i += insertBatchSize {
		end := i + insertBatchSize
		if end > n {
			end = n
		}
		for j := i; j < end; j++ {
			members[j-i] = fmt.Sprintf("member:%d", j)
		}
		if _, err := s.SAdd(key, members[:end-i]...); err != nil {
			return fmt.Errorf("batch at %d: %w", i, err)
		}
	}
	return nil
}

func batchInsertZSet(s *BotreonStore, key string, n int) error {
	members := make([]ZSetMember, insertBatchSize)
	for i := 0; i < n; i += insertBatchSize {
		end := i + insertBatchSize
		if end > n {
			end = n
		}
		for j := i; j < end; j++ {
			members[j-i] = ZSetMember{
				Member: fmt.Sprintf("member:%d", j),
				Score:  float64(j),
			}
		}
		if err := s.ZAdd(key, members[:end-i]); err != nil {
			return fmt.Errorf("batch at %d: %w", i, err)
		}
	}
	return nil
}

func TestBoundary_LargeList100K(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping 100K list test in short mode")
	}
	s := setupBoundaryStore(t)
	const n = 100000

	t.Logf("inserting %d list elements (batch=%d)...", n, insertBatchSize)
	if err := batchInsertList(s, "biglist", n); err != nil {
		t.Fatalf("batch insert failed: %v", err)
	}

	llen, err := s.LLen("biglist")
	if err != nil {
		t.Fatalf("LLen failed: %v", err)
	}
	if llen != n {
		t.Fatalf("LLen: got %d, want %d", llen, n)
	}

	vals, err := s.LRange("biglist", 0, -1)
	if err != nil {
		t.Fatalf("LRange failed: %v", err)
	}
	if len(vals) != n {
		t.Fatalf("LRange count: got %d, want %d", len(vals), n)
	}
}

func TestBoundary_LargeSet100K(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping 100K set test in short mode")
	}
	s := setupBoundaryStore(t)
	const n = 100000

	t.Logf("inserting %d set members (batch=%d)...", n, insertBatchSize)
	if err := batchInsertSet(s, "bigset", n); err != nil {
		t.Fatalf("batch insert failed: %v", err)
	}

	card, err := s.SCard("bigset")
	if err != nil {
		t.Fatalf("SCard failed: %v", err)
	}
	if card != n {
		t.Fatalf("SCard: got %d, want %d", card, n)
	}

	members, err := s.SMembers("bigset")
	if err != nil {
		t.Fatalf("SMembers failed: %v", err)
	}
	if len(members) != n {
		t.Fatalf("SMembers count: got %d, want %d", len(members), n)
	}
}

func TestBoundary_LargeZSet100K(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping 100K zset test in short mode")
	}
	s := setupBoundaryStore(t)
	const n = 100000

	t.Logf("inserting %d zset members (batch=%d)...", n, insertBatchSize)
	if err := batchInsertZSet(s, "bigzset", n); err != nil {
		t.Fatalf("batch insert failed: %v", err)
	}

	card, err := s.ZCard("bigzset")
	if err != nil {
		t.Fatalf("ZCard failed: %v", err)
	}
	if card != n {
		t.Fatalf("ZCard: got %d, want %d", card, n)
	}

	results, err := s.ZRange("bigzset", 0, -1)
	if err != nil {
		t.Fatalf("ZRange failed: %v", err)
	}
	if len(results) != n {
		t.Fatalf("ZRange count: got %d, want %d", len(results), n)
	}
}

func TestBoundary_LargeHash100K(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping 100K hash test in short mode")
	}
	s := setupBoundaryStore(t)
	const n = 100000

	t.Logf("inserting %d hash fields...", n)
	for i := 0; i < n; i++ {
		field := fmt.Sprintf("field:%d", i)
		if err := s.HSet("bighash", field, fmt.Sprintf("val:%d", i)); err != nil {
			t.Fatalf("HSet %d failed: %v", i, err)
		}
	}

	hlen, err := s.HLen("bighash")
	if err != nil {
		t.Fatalf("HLen failed: %v", err)
	}
	if hlen != n {
		t.Fatalf("HLen: got %d, want %d", hlen, n)
	}

	keys, err := s.HKeys("bighash")
	if err != nil {
		t.Fatalf("HKeys failed: %v", err)
	}
	if len(keys) != n {
		t.Fatalf("HKeys count: got %d, want %d", len(keys), n)
	}
}

func makeTestValues(n int) []string {
	vals := make([]string, n)
	for i := 0; i < n; i++ {
		vals[i] = fmt.Sprintf("val:%d", i)
	}
	return vals
}

func TestBoundary_LargeStream100K(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping 100K stream test in short mode")
	}
	s := setupBoundaryStore(t)
	const n = 100000

	// Insert 100K entries using batch XAdd
	for i := 0; i < n; i += insertBatchSize {
		end := i + insertBatchSize
		if end > n {
			end = n
		}
		for j := i; j < end; j++ {
			id := fmt.Sprintf("%d-%d", j/1000, j%1000)
			fields := map[string]string{
				"seq":  fmt.Sprintf("%d", j),
				"data": fmt.Sprintf("payload:%d", j),
			}
			if _, err := s.XAdd("bigstream", StreamXAddOptions{}, id, fields); err != nil {
				t.Fatalf("XAdd %d failed: %v", j, err)
			}
		}
	}

	// Verify length
	length, err := s.XLen("bigstream")
	if err != nil {
		t.Fatalf("XLen failed: %v", err)
	}
	if length != n {
		t.Fatalf("XLen: got %d, want %d", length, n)
	}

	// Verify range query returns all entries
	entries, err := s.XRange("bigstream", "-", "+", int64(n))
	if err != nil {
		t.Fatalf("XRange failed: %v", err)
	}
	if len(entries) != n {
		t.Fatalf("XRange count: got %d, want %d", len(entries), n)
	}

	// Verify first and last entry
	if entries[0].ID == "" {
		t.Fatal("first entry has empty ID")
	}
	if entries[len(entries)-1].ID == "" {
		t.Fatal("last entry has empty ID")
	}
}
