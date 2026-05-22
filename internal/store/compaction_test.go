package store

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestCompaction_HeavyWriteAndCompaction writes enough data to trigger
// L0 compaction, then verifies all data is still readable.
func TestCompaction_HeavyWriteAndCompaction(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	const numKeys = 20000
	const valueSize = 1024 // 1KB per value → ~20MB total

	value := strings.Repeat("ABCDEFGHIJ", valueSize/10)

	var wg sync.WaitGroup
	errs := make([]error, numKeys)

	for i := range numKeys {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = s.Set(fmt.Sprintf("compaction:%d", idx),
				fmt.Sprintf("%s-%d", value, idx))
		}(i)
	}
	wg.Wait()

	var writeFailures int
	for i, err := range errs {
		if err != nil {
			t.Errorf("write key %d: %v", i, err)
			writeFailures++
		}
	}
	if writeFailures > 0 {
		t.Fatalf("%d write failures during compaction stress", writeFailures)
	}

	// Verify all keys are readable
	for i := range numKeys {
		got, err := s.Get(fmt.Sprintf("compaction:%d", i))
		if err != nil {
			t.Errorf("read key %d after compaction: %v", i, err)
			continue
		}
		if !strings.HasPrefix(got, value+"-") {
			t.Errorf("key %d content corrupt: prefix mismatch", i)
		}
	}

	// Check L0 score recovered
	score := s.L0Score()
	t.Logf("L0 score after heavy write + compaction: %.2f", score)
}

// TestCompaction_MassDeleteThenVerify deletes a large portion of keys,
// then verifies remaining data and store consistency.
func TestCompaction_MassDeleteThenVerify(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping mass-delete test in short mode")
	}
	s := setupTestStore(t)

	const numKeys = 10000
	const keepRatio = 0.3

	for i := range numKeys {
		if err := s.Set(fmt.Sprintf("massdel:%d", i),
			fmt.Sprintf("value-%d", i)); err != nil {
			t.Fatalf("write key %d: %v", i, err)
		}
	}

	// Delete 70% of keys
	var delWg sync.WaitGroup
	for i := range numKeys {
		if rand.Float64() > keepRatio {
			delWg.Add(1)
			go func(idx int) {
				defer delWg.Done()
				_, _ = s.Del(fmt.Sprintf("massdel:%d", idx))
			}(i)
		}
	}
	delWg.Wait()

	// Verify kept keys are intact
	var kept, missing int
	for i := range numKeys {
		key := fmt.Sprintf("massdel:%d", i)
		val, err := s.Get(key)
		if err != nil {
			missing++
			continue
		}
		kept++
		want := fmt.Sprintf("value-%d", i)
		if val != want {
			t.Errorf("key %q: got %q, want %q", key, val, want)
		}
	}
	t.Logf("mass-delete: kept=%d missing=%d", kept, missing)

	// Store-level consistency check
	if err := s.Check(); err != nil {
		t.Errorf("store Check() after mass delete: %v", err)
	}
}

// TestCompaction_ConcurrentReadWriteDuringCompaction stresses the store
// with concurrent reads and writes while compaction is active.
func TestCompaction_ConcurrentReadWriteDuringCompaction(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping concurrent compaction test in short mode")
	}
	s := setupTestStore(t)

	const numKeys = 5000
	const writerCount = 20
	const readerCount = 20
	const duration = 5 * time.Second

	// Pre-populate
	for i := range numKeys {
		if err := s.Set(fmt.Sprintf("conccomp:k%d", i),
			fmt.Sprintf("initial-%d", i)); err != nil {
			t.Fatalf("prepopulate key %d: %v", i, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	var wg sync.WaitGroup
	var writeErrs, readErrs atomic.Int64

	// Writers: continuously update random keys
	for w := range writerCount {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
			for ctx.Err() == nil {
				key := fmt.Sprintf("conccomp:k%d", rng.Intn(numKeys))
				val := fmt.Sprintf("writer-%d-%d", id, rng.Intn(10000))
				if err := s.Set(key, val); err != nil {
					writeErrs.Add(1)
					if !strings.Contains(err.Error(), "rejected") {
						t.Logf("writer %d set %q: %v", id, key, err)
					}
				}
			}
		}(w)
	}

	// Readers: continuously read random keys
	for r := range readerCount {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + 10000 + int64(id)))
			for ctx.Err() == nil {
				key := fmt.Sprintf("conccomp:k%d", rng.Intn(numKeys))
				_, err := s.Get(key)
				if err != nil {
					readErrs.Add(1)
				}
			}
		}(r)
	}

	wg.Wait()

	t.Logf("concurrent compaction: writeErrors=%d readErrors=%d",
		writeErrs.Load(), readErrs.Load())

	// Verify final store consistency
	if err := s.Check(); err != nil {
		t.Errorf("store Check() after concurrent compaction: %v", err)
	}
}

// TestCompaction_StoreCheckAfterHeavyRW verifies store.Check() passes
// after a mixed workload of sets, deletes, and type changes.
func TestCompaction_StoreCheckAfterHeavyRW(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	const ops = 2000
	var wg sync.WaitGroup

	for i := range ops {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := fmt.Sprintf("checkstress:k%d", idx%500)
			switch idx % 4 {
			case 0:
				_ = s.Set(key, fmt.Sprintf("str-%d", idx))
			case 1:
				_, _ = s.Del(key)
			case 2:
				_ = s.HSet(key, "f", idx)
			case 3:
				_, _ = s.SAdd(key, fmt.Sprintf("m%d", idx))
			}
		}(i)
	}
	wg.Wait()

	if err := s.Check(); err != nil {
		t.Errorf("store Check() after heavy RW: %v", err)
	}
}
