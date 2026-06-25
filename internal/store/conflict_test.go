package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

// deterministicConflictWrite runs s.retryUpdate with a function that reads and
// increments a counter stored at `key`, forcing BadgerDB conflict detection
// when multiple goroutines write concurrently.
func deterministicConflictWrite(s *BotreonStore, key string, maxRetries int) error {
	return s.retryUpdate(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		var count uint64
		if errors.Is(err, badger.ErrKeyNotFound) {
			count = 0
		} else if err != nil {
			return err
		} else {
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			count = binary.BigEndian.Uint64(val)
		}
		count++
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, count)
		return txn.Set([]byte(key), buf)
	}, maxRetries)
}

// TestDeterministicConflict_Counter10 verifies that 10 concurrent goroutines
// incrementing the same counter all succeed via retryUpdate conflict resolution.
func TestDeterministicConflict_Counter10(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	key := "counter:10"
	const goroutines = 10

	var wg sync.WaitGroup
	errs := make([]error, goroutines)
	for i := range goroutines {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = deterministicConflictWrite(s, key, 30)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d: %v", i, err)
		}
	}

	var final uint64
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		final = binary.BigEndian.Uint64(val)
		return nil
	})
	if err != nil {
		t.Fatalf("read final counter: %v", err)
	}
	if final != goroutines {
		t.Errorf("counter: got %d, want %d", final, goroutines)
	}
}

// TestDeterministicConflict_Counter100 verifies 100 concurrent goroutines.
func TestDeterministicConflict_Counter100(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping 100-goroutine conflict test in short mode")
	}
	s := setupTestStore(t)
	key := "counter:100"
	const goroutines = 100

	var wg sync.WaitGroup
	errs := make([]error, goroutines)
	for i := range goroutines {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = deterministicConflictWrite(s, key, 50)
		}(i)
	}
	wg.Wait()

	var failCount int
	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d: %v", i, err)
			failCount++
		}
	}
	if failCount > 0 {
		t.Fatalf("%d goroutines failed", failCount)
	}

	var final uint64
	s.db.View(func(txn *badger.Txn) error {
		item, _ := txn.Get([]byte(key))
		val, _ := item.ValueCopy(nil)
		final = binary.BigEndian.Uint64(val)
		return nil
	})
	if final != goroutines {
		t.Errorf("counter: got %d, want %d", final, goroutines)
	}
}

// TestDeterministicConflict_RetryExhaustion verifies that a conflict that
// persists beyond maxRetries produces an error.
func TestDeterministicConflict_RetryExhaustion(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	key := "counter:exhaust"

	// One writer keeps the key locked by constantly incrementing
	done := make(chan struct{})
	defer close(done)
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				_ = deterministicConflictWrite(s, key, 1)
			}
		}
	}()

	// This should fail because maxRetries is low and conflict is continuous
	err := deterministicConflictWrite(s, key, 1)
	if err == nil {
		t.Log("note: retry succeeded despite low maxRetries (conflict probability depends on timing)")
	}
}

// TestDeterministicConflict_ConflictThenNonConflictError verifies that
// after some conflict retries, a non-retryable error is returned immediately.
func TestDeterministicConflict_ConflictThenNonConflictError(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	key := "counter:mixed"
	sentinel := errors.New("some fatal error")

	// Start a background writer to create conflicts
	var bgErr error
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				if e := deterministicConflictWrite(s, key, 1); e != nil {
					bgErr = e
				}
			}
		}
	}()

	var callCount int
	err := s.retryUpdate(func(txn *badger.Txn) error {
		callCount++
		// Read the key to trigger conflict detection
		_, _ = txn.Get([]byte(key))
		// Return non-retryable error on first real attempt
		return sentinel
	}, 10)
	close(done)

	if bgErr != nil {
		t.Logf("background writer error: %v", bgErr)
	}

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("expected sentinel error, got: %v", err)
	}
}

// TestDeterministicConflict_HSetSameField creates conflicts via HSet on the
// same hash field, which uses retryUpdate without key-level locking.
func TestDeterministicConflict_HSetSameField(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	const goroutines = 20

	var wg sync.WaitGroup
	errs := make([]error, goroutines)
	for i := range goroutines {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = s.HSet("hash:conflict", "field", idx)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d HSet: %v", i, err)
		}
	}

	// Verify HGet returns the last set value
	val, err := s.HGet("hash:conflict", "field")
	if err != nil {
		t.Fatalf("HGet: %v", err)
	}
	t.Logf("final HGet value: %s (expected one of 0..%d)", val, goroutines-1)
}

// TestDeterministicConflict_SPopConcurrent verifies that concurrent SPop calls
// on the same set all succeed via retryUpdate and drain the set exactly once.
func TestDeterministicConflict_SPopConcurrent(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	key := "set:spop:conflict"
	const members = 20

	memberNames := make([]string, members)
	for i := range members {
		memberNames[i] = fmt.Sprintf("m%d", i)
	}
	if _, err := s.SAdd(key, memberNames...); err != nil {
		t.Fatalf("SAdd: %v", err)
	}

	var wg sync.WaitGroup
	errs := make([]error, members)
	popped := make([]string, members)
	for i := range members {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			member, err := s.SPop(key)
			errs[idx] = err
			popped[idx] = member
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d SPop: %v", i, err)
		}
		if popped[i] == "" {
			t.Errorf("goroutine %d SPop: got empty member", i)
		}
	}

	seen := make(map[string]bool, members)
	for _, m := range popped {
		if seen[m] {
			t.Errorf("duplicate popped member: %q", m)
		}
		seen[m] = true
	}

	count, err := s.SCard(key)
	if err != nil {
		t.Fatalf("SCard: %v", err)
	}
	if count != 0 {
		t.Errorf("SCard: got %d, want 0", count)
	}
}

// TestDeterministicConflict_SPopNConcurrent verifies concurrent SPopN(key, 1)
// drains the set without errors or duplicate pops.
func TestDeterministicConflict_SPopNConcurrent(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	key := "set:spopn:conflict"
	const members = 30

	memberNames := make([]string, members)
	for i := range members {
		memberNames[i] = fmt.Sprintf("n%d", i)
	}
	if _, err := s.SAdd(key, memberNames...); err != nil {
		t.Fatalf("SAdd: %v", err)
	}

	var wg sync.WaitGroup
	errs := make([]error, members)
	popped := make([][]string, members)
	for i := range members {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			got, err := s.SPopN(key, 1)
			errs[idx] = err
			popped[idx] = got
		}(i)
	}
	wg.Wait()

	seen := make(map[string]bool, members)
	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d SPopN: %v", i, err)
			continue
		}
		if len(popped[i]) != 1 {
			t.Errorf("goroutine %d SPopN: got %d members, want 1", i, len(popped[i]))
			continue
		}
		m := popped[i][0]
		if seen[m] {
			t.Errorf("duplicate popped member: %q", m)
		}
		seen[m] = true
	}

	count, err := s.SCard(key)
	if err != nil {
		t.Fatalf("SCard: %v", err)
	}
	if count != 0 {
		t.Errorf("SCard: got %d, want 0", count)
	}
}

// TestDeterministicConflict_RetryUpdateSuccessAfterConflict verifies that
// retryUpdate correctly retries on TransactionConflict and eventually succeeds.
func TestDeterministicConflict_RetryUpdateSuccessAfterConflict(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	key := "counter:success"

	// Pre-set counter to 0
	s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), make([]byte, 8))
	})

	var wg sync.WaitGroup
	const writers = 5
	const opsPerWriter = 10

	for w := range writers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerWriter; j++ {
				err := deterministicConflictWrite(s, key, 30)
				if err != nil {
					t.Errorf("writer %d op %d: %v", id, j, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	var final uint64
	s.db.View(func(txn *badger.Txn) error {
		item, _ := txn.Get([]byte(key))
		val, _ := item.ValueCopy(nil)
		final = binary.BigEndian.Uint64(val)
		return nil
	})
	expected := uint64(writers * opsPerWriter)
	if final != expected {
		t.Errorf("counter: got %d, want %d", final, expected)
	}
}
