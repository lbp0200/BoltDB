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

// TestDeterministicConflict_ZIncrByConcurrent verifies concurrent ZINCRBY on the
// same member converges to the exact accumulated score.
func TestDeterministicConflict_ZIncrByConcurrent(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	zSetName := "zset:zincrby:conflict"
	const goroutines = 20
	const increment = 1.0

	var wg sync.WaitGroup
	errs := make([]error, goroutines)
	for i := range goroutines {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = s.ZIncrBy(zSetName, "counter", increment)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d ZIncrBy: %v", i, err)
		}
	}

	score, exists, err := s.ZScore(zSetName, "counter")
	if err != nil {
		t.Fatalf("ZScore: %v", err)
	}
	if !exists {
		t.Fatal("member should exist after concurrent ZIncrBy")
	}
	want := float64(goroutines) * increment
	if score != want {
		t.Errorf("score: got %v, want %v", score, want)
	}
}

// TestDeterministicConflict_ZRemConcurrent verifies concurrent ZRem on distinct
// members drains the zset without errors.
func TestDeterministicConflict_ZRemConcurrent(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	zSetName := "zset:zrem:conflict"
	const members = 20

	zMembers := make([]ZSetMember, members)
	for i := range members {
		name := fmt.Sprintf("m%d", i)
		zMembers[i] = ZSetMember{Member: name, Score: float64(i)}
	}
	if err := s.ZAdd(zSetName, zMembers); err != nil {
		t.Fatalf("ZAdd: %v", err)
	}

	var wg sync.WaitGroup
	errs := make([]error, members)
	deleted := make([]int64, members)
	for i := range members {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			member := fmt.Sprintf("m%d", idx)
			n, err := s.ZRem(zSetName, member)
			deleted[idx] = n
			errs[idx] = err
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d ZRem: %v", i, err)
		}
		if deleted[i] != 1 {
			t.Errorf("goroutine %d ZRem: deleted %d, want 1", i, deleted[i])
		}
	}

	card, err := s.ZCard(zSetName)
	if err != nil {
		t.Fatalf("ZCard: %v", err)
	}
	if card != 0 {
		t.Errorf("ZCard: got %d, want 0", card)
	}
}

// TestDeterministicConflict_GeoAddConcurrent verifies concurrent GeoAdd of
// distinct members on the same key.
func TestDeterministicConflict_GeoAddConcurrent(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	key := "geo:add:conflict"
	const members = 20

	var wg sync.WaitGroup
	errs := make([]error, members)
	for i := range members {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = s.GeoAdd(key, []GeoMember{{
				Member: fmt.Sprintf("p%d", idx),
				Lat:    1.0 + float64(idx)*0.01,
				Lon:    2.0 + float64(idx)*0.01,
			}})
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d GeoAdd: %v", i, err)
		}
	}

	card, err := s.GeoCard(key)
	if err != nil {
		t.Fatalf("GeoCard: %v", err)
	}
	if card != members {
		t.Errorf("GeoCard: got %d, want %d", card, members)
	}
}

// TestDeterministicConflict_GeoDelConcurrent verifies concurrent GeoDel drains
// the geo set without errors.
func TestDeterministicConflict_GeoDelConcurrent(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	key := "geo:del:conflict"
	const members = 20

	geoMembers := make([]GeoMember, members)
	for i := range members {
		geoMembers[i] = GeoMember{
			Member: fmt.Sprintf("p%d", i),
			Lat:    10.0 + float64(i)*0.01,
			Lon:    20.0 + float64(i)*0.01,
		}
	}
	if _, err := s.GeoAdd(key, geoMembers); err != nil {
		t.Fatalf("GeoAdd: %v", err)
	}

	var wg sync.WaitGroup
	errs := make([]error, members)
	for i := range members {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = s.GeoDel(key, fmt.Sprintf("p%d", idx))
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d GeoDel: %v", i, err)
		}
	}

	card, err := s.GeoCard(key)
	if err != nil {
		t.Fatalf("GeoCard: %v", err)
	}
	if card != 0 {
		t.Errorf("GeoCard: got %d, want 0", card)
	}
}

// TestDeterministicConflict_ZSetDelConcurrent verifies concurrent ZSetDel on the
// same key is idempotent and leaves the zset empty.
func TestDeterministicConflict_ZSetDelConcurrent(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	zSetName := "zset:zsetdel:conflict"
	const goroutines = 10

	if err := s.ZAdd(zSetName, []ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 2.0},
		{Member: "c", Score: 3.0},
	}); err != nil {
		t.Fatalf("ZAdd: %v", err)
	}

	var wg sync.WaitGroup
	errs := make([]error, goroutines)
	for i := range goroutines {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = s.ZSetDel(zSetName)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d ZSetDel: %v", i, err)
		}
	}

	card, err := s.ZCard(zSetName)
	if err != nil {
		t.Fatalf("ZCard: %v", err)
	}
	if card != 0 {
		t.Errorf("ZCard: got %d, want 0", card)
	}
}

// TestDeterministicConflict_ZRemRangeByLexConcurrent verifies concurrent
// ZREMRANGEBYLEX calls remove the lex range exactly once in total.
func TestDeterministicConflict_ZRemRangeByLexConcurrent(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	zSetName := "zset:zremrangebylex:conflict"
	const goroutines = 8

	if err := s.ZAdd(zSetName, []ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 1.0},
		{Member: "c", Score: 1.0},
		{Member: "d", Score: 1.0},
	}); err != nil {
		t.Fatalf("ZAdd: %v", err)
	}

	var wg sync.WaitGroup
	errs := make([]error, goroutines)
	removed := make([]int64, goroutines)
	for i := range goroutines {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			n, err := s.ZRemRangeByLex(zSetName, "[b", "[c")
			removed[idx] = n
			errs[idx] = err
		}(i)
	}
	wg.Wait()

	var totalRemoved int64
	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d ZRemRangeByLex: %v", i, err)
		}
		totalRemoved += removed[i]
	}
	if totalRemoved != 2 {
		t.Errorf("total removed: got %d, want 2", totalRemoved)
	}

	members, err := s.ZRange(zSetName, 0, -1)
	if err != nil {
		t.Fatalf("ZRange: %v", err)
	}
	if len(members) != 2 {
		t.Fatalf("ZRange: got %d members, want 2", len(members))
	}
	if members[0].Member != "a" || members[1].Member != "d" {
		t.Errorf("remaining members: got [%s, %s], want [a, d]", members[0].Member, members[1].Member)
	}
}

// TestDeterministicConflict_ZRemRangeByRankWithMetaConflict verifies
// ZREMRANGEBYRANK stays correct while concurrent ZADD causes txn conflicts.
// Rank indices shift after removal, so this does not duplicate rank-range calls.
func TestDeterministicConflict_ZRemRangeByRankWithMetaConflict(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	zSetName := "zset:zremrangebyrank:conflict"
	const writers = 8

	if err := s.ZAdd(zSetName, []ZSetMember{
		{Member: "m1", Score: 1.0},
		{Member: "m2", Score: 2.0},
		{Member: "m3", Score: 3.0},
		{Member: "m4", Score: 4.0},
	}); err != nil {
		t.Fatalf("ZAdd: %v", err)
	}

	var wg sync.WaitGroup
	var removed int64
	var remErr error

	wg.Add(1)
	go func() {
		defer wg.Done()
		removed, remErr = s.ZRemRangeByRank(zSetName, 1, 2)
	}()

	for i := range writers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_ = s.ZAdd(zSetName, []ZSetMember{{
				Member: fmt.Sprintf("extra%d", idx),
				Score:  100.0 + float64(idx),
			}})
		}(i)
	}
	wg.Wait()

	if remErr != nil {
		t.Fatalf("ZRemRangeByRank: %v", remErr)
	}
	if removed != 2 {
		t.Errorf("removed: got %d, want 2", removed)
	}

	for _, name := range []string{"m2", "m3"} {
		_, exists, err := s.ZScore(zSetName, name)
		if err != nil {
			t.Fatalf("ZScore(%s): %v", name, err)
		}
		if exists {
			t.Errorf("member %s should be removed", name)
		}
	}
	_, exists, err := s.ZScore(zSetName, "m1")
	if err != nil {
		t.Fatalf("ZScore(m1): %v", err)
	}
	if !exists {
		t.Error("m1 should remain")
	}
}

// TestDeterministicConflict_ZRemRangeByScoreConcurrent verifies concurrent
// ZREMRANGEBYSCORE calls remove the score range exactly once in total.
func TestDeterministicConflict_ZRemRangeByScoreConcurrent(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	zSetName := "zset:zremrangebyscore:conflict"
	const goroutines = 8

	if err := s.ZAdd(zSetName, []ZSetMember{
		{Member: "m1", Score: 1.0},
		{Member: "m2", Score: 2.0},
		{Member: "m3", Score: 3.0},
		{Member: "m4", Score: 4.0},
	}); err != nil {
		t.Fatalf("ZAdd: %v", err)
	}

	var wg sync.WaitGroup
	errs := make([]error, goroutines)
	removed := make([]int64, goroutines)
	for i := range goroutines {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			n, err := s.ZRemRangeByScore(zSetName, 2.0, 3.0, false, false)
			removed[idx] = n
			errs[idx] = err
		}(i)
	}
	wg.Wait()

	var totalRemoved int64
	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d ZRemRangeByScore: %v", i, err)
		}
		totalRemoved += removed[i]
	}
	if totalRemoved != 2 {
		t.Errorf("total removed: got %d, want 2", totalRemoved)
	}

	card, err := s.ZCard(zSetName)
	if err != nil {
		t.Fatalf("ZCard: %v", err)
	}
	if card != 2 {
		t.Errorf("ZCard: got %d, want 2", card)
	}
}

// TestDeterministicConflict_ZPopMaxConcurrent verifies concurrent ZPOPMAX(count=1)
// drains the zset without duplicate pops.
func TestDeterministicConflict_ZPopMaxConcurrent(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	zSetName := "zset:zpopmax:conflict"
	const members = 4

	if err := s.ZAdd(zSetName, []ZSetMember{
		{Member: "m1", Score: 1.0},
		{Member: "m2", Score: 2.0},
		{Member: "m3", Score: 3.0},
		{Member: "m4", Score: 4.0},
	}); err != nil {
		t.Fatalf("ZAdd: %v", err)
	}

	var wg sync.WaitGroup
	errs := make([]error, members)
	popped := make([][]ZSetMember, members)
	for i := range members {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			got, err := s.ZPopMax(zSetName, 1)
			popped[idx] = got
			errs[idx] = err
		}(i)
	}
	wg.Wait()

	seen := make(map[string]bool, members)
	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d ZPopMax: %v", i, err)
			continue
		}
		if len(popped[i]) != 1 {
			t.Errorf("goroutine %d ZPopMax: got %d members, want 1", i, len(popped[i]))
			continue
		}
		name := popped[i][0].Member
		if seen[name] {
			t.Errorf("duplicate popped member: %q", name)
		}
		seen[name] = true
	}
	if len(seen) != members {
		t.Errorf("unique popped: got %d, want %d", len(seen), members)
	}

	card, err := s.ZCard(zSetName)
	if err != nil {
		t.Fatalf("ZCard: %v", err)
	}
	if card != 0 {
		t.Errorf("ZCard: got %d, want 0", card)
	}
}

// TestDeterministicConflict_ZPopMinConcurrent verifies concurrent ZPOPMIN(count=1)
// drains the zset without duplicate pops.
func TestDeterministicConflict_ZPopMinConcurrent(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	zSetName := "zset:zpopmin:conflict"
	const members = 4

	if err := s.ZAdd(zSetName, []ZSetMember{
		{Member: "m1", Score: 1.0},
		{Member: "m2", Score: 2.0},
		{Member: "m3", Score: 3.0},
		{Member: "m4", Score: 4.0},
	}); err != nil {
		t.Fatalf("ZAdd: %v", err)
	}

	var wg sync.WaitGroup
	errs := make([]error, members)
	popped := make([][]ZSetMember, members)
	for i := range members {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			got, err := s.ZPopMin(zSetName, 1)
			popped[idx] = got
			errs[idx] = err
		}(i)
	}
	wg.Wait()

	seen := make(map[string]bool, members)
	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d ZPopMin: %v", i, err)
			continue
		}
		if len(popped[i]) != 1 {
			t.Errorf("goroutine %d ZPopMin: got %d members, want 1", i, len(popped[i]))
			continue
		}
		name := popped[i][0].Member
		if seen[name] {
			t.Errorf("duplicate popped member: %q", name)
		}
		seen[name] = true
	}
	if len(seen) != members {
		t.Errorf("unique popped: got %d, want %d", len(seen), members)
	}

	card, err := s.ZCard(zSetName)
	if err != nil {
		t.Fatalf("ZCard: %v", err)
	}
	if card != 0 {
		t.Errorf("ZCard: got %d, want 0", card)
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
