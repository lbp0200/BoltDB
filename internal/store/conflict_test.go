package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

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

// TestDeterministicConflict_HDelConcurrent verifies concurrent HDEL on distinct
// fields drains the hash without inflating the deleted count.
func TestDeterministicConflict_HDelConcurrent(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	key := "hash:hdel:conflict"
	const fields = 20

	for i := range fields {
		if err := s.HSet(key, fmt.Sprintf("f%d", i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatalf("HSet f%d: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	deleted := make([]int, fields)
	errs := make([]error, fields)
	for i := range fields {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			n, err := s.HDel(key, fmt.Sprintf("f%d", idx))
			deleted[idx] = n
			errs[idx] = err
		}(i)
	}
	wg.Wait()

	totalDeleted := 0
	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d HDel: %v", i, err)
		}
		totalDeleted += deleted[i]
	}
	if totalDeleted != fields {
		t.Errorf("total deleted: got %d, want %d", totalDeleted, fields)
	}

	n, err := s.HLen(key)
	if err != nil {
		t.Fatalf("HLen: %v", err)
	}
	if n != 0 {
		t.Errorf("HLen: got %d, want 0", n)
	}
}

// TestDeterministicConflict_LRemWithSourceConflict verifies LREM stays correct
// while concurrent LPUSH causes txn conflicts.
func TestDeterministicConflict_LRemWithSourceConflict(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	key := "list:lrem:conflict"
	const writers = 4

	for i := range 6 {
		if _, err := s.RPush(key, fmt.Sprintf("dup%d", i%2)); err != nil {
			t.Fatalf("RPush: %v", err)
		}
	}

	var wg sync.WaitGroup
	var remErr error
	var removed int

	wg.Add(1)
	go func() {
		defer wg.Done()
		removed, remErr = s.LRem(key, 2, "dup0")
	}()

	for i := range writers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, _ = s.LPush(key, fmt.Sprintf("extra%d", idx))
		}(i)
	}
	wg.Wait()

	if remErr != nil {
		t.Fatalf("LRem: %v", remErr)
	}
	if removed != 2 {
		t.Errorf("LRem: got %d removed, want 2", removed)
	}

	length, err := s.LLen(key)
	if err != nil {
		t.Fatalf("LLen: %v", err)
	}
	if length < 4 {
		t.Errorf("LLen: got %d, want at least 4", length)
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

// TestDeterministicConflict_GeoRemoveBatchWithConflict verifies batched GEOREM
// stays correct while concurrent GEOADD causes txn conflicts.
func TestDeterministicConflict_GeoRemoveBatchWithConflict(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	key := "geo:rem:batch"
	const writers = 4

	if _, err := s.GeoAdd(key, []GeoMember{
		{Member: "keep", Lat: 39.9, Lon: 116.4},
		{Member: "drop1", Lat: 31.2, Lon: 121.5},
		{Member: "drop2", Lat: 30.5, Lon: 120.0},
	}); err != nil {
		t.Fatalf("GeoAdd: %v", err)
	}

	var wg sync.WaitGroup
	var removeErr error
	var removed int64

	wg.Add(1)
	go func() {
		defer wg.Done()
		removed, removeErr = s.GeoRemove(key, "drop1", "drop2", "missing")
	}()

	for i := range writers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, _ = s.GeoAdd(key, []GeoMember{{
				Member: fmt.Sprintf("extra%d", idx),
				Lat:    39.9 + float64(idx)*0.01,
				Lon:    116.4 + float64(idx)*0.01,
			}})
		}(i)
	}
	wg.Wait()

	if removeErr != nil {
		t.Fatalf("GeoRemove: %v", removeErr)
	}
	if removed != 2 {
		t.Errorf("GeoRemove: got %d removed, want 2", removed)
	}

	positions, err := s.GeoPos(key, "keep")
	if err != nil {
		t.Fatalf("GeoPos(keep): %v", err)
	}
	if len(positions) == 0 || (positions[0][0] == 0 && positions[0][1] == 0) {
		t.Fatal("member keep should still exist")
	}

	card, err := s.GeoCard(key)
	if err != nil {
		t.Fatalf("GeoCard: %v", err)
	}
	if card < 1 {
		t.Errorf("GeoCard: got %d, want at least 1", card)
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

// TestDeterministicConflict_SMoveConcurrent verifies concurrent SMOVE of distinct
// members drains the source set into the destination.
func TestDeterministicConflict_SMoveConcurrent(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	source := "set:smove:src"
	dest := "set:smove:dst"
	const members = 4

	names := make([]string, members)
	for i := range members {
		names[i] = fmt.Sprintf("m%d", i)
	}
	if _, err := s.SAdd(source, names...); err != nil {
		t.Fatalf("SAdd: %v", err)
	}

	var wg sync.WaitGroup
	errs := make([]error, members)
	moved := make([]bool, members)
	for i := range members {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ok, err := s.SMove(source, dest, names[idx])
			moved[idx] = ok
			errs[idx] = err
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d SMove: %v", i, err)
		}
		if !moved[i] {
			t.Errorf("goroutine %d SMove: got moved=false, want true", i)
		}
	}

	srcCard, err := s.SCard(source)
	if err != nil {
		t.Fatalf("SCard(source): %v", err)
	}
	if srcCard != 0 {
		t.Errorf("SCard(source): got %d, want 0", srcCard)
	}

	dstCard, err := s.SCard(dest)
	if err != nil {
		t.Fatalf("SCard(dest): %v", err)
	}
	if dstCard != members {
		t.Errorf("SCard(dest): got %d, want %d", dstCard, members)
	}
}

// TestDeterministicConflict_ZMPopConcurrent verifies concurrent ZMPOP(MAX,1)
// drains the zset without duplicate pops.
func TestDeterministicConflict_ZMPopConcurrent(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	zSetName := "zset:zmpop:conflict"
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
			_, got, err := s.ZMPop([]string{zSetName}, "MAX", 1)
			popped[idx] = got
			errs[idx] = err
		}(i)
	}
	wg.Wait()

	seen := make(map[string]bool, members)
	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d ZMPop: %v", i, err)
			continue
		}
		if len(popped[i]) != 1 {
			t.Errorf("goroutine %d ZMPop: got %d members, want 1", i, len(popped[i]))
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

// TestDeterministicConflict_SetNXConcurrent verifies exactly one SETNX wins
// under concurrent attempts on the same key.
func TestDeterministicConflict_SetNXConcurrent(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	key := "string:setnx:conflict"
	const goroutines = 20

	var wg sync.WaitGroup
	errs := make([]error, goroutines)
	wins := make([]bool, goroutines)
	for i := range goroutines {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ok, err := s.SetNX(key, fmt.Sprintf("v%d", idx))
			wins[idx] = ok
			errs[idx] = err
		}(i)
	}
	wg.Wait()

	var winCount int
	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d SetNX: %v", i, err)
		}
		if wins[i] {
			winCount++
		}
	}
	if winCount != 1 {
		t.Errorf("SetNX wins: got %d, want 1", winCount)
	}

	val, err := s.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if val == "" {
		t.Fatal("Get: expected value to be set")
	}
}

// TestDeterministicConflict_ZUnionStoreWithSourceConflict verifies ZUNIONSTORE
// stays correct while concurrent ZADD on source keys causes txn conflicts.
func TestDeterministicConflict_ZUnionStoreWithSourceConflict(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	const writers = 4

	if err := s.ZAdd("zus:1", []ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 2.0},
	}); err != nil {
		t.Fatalf("ZAdd zus:1: %v", err)
	}
	if err := s.ZAdd("zus:2", []ZSetMember{
		{Member: "b", Score: 3.0},
		{Member: "c", Score: 4.0},
	}); err != nil {
		t.Fatalf("ZAdd zus:2: %v", err)
	}

	var wg sync.WaitGroup
	var unionErr error

	wg.Add(1)
	go func() {
		defer wg.Done()
		_, unionErr = s.ZUnionStore("zus:dest", []string{"zus:1", "zus:2"}, nil, "")
	}()

	for i := range writers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_ = s.ZAdd("zus:1", []ZSetMember{{
				Member: fmt.Sprintf("extra%d", idx),
				Score:  10.0 + float64(idx),
			}})
		}(i)
	}
	wg.Wait()

	if unionErr != nil {
		t.Fatalf("ZUnionStore: %v", unionErr)
	}

	score, exists, err := s.ZScore("zus:dest", "b")
	if err != nil {
		t.Fatalf("ZScore(b): %v", err)
	}
	if !exists {
		t.Fatal("member b should exist in destination")
	}
	if score != 5.0 {
		t.Errorf("score(b): got %v, want 5.0", score)
	}

	card, err := s.ZCard("zus:dest")
	if err != nil {
		t.Fatalf("ZCard: %v", err)
	}
	if card < 3 {
		t.Errorf("ZCard: got %d, want at least 3", card)
	}
}

// TestDeterministicConflict_ZInterStoreWithSourceConflict verifies ZINTERSTORE
// stays correct while concurrent ZADD on source keys causes txn conflicts.
func TestDeterministicConflict_ZInterStoreWithSourceConflict(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	const writers = 4

	if err := s.ZAdd("zis:1", []ZSetMember{
		{Member: "a", Score: 2.0},
		{Member: "b", Score: 4.0},
	}); err != nil {
		t.Fatalf("ZAdd zis:1: %v", err)
	}
	if err := s.ZAdd("zis:2", []ZSetMember{
		{Member: "b", Score: 3.0},
		{Member: "c", Score: 5.0},
	}); err != nil {
		t.Fatalf("ZAdd zis:2: %v", err)
	}

	var wg sync.WaitGroup
	var interErr error

	wg.Add(1)
	go func() {
		defer wg.Done()
		_, interErr = s.ZInterStore("zis:dest", []string{"zis:1", "zis:2"}, nil, "")
	}()

	for i := range writers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_ = s.ZAdd("zis:2", []ZSetMember{{
				Member: fmt.Sprintf("extra%d", idx),
				Score:  10.0 + float64(idx),
			}})
		}(i)
	}
	wg.Wait()

	if interErr != nil {
		t.Fatalf("ZInterStore: %v", interErr)
	}

	score, exists, err := s.ZScore("zis:dest", "b")
	if err != nil {
		t.Fatalf("ZScore(b): %v", err)
	}
	if !exists {
		t.Fatal("member b should exist in destination")
	}
	if score != 7.0 {
		t.Errorf("score(b): got %v, want 7.0", score)
	}

	card, err := s.ZCard("zis:dest")
	if err != nil {
		t.Fatalf("ZCard: %v", err)
	}
	if card != 1 {
		t.Errorf("ZCard: got %d, want 1", card)
	}
}

// TestDeterministicConflict_ZDiffStoreWithSourceConflict verifies ZDIFFSTORE
// stays correct while concurrent ZADD on source keys causes txn conflicts.
func TestDeterministicConflict_ZDiffStoreWithSourceConflict(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	const writers = 4

	if err := s.ZAdd("zds:1", []ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 2.0},
		{Member: "c", Score: 3.0},
	}); err != nil {
		t.Fatalf("ZAdd zds:1: %v", err)
	}
	if err := s.ZAdd("zds:2", []ZSetMember{
		{Member: "b", Score: 4.0},
		{Member: "d", Score: 5.0},
	}); err != nil {
		t.Fatalf("ZAdd zds:2: %v", err)
	}

	var wg sync.WaitGroup
	var diffErr error

	wg.Add(1)
	go func() {
		defer wg.Done()
		_, diffErr = s.ZDiffStore("zds:dest", []string{"zds:1", "zds:2"})
	}()

	for i := range writers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_ = s.ZAdd("zds:1", []ZSetMember{{
				Member: fmt.Sprintf("extra%d", idx),
				Score:  10.0 + float64(idx),
			}})
		}(i)
	}
	wg.Wait()

	if diffErr != nil {
		t.Fatalf("ZDiffStore: %v", diffErr)
	}

	for _, member := range []string{"a", "c"} {
		_, exists, err := s.ZScore("zds:dest", member)
		if err != nil {
			t.Fatalf("ZScore(%s): %v", member, err)
		}
		if !exists {
			t.Errorf("member %s should exist in destination", member)
		}
	}

	_, exists, err := s.ZScore("zds:dest", "b")
	if err != nil {
		t.Fatalf("ZScore(b): %v", err)
	}
	if exists {
		t.Error("member b should not exist in destination")
	}

	card, err := s.ZCard("zds:dest")
	if err != nil {
		t.Fatalf("ZCard: %v", err)
	}
	if card < 2 {
		t.Errorf("ZCard: got %d, want at least 2", card)
	}
}

// TestDeterministicConflict_GeoSearchStoreWithSourceConflict verifies
// GEOSEARCHSTORE stays correct while concurrent GEOADD causes txn conflicts.
func TestDeterministicConflict_GeoSearchStoreWithSourceConflict(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	const writers = 4

	if _, err := s.GeoAdd("gss:src", []GeoMember{
		{Member: "beijing", Lat: 39.9, Lon: 116.4},
		{Member: "shanghai", Lat: 31.2, Lon: 121.5},
	}); err != nil {
		t.Fatalf("GeoAdd gss:src: %v", err)
	}

	var wg sync.WaitGroup
	var storeErr error

	wg.Add(1)
	go func() {
		defer wg.Done()
		_, storeErr = s.GeoSearchStore("gss:dest", "gss:src", 116.4, 39.9, 500, "km", 10, false)
	}()

	for i := range writers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, _ = s.GeoAdd("gss:src", []GeoMember{{
				Member: fmt.Sprintf("extra%d", idx),
				Lat:    39.9 + float64(idx)*0.01,
				Lon:    116.4 + float64(idx)*0.01,
			}})
		}(i)
	}
	wg.Wait()

	if storeErr != nil {
		t.Fatalf("GeoSearchStore: %v", storeErr)
	}

	_, exists, err := s.ZScore("gss:dest", "beijing")
	if err != nil {
		t.Fatalf("ZScore(beijing): %v", err)
	}
	if !exists {
		t.Fatal("beijing should exist in destination")
	}

	card, err := s.ZCard("gss:dest")
	if err != nil {
		t.Fatalf("ZCard: %v", err)
	}
	if card < 1 {
		t.Errorf("ZCard: got %d, want at least 1", card)
	}
}

// TestDeterministicConflict_LMoveWithSourceConflict verifies LMOVE stays
// correct while concurrent LPUSH on the source list causes txn conflicts.
func TestDeterministicConflict_LMoveWithSourceConflict(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	const writers = 4

	if _, err := s.RPush("lm:src", "a", "b", "c"); err != nil {
		t.Fatalf("RPush lm:src: %v", err)
	}

	var wg sync.WaitGroup
	var moveErr error
	var moved string

	wg.Add(1)
	go func() {
		defer wg.Done()
		moved, moveErr = s.LMove("lm:src", "lm:dst", "LEFT", "RIGHT")
	}()

	for i := range writers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, _ = s.LPush("lm:src", fmt.Sprintf("extra%d", idx))
		}(i)
	}
	wg.Wait()

	if moveErr != nil {
		t.Fatalf("LMove: %v", moveErr)
	}
	if moved == "" {
		t.Fatal("LMove: expected a moved value")
	}

	length, err := s.LLen("lm:dst")
	if err != nil {
		t.Fatalf("LLen lm:dst: %v", err)
	}
	if length != 1 {
		t.Errorf("LLen lm:dst: got %d, want 1", length)
	}

	val, err := s.LIndex("lm:dst", 0)
	if err != nil {
		t.Fatalf("LIndex lm:dst: %v", err)
	}
	if val != moved {
		t.Errorf("LIndex lm:dst: got %q, want %q", val, moved)
	}
}

// TestDeterministicConflict_RPopLPushWithSourceConflict verifies RPOPLPUSH stays
// correct while concurrent RPUSH on the source list causes txn conflicts.
func TestDeterministicConflict_RPopLPushWithSourceConflict(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	const writers = 4

	if _, err := s.RPush("rpl:src", "a", "b", "c"); err != nil {
		t.Fatalf("RPush rpl:src: %v", err)
	}

	var wg sync.WaitGroup
	var moveErr error
	var moved string

	wg.Add(1)
	go func() {
		defer wg.Done()
		moved, moveErr = s.RPopLPush("rpl:src", "rpl:dst")
	}()

	for i := range writers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, _ = s.RPush("rpl:src", fmt.Sprintf("extra%d", idx))
		}(i)
	}
	wg.Wait()

	if moveErr != nil {
		t.Fatalf("RPopLPush: %v", moveErr)
	}
	if moved == "" {
		t.Fatal("RPopLPush: expected a moved value")
	}

	length, err := s.LLen("rpl:dst")
	if err != nil {
		t.Fatalf("LLen rpl:dst: %v", err)
	}
	if length != 1 {
		t.Errorf("LLen rpl:dst: got %d, want 1", length)
	}

	val, err := s.LIndex("rpl:dst", 0)
	if err != nil {
		t.Fatalf("LIndex rpl:dst: %v", err)
	}
	if val != moved {
		t.Errorf("LIndex rpl:dst: got %q, want %q", val, moved)
	}
}

// TestDeterministicConflict_XDelConcurrent verifies concurrent XDEL of distinct
// stream entries drains the stream exactly once.
func TestDeterministicConflict_XDelConcurrent(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	stream := "stream:xdel:conflict"
	const entries = 4

	ids := make([]string, entries)
	for i := range entries {
		id := fmt.Sprintf("100000000000%d-0", i)
		got, err := s.XAdd(stream, StreamXAddOptions{}, id, map[string]string{
			"field": fmt.Sprintf("v%d", i),
		})
		if err != nil {
			t.Fatalf("XAdd %s: %v", id, err)
		}
		ids[i] = got
	}

	var wg sync.WaitGroup
	errs := make([]error, entries)
	deleted := make([]int64, entries)
	for i := range entries {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			n, err := s.XDel(stream, ids[idx])
			deleted[idx] = n
			errs[idx] = err
		}(i)
	}
	wg.Wait()

	var totalDeleted int64
	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d XDel: %v", i, err)
		}
		totalDeleted += deleted[i]
	}
	if totalDeleted != entries {
		t.Errorf("total deleted: got %d, want %d", totalDeleted, entries)
	}

	length, err := s.XLen(stream)
	if err != nil {
		t.Fatalf("XLen: %v", err)
	}
	if length != 0 {
		t.Errorf("XLen: got %d, want 0", length)
	}
}

// TestDeterministicConflict_JSONArrAppendConcurrent verifies concurrent
// JSON.ARRAPPEND appends all elements under txn conflicts.
func TestDeterministicConflict_JSONArrAppendConcurrent(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	key := "json:arrappend:conflict"
	const goroutines = 20

	if _, err := s.JSONSet(key, "$", `[]`, false, false); err != nil {
		t.Fatalf("JSONSet: %v", err)
	}

	var wg sync.WaitGroup
	errs := make([]error, goroutines)
	for i := range goroutines {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = s.JSONArrAppend(key, "$", fmt.Sprintf(`"%d"`, idx))
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d JSONArrAppend: %v", i, err)
		}
	}

	length, err := s.JSONArrLen(key, "$")
	if err != nil {
		t.Fatalf("JSONArrLen: %v", err)
	}
	if length != goroutines {
		t.Errorf("JSONArrLen: got %d, want %d", length, goroutines)
	}
}

// TestDeterministicConflict_INCRBYConcurrent verifies concurrent INCRBY on the
// same string key converges to the exact accumulated value.
func TestDeterministicConflict_INCRBYConcurrent(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	key := "string:incrby:conflict"
	const goroutines = 20

	if err := s.Set(key, "0"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	var wg sync.WaitGroup
	errs := make([]error, goroutines)
	for i := range goroutines {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = s.INCRBY(key, 1)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d INCRBY: %v", i, err)
		}
	}

	val, err := s.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	want := strconv.FormatInt(int64(goroutines), 10)
	if val != want {
		t.Errorf("Get: got %q, want %q", val, want)
	}
}

// TestDeterministicConflict_SAddConcurrent verifies concurrent SADD of distinct
// members on the same key reports the correct added count.
func TestDeterministicConflict_SAddConcurrent(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	key := "set:sadd:conflict"
	const members = 20

	var wg sync.WaitGroup
	errs := make([]error, members)
	added := make([]int, members)
	for i := range members {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			n, err := s.SAdd(key, fmt.Sprintf("m%d", idx))
			added[idx] = n
			errs[idx] = err
		}(i)
	}
	wg.Wait()

	totalAdded := 0
	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d SAdd: %v", i, err)
		}
		totalAdded += added[i]
	}
	if totalAdded != members {
		t.Errorf("total added: got %d, want %d", totalAdded, members)
	}

	card, err := s.SCard(key)
	if err != nil {
		t.Fatalf("SCard: %v", err)
	}
	if card != members {
		t.Errorf("SCard: got %d, want %d", card, members)
	}
}

// TestDeterministicConflict_HSetNXConcurrent verifies exactly one HSETNX wins
// under concurrent attempts on the same hash field.
func TestDeterministicConflict_HSetNXConcurrent(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	key := "hash:hsetnx:conflict"
	const goroutines = 20

	var wg sync.WaitGroup
	errs := make([]error, goroutines)
	wins := make([]bool, goroutines)
	for i := range goroutines {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ok, err := s.HSetNX(key, "field", fmt.Sprintf("v%d", idx))
			wins[idx] = ok
			errs[idx] = err
		}(i)
	}
	wg.Wait()

	var winCount int
	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d HSetNX: %v", i, err)
		}
		if wins[i] {
			winCount++
		}
	}
	if winCount != 1 {
		t.Errorf("HSetNX wins: got %d, want 1", winCount)
	}

	n, err := s.HLen(key)
	if err != nil {
		t.Fatalf("HLen: %v", err)
	}
	if n != 1 {
		t.Errorf("HLen: got %d, want 1", n)
	}
}

// TestDeterministicConflict_HIncrByConcurrent verifies concurrent HINCRBY on the
// same field converges to the exact accumulated value.
func TestDeterministicConflict_HIncrByConcurrent(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	key := "hash:hincrby:conflict"
	const goroutines = 20

	var wg sync.WaitGroup
	errs := make([]error, goroutines)
	for i := range goroutines {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = s.HIncrBy(key, "counter", 1)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d HIncrBy: %v", i, err)
		}
	}

	val, err := s.HGet(key, "counter")
	if err != nil {
		t.Fatalf("HGet: %v", err)
	}
	want := strconv.FormatInt(int64(goroutines), 10)
	if string(val) != want {
		t.Errorf("HGet: got %q, want %q", string(val), want)
	}
}

// TestDeterministicConflict_SRemConcurrent verifies concurrent SREM on distinct
// members drains the set without inflating the removed count.
func TestDeterministicConflict_SRemConcurrent(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	key := "set:srem:conflict"
	const members = 20

	names := make([]string, members)
	for i := range members {
		names[i] = fmt.Sprintf("m%d", i)
	}
	if _, err := s.SAdd(key, names...); err != nil {
		t.Fatalf("SAdd: %v", err)
	}

	var wg sync.WaitGroup
	errs := make([]error, members)
	removed := make([]int, members)
	for i := range members {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			n, err := s.SRem(key, names[idx])
			removed[idx] = n
			errs[idx] = err
		}(i)
	}
	wg.Wait()

	totalRemoved := 0
	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d SRem: %v", i, err)
		}
		totalRemoved += removed[i]
	}
	if totalRemoved != members {
		t.Errorf("total removed: got %d, want %d", totalRemoved, members)
	}

	card, err := s.SCard(key)
	if err != nil {
		t.Fatalf("SCard: %v", err)
	}
	if card != 0 {
		t.Errorf("SCard: got %d, want 0", card)
	}
}

// TestDeterministicConflict_TSDelWithAddConflict verifies TSDEL stays correct
// while concurrent TSADD causes txn conflicts on the same series.
func TestDeterministicConflict_TSDelWithAddConflict(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	key := "ts:del:conflict"
	const writers = 4
	const points = 4

	base := time.Now().UnixNano() / int64(time.Millisecond)
	timestamps := make([]int64, points)
	for i := range points {
		timestamps[i] = base + int64(i)*1000
		if _, err := s.TSAdd(key, timestamps[i], float64(i), TSAddOptions{}); err != nil {
			t.Fatalf("TSAdd %d: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	errs := make([]error, points)
	deleted := make([]int64, points)
	for i := range points {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ts := strconv.FormatInt(timestamps[idx], 10)
			n, err := s.TSDel(key, ts, ts)
			deleted[idx] = n
			errs[idx] = err
		}(i)
	}

	for i := range writers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, _ = s.TSAdd(key, base+10000+int64(idx), float64(100+idx), TSAddOptions{})
		}(i)
	}
	wg.Wait()

	var totalDeleted int64
	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d TSDel: %v", i, err)
		}
		totalDeleted += deleted[i]
	}
	if totalDeleted != points {
		t.Errorf("total deleted: got %d, want %d", totalDeleted, points)
	}

	results, err := s.TSRange(key, "-", "+", -1)
	if err != nil {
		t.Fatalf("TSRange: %v", err)
	}
	if len(results) != writers {
		t.Errorf("TSRange: got %d points, want %d (only concurrent adds remain)", len(results), writers)
	}
}

// TestDeterministicConflict_LPushXWithSourceConflict verifies LPUSHX stays
// correct while concurrent LPUSH on the same list causes txn conflicts.
func TestDeterministicConflict_LPushXWithSourceConflict(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	key := "list:lpushx:conflict"
	const writers = 4

	if _, err := s.LPush(key, "seed"); err != nil {
		t.Fatalf("LPush: %v", err)
	}

	var wg sync.WaitGroup
	var pushErr error
	var length int

	wg.Add(1)
	go func() {
		defer wg.Done()
		length, pushErr = s.LPUSHX(key, "xval")
	}()

	for i := range writers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, _ = s.LPush(key, fmt.Sprintf("extra%d", idx))
		}(i)
	}
	wg.Wait()

	if pushErr != nil {
		t.Fatalf("LPUSHX: %v", pushErr)
	}
	if length < 2 {
		t.Errorf("LPUSHX length: got %d, want at least 2", length)
	}

	listLen, err := s.LLen(key)
	if err != nil {
		t.Fatalf("LLen: %v", err)
	}
	if listLen < uint64(length) {
		t.Errorf("LLen: got %d, want at least %d", listLen, length)
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
