package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

// sharedStore is a single BadgerDB instance reused across all tests.
// This reduces test suite runtime from 180s+ to ~30s by avoiding
// goroutine accumulation from creating 279+ separate Badger instances.
var sharedStore *BotreonStore

// TestMain sets up the shared store before tests and closes it after.
func TestMain(m *testing.M) {
	// Use a dedicated temp directory for the test suite
	dbPath := fmt.Sprintf("%s/boltdb_shared_test", os.TempDir())
	// Clean up any previous run
	os.RemoveAll(dbPath)

	var err error
	sharedStore, err = NewBadgerStore(dbPath)
	if err != nil {
		fmt.Printf("failed to create shared store: %v\n", err)
		os.Exit(1)
	}

	// Run tests
	code := m.Run()

	// Clean up: attempt non-blocking close with timeout
	// The publisher goroutine can block indefinitely; we prioritize test
	// completion over graceful shutdown since temp dirs are auto-cleaned.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- sharedStore.Close()
	}()
	select {
	case err := <-done:
		if err != nil {
			fmt.Printf("failed to close shared store: %v\n", err)
		}
	case <-ctx.Done():
		// Close is blocked (known BadgerDB doWrites bug). Exit anyway.
	}
	os.RemoveAll(dbPath)

	os.Exit(code)
}

// setupTestStore returns the shared store after clearing all data.
// Each test gets a clean database state without creating a new instance.
// The returned store is the shared singleton; callers should NOT close it.
func setupTestStore(t *testing.T) *BotreonStore {
	t.Helper()
	if sharedStore == nil {
		t.Fatal("sharedStore not initialized - TestMain not running?")
	}
	// Clear all data synchronously before each test.
	// ClearAllData uses iterative deletion and must succeed for test isolation.
	if err := sharedStore.ClearAllData(); err != nil {
		t.Fatalf("ClearAllData failed: %v", err)
	}
	if sharedStore.readCache != nil {
		sharedStore.readCache.Clear()
	}

	return sharedStore
}

// mustSet is a helper that calls Set and fails the test on error
func mustSet(t *testing.T, store *BotreonStore, key, value string) {
	t.Helper()
	if err := store.Set(key, value); err != nil {
		t.Fatalf("failed to set key %q: %v", key, err)
	}
}

// mustZAdd is a helper that calls ZAdd and fails the test on error
func mustZAdd(t *testing.T, store *BotreonStore, key string, members []ZSetMember) {
	t.Helper()
	if err := store.ZAdd(key, members); err != nil {
		t.Fatalf("failed to zadd to %q: %v", key, err)
	}
}

// mustLPush is a helper that calls LPush and fails the test on error
func mustLPush(t *testing.T, store *BotreonStore, key string, values ...string) int {
	t.Helper()
	n, err := store.LPush(key, values...)
	if err != nil {
		t.Fatalf("failed to lpush to %q: %v", key, err)
	}
	return n
}

// mustRPush is a helper that calls RPush and fails the test on error
func mustRPush(t *testing.T, store *BotreonStore, key string, values ...string) int {
	t.Helper()
	n, err := store.RPush(key, values...)
	if err != nil {
		t.Fatalf("failed to rpush to %q: %v", key, err)
	}
	return n
}

// mustHSet is a helper that calls HSet and fails the test on error
func mustHSet(t *testing.T, store *BotreonStore, key, field string, value interface{}) {
	t.Helper()
	if err := store.HSet(key, field, value); err != nil {
		t.Fatalf("failed to hset %q:%q: %v", key, field, err)
	}
}

// mustSAdd is a helper that calls SAdd and fails the test on error
func mustSAdd(t *testing.T, store *BotreonStore, key string, members ...string) int {
	t.Helper()
	n, err := store.SAdd(key, members...)
	if err != nil {
		t.Fatalf("failed to sadd to %q: %v", key, err)
	}
	return n
}
