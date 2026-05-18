package store

import (
	"context"
	"testing"
)

// setupTestStore creates a new store in a temp directory for each test.
// The store is automatically closed and cleaned up via t.Cleanup.
func setupTestStore(t *testing.T) *BotreonStore {
	t.Helper()
	dir := t.TempDir()
	store, err := NewBadgerStore(dir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), CloseTimeout)
		defer cancel()
		done := make(chan error, 1)
		go func() {
			done <- store.Close()
		}()
		select {
		case <-done:
		case <-ctx.Done():
		}
	})
	return store
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
