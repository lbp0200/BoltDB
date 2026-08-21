package store

import (
	"testing"
)

// setupTestStore creates a new store in a temp directory for each test.
// The store is automatically closed and cleaned up via t.Cleanup.
// Uses CloseWithTimeout to prevent BadgerDB goroutine leak on test exit.
func setupTestStore(t *testing.T) *BotreonStore {
	t.Helper()
	dir := t.TempDir()
	store, err := NewBadgerStore(dir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	t.Cleanup(func() {
		// CloseWithTimeout prevents BadgerDB's doWrites drain bug
		// from hanging the test suite. A leaking Close() goroutine
		// blocks t.Cleanup from returning; CloseWithTimeout ensures
		// the goroutine is abandoned after CloseTimeout (2s) rather
		// than leaking into the next test.
		_ = store.CloseWithTimeout(CloseTimeout)
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

// TestExpireNonPositiveTTLDeletes verifies Redis semantics: EXPIRE/PEXPIRE with a
// non-positive TTL (0 or negative) must delete the key immediately instead of
// setting a far-future expiry (regression: uint64 conversion of a negative TTL
// produced a huge expiresAt, leaving the key alive forever).
func TestExpireNonPositiveTTLDeletes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		run  func(t *testing.T, s *BotreonStore, key string)
	}{
		{
			name: "Expire-0",
			run: func(t *testing.T, s *BotreonStore, key string) {
				if _, err := s.Expire(key, 0); err != nil {
					t.Fatalf("Expire 0: %v", err)
				}
			},
		},
		{
			name: "Expire-negative",
			run: func(t *testing.T, s *BotreonStore, key string) {
				if _, err := s.Expire(key, -5); err != nil {
					t.Fatalf("Expire -5: %v", err)
				}
			},
		},
		{
			name: "PExpire-0",
			run: func(t *testing.T, s *BotreonStore, key string) {
				if _, err := s.PExpire(key, 0); err != nil {
					t.Fatalf("PExpire 0: %v", err)
				}
			},
		},
		{
			name: "PExpire-negative",
			run: func(t *testing.T, s *BotreonStore, key string) {
				if _, err := s.PExpire(key, -5000); err != nil {
					t.Fatalf("PExpire -5000: %v", err)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := setupTestStore(t)
			key := "ttl_delete_" + tc.name
			if err := s.Set(key, "v"); err != nil {
				t.Fatalf("Set: %v", err)
			}
			tc.run(t, s, key)
			exists, err := s.Exists(key)
			if err != nil {
				t.Fatalf("Exists: %v", err)
			}
			if exists {
				t.Fatalf("%s: key still exists, Redis semantics require deletion", tc.name)
			}
		})
	}
}
