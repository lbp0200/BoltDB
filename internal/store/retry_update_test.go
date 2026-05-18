package store

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

func TestRetryUpdate_SuccessOnFirstTry(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	callCount := 0
	err := s.retryUpdate(func(txn *badger.Txn) error {
		callCount++
		return nil
	}, 3)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if callCount != 1 {
		t.Errorf("expected 1 call, got %d", callCount)
	}
}

func TestRetryUpdate_SuccessAfterConflict(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	callCount := 0
	err := s.retryUpdate(func(txn *badger.Txn) error {
		callCount++
		if callCount < 3 {
			return badger.ErrConflict
		}
		return nil
	}, 5)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if callCount != 3 {
		t.Errorf("expected 3 calls, got %d", callCount)
	}
}

func TestRetryUpdate_SuccessAfterWritesBlocked(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	callCount := 0
	err := s.retryUpdate(func(txn *badger.Txn) error {
		callCount++
		if callCount < 2 {
			return errors.New("Writes are blocked")
		}
		return nil
	}, 5)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if callCount != 2 {
		t.Errorf("expected 2 calls, got %d", callCount)
	}
}

func TestRetryUpdate_SuccessAfterWritesBlockedWithConflicts(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	callCount := 0
	err := s.retryUpdate(func(txn *badger.Txn) error {
		callCount++
		switch callCount {
		case 1:
			return badger.ErrConflict
		case 2:
			return errors.New("Writes are blocked")
		default:
			return nil
		}
	}, 5)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if callCount != 3 {
		t.Errorf("expected 3 calls, got %d", callCount)
	}
}

func TestRetryUpdate_ConflictExhaustRetries(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	maxRetries := 3
	callCount := 0
	err := s.retryUpdate(func(txn *badger.Txn) error {
		callCount++
		return badger.ErrConflict
	}, maxRetries)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if callCount != maxRetries {
		t.Errorf("expected %d calls, got %d", maxRetries, callCount)
	}
	if !strings.Contains(err.Error(), "max retries exhausted") {
		t.Errorf("expected 'max retries exhausted' in error, got: %v", err)
	}
}

func TestRetryUpdate_WritesBlockedExhaustRetries(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	maxRetries := 3
	callCount := 0
	err := s.retryUpdate(func(txn *badger.Txn) error {
		callCount++
		return errors.New("Writes are blocked")
	}, maxRetries)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if callCount != maxRetries {
		t.Errorf("expected %d calls, got %d", maxRetries, callCount)
	}
}

func TestRetryUpdate_NonRetryableError(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	callCount := 0
	sentinel := "some non-retryable error"
	err := s.retryUpdate(func(txn *badger.Txn) error {
		callCount++
		return errors.New(sentinel)
	}, 5)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if callCount != 1 {
		t.Errorf("expected 1 call, got %d", callCount)
	}
	if !strings.Contains(err.Error(), sentinel) {
		t.Errorf("expected sentinel error in message, got: %v", err)
	}
}

func TestRetryUpdate_NonRetryableAfterRetries(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	// Use "Writes are blocked" instead of ErrConflict because BadgerDB's
	// Update() internally retries on ErrConflict, inflating the call count.
	callCount := 0
	sentinel := "non-retryable after retries"
	err := s.retryUpdate(func(txn *badger.Txn) error {
		callCount++
		if callCount < 3 {
			return errors.New("Writes are blocked")
		}
		return errors.New(sentinel)
	}, 5)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if callCount != 3 {
		t.Errorf("expected 3 calls, got %d", callCount)
	}
	if !strings.Contains(err.Error(), sentinel) {
		t.Errorf("expected sentinel error in message, got: %v", err)
	}
}

func TestRetryUpdate_SingleRetry(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	err := s.retryUpdate(func(txn *badger.Txn) error {
		return badger.ErrConflict
	}, 1)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "max retries exhausted") {
		t.Errorf("expected 'max retries exhausted', got: %v", err)
	}
}

// TestRetryUpdate_BackoffBounds validates that conflict backoff does not exceed expected cap.
func TestRetryUpdate_ConflictBackoffCap(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	start := time.Now()
	_ = s.retryUpdate(func(txn *badger.Txn) error {
		return badger.ErrConflict
	}, 20)
	elapsed := time.Since(start)
	// 20 retries with cap at 50ms = 1+2+4+8+16+32+50*14 ≈ ~773ms max without jitter
	// With jitter up to 50%: ~1160ms. Giving 2x headroom for GC/CI noise.
	maxExpected := 2 * time.Second
	if elapsed > maxExpected {
		t.Errorf("conflict backoff exceeded max %v: took %v", maxExpected, elapsed)
	}
}

func TestRetryUpdate_ErrorWrap(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	inner := errors.New("Writes are blocked")
	err := s.retryUpdate(func(txn *badger.Txn) error {
		return inner
	}, 2)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, inner) {
		t.Errorf("expected error to wrap inner error: %v", err)
	}
}

func TestRetryUpdate_NoSideEffectOnSuccess(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	key := fmt.Sprintf("retry-success-%d", time.Now().UnixNano())
	err := s.retryUpdate(func(txn *badger.Txn) error {
		e := badger.NewEntry([]byte(key), []byte("value"))
		return txn.SetEntry(e)
	}, 3)
	if err != nil {
		t.Fatalf("Set via retryUpdate failed: %v", err)
	}
	var val []byte
	err = s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		return err
	})
	if err != nil {
		t.Fatalf("db.View failed: %v", err)
	}
	if string(val) != "value" {
		t.Errorf("got %q, want %q", string(val), "value")
	}
}
