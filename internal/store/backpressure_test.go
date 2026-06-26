package store

import (
	"sync"
	"testing"
	"time"

	"github.com/zeebo/assert"
)

func TestWriteSlot_AcquireRelease(t *testing.T) {
	t.Parallel()
	ws := newWriteSlot(5)

	// Acquire should not block (slots available)
	ws.Acquire()
	ws.Acquire()
	assert.Equal(t, 2, len(ws.ch))

	ws.Release()
	assert.Equal(t, 1, len(ws.ch))

	ws.Release()
	assert.Equal(t, 0, len(ws.ch))
}

func TestWriteSlot_AcquireBlocksWhenFull(t *testing.T) {
	t.Parallel()
	ws := newWriteSlot(1) // max 1 concurrent

	ws.Acquire() // fills the slot

	// Second acquire should block
	acquired := make(chan struct{})
	go func() {
		ws.Acquire()
		close(acquired)
	}()

	select {
	case <-acquired:
		t.Fatal("Acquire should block when slot is full")
	case <-time.After(10 * time.Millisecond):
		// Expected — blocked
	}

	ws.Release() // free the slot

	select {
	case <-acquired:
		// Now acquired
		ws.Release() // balance
	case <-time.After(time.Second):
		t.Fatal("Acquire should unblock after Release")
	}
}

func TestWriteSlot_ReleaseNoBlock(t *testing.T) {
	t.Parallel()
	ws := newWriteSlot(3)

	// Release on empty slot should not panic
	ws.Release()
	ws.Release()

	// Acquire should still work
	ws.Acquire()
	ws.Release()
}

func TestWriteSlot_ConcurrentAcquireRelease(t *testing.T) {
	t.Parallel()
	ws := newWriteSlot(5)
	var wg sync.WaitGroup

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ws.Acquire()
			time.Sleep(time.Microsecond)
			ws.Release()
		}()
	}

	wg.Wait()
	// All goroutines should complete without deadlock
}

func TestWriteSlot_MaxCapacity(t *testing.T) {
	t.Parallel()
	ws := newWriteSlot(3)
	for i := 0; i < 3; i++ {
		ws.Acquire()
	}
	assert.Equal(t, 3, len(ws.ch))

	// Release all
	for i := 0; i < 3; i++ {
		ws.Release()
	}
	assert.Equal(t, 0, len(ws.ch))
}

func TestPreWriteCheck_NoBackpressure(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	// When backpressure slot is nil, preWriteCheck should pass through
	// We can't easily set it to nil after creation since the store
	// initializes it in NewBotreonStore. Instead, test with disabled config.
	cfg := s.GetBackpressureConfig()
	cfg.Enabled = false
	s.SetBackpressureConfig(cfg)

	delay, reject := s.preWriteCheck()
	assert.Equal(t, time.Duration(0), delay)
	assert.False(t, reject)
}

func TestPreWriteCheck_CleanDB(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	// With a clean BadgerDB, L0 score is 0. Both thresholds should be
	// above 0 (default: 8 and 20). preWriteCheck should allow the write.
	delay, reject := s.preWriteCheck()
	assert.Equal(t, time.Duration(0), delay)
	assert.False(t, reject)
}

func TestPreWriteCheck_WithCustomConfig(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	// Set thresholds that the clean DB score (0) will be below both
	cfg := s.GetBackpressureConfig()
	cfg.L0SoftThreshold = 100.0 // well above clean DB score
	cfg.L0HardThreshold = 200.0
	s.SetBackpressureConfig(cfg)

	delay, reject := s.preWriteCheck()
	assert.Equal(t, time.Duration(0), delay)
	assert.False(t, reject)
}

func TestDefaultBackpressureConfig(t *testing.T) {
	t.Parallel()
	cfg := DefaultBackpressureConfig()
	assert.True(t, cfg.Enabled)
	assert.Equal(t, 50, cfg.MaxConcurrentWrites)
	assert.Equal(t, 8.0, cfg.L0SoftThreshold)
	assert.Equal(t, 20.0, cfg.L0HardThreshold)
	assert.Equal(t, time.Second, cfg.MaxPreDelay)
}
