package store

import (
	"testing"
	"time"

	"github.com/zeebo/assert"
)

// TestDegradationGate_RetryMetrics 验证操作后 retry metrics 归零
func TestDegradationGate_RetryMetrics(t *testing.T) {
	s := setupTestStore(t)

	for i := 0; i < 100; i++ {
		assert.NoError(t, s.Set("k", "v"))
	}

	metrics := s.GetRetryMetrics()
	assert.Equal(t, int32(0), metrics.ActiveRetries)
	assert.Equal(t, int32(0), metrics.L0Rejected)
	assert.Equal(t, int32(0), metrics.L0Delayed)
}

// TestDegradationGate_L0Score 验证大量写入后 L0 可管理
func TestDegradationGate_L0Score(t *testing.T) {
	s := setupTestStore(t)

	for i := 0; i < 5000; i++ {
		assert.NoError(t, s.Set("lg:k", "v"))
	}

	time.Sleep(500 * time.Millisecond)

	metrics := s.GetRetryMetrics()
	if metrics.L0Rejected > 0 {
		t.Errorf("L0Rejected: got %d, want 0", metrics.L0Rejected)
	}
}

// TestDegradationGate_BackpressureRecovery 验证背压后恢复
func TestDegradationGate_BackpressureRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping backpressure recovery test in short mode")
	}
	s := setupTestStore(t)

	for i := 0; i < 2000; i++ {
		assert.NoError(t, s.Set("bp:k", "v"))
	}

	metrics := s.GetRetryMetrics()
	if metrics.L0Rejected > 0 {
		t.Errorf("L0Rejected: got %d, want 0", metrics.L0Rejected)
	}
	if metrics.L0Delayed > 0 {
		t.Logf("L0Delayed: %d (acceptable if < 10)", metrics.L0Delayed)
	}
}
