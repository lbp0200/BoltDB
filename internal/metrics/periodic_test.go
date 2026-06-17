package metrics

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zeebo/assert"
)

func TestWriteGoroutineStack(t *testing.T) {
	t.Parallel()
	stack := WriteGoroutineStack()
	assert.True(t, len(stack) > 0)
	assert.True(t, strings.Contains(stack, "WriteGoroutineStack"))
}

func TestStartPeriodicSnapshot_CancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var wg sync.WaitGroup
	c := NewCollector()
	StartPeriodicSnapshot(ctx, c, 100*time.Millisecond, &wg)

	wg.Wait()
}

func TestStartPeriodicSnapshot_NormalOperation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	c := NewCollector()
	c.RetryMetricsFn = func() (int64, int64, int64, int64, int64, float64) {
		return 5, 20, 0, 1, 0, 2.0
	}

	StartPeriodicSnapshot(ctx, c, 50*time.Millisecond, &wg)

	time.Sleep(120 * time.Millisecond)
	cancel()

	wg.Wait()
}

func TestStartPeriodicSnapshot_MultipleTicks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	c := NewCollector()
	var tickCount atomic.Int64
	c.RetryMetricsFn = func() (int64, int64, int64, int64, int64, float64) {
		tickCount.Add(1)
		return tickCount.Load(), 0, 0, 0, 0, 0
	}

	StartPeriodicSnapshot(ctx, c, 30*time.Millisecond, &wg)

	time.Sleep(100 * time.Millisecond)
	cancel()

	wg.Wait()
	assert.True(t, tickCount.Load() > 0)
}
