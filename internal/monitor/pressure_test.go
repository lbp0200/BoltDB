package monitor

import (
	"context"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/zeebo/assert"
)

func TestNewPressureMonitor(t *testing.T) {
	t.Parallel()
	pm := NewPressureMonitor(nil, nil)
	assert.NotEqual(t, nil, pm)
	assert.Equal(t, 0, len(pm.Samples()))
}

func TestPressureMonitor_Latest_Empty(t *testing.T) {
	t.Parallel()
	pm := NewPressureMonitor(nil, nil)
	latest := pm.Latest()
	assert.Equal(t, 0, latest.Goroutines)
}

func TestPressureMonitor_Stopped_Initially(t *testing.T) {
	t.Parallel()
	pm := NewPressureMonitor(nil, nil)
	assert.False(t, pm.Stopped())
}

func TestPressureMonitor_Start_Stop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pm := NewPressureMonitor(nil, nil)
	pm.Start(ctx, 20*time.Millisecond)

	time.Sleep(60 * time.Millisecond)
	cancel()

	time.Sleep(50 * time.Millisecond)
	assert.True(t, pm.Stopped())
	assert.True(t, len(pm.Samples()) > 0)
}

func TestPressureMonitor_Start_AlreadyStopped(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	pm := NewPressureMonitor(nil, nil)
	pm.Start(ctx, 20*time.Millisecond)

	time.Sleep(50 * time.Millisecond)
	assert.True(t, pm.Stopped())
}

func TestPressureMonitor_Samples_Snapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pm := NewPressureMonitor(nil, nil)
	pm.Start(ctx, 20*time.Millisecond)

	time.Sleep(50 * time.Millisecond)
	cancel()

	time.Sleep(50 * time.Millisecond)
	s1 := pm.Samples()
	s2 := pm.Samples()
	assert.Equal(t, len(s1), len(s2))
}

func TestPressureMonitor_SetClusterHealth_Empty(t *testing.T) {
	t.Parallel()
	pm := NewPressureMonitor(nil, nil)
	pm.SetClusterHealth(3, 3, 0, false)
}

func TestPressureMonitor_SetClusterHealth(t *testing.T) {
	pm := NewPressureMonitor(nil, nil)

	pm.mu.Lock()
	pm.samples = append(pm.samples, PressureSample{})
	pm.mu.Unlock()

	pm.SetClusterHealth(5, 4, 2, true)
	latest := pm.Latest()
	assert.Equal(t, 5, latest.TotalSentinels)
	assert.Equal(t, 4, latest.AgreedSentinels)
	assert.Equal(t, int64(2), latest.LeaderChanges)
	assert.True(t, latest.ClusterFragmented)
}

func TestPressureMonitor_EnableTemporalAnalysis(t *testing.T) {
	t.Parallel()
	pm := NewPressureMonitor(nil, nil)
	pm.EnableTemporalAnalysis()
	assert.NotEqual(t, nil, pm.temporal)
}

func TestPressureMonitor_EnableTemporalAnalysis_Idempotent(t *testing.T) {
	t.Parallel()
	pm := NewPressureMonitor(nil, nil)
	pm.EnableTemporalAnalysis()
	pm.EnableTemporalAnalysis()
	assert.NotEqual(t, nil, pm.temporal)
}

func TestPressureMonitor_BasinAnalysis_Empty(t *testing.T) {
	t.Parallel()
	pm := NewPressureMonitor(nil, nil)
	info := pm.BasinAnalysis()
	assert.Equal(t, BasinUnknown, info.CurrentBasin)
}

func TestPressureMonitor_TemporalAnalysis_NotEnabled(t *testing.T) {
	t.Parallel()
	pm := NewPressureMonitor(nil, nil)
	ta := pm.TemporalAnalysis()
	assert.Equal(t, TrajectoryInsufficientData, ta.Trajectory)
}

func TestFormatSnapshot_Basic(t *testing.T) {
	t.Parallel()
	now := time.Now()
	s := PressureSample{
		Timestamp:     now,
		Goroutines:    100,
		LastL0Score:   3.5,
		ActiveRetries: 2,
		TotalRetries:  10,
		WritesBlocked: 0,
		L0Rejected:    0,
		L0Delayed:     1,
	}
	out := FormatSnapshot(s)
	assert.True(t, strings.Contains(out, "go=100"))
	assert.True(t, strings.Contains(out, "L0=3.5"))
	assert.True(t, strings.Contains(out, "ar=2"))
	assert.True(t, strings.Contains(out, "pressure=3"))
}

func TestFormatSnapshot_WithReplication(t *testing.T) {
	t.Parallel()
	now := time.Now()
	s := PressureSample{
		Timestamp:       now,
		Goroutines:      100,
		LastL0Score:     2.0,
		MasterOffset:    5000,
		SlaveOffset:     4900,
		BacklogSize:     65536,
		ReconnectCount:  2,
		ConnectedSlaves: 1,
	}
	out := FormatSnapshot(s)
	assert.True(t, strings.Contains(out, "mo=5000"))
	assert.True(t, strings.Contains(out, "so=4900"))
	assert.True(t, strings.Contains(out, "recon=2"))
	assert.True(t, strings.Contains(out, "slaves=1"))
}

func TestFormatSnapshot_WithSentinels(t *testing.T) {
	t.Parallel()
	now := time.Now()
	s := PressureSample{
		Timestamp:         now,
		Goroutines:        100,
		LastL0Score:       1.0,
		TotalSentinels:    3,
		AgreedSentinels:   3,
		LeaderChanges:     1,
		ClusterFragmented: false,
	}
	out := FormatSnapshot(s)
	assert.True(t, strings.Contains(out, "sentinels=3/3"))
	assert.True(t, strings.Contains(out, "lc=1"))
	assert.True(t, strings.Contains(out, "frag=false"))
}

func TestFormatSnapshot_Empty(t *testing.T) {
	t.Parallel()
	s := PressureSample{}
	out := FormatSnapshot(s)
	assert.True(t, len(out) > 0)
}

func TestMaxHeap_Empty(t *testing.T) {
	t.Parallel()
	assert.Equal(t, uint64(0), MaxHeap(nil))
}

func TestMaxHeap_Single(t *testing.T) {
	t.Parallel()
	samples := []PressureSample{
		{Mem: runtime.MemStats{HeapInuse: 1000}},
	}
	assert.Equal(t, uint64(1000), MaxHeap(samples))
}

func TestMaxHeap_Multiple(t *testing.T) {
	t.Parallel()
	samples := []PressureSample{
		{Mem: runtime.MemStats{HeapInuse: 1000}},
		{Mem: runtime.MemStats{HeapInuse: 5000}},
		{Mem: runtime.MemStats{HeapInuse: 2000}},
	}
	assert.Equal(t, uint64(5000), MaxHeap(samples))
}

func TestPressureMonitor_LogSummary_Empty(t *testing.T) {
	t.Parallel()
	pm := NewPressureMonitor(nil, nil)
	pm.LogSummary(t)
}

func TestPressureMonitor_LogSummary_WithSamples(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pm := NewPressureMonitor(nil, nil)
	pm.interval = 20 * time.Millisecond
	pm.Start(ctx, 20*time.Millisecond)

	time.Sleep(60 * time.Millisecond)
	cancel()

	time.Sleep(50 * time.Millisecond)
	pm.LogSummary(t)
}

func TestPressureMonitor_HealthScore(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pm := NewPressureMonitor(nil, nil)
	pm.Start(ctx, 20*time.Millisecond)

	time.Sleep(60 * time.Millisecond)
	cancel()

	time.Sleep(50 * time.Millisecond)
	hs := pm.HealthScore(runtime.NumGoroutine())
	assert.True(t, hs.Overall >= 0.0)
	assert.True(t, hs.Overall <= 1.0)
}

func TestPressureMonitor_HealthScore_Empty(t *testing.T) {
	t.Parallel()
	pm := NewPressureMonitor(nil, nil)
	hs := pm.HealthScore(0)
	assert.Equal(t, 0.0, hs.Overall)
}

func TestPressureMonitor_SetJSONLPath(t *testing.T) {
	tmpFile := t.TempDir() + "/test.jsonl"
	pm := NewPressureMonitor(nil, nil)
	err := pm.SetJSONLPath(tmpFile)
	assert.NoError(t, err)
	assert.NotEqual(t, nil, pm.jsonlFile)

	info, err := os.Stat(tmpFile)
	assert.NoError(t, err)
	assert.True(t, info.Size() == 0)
}

func TestPressureMonitor_SetJSONLPath_InvalidPath(t *testing.T) {
	pm := NewPressureMonitor(nil, nil)
	err := pm.SetJSONLPath("/nonexistent/dir/test.jsonl")
	assert.Error(t, err)
}

func TestPressureMonitor_Start_WithJSONL(t *testing.T) {
	tmpFile := t.TempDir() + "/test.jsonl"
	ctx, cancel := context.WithCancel(context.Background())
	pm := NewPressureMonitor(nil, nil)
	err := pm.SetJSONLPath(tmpFile)
	assert.NoError(t, err)

	pm.Start(ctx, 20*time.Millisecond)
	time.Sleep(60 * time.Millisecond)
	cancel()
	time.Sleep(50 * time.Millisecond)

	assert.True(t, pm.Stopped())
	info, err := os.Stat(tmpFile)
	assert.NoError(t, err)
	assert.True(t, info.Size() > 0)
}
