package regressions

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/monitor"
)

// TestRegressionDiskPressureDegradation verifies that under sustained write
// pressure (simulating slow disk via high concurrency), the backpressure
// system correctly triggers soft/hard thresholds and recovers after load.
//
// Expected: L0 stays bounded, no panic, writes resume after pressure drops.
func TestRegressionDiskPressureDegradation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heavy regression in short mode")
	}

	srv := StartRegression(t)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pm := srv.NewMonitor(2 * time.Second)
	pm.Start(ctx, 2*time.Second)

	time.Sleep(2 * time.Second)
	baseline := runtime.NumGoroutine()

	// Phase 1: ramp up — 20 writers for 20s
	t.Log("disk-pressure: phase 1 — ramp up (20 writers, 20s)")
	errCh := make(chan error, 200)
	srv.RunLoad(ctx, 20, 20*time.Second, errCh)

	s1 := pm.Latest()
	t.Logf("disk-pressure: after ramp — L0=%.1f go=%d heap=%.0fM",
		s1.LastL0Score, s1.Goroutines, float64(s1.Mem.HeapInuse)/1024/1024)

	// Phase 2: sustained pressure — 30 writers for 25s
	t.Log("disk-pressure: phase 2 — sustained (30 writers, 25s)")
	srv.RunLoad(ctx, 30, 25*time.Second, errCh)

	s2 := pm.Latest()
	t.Logf("disk-pressure: after sustained — L0=%.1f go=%d heap=%.0fM",
		s2.LastL0Score, s2.Goroutines, float64(s2.Mem.HeapInuse)/1024/1024)

	// Phase 3: drain — stop writes, let compaction catch up
	t.Log("disk-pressure: phase 3 — drain (15s)")
	time.Sleep(15 * time.Second)

	pm.LogSummary(t)

	// Drain errors
	close(errCh)
	errCount := 0
	for err := range errCh {
		if errCount == 0 {
			t.Logf("disk-pressure: first error: %v", err)
		}
		errCount++
	}

	assertion := monitor.DefaultDegradationAssertion()
	assertion.MaxGoroutineDelta = 60
	assertion.MaxActiveRetries = 100
	assertion.L0DegradedThreshold = 20
	assertion.MaxL0Score = 25
	level := pm.CheckDegradation(t, assertion, baseline)

	t.Logf("disk-pressure: degradation=%s errors=%d", level, errCount)

	// Key assertion: L0 must recover after drain
	recovered := srv.WaitForStableL0(ctx, pm, 15, 30*time.Second)
	if !recovered {
		t.Errorf("disk-pressure: L0 did not recover within 30s of drain")
	} else {
		t.Log("disk-pressure: PASS: L0 recovered after drain")
	}

	// Verify server is still functional
	srv.Client.Set(ctx, "disk:post-pressure", "ok", 0)
	val, err := srv.Client.Get(ctx, "disk:post-pressure").Result()
	if err != nil || val != "ok" {
		t.Errorf("disk-pressure: server not functional after pressure: %v", err)
	} else {
		t.Log("disk-pressure: PASS: server functional after pressure")
	}

	if err := srv.DB.Check(); err != nil {
		t.Errorf("disk-pressure: DB check: %v", err)
	} else {
		t.Log("disk-pressure: PASS: DB integrity check")
	}
}
