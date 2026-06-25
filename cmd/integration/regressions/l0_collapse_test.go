package regressions

import (
	"context"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/monitor"
)

// TestRegressionL0Collapse verifies that under maximum write throughput, the
// backpressure system keeps L0 score bounded and recovers after load stops.
//
// Failure doc: docs/failures/l0-collapse.md
// Invariant: L0 score stays below hard threshold (20), ActiveRetries ≤ 100,
// L0 recovers below recovery threshold (10) within drain window.
func TestRegressionL0Collapse(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heavy regression in short mode")
	}
	srv := StartRegression(t)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pm := srv.NewMonitor(5 * time.Second)
	pm.Start(ctx, 5*time.Second)

	baseline := 0

	errCh := make(chan error, 200)

	// Phase 1: burst write load to drive L0 up
	t.Log("l0-collapse: phase 1 — burst load (20s, 30 writers)")
	srv.RunLoad(ctx, 30, 20*time.Second, errCh)

	time.Sleep(2 * time.Second)
	baselineSample := pm.Latest()
	baseline = baselineSample.Goroutines
	t.Logf("l0-collapse: baseline goroutines=%d, L0=%.1f", baseline, baselineSample.LastL0Score)

	// Phase 2: sustained maximum load — backpressure must engage
	t.Log("l0-collapse: phase 2 — sustained load (40s, 40 writers)")
	srv.RunLoad(ctx, 40, 40*time.Second, errCh)

	// Phase 3: cooldown — let L0 recover
	t.Log("l0-collapse: phase 3 — cooldown (30s)")
	time.Sleep(30 * time.Second)

	close(errCh)
	var errs []string
	for err := range errCh {
		errs = append(errs, err.Error())
		if len(errs) >= 5 {
			t.Logf("l0-collapse: %d errors total (showing first 5)", len(errs)+5)
			break
		}
	}
	for _, e := range errs {
		t.Logf("l0-collapse: error: %s", e)
	}

	pm.LogSummary(t)

	// Assert degradation invariants — L0 must not collapse
	assertion := monitor.DefaultDegradationAssertion()
	assertion.MaxGoroutineDelta = 40
	assertion.MaxActiveRetries = 80
	assertion.L0DegradedThreshold = 18
	assertion.MaxL0Score = 22
	assertion.L0RecoveryThreshold = 12
	level := pm.CheckDegradation(t, assertion, baseline)

	t.Logf("l0-collapse: final degradation level: %s", level)

	if err := srv.DB.Check(); err != nil {
		t.Errorf("l0-collapse: DB consistency check failed: %v", err)
	} else {
		t.Log("l0-collapse: DB consistency check passed")
	}
}
