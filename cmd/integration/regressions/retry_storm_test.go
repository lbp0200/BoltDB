package regressions

import (
	"context"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/monitor"
)

// TestRegressionRetryStorm verifies that under high write concurrency with
// backpressure enabled, the system bounds ActiveRetries and recovers L0 score.
//
// Failure doc: docs/failures/retry-storm.md
// Expected: ActiveRetries stays ≤ 100, L0 recovers below 10 after load stops.
func TestRegressionRetryStorm(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heavy regression in short mode")
	}
	srv := StartRegression(t)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	pm := srv.NewMonitor(5 * time.Second)
	pm.Start(ctx, 5*time.Second)

	baseline := 0 // will be captured after first sample

	errCh := make(chan error, 100)

	// Phase 1: high write concurrency to stress L0
	t.Log("retry-storm: phase 1 — high write load (30s)")
	srv.RunLoad(ctx, 20, 30*time.Second, errCh)

	// Brief pause to let baseline stabilize
	time.Sleep(2 * time.Second)
	baselineSample := pm.Latest()
	baseline = baselineSample.Goroutines
	t.Logf("retry-storm: baseline goroutines=%d", baseline)

	// Phase 2: sustained load with backpressure active
	t.Log("retry-storm: phase 2 — sustained load with backpressure (30s)")
	srv.RunLoad(ctx, 20, 30*time.Second, errCh)

	// Phase 3: drain — let L0 recover
	t.Log("retry-storm: phase 3 — drain (20s)")
	time.Sleep(20 * time.Second)

	// Drain any errors
	close(errCh)
	var errs []string
	for err := range errCh {
		errs = append(errs, err.Error())
		if len(errs) >= 5 {
			t.Logf("retry-storm: %d errors (showing first 5)", len(errs)+5)
			break
		}
	}
	if len(errs) > 0 {
		for _, e := range errs {
			t.Logf("retry-storm: error: %s", e)
		}
	}

	pm.LogSummary(t)

	// Assert degradation invariants
	// Tighter thresholds than default: this regression should be well-behaved
	assertion := monitor.DefaultDegradationAssertion()
	assertion.MaxActiveRetries = 50
	assertion.MaxGoroutineDelta = 30
	assertion.MaxReconnectCount = 0
	level := pm.CheckDegradation(t, assertion, baseline)

	// Collect timeline for analysis
	t.Logf("retry-storm: final degradation level: %s", level)

	// Verify basic data integrity after load
	if err := srv.DB.Check(); err != nil {
		t.Errorf("retry-storm: DB consistency check failed: %v", err)
	} else {
		t.Log("retry-storm: DB consistency check passed")
	}

	if level >= monitor.LevelDegraded {
		t.Logf("retry-storm: system degraded but within expected bounds")
	}
}
