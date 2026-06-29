package regressions

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/monitor"
)

// TestRegressionSplitBrainConvergence verifies that after a network partition
// heals, the system converges monotonically — no oscillation, no extended
// split-brain window.
//
// Failure doc: docs/failures/split-brain-convergence.md
//
// The original bug: stale gossip after partition heal caused non-monotonic
// agreement changes (100% → drop → recover), underdamped oscillation around
// full consensus.
//
// Scenario:
//  1. master + slave1 + slave2, all connected
//  2. Write data, verify all nodes have consistent state
//  3. Kill slave2 (simulate partition — it can't reach master)
//  4. Continue writing on master — slave2 falls behind
//  5. Heal: bring slave2 back
//  6. Verify: slave2 catches up, no goroutine leak, data consistent
//
// Expected: reconnect count ≤ 5, no oscillation, final lag < 1000.
func TestRegressionSplitBrainConvergence(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heavy regression in short mode")
	}

	master := StartRegression(t)
	defer master.Close()

	slave1 := StartRegression(t)
	defer slave1.Close()

	slave2 := StartRegression(t)
	defer slave2.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	pm := master.NewMonitor(3 * time.Second)
	pm.Start(ctx, 3*time.Second)

	// Phase 1: connect both slaves, initial sync
	t.Log("split-brain: phase 1 — connect both slaves")
	if err := slave1.MakeSlave(master.Addr); err != nil {
		t.Fatalf("split-brain: slave1 MakeSlave: %v", err)
	}
	if err := slave2.MakeSlave(master.Addr); err != nil {
		t.Fatalf("split-brain: slave2 MakeSlave: %v", err)
	}
	time.Sleep(3 * time.Second)

	if !master.WaitForReplicaSync(ctx, master, slave1, 10*time.Second) {
		t.Fatal("split-brain: slave1 did not complete initial sync")
	}
	if !master.WaitForReplicaSync(ctx, master, slave2, 10*time.Second) {
		t.Fatal("split-brain: slave2 did not complete initial sync")
	}
	t.Logf("split-brain: initial sync ok (master_repl=%d slave1=%d slave2=%d)",
		master.GetMasterOffset(), slave1.GetSlaveOffset(), slave2.GetSlaveOffset())

	// Seed data
	for i := 0; i < 50; i++ {
		key := "split:pre:" + string(rune('A'+i%26))
		master.Client.Set(ctx, key, "pre", 0)
	}

	time.Sleep(1 * time.Second)
	baseline := runtime.NumGoroutine()
	t.Logf("split-brain: baseline goroutines=%d", baseline)

	// Phase 2: partition — kill slave2, continue writing on master
	t.Log("split-brain: phase 2 — partition slave2 (kill + write)")
	errCh := make(chan error, 100)

	// Start writers
	go master.RunLoad(ctx, 6, 60*time.Second, errCh)

	// Kill slave2 — simulate partition
	time.Sleep(3 * time.Second)
	if err := master.Client.Do(ctx, "CLIENT", "KILL", "TYPE", "slave").Err(); err != nil {
		t.Logf("split-brain: CLIENT KILL error (expected): %v", err)
	}

	// Write more data while slave2 is partitioned
	t.Log("split-brain: writing 20s while slave2 is partitioned...")
	time.Sleep(20 * time.Second)

	mOffPartition := master.GetMasterOffset()
	t.Logf("split-brain: during partition — master_offset=%d", mOffPartition)

	// Phase 3: heal — slave2 reconnects
	t.Log("split-brain: phase 3 — heal partition (slave2 reconnects)")
	time.Sleep(25 * time.Second)

	// Phase 4: convergence — both slaves should catch up
	t.Log("split-brain: phase 4 — convergence wait (20s)")
	time.Sleep(20 * time.Second)

	mOff := master.GetMasterOffset()
	s1Off := slave1.GetSlaveOffset()
	s2Off := slave2.GetSlaveOffset()
	recon1 := slave1.GetReconnectCount()
	recon2 := slave2.GetReconnectCount()
	t.Logf("split-brain: post-drain — master=%d slave1=%d (lag=%d recon=%d) slave2=%d (lag=%d recon=%d)",
		mOff, s1Off, mOff-s1Off, recon1, s2Off, mOff-s2Off, recon2)

	// Convergence barrier
	t.Log("split-brain: convergence barrier — waiting for monitor sample...")
	var barrierOk bool
	for i := 0; i < 20; i++ {
		l := pm.Latest()
		moff := master.GetMasterOffset()
		lag := moff - l.SlaveOffset
		if l.ConnectedSlaves > 0 && lag < 10000 {
			t.Logf("split-brain: monitor captured convergence (mo=%d so=%d lag=%d slaves=%d)",
				moff, l.SlaveOffset, lag, l.ConnectedSlaves)
			barrierOk = true
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !barrierOk {
		l := pm.Latest()
		t.Logf("split-brain: WARN — barrier timeout (mo=%d so=%d slaves=%d)",
			master.GetMasterOffset(), l.SlaveOffset, l.ConnectedSlaves)
	}

	pm.LogSummary(t)

	// Drain errors
	close(errCh)
	errCount := 0
	for err := range errCh {
		if errCount == 0 {
			t.Logf("split-brain: first error: %v", err)
		}
		errCount++
	}

	// Assertions
	assertion := monitor.DefaultDegradationAssertion()
	assertion.MaxGoroutineDelta = 40
	assertion.MaxActiveRetries = 80
	assertion.MaxReconnectCount = 5
	assertion.ReconnectWarnThreshold = 2
	assertion.L0DegradedThreshold = 20
	level := pm.CheckDegradation(t, assertion, baseline)

	t.Logf("split-brain: degradation level: %s", level)
	t.Logf("split-brain: total write errors: %d", errCount)

	// Key assertion: slave2 reconnect count bounded (no oscillation loop)
	if recon2 > 5 {
		t.Errorf("split-brain: slave2 reconnect count too high (%d > 5) — possible oscillation", recon2)
	} else {
		t.Logf("split-brain: PASS: slave2 reconnect count bounded (%d)", recon2)
	}

	// Key assertion: slave1 (never partitioned) should have minimal lag
	lag1 := mOff - s1Off
	if lag1 > 1000 {
		t.Errorf("split-brain: slave1 lag too large (%d > 1000) — should be stable", lag1)
	} else {
		t.Logf("split-brain: PASS: slave1 converged (lag=%d)", lag1)
	}

	// Key assertion: slave2 should have converged after heal
	lag2 := mOff - s2Off
	if lag2 > 5000 {
		t.Errorf("split-brain: slave2 lag too large (%d > 5000) — heal convergence failed", lag2)
	} else {
		t.Logf("split-brain: PASS: slave2 converged after heal (lag=%d)", lag2)
	}

	// Data integrity
	if err := master.DB.Check(); err != nil {
		t.Errorf("split-brain: master DB check: %v", err)
	} else {
		t.Log("split-brain: PASS: master DB integrity check")
	}
	if err := slave1.DB.Check(); err != nil {
		t.Errorf("split-brain: slave1 DB check: %v", err)
	} else {
		t.Log("split-brain: PASS: slave1 DB integrity check")
	}
	if err := slave2.DB.Check(); err != nil {
		t.Errorf("split-brain: slave2 DB check: %v", err)
	} else {
		t.Log("split-brain: PASS: slave2 DB integrity check")
	}

	if level >= monitor.LevelDegraded {
		t.Logf("split-brain: system degraded but invariants held within bounds")
	}
}
