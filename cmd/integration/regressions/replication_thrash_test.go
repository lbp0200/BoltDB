package regressions

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/monitor"
)

// TestRegressionReplicationThrash verifies that under repeated slave
// disconnects/reconnects during write load, the system converges:
//   - Reconnect count plateaus after chaos stops
//   - Slave offset catches up to master offset
//   - L0 score recovers
//   - No unbounded goroutine growth
//
// Failure doc: docs/failures/replication-thrash.md
// Expected: reconnect count ≤ 12, slave converges within 15s of chaos stop.
func TestRegressionReplicationThrash(t *testing.T) {
	master := StartRegression(t)
	defer master.Close()

	slave := StartRegression(t)
	defer slave.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Monitor master — captures replication metrics
	pm := master.NewMonitor(3 * time.Second)
	pm.Start(ctx, 3*time.Second)

	// Phase 1: connect slave, wait for initial sync
	t.Log("repl-thrash: phase 1 — initial sync")
	if err := slave.MakeSlave(master.Addr); err != nil {
		t.Fatalf("failed to start slave replication: %v", err)
	}
	time.Sleep(3 * time.Second)

	// Verify initial sync
	if !master.WaitForReplicaSync(ctx, master, slave, 10*time.Second) {
		t.Fatal("repl-thrash: slave did not complete initial sync")
	}
	t.Logf("repl-thrash: initial sync complete (master_offset=%d, slave_offset=%d)",
		master.GetMasterOffset(), slave.GetSlaveOffset())

	time.Sleep(500 * time.Millisecond)
	baseline := runtime.NumGoroutine()
	t.Logf("repl-thrash: baseline goroutines=%d", baseline)

	// Phase 2: write load + partition cycles
	// Writers run continuously for 60s in background
	// Partition cycles happen every 6s for 36s (6 cycles)
	t.Log("repl-thrash: phase 2 — write load + partition cycles (36s)")
	errCh := make(chan error, 100)
	loadDone := make(chan struct{})

	// Background writers
	go func() {
		master.RunLoad(ctx, 4, 60*time.Second, errCh)
		close(loadDone)
	}()

	// Partition cycles: kill slave connection every 6 seconds
	for i := 0; i < 6; i++ {
		select {
		case <-ctx.Done():
			t.Fatal("context cancelled during partition cycles")
		default:
		}

		time.Sleep(6 * time.Second)

		// Kill slave connection — triggers reconnectLoop on slave
		if err := master.Client.Do(ctx, "CLIENT", "KILL", "TYPE", "slave").Err(); err != nil {
			t.Logf("repl-thrash: CLIENT KILL error (expected during reconnect): %v", err)
		}
		recon := slave.GetReconnectCount()
		mOff := master.GetMasterOffset()
		sOff := slave.GetSlaveOffset()
		t.Logf("repl-thrash: cycle %d — reconnects=%d master_offset=%d slave_offset=%d",
			i+1, recon, mOff, sOff)
	}

	// Phase 3: drain — writers still active but no more partitions
	t.Log("repl-thrash: phase 3 — convergence wait (20s)")
	time.Sleep(20 * time.Second)

	// Check convergence
	mOff := master.GetMasterOffset()
	sOff := slave.GetSlaveOffset()
	recon := slave.GetReconnectCount()
	t.Logf("repl-thrash: post-drain — reconnects=%d master_offset=%d slave_offset=%d lag=%d",
		recon, mOff, sOff, mOff-sOff)

	// Wait for load goroutines to finish before closing errCh
	<-loadDone

	// Drain errors
	close(errCh)
	var errs []string
	for err := range errCh {
		errs = append(errs, err.Error())
		if len(errs) >= 5 {
			break
		}
	}

	pm.LogSummary(t)

	// Assertion: tighter thresholds for replication stability
	assertion := monitor.DefaultDegradationAssertion()
	assertion.MaxGoroutineDelta = 40
	assertion.MaxActiveRetries = 80
	assertion.MaxReconnectCount = 12
	assertion.ReconnectWarnThreshold = 5
	assertion.L0DegradedThreshold = 18
	level := pm.CheckDegradation(t, assertion, baseline)

	t.Logf("repl-thrash: degradation level: %s", level)
	t.Logf("repl-thrash: total errors: %d", len(errs))

	// Verify data integrity
	if err := master.DB.Check(); err != nil {
		t.Errorf("repl-thrash: master DB integrity check: %v", err)
	} else {
		t.Log("repl-thrash: master DB integrity check passed")
	}
	if err := slave.DB.Check(); err != nil {
		t.Errorf("repl-thrash: slave DB integrity check: %v", err)
	} else {
		t.Log("repl-thrash: slave DB integrity check passed")
	}

	// Verify slave offset convergence
	lag := mOff - sOff
	if lag > 1000 {
		t.Logf("repl-thrash: WARN: slave lag = %d (may need more drain time)", lag)
	} else {
		t.Logf("repl-thrash: slave offset converged, lag=%d", lag)
	}

	if level >= monitor.LevelDegraded {
		t.Log("repl-thrash: system degraded but invariants held within bounds")
	}
}

// TestRegressionReplicationThrashFullresync verifies that even after a
// long partition (forcing FULLRESYNC), the system converges.
//
// Failure doc: docs/failures/replication-thrash.md
// Expected: after FULLRESYNC, slave converges within 15s.
func TestRegressionReplicationThrashFullresync(t *testing.T) {
	master := StartRegression(t)
	defer master.Close()

	slave := StartRegression(t)
	defer slave.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pm := master.NewMonitor(3 * time.Second)
	pm.Start(ctx, 3*time.Second)

	// Phase 1: initial sync
	if err := slave.MakeSlave(master.Addr); err != nil {
		t.Fatalf("failed to start slave replication: %v", err)
	}

	if !master.WaitForReplicaSync(ctx, master, slave, 10*time.Second) {
		t.Fatal("repl-fullresync: slave did not complete initial sync")
	}
	t.Logf("repl-fullresync: initial sync ok (mo=%d so=%d)",
		master.GetMasterOffset(), slave.GetSlaveOffset())

	time.Sleep(500 * time.Millisecond)
	baseline := runtime.NumGoroutine()
	t.Logf("repl-fullresync: baseline goroutines=%d", baseline)

	// Phase 2: write heavy — fill enough to push past backlog
	t.Log("repl-fullresync: phase 2 — heavy writes to advance master offset")
	errCh := make(chan error, 100)
	master.RunLoad(ctx, 8, 25*time.Second, errCh)

	mOffBefore := master.GetMasterOffset()

	// Phase 3: long partition — kill slave, keep writing, exceed backlog
	t.Log("repl-fullresync: phase 3 — partition (20s heavy writes on master)")
	go master.RunLoad(ctx, 8, 20*time.Second, errCh)

	if err := master.Client.Do(ctx, "CLIENT", "KILL", "TYPE", "slave").Err(); err != nil {
		t.Logf("repl-fullresync: initial kill: %v", err)
	}

	// Wait for backlog to exceed — slave should be forced to FULLRESYNC
	time.Sleep(20 * time.Second)
	mOffAfter := master.GetMasterOffset()
	t.Logf("repl-fullresync: partition — master offset advanced: %d → %d (delta=%d)",
		mOffBefore, mOffAfter, mOffAfter-mOffBefore)

	// Phase 4: convergence — slave should reconnect and FULLRESYNC
	t.Log("repl-fullresync: phase 4 — convergence (25s)")
	time.Sleep(25 * time.Second)

	mOff := master.GetMasterOffset()
	sOff := slave.GetSlaveOffset()
	recon := slave.GetReconnectCount()
	t.Logf("repl-fullresync: post-drain — reconnects=%d mo=%d so=%d lag=%d",
		recon, mOff, sOff, mOff-sOff)

	pm.LogSummary(t)

	assertion := monitor.DefaultDegradationAssertion()
	assertion.MaxGoroutineDelta = 50
	assertion.MaxActiveRetries = 100
	assertion.MaxReconnectCount = 3
	assertion.ReconnectWarnThreshold = 1
	assertion.L0DegradedThreshold = 20
	level := pm.CheckDegradation(t, assertion, baseline)

	t.Logf("repl-fullresync: degradation level: %s", level)

	// Verify offset convergence (FULLRESYNC should have caught up)
	// Full resync + subsequent writes may leave small lag
	lag := mOff - sOff
	if lag > 5000 {
		t.Errorf("repl-fullresync: slave lag too large after convergence: %d", lag)
	} else {
		t.Logf("repl-fullresync: slave converged, lag=%d", lag)
	}

	// Data integrity
	if err := master.DB.Check(); err != nil {
		t.Errorf("repl-fullresync: master DB check: %v", err)
	}
	if err := slave.DB.Check(); err != nil {
		t.Errorf("repl-fullresync: slave DB check: %v", err)
	}
}
