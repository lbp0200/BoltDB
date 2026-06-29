package regressions

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/monitor"
)

// TestRegressionWriteDeadlineStorm verifies that during FULLRESYNC,
// heavy writes on the master do NOT cause a reconnect storm.
//
// Failure doc: docs/failures/replication-write-deadline.md
//
// The original bug: SetWriteDeadline on slave TCP during RDB loading caused
// bufio.Writer to enter an unrecoverable state, triggering repeated
// FULLRESYNC cycles (each losing writes).
//
// Fix: removed write deadline — stalls during RDB loading are bounded by
// definition (slave finishes loading and drains the buffer).
//
// Scenario:
//  1. master + slave, initial sync
//  2. Write heavy data to master (exceed backlog)
//  3. Kill slave connection → triggers FULLRESYNC
//  4. During FULLRESYNC, continue heavy writes
//  5. Slave reconnects → verify data integrity, bounded reconnects
//
// Expected: reconnect count ≤ 5, final lag < 5000, no goroutine leak.
func TestRegressionWriteDeadlineStorm(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heavy regression in short mode")
	}

	master := StartRegression(t)
	defer master.Close()

	slave := StartRegression(t)
	defer slave.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	pm := master.NewMonitor(3 * time.Second)
	pm.Start(ctx, 3*time.Second)

	// Phase 1: initial sync
	t.Log("write-deadline: phase 1 — initial sync")
	if err := slave.MakeSlave(master.Addr); err != nil {
		t.Fatalf("failed to start slave replication: %v", err)
	}
	time.Sleep(3 * time.Second)

	if !master.WaitForReplicaSync(ctx, master, slave, 10*time.Second) {
		t.Fatal("write-deadline: slave did not complete initial sync")
	}
	t.Logf("write-deadline: initial sync ok (mo=%d so=%d)",
		master.GetMasterOffset(), slave.GetSlaveOffset())

	time.Sleep(500 * time.Millisecond)
	baseline := runtime.NumGoroutine()
	t.Logf("write-deadline: baseline goroutines=%d", baseline)

	// Phase 2: heavy writes to advance master past backlog
	t.Log("write-deadline: phase 2 — heavy writes (15s)")
	errCh := make(chan error, 100)
	master.RunLoad(ctx, 8, 15*time.Second, errCh)

	mOffBefore := master.GetMasterOffset()
	t.Logf("write-deadline: after initial writes — master_offset=%d", mOffBefore)

	// Phase 3: kill slave to trigger FULLRESYNC, then keep writing heavily
	// This simulates the scenario where RDB loading happens while writes continue
	t.Log("write-deadline: phase 3 — kill slave + heavy writes during FULLRESYNC")

	// Kill slave connection from master side
	if err := master.Client.Do(ctx, "CLIENT", "KILL", "TYPE", "slave").Err(); err != nil {
		t.Logf("write-deadline: CLIENT KILL error (expected): %v", err)
	}

	// Keep writing heavily while slave is doing FULLRESYNC
	go master.RunLoad(ctx, 8, 30*time.Second, errCh)

	// Wait for the storm to potentially happen — give time for FULLRESYNC
	// The original bug would show reconnect count climbing rapidly here
	t.Log("write-deadline: waiting 25s for FULLRESYNC storm window...")
	time.Sleep(25 * time.Second)

	reconDuringStorm := slave.GetReconnectCount()
	t.Logf("write-deadline: during storm — reconnects=%d", reconDuringStorm)

	// Phase 4: convergence — wait for slave to catch up
	t.Log("write-deadline: phase 4 — convergence wait (30s)")
	time.Sleep(30 * time.Second)

	mOff := master.GetMasterOffset()
	sOff := slave.GetSlaveOffset()
	recon := slave.GetReconnectCount()
	lag := mOff - sOff
	t.Logf("write-deadline: post-drain — reconnects=%d mo=%d so=%d lag=%d",
		recon, mOff, sOff, lag)

	// Convergence barrier
	t.Log("write-deadline: convergence barrier — waiting for monitor sample...")
	var barrierOk bool
	for i := 0; i < 20; i++ {
		l := pm.Latest()
		moff := master.GetMasterOffset()
		lag := moff - l.SlaveOffset
		if l.ConnectedSlaves > 0 && lag < 10000 {
			t.Logf("write-deadline: monitor captured convergence (mo=%d so=%d lag=%d)",
				moff, l.SlaveOffset, lag)
			barrierOk = true
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !barrierOk {
		l := pm.Latest()
		t.Logf("write-deadline: WARN — barrier timeout (mo=%d so=%d slaves=%d)",
			master.GetMasterOffset(), l.SlaveOffset, l.ConnectedSlaves)
	}

	pm.LogSummary(t)

	// Drain errors
	close(errCh)
	errCount := 0
	for err := range errCh {
		if errCount == 0 {
			t.Logf("write-deadline: first error: %v", err)
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

	t.Logf("write-deadline: degradation level: %s", level)
	t.Logf("write-deadline: total write errors: %d", errCount)

	// Key assertion: reconnect count should be bounded (no reconnect storm)
	if recon > 5 {
		t.Errorf("write-deadline: reconnect count too high (%d > 5) — possible storm", recon)
	} else {
		t.Logf("write-deadline: PASS: reconnect count bounded (%d)", recon)
	}

	// Key assertion: final lag should be reasonable
	if lag > 5000 {
		t.Errorf("write-deadline: slave lag too large (%d > 5000)", lag)
	} else {
		t.Logf("write-deadline: PASS: slave converged (lag=%d)", lag)
	}

	// Data integrity
	if err := master.DB.Check(); err != nil {
		t.Errorf("write-deadline: master DB check: %v", err)
	} else {
		t.Log("write-deadline: PASS: master DB integrity check")
	}
	if err := slave.DB.Check(); err != nil {
		t.Errorf("write-deadline: slave DB check: %v", err)
	} else {
		t.Log("write-deadline: PASS: slave DB integrity check")
	}

	if level >= monitor.LevelDegraded {
		t.Logf("write-deadline: system degraded but invariants held within bounds")
	}
}
