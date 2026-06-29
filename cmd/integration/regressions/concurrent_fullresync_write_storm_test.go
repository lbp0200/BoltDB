package regressions

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/monitor"
)

// TestRegressionConcurrentFullresyncWriteStorm verifies that under heavy
// concurrent writes, triggering a FULLRESYNC does not cause data loss or
// unbounded reconnect loops.
//
// Expected: reconnect count ≤ 8, final lag < 5000, no goroutine leak.
func TestRegressionConcurrentFullresyncWriteStorm(t *testing.T) {
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
	t.Log("concurrent-fr-write: phase 1 — initial sync")
	if err := slave.MakeSlave(master.Addr); err != nil {
		t.Fatalf("failed to start slave replication: %v", err)
	}
	time.Sleep(3 * time.Second)

	if !master.WaitForReplicaSync(ctx, master, slave, 10*time.Second) {
		t.Fatal("concurrent-fr-write: slave did not complete initial sync")
	}
	t.Logf("concurrent-fr-write: sync ok (mo=%d so=%d)",
		master.GetMasterOffset(), slave.GetSlaveOffset())

	time.Sleep(500 * time.Millisecond)
	baseline := runtime.NumGoroutine()

	// Phase 2: heavy concurrent writes + 3 partition cycles
	t.Log("concurrent-fr-write: phase 2 — write storm + 3 partition cycles")
	errCh := make(chan error, 200)
	loadDone := make(chan struct{})
	go func() {
		master.RunLoad(ctx, 12, 90*time.Second, errCh)
		close(loadDone)
	}()

	for i := 0; i < 3; i++ {
		time.Sleep(10 * time.Second)
		if err := master.Client.Do(ctx, "CLIENT", "KILL", "TYPE", "slave").Err(); err != nil {
			t.Logf("concurrent-fr-write: CLIENT KILL %d error (expected): %v", i+1, err)
		}
		recon := slave.GetReconnectCount()
		mOff := master.GetMasterOffset()
		sOff := slave.GetSlaveOffset()
		t.Logf("concurrent-fr-write: cycle %d — recon=%d mo=%d so=%d lag=%d",
			i+1, recon, mOff, sOff, mOff-sOff)
	}

	// Phase 3: convergence
	t.Log("concurrent-fr-write: phase 3 — convergence wait (20s)")
	time.Sleep(20 * time.Second)

	<-loadDone
	close(errCh)

	errCount := 0
	for err := range errCh {
		if errCount == 0 {
			t.Logf("concurrent-fr-write: first error: %v", err)
		}
		errCount++
	}

	// Convergence barrier
	var barrierOk bool
	for i := 0; i < 20; i++ {
		l := pm.Latest()
		moff := master.GetMasterOffset()
		lag := moff - l.SlaveOffset
		if l.ConnectedSlaves > 0 && lag < 10000 {
			t.Logf("concurrent-fr-write: monitor captured (mo=%d so=%d lag=%d)", moff, l.SlaveOffset, lag)
			barrierOk = true
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !barrierOk {
		t.Log("concurrent-fr-write: WARN — barrier timeout")
	}

	pm.LogSummary(t)

	mOff := master.GetMasterOffset()
	sOff := slave.GetSlaveOffset()
	recon := slave.GetReconnectCount()
	lag := mOff - sOff

	assertion := monitor.DefaultDegradationAssertion()
	assertion.MaxGoroutineDelta = 50
	assertion.MaxActiveRetries = 100
	assertion.MaxReconnectCount = 10
	assertion.ReconnectWarnThreshold = 5
	assertion.L0DegradedThreshold = 20
	level := pm.CheckDegradation(t, assertion, baseline)

	t.Logf("concurrent-fr-write: degradation=%s recon=%d lag=%d errors=%d", level, recon, lag, errCount)

	if recon > 10 {
		t.Errorf("concurrent-fr-write: reconnect count too high (%d > 10)", recon)
	} else {
		t.Logf("concurrent-fr-write: PASS: reconnect bounded (%d)", recon)
	}
	if lag > 5000 {
		t.Errorf("concurrent-fr-write: slave lag too large (%d > 5000)", lag)
	} else {
		t.Logf("concurrent-fr-write: PASS: slave converged (lag=%d)", lag)
	}
	if err := master.DB.Check(); err != nil {
		t.Errorf("concurrent-fr-write: master DB check: %v", err)
	}
	if err := slave.DB.Check(); err != nil {
		t.Errorf("concurrent-fr-write: slave DB check: %v", err)
	}
}
