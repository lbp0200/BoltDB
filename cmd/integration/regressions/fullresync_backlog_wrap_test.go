package regressions

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"
)

// TestRegressionFullresyncBacklogWrap verifies that when the backlog fills
// and wraps during a FULLRESYNC, no keys are lost on the slave.
//
// Hypothesis H2: If the backlog ring buffer wraps during RDB generation/transfer,
// the gap-fill window [snapshotOffset, currentOffset) could contain holes
// that were captured neither in the RDB snapshot nor in the backlog.
//
// This test pre-populates data, starts moderate concurrent writes, triggers
// FULLRESYNC under load, stops the writes, lets the slave converge, then
// compares ALL keys between master and slave.
func TestRegressionFullresyncBacklogWrap(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heavy regression in short mode")
	}

	master := StartRegression(t)
	defer master.Close()

	slave := StartRegression(t)
	defer slave.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	baseline := runtime.NumGoroutine()

	// ========================================================================
	// PHASE 1: Pre-populate master with 5K keys (mixed types)
	// ========================================================================
	t.Log("backlog-wrap: phase 1 — pre-populating 5K keys")

	popCtx, popCancel := context.WithTimeout(ctx, 60*time.Second)
	for i := 0; i < 3000; i++ {
		if popCtx.Err() != nil {
			popCancel()
			t.Fatalf("pre-populate timeout at key %d", i)
		}
		master.Client.Set(popCtx, fmt.Sprintf("pre:s:%04d", i), fmt.Sprintf("v%d", i), 0)
	}
	for i := 0; i < 1000; i++ {
		if popCtx.Err() != nil {
			popCancel()
			t.Fatalf("pre-populate hash timeout at %d", i)
		}
		master.Client.HSet(popCtx, fmt.Sprintf("pre:h:%04d", i), "f1", "v1", "f2", "v2")
	}
	for i := 0; i < 500; i++ {
		if popCtx.Err() != nil {
			popCancel()
			t.Fatalf("pre-populate list timeout at %d", i)
		}
		p := master.Client.Pipeline()
		p.RPush(popCtx, fmt.Sprintf("pre:l:%04d", i), "a", "b", "c")
		p.Exec(popCtx)
	}
	for i := 0; i < 500; i++ {
		if popCtx.Err() != nil {
			popCancel()
			t.Fatalf("pre-populate set timeout at %d", i)
		}
		p := master.Client.Pipeline()
		p.SAdd(popCtx, fmt.Sprintf("pre:set:%04d", i), "x", "y", "z")
		p.Exec(popCtx)
	}
	popCancel()
	preCount := 5000
	t.Logf("backlog-wrap: pre-populated %d keys", preCount)

	// ========================================================================
	// PHASE 2: Start concurrent writes (moderate throughput for 30s)
	// ========================================================================
	t.Log("backlog-wrap: phase 2 — starting writes (4 writers, ~1K ops/s each)")

	loadCtx, loadCancel := context.WithCancel(ctx)
	errCh := make(chan error, 200)
	var loadWg sync.WaitGroup

	for w := 0; w < 4; w++ {
		writerID := w
		loadWg.Add(1)
		go func() {
			defer loadWg.Done()
			rng := rand.New(rand.NewSource(int64(writerID)))
			for loadCtx.Err() == nil {
				key := fmt.Sprintf("storm:s:%06d", rng.Intn(50000))
				if err := master.Client.Set(loadCtx, key, fmt.Sprintf("v:%d", rng.Intn(100000)), 0).Err(); err != nil {
					select {
					case errCh <- fmt.Errorf("writer %d: %w", writerID, err):
					default:
					}
					return
				}
				time.Sleep(time.Duration(rng.Intn(1000)) * time.Microsecond)
			}
		}()
	}

	// Let writes ramp up
	time.Sleep(2 * time.Second)

	// ========================================================================
	// PHASE 3: Trigger FULLRESYNC under write load
	// ========================================================================
	t.Log("backlog-wrap: phase 3 — FULLRESYNC under load")

	if err := slave.MakeSlave(master.Addr); err != nil {
		loadCancel()
		t.Fatalf("MakeSlave failed: %v", err)
	}
	defer slave.StopSlave()

	// Wait for sync (generous timeout since writes are ongoing)
	t.Log("backlog-wrap: waiting for sync (writes continue)...")
	syncStart := time.Now()
	syncOk := slave.WaitForReplicaSync(ctx, master, slave, 120*time.Second)

	// Stop writes
	loadCancel()
	loadWg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Logf("backlog-wrap: write error (non-fatal): %v", err)
		}
	}

	if !syncOk {
		t.Logf("backlog-wrap: sync timed out during write storm (duration=%v) — waiting for convergence after load stop",
			time.Since(syncStart))
		// Give it more time after writes stop
		syncOk = slave.WaitForReplicaSync(ctx, master, slave, 60*time.Second)
		if !syncOk {
			t.Fatalf("backlog-wrap: slave STILL did not sync after writes stopped")
		}
	}
	t.Logf("backlog-wrap: sync completed in %v (mo=%d so=%d)",
		time.Since(syncStart), master.GetMasterOffset(), slave.GetSlaveOffset())

	// Allow residual lag to close
	time.Sleep(5 * time.Second)
	mo := master.GetMasterOffset()
	so := slave.GetSlaveOffset()
	lag := mo - so
	t.Logf("backlog-wrap: final offsets mo=%d so=%d lag=%d", mo, so, lag)
	if lag > 10000 {
		t.Errorf("backlog-wrap: final lag too large (%d > 10000)", lag)
	}

	// ========================================================================
	// PHASE 4: Compare ALL keys via SCAN
	// ========================================================================
	t.Log("backlog-wrap: phase 4 — comparing all keys")

	masterKeys := scanAllKeys(ctx, t, master.Client, "master")
	slaveKeys := scanAllKeys(ctx, t, slave.Client, "slave")

	t.Logf("backlog-wrap: master=%d keys, slave=%d keys", len(masterKeys), len(slaveKeys))

	lostKeys := 0
	ghostKeys := 0
	for k := range masterKeys {
		if !slaveKeys[k] {
			if lostKeys < 5 {
				t.Errorf("backlog-wrap: lost key: %q", k)
			}
			lostKeys++
		}
	}
	for k := range slaveKeys {
		if !masterKeys[k] {
			if ghostKeys < 5 {
				t.Errorf("backlog-wrap: ghost key: %q", k)
			}
			ghostKeys++
		}
	}

	// ========================================================================
	// REPORT
	// ========================================================================
	recon := slave.GetReconnectCount()
	delta := runtime.NumGoroutine() - baseline

	t.Logf("backlog-wrap: recon=%d lost=%d ghost=%d lag=%d goroutine_delta=%d",
		recon, lostKeys, ghostKeys, lag, delta)

	if lostKeys > 0 {
		t.Errorf("backlog-wrap: FAIL: %d keys lost", lostKeys)
	} else {
		t.Log("backlog-wrap: ✅ zero key loss")
	}
	if ghostKeys > 0 {
		t.Errorf("backlog-wrap: FAIL: %d ghost keys", ghostKeys)
	} else {
		t.Log("backlog-wrap: ✅ zero ghost keys")
	}
	if delta > 50 {
		t.Errorf("backlog-wrap: goroutine leak (%d > 50)", delta)
	} else {
		t.Logf("backlog-wrap: ✅ goroutine delta OK (%d)", delta)
	}
	if recon > 10 {
		t.Errorf("backlog-wrap: reconnect count high (%d > 10)", recon)
	} else {
		t.Logf("backlog-wrap: ✅ reconnect count OK (%d)", recon)
	}
}
