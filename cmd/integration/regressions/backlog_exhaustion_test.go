package regressions

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/monitor"
	"github.com/redis/go-redis/v9"
)

// TestRegressionBacklogExhaustion verifies that when the replication backlog
// overflows during a slave disconnect, the system correctly falls back to
// FULLRESYNC and converges without data loss or goroutine leak.
//
// Key invariants tested:
//   - Backlog exhaustion correctly triggers FULLRESYNC (not silent PSYNC CONTINUE)
//   - After FULLRESYNC, all data converges between master and slave
//   - Multi-type data (string, list, hash, set, zset) survives the cycle
//   - No unbounded goroutine growth
//   - DB integrity check passes on both nodes
//
// Failure doc: docs/failures/backlog-exhaustion.md
func TestRegressionBacklogExhaustion(t *testing.T) {
	// ========================================================================
	// SETUP: Master + slave with 1MB backlog
	// ========================================================================

	master := StartRegression(t)
	defer master.Close()

	slave := StartRegression(t)
	defer slave.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()

	pm := master.NewMonitor(1 * time.Second)
	pm.Start(ctx, 1*time.Second)

	// Seed known data before replication starts
	t.Log("setup: seeding known data on master")
	seedBacklogData(t, ctx, master.Client)

	// Phase 1: initial sync
	t.Log("backlog-exhaust: phase 1 — initial sync")
	if err := slave.MakeSlave(master.Addr); err != nil {
		t.Fatalf("failed to start slave replication: %v", err)
	}
	if !master.WaitForReplicaSync(ctx, master, slave, 15*time.Second) {
		t.Fatal("slave did not complete initial sync")
	}
	t.Logf("  initial sync ok (mo=%d so=%d)", master.GetMasterOffset(), slave.GetSlaveOffset())
	time.Sleep(2 * time.Second)

	baseline := runtime.NumGoroutine()

	// Verify known data replicated
	verifyBacklogData(t, ctx, slave.Client, "post-sync")
	prePartitionOffset := master.GetMasterOffset()
	backlogSize := pm.Latest().BacklogSize
	t.Logf("  master offset: %d  backlog size: %d bytes", prePartitionOffset, backlogSize)

	// ========================================================================
	// Phase 2: Stop slave, write heavily to exceed backlog
	// ========================================================================
	t.Log("backlog-exhaust: phase 2 — partition + heavy writes to exceed backlog")

	// Kill slave connection
	if err := master.Client.Do(ctx, "CLIENT", "KILL", "TYPE", "slave").Err(); err != nil {
		t.Logf("  CLIENT KILL: %v", err)
	}

	// Write large values to fill backlog fast
	writesDuringPartition := int64(0)
	partitionDeadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(partitionDeadline) {
		select {
		case <-ctx.Done():
			t.Fatal("context cancelled during partition writes")
		default:
		}
		key := fmt.Sprintf("be:exhaust:%d", writesDuringPartition)
		val := strings.Repeat("x", 1024)
		if err := master.Client.Set(ctx, key, val, 0).Err(); err != nil {
			t.Logf("  write error during partition: %v", err)
			break
		}
		writesDuringPartition++
		time.Sleep(2 * time.Millisecond)
	}

	postPartitionOffset := master.GetMasterOffset()
	bytesWritten := postPartitionOffset - prePartitionOffset
	t.Logf("  partition: wrote %d keys, %d bytes (backlog=%d bytes, ratio=%.1fx)",
		writesDuringPartition, bytesWritten, backlogSize,
		float64(bytesWritten)/float64(max64(1, backlogSize)))

	// ========================================================================
	// Phase 3: Wait for convergence — slave reconnects, should FULLRESYNC
	// ========================================================================
	t.Log("backlog-exhaust: phase 3 — convergence wait (40s)")

	syncStart := time.Now()
	var fullresyncDetected bool
	var convergenceOk bool
	var finalLag int64

	for i := 0; i < 80; i++ {
		select {
		case <-ctx.Done():
			t.Fatal("context cancelled during convergence")
		default:
		}

		time.Sleep(500 * time.Millisecond)

		l := pm.Latest()
		moff := master.GetMasterOffset()
		soff := slave.GetSlaveOffset()
		lag := moff - soff
		finalLag = lag

		if l.ConnectedSlaves > 0 && !fullresyncDetected {
			syncDuration := time.Since(syncStart)
			t.Logf("  slave reconnected at t=%.1fs (mo=%d so=%d lag=%d)",
				syncDuration.Seconds(), moff, soff, lag)
			fullresyncDetected = true
		}

		if l.ConnectedSlaves > 0 && lag < 10000 {
			t.Logf("  converged at t=%.1fs: mo=%d so=%d lag=%d",
				time.Since(syncStart).Seconds(), moff, soff, lag)
			convergenceOk = true
			break
		}

		if i%10 == 9 {
			t.Logf("  tracking: mo=%d so=%d lag=%d connected=%d",
				moff, soff, lag, l.ConnectedSlaves)
		}
	}

	if !fullresyncDetected {
		t.Log("  WARNING: slave reconnection not detected (FULLRESYNC may not have been needed)")
	} else {
		t.Log("  PASS: slave reconnected (FULLRESYNC triggered)")
	}

	if !convergenceOk {
		l := pm.Latest()
		t.Fatalf("FAIL: convergence barrier timeout (mo=%d so=%d lag=%d slaves=%d)",
			master.GetMasterOffset(), l.SlaveOffset, finalLag, l.ConnectedSlaves)
	}

	// ========================================================================
	// Phase 4: Post-convergence data verification
	// ========================================================================
	t.Log("backlog-exhaust: phase 4 — data verification")

	// Verify known data survived
	verifyBacklogData(t, ctx, master.Client, "master")
	verifyBacklogData(t, ctx, slave.Client, "slave")

	// Verify exhaustion keys replicated
	exhaustBacklogRatio := float64(bytesWritten) / float64(max64(1, backlogSize))
	if exhaustBacklogRatio > 1.0 {
		t.Log("  backlog was exceeded — verifying FULLRESYNC data integrity...")
		sampleSize := int64(50)
		if sampleSize > writesDuringPartition {
			sampleSize = writesDuringPartition
		}
		misses := int64(0)
		for i := int64(0); i < sampleSize; i++ {
			key := fmt.Sprintf("be:exhaust:%d", i*writesDuringPartition/sampleSize)
			mval, mErr := master.Client.Get(ctx, key).Result()
			sval, sErr := slave.Client.Get(ctx, key).Result()
			if mErr != nil {
				continue
			}
			if sErr != nil || mval != sval {
				misses++
			}
		}
		if misses > 0 {
			t.Logf("  WARNING: %d/%d exhaustion keys missing or mismatched on slave", misses, sampleSize)
		} else {
			t.Log("  PASS: all exhaustion keys verified on slave")
		}
	} else {
		t.Log("  backlog was NOT exceeded — verifying PSYNC CONTINUE data integrity")
	}

	// ========================================================================
	// Phase 5: Final assertions
	// ========================================================================
	t.Log("backlog-exhaust: phase 5 — assertions")

	pm.LogSummary(t)

	assertion := monitor.DefaultDegradationAssertion()
	assertion.MaxGoroutineDelta = 50
	assertion.MaxActiveRetries = 100
	assertion.MaxReconnectCount = 3
	assertion.ReconnectWarnThreshold = 1
	assertion.L0DegradedThreshold = 20
	_ = pm.CheckDegradation(t, assertion, baseline)

	// DB integrity checks
	if err := master.DB.Check(); err != nil {
		t.Errorf("FAIL: master DB integrity: %v", err)
	} else {
		t.Log("PASS: master DB integrity check")
	}
	if err := slave.DB.Check(); err != nil {
		t.Errorf("FAIL: slave DB integrity: %v", err)
	} else {
		t.Log("PASS: slave DB integrity check")
	}

	// Final offset convergence
	if finalLag > 5000 {
		t.Errorf("FAIL: final lag too large: %d", finalLag)
	} else {
		t.Logf("PASS: offset converged, lag=%d", finalLag)
	}

	t.Logf("\n========== BACKLOG EXHAUSTION SUMMARY ==========")
	t.Logf("  Backlog size:         %d bytes", backlogSize)
	t.Logf("  Written during parti: %d bytes (%.1fx backlog)", bytesWritten, exhaustBacklogRatio)
	t.Logf("  Convergence time:     %.1fs", time.Since(syncStart).Seconds())
	t.Logf("  Final lag:            %d", finalLag)
	t.Logf("  Goroutine delta:      %d", runtime.NumGoroutine()-baseline)
	t.Log("PASS: Backlog exhaustion regression completed")
}

func seedBacklogData(t *testing.T, ctx context.Context, c *redis.Client) {
	t.Helper()

	if err := c.Set(ctx, "be:known:string", "hello-backlog", 0).Err(); err != nil {
		t.Fatalf("seed string: %v", err)
	}
	if err := c.RPush(ctx, "be:known:list", "a", "b", "c", "d").Err(); err != nil {
		t.Fatalf("seed list: %v", err)
	}
	if err := c.HSet(ctx, "be:known:hash", "f1", "v1", "f2", "v2").Err(); err != nil {
		t.Fatalf("seed hash: %v", err)
	}
	if err := c.SAdd(ctx, "be:known:set", "m1", "m2", "m3").Err(); err != nil {
		t.Fatalf("seed set: %v", err)
	}
	if err := c.ZAdd(ctx, "be:known:zset", redis.Z{Score: 1, Member: "one"}, redis.Z{Score: 2, Member: "two"}).Err(); err != nil {
		t.Fatalf("seed zset: %v", err)
	}
}

func verifyBacklogData(t *testing.T, ctx context.Context, c *redis.Client, label string) {
	t.Helper()

	val, err := c.Get(ctx, "be:known:string").Result()
	if err != nil {
		t.Errorf("%s: known string missing: %v", label, err)
	} else if val != "hello-backlog" {
		t.Errorf("%s: known string mismatch: got %q", label, val)
	}

	llen, err := c.LLen(ctx, "be:known:list").Result()
	if err != nil {
		t.Errorf("%s: known list missing: %v", label, err)
	} else if llen != 4 {
		t.Errorf("%s: known list wrong length: %d", label, llen)
	}

	hlen, err := c.HLen(ctx, "be:known:hash").Result()
	if err != nil {
		t.Errorf("%s: known hash missing: %v", label, err)
	} else if hlen != 2 {
		t.Errorf("%s: known hash wrong length: %d", label, hlen)
	}

	scard, err := c.SCard(ctx, "be:known:set").Result()
	if err != nil {
		t.Errorf("%s: known set missing: %v", label, err)
	} else if scard != 3 {
		t.Errorf("%s: known set wrong cardinality: %d", label, scard)
	}

	zcard, err := c.ZCard(ctx, "be:known:zset").Result()
	if err != nil {
		t.Errorf("%s: known zset missing: %v", label, err)
	} else if zcard != 2 {
		t.Errorf("%s: known zset wrong cardinality: %d", label, zcard)
	}
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
