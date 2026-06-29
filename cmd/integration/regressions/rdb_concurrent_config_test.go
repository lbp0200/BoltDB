package regressions

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/monitor"
)

// TestRegressionRdbConcurrentConfigChange verifies that triggering BGSAVE
// while writing data and issuing CONFIG SET does not corrupt the database
// or cause the server to crash.
//
// Note: CONFIG SET is currently a no-op in BoltDB (returns OK without
// applying). This test verifies the command doesn't interfere with BGSAVE.
//
// Expected: BGSAVE completes, server healthy, data consistent.
func TestRegressionRdbConcurrentConfigChange(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heavy regression in short mode")
	}

	srv := StartRegression(t)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	pm := srv.NewMonitor(3 * time.Second)
	pm.Start(ctx, 3*time.Second)

	time.Sleep(2 * time.Second)
	baseline := runtime.NumGoroutine()

	// Phase 1: seed data
	t.Log("rdb-config: phase 1 — seed data")
	for i := 0; i < 200; i++ {
		key := "rdb:seed:" + string(rune('A'+i%26)) + string(rune('0'+i/26))
		srv.Client.Set(ctx, key, "value", 0)
	}
	t.Log("rdb-config: seeded 200 keys")

	// Phase 2: concurrent BGSAVE + writes + CONFIG SET
	t.Log("rdb-config: phase 2 — concurrent BGSAVE + writes + CONFIG SET")
	errCh := make(chan error, 100)

	// Background writers
	loadDone := make(chan struct{})
	go func() {
		srv.RunLoad(ctx, 6, 40*time.Second, errCh)
		close(loadDone)
	}()

	// Issue BGSAVE 3 times with CONFIG SET in between
	for i := 0; i < 3; i++ {
		// BGSAVE
		result, err := srv.Client.Do(ctx, "BGSAVE").Result()
		if err != nil {
			t.Logf("rdb-config: BGSAVE %d error: %v", i+1, err)
		} else {
			t.Logf("rdb-config: BGSAVE %d result: %v", i+1, result)
		}

		// CONFIG SET (no-op but shouldn't crash)
		for _, param := range []string{"maxmemory", "appendonly", "save"} {
			_, err := srv.Client.Do(ctx, "CONFIG", "SET", param, "1").Result()
			if err != nil {
				t.Logf("rdb-config: CONFIG SET %s error: %v", param, err)
			}
		}

		time.Sleep(5 * time.Second)
	}

	// Phase 3: wait for writers to finish
	t.Log("rdb-config: phase 3 — drain")
	<-loadDone
	close(errCh)

	errCount := 0
	for err := range errCh {
		if errCount == 0 {
			t.Logf("rdb-config: first error: %v", err)
		}
		errCount++
	}

	// Convergence barrier
	var barrierOk bool
	for i := 0; i < 10; i++ {
		if pm.Latest().Goroutines > 0 {
			barrierOk = true
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !barrierOk {
		t.Log("rdb-config: WARN — barrier timeout")
	}

	pm.LogSummary(t)

	assertion := monitor.DefaultDegradationAssertion()
	assertion.MaxGoroutineDelta = 30
	assertion.MaxActiveRetries = 50
	level := pm.CheckDegradation(t, assertion, baseline)

	t.Logf("rdb-config: degradation=%s errors=%d", level, errCount)

	// Verify data integrity
	srv.Client.Set(ctx, "rdb:final", "check", 0)
	val, err := srv.Client.Get(ctx, "rdb:final").Result()
	if err != nil || val != "check" {
		t.Errorf("rdb-config: post-test read failed: %v", err)
	} else {
		t.Log("rdb-config: PASS: server functional after BGSAVE + CONFIG SET storm")
	}

	if err := srv.DB.Check(); err != nil {
		t.Errorf("rdb-config: DB check: %v", err)
	} else {
		t.Log("rdb-config: PASS: DB integrity check")
	}
}
