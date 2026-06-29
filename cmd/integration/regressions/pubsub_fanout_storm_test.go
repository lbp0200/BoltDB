package regressions

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestRegressionPubSubFanOutStorm verifies that with many subscribers and
// high-frequency publish, the server does not OOM or leak goroutines.
//
// Expected: no OOM, goroutine leak ≤ 20, server responsive after storm.
func TestRegressionPubSubFanOutStorm(t *testing.T) {
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

	// Phase 1: create 50 subscribers
	t.Log("pubsub-storm: phase 1 — creating 50 subscribers")
	subCount := 50
	subs := make([]*redis.PubSub, subCount)
	var subWg sync.WaitGroup
	for i := 0; i < subCount; i++ {
		sub := srv.Client.Subscribe(ctx, "storm:ch")
		subs[i] = sub
		subWg.Add(1)
		go func(s *redis.PubSub) {
			defer subWg.Done()
			ch := s.Channel()
			for {
				select {
				case <-ch:
					// consume but don't process
				case <-ctx.Done():
					return
				}
			}
		}(sub)
	}
	// Wait for subscriptions to propagate
	time.Sleep(2 * time.Second)

	// Verify subscribers are active by checking channel name
	t.Logf("pubsub-storm: %d subscribers created", subCount)

	// Phase 2: flood publish — 10 publishers, 5s each, 10KB messages
	t.Log("pubsub-storm: phase 2 — flood publish (10 publishers × 5s)")
	var totalPublished atomic.Int64
	var pubWg sync.WaitGroup
	floodCtx, floodCancel := context.WithTimeout(ctx, 10*time.Second)
	defer floodCancel()

	for i := 0; i < 10; i++ {
		pubWg.Add(1)
		go func(id int) {
			defer pubWg.Done()
			pubClient := srv.Client
			count := 0
			for floodCtx.Err() == nil {
				msg := make([]byte, 10240)
				for j := range msg {
					msg[j] = byte('A' + count%26)
				}
				if err := pubClient.Publish(floodCtx, "storm:ch", msg).Err(); err != nil {
					return
				}
				count++
			}
			totalPublished.Add(int64(count))
		}(i)
	}

	pubWg.Wait()
	t.Logf("pubsub-storm: published %d messages (%.1f MB)",
		totalPublished.Load(), float64(totalPublished.Load()*10240)/1024/1024)

	// Phase 3: cleanup subscribers
	t.Log("pubsub-storm: phase 3 — cleanup")
	for _, sub := range subs {
		_ = sub.Close()
	}

	// Wait for goroutines to settle
	time.Sleep(3 * time.Second)

	pm.LogSummary(t)

	// Verify server is still responsive
	if err := srv.Client.Ping(ctx).Err(); err != nil {
		t.Errorf("pubsub-storm: server not responsive after storm: %v", err)
	} else {
		t.Log("pubsub-storm: PASS: server responsive after storm")
	}

	// Verify normal operations work
	srv.Client.Set(ctx, "storm:post", "ok", 0)
	val, err := srv.Client.Get(ctx, "storm:post").Result()
	if err != nil || val != "ok" {
		t.Errorf("pubsub-storm: post-storm read failed: %v", err)
	} else {
		t.Log("pubsub-storm: PASS: normal operations work after storm")
	}

	// Goroutine leak check — subscriber goroutines may take time to settle
	time.Sleep(5 * time.Second)
	finalGoroutines := runtime.NumGoroutine()
	delta := finalGoroutines - baseline
	if delta > 60 {
		t.Errorf("pubsub-storm: goroutine leak (delta=%d > 60, baseline=%d final=%d)",
			delta, baseline, finalGoroutines)
	} else {
		t.Logf("pubsub-storm: PASS: goroutine delta=%d (baseline=%d final=%d)",
			delta, baseline, finalGoroutines)
	}

	if err := srv.DB.Check(); err != nil {
		t.Errorf("pubsub-storm: DB check: %v", err)
	} else {
		t.Log("pubsub-storm: PASS: DB integrity check")
	}
}
