package regressions

import (
	"context"
	"testing"
	"time"
)

// TestRegressionClientBufferOverflow verifies that when a subscriber's
// output buffer exceeds the configured limit, the server handles it
// gracefully without crashing.
//
// Expected: server healthy, other clients unaffected.
func TestRegressionClientBufferOverflow(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heavy regression in short mode")
	}

	srv := StartRegression(t)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Phase 1: baseline
	t.Log("client-buf: phase 1 — baseline")
	srv.Client.Set(ctx, "buf:baseline", "ok", 0)
	val, err := srv.Client.Get(ctx, "buf:baseline").Result()
	if err != nil || val != "ok" {
		t.Fatalf("client-buf: baseline failed: %v", err)
	}
	t.Log("client-buf: baseline ok")

	// Phase 2: create a subscriber that won't consume messages
	// The server will buffer messages and emit "channel full" warnings.
	sub := srv.Client.Subscribe(ctx, "buf:flood")
	defer sub.Close()
	time.Sleep(500 * time.Millisecond)

	// Phase 3: flood the channel with large messages
	t.Log("client-buf: phase 2 — flooding channel")
	floodCtx, floodCancel := context.WithTimeout(ctx, 10*time.Second)
	defer floodCancel()

	var publishCount int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		for floodCtx.Err() == nil {
			msg := make([]byte, 10240) // 10KB per message
			for i := range msg {
				msg[i] = 'x'
			}
			if err := srv.Client.Publish(floodCtx, "buf:flood", msg).Err(); err != nil {
				return
			}
			publishCount++
		}
	}()

	<-done
	t.Logf("client-buf: published %d messages (%.1f MB)", publishCount, float64(publishCount*10240)/1024/1024)

	// Phase 4: verify server is still healthy
	time.Sleep(2 * time.Second)
	t.Log("client-buf: phase 3 — verifying server health")

	if err := srv.Client.Ping(ctx).Err(); err != nil {
		t.Errorf("client-buf: server unhealthy after flood: %v", err)
	} else {
		t.Log("client-buf: PASS: server still healthy after flood")
	}

	srv.Client.Set(ctx, "buf:post-flood", "ok", 0)
	val, err = srv.Client.Get(ctx, "buf:post-flood").Result()
	if err != nil || val != "ok" {
		t.Errorf("client-buf: post-flood read failed: %v", err)
	} else {
		t.Log("client-buf: PASS: normal operations unaffected")
	}

	t.Log("client-buf: PASS: client buffer overflow handled gracefully")
}
