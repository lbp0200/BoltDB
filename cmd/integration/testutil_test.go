package integration

import (
	"context"
	"testing"
)

// verifyServerLiveness confirms the server can complete a SET+GET round-trip.
// Used as a post-disruption health check in interrupt/concurrent tests.
func verifyServerLiveness(t *testing.T, srv *IsolatedServer) {
	t.Helper()
	ctx := context.Background()
	key := "liveness:" + t.Name()
	if err := srv.Client.Set(ctx, key, "ok", 0).Err(); err != nil {
		t.Fatalf("liveness SET failed: %v", err)
	}
	val, err := srv.Client.Get(ctx, key).Result()
	if err != nil {
		t.Fatalf("liveness GET failed: %v", err)
	}
	if val != "ok" {
		t.Errorf("liveness GET = %q, want %q", val, "ok")
	}
}
