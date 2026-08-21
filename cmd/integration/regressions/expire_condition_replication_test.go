package regressions

import (
	"context"
	"testing"
	"time"
)

// TestRegressionCanonicalExpireConditionRejected verifies that a conditional
// EXPIRE (NX/XX/GT/LT) whose condition FAILS on the master (returns 0 because
// the key already has a TTL) is NOT propagated to the slave as a replacement
// absolute expiry. If the master's condition is rejected and the TTL is left
// unchanged, the slave must also leave it unchanged — NOT set it to the
// PEXPIREAT absolute timestamp of the rejected call.
//
// Path: handleEXPIRE (condition rejected → returns Integer 0) →
// handler_core.go canonicalization to PEXPIREAT unconditionally →
// PropagateCommand → slave PEXPIREAT apply.
func TestRegressionCanonicalExpireConditionRejected(t *testing.T) {
	master := StartRegression(t)
	defer master.Close()

	slave := StartRegression(t)
	defer slave.Close()

	ctx := context.Background()

	if err := slave.MakeSlave(master.Addr); err != nil {
		t.Fatalf("MakeSlave failed: %v", err)
	}
	defer slave.StopSlave()

	if !slave.WaitForReplicaSync(ctx, master, slave, 30*time.Second) {
		t.Fatal("slave did not sync in time")
	}

	// Seed a key WITH a TTL first (state our NX should reject).
	if err := master.Client.Set(ctx, "expire:cond", "val", 0).Err(); err != nil {
		t.Fatalf("SET failed: %v", err)
	}
	if err := master.Client.Expire(ctx, "expire:cond", 100*time.Second).Err(); err != nil {
		t.Fatalf("seed EXPIRE failed: %v", err)
	}
	if !slave.WaitForReplicaSync(ctx, master, slave, 30*time.Second) {
		t.Fatal("slave did not sync after seed")
	}

	masterTTL, err := master.Client.TTL(ctx, "expire:cond").Result()
	if err != nil {
		t.Fatalf("master TTL read failed: %v", err)
	}

	// NX should REJECT since the key already has a TTL → returns 0.
	nxResult, err := master.Client.Do(ctx, "EXPIRE", "expire:cond", "200", "NX").Result()
	if err != nil {
		t.Fatalf("EXPIRE NX failed: %v", err)
	}
	switch v := nxResult.(type) {
	case int64:
		if v != 0 {
			t.Fatalf("expected EXPIRE NX to return 0 (key already has TTL), got %v", v)
		}
	default:
		t.Fatalf("expected EXPIRE NX to return int64, got %v", nxResult)
	}

	// Master TTL must be unchanged after the rejected NX.
	masterTTL2, err := master.Client.TTL(ctx, "expire:cond").Result()
	if err != nil {
		t.Fatalf("master TTL re-read failed: %v", err)
	}
	if masterTTL2 != masterTTL {
		t.Fatalf("master TTL changed after rejected NX: %v -> %v", masterTTL, masterTTL2)
	}

	// Wait for replication then verify slave TTL stays consistent with master.
	if !slave.WaitForReplicaSync(ctx, master, slave, 30*time.Second) {
		t.Fatal("slave did not sync after rejected NX")
	}
	slaveTTL, err := slave.Client.TTL(ctx, "expire:cond").Result()
	if err != nil {
		t.Fatalf("slave TTL read failed: %v", err)
	}
	diff := masterTTL - slaveTTL
	if diff < 0 || diff > 5*time.Second {
		t.Fatalf("slave TTL drifted after rejected NX: masterTTL=%v slaveTTL=%v (diff=%v)", masterTTL, slaveTTL, diff)
	}
}
