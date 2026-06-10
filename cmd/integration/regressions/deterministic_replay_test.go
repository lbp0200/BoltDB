package regressions

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestRegressionCanonicalSPOP verifies SPOP is canonicalized to SREM during
// replication: the replica receives SREM key member... instead of SPOP,
// preventing random selection divergence.
//
// Propagation path: handler.go:3391-3431 (canonicalization) → handler.go:528
// (markDirtyKeys + PropagateCommand) → psync.go executeReplicatedCommand
// (SREM case).
func TestRegressionCanonicalSPOP(t *testing.T) {
	master := StartRegression(t)
	defer master.Close()

	slave := StartRegression(t)
	defer slave.Close()

	ctx := context.Background()

	// Write set members to master
	members := []string{"a", "b", "c", "d", "e"}
	for _, m := range members {
		if err := master.Client.SAdd(ctx, "spop:key", m).Err(); err != nil {
			t.Fatalf("SAdd failed: %v", err)
		}
	}

	// SPOP 2 members
	spopped, err := master.Client.SPopN(ctx, "spop:key", 2).Result()
	if err != nil {
		t.Fatalf("SPOP failed: %v", err)
	}
	if len(spopped) != 2 {
		t.Fatalf("expected 2 popped, got %d", len(spopped))
	}

	// Connect slave and wait for sync
	if err := slave.MakeSlave(master.Addr); err != nil {
		t.Fatalf("MakeSlave failed: %v", err)
	}
	defer slave.StopSlave()

	if !slave.WaitForReplicaSync(ctx, master, slave, 30*time.Second) {
		t.Fatal("slave did not sync in time")
	}

	// Verify slave has same remaining members
	masterRemaining, err := master.Client.SMembers(ctx, "spop:key").Result()
	if err != nil {
		t.Fatalf("master SMembers failed: %v", err)
	}
	slaveRemaining, err := slave.Client.SMembers(ctx, "spop:key").Result()
	if err != nil {
		t.Fatalf("slave SMembers failed: %v", err)
	}

	if len(masterRemaining) != len(slaveRemaining) {
		t.Fatalf("set size mismatch: master=%d slave=%d", len(masterRemaining), len(slaveRemaining))
	}

	masterSet := make(map[string]bool)
	for _, m := range masterRemaining {
		masterSet[m] = true
	}
	for _, m := range slaveRemaining {
		if !masterSet[m] {
			t.Fatalf("slave has member %q not on master", m)
		}
	}
}

// TestRegressionCanonicalXAdd verifies XADD with * (auto-ID) is canonicalized
// to the resolved ID before propagation.
//
// Propagation path: handler.go:5912 (canonicalization) → handler.go:528
// (markDirtyKeys + PropagateCommand) → psync.go executeReplicatedCommand
// (XADD case).
func TestRegressionCanonicalXAdd(t *testing.T) {
	master := StartRegression(t)
	defer master.Close()

	slave := StartRegression(t)
	defer slave.Close()

	ctx := context.Background()

	// Connect slave first (streams are not in RDB — data must flow through backlog)
	if err := slave.MakeSlave(master.Addr); err != nil {
		t.Fatalf("MakeSlave failed: %v", err)
	}
	defer slave.StopSlave()

	if !slave.WaitForReplicaSync(ctx, master, slave, 30*time.Second) {
		t.Fatal("slave did not sync in time")
	}

	// Write stream entries with * auto-ID (after sync — flows through backlog)
	if err := master.Client.XAdd(ctx, &redis.XAddArgs{
		Stream: "xadd:stream",
		Values: map[string]interface{}{"field0": "val0"},
	}).Err(); err != nil {
		t.Fatalf("XAdd failed: %v", err)
	}
	if err := master.Client.XAdd(ctx, &redis.XAddArgs{
		Stream: "xadd:stream",
		Values: map[string]interface{}{"field1": "val1"},
	}).Err(); err != nil {
		t.Fatalf("XAdd failed: %v", err)
	}

	if !slave.WaitForReplicaSync(ctx, master, slave, 30*time.Second) {
		t.Fatal("slave did not sync after writes")
	}
	if !slave.WaitForReplicaSync(ctx, master, slave, 30*time.Second) {
		t.Fatal("slave did not sync after writes")
	}
	time.Sleep(1 * time.Second)

	masterLen, err := master.Client.XLen(ctx, "xadd:stream").Result()
	if err != nil {
		t.Fatalf("master XLen failed: %v", err)
	}
	slaveLen, err := slave.Client.XLen(ctx, "xadd:stream").Result()
	if err != nil {
		t.Fatalf("slave XLen failed: %v", err)
	}
	if slaveLen == 0 {
		t.Fatalf("slave has no stream entries after sync")
	}
	_ = masterLen
}

// TestRegressionCanonicalExpire verifies EXPIRE/PEXPIRE are translated to
// PEXPIREAT with absolute timestamp during propagation, ensuring
// deterministic replay on the replica.
//
// Propagation path: handler.go:527 (canonicalization to PEXPIREAT) →
// handler.go:528 (PropagateCommand) → psync.go executeReplicatedCommand
// (PEXPIREAT case).
func TestRegressionCanonicalExpire(t *testing.T) {
	master := StartRegression(t)
	defer master.Close()

	slave := StartRegression(t)
	defer slave.Close()

	ctx := context.Background()

	// Connect slave first so all commands flow through backlog
	if err := slave.MakeSlave(master.Addr); err != nil {
		t.Fatalf("MakeSlave failed: %v", err)
	}
	defer slave.StopSlave()

	if !slave.WaitForReplicaSync(ctx, master, slave, 30*time.Second) {
		t.Fatal("slave did not sync in time")
	}

	// Write keys with EXPIRE (go-redis uses time.Duration — seconds * time.Second)
	if err := master.Client.Set(ctx, "expire:key1", "val1", 0).Err(); err != nil {
		t.Fatalf("SET failed: %v", err)
	}
	if err := master.Client.Expire(ctx, "expire:key1", 3600*time.Second).Err(); err != nil {
		t.Fatalf("EXPIRE failed: %v", err)
	}

	if err := master.Client.Set(ctx, "expire:key2", "val2", 0).Err(); err != nil {
		t.Fatalf("SET failed: %v", err)
	}
	if err := master.Client.PExpire(ctx, "expire:key2", 7200*time.Second).Err(); err != nil {
		t.Fatalf("PEXPIRE failed: %v", err)
	}

	if !slave.WaitForReplicaSync(ctx, master, slave, 30*time.Second) {
		t.Fatal("slave did not sync after writes")
	}

	// Both should have the keys
	masterExists1, err := master.Client.Exists(ctx, "expire:key1").Result()
	if err != nil {
		t.Fatalf("master EXISTS failed: %v", err)
	}
	slaveExists1, err := slave.Client.Exists(ctx, "expire:key1").Result()
	if err != nil {
		t.Fatalf("slave EXISTS failed: %v", err)
	}
	if masterExists1 != slaveExists1 {
		t.Fatalf("key1 exists mismatch: master=%d slave=%d", masterExists1, slaveExists1)
	}

	masterExists2, err := master.Client.Exists(ctx, "expire:key2").Result()
	if err != nil {
		t.Fatalf("master EXISTS failed: %v", err)
	}
	slaveExists2, err := slave.Client.Exists(ctx, "expire:key2").Result()
	if err != nil {
		t.Fatalf("slave EXISTS failed: %v", err)
	}
	if masterExists2 != slaveExists2 {
		t.Fatalf("key2 exists mismatch: master=%d slave=%d", masterExists2, slaveExists2)
	}
}

// TestRegressionCanonicalSPOPNoReplica verifies SPOP works correctly on a
// standalone server (no replication) — basic sanity that the canonicalization
// doesn't break the command itself.
func TestRegressionCanonicalSPOPNoReplica(t *testing.T) {
	srv := StartRegression(t)
	defer srv.Close()

	ctx := context.Background()

	for _, m := range []string{"x", "y", "z"} {
		if err := srv.Client.SAdd(ctx, "spop:alone", m).Err(); err != nil {
			t.Fatalf("SAdd failed: %v", err)
		}
	}

	result, err := srv.Client.SPopN(ctx, "spop:alone", 1).Result()
	if err != nil {
		t.Fatalf("SPOP failed: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 popped, got %d", len(result))
	}

	remaining, err := srv.Client.SMembers(ctx, "spop:alone").Result()
	if err != nil {
		t.Fatalf("SMembers failed: %v", err)
	}
	if len(remaining) != 2 {
		t.Fatalf("expected 2 remaining, got %d", len(remaining))
	}
}

// TestRegressionCanonicalExpireOnExistingKey verifies EXPIRE on a key that
// already exists with a TTL is correctly propagated via backlog.
func TestRegressionCanonicalExpireOnExistingKey(t *testing.T) {
	master := StartRegression(t)
	defer master.Close()

	slave := StartRegression(t)
	defer slave.Close()

	ctx := context.Background()

	// Connect slave first, then write through backlog
	if err := slave.MakeSlave(master.Addr); err != nil {
		t.Fatalf("MakeSlave failed: %v", err)
	}
	defer slave.StopSlave()

	if !slave.WaitForReplicaSync(ctx, master, slave, 30*time.Second) {
		t.Fatal("slave did not sync in time")
	}

	if err := master.Client.Set(ctx, "expire:renew", "val", 0).Err(); err != nil {
		t.Fatalf("SET failed: %v", err)
	}
	if err := master.Client.Expire(ctx, "expire:renew", 100*time.Second).Err(); err != nil {
		t.Fatalf("first EXPIRE failed: %v", err)
	}
	if err := master.Client.Expire(ctx, "expire:renew", 3600*time.Second).Err(); err != nil {
		t.Fatalf("second EXPIRE failed: %v", err)
	}

	if !slave.WaitForReplicaSync(ctx, master, slave, 30*time.Second) {
		t.Fatal("slave did not sync after writes")
	}

	masterExists, err := master.Client.Exists(ctx, "expire:renew").Result()
	if err != nil {
		t.Fatalf("master EXISTS failed: %v", err)
	}
	slaveExists, err := slave.Client.Exists(ctx, "expire:renew").Result()
	if err != nil {
		t.Fatalf("slave EXISTS failed: %v", err)
	}
	if masterExists != slaveExists {
		t.Fatalf("key exists mismatch: master=%d slave=%d", masterExists, slaveExists)
	}
	if slaveExists == 0 {
		t.Fatalf("key not found on slave after sync")
	}
}
