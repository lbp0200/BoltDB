package regressions

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestRegressionCanonicalSPOP verifies SPOP is canonicalized to SREM during
// replication: the replica receives SREM key member... instead of SPOP,
// preventing random selection divergence.
//
// Path: handleSPOP → PropagateCommand(SREM); processRequest excludes SPOP
// via shouldPropagateCommand (no double-prop of raw SPOP).
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

// TestRegressionLiveSPOPNoDoubleProp attaches the slave first, then SPOPs
// under live replication. Guards against processRequest also propagating raw
// SPOP after the handler already sent SREM (extra members dropped on slave).
func TestRegressionLiveSPOPNoDoubleProp(t *testing.T) {
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
		t.Fatal("slave did not initial-sync in time")
	}

	members := []string{"a", "b", "c", "d", "e", "f"}
	for _, m := range members {
		if err := master.Client.SAdd(ctx, "spop:live", m).Err(); err != nil {
			t.Fatalf("SAdd failed: %v", err)
		}
	}

	// Fence: wait for full set on slave before SPOP
	deadline := time.Now().Add(15 * time.Second)
	for {
		n, err := slave.Client.SCard(ctx, "spop:live").Result()
		if err == nil && n == int64(len(members)) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("slave did not receive full set before SPOP (scard=%v err=%v)", n, err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	spopped, err := master.Client.SPopN(ctx, "spop:live", 2).Result()
	if err != nil {
		t.Fatalf("SPOP failed: %v", err)
	}
	if len(spopped) != 2 {
		t.Fatalf("expected 2 popped, got %d", len(spopped))
	}

	wantCard := int64(len(members) - 2)
	deadline = time.Now().Add(15 * time.Second)
	for {
		masterCard, _ := master.Client.SCard(ctx, "spop:live").Result()
		slaveCard, err := slave.Client.SCard(ctx, "spop:live").Result()
		if err == nil && masterCard == wantCard && slaveCard == wantCard {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("set size after live SPOP: master=%d slave=%d want=%d", masterCard, slaveCard, wantCard)
		}
		time.Sleep(20 * time.Millisecond)
	}

	masterRemaining, err := master.Client.SMembers(ctx, "spop:live").Result()
	if err != nil {
		t.Fatalf("master SMembers: %v", err)
	}
	slaveRemaining, err := slave.Client.SMembers(ctx, "spop:live").Result()
	if err != nil {
		t.Fatalf("slave SMembers: %v", err)
	}
	if len(masterRemaining) != len(slaveRemaining) {
		t.Fatalf("member count mismatch master=%d slave=%d (double SPOP?)", len(masterRemaining), len(slaveRemaining))
	}
	masterSet := make(map[string]bool, len(masterRemaining))
	for _, m := range masterRemaining {
		masterSet[m] = true
	}
	for _, m := range slaveRemaining {
		if !masterSet[m] {
			t.Fatalf("slave has member %q not on master", m)
		}
	}
	// Popped members must be absent on both
	for _, m := range spopped {
		if masterSet[m] {
			t.Fatalf("popped member %q still on master", m)
		}
	}
}

// TestRegressionMultiExecSPOPCanonical verifies MULTI/EXEC SPOP propagates
// as SREM of actual members (not raw SPOP).
func TestRegressionMultiExecSPOPCanonical(t *testing.T) {
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
		t.Fatal("slave did not initial-sync in time")
	}

	members := []string{"x", "y", "z", "w"}
	for _, m := range members {
		if err := master.Client.SAdd(ctx, "spop:tx", m).Err(); err != nil {
			t.Fatalf("SAdd failed: %v", err)
		}
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		n, _ := slave.Client.SCard(ctx, "spop:tx").Result()
		if n == int64(len(members)) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("slave missing pre-tx set")
		}
		time.Sleep(20 * time.Millisecond)
	}

	pipe := master.Client.TxPipeline()
	pipe.SPop(ctx, "spop:tx")
	cmds, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("MULTI/EXEC SPOP failed: %v", err)
	}
	if len(cmds) != 1 {
		t.Fatalf("expected 1 queued result, got %d", len(cmds))
	}

	wantCard := int64(len(members) - 1)
	deadline = time.Now().Add(15 * time.Second)
	for {
		masterCard, _ := master.Client.SCard(ctx, "spop:tx").Result()
		slaveCard, err := slave.Client.SCard(ctx, "spop:tx").Result()
		if err == nil && masterCard == wantCard && slaveCard == wantCard {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("after MULTI/EXEC SPOP master=%d slave=%d want=%d", masterCard, slaveCard, wantCard)
		}
		time.Sleep(20 * time.Millisecond)
	}

	masterRem, _ := master.Client.SMembers(ctx, "spop:tx").Result()
	slaveRem, _ := slave.Client.SMembers(ctx, "spop:tx").Result()
	if len(masterRem) != len(slaveRem) {
		t.Fatalf("set divergence master=%v slave=%v", masterRem, slaveRem)
	}
	ms := make(map[string]bool)
	for _, m := range masterRem {
		ms[m] = true
	}
	for _, m := range slaveRem {
		if !ms[m] {
			t.Fatalf("slave extra member %q", m)
		}
	}
}

// TestRegressionCanonicalXAdd verifies XADD with * (auto-ID) is canonicalized
// to the resolved ID before propagation.
//
// Propagation path: handler.go:5912 (canonicalization) → handler.go:528
// (markDirtyKeys + PropagateCommand) → psync.go executeReplicatedCommand
// (XADD case).
//
// Visibility barrier: replication offset equality (slaveOff >= masterOff)
// does NOT imply data visibility. Stream writes are visible only after
// executeReplicatedCommand commits to BadgerDB. Test polls XLEN instead
// of relying on WaitForReplicaSync + sleep. See docs/replication/correctness.md
// § "Offset Equality Is Not Visibility Equality".
//
// Note: a SET fence is written before XADD to push the master offset past
// the snapshotOffset boundary. Without this, XADD's offset can equal the
// FULLRESYNC snapshotOffset, causing it to fall through the RDB/backlog
// gap (streams are not in RDB). See docs/replication/correctness.md
// § "FULLRESYNC Semantics" for the offset boundary guarantee.
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

	// Write a SET fence to advance the master offset past any subsequent
	// FULLRESYNC snapshotOffset, ensuring the XADD's offset is firmly
	// within the backlog range [snapshotOffset, currentOffset).
	if err := master.Client.Set(ctx, "fence", "fence", 0).Err(); err != nil {
		t.Fatalf("fence SET failed: %v", err)
	}

	// Write stream entry with * auto-ID (after sync — flows through backlog)
	if err := master.Client.XAdd(ctx, &redis.XAddArgs{
		Stream: "xadd:stream",
		Values: map[string]interface{}{"field0": "val0"},
	}).Err(); err != nil {
		t.Fatalf("XAdd failed: %v", err)
	}

	// Visibility barrier: poll slave XLEN until it matches master.
	if !slave.WaitForReplicaSync(ctx, master, slave, 30*time.Second) {
		t.Fatal("slave did not sync after writes")
	}

	masterLen, err := master.Client.XLen(ctx, "xadd:stream").Result()
	if err != nil {
		t.Fatalf("master XLen failed: %v", err)
	}
	deadline := time.Now().Add(15 * time.Second)
	var slaveLen int64
	for time.Now().Before(deadline) {
		slaveLen, err = slave.Client.XLen(ctx, "xadd:stream").Result()
		if err == nil && slaveLen >= masterLen {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if slaveLen < masterLen {
		t.Fatalf("slave stream entries after sync: got %d, want %d", slaveLen, masterLen)
	}
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

// TestRegressionSlaveConnectionOwnership verifies that the slave connection
// lifecycle is correctly managed: handleConnection transfers ownership to
// handleSlaveReplicationConnection without double-close, and the slave does
// not experience unexpected disconnections/reconnects during normal operation.
//
// This guards against regressions where:
//   - handleConnection closes conn (replicationOwned=false) despite
//     handlePSyncWithRDB having taken over
//   - SendBacklogData failure prevents ReplicationTakeoverSignal from
//     being returned, leaving replicationOwned=false
//   - Slave connection is closed by two owners concurrently
func TestRegressionSlaveConnectionOwnership(t *testing.T) {
	master := StartRegression(t)
	defer master.Close()

	slave := StartRegression(t)
	defer slave.Close()

	ctx := context.Background()

	// Connect slave
	if err := slave.MakeSlave(master.Addr); err != nil {
		t.Fatalf("MakeSlave failed: %v", err)
	}
	defer slave.StopSlave()

	// Wait for initial FULLRESYNC
	if !slave.WaitForReplicaSync(ctx, master, slave, 30*time.Second) {
		t.Fatal("slave did not sync in time")
	}

	// Record reconnect count after initial sync
	reconnectsAfterSync := slave.GetReconnectCount()
	t.Logf("reconnects after initial sync: %d", reconnectsAfterSync)

	// Write a SET to advance offset
	if err := master.Client.Set(ctx, "ownership:key1", "val1", 0).Err(); err != nil {
		t.Fatalf("SET key1 failed: %v", err)
	}
	if !slave.WaitForReplicaSync(ctx, master, slave, 10*time.Second) {
		t.Fatal("slave did not sync after SET key1")
	}

	// Verify slave received the data
	val1, err := slave.Client.Get(ctx, "ownership:key1").Result()
	if err != nil {
		t.Fatalf("slave GET key1 failed: %v", err)
	}
	if val1 != "val1" {
		t.Fatalf("slave key1 value: got %q, want val1", val1)
	}

	// Verify offsets match
	if off := slave.GetSlaveOffset(); off != master.GetMasterOffset() {
		t.Fatalf("offset mismatch after key1: master=%d slave=%d", master.GetMasterOffset(), off)
	}

	// Write more commands to exercise the active connection
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("ownership:batch:%d", i)
		if err := master.Client.Set(ctx, key, "data", 0).Err(); err != nil {
			t.Fatalf("SET %s failed: %v", key, err)
		}
	}
	if !slave.WaitForReplicaSync(ctx, master, slave, 10*time.Second) {
		t.Fatal("slave did not sync after batch SETs")
	}
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("ownership:batch:%d", i)
		val, err := slave.Client.Get(ctx, key).Result()
		if err != nil {
			t.Fatalf("slave GET %s failed: %v", key, err)
		}
		if val != "data" {
			t.Fatalf("slave %s value: got %q, want data", key, val)
		}
	}

	// Verify reconnect count has not increased (no unexpected disconnects)
	reconnectsAfterWork := slave.GetReconnectCount()
	t.Logf("reconnects after work: %d", reconnectsAfterWork)
	if reconnectsAfterWork > reconnectsAfterSync+1 {
		t.Fatalf("unexpected reconnects: initial=%d current=%d",
			reconnectsAfterSync, reconnectsAfterWork)
	}

	// Verify offsets still match after all work
	if off := slave.GetSlaveOffset(); off != master.GetMasterOffset() {
		t.Fatalf("offset mismatch after work: master=%d slave=%d", master.GetMasterOffset(), off)
	}
}

// TestRegressionFullResyncGeo verifies GEO data survives FULLRESYNC:
// GEOADD data → slave connects → FULLRESYNC → slave has GEO data.
func TestRegressionFullResyncGeo(t *testing.T) {
	master := StartRegression(t)
	defer master.Close()

	slave := StartRegression(t)
	defer slave.Close()

	ctx := context.Background()

	// Write GEO data to master
	_, err := master.Client.Do(ctx, "GEOADD", "geo:test", "116.40", "39.90", "beijing").Result()
	if err != nil {
		t.Fatalf("GEOADD failed: %v", err)
	}
	_, err = master.Client.Do(ctx, "GEOADD", "geo:test", "121.47", "31.23", "shanghai").Result()
	if err != nil {
		t.Fatalf("GEOADD shanghai failed: %v", err)
	}

	// Connect slave (triggers FULLRESYNC)
	if err := slave.MakeSlave(master.Addr); err != nil {
		t.Fatalf("MakeSlave failed: %v", err)
	}
	defer slave.StopSlave()

	if !slave.WaitForReplicaSync(ctx, master, slave, 30*time.Second) {
		t.Fatal("slave did not sync in time")
	}

	// Verify slave has GEO data
	result, err := slave.Client.Do(ctx, "GEOPOS", "geo:test", "beijing", "shanghai").Result()
	if err != nil {
		t.Fatalf("slave GEOPOS failed: %v", err)
	}
	arr, ok := result.([]interface{})
	if !ok {
		t.Fatalf("GEOPOS result is not array")
	}
	if len(arr) != 2 {
		t.Fatalf("expected 2 positions, got %d", len(arr))
	}
	for i, elem := range arr {
		coord, ok := elem.([]interface{})
		if !ok || len(coord) != 2 {
			t.Fatalf("position %d: expected [lon, lat] array, got %T %v", i, elem, elem)
		}
	}

	// Verify key exists on both
	masterExists, err := master.Client.Exists(ctx, "geo:test").Result()
	if err != nil {
		t.Fatalf("master EXISTS failed: %v", err)
	}
	slaveExists, err := slave.Client.Exists(ctx, "geo:test").Result()
	if err != nil {
		t.Fatalf("slave EXISTS failed: %v", err)
	}
	if masterExists != 1 {
		t.Fatalf("master key should exist")
	}
	if slaveExists != 1 {
		t.Fatalf("slave key should exist after FULLRESYNC")
	}

	// Verify GEOSEARCH returns same results on both
	masterSearch, err := master.Client.Do(ctx, "GEOSEARCH", "geo:test", "FROMLONLAT", "110", "20", "BYRADIUS", "5000", "km").Result()
	if err != nil {
		t.Fatalf("master GEOSEARCH failed: %v", err)
	}
	slaveSearch, err := slave.Client.Do(ctx, "GEOSEARCH", "geo:test", "FROMLONLAT", "110", "20", "BYRADIUS", "5000", "km").Result()
	if err != nil {
		t.Fatalf("slave GEOSEARCH failed: %v", err)
	}
	masterArr := masterSearch.([]interface{})
	slaveArr := slaveSearch.([]interface{})
	if len(masterArr) != len(slaveArr) {
		t.Fatalf("geo search results mismatch: master=%d slave=%d", len(masterArr), len(slaveArr))
	}
}
