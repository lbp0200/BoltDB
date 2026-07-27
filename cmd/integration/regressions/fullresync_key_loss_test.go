package regressions

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestRegressionFullresyncKeyLoss verifies that after FULLRESYNC, the slave
// has exactly the same keys (all types) as the master.
//
// This is a diagnostic test for TODO(C): investigating why the slave may still
// be missing keys that the master has after a full resynchronization.
//
// It writes a comprehensive set of keys across ALL supported data types,
// triggers a FULLRESYNC, then systematically compares every key's existence,
// type, and value between master and slave.
func TestRegressionFullresyncKeyLoss(t *testing.T) {
	master := StartRegression(t)
	defer master.Close()

	slave := StartRegression(t)
	defer slave.Close()

	ctx := context.Background()

	// ========================================================================
	// PHASE 1: Write known keys of every data type to master
	// ========================================================================

	// We track expected keys in a deterministic set to compare after replication.
	// All values are compared via fmt.Sprint — type-safe for a diagnostic test.
	type expectedKey struct {
		key       string
		typeStr   string // "string", "hash", "list", "set", "zset", "stream", "geo"
		masterVal interface{}
	}
	var expected []expectedKey

	// --- STRING ---
	stringKeys := []string{"s:alpha", "s:beta", "s:数字", "s:emoji:🔥"}
	for _, k := range stringKeys {
		master.Client.Set(ctx, k, "value:"+k, 0)
		expected = append(expected, expectedKey{key: k, typeStr: "string"})
	}

	// --- STRING with TTL ---
	master.Client.Set(ctx, "s:ttl", "will-expire", 10*time.Minute)
	expected = append(expected, expectedKey{key: "s:ttl", typeStr: "string"})

	// --- HASH ---
	master.Client.HSet(ctx, "h:user", map[string]string{
		"name": "alice",
		"age":  "30",
		"city": "tokyo",
	})
	expected = append(expected, expectedKey{key: "h:user", typeStr: "hash"})

	master.Client.HSet(ctx, "h:large", "f1", "v1", "f2", "v2", "f3", "v3", "f4", "v4", "f5", "v5")
	expected = append(expected, expectedKey{key: "h:large", typeStr: "hash"})

	// --- LIST ---
	for _, v := range []string{"a", "b", "c", "d", "e"} {
		master.Client.RPush(ctx, "l:letters", v)
	}
	expected = append(expected, expectedKey{key: "l:letters", typeStr: "list"})

	// --- SET ---
	for _, v := range []string{"x", "y", "z", "w"} {
		master.Client.SAdd(ctx, "set:coords", v)
	}
	expected = append(expected, expectedKey{key: "set:coords", typeStr: "set"})

	// --- ZSET ---
	master.Client.ZAdd(ctx, "z:scores", redis.Z{Score: 95.5, Member: "alice"}, redis.Z{Score: 88.0, Member: "bob"}, redis.Z{Score: 72.3, Member: "carol"})
	expected = append(expected, expectedKey{key: "z:scores", typeStr: "zset"})

	// --- STREAM ---
	for i := 0; i < 3; i++ {
		master.Client.XAdd(ctx, &redis.XAddArgs{
			Stream: "stream:events",
			ID:     fmt.Sprintf("100-%d", i),
			Values: map[string]interface{}{"event": fmt.Sprintf("e%d", i), "ts": fmt.Sprintf("%d", i)},
		})
	}
	expected = append(expected, expectedKey{key: "stream:events", typeStr: "stream"})

	// --- GEO ---
	master.Client.GeoAdd(ctx, "geo:places", &redis.GeoLocation{Name: "beijing", Latitude: 39.90, Longitude: 116.40})
	master.Client.GeoAdd(ctx, "geo:places", &redis.GeoLocation{Name: "shanghai", Latitude: 31.23, Longitude: 121.47})
	expected = append(expected, expectedKey{key: "geo:places", typeStr: "geo"})

	// --- EMPTY data type corner cases ---
	// Write and then delete a key (should NOT appear on either side)
	master.Client.Set(ctx, "s:deleted", "gone", 0)
	master.Client.Del(ctx, "s:deleted")

	// Write a key then overwrite its value
	master.Client.Set(ctx, "s:overwritten", "old", 0)
	master.Client.Set(ctx, "s:overwritten", "new", 0)
	expected = append(expected, expectedKey{key: "s:overwritten", typeStr: "string"})

	t.Logf("Wrote %d expected keys to master", len(expected))

	// ========================================================================
	// PHASE 2: Connect slave (triggers FULLRESYNC)
	// ========================================================================

	if err := slave.MakeSlave(master.Addr); err != nil {
		t.Fatalf("MakeSlave failed: %v", err)
	}
	defer slave.StopSlave()

	if !slave.WaitForReplicaSync(ctx, master, slave, 60*time.Second) {
		t.Fatalf("slave did not sync within timeout")
	}

	t.Logf("Slave synced: master_offset=%d slave_offset=%d",
		master.GetMasterOffset(), slave.GetSlaveOffset())

	// ========================================================================
	// PHASE 3: Compare every expected key between master and slave
	// ========================================================================

	type keyDiff struct {
		key       string
		issue     string
		masterVal interface{}
		slaveVal  interface{}
	}
	var diffs []keyDiff

	for _, ek := range expected {
		// 3a. Check key EXISTS on master (sanity — should always be true)
		mExists, err := master.Client.Exists(ctx, ek.key).Result()
		if err != nil {
			t.Fatalf("master EXISTS %s failed: %v", ek.key, err)
		}
		if mExists == 0 {
			t.Fatalf("BUG in test: key %s should exist on master", ek.key)
		}

		// 3b. Check key EXISTS on slave
		sExists, err := slave.Client.Exists(ctx, ek.key).Result()
		if err != nil {
			t.Fatalf("slave EXISTS %s failed: %v", ek.key, err)
		}
		if sExists == 0 {
			diffs = append(diffs, keyDiff{
				key:   ek.key,
				issue: fmt.Sprintf("MISSING on slave (exists=%d)", sExists),
			})
			continue
		}

		// 3c. Check TYPE matches
		mType, err := master.Client.Type(ctx, ek.key).Result()
		if err != nil {
			t.Fatalf("master TYPE %s failed: %v", ek.key, err)
		}
		sType, err := slave.Client.Type(ctx, ek.key).Result()
		if err != nil {
			t.Fatalf("slave TYPE %s failed: %v", ek.key, err)
		}
		if mType != sType {
			diffs = append(diffs, keyDiff{
				key:   ek.key,
				issue: fmt.Sprintf("TYPE mismatch: master=%s slave=%s", mType, sType),
			})
			continue
		}

		// 3d. Check VALUE matches (using type-specific reading)
		var masterVal, slaveVal interface{}
		var readErr error

		switch ek.typeStr {
		case "string":
			masterVal, readErr = master.Client.Get(ctx, ek.key).Result()
			if readErr != nil {
				t.Fatalf("master GET %s failed: %v", ek.key, readErr)
			}
			slaveVal, readErr = slave.Client.Get(ctx, ek.key).Result()
			if readErr != nil {
				diffs = append(diffs, keyDiff{
					key:   ek.key,
					issue: fmt.Sprintf("slave GET failed: %v", readErr),
				})
				continue
			}

		case "hash":
			masterVal, readErr = master.Client.HGetAll(ctx, ek.key).Result()
			if readErr != nil {
				t.Fatalf("master HGETALL %s failed: %v", ek.key, readErr)
			}
			slaveVal, readErr = slave.Client.HGetAll(ctx, ek.key).Result()
			if readErr != nil {
				diffs = append(diffs, keyDiff{
					key:   ek.key,
					issue: fmt.Sprintf("slave HGETALL failed: %v", readErr),
				})
				continue
			}

		case "list":
			masterVal, readErr = master.Client.LRange(ctx, ek.key, 0, -1).Result()
			if readErr != nil {
				t.Fatalf("master LRANGE %s failed: %v", ek.key, readErr)
			}
			slaveVal, readErr = slave.Client.LRange(ctx, ek.key, 0, -1).Result()
			if readErr != nil {
				diffs = append(diffs, keyDiff{
					key:   ek.key,
					issue: fmt.Sprintf("slave LRANGE failed: %v", readErr),
				})
				continue
			}

		case "set":
			masterVal, readErr = master.Client.SMembers(ctx, ek.key).Result()
			if readErr != nil {
				t.Fatalf("master SMEMBERS %s failed: %v", ek.key, readErr)
			}
			slaveVal, readErr = slave.Client.SMembers(ctx, ek.key).Result()
			if readErr != nil {
				diffs = append(diffs, keyDiff{
					key:   ek.key,
					issue: fmt.Sprintf("slave SMEMBERS failed: %v", readErr),
				})
				continue
			}

		case "zset":
			// Use ZRANGE (members only) + ZSCORE for each member.
			// ZRangeWithScores output can differ by go-redis version,
			// so we compare members and individual scores separately.
			mMembers, mErr := master.Client.ZRange(ctx, ek.key, 0, -1).Result()
			if mErr != nil {
				t.Fatalf("master ZRANGE %s failed: %v", ek.key, mErr)
			}
			sMembers, sErr := slave.Client.ZRange(ctx, ek.key, 0, -1).Result()
			if sErr != nil {
				diffs = append(diffs, keyDiff{
					key:   ek.key,
					issue: fmt.Sprintf("slave ZRANGE failed: %v", sErr),
				})
				continue
			}
			if fmt.Sprint(mMembers) != fmt.Sprint(sMembers) {
				diffs = append(diffs, keyDiff{
					key:       ek.key,
					issue:     fmt.Sprintf("member set mismatch: master=%v slave=%v", mMembers, sMembers),
					masterVal: mMembers,
					slaveVal:  sMembers,
				})
				continue
			}
			// ZSCORE each member individually
			for _, member := range mMembers {
				mScore, mErr := master.Client.ZScore(ctx, ek.key, member).Result()
				if mErr != nil {
					t.Fatalf("master ZSCORE %s %s failed: %v", ek.key, member, mErr)
				}
				sScore, sErr := slave.Client.ZScore(ctx, ek.key, member).Result()
				if sErr != nil {
					diffs = append(diffs, keyDiff{
						key:   ek.key,
						issue: fmt.Sprintf("slave ZSCORE %s failed: %v", member, sErr),
					})
					continue
				}
				if fmt.Sprint(mScore) != fmt.Sprint(sScore) {
					diffs = append(diffs, keyDiff{
						key:       ek.key,
						issue:     fmt.Sprintf("ZSCORE %s mismatch", member),
						masterVal: mScore,
						slaveVal:  sScore,
					})
				}
			}
			// Skip the generic fmt.Sprint comparison for zset
			continue

		case "stream":
			masterVal, readErr = master.Client.XRange(ctx, ek.key, "-", "+").Result()
			if readErr != nil {
				t.Fatalf("master XRANGE %s failed: %v", ek.key, readErr)
			}
			slaveVal, readErr = slave.Client.XRange(ctx, ek.key, "-", "+").Result()
			if readErr != nil {
				diffs = append(diffs, keyDiff{
					key:   ek.key,
					issue: fmt.Sprintf("slave XRANGE failed: %v", readErr),
				})
				continue
			}

		case "geo":
			// Use GEOPOS with specific members and check coordinates.
			members := []string{"beijing", "shanghai"}
			mPos, mErr := master.Client.GeoPos(ctx, ek.key, members...).Result()
			if mErr != nil {
				t.Fatalf("master GEOPOS %s failed: %v", ek.key, mErr)
			}
			sPos, sErr := slave.Client.GeoPos(ctx, ek.key, members...).Result()
			if sErr != nil {
				diffs = append(diffs, keyDiff{
					key:   ek.key,
					issue: fmt.Sprintf("slave GEOPOS failed: %v", sErr),
				})
				continue
			}
			if len(mPos) != len(sPos) {
				diffs = append(diffs, keyDiff{
					key:       ek.key,
					issue:     fmt.Sprintf("position count mismatch: master=%d slave=%d", len(mPos), len(sPos)),
					masterVal: mPos,
					slaveVal:  sPos,
				})
				continue
			}
			for i, name := range members {
				if mPos[i] == nil && sPos[i] == nil {
					continue
				}
				if mPos[i] == nil || sPos[i] == nil {
					diffs = append(diffs, keyDiff{
						key:       ek.key,
						issue:     fmt.Sprintf("%s: one side has nil position", name),
						masterVal: mPos[i],
						slaveVal:  sPos[i],
					})
					continue
				}
				latDiff := mPos[i].Latitude - sPos[i].Latitude
				lonDiff := mPos[i].Longitude - sPos[i].Longitude
				if latDiff*latDiff+lonDiff*lonDiff > 0.0001 {
					diffs = append(diffs, keyDiff{
						key:       ek.key,
						issue:     fmt.Sprintf("%s: position mismatch", name),
						masterVal: fmt.Sprintf("lat=%.4f lon=%.4f", mPos[i].Latitude, mPos[i].Longitude),
						slaveVal:  fmt.Sprintf("lat=%.4f lon=%.4f", sPos[i].Latitude, sPos[i].Longitude),
					})
				}
			}
			// Skip the generic fmt.Sprint comparison (pointers)
			continue
		}

		// Generic value comparison for string/hash/list/set/stream
		if fmt.Sprint(masterVal) != fmt.Sprint(slaveVal) {
			diffs = append(diffs, keyDiff{
				key:       ek.key,
				issue:     "value mismatch",
				masterVal: masterVal,
				slaveVal:  slaveVal,
			})
		}
	}

	// ========================================================================
	// PHASE 4: SCAN all keys on both sides for keys NOT in our expected set
	// ========================================================================

	// This catches unexpected keys (orphan data) and also verifies SCAN
	// consistency under replication.
	masterKeys := scanAllKeys(ctx, t, master.Client, "master")
	slaveKeys := scanAllKeys(ctx, t, slave.Client, "slave")

	// Build a set of expected key names
	expectedSet := make(map[string]bool, len(expected))
	for _, ek := range expected {
		expectedSet[ek.key] = true
	}

	// Keys on master but not on slave
	for k := range masterKeys {
		if !slaveKeys[k] {
			diffs = append(diffs, keyDiff{
				key:   k,
				issue: "key exists on master but NOT on slave (SCAN comparison)",
			})
		}
	}

	// Keys on slave but not on master (ghost keys — should not happen)
	for k := range slaveKeys {
		if !masterKeys[k] {
			diffs = append(diffs, keyDiff{
				key:   k,
				issue: "key exists on slave but NOT on master (ghost key)",
			})
		}
	}

	// ========================================================================
	// REPORT
	// ========================================================================

	t.Logf("SCAN results: master has %d keys, slave has %d keys", len(masterKeys), len(slaveKeys))

	if len(diffs) > 0 {
		t.Errorf("Found %d difference(s) between master and slave after FULLRESYNC:", len(diffs))
		for i, d := range diffs {
			t.Errorf("  [%d] key=%q issue=%s", i+1, d.key, d.issue)
			if d.masterVal != nil {
				t.Errorf("       master=%v", d.masterVal)
			}
			if d.slaveVal != nil {
				t.Errorf("       slave=%v", d.slaveVal)
			}
		}
	} else {
		t.Logf("✅ ALL %d expected keys verified: master ≡ slave (types + values match)", len(expected))
	}
}

// scanAllKeys returns the set of all keys visible to SCAN on the given client.
func scanAllKeys(ctx context.Context, t *testing.T, client *redis.Client, label string) map[string]bool {
	t.Helper()
	keys := make(map[string]bool)
	iter := client.Scan(ctx, 0, "*", 10000).Iterator()
	count := 0
	for iter.Next(ctx) {
		keys[iter.Val()] = true
		count++
	}
	if err := iter.Err(); err != nil {
		t.Fatalf("%s SCAN iteration failed: %v", label, err)
	}
	return keys
}
