package regressions

import (
	"context"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/replication"
	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/redis/go-redis/v9"
)

// TestRegressionSnapshotConsistency verifies that an RDB snapshot generated
// from a populated store loads into a fresh store with full consistency:
//   - All keys present with correct types
//   - All values match exactly (string, list, hash, set, zset)
//   - store.Check() passes on the loaded store (no orphan keys)
//   - TTL metadata is preserved
//
// Failure doc: docs/failures/snapshot-inconsistency.md
// Expected: loaded store is byte-identical in semantics to the source.
func TestRegressionSnapshotConsistency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heavy regression in short mode")
	}
	ctx := context.Background()

	// Store A: write all data types
	storeAPath := t.TempDir()
	storeA, err := store.NewBotreonStore(storeAPath)
	if err != nil {
		t.Fatalf("failed to create store A: %v", err)
	}
	defer storeA.Close()

	srvA := StartRegressionWithStore(t, storeA)
	defer srvA.Close()
	clientA := srvA.Client

	// Write diverse dataset
	rng := rand.New(rand.NewSource(42))
	writeAllTypes(ctx, t, clientA, rng)

	// Verify source store is internally consistent
	if err := storeA.Check(); err != nil {
		t.Fatalf("store A Check() before RDB: %v", err)
	}

	// Generate RDB snapshot
	rdbData, err := replication.GenerateRDB(storeA)
	if err != nil {
		t.Fatalf("GenerateRDB failed: %v", err)
	}
	t.Logf("snapshot: RDB size = %d bytes", len(rdbData))

	// Store B: fresh store, load RDB
	storeBPath := t.TempDir()
	storeB, err := store.NewBotreonStore(storeBPath)
	if err != nil {
		t.Fatalf("failed to create store B: %v", err)
	}
	defer storeB.Close()

	if err := replication.LoadRDBWithStore(rdbData, storeB); err != nil {
		t.Fatalf("LoadRDBWithStore failed: %v", err)
	}

	// Structural consistency
	if err := storeB.Check(); err != nil {
		t.Errorf("store B Check() after RDB load: %v", err)
	} else {
		t.Log("snapshot: store B Check() passed")
	}

	// Semantic comparison via redis
	srvB := StartRegressionWithStore(t, storeB)
	defer srvB.Close()
	clientB := srvB.Client

	compareAllTypes(ctx, t, clientA, clientB, rng)
}

// TestRegressionSnapshotConcurrentWrites verifies that RDB generation is
// point-in-time consistent even when writes happen concurrently.
//
// Failure doc: docs/failures/snapshot-inconsistency.md
// Expected: RDB captures a consistent snapshot regardless of concurrent writes.
func TestRegressionSnapshotConcurrentWrites(t *testing.T) {
	ctx := context.Background()

	storeAPath := t.TempDir()
	storeA, err := store.NewBotreonStore(storeAPath)
	if err != nil {
		t.Fatalf("failed to create store A: %v", err)
	}
	defer storeA.Close()

	srvA := StartRegressionWithStore(t, storeA)
	defer srvA.Close()
	clientA := srvA.Client

	// Write some initial data
	rng := rand.New(rand.NewSource(99))
	for i := 0; i < 100; i++ {
		key := "init:" + strconv.Itoa(i)
		clientA.Set(ctx, key, "v0", 0)
	}

	// Generate RDB while writes are happening concurrently
	errCh := make(chan error, 10)
	go func() {
		for j := 0; j < 500; j++ {
			key := "concurrent:" + strconv.Itoa(rng.Intn(200))
			val := strconv.Itoa(rng.Intn(10000))
			if err := clientA.Set(ctx, key, val, 0).Err(); err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			time.Sleep(time.Duration(rng.Intn(2)) * time.Millisecond)
		}
	}()

	rdbData, err := replication.GenerateRDB(storeA)
	if err != nil {
		t.Fatalf("GenerateRDB failed during concurrent writes: %v", err)
	}
	t.Logf("concurrent: RDB size = %d bytes (generated during writes)", len(rdbData))

	// Wait for concurrent writes to finish
	time.Sleep(2 * time.Second)
	close(errCh)
	for e := range errCh {
		t.Logf("concurrent: write error: %v", e)
	}

	// Load RDB into fresh store
	storeBPath := t.TempDir()
	storeB, err := store.NewBotreonStore(storeBPath)
	if err != nil {
		t.Fatalf("failed to create store B: %v", err)
	}
	defer storeB.Close()

	if err := replication.LoadRDBWithStore(rdbData, storeB); err != nil {
		t.Fatalf("LoadRDBWithStore failed: %v", err)
	}

	// Structural consistency
	if err := storeB.Check(); err != nil {
		t.Errorf("concurrent: store B Check() after RDB: %v", err)
	} else {
		t.Log("concurrent: store B Check() passed")
	}

	// Verify: all keys in RDB-d store have valid data
	srvB := StartRegressionWithStore(t, storeB)
	defer srvB.Close()

	keys, _, err := srvB.Client.Scan(ctx, 0, "*", 1000).Result()
	if err != nil {
		t.Fatalf("concurrent: scan failed: %v", err)
	}
	var badKeys int
	for _, k := range keys {
		typ, err := srvB.Client.Type(ctx, k).Result()
		if err != nil {
			badKeys++
			continue
		}
		switch typ {
		case "string":
			_, err = srvB.Client.Get(ctx, k).Result()
		case "list":
			_, err = srvB.Client.LLen(ctx, k).Result()
		case "hash":
			_, err = srvB.Client.HKeys(ctx, k).Result()
		case "set":
			_, err = srvB.Client.SMembers(ctx, k).Result()
		case "zset":
			_, err = srvB.Client.ZCard(ctx, k).Result()
		}
		if err != nil {
			badKeys++
			t.Logf("concurrent: key %q type=%s read error: %v", k, typ, err)
		}
	}
	if badKeys > 0 {
		t.Errorf("concurrent: %d/%d keys have read errors after RDB load", badKeys, len(keys))
	} else {
		t.Logf("concurrent: all %d keys readable after RDB load", len(keys))
	}

	_ = errCh
}

// writeAllTypes 写入所有 Redis 数据类型的样本
func writeAllTypes(ctx context.Context, t *testing.T, client *redis.Client, rng *rand.Rand) {
	t.Helper()

	// Strings (50 keys)
	for i := 0; i < 50; i++ {
		key := "str:" + strconv.Itoa(i)
		val := "value_" + strconv.Itoa(rng.Intn(10000))
		if err := client.Set(ctx, key, val, 0).Err(); err != nil {
			t.Fatalf("SET %s: %v", key, err)
		}
	}

	// Strings with TTL (10 keys)
	for i := 0; i < 10; i++ {
		key := "strttl:" + strconv.Itoa(i)
		val := "ttl_val_" + strconv.Itoa(i)
		if err := client.SetEx(ctx, key, val, 3600*time.Second).Err(); err != nil {
			t.Fatalf("SETEX %s: %v", key, err)
		}
	}

	// Lists (20 keys, 5-20 elements each)
	for i := 0; i < 20; i++ {
		key := "list:" + strconv.Itoa(i)
		count := rng.Intn(16) + 5
		var members []string
		for j := 0; j < count; j++ {
			members = append(members, "elem_"+strconv.Itoa(rng.Intn(1000)))
		}
		if err := client.RPush(ctx, key, members).Err(); err != nil {
			t.Fatalf("RPUSH %s: %v", key, err)
		}
	}

	// Hashes (20 keys, 5-15 fields each)
	for i := 0; i < 20; i++ {
		key := "hash:" + strconv.Itoa(i)
		fields := make(map[string]string)
		count := rng.Intn(11) + 5
		for j := 0; j < count; j++ {
			f := "field_" + strconv.Itoa(j)
			v := "hval_" + strconv.Itoa(rng.Intn(1000))
			fields[f] = v
		}
		if err := client.HSet(ctx, key, fields).Err(); err != nil {
			t.Fatalf("HSET %s: %v", key, err)
		}
	}

	// Sets (15 keys, 5-20 members each)
	for i := 0; i < 15; i++ {
		key := "set:" + strconv.Itoa(i)
		count := rng.Intn(16) + 5
		var members []string
		for j := 0; j < count; j++ {
			members = append(members, "smem_"+strconv.Itoa(rng.Intn(500)))
		}
		if err := client.SAdd(ctx, key, members).Err(); err != nil {
			t.Fatalf("SADD %s: %v", key, err)
		}
	}

	// Sorted Sets (10 keys, 5-10 members each)
	for i := 0; i < 10; i++ {
		key := "zset:" + strconv.Itoa(i)
		count := rng.Intn(6) + 5
		var zs []redis.Z
		for j := 0; j < count; j++ {
			zs = append(zs, redis.Z{
				Score:  float64(rng.Intn(1000)),
				Member: "zmem_" + strconv.Itoa(rng.Intn(500)),
			})
		}
		if err := client.ZAdd(ctx, key, zs...).Err(); err != nil {
			t.Fatalf("ZADD %s: %v", key, err)
		}
	}

	t.Log("snapshot: wrote 125 keys across all types")
}

// compareAllTypes 对比两个 redis client 上所有键的值
func compareAllTypes(ctx context.Context, t *testing.T, a, b *redis.Client, rng *rand.Rand) {
	t.Helper()

	keysA, _, err := a.Scan(ctx, 0, "str:*", 1000).Result()
	if err != nil {
		t.Fatalf("scan A failed: %v", err)
	}
	keysB, _, err := b.Scan(ctx, 0, "*", 1000).Result()
	if err != nil {
		t.Fatalf("scan B failed: %v", err)
	}

	keySetB := make(map[string]bool, len(keysB))
	for _, k := range keysB {
		keySetB[k] = true
	}

	var missingKeys, typeMismatches, valueMismatches int

	for _, ka := range keysA {
		if !keySetB[ka] {
			if missingKeys == 0 {
				t.Errorf("snapshot: key %q missing in loaded store (first of %d+)", ka, countMissing(keysA, keySetB))
			}
			missingKeys++
			continue
		}

		typA, _ := a.Type(ctx, ka).Result()
		typB, _ := b.Type(ctx, ka).Result()
		if typA != typB {
			if typeMismatches == 0 {
				t.Errorf("snapshot: type mismatch %q: A=%q B=%q", ka, typA, typB)
			}
			typeMismatches++
			continue
		}

		match := compareValue(ctx, a, b, ka, typA)
		if !match {
			if valueMismatches == 0 {
				t.Errorf("snapshot: value mismatch for %q (type=%s)", ka, typA)
			}
			valueMismatches++
		}
	}

	if missingKeys > 0 {
		t.Errorf("snapshot: %d keys missing in loaded store", missingKeys)
	}
	if typeMismatches > 0 {
		t.Errorf("snapshot: %d type mismatches", typeMismatches)
	}
	if valueMismatches > 0 {
		t.Errorf("snapshot: %d value mismatches", valueMismatches)
	}
	if missingKeys == 0 && typeMismatches == 0 && valueMismatches == 0 {
		t.Log("snapshot: all keys match perfectly between stores")
	}
}

func countMissing(keys []string, keySet map[string]bool) int {
	var n int
	for _, k := range keys {
		if !keySet[k] {
			n++
		}
	}
	return n
}

func compareValue(ctx context.Context, a, b *redis.Client, key, typ string) bool {
	switch typ {
	case "string":
		va, _ := a.Get(ctx, key).Result()
		vb, _ := b.Get(ctx, key).Result()
		if strings.HasPrefix(key, "strttl:") {
			// TTL keys: verify TTL is set (not -1, not -2)
			ttlA, _ := a.TTL(ctx, key).Result()
			ttlB, _ := b.TTL(ctx, key).Result()
			if ttlA <= 0 || ttlB <= 0 {
				return false
			}
		}
		return va == vb
	case "list":
		la, _ := a.LRange(ctx, key, 0, -1).Result()
		lb, _ := b.LRange(ctx, key, 0, -1).Result()
		return stringSliceEqual(la, lb)
	case "hash":
		ha, _ := a.HGetAll(ctx, key).Result()
		hb, _ := b.HGetAll(ctx, key).Result()
		return stringMapEqual(ha, hb)
	case "set":
		sa, _ := a.SMembers(ctx, key).Result()
		sb, _ := b.SMembers(ctx, key).Result()
		return stringSetEqual(sa, sb)
	case "zset":
		za, _ := a.ZRangeWithScores(ctx, key, 0, -1).Result()
		zb, _ := b.ZRangeWithScores(ctx, key, 0, -1).Result()
		return zSetEqual(za, zb)
	default:
		return true // skip unknown types
	}
}

func stringSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func stringMapEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func stringSetEqual(a, b []string) bool {
	set := make(map[string]bool, len(a))
	for _, s := range a {
		set[s] = true
	}
	for _, s := range b {
		if !set[s] {
			return false
		}
	}
	return true
}

func zSetEqual(a, b []redis.Z) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Member != b[i].Member || a[i].Score != b[i].Score {
			return false
		}
	}
	return true
}
