package integration

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/backup"
	"github.com/lbp0200/BoltDB/internal/replication"
	"github.com/lbp0200/BoltDB/internal/server"
	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/redis/go-redis/v9"
	"github.com/zeebo/assert"
)

// retryTimeout is the default timeout for retryable operations during dataset comparison
const retryTimeout = 30 * time.Second

const soakReplDefaultDuration = 5 * time.Minute
const soakReplMaxDuration = 24 * time.Hour

func getSoakReplDuration() time.Duration {
	s := os.Getenv("SOAK_REPL_DURATION")
	if s == "" {
		return soakReplDefaultDuration
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return soakReplDefaultDuration
	}
	if d > soakReplMaxDuration {
		return soakReplMaxDuration
	}
	return d
}

// TestSoakReplication is a long-running master-slave replication soak test.
// It runs random command sequences on the master, randomly disconnects/reconnects
// the slave, and finally compares datasets for divergence.
//
// Usage:
//
//	go test -race -timeout 30m ./cmd/integration/ -run TestSoakReplication
//	SOAK_REPL_DURATION=3h go test -race -timeout 4h ./cmd/integration/ -run TestSoakReplication
//	SOAK_REPL_DURATION=30m SOAK_REPL_WRITERS=10 go test -race -timeout 40m ./cmd/integration/ -run TestSoakReplication
type soakReplEnv struct {
	masterClient *redis.Client
	slaveClient  *redis.Client
	masterDB     *store.BotreonStore
	slaveDB      *store.BotreonStore
	masterRepl   *replication.ReplicationManager
	slaveRepl    *replication.ReplicationManager
	cleanup      func()
}

func setupSoakReplication(t *testing.T) *soakReplEnv {
	t.Helper()
	var err error

	masterDBPath := t.TempDir()
	masterDB, err := store.NewBotreonStore(masterDBPath)
	if err != nil {
		t.Fatalf("failed to create master store: %v", err)
	}

	masterPubsub := store.NewPubSubManager()
	masterBackup := backup.NewBackupManager(masterDB, masterDBPath+"/backup")
	masterRepl := replication.NewReplicationManager(masterDB)

	masterHandler := &server.Handler{
		Db:          masterDB,
		Replication: masterRepl,
		Backup:      masterBackup,
		PubSub:      masterPubsub,
	}

	masterListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		masterDB.Close()
		t.Fatalf("failed to listen on master: %v", err)
	}

	go func() {
		_ = masterHandler.ServeTCP(masterListener)
	}()
	time.Sleep(50 * time.Millisecond)

	masterClient := redis.NewClient(&redis.Options{
		Addr: masterListener.Addr().String(),
	})
	ctx := context.Background()
	if _, err := masterClient.Ping(ctx).Result(); err != nil {
		masterListener.Close()
		masterDB.Close()
		t.Fatalf("failed to ping master: %v", err)
	}

	slaveDBPath := t.TempDir()
	slaveDB, err := store.NewBotreonStore(slaveDBPath)
	if err != nil {
		masterListener.Close()
		masterDB.Close()
		t.Fatalf("failed to create slave store: %v", err)
	}

	slavePubsub := store.NewPubSubManager()
	slaveBackup := backup.NewBackupManager(slaveDB, slaveDBPath+"/backup")
	slaveRepl := replication.NewReplicationManager(slaveDB)

	slaveHandler := &server.Handler{
		Db:          slaveDB,
		Replication: slaveRepl,
		Backup:      slaveBackup,
		PubSub:      slavePubsub,
	}

	slaveListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		masterListener.Close()
		masterDB.Close()
		slaveDB.Close()
		t.Fatalf("failed to listen on slave: %v", err)
	}

	go func() {
		_ = slaveHandler.ServeTCP(slaveListener)
	}()
	time.Sleep(50 * time.Millisecond)

	slaveClient := redis.NewClient(&redis.Options{
		Addr: slaveListener.Addr().String(),
	})
	if _, err := slaveClient.Ping(ctx).Result(); err != nil {
		masterListener.Close()
		masterDB.Close()
		slaveListener.Close()
		slaveDB.Close()
		t.Fatalf("failed to ping slave: %v", err)
	}

	if err := replication.StartSlaveReplication(slaveRepl, slaveDB, masterListener.Addr().String()); err != nil {
		masterListener.Close()
		masterDB.Close()
		slaveListener.Close()
		slaveDB.Close()
		t.Fatalf("failed to start slave replication: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	cleanup := func() {
		slaveClient.Close()
		masterClient.Close()
		slaveListener.Close()
		masterListener.Close()
		slaveDB.Close()
		masterDB.Close()
	}

	return &soakReplEnv{
		masterClient: masterClient,
		slaveClient:  slaveClient,
		masterDB:     masterDB,
		slaveDB:      slaveDB,
		masterRepl:   masterRepl,
		slaveRepl:    slaveRepl,
		cleanup:      cleanup,
	}
}

func TestSoakReplication(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping soak replication test in short mode")
	}
	duration := getSoakReplDuration()
	writers := getSoakReplWriters()

	t.Logf("soak-repl: duration=%v, writers=%d", duration, writers)
	t.Logf("soak-repl: set SOAK_REPL_DURATION (e.g. SOAK_REPL_DURATION=3h) to extend")
	t.Logf("soak-repl: set SOAK_REPL_WRITERS (default=4) to change write concurrency")

	env := setupSoakReplication(t)
	defer env.cleanup()

	master := env.masterClient
	slave := env.slaveClient
	ctx := context.Background()

	// verify initial replication
	time.Sleep(200 * time.Millisecond)
	err := master.Set(ctx, "soak:init", "ok", 0).Err()
	assert.NoError(t, err)
	time.Sleep(200 * time.Millisecond)
	val, err := slave.Get(ctx, "soak:init").Result()
	assert.NoError(t, err)
	assert.Equal(t, "ok", val)

	baseline := runtime.NumGoroutine()

	// 压力监控（监控 master）
	pm := NewPressureMonitor(env.masterDB, env.masterRepl)
	if jdir := os.Getenv("SOAK_JSONL_DIR"); jdir != "" {
		os.MkdirAll(jdir, 0755)
		jpath := filepath.Join(jdir, fmt.Sprintf("soak-repl-%s.jsonl", time.Now().Format("20060102-150405")))
		if err := pm.SetJSONLPath(jpath); err != nil {
			t.Logf("soak-repl: failed to create JSONL %s: %v", jpath, err)
		} else {
			t.Logf("soak-repl: JSONL timeline → %s", jpath)
		}
	}
	soakCtx, soakCancel := context.WithTimeout(ctx, duration)
	defer soakCancel()
	pm.Start(soakCtx, 30*time.Second)

	errCh := make(chan error, writers*10)
	var wg sync.WaitGroup

	// writer goroutines
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					errCh <- fmt.Errorf("writer %d panicked: %v", id, r)
				}
			}()
			runSoakReplWriter(soakCtx, master, id, errCh)
		}(i)
	}

	// lifecycle chaos goroutine: periodically disconnect/reconnect slave
	wg.Add(1)
	go func() {
		defer wg.Done()
		runSoakReplLifecycle(soakCtx, t, master, slave, errCh)
	}()

	// drain errCh in background to prevent deadlock when buffer fills
	var errs []error
	errsDone := make(chan struct{})
	go func() {
		for err := range errCh {
			errs = append(errs, err)
		}
		close(errsDone)
	}()

	wg.Wait()
	close(errCh)
	<-errsDone
	if len(errs) > 0 {
		t.Errorf("soak-repl: %d errors during run (first 10 shown):", len(errs))
		for i, err := range errs {
			if i >= 10 {
				break
			}
			t.Errorf("  %v", err)
		}
	}

	// Wait for replication to stabilize after chaos
	time.Sleep(5 * time.Second)

	// 压力汇总 + 退化检查
	pm.LogSummary(t)
	level := pm.CheckDegradation(t, DefaultDegradationAssertion(), baseline)
	t.Logf("soak-repl: degradation level: %s", level)

	// 健康评分
	health := pm.HealthScore(baseline)
	t.Log(health.String())

	// Final verification: compare master and slave datasets
	t.Log("soak-repl: comparing master/slave datasets...")
	compareDatasets(t, master, slave)

	final := runtime.NumGoroutine()
	leak := final - baseline
	t.Logf("soak-repl: goroutine delta=%d (baseline=%d, final=%d)", leak, baseline, final)
	if leak > 50 {
		t.Errorf("goroutine leak after soak-repl: %d (baseline=%d, final=%d)", leak, baseline, final)
	}
}

func getSoakReplWriters() int {
	s := os.Getenv("SOAK_REPL_WRITERS")
	if s == "" {
		return 4
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return 4
	}
	if n > 20 {
		return 20
	}
	return n
}

func runSoakReplWriter(ctx context.Context, master *redis.Client, id int, errCh chan<- error) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))

	for ctx.Err() == nil {
		roll := rng.Intn(100)

		switch {
		case roll < 30:
			replSoakSetGet(ctx, master, rng, errCh)
		case roll < 45:
			replSoakList(ctx, master, rng, errCh)
		case roll < 55:
			replSoakHash(ctx, master, rng, errCh)
		case roll < 65:
			replSoakSet(ctx, master, rng, errCh)
		case roll < 72:
			replSoakZSet(ctx, master, rng, errCh)
		case roll < 80:
			replSoakCounter(ctx, master, rng, errCh)
		case roll < 85:
			replSoakTransaction(ctx, master, rng, errCh)
		case roll < 92:
			replSoakDelete(ctx, master, rng, errCh)
		default:
			replSoakTTL(ctx, master, rng, errCh)
		}

		// small pause between ops to avoid overwhelming
		time.Sleep(time.Duration(rng.Intn(10)) * time.Millisecond)
	}
}

func replSoakSetGet(ctx context.Context, master *redis.Client, rng *rand.Rand, errCh chan<- error) {
	key := fmt.Sprintf("soak:str:%d", rng.Intn(200))
	val := fmt.Sprintf("v:%d", rng.Intn(10000))
	if rng.Intn(4) == 0 {
		// SET with EX
		ttl := rng.Intn(300) + 10
		if err := master.SetEx(ctx, key, val, time.Duration(ttl)*time.Second).Err(); err != nil {
			errCh <- fmt.Errorf("SETEX %s: %w", key, err)
		}
	} else {
		if err := master.Set(ctx, key, val, 0).Err(); err != nil {
			errCh <- fmt.Errorf("SET %s: %w", key, err)
		}
	}
	if rng.Intn(3) == 0 {
		if _, err := master.Get(ctx, key).Result(); err != nil && err != redis.Nil {
			errCh <- fmt.Errorf("GET %s: %w", key, err)
		}
	}
	if rng.Intn(10) == 0 {
		if _, err := master.Append(ctx, key, "x").Result(); err != nil {
			errCh <- fmt.Errorf("APPEND %s: %w", key, err)
		}
	}
}

func replSoakList(ctx context.Context, master *redis.Client, rng *rand.Rand, errCh chan<- error) {
	key := fmt.Sprintf("soak:list:%d", rng.Intn(50))
	switch rng.Intn(6) {
	case 0, 1:
		val := fmt.Sprintf("lv%d", rng.Intn(1000))
		if _, err := master.LPush(ctx, key, val).Result(); err != nil {
			errCh <- fmt.Errorf("LPUSH %s: %w", key, err)
		}
	case 2, 3:
		val := fmt.Sprintf("rv%d", rng.Intn(1000))
		if _, err := master.RPush(ctx, key, val).Result(); err != nil {
			errCh <- fmt.Errorf("RPUSH %s: %w", key, err)
		}
	case 4:
		if _, err := master.LPop(ctx, key).Result(); err != nil && err != redis.Nil {
			errCh <- fmt.Errorf("LPOP %s: %w", key, err)
		}
	case 5:
		if _, err := master.RPop(ctx, key).Result(); err != nil && err != redis.Nil {
			errCh <- fmt.Errorf("RPOP %s: %w", key, err)
		}
	}
}

func replSoakHash(ctx context.Context, master *redis.Client, rng *rand.Rand, errCh chan<- error) {
	key := fmt.Sprintf("soak:hash:%d", rng.Intn(50))
	field := fmt.Sprintf("f%d", rng.Intn(20))
	val := fmt.Sprintf("hv%d", rng.Intn(1000))
	if rng.Intn(4) == 0 {
		if _, err := master.HDel(ctx, key, field).Result(); err != nil {
			errCh <- fmt.Errorf("HDEL %s: %w", key, err)
		}
	} else {
		if err := master.HSet(ctx, key, field, val).Err(); err != nil {
			errCh <- fmt.Errorf("HSET %s: %w", key, err)
		}
	}
}

func replSoakSet(ctx context.Context, master *redis.Client, rng *rand.Rand, errCh chan<- error) {
	key := fmt.Sprintf("soak:set:%d", rng.Intn(30))
	member := fmt.Sprintf("m%d", rng.Intn(50))
	if rng.Intn(3) == 0 {
		if _, err := master.SRem(ctx, key, member).Result(); err != nil {
			errCh <- fmt.Errorf("SREM %s: %w", key, err)
		}
	} else {
		if _, err := master.SAdd(ctx, key, member).Result(); err != nil {
			errCh <- fmt.Errorf("SADD %s: %w", key, err)
		}
	}
	if rng.Intn(5) == 0 {
		if _, err := master.SPop(ctx, key).Result(); err != nil && err != redis.Nil {
			errCh <- fmt.Errorf("SPOP %s: %w", key, err)
		}
	}
}

func replSoakZSet(ctx context.Context, master *redis.Client, rng *rand.Rand, errCh chan<- error) {
	key := fmt.Sprintf("soak:zset:%d", rng.Intn(30))
	member := fmt.Sprintf("zm%d", rng.Intn(50))
	score := rng.Float64() * 1000
	if rng.Intn(3) == 0 {
		if _, err := master.ZRem(ctx, key, member).Result(); err != nil {
			errCh <- fmt.Errorf("ZREM %s: %w", key, err)
		}
	} else {
		if _, err := master.ZAdd(ctx, key, redis.Z{Score: score, Member: member}).Result(); err != nil {
			errCh <- fmt.Errorf("ZADD %s: %w", key, err)
		}
	}
}

func replSoakCounter(ctx context.Context, master *redis.Client, rng *rand.Rand, errCh chan<- error) {
	key := fmt.Sprintf("soak:cnt:%d", rng.Intn(30))
	switch rng.Intn(4) {
	case 0:
		if _, err := master.Incr(ctx, key).Result(); err != nil {
			errCh <- fmt.Errorf("INCR %s: %w", key, err)
		}
	case 1:
		delta := rng.Int63n(100) + 1
		if _, err := master.IncrBy(ctx, key, delta).Result(); err != nil {
			errCh <- fmt.Errorf("INCRBY %s: %w", key, err)
		}
	case 2:
		if _, err := master.Decr(ctx, key).Result(); err != nil {
			errCh <- fmt.Errorf("DECR %s: %w", key, err)
		}
	case 3:
		delta := rng.Int63n(100) + 1
		if _, err := master.DecrBy(ctx, key, delta).Result(); err != nil {
			errCh <- fmt.Errorf("DECRBY %s: %w", key, err)
		}
	}
}

func replSoakTransaction(ctx context.Context, master *redis.Client, rng *rand.Rand, errCh chan<- error) {
	pipe := master.TxPipeline()
	n := rng.Intn(5) + 1
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("soak:txn:%d:%d", rng.Intn(10), i)
		val := fmt.Sprintf("txv%d", rng.Intn(1000))
		pipe.Set(ctx, key, val, 0)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		errCh <- fmt.Errorf("TX pipeline: %w", err)
	}
}

func replSoakDelete(ctx context.Context, master *redis.Client, rng *rand.Rand, errCh chan<- error) {
	key := fmt.Sprintf("soak:%s:%d", randType(rng), rng.Intn(30))
	if _, err := master.Del(ctx, key).Result(); err != nil {
		errCh <- fmt.Errorf("DEL %s: %w", key, err)
	}
}

func replSoakTTL(ctx context.Context, master *redis.Client, rng *rand.Rand, errCh chan<- error) {
	key := fmt.Sprintf("soak:ttl:%d", rng.Intn(30))
	switch rng.Intn(3) {
	case 0:
		_, err := master.Expire(ctx, key, time.Duration(rng.Intn(3600)+1)*time.Second).Result()
		if err != nil && err != redis.Nil {
			errCh <- fmt.Errorf("EXPIRE %s: %w", key, err)
		}
	case 1:
		if _, err := master.Persist(ctx, key).Result(); err != nil && err != redis.Nil {
			errCh <- fmt.Errorf("PERSIST %s: %w", key, err)
		}
	case 2:
		_, err := master.TTL(ctx, key).Result()
		if err != nil && err != redis.Nil {
			errCh <- fmt.Errorf("TTL %s: %w", key, err)
		}
	}
}

func randType(rng *rand.Rand) string {
	types := []string{"str", "list", "hash", "set", "zset", "cnt"}
	return types[rng.Intn(len(types))]
}

// runSoakReplLifecycle periodically disconnects and reconnects the slave
// to simulate network partitions during the soak test.
func runSoakReplLifecycle(ctx context.Context, t *testing.T, master, slaveClient *redis.Client, errCh chan<- error) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + 9999))
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		// 30% chance of a lifecycle event each tick
		if rng.Intn(100) >= 30 {
			continue
		}

		// Disconnect: use CLIENT KILL to drop all slave connections
		// The slave's SlaveReconnector should automatically reconnect
		t.Log("soak-repl: lifecycle event — killing slave connections")
		_ = master.Do(ctx, "CLIENT", "KILL", "TYPE", "slave")

		// Random brief partition
		if rng.Intn(2) == 0 {
			sleepDur := time.Duration(rng.Intn(500)+100) * time.Millisecond
			time.Sleep(sleepDur)
		}

		// Verify the slave reconnected and data is still flowing
		checkKey := fmt.Sprintf("soak:lifecycle:%d", rng.Intn(100))
		checkVal := fmt.Sprintf("check:%d", time.Now().UnixNano())
		if err := master.Set(ctx, checkKey, checkVal, 0).Err(); err != nil {
			errCh <- fmt.Errorf("lifecycle check SET: %w", err)
			continue
		}

		time.Sleep(500 * time.Millisecond)

		// Check on the slave (non-fatal — replication may still be syncing)
		slaveVal, err := slaveClient.Get(ctx, checkKey).Result()
		if err != nil {
			// This can happen if slave is still reconnecting
			t.Logf("soak-repl: lifecycle check pending (slave may be reconnecting): %v", err)
		} else if slaveVal != checkVal {
			errCh <- fmt.Errorf("lifecycle check value mismatch: master=%q slave=%q", checkVal, slaveVal)
		} else {
			t.Log("soak-repl: lifecycle check passed — slave reconnected and synced")
		}
	}
}

// scanKeysWithRetry scans all keys using SCAN with retry and backoff.
func scanKeysWithRetry(client *redis.Client, label string) ([]string, error) {
	deadline := time.Now().Add(retryTimeout)
	for attempt := 0; ; attempt++ {
		var allKeys []string
		var cursor uint64
		ok := true
		for ok {
			scanCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			keys, nextCursor, err := client.Scan(scanCtx, cursor, "*", int64(1000)).Result()
			cancel()
			if err != nil {
				if time.Now().After(deadline) {
					return nil, fmt.Errorf("%s SCAN failed after %d attempts: %w", label, attempt+1, err)
				}
				backoff := time.Duration(1<<min(attempt, 6)) * 100 * time.Millisecond
				time.Sleep(backoff)
				ok = false // retry from start
				continue
			}
			allKeys = append(allKeys, keys...)
			cursor = nextCursor
			if cursor == 0 {
				return allKeys, nil
			}
		}
	}
}

// compareDatasets scans all keys on master and slave and compares them.
func compareDatasets(t *testing.T, master, slave *redis.Client) {
	ctx := context.Background()

	masterKeys, err := scanKeysWithRetry(master, "master")
	if err != nil {
		t.Fatalf("master SCAN failed: %v", err)
	}

	slaveKeys, err := scanKeysWithRetry(slave, "slave")
	if err != nil {
		t.Fatalf("slave SCAN failed: %v", err)
	}

	t.Logf("soak-repl: master keys=%d, slave keys=%d", len(masterKeys), len(slaveKeys))

	masterSet := make(map[string]bool, len(masterKeys))
	for _, k := range masterKeys {
		masterSet[k] = true
	}

	// Check for keys on master missing from slave
	var missingOnSlave, extraOnSlave []string
	for _, k := range masterKeys {
		if !containsString(slaveKeys, k) {
			missingOnSlave = append(missingOnSlave, k)
		}
	}
	for _, k := range slaveKeys {
		if !masterSet[k] {
			extraOnSlave = append(extraOnSlave, k)
		}
	}

	if len(missingOnSlave) > 0 {
		t.Errorf("soak-repl: %d keys missing on slave (showing first 20): %v",
			len(missingOnSlave), truncateList(missingOnSlave, 20))
	}
	if len(extraOnSlave) > 0 {
		t.Errorf("soak-repl: %d extra keys on slave (showing first 20): %v",
			len(extraOnSlave), truncateList(extraOnSlave, 20))
	}

	// Compare values for each key present on both
	// Use a sample: check all key types and values OR skip if too many
	maxCompare := 2000
	compared := 0
	var typeMismatches, valueMismatches int

	commonKeys := intersectStringSets(masterKeys, slaveKeys)

	for _, key := range commonKeys {
		if compared >= maxCompare {
			break
		}
		compared++

		mType, err := master.Type(ctx, key).Result()
		if err != nil {
			continue
		}
		sType, err := slave.Type(ctx, key).Result()
		if err != nil {
			continue
		}
		if mType != sType {
			if typeMismatches == 0 {
				t.Errorf("soak-repl: type mismatch for %q: master=%q slave=%q", key, mType, sType)
			}
			typeMismatches++
			continue
		}

		switch mType {
		case "string":
			mVal, _ := master.Get(ctx, key).Result()
			sVal, _ := slave.Get(ctx, key).Result()
			if mVal != sVal {
				if valueMismatches == 0 {
					t.Errorf("soak-repl: value mismatch for string %q: master=%q slave=%q", key, mVal, sVal)
				}
				valueMismatches++
			}
		case "list":
			mItems, _ := master.LRange(ctx, key, 0, -1).Result()
			sItems, _ := slave.LRange(ctx, key, 0, -1).Result()
			if !stringSliceEqual(mItems, sItems) {
				if valueMismatches == 0 {
					t.Errorf("soak-repl: list mismatch for %q: master=%v slave=%v", key, mItems, sItems)
				}
				valueMismatches++
			}
		case "hash":
			mFields, _ := master.HGetAll(ctx, key).Result()
			sFields, _ := slave.HGetAll(ctx, key).Result()
			if !mapEqual(mFields, sFields) {
				if valueMismatches == 0 {
					t.Errorf("soak-repl: hash mismatch for %q: master=%v slave=%v", key, mFields, sFields)
				}
				valueMismatches++
			}
		case "set":
			mMembers, _ := master.SMembers(ctx, key).Result()
			sMembers, _ := slave.SMembers(ctx, key).Result()
			if !stringSetEqual(mMembers, sMembers) {
				if valueMismatches == 0 {
					t.Errorf("soak-repl: set mismatch for %q: master=%v slave=%v", key, mMembers, sMembers)
				}
				valueMismatches++
			}
		case "zset":
			mMembers, _ := master.ZRangeWithScores(ctx, key, 0, -1).Result()
			sMembers, _ := slave.ZRangeWithScores(ctx, key, 0, -1).Result()
			if !zSetEqual(mMembers, sMembers) {
				if valueMismatches == 0 {
					t.Errorf("soak-repl: zset mismatch for %q", key)
				}
				valueMismatches++
			}
		}
	}

	if typeMismatches > 0 {
		t.Errorf("soak-repl: %d type mismatches between master and slave", typeMismatches)
	}
	if valueMismatches > 0 {
		t.Errorf("soak-repl: %d value mismatches between master and slave", valueMismatches)
	}

	// Check replication info
	mInfo, err := master.Info(ctx, "replication").Result()
	if err == nil {
		t.Logf("soak-repl: master INFO replication:\n%s", summarizeReplInfo(mInfo))
	}
	sInfo, err := slave.Info(ctx, "replication").Result()
	if err == nil {
		t.Logf("soak-repl: slave INFO replication:\n%s", summarizeReplInfo(sInfo))
	}

	if typeMismatches == 0 && valueMismatches == 0 && len(missingOnSlave) == 0 && len(extraOnSlave) == 0 {
		t.Log("soak-repl: datasets fully consistent ✓")
	} else {
		t.Errorf("soak-repl: datasets DIVERGENT: type=%d value=%d missing=%d extra=%d",
			typeMismatches, valueMismatches, len(missingOnSlave), len(extraOnSlave))
	}

}

func truncateList(list []string, n int) []string {
	if len(list) <= n {
		return list
	}
	return list[:n]
}

func intersectStringSets(a, b []string) []string {
	setB := make(map[string]bool, len(b))
	for _, s := range b {
		setB[s] = true
	}
	var result []string
	for _, s := range a {
		if setB[s] {
			result = append(result, s)
		}
	}
	return result
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

func stringSetEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
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

func mapEqual(a, b map[string]string) bool {
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

func summarizeReplInfo(info string) string {
	lines := strings.Split(info, "\n")
	var relevant []string
	for _, line := range lines {
		if strings.Contains(line, "role:") ||
			strings.Contains(line, "master_repl_offset:") ||
			strings.Contains(line, "connected_slaves:") ||
			strings.Contains(line, "slave_repl_offset:") ||
			strings.Contains(line, "master_link_status:") ||
			strings.Contains(line, "repl_backlog_active:") ||
			strings.Contains(line, "repl_backlog_size:") ||
			strings.Contains(line, "slave_read_repl_offset:") {
			relevant = append(relevant, line)
		}
	}
	return strings.Join(relevant, "\n")
}
