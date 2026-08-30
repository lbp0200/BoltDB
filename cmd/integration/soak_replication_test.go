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

const soakReplDefaultDuration = 30 * time.Second
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

// TestSoakReplication is a long-running master-slave replication stability soak.
// It runs random command sequences on the master while randomly disconnecting/reconnecting
// the slave, then measures system health (L0, retries, goroutines, convergence).
//
// Dataset comparison is informational by default — FULLRESYNC has a microsecond
// duplicate window (badger MVCC snapshot ≠ replication offset boundary). Writes
// committed between snapshotOffset capture and db.View() start appear in both
// the RDB and backlog. This is a known, bounded tradeoff (see FULLRESYNC
// Semantics in AGENTS.md). The test validates that backpressure/compaction/
// retry-storm stability holds, NOT strict linearizable consistency.
//
// For strict equality: set SOAK_REPL_STRICT_EQUALITY=1.
// For strict correctness regression tests, use the suite:
//
//	go test -race -timeout 600s ./cmd/integration/regressions/...
//
// Usage:
//
//	go test -race -timeout 30m ./cmd/integration/ -run TestSoakReplication
//	SOAK_REPL_DURATION=3h go test -race -timeout 4h ./cmd/integration/ -run TestSoakReplication
//	SOAK_REPL_DURATION=30m SOAK_REPL_WRITERS=10 go test -race -timeout 40m ./cmd/integration/ -run TestSoakReplication
//	SOAK_REPL_STRICT_EQUALITY=1 SOAK_REPL_DURATION=30m go test -race -timeout 60s ./cmd/integration/ -run TestSoakReplication
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
		masterBackup.Wait()
		slaveBackup.Wait()
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
	t.Parallel()
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
	pm.EnableTemporalAnalysis()
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
	// Set SOAK_REPL_LIFECYCLE=0 to disable (isolate protocol correctness baseline)
	if os.Getenv("SOAK_REPL_LIFECYCLE") != "0" {
		wg.Add(1)
		lifecycleGrace := 5 * time.Minute
		if duration < lifecycleGrace {
			lifecycleGrace = duration / 2
		}
		go func() {
			defer wg.Done()
			runSoakReplLifecycle(soakCtx, t, master, slave, errCh, lifecycleGrace)
		}()
	} else {
		t.Log("soak-repl: lifecycle chaos disabled (SOAK_REPL_LIFECYCLE=0)")
	}

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
	// Barrier: connected slave + master/slave offsets match (lag <= 0)
	converged := waitReplicationConvergence(ctx, master, slave, 120*time.Second, t)
	if !converged {
		t.Logf("soak-repl: WARN — replication convergence barrier not met; dataset comparison may show false divergence")
	} else {
		t.Log("soak-repl: replication convergence barrier OK")
	}

	// 压力汇总 + 退化检查
	pm.LogSummary(t)
	level := pm.CheckDegradation(t, DefaultDegradationAssertion(), baseline)
	t.Logf("soak-repl: degradation level: %s", level)

	// 健康评分
	health := pm.HealthScore(baseline)
	t.Log(health.String())

	// 时间序列分析
	if ta := pm.TemporalAnalysis(); ta.Trajectory != TrajectoryInsufficientData {
		t.Log(ta.FormatReport())
	}

	// 吸引子 basin 分析
	ba := pm.BasinAnalysis()
	t.Log(ba.FormatReport())

	// 保存结构化报告
	if rdir := os.Getenv("SOAK_REPORT_DIR"); rdir != "" {
		saveSoakReport(rdir, "replication", pm, baseline, duration, level)
		t.Logf("soak-repl: report saved to %s", rdir)
		saveEvolutionReport(rdir, "replication")
		t.Logf("soak-repl: evolution report saved to %s", rdir)
	}

	// Final verification: compare master and slave datasets
	strictEquality := os.Getenv("SOAK_REPL_STRICT_EQUALITY") != ""
	t.Log("soak-repl: comparing master/slave datasets...")
	compareDatasets(t, master, slave, strictEquality)

	final := runtime.NumGoroutine()
	leak := final - baseline
	t.Logf("soak-repl: goroutine delta=%d (baseline=%d, final=%d)", leak, baseline, final)
	if leak > 50 {
		t.Errorf("goroutine leak after soak-repl: %d (baseline=%d, final=%d)", leak, baseline, final)
	}
}

// TestSoakReplicationShortStrict is a short-duration strict-equality replication soak.
// It validates correctness under reconnect/lifecycle chaos where deterministic replay
// guarantees must hold (SPOP, XADD, EXPIRE canonicalization, etc.).
//
// Strict equality is ON by default — any divergence between master and slave datasets
// is a test failure. This test focuses on:
//   - PSYNC CONTINUE gap-fill correctness
//   - Deterministic command replay (no nondeterminism from RNG or time-on-replica)
//   - Offset tracking accuracy through reconnect cycles
//   - No structural corruption after FULLRESYNC cycles
//
// Default duration: 10 minutes (suitable for moderate stress without stability drift).
// For longer stability analysis (L0/retry/goroutine/basin), use TestSoakReplication.
//
// Usage:
//
//	SOAK_REPL_DURATION=10m go test -race -timeout 15m ./cmd/integration/ -run TestSoakReplicationShortStrict
//	SOAK_REPL_DURATION=5m go test -race -timeout 8m ./cmd/integration/ -run TestSoakReplicationShortStrict
func TestSoakReplicationShortStrict(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping soak replication short strict test in short mode")
	}
	duration := getSoakReplDuration()
	writers := getSoakReplWriters()

	t.Logf("soak-repl-short: duration=%v, writers=%d, strictEquality=ON", duration, writers)
	t.Logf("soak-repl-short: validates lifecycle/deterministic replay correctness")
	t.Logf("soak-repl-short: set SOAK_REPL_DURATION (default=10m) to extend")

	env := setupSoakReplication(t)
	defer env.cleanup()

	master := env.masterClient
	slave := env.slaveClient
	ctx := context.Background()

	time.Sleep(200 * time.Millisecond)
	err := master.Set(ctx, "soak:init", "ok", 0).Err()
	assert.NoError(t, err)
	time.Sleep(200 * time.Millisecond)
	val, err := slave.Get(ctx, "soak:init").Result()
	assert.NoError(t, err)
	assert.Equal(t, "ok", val)

	baseline := runtime.NumGoroutine()

	pm := NewPressureMonitor(env.masterDB, env.masterRepl)
	pm.EnableTemporalAnalysis()
	if jdir := os.Getenv("SOAK_JSONL_DIR"); jdir != "" {
		os.MkdirAll(jdir, 0755)
		jpath := filepath.Join(jdir, fmt.Sprintf("soak-repl-short-%s.jsonl", time.Now().Format("20060102-150405")))
		if err := pm.SetJSONLPath(jpath); err != nil {
			t.Logf("soak-repl-short: failed to create JSONL %s: %v", jpath, err)
		} else {
			t.Logf("soak-repl-short: JSONL timeline → %s", jpath)
		}
	}
	soakCtx, soakCancel := context.WithTimeout(ctx, duration)
	defer soakCancel()
	pm.Start(soakCtx, 30*time.Second)

	errCh := make(chan error, writers*10)
	var wg sync.WaitGroup

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

	if os.Getenv("SOAK_REPL_LIFECYCLE") != "0" {
		wg.Add(1)
		lifecycleGrace := 5 * time.Minute
		if duration < lifecycleGrace {
			lifecycleGrace = duration / 2
		}
		go func() {
			defer wg.Done()
			runSoakReplLifecycle(soakCtx, t, master, slave, errCh, lifecycleGrace)
		}()
	}

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
		t.Errorf("soak-repl-short: %d errors during run (first 10 shown):", len(errs))
		for i, err := range errs {
			if i >= 10 {
				break
			}
			t.Errorf("  %v", err)
		}
	}

	converged := waitReplicationConvergence(ctx, master, slave, 120*time.Second, t)
	if !converged {
		t.Logf("soak-repl-short: WARN — replication convergence barrier not met; dataset comparison may show false divergence")
	} else {
		t.Log("soak-repl-short: replication convergence barrier OK")
	}

	pm.LogSummary(t)
	level := pm.CheckDegradation(t, DefaultDegradationAssertion(), baseline)
	t.Logf("soak-repl-short: degradation level: %s", level)

	health := pm.HealthScore(baseline)
	t.Log(health.String())

	if ta := pm.TemporalAnalysis(); ta.Trajectory != TrajectoryInsufficientData {
		t.Log(ta.FormatReport())
	}

	ba := pm.BasinAnalysis()
	t.Log(ba.FormatReport())

	if rdir := os.Getenv("SOAK_REPORT_DIR"); rdir != "" {
		saveSoakReport(rdir, "replication-short-strict", pm, baseline, duration, level)
		t.Logf("soak-repl-short: report saved to %s", rdir)
	}

	// Short strict soak: strict equality is ALWAYS on (hard failure on divergence)
	t.Log("soak-repl-short: comparing master/slave datasets (STRICT EQUALITY)...")
	compareDatasets(t, master, slave, true)

	final := runtime.NumGoroutine()
	leak := final - baseline
	t.Logf("soak-repl-short: goroutine delta=%d (baseline=%d, final=%d)", leak, baseline, final)
	if leak > 50 {
		t.Errorf("goroutine leak after soak-repl-short: %d (baseline=%d, final=%d)", leak, baseline, final)
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
func runSoakReplLifecycle(ctx context.Context, t *testing.T, master, slaveClient *redis.Client, errCh chan<- error, gracePeriod time.Duration) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + 9999))
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		// Stop early to allow convergence barrier time
		if deadline, ok := ctx.Deadline(); ok {
			if time.Until(deadline) < gracePeriod {
				t.Log("soak-repl: lifecycle stopping early for convergence grace period")
				return
			}
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

// replicationOffsetsConverged is the soak post-run success rule: the replica
// offset has caught the master. A lag that stays at a positive constant is a
// stalled replica (reconnect backoff, mis-framed stream), not success — the
// same hole the snapshot-overlap regression already closed. See
// docs/failures/repl-offset-boundary-drift.md (stable lag=142).
func replicationOffsetsConverged(masterOffset, slaveOffset int64) bool {
	return masterOffset-slaveOffset <= 0
}

func TestReplicationOffsetsConverged(t *testing.T) {
	t.Parallel()

	if !replicationOffsetsConverged(100, 100) {
		t.Fatal("lag==0 must count as converged")
	}
	if !replicationOffsetsConverged(0, 0) {
		t.Fatal("zero offsets must count as converged")
	}
	if !replicationOffsetsConverged(50, 60) {
		t.Fatal("negative lag (replica ahead) must count as converged")
	}

	// Former waiter returned true after N identical positive lags. Field
	// case from docs/failures/repl-offset-boundary-drift.md: mo=1844104
	// so=1843962, declared "converged with stable lag=142".
	const formerStableSamples = 3
	const mo, so int64 = 1844104, 1843962
	for i := 0; i < formerStableSamples; i++ {
		if replicationOffsetsConverged(mo, so) {
			t.Fatalf("frozen lag=%d treated as converged at sample %d/%d", mo-so, i+1, formerStableSamples)
		}
	}
}

// waitReplicationConvergence polls master/slave INFO replication until the
// replica is connected and replicationOffsetsConverged is true. A frozen
// positive lag is never treated as success; timeout with lag > 0 is failure.
func waitReplicationConvergence(ctx context.Context, master, slave *redis.Client, timeout time.Duration, t *testing.T) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		mInfo, err := master.Info(ctx, "replication").Result()
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		connected := parseConnectedSlaves(mInfo)
		if connected == 0 {
			t.Logf("converge-barrier: waiting for slave to connect...")
			time.Sleep(500 * time.Millisecond)
			continue
		}

		mOff := parseMasterReplOffset(mInfo)
		sInfo, err := slave.Info(ctx, "replication").Result()
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		sOff := parseSlaveReplOffset(sInfo)

		if replicationOffsetsConverged(mOff, sOff) {
			t.Logf("converge-barrier: fully converged (mo=%d so=%d)", mOff, sOff)
			return true
		}

		time.Sleep(500 * time.Millisecond)
	}

	mInfo, _ := master.Info(ctx, "replication").Result()
	sInfo, _ := slave.Info(ctx, "replication").Result()
	t.Logf("converge-barrier: TIMEOUT — frozen positive lag is not convergence")
	t.Logf("converge-barrier: TIMEOUT — master info: %s", summarizeReplInfo(mInfo))
	t.Logf("converge-barrier: TIMEOUT — slave info: %s", summarizeReplInfo(sInfo))
	return false
}

func parseConnectedSlaves(info string) int {
	for _, line := range strings.Split(info, "\n") {
		if strings.HasPrefix(line, "connected_slaves:") {
			n, _ := strconv.Atoi(strings.TrimPrefix(line, "connected_slaves:"))
			return n
		}
	}
	return 0
}

func parseMasterReplOffset(info string) int64 {
	for _, line := range strings.Split(info, "\n") {
		if strings.HasPrefix(line, "master_repl_offset:") {
			n, _ := strconv.ParseInt(strings.TrimPrefix(line, "master_repl_offset:"), 10, 64)
			return n
		}
	}
	return 0
}

func parseSlaveReplOffset(info string) int64 {
	for _, line := range strings.Split(info, "\n") {
		if strings.HasPrefix(line, "slave_repl_offset:") {
			n, _ := strconv.ParseInt(strings.TrimPrefix(line, "slave_repl_offset:"), 10, 64)
			return n
		}
	}
	return 0
}

// compareDatasets scans all keys on master and slave and compares them.
// When strict is true, divergence calls t.Errorf (hard failure).
// When strict is false (default), divergence is logged as informational
// (allows for FULLRESYNC's known microsecond duplicate window).
func compareDatasets(t *testing.T, master, slave *redis.Client, strict bool) {
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
		t.Logf("soak-repl: %d keys missing on slave (showing first 20): %v",
			len(missingOnSlave), truncateList(missingOnSlave, 20))
	}
	if len(extraOnSlave) > 0 {
		t.Logf("soak-repl: %d extra keys on slave (showing first 20): %v",
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
				t.Logf("soak-repl: type mismatch for %q: master=%q slave=%q", key, mType, sType)
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
					t.Logf("soak-repl: value mismatch for string %q: master=%q slave=%q", key, mVal, sVal)
				}
				valueMismatches++
			}
		case "list":
			mItems, _ := master.LRange(ctx, key, 0, -1).Result()
			sItems, _ := slave.LRange(ctx, key, 0, -1).Result()
			if !stringSliceEqual(mItems, sItems) {
				if valueMismatches == 0 {
					t.Logf("soak-repl: list mismatch for %q: master=%v slave=%v", key, mItems, sItems)
				}
				valueMismatches++
			}
		case "hash":
			mFields, _ := master.HGetAll(ctx, key).Result()
			sFields, _ := slave.HGetAll(ctx, key).Result()
			if !mapEqual(mFields, sFields) {
				if valueMismatches == 0 {
					t.Logf("soak-repl: hash mismatch for %q: master=%v slave=%v", key, mFields, sFields)
				}
				valueMismatches++
			}
		case "set":
			mMembers, _ := master.SMembers(ctx, key).Result()
			sMembers, _ := slave.SMembers(ctx, key).Result()
			if !stringSetEqual(mMembers, sMembers) {
				if valueMismatches == 0 {
					t.Logf("soak-repl: set mismatch for %q: master=%v slave=%v", key, mMembers, sMembers)
				}
				valueMismatches++
			}
		case "zset":
			mMembers, _ := master.ZRangeWithScores(ctx, key, 0, -1).Result()
			sMembers, _ := slave.ZRangeWithScores(ctx, key, 0, -1).Result()
			if !zSetEqual(mMembers, sMembers) {
				if valueMismatches == 0 {
					t.Logf("soak-repl: zset mismatch for %q", key)
				}
				valueMismatches++
			}
		}
	}

	if typeMismatches > 0 {
		t.Logf("soak-repl: %d type mismatches between master and slave", typeMismatches)
	}
	if valueMismatches > 0 {
		t.Logf("soak-repl: %d value mismatches between master and slave", valueMismatches)
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

	diverged := typeMismatches > 0 || valueMismatches > 0 || len(missingOnSlave) > 0 || len(extraOnSlave) > 0

	if !diverged {
		t.Log("soak-repl: datasets fully consistent ✓")
	} else if strict {
		t.Errorf("soak-repl: datasets DIVERGENT (strict mode): type=%d value=%d missing=%d extra=%d",
			typeMismatches, valueMismatches, len(missingOnSlave), len(extraOnSlave))
	} else {
		t.Logf("soak-repl: datasets DIVERGENT (informational — FULLRESYNC duplicate window): type=%d value=%d missing=%d extra=%d",
			typeMismatches, valueMismatches, len(missingOnSlave), len(extraOnSlave))
	}

	if diverged {
		profileDivergence(t, master, slave,
			missingOnSlave, extraOnSlave,
			valueMismatches > 0)
	}
}

// divergentKey represents a single key that diverged, with its type.
type divergentKey struct {
	Key  string `json:"key"`
	Type string `json:"type"`
}

// divergenceProfile holds the breakdown of dataset divergence by key type.
// It distinguishes SCAN-only artifacts from truly missing/extra keys
// by direct EXISTS verification on each divergent key.
type divergenceProfile struct {
	TrulyMissing map[string]int `json:"truly_missing"`
	ScanMissed   map[string]int `json:"scan_missed"`
	TrulyExtra   map[string]int `json:"truly_extra"`
	ScanExtra    map[string]int `json:"scan_extra"`

	ValueMissByType map[string]int `json:"value_mismatch_by_type"`

	TopTrulyMissing []divergentKey `json:"top_truly_missing"`
	TopScanMissed   []divergentKey `json:"top_scan_missed"`
	TopTrulyExtra   []divergentKey `json:"top_truly_extra"`
	TopScanExtra    []divergentKey `json:"top_scan_extra"`
}

// soakKeyType maps a soak-format key like "soak:list:17" to its Redis type.
func soakKeyType(key string) string {
	// Known soak prefixes:
	//   soak:str:* → string (SET/GET)
	//   soak:cnt:* → string (INCR)
	//   soak:ttl:* → string (EXPIRE)
	//   soak:txn:* → string (MULTI/EXEC)
	//   soak:list:* → list
	//   soak:hash:* → hash
	//   soak:set:*  → set
	//   soak:zset:* → zset
	parts := strings.SplitN(key, ":", 3)
	if len(parts) < 2 {
		return "unknown"
	}
	switch parts[1] {
	case "str", "cnt", "ttl", "txn":
		return "string"
	case "list":
		return "list"
	case "hash":
		return "hash"
	case "set":
		return "set"
	case "zset":
		return "zset"
	default:
		return "unknown"
	}
}

// profileDivergence produces a detailed breakdown of dataset divergence by key type.
// It categorises missing/extra keys and distinguishes SCAN-only artifacts
// from truly divergent keys by direct EXISTS verification.
//
// For each key reported as "missing" (present in master SCAN but absent from slave SCAN),
// we issue EXISTS on the slave. If EXISTS returns 1, the key is a SCAN artifact
// (SCAN missed it but it really exists). Otherwise it is truly missing.
// Same logic for "extra" keys (present in slave SCAN but absent from master SCAN).
func profileDivergence(t *testing.T, master, slave *redis.Client,
	missingOnSlave, extraOnSlave []string,
	hasValueMiss bool) {

	ctx := context.Background()
	p := divergenceProfile{
		TrulyMissing:    make(map[string]int),
		ScanMissed:      make(map[string]int),
		TrulyExtra:      make(map[string]int),
		ScanExtra:       make(map[string]int),
		ValueMissByType: make(map[string]int),
	}

	// Phase 1 — verify "missing" keys: EXISTS on slave to distinguish
	// SCAN artifact from true missing.
	for _, k := range missingOnSlave {
		typ := soakKeyType(k)
		if typ == "unknown" {
			rt, err := master.Type(ctx, k).Result()
			if err == nil {
				typ = rt
			}
		}

		n, err := slave.Exists(ctx, k).Result()
		exists := err == nil && n > 0
		if exists {
			p.ScanMissed[typ]++
			if len(p.TopScanMissed) < 20 {
				p.TopScanMissed = append(p.TopScanMissed, divergentKey{Key: k, Type: typ})
			}
		} else {
			p.TrulyMissing[typ]++
			if len(p.TopTrulyMissing) < 20 {
				p.TopTrulyMissing = append(p.TopTrulyMissing, divergentKey{Key: k, Type: typ})
			}
		}
	}

	// Phase 2 — verify "extra" keys: EXISTS on master to distinguish
	// SCAN artifact from true extra.
	for _, k := range extraOnSlave {
		typ := soakKeyType(k)
		if typ == "unknown" {
			rt, err := slave.Type(ctx, k).Result()
			if err == nil {
				typ = rt
			}
		}

		n, err := master.Exists(ctx, k).Result()
		exists := err == nil && n > 0
		if exists {
			// SCAN on master missed this key — it really exists on master too
			p.ScanExtra[typ]++
			if len(p.TopScanExtra) < 20 {
				p.TopScanExtra = append(p.TopScanExtra, divergentKey{Key: k, Type: typ})
			}
		} else {
			// Key genuinely does not exist on master
			p.TrulyExtra[typ]++
			if len(p.TopTrulyExtra) < 20 {
				p.TopTrulyExtra = append(p.TopTrulyExtra, divergentKey{Key: k, Type: typ})
			}
		}
	}

	// Phase 3 — value mismatches: re-scan common keys that diverged to categorize by type
	if hasValueMiss {
		masterKeys, _ := scanKeysWithRetry(master, "master")
		slaveKeys, _ := scanKeysWithRetry(slave, "slave")
		common := intersectStringSets(masterKeys, slaveKeys)

		for _, key := range common {
			mType, err := master.Type(ctx, key).Result()
			if err != nil {
				continue
			}
			sType, err := slave.Type(ctx, key).Result()
			if err != nil {
				continue
			}
			if mType != sType {
				continue // already counted as type mismatch
			}

			var diverged bool
			switch mType {
			case "string":
				mv, _ := master.Get(ctx, key).Result()
				sv, _ := slave.Get(ctx, key).Result()
				diverged = mv != sv
			case "list":
				mv, _ := master.LRange(ctx, key, 0, -1).Result()
				sv, _ := slave.LRange(ctx, key, 0, -1).Result()
				diverged = !stringSliceEqual(mv, sv)
			case "hash":
				mv, _ := master.HGetAll(ctx, key).Result()
				sv, _ := slave.HGetAll(ctx, key).Result()
				diverged = !mapEqual(mv, sv)
			case "set":
				mv, _ := master.SMembers(ctx, key).Result()
				sv, _ := slave.SMembers(ctx, key).Result()
				diverged = !stringSetEqual(mv, sv)
			case "zset":
				mv, _ := master.ZRangeWithScores(ctx, key, 0, -1).Result()
				sv, _ := slave.ZRangeWithScores(ctx, key, 0, -1).Result()
				diverged = !zSetEqual(mv, sv)
			}
			if diverged {
				p.ValueMissByType[mType]++
			}
		}
	}

	// Print formatted profile
	totalMissing := len(missingOnSlave)
	totalExtra := len(extraOnSlave)
	scanMissedTotal := 0
	for _, c := range p.ScanMissed {
		scanMissedTotal += c
	}
	trulyMissingTotal := 0
	for _, c := range p.TrulyMissing {
		trulyMissingTotal += c
	}

	t.Logf("=== Divergence Profile ===")
	t.Logf("Total SCAN-discrepant keys: missing=%d  extra=%d", totalMissing, totalExtra)
	t.Logf("")

	if scanMissedTotal > 0 || trulyMissingTotal > 0 {
		t.Logf("--- Missing keys (on master, not on slave SCAN) ---")
		t.Logf("  Truly missing: %d  |  SCAN artifact: %d",
			trulyMissingTotal, scanMissedTotal)
		if trulyMissingTotal > 0 {
			t.Logf("  Truly missing by type:")
			printTypeBreakdown(t, p.TrulyMissing)
			for _, dk := range p.TopTrulyMissing {
				t.Logf("    MISS %q  (%s)", dk.Key, dk.Type)
			}
		}
		if scanMissedTotal > 0 {
			t.Logf("  SCAN-missed (key exists on slave but SCAN skipped it):")
			printTypeBreakdown(t, p.ScanMissed)
			for _, dk := range p.TopScanMissed {
				t.Logf("    SCAN_MISSED %q  (%s)", dk.Key, dk.Type)
			}
		}
		t.Logf("")
	}

	scanExtraTotal := 0
	for _, c := range p.ScanExtra {
		scanExtraTotal += c
	}
	trulyExtraTotal := 0
	for _, c := range p.TrulyExtra {
		trulyExtraTotal += c
	}

	if scanExtraTotal > 0 || trulyExtraTotal > 0 {
		t.Logf("--- Extra keys (on slave, not on master SCAN) ---")
		t.Logf("  Truly extra: %d  |  SCAN artifact: %d",
			trulyExtraTotal, scanExtraTotal)
		if trulyExtraTotal > 0 {
			t.Logf("  Truly extra by type:")
			printTypeBreakdown(t, p.TrulyExtra)
		}
		if scanExtraTotal > 0 {
			t.Logf("  SCAN-missed on master (key exists on master but SCAN skipped it):")
			printTypeBreakdown(t, p.ScanExtra)
			for _, dk := range p.TopScanExtra {
				t.Logf("    SCAN_EXTRA %q  (%s)", dk.Key, dk.Type)
			}
		}
		t.Logf("")
	}

	if hasValueMiss {
		t.Logf("--- Value Mismatches ---")
		printTypeBreakdown(t, p.ValueMissByType)
	}
}

func printTypeBreakdown(t *testing.T, m map[string]int) {
	total := 0
	for _, c := range m {
		total += c
	}
	if total == 0 {
		t.Logf("  (none)")
		return
	}
	// Print by count descending
	type pair struct {
		typ   string
		count int
	}
	sorted := make([]pair, 0, len(m))
	for typ, count := range m {
		sorted = append(sorted, pair{typ, count})
	}
	// Simple insertion sort by count desc (small N = at most 6 types)
	for i := 1; i < len(sorted); i++ {
		j := i
		for j > 0 && sorted[j].count > sorted[j-1].count {
			sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
			j--
		}
	}
	for _, p := range sorted {
		pct := float64(p.count) / float64(total) * 100
		t.Logf("  %6s: %3d  (%5.1f%%)", p.typ, p.count, pct)
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
			strings.Contains(line, "slave_read_repl_offset:") ||
			strings.Contains(line, "repl_send_drop_count:") ||
			strings.Contains(line, "repl_apply_skip_count:") {
			relevant = append(relevant, line)
		}
	}
	return strings.Join(relevant, "\n")
}
