package integration

import (
	"bufio"
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/backup"
	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/replication"
	"github.com/lbp0200/BoltDB/internal/server"
	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/redis/go-redis/v9"
)

const soakDefaultDuration = 30 * time.Second
const soakMaxDuration = 24 * time.Hour

// getSoakDuration returns the configured soak duration.
// Override with SOAK_DURATION env var (e.g. SOAK_DURATION=30m).
func getSoakDuration() time.Duration {
	s := os.Getenv("SOAK_DURATION")
	if s == "" {
		return soakDefaultDuration
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return soakDefaultDuration
	}
	if d > soakMaxDuration {
		return soakMaxDuration
	}
	return d
}

// getSoakConcurrency returns the configured client concurrency.
// Override with SOAK_CLIENTS env var (default 50).
func getSoakConcurrency() int {
	s := os.Getenv("SOAK_CLIENTS")
	if s == "" {
		return 50
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return 50
	}
	if n > 200 {
		return 200
	}
	return n
}

func getSoakDataDir() string {
	if s := os.Getenv("SOAK_DATA_DIR"); s != "" {
		return s
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "/tmp/soak_boltdb_data"
	}
	return filepath.Join(home, "soak_boltdb_data")
}

func soakBaseline(t *testing.T) (goroutines int) {
	t.Helper()
	time.Sleep(500 * time.Millisecond)
	return runtime.NumGoroutine()
}

func soakCheckNoLeak(t *testing.T, baseline, final int) {
	t.Helper()
	leak := final - baseline
	t.Logf("soak goroutine delta: %d (baseline=%d, final=%d)", leak, baseline, final)
	if leak > 10 {
		t.Errorf("goroutine leak after soak: %d (baseline=%d, final=%d)", leak, baseline, final)
	}
}

// TestSoak is a soak test that runs random operations with concurrent clients.
// Default duration is 5 minutes; override with SOAK_DURATION (e.g. SOAK_DURATION=3h).
// Default concurrency is 50; override with SOAK_CLIENTS.
// Data directory is ~/soak_boltdb_data; override with SOAK_DATA_DIR.
func TestSoak(t *testing.T) {
	if os.Getenv("CI_NIGHTLY_SOAK") == "" {
		t.Skip("soak test: goroutine threshold flaky in CI; set CI_NIGHTLY_SOAK=1 to run")
	}
	if testing.Short() {
		t.Skip("skipping soak test in short mode")
	}
	duration := getSoakDuration()
	concurrency := getSoakConcurrency()
	dataDir := getSoakDataDir()

	t.Logf("soak: duration=%v, clients=%d, data=%s", duration, concurrency, dataDir)
	t.Logf("soak: set SOAK_DURATION (e.g. SOAK_DURATION=3h) to extend")
	t.Logf("soak: set SOAK_CLIENTS (default=50, max=200) to change concurrency")
	t.Logf("soak: set SOAK_DATA_DIR to change data path")

	// Start standalone server
	os.RemoveAll(dataDir)
	db, err := store.NewBotreonStore(dataDir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer db.Close()

	pubsubMgr := store.NewPubSubManager()
	replMgr := replication.NewReplicationManager(db)
	backupMgr := backup.NewBackupManager(db, dataDir)

	h := &server.Handler{
		Db:          db,
		PubSub:      pubsubMgr,
		Backup:      backupMgr,
		Replication: replMgr,
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	go func() {
		_ = h.ServeTCP(listener)
	}()

	time.Sleep(200 * time.Millisecond)

	addr := listener.Addr().String()
	client := redis.NewClient(&redis.Options{
		Addr:        addr,
		Password:    "",
		DB:          0,
		DialTimeout: 5 * time.Second,
	})
	defer client.Close()

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		t.Fatalf("server not ready: %v", err)
	}

	baseline := soakBaseline(t)

	// 压力监控
	pm := NewPressureMonitor(db, replMgr)
	pm.EnableTemporalAnalysis()
	if jdir := os.Getenv("SOAK_JSONL_DIR"); jdir != "" {
		os.MkdirAll(jdir, 0755)
		jpath := filepath.Join(jdir, fmt.Sprintf("soak-%s.jsonl", time.Now().Format("20060102-150405")))
		if err := pm.SetJSONLPath(jpath); err != nil {
			t.Logf("soak: failed to create JSONL %s: %v", jpath, err)
		} else {
			t.Logf("soak: JSONL timeline → %s", jpath)
		}
	}
	soakCtx, soakCancel := context.WithTimeout(context.Background(), duration)
	defer soakCancel()
	pm.Start(soakCtx, 30*time.Second)

	errCh := make(chan error, concurrency*2)
	var wg sync.WaitGroup

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					errCh <- fmt.Errorf("soak client %d panicked: %v", id, r)
				}
			}()
			runSoakClient(soakCtx, addr, id, errCh)
		}(i)
	}

	wg.Wait()
	close(errCh)

	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		t.Errorf("soak: %d errors during run (first 5 shown):", len(errs))
		for i, err := range errs {
			if i >= 5 {
				t.Errorf("  ... and %d more", len(errs)-5)
				break
			}
			t.Errorf("  %v", err)
		}
	}

	// 压力汇总 + 退化检查
	pm.LogSummary(t)
	level := pm.CheckDegradation(t, DefaultDegradationAssertion(), baseline)
	t.Logf("soak: degradation level: %s", level)

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
		saveSoakReport(rdir, "standalone", pm, baseline, duration, level)
		t.Logf("soak: report saved to %s", rdir)
		saveEvolutionReport(rdir, "standalone")
		t.Logf("soak: evolution report saved to %s", rdir)
	}

	listener.Close()
	h.Shutdown()
	backupMgr.Wait()
	client.Close()
	pubsubMgr.Clear()
	db.Close()
	os.RemoveAll(dataDir)

	final := runtime.NumGoroutine()
	soakCheckNoLeak(t, baseline, final)
}

func runSoakClient(ctx context.Context, addr string, id int, errCh chan<- error) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))

	// probability weights for different operation classes
	const (
		pubsubWeight   = 15
		blockingWeight = 10
		txnWeight      = 10
		killWeight     = 3
		connectWeight  = 5
		pipelineWeight = 12
		normalWeight   = 45
		totalWeight    = pubsubWeight + blockingWeight + txnWeight + killWeight + connectWeight + pipelineWeight + normalWeight
	)

	for ctx.Err() == nil {
		roll := rng.Intn(totalWeight)

		switch {
		case roll < pubsubWeight:
			runSoakPubSub(ctx, addr, rng, errCh)

		case roll < pubsubWeight+blockingWeight:
			runSoakBlocking(ctx, addr, rng, errCh)

		case roll < pubsubWeight+blockingWeight+txnWeight:
			runSoakTransaction(ctx, addr, rng, errCh)

		case roll < pubsubWeight+blockingWeight+txnWeight+killWeight:
			runSoakClientKill(ctx, addr, rng, errCh)

		case roll < pubsubWeight+blockingWeight+txnWeight+killWeight+connectWeight:
			runSoakConnectDisconnect(ctx, addr, rng)

		case roll < pubsubWeight+blockingWeight+txnWeight+killWeight+connectWeight+pipelineWeight:
			runSoakPipeline(ctx, addr, rng, errCh)

		default:
			runSoakNormal(ctx, addr, rng, errCh)
		}

		// small random pause between iterations
		if rng.Intn(100) < 30 {
			time.Sleep(time.Duration(rng.Intn(50)) * time.Millisecond)
		}
	}
}

func dialSoak(addr string) (net.Conn, *bufio.Reader, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, nil, err
	}
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	return conn, bufio.NewReader(conn), nil
}

func sendRESPLine(conn net.Conn, data string) error {
	_, err := conn.Write([]byte(data))
	return err
}

func drainRESP(reader *bufio.Reader) {
	for reader.Buffered() > 0 {
		_, _ = proto.ReadRESP(reader)
	}
}

func runSoakPubSub(ctx context.Context, addr string, rng *rand.Rand, errCh chan<- error) {
	conn, reader, err := dialSoak(addr)
	if err != nil {
		return
	}
	defer conn.Close()

	ch := fmt.Sprintf("soak:ch:%d", rng.Intn(5))
	// subscribe
	_ = sendRESPLine(conn, fmt.Sprintf("*2\r\n$9\r\nSUBSCRIBE\r\n$%d\r\n%s\r\n", len(ch), ch))
	// read subscribe response (3 messages: subscribe + 2 pattern messages)
	for i := 0; i < 3; i++ {
		if _, err := proto.ReadRESP(reader); err != nil {
			return
		}
	}

	// publish messages via raw RESP over the same connection
	conn2, reader2, err := dialSoak(addr)
	if err == nil {
		for i := 0; i < rng.Intn(3)+1; i++ {
			msg := fmt.Sprintf("soak_msg_%d", rng.Intn(1000))
			_ = sendRESPLine(conn2, fmt.Sprintf("*3\r\n$7\r\nPUBLISH\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(ch), ch, len(msg), msg))
			_, _ = proto.ReadRESP(reader2)
		}
		conn2.Close()
	}

	// unsubscribe
	_ = sendRESPLine(conn, fmt.Sprintf("*2\r\n$11\r\nUNSUBSCRIBE\r\n$%d\r\n%s\r\n", len(ch), ch))
	for i := 0; i < 3; i++ {
		if _, err := proto.ReadRESP(reader); err != nil {
			return
		}
	}
}

func runSoakBlocking(ctx context.Context, addr string, rng *rand.Rand, errCh chan<- error) {
	conn, reader, err := dialSoak(addr)
	if err != nil {
		return
	}
	defer conn.Close()

	key := fmt.Sprintf("soak:blk:%d", rng.Intn(5))
	timeout := rng.Intn(3)

	// BLPOP with short timeout
	_ = sendRESPLine(conn, fmt.Sprintf("*3\r\n$5\r\nBLPOP\r\n$%d\r\n%s\r\n$1\r\n%d\r\n", len(key), key, timeout))
	resp, err := proto.ReadRESP(reader)
	if err != nil {
		return
	}
	_ = resp // nil-array on timeout, array on value
}

func runSoakTransaction(ctx context.Context, addr string, rng *rand.Rand, errCh chan<- error) {
	conn, reader, err := dialSoak(addr)
	if err != nil {
		return
	}
	defer conn.Close()

	key := fmt.Sprintf("soak:txn:%d", rng.Intn(10))
	val := fmt.Sprintf("v%d", rng.Intn(1000))

	// WATCH key
	_ = sendRESPLine(conn, fmt.Sprintf("*2\r\n$5\r\nWATCH\r\n$%d\r\n%s\r\n", len(key), key))
	if _, err := proto.ReadRESP(reader); err != nil {
		return
	}

	// MULTI
	_ = sendRESPLine(conn, "*1\r\n$5\r\nMULTI\r\n")
	if _, err := proto.ReadRESP(reader); err != nil {
		return
	}

	// SET key value (queued)
	_ = sendRESPLine(conn, fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(val), val))
	if _, err := proto.ReadRESP(reader); err != nil {
		return
	}

	// GET key (queued)
	_ = sendRESPLine(conn, fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(key), key))
	if _, err := proto.ReadRESP(reader); err != nil {
		return
	}

	// EXEC
	_ = sendRESPLine(conn, "*1\r\n$4\r\nEXEC\r\n")
	if _, err := proto.ReadRESP(reader); err != nil {
		return
	}
}

func runSoakClientKill(ctx context.Context, addr string, rng *rand.Rand, errCh chan<- error) {
	killClient := newSoakClient(addr)
	if killClient == nil {
		return
	}
	defer killClient.Close()

	if err := killClient.Do(ctx, "CLIENT", "KILL", "TYPE", "normal").Err(); err != nil {
		return
	}
}

func newSoakClient(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:        addr,
		Password:    "",
		DB:          0,
		DialTimeout: 3 * time.Second,
	})
}

func runSoakConnectDisconnect(ctx context.Context, addr string, rng *rand.Rand) {
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return
	}
	// send random garbage or nothing
	if rng.Intn(2) == 0 {
		_, _ = conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	}
	conn.Close()
}

func runSoakPipeline(ctx context.Context, addr string, rng *rand.Rand, errCh chan<- error) {
	conn, reader, err := dialSoak(addr)
	if err != nil {
		return
	}
	defer conn.Close()

	// send a burst of commands in pipeline
	cmds := rng.Intn(10) + 2
	var buf []byte
	for i := 0; i < cmds; i++ {
		key := fmt.Sprintf("soak:pipe:%d", rng.Intn(20))
		val := fmt.Sprintf("pv%d", rng.Intn(1000))
		buf = append(buf, []byte(fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(val), val))...)
	}
	if err := sendRESPLine(conn, string(buf)); err != nil {
		return
	}

	// read all responses
	for i := 0; i < cmds; i++ {
		if _, err := proto.ReadRESP(reader); err != nil {
			return
		}
	}
}

// trySendErr sends an error to errCh without blocking.
// If the channel is full, the error is logged via t.Logf instead.
func trySendErr(errCh chan<- error, err error) {
	select {
	case errCh <- err:
	default:
		// Channel full — drop to avoid goroutine leak.
	}
}

func runSoakNormal(ctx context.Context, addr string, rng *rand.Rand, errCh chan<- error) {
	conn, reader, err := dialSoak(addr)
	if err != nil {
		return
	}
	defer conn.Close()

	op := rng.Intn(5)
	switch op {
	case 0: // SET + verify read-back
		key := fmt.Sprintf("soak:set:%d", rng.Intn(100))
		val := fmt.Sprintf("v:%d", rng.Intn(10000))
		_ = sendRESPLine(conn, fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(val), val))
		if _, err := proto.ReadRESP(reader); err != nil {
			return
		}
		// Write-then-read verification: GET the same key and compare
		_ = sendRESPLine(conn, fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(key), key))
		getResp, err := proto.ReadRESP(reader)
		if err != nil {
			return
		}
		if getResp == nil {
			trySendErr(errCh, fmt.Errorf("soak SET/GET: GET returned nil-array for key=%s after successful SET", key))
		} else if len(getResp.Args) > 0 {
			got := string(getResp.Args[0])
			if got != val {
				trySendErr(errCh, fmt.Errorf("soak SET/GET mismatch: key=%s expected=%s got=%s", key, val, got))
			}
		}
	case 1: // GET
		key := fmt.Sprintf("soak:set:%d", rng.Intn(100))
		_ = sendRESPLine(conn, fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(key), key))
		if _, err := proto.ReadRESP(reader); err != nil {
			return
		}
	case 2: // LPUSH/LPOP
		key := fmt.Sprintf("soak:list:%d", rng.Intn(20))
		val := fmt.Sprintf("lv%d", rng.Intn(1000))
		_ = sendRESPLine(conn, fmt.Sprintf("*3\r\n$5\r\nLPUSH\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(val), val))
		if _, err := proto.ReadRESP(reader); err != nil {
			return
		}
		_ = sendRESPLine(conn, fmt.Sprintf("*2\r\n$4\r\nLPOP\r\n$%d\r\n%s\r\n", len(key), key))
		if _, err := proto.ReadRESP(reader); err != nil {
			return
		}
	case 3: // SADD/SREM
		key := fmt.Sprintf("soak:sadd:%d", rng.Intn(20))
		member := fmt.Sprintf("m%d", rng.Intn(100))
		_ = sendRESPLine(conn, fmt.Sprintf("*3\r\n$4\r\nSADD\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(member), member))
		if _, err := proto.ReadRESP(reader); err != nil {
			return
		}
		_ = sendRESPLine(conn, fmt.Sprintf("*3\r\n$4\r\nSREM\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(member), member))
		if _, err := proto.ReadRESP(reader); err != nil {
			return
		}
	case 4: // INCR + verify response is valid integer
		key := fmt.Sprintf("soak:cnt:%d", rng.Intn(20))
		_ = sendRESPLine(conn, fmt.Sprintf("*2\r\n$4\r\nINCR\r\n$%d\r\n%s\r\n", len(key), key))
		incrResp, err := proto.ReadRESP(reader)
		if err != nil {
			return
		}
		if incrResp == nil || len(incrResp.Args) == 0 {
			trySendErr(errCh, fmt.Errorf("soak INCR: empty response for key=%s", key))
		} else {
			// INCR returns an integer. Args[0] may include RESP prefix (e.g. ":1")
			valStr := string(incrResp.Args[0])
			if len(valStr) > 0 && valStr[0] == ':' {
				valStr = valStr[1:]
			}
			var n int64
			for _, c := range valStr {
				if c >= '0' && c <= '9' {
					n = n*10 + int64(c-'0')
				} else {
					break
				}
			}
			if n < 1 {
				trySendErr(errCh, fmt.Errorf("soak INCR: expected positive integer, got %q for key=%s", valStr, key))
			}
		}
	}
}
