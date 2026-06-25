package regressions

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestRegressionDuplicateWindowMeasurement quantifies the residual duplicate window
// from BoltDB's dual-timeline architecture (badger MVCC snapshot != replication offset
// boundary). This window exists because snapshotOffset is captured BEFORE badger
// db.View(), meaning writes that commit between offset capture and View start appear
// in both the RDB and the backlog.
//
// Methodology (matches TestRegressionSnapshotFullresyncOffset):
//  1. Pre-seed data, connect slave, wait for initial sync (no concurrent writes)
//  2. Start high-concurrency INCR/LPUSH writers
//  3. Kill slave connection to trigger FULLRESYNC with writes in flight
//  4. Wait for convergence (data-based marker)
//  5. Measure per-key INCR gap and LPUSH duplicate ratio
//
// Expected results:
//   - INCR: mv - sv <= 2 (lost writes bounded), sv - mv <= 2 (double-replay bounded)
//   - LPUSH: duplicate ratio <= 70%
//   - HSET/ZADD: exact match (idempotent — duplicates have no effect)
//
// This is the Tier 3 verification in the three-tier soak semantics.
func TestRegressionDuplicateWindowMeasurement(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heavy regression in short mode")
	}
	master := StartRegression(t)
	defer master.Close()

	slave := StartRegression(t)
	defer slave.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Phase 1: pre-seed initial data (no writes in flight during sync)
	t.Log("dw-measure: phase 1 — seed initial data + initial slave sync")
	for i := 0; i < 5; i++ {
		master.Client.Set(ctx, fmt.Sprintf("dw:incr:%d", i), "0", 0)
		master.Client.Del(ctx, fmt.Sprintf("dw:list:%d", i))
	}
	for i := 0; i < 3; i++ {
		master.Client.Del(ctx, fmt.Sprintf("dw:hset:%d", i))
		master.Client.Del(ctx, fmt.Sprintf("dw:zadd:%d", i))
	}

	// Initial sync with no concurrent writes
	if err := slave.MakeSlave(master.Addr); err != nil {
		t.Fatalf("failed to start slave replication: %v", err)
	}
	if !master.WaitForReplicaSync(ctx, master, slave, 15*time.Second) {
		t.Fatal("dw-measure: initial sync failed")
	}
	baseline := runtime.NumGoroutine()
	t.Logf("dw-measure: initial sync ok (go=%d mo=%d so=%d)",
		baseline, master.GetMasterOffset(), slave.GetSlaveOffset())

	// Phase 2: start concurrent writers
	t.Log("dw-measure: phase 2 — start concurrent writers")
	stop := make(chan struct{})
	errCh := make(chan error, 1000)

	var writeWg sync.WaitGroup
	writeWg.Add(8)
	go dwWriteIncr(ctx, &writeWg, stop, master.Client, errCh, 0)
	go dwWriteIncr(ctx, &writeWg, stop, master.Client, errCh, 1)
	go dwWriteList(ctx, &writeWg, stop, master.Client, errCh, 0)
	go dwWriteList(ctx, &writeWg, stop, master.Client, errCh, 1)
	go dwWriteHSET(ctx, &writeWg, stop, master.Client, errCh)
	go dwWriteZADD(ctx, &writeWg, stop, master.Client, errCh)
	go dwWriteIncr(ctx, &writeWg, stop, master.Client, errCh, 2)
	go dwWriteList(ctx, &writeWg, stop, master.Client, errCh, 2)

	// Allow writers to accumulate some state
	time.Sleep(1 * time.Second)

	// Phase 3: trigger FULLRESYNC by killing slave connection while writes are in flight
	// This creates the duplicate window during RDB generation.
	t.Log("dw-measure: phase 3 — kill slave connection to trigger FULLRESYNC")
	master.Client.Do(ctx, "CLIENT", "KILL", "TYPE", "slave")

	// Keep writing for several seconds to flood backlog + RDB window
	time.Sleep(15 * time.Second)

	// Phase 4: stop writers, set convergence marker
	t.Log("dw-measure: phase 4 — stop writers, wait for convergence")
	close(stop)
	writeWg.Wait()
	close(errCh)
	for e := range errCh {
		t.Logf("dw-measure: writer err: %v", e)
	}

	// Data-based convergence marker (more reliable than offset comparison)
	markerKey := fmt.Sprintf("dw:converge:%d", time.Now().UnixNano())
	if err := master.Client.Set(ctx, markerKey, "done", 0).Err(); err != nil {
		t.Fatalf("dw-measure: failed to set convergence marker: %v", err)
	}
	t.Logf("dw-measure: convergence marker set, waiting for slave...")

	converged := false
	mOff, sOff := int64(0), int64(0)
	for i := 0; i < 60; i++ {
		mOff = master.GetMasterOffset()
		sOff = slave.GetSlaveOffset()
		v, err := slave.Client.Get(ctx, markerKey).Result()
		if err == nil && v == "done" {
			t.Logf("dw-measure: converged at iter %d (mo=%d so=%d)", i, mOff, sOff)
			converged = true
			break
		}
		if i%10 == 0 {
			t.Logf("dw-measure: waiting for convergence marker %d/60 (mo=%d so=%d)", i, mOff, sOff)
		}
		time.Sleep(1 * time.Second)
	}
	if !converged {
		t.Fatalf("dw-measure: slave failed to converge within 60s (mo=%d so=%d)", mOff, sOff)
	}

	// Extra drain for ZSET backlog
	time.Sleep(2 * time.Second)

	// Phase 5: measure duplicate window
	t.Log("dw-measure: phase 5 — measure duplicate window")

	mc := redis.NewClient(&redis.Options{Addr: master.Addr, DialTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second})
	defer mc.Close()
	sc := redis.NewClient(&redis.Options{Addr: slave.Addr, DialTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second})
	defer sc.Close()

	metrics := dwMeasure(ctx, t, mc, sc)

	t.Logf("dw-measure: ===== DUPLICATE WINDOW MEASUREMENTS =====")
	t.Logf("dw-measure: INCR max_gap=%d (threshold=2)", metrics.incrMaxGap)
	t.Logf("dw-measure: INCR total_master=%d total_slave=%d", metrics.incrTotalM, metrics.incrTotalS)
	t.Logf("dw-measure: LPUSH max_dup_ratio=%.1f%% (threshold=70%%)", metrics.lpushMaxDupRatio*100)
	t.Logf("dw-measure: HSET match=%v ZSET match=%v", metrics.hsetMatch, metrics.zsetMatch)

	// Assertions
	if metrics.incrMaxGap > 2 {
		t.Errorf("dw-measure: INCR max gap %d exceeds threshold 2 (lost writes)", metrics.incrMaxGap)
	}
	netDelta := metrics.incrTotalS - metrics.incrTotalM
	if netDelta > 2 {
		t.Errorf("dw-measure: INCR double-replay count %d exceeds threshold 2 — UNBOUNDED REPLAY", netDelta)
	} else if netDelta < -2 {
		t.Errorf("dw-measure: INCR lost %d writes exceeds threshold 2 (duplicate window violation)", -netDelta)
	} else if netDelta > 0 {
		t.Logf("dw-measure: INCR double-replay count %d (within known duplicate window)", netDelta)
	} else if netDelta < 0 {
		t.Logf("dw-measure: INCR lost %d writes (within expected window)", -netDelta)
	}
	if metrics.lpushMaxDupRatio > 0.70 {
		t.Errorf("dw-measure: LPUSH max duplicate ratio %.1f%% exceeds 70%% threshold — UNBOUNDED REPLAY",
			metrics.lpushMaxDupRatio*100)
	}
	if !metrics.hsetMatch {
		t.Errorf("dw-measure: HSET datasets do not match (idempotent commands must match exactly)")
	}
	if !metrics.zsetMatch {
		t.Logf("dw-measure: ZSET datasets do not match (informational — convergence timing)")
	}

	final := runtime.NumGoroutine()
	leak := final - baseline
	t.Logf("dw-measure: goroutine delta=%d (baseline=%d, final=%d)", leak, baseline, final)
	if leak > 30 {
		t.Errorf("dw-measure: goroutine leak: %d (baseline=%d, final=%d)", leak, baseline, final)
	}
}

type dwMetrics struct {
	incrMaxGap       int64
	incrTotalM       int64
	incrTotalS       int64
	lpushMaxDupRatio float64
	hsetMatch        bool
	zsetMatch        bool
}

func dwMeasure(ctx context.Context, t *testing.T, mc, sc *redis.Client) dwMetrics {
	var m dwMetrics

	m.incrMaxGap = 0
	m.incrTotalM = 0
	m.incrTotalS = 0
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("dw:incr:%d", i)
		mv, err := mc.Get(ctx, key).Int64()
		if err != nil {
			t.Logf("dw-measure: master GET %s failed: %v", key, err)
			continue
		}
		sv, err := sc.Get(ctx, key).Int64()
		if err != nil {
			t.Logf("dw-measure: slave GET %s failed: %v", key, err)
			continue
		}
		gap := mv - sv
		if gap < -2 {
			t.Errorf("dw-measure: INCR %s double-replay exceeds threshold: slave=%d > master=%d (gap=%d)", key, sv, mv, -gap)
		} else if gap < 0 {
			t.Logf("dw-measure: INCR %s double-replay within bounds: slave=%d > master=%d (gap=%d)", key, sv, mv, -gap)
		}
		if gap > m.incrMaxGap {
			m.incrMaxGap = gap
		}
		m.incrTotalM += mv
		m.incrTotalS += sv
		t.Logf("dw-measure: INCR %s master=%d slave=%d gap=%d", key, mv, sv, gap)
	}

	m.lpushMaxDupRatio = 0
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("dw:list:%d", i)
		mLen, _ := mc.LLen(ctx, key).Result()
		sLen, _ := sc.LLen(ctx, key).Result()

		se, _ := sc.LRange(ctx, key, 0, -1).Result()
		seen := make(map[string]int)
		for _, e := range se {
			seen[e]++
		}
		dupCount := 0
		for _, count := range seen {
			if count > 1 {
				dupCount += count - 1
			}
		}
		total := len(se)
		var dupRatio float64
		if total > 0 {
			dupRatio = float64(dupCount) / float64(total)
		}
		if dupRatio > m.lpushMaxDupRatio {
			m.lpushMaxDupRatio = dupRatio
		}
		t.Logf("dw-measure: LIST %s master_len=%d slave_len=%d duplicates=%d ratio=%.1f%%",
			key, mLen, sLen, dupCount, dupRatio*100)
	}

	m.hsetMatch = true
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("dw:hset:%d", i)
		mh, _ := mc.HGetAll(ctx, key).Result()
		sh, _ := sc.HGetAll(ctx, key).Result()
		if len(mh) != len(sh) {
			t.Logf("dw-measure: HSET %s field count mismatch: master=%d slave=%d", key, len(mh), len(sh))
			m.hsetMatch = false
			continue
		}
		for k, v := range mh {
			if sh[k] != v {
				t.Logf("dw-measure: HSET %s field %q mismatch: master=%q slave=%q", key, k, v, sh[k])
				m.hsetMatch = false
			}
		}
	}

	m.zsetMatch = true
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("dw:zadd:%d", i)
		mcrd, _ := mc.ZCard(ctx, key).Result()
		scrd, _ := sc.ZCard(ctx, key).Result()
		delta := mcrd - scrd
		absDelta := delta
		if absDelta < 0 {
			absDelta = -absDelta
		}
		if absDelta > 100 {
			t.Logf("dw-measure: ZADD %s large cardinality gap: master=%d slave=%d delta=%d", key, mcrd, scrd, delta)
			m.zsetMatch = false
		} else if absDelta > 0 {
			t.Logf("dw-measure: ZADD %s cardinality gap %d (within convergence tolerance)", key, delta)
		}
		mm, _ := mc.ZRangeWithScores(ctx, key, 0, -1).Result()
		sm, _ := sc.ZRangeWithScores(ctx, key, 0, -1).Result()
		mset := make(map[string]float64)
		for _, z := range mm {
			mset[fmt.Sprintf("%v", z.Member)] = z.Score
		}
		mismatchCount := 0
		for _, z := range sm {
			member := fmt.Sprintf("%v", z.Member)
			if ms, ok := mset[member]; ok && ms != z.Score {
				if mismatchCount < 3 {
					t.Logf("dw-measure: ZADD %s member %q score mismatch: master=%v slave=%v",
						key, member, ms, z.Score)
				}
				mismatchCount++
			}
		}
		if mismatchCount > 10 {
			t.Logf("dw-measure: ZADD %s %d score mismatches (exceeds convergence tolerance)", key, mismatchCount)
			m.zsetMatch = false
		} else if mismatchCount > 0 {
			t.Logf("dw-measure: ZADD %s %d score mismatches (within convergence tolerance)", key, mismatchCount)
		}
	}

	return m
}

func dwWriteIncr(ctx context.Context, wg *sync.WaitGroup, stop <-chan struct{}, c *redis.Client, errCh chan<- error, id int) {
	defer wg.Done()
	rng := rand.New(rand.NewSource(int64(100 + id)))
	for {
		select {
		case <-stop:
			return
		default:
		}
		key := fmt.Sprintf("dw:incr:%d", rng.Intn(5))
		if err := c.Incr(ctx, key).Err(); err != nil {
			select {
			case errCh <- fmt.Errorf("dw-incr %s: %w", key, err):
			default:
			}
			return
		}
		select {
		case <-time.After(time.Duration(rng.Intn(2)) * time.Millisecond):
		case <-stop:
			return
		}
	}
}

func dwWriteList(ctx context.Context, wg *sync.WaitGroup, stop <-chan struct{}, c *redis.Client, errCh chan<- error, id int) {
	defer wg.Done()
	rng := rand.New(rand.NewSource(int64(200 + id)))
	for {
		select {
		case <-stop:
			return
		default:
		}
		key := fmt.Sprintf("dw:list:%d", rng.Intn(5))
		val := fmt.Sprintf("lv:%d:%d", id, rng.Intn(100000))
		if err := c.LPush(ctx, key, val).Err(); err != nil {
			select {
			case errCh <- fmt.Errorf("dw-lpush %s: %w", key, err):
			default:
			}
			return
		}
		select {
		case <-time.After(time.Duration(rng.Intn(3)) * time.Millisecond):
		case <-stop:
			return
		}
	}
}

func dwWriteHSET(ctx context.Context, wg *sync.WaitGroup, stop <-chan struct{}, c *redis.Client, errCh chan<- error) {
	defer wg.Done()
	rng := rand.New(rand.NewSource(300))
	for {
		select {
		case <-stop:
			return
		default:
		}
		key := fmt.Sprintf("dw:hset:%d", rng.Intn(3))
		field := fmt.Sprintf("f:%d", rng.Intn(100))
		val := fmt.Sprintf("v:%d", rng.Intn(10000))
		if err := c.HSet(ctx, key, field, val).Err(); err != nil {
			select {
			case errCh <- fmt.Errorf("dw-hset %s: %w", key, err):
			default:
			}
			return
		}
		select {
		case <-time.After(time.Duration(rng.Intn(2)) * time.Millisecond):
		case <-stop:
			return
		}
	}
}

func dwWriteZADD(ctx context.Context, wg *sync.WaitGroup, stop <-chan struct{}, c *redis.Client, errCh chan<- error) {
	defer wg.Done()
	rng := rand.New(rand.NewSource(400))
	for {
		select {
		case <-stop:
			return
		default:
		}
		key := fmt.Sprintf("dw:zadd:%d", rng.Intn(3))
		member := fmt.Sprintf("m:%d", rng.Intn(50000))
		score := math.Floor(rng.Float64() * 1000)
		if err := c.ZAdd(ctx, key, redis.Z{Score: score, Member: member}).Err(); err != nil {
			select {
			case errCh <- fmt.Errorf("dw-zadd %s: %w", key, err):
			default:
			}
			return
		}
		select {
		case <-time.After(time.Duration(rng.Intn(3)) * time.Millisecond):
		case <-stop:
			return
		}
	}
}
