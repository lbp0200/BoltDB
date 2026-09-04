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

	"github.com/lbp0200/BoltDB/internal/store"
)

// TestRegressionDuplicateWindowMeasurement checks master/slave convergence
// across a FULLRESYNC taken while writes are in flight (Issue #3).
//
// List values are globally unique and INCR counters are monotonic, so any
// slave-side surplus is an unambiguous double-apply and any deficit an
// unambiguous lost write.
//
// Methodology:
//  1. Pre-seed data, connect slave, wait for initial sync (no concurrent writes)
//  2. Start high-concurrency INCR/LPUSH writers
//  3. Kill slave connection to trigger FULLRESYNC with writes in flight
//  4. Wait for convergence (data-based marker)
//  5. Assert exact INCR/LPUSH convergence
//
// A pass is necessary but not sufficient: the duplicate window only catches
// writes sitting between commit and PropagateCommand at the instant
// snapshotOffset is read (~1 FULLRESYNC in several hundred at this rate), so
// the storm rarely lands in it. internal/replication/fullresync_boundary_test.go
// proves the window non-zero without racing for it.
// See docs/failures/snapshot-inconsistency.md §4.
func TestRegressionDuplicateWindowMeasurement(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heavy regression in short mode")
	}
	masterDB, err := store.NewBotreonStore(t.TempDir())
	if err != nil {
		t.Fatalf("dw-measure: create store: %v", err)
	}
	master := StartRegressionWithStore(t, masterDB)
	defer master.Close()
	defer masterDB.Close()

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
	if !master.WaitForReplicaSync(ctx, master, slave, 5*time.Second) {
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
			if sOff >= mOff {
				t.Logf("dw-measure: marker visible and offsets converged at iter %d (mo=%d so=%d lag=%d send_drop=%d apply_skip=%d)",
					i, mOff, sOff, mOff-sOff, master.ReplSendDropCount(), slave.ReplApplySkipCount())
				converged = true
				break
			}
			// marker 可见但从节点 offset 仍落后：可能是尾巴缺口（命令已入
			// backlog 但投递静默中断）。停滞检测（readCommandLoop + GETACK）
			// 会触发重连自愈，这里继续等待 offset 收敛，避免在缺口未补齐时
			// 测量出假亏空。
			t.Logf("dw-measure: marker visible but slave behind (mo=%d so=%d lag=%d) — waiting for tail catch-up", mOff, sOff, mOff-sOff)
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
	sendDrop := master.ReplSendDropCount()
	applySkip := slave.ReplApplySkipCount()

	// Re-read offsets at measure time — the poll-loop values are stale
	// (captured before the marker GET), which can mask a still-caught-up
	// replica or a genuine tail gap.
	freshMo := master.GetMasterOffset()
	freshSo := slave.GetSlaveOffset()
	t.Logf("dw-measure: fresh offsets at measure (mo=%d so=%d lag=%d)", freshMo, freshSo, freshMo-freshSo)

	t.Logf("dw-measure: ===== DUPLICATE WINDOW MEASUREMENTS =====")
	t.Logf("dw-measure: INCR max_gap=%d (threshold=0)", metrics.incrMaxGap)
	t.Logf("dw-measure: INCR total_master=%d total_slave=%d", metrics.incrTotalM, metrics.incrTotalS)
	t.Logf("dw-measure: LPUSH extra_on_slave=%d missing_on_slave=%d (threshold=0/0)", metrics.lpushExtra, metrics.lpushMissing)
	t.Logf("dw-measure: HSET match=%v ZSET match=%v", metrics.hsetMatch, metrics.zsetMatch)
	t.Logf("dw-measure: drop-paths send_drop=%d apply_skip=%d (mo=%d so=%d)", sendDrop, applySkip, mOff, sOff)

	// Exact-equality assertions. These are sound now that writers emit unique
	// values (an earlier revision counted repeats inside the slave list, which
	// birthday collisions on the master make unsatisfiable).
	//
	// Storm probe for Issue #3. The deterministic certificate is
	// internal/replication/fullresync_boundary_test.go (fence spans commit →
	// offset). A pass here is necessary but the unit test is the proof.
	if applySkip != 0 {
		t.Errorf("dw-measure: repl_apply_skip_count=%d != 0 (readCommandLoop skipped apply but advanced offset — silent data loss)", applySkip)
	}
	if sendDrop != 0 {
		t.Logf("dw-measure: repl_send_drop_count=%d (live SendCommand failed; command remains in backlog)", sendDrop)
	}

	if metrics.incrMaxGap != 0 {
		t.Errorf("dw-measure: INCR max gap %d != 0 (boundary violated; send_drop=%d apply_skip=%d)", metrics.incrMaxGap, sendDrop, applySkip)
	}
	netDelta := metrics.incrTotalS - metrics.incrTotalM
	if netDelta != 0 {
		t.Errorf("dw-measure: INCR total delta %d != 0 (boundary violated; send_drop=%d apply_skip=%d)", netDelta, sendDrop, applySkip)
	}
	if metrics.lpushExtra != 0 {
		t.Errorf("dw-measure: LPUSH %d extra element copies on slave (double-apply: write present in both RDB and backlog; send_drop=%d apply_skip=%d)", metrics.lpushExtra, sendDrop, applySkip)
	}
	if metrics.lpushMissing != 0 {
		t.Errorf("dw-measure: LPUSH %d element copies missing on slave (lost write at the snapshot/backlog boundary; send_drop=%d apply_skip=%d)", metrics.lpushMissing, sendDrop, applySkip)
		if sendDrop != 0 && applySkip == 0 {
			t.Errorf("dw-measure: LIST deficit with send_drop=%d apply_skip=0 — send-side drop is the remaining suspect (backlog/FULLRESYNC may not have recovered it)", sendDrop)
		}
		if sendDrop == 0 && applySkip == 0 {
			t.Errorf("dw-measure: LIST deficit with both drop-paths at 0 — hole is not from SendCommand failure or transient apply skip")
		}
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

	// ts 双轨（S2——④）：master 的传播日志键覆盖全部写入（dw 测量的 ts 透镜补充——
	// commit 即记日志——日志键计数 > 0）。
	logEntries, err := masterDB.ReplLogEntries()
	if err != nil {
		t.Fatalf("dw-measure: ReplLogEntries: %v", err)
	}
	if len(logEntries) == 0 {
		t.Errorf("dw-measure: no repl log entries on master (ts lens)")
	}
}

type dwMetrics struct {
	incrMaxGap   int64
	incrTotalM   int64
	incrTotalS   int64
	lpushExtra   int
	lpushMissing int
	hsetMatch    bool
	zsetMatch    bool
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
		if gap != 0 {
			t.Errorf("dw-measure: INCR %s mismatch: master=%d slave=%d gap=%d (linearizable boundary requires 0)", key, mv, sv, gap)
		}
		absGap := gap
		if absGap < 0 {
			absGap = -absGap
		}
		if absGap > m.incrMaxGap {
			m.incrMaxGap = absGap
		}
		m.incrTotalM += mv
		m.incrTotalS += sv
		t.Logf("dw-measure: INCR %s master=%d slave=%d gap=%d", key, mv, sv, gap)
	}

	m.lpushExtra = 0
	m.lpushMissing = 0
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("dw:list:%d", i)
		mLen, _ := mc.LLen(ctx, key).Result()
		sLen, _ := sc.LLen(ctx, key).Result()

		// Writers emit globally-unique values and are quiesced, so the two
		// multisets must match exactly: extra copies on the slave are a
		// double-apply, missing copies are a lost write.
		me, _ := mc.LRange(ctx, key, 0, -1).Result()
		se, _ := sc.LRange(ctx, key, 0, -1).Result()
		mcount := make(map[string]int, len(me))
		for _, e := range me {
			mcount[e]++
		}
		scount := make(map[string]int, len(se))
		for _, e := range se {
			scount[e]++
		}
		var extra, missing int
		for v, n := range mcount {
			if d := scount[v] - n; d > 0 {
				extra += d
			} else if d < 0 {
				missing += -d
				t.Logf("dw-measure: MISSING value %q on slave: master_count=%d slave_count=%d (list=%s)", v, n, scount[v], key)
			}
		}
		for v, n := range scount {
			if _, ok := mcount[v]; !ok {
				extra += n
			}
		}
		m.lpushExtra += extra
		m.lpushMissing += missing

		t.Logf("dw-measure: LIST %s master_len=%d slave_len=%d extra_on_slave=%d missing_on_slave=%d",
			key, mLen, sLen, extra, missing)
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
	seq := 0
	for {
		select {
		case <-stop:
			return
		default:
		}
		key := fmt.Sprintf("dw:list:%d", rng.Intn(5))
		// Unique per push: makes any slave-side repeat a double-apply rather
		// than a birthday collision from a bounded value space.
		val := fmt.Sprintf("lv:%d:%d", id, seq)
		seq++
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
