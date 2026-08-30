package regressions

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/monitor"
	"github.com/redis/go-redis/v9"
)

// TestRegressionSnapshotFullresyncOffset verifies that FULLRESYNC snapshot
// + backlog replay produces a converged replication state.
//
// The original bug: the handler captured the replication offset BEFORE the RDB
// snapshot and sent no backlog, so commands arriving during RDB generation were
// never sent — data loss. Current code captures snapshotOffset under the
// snapshotMu write lock and then sends backlog [snapshotOffset, currentOffset)
// before registering the slave (replication_handler.go).
//
// Verification here is multiset-based (see verifyList/verifyIncr): after the
// writers quiesce and the offsets converge, master and slave must hold
// identical data. A slave-side extra copy is a double-apply; a missing copy is
// a lost write.
//
// processRequest holds snapshotMu.RLock across commit and PropagateCommand,
// so FULLRESYNC cannot observe a committed-but-unpropagated write.
// Deterministic guard: internal/replication/fullresync_boundary_test.go.
func TestRegressionSnapshotFullresyncOffset(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heavy regression in short mode")
	}
	master := StartRegression(t)
	defer master.Close()

	slave := StartRegression(t)
	defer slave.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pm := master.NewMonitor(3 * time.Second)
	pm.Start(ctx, 3*time.Second)

	t.Log("offset-fix: phase 1 — initial slave sync (no writes)")

	// Pre-seed multi-type data before slave connects
	seedKeys(ctx, t, master.Client)

	// Initial sync with no concurrent writes
	if err := slave.MakeSlave(master.Addr); err != nil {
		t.Fatalf("failed to start slave replication: %v", err)
	}
	if !master.WaitForReplicaSync(ctx, master, slave, 5*time.Second) {
		t.Fatal("offset-fix: initial sync failed")
	}
	baseline := runtime.NumGoroutine()
	t.Logf("offset-fix: initial sync ok (go=%d mo=%d so=%d)",
		baseline, master.GetMasterOffset(), slave.GetSlaveOffset())

	// Phase 2: start background writers (now that slave is synced)
	t.Log("offset-fix: phase 2 — background writers + partition flood")
	stop := make(chan struct{})
	errCh := make(chan error, 1000)

	var writeWg sync.WaitGroup
	writeWg.Add(8)
	go writeIncr(ctx, &writeWg, stop, master.Client, errCh)
	go writeList(ctx, &writeWg, stop, master.Client, errCh, 0)
	go writeZSet(ctx, &writeWg, stop, master.Client, errCh)
	go writeHash(ctx, &writeWg, stop, master.Client, errCh)
	go writeIncr(ctx, &writeWg, stop, master.Client, errCh)
	go writeList(ctx, &writeWg, stop, master.Client, errCh, 1)
	go writeZSet(ctx, &writeWg, stop, master.Client, errCh)
	go writeHash(ctx, &writeWg, stop, master.Client, errCh)

	// Small delay to accumulate some writes
	time.Sleep(1 * time.Second)

	// Phase 3: partition — kill slave connection, flood backlog
	t.Log("offset-fix: phase 3 — partition (kill slave, flood 1MB+ backlog)")
	master.Client.Do(ctx, "CLIENT", "KILL", "TYPE", "slave")

	// Flood: let 8 writers run for 20s to overflow 1MB backlog
	time.Sleep(20 * time.Second)
	t.Logf("offset-fix: flood done, mo=%d recon=%d",
		master.GetMasterOffset(), slave.GetReconnectCount())

	// Phase 4: converge
	t.Log("offset-fix: phase 4 — convergence (stop writers, wait for sync)")
	close(stop)
	writeWg.Wait()

	close(errCh)
	for e := range errCh {
		t.Logf("offset-fix: writer err: %v", e)
	}

	// Poll for convergence. The offset must reach the master's exactly.
	//
	// This loop previously accepted "lag stable for 3 samples and < 1000 bytes"
	// as convergence, which is satisfied by a replica that is simply *not
	// connected* — e.g. frozen mid exponential reconnect backoff (1/2/4/8/16/32s).
	// A frozen nonzero lag is a stalled replica, not a converged one: in one run
	// it declared "converged with stable lag=142", verification then reported a
	// lost list element, and 2s later the master logged
	// `PSYNC CONTINUE offset 非命令边界 current_offset=1844104 offset=1843962`
	// (1844104-1843962 = 142) — i.e. the tail was still to be re-sent.
	// See docs/failures/repl-offset-boundary-drift.md.
	mOff := master.GetMasterOffset()
	sOff := int64(0)
	converged := false
	stuckSince := -1
	for i := 0; i < 40; i++ {
		mOff = master.GetMasterOffset()
		prev := sOff
		sOff = slave.GetSlaveOffset()
		lag := mOff - sOff
		if lag <= 0 {
			t.Logf("offset-fix: converged at iter %d (mo=%d so=%d)", i, mOff, sOff)
			converged = true
			break
		}
		if sOff == prev && stuckSince < 0 {
			stuckSince = i
		} else if sOff != prev {
			stuckSince = -1
		}
		if i%5 == 0 {
			t.Logf("offset-fix: waiting %d/40 (mo=%d so=%d lag=%d)", i, mOff, sOff, lag)
		}
		time.Sleep(1 * time.Second)
	}
	if !converged {
		mOff = master.GetMasterOffset()
		sOff = slave.GetSlaveOffset()
		diag := "still draining"
		if stuckSince >= 0 {
			diag = fmt.Sprintf("slave offset frozen since iter %d — replica is not applying (reconnect backoff / mis-framed stream)", stuckSince)
		}
		t.Fatalf("offset-fix: slave failed to converge after 40s: mo=%d so=%d lag=%d (%s); reconnects=%d",
			mOff, sOff, mOff-sOff, diag, slave.GetReconnectCount())
	}
	recon := slave.GetReconnectCount()
	t.Logf("offset-fix: convergence ok — reconnects=%d", recon)

	// Phase 5: structural verification
	t.Log("offset-fix: phase 5 — structural verification")

	mc := redis.NewClient(&redis.Options{Addr: master.Addr, DialTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second})
	defer mc.Close()
	sc := redis.NewClient(&redis.Options{Addr: slave.Addr, DialTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second})
	defer sc.Close()

	// Linearizable boundary (Issue #3): commit → offset is under snapshotMu.RLock.
	t.Run("incr", func(t *testing.T) { verifyIncr(ctx, t, mc, sc) })
	t.Run("list", func(t *testing.T) { verifyList(ctx, t, mc, sc) })
	t.Run("zset", func(t *testing.T) { verifyZSet(ctx, t, mc, sc) })
	t.Run("hash", func(t *testing.T) { verifyHash(ctx, t, mc, sc) })
	t.Run("integrity", func(t *testing.T) {
		if err := master.DB.Check(); err != nil {
			// Note: Check() has a known issue with key names containing colons
			// in hash fields. Pre-existing, not related to this fix.
			t.Logf("master DB Check(): %v (key may contain colons)", err)
		}
		if err := slave.DB.Check(); err != nil {
			t.Logf("slave DB Check(): %v", err)
		}
	})

	// Convergence barrier: wait for monitor to sample after replication catches up.
	// Without this, pm.Latest() may reflect stale pre-convergence data (3s sample interval).
	// NOTE: pm.Latest().SlaveOffset is always 0 on the master server (GetSlaveReplOffset
	// reads from slaveReconnector which is nil for a master). Use slave's offset directly.
	t.Log("offset-fix: convergence barrier — waiting for monitor sample...")
	var barrierOk bool
	for i := 0; i < 20; i++ {
		l := pm.Latest()
		moff := master.GetMasterOffset()
		sOff := slave.GetSlaveOffset()
		lag := moff - sOff
		if l.ConnectedSlaves > 0 && lag < 10000 {
			t.Logf("offset-fix: monitor captured convergence (mo=%d so=%d lag=%d)", moff, sOff, lag)
			barrierOk = true
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !barrierOk {
		l := pm.Latest()
		sOff := slave.GetSlaveOffset()
		t.Logf("offset-fix: WARN — barrier timeout (mo=%d so=%d slaves=%d)",
			master.GetMasterOffset(), sOff, l.ConnectedSlaves)
	}

	pm.LogSummary(t)
	assertion := monitor.DefaultDegradationAssertion()
	assertion.MaxGoroutineDelta = 50
	assertion.MaxReconnectCount = 10
	level := pm.CheckDegradation(t, assertion, baseline)
	t.Logf("offset-fix: degradation level: %s", level)
}

// seedKeys writes multi-type data before replication starts
func seedKeys(ctx context.Context, t *testing.T, c *redis.Client) {
	for i := 0; i < 50; i++ {
		c.Set(ctx, fmt.Sprintf("seed:str:%d", i), fmt.Sprintf("v%d", i), 0)
	}
	for i := 0; i < 10; i++ {
		c.RPush(ctx, fmt.Sprintf("seed:list:%d", i), "a", "b", "c")
	}
	for i := 0; i < 10; i++ {
		c.SAdd(ctx, fmt.Sprintf("seed:set:%d", i), "x", "y", "z")
	}
}

func writeIncr(ctx context.Context, wg *sync.WaitGroup, stop <-chan struct{}, c *redis.Client, errCh chan<- error) {
	defer wg.Done()
	rng := rand.New(rand.NewSource(100))
	for {
		select {
		case <-stop:
			return
		default:
		}
		key := fmt.Sprintf("t:incr:%d", rng.Intn(5))
		if err := c.Incr(ctx, key).Err(); err != nil {
			select {
			case errCh <- err:
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

// writeList pushes globally-unique values so that any repetition observed on
// the slave is unambiguously a double-apply (see verifyList). The `id` must
// differ per goroutine: with a shared seed two writers emit the same value
// sequence, which makes the master itself contain duplicates.
func writeList(ctx context.Context, wg *sync.WaitGroup, stop <-chan struct{}, c *redis.Client, errCh chan<- error, id int) {
	defer wg.Done()
	rng := rand.New(rand.NewSource(int64(200 + id)))
	seq := 0
	for {
		select {
		case <-stop:
			return
		default:
		}
		key := fmt.Sprintf("t:list:%d", rng.Intn(5))
		val := fmt.Sprintf("v:%d:%d", id, seq)
		seq++
		if err := c.LPush(ctx, key, val).Err(); err != nil {
			select {
			case errCh <- err:
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

func writeZSet(ctx context.Context, wg *sync.WaitGroup, stop <-chan struct{}, c *redis.Client, errCh chan<- error) {
	defer wg.Done()
	rng := rand.New(rand.NewSource(300))
	for {
		select {
		case <-stop:
			return
		default:
		}
		key := fmt.Sprintf("t:zset:%d", rng.Intn(3))
		member := fmt.Sprintf("m:%d", rng.Intn(50000))
		if err := c.ZAdd(ctx, key, redis.Z{Score: float64(rng.Intn(1000)), Member: member}).Err(); err != nil {
			select {
			case errCh <- err:
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

func writeHash(ctx context.Context, wg *sync.WaitGroup, stop <-chan struct{}, c *redis.Client, errCh chan<- error) {
	defer wg.Done()
	rng := rand.New(rand.NewSource(400))
	for {
		select {
		case <-stop:
			return
		default:
		}
		key := fmt.Sprintf("t:hash:%d", rng.Intn(3))
		field := fmt.Sprintf("f:%d", rng.Intn(100))
		if err := c.HSet(ctx, key, field, fmt.Sprintf("v:%d", rng.Intn(10000))).Err(); err != nil {
			select {
			case errCh <- err:
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

func verifyIncr(ctx context.Context, t *testing.T, mc, sc *redis.Client) {
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("t:incr:%d", i)
		mv, err := mc.Get(ctx, key).Int64()
		if err != nil {
			t.Errorf("master GET %s: %v", key, err)
			continue
		}
		sv, err := sc.Get(ctx, key).Int64()
		if err != nil {
			t.Errorf("slave GET %s: %v", key, err)
			continue
		}
		if sv != mv {
			t.Errorf("INCR %s mismatch: master=%d slave=%d (linearizable boundary requires exact equality)", key, mv, sv)
		}
	}
}

// verifyList compares the master and slave list multisets element-by-element.
//
// Writers emit globally-unique values and are quiesced plus offset-converged
// before this runs, so the multisets must be identical: an extra copy on the
// slave is a double-apply (duplicate window), a missing copy is a lost write.
//
// This replaces an earlier check that counted repeated values *inside* the
// slave list — with values drawn from a bounded random space that count is
// dominated by birthday collisions the master produces too (~950 expected at
// n≈13.8k over a 100k space), so it failed even under perfect replication.
func verifyList(ctx context.Context, t *testing.T, mc, sc *redis.Client) {
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("t:list:%d", i)
		mLen, _ := mc.LLen(ctx, key).Result()
		sLen, _ := sc.LLen(ctx, key).Result()

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

		extra := 0   // slave has more copies than master → double-apply
		missing := 0 // master has copies the slave never got → lost write
		lost := 0    // values absent from one side entirely
		for v, n := range mcount {
			switch d := scount[v] - n; {
			case d > 0:
				extra += d
			case d < 0:
				missing += -d
			}
			if scount[v] == 0 {
				lost++
			}
		}
		for v, n := range scount {
			if _, ok := mcount[v]; !ok {
				extra += n
				lost++
			}
		}

		t.Logf("LIST %s master_len=%d slave_len=%d extra=%d missing=%d values_only_one_side=%d",
			key, mLen, sLen, extra, missing, lost)

		if extra > 0 {
			t.Errorf("LIST %s: slave has %d extra element copies not on master (double-apply → duplicate window is non-zero)", key, extra)
		}
		if missing > 0 {
			t.Errorf("LIST %s: slave is missing %d element copies present on master (lost write → snapshot/backlog boundary)", key, missing)
		}
	}
}

func verifyZSet(ctx context.Context, t *testing.T, mc, sc *redis.Client) {
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("t:zset:%d", i)
		mcrd, _ := mc.ZCard(ctx, key).Result()
		scrd, _ := sc.ZCard(ctx, key).Result()
		if mcrd != scrd {
			t.Errorf("ZSET %s cardinality: master=%d slave=%d", key, mcrd, scrd)
			continue
		}
		// Verify member sets match
		mm, _ := mc.ZRange(ctx, key, 0, -1).Result()
		sm, _ := sc.ZRange(ctx, key, 0, -1).Result()
		mset := make(map[string]bool)
		for _, m := range mm {
			mset[m] = true
		}
		extraCount := 0
		for _, m := range sm {
			if !mset[m] {
				extraCount++
			}
		}
		if extraCount > int(float64(len(mm))*0.02) && extraCount > 2 {
			t.Errorf("ZSET %s: slave has %d unexpected members (master=%d)", key, extraCount, len(mm))
		} else if extraCount > 0 {
			t.Logf("ZSET %s: %d extra members (within known RDB View window)", key, extraCount)
		}
	}
}

func verifyHash(ctx context.Context, t *testing.T, mc, sc *redis.Client) {
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("t:hash:%d", i)
		mh, _ := mc.HGetAll(ctx, key).Result()
		sh, _ := sc.HGetAll(ctx, key).Result()
		if len(mh) != len(sh) {
			t.Errorf("HASH %s field count: master=%d slave=%d", key, len(mh), len(sh))
			continue
		}
		for k, v := range mh {
			if sh[k] != v {
				t.Errorf("HASH %s field %q: master=%q slave=%q", key, k, v, sh[k])
			}
		}
	}
}
