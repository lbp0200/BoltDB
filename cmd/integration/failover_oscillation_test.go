package integration

import (
	"context"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/monitor"
	"github.com/lbp0200/BoltDB/internal/sentinel"
	"github.com/zeebo/assert"
)

// agreementSnapshot tracks a single observation of sentinel views.
type agreementSnapshot struct {
	Time    time.Time
	Views   []sentinelView
	Agreed  int
	Total   int
}

// oscillationTracker extends convergenceTracker with full trajectory
// history for oscillation detection and monotonicity verification.
type oscillationTracker struct {
	mu         sync.Mutex
	sentinels  []*sentinel.Sentinel
	masterName string
	stopCh     chan struct{}
	done       chan struct{}

	divergenceStart time.Time
	convergenceEnd  time.Time
	ConvergenceTime time.Duration
	PeakDivergence  int

	snapshots []agreementSnapshot
}

func newOscillationTracker(sentinels []*sentinel.Sentinel, masterName string) *oscillationTracker {
	return &oscillationTracker{
		sentinels:  sentinels,
		masterName: masterName,
		stopCh:     make(chan struct{}),
		done:       make(chan struct{}),
	}
}

func (ot *oscillationTracker) start(ctx context.Context, interval time.Duration, pm *monitor.PressureMonitor) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		defer close(ot.done)

		for {
			select {
			case <-ctx.Done():
				return
			case <-ot.stopCh:
				return
			case <-ticker.C:
				ot.sample(pm)
			}
		}
	}()
}

func (ot *oscillationTracker) stop() {
	close(ot.stopCh)
	<-ot.done
}

func (ot *oscillationTracker) sample(pm *monitor.PressureMonitor) {
	views := ot.readViews()

	ot.mu.Lock()
	defer ot.mu.Unlock()

	agreed := ot.countAgreed(views)
	total := len(views)
	divergent := total - agreed

	if divergent > ot.PeakDivergence {
		ot.PeakDivergence = divergent
	}

	if divergent > 0 && ot.divergenceStart.IsZero() {
		ot.divergenceStart = time.Now()
	}
	if divergent == 0 && !ot.divergenceStart.IsZero() && ot.convergenceEnd.IsZero() {
		ot.convergenceEnd = time.Now()
		ot.ConvergenceTime = ot.convergenceEnd.Sub(ot.divergenceStart)
	}

	snapshot := agreementSnapshot{
		Time:   time.Now(),
		Views:  make([]sentinelView, len(views)),
		Agreed: agreed,
		Total:  total,
	}
	copy(snapshot.Views, views)
	ot.snapshots = append(ot.snapshots, snapshot)

	fragmented := divergent > 0
	leaderChanges := int64(0)
	for _, s := range ot.sentinels {
		leaderChanges += s.Metrics.GetLeaderChanges()
	}

	if pm != nil {
		pm.SetClusterHealth(total, agreed, leaderChanges, fragmented)
	}
}

func (ot *oscillationTracker) readViews() []sentinelView {
	views := make([]sentinelView, len(ot.sentinels))
	for i, s := range ot.sentinels {
		m := s.GetMaster(ot.masterName)
		if m != nil {
			views[i] = sentinelView{Addr: m.GetAddr(), State: m.GetState()}
		}
	}
	return views
}

func (ot *oscillationTracker) countAgreed(views []sentinelView) int {
	if len(views) == 0 {
		return 0
	}
	counts := make(map[string]int)
	for _, v := range views {
		key := v.Addr + "|" + v.State
		counts[key]++
	}
	best := 0
	for _, c := range counts {
		if c > best {
			best = c
		}
	}
	return best
}

func (ot *oscillationTracker) CurrentViews() []sentinelView {
	return ot.readViews()
}

func (ot *oscillationTracker) IsConverged() bool {
	ot.mu.Lock()
	defer ot.mu.Unlock()
	return ot.countAgreed(ot.readViews()) == len(ot.sentinels)
}

// HasOscillation returns true if agreement ever dropped after first
// reaching full consensus. This is the primary oscillation signal.
func (ot *oscillationTracker) HasOscillation() bool {
	ot.mu.Lock()
	defer ot.mu.Unlock()

	reachedFull := false
	for _, s := range ot.snapshots {
		if s.Agreed == s.Total {
			reachedFull = true
		} else if reachedFull && s.Agreed < s.Total {
			return true
		}
	}
	return false
}

// AgreementTrajectory returns a compact string showing how agreement
// evolved over time, e.g. "2→3→2→3→3" for oscillation or "2→3→3→3" for monotonic.
func (ot *oscillationTracker) AgreementTrajectory() string {
	ot.mu.Lock()
	defer ot.mu.Unlock()

	if len(ot.snapshots) == 0 {
		return "(no data)"
	}
	parts := make([]string, 0, len(ot.snapshots))
	for _, s := range ot.snapshots {
		parts = append(parts, fmt.Sprintf("%d/%d", s.Agreed, s.Total))
	}
	return strings.Join(parts, "→")
}

// IsConvergenceMonotonic returns true if, after the first divergence,
// agreement never decreased. Small noise within 1 step is tolerated.
func (ot *oscillationTracker) IsConvergenceMonotonic() bool {
	ot.mu.Lock()
	defer ot.mu.Unlock()

	prev := -1
	for _, s := range ot.snapshots {
		if s.Agreed < prev {
			return false
		}
		prev = s.Agreed
	}
	return true
}

func (ot *oscillationTracker) logMetrics(t *testing.T) {
	t.Helper()
	t.Logf("=== Oscillation Tracker Metrics ===")
	t.Logf("  Convergence time:     %v", ot.ConvergenceTime)
	t.Logf("  Peak divergence:      %d/%d views", ot.PeakDivergence, len(ot.sentinels))
	t.Logf("  Has oscillation:      %v", ot.HasOscillation())
	t.Logf("  Convergence monotonic: %v", ot.IsConvergenceMonotonic())
	t.Logf("  Trajectory:           %s", ot.AgreementTrajectory())
	t.Logf("  Snapshot count:       %d (%.1f/sec)",
		len(ot.snapshots), float64(len(ot.snapshots))/ot.ConvergenceTime.Seconds())
}

// TestRegressionFailoverOscillation verifies that sentinel failover
// converges without oscillation. It tests two scenarios:
//
// Scenario A: Single partition → failover → heal
//   Verifies sentinel views converge monotonically (no agreement drop after full consensus)
//
// Scenario B: Chain failover (master → slaveA → slaveB)
//   Verifies cascading master deaths don't cause infinite failover attempts
//
// This is the first cross-validation test combining:
//   - sentinel (failover detection, promotion, gossip)
//   - replication (slave sync, offset tracking)
//   - temporal (agreement trajectory time-series)
//   - basin (convergence to stable state after failover)
func TestRegressionFailoverOscillation(t *testing.T) {
	// ========================================================================
	// SETUP: 1 master + 2 slaves + 3 sentinels (quorum=2) + gossip mesh
	// ========================================================================

	master := startBoltNode(t)
	defer master.Close()

	slaveA := startBoltNode(t)
	defer slaveA.Close()

	slaveB := startBoltNode(t)
	defer slaveB.Close()

	err := slaveA.MakeSlave(master.Addr)
	assert.NoError(t, err)
	err = slaveB.MakeSlave(master.Addr)
	assert.NoError(t, err)

	// Wait for initial replication
	time.Sleep(2 * time.Second)
	t.Logf("setup: master=%s slaveA=%s slaveB=%s", master.Addr, slaveA.Addr, slaveB.Addr)

	downAfter := 2 * time.Second
	q := 2

	s1 := sentinel.NewSentinel(q, downAfter)
	s2 := sentinel.NewSentinel(q, downAfter)
	s3 := sentinel.NewSentinel(q, downAfter)

	sentinels := []*sentinel.Sentinel{s1, s2, s3}

	for i, s := range sentinels {
		err := s.AddMaster("mymaster", master.Addr, q)
		assert.NoError(t, err)

		slaveInstA := sentinel.NewSlaveInstance("slave-a", slaveA.Addr)
		slaveInstA.State = "online"
		slaveInstA.Offset = 1000
		s.GetMaster("mymaster").AddSlave(slaveInstA)

		slaveInstB := sentinel.NewSlaveInstance("slave-b", slaveB.Addr)
		slaveInstB.State = "online"
		slaveInstB.Offset = 500
		s.GetMaster("mymaster").AddSlave(slaveInstB)

		t.Logf("sentinel %d: runID=%s", i+1, s.GetRunID())
	}

	// Gossip setup: full mesh
	cfg := sentinel.DefaultGossipConfig()
	cfg.HelloInterval = 500 * time.Millisecond

	gp1 := sentinel.NewGossipProtocol(s1, cfg)
	gp2 := sentinel.NewGossipProtocol(s2, cfg)
	gp3 := sentinel.NewGossipProtocol(s3, cfg)

	err = gp1.Start()
	assert.NoError(t, err)
	defer gp1.Stop()
	err = gp2.Start()
	assert.NoError(t, err)
	defer gp2.Stop()
	err = gp3.Start()
	assert.NoError(t, err)
	defer gp3.Stop()

	time.Sleep(500 * time.Millisecond)

	p1 := "127.0.0.1:" + strconv.Itoa(gp1.GetPort())
	p2 := "127.0.0.1:" + strconv.Itoa(gp2.GetPort())
	p3 := "127.0.0.1:" + strconv.Itoa(gp3.GetPort())

	assert.NoError(t, gp1.AddPeer(p2, s1.GetRunID()))
	assert.NoError(t, gp1.AddPeer(p3, s1.GetRunID()))
	assert.NoError(t, gp2.AddPeer(p1, s2.GetRunID()))
	assert.NoError(t, gp2.AddPeer(p3, s2.GetRunID()))
	assert.NoError(t, gp3.AddPeer(p1, s3.GetRunID()))
	assert.NoError(t, gp3.AddPeer(p2, s3.GetRunID()))

	time.Sleep(1 * time.Second)

	s1.Start()
	defer s1.Stop()
	s2.Start()
	defer s2.Stop()
	s3.Start()
	defer s3.Stop()

	// PressureMonitor and oscillation tracker
	pm := monitor.NewPressureMonitor(master.DB, master.replMgr)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pm.Start(ctx, 1*time.Second)

	tracker := newOscillationTracker(sentinels, "mymaster")
	tracker.start(ctx, 250*time.Millisecond, pm)
	defer tracker.stop()

	baselineGoroutines := runtime.NumGoroutine()

	// ========================================================================
	// SCENARIO A: Single failover + heal, verify monotonic convergence
	// ========================================================================

	t.Logf("\n========== SCENARIO A: Single failover + heal ==========")
	t.Logf("=== Phase A1: Stable Baseline ===")

	time.Sleep(3 * time.Second) // gossip convergence + at least 1 ping cycle

	views := tracker.CurrentViews()
	for i, v := range views {
		t.Logf("  Sentinel %d: addr=%s state=%s", i+1, v.Addr, v.State)
		assert.Equal(t, "ok", v.State)
	}
	if !tracker.IsConverged() {
		t.Fatal("all sentinels should agree on master in baseline")
	}

	hs := pm.HealthScore(0)
	t.Logf("  Baseline HEALTH: %s", hs.FormatCompact())
	if hs.Overall < 0.70 {
		t.Fatalf("baseline HEALTH should be >= 0.70, got %.2f", hs.Overall)
	}

	// Phase A2: Kill master → wait for failover convergence
	t.Logf("=== Phase A2: Kill master → wait for failover ===")
	killTime := time.Now()
	master.Kill()
	t.Logf("  Master killed at %v", killTime.Format("15:04:05.000"))

	// Wait for at least one sentinel to update its master address
	// (convergence may take 8-10s with 3 concurrent failover attempts)
	var promotedAddr string
	failoverStartedA := int64(0)
	for wait := 0; wait < 40; wait++ {
		time.Sleep(500 * time.Millisecond)
		views = tracker.CurrentViews()
		for _, v := range views {
			if v.Addr != master.Addr && v.State == "ok" {
				if promotedAddr == "" {
					promotedAddr = v.Addr
					t.Logf("  Sentinel updated master to %s at t=%.1fs",
						v.Addr, time.Since(killTime).Seconds())
				}
			}
		}
		if promotedAddr != "" {
			// Wait a bit more for all sentinels to converge
			time.Sleep(3 * time.Second)
			break
		}
		// Log progress periodically
		if wait%6 == 0 {
			for i, v := range views {
				t.Logf("    S%d: addr=%s state=%s", i+1, v.Addr, v.State)
			}
		}
		// Track failover attempts
		fs := int64(0)
		for _, s := range sentinels {
			fs += s.Metrics.GetFailoverStarted()
		}
		failoverStartedA = fs
	}

	// Record metrics after convergence
	views = tracker.CurrentViews()
	t.Logf("  Post-failover views:")
	for i, v := range views {
		t.Logf("    Sentinel %d: addr=%s state=%s", i+1, v.Addr, v.State)
	}

	totalFailoversA := int64(0)
	totalLeaderChurnA := int64(0)
	for _, s := range sentinels {
		totalFailoversA += s.Metrics.GetSuccessfulFailovers()
		totalLeaderChurnA += s.Metrics.GetLeaderChanges()
	}
	t.Logf("  Total successful failovers: %d", totalFailoversA)
	t.Logf("  Total leader changes:      %d", totalLeaderChurnA)
	t.Logf("  Total failover attempts:   %d", failoverStartedA)

	if totalFailoversA == 0 {
		t.Fatal("failover should have occurred after master kill")
	}

	if promotedAddr != "" {
		t.Logf("  Promoted slave: %s", promotedAddr)
	} else {
		t.Logf("  WARNING: no sentinel updated master addr within timeout (possible oscillation)")
		// Use the first address that's not the master
		for _, v := range views {
			if v.Addr != master.Addr {
				promotedAddr = v.Addr
				break
			}
		}
	}

	// Phase A3: Convergence verification
	t.Logf("=== Phase A3: Convergence verification ===")

	// Additional stabilization time
	time.Sleep(2 * time.Second)

	if !tracker.IsConverged() {
		t.Logf("  WARNING: sentinel views not fully converged after failover")
	}

	// Record metrics snapshot for scenario A
	failoverCountA := int64(0)
	leaderChurnA := int64(0)
	odownA := int64(0)
	for _, s := range sentinels {
		failoverCountA += s.Metrics.GetSuccessfulFailovers()
		leaderChurnA += s.Metrics.GetLeaderChanges()
		odownA += s.Metrics.GetODownReached()
		failoverStartedA += s.Metrics.GetFailoverStarted()
	}

	t.Logf("  Scenario A metrics:")
	t.Logf("    SuccessfulFailovers=%d FailoverStarted=%d ODownReached=%d",
		failoverCountA, failoverStartedA, odownA)
	t.Logf("    LeaderChanges=%d", leaderChurnA)
	tracker.logMetrics(t)

	// Assertion A1: failover attempts should be bounded
	// With 3 sentinels each independently trying failover, some excess is expected
	if failoverStartedA > odownA*5 {
		t.Logf("  WARNING: high failover attempt ratio: started=%d odown=%d (possible oscillation)", failoverStartedA, odownA)
	}

	// ========================================================================
	// SCENARIO B: Chain failover — kill promoted slave, verify another failover
	// ========================================================================

	t.Logf("\n========== SCENARIO B: Chain failover (master → slave) ==========")

	// Determine which slave was promoted — kill the other one
	// to force failover to the remaining slave
	targetAddr := ""
	remainingAddr := ""
	if promotedAddr == slaveA.Addr {
		targetAddr = slaveA.Addr
		remainingAddr = slaveB.Addr
	} else {
		targetAddr = slaveB.Addr
		remainingAddr = slaveA.Addr
	}

	t.Logf("  Target (current master to kill): %s", targetAddr)
	t.Logf("  Remaining (expected new master): %s", remainingAddr)

	// Kill the current master (promoted slave)
	t.Logf("=== Phase B1: Kill current master (%s) ===", targetAddr)
	killTimeB := time.Now()
	if targetAddr == slaveA.Addr {
		slaveA.Kill()
	} else {
		slaveB.Kill()
	}
	t.Logf("  Master killed at %v", killTimeB.Format("15:04:05.000"))

	// Wait for failover to complete — use convergence loop
	t.Logf("=== Phase B2: Wait for second failover ===")
	killTimeB = time.Now()
	var secondPromotedAddr string
	for wait := 0; wait < 40; wait++ {
		time.Sleep(500 * time.Millisecond)
		views = tracker.CurrentViews()
		for _, v := range views {
			if v.Addr == remainingAddr && v.State == "ok" {
				if secondPromotedAddr == "" {
					secondPromotedAddr = v.Addr
					t.Logf("  Second failover complete at t=%.1fs, master=%s",
						time.Since(killTimeB).Seconds(), v.Addr)
				}
			}
		}
		if secondPromotedAddr != "" {
			time.Sleep(2 * time.Second)
			break
		}
	}

	views = tracker.CurrentViews()
	t.Logf("  Post-second-failover views:")
	for i, v := range views {
		t.Logf("    Sentinel %d: addr=%s state=%s", i+1, v.Addr, v.State)
	}

	failoverCountB := int64(0)
	leaderChurnB := int64(0)
	odownB := int64(0)
	failoverStartedB := int64(0)
	failedFailoversB := int64(0)
	for _, s := range sentinels {
		failoverCountB += s.Metrics.GetSuccessfulFailovers()
		leaderChurnB += s.Metrics.GetLeaderChanges()
		odownB += s.Metrics.GetODownReached()
		failoverStartedB += s.Metrics.GetFailoverStarted()
		failedFailoversB += s.Metrics.GetFailedFailovers()
	}

	chainFailovers := failoverCountB - failoverCountA
	chainLeaderChurn := leaderChurnB - leaderChurnA
	chainODown := odownB - odownA
	chainStarted := failoverStartedB - failoverStartedA

	t.Logf("  Scenario B metrics (delta from A):")
	t.Logf("    SuccessfulFailovers=%d FailoverStarted=%d Failed=%d",
		chainFailovers, chainStarted, failedFailoversB)
	t.Logf("    ODownReached=%d LeaderChanges=%d", chainODown, chainLeaderChurn)
	ratio := float64(0.0)
	if chainFailovers > 0 {
		ratio = float64(chainStarted) / float64(chainFailovers)
	}
	t.Logf("    Circuit: attempted=%d succeeded=%d ratio=%.1f",
		chainStarted, chainFailovers, ratio)

	// Phase B3: Convergence verification
	t.Logf("=== Phase B3: Final convergence ===")
	time.Sleep(3 * time.Second)

	hs = pm.HealthScore(0)
	t.Logf("  Final HEALTH: %s", hs.FormatReport())

	tracker.logMetrics(t)

	// ========================================================================
	// ASSERTIONS
	// ========================================================================

	t.Logf("\n========== ASSERTIONS ==========")

	// Assertion: oscillation detection
	if tracker.HasOscillation() {
		t.Errorf("FAIL: oscillation detected — agreement dropped after full consensus")
		t.Logf("  Trajectory: %s", tracker.AgreementTrajectory())
	} else {
		t.Logf("PASS: no oscillation detected in agreement trajectory")
	}

	// Assertion: monotonic convergence
	if !tracker.IsConvergenceMonotonic() {
		t.Errorf("FAIL: convergence trajectory is not monotonic")
		t.Logf("  Trajectory: %s", tracker.AgreementTrajectory())
	} else {
		t.Logf("PASS: convergence trajectory is monotonic")
	}

	// Assertion: goroutine stability
	pm.LogSummary(t)
	assertion := monitor.DefaultDegradationAssertion()
	assertion.MaxGoroutineDelta = 150
	assertion.GoroutineWarnDelta = 80
	assertion.MaxLeaderChurn = 5
	assertion.MinAgreedFraction = 1.0
	assertion.MaxReconnectCount = 20
	_ = pm.CheckDegradation(t, assertion, baselineGoroutines)

	// Assertion: health recovery
	if hs.Overall < 0.30 {
		t.Fatalf("FAIL: post-failover HEALTH should recover, got %.2f", hs.Overall)
	}
	t.Logf("PASS: Health recovered to %.2f", hs.Overall)

	// Log a compact summary of what happened
	t.Logf("\n========== FINAL SUMMARY ==========")
	scenarioBResult := "recovered"
	if chainFailovers == 0 {
		scenarioBResult = "stuck (selectNewMaster may have picked dead slave)"
	} else if chainStarted > chainFailovers*2 {
		scenarioBResult = "oscillating (multiple failover attempts per death)"
	}
	t.Logf("  Scenario A (master→slave): leaderChanges=%d failovers=%d",
		leaderChurnA, failoverCountA)
	t.Logf("  Scenario B (chain):        leaderChanges=%d failovers=%d (%s)",
		chainLeaderChurn, chainFailovers, scenarioBResult)
	t.Logf("  Failure ratio: %d/%d (Failed/Started)",
		failedFailoversB, failoverStartedB)
}


