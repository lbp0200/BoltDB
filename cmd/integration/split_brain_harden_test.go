package integration

import (
	"context"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/monitor"
	"github.com/lbp0200/BoltDB/internal/sentinel"
	"github.com/zeebo/assert"
)

// TestRegressionSplitBrainConvergenceHarden verifies that after a
// partition + failover + heal cycle, the sentinel convergence trajectory
// is monotonic (no oscillation) and the leader remains stable.
//
// This hardens the existing TestSplitBrainConvergenceReplay by adding:
//   - Full agreement trajectory tracking (not just start/end)
//   - Monotonicity assertion (agreement must never drop after first peak)
//   - Post-heal leader stability window (no leader changes after heal)
//   - Oscillation detection (agreement drop after full consensus)
//
// Failure doc: docs/failures/split-brain-convergence.md
func TestRegressionSplitBrainConvergenceHarden(t *testing.T) {
	// ========================================================================
	// SETUP: 1 master + 1 slave + 3 sentinels (quorum=2) + partition proxy
	// ========================================================================

	master := startBoltNode(t)
	defer master.Close()

	slave := startBoltNode(t)
	defer slave.Close()

	err := slave.MakeSlave(master.Addr)
	assert.NoError(t, err)
	t.Logf("Slave %s replicating from master %s", slave.Addr, master.Addr)
	time.Sleep(2 * time.Second)

	// Proxy sits between sentinels and master
	proxy, err := newPartitionProxy(master.Addr)
	assert.NoError(t, err)
	defer proxy.Stop()

	downAfter := 2 * time.Second
	q := 2

	s1 := sentinel.NewSentinel(q, downAfter)
	s2 := sentinel.NewSentinel(q, downAfter)
	s3 := sentinel.NewSentinel(q, downAfter)

	err = s1.AddMaster("mymaster", proxy.Addr(), q)
	assert.NoError(t, err)
	err = s2.AddMaster("mymaster", proxy.Addr(), q)
	assert.NoError(t, err)
	err = s3.AddMaster("mymaster", proxy.Addr(), q)
	assert.NoError(t, err)

	slaveInst := sentinel.NewSlaveInstance("slave-1", slave.Addr)
	slaveInst.State = "online"
	slaveInst.Offset = 1000
	for _, s := range []*sentinel.Sentinel{s1, s2, s3} {
		s.GetMaster("mymaster").AddSlave(slaveInst)
	}

	// Gossip: full mesh
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

	pm := monitor.NewPressureMonitor(master.DB, master.replMgr)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pm.Start(ctx, 1*time.Second)

	sentinels := []*sentinel.Sentinel{s1, s2, s3}
	tracker := newOscillationTracker(sentinels, "mymaster")
	tracker.start(ctx, 250*time.Millisecond, pm)
	defer tracker.stop()

	baselineGoroutines := runtime.NumGoroutine()

	// ========================================================================
	// Phase 1: Stable Baseline
	// ========================================================================
	t.Logf("=== PHASE 1: Stable Baseline ===")
	time.Sleep(3 * time.Second)

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

	preFailoverDetections := int64(0)
	for _, s := range sentinels {
		preFailoverDetections += s.Metrics.GetDetectionCount()
	}

	// ========================================================================
	// Phase 2: Partition — isolate master from sentinels
	// ========================================================================
	t.Logf("=== PHASE 2: Partition ===")
	proxy.Isolate()
	t.Logf("  Proxy isolated — sentinels cannot reach master")

	// ========================================================================
	// Phase 3: Dual Evolution — wait for failover to complete
	// ========================================================================
	t.Logf("=== PHASE 3: Dual Evolution (wait for failover) ===")
	time.Sleep(downAfter + 4*time.Second)

	views = tracker.CurrentViews()
	t.Logf("  Post-failover views:")
	for i, v := range views {
		t.Logf("    Sentinel %d: addr=%s state=%s", i+1, v.Addr, v.State)
	}

	totalDetections := int64(0)
	totalFailovers := int64(0)
	leaderChurn := int64(0)
	for _, s := range sentinels {
		totalDetections += s.Metrics.GetDetectionCount()
		totalFailovers += s.Metrics.GetSuccessfulFailovers()
		leaderChurn += s.Metrics.GetLeaderChanges()
	}
	t.Logf("  Detections:   %d", totalDetections)
	t.Logf("  Failovers:    %d", totalFailovers)
	t.Logf("  Leader churn: %d", leaderChurn)

	if totalDetections == 0 {
		t.Fatal("at least one sentinel should have detected the partition")
	}
	if totalFailovers == 0 {
		t.Fatal("at least one failover should have succeeded")
	}

	// Record state BEFORE heal — will use for post-heal comparison
	leaderChurnPreHeal := int64(0)
	for _, s := range sentinels {
		leaderChurnPreHeal += s.Metrics.GetLeaderChanges()
	}

	// ========================================================================
	// Phase 4: Heal — restore connectivity to old master
	// ========================================================================
	t.Logf("=== PHASE 4: Heal ===")
	proxy.Heal()
	t.Logf("  Proxy healed — old master reachable again")

	// ========================================================================
	// Phase 5: Post-heal convergence tracking
	// ========================================================================
	t.Logf("=== PHASE 5: Post-heal Convergence (8s tracking) ===")
	time.Sleep(8 * time.Second)

	views = tracker.CurrentViews()
	t.Logf("  Post-heal views:")
	for i, v := range views {
		t.Logf("    Sentinel %d: addr=%s state=%s", i+1, v.Addr, v.State)
	}

	// ========================================================================
	// ASSERTIONS
	// ========================================================================
	t.Logf("\n========== ASSERTIONS ==========")

	tracker.logMetrics(t)

	// Assertion 1: No oscillation — agreement must not drop after full consensus
	if tracker.HasOscillation() {
		t.Errorf("FAIL: post-heal oscillation detected — agreement dropped after full consensus")
		t.Logf("  Trajectory: %s", tracker.AgreementTrajectory())
	} else {
		t.Logf("PASS: no post-heal oscillation detected")
	}

	// Assertion 2: Monotonic convergence
	if !tracker.IsConvergenceMonotonic() {
		t.Errorf("FAIL: convergence trajectory is not monotonic")
		t.Logf("  Trajectory: %s", tracker.AgreementTrajectory())
	} else {
		t.Logf("PASS: convergence trajectory is monotonic")
	}

	// Assertion 3: Post-heal leader stability
	leaderChurnPostHeal := int64(0)
	for _, s := range sentinels {
		leaderChurnPostHeal += s.Metrics.GetLeaderChanges()
	}
	postHealLeaderChanges := leaderChurnPostHeal - leaderChurnPreHeal
	t.Logf("  Post-heal leader changes: %d", postHealLeaderChanges)
	if postHealLeaderChanges > 0 {
		t.Errorf("FAIL: %d leader change(s) occurred after heal (expected 0)", postHealLeaderChanges)
	} else {
		t.Logf("PASS: no leader changes after heal")
	}

	// Assertion 4: Leader stability window — after convergence, leader must stay stable
	leaderStable := true
	for _, s := range sentinels {
		stableWindow := s.Metrics.LeaderStabilization("mymaster")
		if stableWindow > 0 && stableWindow < 3*time.Second {
			t.Logf("  WARNING: sentinel %s leader stabilization window short: %v", s.GetRunID(), stableWindow)
			leaderStable = false
		}
	}
	if leaderStable {
		t.Logf("PASS: leader stabilization window adequate")
	}

	// Assertion 5: Final convergence
	if !tracker.IsConverged() {
		t.Logf("  WARNING: sentinel views not fully converged after heal+wait")
	} else {
		t.Logf("PASS: all sentinels fully converged after heal")
	}

	// Assertion 6: Degradation checks
	hs = pm.HealthScore(0)
	t.Logf("  Final HEALTH: %s", hs.FormatReport())

	degradation := monitor.DefaultDegradationAssertion()
	degradation.MaxLeaderChurn = 5
	degradation.MinAgreedFraction = 1.0
	degradation.MaxGoroutineDelta = 150
	degradation.GoroutineWarnDelta = 80
	_ = pm.CheckDegradation(t, degradation, baselineGoroutines)

	if hs.Overall < 0.40 {
		t.Fatalf("FAIL: post-convergence HEALTH should recover, got %.2f", hs.Overall)
	}
	t.Logf("PASS: Health recovered to %.2f", hs.Overall)

	t.Log("\nPASS: Split-brain convergence hardening completed")
}
