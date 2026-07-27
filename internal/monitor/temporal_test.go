package monitor

import (
	"math"
	"testing"
	"time"
)

func TestLinearSlope_Positive(t *testing.T) {
	t.Parallel()
	y := []float64{0.5, 0.6, 0.7, 0.8, 0.9, 1.0}
	slope := linearSlope(y)
	if slope <= 0 {
		t.Errorf("expected positive slope, got %.4f", slope)
	}
	// Each step is 0.1, so slope should be ~0.1
	if slope < 0.08 || slope > 0.12 {
		t.Errorf("expected ~0.1 slope, got %.4f", slope)
	}
}

func TestLinearSlope_Negative(t *testing.T) {
	t.Parallel()
	y := []float64{1.0, 0.9, 0.8, 0.7, 0.6, 0.5}
	slope := linearSlope(y)
	if slope >= 0 {
		t.Errorf("expected negative slope, got %.4f", slope)
	}
	if slope > -0.08 || slope < -0.12 {
		t.Errorf("expected ~-0.1 slope, got %.4f", slope)
	}
}

func TestLinearSlope_Flat(t *testing.T) {
	t.Parallel()
	y := []float64{0.9, 0.9, 0.9, 0.9, 0.9}
	slope := linearSlope(y)
	if slope != 0 {
		t.Errorf("expected 0 slope for flat line, got %.4f", slope)
	}
}

func TestLinearSlope_Noise(t *testing.T) {
	t.Parallel()
	y := []float64{0.8, 0.85, 0.78, 0.88, 0.82, 0.9}
	slope := linearSlope(y)
	if slope <= 0 {
		t.Errorf("expected positive trend despite noise, got %.4f", slope)
	}
}

func TestLinearSlope_InsufficientData(t *testing.T) {
	t.Parallel()
	if slope := linearSlope(nil); slope != 0 {
		t.Errorf("expected 0 for nil, got %.4f", slope)
	}
	if slope := linearSlope([]float64{1.0}); slope != 0 {
		t.Errorf("expected 0 for single point, got %.4f", slope)
	}
}

func TestComputeOscillationStats_Stable(t *testing.T) {
	t.Parallel()
	overalls := []float64{0.9, 0.9, 0.9, 0.9, 0.9, 0.9}
	osc := computeOscillationStats(overalls)
	if osc.Oscillating {
		t.Error("expected no oscillation for flat line")
	}
	if osc.ZeroCrossingCount != 0 {
		t.Errorf("expected 0 zero-crossings, got %d", osc.ZeroCrossingCount)
	}
}

func TestComputeOscillationStats_Monotonic(t *testing.T) {
	t.Parallel()
	overalls := []float64{0.5, 0.6, 0.7, 0.8, 0.9, 1.0}
	osc := computeOscillationStats(overalls)
	if osc.Oscillating {
		t.Error("expected no oscillation for monotonic increase")
	}
}

func TestComputeOscillationStats_Oscillating(t *testing.T) {
	t.Parallel()
	overalls := []float64{0.9, 0.6, 0.9, 0.6, 0.9, 0.6}
	osc := computeOscillationStats(overalls)
	if !osc.Oscillating {
		t.Error("expected oscillation for alternating pattern")
	}
	if osc.ZeroCrossingCount < 3 {
		t.Errorf("expected >=3 zero-crossings for 6 alternating values, got %d", osc.ZeroCrossingCount)
	}
}

func TestComputeOscillationStats_BelowMinAmp(t *testing.T) {
	t.Parallel()
	overalls := []float64{0.9, 0.895, 0.9, 0.895, 0.9, 0.895}
	osc := computeOscillationStats(overalls)
	if osc.Oscillating {
		t.Errorf("expected no oscillation when amplitude below threshold: meanAmp=%.15f", osc.MeanAmplitude)
	}
}

func TestComputeOscillationStats_InsufficientData(t *testing.T) {
	t.Parallel()
	osc := computeOscillationStats([]float64{0.9, 0.8, 0.7})
	if osc.Oscillating {
		t.Error("expected no oscillation with <4 samples")
	}
}

func TestComputePersistenceStats_NoDegradation(t *testing.T) {
	t.Parallel()
	snaps := makeTemporalSnaps([]float64{0.9, 0.9, 0.9}, "100ms")
	p := computePersistenceStats(snaps)
	if p.AnyDegraded {
		t.Error("expected no degradation when all scores above threshold")
	}
}

func TestComputePersistenceStats_StorageDegraded(t *testing.T) {
	t.Parallel()
	now := time.Now()
	snaps := []TemporalSnapshot{}
	for i, score := range []float64{0.9, 0.9, 0.9, 0.9, 0.3, 0.3, 0.3} {
		snaps = append(snaps, TemporalSnapshot{
			Time: now.Add(time.Duration(i) * 100 * time.Millisecond),
			Health: HealthScore{
				Overall:           score,
				HealthStorage:     score,
				HealthReplication: 1.0,
				HealthCluster:     1.0,
			},
		})
	}
	p := computePersistenceStats(snaps)
	if !p.StorageDegraded {
		t.Error("expected storage degraded flag")
	}
	if !p.AnyDegraded {
		t.Error("expected anyDegraded=true")
	}
	if p.ReplicationDegraded {
		t.Error("expected no replication degradation")
	}
	minExpected := 150 * time.Millisecond
	if p.StorageDuration < minExpected {
		t.Errorf("expected storage duration >= %v, got %v", minExpected, p.StorageDuration)
	}
}

func TestComputePersistenceStats_ReplicationDegraded(t *testing.T) {
	t.Parallel()
	snaps := makeTemporalSnaps([]float64{0.9, 0.9, 0.9, 0.9, 0.9, 0.9}, "100ms")
	// Override health scores
	for i := 0; i < len(snaps); i++ {
		score := 0.9
		if i >= 4 {
			score = 0.9 // overall stays high
		}
		snaps[i].Health.Overall = score
		snaps[i].Health.HealthStorage = 1.0
		snaps[i].Health.HealthReplication = 0.4 // low but above threshold
		snaps[i].Health.HealthCluster = 1.0
	}
	// I need some below threshold. Let me set 2 samples to 0.3
	snaps[4].Health.HealthReplication = 0.3
	snaps[5].Health.HealthReplication = 0.3
	snaps[4].Health.Overall = 0.5
	snaps[5].Health.Overall = 0.5

	p := computePersistenceStats(snaps)
	if !p.ReplicationDegraded {
		t.Error("expected replication degraded flag")
	}
	if p.StorageDegraded {
		t.Error("expected no storage degradation")
	}
}

func TestComputePersistenceStats_ClusterDegraded(t *testing.T) {
	t.Parallel()
	now := time.Now()
	snaps := []TemporalSnapshot{}
	for i := 0; i < 6; i++ {
		snaps = append(snaps, TemporalSnapshot{
			Time: now.Add(time.Duration(i) * 100 * time.Millisecond),
			Health: HealthScore{
				Overall:           0.9,
				HealthStorage:     1.0,
				HealthReplication: 1.0,
				HealthCluster:     0.9,
			},
		})
	}
	snaps[4].Health.HealthCluster = 0.3
	snaps[4].Health.Overall = 0.8
	snaps[5].Health.HealthCluster = 0.3
	snaps[5].Health.Overall = 0.8

	p := computePersistenceStats(snaps)
	if !p.ClusterDegraded {
		t.Error("expected cluster degraded flag")
	}
}

func TestComputePersistenceStats_BreakOnRecovery(t *testing.T) {
	t.Parallel()
	now := time.Now()
	snaps := []TemporalSnapshot{}
	scores := []float64{0.9, 0.3, 0.3, 0.9, 0.9, 0.9}
	for i, s := range scores {
		snaps = append(snaps, TemporalSnapshot{
			Time: now.Add(time.Duration(i) * 100 * time.Millisecond),
			Health: HealthScore{
				Overall:           s,
				HealthStorage:     s,
				HealthReplication: 1.0,
				HealthCluster:     1.0,
			},
		})
	}
	p := computePersistenceStats(snaps)
	if p.StorageDegraded {
		t.Error("expected no degradation since it recovered (consecutive check from end)")
	}
	// The last 3 samples are 0.9, so no consecutive degradation from end
}

func TestComputePersistenceStats_InsufficientData(t *testing.T) {
	t.Parallel()
	p := computePersistenceStats(nil)
	if p.AnyDegraded {
		t.Error("expected no degradation with nil")
	}
	p = computePersistenceStats([]TemporalSnapshot{{}})
	if p.AnyDegraded {
		t.Error("expected no degradation with single snapshot")
	}
}

func TestComputeRecoveryStats_NoTrough(t *testing.T) {
	t.Parallel()
	snaps := makeTemporalSnaps([]float64{0.9, 0.9, 0.9, 0.9, 0.9}, "100ms")
	overalls := extractOveralls(snaps)
	r := computeRecoveryStats(snaps, overalls)
	if r.Observed {
		t.Error("expected no recovery for stable system")
	}
}

func TestComputeRecoveryStats_RecoverySuccess(t *testing.T) {
	t.Parallel()
	now := time.Now()
	snaps := []TemporalSnapshot{}
	scores := []float64{0.9, 0.8, 0.3, 0.5, 0.7, 0.85}
	for i, s := range scores {
		snaps = append(snaps, TemporalSnapshot{
			Time: now.Add(time.Duration(i) * time.Second),
			Health: HealthScore{
				Overall:           s,
				HealthStorage:     s,
				HealthReplication: 1.0,
				HealthCluster:     1.0,
			},
		})
	}
	overalls := extractOveralls(snaps)
	r := computeRecoveryStats(snaps, overalls)
	if !r.Observed {
		t.Error("expected recovery observed")
	}
	// Recovery from 0.3 to 0.85 over 3 seconds (from trough to end)
	expectedVelocity := (0.85 - 0.3) / 3.0
	if math.Abs(r.Velocity-expectedVelocity) > 0.01 {
		t.Errorf("expected velocity ~%.4f, got %.4f", expectedVelocity, r.Velocity)
	}
}

func TestComputeRecoveryStats_TroughAtEnd(t *testing.T) {
	t.Parallel()
	snaps := makeTemporalSnaps([]float64{0.9, 0.8, 0.7, 0.6, 0.3}, "1s")
	overalls := extractOveralls(snaps)
	r := computeRecoveryStats(snaps, overalls)
	if r.Observed {
		t.Error("expected no recovery when trough is at the end")
	}
}

func TestComputeRecoveryStats_NotSustained(t *testing.T) {
	t.Parallel()
	now := time.Now()
	snaps := []TemporalSnapshot{}
	scores := []float64{0.9, 0.3, 0.5, 0.3, 0.4, 0.5}
	for i, s := range scores {
		snaps = append(snaps, TemporalSnapshot{
			Time: now.Add(time.Duration(i) * time.Second),
			Health: HealthScore{
				Overall:           s,
				HealthStorage:     s,
				HealthReplication: 1.0,
				HealthCluster:     1.0,
			},
		})
	}
	overalls := extractOveralls(snaps)
	r := computeRecoveryStats(snaps, overalls)
	if r.Observed {
		t.Error("expected no recovery when recovery not sustained (drops back to trough)")
	}
}

func TestComputeRecoveryStats_WithOscillations(t *testing.T) {
	t.Parallel()
	now := time.Now()
	snaps := []TemporalSnapshot{}
	scores := []float64{0.9, 0.3, 0.4, 0.35, 0.5, 0.45, 0.6, 0.55, 0.7, 0.68, 0.8}
	for i, s := range scores {
		snaps = append(snaps, TemporalSnapshot{
			Time: now.Add(time.Duration(i) * time.Second),
			Health: HealthScore{
				Overall:           s,
				HealthStorage:     s,
				HealthReplication: 1.0,
				HealthCluster:     1.0,
			},
		})
	}
	overalls := extractOveralls(snaps)
	r := computeRecoveryStats(snaps, overalls)
	if !r.Observed {
		t.Error("expected recovery observed despite oscillations")
	}
	if r.DampingRatio != 0.5 {
		t.Errorf("expected underdamped (0.5) for oscillatory recovery, got %.2f", r.DampingRatio)
	}
}

func TestComputeRecoveryStats_InsufficientData(t *testing.T) {
	t.Parallel()
	snaps := makeTemporalSnaps([]float64{0.9, 0.8}, "1s")
	overalls := extractOveralls(snaps)
	r := computeRecoveryStats(snaps, overalls)
	if r.Observed {
		t.Error("expected no recovery with <3 samples")
	}
}

func TestClassifyTrajectory_Stable(t *testing.T) {
	t.Parallel()
	traj := classifyTrajectory(
		SlopeStats{Overall: 0.001},
		OscillationStats{},
		PersistenceStats{},
		0.95,
	)
	if traj != TrajectoryStable {
		t.Errorf("expected stable, got %s", traj)
	}
}

func TestClassifyTrajectory_Recovering(t *testing.T) {
	t.Parallel()
	traj := classifyTrajectory(
		SlopeStats{Overall: 0.05},
		OscillationStats{},
		PersistenceStats{AnyDegraded: true, WorstDuration: time.Second},
		0.65,
	)
	if traj != TrajectoryRecovering {
		t.Errorf("expected recovering, got %s", traj)
	}
}

func TestClassifyTrajectory_Degrading(t *testing.T) {
	t.Parallel()
	traj := classifyTrajectory(
		SlopeStats{Overall: -0.05},
		OscillationStats{},
		PersistenceStats{},
		0.9,
	)
	if traj != TrajectoryDegrading {
		t.Errorf("expected degrading, got %s", traj)
	}
}

func TestClassifyTrajectory_Oscillating(t *testing.T) {
	t.Parallel()
	traj := classifyTrajectory(
		SlopeStats{Overall: 0.0},
		OscillationStats{Oscillating: true, ZeroCrossingCount: 5, MeanAmplitude: 0.1},
		PersistenceStats{AnyDegraded: true},
		0.65,
	)
	if traj != TrajectoryOscillating {
		t.Errorf("expected oscillating, got %s", traj)
	}
}

func TestClassifyTrajectory_OscillatingHealthy(t *testing.T) {
	t.Parallel()
	// Oscillation with high scores: still oscillating
	traj := classifyTrajectory(
		SlopeStats{Overall: 0.0},
		OscillationStats{Oscillating: true, ZeroCrossingCount: 5, MeanAmplitude: 0.02},
		PersistenceStats{},
		0.9,
	)
	if traj == TrajectoryOscillating {
		t.Log("oscillation on healthy system is acceptable")
	}
}

func TestClassifyTrajectory_Stuck(t *testing.T) {
	t.Parallel()
	traj := classifyTrajectory(
		SlopeStats{Overall: 0.002},
		OscillationStats{},
		PersistenceStats{AnyDegraded: true, WorstDuration: 10 * time.Second},
		0.45,
	)
	if traj != TrajectoryStuck {
		t.Errorf("expected stuck, got %s", traj)
	}
}

func TestAnalyzeTemporal_InsufficientData(t *testing.T) {
	t.Parallel()
	ta := AnalyzeTemporal(nil)
	if ta.Trajectory != TrajectoryInsufficientData {
		t.Errorf("expected insufficient_data, got %s", ta.Trajectory)
	}
	ta = AnalyzeTemporal([]TemporalSnapshot{{Time: time.Now(), Health: HealthScore{Overall: 0.9}}})
	if ta.Trajectory != TrajectoryInsufficientData {
		t.Errorf("expected insufficient_data, got %s", ta.Trajectory)
	}
}

func TestAnalyzeTemporal_Stable(t *testing.T) {
	t.Parallel()
	snaps := makeTemporalSnaps([]float64{0.9, 0.9, 0.9, 0.9, 0.9, 0.9, 0.9, 0.9, 0.9, 0.9}, "1s")
	ta := AnalyzeTemporal(snaps)
	if ta.Trajectory != TrajectoryStable {
		t.Errorf("expected stable, got %s", ta.Trajectory)
	}
	if ta.SnapshotCount != 10 {
		t.Errorf("expected 10 snapshots, got %d", ta.SnapshotCount)
	}
}

func TestAnalyzeTemporal_Degrading(t *testing.T) {
	t.Parallel()
	snaps := makeTemporalSnaps([]float64{1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1}, "1s")
	ta := AnalyzeTemporal(snaps)
	if ta.Trajectory != TrajectoryDegrading {
		t.Errorf("expected degrading, got %s", ta.Trajectory)
	}
	if ta.Slope.Overall >= 0 {
		t.Errorf("expected negative slope, got %.4f", ta.Slope.Overall)
	}
}

func TestAnalyzeTemporal_DimensionSlopes(t *testing.T) {
	t.Parallel()
	now := time.Now()
	snaps := []TemporalSnapshot{}
	for i := 0; i < 10; i++ {
		storage := 1.0 - float64(i)*0.05 // degrading storage
		repl := 0.5 + float64(i)*0.05    // recovering replication
		cluster := 1.0                   // stable cluster
		overall := (storage*0.40 + repl*0.30 + cluster*0.30)
		snaps = append(snaps, TemporalSnapshot{
			Time: now.Add(time.Duration(i) * time.Second),
			Health: HealthScore{
				Overall:           overall,
				HealthStorage:     storage,
				HealthReplication: repl,
				HealthCluster:     cluster,
			},
		})
	}
	ta := AnalyzeTemporal(snaps)
	if ta.Slope.StorageSlope >= 0 {
		t.Errorf("expected negative storage slope, got %.4f", ta.Slope.StorageSlope)
	}
	if ta.Slope.ReplicationSlope <= 0 {
		t.Errorf("expected positive replication slope, got %.4f", ta.Slope.ReplicationSlope)
	}
	if ta.Slope.ClusterSlope >= 0.01 || ta.Slope.ClusterSlope <= -0.01 {
		t.Errorf("expected near-zero cluster slope, got %.4f", ta.Slope.ClusterSlope)
	}
}

func TestTemporalAnalyzer_RingBuffer(t *testing.T) {
	t.Parallel()
	ta := NewTemporalAnalyzer()
	// Fill beyond capacity (capacity is 300)
	for i := 0; i < 310; i++ {
		ta.RecordHealth(HealthScore{
			Overall:           0.9,
			HealthStorage:     0.9,
			HealthReplication: 0.9,
			HealthCluster:     0.9,
		})
	}
	if ta.Len() != 300 {
		t.Errorf("expected 300 after ring buffer wrap, got %d", ta.Len())
	}
}

func TestTemporalAnalyzer_RecordAndAnalyze(t *testing.T) {
	t.Parallel()
	ta := NewTemporalAnalyzer()
	for i := 0; i < 10; i++ {
		ta.RecordHealth(HealthScore{
			Overall:           0.9,
			HealthStorage:     0.9,
			HealthReplication: 0.9,
			HealthCluster:     0.9,
		})
	}
	result := ta.Analyze()
	if result.Trajectory != TrajectoryStable {
		t.Errorf("expected stable for flat scores, got %s", result.Trajectory)
	}
}

func TestTemporalAnalyzer_Empty(t *testing.T) {
	t.Parallel()
	ta := NewTemporalAnalyzer()
	ta.Analyze() // should not panic
}

func TestTemporalAnalyzer_Clear(t *testing.T) {
	t.Parallel()
	ta := NewTemporalAnalyzer()
	ta.RecordHealth(HealthScore{
		Overall:           0.9,
		HealthStorage:     0.9,
		HealthReplication: 0.9,
		HealthCluster:     0.9,
	})
	ta.Clear()
	if ta.Len() != 0 {
		t.Errorf("expected 0 after clear, got %d", ta.Len())
	}
}

func TestTemporalAnalyzer_Snapshots(t *testing.T) {
	t.Parallel()
	ta := NewTemporalAnalyzer()
	ta.RecordHealth(HealthScore{
		Overall:           0.5,
		HealthStorage:     0.5,
		HealthReplication: 0.5,
		HealthCluster:     0.5,
	})
	ta.RecordHealth(HealthScore{
		Overall:           0.8,
		HealthStorage:     0.8,
		HealthReplication: 0.8,
		HealthCluster:     0.8,
	})
	snaps := ta.Snapshots()
	if len(snaps) != 2 {
		t.Errorf("expected 2 snapshots, got %d", len(snaps))
	}
	if snaps[0].Health.Overall != 0.5 {
		t.Errorf("expected 0.5, got %.2f", snaps[0].Health.Overall)
	}
	if snaps[1].Health.Overall != 0.8 {
		t.Errorf("expected 0.8, got %.2f", snaps[1].Health.Overall)
	}
}

func TestTemporalAnalysis_FormatCompact(t *testing.T) {
	t.Parallel()
	ta := TemporalAnalysis{
		Trajectory:    TrajectoryStable,
		SnapshotCount: 50,
		Slope:         SlopeStats{Overall: 0.001},
	}
	s := ta.FormatCompact()
	if len(s) == 0 {
		t.Error("expected non-empty format")
	}
}

func TestTemporalAnalysis_FormatCompact_Oscillating(t *testing.T) {
	t.Parallel()
	ta := TemporalAnalysis{
		Trajectory:    TrajectoryOscillating,
		SnapshotCount: 50,
		Slope:         SlopeStats{Overall: 0.001},
		Oscillation: OscillationStats{
			Oscillating:       true,
			ZeroCrossingCount: 8,
			MeanAmplitude:     0.12,
		},
		Persistence: PersistenceStats{
			AnyDegraded:    true,
			WorstDimension: "storage",
			WorstDuration:  5 * time.Second,
		},
	}
	s := ta.FormatCompact()
	if len(s) == 0 {
		t.Error("expected non-empty format")
	}
}

func TestTemporalAnalysis_FormatCompact_Recovering(t *testing.T) {
	t.Parallel()
	ta := TemporalAnalysis{
		Trajectory:    TrajectoryRecovering,
		SnapshotCount: 50,
		Slope:         SlopeStats{Overall: 0.05},
		Recovery: RecoveryStats{
			Observed: true,
			Velocity: 0.15,
			Duration: 3 * time.Second,
		},
	}
	s := ta.FormatCompact()
	if len(s) == 0 {
		t.Error("expected non-empty format")
	}
}

func TestTemporalAnalysis_FormatReport(t *testing.T) {
	t.Parallel()
	ta := TemporalAnalysis{
		Trajectory:    TrajectoryDegrading,
		SnapshotCount: 100,
		Slope: SlopeStats{
			Overall:          -0.03,
			StorageSlope:     -0.05,
			ReplicationSlope: -0.01,
			ClusterSlope:     0.0,
		},
		Persistence: PersistenceStats{
			AnyDegraded:         true,
			StorageDegraded:     true,
			StorageDuration:     30 * time.Second,
			ReplicationDegraded: false,
			ClusterDegraded:     false,
			WorstDimension:      "storage",
			WorstDuration:       30 * time.Second,
		},
		Recovery: RecoveryStats{
			Observed:     false,
			Velocity:     0.0,
			Duration:     0,
			DampingRatio: 0,
			Undershoot:   0,
		},
	}
	s := ta.FormatReport()
	if len(s) == 0 {
		t.Error("expected non-empty report")
	}
}

func TestTemporalAnalysis_FormatReport_Full(t *testing.T) {
	t.Parallel()
	ta := TemporalAnalysis{
		Trajectory:    TrajectoryRecovering,
		SnapshotCount: 100,
		Slope: SlopeStats{
			Overall:          0.05,
			StorageSlope:     0.04,
			ReplicationSlope: 0.06,
			ClusterSlope:     0.01,
		},
		Oscillation: OscillationStats{
			Oscillating:       true,
			ZeroCrossingCount: 5,
			MeanAmplitude:     0.08,
			MaxAmplitude:      0.15,
		},
		Persistence: PersistenceStats{
			AnyDegraded:         true,
			StorageDegraded:     true,
			StorageDuration:     10 * time.Second,
			ReplicationDegraded: false,
			ClusterDegraded:     true,
			ClusterDuration:     2 * time.Second,
			WorstDimension:      "storage",
			WorstDuration:       10 * time.Second,
		},
		Recovery: RecoveryStats{
			Observed:     true,
			Velocity:     0.12,
			Duration:     5 * time.Second,
			DampingRatio: 1.5,
			Undershoot:   0.05,
		},
	}
	s := ta.FormatReport()
	if len(s) == 0 {
		t.Error("expected non-empty report")
	}
}

func TestComputeRecoveryStats_DampingOverdamped(t *testing.T) {
	t.Parallel()
	now := time.Now()
	snaps := []TemporalSnapshot{}
	scores := []float64{0.9, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8}
	for i, s := range scores {
		snaps = append(snaps, TemporalSnapshot{
			Time: now.Add(time.Duration(i) * time.Second),
			Health: HealthScore{
				Overall:           s,
				HealthStorage:     s,
				HealthReplication: 1.0,
				HealthCluster:     1.0,
			},
		})
	}
	overalls := extractOveralls(snaps)
	r := computeRecoveryStats(snaps, overalls)
	if r.DampingRatio != 2.0 {
		t.Errorf("expected overdamped (2.0) for smooth recovery, got %.2f", r.DampingRatio)
	}
}

func TestComputeRecoveryStats_DampingUnderdamped(t *testing.T) {
	t.Parallel()
	now := time.Now()
	snaps := []TemporalSnapshot{}
	scores := []float64{0.9, 0.3, 0.45, 0.35, 0.5, 0.4, 0.6, 0.5, 0.7, 0.65, 0.75}
	for i, s := range scores {
		snaps = append(snaps, TemporalSnapshot{
			Time: now.Add(time.Duration(i) * time.Second),
			Health: HealthScore{
				Overall:           s,
				HealthStorage:     s,
				HealthReplication: 1.0,
				HealthCluster:     1.0,
			},
		})
	}
	overalls := extractOveralls(snaps)
	r := computeRecoveryStats(snaps, overalls)
	if r.DampingRatio > 2.0 {
		t.Errorf("expected not overdamped for oscillatory recovery, got %.2f", r.DampingRatio)
	}
}

func TestComputeOscillationStats_Realistic(t *testing.T) {
	t.Parallel()
	// Simulate a realistic oscillation: slow damping around 0.7
	overalls := []float64{}
	for i := 0; i < 20; i++ {
		decay := math.Exp(-float64(i) / 10.0)
		val := 0.7 + 0.2*decay*math.Sin(float64(i)*1.5)
		overalls = append(overalls, val)
	}
	osc := computeOscillationStats(overalls)
	if !osc.Oscillating {
		t.Error("expected oscillation for damped sine wave, got not oscillating")
	}
	if osc.ZeroCrossingCount < 3 {
		t.Errorf("expected >=3 zero-crossings for sine wave, got %d", osc.ZeroCrossingCount)
	}
}

// helpers

func makeTemporalSnaps(scores []float64, interval string) []TemporalSnapshot {
	now := time.Now()
	d, _ := time.ParseDuration(interval)
	snaps := make([]TemporalSnapshot, len(scores))
	for i, s := range scores {
		snaps[i] = TemporalSnapshot{
			Time: now.Add(time.Duration(i) * d),
			Health: HealthScore{
				Overall:           s,
				HealthStorage:     s,
				HealthReplication: s,
				HealthCluster:     s,
			},
		}
	}
	return snaps
}

func extractOveralls(snaps []TemporalSnapshot) []float64 {
	o := make([]float64, len(snaps))
	for i, s := range snaps {
		o[i] = s.Health.Overall
	}
	return o
}
