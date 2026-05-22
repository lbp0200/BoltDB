package monitor

import (
	"fmt"
	"math"
	"strings"
	"sync"
	"time"
)

const (
	TrajectoryStable           = "stable"
	TrajectoryRecovering       = "recovering"
	TrajectoryDegrading        = "degrading"
	TrajectoryOscillating      = "oscillating"
	TrajectoryStuck            = "stuck"
	TrajectoryInsufficientData = "insufficient_data"

	defaultTemporalCapacity = 300

	minSlopeThreshold = 0.005
	degradedThreshold = 0.50
	warnThreshold     = 0.70
	oscillationMinAmp = 0.01
	oscillationMinZC  = 3
)

type TemporalSnapshot struct {
	Time   time.Time
	Health HealthScore
}

type SlopeStats struct {
	Overall          float64
	StorageSlope     float64
	ReplicationSlope float64
	ClusterSlope     float64
}

type OscillationStats struct {
	Oscillating       bool
	ZeroCrossingCount int
	MeanAmplitude     float64
	MaxAmplitude      float64
}

type PersistenceStats struct {
	AnyDegraded         bool
	StorageDegraded     bool
	StorageDuration     time.Duration
	ReplicationDegraded bool
	ReplicationDuration time.Duration
	ClusterDegraded     bool
	ClusterDuration     time.Duration
	WorstDimension      string
	WorstDuration       time.Duration
}

type RecoveryStats struct {
	Observed     bool
	Velocity     float64
	Duration     time.Duration
	DampingRatio float64
	Undershoot   float64
}

type TemporalAnalysis struct {
	Slope         SlopeStats
	Oscillation   OscillationStats
	Persistence   PersistenceStats
	Recovery      RecoveryStats
	Trajectory    string
	SnapshotCount int
	Basin         BasinAttractorInfo
}

func (ta TemporalAnalysis) FormatCompact() string {
	var b strings.Builder
	fmt.Fprintf(&b, "[trajectory=%s samples=%d", ta.Trajectory, ta.SnapshotCount)
	fmt.Fprintf(&b, " slope=%.4f", ta.Slope.Overall)
	if ta.Oscillation.Oscillating {
		fmt.Fprintf(&b, " osc=yes(zc=%d amp=%.3f)", ta.Oscillation.ZeroCrossingCount, ta.Oscillation.MeanAmplitude)
	}
	if ta.Persistence.AnyDegraded {
		fmt.Fprintf(&b, " persist=%s(%v)", ta.Persistence.WorstDimension, ta.Persistence.WorstDuration.Round(time.Second))
	}
	if ta.Recovery.Observed {
		fmt.Fprintf(&b, " recover=%.2f/s(%v)", ta.Recovery.Velocity, ta.Recovery.Duration.Round(time.Second))
	}
	if ta.Basin.CurrentBasin != BasinUnknown {
		fmt.Fprintf(&b, " %s", ta.Basin.FormatCompact())
	}
	b.WriteString("]")
	return b.String()
}

func (ta TemporalAnalysis) FormatReport() string {
	var b strings.Builder
	b.WriteString("\nTEMPORAL ANALYSIS\n")
	b.WriteString("------------------\n")
	fmt.Fprintf(&b, "  trajectory: %s (%d snapshots)\n", ta.Trajectory, ta.SnapshotCount)
	fmt.Fprintf(&b, "  slope: overall=%.4f S=%.4f R=%.4f C=%.4f\n",
		ta.Slope.Overall, ta.Slope.StorageSlope, ta.Slope.ReplicationSlope, ta.Slope.ClusterSlope)
	if ta.Oscillation.Oscillating {
		fmt.Fprintf(&b, "  oscillation: yes (zero-crossings=%d mean-amp=%.3f max-amp=%.3f)\n",
			ta.Oscillation.ZeroCrossingCount, ta.Oscillation.MeanAmplitude, ta.Oscillation.MaxAmplitude)
	} else {
		b.WriteString("  oscillation: no\n")
	}
	if ta.Persistence.AnyDegraded {
		fmt.Fprintf(&b, "  persistence: storage=%v repl=%v cluster=%v worst=%s(%v)\n",
			ta.Persistence.StorageDuration.Round(time.Second),
			ta.Persistence.ReplicationDuration.Round(time.Second),
			ta.Persistence.ClusterDuration.Round(time.Second),
			ta.Persistence.WorstDimension,
			ta.Persistence.WorstDuration.Round(time.Second))
	}
	if ta.Recovery.Observed {
		fmt.Fprintf(&b, "  recovery: velocity=%.4f/s duration=%v damping=%.2f undershoot=%.3f\n",
			ta.Recovery.Velocity, ta.Recovery.Duration.Round(time.Second),
			ta.Recovery.DampingRatio, ta.Recovery.Undershoot)
	}
	if ta.Basin.CurrentBasin != BasinUnknown && ta.Basin.CurrentBasin != BasinHealthy {
		b.WriteString(ta.Basin.FormatReport())
	}
	b.WriteString("------------------\n")
	return b.String()
}

type TemporalAnalyzer struct {
	mu        sync.Mutex
	snapshots []TemporalSnapshot
	capacity  int
}

func NewTemporalAnalyzer() *TemporalAnalyzer {
	return &TemporalAnalyzer{
		snapshots: make([]TemporalSnapshot, 0, defaultTemporalCapacity),
		capacity:  defaultTemporalCapacity,
	}
}

func (ta *TemporalAnalyzer) Record(snapshot TemporalSnapshot) {
	ta.mu.Lock()
	defer ta.mu.Unlock()
	if len(ta.snapshots) >= ta.capacity {
		ta.snapshots = ta.snapshots[1:]
	}
	ta.snapshots = append(ta.snapshots, snapshot)
}

func (ta *TemporalAnalyzer) RecordHealth(h HealthScore) {
	ta.Record(TemporalSnapshot{Time: time.Now(), Health: h})
}

func (ta *TemporalAnalyzer) Snapshots() []TemporalSnapshot {
	ta.mu.Lock()
	defer ta.mu.Unlock()
	result := make([]TemporalSnapshot, len(ta.snapshots))
	copy(result, ta.snapshots)
	return result
}

func (ta *TemporalAnalyzer) Analyze() TemporalAnalysis {
	snaps := ta.Snapshots()
	return AnalyzeTemporal(snaps)
}

func (ta *TemporalAnalyzer) Clear() {
	ta.mu.Lock()
	defer ta.mu.Unlock()
	ta.snapshots = ta.snapshots[:0]
}

func (ta *TemporalAnalyzer) Len() int {
	ta.mu.Lock()
	defer ta.mu.Unlock()
	return len(ta.snapshots)
}

func AnalyzeTemporal(snaps []TemporalSnapshot) TemporalAnalysis {
	if len(snaps) < 2 {
		return TemporalAnalysis{
			Trajectory:    TrajectoryInsufficientData,
			SnapshotCount: len(snaps),
		}
	}

	n := len(snaps)
	overalls := make([]float64, n)
	storages := make([]float64, n)
	repls := make([]float64, n)
	clusters := make([]float64, n)
	for i, s := range snaps {
		overalls[i] = s.Health.Overall
		storages[i] = s.Health.HealthStorage
		repls[i] = s.Health.HealthReplication
		clusters[i] = s.Health.HealthCluster
	}

	slope := computeSlopeStats(overalls, storages, repls, clusters)
	oscillation := computeOscillationStats(overalls)
	persistence := computePersistenceStats(snaps)
	recovery := computeRecoveryStats(snaps, overalls)
	trajectory := classifyTrajectory(slope, oscillation, persistence, overalls[n-1])

	return TemporalAnalysis{
		Slope:         slope,
		Oscillation:   oscillation,
		Persistence:   persistence,
		Recovery:      recovery,
		Trajectory:    trajectory,
		SnapshotCount: n,
	}
}

func computeSlopeStats(overall, storage, repl, cluster []float64) SlopeStats {
	return SlopeStats{
		Overall:          linearSlope(overall),
		StorageSlope:     linearSlope(storage),
		ReplicationSlope: linearSlope(repl),
		ClusterSlope:     linearSlope(cluster),
	}
}

func linearSlope(y []float64) float64 {
	n := len(y)
	if n < 2 {
		return 0
	}

	sumX := float64(n * (n - 1) / 2)
	sumY := 0.0
	sumXY := 0.0
	sumX2 := 0.0

	for i, yi := range y {
		xi := float64(i)
		sumY += yi
		sumXY += xi * yi
		sumX2 += xi * xi
	}

	denom := float64(n)*sumX2 - sumX*sumX
	if denom == 0 {
		return 0
	}

	return (float64(n)*sumXY - sumX*sumY) / denom
}

func computeOscillationStats(overalls []float64) OscillationStats {
	if len(overalls) < 4 {
		return OscillationStats{}
	}

	zeroCrossings := 0
	var prevDelta float64
	var ampSum float64
	var ampMax float64
	ampCount := 0

	for i := 1; i < len(overalls); i++ {
		delta := overalls[i] - overalls[i-1]
		if i > 1 {
			if (delta > 0 && prevDelta < 0) || (delta < 0 && prevDelta > 0) {
				zeroCrossings++
				amp := math.Abs(delta)
				ampSum += amp
				if amp > ampMax {
					ampMax = amp
				}
				ampCount++
			}
		}
		prevDelta = delta
	}

	var meanAmp float64
	if ampCount > 0 {
		meanAmp = ampSum / float64(ampCount)
	}

	return OscillationStats{
		Oscillating:       zeroCrossings >= oscillationMinZC && meanAmp > oscillationMinAmp-1e-12,
		ZeroCrossingCount: zeroCrossings,
		MeanAmplitude:     meanAmp,
		MaxAmplitude:      ampMax,
	}
}

func computePersistenceStats(snaps []TemporalSnapshot) PersistenceStats {
	if len(snaps) < 2 {
		return PersistenceStats{}
	}

	storageEnd := -1
	storageStart := -1
	replEnd := -1
	replStart := -1
	clusterEnd := -1
	clusterStart := -1

	for i := len(snaps) - 1; i >= 0; i-- {
		h := snaps[i].Health
		if h.HealthStorage < degradedThreshold {
			if storageEnd == -1 {
				storageEnd = i
			}
			storageStart = i
		} else {
			break
		}
	}

	for i := len(snaps) - 1; i >= 0; i-- {
		h := snaps[i].Health
		if h.HealthReplication < degradedThreshold {
			if replEnd == -1 {
				replEnd = i
			}
			replStart = i
		} else {
			break
		}
	}

	for i := len(snaps) - 1; i >= 0; i-- {
		h := snaps[i].Health
		if h.HealthCluster < degradedThreshold {
			if clusterEnd == -1 {
				clusterEnd = i
			}
			clusterStart = i
		} else {
			break
		}
	}

	var storageDur, replDur, clusterDur time.Duration

	if storageStart >= 0 && storageEnd >= 0 && storageEnd > storageStart {
		storageDur = snaps[storageEnd].Time.Sub(snaps[storageStart].Time)
	} else if storageStart >= 0 && storageEnd >= 0 {
		storageDur = snaps[storageEnd].Time.Sub(snaps[storageStart].Time)
	}

	if replStart >= 0 && replEnd >= 0 {
		replDur = snaps[replEnd].Time.Sub(snaps[replStart].Time)
	}

	if clusterStart >= 0 && clusterEnd >= 0 {
		clusterDur = snaps[clusterEnd].Time.Sub(snaps[clusterStart].Time)
	}

	anyDegraded := storageStart >= 0 || replStart >= 0 || clusterStart >= 0

	worstDim := "storage"
	worstDur := storageDur
	if replDur > worstDur {
		worstDur = replDur
		worstDim = "replication"
	}
	if clusterDur > worstDur {
		worstDur = clusterDur
		worstDim = "cluster"
	}

	return PersistenceStats{
		AnyDegraded:         anyDegraded,
		StorageDegraded:     storageStart >= 0,
		StorageDuration:     storageDur,
		ReplicationDegraded: replStart >= 0,
		ReplicationDuration: replDur,
		ClusterDegraded:     clusterStart >= 0,
		ClusterDuration:     clusterDur,
		WorstDimension:      worstDim,
		WorstDuration:       worstDur,
	}
}

func computeRecoveryStats(snaps []TemporalSnapshot, overalls []float64) RecoveryStats {
	n := len(overalls)
	if n < 3 {
		return RecoveryStats{}
	}

	troughIdx := 0
	for i := 1; i < n; i++ {
		if overalls[i] < overalls[troughIdx] {
			troughIdx = i
		}
	}

	if troughIdx >= n-1 {
		return RecoveryStats{}
	}

	recoveryStart := -1
	for i := troughIdx + 1; i < n; i++ {
		if overalls[i] > overalls[troughIdx] {
			recoveryStart = i
			break
		}
	}

	if recoveryStart < 0 {
		return RecoveryStats{}
	}

	recoveryEnd := n - 1

	// Verify sustained recovery (not a blip)
	sustained := true
	for i := recoveryStart + 1; i < n; i++ {
		if overalls[i] <= overalls[troughIdx] {
			sustained = false
			break
		}
	}

	if !sustained {
		return RecoveryStats{}
	}

	recTime := snaps[recoveryEnd].Time.Sub(snaps[troughIdx].Time)
	recSeconds := recTime.Seconds()
	var velocity float64
	if recSeconds > 0 {
		velocity = (overalls[recoveryEnd] - overalls[troughIdx]) / recSeconds
	}

	oscillations := 0
	var prevDelta float64
	for i := troughIdx + 1; i <= recoveryEnd; i++ {
		delta := overalls[i] - overalls[i-1]
		if i > troughIdx+1 && ((delta > 0 && prevDelta < 0) || (delta < 0 && prevDelta > 0)) {
			oscillations++
		}
		prevDelta = delta
	}

	var dampingRatio float64
	switch {
	case oscillations <= 1:
		dampingRatio = 2.0
	case oscillations <= 3:
		dampingRatio = 1.0
	default:
		dampingRatio = 0.5
	}

	preDegScore := overalls[troughIdx-1]
	if troughIdx > 1 {
		for i := 0; i < troughIdx; i++ {
			if overalls[i] > preDegScore {
				preDegScore = overalls[i]
			}
		}
	}

	undershoot := 0.0
	finalScore := overalls[recoveryEnd]
	if finalScore < preDegScore {
		undershoot = preDegScore - finalScore
	}

	return RecoveryStats{
		Observed:     true,
		Velocity:     velocity,
		Duration:     recTime,
		DampingRatio: dampingRatio,
		Undershoot:   undershoot,
	}
}

func classifyTrajectory(slope SlopeStats, osc OscillationStats, persist PersistenceStats, currentOverall float64) string {
	if osc.Oscillating && (persist.AnyDegraded || currentOverall < warnThreshold) {
		return TrajectoryOscillating
	}

	if persist.AnyDegraded && math.Abs(slope.Overall) < minSlopeThreshold {
		return TrajectoryStuck
	}

	if slope.Overall < -minSlopeThreshold {
		return TrajectoryDegrading
	}

	if slope.Overall > minSlopeThreshold && (persist.AnyDegraded || currentOverall < warnThreshold) {
		return TrajectoryRecovering
	}

	return TrajectoryStable
}
