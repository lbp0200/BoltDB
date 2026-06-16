package monitor

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/zeebo/assert"
)

func TestComputeGoroutineHealth(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 1.0, computeGoroutineHealth(100, 100))
	assert.Equal(t, 1.0, computeGoroutineHealth(110, 100))
	assert.Equal(t, 1.0, computeGoroutineHealth(109, 100))
	assert.True(t, computeGoroutineHealth(130, 100) < 1.0)
	assert.True(t, computeGoroutineHealth(130, 100) > 0.0)
	assert.Equal(t, 0.0, computeGoroutineHealth(150, 100))
	assert.Equal(t, 0.0, computeGoroutineHealth(200, 100))
	assert.True(t, computeGoroutineHealth(100, 80) < 1.0)
}

func TestComputeL0RecoveryHealth(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 1.0, computeL0RecoveryHealth(0.0))
	assert.Equal(t, 1.0, computeL0RecoveryHealth(8.0))
	assert.True(t, computeL0RecoveryHealth(10.0) < 1.0)
	assert.True(t, computeL0RecoveryHealth(10.0) > 0.5)
	assert.True(t, computeL0RecoveryHealth(15.0) <= 0.5)
	assert.True(t, computeL0RecoveryHealth(20.0) > 0.0)
	assert.Equal(t, 0.0, computeL0RecoveryHealth(25.0))
	assert.Equal(t, 0.0, computeL0RecoveryHealth(30.0))
}

func TestComputeL0StressHealth(t *testing.T) {
	t.Parallel()
	samples := []PressureSample{
		{LastL0Score: 5.0},
		{LastL0Score: 10.0},
		{LastL0Score: 14.9},
	}
	assert.Equal(t, 1.0, computeL0StressHealth(samples))

	samples2 := []PressureSample{
		{LastL0Score: 5.0},
		{LastL0Score: 16.0},
	}
	assert.True(t, computeL0StressHealth(samples2) < 1.0)
	assert.True(t, computeL0StressHealth(samples2) > 0.0)

	samples3 := []PressureSample{
		{LastL0Score: 5.0},
		{LastL0Score: 25.0},
	}
	assert.Equal(t, 0.0, computeL0StressHealth(samples3))

	samples4 := []PressureSample{
		{LastL0Score: 30.0},
	}
	assert.Equal(t, 0.0, computeL0StressHealth(samples4))
}

func TestComputeRetryHealth(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 1.0, computeRetryHealth(PressureSample{ActiveRetries: 0}))
	assert.Equal(t, 1.0, computeRetryHealth(PressureSample{ActiveRetries: 5}))
	assert.True(t, computeRetryHealth(PressureSample{ActiveRetries: 10}) < 1.0)
	assert.True(t, computeRetryHealth(PressureSample{ActiveRetries: 10}) > 0.5)
	assert.True(t, computeRetryHealth(PressureSample{ActiveRetries: 30}) <= 0.5)
	assert.True(t, computeRetryHealth(PressureSample{ActiveRetries: 50}) > 0.0)
	assert.Equal(t, 0.0, computeRetryHealth(PressureSample{ActiveRetries: 100}))
	assert.Equal(t, 0.0, computeRetryHealth(PressureSample{ActiveRetries: 200}))
}

func TestComputeReplicationHealth_Standalone(t *testing.T) {
	t.Parallel()
	latest := PressureSample{ConnectedSlaves: 0, ReconnectCount: 0}
	assert.Equal(t, 1.0, computeReplicationHealth(latest))
}

func TestComputeReplicationHealth_MasterWithSlaves(t *testing.T) {
	t.Parallel()
	latest := PressureSample{ConnectedSlaves: 2, SlaveOffset: 0, ReconnectCount: 0}
	assert.Equal(t, 1.0, computeReplicationHealth(latest))

	latest = PressureSample{ConnectedSlaves: 2, SlaveOffset: 0, ReconnectCount: 5}
	result := computeReplicationHealth(latest)
	assert.True(t, result > 0.0)
	assert.True(t, result < 1.0)
}

func TestComputeReplicationHealth_Slave(t *testing.T) {
	t.Parallel()
	latest := PressureSample{
		ConnectedSlaves: 0, SlaveOffset: 100,
		MasterOffset: 100, ReconnectCount: 0,
	}
	assert.Equal(t, 1.0, computeReplicationHealth(latest))

	latest = PressureSample{
		ConnectedSlaves: 0, SlaveOffset: 100,
		MasterOffset: 300, ReconnectCount: 1,
	}
	result := computeReplicationHealth(latest)
	assert.True(t, result > 0.3)
	assert.True(t, result < 1.0)

	latest = PressureSample{
		ConnectedSlaves: 0, SlaveOffset: 100,
		MasterOffset: 20000, ReconnectCount: 1,
	}
	r2 := computeReplicationHealth(latest)
	assert.True(t, r2 > 0.0)
	assert.True(t, r2 < 0.5)
}

func TestComputeReplicationHealth_SlaveWithLagAndReconnects(t *testing.T) {
	t.Parallel()
	latest := PressureSample{
		ConnectedSlaves: 0, SlaveOffset: 100,
		MasterOffset: 110, ReconnectCount: 10,
	}
	result := computeReplicationHealth(latest)
	assert.True(t, result < 1.0)
	assert.True(t, result > 0.0)
}

func TestComputeVolatilityHealth_FewSamples(t *testing.T) {
	t.Parallel()
	samples := []PressureSample{
		{LastL0Score: 5.0},
		{LastL0Score: 6.0},
	}
	assert.Equal(t, 1.0, computeVolatilityHealth(samples))
}

func TestComputeVolatilityHealth_LowVariance(t *testing.T) {
	t.Parallel()
	samples := []PressureSample{
		{LastL0Score: 5.0}, {LastL0Score: 5.1},
		{LastL0Score: 5.2}, {LastL0Score: 5.0},
		{LastL0Score: 5.1}, {LastL0Score: 5.0},
	}
	assert.Equal(t, 1.0, computeVolatilityHealth(samples))
}

func TestComputeVolatilityHealth_HighVariance(t *testing.T) {
	t.Parallel()
	samples := []PressureSample{
		{LastL0Score: 0.0}, {LastL0Score: 20.0},
		{LastL0Score: 0.0}, {LastL0Score: 25.0},
		{LastL0Score: 0.0}, {LastL0Score: 18.0},
	}
	assert.True(t, computeVolatilityHealth(samples) < 1.0)
}

func TestComputeVolatilityHealth_ExtremeVariance(t *testing.T) {
	t.Parallel()
	samples := []PressureSample{
		{LastL0Score: 0.0}, {LastL0Score: 100.0},
		{LastL0Score: 0.0}, {LastL0Score: 100.0},
		{LastL0Score: 0.0}, {LastL0Score: 100.0},
		{LastL0Score: 0.0}, {LastL0Score: 100.0},
	}
	result := computeVolatilityHealth(samples)
	assert.True(t, result >= 0.0)
}

func TestComputeVolatilityHealth_ZeroMean(t *testing.T) {
	t.Parallel()
	samples := []PressureSample{
		{LastL0Score: 0.0}, {LastL0Score: 0.0},
		{LastL0Score: 0.0}, {LastL0Score: 0.0},
		{LastL0Score: 0.0}, {LastL0Score: 0.0},
	}
	assert.Equal(t, 1.0, computeVolatilityHealth(samples))
}

func TestComputeRecoveryTimeHealth_FewSamples(t *testing.T) {
	t.Parallel()
	samples := []PressureSample{{LastL0Score: 5.0}}
	dur, health := computeRecoveryTimeHealth(samples)
	assert.Equal(t, time.Duration(0), dur)
	assert.Equal(t, 1.0, health)
}

func TestComputeRecoveryTimeHealth_AlreadyRecovered(t *testing.T) {
	t.Parallel()
	now := time.Now()
	samples := []PressureSample{
		{Timestamp: now, LastL0Score: 5.0},
		{Timestamp: now.Add(time.Second), LastL0Score: 3.0},
	}
	dur, health := computeRecoveryTimeHealth(samples)
	assert.Equal(t, time.Duration(0), dur)
	assert.Equal(t, 1.0, health)
}

func TestComputeRecoveryTimeHealth_FastRecovery(t *testing.T) {
	t.Parallel()
	now := time.Now()
	samples := []PressureSample{
		{Timestamp: now, LastL0Score: 5.0},
		{Timestamp: now.Add(1 * time.Second), LastL0Score: 15.0},
		{Timestamp: now.Add(2 * time.Second), LastL0Score: 20.0},
		{Timestamp: now.Add(4 * time.Second), LastL0Score: 5.0},
	}
	dur, health := computeRecoveryTimeHealth(samples)
	assert.True(t, dur > 0)
	assert.Equal(t, 1.0, health)
}

func TestComputeRecoveryTimeHealth_SlowRecovery(t *testing.T) {
	t.Parallel()
	now := time.Now()
	samples := []PressureSample{
		{Timestamp: now, LastL0Score: 5.0},
		{Timestamp: now.Add(1 * time.Second), LastL0Score: 20.0},
		{Timestamp: now.Add(30 * time.Second), LastL0Score: 5.0},
	}
	dur, health := computeRecoveryTimeHealth(samples)
	assert.True(t, dur > 0)
	assert.True(t, health < 1.0)
}

func TestComputeRecoveryTimeHealth_NoRecovery(t *testing.T) {
	t.Parallel()
	now := time.Now()
	samples := []PressureSample{
		{Timestamp: now, LastL0Score: 5.0},
		{Timestamp: now.Add(1 * time.Second), LastL0Score: 20.0},
		{Timestamp: now.Add(2 * time.Second), LastL0Score: 22.0},
		{Timestamp: now.Add(3 * time.Second), LastL0Score: 21.0},
		{Timestamp: now.Add(4 * time.Second), LastL0Score: 19.0},
	}
	dur, health := computeRecoveryTimeHealth(samples)
	assert.Equal(t, 0.0, health)
	assert.True(t, dur > 0)
}

func TestComputeRecoveryTimeHealth_NeverPeaked(t *testing.T) {
	t.Parallel()
	now := time.Now()
	samples := []PressureSample{
		{Timestamp: now, LastL0Score: 5.0},
		{Timestamp: now.Add(1 * time.Second), LastL0Score: 4.0},
		{Timestamp: now.Add(2 * time.Second), LastL0Score: 6.0},
	}
	dur, health := computeRecoveryTimeHealth(samples)
	assert.Equal(t, time.Duration(0), dur)
	assert.Equal(t, 1.0, health)
}

func TestComputeStorageDimension(t *testing.T) {
	t.Parallel()
	h := HealthScore{
		L0RecoveryHealth:   1.0,
		L0StressHealth:     1.0,
		RetryHealth:        1.0,
		VolatilityHealth:   1.0,
		RecoveryTimeHealth: 1.0,
		GoroutineHealth:    1.0,
	}
	result := computeStorageDimension(h)
	assert.True(t, math.Abs(result-1.0) < 1e-9)

	h2 := HealthScore{
		L0RecoveryHealth:   0.5,
		L0StressHealth:     0.5,
		RetryHealth:        0.5,
		VolatilityHealth:   0.5,
		RecoveryTimeHealth: 0.5,
		GoroutineHealth:    0.5,
	}
	result = computeStorageDimension(h2)
	assert.True(t, result > 0.0)
	assert.True(t, result < 1.0)
}

func TestComputeClusterHealth_NoSentinels(t *testing.T) {
	t.Parallel()
	latest := PressureSample{TotalSentinels: 0}
	assert.Equal(t, 1.0, computeClusterHealth(latest, nil))
}

func TestComputeClusterHealth_Fragmented(t *testing.T) {
	t.Parallel()
	latest := PressureSample{
		TotalSentinels:    3,
		ClusterFragmented: true,
	}
	assert.Equal(t, 0.2, computeClusterHealth(latest, nil))
}

func TestComputeClusterHealth_SingleSentinel(t *testing.T) {
	t.Parallel()
	latest := PressureSample{
		TotalSentinels:  1,
		AgreedSentinels: 1,
	}
	assert.Equal(t, 1.0, computeClusterHealth(latest, nil))
}

func TestComputeClusterHealth_FullAgreement(t *testing.T) {
	t.Parallel()
	latest := PressureSample{
		TotalSentinels:  5,
		AgreedSentinels: 5,
	}
	assert.Equal(t, 1.0, computeClusterHealth(latest, nil))
}

func TestComputeClusterHealth_PartialAgreement(t *testing.T) {
	t.Parallel()
	latest := PressureSample{
		TotalSentinels:  4,
		AgreedSentinels: 3,
	}
	result := computeClusterHealth(latest, nil)
	assert.True(t, result > 0.0)
	assert.True(t, result < 1.0)
}

func TestComputeClusterHealth_MinorityAgreement(t *testing.T) {
	t.Parallel()
	latest := PressureSample{
		TotalSentinels:  4,
		AgreedSentinels: 1,
	}
	assert.Equal(t, 0.0, computeClusterHealth(latest, nil))
}

func TestComputeConvergenceTime_NoFragmentation(t *testing.T) {
	t.Parallel()
	now := time.Now()
	samples := []PressureSample{
		{Timestamp: now, TotalSentinels: 3, ClusterFragmented: false},
		{Timestamp: now.Add(time.Second), TotalSentinels: 3, ClusterFragmented: false},
	}
	assert.Equal(t, time.Duration(0), computeConvergenceTime(samples))
}

func TestComputeConvergenceTime_NoSentinels(t *testing.T) {
	t.Parallel()
	now := time.Now()
	samples := []PressureSample{
		{Timestamp: now, TotalSentinels: 0},
		{Timestamp: now.Add(time.Second), TotalSentinels: 0},
	}
	assert.Equal(t, time.Duration(0), computeConvergenceTime(samples))
}

func TestComputeConvergenceTime_Fragmented(t *testing.T) {
	t.Parallel()
	now := time.Now()
	samples := []PressureSample{
		{Timestamp: now, TotalSentinels: 3, ClusterFragmented: false},
		{Timestamp: now.Add(1 * time.Second), TotalSentinels: 3, ClusterFragmented: true},
		{Timestamp: now.Add(2 * time.Second), TotalSentinels: 3, ClusterFragmented: true},
		{Timestamp: now.Add(3 * time.Second), TotalSentinels: 3, ClusterFragmented: false},
	}
	assert.Equal(t, 1*time.Second, computeConvergenceTime(samples))
}

func TestComputeConvergenceTime_SingleFragmentedSample(t *testing.T) {
	t.Parallel()
	now := time.Now()
	samples := []PressureSample{
		{Timestamp: now, TotalSentinels: 3, ClusterFragmented: true},
	}
	assert.Equal(t, time.Duration(0), computeConvergenceTime(samples))
}

func TestComputeClusterDimension_NoSentinels(t *testing.T) {
	t.Parallel()
	latest := PressureSample{TotalSentinels: 0}
	assert.Equal(t, 1.0, computeClusterDimension(latest, nil))
}

func TestComputeClusterDimension_Fragmented(t *testing.T) {
	t.Parallel()
	now := time.Now()
	latest := PressureSample{
		TotalSentinels:    3,
		AgreedSentinels:   3,
		LeaderChanges:     0,
		ClusterFragmented: true,
	}
	samples := []PressureSample{
		{Timestamp: now, TotalSentinels: 3, ClusterFragmented: false},
		{Timestamp: now.Add(time.Second), TotalSentinels: 3, ClusterFragmented: true},
	}
	result := computeClusterDimension(latest, samples)
	assert.True(t, result > 0.0)
}

func TestComputeClusterDimension_HighLeaderChurn(t *testing.T) {
	t.Parallel()
	latest := PressureSample{
		TotalSentinels:  3,
		AgreedSentinels: 3,
		LeaderChanges:   20,
	}
	result := computeClusterDimension(latest, nil)
	assert.True(t, result > 0.0)
	assert.True(t, result < 1.0)
}

func TestComputeOverall_Simple(t *testing.T) {
	t.Parallel()
	h := HealthScore{
		HealthStorage:     1.0,
		HealthReplication: 1.0,
		HealthCluster:     1.0,
	}
	assert.Equal(t, 1.0, computeOverall(h))
}

func TestComputeOverall_MidRange(t *testing.T) {
	t.Parallel()
	h := HealthScore{
		HealthStorage:     0.5,
		HealthReplication: 0.5,
		HealthCluster:     0.5,
	}
	result := computeOverall(h)
	assert.True(t, result > 0.0)
	assert.Equal(t, 0.5, result)
}

func TestComputeOverall_Zero(t *testing.T) {
	t.Parallel()
	h := HealthScore{
		HealthStorage:     0.0,
		HealthReplication: 0.0,
		HealthCluster:     0.0,
	}
	assert.Equal(t, 0.0, computeOverall(h))
}

func TestComputeOverall_RiskEnvelope(t *testing.T) {
	t.Parallel()
	h := HealthScore{
		HealthStorage:     1.0,
		HealthReplication: 1.0,
		HealthCluster:     0.3,
	}
	result := computeOverall(h)
	assert.Equal(t, 0.6, result)
}

func TestComputeOverall_Clamped(t *testing.T) {
	t.Parallel()
	h := HealthScore{
		HealthStorage:     1.5,
		HealthReplication: 1.5,
		HealthCluster:     1.5,
	}
	result := computeOverall(h)
	assert.Equal(t, 1.0, result)
}

func TestComputeOverall_Negative(t *testing.T) {
	t.Parallel()
	h := HealthScore{
		HealthStorage:     -0.5,
		HealthReplication: -0.5,
		HealthCluster:     -0.5,
	}
	assert.Equal(t, 0.0, computeOverall(h))
}

func TestHealthScoreLevel(t *testing.T) {
	t.Parallel()
	h := HealthScore{Overall: 0.90}
	assert.Equal(t, int(LevelOK), h.Level())

	h = HealthScore{Overall: 0.85}
	assert.Equal(t, int(LevelOK), h.Level())

	h = HealthScore{Overall: 0.80}
	assert.Equal(t, int(LevelWarn), h.Level())

	h = HealthScore{Overall: 0.70}
	assert.Equal(t, int(LevelWarn), h.Level())

	h = HealthScore{Overall: 0.60}
	assert.Equal(t, int(LevelDegraded), h.Level())

	h = HealthScore{Overall: 0.50}
	assert.Equal(t, int(LevelDegraded), h.Level())

	h = HealthScore{Overall: 0.40}
	assert.Equal(t, int(LevelFail), h.Level())

	h = HealthScore{Overall: 0.0}
	assert.Equal(t, int(LevelFail), h.Level())
}

func TestHealthScoreString(t *testing.T) {
	t.Parallel()
	h := HealthScore{
		Overall:            0.95,
		HealthStorage:      0.98,
		HealthReplication:  0.92,
		HealthCluster:      0.90,
		GoroutineHealth:    1.0,
		L0RecoveryHealth:   1.0,
		L0StressHealth:     1.0,
		RetryHealth:        1.0,
		ReplicationHealth:  0.92,
		VolatilityHealth:   1.0,
		RecoveryTimeHealth: 1.0,
		ClusterHealth:      0.90,
	}
	out := h.String()
	assert.True(t, strings.Contains(out, "HEALTH SCORE REPORT"))
	assert.True(t, strings.Contains(out, "FINAL HEALTH:"))
	assert.True(t, strings.Contains(out, "0.95"))
}

func TestFormatReport(t *testing.T) {
	t.Parallel()
	h := HealthScore{
		Overall:            0.85,
		HealthStorage:      0.90,
		HealthReplication:  0.85,
		HealthCluster:      0.80,
		GoroutineHealth:    0.95,
		L0RecoveryHealth:   0.90,
		L0StressHealth:     0.85,
		RetryHealth:        0.80,
		ReplicationHealth:  0.85,
		VolatilityHealth:   0.90,
		RecoveryTimeHealth: 1.0,
		ClusterHealth:      0.80,
	}
	report := h.FormatReport()
	assert.True(t, strings.HasPrefix(report, "HEALTH: "))
	assert.True(t, strings.Contains(report, "S=0.90"))
	assert.True(t, strings.Contains(report, "R=0.85"))
	assert.True(t, strings.Contains(report, "C=0.80"))
}

func TestFormatCompact(t *testing.T) {
	t.Parallel()
	h := HealthScore{
		Overall:            0.75,
		HealthStorage:      0.80,
		HealthReplication:  0.75,
		HealthCluster:      0.70,
	}
	line := h.FormatCompact()
	assert.Equal(t, "0.75 [WARN] S=0.80 R=0.75 C=0.70", line)
}

func TestComputeHealth_EmptySamples(t *testing.T) {
	t.Parallel()
	h := ComputeHealth(nil, 0)
	assert.Equal(t, 0.0, h.Overall)
}

func TestComputeHealth_Integration(t *testing.T) {
	t.Parallel()
	now := time.Now()
	samples := []PressureSample{
		{Timestamp: now, LastL0Score: 5.0, ActiveRetries: 0, Goroutines: 100,
			MasterOffset: 1000, SlaveOffset: 1000},
		{Timestamp: now.Add(time.Second), LastL0Score: 8.0, ActiveRetries: 2, Goroutines: 105,
			MasterOffset: 2000, SlaveOffset: 2000},
		{Timestamp: now.Add(2 * time.Second), LastL0Score: 3.0, ActiveRetries: 1, Goroutines: 102,
			MasterOffset: 3000, SlaveOffset: 3000},
	}

	h := ComputeHealth(samples, 100)
	assert.True(t, h.Overall > 0.5)
	assert.True(t, h.Overall <= 1.0)
	assert.True(t, h.GoroutineHealth > 0.0)
	assert.True(t, h.L0RecoveryHealth > 0.0)
	assert.True(t, h.L0StressHealth > 0.0)
}

func TestLevelLabel(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "OK", levelLabel(int(LevelOK)))
	assert.Equal(t, "WARN", levelLabel(int(LevelWarn)))
	assert.Equal(t, "DEGRADED", levelLabel(int(LevelDegraded)))
	assert.Equal(t, "FAIL", levelLabel(int(LevelFail)))
	assert.Equal(t, "UNKNOWN", levelLabel(999))
}
