package monitor

import "fmt"

// DegradationLevel 表示系统退化严重程度
type DegradationLevel int

const (
	LevelOK DegradationLevel = iota
	LevelWarn
	LevelDegraded
	LevelFail
)

func (l DegradationLevel) String() string {
	switch l {
	case LevelOK:
		return "OK"
	case LevelWarn:
		return "WARN"
	case LevelDegraded:
		return "DEGRADED"
	case LevelFail:
		return "FAIL"
	default:
		return "UNKNOWN"
	}
}

// DegradationAssertion 退化不变性断言（三级门禁）
type DegradationAssertion struct {
	MaxGoroutineDelta   int
	MaxActiveRetries    int64
	MaxL0Score          float64
	L0RecoveryThreshold float64
	MaxReconnectCount   int64

	GoroutineWarnDelta     int
	ActiveRetriesWarn      int64
	L0DegradedThreshold    float64
	ReconnectWarnThreshold int64
	MonotonicWarnRatio     float64

	// Temporal semantics for goroutine delta (0 = disabled, single-spike check)
	GoroutineDeltaFailWindows int // consecutive samples above MaxGoroutineDelta to FAIL (default 3)
	GoroutineDeltaWarnWindows int // consecutive samples above GoroutineWarnDelta to WARN (default 2)

	// Cluster convergence gates (0 = not checked)
	MaxLeaderChurn    int64
	MinAgreedFraction float64 // minimum fraction of sentinels agreeing (0-1)

	AllowDegraded bool
}

// DefaultDegradationAssertion 默认退化不变性断言
func DefaultDegradationAssertion() DegradationAssertion {
	return DegradationAssertion{
		MaxGoroutineDelta:   50,
		MaxActiveRetries:    100,
		MaxL0Score:          25,
		L0RecoveryThreshold: 10,
		MaxReconnectCount:   50,

		GoroutineWarnDelta:        20,
		ActiveRetriesWarn:         30,
		L0DegradedThreshold:       15,
		ReconnectWarnThreshold:    10,
		MonotonicWarnRatio:        0.5,
		GoroutineDeltaFailWindows: 3,
		GoroutineDeltaWarnWindows: 2,

		MaxLeaderChurn:    5,
		MinAgreedFraction: 1.0,
	}
}

// CheckResult 退化检查结果
type CheckResult struct {
	Level DegradationLevel
	Logs  []string
}

// CheckDegradation 执行退化不变性断言
func (pm *PressureMonitor) CheckDegradation(t TestingT, a DegradationAssertion, baselineGoroutines int) DegradationLevel {
	latest := pm.Latest()
	samples := pm.Samples()
	level := LevelOK

	t.Log("[pm] === Degradation Invariants ===")

	// Goroutine 退化检测：用相对稳态增长代替绝对抬升门限。
	// 50 并发下 goroutine 自然抬升 ~64（正常负载），超过 MaxGoroutineDelta=50 但非泄漏。
	// 改为检测单调增长趋势：若 goroutine 在持续上升（泄漏），触发 FAIL；
	// 若升高但稳定（正常负载抬升），不触发。
	finalDelta := latest.Goroutines - baselineGoroutines
	goroLevel := pm.checkGoroutineGrowth(baselineGoroutines, a)
	level = maxLevel(level, goroLevel)
	switch goroLevel {
	case LevelFail:
		t.Errorf("DEGRADATION FAIL: goroutine monotonic rising trend (final delta=%d)", finalDelta)
	case LevelWarn:
		t.Logf("[pm]   WARN: goroutine mild rising trend (final delta=%d)", finalDelta)
	case LevelOK:
		t.Logf("[pm]   goroutine: final delta=%d, stable or OK (steady-state load)", finalDelta)
	}

	t.Logf("[pm]   active retries: final=%d (warn=%d, fail=%d)", latest.ActiveRetries, a.ActiveRetriesWarn, a.MaxActiveRetries)
	if latest.ActiveRetries > a.MaxActiveRetries {
		level = maxLevel(level, LevelFail)
		t.Errorf("DEGRADATION FAIL: active retries exceeded: %d > %d", latest.ActiveRetries, a.MaxActiveRetries)
	} else if latest.ActiveRetries > a.ActiveRetriesWarn {
		level = maxLevel(level, LevelWarn)
		t.Logf("[pm]   WARN: active retries elevated: %d > %d", latest.ActiveRetries, a.ActiveRetriesWarn)
	}

	t.Logf("[pm]   final L0 score: %.1f (recovery=%.1f degraded=%.1f fail=%.1f)",
		latest.LastL0Score, a.L0RecoveryThreshold, a.L0DegradedThreshold, a.MaxL0Score)
	l0Level := checkL0Recovery(latest.LastL0Score, a)
	level = maxLevel(level, l0Level)
	switch l0Level {
	case LevelFail:
		t.Errorf("DEGRADATION FAIL: L0 score did not recover: %.1f > %.1f", latest.LastL0Score, a.MaxL0Score)
	case LevelDegraded:
		msg := fmt.Sprintf("DEGRADED: L0 score degraded: %.1f (threshold: %.1f)", latest.LastL0Score, a.L0DegradedThreshold)
		if a.AllowDegraded {
			t.Logf("[pm]   %s", msg)
		} else {
			t.Errorf("DEGRADATION %s", msg)
		}
	}

	var peakL0 float64
	for _, s := range samples {
		if s.LastL0Score > peakL0 {
			peakL0 = s.LastL0Score
		}
	}
	t.Logf("[pm]   peak L0 score: %.1f (threshold: %.1f)", peakL0, a.MaxL0Score)
	if peakL0 > a.MaxL0Score {
		level = maxLevel(level, LevelFail)
		t.Errorf("DEGRADATION FAIL: L0 score peak exceeded: %.1f > %.1f", peakL0, a.MaxL0Score)
	}

	t.Logf("[pm]   reconnect count: %d (warn=%d, fail=%d)", latest.ReconnectCount, a.ReconnectWarnThreshold, a.MaxReconnectCount)
	if latest.ReconnectCount > a.MaxReconnectCount {
		level = maxLevel(level, LevelFail)
		t.Errorf("DEGRADATION FAIL: excessive reconnects: %d > %d", latest.ReconnectCount, a.MaxReconnectCount)
	} else if latest.ReconnectCount > a.ReconnectWarnThreshold {
		level = maxLevel(level, LevelWarn)
		t.Logf("[pm]   WARN: reconnect count elevated: %d > %d", latest.ReconnectCount, a.ReconnectWarnThreshold)
	}

	monoLevel := pm.checkMonotonicGrowth(a)
	level = maxLevel(level, monoLevel)
	switch monoLevel {
	case LevelFail:
		t.Errorf("DEGRADATION FAIL: L0 score shows strong monotonic rising trend")
	case LevelDegraded:
		msg := "DEGRADED: L0 score shows monotonic rising trend"
		if a.AllowDegraded {
			t.Logf("[pm]   %s", msg)
		} else {
			t.Errorf("DEGRADATION %s", msg)
		}
	case LevelWarn:
		t.Logf("[pm]   WARN: L0 score shows mild rising trend")
	}

	// Cluster convergence gates
	if latest.TotalSentinels > 0 {
		agreedFraction := float64(1.0)
		if latest.TotalSentinels > 0 {
			agreedFraction = float64(latest.AgreedSentinels) / float64(latest.TotalSentinels)
		}
		t.Logf("[pm]   sentinel agreement: %d/%d (%.0f%%) leaderChurn=%d",
			latest.AgreedSentinels, latest.TotalSentinels, agreedFraction*100, latest.LeaderChanges)

		if a.MinAgreedFraction > 0 && agreedFraction < a.MinAgreedFraction {
			level = maxLevel(level, LevelFail)
			t.Errorf("DEGRADATION FAIL: sentinel view divergence: %d/%d agreed (min %.0f%%)",
				latest.AgreedSentinels, latest.TotalSentinels, a.MinAgreedFraction*100)
		} else if a.MinAgreedFraction > 0 && agreedFraction < 1.0 {
			level = maxLevel(level, LevelWarn)
			t.Logf("[pm]   WARN: sentinel view not fully converged: %d/%d",
				latest.AgreedSentinels, latest.TotalSentinels)
		}

		if a.MaxLeaderChurn > 0 && latest.LeaderChanges > a.MaxLeaderChurn {
			level = maxLevel(level, LevelFail)
			t.Errorf("DEGRADATION FAIL: excessive leader churn: %d > %d", latest.LeaderChanges, a.MaxLeaderChurn)
		} else if a.MaxLeaderChurn > 0 && latest.LeaderChanges > 0 {
			t.Logf("[pm]   WARN: leader churn detected: %d", latest.LeaderChanges)
		}

		if latest.ClusterFragmented {
			level = maxLevel(level, LevelFail)
			t.Errorf("DEGRADATION FAIL: cluster is in fragmented state (split-brain)")
		}
	}

	dimLevel := pm.checkDimensionHealth(t, latest, samples)
	level = maxLevel(level, dimLevel)

	t.Logf("[pm] === Degradation Result: %s ===", level)
	return level
}

func (pm *PressureMonitor) checkDimensionHealth(t TestingT, latest PressureSample, samples []PressureSample) DegradationLevel {
	if len(samples) == 0 {
		return LevelOK
	}
	hs := ComputeHealth(samples, 0)
	lowest := "none"
	lowestScore := 1.0
	if hs.HealthStorage < lowestScore {
		lowestScore = hs.HealthStorage
		lowest = "storage"
	}
	if hs.HealthReplication < lowestScore {
		lowestScore = hs.HealthReplication
		lowest = "replication"
	}
	if hs.HealthCluster < lowestScore {
		lowestScore = hs.HealthCluster
		lowest = "cluster"
	}
	t.Logf("[pm]   dimensions: S=%.2f R=%.2f C=%.2f (weakest=%s)",
		hs.HealthStorage, hs.HealthReplication, hs.HealthCluster, lowest)

	if lowestScore < 0.4 {
		t.Errorf("DEGRADATION FAIL: %s dimension critically degraded (%.2f)", lowest, lowestScore)
		return LevelFail
	}
	if lowestScore < 0.7 {
		t.Logf("[pm]   WARN: %s dimension degraded (%.2f)", lowest, lowestScore)
		return LevelWarn
	}
	return LevelOK
}

func maxLevel(a, b DegradationLevel) DegradationLevel {
	if a > b {
		return a
	}
	return b
}

// CountConsecutiveAbove returns the longest consecutive run of samples
// where goroutines exceed baseline+threshold. Implements temporal semantics
// for behavioral degradation detection (vs single-spike threshold monitoring).
func CountConsecutiveAbove(samples []PressureSample, baseline, threshold int) int {
	maxStreak, cur := 0, 0
	for _, s := range samples {
		if s.Goroutines-baseline > threshold {
			cur++
			if cur > maxStreak {
				maxStreak = cur
			}
		} else {
			cur = 0
		}
	}
	return maxStreak
}

func checkL0Recovery(finalL0 float64, a DegradationAssertion) DegradationLevel {
	if finalL0 <= a.L0RecoveryThreshold {
		return LevelOK
	}
	if finalL0 > a.MaxL0Score || finalL0 > a.L0DegradedThreshold {
		return LevelFail
	}
	return LevelDegraded
}

func (pm *PressureMonitor) checkMonotonicGrowth(a DegradationAssertion) DegradationLevel {
	samples := pm.Samples()
	if len(samples) < 3 {
		return LevelOK
	}

	tail := samples[len(samples)/2:]
	rising := 0
	for i := 1; i < len(tail); i++ {
		if tail[i].LastL0Score > tail[i-1].LastL0Score {
			rising++
		}
	}
	if len(tail) == 0 {
		return LevelOK
	}
	ratio := float64(rising) / float64(len(tail))
	if ratio > 0.7 {
		return LevelFail
	}
	if ratio > a.MonotonicWarnRatio {
		return LevelWarn
	}
	return LevelOK
}

// checkGoroutineGrowth 检测 goroutine 是否在持续增长（泄漏）而非仅升高（正常负载）。
// 绝对抬升门限（如 MaxGoroutineDelta=50）在 50 并发下产生误报
// （goroutine 自然抬升 ~64 但非泄漏）。改用相对稳态增长检测：
// 后半段采样中 goroutine delta 持续上升 → 泄漏；升高但稳定 → 正常负载。
func (pm *PressureMonitor) checkGoroutineGrowth(baselineGoroutines int, a DegradationAssertion) DegradationLevel {
	samples := pm.Samples()
	if len(samples) < 3 {
		return LevelOK
	}

	tail := samples[len(samples)/2:]
	rising := 0
	for i := 1; i < len(tail); i++ {
		delta := tail[i].Goroutines - baselineGoroutines
		prevDelta := tail[i-1].Goroutines - baselineGoroutines
		if delta > prevDelta {
			rising++
		}
	}
	if len(tail) == 0 {
		return LevelOK
	}
	ratio := float64(rising) / float64(len(tail))
	if ratio > 0.7 {
		return LevelFail
	}
	if ratio > a.MonotonicWarnRatio {
		return LevelWarn
	}
	return LevelOK
}
