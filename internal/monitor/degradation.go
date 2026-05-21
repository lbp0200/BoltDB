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

		GoroutineWarnDelta:     20,
		ActiveRetriesWarn:      30,
		L0DegradedThreshold:    15,
		ReconnectWarnThreshold: 10,
		MonotonicWarnRatio:     0.5,
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

	goDelta := latest.Goroutines - baselineGoroutines
	t.Logf("[pm]   goroutine delta: %d (warn=%d, fail=%d)", goDelta, a.GoroutineWarnDelta, a.MaxGoroutineDelta)
	if goDelta > a.MaxGoroutineDelta {
		level = maxLevel(level, LevelFail)
		t.Errorf("DEGRADATION FAIL: goroutine unbounded growth: delta=%d > %d", goDelta, a.MaxGoroutineDelta)
	} else if goDelta > a.GoroutineWarnDelta {
		level = maxLevel(level, LevelWarn)
		t.Logf("[pm]   WARN: goroutine delta elevated: %d > %d", goDelta, a.GoroutineWarnDelta)
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

	t.Logf("[pm] === Degradation Result: %s ===", level)
	return level
}

func maxLevel(a, b DegradationLevel) DegradationLevel {
	if a > b {
		return a
	}
	return b
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
