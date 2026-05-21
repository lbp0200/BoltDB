package monitor

import (
	"fmt"
	"math"
	"strings"
	"time"
)

// HealthScore 系统健康评分（0.0-1.0），将多维稳定性指标压缩为标量。
// 由 PressureMonitor.HealthScore() 从采样轨迹计算得出。
type HealthScore struct {
	Overall            float64       // 综合评分
	GoroutineHealth    float64       // goroutine 增长健康状况
	L0RecoveryHealth   float64       // L0 恢复状况（最终 L0）
	L0StressHealth     float64       // L0 峰值压力
	RetryHealth        float64       // 重试/背压健康状况
	ReplicationHealth  float64       // 复制延迟 + 重连健康状况
	VolatilityHealth   float64       // L0 波动性（尾部变异系数）
	RecoveryTime       time.Duration // L0 从峰值恢复到 <10 的时间
	RecoveryTimeHealth float64       // 恢复时间健康状况
}

const (
	l0RecoveryThreshold  = 8.0
	l0DegradedThreshold  = 15.0
	l0FailThreshold      = 25.0
	goroutineFailDelta   = 50
	goroutineWarnDelta   = 10
	retryWarnLimit       = 5
	retryDegradedLimit   = 30
	retryFailLimit       = 100
	reconnectHealthy     = 3
	reconnectPenaltyMax  = 0.3
	reconnectPenaltyStep = 20.0
	lagHealthy           = 100
	lagDegraded          = 10000
	volatilityHealthy    = 0.2
	volatilityFail       = 1.0
	recoveryTimeHealthy  = 10 * time.Second
	recoveryTimeFail     = 60 * time.Second
)

var healthWeights = []struct {
	name   string
	weight float64
	value  func(h HealthScore) float64
}{
	{"goroutine", 0.20, func(h HealthScore) float64 { return h.GoroutineHealth }},
	{"L0 recovery", 0.20, func(h HealthScore) float64 { return h.L0RecoveryHealth }},
	{"L0 stress", 0.15, func(h HealthScore) float64 { return h.L0StressHealth }},
	{"retry", 0.15, func(h HealthScore) float64 { return h.RetryHealth }},
	{"replication", 0.15, func(h HealthScore) float64 { return h.ReplicationHealth }},
	{"volatility", 0.10, func(h HealthScore) float64 { return h.VolatilityHealth }},
	{"recovery time", 0.05, func(h HealthScore) float64 { return h.RecoveryTimeHealth }},
}

// ComputeHealth 从压力采样数组计算健康评分。
// baselineGoroutines 是测试开始时的 goroutine 数量基线。
func ComputeHealth(samples []PressureSample, baselineGoroutines int) HealthScore {
	if len(samples) == 0 {
		return HealthScore{}
	}

	latest := samples[len(samples)-1]

	h := HealthScore{}
	h.GoroutineHealth = computeGoroutineHealth(latest.Goroutines, baselineGoroutines)
	h.L0RecoveryHealth = computeL0RecoveryHealth(latest.LastL0Score)
	h.L0StressHealth = computeL0StressHealth(samples)
	h.RetryHealth = computeRetryHealth(latest)
	h.ReplicationHealth = computeReplicationHealth(latest)
	h.VolatilityHealth = computeVolatilityHealth(samples)
	h.RecoveryTime, h.RecoveryTimeHealth = computeRecoveryTimeHealth(samples)

	h.Overall = computeOverall(h)
	return h
}

func computeGoroutineHealth(current, baseline int) float64 {
	delta := float64(current - baseline)
	if delta <= float64(goroutineWarnDelta) {
		return 1.0
	}
	if delta >= float64(goroutineFailDelta) {
		return 0.0
	}
	return 1.0 - (delta-float64(goroutineWarnDelta))/float64(goroutineFailDelta-goroutineWarnDelta)
}

func computeL0RecoveryHealth(finalL0 float64) float64 {
	if finalL0 <= l0RecoveryThreshold {
		return 1.0
	}
	if finalL0 <= l0DegradedThreshold {
		return 1.0 - (finalL0-l0RecoveryThreshold)/(l0DegradedThreshold-l0RecoveryThreshold)*0.5
	}
	if finalL0 >= l0FailThreshold {
		return 0.0
	}
	return 0.5 - (finalL0-l0DegradedThreshold)/(l0FailThreshold-l0DegradedThreshold)*0.5
}

func computeL0StressHealth(samples []PressureSample) float64 {
	var peak float64
	for _, s := range samples {
		if s.LastL0Score > peak {
			peak = s.LastL0Score
		}
	}
	if peak <= l0DegradedThreshold {
		return 1.0
	}
	if peak >= l0FailThreshold {
		return 0.0
	}
	return 1.0 - (peak-l0DegradedThreshold)/(l0FailThreshold-l0DegradedThreshold)
}

func computeRetryHealth(latest PressureSample) float64 {
	active := float64(latest.ActiveRetries)
	if active <= float64(retryWarnLimit) {
		return 1.0
	}
	if active <= float64(retryDegradedLimit) {
		return 1.0 - (active-float64(retryWarnLimit))/(float64(retryDegradedLimit)-float64(retryWarnLimit))*0.5
	}
	if active >= float64(retryFailLimit) {
		return 0.0
	}
	return 0.5 - (active-float64(retryDegradedLimit))/(float64(retryFailLimit)-float64(retryDegradedLimit))*0.5
}

func computeReplicationHealth(latest PressureSample) float64 {
	lag := latest.MasterOffset - latest.SlaveOffset
	if lag < 0 {
		lag = 0
	}
	var replScore float64
	if lag <= lagHealthy {
		replScore = 1.0
	} else if lag >= lagDegraded {
		replScore = 0.3
	} else {
		replScore = 1.0 - float64(lag-lagHealthy)/float64(lagDegraded-lagHealthy)*0.7
	}
	reconnects := float64(latest.ReconnectCount)
	if reconnects > float64(reconnectHealthy) {
		penalty := (reconnects - float64(reconnectHealthy)) / reconnectPenaltyStep
		if penalty > reconnectPenaltyMax {
			penalty = reconnectPenaltyMax
		}
		replScore -= penalty
	}
	if replScore < 0 {
		return 0.0
	}
	return replScore
}

func computeVolatilityHealth(samples []PressureSample) float64 {
	if len(samples) < 4 {
		return 1.0
	}
	// 取后 1/3 样本计算 L0 变异系数
	tail := samples[len(samples)/2:]
	var sum, sumSq float64
	count := 0
	for _, s := range tail {
		sum += s.LastL0Score
		sumSq += s.LastL0Score * s.LastL0Score
		count++
	}
	if count == 0 {
		return 1.0
	}
	mean := sum / float64(count)
	if mean < 0.01 {
		return 1.0
	}
	variance := sumSq/float64(count) - mean*mean
	if variance < 0 {
		variance = 0
	}
	cv := math.Sqrt(variance) / mean
	if cv <= volatilityHealthy {
		return 1.0
	}
	if cv >= volatilityFail {
		return 0.0
	}
	return 1.0 - (cv-volatilityHealthy)/(volatilityFail-volatilityHealthy)
}

func computeRecoveryTimeHealth(samples []PressureSample) (time.Duration, float64) {
	if len(samples) < 2 {
		return 0, 1.0
	}
	// 找到 L0 峰值位置
	peakIdx := 0
	for i := 1; i < len(samples); i++ {
		if samples[i].LastL0Score > samples[peakIdx].LastL0Score {
			peakIdx = i
		}
	}
	// 如果峰值已经是低的，恢复时间为 0
	if samples[peakIdx].LastL0Score <= l0RecoveryThreshold {
		return 0, 1.0
	}
	// 找到峰值后第一个恢复到 <10 的采样
	recoveryIdx := -1
	for i := peakIdx + 1; i < len(samples); i++ {
		if samples[i].LastL0Score <= l0RecoveryThreshold {
			recoveryIdx = i
			break
		}
	}
	if recoveryIdx == -1 {
		// 从未恢复
		dur := samples[len(samples)-1].Timestamp.Sub(samples[peakIdx].Timestamp)
		return dur, 0.0
	}
	dur := samples[recoveryIdx].Timestamp.Sub(samples[peakIdx].Timestamp)
	if dur <= recoveryTimeHealthy {
		return dur, 1.0
	}
	if dur >= recoveryTimeFail {
		return dur, 0.0
	}
	return dur, 1.0 - float64(dur-recoveryTimeHealthy)/float64(recoveryTimeFail-recoveryTimeHealthy)
}

func computeOverall(h HealthScore) float64 {
	var total float64
	for _, w := range healthWeights {
		total += w.weight * w.value(h)
	}
	return total
}

// String 返回健康评分多行文本报告
func (h HealthScore) String() string {
	var b strings.Builder
	b.WriteString("\nHEALTH SCORE REPORT\n")
	b.WriteString("-------------------\n")
	for _, w := range healthWeights {
		fmt.Fprintf(&b, "  %-16s %.2f\n", w.name+":", w.value(h))
	}
	if h.RecoveryTime > 0 {
		fmt.Fprintf(&b, "  %-16s %v\n", "recovery time:", h.RecoveryTime)
	}
	b.WriteString("-------------------\n")
	fmt.Fprintf(&b, "  FINAL HEALTH: %.2f  STATUS: %s\n", h.Overall, levelLabel(h.Level()))
	return b.String()
}

// Level 根据 Overall 评分返回退化等级
func (h HealthScore) Level() int {
	if h.Overall >= 0.85 {
		return int(LevelOK)
	}
	if h.Overall >= 0.70 {
		return int(LevelWarn)
	}
	if h.Overall >= 0.50 {
		return int(LevelDegraded)
	}
	return int(LevelFail)
}

// FormatReport 返回符合 testing.T.Log 的紧凑报告
func (h HealthScore) FormatReport() string {
	var b strings.Builder
	b.WriteString("HEALTH: ")
	b.WriteString(h.FormatCompact())
	for _, w := range healthWeights {
		fmt.Fprintf(&b, " %s=%.2f", w.name, w.value(h))
	}
	if h.RecoveryTime > 0 {
		fmt.Fprintf(&b, " recover=%v", h.RecoveryTime.Round(time.Second))
	}
	return b.String()
}

// FormatCompact 返回一行摘要
func (h HealthScore) FormatCompact() string {
	return fmt.Sprintf("%.2f [%s]", h.Overall, levelLabel(h.Level()))
}

func levelLabel(level int) string {
	switch DegradationLevel(level) {
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
