package monitor

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// EvolutionRun 单次历史 run 的摘要（从 summary JSON 加载）
type EvolutionRun struct {
	Timestamp        time.Time `json:"ts"`
	Duration         string    `json:"duration"`
	Samples          int       `json:"samples"`
	HealthOverall    float64   `json:"health_overall"`
	HealthStorage    float64   `json:"health_storage"`
	HealthRepl       float64   `json:"health_replication"`
	HealthCluster    float64   `json:"health_cluster"`
	Trajectory       string    `json:"trajectory"`
	Basin            string    `json:"basin"`
	BasinDepth       float64   `json:"basin_depth"`
	L0Final          float64   `json:"l0_final"`
	L0Peak           float64   `json:"l0_peak"`
	L0Velocity       float64   `json:"l0_velocity"`
	L0Acceleration   float64   `json:"l0_acceleration"`
	ActiveRetries    int64     `json:"active_retries"`
	GoroutineDelta   int       `json:"goroutine_delta"`
	LimitCycle       bool      `json:"limit_cycle"`
	Converging       bool      `json:"converging"`
	InDegradation    bool      `json:"in_degradation"`
	Escapable        bool      `json:"escapable"`
	DegradationLevel string    `json:"degradation_level"`

	// Recovery dynamics (from temporal analysis)
	RecoveryVelocity       float64 `json:"recovery_velocity,omitempty"`
	RecoveryDurationSec    float64 `json:"recovery_duration_sec,omitempty"`
	PersistenceDurationSec float64 `json:"persistence_duration_sec,omitempty"`
	OscillationDetected    bool    `json:"oscillation_detected,omitempty"`
}

// EvolutionReport 跨 run 演化趋势分析
type EvolutionReport struct {
	AnalysisTime time.Time
	Runs         []EvolutionRun
	RunCount     int
	SpanDays     float64

	HealthSlope      float64
	StorageSlope     float64
	ReplicationSlope float64
	ClusterSlope     float64
	BasinDepthSlope  float64
	L0PeakSlope      float64

	HealthTrend      string
	StorageTrend     string
	ReplicationTrend string
	ClusterTrend     string
	BasinDepthTrend  string
	OscillationTrend string

	RegimeShiftDetected    bool
	RegimeShiftDescription string
	EscalatingDegradation  bool
	Warnings               []string

	Level DegradationLevel

	// Evolution Gate v1 fields
	HealthSlopeRecent     float64 // slope over last 3 runs
	RecoveryTimeSlope     float64 // trend in recovery duration (positive = slower recovery)
	PersistenceSlope      float64 // trend in persistence duration (positive = longer degradation)
	RecoveryVelocitySlope float64 // trend in recovery speed (negative = slowing)

	PersistenceTrend      string
	RecoveryTimeTrend     string
	RecoveryVelocityTrend string

	RegimeShiftToWorse bool
	GateReasons        []string
}

const (
	trendImproving = "improving"
	trendStable    = "stable"
	trendDegrading = "degrading"
)

// LoadEvolutionHistory 从目录加载所有 `prefix-*.json` 历史摘要文件
func LoadEvolutionHistory(dir, prefix string) ([]EvolutionRun, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read history dir %s: %w", dir, err)
	}

	var runs []EvolutionRun
	for _, e := range entries {
		if e.IsDir() || !strings.HasPrefix(e.Name(), prefix+"-") || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			continue
		}
		var run EvolutionRun
		if err := json.Unmarshal(raw, &run); err != nil {
			continue
		}
		if run.Timestamp.IsZero() {
			continue
		}
		runs = append(runs, run)
	}

	sort.Slice(runs, func(i, j int) bool {
		return runs[i].Timestamp.Before(runs[j].Timestamp)
	})
	return runs, nil
}

// AnalyzeEvolution 对历史 runs 做演化趋势分析
func AnalyzeEvolution(runs []EvolutionRun) EvolutionReport {
	r := EvolutionReport{
		AnalysisTime: time.Now(),
		Runs:         runs,
		RunCount:     len(runs),
	}

	if len(runs) < 2 {
		r.HealthTrend = trendStable
		r.StorageTrend = trendStable
		r.ReplicationTrend = trendStable
		r.ClusterTrend = trendStable
		r.BasinDepthTrend = trendStable
		r.OscillationTrend = trendStable
		r.PersistenceTrend = trendStable
		r.RecoveryTimeTrend = trendStable
		r.RecoveryVelocityTrend = trendStable
		return r
	}

	r.SpanDays = runs[len(runs)-1].Timestamp.Sub(runs[0].Timestamp).Hours() / 24

	trends := []struct {
		name   string
		values []float64
		slope  *float64
		result *string
	}{
		{"health_overall", extractFloat(runs, func(v EvolutionRun) float64 { return v.HealthOverall }), &r.HealthSlope, &r.HealthTrend},
		{"health_storage", extractFloat(runs, func(v EvolutionRun) float64 { return v.HealthStorage }), &r.StorageSlope, &r.StorageTrend},
		{"health_replication", extractFloat(runs, func(v EvolutionRun) float64 { return v.HealthRepl }), &r.ReplicationSlope, &r.ReplicationTrend},
		{"health_cluster", extractFloat(runs, func(v EvolutionRun) float64 { return v.HealthCluster }), &r.ClusterSlope, &r.ClusterTrend},
		{"basin_depth", extractFloat(runs, func(v EvolutionRun) float64 { return v.BasinDepth }), &r.BasinDepthSlope, &r.BasinDepthTrend},
		{"l0_peak", extractFloat(runs, func(v EvolutionRun) float64 { return v.L0Peak }), &r.L0PeakSlope, nil},
	}

	for _, t := range trends {
		*t.slope = linearSlope(t.values)
	}

	fixTrendDirection(&r.HealthTrend, r.HealthSlope, true)
	fixTrendDirection(&r.StorageTrend, r.StorageSlope, true)
	fixTrendDirection(&r.ReplicationTrend, r.ReplicationSlope, true)
	fixTrendDirection(&r.ClusterTrend, r.ClusterSlope, true)
	fixTrendDirection(&r.BasinDepthTrend, r.BasinDepthSlope, false)

	r.OscillationTrend = detectOscillationTrend(runs)

	r.RegimeShiftDetected, r.RegimeShiftDescription = detectRegimeShift(runs)
	r.RegimeShiftToWorse = detectRegimeShiftToWorse(runs)

	r.EscalatingDegradation = detectEscalation(runs)

	// Evolution Gate v1: recent health slope (last 3 runs)
	r.HealthSlopeRecent = computeRecentSlope(runs, func(v EvolutionRun) float64 { return v.HealthOverall })

	// Recovery and persistence trends across runs
	r.RecoveryTimeSlope = computeSparseSlope(runs, func(v EvolutionRun) float64 { return v.RecoveryDurationSec })
	r.PersistenceSlope = computeSparseSlope(runs, func(v EvolutionRun) float64 { return v.PersistenceDurationSec })
	r.RecoveryVelocitySlope = computeSparseSlope(runs, func(v EvolutionRun) float64 { return v.RecoveryVelocity })

	r.PersistenceTrend = classifyDirection(r.PersistenceSlope, false)
	r.RecoveryTimeTrend = classifyDirection(r.RecoveryTimeSlope, false)
	r.RecoveryVelocityTrend = classifyDirection(r.RecoveryVelocitySlope, true)

	r.Warnings = buildWarnings(runs, r)
	r.Level, r.GateReasons = computeEvolutionLevel(r)

	return r
}

// fixTrendDirection corrects the slope-to-trend mapping.
// For health metrics: positive slope = improving
// For non-health metrics (basin depth, L0, persistence, recovery time): positive slope = degrading
func fixTrendDirection(trend *string, slope float64, isHealth bool) {
	if math.Abs(slope) < 0.005 {
		*trend = trendStable
		return
	}
	if isHealth {
		if slope > 0 {
			*trend = trendImproving
		} else {
			*trend = trendDegrading
		}
	} else {
		if slope > 0 {
			*trend = trendDegrading
		} else {
			*trend = trendImproving
		}
	}
}

func extractFloat(runs []EvolutionRun, fn func(EvolutionRun) float64) []float64 {
	out := make([]float64, len(runs))
	for i, r := range runs {
		out[i] = fn(r)
	}
	return out
}

var basinOrder = map[string]int{
	"healthy":   0,
	"stressed":  1,
	"degraded":  2,
	"collapsed": 3,
}

// computeRecentSlope computes linear slope over last k runs (min 2, max k=3).
func computeRecentSlope(runs []EvolutionRun, fn func(EvolutionRun) float64) float64 {
	n := len(runs)
	if n < 2 {
		return 0
	}
	k := 3
	if n < k {
		k = n
	}
	recent := runs[n-k:]
	vals := make([]float64, k)
	for i, r := range recent {
		vals[i] = fn(r)
	}
	return linearSlope(vals)
}

// computeSparseSlope computes slope across non-zero values only.
// If fewer than 2 non-zero values, returns 0.
func computeSparseSlope(runs []EvolutionRun, fn func(EvolutionRun) float64) float64 {
	var vals []float64
	for _, r := range runs {
		v := fn(r)
		if v > 0 {
			vals = append(vals, v)
		}
	}
	if len(vals) < 2 {
		return 0
	}
	return linearSlope(vals)
}

// classifyDirection maps a slope to a trend string.
// isHealth: true = positive slope is improving.
func classifyDirection(slope float64, isHealth bool) string {
	if math.Abs(slope) < 0.005 {
		return trendStable
	}
	if isHealth {
		if slope > 0 {
			return trendImproving
		}
		return trendDegrading
	}
	if slope > 0 {
		return trendDegrading
	}
	return trendImproving
}

// detectRegimeShiftToWorse checks if basin transitions are to worse states.
func detectRegimeShiftToWorse(runs []EvolutionRun) bool {
	if len(runs) < 3 {
		return false
	}
	for i := 1; i < len(runs)-1; i++ {
		prevOrder, prevOk := basinOrder[runs[i-1].Basin]
		curOrder, curOk := basinOrder[runs[i].Basin]
		nextOrder, nextOk := basinOrder[runs[i+1].Basin]
		if prevOk && curOk && nextOk && curOrder > prevOrder && curOrder == nextOrder {
			return true
		}
	}
	return false
}

func detectOscillationTrend(runs []EvolutionRun) string {
	if len(runs) < 3 {
		return trendStable
	}
	oscCount := 0
	for _, r := range runs {
		if r.LimitCycle {
			oscCount++
		}
	}
	frac := float64(oscCount) / float64(len(runs))
	if frac > 0.5 {
		return trendDegrading
	}
	if frac > 0.25 {
		return "mixed"
	}
	return trendStable
}

func detectRegimeShift(runs []EvolutionRun) (bool, string) {
	if len(runs) < 3 {
		return false, ""
	}
	// Check if basin permanently transitioned and stayed
	basins := make([]string, len(runs))
	for i, r := range runs {
		basins[i] = r.Basin
	}
	// Find the first transition that sticks for >= 2 consecutive runs
	for i := 1; i < len(basins)-1; i++ {
		if basins[i] != basins[i-1] && basins[i] == basins[i+1] {
			return true, fmt.Sprintf("basin regime shift at run #%d: %s → %s (persistent)",
				i+1, basins[i-1], basins[i])
		}
	}
	return false, ""
}

func detectEscalation(runs []EvolutionRun) bool {
	if len(runs) < 3 {
		return false
	}
	// Check if degradation_level is getting worse or in_degradation increasing
	degradedCount := 0
	for _, r := range runs {
		if r.InDegradation {
			degradedCount++
		}
	}
	// More than half of recent runs in degradation = escalating
	if degradedCount > len(runs)/2 {
		return true
	}
	// Check if last run is worse than first run
	if runs[len(runs)-1].HealthOverall < runs[0].HealthOverall-0.1 {
		return true
	}
	return false
}

func buildWarnings(runs []EvolutionRun, r EvolutionReport) []string {
	var warnings []string
	if r.RegimeShiftDetected {
		warnings = append(warnings, r.RegimeShiftDescription)
	}
	if r.EscalatingDegradation {
		warnings = append(warnings, "degradation is escalating across runs")
	}
	if len(runs) > 1 {
		last := runs[len(runs)-1]
		prev := runs[len(runs)-2]
		if last.HealthOverall-prev.HealthOverall < -0.1 {
			warnings = append(warnings, fmt.Sprintf("health dropped %.2f since last run", prev.HealthOverall-last.HealthOverall))
		}
		if last.BasinDepth-prev.BasinDepth > 0.2 {
			warnings = append(warnings, "basin depth significantly increased")
		}
	}
	return warnings
}

// computeEvolutionLevel converts evolution signals to a DegradationLevel gate.
// Used by CI to gate on long-term degradation trends.
// Returns (level, reasons).
func computeEvolutionLevel(r EvolutionReport) (DegradationLevel, []string) {
	if r.RunCount < 3 {
		return LevelOK, nil
	}

	var reasons []string

	// === FAIL CONDITIONS ===

	// 1. Health slope recently < -0.02 (sustained degradation in recent runs)
	if r.RunCount >= 3 && r.HealthSlopeRecent < -0.02 {
		reasons = append(reasons, fmt.Sprintf(
			"health slope in last 3 runs: %.4f (threshold: -0.02)", r.HealthSlopeRecent))
	}

	// 2. Regime shift to worse basin (stressed/degraded/collapsed)
	if r.RegimeShiftToWorse {
		reasons = append(reasons, "regime shift to worse basin state (sustained)")
	}

	// 3. Sustained oscillation pattern across runs
	if r.OscillationTrend == trendDegrading {
		reasons = append(reasons, "sustained oscillation pattern detected across runs")
	}

	// FAIL if any FAIL condition triggered
	if len(reasons) > 0 {
		return LevelFail, reasons
	}

	// === WARN CONDITIONS ===

	// 4. Health dropping for 3+ consecutive runs
	if len(r.Runs) >= 4 {
		last3 := r.Runs[len(r.Runs)-3:]
		allDropping := true
		for i := 1; i < len(last3); i++ {
			if last3[i].HealthOverall >= last3[i-1].HealthOverall {
				allDropping = false
				break
			}
		}
		if allDropping {
			reasons = append(reasons, "health dropped for 3 consecutive runs")
		}
	}

	// 5. Storage + health both degrading
	if r.StorageTrend == trendDegrading && r.HealthTrend == trendDegrading {
		reasons = append(reasons, "storage and overall health both degrading")
	}

	// 6. Degradation persistence increasing across runs
	if r.PersistenceTrend == trendDegrading {
		reasons = append(reasons, fmt.Sprintf(
			"degradation persistence increasing (slope=%.4f)", r.PersistenceSlope))
	}

	// 7. Recovery time increasing (recovery getting slower)
	if r.RecoveryTimeTrend == trendDegrading {
		reasons = append(reasons, fmt.Sprintf(
			"recovery time increasing (slope=%.4f)", r.RecoveryTimeSlope))
	}

	// 8. Escalating degradation (existing check)
	if r.EscalatingDegradation {
		reasons = append(reasons, "degradation escalating across runs")
	}

	if len(reasons) > 0 {
		return LevelWarn, reasons
	}

	return LevelOK, nil
}

// FormatReport 输出演化趋势 Markdown 报告
func (r EvolutionReport) FormatReport() string {
	var b strings.Builder

	fmt.Fprintf(&b, "# Evolution Analysis\n\n")
	fmt.Fprintf(&b, "- **Analysis Time**: %s\n", r.AnalysisTime.Format(time.RFC3339))
	fmt.Fprintf(&b, "- **Runs Analyzed**: %d\n", r.RunCount)
	fmt.Fprintf(&b, "- **Time Span**: %.1f days\n", r.SpanDays)
	fmt.Fprintf(&b, "- **Date Range**: %s → %s\n\n",
		r.Runs[0].Timestamp.Format("2006-01-02"),
		r.Runs[len(r.Runs)-1].Timestamp.Format("2006-01-02"))

	if r.RunCount < 2 {
		b.WriteString("_Insufficient data for trend analysis (need ≥2 runs)._")
		return b.String()
	}

	if r.Level != LevelOK {
		fmt.Fprintf(&b, "**Evolution Gate: %s**\n\n", r.Level)
	}

	b.WriteString("## Trend Summary\n\n")
	b.WriteString("| Metric | Slope (per run) | Direction |\n")
	b.WriteString("|--------|----------------|----------|\n")
	writeTrendRow(&b, "Health (Overall)", r.HealthSlope, r.HealthTrend)
	writeTrendRow(&b, "Health (Storage)", r.StorageSlope, r.StorageTrend)
	writeTrendRow(&b, "Health (Replication)", r.ReplicationSlope, r.ReplicationTrend)
	writeTrendRow(&b, "Health (Cluster)", r.ClusterSlope, r.ClusterTrend)
	writeTrendRow(&b, "Basin Depth", r.BasinDepthSlope, r.BasinDepthTrend)
	writeTrendRow(&b, "L0 Peak", r.L0PeakSlope, "")
	fmt.Fprintf(&b, "| Oscillation | — | %s |\n", r.OscillationTrend)
	b.WriteString("\n")

	// === Evolution Gate section ===
	b.WriteString("## Evolution Gate\n\n")
	fmt.Fprintf(&b, "**Status**: %s\n\n", r.Level)
	if len(r.GateReasons) > 0 {
		b.WriteString("**Reasons**:\n")
		for _, reason := range r.GateReasons {
			fmt.Fprintf(&b, "- %s\n", reason)
		}
		b.WriteString("\n")
	} else {
		b.WriteString("No degradation signals detected.\n\n")
	}

	// Gate-specific metrics
	if r.RunCount >= 3 {
		b.WriteString("**Gate Metrics**:\n")
		fmt.Fprintf(&b, "- Health slope (recent 3 runs): %+.4f\n", r.HealthSlopeRecent)
	}
	if r.RecoveryTimeSlope != 0 {
		fmt.Fprintf(&b, "- Recovery time slope: %+.4f\n", r.RecoveryTimeSlope)
	}
	if r.PersistenceSlope != 0 {
		fmt.Fprintf(&b, "- Persistence slope: %+.4f\n", r.PersistenceSlope)
	}
	if r.RecoveryVelocitySlope != 0 {
		fmt.Fprintf(&b, "- Recovery velocity slope: %+.4f\n", r.RecoveryVelocitySlope)
	}
	b.WriteString("\n")

	if len(r.Warnings) > 0 {
		b.WriteString("## Warnings\n\n")
		for _, w := range r.Warnings {
			fmt.Fprintf(&b, "- WARNING: %s\n", w)
		}
		b.WriteString("\n")
	}

	b.WriteString("## Run History\n\n")
	b.WriteString("| # | Date | Health | Storage | Repl | Cluster | Basin | Depth | L0 Peak | Osc | Deg? | Recov(s) | Persist(s) |\n")
	b.WriteString("|---|------|--------|---------|------|---------|-------|-------|---------|-----|------|----------|------------|\n")
	for i, run := range r.Runs {
		osc := ""
		if run.OscillationDetected {
			osc = "OSC"
		} else if run.LimitCycle {
			osc = "CYC"
		}
		deg := ""
		if run.InDegradation {
			deg = "DEG"
		} else if run.DegradationLevel == "FAIL" {
			deg = "FAIL"
		}
		recStr := "-"
		if run.RecoveryDurationSec > 0 {
			recStr = fmt.Sprintf("%.0f", run.RecoveryDurationSec)
		}
		persStr := "-"
		if run.PersistenceDurationSec > 0 {
			persStr = fmt.Sprintf("%.0f", run.PersistenceDurationSec)
		}
		fmt.Fprintf(&b, "| %d | %s | %.2f | %.2f | %.2f | %.2f | %s | %.2f | %.1f | %s | %s | %s | %s |\n",
			i+1,
			run.Timestamp.Format("01-02"),
			run.HealthOverall,
			run.HealthStorage,
			run.HealthRepl,
			run.HealthCluster,
			run.Basin,
			run.BasinDepth,
			run.L0Peak,
			osc,
			deg,
			recStr,
			persStr,
		)
	}
	b.WriteString("\n")

	return b.String()
}

func writeTrendRow(b *strings.Builder, label string, slope float64, direction string) {
	if direction == "" {
		direction = classifySlope(slope, label)
	}
	fmt.Fprintf(b, "| %s | %+.4f | %s |\n", label, slope, direction)
}

func classifySlope(slope float64, metric string) string {
	if math.Abs(slope) < 0.005 {
		return trendStable
	}
	// Health metrics: positive = improving
	isHealth := strings.Contains(metric, "Health") || strings.Contains(metric, "health")
	if isHealth {
		if slope > 0 {
			return trendImproving
		}
		return trendDegrading
	}
	// Non-health (basin depth, L0): positive = degrading
	if slope > 0 {
		return trendDegrading
	}
	return trendImproving
}

// SaveEvolutionHistory 保存 run 摘要到历史目录（带时间戳的文件名）
func SaveEvolutionHistory(historyDir, name string, summaryJSON []byte, timestamp time.Time) error {
	if err := os.MkdirAll(historyDir, 0755); err != nil {
		return err
	}
	filename := fmt.Sprintf("%s-%s.json", name, timestamp.Format("20060102-150405"))
	path := filepath.Join(historyDir, filename)
	return os.WriteFile(path, summaryJSON, 0644)
}
