package integration

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/lbp0200/BoltDB/internal/monitor"
)

// SoakRunSummary captures key metrics for cross-run comparison and historical tracking.
type SoakRunSummary struct {
	Timestamp           time.Time `json:"ts"`
	Duration            string    `json:"duration"`
	Samples             int       `json:"samples"`
	HealthOverall       float64   `json:"health_overall"`
	HealthStorage       float64   `json:"health_storage"`
	HealthRepl          float64   `json:"health_replication"`
	HealthCluster       float64   `json:"health_cluster"`
	Trajectory          string    `json:"trajectory"`
	Basin               string    `json:"basin"`
	BasinDepth          float64   `json:"basin_depth"`
	L0Final             float64   `json:"l0_final"`
	L0Peak              float64   `json:"l0_peak"`
	L0Velocity          float64   `json:"l0_velocity"`
	L0Acceleration      float64   `json:"l0_acceleration"`
	ActiveRetries       int64     `json:"active_retries"`
	GoroutineDelta      int       `json:"goroutine_delta"`
	GoroutineFailStreak int       `json:"goroutine_fail_streak"`
	GoroutineWarnStreak int       `json:"goroutine_warn_streak"`
	LimitCycle          bool      `json:"limit_cycle"`
	Converging          bool      `json:"converging"`
	ConvergenceTarget   string    `json:"convergence_target,omitempty"`
	InDegradation       bool      `json:"in_degradation"`
	Escapable           bool      `json:"escapable"`
	DegradationLevel    string    `json:"degradation_level"`
}

// saveSoakReport writes a Markdown report + JSON summary to the specified directory.
// Called at the end of soak tests when CI_NIGHTLY_SOAK is set.
//
//nolint:unused,errcheck
func saveSoakReport(dir, name string, pm *PressureMonitor, baseline int, duration time.Duration, degLevel DegradationLevel) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return
	}

	health := pm.HealthScore(baseline)
	temporal := pm.TemporalAnalysis()
	basin := pm.BasinAnalysis()
	latest := pm.Latest()
	samples := pm.Samples()

	def := DefaultDegradationAssertion()
	summary := SoakRunSummary{
		Timestamp:           time.Now(),
		Duration:            duration.String(),
		Samples:             len(samples),
		HealthOverall:       health.Overall,
		HealthStorage:       health.HealthStorage,
		HealthRepl:          health.HealthReplication,
		HealthCluster:       health.HealthCluster,
		Trajectory:          temporal.Trajectory,
		Basin:               basin.CurrentBasin.String(),
		BasinDepth:          basin.Depth,
		L0Final:             latest.LastL0Score,
		L0Velocity:          basin.L0Velocity,
		L0Acceleration:      basin.L0Acceleration,
		ActiveRetries:       latest.ActiveRetries,
		GoroutineDelta:      latest.Goroutines - baseline,
		GoroutineFailStreak: monitor.CountConsecutiveAbove(samples, baseline, def.MaxGoroutineDelta),
		GoroutineWarnStreak: monitor.CountConsecutiveAbove(samples, baseline, def.GoroutineWarnDelta),
		LimitCycle:          basin.LimitCycle,
		Converging:          basin.Converging,
		InDegradation:       basin.InDegradation,
		Escapable:           basin.Escapable,
		DegradationLevel:    degLevel.String(),
	}

	var peak float64
	for _, s := range samples {
		if s.LastL0Score > peak {
			peak = s.LastL0Score
		}
	}
	summary.L0Peak = peak

	if basin.Converging {
		summary.ConvergenceTarget = basin.ConvergenceTarget.String()
	}

	// JSON summary
	var summaryJSON []byte
	jpath := filepath.Join(dir, name+"-summary.json")
	jf, err := os.Create(jpath)
	if err == nil {
		enc := json.NewEncoder(jf)
		enc.SetIndent("", "  ")
		_ = enc.Encode(summary)
		_ = jf.Close()
		summaryJSON, _ = json.Marshal(summary)
	}

	// Archive timestamped copy to history/ for cross-run evolution analysis
	if len(summaryJSON) > 0 {
		_ = monitor.SaveEvolutionHistory(filepath.Join(dir, "history"), name, summaryJSON, summary.Timestamp)
	}

	rpath := filepath.Join(dir, name+"-report.md")
	rf, err := os.Create(rpath)
	if err != nil {
		return
	}
	defer func() { _ = rf.Close() }()

	fmt.Fprintf(rf, "# Soak Report: %s\n\n", name)
	fmt.Fprintf(rf, "- **Timestamp**: %s\n", summary.Timestamp.Format(time.RFC3339))
	fmt.Fprintf(rf, "- **Duration**: %s\n", duration)
	fmt.Fprintf(rf, "- **Samples**: %d\n\n", len(samples))

	fmt.Fprintf(rf, "## Summary\n\n")
	fmt.Fprintf(rf, "| Metric | Value |\n|---|---|\n")
	fmt.Fprintf(rf, "| Overall Health | %.2f |\n", summary.HealthOverall)
	fmt.Fprintf(rf, "| Degradation Level | %s |\n", summary.DegradationLevel)
	fmt.Fprintf(rf, "| Trajectory | %s |\n", summary.Trajectory)
	fmt.Fprintf(rf, "| Basin | %s (depth=%.2f) |\n", summary.Basin, summary.BasinDepth)
	fmt.Fprintf(rf, "| L0 Final | %.1f |\n", summary.L0Final)
	fmt.Fprintf(rf, "| L0 Peak | %.1f |\n", summary.L0Peak)
	fmt.Fprintf(rf, "| L0 Velocity | %+.4f /s |\n", summary.L0Velocity)
	fmt.Fprintf(rf, "| L0 Acceleration | %+.4f /s² |\n", summary.L0Acceleration)
	fmt.Fprintf(rf, "| Active Retries | %d |\n", summary.ActiveRetries)
	fmt.Fprintf(rf, "| Goroutine Delta (final) | %d |\n", summary.GoroutineDelta)
	fmt.Fprintf(rf, "| Goroutine Fail Streak | %d windows |\n", summary.GoroutineFailStreak)
	fmt.Fprintf(rf, "| Goroutine Warn Streak | %d windows |\n", summary.GoroutineWarnStreak)
	fmt.Fprintf(rf, "| Limit Cycle | %v |\n", summary.LimitCycle)
	fmt.Fprintf(rf, "| Converging | %v", summary.Converging)
	if summary.Converging {
		fmt.Fprintf(rf, " → %s", summary.ConvergenceTarget)
	}
	fmt.Fprintf(rf, " |\n")
	fmt.Fprintf(rf, "| In Degradation | %v |\n", summary.InDegradation)
	fmt.Fprintf(rf, "| Escapable | %v |\n", summary.Escapable)

	fmt.Fprintf(rf, "\n## Health Score\n\n```\n%s\n```\n", health.FormatReport())
	fmt.Fprintf(rf, "\n## Temporal Analysis\n\n```\n%s\n```\n", temporal.FormatReport())
	fmt.Fprintf(rf, "\n## Basin Analysis\n\n```\n%s\n```\n", basin.FormatReport())
}

// saveEvolutionReport loads history and writes evolution analysis markdown.
//
//nolint:unused
func saveEvolutionReport(dir, name string) {
	historyDir := filepath.Join(dir, "history")
	runs, err := monitor.LoadEvolutionHistory(historyDir, name)
	if err != nil || len(runs) < 2 {
		return
	}
	report := monitor.AnalyzeEvolution(runs)

	epath := filepath.Join(dir, name+"-evolution.md")
	ef, err := os.Create(epath)
	if err != nil {
		return
	}
	defer func() { _ = ef.Close() }()
	_, _ = ef.WriteString(report.FormatReport())
}
