package monitor

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/lbp0200/BoltDB/internal/logger"
)

const (
	AnomalyBasinDrift             = "basin_drift"
	AnomalyBasinShift             = "basin_shift"
	AnomalyOscillationGrowth      = "oscillation_growth"
	AnomalyHealthDecline          = "health_decline"
	AnomalyConvergenceDegradation = "convergence_degradation"
	AnomalyL0Escalation           = "l0_escalation"
	AnomalyDegradationOnset       = "degradation_onset"
	AnomalyRecoveryGrowth         = "recovery_growth"
	AnomalyPersistenceGrowth      = "persistence_growth"
	AnomalyGoroutineLeak          = "goroutine_leak"
)

type Anomaly struct {
	Type      string  `json:"type"`
	Severity  string  `json:"severity"`
	FirstSeen string  `json:"first_seen,omitempty"`
	Commit    string  `json:"commit,omitempty"`
	Message   string  `json:"message"`
	Metric    string  `json:"metric,omitempty"`
	Value     float64 `json:"value,omitempty"`
	Threshold float64 `json:"threshold,omitempty"`
	Slope     float64 `json:"slope,omitempty"`
}

type AnomalyReport struct {
	Version    int       `json:"version"`
	Timestamp  time.Time `json:"timestamp"`
	Prefix     string    `json:"prefix"`
	RunCount   int       `json:"run_count"`
	SpanDays   float64   `json:"span_days,omitempty"`
	Stable     bool      `json:"stable"`
	Confidence string    `json:"confidence"`
	Anomalies  []Anomaly `json:"anomalies,omitempty"`
}

func DetectAnomalies(runs []EvolutionRun, prefix, repoDir string) AnomalyReport {
	r := AnomalyReport{
		Version:    1,
		Timestamp:  time.Now(),
		Prefix:     prefix,
		RunCount:   len(runs),
		Confidence: "high",
		Stable:     true,
	}
	if len(runs) < 2 {
		r.Confidence = "low"
		return r
	}

	r.SpanDays = runs[len(runs)-1].Timestamp.Sub(runs[0].Timestamp).Hours() / 24

	var anomalies []Anomaly
	anomalies = append(anomalies, detectBasinDrift(runs)...)
	anomalies = append(anomalies, detectBasinShift(runs)...)
	anomalies = append(anomalies, detectOscillationGrowth(runs)...)
	anomalies = append(anomalies, detectHealthDecline(runs)...)
	anomalies = append(anomalies, detectConvergenceDegradation(runs)...)
	anomalies = append(anomalies, detectL0Escalation(runs)...)
	anomalies = append(anomalies, detectDegradationOnset(runs)...)
	anomalies = append(anomalies, detectRecoveryGrowth(runs)...)
	anomalies = append(anomalies, detectPersistenceGrowth(runs)...)
	anomalies = append(anomalies, detectGoroutineLeak(runs)...)

	severe := 0
	for _, a := range anomalies {
		if a.Severity == "high" || a.Severity == "critical" {
			severe++
		}
	}
	if len(anomalies) == 0 {
		r.Stable = true
		r.Confidence = "high"
	} else if severe > 0 {
		r.Stable = false
		r.Confidence = "high"
	} else {
		r.Stable = false
		r.Confidence = "medium"
	}

	if repoDir != "" && len(anomalies) > 0 {
		correlateCommits(&r, runs, repoDir)
	}

	r.Anomalies = anomalies
	return r
}

func (r AnomalyReport) FormatAnomalyReport() string {
	var b strings.Builder

	fmt.Fprintf(&b, "# Evolution Anomaly Report\n\n")
	fmt.Fprintf(&b, "- **Prefix**: %s\n", r.Prefix)
	fmt.Fprintf(&b, "- **Runs Analyzed**: %d\n", r.RunCount)
	if r.SpanDays > 0 {
		fmt.Fprintf(&b, "- **Time Span**: %.1f days\n", r.SpanDays)
	}
	fmt.Fprintf(&b, "- **Status**: %s\n", map[bool]string{true: "STABLE", false: "UNSTABLE"}[r.Stable])
	fmt.Fprintf(&b, "- **Confidence**: %s\n\n", r.Confidence)

	if len(r.Anomalies) == 0 {
		b.WriteString("No anomalies detected.\n")
		return b.String()
	}

	b.WriteString("## Anomalies\n\n")
	for i, a := range r.Anomalies {
		fmt.Fprintf(&b, "### %d. %s [%s]\n", i+1, a.Type, strings.ToUpper(a.Severity))
		fmt.Fprintf(&b, "- **Message**: %s\n", a.Message)
		if a.FirstSeen != "" {
			fmt.Fprintf(&b, "- **First Seen**: %s\n", a.FirstSeen)
		}
		if a.Commit != "" {
			fmt.Fprintf(&b, "- **Commit**: %s\n", a.Commit)
		}
		if a.Metric != "" {
			extra := fmt.Sprintf(" (value=%.4f", a.Value)
			if a.Slope != 0 {
				extra += fmt.Sprintf(", slope=%+.4f", a.Slope)
			}
			if a.Threshold > 0 {
				extra += fmt.Sprintf(", threshold=%.4f", a.Threshold)
			}
			extra += ")"
			fmt.Fprintf(&b, "- **Metric**: %s%s\n", a.Metric, extra)
		}
		b.WriteString("\n")
	}

	return b.String()
}

func (r AnomalyReport) FormatAnomalyJSON() ([]byte, error) {
	return json.MarshalIndent(r, "", "  ")
}

func detectBasinDrift(runs []EvolutionRun) []Anomaly {
	if len(runs) < 3 {
		return nil
	}
	vals := extractFloat(runs[len(runs)-3:], func(v EvolutionRun) float64 { return v.BasinDepth })
	slope := linearSlope(vals)
	if math.Abs(slope) > 0.02 {
		severity := "medium"
		threshold := 0.02
		if math.Abs(slope) > 0.05 {
			severity = "high"
			threshold = 0.02
		}
		var msg string
		if slope > 0 {
			msg = "basin depth increasing at " + fmt.Sprintf("%+.4f/run over last 3 runs (shallowing)", slope)
		} else {
			msg = "basin depth declining at " + fmt.Sprintf("%+.4f/run over last 3 runs (deepening)", slope)
		}
		return []Anomaly{{
			Type:      AnomalyBasinDrift,
			Severity:  severity,
			Message:   msg,
			Metric:    "basin_depth",
			Slope:     slope,
			Threshold: threshold,
		}}
	}
	return nil
}

func detectBasinShift(runs []EvolutionRun) []Anomaly {
	if len(runs) < 2 {
		return nil
	}
	last := runs[len(runs)-1]
	prev := runs[len(runs)-2]
	order := map[string]int{"healthy": 0, "stressed": 1, "degraded": 2, "collapsed": 3}
	lo, lok := order[last.Basin]
	po, pok := order[prev.Basin]
	if lok && pok && lo > po {
		severity := "high"
		if lo-po >= 2 {
			severity = "critical"
		}
		return []Anomaly{{
			Type:     AnomalyBasinShift,
			Severity: severity,
			Message:  fmt.Sprintf("basin shifted from %s to %s", prev.Basin, last.Basin),
			Metric:   "basin",
		}}
	}
	return nil
}

func detectOscillationGrowth(runs []EvolutionRun) []Anomaly {
	if len(runs) < 3 {
		return nil
	}
	recent := runs[len(runs)-3:]
	oscCount := 0
	for _, r := range recent {
		if r.OscillationDetected || r.LimitCycle {
			oscCount++
		}
	}
	if oscCount >= 2 {
		return []Anomaly{{
			Type:     AnomalyOscillationGrowth,
			Severity: "high",
			Message:  fmt.Sprintf("oscillation detected in %d of last 3 runs", oscCount),
			Metric:   "oscillation_detected",
		}}
	}
	all := runs
	oscTotal := 0
	for _, r := range all {
		if r.OscillationDetected || r.LimitCycle {
			oscTotal++
		}
	}
	if oscTotal > len(all)/2 {
		return []Anomaly{{
			Type:     AnomalyOscillationGrowth,
			Severity: "high",
			Message:  fmt.Sprintf("oscillation present in majority of runs (%d/%d)", oscTotal, len(all)),
			Metric:   "oscillation_detected",
		}}
	}
	return nil
}

func detectHealthDecline(runs []EvolutionRun) []Anomaly {
	if len(runs) < 3 {
		return nil
	}
	vals := extractFloat(runs[len(runs)-3:], func(v EvolutionRun) float64 { return v.HealthOverall })
	slope := linearSlope(vals)

	var anomalies []Anomaly
	if slope < -0.01 {
		severity := "medium"
		threshold := 0.01
		if slope < -0.02 {
			severity = "high"
		}
		anomalies = append(anomalies, Anomaly{
			Type:      AnomalyHealthDecline,
			Severity:  severity,
			Message:   fmt.Sprintf("health declining at %+.4f/run over last 3 runs", slope),
			Metric:    "health_overall",
			Slope:     slope,
			Threshold: threshold,
		})
	}

	last := runs[len(runs)-1]
	prev := runs[len(runs)-2]
	if last.HealthOverall-prev.HealthOverall < -0.1 {
		anomalies = append(anomalies, Anomaly{
			Type:     AnomalyHealthDecline,
			Severity: "high",
			Message:  fmt.Sprintf("health dropped %.2f since last run", prev.HealthOverall-last.HealthOverall),
			Metric:   "health_overall",
			Value:    last.HealthOverall,
		})
	}

	return anomalies
}

func detectConvergenceDegradation(runs []EvolutionRun) []Anomaly {
	if len(runs) < 3 {
		return nil
	}
	var anomalies []Anomaly

	last := runs[len(runs)-1]
	prev := runs[len(runs)-2]
	if prev.Converging && !last.Converging {
		anomalies = append(anomalies, Anomaly{
			Type:     AnomalyConvergenceDegradation,
			Severity: "high",
			Message:  "system stopped converging (converging: true → false)",
			Metric:   "converging",
		})
	}

	vals := extractFloat(runs[len(runs)-3:], func(v EvolutionRun) float64 { return v.ConvergenceProb })
	if hasNonZero(vals) {
		slope := linearSlope(vals)
		if slope < -0.05 {
			anomalies = append(anomalies, Anomaly{
				Type:      AnomalyConvergenceDegradation,
				Severity:  "medium",
				Message:   fmt.Sprintf("convergence probability declining at %+.4f/run", slope),
				Metric:    "convergence_prob",
				Slope:     slope,
				Threshold: 0.05,
			})
		}
	}

	return anomalies
}

func detectL0Escalation(runs []EvolutionRun) []Anomaly {
	if len(runs) < 3 {
		return nil
	}
	vals := extractFloat(runs[len(runs)-3:], func(v EvolutionRun) float64 { return v.L0Peak })
	slope := linearSlope(vals)
	if slope > 0.5 {
		severity := "medium"
		if slope > 1.0 {
			severity = "high"
		}
		return []Anomaly{{
			Type:      AnomalyL0Escalation,
			Severity:  severity,
			Message:   fmt.Sprintf("L0 peak rising at %+.2f/run over last 3 runs", slope),
			Metric:    "l0_peak",
			Slope:     slope,
			Threshold: 0.5,
		}}
	}
	return nil
}

func detectDegradationOnset(runs []EvolutionRun) []Anomaly {
	if len(runs) < 2 {
		return nil
	}
	last := runs[len(runs)-1]
	prev := runs[len(runs)-2]
	if !prev.InDegradation && last.InDegradation {
		return []Anomaly{{
			Type:     AnomalyDegradationOnset,
			Severity: "critical",
			Message:  fmt.Sprintf("degradation onset: system entered degradation (level=%s)", last.DegradationLevel),
			Metric:   "in_degradation",
		}}
	}
	return nil
}

func detectRecoveryGrowth(runs []EvolutionRun) []Anomaly {
	if len(runs) < 3 {
		return nil
	}
	vals := extractFloat(runs[len(runs)-3:], func(v EvolutionRun) float64 { return v.RecoveryDurationSec })
	if !hasNonZero(vals) {
		return nil
	}
	slope := linearSlope(vals)
	if slope > 5.0 {
		severity := "medium"
		if slope > 15.0 {
			severity = "high"
		}
		return []Anomaly{{
			Type:      AnomalyRecoveryGrowth,
			Severity:  severity,
			Message:   fmt.Sprintf("recovery duration increasing at %+.1fs/run", slope),
			Metric:    "recovery_duration_sec",
			Slope:     slope,
			Threshold: 5.0,
		}}
	}
	return nil
}

func detectPersistenceGrowth(runs []EvolutionRun) []Anomaly {
	if len(runs) < 3 {
		return nil
	}
	vals := extractFloat(runs[len(runs)-3:], func(v EvolutionRun) float64 { return v.PersistenceDurationSec })
	if !hasNonZero(vals) {
		return nil
	}
	slope := linearSlope(vals)
	if slope > 5.0 {
		severity := "medium"
		if slope > 15.0 {
			severity = "high"
		}
		return []Anomaly{{
			Type:      AnomalyPersistenceGrowth,
			Severity:  severity,
			Message:   fmt.Sprintf("degradation persistence increasing at %+.1fs/run", slope),
			Metric:    "persistence_duration_sec",
			Slope:     slope,
			Threshold: 5.0,
		}}
	}
	return nil
}

func detectGoroutineLeak(runs []EvolutionRun) []Anomaly {
	if len(runs) < 3 {
		return nil
	}
	recent := runs[len(runs)-3:]
	positiveDelta := 0
	for _, r := range recent {
		if r.GoroutineDelta > 0 {
			positiveDelta++
		}
	}
	if positiveDelta >= 2 {
		return []Anomaly{{
			Type:     AnomalyGoroutineLeak,
			Severity: "medium",
			Message:  fmt.Sprintf("goroutine delta positive in %d of last 3 runs", positiveDelta),
			Metric:   "goroutine_delta",
		}}
	}
	return nil
}

func hasNonZero(vals []float64) bool {
	for _, v := range vals {
		if v > 0 {
			return true
		}
	}
	return false
}

func correlateCommits(r *AnomalyReport, runs []EvolutionRun, repoDir string) {
	if len(runs) < 2 {
		return
	}

	start := runs[len(runs)-2].Timestamp
	end := runs[len(runs)-1].Timestamp

	commits := getCommitsInWindow(repoDir, start, end)
	if len(commits) == 0 {
		commits = getCommitsInWindow(repoDir, runs[0].Timestamp, end)
	}
	if len(commits) == 0 {
		return
	}

	commitMap := make(map[string]CommitInfo)
	for _, c := range commits {
		commitMap[c.Hash[:7]] = c
	}

	hashes := make([]string, 0, len(commitMap))
	for h := range commitMap {
		hashes = append(hashes, h)
	}
	sort.Slice(hashes, func(i, j int) bool {
		return commitMap[hashes[i]].When.After(commitMap[hashes[j]].When)
	})

	for i := range r.Anomalies {
		a := &r.Anomalies[i]
		if len(hashes) > 0 {
			a.Commit = hashes[0]
		}

		switch a.Type {
		case AnomalyBasinShift, AnomalyDegradationOnset, AnomalyOscillationGrowth:
			ts := findTransitionRun(runs, a.Type)
			if !ts.IsZero() {
				a.FirstSeen = ts.Format("2006-01-02")
				for _, c := range commits {
					if !c.When.Before(ts.Add(-2*time.Hour)) && !c.When.After(ts.Add(2*time.Hour)) {
						a.Commit = c.Hash[:7]
						break
					}
				}
			}
		default:
			a.FirstSeen = runs[0].Timestamp.Format("2006-01-02")
		}
	}
}

type CommitInfo struct {
	Hash string
	When time.Time
}

func getCommitsInWindow(repoDir string, since, until time.Time) []CommitInfo {
	cmd := exec.Command("git", "log",
		"--oneline",
		"--format=%H %ct",
		"--since="+since.Format(time.RFC3339),
		"--until="+until.Format(time.RFC3339),
	)
	cmd.Dir = repoDir
	out, err := cmd.Output()
	if err != nil {
		return nil
	}

	var commits []CommitInfo
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, " ", 2)
		if len(parts) != 2 {
			continue
		}
		var ts int64
		if _, err := fmt.Sscanf(parts[1], "%d", &ts); err != nil {
			continue
		}
		commits = append(commits, CommitInfo{
			Hash: parts[0],
			When: time.Unix(ts, 0),
		})
	}
	sort.Slice(commits, func(i, j int) bool {
		return commits[i].When.After(commits[j].When)
	})
	return commits
}

func findTransitionRun(runs []EvolutionRun, anomalyType string) time.Time {
	for i := len(runs) - 1; i >= 1; i-- {
		prev := runs[i-1]
		cur := runs[i]
		switch anomalyType {
		case AnomalyBasinShift:
			order := map[string]int{"healthy": 0, "stressed": 1, "degraded": 2, "collapsed": 3}
			po, pok := order[prev.Basin]
			co, cok := order[cur.Basin]
			if pok && cok && co > po {
				return cur.Timestamp
			}
		case AnomalyDegradationOnset:
			if !prev.InDegradation && cur.InDegradation {
				return cur.Timestamp
			}
		case AnomalyOscillationGrowth:
			if !prev.OscillationDetected && cur.OscillationDetected {
				return cur.Timestamp
			}
		}
	}
	return time.Time{}
}

func SaveAnomalyReport(dir, prefix string, report AnomalyReport) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return
	}

	jsonData, err := report.FormatAnomalyJSON()
	if err != nil {
		return
	}

	jpath := filepath.Join(dir, prefix+"-anomaly.json")
	if err := os.WriteFile(jpath, jsonData, 0644); err != nil {
		logger.Warning("保存异常JSON报告失败: %v", err)
	}

	mdPath := filepath.Join(dir, prefix+"-anomaly.md")
	if err := os.WriteFile(mdPath, []byte(report.FormatAnomalyReport()), 0644); err != nil {
		logger.Warning("保存异常Markdown报告失败: %v", err)
	}
}
