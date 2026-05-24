package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/lbp0200/BoltDB/internal/monitor"
)

// GateResult is the machine-readable evolution gate output for CI consumption.
type GateResult struct {
	GateVersion int      `json:"gate_version"`
	Timestamp   string   `json:"timestamp"`
	Level       string   `json:"level"`
	Reasons     []string `json:"reasons,omitempty"`
	RunCount    int      `json:"run_count"`
	SpanDays    float64  `json:"span_days,omitempty"`

	HealthSlope       float64 `json:"health_slope_all,omitempty"`
	HealthSlopeRecent float64 `json:"health_slope_recent,omitempty"`
	StorageSlope      float64 `json:"storage_slope,omitempty"`
	ReplSlope         float64 `json:"replication_slope,omitempty"`
	ClusterSlope      float64 `json:"cluster_slope,omitempty"`
	BasinDepthSlope   float64 `json:"basin_depth_slope,omitempty"`
	L0PeakSlope       float64 `json:"l0_peak_slope,omitempty"`

	HealthTrend       string `json:"health_trend,omitempty"`
	StorageTrend      string `json:"storage_trend,omitempty"`
	ReplicationTrend  string `json:"replication_trend,omitempty"`
	ClusterTrend      string `json:"cluster_trend,omitempty"`
	OscillationTrend  string `json:"oscillation_trend,omitempty"`
	PersistenceTrend  string `json:"persistence_trend,omitempty"`
	RecoveryTimeTrend string `json:"recovery_time_trend,omitempty"`

	RecoveryTimeSlope     float64 `json:"recovery_time_slope,omitempty"`
	PersistenceSlope      float64 `json:"persistence_slope,omitempty"`
	RecoveryVelocitySlope float64 `json:"recovery_velocity_slope,omitempty"`

	RegimeShiftDetected   bool     `json:"regime_shift_detected,omitempty"`
	RegimeShiftToWorse    bool     `json:"regime_shift_to_worse,omitempty"`
	EscalatingDegradation bool     `json:"escalating_degradation,omitempty"`
	Warnings              []string `json:"warnings,omitempty"`
}

func buildGateResult(r monitor.EvolutionReport) GateResult {
	g := GateResult{
		GateVersion: 1,
		Timestamp:   time.Now().Format(time.RFC3339),
		Level:       r.Level.String(),
		Reasons:     r.GateReasons,
		RunCount:    r.RunCount,
		SpanDays:    r.SpanDays,
	}
	if r.RunCount < 2 {
		return g
	}
	g.HealthSlope = r.HealthSlope
	g.HealthSlopeRecent = r.HealthSlopeRecent
	g.StorageSlope = r.StorageSlope
	g.ReplSlope = r.ReplicationSlope
	g.ClusterSlope = r.ClusterSlope
	g.BasinDepthSlope = r.BasinDepthSlope
	g.L0PeakSlope = r.L0PeakSlope
	g.HealthTrend = r.HealthTrend
	g.StorageTrend = r.StorageTrend
	g.ReplicationTrend = r.ReplicationTrend
	g.ClusterTrend = r.ClusterTrend
	g.OscillationTrend = r.OscillationTrend
	g.PersistenceTrend = r.PersistenceTrend
	g.RecoveryTimeTrend = r.RecoveryTimeTrend
	g.RecoveryTimeSlope = r.RecoveryTimeSlope
	g.PersistenceSlope = r.PersistenceSlope
	g.RecoveryVelocitySlope = r.RecoveryVelocitySlope
	g.RegimeShiftDetected = r.RegimeShiftDetected
	g.RegimeShiftToWorse = r.RegimeShiftToWorse
	g.EscalatingDegradation = r.EscalatingDegradation
	g.Warnings = r.Warnings
	return g
}

func main() {
	historyDir := flag.String("dir", "", "path to history directory (e.g. /tmp/soak-data/report/history)")
	prefix := flag.String("prefix", "standalone", "history file prefix (standalone, replication)")
	jsonOut := flag.Bool("json", false, "output machine-readable JSON instead of human-readable report")
	driftOut := flag.Bool("drift", false, "output drift report JSON (last run vs trailing window average)")
	anomalyOut := flag.Bool("anomaly", false, "output anomaly detection report (markdown)")
	anomalyJSON := flag.Bool("anomaly-json", false, "output anomaly detection report (JSON)")
	repoDir := flag.String("repo", "", "git repo root for commit correlation (default: auto-detect from CWD)")
	flag.Parse()

	if *historyDir == "" {
		fmt.Fprintln(os.Stderr, "usage: evolution -dir=<history-dir> [-prefix=<name>] [-json] [-drift] [-anomaly] [-anomaly-json]")
		os.Exit(2)
	}

	if *repoDir == "" {
		cwd, _ := os.Getwd()
		*repoDir = cwd
	}

	// If -dir points to the report dir itself, append /history
	info, err := os.Stat(*historyDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: cannot access %s: %v\n", *historyDir, err)
		os.Exit(1)
	}
	if !info.IsDir() {
		fmt.Fprintf(os.Stderr, "error: %s is not a directory\n", *historyDir)
		os.Exit(1)
	}

	checkDir := *historyDir
	if _, err := os.Stat(filepath.Join(*historyDir, "history")); err == nil {
		checkDir = filepath.Join(*historyDir, "history")
	}

	runs, err := monitor.LoadEvolutionHistory(checkDir, *prefix)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	if len(runs) == 0 {
		if *jsonOut {
			g := GateResult{
				GateVersion: 1,
				Timestamp:   time.Now().Format(time.RFC3339),
				Level:       "PASS",
				RunCount:    0,
			}
			_ = json.NewEncoder(os.Stdout).Encode(g)
		} else {
			fmt.Fprintf(os.Stderr, "no history files found for prefix %q in %s\n", *prefix, checkDir)
		}
		os.Exit(0)
	}

	// Anomaly detection (independent of evolution analysis)
	if *anomalyOut || *anomalyJSON {
		ar := monitor.DetectAnomalies(runs, *prefix, *repoDir)
		if *anomalyJSON {
			_ = json.NewEncoder(os.Stdout).Encode(ar)
		}
		if *anomalyOut {
			fmt.Println(ar.FormatAnomalyReport())
		}
		return
	}

	report := monitor.AnalyzeEvolution(runs)

	exitCode := 0
	switch report.Level {
	case monitor.LevelFail:
		exitCode = 1
	}

	if *driftOut {
		drift := report.ComputeDrift(3)
		_ = json.NewEncoder(os.Stdout).Encode(drift)
		return
	}

	if *jsonOut {
		g := buildGateResult(report)
		_ = json.NewEncoder(os.Stdout).Encode(g)
	} else {
		fmt.Println(report.FormatReport())

		gateStatus := "PASS"
		switch report.Level {
		case monitor.LevelFail:
			gateStatus = "FAIL"
		case monitor.LevelWarn:
			gateStatus = "WARN"
		}

		fmt.Println("\n=== EVOLUTION GATE ===")
		fmt.Printf("Status: %s\n", gateStatus)
		if len(report.GateReasons) > 0 {
			fmt.Println("Reasons:")
			for _, reason := range report.GateReasons {
				fmt.Printf("- %s\n", reason)
			}
		}
		fmt.Printf("Level: %s\n", report.Level)
		fmt.Printf("Runs analyzed: %d\n", report.RunCount)
		if report.RunCount >= 2 {
			fmt.Printf("Health slope (all): %+.4f\n", report.HealthSlope)
			fmt.Printf("Health slope (recent 3): %+.4f\n", report.HealthSlopeRecent)
			fmt.Printf("Oscillation trend: %s\n", report.OscillationTrend)
			fmt.Printf("Persistence trend: %s\n", report.PersistenceTrend)
			fmt.Printf("Recovery time trend: %s\n", report.RecoveryTimeTrend)
		}
	}

	os.Exit(exitCode)
}
