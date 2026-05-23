package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/lbp0200/BoltDB/internal/monitor"
)

func main() {
	historyDir := flag.String("dir", "", "path to history directory (e.g. /tmp/soak-data/report/history)")
	prefix := flag.String("prefix", "standalone", "history file prefix (standalone, replication)")
	flag.Parse()

	if *historyDir == "" {
		fmt.Fprintln(os.Stderr, "usage: evolution -dir=<history-dir> [-prefix=<name>]")
		os.Exit(2)
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

	// Check if this is the report dir (no history subdir) or the history dir itself
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
		fmt.Fprintf(os.Stderr, "no history files found for prefix %q in %s\n", *prefix, checkDir)
		os.Exit(0)
	}

	report := monitor.AnalyzeEvolution(runs)
	fmt.Println(report.FormatReport())

	// === Evolution Gate v1: structured output ===
	gateStatus := "PASS"
	exitCode := 0
	switch report.Level {
	case monitor.LevelFail:
		gateStatus = "FAIL"
		exitCode = 1
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

	os.Exit(exitCode)
}
