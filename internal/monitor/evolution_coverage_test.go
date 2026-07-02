package monitor

import (
	"strings"
	"testing"
	"time"
)

func TestDetectRegimeShiftToWorse(t *testing.T) {
	tests := []struct {
		name string
		runs []EvolutionRun
		want bool
	}{
		{"less than 3 runs", []EvolutionRun{{}, {}}, false},
		{"no shift", []EvolutionRun{
			{Basin: "healthy"}, {Basin: "healthy"}, {Basin: "healthy"},
		}, false},
		{"shift but not persistent",
			[]EvolutionRun{
				{Basin: "healthy"},
				{Basin: "stressed"},
				{Basin: "healthy"},
			}, false},
		{"shift to stressed and stays",
			[]EvolutionRun{
				{Basin: "healthy"},
				{Basin: "stressed"},
				{Basin: "stressed"},
			}, true},
		{"shift to degraded and stays",
			[]EvolutionRun{
				{Basin: "stressed"},
				{Basin: "degraded"},
				{Basin: "degraded"},
			}, true},
		{"unknown basin in between",
			[]EvolutionRun{
				{Basin: "healthy"},
				{Basin: "unknown"},
				{Basin: "stressed"},
			}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := detectRegimeShiftToWorse(tt.runs)
			if got != tt.want {
				t.Errorf("detectRegimeShiftToWorse() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDetectOscillationTrend(t *testing.T) {
	tests := []struct {
		name string
		runs []EvolutionRun
		want string
	}{
		{"less than 3 runs", []EvolutionRun{{LimitCycle: true}}, trendStable},
		{"no oscillation", []EvolutionRun{
			{LimitCycle: false}, {LimitCycle: false}, {LimitCycle: false},
		}, trendStable},
		{"mixed (>0.25)", []EvolutionRun{
			{LimitCycle: true}, {LimitCycle: false}, {LimitCycle: false},
		}, "mixed"},
		{"majority (>0.5)", []EvolutionRun{
			{LimitCycle: true}, {LimitCycle: true}, {LimitCycle: false},
		}, trendDegrading},
		{"all oscillating", []EvolutionRun{
			{LimitCycle: true}, {LimitCycle: true}, {LimitCycle: true},
		}, trendDegrading},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := detectOscillationTrend(tt.runs)
			if got != tt.want {
				t.Errorf("detectOscillationTrend() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestDetectRegimeShift(t *testing.T) {
	tests := []struct {
		name     string
		runs     []EvolutionRun
		want     bool
		wantDesc string
	}{
		{"less than 3 runs", []EvolutionRun{{}, {}}, false, ""},
		{"no shift", []EvolutionRun{
			{Basin: "healthy"}, {Basin: "healthy"}, {Basin: "healthy"},
		}, false, ""},
		{"shift and sticks",
			[]EvolutionRun{
				{Basin: "healthy"},
				{Basin: "stressed"},
				{Basin: "stressed"},
			}, true, "basin regime shift at run #2: healthy → stressed (persistent)"},
		{"shift but doesn't stick",
			[]EvolutionRun{
				{Basin: "healthy"},
				{Basin: "stressed"},
				{Basin: "healthy"},
			}, false, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, desc := detectRegimeShift(tt.runs)
			if got != tt.want {
				t.Errorf("detectRegimeShift() = %v, want %v", got, tt.want)
			}
			if desc != tt.wantDesc {
				t.Errorf("detectRegimeShift() desc = %q, want %q", desc, tt.wantDesc)
			}
		})
	}
}

func TestDetectEscalation(t *testing.T) {
	tests := []struct {
		name string
		runs []EvolutionRun
		want bool
	}{
		{"less than 3 runs", []EvolutionRun{{}, {}}, false},
		{"no degradation",
			[]EvolutionRun{
				{HealthOverall: 0.85, InDegradation: false},
				{HealthOverall: 0.84, InDegradation: false},
				{HealthOverall: 0.83, InDegradation: false},
			}, false},
		{"majority in degradation",
			[]EvolutionRun{
				{HealthOverall: 0.9, InDegradation: false},
				{HealthOverall: 0.5, InDegradation: true},
				{HealthOverall: 0.4, InDegradation: true},
			}, true},
		{"health drop > 0.1",
			[]EvolutionRun{
				{HealthOverall: 0.95, InDegradation: false},
				{HealthOverall: 0.80, InDegradation: false},
				{HealthOverall: 0.80, InDegradation: false},
			}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := detectEscalation(tt.runs)
			if got != tt.want {
				t.Errorf("detectEscalation() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBuildWarnings(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name string
		runs []EvolutionRun
		rpt  EvolutionReport
		chk  func([]string) bool
	}{
		{"regime shift detected",
			nil,
			EvolutionReport{RegimeShiftDetected: true, RegimeShiftDescription: "regime shift"},
			func(w []string) bool { return containsStr(strings.Join(w, " "), "regime shift") },
		},
		{"escalating degradation",
			nil,
			EvolutionReport{EscalatingDegradation: true},
			func(w []string) bool { return containsStr(strings.Join(w, " "), "escalating") },
		},
		{"health drop > 0.1",
			[]EvolutionRun{
				{Timestamp: now.Add(-2 * time.Hour), HealthOverall: 0.9},
				{Timestamp: now.Add(-1 * time.Hour), HealthOverall: 0.7},
			},
			EvolutionReport{},
			func(w []string) bool {
				return containsStr(strings.Join(w, " "), "health dropped")
			},
		},
		{"basin depth increase > 0.2",
			[]EvolutionRun{
				{Timestamp: now.Add(-2 * time.Hour), BasinDepth: 0.5},
				{Timestamp: now.Add(-1 * time.Hour), BasinDepth: 0.8},
			},
			EvolutionReport{},
			func(w []string) bool {
				return containsStr(strings.Join(w, " "), "basin depth")
			},
		},
		{"single run no warnings",
			[]EvolutionRun{{Timestamp: now}},
			EvolutionReport{},
			func(w []string) bool { return len(w) == 0 },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildWarnings(tt.runs, tt.rpt)
			if !tt.chk(got) {
				t.Errorf("buildWarnings() = %v, expected condition not met", got)
			}
		})
	}
}

func TestComputeEvolutionLevel(t *testing.T) {
	t.Run("run count < 3", func(t *testing.T) {
		r := EvolutionReport{RunCount: 2}
		level, reasons := computeEvolutionLevel(r)
		if level != LevelOK {
			t.Errorf("expected LevelOK, got %s", level)
		}
		if reasons != nil {
			t.Errorf("expected nil reasons, got %v", reasons)
		}
	})

	t.Run("health slope recent < -0.02 fails", func(t *testing.T) {
		r := EvolutionReport{
			RunCount:          3,
			HealthSlopeRecent: -0.03,
			Runs:              []EvolutionRun{{}, {}, {}},
		}
		level, reasons := computeEvolutionLevel(r)
		if level != LevelFail {
			t.Errorf("expected LevelFail, got %s", level)
		}
		if !containsStr(strings.Join(reasons, " "), "health slope") {
			t.Errorf("expected health slope reason, got %v", reasons)
		}
	})

	t.Run("regime shift to worse fails", func(t *testing.T) {
		r := EvolutionReport{
			RunCount:           3,
			RegimeShiftToWorse: true,
			Runs:               []EvolutionRun{{}, {}, {}},
		}
		level, reasons := computeEvolutionLevel(r)
		if level != LevelFail {
			t.Errorf("expected LevelFail, got %s", level)
		}
		if !containsStr(strings.Join(reasons, " "), "regime shift") {
			t.Errorf("expected regime shift reason, got %v", reasons)
		}
	})

	t.Run("oscillation degrading fails", func(t *testing.T) {
		r := EvolutionReport{
			RunCount:         3,
			OscillationTrend: trendDegrading,
			Runs:             []EvolutionRun{{}, {}, {}},
		}
		level, reasons := computeEvolutionLevel(r)
		if level != LevelFail {
			t.Errorf("expected LevelFail, got %s", level)
		}
		if !containsStr(strings.Join(reasons, " "), "oscillation") {
			t.Errorf("expected oscillation reason, got %v", reasons)
		}
	})

	t.Run("health dropping 3 consecutive runs warns", func(t *testing.T) {
		now := time.Now()
		r := EvolutionReport{
			RunCount: 4,
			Runs: []EvolutionRun{
				{Timestamp: now.Add(-3 * time.Hour), HealthOverall: 0.9},
				{Timestamp: now.Add(-2 * time.Hour), HealthOverall: 0.8},
				{Timestamp: now.Add(-1 * time.Hour), HealthOverall: 0.7},
				{Timestamp: now, HealthOverall: 0.6},
			},
		}
		level, reasons := computeEvolutionLevel(r)
		if level != LevelWarn {
			t.Errorf("expected LevelWarn, got %s", level)
		}
		if !containsStr(strings.Join(reasons, " "), "3 consecutive") {
			t.Errorf("expected consecutive drop reason, got %v", reasons)
		}
	})

	t.Run("2+ dimensions degrading warns", func(t *testing.T) {
		r := EvolutionReport{
			RunCount:         3,
			StorageTrend:     trendDegrading,
			ReplicationTrend: trendDegrading,
			Runs:             []EvolutionRun{{}, {}, {}},
		}
		level, reasons := computeEvolutionLevel(r)
		if level != LevelWarn {
			t.Errorf("expected LevelWarn, got %s", level)
		}
		if !containsStr(strings.Join(reasons, " "), "dimensions") {
			t.Errorf("expected dimensions reason, got %v", reasons)
		}
	})

	t.Run("persistence degrading warns", func(t *testing.T) {
		r := EvolutionReport{
			RunCount:         3,
			PersistenceTrend: trendDegrading,
			PersistenceSlope: 0.01,
			Runs:             []EvolutionRun{{}, {}, {}},
		}
		level, reasons := computeEvolutionLevel(r)
		if level != LevelWarn {
			t.Errorf("expected LevelWarn, got %s", level)
		}
		if !containsStr(strings.Join(reasons, " "), "persistence") {
			t.Errorf("expected persistence reason, got %v", reasons)
		}
	})

	t.Run("recovery time degrading warns", func(t *testing.T) {
		r := EvolutionReport{
			RunCount:          3,
			RecoveryTimeTrend: trendDegrading,
			RecoveryTimeSlope: 0.01,
			Runs:              []EvolutionRun{{}, {}, {}},
		}
		level, reasons := computeEvolutionLevel(r)
		if level != LevelWarn {
			t.Errorf("expected LevelWarn, got %s", level)
		}
		if !containsStr(strings.Join(reasons, " "), "recovery time") {
			t.Errorf("expected recovery time reason, got %v", reasons)
		}
	})

	t.Run("escalating degradation warns", func(t *testing.T) {
		r := EvolutionReport{
			RunCount:              3,
			EscalatingDegradation: true,
			Runs:                  []EvolutionRun{{}, {}, {}},
		}
		level, reasons := computeEvolutionLevel(r)
		if level != LevelWarn {
			t.Errorf("expected LevelWarn, got %s", level)
		}
		if !containsStr(strings.Join(reasons, " "), "escalating") {
			t.Errorf("expected escalating reason, got %v", reasons)
		}
	})

	t.Run("no conditions triggers", func(t *testing.T) {
		r := EvolutionReport{
			RunCount: 3,
			Runs:     []EvolutionRun{{}, {}, {}},
		}
		level, reasons := computeEvolutionLevel(r)
		if level != LevelOK {
			t.Errorf("expected LevelOK, got %s", level)
		}
		if reasons != nil {
			t.Errorf("expected nil reasons, got %v", reasons)
		}
	})
}

func TestAnalyzeEvolution(t *testing.T) {
	t.Run("less than 2 runs", func(t *testing.T) {
		runs := []EvolutionRun{{Timestamp: time.Now()}}
		r := AnalyzeEvolution(runs)
		if r.HealthTrend != trendStable {
			t.Errorf("expected stable trend, got %s", r.HealthTrend)
		}
		if r.RunCount != 1 {
			t.Errorf("expected RunCount=1, got %d", r.RunCount)
		}
	})

	t.Run("improving health", func(t *testing.T) {
		now := time.Now()
		runs := []EvolutionRun{
			{Timestamp: now.Add(-48 * time.Hour), HealthOverall: 0.5, HealthStorage: 0.5, HealthRepl: 0.5, HealthCluster: 0.5, BasinDepth: 0.5, L0Peak: 5},
			{Timestamp: now.Add(-24 * time.Hour), HealthOverall: 0.7, HealthStorage: 0.7, HealthRepl: 0.7, HealthCluster: 0.7, BasinDepth: 0.4, L0Peak: 4},
			{Timestamp: now, HealthOverall: 0.9, HealthStorage: 0.9, HealthRepl: 0.9, HealthCluster: 0.9, BasinDepth: 0.3, L0Peak: 3},
		}
		r := AnalyzeEvolution(runs)
		if r.RunCount != 3 {
			t.Errorf("expected RunCount=3, got %d", r.RunCount)
		}
		if r.HealthTrend != trendImproving {
			t.Errorf("expected improving health, got %s", r.HealthTrend)
		}
		if r.StorageTrend != trendImproving {
			t.Errorf("expected improving storage, got %s", r.StorageTrend)
		}
		if r.BasinDepthTrend != trendImproving {
			t.Errorf("expected improving basin depth (lower = better), got %s", r.BasinDepthTrend)
		}
		if r.SpanDays <= 0 {
			t.Errorf("expected positive SpanDays, got %f", r.SpanDays)
		}
	})

	t.Run("degrading health triggers warnings", func(t *testing.T) {
		now := time.Now()
		runs := []EvolutionRun{
			{Timestamp: now.Add(-72 * time.Hour), HealthOverall: 0.9, HealthStorage: 0.9, HealthRepl: 0.9, HealthCluster: 0.9, BasinDepth: 0.2, L0Peak: 2},
			{Timestamp: now.Add(-48 * time.Hour), HealthOverall: 0.5, HealthStorage: 0.5, HealthRepl: 0.5, HealthCluster: 0.5, BasinDepth: 0.6, L0Peak: 10, LimitCycle: false, InDegradation: true},
			{Timestamp: now.Add(-24 * time.Hour), HealthOverall: 0.4, HealthStorage: 0.4, HealthRepl: 0.4, HealthCluster: 0.4, BasinDepth: 0.7, L0Peak: 15, LimitCycle: false, InDegradation: true},
			{Timestamp: now, HealthOverall: 0.3, HealthStorage: 0.3, HealthRepl: 0.3, HealthCluster: 0.3, BasinDepth: 0.8, L0Peak: 20, LimitCycle: false, InDegradation: true},
		}
		r := AnalyzeEvolution(runs)
		if r.HealthTrend != trendDegrading {
			t.Errorf("expected degrading health, got %s", r.HealthTrend)
		}
		if r.GateReasons == nil {
			t.Error("expected gate reasons for degrading health")
		}
	})

	t.Run("healthy system no warnings", func(t *testing.T) {
		now := time.Now()
		runs := []EvolutionRun{
			{Timestamp: now.Add(-48 * time.Hour), HealthOverall: 0.95, HealthStorage: 0.95, HealthRepl: 0.95, HealthCluster: 0.95, BasinDepth: 0.1, L0Peak: 1},
			{Timestamp: now.Add(-24 * time.Hour), HealthOverall: 0.96, HealthStorage: 0.96, HealthRepl: 0.96, HealthCluster: 0.96, BasinDepth: 0.1, L0Peak: 1},
			{Timestamp: now, HealthOverall: 0.97, HealthStorage: 0.97, HealthRepl: 0.97, HealthCluster: 0.97, BasinDepth: 0.09, L0Peak: 1},
		}
		r := AnalyzeEvolution(runs)
		if r.Level != LevelOK {
			t.Errorf("expected LevelOK, got %s %v", r.Level, r.GateReasons)
		}
	})

	t.Run("regime shift and escalation", func(t *testing.T) {
		now := time.Now()
		runs := []EvolutionRun{
			{Timestamp: now.Add(-72 * time.Hour), HealthOverall: 0.9, Basin: "healthy", HealthStorage: 0.9, HealthRepl: 0.9, HealthCluster: 0.9, BasinDepth: 0.2, L0Peak: 2},
			{Timestamp: now.Add(-48 * time.Hour), HealthOverall: 0.5, Basin: "stressed", HealthStorage: 0.5, HealthRepl: 0.5, HealthCluster: 0.5, BasinDepth: 0.6, L0Peak: 10, InDegradation: true},
			{Timestamp: now.Add(-24 * time.Hour), HealthOverall: 0.4, Basin: "stressed", HealthStorage: 0.4, HealthRepl: 0.4, HealthCluster: 0.4, BasinDepth: 0.7, L0Peak: 15, InDegradation: true},
			{Timestamp: now, HealthOverall: 0.3, Basin: "stressed", HealthStorage: 0.3, HealthRepl: 0.3, HealthCluster: 0.3, BasinDepth: 0.8, L0Peak: 20, InDegradation: true},
		}
		r := AnalyzeEvolution(runs)
		if !r.RegimeShiftToWorse {
			t.Error("expected RegimeShiftToWorse")
		}
		if !r.RegimeShiftDetected {
			t.Error("expected RegimeShiftDetected")
		}
	})

	t.Run("recovery and persistence data", func(t *testing.T) {
		now := time.Now()
		runs := []EvolutionRun{
			{Timestamp: now.Add(-48 * time.Hour), HealthOverall: 0.9, RecoveryDurationSec: 10, PersistenceDurationSec: 5, RecoveryVelocity: 0.5, HealthStorage: 0.9, HealthRepl: 0.9, HealthCluster: 0.9, BasinDepth: 0.2, L0Peak: 2},
			{Timestamp: now.Add(-24 * time.Hour), HealthOverall: 0.8, RecoveryDurationSec: 20, PersistenceDurationSec: 10, RecoveryVelocity: 0.3, HealthStorage: 0.8, HealthRepl: 0.8, HealthCluster: 0.8, BasinDepth: 0.3, L0Peak: 3},
			{Timestamp: now, HealthOverall: 0.7, RecoveryDurationSec: 30, PersistenceDurationSec: 15, RecoveryVelocity: 0.2, HealthStorage: 0.7, HealthRepl: 0.7, HealthCluster: 0.7, BasinDepth: 0.4, L0Peak: 4},
		}
		r := AnalyzeEvolution(runs)
		if r.RecoveryTimeSlope <= 0 {
			t.Errorf("expected positive recovery time slope, got %f", r.RecoveryTimeSlope)
		}
		if r.PersistenceSlope <= 0 {
			t.Errorf("expected positive persistence slope, got %f", r.PersistenceSlope)
		}
		if r.RecoveryVelocitySlope >= 0 {
			t.Errorf("expected negative recovery velocity slope, got %f", r.RecoveryVelocitySlope)
		}
	})

	t.Run("oscillation trend in runs", func(t *testing.T) {
		now := time.Now()
		runs := []EvolutionRun{
			{Timestamp: now.Add(-48 * time.Hour), HealthOverall: 0.9, LimitCycle: true, HealthStorage: 0.9, HealthRepl: 0.9, HealthCluster: 0.9, BasinDepth: 0.2, L0Peak: 2},
			{Timestamp: now.Add(-24 * time.Hour), HealthOverall: 0.8, LimitCycle: true, HealthStorage: 0.8, HealthRepl: 0.8, HealthCluster: 0.8, BasinDepth: 0.3, L0Peak: 3},
			{Timestamp: now, HealthOverall: 0.7, LimitCycle: true, HealthStorage: 0.7, HealthRepl: 0.7, HealthCluster: 0.7, BasinDepth: 0.4, L0Peak: 4},
		}
		r := AnalyzeEvolution(runs)
		if r.OscillationTrend != trendDegrading {
			t.Errorf("expected degrading oscillation, got %s", r.OscillationTrend)
		}
	})
}

func TestComputeDrift(t *testing.T) {
	t.Run("less than 2 runs", func(t *testing.T) {
		r := EvolutionReport{RunCount: 1}
		d := r.ComputeDrift(3)
		if d.RunCount != 1 {
			t.Errorf("expected RunCount=1, got %d", d.RunCount)
		}
		if len(d.Metrics) != 0 {
			t.Errorf("expected empty metrics, got %d", len(d.Metrics))
		}
	})

	t.Run("worsening drift", func(t *testing.T) {
		now := time.Now()
		r := EvolutionReport{
			RunCount: 5,
			Runs: []EvolutionRun{
				{Timestamp: now.Add(-4 * time.Hour), HealthOverall: 0.95, HealthStorage: 0.95, HealthRepl: 0.95, HealthCluster: 0.95, BasinDepth: 0.1, L0Peak: 2, Basin: "healthy"},
				{Timestamp: now.Add(-3 * time.Hour), HealthOverall: 0.94, HealthStorage: 0.94, HealthRepl: 0.94, HealthCluster: 0.94, BasinDepth: 0.11, L0Peak: 2, Basin: "healthy"},
				{Timestamp: now.Add(-2 * time.Hour), HealthOverall: 0.93, HealthStorage: 0.93, HealthRepl: 0.93, HealthCluster: 0.93, BasinDepth: 0.12, L0Peak: 3, Basin: "healthy"},
				{Timestamp: now.Add(-1 * time.Hour), HealthOverall: 0.92, HealthStorage: 0.92, HealthRepl: 0.92, HealthCluster: 0.92, BasinDepth: 0.13, L0Peak: 3, Basin: "healthy"},
				{Timestamp: now, HealthOverall: 0.88, HealthStorage: 0.88, HealthRepl: 0.88, HealthCluster: 0.88, BasinDepth: 0.15, L0Peak: 4, Basin: "healthy"},
			},
		}
		d := r.ComputeDrift(3)
		if len(d.Metrics) != 6 {
			t.Fatalf("expected 6 metrics, got %d", len(d.Metrics))
		}
		if d.BasinDrift == "" {
			t.Error("expected non-empty BasinDrift")
		}
	})

	t.Run("stable drift no warnings", func(t *testing.T) {
		now := time.Now()
		r := EvolutionReport{
			RunCount: 4,
			Runs: []EvolutionRun{
				{Timestamp: now.Add(-3 * time.Hour), HealthOverall: 0.95, HealthStorage: 0.95, HealthRepl: 0.95, HealthCluster: 0.95, BasinDepth: 0.1, L0Peak: 2, Basin: "healthy"},
				{Timestamp: now.Add(-2 * time.Hour), HealthOverall: 0.95, HealthStorage: 0.95, HealthRepl: 0.95, HealthCluster: 0.95, BasinDepth: 0.1, L0Peak: 2, Basin: "healthy"},
				{Timestamp: now.Add(-1 * time.Hour), HealthOverall: 0.94, HealthStorage: 0.94, HealthRepl: 0.94, HealthCluster: 0.94, BasinDepth: 0.11, L0Peak: 2, Basin: "healthy"},
				{Timestamp: now, HealthOverall: 0.96, HealthStorage: 0.96, HealthRepl: 0.96, HealthCluster: 0.96, BasinDepth: 0.09, L0Peak: 2, Basin: "healthy"},
			},
		}
		d := r.ComputeDrift(3)
		allStable := true
		for _, m := range d.Metrics {
			if m.Signal == "WARN" {
				allStable = false
				break
			}
		}
		if !allStable {
			t.Errorf("expected all stable signals, got: %+v", d.Metrics)
		}
	})

	t.Run("basin worsening", func(t *testing.T) {
		now := time.Now()
		r := EvolutionReport{
			RunCount: 4,
			Runs: []EvolutionRun{
				{Timestamp: now.Add(-3 * time.Hour), HealthOverall: 0.95, HealthStorage: 0.95, HealthRepl: 0.95, HealthCluster: 0.95, BasinDepth: 0.1, L0Peak: 2, Basin: "healthy"},
				{Timestamp: now.Add(-2 * time.Hour), HealthOverall: 0.94, HealthStorage: 0.94, HealthRepl: 0.94, HealthCluster: 0.94, BasinDepth: 0.11, L0Peak: 2, Basin: "healthy"},
				{Timestamp: now.Add(-1 * time.Hour), HealthOverall: 0.93, HealthStorage: 0.93, HealthRepl: 0.93, HealthCluster: 0.93, BasinDepth: 0.12, L0Peak: 3, Basin: "healthy"},
				{Timestamp: now, HealthOverall: 0.92, HealthStorage: 0.92, HealthRepl: 0.92, HealthCluster: 0.92, BasinDepth: 0.13, L0Peak: 3, Basin: "stressed"},
			},
		}
		d := r.ComputeDrift(3)
		if !d.BasinWorsening {
			t.Error("expected BasinWorsening=true")
		}
	})

	t.Run("escalation and regime shift propagated", func(t *testing.T) {
		r := EvolutionReport{
			RunCount:              4,
			EscalatingDegradation: true,
			RegimeShiftDetected:   true,
			Runs: []EvolutionRun{
				{HealthOverall: 0.9, HealthStorage: 0.9, HealthRepl: 0.9, HealthCluster: 0.9, BasinDepth: 0.2, L0Peak: 2, Basin: "healthy"},
				{HealthOverall: 0.8, HealthStorage: 0.8, HealthRepl: 0.8, HealthCluster: 0.8, BasinDepth: 0.3, L0Peak: 3, Basin: "stressed"},
				{HealthOverall: 0.7, HealthStorage: 0.7, HealthRepl: 0.7, HealthCluster: 0.7, BasinDepth: 0.4, L0Peak: 4, Basin: "stressed"},
				{HealthOverall: 0.6, HealthStorage: 0.6, HealthRepl: 0.6, HealthCluster: 0.6, BasinDepth: 0.5, L0Peak: 5, Basin: "stressed"},
			},
		}
		d := r.ComputeDrift(3)
		if !d.Escalation {
			t.Error("expected Escalation=true")
		}
		if !d.RegimeShift {
			t.Error("expected RegimeShift=true")
		}
	})
}

func TestFormatEvolutionReport(t *testing.T) {
	t.Run("insufficient data", func(t *testing.T) {
		r := EvolutionReport{
			AnalysisTime: time.Now(),
			RunCount:     1,
			Runs: []EvolutionRun{
				{Timestamp: time.Now(), HealthOverall: 0.9},
			},
			SpanDays: 0,
		}
		report := r.FormatReport()
		if !containsStr(report, "Insufficient data") {
			t.Errorf("expected insufficient data message, got:\n%s", report)
		}
	})

	t.Run("passing gate", func(t *testing.T) {
		now := time.Now()
		r := EvolutionReport{
			AnalysisTime: time.Now(),
			RunCount:     3,
			Runs: []EvolutionRun{
				{Timestamp: now.Add(-48 * time.Hour), HealthOverall: 0.9, HealthStorage: 0.9, HealthRepl: 0.9, HealthCluster: 0.9, BasinDepth: 0.2, L0Peak: 2, Basin: "healthy"},
				{Timestamp: now.Add(-24 * time.Hour), HealthOverall: 0.95, HealthStorage: 0.95, HealthRepl: 0.95, HealthCluster: 0.95, BasinDepth: 0.15, L0Peak: 1, Basin: "healthy"},
				{Timestamp: now, HealthOverall: 0.97, HealthStorage: 0.97, HealthRepl: 0.97, HealthCluster: 0.97, BasinDepth: 0.1, L0Peak: 1, Basin: "healthy"},
			},
			HealthSlope:      0.035,
			StorageSlope:     0.035,
			ReplicationSlope: 0.035,
			ClusterSlope:     0.035,
			BasinDepthSlope:  -0.05,
			L0PeakSlope:      -0.5,
			HealthTrend:      trendImproving,
			StorageTrend:     trendImproving,
			ReplicationTrend: trendImproving,
			ClusterTrend:     trendImproving,
			BasinDepthTrend:  trendImproving,
			OscillationTrend: trendStable,
			Level:            LevelOK,
			SpanDays:         2.0,
		}
		report := r.FormatReport()
		if !containsStr(report, "No degradation signals detected") {
			t.Errorf("expected no degradation signals, got:\n%s", report)
		}
		if !containsStr(report, "Health (Overall)") {
			t.Errorf("expected trend summary table, got:\n%s", report)
		}
	})

	t.Run("failing gate with reasons", func(t *testing.T) {
		now := time.Now()
		r := EvolutionReport{
			AnalysisTime:      time.Now(),
			RunCount:          3,
			Level:             LevelFail,
			GateReasons:       []string{"health slope in last 3 runs: -0.0300 (threshold: -0.02)"},
			HealthSlopeRecent: -0.03,
			Runs: []EvolutionRun{
				{Timestamp: now.Add(-48 * time.Hour), HealthOverall: 0.9, HealthStorage: 0.9, HealthRepl: 0.9, HealthCluster: 0.9, BasinDepth: 0.2, L0Peak: 2, Basin: "healthy"},
				{Timestamp: now.Add(-24 * time.Hour), HealthOverall: 0.85, HealthStorage: 0.85, HealthRepl: 0.85, HealthCluster: 0.85, BasinDepth: 0.25, L0Peak: 3, Basin: "healthy"},
				{Timestamp: now, HealthOverall: 0.8, HealthStorage: 0.8, HealthRepl: 0.8, HealthCluster: 0.8, BasinDepth: 0.3, L0Peak: 4, Basin: "healthy"},
			},
			SpanDays: 2.0,
		}
		report := r.FormatReport()
		if !containsStr(report, "Evolution Gate: FAIL") {
			t.Errorf("expected FAIL gate, got:\n%s", report)
		}
		if !containsStr(report, "health slope") {
			t.Errorf("expected gate reasons in report, got:\n%s", report)
		}
	})

	t.Run("suppressed convergence", func(t *testing.T) {
		now := time.Now()
		r := EvolutionReport{
			AnalysisTime:            time.Now(),
			RunCount:                3,
			Level:                   LevelWarn,
			SuppressedByConvergence: true,
			AvgConvergenceProb:      0.85,
			GateReasons:             []string{"fail suppressed: transient, system converging (prob=0.85)"},
			Runs: []EvolutionRun{
				{Timestamp: now.Add(-48 * time.Hour), HealthOverall: 0.9, HealthStorage: 0.9, HealthRepl: 0.9, HealthCluster: 0.9, BasinDepth: 0.2, L0Peak: 2, Basin: "healthy"},
				{Timestamp: now.Add(-24 * time.Hour), HealthOverall: 0.85, HealthStorage: 0.85, HealthRepl: 0.85, HealthCluster: 0.85, BasinDepth: 0.25, L0Peak: 3, Basin: "healthy"},
				{Timestamp: now, HealthOverall: 0.82, HealthStorage: 0.82, HealthRepl: 0.82, HealthCluster: 0.82, BasinDepth: 0.28, L0Peak: 3, Basin: "healthy"},
			},
			SpanDays: 2.0,
		}
		report := r.FormatReport()
		if !containsStr(report, "False Positive Suppression") {
			t.Errorf("expected suppression mention, got:\n%s", report)
		}
	})

	t.Run("warnings section", func(t *testing.T) {
		now := time.Now()
		r := EvolutionReport{
			AnalysisTime: time.Now(),
			RunCount:     4,
			Runs: []EvolutionRun{
				{Timestamp: now.Add(-72 * time.Hour), HealthOverall: 0.9, HealthStorage: 0.9, HealthRepl: 0.9, HealthCluster: 0.9, BasinDepth: 0.2, L0Peak: 2, Basin: "healthy"},
				{Timestamp: now.Add(-48 * time.Hour), HealthOverall: 0.85, HealthStorage: 0.85, HealthRepl: 0.85, HealthCluster: 0.85, BasinDepth: 0.25, L0Peak: 3, Basin: "healthy"},
				{Timestamp: now.Add(-24 * time.Hour), HealthOverall: 0.8, HealthStorage: 0.8, HealthRepl: 0.8, HealthCluster: 0.8, BasinDepth: 0.3, L0Peak: 4, Basin: "healthy"},
				{Timestamp: now, HealthOverall: 0.75, HealthStorage: 0.75, HealthRepl: 0.75, HealthCluster: 0.75, BasinDepth: 0.35, L0Peak: 5, Basin: "healthy"},
			},
			SpanDays: 3.0,
			Warnings: []string{"health dropped 0.05 since last run"},
		}
		report := r.FormatReport()
		if !containsStr(report, "## Warnings") {
			t.Errorf("expected warnings section, got:\n%s", report)
		}
		if !containsStr(report, "health dropped") {
			t.Errorf("expected health drop warning, got:\n%s", report)
		}
	})

	t.Run("oscillation and limit cycle display", func(t *testing.T) {
		now := time.Now()
		r := EvolutionReport{
			AnalysisTime: time.Now(),
			RunCount:     3,
			Runs: []EvolutionRun{
				{Timestamp: now.Add(-48 * time.Hour), HealthOverall: 0.9, HealthStorage: 0.9, HealthRepl: 0.9, HealthCluster: 0.9, BasinDepth: 0.2, L0Peak: 2, Basin: "healthy", OscillationDetected: true},
				{Timestamp: now.Add(-24 * time.Hour), HealthOverall: 0.85, HealthStorage: 0.85, HealthRepl: 0.85, HealthCluster: 0.85, BasinDepth: 0.25, L0Peak: 3, Basin: "healthy", LimitCycle: true},
				{Timestamp: now, HealthOverall: 0.8, HealthStorage: 0.8, HealthRepl: 0.8, HealthCluster: 0.8, BasinDepth: 0.3, L0Peak: 4, Basin: "healthy", Converging: true, ConvergenceProb: 0.8, RecoveryDurationSec: 60, PersistenceDurationSec: 30},
			},
			SpanDays: 2.0,
		}
		report := r.FormatReport()
		if !containsStr(report, "OSC") {
			t.Errorf("expected OSC marker, got:\n%s", report)
		}
		if !containsStr(report, "CYC") {
			t.Errorf("expected CYC marker, got:\n%s", report)
		}
		if !containsStr(report, "80%") {
			t.Errorf("expected convergence percentage, got:\n%s", report)
		}
	})
}

func TestWriteTrendRow(t *testing.T) {
	t.Run("with direction", func(t *testing.T) {
		var b strings.Builder
		writeTrendRow(&b, "Health (Overall)", 0.035, "improving")
		got := b.String()
		if !containsStr(got, "Health (Overall)") {
			t.Errorf("expected label in output, got: %s", got)
		}
		if !containsStr(got, "+0.0350") {
			t.Errorf("expected formatted slope in output, got: %s", got)
		}
		if !containsStr(got, "improving") {
			t.Errorf("expected direction in output, got: %s", got)
		}
	})

	t.Run("without direction uses classifySlope", func(t *testing.T) {
		var b strings.Builder
		writeTrendRow(&b, "L0 Peak", -0.5, "")
		got := b.String()
		if !containsStr(got, "L0 Peak") {
			t.Errorf("expected label in output, got: %s", got)
		}
	})
}

func TestFindTransitionRun(t *testing.T) {
	now := time.Now()
	t.Run("basin shift", func(t *testing.T) {
		runs := []EvolutionRun{
			{Timestamp: now.Add(-2 * time.Hour), Basin: "healthy"},
			{Timestamp: now.Add(-1 * time.Hour), Basin: "stressed"},
			{Timestamp: now, Basin: "degraded"},
		}
		ts := findTransitionRun(runs, AnomalyBasinShift)
		if ts.IsZero() {
			t.Fatal("expected non-zero timestamp for basin shift")
		}
		if !ts.Equal(now) {
			t.Errorf("expected transition at now, got %v", ts)
		}
	})

	t.Run("degradation onset", func(t *testing.T) {
		runs := []EvolutionRun{
			{Timestamp: now.Add(-2 * time.Hour), InDegradation: false},
			{Timestamp: now.Add(-1 * time.Hour), InDegradation: false},
			{Timestamp: now, InDegradation: true},
		}
		ts := findTransitionRun(runs, AnomalyDegradationOnset)
		if ts.IsZero() {
			t.Fatal("expected non-zero timestamp for degradation onset")
		}
		if !ts.Equal(now) {
			t.Errorf("expected transition at now, got %v", ts)
		}
	})

	t.Run("oscillation growth", func(t *testing.T) {
		runs := []EvolutionRun{
			{Timestamp: now.Add(-2 * time.Hour), OscillationDetected: false},
			{Timestamp: now.Add(-1 * time.Hour), OscillationDetected: false},
			{Timestamp: now, OscillationDetected: true},
		}
		ts := findTransitionRun(runs, AnomalyOscillationGrowth)
		if ts.IsZero() {
			t.Fatal("expected non-zero timestamp for oscillation growth")
		}
		if !ts.Equal(now) {
			t.Errorf("expected transition at now, got %v", ts)
		}
	})

	t.Run("no match returns zero", func(t *testing.T) {
		runs := []EvolutionRun{
			{Timestamp: now.Add(-2 * time.Hour), Basin: "healthy"},
			{Timestamp: now.Add(-1 * time.Hour), Basin: "healthy"},
			{Timestamp: now, Basin: "healthy"},
		}
		ts := findTransitionRun(runs, AnomalyBasinShift)
		if !ts.IsZero() {
			t.Errorf("expected zero timestamp, got %v", ts)
		}
	})

	t.Run("empty runs returns zero", func(t *testing.T) {
		ts := findTransitionRun(nil, AnomalyBasinShift)
		if !ts.IsZero() {
			t.Errorf("expected zero timestamp, got %v", ts)
		}
	})

	t.Run("unknown anomaly type returns zero", func(t *testing.T) {
		runs := []EvolutionRun{
			{Timestamp: now.Add(-2 * time.Hour), Basin: "healthy"},
			{Timestamp: now, Basin: "stressed"},
		}
		ts := findTransitionRun(runs, "unknown_type")
		if !ts.IsZero() {
			t.Errorf("expected zero timestamp for unknown type, got %v", ts)
		}
	})

	t.Run("no upward basin shift returns zero", func(t *testing.T) {
		runs := []EvolutionRun{
			{Timestamp: now.Add(-2 * time.Hour), Basin: "stressed"},
			{Timestamp: now.Add(-1 * time.Hour), Basin: "healthy"},
			{Timestamp: now, Basin: "healthy"},
		}
		ts := findTransitionRun(runs, AnomalyBasinShift)
		if !ts.IsZero() {
			t.Errorf("expected zero timestamp for improving basin, got %v", ts)
		}
	})
}
