package monitor

import (
	"math"
	"strings"
	"testing"
	"time"
)

func makeRun(converging bool, prob float64) EvolutionRun {
	return EvolutionRun{
		Timestamp:       time.Now(),
		Converging:      converging,
		ConvergenceProb: prob,
	}
}

// health_slope < -0.02 + high convergence prob → FAIL → WARN
func TestApplyConvergenceSuppression_HealthSlopeSuppressed(t *testing.T) {
	r := EvolutionReport{
		Level:             LevelFail,
		GateReasons:       []string{"health slope in last 3 runs: -0.0300 (threshold: -0.02)"},
		HealthSlopeRecent: -0.03,
		Runs: []EvolutionRun{
			makeRun(true, 0.8),
			makeRun(true, 0.85),
			makeRun(true, 0.9),
		},
	}
	r = applyConvergenceSuppression(r)

	if r.Level != LevelWarn {
		t.Errorf("expected LevelWarn after suppression, got %s", r.Level)
	}
	if !r.SuppressedByConvergence {
		t.Error("expected SuppressedByConvergence=true")
	}
	if r.AvgConvergenceProb <= 0.7 {
		t.Errorf("expected AvgConvergenceProb > 0.7, got %.2f", r.AvgConvergenceProb)
	}

	hasSuppressionReason := false
	for _, reason := range r.GateReasons {
		if strings.Contains(reason, "fail suppressed") {
			hasSuppressionReason = true
			break
		}
	}
	if !hasSuppressionReason {
		t.Error("expected suppression reason in GateReasons")
	}
}

// sustained_oscillation + high convergence prob → FAIL → WARN
func TestApplyConvergenceSuppression_OscillationSuppressed(t *testing.T) {
	r := EvolutionReport{
		Level:            LevelFail,
		GateReasons:      []string{"sustained oscillation pattern detected across runs"},
		OscillationTrend: trendDegrading,
		Runs: []EvolutionRun{
			makeRun(true, 0.85),
			makeRun(true, 0.8),
			makeRun(true, 0.9),
		},
	}
	r = applyConvergenceSuppression(r)

	if r.Level != LevelWarn {
		t.Errorf("expected LevelWarn after suppression, got %s", r.Level)
	}
	if !r.SuppressedByConvergence {
		t.Error("expected SuppressedByConvergence=true")
	}
}

// regime_shift_to_worse + high convergence prob → still FAIL
func TestApplyConvergenceSuppression_RegimeShiftNeverSuppressed(t *testing.T) {
	r := EvolutionReport{
		Level:              LevelFail,
		GateReasons:        []string{"regime shift to worse basin state (sustained)"},
		RegimeShiftToWorse: true,
		Runs: []EvolutionRun{
			makeRun(true, 0.9),
			makeRun(true, 0.85),
			makeRun(true, 0.95),
		},
	}
	r = applyConvergenceSuppression(r)

	if r.Level != LevelFail {
		t.Errorf("expected LevelFail (regime shift never suppressed), got %s", r.Level)
	}
	if r.SuppressedByConvergence {
		t.Error("expected SuppressedByConvergence=false for regime shift")
	}
}

// evolving_degradation + high convergence prob → still FAIL
func TestApplyConvergenceSuppression_EscalatingDegradationNeverSuppressed(t *testing.T) {
	r := EvolutionReport{
		Level:                 LevelFail,
		GateReasons:           []string{"degradation escalating across runs"},
		EscalatingDegradation: true,
		Runs: []EvolutionRun{
			makeRun(true, 0.8),
			makeRun(true, 0.85),
			makeRun(true, 0.9),
		},
	}
	r = applyConvergenceSuppression(r)

	if r.Level != LevelFail {
		t.Errorf("expected LevelFail (escalating degradation never suppressed), got %s", r.Level)
	}
	if r.SuppressedByConvergence {
		t.Error("expected SuppressedByConvergence=false for escalating degradation")
	}
}

// regime shift + health slope (mixed): regime shift dominates → FAIL preserved
func TestApplyConvergenceSuppression_MixedConditionsRegimeShiftDominates(t *testing.T) {
	r := EvolutionReport{
		Level:              LevelFail,
		GateReasons: []string{
			"regime shift to worse basin state (sustained)",
			"health slope in last 3 runs: -0.0300 (threshold: -0.02)",
		},
		RegimeShiftToWorse: true,
		HealthSlopeRecent:  -0.03,
		Runs: []EvolutionRun{
			makeRun(true, 0.9),
			makeRun(true, 0.85),
			makeRun(true, 0.95),
		},
	}
	r = applyConvergenceSuppression(r)

	if r.Level != LevelFail {
		t.Errorf("expected LevelFail (regime shift dominates), got %s", r.Level)
	}
	if r.SuppressedByConvergence {
		t.Error("expected SuppressedByConvergence=false when regime shift present")
	}
}

// not enough convergence signal → FAIL not suppressed
func TestApplyConvergenceSuppression_NotEnoughConvergence(t *testing.T) {
	r := EvolutionReport{
		Level:             LevelFail,
		GateReasons:       []string{"health slope in last 3 runs: -0.0300 (threshold: -0.02)"},
		HealthSlopeRecent: -0.03,
		Runs: []EvolutionRun{
			makeRun(true, 0.8),
			makeRun(false, 0.2),
			makeRun(false, 0.3),
		},
	}
	r = applyConvergenceSuppression(r)

	if r.Level != LevelFail {
		t.Errorf("expected LevelFail (not enough convergence), got %s", r.Level)
	}
	if r.SuppressedByConvergence {
		t.Error("expected SuppressedByConvergence=false for insufficient convergence")
	}
}

// single run → convergingCount=1 < threshold(2) → no suppression
func TestApplyConvergenceSuppression_SingleRunNoSuppression(t *testing.T) {
	r := EvolutionReport{
		Level:             LevelFail,
		GateReasons:       []string{"health slope in last 3 runs: -0.0300 (threshold: -0.02)"},
		HealthSlopeRecent: -0.03,
		Runs: []EvolutionRun{
			makeRun(true, 0.8),
		},
	}
	r = applyConvergenceSuppression(r)

	if r.Level != LevelFail {
		t.Errorf("expected LevelFail (single run), got %s", r.Level)
	}
	if r.SuppressedByConvergence {
		t.Error("expected SuppressedByConvergence=false for single run")
	}
}

// not LevelFail → no-op, suppression is a no-op
func TestApplyConvergenceSuppression_WarnLevelNoOp(t *testing.T) {
	r := EvolutionReport{
		Level:       LevelWarn,
		GateReasons: []string{"health dropped for 3 consecutive runs"},
		Runs: []EvolutionRun{
			makeRun(true, 0.8),
			makeRun(true, 0.85),
			makeRun(true, 0.9),
		},
	}
	r = applyConvergenceSuppression(r)

	if r.Level != LevelWarn {
		t.Errorf("expected LevelWarn unchanged, got %s", r.Level)
	}
	if r.SuppressedByConvergence {
		t.Error("expected SuppressedByConvergence=false for LevelWarn")
	}
}

// no runs → no-op
func TestApplyConvergenceSuppression_NoRunsNoOp(t *testing.T) {
	r := EvolutionReport{
		Level: LevelFail,
	}
	r = applyConvergenceSuppression(r)

	if r.Level != LevelFail {
		t.Errorf("expected LevelFail unchanged, got %s", r.Level)
	}
}

func TestClassifyDirection(t *testing.T) {
	tests := []struct {
		slope    float64
		isHealth bool
		want     string
	}{
		{0.0, true, trendStable},
		{0.001, true, trendStable},
		{0.01, true, trendImproving},
		{-0.01, true, trendDegrading},
		{0.01, false, trendDegrading},
		{-0.01, false, trendImproving},
	}
	for _, tt := range tests {
		got := classifyDirection(tt.slope, tt.isHealth)
		if got != tt.want {
			t.Errorf("classifyDirection(%v, %v) = %s, want %s", tt.slope, tt.isHealth, got, tt.want)
		}
	}
}

func TestFixTrendDirection(t *testing.T) {
	tests := []struct {
		initial  string
		slope    float64
		isHealth bool
		want     string
	}{
		{"", 0.0, true, trendStable},
		{"", 0.001, true, trendStable},
		{"", 0.01, true, trendImproving},
		{"", -0.01, true, trendDegrading},
		{"", 0.01, false, trendDegrading},
		{"", -0.01, false, trendImproving},
	}
	for _, tt := range tests {
		trend := tt.initial
		fixTrendDirection(&trend, tt.slope, tt.isHealth)
		if trend != tt.want {
			t.Errorf("fixTrendDirection({%v}, %v, %v) = %s, want %s", tt.initial, tt.slope, tt.isHealth, trend, tt.want)
		}
	}
}

func TestExtractFloat(t *testing.T) {
	runs := []EvolutionRun{
		{HealthOverall: 0.5},
		{HealthOverall: 0.8},
		{HealthOverall: 0.3},
	}
	vals := extractFloat(runs, func(r EvolutionRun) float64 { return r.HealthOverall })
	if len(vals) != 3 {
		t.Fatalf("expected 3 values, got %d", len(vals))
	}
	if math.Abs(vals[0]-0.5) > 1e-9 || math.Abs(vals[1]-0.8) > 1e-9 || math.Abs(vals[2]-0.3) > 1e-9 {
		t.Errorf("unexpected values: %v", vals)
	}
}

func TestComputeRecentSlope_FewRuns(t *testing.T) {
	runs := []EvolutionRun{{HealthOverall: 0.5}}
	slope := computeRecentSlope(runs, func(r EvolutionRun) float64 { return r.HealthOverall })
	if slope != 0 {
		t.Errorf("expected 0 for single run, got %f", slope)
	}
}

func TestComputeRecentSlope_TwoRuns(t *testing.T) {
	runs := []EvolutionRun{
		{HealthOverall: 0.5},
		{HealthOverall: 0.7},
	}
	slope := computeRecentSlope(runs, func(r EvolutionRun) float64 { return r.HealthOverall })
	if slope <= 0 {
		t.Errorf("expected positive slope, got %f", slope)
	}
}

func TestComputeRecentSlope_ThreeRuns(t *testing.T) {
	runs := []EvolutionRun{
		{HealthOverall: 0.9},
		{HealthOverall: 0.8},
		{HealthOverall: 0.7},
		{HealthOverall: 0.6},
		{HealthOverall: 0.5},
	}
	slope := computeRecentSlope(runs, func(r EvolutionRun) float64 { return r.HealthOverall })
	if slope >= 0 {
		t.Errorf("expected negative slope, got %f", slope)
	}
}

func TestComputeSparseSlope_FewNonZero(t *testing.T) {
	runs := []EvolutionRun{{HealthOverall: 0.0}, {HealthOverall: 0.5}, {HealthOverall: 0.7}}
	slope := computeSparseSlope(runs, func(r EvolutionRun) float64 { return r.HealthOverall })
	if slope <= 0 {
		t.Errorf("expected positive slope, got %f", slope)
	}
}

func TestComputeSparseSlope_AllZero(t *testing.T) {
	runs := []EvolutionRun{{HealthOverall: 0.0}, {HealthOverall: 0.0}}
	slope := computeSparseSlope(runs, func(r EvolutionRun) float64 { return r.HealthOverall })
	if slope != 0 {
		t.Errorf("expected 0 for all zero, got %f", slope)
	}
}

func TestComputeSparseSlope_SingleNonZero(t *testing.T) {
	runs := []EvolutionRun{{HealthOverall: 0.0}, {HealthOverall: 0.0}, {HealthOverall: 0.5}}
	slope := computeSparseSlope(runs, func(r EvolutionRun) float64 { return r.HealthOverall })
	if slope != 0 {
		t.Errorf("expected 0 for single non-zero, got %f", slope)
	}
}

func TestClassifySlope(t *testing.T) {
	tests := []struct {
		slope  float64
		metric string
		want   string
	}{
		{0.0, "health", trendStable},
		{0.003, "health", trendStable},
		{0.01, "HealthOverall", trendImproving},
		{-0.01, "HealthOverall", trendDegrading},
		{0.01, "L0Final", trendDegrading},
		{-0.01, "L0Final", trendImproving},
		{0.01, "basin_depth", trendDegrading},
	}
	for _, tt := range tests {
		got := classifySlope(tt.slope, tt.metric)
		if got != tt.want {
			t.Errorf("classifySlope(%v, %s) = %s, want %s", tt.slope, tt.metric, got, tt.want)
		}
	}
}
