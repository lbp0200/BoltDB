package monitor

import (
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
