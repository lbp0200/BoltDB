package monitor

import (
	"testing"
	"time"
)

func TestClassifyBasin_Healthy(t *testing.T) {
	s := PressureSample{LastL0Score: 3.0, ActiveRetries: 0}
	if b := classifyBasin(s); b != BasinHealthy {
		t.Errorf("expected healthy for L0=3, got %s", b)
	}

	s = PressureSample{LastL0Score: 7.9, ActiveRetries: 2}
	if b := classifyBasin(s); b != BasinHealthy {
		t.Errorf("expected healthy for L0=7.9, got %s", b)
	}

	s = PressureSample{LastL0Score: 0, ActiveRetries: 4}
	if b := classifyBasin(s); b != BasinHealthy {
		t.Errorf("expected healthy for retries=4, got %s", b)
	}
}

func TestClassifyBasin_Stressed(t *testing.T) {
	s := PressureSample{LastL0Score: 8.0, ActiveRetries: 0}
	if b := classifyBasin(s); b != BasinStressed {
		t.Errorf("expected stressed for L0=8, got %s", b)
	}

	s = PressureSample{LastL0Score: 10.0, ActiveRetries: 10}
	if b := classifyBasin(s); b != BasinStressed {
		t.Errorf("expected stressed for L0=10, got %s", b)
	}

	s = PressureSample{LastL0Score: 2.0, ActiveRetries: 5}
	if b := classifyBasin(s); b != BasinStressed {
		t.Errorf("expected stressed for retries=5, got %s", b)
	}

	s = PressureSample{LastL0Score: 19.9, ActiveRetries: 29}
	if b := classifyBasin(s); b != BasinStressed {
		t.Errorf("expected stressed for L0=19.9, retries=29, got %s", b)
	}
}

func TestClassifyBasin_Degraded(t *testing.T) {
	s := PressureSample{LastL0Score: 20.0, ActiveRetries: 0}
	if b := classifyBasin(s); b != BasinDegraded {
		t.Errorf("expected degraded for L0=20, got %s", b)
	}

	s = PressureSample{LastL0Score: 15.0, ActiveRetries: 30}
	if b := classifyBasin(s); b != BasinDegraded {
		t.Errorf("expected degraded for retries=30, got %s", b)
	}

	s = PressureSample{LastL0Score: 24.9, ActiveRetries: 50}
	if b := classifyBasin(s); b != BasinDegraded {
		t.Errorf("expected degraded for L0=24.9, got %s", b)
	}
}

func TestClassifyBasin_Collapsed(t *testing.T) {
	s := PressureSample{LastL0Score: 25.0, ActiveRetries: 0}
	if b := classifyBasin(s); b != BasinCollapsed {
		t.Errorf("expected collapsed for L0=25, got %s", b)
	}

	s = PressureSample{LastL0Score: 10.0, ActiveRetries: 100}
	if b := classifyBasin(s); b != BasinCollapsed {
		t.Errorf("expected collapsed for retries=100, got %s", b)
	}

	s = PressureSample{LastL0Score: 30.0, ActiveRetries: 200}
	if b := classifyBasin(s); b != BasinCollapsed {
		t.Errorf("expected collapsed for L0=30+retries=200, got %s", b)
	}
}

func TestClassifyBasin_Unknown(t *testing.T) {
	// Empty sample (zero values) should be BasinHealthy (L0=0, retries=0)
	s := PressureSample{}
	if b := classifyBasin(s); b != BasinHealthy {
		t.Errorf("expected healthy for zero sample, got %s", b)
	}
}

func TestBasinDepth_Healthy(t *testing.T) {
	tests := []struct {
		l0      float64
		retries int64
		wantMin float64
		wantMax float64
	}{
		{0, 0, 0.99, 1.01},
		{4.0, 0, 0.49, 0.51},
		{7.9, 0, 0.01, 0.02},
	}
	for _, tc := range tests {
		s := PressureSample{LastL0Score: tc.l0, ActiveRetries: tc.retries}
		d := computeBasinDepth(BasinHealthy, s)
		if d < tc.wantMin || d > tc.wantMax {
			t.Errorf("L0=%.1f: expected depth [%.2f,%.2f], got %.4f", tc.l0, tc.wantMin, tc.wantMax, d)
		}
	}
}

func TestBasinDepth_Stressed(t *testing.T) {
	s := PressureSample{LastL0Score: 14.0, ActiveRetries: 0}
	d := computeBasinDepth(BasinStressed, s)
	if d < 0.45 || d > 0.55 {
		t.Errorf("L0=14 (mid-stressed): expected depth ~0.5, got %.4f", d)
	}

	// At stressed boundary, depth should be near 0
	s = PressureSample{LastL0Score: 8.0, ActiveRetries: 0}
	d = computeBasinDepth(BasinStressed, s)
	if d > 0.1 {
		t.Errorf("L0=8 (boundary): expected depth near 0, got %.4f", d)
	}
}

func TestBasinDepth_Degraded(t *testing.T) {
	s := PressureSample{LastL0Score: 22.5, ActiveRetries: 40}
	d := computeBasinDepth(BasinDegraded, s)
	if d < 0.45 || d > 0.55 {
		t.Errorf("L0=22.5 (mid-degraded): expected depth ~0.5, got %.4f", d)
	}

	s = PressureSample{LastL0Score: 20.0, ActiveRetries: 30}
	d = computeBasinDepth(BasinDegraded, s)
	if d > 0.1 {
		t.Errorf("L0=20 (boundary): expected depth near 0, got %.4f", d)
	}
}

func TestDetectTransitions_None(t *testing.T) {
	samples := makePressureSamples([]float64{2, 3, 4, 3, 2}, []int64{0, 0, 0, 0, 0})
	trans := detectTransitions(samples)
	if len(trans) != 0 {
		t.Errorf("expected 0 transitions for healthy-only, got %d", len(trans))
	}
}

func TestDetectTransitions_HealthyToStressed(t *testing.T) {
	samples := makePressureSamples([]float64{3, 4, 5, 8, 10}, []int64{0, 0, 0, 0, 2})
	trans := detectTransitions(samples)
	if len(trans) < 1 {
		t.Fatal("expected >=1 transition")
	}
	if trans[0].From != BasinHealthy || trans[0].To != BasinStressed {
		t.Errorf("expected healthy->stressed, got %s->%s", trans[0].From, trans[0].To)
	}
}

func TestDetectTransitions_StressedToDegraded(t *testing.T) {
	samples := makePressureSamples([]float64{8, 12, 15, 20, 22}, []int64{2, 5, 10, 15, 20})
	trans := detectTransitions(samples)
	if len(trans) < 1 {
		t.Fatal("expected >=1 transition")
	}
	if trans[len(trans)-1].To != BasinDegraded {
		t.Errorf("expected last transition to degraded, got %s", trans[len(trans)-1].To)
	}
}

func TestDetectTransitions_FullCycle(t *testing.T) {
	// healthy -> stressed -> degraded -> stressed -> healthy
	l0s := []float64{2, 4, 8, 12, 18, 22, 20, 15, 10, 8, 4, 2}
	samples := makePressureSamples(l0s, []int64{0, 0, 0, 2, 5, 20, 15, 10, 5, 2, 0, 0})
	trans := detectTransitions(samples)
	if len(trans) < 2 {
		t.Errorf("expected >=2 transitions for full cycle, got %d", len(trans))
	}
}

func TestComputeL0Velocity_Flat(t *testing.T) {
	samples := makePressureSamples([]float64{10, 10, 10, 10, 10}, []int64{0, 0, 0, 0, 0})
	vel := computeL0Velocity(samples)
	if vel > 0.01 || vel < -0.01 {
		t.Errorf("expected ~0 velocity for flat, got %.4f", vel)
	}
}

func TestComputeL0Velocity_Rising(t *testing.T) {
	// L0 rising from 5 to 15 over ~4 intervals at 100ms each = 0.4s
	samples := makePressureSamplesTime(time.Second, []float64{5, 7, 10, 13, 15}, []int64{0, 0, 0, 0, 0})
	vel := computeL0Velocity(samples)
	if vel <= 0 {
		t.Errorf("expected positive velocity for rising L0, got %.4f", vel)
	}
}

func TestComputeL0Velocity_Falling(t *testing.T) {
	samples := makePressureSamplesTime(time.Second, []float64{20, 18, 15, 12, 8}, []int64{0, 0, 0, 0, 0})
	vel := computeL0Velocity(samples)
	if vel >= 0 {
		t.Errorf("expected negative velocity for falling L0, got %.4f", vel)
	}
}

func TestComputeL0Velocity_InsufficientData(t *testing.T) {
	if v := computeL0Velocity(nil); v != 0 {
		t.Errorf("expected 0 for nil, got %.4f", v)
	}
	if v := computeL0Velocity([]PressureSample{{}}); v != 0 {
		t.Errorf("expected 0 for single sample, got %.4f", v)
	}
	if v := computeL0Velocity([]PressureSample{{}, {}}); v != 0 {
		t.Errorf("expected 0 for two samples, got %.4f", v)
	}
}

func TestComputeL0Acceleration_Constant(t *testing.T) {
	samples := makePressureSamplesTime(time.Second, []float64{5, 6, 7, 8, 9, 10, 11}, []int64{0, 0, 0, 0, 0, 0, 0})
	acc := computeL0Acceleration(samples)
	if acc > 0.05 || acc < -0.05 {
		t.Errorf("expected ~0 acceleration for constant rate, got %.6f", acc)
	}
}

func TestComputeL0Acceleration_Positive(t *testing.T) {
	// Accelerating: gaps increase (0.5, 0.5, 0.8, 1.0, 1.2, 1.5, 1.7, 2.0, 2.3, 2.5)
	l0s := []float64{1.0, 1.5, 2.0, 2.8, 3.8, 5.0, 6.5, 8.2, 10.2, 12.5, 15.0}
	samples := makePressureSamplesTime(500*time.Millisecond, l0s, make([]int64, len(l0s)))
	acc := computeL0Acceleration(samples)
	if acc <= 0 {
		t.Errorf("expected positive acceleration for accelerating L0, got %.6f", acc)
	}
}

func TestComputeL0Acceleration_Negative(t *testing.T) {
	// Decelerating rise: L0 still rises but by smaller amounts (gaps: 2, 2, 1.5, 1, 0.8, 0.6, 0.4, 0.3, 0.2)
	l0s := []float64{1.0, 3.0, 5.0, 6.5, 7.5, 8.3, 8.9, 9.3, 9.6, 9.8}
	samples := makePressureSamplesTime(500*time.Millisecond, l0s, make([]int64, len(l0s)))
	acc := computeL0Acceleration(samples)
	if acc >= 0 {
		t.Errorf("expected negative acceleration for decelerating L0, got %.6f", acc)
	}
}

func TestRetryVelocity_Rising(t *testing.T) {
	samples := makePressureSamplesTime(time.Second, []float64{10, 10, 10, 10, 10}, []int64{5, 10, 20, 40, 80})
	vel := computeRetryVelocity(samples)
	if vel <= 0 {
		t.Errorf("expected positive retry velocity, got %.4f", vel)
	}
}

func TestRetryVelocity_Falling(t *testing.T) {
	samples := makePressureSamplesTime(time.Second, []float64{10, 10, 10, 10, 10}, []int64{80, 40, 20, 10, 5})
	vel := computeRetryVelocity(samples)
	if vel >= 0 {
		t.Errorf("expected negative retry velocity, got %.4f", vel)
	}
}

func TestGoroutineSlope(t *testing.T) {
	samples := make([]PressureSample, 5)
	now := time.Now()
	for i := range samples {
		samples[i] = PressureSample{
			Timestamp:  now.Add(time.Duration(i) * time.Second),
			Goroutines: 100 + i*10,
		}
	}
	slope := computeGoroutineSlope(samples)
	if slope <= 0 {
		t.Errorf("expected positive goroutine slope, got %.4f", slope)
	}
}

func TestDetectLimitCycle_No(t *testing.T) {
	samples := makePressureSamplesTime(time.Second, []float64{5, 6, 7, 8, 9, 10}, []int64{0, 0, 0, 0, 0, 0})
	lc, _, _ := detectLimitCycle(samples, 0.1)
	if lc {
		t.Error("expected no limit cycle for monotonic rise")
	}
}

func TestDetectLimitCycle_Damped(t *testing.T) {
	// Damped oscillation: amplitude decreases over time
	l0s := make([]float64, 20)
	for i := range l0s {
		decay := 1.0 - float64(i)/20.0
		l0s[i] = 10 + 8*decay*sin(float64(i)*1.5)
	}
	samples := makePressureSamplesTime(100*time.Millisecond, l0s, make([]int64, 20))
	lc, _, _ := detectLimitCycle(samples, 0)
	if lc {
		t.Error("expected no limit cycle for damped oscillation")
	}
}

func TestDetectLimitCycle_Sustained(t *testing.T) {
	// Sustained oscillation: constant amplitude
	l0s := make([]float64, 20)
	for i := range l0s {
		l0s[i] = 10 + 5*sin(float64(i)*1.2)
	}
	samples := makePressureSamplesTime(100*time.Millisecond, l0s, make([]int64, 20))
	lc, period, amp := detectLimitCycle(samples, 0)
	if !lc {
		t.Error("expected limit cycle for sustained oscillation")
	}
	if period <= 0 {
		t.Error("expected positive period for limit cycle")
	}
	if amp <= 0 {
		t.Error("expected positive amplitude for limit cycle")
	}
}

func TestPredictConvergence_HealthyStable(t *testing.T) {
	samples := makePressureSamplesTime(500*time.Millisecond, []float64{2, 2, 2, 2, 2}, []int64{0, 0, 0, 0, 0})
	converging, target, prob, _ := predictConvergence(samples, BasinHealthy, 0)
	// Already converged: target == current, so converging=false, but probability is high
	if converging {
		t.Error("stable healthy is already converged, not 'converging to a different state'")
	}
	if target != BasinHealthy {
		t.Errorf("expected target healthy, got %s", target)
	}
	if prob < 0.9 {
		t.Errorf("expected high probability, got %.2f", prob)
	}
}

func TestPredictConvergence_HealthyToStressed(t *testing.T) {
	samples := makePressureSamplesTime(500*time.Millisecond, []float64{3, 4, 5, 6, 7}, []int64{0, 0, 0, 0, 0})
	converging, target, prob, _ := predictConvergence(samples, BasinHealthy, 0.5)
	if !converging {
		t.Error("expected converging for rising L0")
	}
	if target != BasinStressed && target != BasinHealthy {
		t.Errorf("expected target stressed or healthy, got %s", target)
	}
	if prob < 0.3 {
		t.Errorf("expected meaningful probability, got %.2f", prob)
	}
}

func TestPredictConvergence_StressedRecovering(t *testing.T) {
	samples := makePressureSamplesTime(500*time.Millisecond, []float64{16, 14, 12, 10, 8}, []int64{10, 8, 5, 3, 1})
	vel := computeL0Velocity(samples)
	converging, target, _, _ := predictConvergence(samples, BasinStressed, vel)
	if !converging {
		t.Error("expected converging for stressed recovering")
	}
	if target != BasinHealthy {
		t.Errorf("expected target healthy, got %s", target)
	}
}

func TestPredictConvergence_DegradedToStressed(t *testing.T) {
	samples := makePressureSamplesTime(500*time.Millisecond, []float64{25, 23, 21, 19, 18}, []int64{50, 40, 30, 20, 15})
	vel := computeL0Velocity(samples)
	converging, target, _, _ := predictConvergence(samples, BasinDegraded, vel)
	if !converging {
		t.Error("expected converging for degraded recovering")
	}
	if target != BasinStressed {
		t.Errorf("expected target stressed, got %s", target)
	}
}

func TestPredictConvergence_DegradedWorsening(t *testing.T) {
	samples := makePressureSamplesTime(500*time.Millisecond, []float64{20, 21, 22, 23, 24}, []int64{30, 35, 40, 45, 50})
	vel := computeL0Velocity(samples)
	converging, target, prob, _ := predictConvergence(samples, BasinDegraded, vel)
	if !converging {
		t.Error("expected converging (toward collapse)")
	}
	if target != BasinCollapsed {
		t.Errorf("expected target collapsed for worsening L0, got %s", target)
	}
	if prob < 0.5 {
		t.Errorf("expected high probability for obvious collapse, got %.2f", prob)
	}
}

func TestPredictConvergence_Collapsed(t *testing.T) {
	samples := makePressureSamplesTime(500*time.Millisecond, []float64{25, 26, 27, 28, 29}, []int64{100, 110, 120, 130, 140})
	vel := computeL0Velocity(samples)
	converging, target, _, _ := predictConvergence(samples, BasinCollapsed, vel)
	if converging {
		t.Error("collapsed system with rising L0 should NOT show convergence")
	}
	if target != BasinCollapsed {
		t.Errorf("expected target collapsed, got %s", target)
	}
}

func TestComputeEscapability_Healthy(t *testing.T) {
	samples := makePressureSamplesTime(time.Second, []float64{3, 4, 3, 4, 3}, []int64{0, 0, 0, 0, 0})
	esc, score := computeEscapability(samples, BasinHealthy)
	if !esc {
		t.Error("healthy system should always be escapable")
	}
	if score < 0.9 {
		t.Errorf("expected high escapability for healthy, got %.4f", score)
	}
}

func TestComputeEscapability_DegradedRecovering(t *testing.T) {
	samples := makePressureSamplesTime(time.Second, []float64{25, 23, 21, 19, 17}, []int64{50, 40, 30, 20, 10})
	esc, score := computeEscapability(samples, BasinDegraded)
	if !esc {
		t.Error("degraded but recovering should be escapable")
	}
	if score < 0.5 {
		t.Errorf("expected >0.5 escapability for recovering, got %.4f", score)
	}
}

func TestComputeEscapability_DegradedWorsening(t *testing.T) {
	samples := makePressureSamplesTime(time.Second, []float64{20, 22, 24, 26, 28}, []int64{30, 50, 70, 90, 110})
	esc, score := computeEscapability(samples, BasinDegraded)
	if esc {
		t.Error("rapidly worsening degraded system should NOT be escapable")
	}
	if score > 0.4 {
		t.Errorf("expected low escapability for worsening, got %.4f", score)
	}
}

func TestComputeEscapability_Collapsed(t *testing.T) {
	samples := makePressureSamplesTime(time.Second, []float64{28, 29, 30, 31, 32}, []int64{120, 130, 140, 150, 160})
	esc, _ := computeEscapability(samples, BasinCollapsed)
	if esc {
		t.Error("collapsed system with rising L0 should NOT be escapable")
	}
}

func TestDetectHysteresis_NotEnoughTransitions(t *testing.T) {
	trans := []BasinTransition{
		{From: BasinHealthy, To: BasinStressed, L0AtCrossing: 8.0},
		{From: BasinStressed, To: BasinHealthy, L0AtCrossing: 7.5},
		{From: BasinHealthy, To: BasinStressed, L0AtCrossing: 8.5},
	}
	detected, _ := detectHysteresis(trans)
	if detected {
		t.Error("expected no hysteresis with <4 transitions")
	}
}

func TestDetectHysteresis_Detected(t *testing.T) {
	trans := []BasinTransition{
		{From: BasinStressed, To: BasinDegraded, L0AtCrossing: 20.0},
		{From: BasinDegraded, To: BasinStressed, L0AtCrossing: 15.0},
		{From: BasinStressed, To: BasinDegraded, L0AtCrossing: 21.0},
		{From: BasinDegraded, To: BasinStressed, L0AtCrossing: 14.0},
	}
	detected, width := detectHysteresis(trans)
	if !detected {
		t.Error("expected hysteresis with clear entry/exit gap")
	}
	if width < 4.0 || width > 8.0 {
		t.Errorf("expected hysteresis width ~6, got %.1f", width)
	}
}

func TestDetectHysteresis_MultipleEnterExits(t *testing.T) {
	// Entry at ~20 L0, exit at ~10 L0 — clear hysteresis
	trans := []BasinTransition{
		{From: BasinStressed, To: BasinDegraded, L0AtCrossing: 20},
		{From: BasinDegraded, To: BasinStressed, L0AtCrossing: 10},
		{From: BasinStressed, To: BasinDegraded, L0AtCrossing: 20},
		{From: BasinDegraded, To: BasinStressed, L0AtCrossing: 11},
	}
	detected, width := detectHysteresis(trans)
	if !detected {
		t.Error("expected hysteresis detection")
	}
	if width < 8 {
		t.Errorf("expected significant width, got %.1f", width)
	}
}

func TestDegradationDuration(t *testing.T) {
	now := time.Now()
	samples := make([]PressureSample, 5)
	samples[0] = PressureSample{Timestamp: now, LastL0Score: 3, ActiveRetries: 0}
	samples[1] = PressureSample{Timestamp: now.Add(time.Second), LastL0Score: 8, ActiveRetries: 5}
	samples[2] = PressureSample{Timestamp: now.Add(2 * time.Second), LastL0Score: 22, ActiveRetries: 30}
	samples[3] = PressureSample{Timestamp: now.Add(3 * time.Second), LastL0Score: 23, ActiveRetries: 35}
	samples[4] = PressureSample{Timestamp: now.Add(4 * time.Second), LastL0Score: 22, ActiveRetries: 30}

	d := computeDegradationDuration(samples)
	if d < 2*time.Second || d > 3*time.Second {
		t.Errorf("expected ~2-3s degradation, got %v", d)
	}
}

func TestDegradationDuration_NoDegradation(t *testing.T) {
	samples := makePressureSamplesTime(time.Second, []float64{2, 3, 4, 3, 2}, []int64{0, 0, 0, 0, 0})
	d := computeDegradationDuration(samples)
	if d != 0 {
		t.Errorf("expected 0 for no degradation, got %v", d)
	}
}

func TestAnalyzeBasin_InsufficientData(t *testing.T) {
	result := AnalyzeBasin(nil)
	if result.CurrentBasin != BasinUnknown {
		t.Errorf("expected unknown for nil, got %s", result.CurrentBasin)
	}
	if result.TrajectoryPhase != PhaseInsufficientData {
		t.Errorf("expected insufficient_data phase, got %s", result.TrajectoryPhase)
	}

	result = AnalyzeBasin([]PressureSample{{}})
	if result.CurrentBasin != BasinUnknown {
		t.Errorf("expected unknown for single sample, got %s", result.CurrentBasin)
	}
}

func TestAnalyzeBasin_HealthyConverged(t *testing.T) {
	// Truly flat L0 near 2
	samples := makePressureSamplesTime(500*time.Millisecond, []float64{2, 2, 2, 2, 2, 2, 2, 2}, []int64{0, 0, 0, 0, 0, 0, 0, 0})
	result := AnalyzeBasin(samples)
	if result.CurrentBasin != BasinHealthy {
		t.Errorf("expected healthy basin, got %s", result.CurrentBasin)
	}
	if result.TrajectoryPhase != PhaseHealthyConverged {
		t.Errorf("expected healthy_converged, got %s", result.TrajectoryPhase)
	}
	if !result.Escapable {
		t.Error("healthy system should be escapable")
	}
	if result.InDegradation {
		t.Error("healthy system should not be in degradation")
	}
}

func TestAnalyzeBasin_ApproachingStress(t *testing.T) {
	// L0 rising from 3 towards 8
	samples := makePressureSamplesTime(500*time.Millisecond, []float64{3, 4, 5, 6, 7}, []int64{0, 0, 0, 0, 0})
	result := AnalyzeBasin(samples)
	if result.CurrentBasin != BasinHealthy {
		t.Errorf("expected healthy basin (still below 8), got %s", result.CurrentBasin)
	}
	if result.TrajectoryPhase != PhaseApproachingStress {
		t.Errorf("expected approaching_stress, got %s", result.TrajectoryPhase)
	}
	if result.L0Velocity <= 0 {
		t.Errorf("expected positive L0 velocity, got %.4f", result.L0Velocity)
	}
}

func TestAnalyzeBasin_StressedStationary(t *testing.T) {
	samples := makePressureSamplesTime(500*time.Millisecond, []float64{11, 11.5, 10.5, 11, 11.2}, []int64{5, 5, 5, 5, 5})
	result := AnalyzeBasin(samples)
	if result.CurrentBasin != BasinStressed {
		t.Errorf("expected stressed basin, got %s", result.CurrentBasin)
	}
	if result.L0Velocity > 0.05 || result.L0Velocity < -0.05 {
		t.Logf("L0 velocity for stressed stationary: %.4f", result.L0Velocity)
	}
}

func TestAnalyzeBasin_ApproachingDegradation(t *testing.T) {
	// L0 rising from 12 toward 20
	samples := makePressureSamplesTime(500*time.Millisecond, []float64{12, 14, 16, 18, 19}, []int64{5, 8, 12, 15, 18})
	result := AnalyzeBasin(samples)
	if result.CurrentBasin != BasinStressed {
		t.Errorf("expected stressed basin (still below 20), got %s", result.CurrentBasin)
	}
	if result.TrajectoryPhase != PhaseApproachingDegradation {
		t.Errorf("expected approaching_degradation, got %s", result.TrajectoryPhase)
	}
}

func TestAnalyzeBasin_DegradedStuck(t *testing.T) {
	// L0 around 22 with no recovery sign
	samples := makePressureSamplesTime(500*time.Millisecond, []float64{22, 22.5, 22.3, 22.8, 22.1}, []int64{40, 42, 41, 43, 40})
	result := AnalyzeBasin(samples)
	if result.CurrentBasin != BasinDegraded {
		t.Errorf("expected degraded basin, got %s", result.CurrentBasin)
	}
	if !result.InDegradation {
		t.Error("should be in degradation")
	}
}

func TestAnalyzeBasin_EscapingDegradation(t *testing.T) {
	// L0 falling but still in degraded basin (L0 >= 20)
	samples := makePressureSamplesTime(500*time.Millisecond, []float64{25, 24, 23, 22, 21}, []int64{50, 45, 40, 35, 30})
	result := AnalyzeBasin(samples)
	if result.TrajectoryPhase != PhaseEscapingDegradation {
		t.Errorf("expected escaping_degradation, got %s", result.TrajectoryPhase)
	}
	if result.L0Velocity >= 0 {
		t.Errorf("expected negative L0 velocity, got %.4f", result.L0Velocity)
	}
	if result.CurrentBasin != BasinDegraded {
		t.Errorf("expected still in degraded basin, got %s", result.CurrentBasin)
	}
}

func TestAnalyzeBasin_Collapsed(t *testing.T) {
	samples := makePressureSamplesTime(500*time.Millisecond, []float64{26, 27, 28, 29, 30}, []int64{110, 120, 130, 140, 150})
	result := AnalyzeBasin(samples)
	if result.CurrentBasin != BasinCollapsed {
		t.Errorf("expected collapsed basin, got %s", result.CurrentBasin)
	}
	if result.TrajectoryPhase != PhaseCollapsed {
		t.Errorf("expected collapsed phase, got %s", result.TrajectoryPhase)
	}
	if result.Escapable {
		t.Error("collapsed system should NOT be escapable")
	}
}

func TestAnalyzeBasin_AcceleratingCollapse(t *testing.T) {
	// L0 rising in degraded territory but not yet collapsed: 20-24.5 (gaps increasing)
	l0s := []float64{20.0, 20.5, 21.2, 22.0, 23.0, 24.5}
	samples := makePressureSamplesTime(500*time.Millisecond, l0s, []int64{30, 35, 45, 55, 65, 80})
	result := AnalyzeBasin(samples)
	if result.TrajectoryPhase != PhaseAcceleratingCollapse {
		t.Errorf("expected accelerating_into_collapse, got %s (L0=%.1f vel=%.4f acc=%.4f)",
			result.TrajectoryPhase, samples[len(samples)-1].LastL0Score, result.L0Velocity, result.L0Acceleration)
	}
}

func TestAnalyzeBasin_LimitCycle(t *testing.T) {
	// Sustained oscillation around L0 12 (stressed region)
	l0s := make([]float64, 20)
	for i := range l0s {
		l0s[i] = 12 + 4*sin(float64(i)*1.2)
	}
	samples := makePressureSamplesTime(200*time.Millisecond, l0s, make([]int64, 20))
	result := AnalyzeBasin(samples)
	if !result.LimitCycle {
		t.Errorf("expected limit cycle for sustained oscillation, L0 velocity=%.4f", result.L0Velocity)
	}
	if result.TrajectoryPhase != PhaseLimitCycle {
		t.Errorf("expected limit_cycle phase, got %s", result.TrajectoryPhase)
	}
}

func TestAnalyzeBasin_Transitions(t *testing.T) {
	// healthy -> stressed -> healthy
	l0s := []float64{2, 4, 6, 9, 12, 10, 8, 6, 4, 2}
	samples := makePressureSamplesTime(time.Second, l0s, []int64{0, 0, 0, 0, 5, 3, 2, 1, 0, 0})
	result := AnalyzeBasin(samples)
	if len(result.Transitions) < 2 {
		t.Errorf("expected >=2 transitions for healthy->stressed->healthy, got %d", len(result.Transitions))
	}
}

func TestBasinAttractorInfo_FormatCompact(t *testing.T) {
	info := BasinAttractorInfo{
		CurrentBasin:      BasinHealthy,
		Depth:             0.8,
		Stability:         0.95,
		L0Velocity:        -0.02,
		Converging:        true,
		ConvergenceTarget: BasinHealthy,
		ConvergenceProb:   0.95,
		TrajectoryPhase:   PhaseHealthyConverged,
	}
	s := info.FormatCompact()
	if len(s) == 0 {
		t.Error("expected non-empty format")
	}
}

func TestBasinAttractorInfo_FormatCompact_Unknown(t *testing.T) {
	info := BasinAttractorInfo{CurrentBasin: BasinUnknown}
	s := info.FormatCompact()
	if s != "basin=unknown" {
		t.Errorf("expected 'basin=unknown', got %s", s)
	}
}

func TestBasinAttractorInfo_FormatReport(t *testing.T) {
	info := BasinAttractorInfo{
		CurrentBasin:        BasinDegraded,
		Depth:               0.6,
		Stability:           0.4,
		L0Velocity:          -0.15,
		L0Acceleration:      -0.02,
		RetryVelocity:       -5.0,
		LimitCycle:          false,
		InDegradation:       true,
		DegradationDuration: 10 * time.Second,
		Escapable:           true,
		EscapabilityScore:   0.75,
		Converging:          true,
		ConvergenceTarget:   BasinStressed,
		ConvergenceProb:     0.7,
		TrajectoryPhase:     PhaseEscapingDegradation,
	}
	s := info.FormatReport()
	if len(s) == 0 {
		t.Error("expected non-empty report")
	}
}

func TestBasinString(t *testing.T) {
	tests := []struct {
		b   BasinType
		exp string
	}{
		{BasinHealthy, "healthy"},
		{BasinStressed, "stressed"},
		{BasinDegraded, "degraded"},
		{BasinCollapsed, "collapsed"},
		{BasinUnknown, "unknown"},
		{BasinType(99), "unknown"},
	}
	for _, tc := range tests {
		if s := tc.b.String(); s != tc.exp {
			t.Errorf("BasinType(%d).String() = %q, want %q", tc.b, s, tc.exp)
		}
	}
}

// --- helpers ---

func makePressureSamples(l0s []float64, retries []int64) []PressureSample {
	samples := make([]PressureSample, len(l0s))
	now := time.Now()
	for i := range l0s {
		samples[i] = PressureSample{
			Timestamp:     now.Add(time.Duration(i) * 100 * time.Millisecond),
			LastL0Score:   l0s[i],
			ActiveRetries: retries[i],
		}
	}
	return samples
}

func makePressureSamplesTime(interval time.Duration, l0s []float64, retries []int64) []PressureSample {
	samples := make([]PressureSample, len(l0s))
	now := time.Now()
	for i := range l0s {
		ret := int64(0)
		if i < len(retries) {
			ret = retries[i]
		}
		samples[i] = PressureSample{
			Timestamp:     now.Add(time.Duration(i) * interval),
			LastL0Score:   l0s[i],
			ActiveRetries: ret,
		}
	}
	return samples
}

func sin(x float64) float64 {
	if x < 0 {
		return -sin(-x)
	}
	// Simple Taylor approximation for test use
	x = x - float64(int(x/(2*3.141592653589793)))*2*3.141592653589793
	if x > 3.141592653589793 {
		return -sin(x - 3.141592653589793)
	}
	// sin(x) ≈ x - x³/6 for small x
	return x - x*x*x/6.0
}
