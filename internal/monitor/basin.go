package monitor

import (
	"fmt"
	"math"
	"strings"
	"time"
)

// BasinType classifies the system's current attractor basin.
// Each basin corresponds to a regime with distinct dynamics and recovery properties.
//
//	BasinUnknown  — insufficient data / not yet classified
//	BasinHealthy  — L0 < 8, low retries, compaction keeping up
//	BasinStressed — L0 8–20, writes delayed, elevated retries
//	BasinDegraded — L0 ≥ 20, writes rejected, high retries
//	BasinCollapsed — L0 ≥ 25, maxed retries, positive feedback collapse
type BasinType int

const (
	BasinUnknown BasinType = iota
	BasinHealthy
	BasinStressed
	BasinDegraded
	BasinCollapsed
)

func (b BasinType) String() string {
	switch b {
	case BasinHealthy:
		return "healthy"
	case BasinStressed:
		return "stressed"
	case BasinDegraded:
		return "degraded"
	case BasinCollapsed:
		return "collapsed"
	default:
		return "unknown"
	}
}

// TrajectoryPhase adds precision beyond the 5-class trajectory classifier.
// These describe the specific dynamical regime the system is in.
const (
	PhaseHealthyConverged       = "healthy_converged"          // stable in healthy basin
	PhaseApproachingStress      = "approaching_stress"         // healthy but L0 rising
	PhaseStressedStationary     = "stressed_stationary"        // in stressed basin, stable
	PhaseApproachingDegradation = "approaching_degradation"    // stressed, L0 rising toward degraded
	PhaseRecoveringFromStress   = "recovering_from_stress"     // stressed, L0 dropping toward healthy
	PhaseDegradedStuck          = "degraded_stuck"             // degraded, no recovery sign
	PhaseEscapingDegradation    = "escaping_degradation"       // degraded, L0 dropping
	PhaseAcceleratingCollapse   = "accelerating_into_collapse" // degraded, L0 still rising
	PhaseCollapsed              = "collapsed_basin"            // in collapsed basin
	PhaseLimitCycle             = "limit_cycle"                // oscillating between basins
	PhaseInsufficientData       = "insufficient_data"
)

// Basin thresholds aligned with backpressure thresholds (L0Soft=8.0, L0Hard=20.0).
const (
	basinHealthyL0       = 8.0
	basinDegradedL0      = 20.0
	basinCollapsedL0     = 25.0
	basinHealthyRetries  = 5
	basinStressedRetries = 30
	basinDegradedRetries = 100

	minTransitionsForHysteresis = 4
	minSamplesForVelocity       = 3
	minSamplesForAcceleration   = 6

	recoveryVelThreshold = -0.05 // L0 dropping faster than this per sample = convincing recovery
	stuckVelThreshold    = 0.01  // L0 nearly flat while in degradation = stuck
	limitCycleMinZC      = 6
	escapableMaxL0       = 22.0
	escapableMaxRetries  = 50
)

// BasinTransition records a crossing between attractor basins.
type BasinTransition struct {
	From         BasinType
	To           BasinType
	Timestamp    time.Time
	L0AtCrossing float64
}

// BasinAttractorInfo contains the attractor/stability basin analysis result.
type BasinAttractorInfo struct {
	CurrentBasin  BasinType
	PreviousBasin BasinType
	Transitions   []BasinTransition

	// Basin geometry: how deep and how stable in the current basin
	Depth     float64 // 0.0 = at boundary, 1.0 = deep in basin center
	Stability float64 // 0.0 = at boundary / oscillating, 1.0 = firmly inside

	// Phase space dynamics (first and second derivatives of L0)
	L0Velocity     float64 // L0 change rate (units/sec), positive = rising (degrading)
	L0Acceleration float64 // L0 velocity change rate, positive = accelerating into degradation
	RetryVelocity  float64 // ActiveRetries change rate
	GoroutineSlope float64 // goroutine trend (per sample)

	// Limit cycle / persistent oscillation detection
	LimitCycle       bool
	LimitCyclePeriod time.Duration // estimated dominant period
	OscillationAmp   float64       // oscillation amplitude in L0

	// Convergence prediction
	Converging        bool          // on a trajectory to converge?
	ConvergenceTarget BasinType     // predicted convergence basin
	ConvergenceProb   float64       // 0-1 confidence
	TimeToConvergence time.Duration // estimated time to reach target basin

	// Degradation assessment
	InDegradation       bool          // in degraded or collapsed basin
	DegradationDuration time.Duration // time spent in degradation
	Escapable           bool          // can recover without external intervention
	EscapabilityScore   float64       // 0 = stuck, 1 = will recover easily

	// Hysteresis: does the system enter and leave degradation at different L0 thresholds?
	HysteresisDetected bool
	HysteresisWidth    float64 // L0 gap between entry and exit thresholds

	// High-level trajectory phase description
	TrajectoryPhase string
}

// FormatCompact returns a one-line summary of the basin analysis.
func (b BasinAttractorInfo) FormatCompact() string {
	if b.CurrentBasin == BasinUnknown {
		return "basin=unknown"
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "basin=%s depth=%.2f stab=%.2f", b.CurrentBasin, b.Depth, b.Stability)
	fmt.Fprintf(&sb, " L0vel=%.3f L0acc=%.3f", b.L0Velocity, b.L0Acceleration)
	if b.LimitCycle {
		fmt.Fprintf(&sb, " LC=yes(%.0fs)", b.LimitCyclePeriod.Seconds())
	}
	if b.Converging {
		fmt.Fprintf(&sb, " converge=%s(p=%.0f%%)", b.ConvergenceTarget, b.ConvergenceProb*100)
	}
	if b.InDegradation {
		fmt.Fprintf(&sb, " stuck=%v esc=%.0f%%", !b.Escapable, b.EscapabilityScore*100)
	}
	fmt.Fprintf(&sb, " %s", b.TrajectoryPhase)
	return sb.String()
}

// FormatReport returns a multi-line report of the basin analysis.
func (b BasinAttractorInfo) FormatReport() string {
	var sb strings.Builder
	sb.WriteString("BASIN ANALYSIS\n")
	sb.WriteString("--------------\n")

	if b.CurrentBasin == BasinUnknown {
		fmt.Fprintf(&sb, "  basin: unknown (insufficient data)\n")
		sb.WriteString("--------------\n")
		return sb.String()
	}

	fmt.Fprintf(&sb, "  current basin:  %s (depth=%.2f stability=%.2f)\n", b.CurrentBasin, b.Depth, b.Stability)
	if b.PreviousBasin != BasinUnknown {
		fmt.Fprintf(&sb, "  previous basin: %s\n", b.PreviousBasin)
	}
	fmt.Fprintf(&sb, "  transitions:    %d\n", len(b.Transitions))
	fmt.Fprintf(&sb, "  phase:          %s\n", b.TrajectoryPhase)

	fmt.Fprintf(&sb, "  phase space:\n")
	fmt.Fprintf(&sb, "    L0 velocity:      %+.4f /sec\n", b.L0Velocity)
	fmt.Fprintf(&sb, "    L0 acceleration:  %+.4f /sec²\n", b.L0Acceleration)
	fmt.Fprintf(&sb, "    retry velocity:   %+.2f /sec\n", b.RetryVelocity)
	fmt.Fprintf(&sb, "    goroutine slope:  %+.2f /sample\n", b.GoroutineSlope)

	if b.LimitCycle {
		fmt.Fprintf(&sb, "  limit cycle:     yes (period=%.0fs amplitude=%.2f L0)\n",
			b.LimitCyclePeriod.Seconds(), b.OscillationAmp)
	} else {
		fmt.Fprintf(&sb, "  limit cycle:     no\n")
	}

	fmt.Fprintf(&sb, "  convergence:\n")
	fmt.Fprintf(&sb, "    converging:    %v\n", b.Converging)
	if b.Converging {
		fmt.Fprintf(&sb, "    target:        %s (prob=%.0f%%)\n", b.ConvergenceTarget, b.ConvergenceProb*100)
		fmt.Fprintf(&sb, "    eta:           %v\n", b.TimeToConvergence.Round(time.Second))
	}

	if b.InDegradation {
		fmt.Fprintf(&sb, "  degradation:\n")
		fmt.Fprintf(&sb, "    duration:      %v\n", b.DegradationDuration.Round(time.Second))
		fmt.Fprintf(&sb, "    escapable:     %v\n", b.Escapable)
		fmt.Fprintf(&sb, "    escapability:  %.0f%%\n", b.EscapabilityScore*100)
	}

	if b.HysteresisDetected {
		fmt.Fprintf(&sb, "  hysteresis:      yes (width=%.1f L0)\n", b.HysteresisWidth)
	}

	sb.WriteString("--------------\n")
	return sb.String()
}

// classifyBasin determines the attractor basin for a single pressure sample.
func classifyBasin(s PressureSample) BasinType {
	return classifyBasinFromParams(s.LastL0Score, s.ActiveRetries)
}

func classifyBasinFromParams(l0 float64, retries int64) BasinType {
	if l0 >= basinCollapsedL0 || retries >= basinDegradedRetries {
		return BasinCollapsed
	}
	if l0 >= basinDegradedL0 || retries >= basinStressedRetries {
		return BasinDegraded
	}
	if l0 >= basinHealthyL0 || retries >= basinHealthyRetries {
		return BasinStressed
	}
	return BasinHealthy
}

// computeBasinDepth returns how deep the system is in its current basin (0-1).
// 0.0 = at the boundary, 1.0 = deep in the basin center.
func computeBasinDepth(basin BasinType, s PressureSample) float64 {
	switch basin {
	case BasinHealthy:
		if s.LastL0Score <= 0 {
			return 1.0
		}
		depth := 1.0 - s.LastL0Score/basinHealthyL0
		if depth < 0 {
			return 0
		}
		return depth

	case BasinStressed:
		l0Depth := (s.LastL0Score - basinHealthyL0) / (basinDegradedL0 - basinHealthyL0)
		retryDepth := float64(s.ActiveRetries-basinHealthyRetries) / float64(basinStressedRetries-basinHealthyRetries)
		if l0Depth > retryDepth {
			return clamp01(l0Depth)
		}
		return clamp01(retryDepth)

	case BasinDegraded:
		l0Depth := (s.LastL0Score - basinDegradedL0) / (basinCollapsedL0 - basinDegradedL0)
		retryDepth := float64(s.ActiveRetries-basinStressedRetries) / float64(basinDegradedRetries-basinStressedRetries)
		if l0Depth > retryDepth {
			return clamp01(l0Depth)
		}
		return clamp01(retryDepth)

	case BasinCollapsed:
		return 1.0

	default:
		return 0
	}
}

// computeBasinStability measures how stable the system is in its current basin.
// Accounts for proximity to boundary and oscillation amplitude.
func computeBasinStability(basin BasinType, depth float64, l0Vel float64, oscAmp float64) float64 {
	stability := depth

	// Reduce stability if velocity is pushing toward the boundary
	if l0Vel > 0 && depth < 0.5 {
		stability -= l0Vel * 2.0
	}

	// Reduce stability if oscillating near a boundary
	if oscAmp > 1.0 {
		stability -= clamp01(oscAmp / 5.0)
	}

	return clamp01(stability)
}

// detectTransitions finds all basin boundary crossings in the sample history.
func detectTransitions(samples []PressureSample) []BasinTransition {
	if len(samples) < 2 {
		return nil
	}

	var transitions []BasinTransition
	prev := classifyBasin(samples[0])

	for i := 1; i < len(samples); i++ {
		curr := classifyBasin(samples[i])
		if curr != prev {
			transitions = append(transitions, BasinTransition{
				From:         prev,
				To:           curr,
				Timestamp:    samples[i].Timestamp,
				L0AtCrossing: samples[i].LastL0Score,
			})
			prev = curr
		}
	}

	return transitions
}

// computeL0Velocity computes the recent L0 velocity (first derivative) in L0/sec.
// Positive = L0 rising (degrading), Negative = L0 dropping (recovering).
func computeL0Velocity(samples []PressureSample) float64 {
	n := len(samples)
	if n < minSamplesForVelocity {
		return 0
	}

	window := n / 3
	if window < 3 {
		window = 3
	}
	if window > n {
		window = n
	}
	tail := samples[n-window:]

	l0s := make([]float64, len(tail))
	for i, s := range tail {
		l0s[i] = s.LastL0Score
	}

	slope := linearSlope(l0s)

	duration := tail[len(tail)-1].Timestamp.Sub(tail[0].Timestamp).Seconds()
	if duration < 0.001 {
		return 0
	}

	return slope * float64(window) / duration
}

// computeL0Acceleration computes the second derivative of L0 (acceleration).
// Positive = degradation is accelerating, Negative = slowing down / improving.
func computeL0Acceleration(samples []PressureSample) float64 {
	n := len(samples)
	if n < minSamplesForAcceleration {
		return 0
	}

	velocities := make([]float64, 0, n/2)
	halfWindow := 3
	if halfWindow*2+1 >= n {
		halfWindow = n / 4
		if halfWindow < 1 {
			halfWindow = 1
		}
	}

	for i := halfWindow; i < n; i++ {
		start := i - halfWindow
		if start < 0 {
			start = 0
		}
		window := samples[start : i+1]
		if len(window) < 2 {
			continue
		}
		vel := computeL0Velocity(window)
		velocities = append(velocities, vel)
	}

	if len(velocities) < 2 {
		return 0
	}

	return linearSlope(velocities)
}

// computeRetryVelocity computes ActiveRetries change rate (count/sec).
func computeRetryVelocity(samples []PressureSample) float64 {
	n := len(samples)
	if n < 3 {
		return 0
	}

	window := n / 3
	if window < 2 {
		window = 2
	}
	if window > n {
		window = n
	}
	tail := samples[n-window:]

	retries := make([]float64, len(tail))
	for i, s := range tail {
		retries[i] = float64(s.ActiveRetries)
	}

	slope := linearSlope(retries)
	duration := tail[len(tail)-1].Timestamp.Sub(tail[0].Timestamp).Seconds()
	if duration < 0.001 {
		return 0
	}

	return slope * float64(window) / duration
}

// computeGoroutineSlope computes the goroutine trend (goroutines per sample interval).
func computeGoroutineSlope(samples []PressureSample) float64 {
	n := len(samples)
	if n < 3 {
		return 0
	}

	goCounts := make([]float64, n)
	for i, s := range samples {
		goCounts[i] = float64(s.Goroutines)
	}

	return linearSlope(goCounts)
}

// detectLimitCycle checks if the system is in a sustained oscillation (limit cycle).
// Requires: multiple zero-crossings, non-decaying amplitude, regular period.
func detectLimitCycle(samples []PressureSample, l0Vel float64) (bool, time.Duration, float64) {
	if len(samples) < 10 {
		return false, 0, 0
	}

	l0s := make([]float64, len(samples))
	for i, s := range samples {
		l0s[i] = s.LastL0Score
	}

	osc := computeOscillationStats(l0s)
	if !osc.Oscillating || osc.ZeroCrossingCount < limitCycleMinZC {
		return false, 0, 0
	}

	// Check amplitude is not decaying: compare first half to second half
	mid := len(l0s) / 2
	var firstMax, secondMax float64
	firstMin, secondMin := 1e9, 1e9
	for i := 0; i < mid; i++ {
		if l0s[i] > firstMax {
			firstMax = l0s[i]
		}
		if l0s[i] < firstMin {
			firstMin = l0s[i]
		}
	}
	for i := mid; i < len(l0s); i++ {
		if l0s[i] > secondMax {
			secondMax = l0s[i]
		}
		if l0s[i] < secondMin {
			secondMin = l0s[i]
		}
	}

	firstAmp := firstMax - firstMin
	secondAmp := secondMax - secondMin
	ampDecay := firstAmp - secondAmp

	// If amplitude is decaying significantly, it's a damped oscillation, not limit cycle
	if ampDecay > firstAmp*0.3 {
		return false, 0, 0
	}

	period := time.Duration(0)
	if osc.ZeroCrossingCount > 0 {
		duration := samples[len(samples)-1].Timestamp.Sub(samples[0].Timestamp)
		cycles := float64(osc.ZeroCrossingCount) / 2.0
		if cycles > 0.5 {
			period = time.Duration(float64(duration) / cycles)
		}
	}

	return true, period, osc.MeanAmplitude
}

// predictConvergence predicts where the system is heading.
func predictConvergence(samples []PressureSample, current BasinType, l0Vel float64) (bool, BasinType, float64, time.Duration) {
	if len(samples) < 3 {
		return false, BasinUnknown, 0, 0
	}

	latest := samples[len(samples)-1]

	var target BasinType
	var prob float64
	var eta time.Duration

	switch current {
	case BasinHealthy:
		if l0Vel <= 0.001 {
			// Stable in healthy basin
			target = BasinHealthy
			prob = 0.95
		} else {
			// L0 rising, estimate time to reach stressed boundary
			distToBoundary := basinHealthyL0 - latest.LastL0Score
			target = BasinHealthy
			prob = 0.6
			if distToBoundary < 0 || l0Vel > 0.01 {
				distToBoundary = 0
				target = BasinStressed
				prob = 0.5
			}
			if l0Vel > 0.001 {
				etaSec := distToBoundary / l0Vel
				if etaSec > 0 && etaSec < 3600 {
					eta = time.Duration(etaSec * float64(time.Second))
				}
			}
		}

	case BasinStressed:
		if l0Vel < recoveryVelThreshold {
			target = BasinHealthy
			distToHealthy := latest.LastL0Score - basinHealthyL0
			prob = 0.7
			if l0Vel < 0 {
				etaSec := distToHealthy / (-l0Vel)
				if etaSec > 0 && etaSec < 3600 {
					eta = time.Duration(etaSec * float64(time.Second))
				}
			}
		} else if l0Vel > 0.02 {
			target = BasinDegraded
			distToDegraded := basinDegradedL0 - latest.LastL0Score
			prob = 0.6
			if l0Vel > 0 && distToDegraded > 0 {
				etaSec := distToDegraded / l0Vel
				if etaSec > 0 && etaSec < 3600 {
					eta = time.Duration(etaSec * float64(time.Second))
				}
			}
		} else {
			target = BasinStressed
			prob = 0.5
		}

	case BasinDegraded:
		if l0Vel < recoveryVelThreshold {
			target = BasinStressed
			distToStressed := latest.LastL0Score - basinDegradedL0
			prob = 0.6
			if l0Vel < 0 {
				etaSec := distToStressed / (-l0Vel)
				if etaSec > 0 && etaSec < 3600 {
					eta = time.Duration(etaSec * float64(time.Second))
				}
			}
		} else if l0Vel > 0.01 {
			target = BasinCollapsed
			distToCollapsed := basinCollapsedL0 - latest.LastL0Score
			prob = 0.7
			if l0Vel > 0 && distToCollapsed > 0 {
				etaSec := distToCollapsed / l0Vel
				if etaSec > 0 && etaSec < 3600 {
					eta = time.Duration(etaSec * float64(time.Second))
				}
			}
		} else {
			target = BasinDegraded
			prob = 0.4
		}

	case BasinCollapsed:
		if l0Vel < -0.1 {
			target = BasinDegraded
			prob = 0.4
		} else {
			target = BasinCollapsed
			prob = 0.8
		}

	default:
		return false, BasinUnknown, 0, 0
	}

	converging := prob >= 0.5 && target != current
	return converging, target, clamp01(prob), eta
}

// computeEscapability assesses whether the system can recover from its current
// degraded/collapsed state without external intervention.
func computeEscapability(samples []PressureSample, current BasinType) (bool, float64) {
	if current != BasinDegraded && current != BasinCollapsed {
		return true, 1.0
	}

	if len(samples) < 3 {
		return true, 0.8
	}

	latest := samples[len(samples)-1]
	l0Vel := computeL0Velocity(samples)

	score := 0.5

	// L0 velocity: negative (improving) is good
	if l0Vel < -0.1 {
		score += 0.3
	} else if l0Vel < -0.02 {
		score += 0.15
	} else if l0Vel > 0.05 {
		score -= 0.3
	} else if l0Vel > 0.01 {
		score -= 0.1
	}

	// L0 level: lower is more escapable
	if latest.LastL0Score < basinDegradedL0 {
		score += 0.2
	} else if latest.LastL0Score > basinCollapsedL0 {
		score -= 0.3
	}

	// ActiveRetries: lower is more escapable
	if latest.ActiveRetries < 10 {
		score += 0.15
	} else if latest.ActiveRetries > 50 {
		score -= 0.2
	}

	// Check recent trajectory: if L0 has been rising for many consecutive samples, bad
	if len(samples) >= 5 {
		tail := samples[len(samples)-5:]
		rising := 0
		for i := 1; i < len(tail); i++ {
			if tail[i].LastL0Score > tail[i-1].LastL0Score {
				rising++
			}
		}
		if rising >= 4 {
			score -= 0.2
		}
	}

	esc := clamp01(score)
	return esc > 0.3, esc
}

// detectHysteresis checks if the system enters and leaves degradation at different L0 thresholds.
// A system with hysteresis requires more effort to recover than to degrade.
func detectHysteresis(transitions []BasinTransition) (bool, float64) {
	if len(transitions) < minTransitionsForHysteresis {
		return false, 0
	}

	var enterDegraded []float64
	var exitDegraded []float64

	for _, t := range transitions {
		if t.From != BasinDegraded && t.To == BasinDegraded {
			enterDegraded = append(enterDegraded, t.L0AtCrossing)
		}
		if t.From == BasinDegraded && t.To != BasinDegraded {
			exitDegraded = append(exitDegraded, t.L0AtCrossing)
		}
	}

	if len(enterDegraded) == 0 || len(exitDegraded) == 0 {
		return false, 0
	}

	var enterAvg, exitAvg float64
	for _, v := range enterDegraded {
		enterAvg += v
	}
	enterAvg /= float64(len(enterDegraded))

	for _, v := range exitDegraded {
		exitAvg += v
	}
	exitAvg /= float64(len(exitDegraded))

	width := math.Abs(enterAvg - exitAvg)
	return width > 2.0, width
}

// computeDegradationDuration calculates how long the system has been in degradation.
func computeDegradationDuration(samples []PressureSample) time.Duration {
	if len(samples) < 2 {
		return 0
	}

	firstDegraded := -1
	for i, s := range samples {
		b := classifyBasin(s)
		if b == BasinDegraded || b == BasinCollapsed {
			firstDegraded = i
			break
		}
	}

	if firstDegraded == -1 {
		return 0
	}

	return samples[len(samples)-1].Timestamp.Sub(samples[firstDegraded].Timestamp)
}

// classifyTrajectoryPhase determines the high-level dynamical phase description.
func classifyTrajectoryPhase(current BasinType, l0Vel, l0Acc float64,
	limitCycle bool, escapable bool, degraded bool) string {

	if limitCycle {
		return PhaseLimitCycle
	}

	switch current {
	case BasinHealthy:
		if l0Vel > 0.01 {
			return PhaseApproachingStress
		}
		return PhaseHealthyConverged

	case BasinStressed:
		if l0Vel < -0.02 {
			return PhaseRecoveringFromStress
		}
		if l0Vel > 0.02 {
			return PhaseApproachingDegradation
		}
		return PhaseStressedStationary

	case BasinDegraded:
		if l0Vel < -0.02 {
			return PhaseEscapingDegradation
		}
		if l0Vel > 0.02 {
			return PhaseAcceleratingCollapse
		}
		if !escapable {
			return PhaseDegradedStuck
		}
		return PhaseDegradedStuck

	case BasinCollapsed:
		return PhaseCollapsed

	default:
		return PhaseInsufficientData
	}
}

// AnalyzeBasin performs the full attractor/basin analysis on pressure samples.
func AnalyzeBasin(samples []PressureSample) BasinAttractorInfo {
	if len(samples) < 2 {
		return BasinAttractorInfo{CurrentBasin: BasinUnknown, TrajectoryPhase: PhaseInsufficientData}
	}

	latest := samples[len(samples)-1]
	prevBasin := BasinUnknown
	if len(samples) > 1 {
		prevBasin = classifyBasin(samples[len(samples)-2])
	}

	currentBasin := classifyBasin(latest)
	transitions := detectTransitions(samples)
	l0Vel := computeL0Velocity(samples)
	l0Acc := computeL0Acceleration(samples)
	retryVel := computeRetryVelocity(samples)
	goroSlope := computeGoroutineSlope(samples)
	depth := computeBasinDepth(currentBasin, latest)
	limitCycle, lcPeriod, lcAmp := detectLimitCycle(samples, l0Vel)
	oscAmp := lcAmp
	if !limitCycle {
		oscAmp = 0
	}
	stability := computeBasinStability(currentBasin, depth, l0Vel, oscAmp)
	converging, target, prob, eta := predictConvergence(samples, currentBasin, l0Vel)
	degDuration := computeDegradationDuration(samples)
	inDegradation := currentBasin == BasinDegraded || currentBasin == BasinCollapsed
	escapable, escScore := computeEscapability(samples, currentBasin)
	hystDetected, hystWidth := detectHysteresis(transitions)
	trajPhase := classifyTrajectoryPhase(currentBasin, l0Vel, l0Acc,
		limitCycle, escapable, inDegradation)

	return BasinAttractorInfo{
		CurrentBasin:        currentBasin,
		PreviousBasin:       prevBasin,
		Transitions:         transitions,
		Depth:               depth,
		Stability:           stability,
		L0Velocity:          l0Vel,
		L0Acceleration:      l0Acc,
		RetryVelocity:       retryVel,
		GoroutineSlope:      goroSlope,
		LimitCycle:          limitCycle,
		LimitCyclePeriod:    lcPeriod,
		OscillationAmp:      lcAmp,
		Converging:          converging,
		ConvergenceTarget:   target,
		ConvergenceProb:     prob,
		TimeToConvergence:   eta,
		InDegradation:       inDegradation,
		DegradationDuration: degDuration,
		Escapable:           escapable,
		EscapabilityScore:   escScore,
		HysteresisDetected:  hystDetected,
		HysteresisWidth:     hystWidth,
		TrajectoryPhase:     trajPhase,
	}
}

func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}
