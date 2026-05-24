package monitor

import (
	"testing"
	"time"
)

func TestDetectAnomalies_Empty(t *testing.T) {
	r := DetectAnomalies(nil, "standalone", "")
	if r.Stable != true {
		t.Errorf("expected stable=true for empty runs, got stable=%v", r.Stable)
	}
	if r.Confidence != "low" {
		t.Errorf("expected confidence=low for empty runs, got %s", r.Confidence)
	}
}

func TestDetectAnomalies_Stable(t *testing.T) {
	now := time.Now()
	runs := []EvolutionRun{
		{Timestamp: now.Add(-48 * time.Hour), HealthOverall: 0.95, Basin: "healthy", BasinDepth: 0.85, L0Peak: 5.0, GoroutineDelta: 0, Converging: true, ConvergenceProb: 0.9},
		{Timestamp: now.Add(-24 * time.Hour), HealthOverall: 0.94, Basin: "healthy", BasinDepth: 0.84, L0Peak: 5.2, GoroutineDelta: -1, Converging: true, ConvergenceProb: 0.88},
		{Timestamp: now, HealthOverall: 0.95, Basin: "healthy", BasinDepth: 0.86, L0Peak: 4.8, GoroutineDelta: 1, Converging: true, ConvergenceProb: 0.92},
	}
	r := DetectAnomalies(runs, "standalone", "")
	if !r.Stable {
		t.Errorf("expected stable=true, got false with %d anomalies", len(r.Anomalies))
	}
	if r.Confidence != "high" {
		t.Errorf("expected confidence=high, got %s", r.Confidence)
	}
}

func TestDetectAnomalies_BasinDrift(t *testing.T) {
	now := time.Now()
	runs := []EvolutionRun{
		{Timestamp: now.Add(-48 * time.Hour), BasinDepth: 0.85},
		{Timestamp: now.Add(-24 * time.Hour), BasinDepth: 0.55},
		{Timestamp: now, BasinDepth: 0.25},
	}
	r := DetectAnomalies(runs, "standalone", "")
	if r.Stable {
		t.Errorf("expected unstable for basin drift")
	}
	found := false
	for _, a := range r.Anomalies {
		if a.Type == AnomalyBasinDrift {
			found = true
			if a.Severity != "high" {
				t.Errorf("expected severity=high for slope=-0.30, got %s", a.Severity)
			}
		}
	}
	if !found {
		t.Errorf("expected basin_drift anomaly")
	}
}

func TestDetectAnomalies_BasinShift(t *testing.T) {
	now := time.Now()
	runs := []EvolutionRun{
		{Timestamp: now.Add(-48 * time.Hour), Basin: "healthy"},
		{Timestamp: now.Add(-24 * time.Hour), Basin: "stressed"},
		{Timestamp: now, Basin: "degraded"},
	}
	r := DetectAnomalies(runs, "standalone", "")
	found := false
	for _, a := range r.Anomalies {
		if a.Type == AnomalyBasinShift {
			found = true
			if a.Severity != "high" {
				t.Errorf("expected severity=high for 1-step basin shift, got %s", a.Severity)
			}
		}
	}
	if !found {
		t.Errorf("expected basin_shift anomaly")
	}
}

func TestDetectAnomalies_OscillationGrowth(t *testing.T) {
	now := time.Now()
	runs := []EvolutionRun{
		{Timestamp: now.Add(-48 * time.Hour), OscillationDetected: false, LimitCycle: false},
		{Timestamp: now.Add(-24 * time.Hour), OscillationDetected: true, LimitCycle: true},
		{Timestamp: now, OscillationDetected: true, LimitCycle: true},
	}
	r := DetectAnomalies(runs, "standalone", "")
	found := false
	for _, a := range r.Anomalies {
		if a.Type == AnomalyOscillationGrowth {
			found = true
		}
	}
	if !found {
		t.Errorf("expected oscillation_growth anomaly")
	}
}

func TestDetectAnomalies_HealthDecline(t *testing.T) {
	now := time.Now()
	runs := []EvolutionRun{
		{Timestamp: now.Add(-48 * time.Hour), HealthOverall: 0.95},
		{Timestamp: now.Add(-24 * time.Hour), HealthOverall: 0.85},
		{Timestamp: now, HealthOverall: 0.75},
	}
	r := DetectAnomalies(runs, "standalone", "")
	found := false
	for _, a := range r.Anomalies {
		if a.Type == AnomalyHealthDecline && a.Slope < -0.01 {
			found = true
		}
	}
	if !found {
		t.Errorf("expected health_decline anomaly")
	}
}

func TestDetectAnomalies_ConvergenceLoss(t *testing.T) {
	now := time.Now()
	runs := []EvolutionRun{
		{Timestamp: now.Add(-48 * time.Hour), Converging: true, ConvergenceProb: 0.85},
		{Timestamp: now.Add(-24 * time.Hour), Converging: true, ConvergenceProb: 0.70},
		{Timestamp: now, Converging: false, ConvergenceProb: 0.20},
	}
	r := DetectAnomalies(runs, "standalone", "")
	lossFound := false
	trendFound := false
	for _, a := range r.Anomalies {
		if a.Type == AnomalyConvergenceDegradation {
			if a.Message == "system stopped converging (converging: true → false)" {
				lossFound = true
			}
			if a.Slope < -0.05 {
				trendFound = true
			}
		}
	}
	if !lossFound {
		t.Errorf("expected convergence loss anomaly")
	}
	if !trendFound {
		t.Errorf("expected convergence prob trend anomaly")
	}
}

func TestDetectAnomalies_L0Escalation(t *testing.T) {
	now := time.Now()
	runs := []EvolutionRun{
		{Timestamp: now.Add(-48 * time.Hour), L0Peak: 5.0},
		{Timestamp: now.Add(-24 * time.Hour), L0Peak: 12.0},
		{Timestamp: now, L0Peak: 20.0},
	}
	r := DetectAnomalies(runs, "standalone", "")
	found := false
	for _, a := range r.Anomalies {
		if a.Type == AnomalyL0Escalation {
			found = true
		}
	}
	if !found {
		t.Errorf("expected l0_escalation anomaly")
	}
}

func TestDetectAnomalies_DegradationOnset(t *testing.T) {
	now := time.Now()
	runs := []EvolutionRun{
		{Timestamp: now.Add(-48 * time.Hour), InDegradation: false, DegradationLevel: "OK"},
		{Timestamp: now.Add(-24 * time.Hour), InDegradation: false, DegradationLevel: "OK"},
		{Timestamp: now, InDegradation: true, DegradationLevel: "WARN"},
	}
	r := DetectAnomalies(runs, "standalone", "")
	found := false
	for _, a := range r.Anomalies {
		if a.Type == AnomalyDegradationOnset {
			found = true
			if a.Severity != "critical" {
				t.Errorf("expected severity=critical for degradation onset, got %s", a.Severity)
			}
		}
	}
	if !found {
		t.Errorf("expected degradation_onset anomaly")
	}
}

func TestDetectAnomalies_RecoveryGrowth(t *testing.T) {
	now := time.Now()
	runs := []EvolutionRun{
		{Timestamp: now.Add(-48 * time.Hour), RecoveryDurationSec: 0},
		{Timestamp: now.Add(-24 * time.Hour), RecoveryDurationSec: 60},
		{Timestamp: now, RecoveryDurationSec: 180},
	}
	r := DetectAnomalies(runs, "standalone", "")
	found := false
	for _, a := range r.Anomalies {
		if a.Type == AnomalyRecoveryGrowth {
			found = true
		}
	}
	if !found {
		t.Errorf("expected recovery_growth anomaly")
	}
}

func TestDetectAnomalies_PersistenceGrowth(t *testing.T) {
	now := time.Now()
	runs := []EvolutionRun{
		{Timestamp: now.Add(-48 * time.Hour), PersistenceDurationSec: 0},
		{Timestamp: now.Add(-24 * time.Hour), PersistenceDurationSec: 100},
		{Timestamp: now, PersistenceDurationSec: 300},
	}
	r := DetectAnomalies(runs, "standalone", "")
	found := false
	for _, a := range r.Anomalies {
		if a.Type == AnomalyPersistenceGrowth {
			found = true
		}
	}
	if !found {
		t.Errorf("expected persistence_growth anomaly")
	}
}

func TestDetectAnomalies_GoroutineLeak(t *testing.T) {
	now := time.Now()
	runs := []EvolutionRun{
		{Timestamp: now.Add(-48 * time.Hour), GoroutineDelta: 5},
		{Timestamp: now.Add(-24 * time.Hour), GoroutineDelta: 8},
		{Timestamp: now, GoroutineDelta: 15},
	}
	r := DetectAnomalies(runs, "standalone", "")
	found := false
	for _, a := range r.Anomalies {
		if a.Type == AnomalyGoroutineLeak {
			found = true
		}
	}
	if !found {
		t.Errorf("expected goroutine_leak anomaly")
	}
}

func TestFormatAnomalyReport(t *testing.T) {
	r := AnomalyReport{
		Version:    1,
		Timestamp:  time.Now(),
		Prefix:     "standalone",
		RunCount:   3,
		Stable:     false,
		Confidence: "high",
		Anomalies: []Anomaly{
			{Type: "basin_drift", Severity: "high", Message: "test anomaly"},
		},
	}
	md := r.FormatAnomalyReport()
	if md == "" {
		t.Fatal("expected non-empty markdown")
	}
	if !contains(md, "basin_drift") {
		t.Errorf("expected markdown to contain anomaly type")
	}

	jsonData, err := r.FormatAnomalyJSON()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !contains(string(jsonData), "basin_drift") {
		t.Errorf("expected JSON to contain anomaly type")
	}
}

func TestAnomalyReport_Empty(t *testing.T) {
	r := AnomalyReport{
		Version:    1,
		Timestamp:  time.Now(),
		Prefix:     "standalone",
		RunCount:   0,
		Stable:     true,
		Confidence: "high",
	}
	md := r.FormatAnomalyReport()
	if !contains(md, "No anomalies detected") {
		t.Errorf("expected 'No anomalies detected' for empty report, got:\n%s", md)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && containsStr(s, substr)
}

func containsStr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
