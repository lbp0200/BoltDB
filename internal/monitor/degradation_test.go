package monitor

import (
	"testing"
	"time"

	"github.com/zeebo/assert"
)

func TestDegradationLevelString(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "OK", LevelOK.String())
	assert.Equal(t, "WARN", LevelWarn.String())
	assert.Equal(t, "DEGRADED", LevelDegraded.String())
	assert.Equal(t, "FAIL", LevelFail.String())
	assert.Equal(t, "UNKNOWN", DegradationLevel(99).String())
}

func TestDefaultDegradationAssertion(t *testing.T) {
	t.Parallel()
	a := DefaultDegradationAssertion()
	assert.Equal(t, 50, a.MaxGoroutineDelta)
	assert.Equal(t, int64(100), a.MaxActiveRetries)
	assert.Equal(t, 25.0, a.MaxL0Score)
	assert.Equal(t, 10.0, a.L0RecoveryThreshold)
	assert.Equal(t, int64(50), a.MaxReconnectCount)
	assert.Equal(t, 3, a.GoroutineDeltaFailWindows)
	assert.Equal(t, 2, a.GoroutineDeltaWarnWindows)
}

func TestCountConsecutiveAbove_None(t *testing.T) {
	t.Parallel()
	samples := []PressureSample{
		{Goroutines: 100},
		{Goroutines: 105},
		{Goroutines: 102},
	}
	assert.Equal(t, 0, CountConsecutiveAbove(samples, 100, 50))
}

func TestCountConsecutiveAbove_Single(t *testing.T) {
	t.Parallel()
	samples := []PressureSample{
		{Goroutines: 100},
		{Goroutines: 200},
		{Goroutines: 102},
	}
	assert.Equal(t, 1, CountConsecutiveAbove(samples, 100, 50))
}

func TestCountConsecutiveAbove_Streak(t *testing.T) {
	t.Parallel()
	samples := []PressureSample{
		{Goroutines: 100},
		{Goroutines: 200},
		{Goroutines: 210},
		{Goroutines: 220},
		{Goroutines: 102},
	}
	assert.Equal(t, 3, CountConsecutiveAbove(samples, 100, 50))
}

func TestCountConsecutiveAbove_All(t *testing.T) {
	t.Parallel()
	samples := []PressureSample{
		{Goroutines: 200},
		{Goroutines: 210},
		{Goroutines: 220},
	}
	assert.Equal(t, 3, CountConsecutiveAbove(samples, 100, 50))
}

func TestCountConsecutiveAbove_Empty(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 0, CountConsecutiveAbove(nil, 100, 50))
}

func TestCheckL0Recovery_OK(t *testing.T) {
	t.Parallel()
	a := DefaultDegradationAssertion()
	assert.Equal(t, LevelOK, checkL0Recovery(5.0, a))
	assert.Equal(t, LevelOK, checkL0Recovery(10.0, a))
}

func TestCheckL0Recovery_Degraded(t *testing.T) {
	t.Parallel()
	a := DefaultDegradationAssertion()
	result := checkL0Recovery(12.0, a)
	assert.Equal(t, LevelDegraded, result)
}

func TestCheckL0Recovery_Fail(t *testing.T) {
	t.Parallel()
	a := DefaultDegradationAssertion()
	assert.Equal(t, LevelFail, checkL0Recovery(25.0, a))
	assert.Equal(t, LevelFail, checkL0Recovery(30.0, a))
}

func TestCheckMonotonicGrowth_FewSamples(t *testing.T) {
	t.Parallel()
	pm := &PressureMonitor{}

	a := DefaultDegradationAssertion()
	assert.Equal(t, LevelOK, pm.checkMonotonicGrowth(a))
}

func TestCheckMonotonicGrowth_Flat(t *testing.T) {
	t.Parallel()
	pm := &PressureMonitor{}
	pm.mu.Lock()
	pm.samples = []PressureSample{
		{LastL0Score: 5.0}, {LastL0Score: 5.0}, {LastL0Score: 5.0},
		{LastL0Score: 5.0}, {LastL0Score: 5.0}, {LastL0Score: 5.0},
		{LastL0Score: 5.0}, {LastL0Score: 5.0}, {LastL0Score: 5.0},
	}
	pm.mu.Unlock()

	a := DefaultDegradationAssertion()
	assert.Equal(t, LevelOK, pm.checkMonotonicGrowth(a))
}

func TestCheckMonotonicGrowth_StrongRise(t *testing.T) {
	t.Parallel()
	pm := &PressureMonitor{}
	pm.mu.Lock()
	pm.samples = []PressureSample{
		{LastL0Score: 1.0}, {LastL0Score: 2.0}, {LastL0Score: 3.0},
		{LastL0Score: 4.0}, {LastL0Score: 5.0}, {LastL0Score: 6.0},
		{LastL0Score: 7.0}, {LastL0Score: 8.0}, {LastL0Score: 9.0},
		{LastL0Score: 10.0},
	}
	pm.mu.Unlock()

	a := DefaultDegradationAssertion()
	result := pm.checkMonotonicGrowth(a)
	assert.Equal(t, LevelFail, result)
}

func TestCheckMonotonicGrowth_MildRise(t *testing.T) {
	t.Parallel()
	pm := &PressureMonitor{}
	pm.mu.Lock()
	pm.samples = []PressureSample{
		{LastL0Score: 1.0}, {LastL0Score: 2.0}, {LastL0Score: 1.0},
		{LastL0Score: 3.0}, {LastL0Score: 2.0}, {LastL0Score: 4.0},
		{LastL0Score: 4.5}, {LastL0Score: 5.0}, {LastL0Score: 4.0},
		{LastL0Score: 6.0},
	}
	pm.mu.Unlock()

	a := DefaultDegradationAssertion()
	result := pm.checkMonotonicGrowth(a)
	assert.Equal(t, LevelWarn, result)
}

func TestMaxLevel(t *testing.T) {
	t.Parallel()
	assert.Equal(t, LevelFail, maxLevel(LevelOK, LevelFail))
	assert.Equal(t, LevelFail, maxLevel(LevelFail, LevelOK))
	assert.Equal(t, LevelWarn, maxLevel(LevelOK, LevelWarn))
	assert.Equal(t, LevelOK, maxLevel(LevelOK, LevelOK))
	assert.Equal(t, LevelDegraded, maxLevel(LevelDegraded, LevelWarn))
}

type mockTestingT struct {
	*testing.T
}

func (m *mockTestingT) Log(args ...any)   {}
func (m *mockTestingT) Logf(string, ...any) {}
func (m *mockTestingT) Errorf(string, ...any) {}
func (m *mockTestingT) FailNow()             {}

func TestCheckDegradation_OK(t *testing.T) {
	pm := NewPressureMonitor(nil, nil)
	now := time.Now()
	pm.mu.Lock()
	pm.samples = []PressureSample{
		{Timestamp: now, LastL0Score: 3.0, ActiveRetries: 0, Goroutines: 100},
		{Timestamp: now.Add(time.Second), LastL0Score: 4.0, ActiveRetries: 1, Goroutines: 101},
		{Timestamp: now.Add(2 * time.Second), LastL0Score: 2.0, ActiveRetries: 0, Goroutines: 100},
	}
	pm.mu.Unlock()

	mockT := &mockTestingT{T: t}
	a := DefaultDegradationAssertion()
	level := pm.CheckDegradation(mockT, a, 100)
	assert.Equal(t, LevelOK, level)
}

func TestCheckDegradation_GoroutineFail(t *testing.T) {
	pm := NewPressureMonitor(nil, nil)
	now := time.Now()
	pm.mu.Lock()
	pm.samples = []PressureSample{
		{Timestamp: now, LastL0Score: 3.0, ActiveRetries: 0, Goroutines: 100},
		{Timestamp: now.Add(time.Second), LastL0Score: 4.0, ActiveRetries: 1, Goroutines: 200},
		{Timestamp: now.Add(2 * time.Second), LastL0Score: 5.0, ActiveRetries: 0, Goroutines: 210},
		{Timestamp: now.Add(3 * time.Second), LastL0Score: 6.0, ActiveRetries: 0, Goroutines: 220},
	}
	pm.mu.Unlock()

	mockT := &mockTestingT{T: t}
	a := DefaultDegradationAssertion()
	a.MaxGoroutineDelta = 50
	a.GoroutineDeltaFailWindows = 3
	a.GoroutineDeltaWarnWindows = 2
	level := pm.CheckDegradation(mockT, a, 100)
	assert.Equal(t, LevelFail, level)
}

func TestCheckDegradation_ActiveRetriesFail(t *testing.T) {
	pm := NewPressureMonitor(nil, nil)
	pm.mu.Lock()
	pm.samples = []PressureSample{
		{LastL0Score: 3.0, ActiveRetries: 200, Goroutines: 100},
	}
	pm.mu.Unlock()

	mockT := &mockTestingT{T: t}
	a := DefaultDegradationAssertion()
	a.MaxActiveRetries = 100
	level := pm.CheckDegradation(mockT, a, 100)
	assert.Equal(t, LevelFail, level)
}

func TestCheckDegradation_ActiveRetriesWarn(t *testing.T) {
	pm := NewPressureMonitor(nil, nil)
	pm.mu.Lock()
	pm.samples = []PressureSample{
		{LastL0Score: 3.0, ActiveRetries: 50, Goroutines: 100},
	}
	pm.mu.Unlock()

	mockT := &mockTestingT{T: t}
	a := DefaultDegradationAssertion()
	a.MaxActiveRetries = 100
	a.ActiveRetriesWarn = 30
	level := pm.CheckDegradation(mockT, a, 100)
	assert.Equal(t, LevelWarn, level)
}

func TestCheckDegradation_L0PeakFail(t *testing.T) {
	pm := NewPressureMonitor(nil, nil)
	pm.mu.Lock()
	pm.samples = []PressureSample{
		{LastL0Score: 3.0, ActiveRetries: 0, Goroutines: 100},
		{LastL0Score: 30.0, ActiveRetries: 0, Goroutines: 100},
		{LastL0Score: 5.0, ActiveRetries: 0, Goroutines: 100},
	}
	pm.mu.Unlock()

	mockT := &mockTestingT{T: t}
	a := DefaultDegradationAssertion()
	level := pm.CheckDegradation(mockT, a, 100)
	assert.Equal(t, LevelFail, level)
}

func TestCheckDegradation_L0NoRecovery(t *testing.T) {
	pm := NewPressureMonitor(nil, nil)
	pm.mu.Lock()
	pm.samples = []PressureSample{
		{LastL0Score: 3.0, ActiveRetries: 0, Goroutines: 100},
		{LastL0Score: 20.0, ActiveRetries: 0, Goroutines: 100},
		{LastL0Score: 18.0, ActiveRetries: 0, Goroutines: 100},
	}
	pm.mu.Unlock()

	mockT := &mockTestingT{T: t}
	a := DefaultDegradationAssertion()
	level := pm.CheckDegradation(mockT, a, 100)
	assert.Equal(t, LevelFail, level)
}

func TestCheckDegradation_ReconnectFail(t *testing.T) {
	pm := NewPressureMonitor(nil, nil)
	pm.mu.Lock()
	pm.samples = []PressureSample{
		{LastL0Score: 3.0, ActiveRetries: 0, Goroutines: 100, ReconnectCount: 100},
	}
	pm.mu.Unlock()

	mockT := &mockTestingT{T: t}
	a := DefaultDegradationAssertion()
	a.MaxReconnectCount = 50
	level := pm.CheckDegradation(mockT, a, 100)
	assert.Equal(t, LevelFail, level)
}

func TestCheckDegradation_SentinelFail(t *testing.T) {
	pm := NewPressureMonitor(nil, nil)
	pm.mu.Lock()
	pm.samples = []PressureSample{
		{
			LastL0Score:      3.0,
			ActiveRetries:    0,
			Goroutines:       100,
			TotalSentinels:   3,
			AgreedSentinels:  1,
			LeaderChanges:    20,
			ClusterFragmented: true,
		},
	}
	pm.mu.Unlock()

	mockT := &mockTestingT{T: t}
	a := DefaultDegradationAssertion()
	level := pm.CheckDegradation(mockT, a, 100)
	assert.Equal(t, LevelFail, level)
}

func TestCheckDegradation_AllowDegraded(t *testing.T) {
	pm := NewPressureMonitor(nil, nil)
	pm.mu.Lock()
	pm.samples = []PressureSample{
		{LastL0Score: 12.0, ActiveRetries: 0, Goroutines: 100},
	}
	pm.mu.Unlock()

	mockT := &mockTestingT{T: t}
	a := DefaultDegradationAssertion()
	a.AllowDegraded = true
	level := pm.CheckDegradation(mockT, a, 100)
	assert.Equal(t, LevelDegraded, level)
}

func TestCountConsecutiveAbove_BaselineOffset(t *testing.T) {
	t.Parallel()
	// goroutines relative to baseline
	samples := []PressureSample{
		{Goroutines: 100},
		{Goroutines: 200},
		{Goroutines: 210},
		{Goroutines: 100},
	}
	assert.Equal(t, 2, CountConsecutiveAbove(samples, 100, 50))
}
