package integration

import (
	"testing"

	"github.com/lbp0200/BoltDB/internal/monitor"
	"github.com/lbp0200/BoltDB/internal/replication"
	"github.com/lbp0200/BoltDB/internal/store"
)

// Types re-exported from internal/monitor for backward compatibility
type PressureSample = monitor.PressureSample
type PressureMonitor = monitor.PressureMonitor
type DegradationLevel = monitor.DegradationLevel
type DegradationAssertion = monitor.DegradationAssertion
type CheckResult = monitor.CheckResult

const (
	LevelOK       = monitor.LevelOK
	LevelWarn     = monitor.LevelWarn
	LevelDegraded = monitor.LevelDegraded
	LevelFail     = monitor.LevelFail
)

func NewPressureMonitor(s *store.BotreonStore, r *replication.ReplicationManager) *monitor.PressureMonitor {
	return monitor.NewPressureMonitor(s, r)
}

func DefaultDegradationAssertion() monitor.DegradationAssertion {
	return monitor.DefaultDegradationAssertion()
}

func FormatSnapshot(s monitor.PressureSample) string {
	return monitor.FormatSnapshot(s)
}

// Keep the old method signatures working by adding them to a wrapper.
// The soak tests reference these via the integration package.
func init() {
	// Ensure the monitor.TestingT interface is satisfied by *testing.T
	_ = func(t *testing.T) monitor.TestingT { return t }
}
