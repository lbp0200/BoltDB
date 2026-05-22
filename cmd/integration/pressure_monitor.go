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

// Temporal analysis types
type TemporalSnapshot = monitor.TemporalSnapshot
type SlopeStats = monitor.SlopeStats
type OscillationStats = monitor.OscillationStats
type PersistenceStats = monitor.PersistenceStats
type RecoveryStats = monitor.RecoveryStats
type TemporalAnalysis = monitor.TemporalAnalysis
type TemporalAnalyzer = monitor.TemporalAnalyzer

// Basin analysis types
type BasinType = monitor.BasinType
type BasinTransition = monitor.BasinTransition
type BasinAttractorInfo = monitor.BasinAttractorInfo

const (
	TrajectoryStable           = monitor.TrajectoryStable
	TrajectoryRecovering       = monitor.TrajectoryRecovering
	TrajectoryDegrading        = monitor.TrajectoryDegrading
	TrajectoryOscillating      = monitor.TrajectoryOscillating
	TrajectoryStuck            = monitor.TrajectoryStuck
	TrajectoryInsufficientData = monitor.TrajectoryInsufficientData
)

const (
	BasinUnknown   = monitor.BasinUnknown
	BasinHealthy   = monitor.BasinHealthy
	BasinStressed  = monitor.BasinStressed
	BasinDegraded  = monitor.BasinDegraded
	BasinCollapsed = monitor.BasinCollapsed
)

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
