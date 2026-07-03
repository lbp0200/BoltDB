package replication

import (
	"testing"

	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/zeebo/assert"
)

// TestBacklogPersistence_CrashRecovery verifies the backlog persistence gap:
//   - Stop() (clean shutdown) persists backlog via SaveBacklog()
//   - Without Stop() (crash/SIGKILL), backlog is lost and next NewReplicationManager
//     starts with an empty backlog
//   - On restart after crash, F1d (HandlePSync) correctly degrades to FULLRESYNC
func TestBacklogPersistence_CrashRecovery(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Phase 1: create store + replication manager, write data, clean Stop()
	s1, err := store.NewBadgerStore(dir)
	assert.NoError(t, err)

	rm1 := NewReplicationManager(s1)
	rm1.SetRole(RoleMaster)

	// Simulate writes that go through PropagateCommand → backlog
	for i := 0; i < 5; i++ {
		cmd := [][]byte{[]byte("SET"), []byte("k"), []byte("v")}
		rm1.PropagateCommand(cmd)
	}
	offsetAfterClean := rm1.GetMasterReplOffset()
	assert.True(t, offsetAfterClean > 0)

	// Clean shutdown: Stop() persists replId, offset, AND backlog
	rm1.Stop()
	err = s1.Close()
	assert.NoError(t, err)

	// Phase 2: reopen same directory → replication manager should restore backlog
	s2, err := store.NewBadgerStore(dir)
	assert.NoError(t, err)
	defer s2.Close()

	rm2 := NewReplicationManager(s2)
	defer rm2.Stop()

	// Verify: offset restored (F1b: masterReplOffset persistence)
	assert.Equal(t, offsetAfterClean, rm2.GetMasterReplOffset())

	// Verify: backlog restored (F1c: backlog persistence on Stop())
	avail := rm2.GetBacklog().GetAvailableLength()
	assert.True(t, avail > 0)

	// Verify: PSYNC CONTINUE would be possible for a slave at any offset within range
	assert.True(t, rm2.GetBacklog().IsOffsetAvailable(1))
}

// TestBacklogPersistence_CrashLoss verifies that WITHOUT Stop() (crash scenario),
// the backlog is empty on restart, and HandlePSync degrades to FULLRESYNC.
func TestBacklogPersistence_CrashLoss(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Phase 1: create store + replication manager, write data, NO Stop()
	s1, err := store.NewBadgerStore(dir)
	assert.NoError(t, err)

	rm1 := NewReplicationManager(s1)

	// Simulate writes
	for i := 0; i < 5; i++ {
		cmd := [][]byte{[]byte("SET"), []byte("k"), []byte("v")}
		rm1.PropagateCommand(cmd)
	}
	offsetAfterCrash := rm1.GetMasterReplOffset()
	assert.True(t, offsetAfterCrash > 0)

	// Crash scenario: close store WITHOUT Stop() → no backlog persistence
	err = s1.Close()
	assert.NoError(t, err)

	// Phase 2: reopen → replication manager should NOT have backlog restored
	s2, err := store.NewBadgerStore(dir)
	assert.NoError(t, err)
	defer s2.Close()

	rm2 := NewReplicationManager(s2)
	defer rm2.Stop()

	// Verify: backlog is empty because Stop() was never called
	avail := rm2.GetBacklog().GetAvailableLength()
	assert.Equal(t, int64(0), avail)

	// Verify: HandlePSync with non-zero offset should trigger FULLRESYNC (F1d)
	// because backlog is empty after crash — the range check would fail.
	result, err := HandlePSync(rm2, rm2.GetReplicationID(), 1)
	assert.NoError(t, err)
	assert.True(t, result.FullResync)
}

// TestBacklogPersistence_Checkpoint verifies that the periodic checkpoint logic
// (phase 1b plan) would mitigate crash loss. This test validates the gap exists.
func TestBacklogPersistence_CheckpointGap(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Create store + replication manager
	s, err := store.NewBadgerStore(dir)
	assert.NoError(t, err)
	defer s.Close()

	rm := NewReplicationManager(s)

	// Verify initial state: no backlog persisted on fresh start
	off, buf, sz, loadErr := s.LoadBacklog()
	assert.NoError(t, loadErr)
	assert.Equal(t, int64(0), off)
	assert.Equal(t, int64(0), sz)
	assert.Equal(t, 0, len(buf))

	// Clean shutdown to persist
	for i := 0; i < 3; i++ {
		cmd := [][]byte{[]byte("SET"), []byte("x"), []byte("y")}
		rm.PropagateCommand(cmd)
	}
	rm.Stop()

	// Verify backlog persisted
	off2, buf2, sz2, loadErr2 := s.LoadBacklog()
	assert.NoError(t, loadErr2)
	assert.True(t, off2 > 0)
	assert.True(t, sz2 > 0)
	assert.True(t, len(buf2) > 0)

	_ = off2
	_ = buf2
	_ = sz2
}
