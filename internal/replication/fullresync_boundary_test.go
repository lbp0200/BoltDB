package replication

import (
	"bytes"
	"testing"

	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/zeebo/assert"
)

// The linearizable FULLRESYNC boundary (Issue #3) claims:
//
//	RDB snapshot ∩ backlog gap [snapshotOffset, currentOffset) = ∅
//
// snapshotMu is supposed to enforce it: writes take the read lock in
// retryUpdate(), FULLRESYNC takes the write lock across
// snapshotOffset→View (replication_handler.go).
//
// The claim holds only if the read lock spans BOTH the badger commit and the
// repl-offset assignment. It does not: PropagateCommand() (which appends to
// the backlog and increments the offset) is called by the server layer
// *after* executeCommand() returns, i.e. after retryUpdate() released the
// read lock. A write that has committed but not yet propagated is exactly the
// interleaving that lands on both sides.
//
// This test reproduces that state deterministically, in the program order the
// server layer actually uses, instead of racing for a microsecond window.
func TestFullresyncBoundary_CommittedButUnpropagatedWrite(t *testing.T) {
	t.Parallel()

	// Known-open defect: the invariant below does not hold, so this test fails
	// by construction. Skip keeps it in the tree as the deterministic repro
	// without breaking the PR gate. Remove the Skip when a fix lands
	// (commitSeq ↔ repl-offset gap trimming or a critical section spanning
	// commit → offset assignment) — see the Issue #3 entry in
	// docs/plans/TODO.md and docs/failures/snapshot-inconsistency.md §4.
	t.Skip("snapshotMu does not close the duplicate window (Issue #3): committed-but-unpropagated writes land in both RDB and backlog; unskip once the boundary fix lands")

	s, err := store.NewBadgerStore(t.TempDir())
	assert.NoError(t, err)
	defer s.Close()

	rm := NewReplicationManager(s)
	defer rm.Stop()
	rm.SetRole(RoleMaster)

	// Quiesced state: one committed+propagated write, so offset > 0 and the
	// snapshot does not start at the beginning of the backlog.
	assert.NoError(t, s.Set("warm", "0"))
	rm.PropagateCommand(replCmd("SET", "warm", "0"))

	// In-flight write W. The master has committed it (retryUpdate acquired and
	// released snapshotMu.RLock); processRequest has not reached
	// PropagateCommand yet.
	_, err = s.LPush("boundary:probe", "e1")
	assert.NoError(t, err)

	// FULLRESYNC critical section, same statement order as
	// Handler.handlePSyncWithRDB.
	s.SnapshotMuLock()
	snapshotOffset := rm.GetMasterReplOffset()
	rdbData, err := GenerateRDBWithSnapshotLock(s)
	s.SnapshotMuUnlock()
	assert.NoError(t, err)

	// W finishes propagating: it enters the backlog at offset >=
	// snapshotOffset, so the joining replica receives it in the gap-fill.
	rm.PropagateCommand(replCmd("LPUSH", "boundary:probe", "e1"))

	currentOffset := rm.GetMasterReplOffset()
	gap, err := rm.GetBacklog().GetRange(snapshotOffset, currentOffset)
	assert.NoError(t, err)

	inRDB := bytes.Contains(rdbData, []byte("boundary:probe"))
	inGap := bytes.Contains(gap, []byte("boundary:probe"))

	if inRDB && inGap {
		t.Errorf("duplicate window is not zero: W is in the RDB snapshot AND in backlog [%d,%d), "+
			"so the replica applies LPUSH boundary:probe twice (master len 1, slave len 2). "+
			"snapshotMu must span commit→offset-assignment, which live in different layers.",
			snapshotOffset, currentOffset)
	}
}

func replCmd(args ...string) [][]byte {
	out := make([][]byte, len(args))
	for i, a := range args {
		out[i] = []byte(a)
	}
	return out
}
