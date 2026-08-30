package replication

import (
	"bytes"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/zeebo/assert"
)

// The linearizable FULLRESYNC boundary (Issue #3):
//
//	RDB snapshot ∩ backlog gap [snapshotOffset, currentOffset) = ∅
//
// processRequest holds snapshotMu.RLock across executeCommand (commit) and
// PropagateCommand (backlog.Append = offset). FULLRESYNC holds the write lock
// across snapshotOffset → View, so it cannot start while a write is between
// commit and append.
func TestFullresyncBoundary_CommittedButUnpropagatedWrite(t *testing.T) {
	t.Parallel()

	s, err := store.NewBadgerStore(t.TempDir())
	assert.NoError(t, err)
	defer s.Close()

	rm := NewReplicationManager(s)
	defer rm.Stop()
	rm.SetRole(RoleMaster)

	assert.NoError(t, s.Set("warm", "0"))
	rm.PropagateCommand(replCmd("SET", "warm", "0"))

	// Production shape: handler fence around commit + propagate.
	s.SnapshotMuRLock()
	_, err = s.LPush("boundary:probe", "e1")
	assert.NoError(t, err)
	rm.PropagateCommand(replCmd("LPUSH", "boundary:probe", "e1"))
	s.SnapshotMuRUnlock()

	s.SnapshotMuLock()
	snapshotOffset := rm.GetMasterReplOffset()
	rdbData, err := GenerateRDBWithSnapshotLock(s)
	s.SnapshotMuUnlock()
	assert.NoError(t, err)

	currentOffset := rm.GetMasterReplOffset()
	var gap []byte
	if currentOffset > snapshotOffset {
		gap, err = rm.GetBacklog().GetRange(snapshotOffset, currentOffset)
		assert.NoError(t, err)
	}

	inRDB := bytes.Contains(rdbData, []byte("boundary:probe"))
	inGap := bytes.Contains(gap, []byte("boundary:probe"))
	if inRDB && inGap {
		t.Errorf("duplicate window is not zero: W is in the RDB snapshot AND in backlog [%d,%d)",
			snapshotOffset, currentOffset)
	}
	if !inRDB {
		t.Errorf("fenced write missing from RDB (offset=%d)", snapshotOffset)
	}
}

// TestFullresyncBoundary_FenceBlocksSnapshotWriteLock is the actual
// linearizability check: FULLRESYNC cannot take the write lock (and therefore
// cannot capture snapshotOffset / open View) while a write is between commit
// and PropagateCommand.
func TestFullresyncBoundary_FenceBlocksSnapshotWriteLock(t *testing.T) {
	t.Parallel()

	s, err := store.NewBadgerStore(t.TempDir())
	assert.NoError(t, err)
	defer s.Close()

	rm := NewReplicationManager(s)
	defer rm.Stop()
	rm.SetRole(RoleMaster)

	s.SnapshotMuRLock()
	gotWR := make(chan struct{})
	go func() {
		s.SnapshotMuLock()
		close(gotWR)
		s.SnapshotMuUnlock()
	}()
	time.Sleep(20 * time.Millisecond)

	_, err = s.LPush("boundary:probe", "e1")
	assert.NoError(t, err)

	select {
	case <-gotWR:
		t.Fatal("FULLRESYNC write lock acquired while commit-to-offset fence is held")
	case <-time.After(80 * time.Millisecond):
	}

	rm.PropagateCommand(replCmd("LPUSH", "boundary:probe", "e1"))
	s.SnapshotMuRUnlock()

	select {
	case <-gotWR:
	case <-time.After(2 * time.Second):
		t.Fatal("FULLRESYNC write lock did not acquire after fence release")
	}
}

func replCmd(args ...string) [][]byte {
	out := make([][]byte, len(args))
	for i, a := range args {
		out[i] = []byte(a)
	}
	return out
}
