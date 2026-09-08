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
//	RDB snapshot captures store state at a consistent ts point; writes committed
//	before the snapshot MUST appear in the RDB. post-snapshot writes have higher
//	ts and are by construction NOT in the RDB — no byte-level "duplicate window"
//	exists in ts-domain (the log IS the source; RDB reads from store state).
//
// processRequest holds snapshotMu.RLock across executeCommand (commit);
// FULLRESYNC holds the write lock across View, so it cannot start while a
// write is between commit and the next read.
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
	rdbData, err := GenerateRDBWithSnapshotLock(s)
	s.SnapshotMuUnlock()
	assert.NoError(t, err)

	// Fenced write (committed before snapshot point) MUST be in the RDB.
	if !bytes.Contains(rdbData, []byte("boundary:probe")) {
		t.Error("fenced write missing from RDB snapshot")
	}

	// ts 透镜：fenced 写经 commit 必已写传播日志键——快照一致性 ts 侧断言。
	logEntries, err := s.ReplLogEntries()
	assert.NoError(t, err)
	fencedLogFound := false
	for _, e := range logEntries {
		if bytes.Contains(e.Value, []byte("boundary:probe")) {
			fencedLogFound = true
			break
		}
	}
	if !fencedLogFound {
		t.Errorf("fenced write's repl log entry missing (ts lens)")
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
