package replication

import (
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/zeebo/assert"
)

func TestBacklogWAL_New(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	wal, err := NewBacklogWAL(dir)
	assert.NoError(t, err)
	defer wal.Close()

	assert.True(t, len(wal.GetPath()) > 0)
	assert.Equal(t, int64(0), wal.GetTotalEntries())
	assert.Equal(t, int64(0), wal.GetTotalBytes())
}

func TestBacklogWAL_AppendAndReplay(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	wal, err := NewBacklogWAL(dir)
	assert.NoError(t, err)
	defer wal.Close()

	// Append some entries (each command is 27 bytes)
	err = wal.Append(0, []byte("*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n"))
	assert.NoError(t, err)
	err = wal.Append(27, []byte("*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n"))
	assert.NoError(t, err)
	err = wal.Append(54, []byte("*3\r\n$3\r\nSET\r\n$1\r\nc\r\n$1\r\n3\r\n"))
	assert.NoError(t, err)

	err = wal.Flush()
	assert.NoError(t, err)

	assert.Equal(t, int64(3), wal.GetTotalEntries())
	assert.True(t, wal.GetTotalBytes() > 0)

	// Replay into a new backlog
	backlog := NewReplicationBacklog(100)
	err = wal.Replay(backlog)
	assert.NoError(t, err)

	// Verify replay: offset should match last entry's end
	assert.Equal(t, int64(81), backlog.GetCurrentOffset())
	avail := backlog.GetAvailableLength()
	assert.True(t, avail > 0)

	// Verify data accessible
	data, err := backlog.GetRange(0, 27)
	assert.NoError(t, err)
	assert.Equal(t, "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n", string(data))
}

func TestBacklogWAL_AppendAndReplay_CircularOverwrite(t *testing.T) {
	// Not parallel — heavy BadgerDB write + WAL I/O, avoid contention
	dir := t.TempDir()
	wal, err := NewBacklogWAL(dir)
	assert.NoError(t, err)
	defer wal.Close()

	// Append enough entries to fill a small backlog multiple times
	backlog := NewReplicationBacklog(50)
	cmdBytes := []byte("*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$1\r\n1\r\n") // 27 bytes

	// Write 3 commands (81 bytes → backlog wraps around once or twice)
	for i := 0; i < 3; i++ {
		offset := int64(i * 27)
		err = wal.Append(offset, cmdBytes)
		assert.NoError(t, err)
		backlog.Append(cmdBytes) // Append already acquires mu internally
	}

	err = wal.Flush()
	assert.NoError(t, err)

	// Close and reopen WAL
	err = wal.Close()
	assert.NoError(t, err)

	wal2, err := NewBacklogWAL(dir)
	assert.NoError(t, err)
	defer wal2.Close()

	// Replay into empty backlog
	replayed := NewReplicationBacklog(50)
	err = wal2.Replay(replayed)
	assert.NoError(t, err)

	// Verify same offset as original
	assert.Equal(t, backlog.GetCurrentOffset(), replayed.GetCurrentOffset())

	// Verify available data matches
	origAvail := backlog.GetAvailableLength()
	replayAvail := replayed.GetAvailableLength()
	assert.Equal(t, origAvail, replayAvail)
}

func TestBacklogWAL_Replay_EmptyFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	wal, err := NewBacklogWAL(dir)
	assert.NoError(t, err)
	wal.Close()

	// Reopen and replay
	wal2, err := NewBacklogWAL(dir)
	assert.NoError(t, err)
	defer wal2.Close()

	backlog := NewReplicationBacklog(100)
	err = wal2.Replay(backlog)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), backlog.GetCurrentOffset())
}

func TestBacklogWAL_FlushEmpty(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	wal, err := NewBacklogWAL(dir)
	assert.NoError(t, err)
	defer wal.Close()

	// Flush with empty buffer should be a no-op
	err = wal.Flush()
	assert.NoError(t, err)
}

func TestBacklogWAL_CloseFlushes(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	wal, err := NewBacklogWAL(dir)
	assert.NoError(t, err)

	// Append without explicit flush
	err = wal.Append(0, []byte("*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n"))
	assert.NoError(t, err)

	// Close should flush
	err = wal.Close()
	assert.NoError(t, err)

	// Verify data persisted to disk
	data, err := os.ReadFile(filepath.Join(dir, "backlog.wal"))
	assert.NoError(t, err)
	assert.True(t, len(data) > 0)
}

func TestBacklogWAL_Truncate(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	wal, err := NewBacklogWAL(dir)
	assert.NoError(t, err)
	defer wal.Close()

	// Write entries at offsets 0, 27, 54, 81 (each command is 27 bytes)
	for i := 0; i < 4; i++ {
		offset := int64(i * 27)
		err = wal.Append(offset, []byte("*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$1\r\n1\r\n"))
		assert.NoError(t, err)
	}
	err = wal.Flush()
	assert.NoError(t, err)

	// Truncate everything before offset 54 (keep last 2 entries)
	err = wal.Truncate(54)
	assert.NoError(t, err)

	// Read remaining file
	data, err := os.ReadFile(wal.GetPath())
	assert.NoError(t, err)
	assert.True(t, len(data) < 4*30) // should be smaller than original
}

func TestBacklogWAL_Truncate_NotNeeded(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	wal, err := NewBacklogWAL(dir)
	assert.NoError(t, err)
	defer wal.Close()

	err = wal.Append(0, []byte("*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n"))
	assert.NoError(t, err)
	err = wal.Flush()
	assert.NoError(t, err)

	// Truncate with offset 0 (nothing to truncate)
	err = wal.Truncate(0)
	assert.NoError(t, err)
}

func TestBacklogWAL_ReplayTruncated(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	wal, err := NewBacklogWAL(dir)
	assert.NoError(t, err)

	// Write 4 entries (each command is 27 bytes)
	for i := 0; i < 4; i++ {
		offset := int64(i * 27)
		err = wal.Append(offset, []byte("*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$1\r\n1\r\n"))
		assert.NoError(t, err)
	}
	err = wal.Flush()
	assert.NoError(t, err)

	// Truncate at offset 54 (keep entries at offset 54 and 81)
	err = wal.Truncate(54)
	assert.NoError(t, err)

	err = wal.Close()
	assert.NoError(t, err)

	// Reopen and replay
	wal2, err := NewBacklogWAL(dir)
	assert.NoError(t, err)
	defer wal2.Close()

	backlog := NewReplicationBacklog(100)
	err = wal2.Replay(backlog)
	assert.NoError(t, err)

	// Should have the last 2 entries (offset 54 + 27 = 81, then 81 + 27 = 108)
	assert.Equal(t, int64(108), backlog.GetCurrentOffset())
}

func TestBacklogWAL_Truncate_AllConsumed(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	wal, err := NewBacklogWAL(dir)
	assert.NoError(t, err)
	defer wal.Close()

	// Write 4 entries (each command is 27 bytes)
	for i := 0; i < 4; i++ {
		offset := int64(i * 27)
		err = wal.Append(offset, []byte("*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$1\r\n1\r\n"))
		assert.NoError(t, err)
	}
	err = wal.Flush()
	assert.NoError(t, err)

	// Truncate past the last entry: everything is consumed, file must be emptied
	err = wal.Truncate(4 * 27)
	assert.NoError(t, err)

	data, err := os.ReadFile(wal.GetPath())
	assert.NoError(t, err)
	assert.Equal(t, 0, len(data))
}

func TestBacklogWAL_Truncate_ConcurrentAppend(t *testing.T) {
	// Not parallel — WAL I/O, avoid contention
	dir := t.TempDir()
	wal, err := NewBacklogWAL(dir)
	assert.NoError(t, err)
	defer wal.Close()

	const total = 400
	cmd := []byte("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n") // 27 bytes
	var progress atomic.Int64                                  // bytes appended so far
	done := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(done)
		for i := 0; i < total; i++ {
			if err := wal.Append(int64(i*27), cmd); err != nil {
				t.Errorf("append %d: %v", i, err)
				return
			}
			progress.Store(int64(i+1) * 27)
			if i%50 == 49 {
				if err := wal.Flush(); err != nil {
					t.Errorf("flush: %v", err)
					return
				}
			}
		}
	}()

	// Truncate concurrently, keeping only the last 2 entries written so far.
	go func() {
		for {
			select {
			case <-done:
				return
			default:
			}
			retain := progress.Load() - 2*27
			if retain > 0 {
				if err := wal.Truncate(retain); err != nil {
					t.Errorf("truncate: %v", err)
					return
				}
			}
			runtime.Gosched()
		}
	}()

	wg.Wait()

	// Replay: final offset must match the full write stream, and the last two
	// entries must be intact (they are always inside the retained window).
	err = wal.Close()
	assert.NoError(t, err)

	wal2, err := NewBacklogWAL(dir)
	assert.NoError(t, err)
	defer wal2.Close()

	replayed := NewReplicationBacklog(4096)
	err = wal2.Replay(replayed)
	assert.NoError(t, err)
	assert.Equal(t, int64(total*27), replayed.GetCurrentOffset())

	got, err := replayed.GetRange(int64(total*27)-2*27, int64(total*27))
	assert.NoError(t, err)
	assert.Equal(t, string(cmd)+string(cmd), string(got))
}

func TestBacklogWAL_GetFileSize(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	wal, err := NewBacklogWAL(dir)
	assert.NoError(t, err)
	defer wal.Close()

	sz, err := wal.GetFileSize()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), sz)

	err = wal.Append(0, []byte("*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n"))
	assert.NoError(t, err)
	err = wal.Flush()
	assert.NoError(t, err)

	sz, err = wal.GetFileSize()
	assert.NoError(t, err)
	assert.True(t, sz > 0)
}

func TestBacklogWAL_Append_AfterClose(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	wal, err := NewBacklogWAL(dir)
	assert.NoError(t, err)

	err = wal.Close()
	assert.NoError(t, err)

	// Append after close should not panic or error
	err = wal.Append(0, []byte("*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n"))
	assert.NoError(t, err) // silently ignored
}

// TestBacklogWAL_CrashRecovery simulates a crash by not calling Close().
// The WAL file should still have data that can be replayed on restart.
func TestBacklogWAL_CrashRecovery(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	wal, err := NewBacklogWAL(dir)
	assert.NoError(t, err)

	// Write entries (each command is 27 bytes)
	for i := 0; i < 3; i++ {
		offset := int64(i * 27)
		err = wal.Append(offset, []byte("*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$1\r\n1\r\n"))
		assert.NoError(t, err)
	}
	err = wal.Flush()
	assert.NoError(t, err)

	// Don't Close() — simulate crash. Just lose the reference.
	_ = wal

	// Reopen as if restarting after crash
	wal2, err := NewBacklogWAL(dir)
	assert.NoError(t, err)
	defer wal2.Close()

	backlog := NewReplicationBacklog(100)
	err = wal2.Replay(backlog)
	assert.NoError(t, err)

	// Should have recovered all 3 entries
	assert.True(t, backlog.GetCurrentOffset() > 0)
}

// TestBacklogWAL_Replay_Streaming verifies the streaming two-pass replay
// path (used for WAL files >= replayStreamThreshold) produces exactly the
// same backlog state as the direct whole-file replay.
func TestBacklogWAL_Replay_Streaming(t *testing.T) {
	// Not parallel — mutates the package-level replayStreamThreshold.
	oldThreshold := replayStreamThreshold
	replayStreamThreshold = 512 // force streaming path for any non-empty file
	defer func() { replayStreamThreshold = oldThreshold }()

	dir := t.TempDir()
	wal, err := NewBacklogWAL(dir)
	assert.NoError(t, err)
	defer wal.Close()

	cmd := []byte("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n") // 27 bytes
	const n = 100
	for i := 0; i < n; i++ {
		err = wal.Append(int64(i*27), cmd)
		assert.NoError(t, err)
	}
	err = wal.Flush()
	assert.NoError(t, err)

	// Streaming replay must reconstruct the same offset as direct replay.
	streamed := NewReplicationBacklog(4096)
	err = wal.Replay(streamed)
	assert.NoError(t, err)
	assert.Equal(t, int64(n*27), streamed.GetCurrentOffset())

	// Content within the window must match the source bytes.
	got, err := streamed.GetRange(int64(n*27)-3*27, int64(n*27))
	assert.NoError(t, err)
	assert.Equal(t, string(cmd)+string(cmd)+string(cmd), string(got))
}

// TestBacklogWAL_Replay_Streaming_CircularOverwrite verifies the streaming
// path skips entries outside the live window identically to the direct path.
func TestBacklogWAL_Replay_Streaming_CircularOverwrite(t *testing.T) {
	oldThreshold := replayStreamThreshold
	replayStreamThreshold = 512
	defer func() { replayStreamThreshold = oldThreshold }()

	dir := t.TempDir()
	wal, err := NewBacklogWAL(dir)
	assert.NoError(t, err)
	defer wal.Close()

	cmd := []byte("*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$1\r\n1\r\n") // 27 bytes
	const n = 100
	for i := 0; i < n; i++ {
		err = wal.Append(int64(i*27), cmd)
		assert.NoError(t, err)
	}
	err = wal.Flush()
	assert.NoError(t, err)

	// Small backlog (50 bytes): ring wraps many times, only the last 50
	// bytes are available.
	streamed := NewReplicationBacklog(50)
	err = wal.Replay(streamed)
	assert.NoError(t, err)
	assert.Equal(t, int64(n*27), streamed.GetCurrentOffset())

	// Direct (whole-file) replay must give the identical state.
	replayStreamThreshold = oldThreshold
	direct := NewReplicationBacklog(50)
	err = wal.Replay(direct)
	assert.NoError(t, err)
	assert.Equal(t, direct.GetCurrentOffset(), streamed.GetCurrentOffset())
	assert.Equal(t, direct.GetAvailableLength(), streamed.GetAvailableLength())
}

// TestBacklogWAL_Replay_Streaming_TruncatedTail verifies the streaming path
// tolerates a truncated entry at the end of the WAL (crash residue).
func TestBacklogWAL_Replay_Streaming_TruncatedTail(t *testing.T) {
	oldThreshold := replayStreamThreshold
	replayStreamThreshold = 512
	defer func() { replayStreamThreshold = oldThreshold }()

	dir := t.TempDir()
	wal, err := NewBacklogWAL(dir)
	assert.NoError(t, err)
	defer wal.Close()

	cmd := []byte("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n") // 27 bytes
	for i := 0; i < 5; i++ {
		err = wal.Append(int64(i*27), cmd)
		assert.NoError(t, err)
	}
	err = wal.Flush()
	assert.NoError(t, err)

	// Simulate a crash mid-entry: append 10 garbage bytes to the file.
	f, err := os.OpenFile(wal.GetPath(), os.O_APPEND|os.O_WRONLY, 0644)
	assert.NoError(t, err)
	_, err = f.Write([]byte("garbage123"))
	assert.NoError(t, err)
	assert.NoError(t, f.Close())

	// Streaming replay must ignore the truncated tail and keep full entries.
	backlog := NewReplicationBacklog(4096)
	err = wal.Replay(backlog)
	assert.NoError(t, err)
	assert.Equal(t, int64(5*27), backlog.GetCurrentOffset())
}
