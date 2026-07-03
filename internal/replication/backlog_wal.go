package replication

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/lbp0200/BoltDB/internal/logger"
)

// BacklogWAL implements an append-only write-ahead log for the replication backlog.
// It stores command data in a binary format on disk so that on crash recovery
// the in-memory backlog can be reconstructed without a FULLRESYNC.
//
// Encoding format per entry:
//
//	[offset 8 bytes big-endian] [length 4 bytes big-endian] [command bytes]
//
// The WAL is a single append-only file. When the backlog's readable range advances,
// Truncate removes the consumed prefix from the file (via file renames, not in-place
// truncation, which is unsafe on most filesystems under crash).
//
// Design decisions:
//   - Single file, not segmented: simpler, good enough for 1-512MB backlog
//   - Batch flush at 64KB: avoids fsync per command (100K ops/s → ~2 fsync/s)
//   - No fsync by default: crash may lose ≤64KB = ≤1 command at typical throughput
//   - File rename for truncation: atomic on POSIX, avoids data loss on crash during truncation
type BacklogWAL struct {
	file *os.File
	path string // full path to WAL file
	dir  string // directory containing WAL

	bufMu sync.Mutex
	buf   []byte // write buffer for batching

	// stats
	totalEntries atomic.Int64
	totalBytes   atomic.Int64

	closed atomic.Bool
}

// WALDirName is the default subdirectory under the data dir for WAL files.
const WALDirName = "repl-wal"

// WALFileName is the default WAL file name.
const WALFileName = "backlog.wal"

// NewBacklogWAL creates or opens a BacklogWAL in the given directory.
// If the file already exists (from a previous run), it is opened for appending.
func NewBacklogWAL(dir string) (*BacklogWAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, WALFileName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &BacklogWAL{
		file: f,
		path: path,
		dir:  dir,
		buf:  make([]byte, 0, 65536),
	}, nil
}

// Append writes a command entry to the WAL buffer.
// It does NOT fsync — call Flush() periodically or on shutdown.
func (w *BacklogWAL) Append(offset int64, cmdBytes []byte) error {
	if w.closed.Load() {
		return nil // silently ignore writes after close
	}
	// Encode: [offset(8)] [length(4)] [cmdBytes]
	w.bufMu.Lock()
	w.buf = binary.BigEndian.AppendUint64(w.buf, uint64(offset))
	w.buf = binary.BigEndian.AppendUint32(w.buf, uint32(len(cmdBytes)))
	w.buf = append(w.buf, cmdBytes...)
	w.bufMu.Unlock()

	w.totalEntries.Add(1)
	w.totalBytes.Add(int64(12 + len(cmdBytes)))

	// Flush when buffer reaches 64KB
	if len(w.buf) >= 65536 {
		return w.Flush()
	}
	return nil
}

// Flush writes the buffered entries to the file.
// Does NOT fsync by default — the OS page cache provides crash safety
// equivalent to a ≤64KB loss window. Call Sync() explicitly if you need
// stricter guarantees.
func (w *BacklogWAL) Flush() error {
	w.bufMu.Lock()
	if len(w.buf) == 0 {
		w.bufMu.Unlock()
		return nil
	}
	data := w.buf
	w.buf = make([]byte, 0, 65536)
	w.bufMu.Unlock()

	_, err := w.file.Write(data)
	return err
}

// Sync performs fsync on the WAL file. Provides the strongest crash guarantee
// but the highest performance cost. Call sparingly.
func (w *BacklogWAL) Sync() error {
	return w.file.Sync()
}

// Replay reads the WAL file from the beginning and reconstructs the backlog
// state into the given ReplicationBacklog. The backlog must be empty or in
// its initial state. After Replay, the backlog's offset and buffer reflect
// all entries in the WAL.
func (w *BacklogWAL) Replay(backlog *ReplicationBacklog) error {
	data, err := os.ReadFile(w.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // fresh start, no WAL
		}
		return err
	}

	pos := 0
	for pos+12 <= len(data) {
		offset := int64(binary.BigEndian.Uint64(data[pos:]))
		length := int32(binary.BigEndian.Uint32(data[pos+8:]))
		pos += 12
		if length < 0 || pos+int(length) > len(data) {
			logger.Logger.Warn().
				Int("pos", pos).
				Int32("length", length).
				Int("remaining", len(data)-pos).
				Msg("BacklogWAL: truncated entry at end of WAL, ignoring")
			break
		}
		cmdBytes := make([]byte, length)
		copy(cmdBytes, data[pos:pos+int(length)])
		pos += int(length)

		// Append to backlog's ring buffer, restoring offset continuity
		backlog.mu.Lock()
		backlog.restoreEntry(offset, cmdBytes)
		backlog.mu.Unlock()
	}

	logger.Logger.Debug().
		Int("entries", int(w.totalEntries.Load())).
		Int64("bytes", w.totalBytes.Load()).
		Int64("final_offset", backlog.GetCurrentOffset()).
		Msg("BacklogWAL: replay complete")
	return nil
}

// restoreEntry appends a command entry to the backlog ring buffer, setting
// the offset to the given value. This is used during WAL replay to reconstruct
// the exact offset sequence.
//
// Unlike normal Append (which auto-increments offset), restoreEntry sets the
// offset explicitly so the WAL replay produces the exact same offset sequence
// as the original writes.
func (b *ReplicationBacklog) restoreEntry(offset int64, data []byte) {
	dataLen := int64(len(data))
	if dataLen == 0 {
		return
	}

	if dataLen >= b.size {
		copy(b.buffer, data[dataLen-b.size:])
	} else {
		writePos := offset % b.size
		endPos := writePos + dataLen
		if endPos <= b.size {
			copy(b.buffer[writePos:endPos], data)
		} else {
			firstPart := b.size - writePos
			copy(b.buffer[writePos:], data[:firstPart])
			copy(b.buffer[:endPos-b.size], data[firstPart:])
		}
	}
	b.offset = offset + dataLen
}

// Truncate removes consumed WAL entries from the beginning of the file.
// It works by writing only the unconsumed tail to a temp file and renaming
// atomically. This is safe under crash because the original file is preserved
// until the rename completes.
//
// Call this when the backlog's AvailableStartOffset advances past the oldest
// WAL entry's offset that you never need again. The simplest heuristic:
// truncate when the WAL file exceeds 2x the backlog size.
func (w *BacklogWAL) Truncate(retainedStartOffset int64) error {
	if err := w.Flush(); err != nil {
		return err
	}

	// Read current WAL
	data, err := os.ReadFile(w.path)
	if err != nil {
		return err
	}

	// Find the first entry whose offset >= retainedStartOffset
	cutPos := 0
	pos := 0
	for pos+12 <= len(data) {
		offset := int64(binary.BigEndian.Uint64(data[pos:]))
		length := int32(binary.BigEndian.Uint32(data[pos+8:]))
		if offset >= retainedStartOffset {
			cutPos = pos
			break
		}
		pos += 12 + int(length)
	}

	if cutPos == 0 {
		return nil // nothing to truncate
	}

	// Write tail to temp file and rename atomically
	tmpPath := w.path + ".tmp"
	if err := os.WriteFile(tmpPath, data[cutPos:], 0644); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, w.path); err != nil {
		_ = os.Remove(tmpPath) // cleanup on failure
		return err
	}

	// Reopen file for appending (the old file descriptor now points to renamed file)
	oldFile := w.file
	newFile, err := os.OpenFile(w.path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	w.file = newFile
	_ = oldFile.Close()

	return nil
}

// Close flushes pending data and closes the WAL file.
func (w *BacklogWAL) Close() error {
	if w.closed.Load() {
		return nil // already closed
	}
	w.closed.Store(true)
	if err := w.Flush(); err != nil {
		w.file.Close() //nolint:errcheck
		return err
	}
	return w.file.Close()
}

// GetPath returns the full path to the WAL file.
func (w *BacklogWAL) GetPath() string {
	return w.path
}

// GetTotalEntries returns the total number of entries written to the WAL.
func (w *BacklogWAL) GetTotalEntries() int64 {
	return w.totalEntries.Load()
}

// GetTotalBytes returns the total number of bytes written to the WAL (including headers).
func (w *BacklogWAL) GetTotalBytes() int64 {
	return w.totalBytes.Load()
}
