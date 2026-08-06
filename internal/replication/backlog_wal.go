package replication

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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

	// fileMu guards the file handle against replacement by Truncate while a
	// concurrent Flush is writing. Writers take RLock; Truncate/Close take Lock.
	fileMu sync.RWMutex

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
	needFlush := false
	w.bufMu.Lock()
	w.buf = binary.BigEndian.AppendUint64(w.buf, uint64(offset))
	w.buf = binary.BigEndian.AppendUint32(w.buf, uint32(len(cmdBytes)))
	w.buf = append(w.buf, cmdBytes...)
	// Read the length under the lock: Truncate may swap w.buf concurrently.
	needFlush = len(w.buf) >= 65536
	w.bufMu.Unlock()

	w.totalEntries.Add(1)
	w.totalBytes.Add(int64(12 + len(cmdBytes)))

	// Flush when buffer reaches 64KB
	if needFlush {
		return w.Flush()
	}
	return nil
}

// Flush writes the buffered entries to the file.
// Does NOT fsync by default — the OS page cache provides crash safety
// equivalent to a ≤64KB loss window. Call Sync() explicitly if you need
// stricter guarantees.
func (w *BacklogWAL) Flush() error {
	data := w.takeBuffer()
	if len(data) == 0 {
		return nil
	}
	w.fileMu.RLock()
	_, err := w.file.Write(data)
	w.fileMu.RUnlock()
	return err
}

// takeBuffer extracts and clears the write buffer under bufMu.
func (w *BacklogWAL) takeBuffer() []byte {
	w.bufMu.Lock()
	defer w.bufMu.Unlock()
	if len(w.buf) == 0 {
		return nil
	}
	data := w.buf
	w.buf = make([]byte, 0, 65536)
	return data
}

// Sync performs fsync on the WAL file. Provides the strongest crash guarantee
// but the highest performance cost. Call sparingly.
func (w *BacklogWAL) Sync() error {
	return w.file.Sync()
}

// replayStreamThreshold: WAL files at or above this size use the streaming
// two-pass replay instead of reading the whole file into memory. The direct
// path keeps a multi-GB file from allocating multi-GB of RSS on startup.
var replayStreamThreshold int64 = 8 << 20 // 8MB

// Replay reads the WAL file from the beginning and reconstructs the backlog
// state into the given ReplicationBacklog. The backlog must be empty or in
// its initial state. After Replay, the backlog's offset and buffer reflect
// all entries in the WAL.
//
// Large files use a streaming two-pass scan (O(64KB) memory): the first pass
// finds the final offset, the second pass replays only the entries inside
// the live window [finalOffset - backlogSize, finalOffset).
func (w *BacklogWAL) Replay(backlog *ReplicationBacklog) error {
	fi, err := os.Stat(w.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // fresh start, no WAL
		}
		return err
	}
	if fi.Size() < replayStreamThreshold {
		return w.replayDirect(backlog)
	}
	return w.replayStreaming(backlog)
}

// replayDirect reads the whole WAL file and replays every entry (the
// original implementation, fine for bounded files).
func (w *BacklogWAL) replayDirect(backlog *ReplicationBacklog) error {
	data, err := os.ReadFile(w.path)
	if err != nil {
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

// replayStreaming replays a large WAL in two streaming passes with O(64KB)
// memory: the first pass finds the final offset, the second pass restores
// only entries inside the live window [finalOffset - size, finalOffset).
// Entries outside the window are skipped. This is equivalent to
// replayDirect (the ring buffer only retains the last `size` bytes anyway)
// but never allocates a whole-file buffer.
func (w *BacklogWAL) replayStreaming(backlog *ReplicationBacklog) error {
	// Pass 1: scan headers only (skip payloads) to find the final offset.
	var endOffset int64
	if err := w.scanHeaders(func(offset int64, length int32) error {
		endOffset = offset + int64(length)
		return nil
	}); err != nil {
		return err
	}

	retainStart := endOffset - backlog.GetSize()
	if retainStart < 0 {
		retainStart = 0
	}

	// Pass 2: restore only entries inside the live window.
	return w.scanFull(func(offset int64, data []byte) error {
		if offset < retainStart {
			return nil
		}
		backlog.mu.Lock()
		backlog.restoreEntry(offset, data)
		backlog.mu.Unlock()
		return nil
	})
}

// scanHeaders streams over WAL entry headers, discarding payloads.
// Returns nil on a clean EOF or a truncated tail entry (matching
// replayDirect's tolerance).
func (w *BacklogWAL) scanHeaders(fn func(offset int64, length int32) error) error {
	f, err := os.Open(w.path)
	if err != nil {
		return err
	}
	defer f.Close() //nolint:errcheck
	r := bufio.NewReaderSize(f, 64*1024)

	header := make([]byte, 12)
	for {
		if _, err := io.ReadFull(r, header); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil // clean EOF, or truncated tail entry: ignore
			}
			return err
		}
		offset := int64(binary.BigEndian.Uint64(header[0:8]))
		length := int32(binary.BigEndian.Uint32(header[8:12]))
		if length < 0 {
			return fmt.Errorf("backlog WAL corrupted: negative entry length at offset %d", offset)
		}
		if _, err := io.CopyN(io.Discard, r, int64(length)); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return err
		}
		if err := fn(offset, length); err != nil {
			return err
		}
	}
}

// scanFull streams over WAL entries, calling fn with each entry's payload.
// The payload slice is only valid during the callback. Returns nil on a
// clean EOF or a truncated tail entry.
func (w *BacklogWAL) scanFull(fn func(offset int64, data []byte) error) error {
	f, err := os.Open(w.path)
	if err != nil {
		return err
	}
	defer f.Close() //nolint:errcheck
	r := bufio.NewReaderSize(f, 64*1024)

	header := make([]byte, 12)
	for {
		if _, err := io.ReadFull(r, header); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil // clean EOF, or truncated tail entry: ignore
			}
			return err
		}
		offset := int64(binary.BigEndian.Uint64(header[0:8]))
		length := int32(binary.BigEndian.Uint32(header[8:12]))
		if length < 0 {
			return fmt.Errorf("backlog WAL corrupted: negative entry length at offset %d", offset)
		}
		data := make([]byte, length)
		if _, err := io.ReadFull(r, data); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return err
		}
		if err := fn(offset, data); err != nil {
			return err
		}
	}
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
	// Hold the exclusive lock for the whole replacement so no concurrent
	// Flush can write into the old file after it has been renamed away.
	w.fileMu.Lock()
	defer w.fileMu.Unlock()

	// Flush buffered entries to the current file first so no data is lost
	// during the file replacement.
	if data := w.takeBuffer(); len(data) > 0 {
		if _, err := w.file.Write(data); err != nil {
			return err
		}
	}

	// Read current WAL
	data, err := os.ReadFile(w.path)
	if err != nil {
		return err
	}

	// Find the first entry whose offset >= retainedStartOffset
	cutPos := -1
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

	if cutPos == -1 {
		// Every entry is consumed: drop the whole file.
		cutPos = len(data)
	}
	if cutPos == 0 {
		return nil // everything is retained, nothing to truncate
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

// GetFileSize returns the current size of the WAL file on disk.
func (w *BacklogWAL) GetFileSize() (int64, error) {
	w.fileMu.RLock()
	defer w.fileMu.RUnlock()
	st, err := w.file.Stat()
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}

// Close flushes pending data and closes the WAL file.
func (w *BacklogWAL) Close() error {
	if w.closed.Load() {
		return nil // already closed
	}
	w.closed.Store(true)
	w.fileMu.Lock()
	defer w.fileMu.Unlock()
	if data := w.takeBuffer(); len(data) > 0 {
		if _, err := w.file.Write(data); err != nil {
			w.file.Close() //nolint:errcheck
			return err
		}
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
