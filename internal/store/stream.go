package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/lbp0200/BoltDB/internal/logger"
)

const (
	prefixStream  = "stream:"
	streamMeta    = ":meta"
	streamData    = ":data"
	streamGroups  = ":groups"
	streamPending = ":pending"
)

// StreamEntry represents a single entry in a stream
type StreamEntry struct {
	ID        string
	Fields    map[string]string
	Timestamp int64 // Milliseconds for ordering
	Sequence  int64 // Sequence number within the same millisecond
}

// StreamGroup represents a consumer group
type StreamGroup struct {
	Name            string
	LastDeliveredID string
	Consumers       map[string]*StreamConsumer
	Pending         map[string]*StreamPendingEntry // ID -> pending info
}

// StreamConsumer represents a consumer within a group
type StreamConsumer struct {
	Name     string
	LastSeen int64 // Timestamp of last seen
}

// StreamPendingEntry represents a pending entry in a consumer group
type StreamPendingEntry struct {
	ID            string
	Consumer      string
	DeliveryCount int64
	LastDelivery  int64
}

// StreamInfo contains stream metadata
type StreamInfo struct {
	Length         int64
	FirstID        string
	LastID         string
	MaxDeletedID   string
	Groups         map[string]*StreamGroup
	RadixTreeKeys  int64
	RadixTreeNodes int64
}

// StreamXAddOptions contains options for XADD
type StreamXAddOptions struct {
	MaxLen       int64  // Maximum number of entries
	MaxLenApprox int64  // Approximate maximum (more efficient)
	MinID        string // Only add entries with ID >= minID
}

// parseStreamID parses a stream ID string to (timestamp, sequence)
func parseStreamID(id string) (int64, int64, error) {
	if id == "*" {
		// Auto-generate based on current time
		now := time.Now().UnixNano() / int64(time.Millisecond)
		return now, 0, nil
	}

	// Check for special IDs
	if id == "+" || id == "-" {
		return 0, 0, errors.New("invalid stream ID format")
	}

	// Handle ID format: timestamp-sequence or just timestamp
	parts := strings.Split(id, "-")
	if len(parts) == 1 {
		ts, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("invalid stream ID: %s", id)
		}
		return ts, 0, nil
	} else if len(parts) == 2 {
		ts, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("invalid stream ID timestamp: %s", id)
		}
		seq, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("invalid stream ID sequence: %s", id)
		}
		return ts, seq, nil
	}

	return 0, 0, fmt.Errorf("invalid stream ID format: %s", id)
}

// formatStreamID formats (timestamp, sequence) to string
func formatStreamID(timestamp, sequence int64) string {
	return fmt.Sprintf("%d-%d", timestamp, sequence)
}

// compareStreamID compares two stream IDs
// Returns -1 if id1 < id2, 0 if equal, 1 if id1 > id2
func compareStreamID(id1, id2 string) int {
	t1, s1, _ := parseStreamID(id1)
	t2, s2, _ := parseStreamID(id2)
	if t1 < t2 {
		return -1
	}
	if t1 > t2 {
		return 1
	}
	if s1 < s2 {
		return -1
	}
	if s1 > s2 {
		return 1
	}
	return 0
}

// streamKey returns the key for stream metadata
func streamKey(key string) []byte {
	return []byte(prefixStream + key + streamMeta)
}

// streamDataKey returns the key for stream entry data
func streamDataKey(key, id string) []byte {
	return []byte(prefixStream + key + streamData + ":" + id)
}

// streamDataPrefix returns the prefix for all entry data keys
func streamDataPrefix(key string) []byte {
	return []byte(prefixStream + key + streamData + ":")
}

// streamGroupKey returns the key for consumer groups
func streamGroupKey(key string) []byte {
	return []byte(prefixStream + key + streamGroups)
}

// streamGroupDataKey returns the key for a specific group
func streamGroupDataKey(key, group string) []byte {
	return []byte(prefixStream + key + streamGroups + ":" + group)
}

//nolint:unused
func streamPendingKey(key, group string) []byte {
	return []byte(prefixStream + key + streamPending + ":" + group)
}

// encodeStreamMeta encodes stream metadata
type streamMetaData struct {
	Length       int64
	FirstID      int64 // timestamp
	FirstSeq     int64
	LastID       int64 // timestamp
	LastSeq      int64
	MaxDeletedID int64 // timestamp
	MaxDelSeq    int64
}

func encodeStreamMeta(m *streamMetaData) []byte {
	b := make([]byte, 48)
	binary.BigEndian.PutUint64(b[:8], uint64(m.Length))
	binary.BigEndian.PutUint64(b[8:16], uint64(m.FirstID))
	binary.BigEndian.PutUint64(b[16:24], uint64(m.FirstSeq))
	binary.BigEndian.PutUint64(b[24:32], uint64(m.LastID))
	binary.BigEndian.PutUint64(b[32:40], uint64(m.LastSeq))
	binary.BigEndian.PutUint64(b[40:48], uint64(m.MaxDeletedID))
	return b
}

func decodeStreamMeta(b []byte) (*streamMetaData, error) {
	if len(b) != 48 {
		return nil, errors.New("invalid stream metadata size")
	}
	m := &streamMetaData{}
	m.Length = int64(binary.BigEndian.Uint64(b[:8]))
	m.FirstID = int64(binary.BigEndian.Uint64(b[8:16]))
	m.FirstSeq = int64(binary.BigEndian.Uint64(b[16:24]))
	m.LastID = int64(binary.BigEndian.Uint64(b[24:32]))
	m.LastSeq = int64(binary.BigEndian.Uint64(b[32:40]))
	m.MaxDeletedID = int64(binary.BigEndian.Uint64(b[40:48]))
	return m, nil
}

// CreateEmptyStream creates an empty stream with just the TYPE_ key and metadata
func (s *BotreonStore) CreateEmptyStream(key string) error {
	return s.retryUpdate(func(txn *badger.Txn) error {
		// Set type key
		typeKey := TypeOfKeyGet(key)
		if err := txn.Set(typeKey, []byte(KeyTypeStream)); err != nil {
			logger.Logger.Error().Err(err).Str("key", key).Msg("CreateEmptyStream: Failed to set type")
			return err
		}

		// Create empty metadata
		meta := &streamMetaData{
			Length:       0,
			FirstID:      0,
			FirstSeq:     0,
			LastID:       0,
			LastSeq:      0,
			MaxDeletedID: 0,
		}
		return txn.Set(streamKey(key), encodeStreamMeta(meta))
	}, 30)
}

// XAdd adds a new entry to a stream
func (s *BotreonStore) XAdd(key string, opts StreamXAddOptions, id string, fields map[string]string) (string, error) {
	var resultID string

	err := s.retryUpdate(func(txn *badger.Txn) error {
		// Check type first
		typeKey := TypeOfKeyGet(key)
		typeItem, typeErr := txn.Get(typeKey)
		if typeErr == nil {
			typeVal, _ := typeItem.ValueCopy(nil)
			if string(typeVal) != "" && string(typeVal) != KeyTypeStream {
				return ErrWrongType
			}
		} else if !errors.Is(typeErr, badger.ErrKeyNotFound) {
			return typeErr
		}

		// Set type key
		if err := txn.Set(typeKey, []byte(KeyTypeStream)); err != nil {
			logger.Logger.Error().Err(err).Str("key", key).Msg("XAdd: Failed to set type")
			return err
		}

		// Get or create metadata
		metaKey := streamKey(key)
		var meta *streamMetaData

		item, err := txn.Get(metaKey)
		if err == nil && !errors.Is(err, badger.ErrKeyNotFound) {
			err = item.Value(func(val []byte) error {
				meta, err = decodeStreamMeta(val)
				return err
			})
			if err != nil {
				return err
			}
		} else {
			meta = &streamMetaData{}
		}

		// Parse or generate ID
		if id == "" || id == "*" {
			// Auto-generate ID
			timestamp := time.Now().UnixNano() / int64(time.Millisecond)
			if timestamp == meta.LastID {
				meta.LastSeq++
			} else {
				meta.LastSeq = 0
			}
			meta.LastID = timestamp
			id = formatStreamID(meta.LastID, meta.LastSeq)
		} else {
			// User provided explicit ID - preserve it as-is
			ts, seq, err := parseStreamID(id)
			if err != nil {
				return err
			}
			// Check if ID is valid (must be greater than last ID)
			if compareStreamID(id, formatStreamID(meta.LastID, meta.LastSeq)) < 0 {
				return fmt.Errorf("ERR ID must be greater than the last entry ID (%d-%d)", meta.LastID, meta.LastSeq)
			}
			// Update metadata but keep the original ID string
			meta.LastID = ts
			meta.LastSeq = seq
			// Do NOT reformat the ID - preserve user's exact format (e.g. "1000000000000-0" keeps "-0")
		}

		// Check MINID if specified
		if opts.MinID != "" {
			if compareStreamID(id, opts.MinID) < 0 {
				return fmt.Errorf("ERR ID must be >= MINID (%s)", opts.MinID)
			}
		}

		// Store entry data
		entryData, err := json.Marshal(fields)
		if err != nil {
			return err
		}
		dataKey := streamDataKey(key, id)
		if err := txn.Set(dataKey, entryData); err != nil {
			return err
		}

		// Update metadata
		meta.Length++
		if meta.Length == 1 {
			meta.FirstID = meta.LastID
			meta.FirstSeq = meta.LastSeq
		}

		// Handle MAXLEN
		if opts.MaxLen > 0 {
			// Calculate how many entries to remove
			entriesToRemove := meta.Length - opts.MaxLen
			if entriesToRemove > 0 {
				// Remove oldest entries
				removeMeta := &streamMetaData{
					Length:       entriesToRemove,
					FirstID:      meta.FirstID,
					FirstSeq:     meta.FirstSeq,
					MaxDeletedID: meta.FirstID,
					MaxDelSeq:    meta.FirstSeq,
				}
				// Update first ID to the next entry
				// For simplicity, we just iterate and delete
				prefix := streamDataPrefix(key)
				it := txn.NewIterator(badger.DefaultIteratorOptions)
				defer it.Close()

				count := int64(0)
				currentID := meta.FirstID
				currentSeq := meta.FirstSeq
				for it.Seek(prefix); it.ValidForPrefix(prefix) && count < entriesToRemove; it.Next() {
					item := it.Item()
					_ = txn.Delete(item.Key())
					count++
					// Move to next ID
					currentSeq++
					if count < entriesToRemove {
						removeMeta.MaxDelSeq = currentSeq
					}
				}
				meta.Length -= count
				if meta.Length > 0 {
					// Set first ID to the next entry
					currentSeqStr := formatStreamID(currentID, currentSeq)
					nextKey := streamDataKey(key, currentSeqStr)
					_, err := txn.Get(nextKey)
					if err == nil || errors.Is(err, badger.ErrKeyNotFound) {
						// Found next entry, set as first
						nextTS, nextSeq, _ := parseStreamID(currentSeqStr)
						meta.FirstID = nextTS
						meta.FirstSeq = nextSeq
					}
				} else {
					meta.FirstID = meta.LastID
					meta.FirstSeq = meta.LastSeq
				}
				meta.MaxDeletedID = removeMeta.MaxDeletedID
				meta.MaxDelSeq = removeMeta.MaxDelSeq
			}
		}

		// Save metadata
		if err := txn.Set(metaKey, encodeStreamMeta(meta)); err != nil {
			return err
		}

		resultID = id
		return nil
	}, 30)

	// Notify waiting stream readers
	if err == nil && resultID != "" {
		s.notifyStreamRead(key, []StreamEntry{
			{
				ID:     resultID,
				Fields: fields,
			},
		})
	}

	return resultID, err
}

// XLen returns the number of entries in a stream
func (s *BotreonStore) XLen(key string) (int64, error) {
	var length int64

	err := s.db.View(func(txn *badger.Txn) error {
		metaKey := streamKey(key)
		item, err := txn.Get(metaKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			meta, err := decodeStreamMeta(val)
			if err != nil {
				return err
			}
			length = meta.Length
			return nil
		})
	})

	return length, err
}

// XRead reads entries from one or more streams
func (s *BotreonStore) XRead(ctx context.Context, count int64, block int64, args ...string) ([]map[string][]StreamEntry, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(args) < 2 || len(args)%2 != 0 {
		return nil, errors.New("ERR wrong number of arguments for 'XREAD' command")
	}

	result := make([]map[string][]StreamEntry, 0)

	err := s.db.View(func(txn *badger.Txn) error {
		for i := 0; i < len(args); i += 2 {
			key := args[i]
			startID := args[i+1]

			// Get stream metadata
			metaKey := streamKey(key)
			var meta *streamMetaData
			item, err := txn.Get(metaKey)
			if errors.Is(err, badger.ErrKeyNotFound) {
				continue // Skip non-existent streams
			}
			if err != nil {
				return err
			}
			if err := item.Value(func(val []byte) error {
				meta, err = decodeStreamMeta(val)
				return err
			}); err != nil {
				return err
			}

			// Get entries after start ID
			entries := make([]StreamEntry, 0)
			prefix := streamDataPrefix(key)
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()

			startTS, startSeq, _ := parseStreamID(startID)
			if startID == "$" {
				// Only get new entries after last ID
				startTS = meta.LastID
				startSeq = meta.LastSeq + 1
			}

			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				item := it.Item()
				id := string(bytes.TrimPrefix(item.Key(), prefix))

				ts, seq, _ := parseStreamID(id)
				if compareStreamID(id, startID) <= 0 && startID != "$" {
					continue
				}
				if startID == "$" && (ts < startTS || (ts == startTS && seq <= startSeq)) {
					continue
				}

				var fields map[string]string
				if err := item.Value(func(val []byte) error {
					return json.Unmarshal(val, &fields)
				}); err != nil {
					return err
				}

				entries = append(entries, StreamEntry{
					ID:        id,
					Fields:    fields,
					Timestamp: ts,
					Sequence:  seq,
				})

				if count > 0 && int64(len(entries)) >= count {
					break
				}
			}

			if len(entries) > 0 {
				result = append(result, map[string][]StreamEntry{key: entries})
			}
		}
		return nil
	})

	if block > 0 && len(result) == 0 {
		// Implement blocking behavior
		return s.xReadBlocking(ctx, count, block, args)
	}

	if block == 0 && len(result) == 0 && len(args) > 0 {
		return s.xReadBlocking(ctx, count, block, args)
	}

	return result, err
}

// streamKeys extracts stream keys from the args array (key/startID pairs)
func streamKeys(args []string) []string {
	keys := make([]string, 0, len(args)/2)
	for i := 0; i < len(args); i += 2 {
		keys = append(keys, args[i])
	}
	return keys
}

// unregisterStreamBlocking removes a specific channel from all stream keys' wait lists
func (s *BotreonStore) unregisterStreamBlocking(ch chan StreamReadResult, keys []string) {
	s.streamBlockingMu.Lock()
	defer s.streamBlockingMu.Unlock()

	for _, key := range keys {
		chans := s.streamBlockingChans[key]
		for j, c := range chans {
			if c == ch {
				s.streamBlockingChans[key] = append(chans[:j], chans[j+1:]...)
				break
			}
		}
		if len(s.streamBlockingChans[key]) == 0 {
			delete(s.streamBlockingChans, key)
		}
	}
}

// xReadBlocking implements blocking XREAD
func (s *BotreonStore) xReadBlocking(ctx context.Context, count int64, block int64, args []string) ([]map[string][]StreamEntry, error) {
	resultCh := make(chan StreamReadResult, 1)
	keys := streamKeys(args)

	// Set up timeout
	var timeoutCh <-chan time.Time
	if block > 0 {
		timeoutCh = time.After(time.Duration(block) * time.Millisecond)
	}

	// Register this channel for each key BEFORE trying immediate read
	s.streamBlockingMu.Lock()
	for _, key := range keys {
		s.streamBlockingChans[key] = append(s.streamBlockingChans[key], resultCh)
	}
	s.streamBlockingMu.Unlock()

	// Try immediate read AFTER registering the channel
	result, err := s.xReadImmediate(count, args...)
	if err != nil {
		s.unregisterStreamBlocking(resultCh, keys)
		return nil, err
	}
	if len(result) > 0 {
		s.unregisterStreamBlocking(resultCh, keys)
		return result, nil
	}

	// If block is 0 (infinite wait), wait for notification with re-check loop
	if block == 0 {
		for {
			select {
			case <-ctx.Done():
				s.unregisterStreamBlocking(resultCh, keys)
				return nil, nil
			case streamResult := <-resultCh:
				if len(streamResult.Entries) > 0 {
					return []map[string][]StreamEntry{{streamResult.Key: streamResult.Entries}}, nil
				}
				// Spurious wakeup: re-register channel and re-check
				s.streamBlockingMu.Lock()
				for _, key := range keys {
					s.streamBlockingChans[key] = append(s.streamBlockingChans[key], resultCh)
				}
				s.streamBlockingMu.Unlock()
				result, err := s.xReadImmediate(count, args...)
				if err == nil && len(result) > 0 {
					s.unregisterStreamBlocking(resultCh, keys)
					return result, nil
				}
			}
		}
	}

	// Wait for data or timeout
	select {
	case <-ctx.Done():
		s.unregisterStreamBlocking(resultCh, keys)
		return nil, nil
	case streamResult := <-resultCh:
		if len(streamResult.Entries) > 0 {
			s.unregisterStreamBlocking(resultCh, keys)
			return []map[string][]StreamEntry{{streamResult.Key: streamResult.Entries}}, nil
		}
	case <-timeoutCh:
	}

	s.unregisterStreamBlocking(resultCh, keys)
	return nil, nil
}

// xReadImmediate performs an immediate (non-blocking) XREAD
func (s *BotreonStore) xReadImmediate(count int64, args ...string) ([]map[string][]StreamEntry, error) {
	result := make([]map[string][]StreamEntry, 0)

	err := s.db.View(func(txn *badger.Txn) error {
		for i := 0; i < len(args); i += 2 {
			key := args[i]
			startID := args[i+1]

			// Get stream metadata
			metaKey := streamKey(key)
			var meta *streamMetaData
			item, err := txn.Get(metaKey)
			if errors.Is(err, badger.ErrKeyNotFound) {
				continue // Skip non-existent streams
			}
			if err != nil {
				return err
			}
			if err := item.Value(func(val []byte) error {
				meta, err = decodeStreamMeta(val)
				return err
			}); err != nil {
				return err
			}

			// Get entries after start ID
			entries := make([]StreamEntry, 0)
			prefix := streamDataPrefix(key)
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()

			startTS, startSeq, _ := parseStreamID(startID)
			if startID == "$" {
				// Only get new entries after last ID
				startTS = meta.LastID
				startSeq = meta.LastSeq + 1
			}

			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				item := it.Item()
				id := string(bytes.TrimPrefix(item.Key(), prefix))

				ts, seq, _ := parseStreamID(id)
				if compareStreamID(id, startID) <= 0 && startID != "$" {
					continue
				}
				if startID == "$" && (ts < startTS || (ts == startTS && seq <= startSeq)) {
					continue
				}

				var fields map[string]string
				if err := item.Value(func(val []byte) error {
					return json.Unmarshal(val, &fields)
				}); err != nil {
					return err
				}

				entries = append(entries, StreamEntry{
					ID:        id,
					Fields:    fields,
					Timestamp: ts,
					Sequence:  seq,
				})

				if count > 0 && int64(len(entries)) >= count {
					break
				}
			}

			if len(entries) > 0 {
				result = append(result, map[string][]StreamEntry{key: entries})
			}
		}
		return nil
	})

	return result, err
}

// notifyStreamRead notifies waiting stream readers about new data
func (s *BotreonStore) notifyStreamRead(key string, entries []StreamEntry) {
	s.streamBlockingMu.Lock()
	defer s.streamBlockingMu.Unlock()

	chans := s.streamBlockingChans[key]
	for _, ch := range chans {
		select {
		case ch <- StreamReadResult{Key: key, Entries: entries}:
		default:
			// Channel not ready
		}
	}
}

// XRange returns entries in a range
func (s *BotreonStore) XRange(key, start, stop string, count int64) ([]StreamEntry, error) {
	var entries []StreamEntry

	err := s.db.View(func(txn *badger.Txn) error {
		prefix := streamDataPrefix(key)
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		// Parse range bounds
		startTS, startSeq, _ := parseStreamID(start)
		if start == "-" {
			startTS = math.MinInt64
			startSeq = 0
		}
		stopTS, stopSeq, _ := parseStreamID(stop)
		if stop == "+" {
			stopTS = math.MaxInt64
			stopSeq = math.MaxInt64
		}

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			id := string(bytes.TrimPrefix(item.Key(), prefix))

			ts, seq, _ := parseStreamID(id)

			// Check if within range
			if ts < startTS || (ts == startTS && seq < startSeq) {
				continue
			}
			if ts > stopTS || (ts == stopTS && seq > stopSeq) {
				break
			}

			var fields map[string]string
			if err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &fields)
			}); err != nil {
				return err
			}

			entries = append(entries, StreamEntry{
				ID:        id,
				Fields:    fields,
				Timestamp: ts,
				Sequence:  seq,
			})

			if count > 0 && int64(len(entries)) >= count {
				break
			}
		}
		return nil
	})

	return entries, err
}

// XRevRange returns entries in reverse range
func (s *BotreonStore) XRevRange(key, start, stop string, count int64) ([]StreamEntry, error) {
	entries, err := s.XRange(key, stop, start, count)
	if err != nil {
		return nil, err
	}

	// Reverse
	for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
		entries[i], entries[j] = entries[j], entries[i]
	}

	return entries, nil
}

// XDel deletes entries from a stream
func (s *BotreonStore) XDel(key string, ids ...string) (int64, error) {
	var deleted int64

	err := s.retryUpdate(func(txn *badger.Txn) error {
		metaKey := streamKey(key)
		var meta *streamMetaData

		item, err := txn.Get(metaKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := item.Value(func(val []byte) error {
			meta, err = decodeStreamMeta(val)
			return err
		}); err != nil {
			return err
		}

		for _, id := range ids {
			dataKey := streamDataKey(key, id)
			ts, seq, _ := parseStreamID(id)

			// Check if entry exists
			_, err := txn.Get(dataKey)
			if errors.Is(err, badger.ErrKeyNotFound) {
				continue
			}
			if err != nil {
				return err
			}

			// Delete entry
			if err := txn.Delete(dataKey); err != nil {
				return err
			}
			deleted++

			// Update max deleted ID if needed
			if ts > meta.MaxDeletedID || (ts == meta.MaxDeletedID && seq > meta.MaxDelSeq) {
				meta.MaxDeletedID = ts
				meta.MaxDelSeq = seq
			}

			// Update first ID if deleting the first entry
			if ts == meta.FirstID && seq == meta.FirstSeq {
				// Find next entry
				nextID := formatStreamID(ts, seq+1)
				nextKey := streamDataKey(key, nextID)
				nextItem, err := txn.Get(nextKey)
				if err == nil && !errors.Is(err, badger.ErrKeyNotFound) {
					if err := nextItem.Value(func(val []byte) error {
						meta.FirstID = ts
						meta.FirstSeq = seq + 1
						return nil
					}); err != nil {
						return err
					}
				} else {
					// Need to scan forward
					prefix := streamDataPrefix(key)
					it := txn.NewIterator(badger.DefaultIteratorOptions)
					defer it.Close()

					found := false
					for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
						nextEntryID := string(bytes.TrimPrefix(it.Item().Key(), prefix))
						nextTS, nextSeq, _ := parseStreamID(nextEntryID)
						meta.FirstID = nextTS
						meta.FirstSeq = nextSeq
						found = true
						break
					}
					if !found {
						// Stream is empty
						meta.FirstID = meta.LastID
						meta.FirstSeq = meta.LastSeq
					}
				}
			}
		}

		meta.Length -= deleted
		if meta.Length == 0 {
			// Clear metadata
			meta.FirstID = 0
			meta.FirstSeq = 0
		}

		// Save metadata
		if err := txn.Set(metaKey, encodeStreamMeta(meta)); err != nil {
			return err
		}

		return nil
	}, 30)

	return deleted, err
}

// XInfo returns stream information
func (s *BotreonStore) XInfo(key string) (*StreamInfo, error) {
	var info StreamInfo

	err := s.db.View(func(txn *badger.Txn) error {
		metaKey := streamKey(key)
		item, err := txn.Get(metaKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}

		var meta *streamMetaData
		if err := item.Value(func(val []byte) error {
			meta, err = decodeStreamMeta(val)
			return err
		}); err != nil {
			return err
		}

		info.Length = meta.Length
		info.FirstID = formatStreamID(meta.FirstID, meta.FirstSeq)
		info.LastID = formatStreamID(meta.LastID, meta.LastSeq)
		info.MaxDeletedID = formatStreamID(meta.MaxDeletedID, meta.MaxDelSeq)

		// Count groups
		groups := make(map[string]*StreamGroup)
		groupsPrefix := streamGroupKey(key)
		opts := badger.DefaultIteratorOptions
		opts.Prefix = groupsPrefix
		groupIt := txn.NewIterator(opts)
		defer groupIt.Close()

		for groupIt.Seek(groupsPrefix); groupIt.ValidForPrefix(groupsPrefix); groupIt.Next() {
			groupName := string(bytes.TrimPrefix(groupIt.Item().Key(), groupsPrefix))
			groups[groupName] = &StreamGroup{
				Name:      groupName,
				Consumers: make(map[string]*StreamConsumer),
				Pending:   make(map[string]*StreamPendingEntry),
			}
		}
		info.Groups = groups

		return nil
	})

	return &info, err
}

// XTrim trims a stream
func (s *BotreonStore) XTrim(key string, maxLen int64, minID string) (int64, error) {
	var trimmed int64

	err := s.retryUpdate(func(txn *badger.Txn) error {
		metaKey := streamKey(key)
		var meta *streamMetaData

		item, err := txn.Get(metaKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := item.Value(func(val []byte) error {
			meta, err = decodeStreamMeta(val)
			return err
		}); err != nil {
			return err
		}

		prefix := streamDataPrefix(key)
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		var entriesToDelete []string

		if maxLen > 0 && meta.Length > maxLen {
			// Delete oldest entries
			entriesToDelete = append(entriesToDelete, "")
			count := int64(0)
			for it.Seek(prefix); it.ValidForPrefix(prefix) && count < meta.Length-maxLen; it.Next() {
				id := string(bytes.TrimPrefix(it.Item().Key(), prefix))
				entriesToDelete = append(entriesToDelete, id)
				ts, seq, _ := parseStreamID(id)
				if ts > meta.MaxDeletedID || (ts == meta.MaxDeletedID && seq > meta.MaxDelSeq) {
					meta.MaxDeletedID = ts
					meta.MaxDelSeq = seq
				}
				count++
			}
		}

		if minID != "" {
			// Delete entries before minID
			minTS, minSeq, _ := parseStreamID(minID)
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				id := string(bytes.TrimPrefix(it.Item().Key(), prefix))
				ts, seq, _ := parseStreamID(id)
				if ts < minTS || (ts == minTS && seq < minSeq) {
					entriesToDelete = append(entriesToDelete, id)
					if ts > meta.MaxDeletedID || (ts == meta.MaxDeletedID && seq > meta.MaxDelSeq) {
						meta.MaxDeletedID = ts
						meta.MaxDelSeq = seq
					}
				} else {
					break
				}
			}
		}

		// Delete entries
		for _, id := range entriesToDelete[1:] {
			if id == "" {
				continue
			}
			dataKey := streamDataKey(key, id)
			if err := txn.Delete(dataKey); err != nil {
				return err
			}
			trimmed++
		}

		meta.Length -= trimmed
		if meta.Length == 0 {
			meta.FirstID = meta.LastID
			meta.FirstSeq = meta.LastSeq
		} else if trimmed > 0 {
			// Update first ID
			nextKey := streamDataPrefix(key)
			nextItem, err := txn.Get(nextKey)
			if err == nil && !errors.Is(err, badger.ErrKeyNotFound) {
				if err := nextItem.Value(func(val []byte) error {
					nextID := string(bytes.TrimPrefix(nextKey, prefix))
					nextTS, nextSeq, _ := parseStreamID(nextID)
					meta.FirstID = nextTS
					meta.FirstSeq = nextSeq
					return nil
				}); err != nil {
					return err
				}
			}
		}

		// Save metadata
		if err := txn.Set(metaKey, encodeStreamMeta(meta)); err != nil {
			return err
		}

		return nil
	}, 30)

	return trimmed, err
}

// XGroupCreate creates a consumer group
func (s *BotreonStore) XGroupCreate(key, group, startID string) error {
	return s.retryUpdate(func(txn *badger.Txn) error {
		metaKey := streamKey(key)
		typeKey := TypeOfKeyGet(key)

		// Check type first before checking meta key
		typeItem, err := txn.Get(typeKey)
		if err == nil {
			typeVal, _ := typeItem.ValueCopy(nil)
			if string(typeVal) != "" && string(typeVal) != KeyTypeStream {
				return ErrWrongType
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}

		_, err = txn.Get(metaKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			_ = txn.Set(typeKey, []byte(KeyTypeStream))
			meta := &streamMetaData{}
			metaData, _ := json.Marshal(meta)
			_ = txn.Set(metaKey, metaData)
		} else if err != nil {
			return err
		}

		groupKey := streamGroupDataKey(key, group)
		groupData := &StreamGroup{
			Name:            group,
			LastDeliveredID: startID,
			Consumers:       make(map[string]*StreamConsumer),
			Pending:         make(map[string]*StreamPendingEntry),
		}
		data, err := json.Marshal(groupData)
		if err != nil {
			return err
		}
		return txn.Set(groupKey, data)
	}, 30)
}

// XGroupDelConsumer removes a consumer from a group
func (s *BotreonStore) XGroupDelConsumer(key, group, consumer string) (int64, error) {
	var removed int64
	err := s.retryUpdate(func(txn *badger.Txn) error {
		groupKey := streamGroupDataKey(key, group)
		item, err := txn.Get(groupKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}

		var groupData *StreamGroup
		if err := item.Value(func(val []byte) error {
			return json.Unmarshal(val, &groupData)
		}); err != nil {
			return err
		}

		// Count pending messages for this consumer
		if groupData.Pending != nil {
			for id, p := range groupData.Pending {
				if p.Consumer == consumer {
					delete(groupData.Pending, id)
					removed++
				}
			}
		}

		// Delete consumer
		delete(groupData.Consumers, consumer)
		data, err := json.Marshal(groupData)
		if err != nil {
			return err
		}
		return txn.Set(groupKey, data)
	}, 30)
	return removed, err
}

// XGroupDestroy destroys a consumer group
func (s *BotreonStore) XGroupDestroy(key, group string) error {
	return s.retryUpdate(func(txn *badger.Txn) error {
		groupKey := streamGroupDataKey(key, group)
		return txn.Delete(groupKey)
	}, 30)
}

// XGroupSetID sets the last delivered ID for a group
func (s *BotreonStore) XGroupSetID(key, group, id string) error {
	return s.retryUpdate(func(txn *badger.Txn) error {
		groupKey := streamGroupDataKey(key, group)
		item, err := txn.Get(groupKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}

		var groupData *StreamGroup
		if err := item.Value(func(val []byte) error {
			return json.Unmarshal(val, &groupData)
		}); err != nil {
			return err
		}

		groupData.LastDeliveredID = id
		data, err := json.Marshal(groupData)
		if err != nil {
			return err
		}
		return txn.Set(groupKey, data)
	}, 30)
}

// XReadGroup reads from a consumer group
func (s *BotreonStore) XReadGroup(group, consumer string, count int64, block int64, keys ...string) ([]map[string][]StreamEntry, error) {
	result := make([]map[string][]StreamEntry, 0)

	err := s.retryUpdate(func(txn *badger.Txn) error {
		now := time.Now().UnixNano() / int64(time.Millisecond)

		for _, key := range keys {
			// Get group info
			groupKey := streamGroupDataKey(key, group)
			item, err := txn.Get(groupKey)
			if errors.Is(err, badger.ErrKeyNotFound) {
				return nil
			}
			if err != nil {
				return err
			}

			var groupData *StreamGroup
			if err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &groupData)
			}); err != nil {
				return err
			}

			// Ensure consumers map exists
			if groupData.Consumers == nil {
				groupData.Consumers = make(map[string]*StreamConsumer)
			}

			// Update or add consumer
			if _, exists := groupData.Consumers[consumer]; !exists {
				groupData.Consumers[consumer] = &StreamConsumer{
					Name:     consumer,
					LastSeen: now,
				}
			} else {
				groupData.Consumers[consumer].LastSeen = now
			}

			// Ensure pending map exists
			if groupData.Pending == nil {
				groupData.Pending = make(map[string]*StreamPendingEntry)
			}

			// Get entries after last delivered ID
			entries := make([]StreamEntry, 0)
			prefix := streamDataPrefix(key)
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()

			lastTS, lastSeq, _ := parseStreamID(groupData.LastDeliveredID)
			var lastID string

			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				item := it.Item()
				id := string(bytes.TrimPrefix(item.Key(), prefix))

				ts, seq, _ := parseStreamID(id)
				if ts < lastTS || (ts == lastTS && seq <= lastSeq) {
					continue
				}

				var fields map[string]string
				if err := item.Value(func(val []byte) error {
					return json.Unmarshal(val, &fields)
				}); err != nil {
					return err
				}

				entries = append(entries, StreamEntry{
					ID:        id,
					Fields:    fields,
					Timestamp: ts,
					Sequence:  seq,
				})

				// Add to pending if not already pending
				if _, pending := groupData.Pending[id]; !pending {
					groupData.Pending[id] = &StreamPendingEntry{
						ID:            id,
						Consumer:      consumer,
						DeliveryCount: 1,
						LastDelivery:  now,
					}
				}

				lastID = id

				if count > 0 && int64(len(entries)) >= count {
					break
				}
			}

			// Update last delivered ID
			if lastID != "" {
				groupData.LastDeliveredID = lastID
			}

			// Save updated group data
			data, err := json.Marshal(groupData)
			if err != nil {
				return err
			}
			if err := txn.Set(groupKey, data); err != nil {
				return err
			}

			if len(entries) > 0 {
				result = append(result, map[string][]StreamEntry{key: entries})
			}
		}
		return nil
	}, 30)

	return result, err
}

// XAck acknowledges messages in a stream
func (s *BotreonStore) XAck(key, group string, ids ...string) (int64, error) {
	var acknowledged int64

	err := s.retryUpdate(func(txn *badger.Txn) error {
		groupKey := streamGroupDataKey(key, group)
		item, err := txn.Get(groupKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}

		var groupData *StreamGroup
		if err := item.Value(func(val []byte) error {
			return json.Unmarshal(val, &groupData)
		}); err != nil {
			return err
		}

		for _, id := range ids {
			if _, exists := groupData.Pending[id]; exists {
				delete(groupData.Pending, id)
				acknowledged++
			}
		}

		data, err := json.Marshal(groupData)
		if err != nil {
			return err
		}
		return txn.Set(groupKey, data)
	}, 30)

	return acknowledged, err
}

// XPending returns pending information for a group
func (s *BotreonStore) XPending(key, group string) ([]StreamPendingEntry, error) {
	var pending []StreamPendingEntry

	err := s.db.View(func(txn *badger.Txn) error {
		groupKey := streamGroupDataKey(key, group)
		item, err := txn.Get(groupKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}

		var groupData *StreamGroup
		if err := item.Value(func(val []byte) error {
			return json.Unmarshal(val, &groupData)
		}); err != nil {
			return err
		}

		for _, p := range groupData.Pending {
			pending = append(pending, *p)
		}
		return nil
	})

	return pending, err
}

// XClaim claims pending messages
func (s *BotreonStore) XClaim(key, group, consumer string, minIdleTime int64, ids ...string) ([]string, error) {
	var claimed []string

	err := s.retryUpdate(func(txn *badger.Txn) error {
		groupKey := streamGroupDataKey(key, group)
		item, err := txn.Get(groupKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}

		var groupData *StreamGroup
		if err := item.Value(func(val []byte) error {
			return json.Unmarshal(val, &groupData)
		}); err != nil {
			return err
		}

		now := time.Now().UnixNano() / int64(time.Millisecond)

		for _, id := range ids {
			if p, exists := groupData.Pending[id]; exists {
				if minIdleTime > 0 {
					idleTime := now - p.LastDelivery
					if idleTime < minIdleTime {
						continue
					}
				}
				p.Consumer = consumer
				p.LastDelivery = now
				p.DeliveryCount++
				claimed = append(claimed, id)
			}
		}

		data, err := json.Marshal(groupData)
		if err != nil {
			return err
		}
		return txn.Set(groupKey, data)
	}, 30)

	return claimed, err
}

// XInfoGroups returns information about consumer groups
func (s *BotreonStore) XInfoGroups(key string) ([]*StreamGroup, error) {
	var groups []*StreamGroup

	err := s.db.View(func(txn *badger.Txn) error {
		prefix := streamGroupKey(key)
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			var groupData StreamGroup
			if err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &groupData)
			}); err != nil {
				return err
			}
			groups = append(groups, &groupData)
		}
		return nil
	})

	return groups, err
}

// XInfoConsumers returns information about consumers in a group
func (s *BotreonStore) XInfoConsumers(key, group string) ([]*StreamConsumer, error) {
	var consumers []*StreamConsumer

	err := s.db.View(func(txn *badger.Txn) error {
		groupKey := streamGroupDataKey(key, group)
		item, err := txn.Get(groupKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}

		var groupData *StreamGroup
		if err := item.Value(func(val []byte) error {
			return json.Unmarshal(val, &groupData)
		}); err != nil {
			return err
		}

		for _, c := range groupData.Consumers {
			consumers = append(consumers, c)
		}
		return nil
	})

	return consumers, err
}

// StreamType checks if a key is a stream
func (s *BotreonStore) StreamType(key string) (bool, error) {
	var exists bool
	err := s.db.View(func(txn *badger.Txn) error {
		metaKey := streamKey(key)
		_, err := txn.Get(metaKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			exists = false
			return nil
		}
		if err != nil {
			return err
		}
		exists = true
		return nil
	})
	return exists, err
}

// GetStreamEntry retrieves a specific entry from a stream
func (s *BotreonStore) GetStreamEntry(key, id string) (*StreamEntry, error) {
	var entry *StreamEntry

	err := s.db.View(func(txn *badger.Txn) error {
		dataKey := streamDataKey(key, id)
		item, err := txn.Get(dataKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return fmt.Errorf("ERR no such entry")
		}
		if err != nil {
			return err
		}

		var fields map[string]string
		if err := item.Value(func(val []byte) error {
			return json.Unmarshal(val, &fields)
		}); err != nil {
			return err
		}

		ts, seq, _ := parseStreamID(id)
		entry = &StreamEntry{
			ID:        id,
			Fields:    fields,
			Timestamp: ts,
			Sequence:  seq,
		}
		return nil
	})

	return entry, err
}

// XAutoClaimOptions contains options for XAUTOCLAIM
type XAutoClaimOptions struct {
	Count  int64
	JustID bool
}

// XAutoClaimResult contains the result of XAUTOCLAIM
type XAutoClaimResult struct {
	NextID     string
	ClaimedIDs []string
	Messages   []StreamEntry
}

// XAutoClaim automatically claims pending messages
// XAUTOCLAIM key group consumer min-idle-time start [COUNT count] [JUSTID]
func (s *BotreonStore) XAutoClaim(key, group, consumer string, minIdleTime int64, start string, opts XAutoClaimOptions) (*XAutoClaimResult, error) {
	var result XAutoClaimResult

	err := s.retryUpdate(func(txn *badger.Txn) error {
		groupKey := streamGroupDataKey(key, group)
		item, err := txn.Get(groupKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}

		var groupData *StreamGroup
		if err := item.Value(func(val []byte) error {
			return json.Unmarshal(val, &groupData)
		}); err != nil {
			return err
		}

		now := time.Now().UnixNano() / int64(time.Millisecond)

		// Parse start ID
		startTS, startSeq, _ := parseStreamID(start)

		// Find pending entries that meet the idle time requirement
		for id, pending := range groupData.Pending {
			idTS, idSeq, _ := parseStreamID(id)

			// Skip if ID is before start
			if idTS < startTS || (idTS == startTS && idSeq <= startSeq) {
				continue
			}

			// Check idle time
			idleTime := now - pending.LastDelivery
			if idleTime < minIdleTime {
				continue
			}

			// Claim the entry
			pending.Consumer = consumer
			pending.LastDelivery = now
			pending.DeliveryCount++

			result.ClaimedIDs = append(result.ClaimedIDs, id)

			// Get the message content
			dataKey := streamDataKey(key, id)
			msgItem, err := txn.Get(dataKey)
			if err == nil && !errors.Is(err, badger.ErrKeyNotFound) {
				var fields map[string]string
				if err := msgItem.Value(func(val []byte) error {
					return json.Unmarshal(val, &fields)
				}); err == nil {
					result.Messages = append(result.Messages, StreamEntry{
						ID:        id,
						Fields:    fields,
						Timestamp: idTS,
						Sequence:  idSeq,
					})
				}
			}

			// Check count limit
			if opts.Count > 0 && int64(len(result.ClaimedIDs)) >= opts.Count {
				break
			}
		}

		// Update the next start ID
		if len(result.ClaimedIDs) > 0 {
			lastID := result.ClaimedIDs[len(result.ClaimedIDs)-1]
			lastTS, lastSeq, _ := parseStreamID(lastID)
			if lastSeq > 0 {
				result.NextID = formatStreamID(lastTS, lastSeq)
			} else {
				result.NextID = formatStreamID(lastTS, lastSeq)
			}
		} else {
			result.NextID = start
		}

		// Save group data
		data, err := json.Marshal(groupData)
		if err != nil {
			return err
		}
		return txn.Set(groupKey, data)
	}, 30)

	return &result, err
}
