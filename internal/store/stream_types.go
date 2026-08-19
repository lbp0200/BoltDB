package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// ErrStreamNotFound 表示 stream 不存在（XADD NOMKSTREAM 语义）
var ErrStreamNotFound = errors.New("the stream does not exist")

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
	Timestamp int64
	Sequence  int64
}

// StreamGroup represents a consumer group
type StreamGroup struct {
	Name            string
	LastDeliveredID string
	Consumers       map[string]*StreamConsumer
	Pending         map[string]*StreamPendingEntry
}

// StreamConsumer represents a consumer within a group
type StreamConsumer struct {
	Name     string
	LastSeen int64
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
	MaxLen       int64
	MaxLenApprox int64
	MinID        string
	NoMkStream   bool // NOMKSTREAM：stream 不存在时不创建（Redis 语义）
}

// parseStreamID parses a stream ID string to (timestamp, sequence)
func parseStreamID(id string) (int64, int64, error) {
	if id == "*" {
		now := time.Now().UnixNano() / int64(time.Millisecond)
		return now, 0, nil
	}
	if id == "+" || id == "-" {
		return 0, 0, errors.New("invalid stream ID format")
	}
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

// streamMetaData for encoding stream metadata
type streamMetaData struct {
	Length       int64
	FirstID      int64
	FirstSeq     int64
	LastID       int64
	LastSeq      int64
	MaxDeletedID int64
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

// streamKeys extracts stream keys from the args array (key/startID pairs)
func streamKeys(args []string) []string {
	keys := make([]string, 0, len(args)/2)
	for i := 0; i < len(args); i += 2 {
		keys = append(keys, args[i])
	}
	return keys
}

// StreamType checks if a key is a stream
func (s *BotreonStore) StreamType(key string) (bool, error) {
	var exists bool
	err := s.db.View(func(txn *badger.Txn) error {
		if err := checkKeyType(txn, key, KeyTypeStream); err != nil {
			return err
		}
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
