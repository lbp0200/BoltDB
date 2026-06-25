package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/dgraph-io/badger/v4"
)

const (
	prefixTS = "TS:"
)

// TimeSeriesDataPoint represents a single data point in a time series
type TimeSeriesDataPoint struct {
	Timestamp int64   // Unix timestamp in milliseconds
	Value     float64 // The value at this timestamp
}

// TimeSeriesInfo contains metadata about a time series
type TimeSeriesInfo struct {
	TotalSamples   int64  // Total number of data points
	MemoryUsage    int64  // Estimated memory usage
	LastTimestamp  int64  // Last timestamp
	FirstTimestamp int64  // First timestamp
	RetentionTime  int64  // Retention time in milliseconds (0 = unlimited)
	Encoding       string // Encoding type
	ChunkCount     int64  // Number of chunks
}

// TSCreateOptions contains options for TS.CREATE
type TSCreateOptions struct {
	Retention       int64  // Retention time in milliseconds
	Encoding        string // Encoding type (compressed, uncompressed)
	DuplicatePolicy string // Policy for duplicate samples (block, first, last, min, max, sum)
}

// TSAddOptions contains options for TS.ADD
type TSAddOptions struct {
	OnDuplicate string // Policy for duplicate samples (block, skip, update)
}

// parseTimestamp parses a timestamp string to int64 milliseconds
func parseTimestamp(ts string) (int64, error) {
	if ts == "*" {
		return time.Now().UnixNano() / int64(time.Millisecond), nil
	}
	if ts == "-" {
		return 0, nil
	}
	if ts == "+" {
		return math.MaxInt64, nil
	}
	return strconv.ParseInt(ts, 10, 64)
}

// tsMetaKey returns the key for time series metadata
func tsMetaKey(key string) []byte {
	return []byte(fmt.Sprintf("%s%s:meta", prefixTS, key))
}

// tsDataPrefix returns the prefix for all data point keys
func tsDataPrefix(key string) []byte {
	return []byte(fmt.Sprintf("%s%s:data:", prefixTS, key))
}

// tsDataKey returns the key for a specific data point
func tsDataKey(key string, timestamp int64) []byte {
	return []byte(fmt.Sprintf("%s%s:data:%d", prefixTS, key, timestamp))
}

// encodeTSMeta encodes time series metadata
type tsMetaData struct {
	TotalSamples   int64
	FirstTimestamp int64
	LastTimestamp  int64
	Retention      int64 // in milliseconds
	Encoding       string
}

func encodeTSMeta(m *tsMetaData) []byte {
	b := make([]byte, 48)
	binary.BigEndian.PutUint64(b[:8], uint64(m.TotalSamples))
	binary.BigEndian.PutUint64(b[8:16], uint64(m.FirstTimestamp))
	binary.BigEndian.PutUint64(b[16:24], uint64(m.LastTimestamp))
	binary.BigEndian.PutUint64(b[24:32], uint64(m.Retention))
	copy(b[32:], m.Encoding)
	return b
}

func decodeTSMeta(b []byte) (*tsMetaData, error) {
	if len(b) < 40 {
		return nil, errors.New("invalid time series metadata size")
	}
	m := &tsMetaData{}
	m.TotalSamples = int64(binary.BigEndian.Uint64(b[:8]))
	m.FirstTimestamp = int64(binary.BigEndian.Uint64(b[8:16]))
	m.LastTimestamp = int64(binary.BigEndian.Uint64(b[16:24]))
	m.Retention = int64(binary.BigEndian.Uint64(b[24:32]))
	m.Encoding = string(bytes.Trim(b[32:], "\x00"))
	return m, nil
}

// TSCreate implements TS.CREATE command
// TS.CREATE key [RETENTION retention] [ENCODING encoding] [DUPLICATE_POLICY policy]
func (s *BotreonStore) TSCreate(key string, opts TSCreateOptions) error {
	// Check if key already exists
	exists, err := s.Exists(key)
	if err != nil {
		return err
	}
	if exists {
		return errors.New("ERR key already exists")
	}

	// Set type
	typeKey := TypeOfKeyGet(key)
	err = s.retryUpdate(func(txn *badger.Txn) error {
		if err := txn.Set(typeKey, []byte(KeyTypeTimeSeries)); err != nil {
			return err
		}

		// Create metadata
		meta := &tsMetaData{
			TotalSamples:   0,
			FirstTimestamp: 0,
			LastTimestamp:  0,
			Retention:      opts.Retention,
			Encoding:       opts.Encoding,
		}
		if meta.Encoding == "" {
			meta.Encoding = "compressed"
		}

		return txn.Set(tsMetaKey(key), encodeTSMeta(meta))
	}, 30)

	return err
}

// TSAdd implements TS.ADD command
// TS.ADD key timestamp value [ON_DUPLICATE policy]
func (s *BotreonStore) TSAdd(key string, timestamp int64, value float64, opts TSAddOptions) (int64, error) {
	var addedTimestamp int64

	err := s.retryUpdate(func(txn *badger.Txn) error {
		// Set type key if not exists
		typeKey := TypeOfKeyGet(key)
		typeItem, err := txn.Get(typeKey)
		if err == nil {
			// Key exists, check if it's a time series
			err = typeItem.Value(func(val []byte) error {
				keyType := string(val)
				if keyType != "" && keyType != KeyTypeTimeSeries {
					return ErrWrongType
				}
				return nil
			})
			if err != nil {
				return err
			}
		} else if errors.Is(err, badger.ErrKeyNotFound) {
			// Create the time series if it doesn't exist
			if err := txn.Set(typeKey, []byte(KeyTypeTimeSeries)); err != nil {
				return err
			}
			meta := &tsMetaData{
				TotalSamples:   0,
				FirstTimestamp: 0,
				LastTimestamp:  0,
				Retention:      0,
				Encoding:       "compressed",
			}
			if err := txn.Set(tsMetaKey(key), encodeTSMeta(meta)); err != nil {
				return err
			}
		} else {
			return err
		}

		// Get or create metadata
		metaKey := tsMetaKey(key)
		var meta *tsMetaData
		var item *badger.Item

		item, err = txn.Get(metaKey)
		if err == nil {
			err = item.Value(func(val []byte) error {
				meta, err = decodeTSMeta(val)
				return err
			})
			if err != nil {
				return err
			}
		} else if errors.Is(err, badger.ErrKeyNotFound) {
			meta = &tsMetaData{
				TotalSamples: 0,
				Retention:    0,
				Encoding:     "compressed",
			}
		} else {
			return err
		}

		// Check for duplicate timestamp
		dataKey := tsDataKey(key, timestamp)
		_, err = txn.Get(dataKey)
		if err == nil {
			// Timestamp already exists
			switch opts.OnDuplicate {
			case "block":
				return errors.New("ERR duplicate timestamp")
			case "skip":
				return nil
			case "update":
				// Delete old value and replace
				if err := txn.Delete(dataKey); err != nil {
					return err
				}
			default:
				// Default: update
				if err := txn.Delete(dataKey); err != nil {
					return err
				}
			}
		}

		// Store the data point
		valueBytes := make([]byte, 16)
		binary.BigEndian.PutUint64(valueBytes[:8], uint64(timestamp))
		binary.BigEndian.PutUint64(valueBytes[8:], uint64(math.Float64bits(value)))

		if err := txn.Set(dataKey, valueBytes); err != nil {
			return err
		}

		// Update metadata
		meta.TotalSamples++
		if meta.FirstTimestamp == 0 || timestamp < meta.FirstTimestamp {
			meta.FirstTimestamp = timestamp
		}
		if timestamp > meta.LastTimestamp {
			meta.LastTimestamp = timestamp
		}

		// Apply retention policy
		if meta.Retention > 0 {
			minTimestamp := timestamp - meta.Retention
			prefix := tsDataPrefix(key)
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()

			var toDelete [][]byte
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				keyBytes := it.Item().KeyCopy(nil)
				tsStr := string(keyBytes[len(prefix):])
				ts, err := strconv.ParseInt(tsStr, 10, 64)
				if err != nil {
					continue
				}
				if ts < minTimestamp {
					toDelete = append(toDelete, keyBytes)
				}
			}
			for _, k := range toDelete {
				if err := txn.Delete(k); err != nil {
					return err
				}
				meta.TotalSamples--
			}
		}

		// Save metadata
		if err := txn.Set(metaKey, encodeTSMeta(meta)); err != nil {
			return err
		}

		addedTimestamp = timestamp
		return nil
	}, 30)

	return addedTimestamp, err
}

// TSGet implements TS.GET command - get the last data point
func (s *BotreonStore) TSGet(key string) (*TimeSeriesDataPoint, error) {
	var result *TimeSeriesDataPoint

	err := s.db.View(func(txn *badger.Txn) error {
		if err := checkKeyType(txn, key, KeyTypeTimeSeries); err != nil {
			return err
		}
		// Get metadata
		metaKey := tsMetaKey(key)
		item, err := txn.Get(metaKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrKeyNotFound
		}
		if err != nil {
			return err
		}

		var meta *tsMetaData
		if err := item.Value(func(val []byte) error {
			meta, err = decodeTSMeta(val)
			return err
		}); err != nil {
			return err
		}

		if meta.TotalSamples == 0 {
			return ErrKeyNotFound
		}

		// Get the last data point
		dataKey := tsDataKey(key, meta.LastTimestamp)
		dataItem, err := txn.Get(dataKey)
		if err != nil {
			return err
		}

		var dataBytes []byte
		if err := dataItem.Value(func(val []byte) error {
			dataBytes = make([]byte, len(val))
			copy(dataBytes, val)
			return nil
		}); err != nil {
			return err
		}

		timestamp := int64(binary.BigEndian.Uint64(dataBytes[:8]))
		value := math.Float64frombits(binary.BigEndian.Uint64(dataBytes[8:]))

		result = &TimeSeriesDataPoint{
			Timestamp: timestamp,
			Value:     value,
		}
		return nil
	})

	return result, err
}

// TSRange implements TS.RANGE command - get data points in a range
func (s *BotreonStore) TSRange(key string, start, stop string, count int64) ([]TimeSeriesDataPoint, error) {
	var result []TimeSeriesDataPoint

	err := s.db.View(func(txn *badger.Txn) error {
		if err := checkKeyType(txn, key, KeyTypeTimeSeries); err != nil {
			return err
		}
		// Parse timestamps
		startTS, err := parseTimestamp(start)
		if err != nil {
			return fmt.Errorf("ERR invalid start timestamp: %v", err)
		}
		stopTS, err := parseTimestamp(stop)
		if err != nil {
			return fmt.Errorf("ERR invalid stop timestamp: %v", err)
		}

		prefix := tsDataPrefix(key)
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()

			// Extract timestamp from key: ts:key:data:timestamp
			keyBytes := item.KeyCopy(nil)
			tsStr := string(keyBytes[len(prefix):])
			ts, err := strconv.ParseInt(tsStr, 10, 64)
			if err != nil {
				continue
			}

			// Check if within range
			if ts < startTS {
				continue
			}
			if ts > stopTS {
				break
			}

			var dataBytes []byte
			if err := item.Value(func(val []byte) error {
				dataBytes = make([]byte, len(val))
				copy(dataBytes, val)
				return nil
			}); err != nil {
				return err
			}

			value := math.Float64frombits(binary.BigEndian.Uint64(dataBytes[8:]))

			result = append(result, TimeSeriesDataPoint{
				Timestamp: ts,
				Value:     value,
			})

			if count > 0 && int64(len(result)) >= count {
				break
			}
		}
		return nil
	})

	return result, err
}

// TSDel implements TS.DEL command - delete data points in a range
func (s *BotreonStore) TSDel(key string, start, stop string) (int64, error) {
	var deleted int64

	err := s.retryUpdate(func(txn *badger.Txn) error {
		deleted = 0 // reset each attempt; stale value must not survive conflict retry
		if err := checkKeyType(txn, key, KeyTypeTimeSeries); err != nil {
			return err
		}
		// Get metadata
		metaKey := tsMetaKey(key)
		item, err := txn.Get(metaKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrKeyNotFound
		}
		if err != nil {
			return err
		}

		var meta *tsMetaData
		if err := item.Value(func(val []byte) error {
			meta, err = decodeTSMeta(val)
			return err
		}); err != nil {
			return err
		}

		// Parse timestamps
		startTS, err := parseTimestamp(start)
		if err != nil {
			return fmt.Errorf("ERR invalid start timestamp: %v", err)
		}
		stopTS, err := parseTimestamp(stop)
		if err != nil {
			return fmt.Errorf("ERR invalid stop timestamp: %v", err)
		}

		prefix := tsDataPrefix(key)
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		var toDelete [][]byte

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			keyBytes := item.KeyCopy(nil)
			// Extract timestamp from key: ts:key:data:timestamp
			tsStr := string(keyBytes[len(prefix):])
			ts, err := strconv.ParseInt(tsStr, 10, 64)
			if err != nil {
				continue
			}

			if ts >= startTS && ts <= stopTS {
				toDelete = append(toDelete, keyBytes)
				deleted++
			}
		}

		for _, k := range toDelete {
			if err := txn.Delete(k); err != nil {
				return err
			}
		}

		meta.TotalSamples -= deleted
		if meta.TotalSamples > 0 && len(toDelete) > 0 {
			// Find new first timestamp
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				keyBytes := it.Item().KeyCopy(nil)
				tsStr := string(keyBytes[len(prefix):])
				ts, err := strconv.ParseInt(tsStr, 10, 64)
				if err != nil {
					continue
				}
				meta.FirstTimestamp = ts
				break
			}
		} else if meta.TotalSamples == 0 {
			meta.FirstTimestamp = 0
			meta.LastTimestamp = 0
		}

		return txn.Set(metaKey, encodeTSMeta(meta))
	}, 30)

	return deleted, err
}

// TSMGet implements TS.MGET command - get last value from multiple time series
func (s *BotreonStore) TSMGet(filter string, keys ...string) ([]*TimeSeriesDataPoint, error) {
	result := make([]*TimeSeriesDataPoint, len(keys))

	for i, key := range keys {
		dp, err := s.TSGet(key)
		if err != nil {
			if errors.Is(err, ErrKeyNotFound) {
				result[i] = nil
				continue
			}
			return nil, err
		}
		result[i] = dp
	}

	return result, nil
}

// TSInfo implements TS.INFO command
func (s *BotreonStore) TSInfo(key string) (*TimeSeriesInfo, error) {
	var info *TimeSeriesInfo

	err := s.db.View(func(txn *badger.Txn) error {
		if err := checkKeyType(txn, key, KeyTypeTimeSeries); err != nil {
			return err
		}
		metaKey := tsMetaKey(key)
		item, err := txn.Get(metaKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrKeyNotFound
		}
		if err != nil {
			return err
		}

		var meta *tsMetaData
		if err := item.Value(func(val []byte) error {
			meta, err = decodeTSMeta(val)
			return err
		}); err != nil {
			return err
		}

		info = &TimeSeriesInfo{
			TotalSamples:   meta.TotalSamples,
			FirstTimestamp: meta.FirstTimestamp,
			LastTimestamp:  meta.LastTimestamp,
			RetentionTime:  meta.Retention,
			Encoding:       meta.Encoding,
			ChunkCount:     (meta.TotalSamples / 1000) + 1,
		}
		return nil
	})

	return info, err
}

// TSLen implements TS.LEN command
func (s *BotreonStore) TSLen(key string) (int64, error) {
	var length int64

	err := s.db.View(func(txn *badger.Txn) error {
		if err := checkKeyType(txn, key, KeyTypeTimeSeries); err != nil {
			return err
		}
		metaKey := tsMetaKey(key)
		item, err := txn.Get(metaKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrKeyNotFound
		}
		if err != nil {
			return err
		}

		var meta *tsMetaData
		if err := item.Value(func(val []byte) error {
			meta, err = decodeTSMeta(val)
			return err
		}); err != nil {
			return err
		}

		length = meta.TotalSamples
		return nil
	})

	return length, err
}

// TimeSeriesType checks if a key is a time series
func (s *BotreonStore) TimeSeriesType(key string) (bool, error) {
	var exists bool
	err := s.db.View(func(txn *badger.Txn) error {
		metaKey := tsMetaKey(key)
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
