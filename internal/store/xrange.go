package store

import (
	"bytes"
	"encoding/json"
	"math"

	"github.com/dgraph-io/badger/v4"
)

// XRange returns entries in a range
func (s *BotreonStore) XRange(key, start, stop string, count int64) ([]StreamEntry, error) {
	var entries []StreamEntry

	err := s.db.View(func(txn *badger.Txn) error {
		if err := checkKeyType(txn, key, KeyTypeStream); err != nil {
			return err
		}
		prefix := streamDataPrefix(key)
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

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

	for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
		entries[i], entries[j] = entries[j], entries[i]
	}
	return entries, nil
}
