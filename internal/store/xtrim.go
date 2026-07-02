package store

import (
	"bytes"
	"errors"

	"github.com/dgraph-io/badger/v4"
)

// XTrim trims a stream
func (s *BotreonStore) XTrim(key string, maxLen int64, minID string) (int64, error) {
	var trimmed int64

	err := s.retryUpdate(func(txn *badger.Txn) error {
		if err := checkKeyType(txn, key, KeyTypeStream); err != nil {
			return err
		}
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

		if len(entriesToDelete) > 0 {
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
		}

		meta.Length -= trimmed
		if meta.Length == 0 {
			meta.FirstID = meta.LastID
			meta.FirstSeq = meta.LastSeq
		} else if trimmed > 0 {
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

		if err := txn.Set(metaKey, encodeStreamMeta(meta)); err != nil {
			return err
		}
		return nil
	}, 30)

	return trimmed, err
}

// XSetID sets the last-delivered ID of a stream (internal replication command)
func (s *BotreonStore) XSetID(key, lastID string, entriesAdded int64, maxDeletedID string) error {
	return s.retryUpdate(func(txn *badger.Txn) error {
		metaKey := streamKey(key)

		item, err := txn.Get(metaKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			meta := &streamMetaData{
				Length:       0,
				FirstID:      0,
				FirstSeq:     0,
				LastID:       0,
				LastSeq:      0,
				MaxDeletedID: 0,
				MaxDelSeq:    0,
			}
			ts, seq, _ := parseStreamID(lastID)
			meta.LastID = ts
			meta.LastSeq = seq
			if entriesAdded >= 0 {
				meta.Length = entriesAdded
			}
			if maxDeletedID != "" {
				mts, mseq, _ := parseStreamID(maxDeletedID)
				meta.MaxDeletedID = mts
				meta.MaxDelSeq = mseq
			}
			typeKey := TypeOfKeyGet(key)
			if err := txn.Set(typeKey, []byte(KeyTypeStream)); err != nil {
				return err
			}
			data := encodeStreamMeta(meta)
			if err := txn.Set(metaKey, data); err != nil {
				return err
			}
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

		ts, seq, _ := parseStreamID(lastID)
		meta.LastID = ts
		meta.LastSeq = seq
		if entriesAdded >= 0 {
			meta.Length = entriesAdded
		}
		if maxDeletedID != "" {
			mts, mseq, _ := parseStreamID(maxDeletedID)
			meta.MaxDeletedID = mts
			meta.MaxDelSeq = mseq
		}

		data := encodeStreamMeta(meta)
		return txn.Set(metaKey, data)
	}, 30)
}
