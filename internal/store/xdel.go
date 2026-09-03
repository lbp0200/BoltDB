package store

import (
	"bytes"
	"errors"

	"github.com/dgraph-io/badger/v4"
)

// XDel deletes entries from a stream
func (s *BotreonStore) XDel(key string, ids ...string) (int64, error) {
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)
	var deleted int64

	err := s.retryUpdate(func(txn *badger.Txn) error {
		deleted = 0
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

		for _, id := range ids {
			dataKey := streamDataKey(key, id)
			ts, seq, _ := parseStreamID(id)

			_, err := txn.Get(dataKey)
			if errors.Is(err, badger.ErrKeyNotFound) {
				continue
			}
			if err != nil {
				return err
			}

			if err := txn.Delete(dataKey); err != nil {
				return err
			}
			deleted++

			if ts > meta.MaxDeletedID || (ts == meta.MaxDeletedID && seq > meta.MaxDelSeq) {
				meta.MaxDeletedID = ts
				meta.MaxDelSeq = seq
			}

			if ts == meta.FirstID && seq == meta.FirstSeq {
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
						meta.FirstID = meta.LastID
						meta.FirstSeq = meta.LastSeq
					}
				}
			}
		}

		meta.Length -= deleted
		if meta.Length == 0 {
			meta.FirstID = 0
			meta.FirstSeq = 0
		}

		if err := txn.Set(metaKey, encodeStreamMeta(meta)); err != nil {
			return err
		}
		return nil
	}, 30)

	return deleted, err
}
