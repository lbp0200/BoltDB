package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/lbp0200/BoltDB/internal/logger"
)

// CreateEmptyStream creates an empty stream with just the TYPE_ key and metadata
func (s *BotreonStore) CreateEmptyStream(key string) error {
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)
	return s.retryUpdate(func(txn *badger.Txn) error {
		typeKey := TypeOfKeyGet(key)
		if err := txn.Set(typeKey, []byte(KeyTypeStream)); err != nil {
			logger.Logger.Error().Err(err).Str("key", key).Msg("CreateEmptyStream: Failed to set type")
			return err
		}
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
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)
	var resultID string

	err := s.retryUpdate(func(txn *badger.Txn) error {
		typeKey := TypeOfKeyGet(key)
		typeItem, typeErr := txn.Get(typeKey)
		if typeErr == nil {
			typeVal, copyErr := typeItem.ValueCopy(nil)
			if copyErr != nil {
				return copyErr
			}
			if string(typeVal) != "" && string(typeVal) != KeyTypeStream {
				return ErrWrongType
			}
		} else if !errors.Is(typeErr, badger.ErrKeyNotFound) {
			return typeErr
		}
		// NOMKSTREAM：stream 不存在时不创建（Redis 语义报错）
		if errors.Is(typeErr, badger.ErrKeyNotFound) && opts.NoMkStream {
			return ErrStreamNotFound
		}
		if err := txn.Set(typeKey, []byte(KeyTypeStream)); err != nil {
			logger.Logger.Error().Err(err).Str("key", key).Msg("XAdd: Failed to set type")
			return err
		}
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

		if id == "" || id == "*" {
			timestamp := time.Now().UnixNano() / int64(time.Millisecond)
			if timestamp == meta.LastID {
				meta.LastSeq++
			} else {
				meta.LastSeq = 0
			}
			meta.LastID = timestamp
			id = formatStreamID(meta.LastID, meta.LastSeq)
		} else {
			ts, seq, err := parseStreamID(id)
			if err != nil {
				return err
			}
			if compareStreamID(id, formatStreamID(meta.LastID, meta.LastSeq)) < 0 {
				return fmt.Errorf("ERR ID must be greater than the last entry ID (%d-%d)", meta.LastID, meta.LastSeq)
			}
			meta.LastID = ts
			meta.LastSeq = seq
		}

		if opts.MinID != "" {
			if compareStreamID(id, opts.MinID) < 0 {
				return fmt.Errorf("ERR ID must be >= MINID (%s)", opts.MinID)
			}
		}

		entryData, err := json.Marshal(fields)
		if err != nil {
			return err
		}
		dataKey := streamDataKey(key, id)
		if err := txn.Set(dataKey, entryData); err != nil {
			return err
		}

		meta.Length++
		if meta.Length == 1 {
			meta.FirstID = meta.LastID
			meta.FirstSeq = meta.LastSeq
		}

		if opts.MaxLen > 0 {
			entriesToRemove := meta.Length - opts.MaxLen
			if entriesToRemove > 0 {
				removeMeta := &streamMetaData{
					Length:       entriesToRemove,
					FirstID:      meta.FirstID,
					FirstSeq:     meta.FirstSeq,
					MaxDeletedID: meta.FirstID,
					MaxDelSeq:    meta.FirstSeq,
				}
				prefix := streamDataPrefix(key)
				it := txn.NewIterator(badger.DefaultIteratorOptions)
				defer it.Close()

				count := int64(0)
				currentID := meta.FirstID
				currentSeq := meta.FirstSeq
				for it.Seek(prefix); it.ValidForPrefix(prefix) && count < entriesToRemove; it.Next() {
					item := it.Item()
					if err := txn.Delete(item.Key()); err != nil {
						return err
					}
					count++
					currentSeq++
					if count < entriesToRemove {
						removeMeta.MaxDelSeq = currentSeq
					}
				}
				meta.Length -= count
				if meta.Length > 0 {
					currentSeqStr := formatStreamID(currentID, currentSeq)
					nextKey := streamDataKey(key, currentSeqStr)
					_, err := txn.Get(nextKey)
					if err == nil || errors.Is(err, badger.ErrKeyNotFound) {
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

		if err := txn.Set(metaKey, encodeStreamMeta(meta)); err != nil {
			return err
		}
		resultID = id
		return nil
	}, 30, func() []byte {
		// D4 全重放：XADD key [NOMKSTREAM] [MAXLEN [~] <n>] [MINID <id>] id field value...
		args := make([][]byte, 0, 4+2*len(fields))
		args = append(args, []byte("XADD"), []byte(key))
		if opts.NoMkStream {
			args = append(args, []byte("NOMKSTREAM"))
		}
		if opts.MaxLen > 0 {
			args = append(args, []byte("MAXLEN"))
			if opts.MaxLenApprox > 0 {
				args = append(args, []byte("~"))
			}
			args = append(args, []byte(strconv.FormatInt(opts.MaxLen, 10)))
		}
		if opts.MinID != "" {
			args = append(args, []byte("MINID"), []byte(opts.MinID))
		}
		args = append(args, []byte(id))
		for f, v := range fields {
			args = append(args, []byte(f), []byte(v))
		}
		return encodePropagateCommand(args...)
	}())

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
		if err := checkKeyType(txn, key, KeyTypeStream); err != nil {
			return err
		}
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
