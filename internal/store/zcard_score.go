package store

import (
	"errors"
	"strconv"

	"github.com/dgraph-io/badger/v4"
	"github.com/lbp0200/BoltDB/internal/logger"
)

// ZScore 获取成员分数
func (s *BotreonStore) ZScore(zSetName, member string) (float64, bool, error) {
	var score float64
	var exists bool
	err := s.db.View(func(txn *badger.Txn) error {
		if err := checkKeyType(txn, zSetName, KeyTypeSortedSet); err != nil {
			return err
		}
		item, err := txn.Get(sortedSetKeyMember(zSetName, member))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		exists = true
		return item.Value(func(val []byte) error {
			score, _ = decodeDataValue(val)
			return nil
		})
	})
	if err != nil {
		logger.Logger.Error().Err(err).Str("member", member).Str("zset_name", zSetName).Msg("ZScore: Failed to get score")
	}
	return score, exists, err
}

// ZCard 实现 Redis ZCARD 命令
func (s *BotreonStore) ZCard(zSetName string) (int64, error) {
	typeKey := TypeOfKeyGet(zSetName)
	if err := s.db.View(func(txn *badger.Txn) error {
		typeItem, err := txn.Get(typeKey)
		if err == nil {
			typeVal, err := typeItem.ValueCopy(nil)
			if err != nil {
				return err
			}
			keyType := string(typeVal)
			if keyType != "" && keyType != KeyTypeSortedSet {
				return ErrWrongType
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		return nil
	}); err != nil {
		return 0, err
	}

	var card int64
	err := s.db.View(func(txn *badger.Txn) error {
		metaKey := sortedSetKeyMeta(zSetName)
		item, err := txn.Get(metaKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			card = 0
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			meta, err := decodeMeta(val)
			if err != nil {
				return err
			}
			card = meta.Card
			return nil
		})
	})
	return card, err
}

// ZCount 实现 Redis ZCOUNT 命令
func (s *BotreonStore) ZCount(zSetName string, minScore, maxScore float64) (int64, error) {
	var count int64
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		prefix := keyBadgerGet(prefixKeySortedSetBytes, []byte(zSetName+sortedSetIndex))
		opts.Prefix = prefix
		opts.PrefetchValues = false

		it := txn.NewIterator(opts)
		defer it.Close()

		startKey := append(prefix, encodeScore(minScore)...)
		for it.Seek(startKey); it.ValidForPrefix(prefix); it.Next() {
			score, _, _, ok := parseZSetIndexKey(it.Item().Key(), prefix)
			if !ok {
				continue
			}
			if score > maxScore {
				break
			}
			count++
		}
		return nil
	})
	return count, err
}

// ZMScore 实现 Redis ZMSCORE 命令
func (s *BotreonStore) ZMScore(zSetName string, members ...string) ([]float64, error) {
	scores := make([]float64, len(members))
	err := s.db.View(func(txn *badger.Txn) error {
		if err := checkKeyType(txn, zSetName, KeyTypeSortedSet); err != nil {
			return err
		}
		for i, member := range members {
			item, err := txn.Get(sortedSetKeyMember(zSetName, member))
			if errors.Is(err, badger.ErrKeyNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			err = item.Value(func(val []byte) error {
				scores[i], _ = decodeDataValue(val)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	return scores, err
}

// ZIncrBy 实现 Redis ZINCRBY 命令
func (s *BotreonStore) ZIncrBy(zSetName, member string, increment float64) (float64, error) {
	s.keyLockMgr.Lock(zSetName)
	defer s.keyLockMgr.Unlock(zSetName)
	var newScore float64
	err := s.retryUpdate(func(txn *badger.Txn) error {
		newScore = 0
		badgerTypeKey := TypeOfKeyGet(zSetName)
		if err := txn.Set(badgerTypeKey, []byte(KeyTypeSortedSet)); err != nil {
			return err
		}

		dataKey := sortedSetKeyMember(zSetName, member)
		var currentScore float64
		var dataVersion uint32
		memberExists := false

		item, err := txn.Get(dataKey)
		if err == nil {
			memberExists = true
			err = item.Value(func(val []byte) error {
				currentScore, dataVersion = decodeDataValue(val)
				return nil
			})
			if err != nil {
				return err
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}

		newScore = currentScore + increment

		metaKey := sortedSetKeyMeta(zSetName)
		var meta ZSetsMetaValue
		item, err = txn.Get(metaKey)
		if err == nil {
			err = item.Value(func(val []byte) error {
				meta, err = decodeMeta(val)
				return err
			})
			if err != nil {
				return err
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}

		var oldIndexKey []byte
		if memberExists {
			oldIndexKey = sortedSetKeyIndex(zSetName, currentScore, member, dataVersion)
		} else {
			meta.Card++
		}
		meta.Version++

		if oldIndexKey != nil {
			if err := txn.Delete(oldIndexKey); err != nil {
				return err
			}
		}

		if err := txn.Set(dataKey, encodeDataValue(newScore, meta.Version)); err != nil {
			return err
		}
		newIndexKey := sortedSetKeyIndex(zSetName, newScore, member, meta.Version)
		if err := txn.Set(newIndexKey, nil); err != nil {
			return err
		}

		return txn.Set(metaKey, encodeMeta(meta))
	}, 20, encodePropagateCommand([]byte("ZINCRBY"), []byte(zSetName), []byte(member), []byte(strconv.FormatFloat(increment, 'f', -1, 64))))
	s.markZSetDirty(zSetName)
	return newScore, err
}
