package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"

	"github.com/dgraph-io/badger/v4"
	"github.com/lbp0200/BoltDB/internal/logger"
)

const (
	prefixKeySortedSetBytes = "zset:"
	sortedSetIndex          = ":index:"
	sortedSetData           = ":data:"
	UnderScore              = "_"
	KeyTypeSortedSet        = "zset"
)

// ZSetMember 定义排序集成员结构体
type ZSetMember struct {
	Member string
	Score  float64
}

// ZSetsMetaValue 定义元数据结构体，存储成员数量和版本
type ZSetsMetaValue struct {
	Card    int64
	Version uint32
}

// encodeScore 优化分数编码，确保负分数正确排序
func encodeScore(score float64) []byte {
	bits := math.Float64bits(score)
	b := make([]byte, 8)
	if score >= 0 {
		bits = bits ^ 0x8000000000000000
	} else {
		bits = ^bits
	}
	binary.BigEndian.PutUint64(b, bits)
	return b
}

// decodeScore 从字节解码回 float64
func decodeScore(b []byte) float64 {
	bits := binary.BigEndian.Uint64(b)
	if bits&0x8000000000000000 == 0 {
		bits = ^bits
	} else {
		bits = bits ^ 0x8000000000000000
	}
	return math.Float64frombits(bits)
}

// encodeMeta 编码元数据
func encodeMeta(meta ZSetsMetaValue) []byte {
	b := make([]byte, 12)
	// #nosec G115 - Card is bounded by practical sorted set size limits
	binary.BigEndian.PutUint64(b[:8], uint64(meta.Card))
	binary.BigEndian.PutUint32(b[8:], meta.Version)
	return b
}

// decodeMeta 解码元数据
func decodeMeta(b []byte) (ZSetsMetaValue, error) {
	if len(b) != 12 {
		return ZSetsMetaValue{}, errors.New("invalid meta data")
	}
	// #nosec G115 - card is bounded by practical sorted set size limits
	card := int64(binary.BigEndian.Uint64(b[:8]))
	version := binary.BigEndian.Uint32(b[8:])
	return ZSetsMetaValue{Card: card, Version: version}, nil
}

func sortedSetKeyMeta(zSetName string) []byte {
	return keyBadgerGet(prefixKeySortedSetBytes, []byte(zSetName+":meta"))
}

func sortedSetKeyIndex(zSetName string, score float64, member string, version uint32) []byte {
	key := []byte(zSetName + sortedSetIndex)
	key = append(key, encodeScore(score)...)
	key = append(key, []byte(":"+member+":")...)
	key = append(key, encodeVersion(version)...)
	return keyBadgerGet(prefixKeySortedSetBytes, key)
}

func sortedSetKeyMember(zSetName, member string) []byte {
	return keyBadgerGet(prefixKeySortedSetBytes, []byte(zSetName+sortedSetData+member))
}

func encodeVersion(version uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, version)
	return b
}

// parseZSetIndexKey extracts score, member, version from a zset index key.
// Key format: zset:<name>:index:<8 byte score>:<member>:<4 byte version>
// This handles zSet names containing ':' unlike bytes.Split-based parsing.
func parseZSetIndexKey(key, prefix []byte) (score float64, member string, version uint32, ok bool) {
	if !bytes.HasPrefix(key, prefix) {
		return 0, "", 0, false
	}
	remaining := key[len(prefix):]
	if len(remaining) < 14 {
		return 0, "", 0, false
	}
	score = decodeScore(remaining[:8])
	member = string(remaining[9 : len(remaining)-5])
	version = binary.BigEndian.Uint32(remaining[len(remaining)-4:])
	return score, member, version, true
}

func keyBadgerGet(prefix string, key []byte) []byte {
	return append([]byte(prefix), key...)
}

// ZAdd 添加或更新成员分数
func (s *BotreonStore) ZAdd(zSetName string, members []ZSetMember) error {
	if len(members) == 0 {
		return nil
	}
	return s.retryUpdate(func(txn *badger.Txn) error {
		badgerTypeKey := TypeOfKeyGet(zSetName)

		// Check if key already exists with a different type
		item, err := txn.Get(badgerTypeKey)
		if err == nil {
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			keyType := string(val)
			if keyType != "" && keyType != KeyTypeSortedSet {
				return ErrWrongType
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}

		if err := txn.Set(badgerTypeKey, []byte(KeyTypeSortedSet)); err != nil {
			logger.Logger.Error().Err(err).Str("zset_name", zSetName).Msg("ZAdd: Failed to set type key")
			return err
		}

		// 获取元数据
		metaKey := sortedSetKeyMeta(zSetName)
		var meta ZSetsMetaValue
		item, err = txn.Get(metaKey)
		if err == nil {
			err = item.Value(func(val []byte) error {
				meta, err = decodeMeta(val)
				return err
			})
			if err != nil {
				logger.Logger.Error().Err(err).Str("zset_name", zSetName).Msg("ZAdd: Failed to decode meta")
				return err
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			logger.Logger.Error().Err(err).Str("zset_name", zSetName).Msg("ZAdd: Failed to get meta")
			return err
		}
		newMembers := int64(len(members))
		meta.Version++

		// 批量收集操作
		type operation struct {
			dataKey     []byte
			indexKey    []byte
			oldIndexKey []byte
			score       []byte
		}
		ops := make([]operation, 0, len(members))

		for _, m := range members {
			member := m.Member
			score := m.Score
			dataKey := sortedSetKeyMember(zSetName, member)

			// 检查旧分数
			var oldScore float64
			item, err := txn.Get(dataKey)
			if err == nil {
				var oldScoreBytes []byte
				err = item.Value(func(val []byte) error {
					oldScoreBytes = val
					return nil
				})
				if err != nil {
					logger.Logger.Error().Err(err).Str("zset_name", zSetName).Str("member", member).Msg("ZAdd: Failed to get old score")
					return err
				}
				oldScore = decodeScore(oldScoreBytes)
				newMembers-- // 替换现有成员，计数不变
			} else if !errors.Is(err, badger.ErrKeyNotFound) {
				logger.Logger.Error().Err(err).Str("zset_name", zSetName).Str("member", member).Msg("ZAdd: Failed to check member")
				return err
			}

			// 准备操作
			op := operation{
				dataKey:  dataKey,
				indexKey: sortedSetKeyIndex(zSetName, score, member, meta.Version),
				score:    encodeScore(score),
			}
			if err == nil {
				op.oldIndexKey = sortedSetKeyIndex(zSetName, oldScore, member, meta.Version-1)
			}
			ops = append(ops, op)
		}

		// 更新元数据计数
		meta.Card += newMembers
		if err := txn.Set(metaKey, encodeMeta(meta)); err != nil {
			logger.Logger.Error().Err(err).Str("zset_name", zSetName).Msg("ZAdd: Failed to set meta")
			return err
		}

		// 批量执行操作
		for _, op := range ops {
			if op.oldIndexKey != nil {
				if err := txn.Delete(op.oldIndexKey); err != nil {
					logger.Logger.Error().Err(err).Str("zset_name", zSetName).Msg("ZAdd: Failed to delete old index")
					return err
				}
			}
			if err := txn.Set(op.dataKey, op.score); err != nil {
				logger.Logger.Error().Err(err).Str("zset_name", zSetName).Msg("ZAdd: Failed to set data key")
				return err
			}
			if err := txn.Set(op.indexKey, nil); err != nil {
				logger.Logger.Error().Err(err).Str("zset_name", zSetName).Msg("ZAdd: Failed to set index key")
				return err
			}
		}

		// 成功路径不记录日志，避免性能影响
		// 只在 DEBUG 级别记录详细信息
		logger.Logger.Debug().
			Int("members_count", len(members)).
			Str("zset_name", zSetName).
			Int64("card", meta.Card).
			Msg("ZAdd: Successfully added members")
		return nil
	}, 20) // 最多重试 20 次（优化：减少重试次数，大部分冲突在前几次重试就能解决）
}

// ZRangeByScore 获取分数范围内的成员
func (s *BotreonStore) ZRangeByScore(zSetName string, minScore, maxScore float64, offset, count int, minExclusive, maxExclusive bool) ([]ZSetMember, error) {
	var results []ZSetMember
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		prefix := keyBadgerGet(prefixKeySortedSetBytes, []byte(zSetName+sortedSetIndex)) // e.g., "zset:myset:index:"
		opts.Prefix = prefix
		opts.PrefetchValues = false

		it := txn.NewIterator(opts)
		defer it.Close()

		startKey := append(prefix, encodeScore(minScore)...)
		current := 0
		for it.Seek(startKey); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			score, member, _, ok := parseZSetIndexKey(item.Key(), prefix)
			if !ok {
				logger.Logger.Debug().Str("key", string(item.Key())).Msg("ZRangeByScore: Invalid key format")
				continue
			}

			// Check max boundary
			if maxExclusive {
				if score >= maxScore {
					break
				}
			} else {
				if score > maxScore {
					break
				}
			}

			// Check min boundary
			if minExclusive && score <= minScore {
				continue
			}

			if current < offset {
				current++
				continue
			}
			if count > 0 && len(results) >= count {
				break
			}

			results = append(results, ZSetMember{Member: member, Score: score})
		}
		// 成功路径不记录日志，避免性能影响
		logger.Logger.Debug().
			Int("members_count", len(results)).
			Str("zset_name", zSetName).
			Msg("ZRangeByScore: Retrieved members")
		return nil
	})
	return results, err
}

// ZRem 删除成员

func (s *BotreonStore) ZRem(zSetName, member string) (int64, error) {
	var deleted int64 = 0
	err := s.retryUpdate(func(txn *badger.Txn) error {
		// Check if key already exists with a different type
		badgerTypeKey := TypeOfKeyGet(zSetName)
		typeItem, typeErr := txn.Get(badgerTypeKey)
		if typeErr == nil {
			typeVal, err := typeItem.ValueCopy(nil)
			if err != nil {
				return err
			}
			keyType := string(typeVal)
			if keyType != "" && keyType != KeyTypeSortedSet {
				return ErrWrongType
			}
		} else if !errors.Is(typeErr, badger.ErrKeyNotFound) {
			return typeErr
		}

		dataKey := sortedSetKeyMember(zSetName, member)
		item, err := txn.Get(dataKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			// Redis 规范：ZRem 对不存在的成员返回成功（删除 0 个），不是错误
			return nil
		}
		if err != nil {
			logger.Logger.Error().Err(err).Str("data_key", string(dataKey)).Msg("ZRem: Failed to get data key")
			return err
		}

		// 获取分数
		var scoreBytes []byte
		err = item.Value(func(val []byte) error {
			scoreBytes = val
			return nil
		})
		if err != nil {
			logger.Logger.Error().Err(err).Str("member", member).Msg("ZRem: Failed to get score")
			return err
		}
		score := decodeScore(scoreBytes)

		// 获取元数据
		metaKey := sortedSetKeyMeta(zSetName)
		var meta ZSetsMetaValue
		metaItem, err := txn.Get(metaKey)
		if err == nil {
			err = metaItem.Value(func(val []byte) error {
				meta, err = decodeMeta(val)
				return err
			})
			if err != nil {
				logger.Logger.Error().Err(err).Msg("ZRem: Failed to decode meta")
				return err
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			logger.Logger.Error().Err(err).Msg("ZRem: Failed to get meta")
			return err
		}

		if err := txn.Delete(dataKey); err != nil {
			logger.Logger.Error().Err(err).Msg("ZRem: Failed to delete data key")
			return err
		}

		indexKey := sortedSetKeyIndex(zSetName, score, member, meta.Version)
		if err := txn.Delete(indexKey); err != nil {
			logger.Logger.Error().Err(err).Msg("ZRem: Failed to delete index key")
			return err
		}

		// 更新元数据
		meta.Card--
		if meta.Card <= 0 {
			if err := txn.Delete(metaKey); err != nil {
				logger.Logger.Error().Err(err).Msg("ZRem: Failed to delete meta")
				return err
			}
			// 删除 TYPE_ 键（与 Redis 行为一致：空的有序集合不存在）
			if err := txn.Delete(badgerTypeKey); err != nil {
				logger.Logger.Error().Err(err).Msg("ZRem: Failed to delete type key")
				return err
			}
			logger.Logger.Debug().Str("member", member).Str("zset_name", zSetName).Msg("ZRem: Deleted member, set empty")
			deleted = 1
			return nil
		}
		if err := txn.Set(metaKey, encodeMeta(meta)); err != nil {
			logger.Logger.Error().Err(err).Msg("ZRem: Failed to set meta")
			return err
		}

		// 成功路径不记录日志，避免性能影响
		logger.Logger.Debug().
			Str("member", member).
			Str("zset_name", zSetName).
			Int64("card", meta.Card).
			Msg("ZRem: Successfully removed member")
		deleted = 1
		return nil
	}, 20) // 最多重试 20 次（优化：减少重试次数）
	if err != nil {
		return 0, err
	}
	return deleted, nil
}

// ZScore 获取成员分数
func (s *BotreonStore) ZScore(zSetName, member string) (float64, bool, error) {
	// Check if key exists with wrong type
	typeKey := TypeOfKeyGet(zSetName)
	var typeErr error
	_ = s.db.View(func(txn *badger.Txn) error {
		typeItem, err := txn.Get(typeKey)
		if err == nil {
			typeVal, err := typeItem.ValueCopy(nil)
			if err != nil {
				typeErr = err
				return err
			}
			keyType := string(typeVal)
			if keyType != "" && keyType != KeyTypeSortedSet {
				typeErr = ErrWrongType
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			typeErr = err
		}
		return nil
	})
	if typeErr != nil {
		return 0, false, typeErr
	}

	var score float64
	dataKey := sortedSetKeyMember(zSetName, member)

	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(dataKey)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			score = decodeScore(val)
			return nil
		})
	})

	if errors.Is(err, badger.ErrKeyNotFound) {
		return 0, false, nil
	}
	if err != nil {
		logger.Logger.Error().Err(err).Str("member", member).Str("zset_name", zSetName).Msg("ZScore: Failed to get score")
	}
	return score, true, err
}

// ZRange 获取指定排名范围的成员
func (s *BotreonStore) ZRange(zSetName string, start, stop int64) ([]*ZSetMember, error) {
	// Check if key exists with wrong type
	typeKey := TypeOfKeyGet(zSetName)
	var typeErr error
	_ = s.db.View(func(txn *badger.Txn) error {
		typeItem, err := txn.Get(typeKey)
		if err == nil {
			typeVal, err := typeItem.ValueCopy(nil)
			if err != nil {
				typeErr = err
				return err
			}
			keyType := string(typeVal)
			if keyType != "" && keyType != KeyTypeSortedSet {
				typeErr = ErrWrongType
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			typeErr = err
		}
		return nil
	})
	if typeErr != nil {
		return nil, typeErr
	}

	var results []*ZSetMember
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		//prefix := []byte(zSetName + sortedSetIndex) // e.g., "myset:index:"
		//opts.Prefix = prefix
		prefix := keyBadgerGet(prefixKeySortedSetBytes, []byte(zSetName+sortedSetIndex)) // e.g., "zset:myset:index:"
		opts.Prefix = prefix
		opts.PrefetchValues = false

		// 获取元数据
		metaKey := sortedSetKeyMeta(zSetName)
		var totalCount int64
		item, err := txn.Get(metaKey)
		if err == nil {
			var meta ZSetsMetaValue
			err = item.Value(func(val []byte) error {
				meta, err = decodeMeta(val)
				return err
			})
			if err != nil {
				logger.Logger.Error().Err(err).Str("zset_name", zSetName).Msg("ZRange: Failed to decode meta")
				return err
			}
			totalCount = meta.Card
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			logger.Logger.Error().Err(err).Str("zset_name", zSetName).Msg("ZRange: Failed to get meta")
			return err
		}

		// 处理负索引
		if start < 0 {
			start = totalCount + start
		}
		if stop < 0 {
			stop = totalCount + stop
		}
		if start < 0 {
			start = 0
		}
		if stop >= totalCount {
			stop = totalCount - 1
		}
		if start > stop || totalCount == 0 {
			return nil
		}

		it := txn.NewIterator(opts)
		defer it.Close()

		currentIndex := int64(0)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			if currentIndex < start {
				currentIndex++
				continue
			}
			if currentIndex > stop {
				break
			}

			item := it.Item()
			score, member, _, ok := parseZSetIndexKey(item.Key(), prefix)
			if !ok {
				logger.Logger.Debug().Str("key", string(item.Key())).Msg("ZRange: Invalid key format")
				continue
			}

			results = append(results, &ZSetMember{Member: member, Score: score})
			currentIndex++
		}
		// 成功路径不记录日志，避免性能影响
		logger.Logger.Debug().
			Int("members_count", len(results)).
			Str("zset_name", zSetName).
			Msg("ZRange: Retrieved members")
		return nil
	})
	return results, err
}

// ZSetDel 删除整个排序集
func (s *BotreonStore) ZSetDel(zSetName string) error {
	return s.retryUpdate(func(txn *badger.Txn) error {
		dataPrefix := []byte(zSetName + sortedSetData)
		indexPrefix := []byte(zSetName + sortedSetIndex)
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false

		// 删除数据键
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix(dataPrefix); it.Next() {
			if err := txn.Delete(it.Item().Key()); err != nil {
				logger.Logger.Error().Err(err).Str("zset_name", zSetName).Msg("ZSetDel: Failed to delete data key")
				return err
			}
		}

		// 删除索引键
		it = txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix(indexPrefix); it.Next() {
			if err := txn.Delete(it.Item().Key()); err != nil {
				logger.Logger.Error().Err(err).Str("zset_name", zSetName).Msg("ZSetDel: Failed to delete index key")
				return err
			}
		}

		// 删除元数据和类型键
		if err := txn.Delete(sortedSetKeyMeta(zSetName)); err != nil {
			logger.Logger.Error().Err(err).Str("zset_name", zSetName).Msg("ZSetDel: Failed to delete meta")
			return err
		}
		if err := txn.Delete(TypeOfKeyGet(zSetName)); err != nil {
			logger.Logger.Error().Err(err).Str("zset_name", zSetName).Msg("ZSetDel: Failed to delete type key")
			return err
		}

		// 成功路径不记录日志，避免性能影响
		logger.Logger.Debug().Str("zset_name", zSetName).Msg("ZSetDel: Successfully deleted set")
		return nil
	}, 20) // 最多重试 20 次（优化：减少重试次数）
}

// ZCard 实现 Redis ZCARD 命令，获取有序集合中成员的数量
func (s *BotreonStore) ZCard(zSetName string) (int64, error) {
	// Check if key exists with wrong type
	typeKey := TypeOfKeyGet(zSetName)
	var typeErr error
	_ = s.db.View(func(txn *badger.Txn) error {
		typeItem, err := txn.Get(typeKey)
		if err == nil {
			typeVal, err := typeItem.ValueCopy(nil)
			if err != nil {
				typeErr = err
				return err
			}
			keyType := string(typeVal)
			if keyType != "" && keyType != KeyTypeSortedSet {
				typeErr = ErrWrongType
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			typeErr = err
		}
		return nil
	})
	if typeErr != nil {
		return 0, typeErr
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

// ZCount 实现 Redis ZCOUNT 命令，计算在有序集合中指定区间分数的成员数
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

// ZIncrBy 实现 Redis ZINCRBY 命令，增加成员的分数
func (s *BotreonStore) ZIncrBy(zSetName, member string, increment float64) (float64, error) {
	var newScore float64
	err := s.retryUpdate(func(txn *badger.Txn) error {
		badgerTypeKey := TypeOfKeyGet(zSetName)
		if err := txn.Set(badgerTypeKey, []byte(KeyTypeSortedSet)); err != nil {
			return err
		}

		dataKey := sortedSetKeyMember(zSetName, member)
		var currentScore float64
		memberExists := false

		// 获取当前分数
		item, err := txn.Get(dataKey)
		if err == nil {
			memberExists = true
			err = item.Value(func(val []byte) error {
				currentScore = decodeScore(val)
				return nil
			})
			if err != nil {
				return err
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}

		// 计算新分数
		newScore = currentScore + increment

		// 获取元数据
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
			oldIndexKey = sortedSetKeyIndex(zSetName, currentScore, member, meta.Version)
		} else {
			meta.Card++
		}
		meta.Version++

		// 删除旧索引
		if oldIndexKey != nil {
			if err := txn.Delete(oldIndexKey); err != nil {
				return err
			}
		}

		// 设置新数据键和索引键
		if err := txn.Set(dataKey, encodeScore(newScore)); err != nil {
			return err
		}
		newIndexKey := sortedSetKeyIndex(zSetName, newScore, member, meta.Version)
		if err := txn.Set(newIndexKey, nil); err != nil {
			return err
		}

		// 更新元数据
		return txn.Set(metaKey, encodeMeta(meta))
	}, 20) // 最多重试 20 次（优化：减少重试次数）
	return newScore, err
}

// ZRank 实现 Redis ZRANK 命令，返回成员的排名（从0开始，分数从小到大）
func (s *BotreonStore) ZRank(zSetName, member string) (int64, error) {
	var rank int64 = -1
	err := s.db.View(func(txn *badger.Txn) error {
		// 获取成员分数
		dataKey := sortedSetKeyMember(zSetName, member)
		item, err := txn.Get(dataKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		var score float64
		err = item.Value(func(val []byte) error {
			score = decodeScore(val)
			return nil
		})
		if err != nil {
			return err
		}

		// 遍历索引，计算排名
		opts := badger.DefaultIteratorOptions
		prefix := keyBadgerGet(prefixKeySortedSetBytes, []byte(zSetName+sortedSetIndex))
		opts.Prefix = prefix
		opts.PrefetchValues = false

		it := txn.NewIterator(opts)
		defer it.Close()

		rank = 0
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			memberScore, memberName, _, ok := parseZSetIndexKey(it.Item().Key(), prefix)
			if !ok {
				continue
			}
			if memberScore < score || (memberScore == score && memberName < member) {
				rank++
			} else if memberScore == score && memberName == member {
				return nil
			} else {
				break
			}
		}
		rank = -1 // 未找到
		return nil
	})
	return rank, err
}

// ZRevRank 实现 Redis ZREVRANK 命令，返回成员的排名（从0开始，分数从大到小）
func (s *BotreonStore) ZRevRank(zSetName, member string) (int64, error) {
	var rank int64 = -1
	err := s.db.View(func(txn *badger.Txn) error {
		// 检查成员是否存在
		dataKey := sortedSetKeyMember(zSetName, member)
		_, err := txn.Get(dataKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}

		// 获取总数
		metaKey := sortedSetKeyMeta(zSetName)
		var totalCount int64
		metaItem, err := txn.Get(metaKey)
		if err == nil {
			err = metaItem.Value(func(val []byte) error {
				meta, err := decodeMeta(val)
				if err != nil {
					return err
				}
				totalCount = meta.Card
				return nil
			})
			if err != nil {
				return err
			}
		}

		// 计算正向排名，然后转换为反向排名
		forwardRank, err := s.ZRank(zSetName, member)
		if err != nil {
			return err
		}
		if forwardRank == -1 {
			rank = -1
			return nil
		}
		rank = totalCount - 1 - forwardRank
		return nil
	})
	return rank, err
}

// ZRevRange 实现 Redis ZREVRANGE 命令，返回有序集中指定区间内的成员，通过索引，分数从高到低
func (s *BotreonStore) ZRevRange(zSetName string, start, stop int64) ([]*ZSetMember, error) {
	// 先获取正向范围
	forwardResults, err := s.ZRange(zSetName, 0, -1)
	if err != nil {
		return nil, err
	}

	// 反转结果
	totalCount := int64(len(forwardResults))
	if totalCount == 0 {
		return []*ZSetMember{}, nil
	}

	// 处理负索引
	if start < 0 {
		start = totalCount + start
	}
	if stop < 0 {
		stop = totalCount + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= totalCount {
		stop = totalCount - 1
	}
	if start > stop {
		return []*ZSetMember{}, nil
	}

	// 反转并提取范围
	reversed := make([]*ZSetMember, totalCount)
	for i := int64(0); i < totalCount; i++ {
		reversed[i] = forwardResults[totalCount-1-i]
	}

	return reversed[start : stop+1], nil
}

// ZRevRangeByScore 实现 Redis ZREVRANGEBYSCORE 命令，返回有序集中指定分数区间内的成员，分数从高到低排序
func (s *BotreonStore) ZRevRangeByScore(zSetName string, maxScore, minScore float64, offset, count int, minExclusive, maxExclusive bool) ([]ZSetMember, error) {
	// 先获取正向范围
	forwardResults, err := s.ZRangeByScore(zSetName, minScore, maxScore, 0, 0, minExclusive, maxExclusive)
	if err != nil {
		return nil, err
	}

	// 反转结果
	var results []ZSetMember
	for i := len(forwardResults) - 1; i >= 0; i-- {
		results = append(results, forwardResults[i])
	}

	// 应用offset和count
	if offset > 0 && offset < len(results) {
		results = results[offset:]
	} else if offset >= len(results) {
		results = []ZSetMember{}
	}

	if count > 0 && count < len(results) {
		results = results[:count]
	}

	return results, nil
}

// ZRemRangeByRank 实现 Redis ZREMRANGEBYRANK 命令，移除有序集中指定排名区间的所有成员
func (s *BotreonStore) ZRemRangeByRank(zSetName string, start, stop int64) (int64, error) {
	// 先获取范围内的成员
	members, err := s.ZRange(zSetName, start, stop)
	if err != nil {
		return 0, err
	}

	// 删除每个成员
	var removed int64
	for _, member := range members {
		if _, err := s.ZRem(zSetName, member.Member); err != nil {
			return removed, err
		}
		removed++
	}
	return removed, nil
}

// ZRemRangeByScore 实现 Redis ZREMRANGEBYSCORE 命令，移除有序集中指定分数区间的所有成员
func (s *BotreonStore) ZRemRangeByScore(zSetName string, minScore, maxScore float64, minExclusive, maxExclusive bool) (int64, error) {
	// 先获取范围内的成员
	members, err := s.ZRangeByScore(zSetName, minScore, maxScore, 0, 0, minExclusive, maxExclusive)
	if err != nil {
		return 0, err
	}

	// 删除每个成员
	var removed int64
	for _, member := range members {
		if _, err := s.ZRem(zSetName, member.Member); err != nil {
			return removed, err
		}
		removed++
	}
	return removed, nil
}

// ZPopMax 实现 Redis ZPOPMAX 命令，移除并返回有序集合中分数最高的成员
func (s *BotreonStore) ZPopMax(zSetName string, count int) ([]ZSetMember, error) {
	// 先获取最后count个成员（分数最高的）
	members, err := s.ZRevRange(zSetName, 0, int64(count-1))
	if err != nil {
		return nil, err
	}

	// 删除并返回
	var results []ZSetMember
	for _, member := range members {
		if _, err := s.ZRem(zSetName, member.Member); err != nil {
			return results, err
		}
		results = append(results, ZSetMember{Member: member.Member, Score: member.Score})
	}
	return results, nil
}

// ZPopMin 实现 Redis ZPOPMIN 命令，移除并返回有序集合中分数最低的成员
func (s *BotreonStore) ZPopMin(zSetName string, count int) ([]ZSetMember, error) {
	// 先获取前count个成员（分数最低的）
	members, err := s.ZRange(zSetName, 0, int64(count-1))
	if err != nil {
		return nil, err
	}

	// 删除并返回
	var results []ZSetMember
	for _, member := range members {
		if _, err := s.ZRem(zSetName, member.Member); err != nil {
			return results, err
		}
		results = append(results, ZSetMember{Member: member.Member, Score: member.Score})
	}
	return results, nil
}

// ZUnionStore 实现 Redis ZUNIONSTORE 命令，计算并集并存储到目标集合
func (s *BotreonStore) ZUnionStore(destination string, keys []string, weights []float64, aggregate string) (int64, error) {
	// 先收集所有成员的分数（考虑权重和聚合方式）
	memberScores := make(map[string]float64)

	for i, key := range keys {
		weight := 1.0
		if i < len(weights) && weights[i] != 0 {
			weight = weights[i]
		}

		// 获取所有成员
		members, err := s.ZRange(key, 0, -1)
		if err != nil {
			return 0, err
		}

		for _, member := range members {
			score := member.Score * weight
			existingScore, exists := memberScores[member.Member]

			if !exists {
				memberScores[member.Member] = score
			} else {
				// 根据聚合方式计算
				switch aggregate {
				case "SUM", "":
					memberScores[member.Member] = existingScore + score
				case "MIN":
					if score < existingScore {
						memberScores[member.Member] = score
					}
				case "MAX":
					if score > existingScore {
						memberScores[member.Member] = score
					}
				}
			}
		}
	}

	// 删除目标集合的现有数据
	_ = s.ZSetDel(destination)

	// 添加所有成员到目标集合
	zsetMembers := make([]ZSetMember, 0, len(memberScores))
	for member, score := range memberScores {
		zsetMembers = append(zsetMembers, ZSetMember{Member: member, Score: score})
	}

	if len(zsetMembers) > 0 {
		if err := s.ZAdd(destination, zsetMembers); err != nil {
			return 0, err
		}
	}

	return int64(len(memberScores)), nil
}

// ZInterStore 实现 Redis ZINTERSTORE 命令，计算交集并存储到目标集合
func (s *BotreonStore) ZInterStore(destination string, keys []string, weights []float64, aggregate string) (int64, error) {
	if len(keys) == 0 {
		// 删除目标集合
		_ = s.ZSetDel(destination)
		return 0, nil
	}

	// 获取第一个集合的所有成员
	firstMembers, err := s.ZRange(keys[0], 0, -1)
	if err != nil {
		return 0, err
	}

	// 收集所有成员的分数（考虑权重和聚合方式）
	memberScores := make(map[string]float64)
	firstWeight := 1.0
	if len(weights) > 0 && weights[0] != 0 {
		firstWeight = weights[0]
	}

	// 初始化第一个集合的成员
	for _, member := range firstMembers {
		memberScores[member.Member] = member.Score * firstWeight
	}

	// 检查每个成员是否在其他所有集合中
	for i := 1; i < len(keys); i++ {
		weight := 1.0
		if i < len(weights) && weights[i] != 0 {
			weight = weights[i]
		}

		otherMembers, err := s.ZRange(keys[i], 0, -1)
		if err != nil {
			return 0, err
		}

		otherMemberMap := make(map[string]float64)
		for _, member := range otherMembers {
			otherMemberMap[member.Member] = member.Score * weight
		}

		// 只保留在所有集合中都存在的成员
		for member := range memberScores {
			if otherScore, exists := otherMemberMap[member]; exists {
				// 根据聚合方式更新分数
				switch aggregate {
				case "SUM", "":
					memberScores[member] += otherScore
				case "MIN":
					if otherScore < memberScores[member] {
						memberScores[member] = otherScore
					}
				case "MAX":
					if otherScore > memberScores[member] {
						memberScores[member] = otherScore
					}
				}
			} else {
				// 成员不在这个集合中，删除
				delete(memberScores, member)
			}
		}
	}

	// 删除目标集合的现有数据
	_ = s.ZSetDel(destination)

	// 添加所有成员到目标集合
	zsetMembers := make([]ZSetMember, 0, len(memberScores))
	for member, score := range memberScores {
		zsetMembers = append(zsetMembers, ZSetMember{Member: member, Score: score})
	}

	if len(zsetMembers) > 0 {
		if err := s.ZAdd(destination, zsetMembers); err != nil {
			return 0, err
		}
	}

	return int64(len(memberScores)), nil
}

// ZDiffStore 实现 Redis ZDIFFSTORE 命令，计算差集并存储到目标集合
func (s *BotreonStore) ZDiffStore(destination string, keys []string) (int64, error) {
	if len(keys) == 0 {
		// 删除目标集合
		_ = s.ZSetDel(destination)
		return 0, nil
	}

	// 获取第一个集合的所有成员
	firstMembers, err := s.ZRange(keys[0], 0, -1)
	if err != nil {
		return 0, err
	}

	// 构建其他集合的成员集合
	otherMembers := make(map[string]bool)
	for i := 1; i < len(keys); i++ {
		members, err := s.ZRange(keys[i], 0, -1)
		if err != nil {
			return 0, err
		}
		for _, member := range members {
			otherMembers[member.Member] = true
		}
	}

	// 找出只在第一个集合中的成员
	zsetMembers := make([]ZSetMember, 0)
	for _, member := range firstMembers {
		if !otherMembers[member.Member] {
			zsetMembers = append(zsetMembers, ZSetMember{Member: member.Member, Score: member.Score})
		}
	}

	// 删除目标集合的现有数据
	_ = s.ZSetDel(destination)

	// 添加所有成员到目标集合
	if len(zsetMembers) > 0 {
		if err := s.ZAdd(destination, zsetMembers); err != nil {
			return 0, err
		}
	}

	return int64(len(zsetMembers)), nil
}

// ZDiff returns the difference of the first sorted set with all subsequent ones.
func (s *BotreonStore) ZDiff(keys []string) ([]ZSetMember, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	firstMembers, err := s.ZRange(keys[0], 0, -1)
	if err != nil {
		return nil, err
	}

	otherMembers := make(map[string]bool)
	for i := 1; i < len(keys); i++ {
		members, err := s.ZRange(keys[i], 0, -1)
		if err != nil {
			return nil, err
		}
		for _, member := range members {
			otherMembers[member.Member] = true
		}
	}

	result := make([]ZSetMember, 0)
	for _, member := range firstMembers {
		if !otherMembers[member.Member] {
			result = append(result, ZSetMember{Member: member.Member, Score: member.Score})
		}
	}
	return result, nil
}

// ZLexCount 实现 Redis ZLEXCOUNT 命令，计算有序集合中成员值介于min和max之间的成员数量（字典序）
func (s *BotreonStore) ZLexCount(zSetName, min, max string) (int64, error) {
	// 获取所有成员（按分数排序）
	members, err := s.ZRange(zSetName, 0, -1)
	if err != nil {
		return 0, err
	}

	// 按字典序过滤
	var count int64
	for _, member := range members {
		memberStr := member.Member
		if compareLex(min, memberStr, true) && compareLex(memberStr, max, false) {
			count++
		}
	}
	return count, nil
}

// compareLex 比较两个字符串（字典序），支持开区间和闭区间
// a是范围边界（如"[a"或"(a"），b是要比较的成员
// inclusive表示a是否包含在比较结果中（a <= b 或 a < b）
func compareLex(a, b string, inclusive bool) bool {
	// 处理无穷边界
	if a == "-" {
		return true // 负无穷，总是包含
	}
	if a == "+" {
		return false // 正无穷，不包含
	}
	if b == "-" {
		return false
	}
	if b == "+" {
		return true
	}

	// 提取a的实际值和包含性
	var aVal string
	var aIncl bool
	if len(a) > 0 {
		switch a[0] {
		case '(':
			aVal = a[1:]
			aIncl = false
		case '[':
			aVal = a[1:]
			aIncl = true
		default:
			aVal = a
			aIncl = inclusive
		}
	} else {
		aVal = a
		aIncl = inclusive
	}

	// 提取b的实际值
	var bVal string
	if len(b) > 0 {
		if b[0] == '(' || b[0] == '[' {
			bVal = b[1:]
		} else {
			bVal = b
		}
	} else {
		bVal = b
	}

	// 当a是普通成员时，比较类型由b的边界格式决定
	// 当a是边界格式时，比较类型由a的边界格式决定
	var useIncl bool
	if len(a) > 0 && (a[0] == '(' || a[0] == '[') {
		useIncl = aIncl
	} else {
		// a是普通成员，比较类型由b决定
		if len(b) > 0 && (b[0] == '(' || b[0] == '[') {
			useIncl = b[0] == '[' // b的包含性
		} else {
			useIncl = inclusive
		}
	}

	if useIncl {
		return aVal <= bVal
	}
	return aVal < bVal
}

// ZRangeByLex 实现 Redis ZRANGEBYLEX 命令，返回有序集合中成员值介于min和max之间的成员（字典序）
func (s *BotreonStore) ZRangeByLex(zSetName, min, max string, offset, count int) ([]string, error) {
	// 获取所有成员（按分数排序，相同分数按字典序）
	members, err := s.ZRange(zSetName, 0, -1)
	if err != nil {
		return nil, err
	}

	// 按字典序过滤和排序
	var filtered []string
	for _, member := range members {
		memberStr := member.Member
		// min <= member <= max
		minOK := compareLex(min, memberStr, true)
		// 对于max，需要检查member <= max（如果max是闭区间）或member < max（如果max是开区间）
		var maxOK bool
		if max == "+" {
			maxOK = true
		} else if len(max) > 0 && max[0] == '(' {
			// 开区间
			maxOK = memberStr < max[1:]
		} else if len(max) > 0 && max[0] == '[' {
			// 闭区间
			maxOK = memberStr <= max[1:]
		} else {
			maxOK = memberStr <= max
		}
		if minOK && maxOK {
			filtered = append(filtered, memberStr)
		}
	}

	// 应用offset和count
	var results []string
	if offset < 0 {
		offset = 0
	}
	if offset < len(filtered) {
		end := offset + count
		if count <= 0 || end > len(filtered) {
			end = len(filtered)
		}
		results = filtered[offset:end]
	}
	return results, nil
}

// ZRevRangeByLex 实现 Redis ZREVRANGEBYLEX 命令，返回有序集合中成员值介于min和max之间的成员（字典序，反向）
func (s *BotreonStore) ZRevRangeByLex(zSetName, max, min string, offset, count int) ([]string, error) {
	// 先获取正向范围
	results, err := s.ZRangeByLex(zSetName, min, max, 0, 0)
	if err != nil {
		return nil, err
	}

	// 反转结果
	for i, j := 0, len(results)-1; i < j; i, j = i+1, j-1 {
		results[i], results[j] = results[j], results[i]
	}

	// 应用offset和count
	if offset < 0 {
		offset = 0
	}
	if offset < len(results) {
		end := offset + count
		if count <= 0 || end > len(results) {
			end = len(results)
		}
		results = results[offset:end]
	} else {
		results = []string{}
	}

	return results, nil
}

// ZRemRangeByLex 实现 Redis ZREMRANGEBYLEX 命令，移除有序集合中成员值介于min和max之间的成员（字典序）
func (s *BotreonStore) ZRemRangeByLex(zSetName, min, max string) (int64, error) {
	var removed int64
	err := s.retryUpdate(func(txn *badger.Txn) error {
		// 获取范围内的成员
		members, err := s.ZRangeByLex(zSetName, min, max, 0, 0)
		if err != nil {
			return err
		}

		// 删除每个成员
		for _, member := range members {
			if _, err := s.ZRem(zSetName, member); err != nil {
				return err
			}
			removed++
		}
		return nil
	}, 20) // 最多重试 20 次（优化：减少重试次数）
	return removed, err
}

// ZMScore 实现 Redis ZMSCORE 命令，批量获取多个成员的分数
func (s *BotreonStore) ZMScore(zSetName string, members ...string) ([]float64, error) {
	scores := make([]float64, len(members))
	for i, member := range members {
		score, exists, err := s.ZScore(zSetName, member)
		if err != nil {
			return nil, err
		}
		if exists {
			scores[i] = score
		} else {
			scores[i] = 0 // Redis返回nil，这里用0表示不存在
		}
	}
	return scores, nil
}

// BZPopMax 实现 Redis BZPOPMAX 命令，阻塞式弹出分数最高的成员（简化版本：非阻塞）
func (s *BotreonStore) BZPopMax(keys []string, timeout int) (string, *ZSetMember, error) {
	for _, key := range keys {
		members, err := s.ZPopMax(key, 1)
		if err != nil {
			return "", nil, err
		}
		if len(members) > 0 {
			return key, &members[0], nil
		}
	}
	return "", nil, nil
}

// BZPopMin 实现 Redis BZPOPMIN 命令，阻塞式弹出分数最低的成员（简化版本：非阻塞）
func (s *BotreonStore) BZPopMin(keys []string, timeout int) (string, *ZSetMember, error) {
	for _, key := range keys {
		members, err := s.ZPopMin(key, 1)
		if err != nil {
			return "", nil, err
		}
		if len(members) > 0 {
			return key, &members[0], nil
		}
	}
	return "", nil, nil
}

// ZScanResult 定义 ZSCAN 命令的返回结果
type ZScanResult struct {
	Cursor  uint64
	Members []ZSetMember
}

// ZScan 实现 Redis ZSCAN 命令，增量迭代有序集合的成员
func (s *BotreonStore) ZScan(zSetName string, cursor uint64, pattern string, count int) (ZScanResult, error) {
	var result ZScanResult
	result.Cursor = 0
	result.Members = []ZSetMember{}

	if count <= 0 {
		count = 10 // 默认值
	}

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		prefix := keyBadgerGet(prefixKeySortedSetBytes, []byte(zSetName+sortedSetIndex))
		opts.Prefix = prefix
		opts.PrefetchValues = false

		iter := txn.NewIterator(opts)
		defer iter.Close()

		currentPos := uint64(0)
		collected := 0

		// 如果cursor不为0，需要跳过前面的成员
		if cursor > 0 {
			for iter.Seek(prefix); iter.ValidForPrefix(prefix) && currentPos < cursor; iter.Next() {
				currentPos++
			}
		} else {
			iter.Seek(prefix)
		}

		// 收集匹配的成员
		for iter.ValidForPrefix(prefix) && collected < count {
			score, member, _, ok := parseZSetIndexKey(iter.Item().Key(), prefix)
			if ok {
				if pattern == "" || pattern == "*" || matchPattern(member, pattern) {
					result.Members = append(result.Members, ZSetMember{Member: member, Score: score})
					collected++
				}
			}

			currentPos++
			iter.Next()
		}

		// 检查是否还有更多成员
		if iter.ValidForPrefix(prefix) {
			result.Cursor = currentPos
		} else {
			result.Cursor = 0 // 0表示迭代完成
		}

		return nil
	})
	return result, err
}

// ZRandMember returns random members from a sorted set.
// If count > 0: returns up to count distinct members (no repeats).
// If count < 0: returns -count members, allowing repeats.
// If count == 0: returns 1 random member.
func (s *BotreonStore) ZRandMember(zSetName string, count int) ([]ZSetMember, error) {
	var members []ZSetMember

	typeKey := TypeOfKeyGet(zSetName)
	var typeErr error
	_ = s.db.View(func(txn *badger.Txn) error {
		typeItem, err := txn.Get(typeKey)
		if err == nil {
			typeVal, err := typeItem.ValueCopy(nil)
			if err != nil {
				typeErr = err
				return err
			}
			keyType := string(typeVal)
			if keyType != "" && keyType != KeyTypeSortedSet {
				typeErr = ErrWrongType
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			typeErr = err
		}
		return nil
	})
	if typeErr != nil {
		return nil, typeErr
	}

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		prefix := keyBadgerGet(prefixKeySortedSetBytes, []byte(zSetName+sortedSetIndex))
		opts.Prefix = prefix
		opts.PrefetchValues = false

		it := txn.NewIterator(opts)
		defer it.Close()

		var allMembers []ZSetMember
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			score, member, _, ok := parseZSetIndexKey(it.Item().Key(), prefix)
			if ok {
				allMembers = append(allMembers, ZSetMember{Member: member, Score: score})
			}
		}

		if len(allMembers) == 0 {
			return nil
		}

		if count == 0 {
			idx := randomIntn(len(allMembers))
			members = append(members, allMembers[idx])
		} else if count < 0 {
			n := -count
			for i := 0; i < n; i++ {
				idx := randomIntn(len(allMembers))
				members = append(members, allMembers[idx])
			}
		} else {
			n := count
			if n > len(allMembers) {
				n = len(allMembers)
			}
			randomShuffle(len(allMembers), func(i, j int) {
				allMembers[i], allMembers[j] = allMembers[j], allMembers[i]
			})
			members = allMembers[:n]
		}
		return nil
	})
	return members, err
}
