package store

import (
	"errors"
	"strconv"

	"github.com/dgraph-io/badger/v4"
	"github.com/lbp0200/BoltDB/internal/logger"
)

// ZAdd 添加或更新成员分数
func (s *BotreonStore) ZAdd(zSetName string, members []ZSetMember) error {
	if err := s.checkErrorInjector("ZAdd"); err != nil {
		return err
	}
	s.keyLockMgr.Lock(zSetName)
	defer s.keyLockMgr.Unlock(zSetName)
	if len(members) == 0 {
		s.markZSetDirty(zSetName)
		return nil
	}
	var addedNewMember bool
	err := s.retryUpdate(func(txn *badger.Txn) error {
		var err error
		addedNewMember, err = zAddMembersInTxn(txn, zSetName, members)
		return err
	}, 20, func() []byte {
		// D4 全重放：ZADD zSetName <score> <member>...——pairs 摊平
		args := make([][]byte, 0, 1+2*len(members))
		args = append(args, []byte("ZADD"), []byte(zSetName))
		for _, m := range members {
			args = append(args, []byte(strconv.FormatFloat(m.Score, 'f', -1, 64)), []byte(m.Member))
		}
		return encodePropagateCommand(args...)
	}())
	if err == nil && addedNewMember {
		s.notifyBlockingZPop(zSetName)
	}
	s.markZSetDirty(zSetName)
	return err
}

// ZAddOptions 携带 ZADD 的可选参数（Redis 语义）。
// NX: 仅当成员不存在时添加；XX: 仅当成员已存在时更新。
// GT: 仅当新 score 大于旧 score 时更新；LT: 仅当新 score 小于旧 score 时更新。
// CH: 返回添加+更新总数（默认仅返回新增数）。
// INCR: 按增量模式（score = 旧 score + 传入值），一次只能有一个成员。
type ZAddOptions struct {
	NX   bool
	XX   bool
	GT   bool
	LT   bool
	CH   bool
	INCR bool
}

// ZAddWithOptions 实现带选项的 ZADD（NX/XX/GT/LT/CH/INCR）。
// 返回变更计数：默认新增数；CH 时含更新数；INCR 时返回新 score（int64 语义）。
func (s *BotreonStore) ZAddWithOptions(zSetName string, opts ZAddOptions, members []ZSetMember) (int64, error) {
	if err := s.checkErrorInjector("ZAdd"); err != nil {
		return 0, err
	}
	s.keyLockMgr.Lock(zSetName)
	defer s.keyLockMgr.Unlock(zSetName)
	if len(members) == 0 {
		s.markZSetDirty(zSetName)
		return 0, nil
	}
	var changed int64
	var addedNewMember bool
	err := s.retryUpdate(func(txn *badger.Txn) error {
		var err error
		changed, addedNewMember, err = zAddMembersInTxnOpts(txn, zSetName, opts, members)
		return err
	}, 20, func() []byte {
		// D4 全重放：ZADD zSetName [NX|XX] [GT|LT] [CH] [INCR] <score> <member>...
		args := make([][]byte, 0, 1+6+2*len(members))
		args = append(args, []byte("ZADD"), []byte(zSetName))
		if opts.NX {
			args = append(args, []byte("NX"))
		}
		if opts.XX {
			args = append(args, []byte("XX"))
		}
		if opts.GT {
			args = append(args, []byte("GT"))
		}
		if opts.LT {
			args = append(args, []byte("LT"))
		}
		if opts.CH {
			args = append(args, []byte("CH"))
		}
		if opts.INCR {
			args = append(args, []byte("INCR"))
		}
		for _, m := range members {
			args = append(args, []byte(strconv.FormatFloat(m.Score, 'f', -1, 64)), []byte(m.Member))
		}
		return encodePropagateCommand(args...)
	}())
	if err == nil && addedNewMember {
		s.notifyBlockingZPop(zSetName)
	}
	s.markZSetDirty(zSetName)
	return changed, err
}

// zAddMembersInTxnOpts adds/updates members with NX/XX/GT/LT/CH/INCR handling
// inside an open update transaction.
func zAddMembersInTxnOpts(txn *badger.Txn, zSetName string, opts ZAddOptions, members []ZSetMember) (int64, bool, error) {
	if len(members) == 0 {
		return 0, false, nil
	}

	badgerTypeKey := TypeOfKeyGet(zSetName)
	item, err := txn.Get(badgerTypeKey)
	if err == nil {
		val, err := item.ValueCopy(nil)
		if err != nil {
			return 0, false, err
		}
		keyType := string(val)
		if keyType != "" && keyType != KeyTypeSortedSet {
			return 0, false, ErrWrongType
		}
	} else if !errors.Is(err, badger.ErrKeyNotFound) {
		return 0, false, err
	}

	if err := txn.Set(badgerTypeKey, []byte(KeyTypeSortedSet)); err != nil {
		logger.Logger.Error().Err(err).Str("zset_name", zSetName).Msg("ZAdd: Failed to set type key")
		return 0, false, err
	}

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
			return 0, false, err
		}
	} else if !errors.Is(err, badger.ErrKeyNotFound) {
		logger.Logger.Error().Err(err).Str("zset_name", zSetName).Msg("ZAdd: Failed to get meta")
		return 0, false, err
	}

	meta.Version++

	type operation struct {
		dataKey     []byte
		indexKey    []byte
		oldIndexKey []byte
		score       []byte
	}
	ops := make([]operation, 0, len(members))
	var changed int64
	var addedCount int64

	for _, m := range members {
		member := m.Member
		score := m.Score
		dataKey := sortedSetKeyMember(zSetName, member)

		var oldScore float64
		var oldVersion uint32
		exists := false
		item, err = txn.Get(dataKey)
		if err == nil {
			var oldDataVal []byte
			err = item.Value(func(val []byte) error {
				oldDataVal = val
				return nil
			})
			if err != nil {
				logger.Logger.Error().Err(err).Str("zset_name", zSetName).Str("member", member).Msg("ZAdd: Failed to get old score")
				return 0, false, err
			}
			oldScore, oldVersion = decodeDataValue(oldDataVal)
			exists = true
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			logger.Logger.Error().Err(err).Str("zset_name", zSetName).Str("member", member).Msg("ZAdd: Failed to check member")
			return 0, false, err
		}

		// NX: 只添加不更新；XX: 只更新不添加
		if opts.NX && exists {
			continue
		}
		if opts.XX && !exists {
			continue
		}
		// GT/LT: 仅当新分数满足条件时更新（只对已存在成员生效）
		if exists {
			if opts.GT && score <= oldScore {
				continue
			}
			if opts.LT && score >= oldScore {
				continue
			}
		}
		// INCR: 新分数 = 旧分数 + 传入值
		if opts.INCR {
			if exists {
				score = oldScore + m.Score
			}
		}

		op := operation{
			dataKey:  dataKey,
			indexKey: sortedSetKeyIndex(zSetName, score, member, meta.Version),
			score:    encodeDataValue(score, meta.Version),
		}
		if exists {
			op.oldIndexKey = sortedSetKeyIndex(zSetName, oldScore, member, oldVersion)
		}
		ops = append(ops, op)
		changed++
		if !exists {
			addedCount++
		}
	}

	meta.Card += addedCount
	if err := txn.Set(metaKey, encodeMeta(meta)); err != nil {
		logger.Logger.Error().Err(err).Str("zset_name", zSetName).Msg("ZAdd: Failed to set meta")
		return 0, false, err
	}

	for _, op := range ops {
		if op.oldIndexKey != nil {
			if err := txn.Delete(op.oldIndexKey); err != nil {
				logger.Logger.Error().Err(err).Str("zset_name", zSetName).Msg("ZAdd: Failed to delete old index")
				return 0, false, err
			}
		}
		if err := txn.Set(op.dataKey, op.score); err != nil {
			logger.Logger.Error().Err(err).Str("zset_name", zSetName).Msg("ZAdd: Failed to set member score")
			return 0, false, err
		}
		if err := txn.Set(op.indexKey, []byte{1}); err != nil {
			logger.Logger.Error().Err(err).Str("zset_name", zSetName).Msg("ZAdd: Failed to set index")
			return 0, false, err
		}
	}

	// CH: 返回添加+更新总数；默认仅返回新增数
	if !opts.CH {
		changed = addedCount
	}
	return changed, addedCount > 0, nil
}

// zAddMembersInTxn adds or updates members inside an open update transaction.
func zAddMembersInTxn(txn *badger.Txn, zSetName string, members []ZSetMember) (bool, error) {
	if len(members) == 0 {
		return false, nil
	}

	badgerTypeKey := TypeOfKeyGet(zSetName)
	item, err := txn.Get(badgerTypeKey)
	if err == nil {
		val, err := item.ValueCopy(nil)
		if err != nil {
			return false, err
		}
		keyType := string(val)
		if keyType != "" && keyType != KeyTypeSortedSet {
			return false, ErrWrongType
		}
	} else if !errors.Is(err, badger.ErrKeyNotFound) {
		return false, err
	}

	if err := txn.Set(badgerTypeKey, []byte(KeyTypeSortedSet)); err != nil {
		logger.Logger.Error().Err(err).Str("zset_name", zSetName).Msg("ZAdd: Failed to set type key")
		return false, err
	}

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
			return false, err
		}
	} else if !errors.Is(err, badger.ErrKeyNotFound) {
		logger.Logger.Error().Err(err).Str("zset_name", zSetName).Msg("ZAdd: Failed to get meta")
		return false, err
	}

	newMembers := int64(len(members))
	meta.Version++

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

		var oldScore float64
		var oldVersion uint32
		item, err = txn.Get(dataKey)
		if err == nil {
			var oldDataVal []byte
			err = item.Value(func(val []byte) error {
				oldDataVal = val
				return nil
			})
			if err != nil {
				logger.Logger.Error().Err(err).Str("zset_name", zSetName).Str("member", member).Msg("ZAdd: Failed to get old score")
				return false, err
			}
			oldScore, oldVersion = decodeDataValue(oldDataVal)
			newMembers--
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			logger.Logger.Error().Err(err).Str("zset_name", zSetName).Str("member", member).Msg("ZAdd: Failed to check member")
			return false, err
		}

		op := operation{
			dataKey:  dataKey,
			indexKey: sortedSetKeyIndex(zSetName, score, member, meta.Version),
			score:    encodeDataValue(score, meta.Version),
		}
		if err == nil {
			op.oldIndexKey = sortedSetKeyIndex(zSetName, oldScore, member, oldVersion)
		}
		ops = append(ops, op)
	}

	meta.Card += newMembers
	if err := txn.Set(metaKey, encodeMeta(meta)); err != nil {
		logger.Logger.Error().Err(err).Str("zset_name", zSetName).Msg("ZAdd: Failed to set meta")
		return false, err
	}

	for _, op := range ops {
		if op.oldIndexKey != nil {
			if err := txn.Delete(op.oldIndexKey); err != nil {
				logger.Logger.Error().Err(err).Str("zset_name", zSetName).Msg("ZAdd: Failed to delete old index")
				return false, err
			}
		}
		if err := txn.Set(op.dataKey, op.score); err != nil {
			logger.Logger.Error().Err(err).Str("zset_name", zSetName).Msg("ZAdd: Failed to set data key")
			return false, err
		}
		if err := txn.Set(op.indexKey, nil); err != nil {
			logger.Logger.Error().Err(err).Str("zset_name", zSetName).Msg("ZAdd: Failed to set index key")
			return false, err
		}
	}

	return newMembers > 0, nil
}

// zSetDelInTxn deletes an entire sorted set inside an open update transaction.
func zSetDelInTxn(txn *badger.Txn, zSetName string) error {
	dataPrefix := []byte(zSetName + sortedSetData)
	indexPrefix := []byte(zSetName + sortedSetIndex)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false

	it := txn.NewIterator(opts)
	for it.Rewind(); it.ValidForPrefix(dataPrefix); it.Next() {
		if err := txn.Delete(it.Item().Key()); err != nil {
			it.Close()
			logger.Logger.Error().Err(err).Str("zset_name", zSetName).Msg("ZSetDel: Failed to delete data key")
			return err
		}
	}
	it.Close()

	it = txn.NewIterator(opts)
	for it.Rewind(); it.ValidForPrefix(indexPrefix); it.Next() {
		if err := txn.Delete(it.Item().Key()); err != nil {
			it.Close()
			logger.Logger.Error().Err(err).Str("zset_name", zSetName).Msg("ZSetDel: Failed to delete index key")
			return err
		}
	}
	it.Close()

	if err := txn.Delete(sortedSetKeyMeta(zSetName)); err != nil {
		logger.Logger.Error().Err(err).Str("zset_name", zSetName).Msg("ZSetDel: Failed to delete meta")
		return err
	}
	if err := txn.Delete(TypeOfKeyGet(zSetName)); err != nil {
		logger.Logger.Error().Err(err).Str("zset_name", zSetName).Msg("ZSetDel: Failed to delete type key")
		return err
	}
	return nil
}

// ZSetDel 删除整个排序集
func (s *BotreonStore) ZSetDel(zSetName string) error {
	s.keyLockMgr.Lock(zSetName)
	defer s.keyLockMgr.Unlock(zSetName)
	return s.retryUpdate(func(txn *badger.Txn) error {
		return zSetDelInTxn(txn, zSetName)
	}, 20, encodePropagateCommand([]byte("DEL"), []byte(zSetName)))
}

// ZRem 删除成员
func (s *BotreonStore) ZRem(zSetName, member string) (int64, error) {
	s.keyLockMgr.Lock(zSetName)
	defer s.keyLockMgr.Unlock(zSetName)
	var deleted int64 = 0
	err := s.retryUpdate(func(txn *badger.Txn) error {
		deleted = 0
		n, err := zRemMemberInTxn(txn, zSetName, member)
		if err != nil {
			return err
		}
		deleted = n
		return nil
	}, 20, encodePropagateCommand([]byte("ZREM"), []byte(zSetName), []byte(member)))
	if err != nil {
		return 0, err
	}
	s.markZSetDirty(zSetName)
	return deleted, nil
}

// zRemMemberInTxn removes one member inside an open update transaction.
func zRemMemberInTxn(txn *badger.Txn, zSetName, member string) (int64, error) {
	badgerTypeKey := TypeOfKeyGet(zSetName)
	typeItem, typeErr := txn.Get(badgerTypeKey)
	if typeErr == nil {
		typeVal, err := typeItem.ValueCopy(nil)
		if err != nil {
			return 0, err
		}
		keyType := string(typeVal)
		if keyType != "" && keyType != KeyTypeSortedSet {
			return 0, ErrWrongType
		}
	} else if !errors.Is(typeErr, badger.ErrKeyNotFound) {
		return 0, typeErr
	}

	dataKey := sortedSetKeyMember(zSetName, member)
	item, err := txn.Get(dataKey)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return 0, nil
	}
	if err != nil {
		logger.Logger.Error().Err(err).Str("data_key", string(dataKey)).Msg("ZRem: Failed to get data key")
		return 0, err
	}

	var dataVal []byte
	err = item.Value(func(val []byte) error {
		dataVal = val
		return nil
	})
	if err != nil {
		logger.Logger.Error().Err(err).Str("member", member).Msg("ZRem: Failed to read data value")
		return 0, err
	}
	score, memberVersion := decodeDataValue(dataVal)

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
			return 0, err
		}
	} else if !errors.Is(err, badger.ErrKeyNotFound) {
		logger.Logger.Error().Err(err).Msg("ZRem: Failed to get meta")
		return 0, err
	}

	if err := txn.Delete(dataKey); err != nil {
		logger.Logger.Error().Err(err).Msg("ZRem: Failed to delete data key")
		return 0, err
	}

	indexKey := sortedSetKeyIndex(zSetName, score, member, memberVersion)
	if err := txn.Delete(indexKey); err != nil {
		logger.Logger.Error().Err(err).Msg("ZRem: Failed to delete index key")
		return 0, err
	}

	meta.Card--
	if meta.Card <= 0 {
		if err := txn.Delete(metaKey); err != nil {
			logger.Logger.Error().Err(err).Msg("ZRem: Failed to delete meta")
			return 0, err
		}
		if err := txn.Delete(badgerTypeKey); err != nil {
			logger.Logger.Error().Err(err).Msg("ZRem: Failed to delete type key")
			return 0, err
		}
		return 1, nil
	}
	if err := txn.Set(metaKey, encodeMeta(meta)); err != nil {
		logger.Logger.Error().Err(err).Msg("ZRem: Failed to set meta")
		return 0, err
	}
	return 1, nil
}

// ZRemRangeByRank 实现 Redis ZREMRANGEBYRANK 命令
func (s *BotreonStore) ZRemRangeByRank(zSetName string, start, stop int64) (int64, error) {
	s.keyLockMgr.Lock(zSetName)
	defer s.keyLockMgr.Unlock(zSetName)
	var removed int64
	err := s.retryUpdate(func(txn *badger.Txn) error {
		removed = 0
		members, err := zRangeByRankInTxn(txn, zSetName, start, stop)
		if err != nil {
			return err
		}
		for _, member := range members {
			n, err := zRemMemberInTxn(txn, zSetName, member)
			if err != nil {
				return err
			}
			removed += n
		}
		return nil
	}, 20, encodePropagateCommand([]byte("ZREMRANGEBYRANK"), []byte(zSetName), []byte(strconv.FormatInt(start, 10)), []byte(strconv.FormatInt(stop, 10))))
	s.markZSetDirty(zSetName)
	return removed, err
}

// ZRemRangeByScore 实现 Redis ZREMRANGEBYSCORE 命令
func (s *BotreonStore) ZRemRangeByScore(zSetName string, minScore, maxScore float64, minExclusive, maxExclusive bool) (int64, error) {
	s.keyLockMgr.Lock(zSetName)
	defer s.keyLockMgr.Unlock(zSetName)
	var removed int64
	err := s.retryUpdate(func(txn *badger.Txn) error {
		removed = 0
		members, err := zRangeByScoreInTxn(txn, zSetName, minScore, maxScore, minExclusive, maxExclusive)
		if err != nil {
			return err
		}
		for _, member := range members {
			n, err := zRemMemberInTxn(txn, zSetName, member)
			if err != nil {
				return err
			}
			removed += n
		}
		return nil
	}, 20, func() []byte {
		// D4 全重放：ZREMRANGEBYSCORE zSetName <min> <max>——exclusive 用 "(" 前缀
		minS := strconv.FormatFloat(minScore, 'f', -1, 64)
		maxS := strconv.FormatFloat(maxScore, 'f', -1, 64)
		if minExclusive {
			minS = "(" + minS
		}
		if maxExclusive {
			maxS = "(" + maxS
		}
		return encodePropagateCommand([]byte("ZREMRANGEBYSCORE"), []byte(zSetName), []byte(minS), []byte(maxS))
	}())
	s.markZSetDirty(zSetName)
	return removed, err
}

// zRangeAllMembersInTxn returns all members from an open read view inside an update txn.
func zRangeAllMembersInTxn(txn *badger.Txn, zSetName string) ([]ZSetMember, error) {
	prefix := keyBadgerGet(prefixKeySortedSetBytes, []byte(zSetName+sortedSetIndex))
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.PrefetchValues = false

	it := txn.NewIterator(opts)
	defer it.Close()

	var results []ZSetMember
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		score, member, _, ok := parseZSetIndexKey(it.Item().Key(), prefix)
		if !ok {
			continue
		}
		results = append(results, ZSetMember{Member: member, Score: score})
	}
	return results, nil
}

// zRangeByRankInTxn returns member names in the inclusive rank range inside an update txn.
func zRangeByRankInTxn(txn *badger.Txn, zSetName string, start, stop int64) ([]string, error) {
	members, err := zRangeMembersByRankInTxn(txn, zSetName, start, stop)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(members))
	for i, m := range members {
		names[i] = m.Member
	}
	return names, nil
}

// zRangeByScoreInTxn returns member names in the score range inside an update txn.
func zRangeByScoreInTxn(txn *badger.Txn, zSetName string, minScore, maxScore float64, minExclusive, maxExclusive bool) ([]string, error) {
	prefix := keyBadgerGet(prefixKeySortedSetBytes, []byte(zSetName+sortedSetIndex))
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.PrefetchValues = false

	it := txn.NewIterator(opts)
	defer it.Close()

	startKey := append(prefix, encodeScore(minScore)...)
	var results []string
	for it.Seek(startKey); it.ValidForPrefix(prefix); it.Next() {
		score, member, _, memberOK := parseZSetIndexKey(it.Item().Key(), prefix)
		if !memberOK {
			continue
		}
		if maxExclusive {
			if score >= maxScore {
				break
			}
		} else if score > maxScore {
			break
		}
		if minExclusive && score <= minScore {
			continue
		}
		results = append(results, member)
	}
	return results, nil
}

// zReadZSetMembersInTxn reads all members of a zset inside an open transaction.
func zReadZSetMembersInTxn(txn *badger.Txn, zSetName string) ([]ZSetMember, error) {
	if err := checkKeyType(txn, zSetName, KeyTypeSortedSet); err != nil {
		return nil, err
	}
	return zRangeMembersByRankInTxn(txn, zSetName, 0, -1)
}

// zStoreReplaceFromScores replaces a zset with members from a score map.
func zStoreReplaceFromScores(txn *badger.Txn, destination string, memberScores map[string]float64) error {
	if err := zSetDelInTxn(txn, destination); err != nil {
		return err
	}
	if len(memberScores) == 0 {
		return nil
	}
	members := make([]ZSetMember, 0, len(memberScores))
	for member, score := range memberScores {
		members = append(members, ZSetMember{Member: member, Score: score})
	}
	_, err := zAddMembersInTxn(txn, destination, members)
	return err
}

// zStoreReplaceFromMembers replaces a zset with members from a slice.
func zStoreReplaceFromMembers(txn *badger.Txn, destination string, members []ZSetMember) error {
	if err := zSetDelInTxn(txn, destination); err != nil {
		return err
	}
	if len(members) == 0 {
		return nil
	}
	_, err := zAddMembersInTxn(txn, destination, members)
	return err
}

// applyAggregateScore applies MIN/MAX/SUM aggregation.
func applyAggregateScore(existing float64, exists bool, score float64, aggregate string) float64 {
	if !exists {
		return score
	}
	switch aggregate {
	case "MIN":
		if score < existing {
			return score
		}
		return existing
	case "MAX":
		if score > existing {
			return score
		}
		return existing
	default:
		return existing + score
	}
}

// zUnionScoresInTxn computes union of scores from multiple zsets.
func zUnionScoresInTxn(txn *badger.Txn, keys []string, weights []float64, aggregate string) (map[string]float64, error) {
	memberScores := make(map[string]float64)
	for i, key := range keys {
		weight := 1.0
		if i < len(weights) && weights[i] != 0 {
			weight = weights[i]
		}
		members, err := zReadZSetMembersInTxn(txn, key)
		if err != nil {
			return nil, err
		}
		for _, m := range members {
			score := m.Score * weight
			existing, exists := memberScores[m.Member]
			memberScores[m.Member] = applyAggregateScore(existing, exists, score, aggregate)
		}
	}
	return memberScores, nil
}

// zInterScoresInTxn computes intersection of scores from multiple zsets.
// Only the first set is fully read; subsequent sets use txn.Get (O(log n))
// for member existence checks instead of full index scans.
func zInterScoresInTxn(txn *badger.Txn, keys []string, weights []float64, aggregate string) (map[string]float64, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	firstWeight := 1.0
	if len(weights) > 0 && weights[0] != 0 {
		firstWeight = weights[0]
	}
	firstMembers, err := zReadZSetMembersInTxn(txn, keys[0])
	if err != nil {
		return nil, err
	}

	memberScores := make(map[string]float64, len(firstMembers))
	for _, m := range firstMembers {
		memberScores[m.Member] = m.Score * firstWeight
	}

	for i := 1; i < len(keys) && len(memberScores) > 0; i++ {
		weight := 1.0
		if i < len(weights) && weights[i] != 0 {
			weight = weights[i]
		}

		for member := range memberScores {
			memberDataKey := sortedSetKeyMember(keys[i], member)
			item, err := txn.Get(memberDataKey)
			if errors.Is(err, badger.ErrKeyNotFound) {
				delete(memberScores, member)
				continue
			}
			if err != nil {
				return nil, err
			}
			var otherScore float64
			if valErr := item.Value(func(val []byte) error {
				otherScore, _ = decodeDataValue(val)
				return nil
			}); valErr != nil {
				return nil, valErr
			}
			existing := memberScores[member]
			memberScores[member] = applyAggregateScore(existing, true, otherScore*weight, aggregate)
		}
	}
	return memberScores, nil
}

// zDiffMembersInTxn computes difference of members from multiple zsets.
// Only the first set is fully read; subsequent sets use txn.Get (O(log n))
// for member existence checks instead of full index scans.
func zDiffMembersInTxn(txn *badger.Txn, keys []string) ([]ZSetMember, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	firstMembers, err := zReadZSetMembersInTxn(txn, keys[0])
	if err != nil {
		return nil, err
	}

	// Build existence map from all subsequent sets using txn.Get
	excludeSet := make(map[string]struct{})
	for i := 1; i < len(keys); i++ {
		for _, m := range firstMembers {
			if _, excluded := excludeSet[m.Member]; excluded {
				continue
			}
			memberDataKey := sortedSetKeyMember(keys[i], m.Member)
			_, err := txn.Get(memberDataKey)
			if errors.Is(err, badger.ErrKeyNotFound) {
				continue
			}
			if err != nil {
				return nil, err
			}
			excludeSet[m.Member] = struct{}{}
		}
	}

	var result []ZSetMember
	for _, m := range firstMembers {
		if _, excluded := excludeSet[m.Member]; !excluded {
			result = append(result, m)
		}
	}
	return result, nil
}
