package store

import (
	"errors"

	"github.com/dgraph-io/badger/v4"
	"github.com/lbp0200/BoltDB/internal/logger"
)

// zRangeMembersByScoreInTxn returns members with scores in the inclusive score range.
func zRangeMembersByScoreInTxn(txn *badger.Txn, zSetName string, minScore, maxScore float64, minExclusive, maxExclusive bool) ([]ZSetMember, error) {
	prefix := keyBadgerGet(prefixKeySortedSetBytes, []byte(zSetName+sortedSetIndex))
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.PrefetchValues = false

	it := txn.NewIterator(opts)
	defer it.Close()

	startKey := append(prefix, encodeScore(minScore)...)
	var results []ZSetMember
	for it.Seek(startKey); it.ValidForPrefix(prefix); it.Next() {
		score, member, _, ok := parseZSetIndexKey(it.Item().Key(), prefix)
		if !ok {
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
		results = append(results, ZSetMember{Member: member, Score: score})
	}
	return results, nil
}

func applyZSetScoreOffsetCount(members []ZSetMember, offset, count int) []ZSetMember {
	if offset > 0 && offset < len(members) {
		members = members[offset:]
	} else if offset >= len(members) {
		return nil
	}
	if count > 0 && count < len(members) {
		members = members[:count]
	}
	return members
}

// ZRangeByScore 获取分数范围内的成员
func (s *BotreonStore) ZRangeByScore(zSetName string, minScore, maxScore float64, offset, count int, minExclusive, maxExclusive bool) ([]ZSetMember, error) {
	var results []ZSetMember
	err := s.db.View(func(txn *badger.Txn) error {
		members, err := zRangeMembersByScoreInTxn(txn, zSetName, minScore, maxScore, minExclusive, maxExclusive)
		if err != nil {
			return err
		}
		results = applyZSetScoreOffsetCount(members, offset, count)
		logger.Logger.Debug().
			Int("members_count", len(results)).
			Str("zset_name", zSetName).
			Msg("ZRangeByScore: Retrieved members")
		return nil
	})
	return results, err
}

// normalizeRankRange normalizes negative start/stop and validates range.
func normalizeRankRange(totalCount, start, stop int64) (int64, int64, bool) {
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
		return 0, 0, false
	}
	return start, stop, true
}

// zRangeMembersByRankInTxn returns members with scores in the inclusive rank range.
func zRangeMembersByRankInTxn(txn *badger.Txn, zSetName string, start, stop int64) ([]ZSetMember, error) {
	metaKey := sortedSetKeyMeta(zSetName)
	item, err := txn.Get(metaKey)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var meta ZSetsMetaValue
	err = item.Value(func(val []byte) error {
		meta, err = decodeMeta(val)
		return err
	})
	if err != nil {
		return nil, err
	}

	start, stop, ok := normalizeRankRange(meta.Card, start, stop)
	if !ok {
		return nil, nil
	}

	prefix := keyBadgerGet(prefixKeySortedSetBytes, []byte(zSetName+sortedSetIndex))
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.PrefetchValues = false

	it := txn.NewIterator(opts)
	defer it.Close()

	var results []ZSetMember
	currentIndex := int64(0)
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		if currentIndex < start {
			currentIndex++
			continue
		}
		if currentIndex > stop {
			break
		}
		score, member, _, memberOK := parseZSetIndexKey(it.Item().Key(), prefix)
		if !memberOK {
			currentIndex++
			continue
		}
		results = append(results, ZSetMember{Member: member, Score: score})
		currentIndex++
	}
	return results, nil
}

// zRevRangeMembersByRankInTxn returns members highest-score-first for reverse rank range.
func zRevRangeMembersByRankInTxn(txn *badger.Txn, zSetName string, revStart, revStop int64) ([]ZSetMember, error) {
	metaKey := sortedSetKeyMeta(zSetName)
	item, err := txn.Get(metaKey)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var meta ZSetsMetaValue
	err = item.Value(func(val []byte) error {
		meta, err = decodeMeta(val)
		return err
	})
	if err != nil {
		return nil, err
	}

	revStart, revStop, ok := normalizeRankRange(meta.Card, revStart, revStop)
	if !ok {
		return nil, nil
	}

	forwardStart := meta.Card - 1 - revStop
	forwardStop := meta.Card - 1 - revStart
	members, err := zRangeMembersByRankInTxn(txn, zSetName, forwardStart, forwardStop)
	if err != nil {
		return nil, err
	}
	for i, j := 0, len(members)-1; i < j; i, j = i+1, j-1 {
		members[i], members[j] = members[j], members[i]
	}
	return members, nil
}

// ZRange 获取指定排名范围的成员
func (s *BotreonStore) ZRange(zSetName string, start, stop int64) ([]*ZSetMember, error) {
	var members []ZSetMember
	err := s.db.View(func(txn *badger.Txn) error {
		if err := checkKeyType(txn, zSetName, KeyTypeSortedSet); err != nil {
			return err
		}
		var err error
		members, err = zRangeMembersByRankInTxn(txn, zSetName, start, stop)
		if err != nil {
			logger.Logger.Error().Err(err).Str("zset_name", zSetName).Msg("ZRange: Failed to read members")
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	results := make([]*ZSetMember, len(members))
	for i := range members {
		m := members[i]
		results[i] = &ZSetMember{Member: m.Member, Score: m.Score}
	}
	return results, nil
}

// ZRevRange 实现 Redis ZREVRANGE 命令
func (s *BotreonStore) ZRevRange(zSetName string, start, stop int64) ([]*ZSetMember, error) {
	var members []ZSetMember
	err := s.db.View(func(txn *badger.Txn) error {
		if err := checkKeyType(txn, zSetName, KeyTypeSortedSet); err != nil {
			return err
		}
		var err error
		members, err = zRevRangeMembersByRankInTxn(txn, zSetName, start, stop)
		return err
	})
	if err != nil {
		return nil, err
	}
	results := make([]*ZSetMember, len(members))
	for i := range members {
		m := members[i]
		results[i] = &ZSetMember{Member: m.Member, Score: m.Score}
	}
	return results, nil
}

// ZRevRangeByScore 实现 Redis ZREVRANGEBYSCORE 命令
func (s *BotreonStore) ZRevRangeByScore(zSetName string, maxScore, minScore float64, offset, count int, minExclusive, maxExclusive bool) ([]ZSetMember, error) {
	var results []ZSetMember
	err := s.db.View(func(txn *badger.Txn) error {
		members, err := zRangeMembersByScoreInTxn(txn, zSetName, minScore, maxScore, minExclusive, maxExclusive)
		if err != nil {
			return err
		}
		for i := len(members) - 1; i >= 0; i-- {
			results = append(results, members[i])
		}
		results = applyZSetScoreOffsetCount(results, offset, count)
		return nil
	})
	return results, err
}
