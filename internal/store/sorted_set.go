package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"time"

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
	var addedNewMember bool
	err := s.retryUpdate(func(txn *badger.Txn) error {
		var err error
		addedNewMember, err = zAddMembersInTxn(txn, zSetName, members)
		return err
	}, 20)
	if err == nil && addedNewMember {
		s.notifyBlockingZPop(zSetName)
	}
	return err
}

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

// zRemMemberInTxn removes one member inside an open update transaction.
// Returns 1 if deleted, 0 if the member did not exist.
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

	var scoreBytes []byte
	err = item.Value(func(val []byte) error {
		scoreBytes = val
		return nil
	})
	if err != nil {
		logger.Logger.Error().Err(err).Str("member", member).Msg("ZRem: Failed to get score")
		return 0, err
	}
	score := decodeScore(scoreBytes)

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

	indexKey := sortedSetKeyIndex(zSetName, score, member, meta.Version)
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
		logger.Logger.Debug().Str("member", member).Str("zset_name", zSetName).Msg("ZRem: Deleted member, set empty")
		return 1, nil
	}
	if err := txn.Set(metaKey, encodeMeta(meta)); err != nil {
		logger.Logger.Error().Err(err).Msg("ZRem: Failed to set meta")
		return 0, err
	}

	logger.Logger.Debug().
		Str("member", member).
		Str("zset_name", zSetName).
		Int64("card", meta.Card).
		Msg("ZRem: Successfully removed member")
	return 1, nil
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

func memberInLexRange(memberStr, min, max string) bool {
	minOK := compareLex(min, memberStr, true)
	var maxOK bool
	switch {
	case max == "+":
		maxOK = true
	case len(max) > 0 && max[0] == '(':
		maxOK = memberStr < max[1:]
	case len(max) > 0 && max[0] == '[':
		maxOK = memberStr <= max[1:]
	default:
		maxOK = memberStr <= max
	}
	return minOK && maxOK
}

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

func zReadZSetMembersInTxn(txn *badger.Txn, zSetName string) ([]ZSetMember, error) {
	if err := checkKeyType(txn, zSetName, KeyTypeSortedSet); err != nil {
		return nil, err
	}
	return zRangeMembersByRankInTxn(txn, zSetName, 0, -1)
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
		item, err = txn.Get(dataKey)
		if err == nil {
			var oldScoreBytes []byte
			err = item.Value(func(val []byte) error {
				oldScoreBytes = val
				return nil
			})
			if err != nil {
				logger.Logger.Error().Err(err).Str("zset_name", zSetName).Str("member", member).Msg("ZAdd: Failed to get old score")
				return false, err
			}
			oldScore = decodeScore(oldScoreBytes)
			newMembers--
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			logger.Logger.Error().Err(err).Str("zset_name", zSetName).Str("member", member).Msg("ZAdd: Failed to check member")
			return false, err
		}

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

	memberScores := make(map[string]float64)
	for _, m := range firstMembers {
		memberScores[m.Member] = m.Score * firstWeight
	}

	for i := 1; i < len(keys); i++ {
		weight := 1.0
		if i < len(weights) && weights[i] != 0 {
			weight = weights[i]
		}
		otherMembers, err := zReadZSetMembersInTxn(txn, keys[i])
		if err != nil {
			return nil, err
		}
		otherMemberMap := make(map[string]float64, len(otherMembers))
		for _, m := range otherMembers {
			otherMemberMap[m.Member] = m.Score * weight
		}
		for member := range memberScores {
			if otherScore, exists := otherMemberMap[member]; exists {
				existing := memberScores[member]
				memberScores[member] = applyAggregateScore(existing, true, otherScore, aggregate)
			} else {
				delete(memberScores, member)
			}
		}
	}
	return memberScores, nil
}

func zDiffMembersInTxn(txn *badger.Txn, keys []string) ([]ZSetMember, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	firstMembers, err := zReadZSetMembersInTxn(txn, keys[0])
	if err != nil {
		return nil, err
	}

	otherMembers := make(map[string]bool)
	for i := 1; i < len(keys); i++ {
		members, err := zReadZSetMembersInTxn(txn, keys[i])
		if err != nil {
			return nil, err
		}
		for _, m := range members {
			otherMembers[m.Member] = true
		}
	}

	var result []ZSetMember
	for _, m := range firstMembers {
		if !otherMembers[m.Member] {
			result = append(result, m)
		}
	}
	return result, nil
}

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

// ZRem 删除成员
func (s *BotreonStore) ZRem(zSetName, member string) (int64, error) {
	var deleted int64 = 0
	err := s.retryUpdate(func(txn *badger.Txn) error {
		deleted = 0 // reset each attempt; stale value must not survive conflict retry
		n, err := zRemMemberInTxn(txn, zSetName, member)
		if err != nil {
			return err
		}
		deleted = n
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
		return 0, false, err
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
		return nil, err
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
		return zSetDelInTxn(txn, zSetName)
	}, 20) // 最多重试 20 次（优化：减少重试次数）
}

// ZCard 实现 Redis ZCARD 命令，获取有序集合中成员的数量
func (s *BotreonStore) ZCard(zSetName string) (int64, error) {
	// Check if key exists with wrong type
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
		newScore = 0 // reset each attempt; stale value must not survive conflict retry
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

// ZRevRangeByScore 实现 Redis ZREVRANGEBYSCORE 命令，返回有序集中指定分数区间内的成员，分数从高到低排序
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

// ZRemRangeByRank 实现 Redis ZREMRANGEBYRANK 命令，移除有序集中指定排名区间的所有成员
func (s *BotreonStore) ZRemRangeByRank(zSetName string, start, stop int64) (int64, error) {
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
	}, 20)
	return removed, err
}

// ZRemRangeByScore 实现 Redis ZREMRANGEBYSCORE 命令，移除有序集中指定分数区间的所有成员
func (s *BotreonStore) ZRemRangeByScore(zSetName string, minScore, maxScore float64, minExclusive, maxExclusive bool) (int64, error) {
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
	}, 20)
	return removed, err
}

// ZPopMax 实现 Redis ZPOPMAX 命令，移除并返回有序集合中分数最高的成员
func (s *BotreonStore) ZPopMax(zSetName string, count int) ([]ZSetMember, error) {
	if count <= 0 {
		return nil, nil
	}
	var results []ZSetMember
	err := s.retryUpdate(func(txn *badger.Txn) error {
		results = nil
		members, err := zRevRangeMembersByRankInTxn(txn, zSetName, 0, int64(count-1))
		if err != nil {
			return err
		}
		for _, member := range members {
			n, err := zRemMemberInTxn(txn, zSetName, member.Member)
			if err != nil {
				return err
			}
			if n > 0 {
				results = append(results, member)
			}
		}
		return nil
	}, 20)
	return results, err
}

// ZPopMin 实现 Redis ZPOPMIN 命令，移除并返回有序集合中分数最低的成员
func (s *BotreonStore) ZPopMin(zSetName string, count int) ([]ZSetMember, error) {
	if count <= 0 {
		return nil, nil
	}
	var results []ZSetMember
	err := s.retryUpdate(func(txn *badger.Txn) error {
		results = nil
		members, err := zRangeMembersByRankInTxn(txn, zSetName, 0, int64(count-1))
		if err != nil {
			return err
		}
		for _, member := range members {
			n, err := zRemMemberInTxn(txn, zSetName, member.Member)
			if err != nil {
				return err
			}
			if n > 0 {
				results = append(results, member)
			}
		}
		return nil
	}, 20)
	return results, err
}

// ZMPop 实现 Redis ZMPOP 命令，从多个有序集合中弹出元素
// keys: 要操作的键列表
// modifier: "MIN" 或 "MAX"
// count: 要弹出的元素数量
// 返回第一个非空键的键名和被弹出的成员列表
func (s *BotreonStore) ZMPop(keys []string, modifier string, count int) (string, []ZSetMember, error) {
	for _, key := range keys {
		typeKey := TypeOfKeyGet(key)
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
			return "", nil, err
		}

		var members []ZSetMember
		var err error
		if modifier == "MAX" {
			members, err = s.ZPopMax(key, count)
		} else {
			members, err = s.ZPopMin(key, count)
		}
		if err != nil {
			return "", nil, err
		}
		if len(members) > 0 {
			return key, members, nil
		}
	}
	return "", nil, nil
}

// ZUnionStore 实现 Redis ZUNIONSTORE 命令，计算并集并存储到目标集合
func (s *BotreonStore) ZUnionStore(destination string, keys []string, weights []float64, aggregate string) (int64, error) {
	var count int64
	var notify bool
	err := s.retryUpdate(func(txn *badger.Txn) error {
		count = 0
		notify = false
		memberScores, err := zUnionScoresInTxn(txn, keys, weights, aggregate)
		if err != nil {
			return err
		}
		if err := zStoreReplaceFromScores(txn, destination, memberScores); err != nil {
			return err
		}
		count = int64(len(memberScores))
		notify = count > 0
		return nil
	}, 20)
	if err == nil && notify {
		s.notifyBlockingZPop(destination)
	}
	return count, err
}

// ZInterStore 实现 Redis ZINTERSTORE 命令，计算交集并存储到目标集合
func (s *BotreonStore) ZInterStore(destination string, keys []string, weights []float64, aggregate string) (int64, error) {
	var count int64
	var notify bool
	err := s.retryUpdate(func(txn *badger.Txn) error {
		count = 0
		notify = false
		if len(keys) == 0 {
			return zSetDelInTxn(txn, destination)
		}
		memberScores, err := zInterScoresInTxn(txn, keys, weights, aggregate)
		if err != nil {
			return err
		}
		if err := zStoreReplaceFromScores(txn, destination, memberScores); err != nil {
			return err
		}
		count = int64(len(memberScores))
		notify = count > 0
		return nil
	}, 20)
	if err == nil && notify {
		s.notifyBlockingZPop(destination)
	}
	return count, err
}

// ZDiffStore 实现 Redis ZDIFFSTORE 命令，计算差集并存储到目标集合
func (s *BotreonStore) ZDiffStore(destination string, keys []string) (int64, error) {
	var count int64
	var notify bool
	err := s.retryUpdate(func(txn *badger.Txn) error {
		count = 0
		notify = false
		if len(keys) == 0 {
			return zSetDelInTxn(txn, destination)
		}
		members, err := zDiffMembersInTxn(txn, keys)
		if err != nil {
			return err
		}
		if err := zStoreReplaceFromMembers(txn, destination, members); err != nil {
			return err
		}
		count = int64(len(members))
		notify = count > 0
		return nil
	}, 20)
	if err == nil && notify {
		s.notifyBlockingZPop(destination)
	}
	return count, err
}

// ZDiff returns the difference of the first sorted set with all subsequent ones.
func (s *BotreonStore) ZDiff(keys []string) ([]ZSetMember, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	var result []ZSetMember
	err := s.db.View(func(txn *badger.Txn) error {
		var err error
		result, err = zDiffMembersInTxn(txn, keys)
		return err
	})
	return result, err
}

// ZLexCount 实现 Redis ZLEXCOUNT 命令，计算有序集合中成员值介于min和max之间的成员数量（字典序）
func (s *BotreonStore) ZLexCount(zSetName, min, max string) (int64, error) {
	var count int64
	err := s.db.View(func(txn *badger.Txn) error {
		members, err := zReadZSetMembersInTxn(txn, zSetName)
		if err != nil {
			return err
		}
		for _, member := range members {
			if compareLex(min, member.Member, true) && compareLex(member.Member, max, false) {
				count++
			}
		}
		return nil
	})
	return count, err
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
	var filtered []string
	err := s.db.View(func(txn *badger.Txn) error {
		members, err := zReadZSetMembersInTxn(txn, zSetName)
		if err != nil {
			return err
		}
		for _, member := range members {
			memberStr := member.Member
			minOK := compareLex(min, memberStr, true)
			var maxOK bool
			if max == "+" {
				maxOK = true
			} else if len(max) > 0 && max[0] == '(' {
				maxOK = memberStr < max[1:]
			} else if len(max) > 0 && max[0] == '[' {
				maxOK = memberStr <= max[1:]
			} else {
				maxOK = memberStr <= max
			}
			if minOK && maxOK {
				filtered = append(filtered, memberStr)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

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
		removed = 0 // reset each attempt; stale value must not survive conflict retry
		members, err := zRangeAllMembersInTxn(txn, zSetName)
		if err != nil {
			return err
		}

		for _, member := range members {
			if !memberInLexRange(member.Member, min, max) {
				continue
			}
			n, err := zRemMemberInTxn(txn, zSetName, member.Member)
			if err != nil {
				return err
			}
			removed += n
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

// unregisterBlockingZPop removes a specific channel from all keys' wait lists
func (s *BotreonStore) unregisterBlockingZPop(ch chan string, keys []string) {
	s.blockingZPopMu.Lock()
	defer s.blockingZPopMu.Unlock()

	for _, key := range keys {
		chans := s.blockingZPopChans[key]
		for j, c := range chans {
			if c == ch {
				s.blockingZPopChans[key] = append(chans[:j], chans[j+1:]...)
				break
			}
		}
		if len(s.blockingZPopChans[key]) == 0 {
			delete(s.blockingZPopChans, key)
		}
	}
}

// notifyBlockingZPop notifies one waiting channel for a sorted set key
func (s *BotreonStore) notifyBlockingZPop(key string) {
	s.blockingZPopMu.Lock()
	defer s.blockingZPopMu.Unlock()

	chans, exists := s.blockingZPopChans[key]
	if !exists || len(chans) == 0 {
		return
	}

	select {
	case chans[0] <- key:
		s.blockingZPopChans[key] = chans[1:]
	default:
	}
}

// registerAndRecheckZMax registers a channel for keys and re-checks with ZPopMax after registration
func (s *BotreonStore) registerAndRecheckZMax(keys []string, ch chan string) (string, *ZSetMember, bool) {
	s.blockingZPopMu.Lock()
	for _, key := range keys {
		s.blockingZPopChans[key] = append(s.blockingZPopChans[key], ch)
	}
	s.blockingZPopMu.Unlock()

	for _, key := range keys {
		members, err := s.ZPopMax(key, 1)
		if err == nil && len(members) > 0 {
			s.unregisterBlockingZPop(ch, keys)
			return key, &members[0], true
		}
	}
	return "", nil, false
}

// registerAndRecheckZMin registers a channel for keys and re-checks with ZPopMin after registration
func (s *BotreonStore) registerAndRecheckZMin(keys []string, ch chan string) (string, *ZSetMember, bool) {
	s.blockingZPopMu.Lock()
	for _, key := range keys {
		s.blockingZPopChans[key] = append(s.blockingZPopChans[key], ch)
	}
	s.blockingZPopMu.Unlock()

	for _, key := range keys {
		members, err := s.ZPopMin(key, 1)
		if err == nil && len(members) > 0 {
			s.unregisterBlockingZPop(ch, keys)
			return key, &members[0], true
		}
	}
	return "", nil, false
}

// BZPopMaxBlocking 实现 Redis BZPOPMAX 命令，阻塞式弹出分数最高的成员
func (s *BotreonStore) BZPopMaxBlocking(ctx context.Context, keys []string, timeout int) (string, *ZSetMember, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	for _, key := range keys {
		members, err := s.ZPopMax(key, 1)
		if err != nil {
			return "", nil, err
		}
		if len(members) > 0 {
			return key, &members[0], nil
		}
	}

	resultCh := make(chan string, 1)
	var timerCh <-chan time.Time
	if timeout > 0 {
		timer := time.NewTimer(time.Duration(timeout) * time.Second)
		defer func() {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}()
		timerCh = timer.C
	}

	if key, member, ok := s.registerAndRecheckZMax(keys, resultCh); ok {
		return key, member, nil
	}

	select {
	case key := <-resultCh:
		members, err := s.ZPopMax(key, 1)
		if err != nil || len(members) == 0 {
			return "", nil, nil
		}
		return key, &members[0], nil
	case <-timerCh:
		s.unregisterBlockingZPop(resultCh, keys)
		return "", nil, nil
	case <-ctx.Done():
		s.unregisterBlockingZPop(resultCh, keys)
		return "", nil, nil
	}
}

// BZPopMinBlocking 实现 Redis BZPOPMIN 命令，阻塞式弹出分数最低的成员
func (s *BotreonStore) BZPopMinBlocking(ctx context.Context, keys []string, timeout int) (string, *ZSetMember, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	for _, key := range keys {
		members, err := s.ZPopMin(key, 1)
		if err != nil {
			return "", nil, err
		}
		if len(members) > 0 {
			return key, &members[0], nil
		}
	}

	resultCh := make(chan string, 1)
	var timerCh <-chan time.Time
	if timeout > 0 {
		timer := time.NewTimer(time.Duration(timeout) * time.Second)
		defer func() {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}()
		timerCh = timer.C
	}

	if key, member, ok := s.registerAndRecheckZMin(keys, resultCh); ok {
		return key, member, nil
	}

	select {
	case key := <-resultCh:
		members, err := s.ZPopMin(key, 1)
		if err != nil || len(members) == 0 {
			return "", nil, nil
		}
		return key, &members[0], nil
	case <-timerCh:
		s.unregisterBlockingZPop(resultCh, keys)
		return "", nil, nil
	case <-ctx.Done():
		s.unregisterBlockingZPop(resultCh, keys)
		return "", nil, nil
	}
}

// BZPopMax keeps backward compatibility — uses BZPopMaxBlocking with background context
func (s *BotreonStore) BZPopMax(keys []string, timeout int) (string, *ZSetMember, error) {
	ctx := context.Background()
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
		defer cancel()
	}
	return s.BZPopMaxBlocking(ctx, keys, timeout)
}

// BZPopMin keeps backward compatibility — uses BZPopMinBlocking with background context
func (s *BotreonStore) BZPopMin(keys []string, timeout int) (string, *ZSetMember, error) {
	ctx := context.Background()
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
		defer cancel()
	}
	return s.BZPopMinBlocking(ctx, keys, timeout)
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
		return nil, err
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
