package store

import (
	"errors"

	"github.com/dgraph-io/badger/v4"
)

// ZRank 实现 Redis ZRANK 命令
// 使用 in-memory rank cache 实现 O(1) 平均（O(n log n) 首次/写后重建）
func (s *BotreonStore) ZRank(zSetName, member string) (int64, error) {
	// 先查缓存（命中且未 dirty 则 O(1) 返回）
	rank, err := s.getZSetRank(zSetName, member, 0)
	if err != nil {
		return -1, err
	}
	if rank >= 0 {
		return rank, nil
	}

	// 缓存未命中（成员不存在或 need score for cache build）
	// 查 BadgerDB 获取 score，然后走缓存重建路径
	var score float64
	err = s.db.View(func(txn *badger.Txn) error {
		dataKey := sortedSetKeyMember(zSetName, member)
		item, getErr := txn.Get([]byte(dataKey))
		if errors.Is(getErr, badger.ErrKeyNotFound) {
			return badger.ErrKeyNotFound
		}
		if getErr != nil {
			return getErr
		}
		return item.Value(func(val []byte) error {
			score, _ = decodeDataValue(val)
			return nil
		})
	})
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return -1, nil
		}
		return -1, err
	}

	return s.getZSetRank(zSetName, member, score)
}

// ZRevRank 实现 Redis ZREVRANK 命令
// 使用 in-memory rank cache（与 ZRank 共享）
func (s *BotreonStore) ZRevRank(zSetName, member string) (int64, error) {
	forwardRank, err := s.ZRank(zSetName, member)
	if err != nil {
		return -1, err
	}
	if forwardRank == -1 {
		return -1, nil
	}

	var totalCount int64
	err = s.db.View(func(txn *badger.Txn) error {
		metaKey := sortedSetKeyMeta(zSetName)
		metaItem, getErr := txn.Get([]byte(metaKey))
		if errors.Is(getErr, badger.ErrKeyNotFound) {
			totalCount = 0
			return nil
		}
		if getErr != nil {
			return getErr
		}
		return metaItem.Value(func(val []byte) error {
			meta, dErr := decodeMeta(val)
			if dErr != nil {
				return dErr
			}
			totalCount = meta.Card
			return nil
		})
	})
	if err != nil {
		return -1, err
	}
	return totalCount - 1 - forwardRank, nil
}
