package store

import (
	"errors"

	"github.com/dgraph-io/badger/v4"
)

// ZPopMax 实现 Redis ZPOPMAX 命令
func (s *BotreonStore) ZPopMax(zSetName string, count int) ([]ZSetMember, error) {
	if count <= 0 {
		return nil, nil
	}
	s.keyLockMgr.Lock(zSetName)
	defer s.keyLockMgr.Unlock(zSetName)
	var results []ZSetMember
	err := s.retryUpdate(func(txn *badger.Txn) error {
		results = nil
		// key 存在但类型不是 zset（如 string）时返回 WRONGTYPE，
		// 否则 BZPOP 会把它当作空 zset 走阻塞路径而丢失类型错误。
		if err := checkKeyType(txn, zSetName, KeyTypeSortedSet); err != nil {
			return err
		}
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
	}, 20, encodePropagateCommand([]byte("ZPOPMAX"), []byte(zSetName)))
	s.markZSetDirty(zSetName)
	return results, err
}

// ZPopMin 实现 Redis ZPOPMIN 命令
func (s *BotreonStore) ZPopMin(zSetName string, count int) ([]ZSetMember, error) {
	if count <= 0 {
		return nil, nil
	}
	s.keyLockMgr.Lock(zSetName)
	defer s.keyLockMgr.Unlock(zSetName)
	var results []ZSetMember
	err := s.retryUpdate(func(txn *badger.Txn) error {
		results = nil
		// key 存在但类型不是 zset（如 string）时返回 WRONGTYPE。
		if err := checkKeyType(txn, zSetName, KeyTypeSortedSet); err != nil {
			return err
		}
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
	}, 20, encodePropagateCommand([]byte("ZPOPMIN"), []byte(zSetName)))
	s.markZSetDirty(zSetName)
	return results, err
}

// ZMPop 实现 Redis ZMPOP 命令
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
