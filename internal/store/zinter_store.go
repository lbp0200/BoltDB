package store

import (
	"errors"
	"sort"

	"github.com/dgraph-io/badger/v4"
)

// ZUnionStore 实现 Redis ZUNIONSTORE 命令
func (s *BotreonStore) ZUnionStore(destination string, keys []string, weights []float64, aggregate string) (int64, error) {
	unlock := s.keyLockMgr.LockMulti(append([]string{destination}, keys...))
	defer unlock()
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
	s.markZSetDirty(destination)
	return count, err
}

// ZInterStore 实现 Redis ZINTERSTORE 命令
func (s *BotreonStore) ZInterStore(destination string, keys []string, weights []float64, aggregate string) (int64, error) {
	unlock := s.keyLockMgr.LockMulti(append([]string{destination}, keys...))
	defer unlock()
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
	s.markZSetDirty(destination)
	return count, err
}

// ZDiffStore 实现 Redis ZDIFFSTORE 命令
func (s *BotreonStore) ZDiffStore(destination string, keys []string) (int64, error) {
	unlock := s.keyLockMgr.LockMulti(append([]string{destination}, keys...))
	defer unlock()
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
	s.markZSetDirty(destination)
	return count, err
}

// ZInter 实现 Redis ZINTER 命令（Redis 7.0+）
func (s *BotreonStore) ZInter(keys []string, weights []float64, aggregate string) ([]ZSetMember, error) {
	var result []ZSetMember
	err := s.db.View(func(txn *badger.Txn) error {
		memberScores, err := zInterScoresInTxn(txn, keys, weights, aggregate)
		if err != nil {
			return err
		}
		result = make([]ZSetMember, 0, len(memberScores))
		for member, score := range memberScores {
			result = append(result, ZSetMember{Member: member, Score: score})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Score != result[j].Score {
			return result[i].Score < result[j].Score
		}
		return result[i].Member < result[j].Member
	})
	return result, nil
}

// ZInterCard 实现 Redis ZINTERCARD 命令（Redis 7.0+）
func (s *BotreonStore) ZInterCard(keys []string, limit int64) (int64, error) {
	var count int64
	err := s.db.View(func(txn *badger.Txn) error {
		if len(keys) == 0 {
			return nil
		}
		firstMembers, err := zReadZSetMembersInTxn(txn, keys[0])
		if err != nil {
			return err
		}
		for _, member := range firstMembers {
			inAll := true
			for i := 1; i < len(keys); i++ {
				memberScoreKey := sortedSetKeyMember(keys[i], member.Member)
				_, err := txn.Get([]byte(memberScoreKey))
				if errors.Is(err, badger.ErrKeyNotFound) {
					inAll = false
					break
				}
				if err != nil {
					return err
				}
			}
			if inAll {
				count++
				if limit > 0 && count >= limit {
					return nil
				}
			}
		}
		return nil
	})
	return count, err
}

// ZUnion 实现 Redis ZUNION 命令（Redis 7.0+）
func (s *BotreonStore) ZUnion(keys []string, weights []float64, aggregate string) ([]ZSetMember, error) {
	var result []ZSetMember
	err := s.db.View(func(txn *badger.Txn) error {
		memberScores, err := zUnionScoresInTxn(txn, keys, weights, aggregate)
		if err != nil {
			return err
		}
		result = make([]ZSetMember, 0, len(memberScores))
		for member, score := range memberScores {
			result = append(result, ZSetMember{Member: member, Score: score})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Score != result[j].Score {
			return result[i].Score < result[j].Score
		}
		return result[i].Member < result[j].Member
	})
	return result, nil
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
