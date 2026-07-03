package store

import (
	"errors"

	"github.com/dgraph-io/badger/v4"
)

// ZScanResult 定义 ZSCAN 命令的返回结果
type ZScanResult struct {
	Cursor  uint64
	Members []ZSetMember
}

// ZScan 实现 Redis ZSCAN 命令
func (s *BotreonStore) ZScan(zSetName string, cursor uint64, pattern string, count int) (ZScanResult, error) {
	var result ZScanResult
	result.Members = []ZSetMember{}

	if count <= 0 {
		count = 10
	}

	seekKey := s.scanBookmarkLookup(cursor)
	s.scanBookmarkRelease(cursor)

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		prefix := keyBadgerGet(prefixKeySortedSetBytes, []byte(zSetName+sortedSetIndex))
		opts.Prefix = prefix
		opts.PrefetchValues = false

		iter := txn.NewIterator(opts)
		defer iter.Close()

		if seekKey != nil {
			iter.Seek(seekKey)
		} else {
			iter.Seek(prefix)
		}

		collected := 0
		var lastKey []byte
		for iter.ValidForPrefix(prefix) && collected < count {
			score, member, _, ok := parseZSetIndexKey(iter.Item().Key(), prefix)
			if ok {
				if pattern == "" || pattern == "*" || matchPattern(member, pattern) {
					result.Members = append(result.Members, ZSetMember{Member: member, Score: score})
					collected++
				}
			}

			lastKey = iter.Item().KeyCopy(nil)
			iter.Next()
		}

		if iter.ValidForPrefix(prefix) {
			result.Cursor = s.scanBookmarkStore(lastKey)
		} else {
			result.Cursor = 0
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

		if count == 0 {
			var reservoir ZSetMember
			found := false
			i := 0
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				score, member, _, ok := parseZSetIndexKey(it.Item().Key(), prefix)
				if !ok {
					continue
				}
				if !found {
					reservoir = ZSetMember{Member: member, Score: score}
					found = true
				} else {
					j := randomIntn(i + 1)
					if j == 0 {
						reservoir = ZSetMember{Member: member, Score: score}
					}
				}
				i++
			}
			if found {
				members = append(members, reservoir)
			}
		} else if count < 0 {
			var allMembers []ZSetMember
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				score, member, _, ok := parseZSetIndexKey(it.Item().Key(), prefix)
				if ok {
					allMembers = append(allMembers, ZSetMember{Member: member, Score: score})
				}
			}
			if len(allMembers) > 0 {
				n := -count
				for i := 0; i < n; i++ {
					idx := randomIntn(len(allMembers))
					members = append(members, allMembers[idx])
				}
			}
		} else {
			n := count
			reservoir := make([]ZSetMember, 0, n)
			i := 0
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				score, member, _, ok := parseZSetIndexKey(it.Item().Key(), prefix)
				if !ok {
					continue
				}
				if i < n {
					reservoir = append(reservoir, ZSetMember{Member: member, Score: score})
				} else {
					j := randomIntn(i + 1)
					if j < n {
						reservoir[j] = ZSetMember{Member: member, Score: score}
					}
				}
				i++
			}
			members = reservoir
		}
		return nil
	})
	return members, err
}
