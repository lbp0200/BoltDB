package store

import (
	"errors"

	"github.com/dgraph-io/badger/v4"
)

// zRankInTxn returns the forward rank of a member, or -1 if not found.
func zRankInTxn(txn *badger.Txn, zSetName, member string) (int64, error) {
	dataKey := sortedSetKeyMember(zSetName, member)
	item, err := txn.Get(dataKey)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return -1, nil
	}
	if err != nil {
		return 0, err
	}

	var score float64
	err = item.Value(func(val []byte) error {
		score, _ = decodeDataValue(val)
		return nil
	})
	if err != nil {
		return 0, err
	}

	// Read Card from meta for bidirectional decision
	metaKey := sortedSetKeyMeta(zSetName)
	metaItem, err := txn.Get(metaKey)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return -1, nil
	}
	if err != nil {
		return 0, err
	}
	var card int64
	err = metaItem.Value(func(val []byte) error {
		meta, mErr := decodeMeta(val)
		if mErr != nil {
			return mErr
		}
		card = meta.Card
		return nil
	})
	if err != nil {
		return 0, err
	}

	opts := badger.DefaultIteratorOptions
	prefix := keyBadgerGet(prefixKeySortedSetBytes, []byte(zSetName+sortedSetIndex))
	opts.Prefix = prefix
	opts.PrefetchValues = false

	// Forward scan with early abort at midpoint
	it := txn.NewIterator(opts)
	defer it.Close()

	var rank int64
	midpoint := card / 2
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		memberScore, memberName, _, ok := parseZSetIndexKey(it.Item().Key(), prefix)
		if !ok {
			continue
		}
		if memberScore < score || (memberScore == score && memberName < member) {
			rank++
			// If we've passed the midpoint without finding, switch to reverse
			if rank > midpoint && midpoint > 0 {
				it.Close()
				return zRankReverseScan(txn, opts, prefix, score, member, card)
			}
			continue
		}
		if memberScore == score && memberName == member {
			return rank, nil
		}
		return -1, nil
	}
	return -1, nil
}

// zRankReverseScan counts entries after the target from the end.
func zRankReverseScan(txn *badger.Txn, opts badger.IteratorOptions, prefix []byte, score float64, member string, card int64) (int64, error) {
	opts.Reverse = true
	it := txn.NewIterator(opts)
	defer it.Close()

	endPrefix := append(prefix, 0xFF)
	it.Seek(endPrefix)

	var reverseRank int64
	for it.ValidForPrefix(prefix) {
		memberScore, memberName, _, ok := parseZSetIndexKey(it.Item().Key(), prefix)
		if !ok {
			it.Next()
			continue
		}
		if memberScore > score || (memberScore == score && memberName > member) {
			reverseRank++
			it.Next()
			continue
		}
		if memberScore == score && memberName == member {
			return card - 1 - reverseRank, nil
		}
		return -1, nil
	}
	return -1, nil
}

// ZRank 实现 Redis ZRANK 命令
func (s *BotreonStore) ZRank(zSetName, member string) (int64, error) {
	var rank int64 = -1
	err := s.db.View(func(txn *badger.Txn) error {
		var err error
		rank, err = zRankInTxn(txn, zSetName, member)
		return err
	})
	return rank, err
}

// ZRevRank 实现 Redis ZREVRANK 命令
func (s *BotreonStore) ZRevRank(zSetName, member string) (int64, error) {
	var rank int64 = -1
	err := s.db.View(func(txn *badger.Txn) error {
		forwardRank, err := zRankInTxn(txn, zSetName, member)
		if err != nil {
			return err
		}
		if forwardRank == -1 {
			rank = -1
			return nil
		}

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
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}

		rank = totalCount - 1 - forwardRank
		return nil
	})
	return rank, err
}
