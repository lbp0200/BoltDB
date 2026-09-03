package store

import (
	"github.com/dgraph-io/badger/v4"
)

// LTrim 实现 Redis LTRIM 命令
func (s *BotreonStore) LTrim(key string, start, stop int64) error {
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)
	return s.retryUpdate(func(txn *badger.Txn) error {
		length, startID, _, err := s.listGetMetaTxn(txn, key)
		if err != nil {
			return nil
		}
		if length == 0 {
			return nil
		}

		if start < 0 {
			start = int64(length) + start
		}
		if stop < 0 {
			stop = int64(length) + stop
		}
		if start < 0 {
			start = 0
		}
		if stop >= int64(length) {
			stop = int64(length) - 1
		}
		if start > stop {
			return s.deleteList(txn, key)
		}

		var newStartID, newEndID string
		currentNodeID := startID
		currentIndex := int64(0)
		visitedTrim := make(map[string]bool)

		for currentIndex < start {
			if visitedTrim[currentNodeID] {
				return nil
			}
			visitedTrim[currentNodeID] = true
			nextKey := s.listKey(key, currentNodeID, "next")
			item, err := txn.Get([]byte(nextKey))
			if err != nil {
				return err
			}
			nextVal, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			oldNodeID := currentNodeID
			currentNodeID = string(nextVal)
			if err := s.deleteNode(txn, key, oldNodeID); err != nil {
				return err
			}
			currentIndex++
		}
		newStartID = currentNodeID

		for currentIndex < stop {
			if visitedTrim[currentNodeID] {
				return nil
			}
			visitedTrim[currentNodeID] = true
			nextKey := s.listKey(key, currentNodeID, "next")
			item, err := txn.Get([]byte(nextKey))
			if err != nil {
				return err
			}
			nextVal, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			currentNodeID = string(nextVal)
			currentIndex++
		}
		newEndID = currentNodeID

		visitedDelete := make(map[string]bool)
		visitedDelete[newStartID] = true
		visitedDelete[newEndID] = true
		currentNodeID = newEndID
		for {
			nextKey := s.listKey(key, currentNodeID, "next")
			item, err := txn.Get([]byte(nextKey))
			if err != nil {
				break
			}
			nextVal, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			nextID := string(nextVal)
			if nextID == newStartID {
				break
			}
			if visitedDelete[nextID] {
				break
			}
			visitedDelete[nextID] = true
			if err := s.deleteNode(txn, key, nextID); err != nil {
				return err
			}
			currentNodeID = nextID
		}

		if err := txn.Set([]byte(s.listKey(key, newStartID, "prev")), []byte(newEndID)); err != nil {
			return err
		}
		if err := txn.Set([]byte(s.listKey(key, newEndID, "next")), []byte(newStartID)); err != nil {
			return err
		}

		newLength := uint64(stop - start + 1)
		return s.listUpdateMeta(txn, key, newLength, newStartID, newEndID)
	}, 30, encodePropagateCommand([]byte("LTRIM"), []byte(key)))
}
