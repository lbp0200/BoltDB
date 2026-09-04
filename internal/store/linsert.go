package store

import (
	"errors"

	"github.com/dgraph-io/badger/v4"
)

// LInsert 实现 Redis LINSERT 命令
func (s *BotreonStore) LInsert(key string, where string, pivot, value string) (int, error) {
	var newLength int
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)
	err := s.retryUpdate(func(txn *badger.Txn) error {
		length, start, _, err := s.listGetMetaTxn(txn, key)
		if err != nil || length == 0 {
			return nil
		}

		currentNodeID := start
		var pivotNodeID string
		visited := make(map[string]bool)
		for i := uint64(0); i < length; i++ {
			if visited[currentNodeID] {
				break
			}
			visited[currentNodeID] = true
			nodeKey := s.listKey(key, currentNodeID)
			item, err := txn.Get([]byte(nodeKey))
			if err != nil {
				break
			}
			valueBytes, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			if string(valueBytes) == pivot {
				pivotNodeID = currentNodeID
				break
			}
			nextKey := s.listKey(key, currentNodeID, "next")
			item, err = txn.Get([]byte(nextKey))
			if err != nil {
				break
			}
			nextVal, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			currentNodeID = string(nextVal)
		}

		if pivotNodeID == "" {
			newLength = -1
			return nil
		}

		newNodeID, err := s.createNode(txn, key, []byte(value))
		if err != nil {
			return err
		}

		if where == "BEFORE" {
			prevKey := s.listKey(key, pivotNodeID, "prev")
			item, err := txn.Get([]byte(prevKey))
			var prevNodeID string
			if err == nil {
				prevVal, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				prevNodeID = string(prevVal)
			} else if errors.Is(err, badger.ErrKeyNotFound) {
				_, _, end, metaErr := s.listGetMetaTxn(txn, key)
				if metaErr != nil {
					return metaErr
				}
				prevNodeID = end
			} else {
				return err
			}

			if err := s.linkNodes(txn, key, prevNodeID, newNodeID); err != nil {
				return err
			}
			if err := s.linkNodes(txn, key, newNodeID, pivotNodeID); err != nil {
				return err
			}

			_, _, end, metaErr := s.listGetMetaTxn(txn, key)
			if metaErr != nil {
				return metaErr
			}
			newStart := start
			newEnd := end
			if pivotNodeID == start {
				newStart = newNodeID
			}
			if err := s.listUpdateMeta(txn, key, length+1, newStart, newEnd); err != nil {
				return err
			}
		} else {
			nextKey := s.listKey(key, pivotNodeID, "next")
			item, err := txn.Get([]byte(nextKey))
			if err != nil {
				return err
			}
			nextVal, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			nextNodeID := string(nextVal)

			if err := s.linkNodes(txn, key, pivotNodeID, newNodeID); err != nil {
				return err
			}
			if err := s.linkNodes(txn, key, newNodeID, nextNodeID); err != nil {
				return err
			}

			_, _, end, err := s.listGetMetaTxn(txn, key)
			if err != nil {
				return err
			}
			newStart := start
			newEnd := end
			if pivotNodeID == end {
				newEnd = newNodeID
			}
			if err := s.listUpdateMeta(txn, key, length+1, newStart, newEnd); err != nil {
				return err
			}
		}
		newLength = int(length) + 1
		return nil
	}, 30, encodePropagateCommand([]byte("LINSERT"), []byte(key), []byte(where), []byte(pivot), []byte(value)))
	return newLength, err
}

// LRem 实现 Redis LREM 命令
func (s *BotreonStore) LRem(key string, count int64, value string) (int, error) {
	removed := 0
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)
	err := s.retryUpdate(func(txn *badger.Txn) error {
		removed = 0
		length, start, _, err := s.listGetMetaTxn(txn, key)
		if err != nil || length == 0 {
			return nil
		}

		var nodesToRemove []string
		currentNodeID := start
		visitedCount := 0
		visitedNodes := make(map[string]bool)

		// Forward scan (only for count > 0 or count == 0)
		if count >= 0 {
			for visitedCount < int(length) {
				if visitedNodes[currentNodeID] {
					break
				}
				visitedNodes[currentNodeID] = true

				nodeKey := s.listKey(key, currentNodeID)
				item, err := txn.Get([]byte(nodeKey))
				if err != nil {
					break
				}
				valueBytes, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				if string(valueBytes) == value {
					nodesToRemove = append(nodesToRemove, currentNodeID)
					if count > 0 && len(nodesToRemove) >= int(count) {
						break
					}
				}

				nextKey := s.listKey(key, currentNodeID, "next")
				item, err = txn.Get([]byte(nextKey))
				if err != nil {
					break
				}
				nextVal, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				currentNodeID = string(nextVal)
				visitedCount++
			}
		}

		if count < 0 {
			_, _, end, metaErr := s.listGetMetaTxn(txn, key)
			if metaErr != nil {
				return metaErr
			}
			currentNodeID = end
			visitedCount = 0
			visitedNodes = make(map[string]bool)
			for visitedCount < int(length) {
				if visitedNodes[currentNodeID] {
					break
				}
				visitedNodes[currentNodeID] = true

				nodeKey := s.listKey(key, currentNodeID)
				item, err := txn.Get([]byte(nodeKey))
				if err != nil {
					break
				}
				valueBytes, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				if string(valueBytes) == value {
					nodesToRemove = append(nodesToRemove, currentNodeID)
					if len(nodesToRemove) >= int(-count) {
						break
					}
				}

				prevKey := s.listKey(key, currentNodeID, "prev")
				item, err = txn.Get([]byte(prevKey))
				if err != nil {
					break
				}
				prevVal, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				currentNodeID = string(prevVal)
				visitedCount++
			}
		}

		_, currentStart, currentEnd, metaErr := s.listGetMetaTxn(txn, key)
		if metaErr != nil {
			return metaErr
		}

		for _, nodeID := range nodesToRemove {
			prevKey := s.listKey(key, nodeID, "prev")
			nextKey := s.listKey(key, nodeID, "next")
			prevItem, err := txn.Get([]byte(prevKey))
			if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
			nextItem, err := txn.Get([]byte(nextKey))
			if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
			var prevID, nextID string
			if prevItem != nil {
				prevVal, err := prevItem.ValueCopy(nil)
				if err != nil {
					return err
				}
				prevID = string(prevVal)
			}
			if nextItem != nil {
				nextVal, err := nextItem.ValueCopy(nil)
				if err != nil {
					return err
				}
				nextID = string(nextVal)
			}

			if err := s.linkNodes(txn, key, prevID, nextID); err != nil {
				return err
			}

			if nodeID == currentStart {
				currentStart = nextID
			}
			if nodeID == currentEnd {
				currentEnd = prevID
			}

			if err := s.deleteNode(txn, key, nodeID); err != nil {
				return err
			}
			removed++
		}

		if removed > 0 {
			newLength := length - uint64(removed)
			if newLength == 0 {
				currentStart = ""
				currentEnd = ""
			}
			return s.listUpdateMeta(txn, key, newLength, currentStart, currentEnd)
		}
		return nil
	}, 30)
	return removed, err
}
