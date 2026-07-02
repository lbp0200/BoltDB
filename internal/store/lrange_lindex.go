package store

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// LLen 实现 Redis LLEN 命令
func (s *BotreonStore) LLen(key string) (uint64, error) {
	length, err := s.listLength(key)
	return length, err
}

// LIndex 实现 Redis LINDEX 命令
func (s *BotreonStore) LIndex(key string, index int64) (string, error) {
	var value string
	err := s.db.View(func(txn *badger.Txn) error {
		if err := checkKeyType(txn, key, KeyTypeList); err != nil {
			return err
		}
		_, val, err := s.getNodeByIndex(txn, key, index)
		if err != nil {
			return err
		}
		value = val
		return nil
	})
	return value, err
}

// LSet 实现 Redis LSET 命令
func (s *BotreonStore) LSet(key string, index int64, value string) error {
	return s.retryUpdate(func(txn *badger.Txn) error {
		nodeID, _, err := s.getNodeByIndex(txn, key, index)
		if err != nil {
			return err
		}
		if nodeID == "" {
			return fmt.Errorf("index out of range")
		}
		nodeKey := s.listKey(key, nodeID)
		return txn.Set([]byte(nodeKey), []byte(value))
	}, 30)
}

// LRange 实现 Redis LRANGE 命令
func (s *BotreonStore) LRange(key string, start, stop int64) ([]string, error) {
	var result []string
	err := s.db.View(func(txn *badger.Txn) error {
		if err := checkKeyType(txn, key, KeyTypeList); err != nil {
			return err
		}
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
			return nil
		}

		count := stop - start + 1

		if stop > int64(length)/2 {
			_, _, endID, err2 := s.listGetMetaTxn(txn, key)
			if err2 != nil {
				return err2
			}
			r, err := s.lRangeReverse(txn, key, start, stop, count, length, endID)
			result = r
			return err
		}

		r, err := s.lRangeForward(txn, key, start, stop, startID)
		result = r
		return err
	})
	return result, err
}

// lRangeForward 从头部正向遍历
func (s *BotreonStore) lRangeForward(txn *badger.Txn, key string, start, stop int64, startID string) ([]string, error) {
	var result []string
	currentNodeID := startID
	currentIndex := int64(0)
	visitedStart := make(map[string]bool)
	for currentIndex < start {
		if visitedStart[currentNodeID] {
			return result, nil
		}
		visitedStart[currentNodeID] = true
		nextKey := s.listKey(key, currentNodeID, "next")
		item, err := txn.Get([]byte(nextKey))
		if err != nil {
			return result, err
		}
		nextVal, err := item.ValueCopy(nil)
		if err != nil {
			return result, err
		}
		currentNodeID = string(nextVal)
		currentIndex++
	}

	visited := make(map[string]bool)
	for currentIndex <= stop {
		if visited[currentNodeID] {
			break
		}
		visited[currentNodeID] = true

		nodeKey := s.listKey(key, currentNodeID)
		item, err := txn.Get([]byte(nodeKey))
		if err != nil {
			return result, err
		}
		valueBytes, err := item.ValueCopy(nil)
		if err != nil {
			return result, err
		}
		result = append(result, string(valueBytes))

		if currentIndex < stop {
			nextKey := s.listKey(key, currentNodeID, "next")
			item, err = txn.Get([]byte(nextKey))
			if err != nil {
				return result, err
			}
			nextVal, err := item.ValueCopy(nil)
			if err != nil {
				return result, err
			}
			currentNodeID = string(nextVal)
		}
		currentIndex++
	}
	return result, nil
}

// lRangeReverse 从尾部反向遍历
func (s *BotreonStore) lRangeReverse(txn *badger.Txn, key string, start, stop int64, count int64, length uint64, endID string) ([]string, error) {
	currentNodeID := endID
	stepsFromEnd := int64(length-1) - stop
	visitedStart := make(map[string]bool)
	for i := int64(0); i < stepsFromEnd; i++ {
		if visitedStart[currentNodeID] {
			return nil, nil
		}
		visitedStart[currentNodeID] = true
		prevKey := s.listKey(key, currentNodeID, "prev")
		item, err := txn.Get([]byte(prevKey))
		if err != nil {
			return nil, err
		}
		prevVal, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		currentNodeID = string(prevVal)
	}

	tmp := make([]string, 0, count)
	visited := make(map[string]bool)
	for i := int64(0); i < count; i++ {
		if visited[currentNodeID] {
			break
		}
		visited[currentNodeID] = true

		nodeKey := s.listKey(key, currentNodeID)
		item, err := txn.Get([]byte(nodeKey))
		if err != nil {
			return nil, err
		}
		valueBytes, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		tmp = append(tmp, string(valueBytes))

		if i < count-1 {
			prevKey := s.listKey(key, currentNodeID, "prev")
			item, err = txn.Get([]byte(prevKey))
			if err != nil {
				return nil, err
			}
			prevVal, err := item.ValueCopy(nil)
			if err != nil {
				return nil, err
			}
			currentNodeID = string(prevVal)
		}
	}

	result := make([]string, len(tmp))
	for i := len(tmp) - 1; i >= 0; i-- {
		result[len(tmp)-1-i] = tmp[i]
	}
	return result, nil
}

// getNodeByIndex 根据索引获取节点ID和值
func (s *BotreonStore) getNodeByIndex(txn *badger.Txn, key string, index int64) (string, string, error) {
	length, start, _, err := s.listGetMetaTxn(txn, key)
	if err != nil {
		return "", "", err
	}
	return s.getNodeByIndexWithMeta(txn, key, index, length, start)
}

// getNodeByIndexWithMeta 根据索引获取节点ID和值，接受预取的元数据
func (s *BotreonStore) getNodeByIndexWithMeta(txn *badger.Txn, key string, index int64, length uint64, start string) (string, string, error) {
	if length == 0 {
		return "", "", nil
	}

	if index < 0 {
		index = int64(length) + index
	}
	if index < 0 || index >= int64(length) {
		return "", "", nil
	}

	var currentNodeID string
	var currentIndex int64
	visited := make(map[string]bool)
	if index < int64(length)/2 {
		currentNodeID = start
		currentIndex = 0
		for currentIndex < index {
			if visited[currentNodeID] {
				return "", "", nil
			}
			visited[currentNodeID] = true
			nextKey := s.listKey(key, currentNodeID, "next")
			item, err := txn.Get([]byte(nextKey))
			if err != nil {
				return "", "", err
			}
			nextVal, err := item.ValueCopy(nil)
			if err != nil {
				return "", "", err
			}
			currentNodeID = string(nextVal)
			currentIndex++
		}
	} else {
		_, _, end, metaErr := s.listGetMetaTxn(txn, key)
		if metaErr != nil {
			return "", "", metaErr
		}
		currentNodeID = end
		currentIndex = int64(length) - 1
		for currentIndex > index {
			if visited[currentNodeID] {
				return "", "", nil
			}
			visited[currentNodeID] = true
			prevKey := s.listKey(key, currentNodeID, "prev")
			item, err := txn.Get([]byte(prevKey))
			if err != nil {
				return "", "", err
			}
			prevVal, err := item.ValueCopy(nil)
			if err != nil {
				return "", "", err
			}
			currentNodeID = string(prevVal)
			currentIndex--
		}
	}

	nodeKey := s.listKey(key, currentNodeID)
	item, err := txn.Get([]byte(nodeKey))
	if err != nil {
		return "", "", err
	}
	valueBytes, err := item.ValueCopy(nil)
	if err != nil {
		return "", "", err
	}
	return currentNodeID, string(valueBytes), nil
}

// LPos 实现 Redis LPOS 命令
func (s *BotreonStore) LPos(key string, element string, rank, count, maxlen int64) ([]int64, error) {
	var results []int64

	err := s.db.View(func(txn *badger.Txn) error {
		length, startID, _, err := s.listGetMetaTxn(txn, key)
		if err != nil {
			return nil
		}
		if length == 0 {
			return nil
		}

		scanLen := int64(length)
		if maxlen > 0 && maxlen < scanLen {
			scanLen = maxlen
		}

		startIdx := int64(0)
		if rank < 0 {
			startIdx = int64(length) + rank
			if startIdx < 0 {
				startIdx = 0
			}
		}

		currentNodeID := startID
		{
			visitedSeek := make(map[string]bool)
			for i := int64(0); i < startIdx; i++ {
				if visitedSeek[currentNodeID] {
					return nil
				}
				visitedSeek[currentNodeID] = true
				nextKey := s.listKey(key, currentNodeID, "next")
				item, err := txn.Get([]byte(nextKey))
				if err != nil {
					return nil
				}
				nextVal, err := item.ValueCopy(nil)
				if err != nil {
					return nil
				}
				currentNodeID = string(nextVal)
			}
		}

		matches := int64(0)
		found := int64(0)
		currentIndex := startIdx
		visited := make(map[string]bool)
		for currentIndex < scanLen {
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
				break
			}
			val := string(valueBytes)

			if val == element {
				matches++
				if rank > 0 && matches < rank {
				} else if rank < 0 && matches < -rank {
				} else {
					found++
					results = append(results, currentIndex)
					if (count == 0 && rank == 0) || (count > 0 && found >= count) {
						break
					}
				}
			}

			currentIndex++
			if currentIndex < scanLen {
				nextKey := s.listKey(key, currentNodeID, "next")
				item, err := txn.Get([]byte(nextKey))
				if err != nil {
					break
				}
				nextVal, err := item.ValueCopy(nil)
				if err != nil {
					break
				}
				currentNodeID = string(nextVal)
			}
		}

		return nil
	})

	return results, err
}
