package store

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/lbp0200/BoltDB/internal/helper"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
)

// ListNode 结构体定义了链表节点的结构
// ID 是节点的唯一标识符，使用字符串存储
// Value 是节点存储的数据，以字节切片形式存储
// Prev 是指向前一个节点的 ID，使用字符串存储
// Next 是指向后一个节点的 ID，使用字符串存储
type ListNode struct {
	ID    string
	Value []byte // 节点存储的值，以字节切片形式表示
	Prev  string // 指向前一个节点的 ID
	Next  string // 指向后一个节点的 ID
}

// listKey 方法用于生成存储在 Badger 数据库中的键
// key 是链表的主键，以字节切片形式传入
// parts 是可变参数，用于拼接更多的键信息
// 返回一个字节切片，作为存储在数据库中的完整键
func (s *BotreonStore) listKey(key string, parts ...string) string {
	return fmt.Sprintf("%s:%s:%s", KeyTypeList, key, strings.Join(parts, ":"))
}

// listLength 方法用于获取链表的长度
// key 是链表的主键，以字节切片形式传入
// 返回链表的长度（无符号 64 位整数）和可能出现的错误
func (s *BotreonStore) listLength(key string) (uint64, error) {
	var length uint64
	errView := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(TypeOfKeyGet(key))
		if err == nil {
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			keyType := string(val)
			if keyType != "" && keyType != KeyTypeList {
				return ErrWrongType
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		// 获取长度
		// 通过 listKey 方法生成存储长度信息的键
		lengthItem, err := txn.Get([]byte(s.listKey(key, "length")))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				length = 0 // 列表不存在，返回0
				return nil
			}
			return err
		}
		// 从数据库中获取长度值，并将其复制到 lengthVal 中
		lengthVal, err := lengthItem.ValueCopy(nil)
		if err != nil {
			return err
		}
		// 将字节切片形式的长度值转换为无符号 64 位整数
		length = helper.BytesToUint64(lengthVal)
		return nil
	})
	return length, errView
}

func (s *BotreonStore) listGetMetaTxn(txn *badger.Txn, keyRedis string) (length uint64, start, end string, err error) {
	// 获取长度
	lengthItem, errGet := txn.Get([]byte(s.listKey(keyRedis, "length")))
	if errGet != nil {
		if errors.Is(errGet, badger.ErrKeyNotFound) {
			length = 0
			start = ""
			end = ""
			return // 列表不存在，返回默认值
		}
		err = errGet
		return
	}
	lengthVal, errValueCopy := lengthItem.ValueCopy(nil)
	if errValueCopy != nil {
		err = errValueCopy
		return
	}
	length = helper.BytesToUint64(lengthVal)

	// 获取起始节点
	startItem, errStart := txn.Get([]byte(s.listKey(keyRedis, "start")))
	if errStart != nil {
		if !errors.Is(errStart, badger.ErrKeyNotFound) {
			err = errStart
			return
		}
	} else {
		startVal, errValueCopy := startItem.ValueCopy(nil)
		if errValueCopy != nil {
			err = errValueCopy
			return
		}
		start = string(startVal)
	}

	// 获取结束节点
	endItem, errEnd := txn.Get([]byte(s.listKey(keyRedis, "end")))
	if errEnd != nil {
		if !errors.Is(errEnd, badger.ErrKeyNotFound) {
			err = errEnd
			return
		}
	} else {
		endVal, errValueCopy := endItem.ValueCopy(nil)
		if errValueCopy != nil {
			err = errValueCopy
			return
		}
		end = string(endVal)
	}
	return
}

func (s *BotreonStore) listUpdateMeta(txn *badger.Txn, key string, length uint64, start, end string) error {
	// 更新长度
	if err := txn.Set([]byte(s.listKey(key, "length")), helper.Uint64ToBytes(length)); err != nil {
		return err
	}
	// 更新起始节点
	if err := txn.Set([]byte(s.listKey(key, "start")), []byte(start)); err != nil {
		return err
	}
	// 更新结束节点
	return txn.Set([]byte(s.listKey(key, "end")), []byte(end))
}

func (s *BotreonStore) createNode(txn *badger.Txn, key string, value []byte) (string, error) {
	nodeID := uuid.New().String()
	nodeKey := s.listKey(key, nodeID)
	if err := txn.Set([]byte(nodeKey), value); err != nil {
		return "", err
	}
	return nodeID, nil
}

func (s *BotreonStore) linkNodes(txn *badger.Txn, key string, prevID, nextID string) error {
	// 更新前节点的next指针
	if prevID != "" {
		prevNextKey := s.listKey(key, prevID, "next")
		if err := txn.Set([]byte(prevNextKey), []byte(nextID)); err != nil {
			return err
		}
	}
	// 更新后节点的prev指针
	if nextID != "" {
		nextPrevKey := s.listKey(key, nextID, "prev")
		return txn.Set([]byte(nextPrevKey), []byte(prevID))
	}
	return nil
}

// LPush Redis LPUSH 实现
func (s *BotreonStore) LPush(key string, values ...string) (int, error) {
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)

	var finalLength uint64
	err := s.retryUpdate(func(txn *badger.Txn) error {
		// Check if key already exists with a different type
		item, err := txn.Get(TypeOfKeyGet(key))
		if err == nil {
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			keyType := string(val)
			if keyType != "" && keyType != KeyTypeList {
				return ErrWrongType
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		if err := txn.Set(TypeOfKeyGet(key), []byte(KeyTypeList)); err != nil {
			return err
		}
		length, start, end, metaErr := s.listGetMetaTxn(txn, key)
		if metaErr != nil {
			return metaErr
		}
		for _, value := range values {
			// 创建新节点
			nodeID, err := s.createNode(txn, key, []byte(value))
			if err != nil {
				return err
			}

			// 链接节点
			if length == 0 { // 空链表
				start = nodeID
				end = nodeID
				if err := s.linkNodes(txn, key, nodeID, nodeID); err != nil {
					return err
				}
			} else {
				// 链接新节点和原头节点
				if err := s.linkNodes(txn, key, nodeID, start); err != nil {
					return err
				}
				// 更新原头节点的prev指针
				if err := txn.Set([]byte(s.listKey(key, start, "prev")), []byte(nodeID)); err != nil {
					return err
				}
				start = nodeID
			}
			length++
		}

		// 更新元数据
		finalLength = length
		return s.listUpdateMeta(txn, key, length, start, end)
	}, 30)

	// Notify blocking pop waiters
	s.notifyBlockingPop(key, values[0])

	return int(finalLength), err // 返回操作后列表的长度（Redis规范）
}

// RPOP 实现
func (s *BotreonStore) RPop(key string) (string, error) {
	var value string
	err := s.checkTypeBeforeOp(key, KeyTypeList)
	if err != nil {
		return "", err
	}
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)
	err = s.retryUpdate(func(txn *badger.Txn) error {
		length, start, end, err := s.listGetMetaTxn(txn, key)
		if err != nil {
			// 如果列表不存在，返回空字符串
			if length == 0 {
				return nil
			}
			return err
		}
		if length == 0 {
			return nil
		}

		// 获取尾节点值
		endNodeKey := s.listKey(key, end)
		item, err := txn.Get([]byte(endNodeKey))
		if err != nil {
			return err
		}
		valueBytes, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		value = string(valueBytes)

		// 获取新的尾节点
		var newEnd string
		if length > 1 {
			newEndKey := s.listKey(key, end, "prev")
			item, err = txn.Get([]byte(newEndKey))
			if err != nil {
				return err
			}
			newEndVal, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			newEnd = string(newEndVal)
		} else {
			newEnd = ""
			start = ""
		}

		// 更新链表关系
		if length > 1 {
			// 断开旧尾节点连接
			if err := s.linkNodes(txn, key, newEnd, start); err != nil {
				return err
			}
		}

		// 删除旧节点数据
		if err := txn.Delete([]byte(endNodeKey)); err != nil {
			return err
		}
		if err := txn.Delete([]byte(s.listKey(key, end, "prev"))); err != nil {
			return err
		}
		if err := txn.Delete([]byte(s.listKey(key, end, "next"))); err != nil {
			return err
		}

		// 更新元数据
		return s.listUpdateMeta(txn, key, length-1, start, newEnd)
	}, 30)
	return value, err
}

// LLEN 实现
func (s *BotreonStore) LLen(key string) (uint64, error) {
	length, err := s.listLength(key)
	return length, err
}

// getNodeByIndex 根据索引获取节点ID和值
func (s *BotreonStore) getNodeByIndex(txn *badger.Txn, key string, index int64) (string, string, error) {
	length, start, _, err := s.listGetMetaTxn(txn, key)
	if err != nil {
		return "", "", err
	}
	if length == 0 {
		return "", "", nil
	}

	// 处理负数索引
	if index < 0 {
		// #nosec G115 - length is bounded by practical list size limits
		index = int64(length) + index
	}
	// #nosec G115 - length is bounded by practical list size limits
	if index < 0 || index >= int64(length) {
		return "", "", nil
	}

	// 从头部或尾部开始遍历
	var currentNodeID string
	var currentIndex int64
	visited := make(map[string]bool)
	// #nosec G115 - length is bounded by practical list size limits
	if index < int64(length)/2 {
		// 从头部开始
		currentNodeID = start
		currentIndex = 0
		for currentIndex < index {
			// 防止循环链表导致的无限循环
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
		// 从尾部开始
		_, _, end, metaErr := s.listGetMetaTxn(txn, key)
		if metaErr != nil {
			return "", "", metaErr
		}
		currentNodeID = end
		// #nosec G115 - length is bounded by practical list size limits
		currentIndex = int64(length) - 1
		for currentIndex > index {
			// 防止循环链表导致的无限循环
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

	// 获取节点值
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

// RPUSH 实现 Redis RPUSH 命令
func (s *BotreonStore) RPush(key string, values ...string) (int, error) {
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)

	var finalLength uint64
	err := s.retryUpdate(func(txn *badger.Txn) error {
		// Check if key already exists with a different type
		item, err := txn.Get(TypeOfKeyGet(key))
		if err == nil {
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			keyType := string(val)
			if keyType != "" && keyType != KeyTypeList {
				return ErrWrongType
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		if err := txn.Set(TypeOfKeyGet(key), []byte(KeyTypeList)); err != nil {
			return err
		}
		length, start, end, metaErr := s.listGetMetaTxn(txn, key)
		if metaErr != nil {
			return metaErr
		}
		for _, value := range values {
			// 创建新节点
			nodeID, err := s.createNode(txn, key, []byte(value))
			if err != nil {
				return err
			}

			// 链接节点
			if length == 0 { // 空链表
				start = nodeID
				end = nodeID
				if err := s.linkNodes(txn, key, nodeID, nodeID); err != nil {
					return err
				}
			} else {
				// 链接新节点和原尾节点
				if err := s.linkNodes(txn, key, end, nodeID); err != nil {
					return err
				}
				// 更新原尾节点的next指针
				if err := txn.Set([]byte(s.listKey(key, end, "next")), []byte(nodeID)); err != nil {
					return err
				}
				end = nodeID
			}
			length++
		}

		// 更新元数据
		finalLength = length
		return s.listUpdateMeta(txn, key, length, start, end)
	}, 30)
	// #nosec G115 - length is bounded by practical list size limits

	// Notify blocking pop waiters
	s.notifyBlockingPop(key, values[0])

	return int(finalLength), err // 返回操作后列表的长度（Redis规范）
}

// LPOP 实现 Redis LPOP 命令
func (s *BotreonStore) LPop(key string) (string, error) {
	var value string
	err := s.checkTypeBeforeOp(key, KeyTypeList)
	if err != nil {
		return "", err
	}
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)
	err = s.retryUpdate(func(txn *badger.Txn) error {
		length, start, end, err := s.listGetMetaTxn(txn, key)
		if err != nil {
			if length == 0 {
				return nil
			}
			return err
		}
		if length == 0 {
			return nil
		}

		// 获取头节点值
		startNodeKey := s.listKey(key, start)
		item, err := txn.Get([]byte(startNodeKey))
		if err != nil {
			return err
		}
		valueBytes, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		value = string(valueBytes)

		// 获取新的头节点
		var newStart string
		if length > 1 {
			newStartKey := s.listKey(key, start, "next")
			item, err = txn.Get([]byte(newStartKey))
			if err != nil {
				return err
			}
			newStartVal, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			newStart = string(newStartVal)
		} else {
			newStart = ""
			end = ""
		}

		// 更新链表关系
		if length > 1 {
			// 断开旧头节点连接
			if err := s.linkNodes(txn, key, end, newStart); err != nil {
				return err
			}
		}

		// 删除旧节点数据
		if err := txn.Delete([]byte(startNodeKey)); err != nil {
			return err
		}
		if err := txn.Delete([]byte(s.listKey(key, start, "prev"))); err != nil {
			return err
		}
		if err := txn.Delete([]byte(s.listKey(key, start, "next"))); err != nil {
			return err
		}

		// 更新元数据
		return s.listUpdateMeta(txn, key, length-1, newStart, end)
	}, 30)
	return value, err
}

// LINDEX 实现 Redis LINDEX 命令
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

// LRANGE 实现 Redis LRANGE 命令
func (s *BotreonStore) LRange(key string, start, stop int64) ([]string, error) {
	var result []string
	err := s.db.View(func(txn *badger.Txn) error {
		if err := checkKeyType(txn, key, KeyTypeList); err != nil {
			return err
		}
		length, startID, _, err := s.listGetMetaTxn(txn, key)
		if err != nil {
			return nil // 列表不存在，返回空列表
		}
		if length == 0 {
			return nil
		}

		// 处理负数索引
		if start < 0 {
			// #nosec G115 - length is bounded by practical list size limits
			start = int64(length) + start
		}
		if stop < 0 {
			// #nosec G115 - length is bounded by practical list size limits
			stop = int64(length) + stop
		}
		if start < 0 {
			start = 0
		}
		// #nosec G115 - length is bounded by practical list size limits
		if stop >= int64(length) {
			stop = int64(length) - 1
		}
		if start > stop {
			return nil
		}

		// 找到起始节点
		currentNodeID := startID
		currentIndex := int64(0)
		visitedStart := make(map[string]bool)
		for currentIndex < start {
			// 防止循环链表导致的无限循环
			if visitedStart[currentNodeID] {
				return nil
			}
			visitedStart[currentNodeID] = true
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

		// 收集范围内的值
		visited := make(map[string]bool)
		for currentIndex <= stop {
			// 防止循环链表导致的无限循环
			if visited[currentNodeID] {
				break
			}
			visited[currentNodeID] = true

			nodeKey := s.listKey(key, currentNodeID)
			item, err := txn.Get([]byte(nodeKey))
			if err != nil {
				return err
			}
			valueBytes, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			result = append(result, string(valueBytes))

			if currentIndex < stop {
				nextKey := s.listKey(key, currentNodeID, "next")
				item, err = txn.Get([]byte(nextKey))
				if err != nil {
					return err
				}
				nextVal, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				currentNodeID = string(nextVal)
			}
			currentIndex++
		}
		return nil
	})
	return result, err
}

// LSET 实现 Redis LSET 命令
func (s *BotreonStore) LSet(key string, index int64, value string) error {
	return s.retryUpdate(func(txn *badger.Txn) error {
		nodeID, _, err := s.getNodeByIndex(txn, key, index)
		if err != nil {
			return err
		}
		if nodeID == "" {
			return fmt.Errorf("index out of range")
		}

		// 更新节点值
		nodeKey := s.listKey(key, nodeID)
		return txn.Set([]byte(nodeKey), []byte(value))
	}, 30)
}

// LPos 实现 Redis LPOS 命令，返回元素在列表中的索引
// element: 要查找的元素
// rank: 正数从头部开始，负数从尾部开始
// count: 返回多个匹配位置，0 表示返回所有
// maxlen: 最大扫描长度，0 表示扫描整个列表
func (s *BotreonStore) LPos(key string, element string, rank, count, maxlen int64) ([]int64, error) {
	var results []int64

	err := s.db.View(func(txn *badger.Txn) error {
		length, _, _, err := s.listGetMetaTxn(txn, key)
		if err != nil {
			return nil // 列表不存在，返回空
		}
		if length == 0 {
			return nil
		}

		// 计算实际扫描长度
		scanLen := int64(length)
		if maxlen > 0 && maxlen < scanLen {
			scanLen = maxlen
		}

		// 计算起始索引
		startIdx := int64(0)
		if rank < 0 {
			// 从尾部开始，需要调整
			// #nosec G115 - length is bounded by practical list size limits
			startIdx = int64(length) + rank
			if startIdx < 0 {
				startIdx = 0
			}
		}

		matches := int64(0)
		found := int64(0)
		for i := startIdx; i < scanLen; i++ {
			_, val, err := s.getNodeByIndex(txn, key, i)
			if err != nil {
				continue
			}
			if val == element {
				matches++
				// 检查是否达到 rank
				if rank > 0 && matches < rank {
					continue
				}
				if rank < 0 && matches < -rank {
					continue
				}
				found++
				results = append(results, i)

				// 如果只需要一个结果，或者达到了 count 限制
				if (count == 0 && rank == 0) || (count > 0 && found >= count) {
					break
				}
			}
		}

		return nil
	})

	return results, err
}

// LTRIM 实现 Redis LTRIM 命令
func (s *BotreonStore) LTrim(key string, start, stop int64) error {
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)
	return s.retryUpdate(func(txn *badger.Txn) error {
		length, startID, _, err := s.listGetMetaTxn(txn, key)
		if err != nil {
			return nil // 列表不存在，无需操作
		}
		if length == 0 {
			return nil
		}

		// 处理负数索引
		if start < 0 {
			// #nosec G115 - length is bounded by practical list size limits
			start = int64(length) + start
		}
		if stop < 0 {
			// #nosec G115 - length is bounded by practical list size limits
			stop = int64(length) + stop
		}
		if start < 0 {
			start = 0
		}
		// #nosec G115 - length is bounded by practical list size limits
		if stop >= int64(length) {
			stop = int64(length) - 1
		}
		if start > stop {
			// 删除整个列表
			return s.deleteList(txn, key)
		}

		// 找到新的起始和结束节点
		var newStartID, newEndID string
		currentNodeID := startID
		currentIndex := int64(0)
		visitedTrim := make(map[string]bool)

		// 找到起始节点
		for currentIndex < start {
			// 防止循环链表导致的无限循环
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
			// 删除旧节点
			if err := s.deleteNode(txn, key, oldNodeID); err != nil {
				return err
			}
			currentIndex++
		}
		newStartID = currentNodeID

		// 找到结束节点
		for currentIndex < stop {
			// 防止循环链表导致的无限循环
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

		// 删除结束节点之后的所有节点
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
				break // 循环回到起始节点
			}
			if visitedDelete[nextID] {
				break // 防止无限循环
			}
			visitedDelete[nextID] = true
			if err := s.deleteNode(txn, key, nextID); err != nil {
				return err
			}
			currentNodeID = nextID
		}

		// 更新链表关系
		if err := txn.Set([]byte(s.listKey(key, newStartID, "prev")), []byte(newEndID)); err != nil {
			return err
		}
		if err := txn.Set([]byte(s.listKey(key, newEndID, "next")), []byte(newStartID)); err != nil {
			return err
		}

		// 更新元数据
		// #nosec G115 - stop and start are bounded by practical list size limits
		newLength := uint64(stop - start + 1)
		return s.listUpdateMeta(txn, key, newLength, newStartID, newEndID)
	}, 30)
}

// deleteNode 删除一个节点
func (s *BotreonStore) deleteNode(txn *badger.Txn, key, nodeID string) error {
	for _, suffix := range []string{"", "prev", "next"} {
		if err := txn.Delete([]byte(s.listKey(key, nodeID, suffix))); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
	}
	return nil
}

func (s *BotreonStore) lockListKeysOrdered(keys ...string) func() {
	unique := make([]string, 0, len(keys))
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, key)
	}
	sort.Strings(unique)
	for _, key := range unique {
		s.keyLockMgr.Lock(key)
	}
	return func() {
		for i := len(unique) - 1; i >= 0; i-- {
			s.keyLockMgr.Unlock(unique[i])
		}
	}
}

// listPopInTxn pops one element from the head (fromLeft=true) or tail of a list.
func (s *BotreonStore) listPopInTxn(txn *badger.Txn, key string, fromLeft bool) (string, error) {
	length, start, end, err := s.listGetMetaTxn(txn, key)
	if err != nil || length == 0 {
		return "", nil
	}

	var nodeID, neighborID string
	if fromLeft {
		nodeID = start
		if length > 1 {
			item, err := txn.Get([]byte(s.listKey(key, nodeID, "next")))
			if err != nil {
				return "", err
			}
			nextVal, err := item.ValueCopy(nil)
			if err != nil {
				return "", err
			}
			neighborID = string(nextVal)
		}
	} else {
		nodeID = end
		if length > 1 {
			item, err := txn.Get([]byte(s.listKey(key, nodeID, "prev")))
			if err != nil {
				return "", err
			}
			prevVal, err := item.ValueCopy(nil)
			if err != nil {
				return "", err
			}
			neighborID = string(prevVal)
		}
	}

	item, err := txn.Get([]byte(s.listKey(key, nodeID)))
	if err != nil {
		return "", err
	}
	valueBytes, err := item.ValueCopy(nil)
	if err != nil {
		return "", err
	}
	value := string(valueBytes)

	if fromLeft {
		if length > 1 {
			if err := s.linkNodes(txn, key, end, neighborID); err != nil {
				return "", err
			}
			start = neighborID
		} else {
			start = ""
			end = ""
		}
	} else {
		if length > 1 {
			if err := s.linkNodes(txn, key, neighborID, start); err != nil {
				return "", err
			}
			end = neighborID
		} else {
			start = ""
			end = ""
		}
	}

	if err := s.deleteNode(txn, key, nodeID); err != nil {
		return "", err
	}
	if err := s.listUpdateMeta(txn, key, length-1, start, end); err != nil {
		return "", err
	}
	return value, nil
}

// listPushInTxn pushes one element to the head (toLeft=true) or tail of a list.
func (s *BotreonStore) listPushInTxn(txn *badger.Txn, key, value string, toLeft bool) error {
	item, err := txn.Get(TypeOfKeyGet(key))
	if err == nil {
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		keyType := string(val)
		if keyType != "" && keyType != KeyTypeList {
			return ErrWrongType
		}
	} else if !errors.Is(err, badger.ErrKeyNotFound) {
		return err
	}
	if err := txn.Set(TypeOfKeyGet(key), []byte(KeyTypeList)); err != nil {
		return err
	}

	length, start, end, err := s.listGetMetaTxn(txn, key)
	if err != nil {
		return err
	}

	newNodeID, err := s.createNode(txn, key, []byte(value))
	if err != nil {
		return err
	}

	if length == 0 {
		if err := s.linkNodes(txn, key, newNodeID, newNodeID); err != nil {
			return err
		}
		return s.listUpdateMeta(txn, key, 1, newNodeID, newNodeID)
	}

	if toLeft {
		if err := s.linkNodes(txn, key, newNodeID, start); err != nil {
			return err
		}
		if err := txn.Set([]byte(s.listKey(key, start, "prev")), []byte(newNodeID)); err != nil {
			return err
		}
		start = newNodeID
	} else {
		if err := s.linkNodes(txn, key, end, newNodeID); err != nil {
			return err
		}
		if err := txn.Set([]byte(s.listKey(key, end, "next")), []byte(newNodeID)); err != nil {
			return err
		}
		end = newNodeID
	}
	return s.listUpdateMeta(txn, key, length+1, start, end)
}

// listPushXInTxn pushes values only when the key already exists as a list.
// Returns (0, nil) when the key is absent.
func (s *BotreonStore) listPushXInTxn(txn *badger.Txn, key string, values []string, toLeft bool) (uint64, error) {
	typeKey := TypeOfKeyGet(key)
	item, err := txn.Get(typeKey)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	val, err := item.ValueCopy(nil)
	if err != nil {
		return 0, err
	}
	if string(val) != KeyTypeList {
		return 0, ErrWrongType
	}

	length, start, end, err := s.listGetMetaTxn(txn, key)
	if err != nil {
		return 0, err
	}

	for _, value := range values {
		nodeID, err := s.createNode(txn, key, []byte(value))
		if err != nil {
			return 0, err
		}
		if length == 0 {
			start = nodeID
			end = nodeID
			if err := s.linkNodes(txn, key, nodeID, nodeID); err != nil {
				return 0, err
			}
		} else if toLeft {
			if err := s.linkNodes(txn, key, nodeID, start); err != nil {
				return 0, err
			}
			if err := txn.Set([]byte(s.listKey(key, start, "prev")), []byte(nodeID)); err != nil {
				return 0, err
			}
			start = nodeID
		} else {
			if err := s.linkNodes(txn, key, end, nodeID); err != nil {
				return 0, err
			}
			if err := txn.Set([]byte(s.listKey(key, end, "next")), []byte(nodeID)); err != nil {
				return 0, err
			}
			end = nodeID
		}
		length++
	}
	if err := s.listUpdateMeta(txn, key, length, start, end); err != nil {
		return 0, err
	}
	return length, nil
}

// deleteList 删除整个列表
func (s *BotreonStore) deleteList(txn *badger.Txn, key string) error {
	_, start, _, metaErr := s.listGetMetaTxn(txn, key)
	if metaErr != nil {
		return metaErr
	}
	if start != "" {
		currentNodeID := start
		visited := make(map[string]bool)
		for !visited[currentNodeID] {
			visited[currentNodeID] = true
			if err := s.deleteNode(txn, key, currentNodeID); err != nil {
				return err
			}
			nextKey := s.listKey(key, currentNodeID, "next")
			item, err := txn.Get([]byte(nextKey))
			if err != nil {
				break
			}
			nextVal, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			currentNodeID = string(nextVal)
			if currentNodeID == start {
				break
			}
		}
	}
	// 删除元数据
	for _, metaKey := range []string{"length", "start", "end"} {
		if err := txn.Delete([]byte(s.listKey(key, metaKey))); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
	}
	return txn.Delete(TypeOfKeyGet(key))
}

// LINSERT 实现 Redis LINSERT 命令
func (s *BotreonStore) LInsert(key string, where string, pivot, value string) (int, error) {
	var newLength int
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)
	err := s.retryUpdate(func(txn *badger.Txn) error {
		length, start, _, err := s.listGetMetaTxn(txn, key)
		if err != nil || length == 0 {
			return nil // 列表不存在或为空
		}

		// 查找pivot节点
		currentNodeID := start
		var pivotNodeID string
		visited := make(map[string]bool)
		for i := uint64(0); i < length; i++ {
			// 防止循环链表导致的无限循环
			if visited[currentNodeID] {
				break
			}
			visited[currentNodeID] = true
			nodeKey := s.listKey(key, currentNodeID)
			item, err := txn.Get([]byte(nodeKey))
			if err != nil {
				// 节点不存在，停止遍历
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
				// next指针不存在，停止遍历
				break
			}
			nextVal, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			currentNodeID = string(nextVal)
		}

		if pivotNodeID == "" {
			newLength = -1 // pivot not found
			return nil
		}

		// 创建新节点
		newNodeID, err := s.createNode(txn, key, []byte(value))
		if err != nil {
			return err
		}

		if where == "BEFORE" {
			// 在pivot之前插入
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
				// 如果prev不存在，说明pivot是第一个节点，prevNodeID应该是end（循环链表）
				_, _, end, metaErr := s.listGetMetaTxn(txn, key)
				if metaErr != nil {
					return metaErr
				}
				prevNodeID = end
			} else {
				return err
			}

			// 更新链接
			if err := s.linkNodes(txn, key, prevNodeID, newNodeID); err != nil {
				return err
			}
			if err := s.linkNodes(txn, key, newNodeID, pivotNodeID); err != nil {
				return err
			}

			// 如果插入在头部，更新start
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
			// 在pivot之后插入
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

			// 更新链接
			if err := s.linkNodes(txn, key, pivotNodeID, newNodeID); err != nil {
				return err
			}
			if err := s.linkNodes(txn, key, newNodeID, nextNodeID); err != nil {
				return err
			}

			// 如果插入在尾部，更新end
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
	}, 30)
	return newLength, err
}

// LREM 实现 Redis LREM 命令
func (s *BotreonStore) LRem(key string, count int64, value string) (int, error) {
	removed := 0
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)
	err := s.retryUpdate(func(txn *badger.Txn) error {
		removed = 0 // reset each attempt; stale value must not survive conflict retry
		length, start, _, err := s.listGetMetaTxn(txn, key)
		if err != nil || length == 0 {
			return nil
		}

		var nodesToRemove []string
		currentNodeID := start
		visitedCount := 0
		visitedNodes := make(map[string]bool)

		// 收集要删除的节点
		// #nosec G115 - length is bounded by practical list size limits
		for visitedCount < int(length) {
			// 防止循环链表导致的无限循环
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

		// 如果count < 0，从尾部开始
		if count < 0 {
			_, _, end, metaErr := s.listGetMetaTxn(txn, key)
			if metaErr != nil {
				return metaErr
			}
			currentNodeID = end
			visitedCount = 0
			visitedNodes = make(map[string]bool)
			nodesToRemove = []string{}
			// #nosec G115 - length is bounded by practical list size limits
			for visitedCount < int(length) {
				// 防止循环链表导致的无限循环
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

		// 获取当前start和end
		_, currentStart, currentEnd, metaErr := s.listGetMetaTxn(txn, key)
		if metaErr != nil {
			return metaErr
		}

		// 删除节点
		for _, nodeID := range nodesToRemove {
			// 获取前后节点
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

			// 更新链接
			if err := s.linkNodes(txn, key, prevID, nextID); err != nil {
				return err
			}

			// 更新start和end
			if nodeID == currentStart {
				currentStart = nextID
			}
			if nodeID == currentEnd {
				currentEnd = prevID
			}

			// 删除节点
			if err := s.deleteNode(txn, key, nodeID); err != nil {
				return err
			}
			removed++
		}

		// 更新元数据
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

// LMPop 实现 Redis LMPOP 命令，从多个列表中弹出元素
// keys: 要操作的键列表
// modifier: "LEFT" 或 "RIGHT"
// count: 要弹出的元素数量
// 返回第一个非空键的键名和被弹出的元素列表
func (s *BotreonStore) LMPop(keys []string, modifier string, count int) (string, []string, error) {
	for _, key := range keys {
		err := s.checkTypeBeforeOp(key, KeyTypeList)
		if err != nil {
			return "", nil, err
		}
		s.keyLockMgr.Lock(key)
		var results []string
		err = s.retryUpdate(func(txn *badger.Txn) error {
			length, start, end, err := s.listGetMetaTxn(txn, key)
			if err != nil {
				if length == 0 {
					return nil
				}
				return err
			}
			if length == 0 {
				return nil
			}

			currentStart := start
			currentEnd := end
			currentLength := length
			results = make([]string, 0, count)

			for i := 0; i < count && currentLength > 0; i++ {
				var nodeID, nextNodeID string
				if modifier == "LEFT" {
					nodeID = currentStart
					if currentLength > 1 {
						nextKey := s.listKey(key, nodeID, "next")
						item, err := txn.Get([]byte(nextKey))
						if err != nil {
							return err
						}
						nextVal, err := item.ValueCopy(nil)
						if err != nil {
							return err
						}
						nextNodeID = string(nextVal)
					}
				} else {
					nodeID = currentEnd
					if currentLength > 1 {
						prevKey := s.listKey(key, nodeID, "prev")
						item, err := txn.Get([]byte(prevKey))
						if err != nil {
							return err
						}
						prevVal, err := item.ValueCopy(nil)
						if err != nil {
							return err
						}
						nextNodeID = string(prevVal)
					}
				}

				nodeKey := s.listKey(key, nodeID)
				item, err := txn.Get([]byte(nodeKey))
				if err != nil {
					return err
				}
				valueBytes, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				results = append(results, string(valueBytes))

				if err := s.deleteNode(txn, key, nodeID); err != nil {
					return err
				}

				if modifier == "LEFT" {
					currentStart = nextNodeID
					if currentLength > 1 {
						if err := s.linkNodes(txn, key, end, currentStart); err != nil {
							return err
						}
					} else {
						currentStart = ""
						currentEnd = ""
					}
				} else {
					currentEnd = nextNodeID
					if currentLength > 1 {
						if err := s.linkNodes(txn, key, currentEnd, start); err != nil {
							return err
						}
					} else {
						currentStart = ""
						currentEnd = ""
					}
				}
				currentLength--
			}

			return s.listUpdateMeta(txn, key, currentLength, currentStart, currentEnd)
		}, 30)
		s.keyLockMgr.Unlock(key)
		if err != nil {
			return "", nil, err
		}
		if len(results) > 0 {
			return key, results, nil
		}
	}
	return "", nil, nil
}

// RPOPLPUSH 实现 Redis RPOPLPUSH 命令
func (s *BotreonStore) RPopLPush(source, destination string) (string, error) {
	unlock := s.lockListKeysOrdered(source, destination)
	defer unlock()

	var value string
	err := s.retryUpdate(func(txn *badger.Txn) error {
		value = "" // reset each attempt; stale value must not survive conflict retry
		popped, err := s.listPopInTxn(txn, source, false)
		if err != nil {
			return err
		}
		if popped == "" {
			return nil
		}
		value = popped
		return s.listPushInTxn(txn, destination, value, true)
	}, 30)
	return value, err
}

// LPUSHX 实现 Redis LPUSHX 命令，仅当键存在时左推入
func (s *BotreonStore) LPUSHX(key string, values ...string) (int, error) {
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)

	var finalLength uint64
	err := s.retryUpdate(func(txn *badger.Txn) error {
		finalLength = 0
		length, pushErr := s.listPushXInTxn(txn, key, values, true)
		if pushErr != nil {
			return pushErr
		}
		finalLength = length
		return nil
	}, 30)
	if err == nil && finalLength > 0 && len(values) > 0 {
		s.notifyBlockingPop(key, values[0])
	}
	return int(finalLength), err
}

// RPUSHX 实现 Redis RPUSHX 命令，仅当键存在时右推入
func (s *BotreonStore) RPUSHX(key string, values ...string) (int, error) {
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)

	var finalLength uint64
	err := s.retryUpdate(func(txn *badger.Txn) error {
		finalLength = 0
		length, pushErr := s.listPushXInTxn(txn, key, values, false)
		if pushErr != nil {
			return pushErr
		}
		finalLength = length
		return nil
	}, 30)
	if err == nil && finalLength > 0 && len(values) > 0 {
		s.notifyBlockingPop(key, values[len(values)-1])
	}
	return int(finalLength), err
}

// BLPOP 实现 Redis BLPOP 命令，阻塞式左弹出（简化版本：非阻塞）
// 注意：真正的阻塞操作需要额外的机制（如channel或条件变量），这里实现非阻塞版本
func (s *BotreonStore) BLPOP(keys []string, timeout int) (string, string, error) {
	// 简化实现：立即尝试从每个键弹出
	for _, key := range keys {
		value, err := s.LPop(key)
		if err == nil && value != "" {
			return key, value, nil
		}
	}
	// 所有键都为空，返回空结果
	return "", "", nil
}

// BRPOP 实现 Redis BRPOP 命令，阻塞式右弹出（简化版本：非阻塞）
func (s *BotreonStore) BRPOP(keys []string, timeout int) (string, string, error) {
	// 简化实现：立即尝试从每个键弹出
	for _, key := range keys {
		value, err := s.RPop(key)
		if err == nil && value != "" {
			return key, value, nil
		}
	}
	// 所有键都为空，返回空结果
	return "", "", nil
}

// BRPOPLPUSH 实现 Redis BRPOPLPUSH 命令，阻塞式右弹出并左推入（简化版本：非阻塞）
func (s *BotreonStore) BRPOPLPUSH(source, destination string, timeout int) (string, error) {
	// 简化实现：立即尝试执行RPOPLPUSH
	return s.RPopLPush(source, destination)
}

// LMove 实现 Redis LMOVE 命令，原子性地从源列表弹出元素并推入目标列表
// sourceDirection: "LEFT" 或 "RIGHT"
// destinationDirection: "LEFT" 或 "RIGHT"
func (s *BotreonStore) LMove(source, destination, sourceDirection, destinationDirection string) (string, error) {
	var fromLeft, toLeft bool
	switch sourceDirection {
	case "LEFT":
		fromLeft = true
	case "RIGHT":
		fromLeft = false
	default:
		return "", fmt.Errorf("ERR wrong source direction argument")
	}
	switch destinationDirection {
	case "LEFT":
		toLeft = true
	case "RIGHT":
		toLeft = false
	default:
		return "", fmt.Errorf("ERR wrong destination direction argument")
	}

	unlock := s.lockListKeysOrdered(source, destination)
	defer unlock()

	var value string
	err := s.retryUpdate(func(txn *badger.Txn) error {
		value = "" // reset each attempt; stale value must not survive conflict retry
		if err := checkKeyType(txn, source, KeyTypeList); err != nil {
			return err
		}
		popped, err := s.listPopInTxn(txn, source, fromLeft)
		if err != nil {
			return err
		}
		if popped == "" {
			return nil
		}
		value = popped
		return s.listPushInTxn(txn, destination, value, toLeft)
	}, 30)
	if err == nil && value != "" {
		s.notifyBlockingPop(destination, value)
	}
	return value, err
}

// BLMove 实现 Redis BLMOVE 命令，阻塞式LMOVE（简化版本：非阻塞）
func (s *BotreonStore) BLMove(source, destination, sourceDirection, destinationDirection string, timeout float64) (string, error) {
	return s.LMove(source, destination, sourceDirection, destinationDirection)
}

// notifyBlockingPop notifies one waiting channel for a key
func (s *BotreonStore) notifyBlockingPop(key, value string) {
	s.blockingMu.Lock()
	defer s.blockingMu.Unlock()

	chans, exists := s.blockingPopChans[key]
	if !exists || len(chans) == 0 {
		return
	}

	// Notify the first waiting channel
	select {
	case chans[0] <- BlockingResult{Key: key, Value: value}:
		// Remove the notified channel
		s.blockingPopChans[key] = chans[1:]
	default:
		// Channel not ready, keep it
	}
}

// registerBlockingPop registers a channel to wait for a key
func (s *BotreonStore) registerBlockingPop(key string, ch chan BlockingResult) {
	s.blockingMu.Lock()
	defer s.blockingMu.Unlock()

	s.blockingPopChans[key] = append(s.blockingPopChans[key], ch)
}

// unregisterBlockingPop removes a specific channel from all keys' wait lists
func (s *BotreonStore) unregisterBlockingPop(ch chan BlockingResult, keys []string) {
	s.blockingMu.Lock()
	defer s.blockingMu.Unlock()

	for _, key := range keys {
		chans := s.blockingPopChans[key]
		for j, c := range chans {
			if c == ch {
				s.blockingPopChans[key] = append(chans[:j], chans[j+1:]...)
				break
			}
		}
		// Clean up empty map entries to prevent leaks
		if len(s.blockingPopChans[key]) == 0 {
			delete(s.blockingPopChans, key)
		}
	}
}

// registerAndRecheck registers a channel for keys and re-checks for data after registration.
// This closes the TOCTOU race window where a concurrent push's notification
// could arrive between the initial empty-list check and channel registration.
// Returns (key, value, ok) if data was found in the re-check.
func (s *BotreonStore) registerAndRecheck(keys []string, ch chan BlockingResult, popFn func(string) (string, error)) (string, string, bool) {
	s.blockingMu.Lock()
	for _, key := range keys {
		s.blockingPopChans[key] = append(s.blockingPopChans[key], ch)
	}
	s.blockingMu.Unlock()

	// Re-check every key to catch notifications that arrived before registration
	for _, key := range keys {
		value, err := popFn(key)
		if err == nil && value != "" {
			s.unregisterBlockingPop(ch, keys)
			return key, value, true
		}
	}
	return "", "", false
}

// BLPOPBlocking implements blocking left pop with timeout
func (s *BotreonStore) BLPOPBlocking(ctx context.Context, keys []string, timeout int) (string, string, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	// Try non-blocking first
	for _, key := range keys {
		value, err := s.LPop(key)
		if err == nil && value != "" {
			return key, value, nil
		}
	}

	// Create result channel
	resultCh := make(chan BlockingResult, 1)
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

	// Register + re-check to close the TOCTOU race window
	if key, value, ok := s.registerAndRecheck(keys, resultCh, s.LPop); ok {
		return key, value, nil
	}

	// Wait for data, timeout, or context cancellation
	select {
	case result := <-resultCh:
		return result.Key, result.Value, nil
	case <-timerCh:
		s.unregisterBlockingPop(resultCh, keys)
		return "", "", nil
	case <-ctx.Done():
		s.unregisterBlockingPop(resultCh, keys)
		return "", "", nil
	}
}

// BRPOPBlocking implements blocking right pop with timeout
func (s *BotreonStore) BRPOPBlocking(ctx context.Context, keys []string, timeout int) (string, string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	// Try non-blocking first
	for _, key := range keys {
		value, err := s.RPop(key)
		if err == nil && value != "" {
			return key, value, nil
		}
	}

	resultCh := make(chan BlockingResult, 1)
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

	// Register + re-check to close the TOCTOU race window
	if key, value, ok := s.registerAndRecheck(keys, resultCh, s.RPop); ok {
		return key, value, nil
	}

	// Wait for data, timeout, or context cancellation
	select {
	case result := <-resultCh:
		return result.Key, result.Value, nil
	case <-timerCh:
		s.unregisterBlockingPop(resultCh, keys)
		return "", "", nil
	case <-ctx.Done():
		s.unregisterBlockingPop(resultCh, keys)
		return "", "", nil
	}
}

// BRPOPLPUSHBlocking implements blocking rpoplpush with timeout
func (s *BotreonStore) BRPOPLPUSHBlocking(ctx context.Context, source, destination string, timeout int) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if timeout == 0 {
		value, err := s.RPopLPush(source, destination)
		if err != nil || value == "" {
			return "", nil
		}
		return value, nil
	}

	value, err := s.RPopLPush(source, destination)
	if err == nil && value != "" {
		return value, nil
	}

	resultCh := make(chan BlockingResult, 1)
	keys := []string{source}

	// Register + re-check to close the TOCTOU race window
	s.blockingMu.Lock()
	s.blockingPopChans[source] = append(s.blockingPopChans[source], resultCh)
	s.blockingMu.Unlock()

	value, err = s.RPopLPush(source, destination)
	if err == nil && value != "" {
		s.unregisterBlockingPop(resultCh, keys)
		return value, nil
	}

	timeoutDur := time.Duration(timeout) * time.Second
	timer := time.NewTimer(timeoutDur)
	defer func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}()

	for {
		select {
		case <-resultCh:
			value, err := s.RPopLPush(source, destination)
			if err == nil && value != "" {
				return value, nil
			}
			s.registerBlockingPop(source, resultCh)
		case <-timer.C:
			s.unregisterBlockingPop(resultCh, keys)
			return "", nil
		case <-ctx.Done():
			s.unregisterBlockingPop(resultCh, keys)
			return "", nil
		}
	}
}

// BLMoveBlocking implements blocking lmove with timeout
func (s *BotreonStore) BLMoveBlocking(ctx context.Context, source, destination, sourceDirection, destinationDirection string, timeout float64) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if timeout == 0 {
		return s.LMove(source, destination, sourceDirection, destinationDirection)
	}

	value, err := s.LMove(source, destination, sourceDirection, destinationDirection)
	if err == nil && value != "" {
		return value, nil
	}

	resultCh := make(chan BlockingResult, 1)
	keys := []string{source}

	// Register + re-check to close the TOCTOU race window
	s.blockingMu.Lock()
	s.blockingPopChans[source] = append(s.blockingPopChans[source], resultCh)
	s.blockingMu.Unlock()

	value, err = s.LMove(source, destination, sourceDirection, destinationDirection)
	if err == nil && value != "" {
		s.unregisterBlockingPop(resultCh, keys)
		return value, nil
	}

	timeoutDur := time.Duration(timeout) * time.Second
	timer := time.NewTimer(timeoutDur)
	defer func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}()

	for {
		select {
		case <-resultCh:
			value, err := s.LMove(source, destination, sourceDirection, destinationDirection)
			if err == nil && value != "" {
				return value, nil
			}
			s.registerBlockingPop(source, resultCh)
		case <-timer.C:
			s.unregisterBlockingPop(resultCh, keys)
			return "", nil
		case <-ctx.Done():
			s.unregisterBlockingPop(resultCh, keys)
			return "", nil
		}
	}
}
