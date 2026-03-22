package store

import (
	"errors"
	"fmt"
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
		lengthVal, _ := lengthItem.ValueCopy(nil)
		// 将字节切片形式的长度值转换为无符号 64 位整数
		length = helper.BytesToUint64(lengthVal)
		return nil
	})
	return length, errView
}

func (s *BotreonStore) listGetMeta(keyRedis string) (length uint64, start, end string, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		// 获取长度
		lengthItem, errGet := txn.Get([]byte(s.listKey(keyRedis, "length")))
		if errGet != nil {
			if errors.Is(errGet, badger.ErrKeyNotFound) {
				length = 0
				start = ""
				end = ""
				return nil // 列表不存在，返回默认值
			}
			return errGet
		}
		lengthVal, errValueCopy := lengthItem.ValueCopy(nil)
		if errValueCopy != nil {
			return errValueCopy
		}
		length = helper.BytesToUint64(lengthVal)

		// 获取起始节点
		startItem, errStart := txn.Get([]byte(s.listKey(keyRedis, "start")))
		if errStart == nil {
			startVal, _ := startItem.ValueCopy(nil)
			start = string(startVal)
		}

		// 获取结束节点
		endItem, errEnd := txn.Get([]byte(s.listKey(keyRedis, "end")))
		if errEnd == nil {
			endVal, _ := endItem.ValueCopy(nil)
			end = string(endVal)
		}
		return nil
	})
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
	err := s.db.Update(func(txn *badger.Txn) error {
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
		length, start, end, _ := s.listGetMeta(key)
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
	})

	// Notify blocking pop waiters
	s.notifyBlockingPop(key, values[0])

	return int(finalLength), err // 返回操作后列表的长度（Redis规范）
}

// RPOP 实现
func (s *BotreonStore) RPop(key string) (string, error) {
	var value string
	err := s.db.Update(func(txn *badger.Txn) error {
		length, start, end, err := s.listGetMeta(key)
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
		valueBytes, _ := item.ValueCopy(nil)
		value = string(valueBytes)

		// 获取新的尾节点
		var newEnd string
		if length > 1 {
			newEndKey := s.listKey(key, end, "prev")
			item, err = txn.Get([]byte(newEndKey))
			if err != nil {
				return err
			}
			newEndVal, _ := item.ValueCopy(nil)
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
		_ = txn.Delete([]byte(s.listKey(key, end, "prev")))
		_ = txn.Delete([]byte(s.listKey(key, end, "next")))

		// 更新元数据
		return s.listUpdateMeta(txn, key, length-1, start, newEnd)
	})
	return value, err
}

// LLEN 实现
func (s *BotreonStore) LLen(key string) (uint64, error) {
	length, err := s.listLength(key)
	return length, err
}

// getNodeByIndex 根据索引获取节点ID和值
func (s *BotreonStore) getNodeByIndex(txn *badger.Txn, key string, index int64) (string, string, error) {
	length, start, _, err := s.listGetMeta(key)
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
			nextVal, _ := item.ValueCopy(nil)
			currentNodeID = string(nextVal)
			currentIndex++
		}
	} else {
		// 从尾部开始
		_, _, end, _ := s.listGetMeta(key)
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
			prevVal, _ := item.ValueCopy(nil)
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
	valueBytes, _ := item.ValueCopy(nil)
	return currentNodeID, string(valueBytes), nil
}

// RPUSH 实现 Redis RPUSH 命令
func (s *BotreonStore) RPush(key string, values ...string) (int, error) {
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)

	var finalLength uint64
	err := s.db.Update(func(txn *badger.Txn) error {
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
		length, start, end, _ := s.listGetMeta(key)
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
	})
	// #nosec G115 - length is bounded by practical list size limits

	// Notify blocking pop waiters
	s.notifyBlockingPop(key, values[0])

	return int(finalLength), err // 返回操作后列表的长度（Redis规范）
}

// LPOP 实现 Redis LPOP 命令
func (s *BotreonStore) LPop(key string) (string, error) {
	var value string
	err := s.db.Update(func(txn *badger.Txn) error {
		length, start, end, err := s.listGetMeta(key)
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
		valueBytes, _ := item.ValueCopy(nil)
		value = string(valueBytes)

		// 获取新的头节点
		var newStart string
		if length > 1 {
			newStartKey := s.listKey(key, start, "next")
			item, err = txn.Get([]byte(newStartKey))
			if err != nil {
				return err
			}
			newStartVal, _ := item.ValueCopy(nil)
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
		_ = txn.Delete([]byte(s.listKey(key, start, "prev")))
		_ = txn.Delete([]byte(s.listKey(key, start, "next")))

		// 更新元数据
		return s.listUpdateMeta(txn, key, length-1, newStart, end)
	})
	return value, err
}

// LINDEX 实现 Redis LINDEX 命令
func (s *BotreonStore) LIndex(key string, index int64) (string, error) {
	var value string
	err := s.db.View(func(txn *badger.Txn) error {
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
		length, startID, _, err := s.listGetMeta(key)
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
			nextVal, _ := item.ValueCopy(nil)
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
			valueBytes, _ := item.ValueCopy(nil)
			result = append(result, string(valueBytes))

			if currentIndex < stop {
				nextKey := s.listKey(key, currentNodeID, "next")
				item, err = txn.Get([]byte(nextKey))
				if err != nil {
					return err
				}
				nextVal, _ := item.ValueCopy(nil)
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
	return s.db.Update(func(txn *badger.Txn) error {
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
	})
}

// LPos 实现 Redis LPOS 命令，返回元素在列表中的索引
// element: 要查找的元素
// rank: 正数从头部开始，负数从尾部开始
// count: 返回多个匹配位置，0 表示返回所有
// maxlen: 最大扫描长度，0 表示扫描整个列表
func (s *BotreonStore) LPos(key string, element string, rank, count, maxlen int64) ([]int64, error) {
	var results []int64

	err := s.db.View(func(txn *badger.Txn) error {
		length, _, _, err := s.listGetMeta(key)
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
	return s.db.Update(func(txn *badger.Txn) error {
		length, startID, _, err := s.listGetMeta(key)
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
			nextVal, _ := item.ValueCopy(nil)
			oldNodeID := currentNodeID
			currentNodeID = string(nextVal)
			// 删除旧节点
			s.deleteNode(txn, key, oldNodeID)
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
			nextVal, _ := item.ValueCopy(nil)
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
			nextVal, _ := item.ValueCopy(nil)
			nextID := string(nextVal)
			if nextID == newStartID {
				break // 循环回到起始节点
			}
			if visitedDelete[nextID] {
				break // 防止无限循环
			}
			visitedDelete[nextID] = true
			s.deleteNode(txn, key, nextID)
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
	})
}

// deleteNode 删除一个节点
func (s *BotreonStore) deleteNode(txn *badger.Txn, key, nodeID string) {
	_ = txn.Delete([]byte(s.listKey(key, nodeID)))
	_ = txn.Delete([]byte(s.listKey(key, nodeID, "prev")))
	_ = txn.Delete([]byte(s.listKey(key, nodeID, "next")))
}

// deleteList 删除整个列表
func (s *BotreonStore) deleteList(txn *badger.Txn, key string) error {
	_, start, _, _ := s.listGetMeta(key)
	if start != "" {
		// 遍历删除所有节点
		currentNodeID := start
		visited := make(map[string]bool)
		for {
			if visited[currentNodeID] {
				break
			}
			visited[currentNodeID] = true
			s.deleteNode(txn, key, currentNodeID)
			nextKey := s.listKey(key, currentNodeID, "next")
			item, err := txn.Get([]byte(nextKey))
			if err != nil {
				break
			}
			nextVal, _ := item.ValueCopy(nil)
			currentNodeID = string(nextVal)
			if currentNodeID == start {
				break
			}
		}
	}
	// 删除元数据
	_ = txn.Delete([]byte(s.listKey(key, "length")))
	_ = txn.Delete([]byte(s.listKey(key, "start")))
	_ = txn.Delete([]byte(s.listKey(key, "end")))
	return nil
}

// LINSERT 实现 Redis LINSERT 命令
func (s *BotreonStore) LInsert(key string, where string, pivot, value string) (int, error) {
	count := 0
	err := s.db.Update(func(txn *badger.Txn) error {
		length, start, _, err := s.listGetMeta(key)
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
			valueBytes, _ := item.ValueCopy(nil)
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
			nextVal, _ := item.ValueCopy(nil)
			currentNodeID = string(nextVal)
		}

		if pivotNodeID == "" {
			return nil // pivot不存在
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
				prevVal, _ := item.ValueCopy(nil)
				prevNodeID = string(prevVal)
			} else if errors.Is(err, badger.ErrKeyNotFound) {
				// 如果prev不存在，说明pivot是第一个节点，prevNodeID应该是end（循环链表）
				_, _, end, _ := s.listGetMeta(key)
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
			_, _, end, _ := s.listGetMeta(key)
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
			nextVal, _ := item.ValueCopy(nil)
			nextNodeID := string(nextVal)

			// 更新链接
			if err := s.linkNodes(txn, key, pivotNodeID, newNodeID); err != nil {
				return err
			}
			if err := s.linkNodes(txn, key, newNodeID, nextNodeID); err != nil {
				return err
			}

			// 如果插入在尾部，更新end
			_, _, end, err := s.listGetMeta(key)
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
		count = 1
		return nil
	})
	return count, err
}

// LREM 实现 Redis LREM 命令
func (s *BotreonStore) LRem(key string, count int64, value string) (int, error) {
	removed := 0
	err := s.db.Update(func(txn *badger.Txn) error {
		length, start, _, err := s.listGetMeta(key)
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
			valueBytes, _ := item.ValueCopy(nil)
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
			nextVal, _ := item.ValueCopy(nil)
			currentNodeID = string(nextVal)
			visitedCount++
		}

		// 如果count < 0，从尾部开始
		if count < 0 {
			_, _, end, _ := s.listGetMeta(key)
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
				valueBytes, _ := item.ValueCopy(nil)
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
				prevVal, _ := item.ValueCopy(nil)
				currentNodeID = string(prevVal)
				visitedCount++
			}
		}

		// 获取当前start和end
		_, currentStart, currentEnd, _ := s.listGetMeta(key)

		// 删除节点
		for _, nodeID := range nodesToRemove {
			// 获取前后节点
			prevKey := s.listKey(key, nodeID, "prev")
			nextKey := s.listKey(key, nodeID, "next")
			prevItem, _ := txn.Get([]byte(prevKey))
			nextItem, _ := txn.Get([]byte(nextKey))
			var prevID, nextID string
			if prevItem != nil {
				prevVal, _ := prevItem.ValueCopy(nil)
				prevID = string(prevVal)
			}
			if nextItem != nil {
				nextVal, _ := nextItem.ValueCopy(nil)
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
			s.deleteNode(txn, key, nodeID)
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
	})
	return removed, err
}

// RPOPLPUSH 实现 Redis RPOPLPUSH 命令
func (s *BotreonStore) RPopLPush(source, destination string) (string, error) {
	var value string
	err := s.db.Update(func(txn *badger.Txn) error {
		// 从源列表弹出
		sourceLength, sourceStart, sourceEnd, err := s.listGetMeta(source)
		if err != nil || sourceLength == 0 {
			return nil // 源列表不存在或为空
		}

		// 获取源列表尾节点值
		endNodeKey := s.listKey(source, sourceEnd)
		item, err := txn.Get([]byte(endNodeKey))
		if err != nil {
			return err
		}
		valueBytes, _ := item.ValueCopy(nil)
		value = string(valueBytes)

		// 获取新的源列表尾节点
		var newSourceEnd string
		if sourceLength > 1 {
			newEndKey := s.listKey(source, sourceEnd, "prev")
			item, err = txn.Get([]byte(newEndKey))
			if err != nil {
				return err
			}
			newEndVal, _ := item.ValueCopy(nil)
			newSourceEnd = string(newEndVal)
		} else {
			newSourceEnd = ""
			sourceStart = ""
		}

		// 更新源列表
		if sourceLength > 1 {
			if err := s.linkNodes(txn, source, newSourceEnd, sourceStart); err != nil {
				return err
			}
		}
		s.deleteNode(txn, source, sourceEnd)
		if err := s.listUpdateMeta(txn, source, sourceLength-1, sourceStart, newSourceEnd); err != nil {
			return err
		}

		// 推入目标列表头部
		// Check if destination key already exists with a different type
		item, err = txn.Get(TypeOfKeyGet(destination))
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
		if err := txn.Set(TypeOfKeyGet(destination), []byte(KeyTypeList)); err != nil {
			return err
		}
		destLength, destStart, destEnd, _ := s.listGetMeta(destination)

		// 创建新节点
		newNodeID, err := s.createNode(txn, destination, []byte(value))
		if err != nil {
			return err
		}

		// 链接节点
		if destLength == 0 {
			destStart = newNodeID
			destEnd = newNodeID
			if err := s.linkNodes(txn, destination, newNodeID, newNodeID); err != nil {
				return err
			}
		} else {
			if err := s.linkNodes(txn, destination, newNodeID, destStart); err != nil {
				return err
			}
			if err := txn.Set([]byte(s.listKey(destination, destStart, "prev")), []byte(newNodeID)); err != nil {
				return err
			}
			destStart = newNodeID
		}

		// 更新目标列表元数据
		return s.listUpdateMeta(txn, destination, destLength+1, destStart, destEnd)
	})
	return value, err
}

// LPUSHX 实现 Redis LPUSHX 命令，仅当键存在时左推入
func (s *BotreonStore) LPUSHX(key string, values ...string) (int, error) {
	// 先检查键是否存在且是List类型（在 View 事务中）
	var isList bool
	err := s.db.View(func(txn *badger.Txn) error {
		typeKey := TypeOfKeyGet(key)
		item, err := txn.Get(typeKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil // 键不存在
		}
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		keyType := string(val)
		if keyType != KeyTypeList {
			return nil // 不是List类型
		}
		isList = true
		return nil
	})
	if err != nil {
		return 0, err
	}
	if !isList {
		return 0, nil
	}

	// 然后执行LPUSH（在 Update 事务中）
	return s.LPush(key, values...)
}

// RPUSHX 实现 Redis RPUSHX 命令，仅当键存在时右推入
func (s *BotreonStore) RPUSHX(key string, values ...string) (int, error) {
	// 先检查键是否存在且是List类型（在 View 事务中）
	var isList bool
	err := s.db.View(func(txn *badger.Txn) error {
		typeKey := TypeOfKeyGet(key)
		item, err := txn.Get(typeKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil // 键不存在
		}
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		keyType := string(val)
		if keyType != KeyTypeList {
			return nil // 不是List类型
		}
		isList = true
		return nil
	})
	if err != nil {
		return 0, err
	}
	if !isList {
		return 0, nil
	}

	// 然后执行RPUSH（在 Update 事务中）
	return s.RPush(key, values...)
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
	var value string
	var err error

	switch sourceDirection {
	case "LEFT":
		value, err = s.LPop(source)
	case "RIGHT":
		value, err = s.RPop(source)
	default:
		return "", fmt.Errorf("ERR wrong source direction argument")
	}

	if err != nil || value == "" {
		return "", err
	}

	switch destinationDirection {
	case "LEFT":
		_, err = s.LPush(destination, value)
	case "RIGHT":
		_, err = s.RPush(destination, value)
	default:
		return "", fmt.Errorf("ERR wrong destination direction argument")
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

// BLPOPBlocking implements blocking left pop with timeout
func (s *BotreonStore) BLPOPBlocking(keys []string, timeout int) (string, string, error) {
	// Try non-blocking first
	for _, key := range keys {
		value, err := s.LPop(key)
		if err == nil && value != "" {
			return key, value, nil
		}
	}

	// If timeout is 0, return immediately
	if timeout == 0 {
		return "", "", nil
	}

	// Create result channel
	resultCh := make(chan BlockingResult, 1)
	timeoutCh := time.After(time.Duration(timeout) * time.Second)

	// Register this channel for each key
	s.blockingMu.Lock()
	for _, key := range keys {
		s.blockingPopChans[key] = append(s.blockingPopChans[key], resultCh)
	}
	s.blockingMu.Unlock()

	// Wait for data or timeout
	select {
	case result := <-resultCh:
		return result.Key, result.Value, nil
	case <-timeoutCh:
		return "", "", nil
	}
}

// BRPOPBlocking implements blocking right pop with timeout
func (s *BotreonStore) BRPOPBlocking(keys []string, timeout int) (string, string, error) {
	// Try non-blocking first
	for _, key := range keys {
		value, err := s.RPop(key)
		if err == nil && value != "" {
			return key, value, nil
		}
	}

	// If timeout is 0, return immediately
	if timeout == 0 {
		return "", "", nil
	}

	// Create result channel
	resultCh := make(chan BlockingResult, 1)
	timeoutCh := time.After(time.Duration(timeout) * time.Second)

	// Register this channel for each key
	s.blockingMu.Lock()
	for _, key := range keys {
		s.blockingPopChans[key] = append(s.blockingPopChans[key], resultCh)
	}
	s.blockingMu.Unlock()

	// Wait for data or timeout
	select {
	case result := <-resultCh:
		return result.Key, result.Value, nil
	case <-timeoutCh:
		return "", "", nil
	}
}

// BRPOPLPUSHBlocking implements blocking rpoplpush with timeout
func (s *BotreonStore) BRPOPLPUSHBlocking(source, destination string, timeout int) (string, error) {
	if timeout == 0 {
		value, err := s.RPopLPush(source, destination)
		if err != nil || value == "" {
			return "", nil
		}
		return value, nil
	}

	timeoutCh := time.After(time.Duration(timeout) * time.Second)

	for {
		value, err := s.RPopLPush(source, destination)
		if err == nil && value != "" {
			return value, nil
		}

		select {
		case <-timeoutCh:
			return "", nil
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// BLMoveBlocking implements blocking lmove with timeout
func (s *BotreonStore) BLMoveBlocking(source, destination, sourceDirection, destinationDirection string, timeout float64) (string, error) {
	if timeout == 0 {
		return s.LMove(source, destination, sourceDirection, destinationDirection)
	}

	timeoutCh := time.After(time.Duration(timeout) * time.Second)

	for {
		value, err := s.LMove(source, destination, sourceDirection, destinationDirection)
		if err == nil && value != "" {
			return value, nil
		}

		select {
		case <-timeoutCh:
			return "", nil
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}
