package store

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
	"github.com/lbp0200/BoltDB/internal/helper"
)

// ListNode 结构体定义了链表节点的结构
type ListNode struct {
	ID    string
	Value []byte
	Prev  string
	Next  string
}

// listKey 生成存储在 Badger 中的键
func (s *BotreonStore) listKey(key string, parts ...string) string {
	return fmt.Sprintf("%s:%s:%s", KeyTypeList, key, strings.Join(parts, ":"))
}

// listLength 获取链表的长度
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
		lengthItem, err := txn.Get([]byte(s.listKey(key, "length")))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				length = 0
				return nil
			}
			return err
		}
		lengthVal, err := lengthItem.ValueCopy(nil)
		if err != nil {
			return err
		}
		length = helper.BytesToUint64(lengthVal)
		return nil
	})
	return length, errView
}

func (s *BotreonStore) listGetMetaTxn(txn *badger.Txn, keyRedis string) (length uint64, start, end string, err error) {
	lengthItem, errGet := txn.Get([]byte(s.listKey(keyRedis, "length")))
	if errGet != nil {
		if errors.Is(errGet, badger.ErrKeyNotFound) {
			length = 0
			start = ""
			end = ""
			return
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
	if err := txn.Set([]byte(s.listKey(key, "length")), helper.Uint64ToBytes(length)); err != nil {
		return err
	}
	if err := txn.Set([]byte(s.listKey(key, "start")), []byte(start)); err != nil {
		return err
	}
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
	if prevID != "" {
		prevNextKey := s.listKey(key, prevID, "next")
		if err := txn.Set([]byte(prevNextKey), []byte(nextID)); err != nil {
			return err
		}
	}
	if nextID != "" {
		nextPrevKey := s.listKey(key, nextID, "prev")
		return txn.Set([]byte(nextPrevKey), []byte(prevID))
	}
	return nil
}

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
	for _, metaKey := range []string{"length", "start", "end"} {
		if err := txn.Delete([]byte(s.listKey(key, metaKey))); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
	}
	return txn.Delete(TypeOfKeyGet(key))
}
