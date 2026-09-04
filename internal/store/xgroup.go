package store

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// XGroupCreate creates a consumer group
func (s *BotreonStore) XGroupCreate(key, group, startID string) error {
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)
	return s.retryUpdate(func(txn *badger.Txn) error {
		metaKey := streamKey(key)
		typeKey := TypeOfKeyGet(key)

		typeItem, err := txn.Get(typeKey)
		if err == nil {
			typeVal, copyErr := typeItem.ValueCopy(nil)
			if copyErr != nil {
				return copyErr
			}
			if string(typeVal) != "" && string(typeVal) != KeyTypeStream {
				return ErrWrongType
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}

		_, err = txn.Get(metaKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			if err := txn.Set(typeKey, []byte(KeyTypeStream)); err != nil {
				return err
			}
			if err := txn.Set(metaKey, encodeStreamMeta(&streamMetaData{})); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}

		groupKey := streamGroupDataKey(key, group)
		groupData := &StreamGroup{
			Name:            group,
			LastDeliveredID: startID,
			Consumers:       make(map[string]*StreamConsumer),
			Pending:         make(map[string]*StreamPendingEntry),
		}
		data, err := json.Marshal(groupData)
		if err != nil {
			return err
		}
		return txn.Set(groupKey, data)
	}, 30, encodePropagateCommand([]byte("XGROUP"), []byte(key), []byte("CREATE"), []byte(group), []byte(startID)))
}

// XGroupCreateConsumer explicitly creates a consumer in a group.
// Redis XGROUP CREATECONSUMER 语义：返回 1 = 消费者新建，0 = 已存在。
func (s *BotreonStore) XGroupCreateConsumer(key, group, consumer string) (int64, error) {
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)
	var created int64
	err := s.retryUpdate(func(txn *badger.Txn) error {
		created = 0
		groupKey := streamGroupDataKey(key, group)
		item, err := txn.Get(groupKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil // 组不存在：不创建（Redis 返回 0）
		}
		if err != nil {
			return err
		}

		var groupData *StreamGroup
		if err := item.Value(func(val []byte) error {
			return json.Unmarshal(val, &groupData)
		}); err != nil {
			return err
		}

		if groupData.Consumers == nil {
			groupData.Consumers = make(map[string]*StreamConsumer)
		}
		if _, exists := groupData.Consumers[consumer]; !exists {
			groupData.Consumers[consumer] = &StreamConsumer{
				Name:     consumer,
				LastSeen: time.Now().UnixMilli(),
			}
			created = 1
		}

		data, err := json.Marshal(groupData)
		if err != nil {
			return err
		}
		return txn.Set(groupKey, data)
	}, 30, encodePropagateCommand([]byte("XGROUP"), []byte(key), []byte("CREATECONSUMER"), []byte(group), []byte(consumer)))
	return created, err
}

// XGroupDelConsumer removes a consumer from a group
func (s *BotreonStore) XGroupDelConsumer(key, group, consumer string) (int64, error) {
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)
	var removed int64
	err := s.retryUpdate(func(txn *badger.Txn) error {
		removed = 0
		groupKey := streamGroupDataKey(key, group)
		item, err := txn.Get(groupKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}

		var groupData *StreamGroup
		if err := item.Value(func(val []byte) error {
			return json.Unmarshal(val, &groupData)
		}); err != nil {
			return err
		}

		if groupData.Pending != nil {
			for id, p := range groupData.Pending {
				if p.Consumer == consumer {
					delete(groupData.Pending, id)
					removed++
				}
			}
		}

		delete(groupData.Consumers, consumer)
		data, err := json.Marshal(groupData)
		if err != nil {
			return err
		}
		return txn.Set(groupKey, data)
	}, 30, encodePropagateCommand([]byte("XGROUP"), []byte(key), []byte("DELCONSUMER"), []byte(group), []byte(consumer)))
	return removed, err
}

// XGroupDestroy destroys a consumer group
func (s *BotreonStore) XGroupDestroy(key, group string) error {
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)
	return s.retryUpdate(func(txn *badger.Txn) error {
		groupKey := streamGroupDataKey(key, group)
		return txn.Delete(groupKey)
	}, 30, encodePropagateCommand([]byte("XGROUP"), []byte(key), []byte("DESTROY"), []byte(group)))
}

// XGroupSetID sets the last delivered ID for a group
func (s *BotreonStore) XGroupSetID(key, group, id string) error {
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)
	return s.retryUpdate(func(txn *badger.Txn) error {
		groupKey := streamGroupDataKey(key, group)
		item, err := txn.Get(groupKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}

		var groupData *StreamGroup
		if err := item.Value(func(val []byte) error {
			return json.Unmarshal(val, &groupData)
		}); err != nil {
			return err
		}

		groupData.LastDeliveredID = id
		data, err := json.Marshal(groupData)
		if err != nil {
			return err
		}
		return txn.Set(groupKey, data)
	}, 30)
}

// XGroupRestore restores a full consumer group (including consumers + PEL)
// for RDB FULLRESYNC. Overwrites any existing group with the same name.
func (s *BotreonStore) XGroupRestore(key string, group *StreamGroup) error {
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)
	if group == nil || group.Name == "" {
		return nil
	}
	return s.retryUpdate(func(txn *badger.Txn) error {
		metaKey := streamKey(key)
		typeKey := TypeOfKeyGet(key)

		typeItem, err := txn.Get(typeKey)
		if err == nil {
			typeVal, copyErr := typeItem.ValueCopy(nil)
			if copyErr != nil {
				return copyErr
			}
			if string(typeVal) != "" && string(typeVal) != KeyTypeStream {
				return ErrWrongType
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}

		if _, err = txn.Get(metaKey); errors.Is(err, badger.ErrKeyNotFound) {
			if err := txn.Set(typeKey, []byte(KeyTypeStream)); err != nil {
				return err
			}
			if err := txn.Set(metaKey, encodeStreamMeta(&streamMetaData{})); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}

		if group.Consumers == nil {
			group.Consumers = make(map[string]*StreamConsumer)
		}
		if group.Pending == nil {
			group.Pending = make(map[string]*StreamPendingEntry)
		}
		data, err := json.Marshal(group)
		if err != nil {
			return err
		}
		return txn.Set(streamGroupDataKey(key, group.Name), data)
	}, 30)
}
