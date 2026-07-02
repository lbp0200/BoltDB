package store

import (
	"encoding/json"
	"errors"

	"github.com/dgraph-io/badger/v4"
)

// XGroupCreate creates a consumer group
func (s *BotreonStore) XGroupCreate(key, group, startID string) error {
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
	}, 30)
}

// XGroupDelConsumer removes a consumer from a group
func (s *BotreonStore) XGroupDelConsumer(key, group, consumer string) (int64, error) {
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
	}, 30)
	return removed, err
}

// XGroupDestroy destroys a consumer group
func (s *BotreonStore) XGroupDestroy(key, group string) error {
	return s.retryUpdate(func(txn *badger.Txn) error {
		groupKey := streamGroupDataKey(key, group)
		return txn.Delete(groupKey)
	}, 30)
}

// XGroupSetID sets the last delivered ID for a group
func (s *BotreonStore) XGroupSetID(key, group, id string) error {
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
