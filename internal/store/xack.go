package store

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// XAck acknowledges messages in a stream
func (s *BotreonStore) XAck(key, group string, ids ...string) (int64, error) {
	var acknowledged int64

	err := s.retryUpdate(func(txn *badger.Txn) error {
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

		for _, id := range ids {
			if _, exists := groupData.Pending[id]; exists {
				delete(groupData.Pending, id)
				acknowledged++
			}
		}

		data, err := json.Marshal(groupData)
		if err != nil {
			return err
		}
		return txn.Set(groupKey, data)
	}, 30)

	return acknowledged, err
}

// XAckDelRemoveRefs removes PEL references for an entry from ALL consumer groups
func (s *BotreonStore) XAckDelRemoveRefs(key, id string) error {
	return s.retryUpdate(func(txn *badger.Txn) error {
		prefix := []byte(prefixStream + key + streamGroups + ":")
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		var groupKeys [][]byte
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			keyCopy := make([]byte, len(item.Key()))
			copy(keyCopy, item.Key())
			groupKeys = append(groupKeys, keyCopy)
		}

		for _, gk := range groupKeys {
			item, err := txn.Get(gk)
			if err != nil {
				continue
			}
			var groupData *StreamGroup
			if err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &groupData)
			}); err != nil {
				continue
			}
			if _, exists := groupData.Pending[id]; exists {
				delete(groupData.Pending, id)
				data, err := json.Marshal(groupData)
				if err != nil {
					return err
				}
				if err := txn.Set(gk, data); err != nil {
					return err
				}
			}
		}
		return nil
	}, 30)
}

// XIsAckedByAllGroups checks if an entry has been acknowledged by ALL consumer groups
func (s *BotreonStore) XIsAckedByAllGroups(key, id string) (bool, error) {
	var allAcked bool
	err := s.db.View(func(txn *badger.Txn) error {
		dataKey := streamDataKey(key, id)
		_, err := txn.Get(dataKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			allAcked = true
			return nil
		}
		if err != nil {
			return err
		}

		prefix := []byte(prefixStream + key + streamGroups + ":")
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		hasGroups := false
		allAcked = true
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			hasGroups = true
			item := it.Item()
			var groupData *StreamGroup
			if err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &groupData)
			}); err != nil {
				continue
			}
			if _, exists := groupData.Pending[id]; exists {
				allAcked = false
				return nil
			}
		}
		if !hasGroups {
			allAcked = true
		}
		return nil
	})
	return allAcked, err
}

// XNack releases pending messages back to the group's PEL
func (s *BotreonStore) XNack(key, group, consumer string, ids ...string) (int64, error) {
	var released int64

	err := s.retryUpdate(func(txn *badger.Txn) error {
		released = 0
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

		for _, id := range ids {
			if entry, exists := groupData.Pending[id]; exists {
				entry.DeliveryCount = 0
				entry.Consumer = ""
				entry.LastDelivery = 0
				groupData.Pending[id] = entry
				released++
			}
		}

		data, err := json.Marshal(groupData)
		if err != nil {
			return err
		}
		return txn.Set(groupKey, data)
	}, 30)

	return released, err
}

// XPending returns pending information for a group
func (s *BotreonStore) XPending(key, group string) ([]StreamPendingEntry, error) {
	var pending []StreamPendingEntry

	err := s.db.View(func(txn *badger.Txn) error {
		if err := checkKeyType(txn, key, KeyTypeStream); err != nil {
			return err
		}
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

		for _, p := range groupData.Pending {
			pending = append(pending, *p)
		}
		return nil
	})

	return pending, err
}

// XClaim claims pending messages
func (s *BotreonStore) XClaim(key, group, consumer string, minIdleTime int64, ids ...string) ([]string, error) {
	var claimed []string

	err := s.retryUpdate(func(txn *badger.Txn) error {
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

		now := time.Now().UnixNano() / int64(time.Millisecond)

		for _, id := range ids {
			if p, exists := groupData.Pending[id]; exists {
				if minIdleTime > 0 {
					idleTime := now - p.LastDelivery
					if idleTime < minIdleTime {
						continue
					}
				}
				p.Consumer = consumer
				p.LastDelivery = now
				p.DeliveryCount++
				claimed = append(claimed, id)
			}
		}

		data, err := json.Marshal(groupData)
		if err != nil {
			return err
		}
		return txn.Set(groupKey, data)
	}, 30)

	return claimed, err
}
