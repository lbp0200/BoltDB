package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// XInfo returns stream information
func (s *BotreonStore) XInfo(key string) (*StreamInfo, error) {
	var info StreamInfo

	err := s.db.View(func(txn *badger.Txn) error {
		if err := checkKeyType(txn, key, KeyTypeStream); err != nil {
			return err
		}
		metaKey := streamKey(key)
		item, err := txn.Get(metaKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}

		var meta *streamMetaData
		if err := item.Value(func(val []byte) error {
			meta, err = decodeStreamMeta(val)
			return err
		}); err != nil {
			return err
		}

		info.Length = meta.Length
		info.FirstID = formatStreamID(meta.FirstID, meta.FirstSeq)
		info.LastID = formatStreamID(meta.LastID, meta.LastSeq)
		info.MaxDeletedID = formatStreamID(meta.MaxDeletedID, meta.MaxDelSeq)

		groups := make(map[string]*StreamGroup)
		groupsPrefix := streamGroupKey(key)
		opts := badger.DefaultIteratorOptions
		opts.Prefix = groupsPrefix
		groupIt := txn.NewIterator(opts)
		defer groupIt.Close()

		for groupIt.Seek(groupsPrefix); groupIt.ValidForPrefix(groupsPrefix); groupIt.Next() {
			groupName := string(bytes.TrimPrefix(groupIt.Item().Key(), groupsPrefix))
			groups[groupName] = &StreamGroup{
				Name:      groupName,
				Consumers: make(map[string]*StreamConsumer),
				Pending:   make(map[string]*StreamPendingEntry),
			}
		}
		info.Groups = groups

		return nil
	})

	return &info, err
}

// XInfoGroups returns information about consumer groups
func (s *BotreonStore) XInfoGroups(key string) ([]*StreamGroup, error) {
	var groups []*StreamGroup

	err := s.db.View(func(txn *badger.Txn) error {
		if err := checkKeyType(txn, key, KeyTypeStream); err != nil {
			return err
		}
		prefix := streamGroupKey(key)
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			var groupData StreamGroup
			if err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &groupData)
			}); err != nil {
				return err
			}
			groups = append(groups, &groupData)
		}
		return nil
	})

	return groups, err
}

// XInfoConsumers returns information about consumers in a group
func (s *BotreonStore) XInfoConsumers(key, group string) ([]*StreamConsumer, error) {
	var consumers []*StreamConsumer

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

		for _, c := range groupData.Consumers {
			consumers = append(consumers, c)
		}
		return nil
	})

	return consumers, err
}

// GetStreamEntry retrieves a specific entry from a stream
func (s *BotreonStore) GetStreamEntry(key, id string) (*StreamEntry, error) {
	var entry *StreamEntry

	err := s.db.View(func(txn *badger.Txn) error {
		if err := checkKeyType(txn, key, KeyTypeStream); err != nil {
			return err
		}
		dataKey := streamDataKey(key, id)
		item, err := txn.Get(dataKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return fmt.Errorf("ERR no such entry")
		}
		if err != nil {
			return err
		}

		var fields map[string]string
		if err := item.Value(func(val []byte) error {
			return json.Unmarshal(val, &fields)
		}); err != nil {
			return err
		}

		ts, seq, _ := parseStreamID(id)
		entry = &StreamEntry{
			ID:        id,
			Fields:    fields,
			Timestamp: ts,
			Sequence:  seq,
		}
		return nil
	})

	return entry, err
}
