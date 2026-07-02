package store

import (
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// MemoryUsage estimates the memory usage of a key in bytes
// This is an approximation since BadgerDB manages memory internally
func (s *BotreonStore) MemoryUsage(key string) (int64, error) {
	var size int64

	err := s.db.View(func(txn *badger.Txn) error {
		// Get the type key first
		typeKey := TypeOfKeyGet(key)
		item, err := txn.Get(typeKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrKeyNotFound
		}
		if err != nil {
			return err
		}

		// Add size of type key
		size += int64(len(typeKey))

		valCopy, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		keyType := string(valCopy)
		size += int64(len(valCopy))

		// Get the actual value key based on type
		valueKey, err := s.getKeyValueKey(key, keyType)
		if err != nil {
			return err
		}

		// Get the value
		valItem, err := txn.Get(valueKey)
		if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}

		if valItem != nil {
			size += int64(len(valueKey))
			valCopy, err := valItem.ValueCopy(nil)
			if err != nil {
				return err
			}
			size += int64(len(valCopy))
		}

		// For compound types (List, Hash, Set, ZSet), count additional entries
		switch keyType {
		case KeyTypeList:
			// Count all list entries
			prefix := []byte(fmt.Sprintf("%s:%s:", KeyTypeList, key))
			iter := txn.NewIterator(badger.DefaultIteratorOptions)
			defer iter.Close()
			for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
				item := iter.Item()
				size += int64(len(item.KeyCopy(nil)))
				valCopy, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				size += int64(len(valCopy))
			}
		case KeyTypeHash:
			// Count all hash fields
			prefix := []byte(fmt.Sprintf("%s:%s:", KeyTypeHash, key))
			iter := txn.NewIterator(badger.DefaultIteratorOptions)
			defer iter.Close()
			for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
				item := iter.Item()
				size += int64(len(item.KeyCopy(nil)))
				valCopy, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				size += int64(len(valCopy))
			}
		case KeyTypeSet:
			// Count all set members
			prefix := []byte(fmt.Sprintf("%s:%s:", KeyTypeSet, key))
			iter := txn.NewIterator(badger.DefaultIteratorOptions)
			defer iter.Close()
			for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
				item := iter.Item()
				size += int64(len(item.KeyCopy(nil)))
				valCopy, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				size += int64(len(valCopy))
			}
		case KeyTypeSortedSet:
			// Count all zset entries (index and data)
			prefix := []byte(fmt.Sprintf("%s%s:", prefixKeySortedSetBytes, key))
			iter := txn.NewIterator(badger.DefaultIteratorOptions)
			defer iter.Close()
			for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
				item := iter.Item()
				size += int64(len(item.KeyCopy(nil)))
				valCopy, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				size += int64(len(valCopy))
			}
		}

		return nil
	})

	if err != nil {
		return 0, err
	}

	return size, nil
}
