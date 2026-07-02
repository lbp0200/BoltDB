package store

import (
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// Del 删除键，返回删除的数量
func (s *BotreonStore) Del(key string) (int64, error) {
	if err := s.checkErrorInjector("Del"); err != nil {
		return 0, err
	}
	typeKey := TypeOfKeyGet(key)
	var deleted int64

	err := s.retryUpdate(func(txn *badger.Txn) error {
		item, err := txn.Get(typeKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil // 键不存在
		}
		if err != nil {
			return err
		}
		valCopy, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		keyType := string(valCopy)

		switch keyType {
		case KeyTypeString:
			if err := txn.Delete(typeKey); err != nil {
				return err
			}
			if err := txn.Delete([]byte(s.stringKey(key))); err != nil {
				return err
			}
		case KeyTypeList:
			if err := deleteByPrefix(txn, []byte(fmt.Sprintf("%s:%s:", KeyTypeList, key))); err != nil {
				return err
			}
			if err := txn.Delete(typeKey); err != nil {
				return err
			}
		case KeyTypeHash:
			if err := deleteByPrefix(txn, []byte(fmt.Sprintf("%s:%s:", KeyTypeHash, key))); err != nil {
				return err
			}
			if err := txn.Delete(typeKey); err != nil {
				return err
			}
		case KeyTypeSet:
			if err := deleteByPrefix(txn, []byte(fmt.Sprintf("%s:%s:", KeyTypeSet, key))); err != nil {
				return err
			}
			if err := txn.Delete(typeKey); err != nil {
				return err
			}
		case KeyTypeSortedSet:
			if err := deleteByPrefix(txn, []byte(fmt.Sprintf("%s%s:", prefixKeySortedSetBytes, key))); err != nil {
				return err
			}
			if err := txn.Delete(typeKey); err != nil {
				return err
			}
		case KeyTypeJSON:
			if err := txn.Delete([]byte(s.jsonKey(key))); err != nil {
				return err
			}
			if err := txn.Delete(typeKey); err != nil {
				return err
			}
		case KeyTypeTimeSeries:
			if err := deleteByPrefix(txn, []byte(fmt.Sprintf("%s%s:", prefixTS, key))); err != nil {
				return err
			}
			if err := txn.Delete(typeKey); err != nil {
				return err
			}
		case KeyTypeStream:
			if err := deleteByPrefix(txn, []byte("stream:"+key+":")); err != nil {
				return err
			}
			if err := txn.Delete(typeKey); err != nil {
				return err
			}
		case KeyTypeHyperLogLog:
			if err := txn.Delete([]byte("hll:" + key)); err != nil {
				return err
			}
			if err := txn.Delete(typeKey); err != nil {
				return err
			}
		case KeyTypeGeo:
			if err := deleteByPrefix(txn, []byte("geo:"+key+":")); err != nil {
				return err
			}
			if err := txn.Delete(typeKey); err != nil {
				return err
			}
		default:
			if err := txn.Delete(typeKey); err != nil {
				return err
			}
		}
		deleted = 1
		return nil
	}, 30)

	return deleted, err
}

// DelString 删除字符串键
func (s *BotreonStore) DelString(key string) error {
	logFuncTag := "BotreonStoreDelString"
	bKey := []byte(key)
	badgerTypeKey := TypeOfKeyGet(key)
	badgerValueKey := s.stringKey(string(bKey))

	return s.retryUpdate(func(txn *badger.Txn) error {
		errDel := txn.Delete(badgerTypeKey)
		if errDel != nil {
			return fmt.Errorf("%s,Del Badger Type Key:%v", logFuncTag, errDel)
		}
		errDel = txn.Delete([]byte(badgerValueKey))
		if errDel != nil {
			return fmt.Errorf("%s,Del Badger Value Key:%v", logFuncTag, errDel)
		}
		return nil
	}, 30)
}

// deleteByPrefix 删除指定前缀的所有键
func deleteByPrefix(txn *badger.Txn, prefix []byte) error {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	iter := txn.NewIterator(opts)
	defer iter.Close()

	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		if err := txn.Delete(iter.Item().KeyCopy(nil)); err != nil {
			return err
		}
	}
	return nil
}

// checkKeyType 检查键的类型是否符合预期
func checkKeyType(txn *badger.Txn, key string, expectedType string) error {
	item, err := txn.Get(TypeOfKeyGet(key))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	val, err := item.ValueCopy(nil)
	if err != nil {
		return err
	}
	keyType := string(val)
	if keyType != expectedType {
		return ErrWrongType
	}
	return nil
}

// checkTypeBeforeOp 在事务外检查键类型
func (s *BotreonStore) checkTypeBeforeOp(key string, expectedType string) error {
	return s.db.View(func(txn *badger.Txn) error {
		return checkKeyType(txn, key, expectedType)
	})
}
