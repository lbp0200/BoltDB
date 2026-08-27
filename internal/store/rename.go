package store

import (
	"errors"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// Rename 实现 Redis RENAME 命令
func (s *BotreonStore) Rename(key, newKey string) error {
	// Redis compatibility: RENAME with same source and dest is a no-op (+OK)
	if key == newKey {
		return nil
	}
	return s.retryUpdate(func(txn *badger.Txn) error {
		// 检查旧键是否存在
		typeKey := TypeOfKeyGet(key)
		item, err := txn.Get(typeKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return fmt.Errorf("no such key")
		}
		if err != nil {
			return err
		}

		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		keyType := string(val)

		// 如果新键存在，先删除它（在同一事务中）
		newTypeKey := TypeOfKeyGet(newKey)
		newItem, err := txn.Get(newTypeKey)
		if err == nil {
			// 新键存在，需要删除
			newVal, err := newItem.ValueCopy(nil)
			if err != nil {
				return err
			}
			newKeyType := string(newVal)
			// 删除新键的所有相关数据（全类型覆盖，避免旧值残留）
			switch newKeyType {
			case KeyTypeString:
				if err := txn.Delete(newTypeKey); err != nil {
					return err
				}
				if err := txn.Delete([]byte(s.stringKey(newKey))); err != nil {
					return err
				}
			case KeyTypeList:
				if err := deleteByPrefix(txn, []byte(fmt.Sprintf("%s:%s:", KeyTypeList, newKey))); err != nil {
					return err
				}
				if err := txn.Delete(newTypeKey); err != nil {
					return err
				}
			case KeyTypeHash:
				if err := deleteByPrefix(txn, []byte(fmt.Sprintf("%s:%s:", KeyTypeHash, newKey))); err != nil {
					return err
				}
				if err := txn.Delete(newTypeKey); err != nil {
					return err
				}
			case KeyTypeSet:
				if err := deleteByPrefix(txn, []byte(fmt.Sprintf("%s:%s:", KeyTypeSet, newKey))); err != nil {
					return err
				}
				if err := txn.Delete(newTypeKey); err != nil {
					return err
				}
			case KeyTypeSortedSet:
				if err := deleteByPrefix(txn, []byte(fmt.Sprintf("%s%s:", prefixKeySortedSetBytes, newKey))); err != nil {
					return err
				}
				if err := txn.Delete(newTypeKey); err != nil {
					return err
				}
			case KeyTypeJSON:
				if err := txn.Delete([]byte(s.jsonKey(newKey))); err != nil {
					return err
				}
				if err := txn.Delete(newTypeKey); err != nil {
					return err
				}
			case KeyTypeStream:
				if err := deleteByPrefix(txn, []byte("stream:"+newKey+":")); err != nil {
					return err
				}
				if err := txn.Delete(newTypeKey); err != nil {
					return err
				}
			case KeyTypeGeo:
				if err := deleteByPrefix(txn, []byte("geo:"+newKey+":")); err != nil {
					return err
				}
				if err := deleteByPrefix(txn, []byte(fmt.Sprintf("%s%s:", prefixKeySortedSetBytes, newKey))); err != nil {
					return err
				}
				if err := txn.Delete(newTypeKey); err != nil {
					return err
				}
			case KeyTypeTimeSeries:
				if err := deleteByPrefix(txn, []byte(fmt.Sprintf("%s%s:", prefixTS, newKey))); err != nil {
					return err
				}
				if err := txn.Delete(newTypeKey); err != nil {
					return err
				}
			case KeyTypeHyperLogLog:
				if err := txn.Delete([]byte(HyperLogLogPrefix + newKey)); err != nil {
					return err
				}
				if err := txn.Delete(newTypeKey); err != nil {
					return err
				}
			default:
				if err := txn.Delete(newTypeKey); err != nil {
					return err
				}
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}

		// 根据类型复制所有相关键
		switch keyType {
		case KeyTypeString:
			oldValueKey := []byte(s.stringKey(key))
			oldValue, err := txn.Get(oldValueKey)
			if err != nil {
				return err
			}
			valueBytes, err := oldValue.ValueCopy(nil)
			if err != nil {
				return err
			}
			// 设置新键
			if err := txn.Set(newTypeKey, []byte(keyType)); err != nil {
				return err
			}
			newValueKey := []byte(s.stringKey(newKey))
			// 保持TTL（ExpiresAt 始终是秒级 Unix 时间戳）
			expiresAt := oldValue.ExpiresAt()
			if expiresAt > 0 {
				nowUnix := uint64(time.Now().Unix())
				var ttl time.Duration
				if expiresAt > nowUnix {
					ttl = time.Duration(expiresAt-nowUnix) * time.Second
				}
				if ttl > 0 {
					e := badger.NewEntry(newValueKey, valueBytes).WithTTL(ttl)
					if err := txn.SetEntry(e); err != nil {
						return err
					}
				} else {
					// TTL已过期，不设置
					if err := txn.Set(newValueKey, valueBytes); err != nil {
						return err
					}
				}
			} else {
				if err := txn.Set(newValueKey, valueBytes); err != nil {
					return err
				}
			}
			// 删除旧键
			if err := txn.Delete(typeKey); err != nil {
				return err
			}
			return txn.Delete(oldValueKey)
		case KeyTypeList:
			// 复制所有LIST键
			prefix := []byte(fmt.Sprintf("%s:%s:", KeyTypeList, key))
			if err := copyKeysByPrefix(txn, prefix, key, newKey, KeyTypeList); err != nil {
				return err
			}
			if err := txn.Set(newTypeKey, []byte(keyType)); err != nil {
				return err
			}
			// 删除旧键
			if err := deleteByPrefix(txn, prefix); err != nil {
				return err
			}
			return txn.Delete(typeKey)
		case KeyTypeHash:
			prefix := []byte(fmt.Sprintf("%s:%s:", KeyTypeHash, key))
			if err := copyKeysByPrefix(txn, prefix, key, newKey, KeyTypeHash); err != nil {
				return err
			}
			if err := txn.Set(newTypeKey, []byte(keyType)); err != nil {
				return err
			}
			// 删除旧键
			if err := deleteByPrefix(txn, prefix); err != nil {
				return err
			}
			return txn.Delete(typeKey)
		case KeyTypeSet:
			prefix := []byte(fmt.Sprintf("%s:%s:", KeyTypeSet, key))
			if err := copyKeysByPrefix(txn, prefix, key, newKey, KeyTypeSet); err != nil {
				return err
			}
			if err := txn.Set(newTypeKey, []byte(keyType)); err != nil {
				return err
			}
			// 删除旧键
			if err := deleteByPrefix(txn, prefix); err != nil {
				return err
			}
			return txn.Delete(typeKey)
		case KeyTypeSortedSet:
			prefix := []byte(fmt.Sprintf("%s%s:", prefixKeySortedSetBytes, key))
			if err := copyKeysByPrefix(txn, prefix, key, newKey, KeyTypeSortedSet); err != nil {
				return err
			}
			if err := txn.Set(newTypeKey, []byte(keyType)); err != nil {
				return err
			}
			// 删除旧键
			if err := deleteByPrefix(txn, prefix); err != nil {
				return err
			}
			return txn.Delete(typeKey)
		case KeyTypeJSON:
			oldJSONKey := []byte(s.jsonKey(key))
			oldJSONItem, err := txn.Get(oldJSONKey)
			if err != nil {
				return err
			}
			jVal, err := oldJSONItem.ValueCopy(nil)
			if err != nil {
				return err
			}
			expiresAt := oldJSONItem.ExpiresAt()
			if err := txn.Set(newTypeKey, []byte(keyType)); err != nil {
				return err
			}
			newJSONKey := []byte(s.jsonKey(newKey))
			if expiresAt > 0 {
				nowUnix := uint64(time.Now().Unix())
				var ttl time.Duration
				if expiresAt > nowUnix {
					ttl = time.Duration(expiresAt-nowUnix) * time.Second
				}
				if ttl > 0 {
					e := badger.NewEntry(newJSONKey, jVal).WithTTL(ttl)
					if err := txn.SetEntry(e); err != nil {
						return err
					}
				} else if err := txn.Set(newJSONKey, jVal); err != nil {
					return err
				}
			} else if err := txn.Set(newJSONKey, jVal); err != nil {
				return err
			}
			if err := txn.Delete(typeKey); err != nil {
				return err
			}
			return txn.Delete(oldJSONKey)
		case KeyTypeGeo:
			prefix := []byte("geo:" + key + ":")
			if err := copyKeysByPrefix(txn, prefix, key, newKey, KeyTypeGeo); err != nil {
				return err
			}
			// GEO additionally stores zset index/data keys (zset:<key>:*) for range queries.
			zsetPrefix := []byte(fmt.Sprintf("%s%s:", prefixKeySortedSetBytes, key))
			if err := copyKeysByPrefix(txn, zsetPrefix, key, newKey, KeyTypeSortedSet); err != nil {
				return err
			}
			if err := txn.Set(newTypeKey, []byte(keyType)); err != nil {
				return err
			}
			if err := deleteByPrefix(txn, prefix); err != nil {
				return err
			}
			if err := deleteByPrefix(txn, zsetPrefix); err != nil {
				return err
			}
			return txn.Delete(typeKey)
		case KeyTypeStream:
			prefix := []byte("stream:" + key + ":")
			if err := copyKeysByPrefix(txn, prefix, key, newKey, KeyTypeStream); err != nil {
				return err
			}
			if err := txn.Set(newTypeKey, []byte(keyType)); err != nil {
				return err
			}
			if err := deleteByPrefix(txn, prefix); err != nil {
				return err
			}
			return txn.Delete(typeKey)
		case KeyTypeTimeSeries:
			prefix := []byte(fmt.Sprintf("%s%s:", prefixTS, key))
			if err := copyKeysByPrefix(txn, prefix, key, newKey, KeyTypeTimeSeries); err != nil {
				return err
			}
			if err := txn.Set(newTypeKey, []byte(keyType)); err != nil {
				return err
			}
			if err := deleteByPrefix(txn, prefix); err != nil {
				return err
			}
			return txn.Delete(typeKey)
		case KeyTypeHyperLogLog:
			oldHLLKey := []byte(HyperLogLogPrefix + key)
			oldHLLItem, err := txn.Get(oldHLLKey)
			if err != nil {
				return err
			}
			hVal, err := oldHLLItem.ValueCopy(nil)
			if err != nil {
				return err
			}
			expiresAt := oldHLLItem.ExpiresAt()
			if err := txn.Set(newTypeKey, []byte(keyType)); err != nil {
				return err
			}
			newHLLKey := []byte(HyperLogLogPrefix + newKey)
			if expiresAt > 0 {
				nowUnix := uint64(time.Now().Unix())
				var ttl time.Duration
				if expiresAt > nowUnix {
					ttl = time.Duration(expiresAt-nowUnix) * time.Second
				}
				if ttl > 0 {
					e := badger.NewEntry(newHLLKey, hVal).WithTTL(ttl)
					if err := txn.SetEntry(e); err != nil {
						return err
					}
				} else if err := txn.Set(newHLLKey, hVal); err != nil {
					return err
				}
			} else if err := txn.Set(newHLLKey, hVal); err != nil {
				return err
			}
			if err := txn.Delete(typeKey); err != nil {
				return err
			}
			return txn.Delete(oldHLLKey)
		default:
			if err := txn.Set(newTypeKey, []byte(keyType)); err != nil {
				return err
			}
			return txn.Delete(typeKey)
		}
	}, 30)
}

// RenameNX 实现 RENAMENX 命令（仅新键不存在时重命名）
func (s *BotreonStore) RenameNX(key, newKey string) (bool, error) {
	success := false
	err := s.retryUpdate(func(txn *badger.Txn) error {
		// 检查新键是否已存在
		newTypeKey := TypeOfKeyGet(newKey)
		_, err := txn.Get(newTypeKey)
		if err == nil {
			// 新键已存在，返回false
			return nil
		}
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}

		// 新键不存在，执行重命名
		if err := s.Rename(key, newKey); err != nil {
			return err
		}
		success = true
		return nil
	}, 30)
	return success, err
}

// copyKeysByPrefix 复制指定前缀的键到新键名
func copyKeysByPrefix(txn *badger.Txn, oldPrefix []byte, oldKey, newKey, keyType string) error {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = true
	iter := txn.NewIterator(opts)
	defer iter.Close()

	for iter.Seek(oldPrefix); iter.ValidForPrefix(oldPrefix); iter.Next() {
		item := iter.Item()
		oldKeyBytes := item.KeyCopy(nil)
		oldKeyStr := string(oldKeyBytes)

		// 生成新键：按前缀形态区分三类
		var newKeyStr string
		switch keyType {
		case KeyTypeSortedSet:
			// SortedSet: zset:oldKey:... → zset:newKey:...
			oldKeyPrefix := fmt.Sprintf("%s%s:", prefixKeySortedSetBytes, oldKey)
			newKeyStr = fmt.Sprintf("%s%s:%s", prefixKeySortedSetBytes, newKey, oldKeyStr[len(oldKeyPrefix):])
		case KeyTypeGeo:
			// GEO: geo:oldKey:... → geo:newKey:...
			oldKeyPrefix := fmt.Sprintf("geo:%s:", oldKey)
			newKeyStr = fmt.Sprintf("geo:%s:%s", newKey, oldKeyStr[len(oldKeyPrefix):])
		case KeyTypeStream:
			// Stream: stream:oldKey:... → stream:newKey:...
			oldKeyPrefix := fmt.Sprintf("stream:%s:", oldKey)
			newKeyStr = fmt.Sprintf("stream:%s:%s", newKey, oldKeyStr[len(oldKeyPrefix):])
		case KeyTypeTimeSeries:
			// TS: TS:oldKey:... → TS:newKey:...（prefixTS="TS:"）
			oldKeyPrefix := fmt.Sprintf("%s%s:", prefixTS, oldKey)
			newKeyStr = fmt.Sprintf("%s%s:%s", prefixTS, newKey, oldKeyStr[len(oldKeyPrefix):])
		default:
			// 其他类型使用TYPE:oldKey:...格式
			oldKeyPrefix := fmt.Sprintf("%s:%s:", keyType, oldKey)
			newKeyStr = fmt.Sprintf("%s:%s:%s", keyType, newKey, oldKeyStr[len(oldKeyPrefix):])
		}

		// 复制值
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}

		// 设置新键（保持TTL，ExpiresAt 始终是秒级 Unix 时间戳）
		expiresAt := item.ExpiresAt()
		if expiresAt > 0 {
			nowUnix := uint64(time.Now().Unix())
			var ttl time.Duration
			if expiresAt > nowUnix {
				// #nosec G115 - expiresAt is within int64 range
				ttl = time.Duration(expiresAt-nowUnix) * time.Second
			}
			if ttl > 0 {
				e := badger.NewEntry([]byte(newKeyStr), val).WithTTL(ttl)
				if err := txn.SetEntry(e); err != nil {
					return err
				}
			} else {
				// TTL已过期，跳过
				if err := txn.Set([]byte(newKeyStr), val); err != nil {
					return err
				}
			}
		} else {
			if err := txn.Set([]byte(newKeyStr), val); err != nil {
				return err
			}
		}
	}
	return nil
}
