package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc64"
	"strconv"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/lbp0200/BoltDB/internal/logger"
)

// Del 删除键，返回删除的数量
func (s *BotreonStore) Del(key string) (int64, error) {
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

		// 清除读缓存
		if s.readCache != nil {
			s.readCache.Delete(key)
		}

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

func (s *BotreonStore) DelString(key string) error {
	logFuncTag := "BotreonStoreDelString"
	bKey := []byte(key)
	badgerTypeKey := TypeOfKeyGet(key)
	badgerValueKey := s.stringKey(string(bKey))

	// 清除读缓存
	if s.readCache != nil {
		s.readCache.Delete(key)
	}

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

// getKeyValueKey 根据键类型获取值键
func (s *BotreonStore) getKeyValueKey(key string, keyType string) ([]byte, error) {
	switch keyType {
	case KeyTypeString:
		return []byte(s.stringKey(key)), nil
	case KeyTypeList:
		// List的主键是length键
		return []byte(s.listKey(key, "length")), nil
	case KeyTypeHash:
		// Hash的主键是count键
		return []byte(fmt.Sprintf("%s:%s:count", KeyTypeHash, key)), nil
	case KeyTypeSet:
		// Set的主键是count键
		return []byte(s.setKey(key, "count")), nil
	case KeyTypeSortedSet:
		// SortedSet的主键是meta键
		return sortedSetKeyMeta(key), nil
	case KeyTypeJSON:
		// JSON的主键就是json键
		return []byte(s.jsonKey(key)), nil
	case KeyTypeTimeSeries:
		// TimeSeries的主键是meta键
		return tsMetaKey(key), nil
	default:
		return nil, fmt.Errorf("unknown key type: %s", keyType)
	}
}

// EXISTS 实现 Redis EXISTS 命令，检查键是否存在
func (s *BotreonStore) Exists(key string) (bool, error) {
	exists := false
	err := s.db.View(func(txn *badger.Txn) error {
		typeKey := TypeOfKeyGet(key)
		_, err := txn.Get(typeKey)
		if err == nil {
			exists = true
			return nil
		}
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		return err
	})
	return exists, err
}

// Type 实现 Redis TYPE 命令，返回键的类型
func (s *BotreonStore) Type(key string) (string, error) {
	var keyType string
	err := s.db.View(func(txn *badger.Txn) error {
		typeKey := TypeOfKeyGet(key)
		item, err := txn.Get(typeKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			keyType = "none"
			return nil
		}
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		keyType = string(val)
		// 将内部类型转换为Redis类型
		switch keyType {
		case KeyTypeString:
			keyType = "string"
		case KeyTypeList:
			keyType = "list"
		case KeyTypeHash:
			keyType = "hash"
		case KeyTypeSet:
			keyType = "set"
		case KeyTypeSortedSet:
			keyType = "zset"
		case KeyTypeJSON:
			keyType = "json"
		case KeyTypeTimeSeries:
			keyType = "ts"
		default:
			keyType = "none"
		}
		return nil
	})
	return keyType, err
}

// EXPIRE 实现 Redis EXPIRE 命令，设置键的过期时间（秒）
func (s *BotreonStore) Expire(key string, seconds int) (bool, error) {
	success := false
	err := s.retryUpdate(func(txn *badger.Txn) error {
		typeKey := TypeOfKeyGet(key)
		item, err := txn.Get(typeKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil // 键不存在，返回false
		}
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		keyType := string(val)

		// 获取值键
		valueKey, err := s.getKeyValueKey(key, keyType)
		if err != nil {
			return err
		}

		// 获取当前值
		valueItem, err := txn.Get(valueKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}

		valBytes, err := valueItem.ValueCopy(nil)
		if err != nil {
			return err
		}

		// 直接设置TTL：计算过期时间戳（纳秒）
		expiresAt := uint64(time.Now().UnixNano()) + uint64(seconds)*uint64(time.Second)
		e := badger.NewEntry(valueKey, valBytes)
		e.ExpiresAt = expiresAt
		if err := txn.SetEntry(e); err != nil {
			return err
		}

		success = true
		return nil
	}, 30)
	return success, err
}

// EXPIREAT 实现 Redis EXPIREAT 命令，设置键的过期时间（Unix时间戳，秒）
func (s *BotreonStore) ExpireAt(key string, timestamp int64) (bool, error) {
	now := time.Now().Unix()
	ttl := timestamp - now
	if ttl <= 0 {
		// 时间戳已过期，删除键
		if _, err := s.Del(key); err != nil {
			logger.Logger.Warn().Err(err).Str("key", key).Msg("ExpireAt: error deleting expired key")
		}
		return false, nil
	}
	return s.Expire(key, int(ttl))
}

// PEXPIRE 实现 Redis PEXPIRE 命令，设置键的过期时间（毫秒）
func (s *BotreonStore) PExpire(key string, milliseconds int64) (bool, error) {
	success := false
	err := s.retryUpdate(func(txn *badger.Txn) error {
		typeKey := TypeOfKeyGet(key)
		item, err := txn.Get(typeKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil // 键不存在，返回false
		}
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		keyType := string(val)

		// 获取值键
		valueKey, err := s.getKeyValueKey(key, keyType)
		if err != nil {
			return err
		}

		// 获取当前值
		valueItem, err := txn.Get(valueKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}

		valBytes, err := valueItem.ValueCopy(nil)
		if err != nil {
			return err
		}

		// 直接设置TTL：计算过期时间戳（纳秒）
		expiresAt := uint64(time.Now().UnixNano()) + uint64(milliseconds)*uint64(time.Millisecond)
		e := badger.NewEntry(valueKey, valBytes)
		e.ExpiresAt = expiresAt
		if err := txn.SetEntry(e); err != nil {
			return err
		}

		success = true
		return nil
	}, 30)
	return success, err
}

// PEXPIREAT 实现 Redis PEXPIREAT 命令，设置键的过期时间（Unix时间戳，毫秒）
func (s *BotreonStore) PExpireAt(key string, timestampMillis int64) (bool, error) {
	now := time.Now().UnixNano() / int64(time.Millisecond)
	ttl := timestampMillis - now
	if ttl <= 0 {
		// 时间戳已过期，删除键
		if _, err := s.Del(key); err != nil {
			logger.Logger.Warn().Err(err).Str("key", key).Msg("PExpireAt: error deleting expired key")
		}
		return false, nil
	}
	return s.PExpire(key, ttl)
}

// TTL 实现 Redis TTL 命令，获取键的剩余生存时间（秒）
func (s *BotreonStore) TTL(key string) (int64, error) {
	var ttl int64 = -2 // -2表示键不存在
	err := s.db.View(func(txn *badger.Txn) error {
		typeKey := TypeOfKeyGet(key)
		item, err := txn.Get(typeKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil // 键不存在，返回-2
		}
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		keyType := string(val)

		// 获取值键
		valueKey, err := s.getKeyValueKey(key, keyType)
		if err != nil {
			return err
		}

		// 获取值键的TTL
		valueItem, err := txn.Get(valueKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			ttl = -2
			return nil
		}
		if err != nil {
			return err
		}

		// ExpiresAt 返回的是 Unix 纳秒时间戳 (uint64)
		expiresAt := valueItem.ExpiresAt()
		if expiresAt == 0 {
			ttl = -1 // -1表示键存在但没有设置过期时间
			return nil
		}

		// #nosec G115 - expiresAt is a valid Unix timestamp within int64 range
		nowNano := time.Now().UnixNano()
		ttl = int64(expiresAt-uint64(nowNano)) / int64(time.Second)
		if ttl < 0 {
			ttl = -2 // 已过期
		}
		return nil
	})
	return ttl, err
}

// PTTL 实现 Redis PTTL 命令，获取键的剩余生存时间（毫秒）
func (s *BotreonStore) PTTL(key string) (int64, error) {
	var ttl int64 = -2 // -2表示键不存在
	err := s.db.View(func(txn *badger.Txn) error {
		typeKey := TypeOfKeyGet(key)
		item, err := txn.Get(typeKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil // 键不存在，返回-2
		}
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		keyType := string(val)

		// 获取值键
		valueKey, err := s.getKeyValueKey(key, keyType)
		if err != nil {
			return err
		}

		// 获取值键的TTL
		valueItem, err := txn.Get(valueKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			ttl = -2
			return nil
		}
		if err != nil {
			return err
		}

		// ExpiresAt 返回的是 Unix 纳秒时间戳 (uint64)
		expiresAt := valueItem.ExpiresAt()
		if expiresAt == 0 {
			ttl = -1 // -1表示键存在但没有设置过期时间
			return nil
		}

		// #nosec G115 - expiresAt is a valid Unix timestamp within int64 range
		nowNano := time.Now().UnixNano()
		ttl = int64(expiresAt-uint64(nowNano)) / int64(time.Millisecond)
		if ttl < 0 {
			ttl = -2 // 已过期
		}
		return nil
	})
	return ttl, err
}

// PERSIST 实现 Redis PERSIST 命令，移除键的过期时间
func (s *BotreonStore) Persist(key string) (bool, error) {
	var hasTTL bool
	err := s.retryUpdate(func(txn *badger.Txn) error {
		typeKey := TypeOfKeyGet(key)
		item, err := txn.Get(typeKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrKeyNotFound
		}
		if err != nil {
			return err
		}
		typeVal, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		keyType := string(typeVal)

		vk, err := s.getKeyValueKey(key, keyType)
		if err != nil {
			return err
		}

		valueItem, err := txn.Get(vk)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrKeyNotFound
		}
		if err != nil {
			return err
		}
		if valueItem.ExpiresAt() == 0 {
			hasTTL = false
			return nil
		}
		hasTTL = true
		valBytes, err := valueItem.ValueCopy(nil)
		if err != nil {
			return err
		}
		return txn.Set(vk, valBytes)
	}, 30)
	if errors.Is(err, ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return hasTTL, nil
}

// checkKeyType checks that the key has the expected type.
// Returns nil if key doesn't exist (no type info), ErrWrongType if type mismatch.
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

// checkTypeBeforeOp checks key type in a separate View transaction.
func (s *BotreonStore) checkTypeBeforeOp(key string, expectedType string) error {
	return s.db.View(func(txn *badger.Txn) error {
		return checkKeyType(txn, key, expectedType)
	})
}

// RENAME 实现 Redis RENAME 命令，重命名键
func (s *BotreonStore) Rename(key, newKey string) error {
	// 清除读缓存
	if s.readCache != nil {
		s.readCache.Delete(key)
		s.readCache.Delete(newKey)
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
			// 删除新键的所有相关数据
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
			// 保持TTL
			expiresAt := oldValue.ExpiresAt()
			if expiresAt > 0 {
				// #nosec G115 - expiresAt is a valid Unix timestamp within int64 range
				ttl := time.Until(time.Unix(int64(expiresAt), 0))
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
		default:
			if err := txn.Set(newTypeKey, []byte(keyType)); err != nil {
				return err
			}
			return txn.Delete(typeKey)
		}
	}, 30)
}

// copyKeysByPrefix 复制所有匹配前缀的键
func copyKeysByPrefix(txn *badger.Txn, oldPrefix []byte, oldKey, newKey, keyType string) error {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = true
	iter := txn.NewIterator(opts)
	defer iter.Close()

	for iter.Seek(oldPrefix); iter.ValidForPrefix(oldPrefix); iter.Next() {
		item := iter.Item()
		oldKeyBytes := item.KeyCopy(nil)
		oldKeyStr := string(oldKeyBytes)

		// 生成新键
		var newKeyStr string
		if keyType == KeyTypeSortedSet {
			// SortedSet使用zset:前缀，格式是zset:oldKey:...
			// 需要替换为zset:newKey:...
			oldKeyPrefix := fmt.Sprintf("%s%s:", prefixKeySortedSetBytes, oldKey)
			newKeyStr = fmt.Sprintf("%s%s:%s", prefixKeySortedSetBytes, newKey, oldKeyStr[len(oldKeyPrefix):])
		} else {
			// 其他类型使用TYPE:oldKey:...格式
			// 需要替换为TYPE:newKey:...
			oldKeyPrefix := fmt.Sprintf("%s:%s:", keyType, oldKey)
			newKeyStr = fmt.Sprintf("%s:%s:%s", keyType, newKey, oldKeyStr[len(oldKeyPrefix):])
		}

		// 复制值
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}

		// 设置新键（保持TTL）
		expiresAt := item.ExpiresAt()
		if expiresAt > 0 {
			// #nosec G115 - expiresAt is a valid Unix timestamp within int64 range
			ttl := time.Until(time.Unix(int64(expiresAt), 0))
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

// RENAMENX 实现 Redis RENAMENX 命令，仅当新键不存在时重命名
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

// matchPattern 检查键是否匹配模式（支持*和?通配符）
func matchPattern(key, pattern string) bool {
	// 简单的通配符匹配实现
	if pattern == "*" {
		return true
	}
	// 使用简单的字符串匹配（可以改进为正则表达式）
	keyRunes := []rune(key)
	patternRunes := []rune(pattern)

	keyIdx := 0
	patternIdx := 0
	keyStar := -1
	patternStar := -1

	for keyIdx < len(keyRunes) || patternIdx < len(patternRunes) {
		if patternIdx < len(patternRunes) && patternRunes[patternIdx] == '*' {
			keyStar = keyIdx
			patternStar = patternIdx
			patternIdx++
			continue
		}
		if keyIdx < len(keyRunes) && patternIdx < len(patternRunes) &&
			(patternRunes[patternIdx] == '?' || patternRunes[patternIdx] == keyRunes[keyIdx]) {
			keyIdx++
			patternIdx++
			continue
		}
		if keyStar >= 0 {
			// If keyStar has reached/exceeded key length, no more backtracking possible
			if keyStar >= len(keyRunes) {
				return false
			}
			keyStar++
			keyIdx = keyStar
			patternIdx = patternStar + 1
			continue
		}
		return false
	}

	// 处理pattern末尾的*
	for patternIdx < len(patternRunes) && patternRunes[patternIdx] == '*' {
		patternIdx++
	}

	return patternIdx == len(patternRunes)
}

// Keys 实现 Redis KEYS 命令，查找所有匹配给定模式的键
func (s *BotreonStore) Keys(pattern string) ([]string, error) {
	var keys []string
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		iter := txn.NewIterator(opts)
		defer iter.Close()

		// 查找所有TYPE_前缀的键
		prefix := prefixKeyTypeBytes
		for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
			item := iter.Item()
			keyBytes := item.KeyCopy(nil)
			// 提取实际键名（去掉TYPE_前缀）
			key := string(keyBytes[len(prefixKeyTypeBytes):])
			if matchPattern(key, pattern) {
				keys = append(keys, key)
			}
		}
		return nil
	})
	return keys, err
}

// ScanResult 表示SCAN命令的返回结果
type ScanResult struct {
	Cursor uint64
	Keys   []string
}

// Scan 实现 Redis SCAN 命令，增量迭代键空间
func (s *BotreonStore) Scan(cursor uint64, pattern string, count int) (ScanResult, error) {
	var result ScanResult
	result.Cursor = 0
	result.Keys = []string{}

	if count <= 0 {
		count = 10 // 默认值
	}

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		iter := txn.NewIterator(opts)
		defer iter.Close()

		prefix := prefixKeyTypeBytes
		currentPos := uint64(0)
		collected := 0

		// 如果cursor不为0，需要跳过前面的键
		if cursor > 0 {
			// 简单实现：从头开始迭代，跳过cursor个键
			for iter.Seek(prefix); iter.ValidForPrefix(prefix) && currentPos < cursor; iter.Next() {
				currentPos++
			}
		} else {
			iter.Seek(prefix)
		}

		// 收集匹配的键
		for iter.ValidForPrefix(prefix) && collected < count {
			item := iter.Item()
			keyBytes := item.KeyCopy(nil)
			key := string(keyBytes[len(prefixKeyTypeBytes):])

			if pattern == "" || pattern == "*" || matchPattern(key, pattern) {
				result.Keys = append(result.Keys, key)
				collected++
			}

			currentPos++
			iter.Next()
		}

		// 检查是否还有更多键
		if iter.ValidForPrefix(prefix) {
			result.Cursor = currentPos
		} else {
			result.Cursor = 0 // 0表示迭代完成
		}

		return nil
	})
	return result, err
}

// RandomKey 实现 Redis RANDOMKEY 命令，随机返回一个键
func (s *BotreonStore) RandomKey() (string, error) {
	var key string
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		iter := txn.NewIterator(opts)
		defer iter.Close()

		prefix := prefixKeyTypeBytes

		// 先计算总数
		count := 0
		for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
			count++
		}

		if count == 0 {
			return nil // 没有键
		}

		// 随机选择一个位置
		randPos := 0
		if count > 1 {
			// 使用简单的伪随机（实际应该使用crypto/rand）
			randPos = int(time.Now().UnixNano() % int64(count))
		}

		// 迭代到随机位置
		iter.Rewind()
		current := 0
		for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
			if current == randPos {
				item := iter.Item()
				keyBytes := item.KeyCopy(nil)
				key = string(keyBytes[len(prefixKeyTypeBytes):])
				return nil
			}
			current++
		}

		return nil
	})
	return key, err
}

// ObjectRefCount 实现 Redis OBJECT REFCOUNT 命令，返回键的引用计数
func (s *BotreonStore) ObjectRefCount(key string) (int64, error) {
	typeKey := TypeOfKeyGet(key)
	var refcount int64

	err := s.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(typeKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil // 键不存在，返回 nil
		}
		if err != nil {
			return err
		}
		refcount = 1 // 我们总是返回1，因为每个键只存储一次
		return nil
	})

	if errors.Is(err, badger.ErrKeyNotFound) {
		return 0, nil
	}
	return refcount, err
}

// ObjectEncoding 实现 Redis OBJECT ENCODING 命令，返回键的内部编码
func (s *BotreonStore) ObjectEncoding(key string) (string, error) {
	typeKey := TypeOfKeyGet(key)

	var keyType string
	err := s.db.View(func(txn *badger.Txn) error {
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
		keyType = string(valCopy)
		return nil
	})

	if errors.Is(err, badger.ErrKeyNotFound) {
		return "", nil
	}
	if err != nil {
		return "", err
	}

	switch keyType {
	case KeyTypeString:
		return "raw", nil // 简单字符串使用 raw 编码
	case KeyTypeList:
		return "quicklist", nil // 列表使用 quicklist 编码
	case KeyTypeHash:
		return "hashtable", nil // 哈希表使用 hashtable 编码
	case KeyTypeSet:
		return "hashtable", nil // 集合使用 hashtable 编码
	case "zset":
		return "ziplist", nil // 有序集合（如果小）使用 ziplist，否则 skiplist
	default:
		return "", nil
	}
}

// ObjectIdleTime 实现 Redis OBJECT IDLETIME 命令，返回键的空闲时间（秒）
// 注意：由于 BadgerDB 不直接支持 LRU 追踪，我们返回 0
func (s *BotreonStore) ObjectIdleTime(key string) (int64, error) {
	// BadgerDB 不维护访问时间信息，返回 0
	// 如果需要精确实现，需要额外维护访问时间戳
	return 0, nil
}

// writeRDBLength 写入 RDB 长度编码
func writeRDBLength(buf *bytes.Buffer, length uint64) {
	if length < 0x40 {
		buf.WriteByte(byte(length))
	} else if length < 0x4000 {
		buf.WriteByte(byte(((length >> 8) & 0x3F) | 0x40))
		buf.WriteByte(byte(length & 0xFF))
	} else {
		buf.WriteByte(0x80)
		// #nosec G115 - length is bounded by practical RDB size limits
		_ = binary.Write(buf, binary.LittleEndian, uint32(length))
	}
}

// writeRDBString 写入 RDB 字符串（先长度后内容）
func writeRDBString(buf *bytes.Buffer, s string) {
	writeRDBLength(buf, uint64(len(s)))
	buf.WriteString(s)
}

// writeRDBBytes 写入 RDB 字节数组
func writeRDBBytes(buf *bytes.Buffer, b []byte) {
	writeRDBLength(buf, uint64(len(b)))
	buf.Write(b)
}

// Dump 实现 Redis DUMP 命令，使用标准 RDB 格式序列化键值
func (s *BotreonStore) Dump(key string) ([]byte, error) {
	var serializedData []byte
	err := s.db.View(func(txn *badger.Txn) error {
		// 获取键类型
		typeKey := TypeOfKeyGet(key)
		item, err := txn.Get(typeKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return fmt.Errorf("ERR no such key")
		}
		if err != nil {
			return err
		}
		valCopy, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		keyType := string(valCopy)

		// 获取 TTL（毫秒）— 从值键的 ExpiresAt 读取
		var ttl int64 = 0
		valueKey, err := s.getKeyValueKey(key, keyType)
		if err == nil {
			if valItem, err := txn.Get(valueKey); err == nil {
				if expiresAt := valItem.ExpiresAt(); expiresAt > 0 {
					nowNano := time.Now().UnixNano()
					remaining := int64(expiresAt) - nowNano
					if remaining > 0 {
						ttl = remaining / 1_000_000 // 纳秒转毫秒
					}
				}
			}
		}

		// 使用 buf 直接构建 RDB 格式
		buf := &bytes.Buffer{}

		// 写入 RDB header
		buf.WriteString("REDIS0009")

		// 写入 TTL（毫秒精度）
		if ttl > 0 {
			buf.WriteByte(0xFC) // 毫秒精度过期时间
			expireMS := time.Now().UnixMilli() + ttl
			// #nosec G115 - expireMS is within int64 range for practical purposes
			_ = binary.Write(buf, binary.LittleEndian, uint64(expireMS))
		}

		switch keyType {
		case KeyTypeString:
			// 获取字符串值
			strKey := s.stringKey(key)
			valItem, err := txn.Get([]byte(strKey))
			if err != nil {
				return err
			}
			val, err := valItem.ValueCopy(nil)
			if err != nil {
				return err
			}
			buf.WriteByte(0) // STRING type
			writeRDBString(buf, key)
			writeRDBBytes(buf, val)

		case KeyTypeList:
			// 获取列表所有元素
			listData, err := s.getListData(key)
			if err != nil {
				return err
			}
			buf.WriteByte(1) // LIST type
			writeRDBString(buf, key)
			writeRDBLength(buf, uint64(len(listData)))
			for _, elem := range listData {
				writeRDBString(buf, elem)
			}

		case KeyTypeHash:
			// 获取哈希所有字段
			fields, err := s.getAllHashFields(txn, key)
			if err != nil {
				return err
			}
			buf.WriteByte(3) // HASH type
			writeRDBString(buf, key)
			writeRDBLength(buf, uint64(len(fields)))
			for _, field := range fields {
				writeRDBString(buf, field)
				valItem, err := txn.Get([]byte(fmt.Sprintf("%s:%s:%s", KeyTypeHash, key, field)))
				if err != nil {
					return err
				}
				val, err := valItem.ValueCopy(nil)
				if err != nil {
					return err
				}
				writeRDBBytes(buf, val)
			}

		case KeyTypeSet:
			// 获取集合所有成员
			members, err := s.SMembers(key)
			if err != nil {
				return err
			}
			buf.WriteByte(2) // SET type
			writeRDBString(buf, key)
			writeRDBLength(buf, uint64(len(members)))
			for _, member := range members {
				writeRDBString(buf, member)
			}

		case KeyTypeSortedSet:
			// 获取有序集合所有成员
			members, err := s.ZRange(key, 0, -1)
			if err != nil {
				return err
			}
			buf.WriteByte(4) // ZSET type
			writeRDBString(buf, key)
			writeRDBLength(buf, uint64(len(members)))
			for _, m := range members {
				writeRDBString(buf, m.Member)
				scoreBytes := []byte(fmt.Sprintf("%.10g", m.Score))
				writeRDBBytes(buf, scoreBytes)
			}

		default:
			return fmt.Errorf("ERR unsupported key type: %s", keyType)
		}

		// 写入 footer + CRC64 校验和
		buf.WriteByte(0xFF)
		hash := crc64.New(crc64.MakeTable(crc64.ECMA))
		hash.Write(buf.Bytes())
		buf.Write(hash.Sum(nil))

		serializedData = buf.Bytes()
		return nil
	})

	return serializedData, err
}

// readRDBLength 读取 RDB 长度编码
func readRDBLength(buf *bytes.Buffer) (uint64, error) {
	if buf.Len() == 0 {
		return 0, fmt.Errorf("unexpected end of buffer")
	}
	b := buf.Next(1)[0]
	if b&0x80 == 0 {
		return uint64(b & 0x3F), nil
	} else if b&0x40 == 0 {
		if buf.Len() < 1 {
			return 0, fmt.Errorf("unexpected end of buffer")
		}
		b2 := buf.Next(1)[0]
		return uint64(((uint64(b) & 0x3F) << 8) | uint64(b2)), nil
	} else {
		if buf.Len() < 4 {
			return 0, fmt.Errorf("unexpected end of buffer")
		}
		var length uint32
		if err := binary.Read(buf, binary.LittleEndian, &length); err != nil {
			return 0, err
		}
		return uint64(length), nil
	}
}

// readRDBString 读取 RDB 字符串
func readRDBString(buf *bytes.Buffer) (string, error) {
	length, err := readRDBLength(buf)
	if err != nil {
		return "", err
	}
	if buf.Len() < int(length) {
		return "", fmt.Errorf("unexpected end of buffer")
	}
	return string(buf.Next(int(length))), nil
}

// readRDBBytes 读取 RDB 字节数组
func readRDBBytes(buf *bytes.Buffer) ([]byte, error) {
	length, err := readRDBLength(buf)
	if err != nil {
		return nil, err
	}
	if buf.Len() < int(length) {
		return nil, fmt.Errorf("unexpected end of buffer")
	}
	return buf.Next(int(length)), nil
}

// readRDBExpireTime 读取 RDB 过期时间
func readRDBExpireTime(buf *bytes.Buffer) (int64, bool) {
	if buf.Len() == 0 {
		return 0, false
	}
	expireType := buf.Next(1)[0]
	switch expireType {
	case 0xFC:
		var ms int64
		if err := binary.Read(buf, binary.LittleEndian, &ms); err != nil {
			return 0, false
		}
		return ms, true
	case 0xFD:
		var sec int32
		if err := binary.Read(buf, binary.LittleEndian, &sec); err != nil {
			return 0, false
		}
		return int64(sec) * 1000, true
	}
	// 不是过期时间，将字节放回
	_ = buf.UnreadByte()
	return 0, false
}

// Restore 实现 Redis RESTORE 命令，反序列化键值（使用标准 RDB 格式）
func (s *BotreonStore) Restore(key string, serializedData []byte, ttl time.Duration, replace bool) error {
	// 检查键是否已存在
	exists, err := s.Exists(key)
	if err != nil {
		return err
	}
	if exists && !replace {
		return fmt.Errorf("ERR target key already exists")
	}

	// 删除已存在的键
	if exists {
		if _, err := s.Del(key); err != nil {
			return err
		}
	}

	// 使用 RDB 解码器解析数据
	buf := bytes.NewBuffer(serializedData)

	// 检查 RDB magic
	magic := make([]byte, 5)
	if _, err := buf.Read(magic); err != nil {
		return fmt.Errorf("ERR invalid RDB format: %v", err)
	}
	if string(magic) != "REDIS" {
		// 可能是旧格式，尝试向后兼容
		return s.restoreLegacy(key, serializedData, ttl, replace)
	}

	// 读取版本
	version := make([]byte, 4)
	if _, err := buf.Read(version); err != nil {
		return fmt.Errorf("ERR invalid RDB format: %v", err)
	}

	// 读取可选的过期时间
	var expireAt int64 = 0
	if buf.Len() > 0 {
		nextByte := buf.Bytes()[0]
		if nextByte == 0xFC || nextByte == 0xFD {
			if expireMS, ok := readRDBExpireTime(buf); ok {
				expireAt = expireMS
			}
		}
	}

	// 确定最终 TTL
	var finalTTL time.Duration
	if ttl > 0 {
		finalTTL = ttl
	} else if expireAt > 0 {
		now := time.Now().UnixMilli()
		if expireAt > now {
			finalTTL = time.Duration(expireAt-now) * time.Millisecond
		}
	}

	// 读取类型
	if buf.Len() == 0 {
		return fmt.Errorf("ERR invalid RDB format: unexpected end")
	}
	typeByte, _ := buf.ReadByte()

	switch typeByte {
	case 0: // STRING
		_, err := readRDBString(buf)
		if err != nil {
			return fmt.Errorf("ERR invalid RDB format: %v", err)
		}
		value, err := readRDBBytes(buf)
		if err != nil {
			return fmt.Errorf("ERR invalid RDB format: %v", err)
		}
		if finalTTL > 0 {
			return s.SetWithTTL(key, string(value), finalTTL)
		}
		return s.Set(key, string(value))

	case 1: // LIST
		_, err := readRDBString(buf)
		if err != nil {
			return fmt.Errorf("ERR invalid RDB format: %v", err)
		}
		length, err := readRDBLength(buf)
		if err != nil {
			return fmt.Errorf("ERR invalid RDB format: %v", err)
		}
		for i := uint64(0); i < length; i++ {
			val, err := readRDBString(buf)
			if err != nil {
				return fmt.Errorf("ERR invalid RDB format: %v", err)
			}
			if _, err := s.RPush(key, val); err != nil {
				return err
			}
		}
		if finalTTL > 0 {
			if _, err := s.PExpire(key, int64(finalTTL.Milliseconds())); err != nil {
				return err
			}
		}
		return nil

	case 2: // SET
		_, err := readRDBString(buf)
		if err != nil {
			return fmt.Errorf("ERR invalid RDB format: %v", err)
		}
		length, err := readRDBLength(buf)
		if err != nil {
			return fmt.Errorf("ERR invalid RDB format: %v", err)
		}
		for i := uint64(0); i < length; i++ {
			member, err := readRDBString(buf)
			if err != nil {
				return fmt.Errorf("ERR invalid RDB format: %v", err)
			}
			if _, err := s.SAdd(key, member); err != nil {
				return err
			}
		}
		if finalTTL > 0 {
			if _, err := s.PExpire(key, int64(finalTTL.Milliseconds())); err != nil {
				return err
			}
		}
		return nil

	case 3: // HASH
		_, err := readRDBString(buf)
		if err != nil {
			return fmt.Errorf("ERR invalid RDB format: %v", err)
		}
		length, err := readRDBLength(buf)
		if err != nil {
			return fmt.Errorf("ERR invalid RDB format: %v", err)
		}
		for i := uint64(0); i < length; i++ {
			field, err := readRDBString(buf)
			if err != nil {
				return fmt.Errorf("ERR invalid RDB format: %v", err)
			}
			value, err := readRDBBytes(buf)
			if err != nil {
				return fmt.Errorf("ERR invalid RDB format: %v", err)
			}
			if err := s.HSet(key, field, string(value)); err != nil {
				return err
			}
		}
		if finalTTL > 0 {
			if _, err := s.PExpire(key, int64(finalTTL.Milliseconds())); err != nil {
				return err
			}
		}
		return nil

	case 4: // ZSET
		_, err := readRDBString(buf)
		if err != nil {
			return fmt.Errorf("ERR invalid RDB format: %v", err)
		}
		length, err := readRDBLength(buf)
		if err != nil {
			return fmt.Errorf("ERR invalid RDB format: %v", err)
		}
		members := make([]ZSetMember, 0, length)
		for i := uint64(0); i < length; i++ {
			member, err := readRDBString(buf)
			if err != nil {
				return fmt.Errorf("ERR invalid RDB format: %v", err)
			}
			scoreBytes, err := readRDBBytes(buf)
			if err != nil {
				return fmt.Errorf("ERR invalid RDB format: %v", err)
			}
			score, _ := strconv.ParseFloat(string(scoreBytes), 64)
			members = append(members, ZSetMember{Member: member, Score: score})
		}
		if len(members) > 0 {
			if err := s.ZAdd(key, members); err != nil {
				return err
			}
		}
		if finalTTL > 0 {
			if _, err := s.PExpire(key, int64(finalTTL.Milliseconds())); err != nil {
				return err
			}
		}
		return nil

	default:
		return fmt.Errorf("ERR unsupported RDB type: %d", typeByte)
	}
}

// restoreLegacy 恢复旧格式的序列化数据（兼容旧版本）
func (s *BotreonStore) restoreLegacy(key string, serializedData []byte, ttl time.Duration, replace bool) error {
	dataStr := string(serializedData)
	colonIndex := strings.Index(dataStr, ":")
	if colonIndex == -1 {
		return fmt.Errorf("ERR invalid serialized data")
	}

	keyType := dataStr[:colonIndex]
	data := dataStr[colonIndex+1:]

	switch keyType {
	case "string":
		if ttl > 0 {
			return s.SetWithTTL(key, data, ttl)
		}
		return s.Set(key, data)
	case "list":
		if data != "" {
			elems := strings.Split(data, ",")
			_, err := s.LPush(key, elems...)
			return err
		}
		return nil
	case "hash":
		if data != "" {
			fields := strings.Split(data, ",")
			for _, field := range fields {
				eqIndex := strings.Index(field, "=")
				if eqIndex == -1 {
					continue
				}
				fieldName := field[:eqIndex]
				fieldValue := field[eqIndex+1:]
				if err := s.HSet(key, fieldName, fieldValue); err != nil {
					return err
				}
			}
		}
		return nil
	case "set":
		if data != "" {
			members := strings.Split(data, ",")
			for _, member := range members {
				if _, err := s.SAdd(key, member); err != nil {
					return err
				}
			}
		}
		return nil
	default:
		return fmt.Errorf("ERR unsupported key type: %s", keyType)
	}
}

// getListData 获取列表的所有数据（用于 DUMP）
func (s *BotreonStore) getListData(key string) ([]string, error) {
	length, err := s.LLen(key)
	if err != nil {
		return nil, err
	}
	if length == 0 {
		return []string{}, nil
	}

	var elements []string
	for i := uint64(0); i < length; i++ {
		val, err := s.LIndex(key, int64(i))
		if err != nil {
			return nil, err
		}
		elements = append(elements, val)
	}
	return elements, nil
}

// Time 实现 Redis TIME 命令，返回服务器当前时间
func (s *BotreonStore) Time() (int64, int64, error) {
	now := time.Now()
	sec := now.Unix()
	usec := int64(now.Nanosecond() / 1000)
	return sec, usec, nil
}

// nextStartup 在服务启动时执行，恢复数据状态
// 功能：
// 1. 清理过期键
// 2. 清理孤立数据（没有TYPE_键的数据）
// 3. 清理孤立TYPE_键（没有对应数据的TYPE_键）
func (s *BotreonStore) NextStartup() error {
	return s.retryUpdate(func(txn *badger.Txn) error {
		// 1. 清理孤立TYPE_键（没有对应数据的TYPE_键）
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		prefix := prefixKeyTypeBytes
		for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
			item := iter.Item()
			keyBytes := item.KeyCopy(nil)
			key := string(keyBytes[len(prefixKeyTypeBytes):])

			// 获取类型
			val, err := item.ValueCopy(nil)
			if err != nil {
				continue
			}
			keyType := string(val)

			// 检查对应的数据是否存在
			exists, err := s.checkDataExists(txn, key, keyType)
			if err != nil {
				continue
			}
			if !exists {
				// 删除孤立TYPE_键
				if err := txn.Delete(keyBytes); err != nil {
					// 记录日志但继续处理
					continue
				}
			}
		}

		// 2. 清理孤立数据（没有TYPE_键的数据）
		// cleanupOrphaned*Data functions always return nil (internal errors use 'continue')
		_ = cleanupOrphanedData(txn, []byte("string:"))
		_ = cleanupOrphanedListData(txn)
		_ = cleanupOrphanedHashData(txn)
		_ = cleanupOrphanedSetData(txn)
		_ = cleanupOrphanedZSetData(txn)

		return nil
	}, 30)
}

// checkDataExists 检查指定键的数据是否存在
func (s *BotreonStore) checkDataExists(txn *badger.Txn, key, keyType string) (bool, error) {
	switch keyType {
	case KeyTypeString:
		strKey := s.stringKey(key)
		_, err := txn.Get([]byte(strKey))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return false, nil
		}
		return err == nil, err
	case KeyTypeList:
		// List检查length键
		lengthKey := []byte(fmt.Sprintf("%s:%s:length", KeyTypeList, key))
		_, err := txn.Get(lengthKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return false, nil
		}
		return err == nil, err
	case KeyTypeHash:
		// Hash检查count键
		countKey := s.hashCountKey(key)
		_, err := txn.Get(countKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return false, nil
		}
		return err == nil, err
	case KeyTypeSet:
		// Set检查count键
		countKey := s.setKey(key, "count")
		_, err := txn.Get([]byte(countKey))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return false, nil
		}
		return err == nil, err
	case "zset":
		// SortedSet检查meta键
		metaKey := sortedSetKeyMeta(key)
		_, err := txn.Get(metaKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return false, nil
		}
		return err == nil, err
	case KeyTypeJSON:
		// JSON检查json键
		jsonKey := []byte(s.jsonKey(key))
		_, err := txn.Get(jsonKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return false, nil
		}
		return err == nil, err
	case KeyTypeTimeSeries:
		// TimeSeries检查meta键
		metaKey := tsMetaKey(key)
		_, err := txn.Get(metaKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return false, nil
		}
		return err == nil, err
	default:
		return true, nil
	}
}

// cleanupOrphanedData 清理没有TYPE_键的String数据
func cleanupOrphanedData(txn *badger.Txn, dataPrefix []byte) error {
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()

	for iter.Seek(dataPrefix); iter.ValidForPrefix(dataPrefix); iter.Next() {
		item := iter.Item()
		keyBytes := item.KeyCopy(nil)

		// 从key中提取实际键名
		// 格式: string:key
		key := string(keyBytes[len("string:"):])

		// 检查TYPE_键是否存在
		typeKey := TypeOfKeyGet(key)
		_, err := txn.Get(typeKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			// 没有TYPE_键，删除这个数据
			if err := txn.Delete(keyBytes); err != nil {
				continue
			}
		}
	}
	return nil
}

// cleanupOrphanedListData 清理没有TYPE_键的List数据
func cleanupOrphanedListData(txn *badger.Txn) error {
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()

	prefix := []byte(fmt.Sprintf("%s:", KeyTypeList))
	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		item := iter.Item()
		keyBytes := item.KeyCopy(nil)
		keyStr := string(keyBytes)

		// 提取键名：list:key:field -> key
		// 格式: list:key:length 或 list:key:index
		parts := strings.SplitN(keyStr, ":", 3)
		if len(parts) < 2 {
			continue
		}
		key := parts[1]

		// 检查TYPE_键是否存在
		typeKey := TypeOfKeyGet(key)
		_, err := txn.Get(typeKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			// 删除整个list的数据
			listPrefix := []byte(fmt.Sprintf("%s:%s:", KeyTypeList, key))
			if err := deleteByPrefix(txn, listPrefix); err != nil {
				continue
			}
		}
	}
	return nil
}

// cleanupOrphanedHashData 清理没有TYPE_键的Hash数据
func cleanupOrphanedHashData(txn *badger.Txn) error {
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()

	prefix := []byte(fmt.Sprintf("%s:", KeyTypeHash))
	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		item := iter.Item()
		keyBytes := item.KeyCopy(nil)
		keyStr := string(keyBytes)

		// 格式: hash:key:field 或 hash:key:count
		parts := strings.SplitN(keyStr, ":", 3)
		if len(parts) < 2 {
			continue
		}
		key := parts[1]

		// 检查TYPE_键是否存在
		typeKey := TypeOfKeyGet(key)
		_, err := txn.Get(typeKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			// 删除整个hash的数据
			hashPrefix := []byte(fmt.Sprintf("%s:%s:", KeyTypeHash, key))
			if err := deleteByPrefix(txn, hashPrefix); err != nil {
				continue
			}
		}
	}
	return nil
}

// cleanupOrphanedSetData 清理没有TYPE_键的Set数据
func cleanupOrphanedSetData(txn *badger.Txn) error {
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()

	prefix := []byte(fmt.Sprintf("%s:", KeyTypeSet))
	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		item := iter.Item()
		keyBytes := item.KeyCopy(nil)
		keyStr := string(keyBytes)

		// 格式: set:key:member 或 set:key:count
		parts := strings.SplitN(keyStr, ":", 3)
		if len(parts) < 2 {
			continue
		}
		key := parts[1]

		// 检查TYPE_键是否存在
		typeKey := TypeOfKeyGet(key)
		_, err := txn.Get(typeKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			// 删除整个set的数据
			setPrefix := []byte(fmt.Sprintf("%s:%s:", KeyTypeSet, key))
			if err := deleteByPrefix(txn, setPrefix); err != nil {
				continue
			}
		}
	}
	return nil
}

// cleanupOrphanedZSetData 清理没有TYPE_键的SortedSet数据
func cleanupOrphanedZSetData(txn *badger.Txn) error {
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()

	prefix := []byte(prefixKeySortedSetBytes)
	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		item := iter.Item()
		keyBytes := item.KeyCopy(nil)
		keyStr := string(keyBytes)

		// 格式: zset:key:meta, zset:key:index:*, zset:key:data:member
		// 提取键名
		parts := strings.SplitN(keyStr, ":", 3)
		if len(parts) < 2 {
			continue
		}
		key := parts[1]

		// 检查TYPE_键是否存在
		typeKey := TypeOfKeyGet(key)
		_, err := txn.Get(typeKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			// 删除整个zset的数据
			zsetPrefix := []byte(fmt.Sprintf("%s%s:", prefixKeySortedSetBytes, key))
			if err := deleteByPrefix(txn, zsetPrefix); err != nil {
				continue
			}
		}
	}
	return nil
}

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
