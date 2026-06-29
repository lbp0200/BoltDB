package store

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// ErrKeyNotFound 表示键不存在
var ErrKeyNotFound = errors.New("key not found")

// stringKey 方法用于生成存储在 Badger 数据库中的键
func (s *BotreonStore) stringKey(key string) string {
	return fmt.Sprintf("%s:%s", KeyTypeString, key)
}

// Set 实现 Redis SET 命令
func (s *BotreonStore) Set(key string, value string) error {
	if err := s.checkErrorInjector("Set"); err != nil {
		return err
	}
	err := s.retryUpdate(func(txn *badger.Txn) error {
		// Check if key already exists with a different type
		badgerTypeKey := TypeOfKeyGet(key)
		item, err := txn.Get(badgerTypeKey)
		if err == nil {
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			keyType := string(val)
			if keyType != "" && keyType != KeyTypeString {
				return ErrWrongType
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}

		if err := txn.Set(badgerTypeKey, []byte(KeyTypeString)); err != nil {
			return err
		}
		strKey := s.stringKey(key)
		return s.setValueWithCompression(txn, []byte(strKey), []byte(value))
	}, 30)
	if err == nil && s.readCache != nil {
		s.readCache.Set(key, []byte(value))
	}
	return err
}

// SetWithTTL 字符串操作，设置键值对并设置过期时间
func (s *BotreonStore) SetWithTTL(key, value string, ttl time.Duration) error {
	err := s.retryUpdate(func(txn *badger.Txn) error {
		// Check if key already exists with a different type
		typeKey := TypeOfKeyGet(key)
		item, err := txn.Get(typeKey)
		if err == nil {
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			keyType := string(val)
			if keyType != "" && keyType != KeyTypeString {
				return ErrWrongType
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}

		if err := txn.Set(TypeOfKeyGet(key), []byte(KeyTypeString)); err != nil {
			return err
		}
		strKey := s.stringKey(key)
		return s.setEntryWithCompression(txn, []byte(strKey), []byte(value), ttl)
	}, 30)
	if err == nil && s.readCache != nil {
		s.readCache.Set(key, []byte(value))
	}
	return err
}

// SetEX 实现 Redis SETEX 命令，设置键值对并设置过期时间（秒）
func (s *BotreonStore) SetEX(key string, value string, seconds int) error {
	return s.SetWithTTL(key, value, time.Duration(seconds)*time.Second)
}

// PSETEX 实现 Redis PSETEX 命令，设置键值对并设置过期时间（毫秒）
func (s *BotreonStore) PSETEX(key string, value string, milliseconds int64) error {
	return s.SetWithTTL(key, value, time.Duration(milliseconds)*time.Millisecond)
}

// SetNX 实现 Redis SETNX 命令，仅当键不存在时设置
func (s *BotreonStore) SetNX(key string, value string) (bool, error) {
	success := false
	err := s.retryUpdate(func(txn *badger.Txn) error {
		success = false // reset each attempt; stale value must not survive conflict retry
		strKey := s.stringKey(key)
		_, err := txn.Get([]byte(strKey))
		if err == nil {
			// 键已存在
			return nil
		}
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		// 键不存在，可以设置
		if err := txn.Set(TypeOfKeyGet(key), []byte(KeyTypeString)); err != nil {
			return err
		}
		if err := s.setValueWithCompression(txn, []byte(strKey), []byte(value)); err != nil {
			return err
		}
		success = true
		return nil
	}, 30)
	return success, err
}

// GetSet 实现 Redis GETSET 命令，设置新值并返回旧值
func (s *BotreonStore) GetSet(key string, value string) (string, error) {
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)

	var oldValue string
	err := s.retryUpdate(func(txn *badger.Txn) error {
		oldValue = "" // reset each attempt; stale value must not survive conflict retry
		strKey := s.stringKey(key)
		item, err := txn.Get([]byte(strKey))
		if err == nil {
			val, err := s.getValueWithDecompression(item)
			if err != nil {
				return err
			}
			oldValue = string(val)
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		if err := txn.Set(TypeOfKeyGet(key), []byte(KeyTypeString)); err != nil {
			return err
		}
		return s.setValueWithCompression(txn, []byte(strKey), []byte(value))
	}, 30)
	if err != nil {
		return "", err
	}
	if s.readCache != nil {
		s.readCache.Set(key, []byte(value))
	}
	return oldValue, nil
}

// MGet 实现 Redis MGET 命令，获取多个键的值
func (s *BotreonStore) MGet(keys ...string) ([]string, error) {
	values := make([]string, len(keys))
	err := s.db.View(func(txn *badger.Txn) error {
		for i, key := range keys {
			// Check type first to avoid returning garbled data for non-string keys
			typeKey := TypeOfKeyGet(key)
			item, err := txn.Get(typeKey)
			if err != nil {
				if errors.Is(err, badger.ErrKeyNotFound) {
					values[i] = "" // 键不存在返回空字符串
					continue
				}
				return err
			}
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			keyType := string(val)
			if keyType != "" && keyType != KeyTypeString {
				return ErrWrongType
			}

			// Read the string value
			strKey := s.stringKey(key)
			item, err = txn.Get([]byte(strKey))
			if err != nil {
				if errors.Is(err, badger.ErrKeyNotFound) {
					values[i] = "" // 键不存在返回空字符串
					continue
				}
				return err
			}
			val, err = s.getValueWithDecompression(item)
			if err != nil {
				return err
			}
			values[i] = string(val)
		}
		return nil
	})
	return values, err
}

// MSet 实现 Redis MSET 命令，设置多个键值对
func (s *BotreonStore) MSet(keyValues ...string) error {
	if len(keyValues)%2 != 0 {
		return errors.New("MSET requires an even number of arguments")
	}
	return s.retryUpdate(func(txn *badger.Txn) error {
		for i := 0; i < len(keyValues); i += 2 {
			key := keyValues[i]
			value := keyValues[i+1]
			if err := txn.Set(TypeOfKeyGet(key), []byte(KeyTypeString)); err != nil {
				return err
			}
			strKey := s.stringKey(key)
			if err := txn.Set([]byte(strKey), []byte(value)); err != nil {
				return err
			}
		}
		return nil
	}, 30)
}

// MSetNX 实现 Redis MSETNX 命令，仅当所有键都不存在时设置多个键值对
func (s *BotreonStore) MSetNX(keyValues ...string) (bool, error) {
	if len(keyValues)%2 != 0 {
		return false, errors.New("MSETNX requires an even number of arguments")
	}
	success := false
	err := s.retryUpdate(func(txn *badger.Txn) error {
		success = false // reset each attempt; stale value must not survive conflict retry
		// 先检查所有键是否都不存在
		for i := 0; i < len(keyValues); i += 2 {
			key := keyValues[i]
			strKey := s.stringKey(key)
			_, err := txn.Get([]byte(strKey))
			if err == nil {
				// 至少有一个键存在，不能设置
				return nil
			}
			if !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
		}
		// 所有键都不存在，可以设置
		for i := 0; i < len(keyValues); i += 2 {
			key := keyValues[i]
			value := keyValues[i+1]
			if err := txn.Set(TypeOfKeyGet(key), []byte(KeyTypeString)); err != nil {
				return err
			}
			strKey := s.stringKey(key)
			if err := txn.Set([]byte(strKey), []byte(value)); err != nil {
				return err
			}
		}
		success = true
		return nil
	}, 30)
	return success, err
}

// Get 实现 Redis GET 命令
func (s *BotreonStore) Get(key string) (string, error) {
	if err := s.checkErrorInjector("Get"); err != nil {
		return "", err
	}
	// 先检查读缓存
	if s.readCache != nil {
		if cachedValue, found := s.readCache.Get(key); found {
			return string(cachedValue), nil
		}
	}

	var val string
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(TypeOfKeyGet(key))
		if err == nil {
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			keyType := string(val)
			if keyType != "" && keyType != KeyTypeString {
				return ErrWrongType
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		strKey := s.stringKey(key)
		item2, err := txn.Get([]byte(strKey))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return ErrKeyNotFound // 返回特定错误表示键不存在
			}
			return err
		}
		valBytes, err := s.getValueWithDecompression(item2)
		if err != nil {
			return err
		}
		val = string(valBytes)
		return nil
	})

	// 如果成功，更新读缓存
	if err == nil && s.readCache != nil {
		s.readCache.Set(key, []byte(val))
	}

	if errors.Is(err, ErrKeyNotFound) {
		return "", ErrKeyNotFound
	}
	return val, err
}

// getIntValue 获取整数值，如果键不存在或不是整数，返回0和错误
func (s *BotreonStore) getIntValue(txn *badger.Txn, key string) (int64, error) {
	strKey := s.stringKey(key)
	item, err := txn.Get([]byte(strKey))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return 0, nil // 键不存在时返回0
		}
		return 0, err
	}
	val, err := s.getValueWithDecompression(item)
	if err != nil {
		return 0, err
	}
	intVal, err := strconv.ParseInt(string(val), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("value is not an integer")
	}
	return intVal, nil
}

// setIntValue 设置整数值
func (s *BotreonStore) setIntValue(txn *badger.Txn, key string, value int64) error {
	if err := txn.Set(TypeOfKeyGet(key), []byte(KeyTypeString)); err != nil {
		return err
	}
	strKey := s.stringKey(key)
	if err := txn.Set([]byte(strKey), []byte(strconv.FormatInt(value, 10))); err != nil {
		return err
	}
	// Update readCache to maintain consistency
	if s.readCache != nil {
		s.readCache.Set(key, []byte(strconv.FormatInt(value, 10)))
	}
	return nil
}

// INCR 实现 Redis INCR 命令，将键的值加1
func (s *BotreonStore) INCR(key string) (int64, error) {
	return s.INCRBY(key, 1)
}

// INCRBY 实现 Redis INCRBY 命令，将键的值增加指定整数
func (s *BotreonStore) INCRBY(key string, increment int64) (int64, error) {
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)

	var newValue int64
	err := s.retryUpdate(func(txn *badger.Txn) error {
		newValue = 0 // reset each attempt; stale value must not survive conflict retry
		// Check if key exists with a different type before overwriting
		typeKey := TypeOfKeyGet(key)
		item, err := txn.Get(typeKey)
		if err == nil {
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			keyType := string(val)
			if keyType != "" && keyType != KeyTypeString {
				return ErrWrongType
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}

		oldValue, err := s.getIntValue(txn, key)
		if err != nil {
			return err
		}
		// Check for overflow/underflow
		if increment > 0 && oldValue > math.MaxInt64-increment {
			return fmt.Errorf("increment overflow")
		}
		if increment < 0 && oldValue < math.MinInt64-increment {
			return fmt.Errorf("increment overflow")
		}
		newValue = oldValue + increment
		return s.setIntValue(txn, key, newValue)
	}, 30)
	return newValue, err
}

// DECR 实现 Redis DECR 命令，将键的值减1
func (s *BotreonStore) DECR(key string) (int64, error) {
	return s.INCRBY(key, -1)
}

// DECRBY 实现 Redis DECRBY 命令，将键的值减少指定整数
func (s *BotreonStore) DECRBY(key string, decrement int64) (int64, error) {
	return s.INCRBY(key, -decrement)
}

// INCRBYFLOAT 实现 Redis INCRBYFLOAT 命令，将键的值增加指定浮点数
func (s *BotreonStore) INCRBYFLOAT(key string, increment float64) (float64, error) {
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)

	var newValue float64
	var newValueStr string
	err := s.retryUpdate(func(txn *badger.Txn) error {
		newValue = 0
		newValueStr = "" // reset each attempt; stale value must not survive conflict retry
		strKey := s.stringKey(key)
		item, err := txn.Get([]byte(strKey))
		var oldFloat float64
		if err == nil {
			val, err := s.getValueWithDecompression(item)
			if err != nil {
				return err
			}
			oldFloat, err = strconv.ParseFloat(string(val), 64)
			if err != nil {
				return errors.New("value is not a valid float")
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		newValue = oldFloat + increment
		if math.IsInf(newValue, 0) || math.IsNaN(newValue) {
			return fmt.Errorf("increment would produce NaN or Infinity")
		}
		newValueStr = strconv.FormatFloat(newValue, 'f', -1, 64)
		if err := txn.Set(TypeOfKeyGet(key), []byte(KeyTypeString)); err != nil {
			return err
		}
		return s.setValueWithCompression(txn, []byte(strKey), []byte(newValueStr))
	}, 30)
	if err != nil {
		return 0, err
	}
	if s.readCache != nil {
		s.readCache.Set(key, []byte(newValueStr))
	}
	return newValue, nil
}

// APPEND 实现 Redis APPEND 命令，追加字符串
func (s *BotreonStore) APPEND(key string, value string) (int, error) {
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)

	// Single transaction: read old value + write new value (eliminates View→Update gap)
	var newLength int
	var finalValue string
	err := s.retryUpdate(func(txn *badger.Txn) error {
		strKey := s.stringKey(key)
		var existingValue string
		item, err := txn.Get([]byte(strKey))
		if err == nil {
			val, vErr := s.getValueWithDecompression(item)
			if vErr != nil {
				return vErr
			}
			existingValue = string(val)
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}

		newValue := existingValue + value
		newLength = len(newValue)

		// Check type
		badgerTypeKey := TypeOfKeyGet(key)
		typeItem, tErr := txn.Get(badgerTypeKey)
		if tErr == nil {
			typeVal, vErr := typeItem.ValueCopy(nil)
			if vErr != nil {
				return vErr
			}
			keyType := string(typeVal)
			if keyType != "" && keyType != KeyTypeString {
				return ErrWrongType
			}
		} else if !errors.Is(tErr, badger.ErrKeyNotFound) {
			return tErr
		}

		if err := txn.Set(badgerTypeKey, []byte(KeyTypeString)); err != nil {
			return err
		}
		finalValue = newValue
		return s.setValueWithCompression(txn, []byte(strKey), []byte(newValue))
	}, 30)
	if err == nil && s.readCache != nil {
		s.readCache.Set(key, []byte(finalValue))
	}
	return newLength, err
}

// StrLen 实现 Redis STRLEN 命令，获取字符串长度
func (s *BotreonStore) StrLen(key string) (int, error) {
	var length int
	err := s.db.View(func(txn *badger.Txn) error {
		if err := checkKeyType(txn, key, KeyTypeString); err != nil {
			return err
		}
		strKey := s.stringKey(key)
		item, err := txn.Get([]byte(strKey))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				length = 0
				return nil
			}
			return err
		}
		val, err := s.getValueWithDecompression(item)
		if err != nil {
			return err
		}
		length = len(string(val))
		return nil
	})
	return length, err
}

// GetRange 实现 Redis GETRANGE 命令，获取字符串的子串
func (s *BotreonStore) GetRange(key string, start, end int) (string, error) {
	var result string
	err := s.db.View(func(txn *badger.Txn) error {
		if err := checkKeyType(txn, key, KeyTypeString); err != nil {
			return err
		}
		strKey := s.stringKey(key)
		item, err := txn.Get([]byte(strKey))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				result = ""
				return nil
			}
			return err
		}
		val, err := s.getValueWithDecompression(item)
		if err != nil {
			return err
		}
		str := string(val)
		strLen := len(str)
		// 处理负数索引
		if start < 0 {
			start = strLen + start
			if start < 0 {
				start = 0
			}
		}
		if end < 0 {
			end = strLen + end
			if end < 0 {
				end = -1
			}
		}
		if start > strLen {
			result = ""
			return nil
		}
		if end >= strLen {
			end = strLen - 1
		}
		if start > end {
			result = ""
			return nil
		}
		result = str[start : end+1]
		return nil
	})
	return result, err
}

// SetRange 实现 Redis SETRANGE 命令，设置字符串的子串
func (s *BotreonStore) SetRange(key string, offset int, value string) (int, error) {
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)

	var newLength int
	err := s.retryUpdate(func(txn *badger.Txn) error {
		strKey := s.stringKey(key)
		item, err := txn.Get([]byte(strKey))
		var existingValue string
		if err == nil {
			val, err := s.getValueWithDecompression(item)
			if err != nil {
				return err
			}
			existingValue = string(val)
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		if offset > len(existingValue) {
			existingValue += string(make([]byte, offset-len(existingValue)))
		}
		var newValue string
		if offset > 0 {
			newValue = existingValue[:offset]
		}
		newValue += value
		if offset+len(value) < len(existingValue) {
			newValue += existingValue[offset+len(value):]
		}
		newLength = len(newValue)
		if err := txn.Set(TypeOfKeyGet(key), []byte(KeyTypeString)); err != nil {
			return err
		}
		return s.setValueWithCompression(txn, []byte(strKey), []byte(newValue))
	}, 30)
	if err != nil {
		return 0, err
	}
	if s.readCache != nil {
		s.readCache.Delete(key)
	}
	return newLength, nil
}

// getStringBytes 获取字符串的字节数组
func (s *BotreonStore) getStringBytes(txn *badger.Txn, key string) ([]byte, error) {
	strKey := s.stringKey(key)
	item, err := txn.Get([]byte(strKey))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return []byte{}, nil
		}
		return nil, err
	}
	val, err := item.ValueCopy(nil)
	if err != nil {
		return nil, err
	}
	return decompressData(val)
}

// GetBit 实现 Redis GETBIT 命令，获取指定位的值
func (s *BotreonStore) GetBit(key string, offset int) (int, error) {
	var bit int
	err := s.db.View(func(txn *badger.Txn) error {
		data, err := s.getStringBytes(txn, key)
		if err != nil {
			return err
		}
		byteIndex := offset / 8
		bitIndex := offset % 8
		if byteIndex >= len(data) {
			bit = 0
			return nil
		}
		if (data[byteIndex] & (1 << (7 - bitIndex))) != 0 {
			bit = 1
		} else {
			bit = 0
		}
		return nil
	})
	return bit, err
}

// SetBit 实现 Redis SETBIT 命令，设置指定位的值
func (s *BotreonStore) SetBit(key string, offset int, value int) (int, error) {
	// 清除读缓存
	if s.readCache != nil {
		s.readCache.Delete(key)
	}
	var oldBit int
	err := s.retryUpdate(func(txn *badger.Txn) error {
		data, err := s.getStringBytes(txn, key)
		if err != nil {
			return err
		}
		byteIndex := offset / 8
		bitIndex := offset % 8
		// 扩展数据如果需要
		if byteIndex >= len(data) {
			newData := make([]byte, byteIndex+1)
			copy(newData, data)
			data = newData
		}
		// 获取旧位值
		if (data[byteIndex] & (1 << (7 - bitIndex))) != 0 {
			oldBit = 1
		} else {
			oldBit = 0
		}
		// 设置新位值
		if value == 1 {
			data[byteIndex] |= (1 << (7 - bitIndex))
		} else {
			data[byteIndex] &^= (1 << (7 - bitIndex))
		}
		// 保存（带压缩）
		if err := txn.Set(TypeOfKeyGet(key), []byte(KeyTypeString)); err != nil {
			return err
		}
		strKey := s.stringKey(key)
		return s.setValueWithCompression(txn, []byte(strKey), data)
	}, 30)
	return oldBit, err
}

// BitCount 实现 Redis BITCOUNT 命令，计算字符串中1的位数
func (s *BotreonStore) BitCount(key string, start, end int) (int, error) {
	var count int
	err := s.db.View(func(txn *badger.Txn) error {
		data, err := s.getStringBytes(txn, key)
		if err != nil {
			return err
		}
		if len(data) == 0 {
			return nil
		}
		// 处理负数索引
		if start < 0 {
			start = len(data) + start
			if start < 0 {
				start = 0
			}
		}
		if end < 0 {
			end = len(data) + end
			if end < 0 {
				end = -1
			}
		}
		if start > len(data) {
			return nil
		}
		if end >= len(data) {
			end = len(data) - 1
		}
		if start > end {
			return nil
		}
		// 计算指定范围内的1的位数
		for i := start; i <= end; i++ {
			for j := 0; j < 8; j++ {
				if (data[i] & (1 << (7 - j))) != 0 {
					count++
				}
			}
		}
		return nil
	})
	return count, err
}

// BitOp 实现 Redis BITOP 命令，位操作
func (s *BotreonStore) BitOp(op string, destKey string, keys ...string) (int, error) {
	// 清除读缓存
	if s.readCache != nil {
		s.readCache.Delete(destKey)
	}
	var resultLength int
	err := s.retryUpdate(func(txn *badger.Txn) error {
		if len(keys) == 0 {
			return errors.New("BITOP requires at least one source key")
		}
		// 获取所有源键的数据
		sources := make([][]byte, len(keys))
		maxLen := 0
		for i, key := range keys {
			data, err := s.getStringBytes(txn, key)
			if err != nil {
				return err
			}
			sources[i] = data
			if len(data) > maxLen {
				maxLen = len(data)
			}
		}
		if maxLen == 0 {
			// 所有键都为空，结果也为空
			if err := txn.Set(TypeOfKeyGet(destKey), []byte(KeyTypeString)); err != nil {
				return err
			}
			strKey := s.stringKey(destKey)
			return txn.Set([]byte(strKey), []byte{})
		}
		// 执行位操作
		result := make([]byte, maxLen)
		for i := 0; i < maxLen; i++ {
			var byteVal byte
			switch op {
			case "AND":
				byteVal = 0xFF
				for _, src := range sources {
					var b byte
					if i < len(src) {
						b = src[i]
					}
					byteVal &= b
				}
			case "OR":
				for _, src := range sources {
					var b byte
					if i < len(src) {
						b = src[i]
					}
					byteVal |= b
				}
			case "XOR":
				for _, src := range sources {
					var b byte
					if i < len(src) {
						b = src[i]
					}
					byteVal ^= b
				}
			case "NOT":
				if len(keys) != 1 {
					return errors.New("BITOP NOT requires exactly one source key")
				}
				var b byte
				if i < len(sources[0]) {
					b = sources[0][i]
				}
				byteVal = ^b
			default:
				return fmt.Errorf("unknown bitop operation: %s", op)
			}
			result[i] = byteVal
		}
		resultLength = len(result)
		// 保存结果
		if err := txn.Set(TypeOfKeyGet(destKey), []byte(KeyTypeString)); err != nil {
			return err
		}
		strKey := s.stringKey(destKey)
		return txn.Set([]byte(strKey), result)
	}, 30)
	return resultLength, err
}

// BitPos 实现 Redis BITPOS 命令，查找第一个设置或清除的位
func (s *BotreonStore) BitPos(key string, bit int, start, end int) (int, error) {
	pos := -1
	err := s.db.View(func(txn *badger.Txn) error {
		data, err := s.getStringBytes(txn, key)
		if err != nil {
			return err
		}
		if len(data) == 0 {
			if bit == 1 {
				pos = -1
			} else {
				pos = 0
			}
			return nil
		}
		// 处理负数索引
		if start < 0 {
			start = len(data) + start
			if start < 0 {
				start = 0
			}
		}
		if end < 0 {
			end = len(data) + end
			if end < 0 {
				end = -1
			}
		}
		if start > len(data) {
			pos = -1
			return nil
		}
		if end >= len(data) {
			end = len(data) - 1
		}
		if start > end {
			pos = -1
			return nil
		}
		// 查找指定位
		for i := start; i <= end; i++ {
			for j := 0; j < 8; j++ {
				bitOffset := i*8 + j
				currentBit := 0
				if (data[i] & (1 << (7 - j))) != 0 {
					currentBit = 1
				}
				if currentBit == bit {
					pos = bitOffset
					return nil
				}
			}
		}
		// 未找到
		pos = -1
		return nil
	})
	return pos, err
}

// BitLen 实现 Redis BITLEN 命令，获取字符串的位长度
func (s *BotreonStore) BitLen(key string) (int, error) {
	var length int
	err := s.db.View(func(txn *badger.Txn) error {
		data, err := s.getStringBytes(txn, key)
		if err != nil {
			return err
		}
		length = len(data) * 8
		return nil
	})
	return length, err
}

// BitFieldResult represents the result of a BITFIELD operation
type BitFieldResult struct {
	Value       int64 // The result value (nil if overflow)
	Overshifted bool  // Whether the result was overflowed/undershifted
}

// BitFieldResultWithOverflow wraps a result with overflow information
type BitFieldResultWithOverflow struct {
	Value    int64
	Overflow string // "overflow", "sat", "fail"
}

// parseBitFieldType parses a type string like "i8", "u16", "i32", "u64" etc.
func parseBitFieldType(typeStr string) (isSigned bool, bits int, err error) {
	if len(typeStr) < 2 {
		return false, 0, fmt.Errorf("invalid type: %s", typeStr)
	}
	switch typeStr[0] {
	case 'i':
		isSigned = true
	case 'u':
		isSigned = false
	default:
		return false, 0, fmt.Errorf("invalid type: %s", typeStr)
	}
	bits, err = strconv.Atoi(typeStr[1:])
	if err != nil {
		return false, 0, fmt.Errorf("invalid bit count: %s", typeStr)
	}
	if bits <= 0 || bits > 64 {
		return false, 0, fmt.Errorf("invalid bit count: %d", bits)
	}
	return isSigned, bits, nil
}

// BitField implements the BITFIELD command
// BITFIELD key [GET type offset | SET type offset value | INCRBY type offset increment] ...
func (s *BotreonStore) BitField(key string, operations []string) ([]interface{}, error) {
	// Parse operations
	type operation struct {
		op       string // "GET", "SET", "INCRBY"
		isSigned bool
		bits     int
		offset   int64
		value    int64
	}

	ops := make([]operation, 0, len(operations))
	for i := 0; i < len(operations); {
		opName := operations[i]
		i++

		if i >= len(operations) {
			return nil, fmt.Errorf("BITFIELD: missing arguments for %s", opName)
		}

		// Next argument should be the type (e.g., "u8", "i16")
		typeStr := operations[i]
		i++

		isSigned, bits, err := parseBitFieldType(typeStr)
		if err != nil {
			return nil, err
		}

		if i >= len(operations) {
			return nil, fmt.Errorf("BITFIELD: missing offset for %s %s", opName, typeStr)
		}

		offsetStr := operations[i]
		i++

		offset, err := strconv.ParseInt(offsetStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("BITFIELD: invalid offset: %s", offsetStr)
		}

		var value int64 = 0
		if opName == "SET" || opName == "INCRBY" {
			if i >= len(operations) {
				return nil, fmt.Errorf("BITFIELD: missing value for %s %s", opName, typeStr)
			}
			valueStr := operations[i]
			i++
			value, err = strconv.ParseInt(valueStr, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("BITFIELD: invalid value: %s", valueStr)
			}
		}

		ops = append(ops, operation{
			op:       opName,
			isSigned: isSigned,
			bits:     bits,
			offset:   offset,
			value:    value,
		})
	}

	// Execute operations in a transaction
	results := make([]interface{}, 0, len(ops))

	err := s.retryUpdate(func(txn *badger.Txn) error {
		// First, get the current value
		data, err := s.getStringBytes(txn, key)
		if err != nil {
			if errors.Is(err, ErrKeyNotFound) {
				// Key doesn't exist, treat as all zeros
				data = []byte{}
			} else {
				return err
			}
		}
		originalLen := len(data)
		var remainingTTL time.Duration

		for _, op := range ops {
			// Convert offset (can be negative, meaning from end)
			bitOffset := op.offset
			if bitOffset < 0 {
				// Negative offset means from the end
				// For simplicity, we don't support this for now
				return fmt.Errorf("BITFIELD: negative offset not supported")
			}

			// Calculate byte and bit positions
			byteIndex := int(bitOffset) / 8
			numBytes := (int(bitOffset) + int(op.bits) + 7) / 8

			// Ensure data is long enough
			if byteIndex >= len(data) {
				newData := make([]byte, numBytes)
				copy(newData, data)
				data = newData
			}

			// Extract value from data (LSB first: offset 0 = bit 0, offset 1 = bit 1, etc.)
			var extractedValue int64 = 0
			for j := 0; j < op.bits; j++ {
				currentBitOffset := bitOffset + int64(j)
				currentByteIndex := int(currentBitOffset) / 8
				currentBitIndex := uint(currentBitOffset) % 8
				if currentByteIndex < len(data) && (data[currentByteIndex]&(1<<currentBitIndex)) != 0 {
					extractedValue |= int64(1) << uint(j)
				}
			}

			// For signed types, convert from unsigned to signed
			if op.isSigned {
				// Check if the sign bit is set
				signBit := int64(1) << (uint(op.bits) - 1)
				if (extractedValue & signBit) != 0 {
					// Sign extend: fill all bits above op.bits with 1s
					// mask = all 1s for bits >= op.bits (e.g., 0xFF...80 for 8 bits)
					mask := ^(int64(1) << uint(op.bits)) + 1
					extractedValue = extractedValue | mask
				}
			}

			var resultValue int64
			switch op.op {
			case "GET":
				resultValue = extractedValue
				results = append(results, resultValue)
			case "SET":
				// Write value to data (LSB first)
				for j := 0; j < op.bits; j++ {
					currentBitOffset := bitOffset + int64(j)
					currentByteIndex := int(currentBitOffset) / 8
					currentBitIndex := uint(currentBitOffset) % 8
					bitValue := (op.value >> uint(j)) & 1
					if bitValue == 1 {
						data[currentByteIndex] |= (1 << currentBitIndex)
					} else {
						data[currentByteIndex] &^= (1 << currentBitIndex)
					}
				}
				resultValue = extractedValue
				results = append(results, resultValue)
			case "INCRBY":
				// Calculate new value with overflow handling
				// Default overflow mode is WRAP (wrap around)
				newValue := extractedValue + op.value

				// For WRAP mode (default), wrap around
				if op.isSigned {
					// Signed wrap around (modulo 2^N)
					bits := op.bits
					if bits < 64 {
						mod := int64(1) << uint(bits)
						newValue = newValue % mod
						if newValue >= int64(1)<<uint(bits-1) {
							newValue -= mod
						}
					}
					// For 64-bit, Go's int64 already wraps
				} else {
					maxVal := int64(1)<<uint(op.bits) - 1
					// For unsigned, wrap around naturally (modulo behavior)
					if newValue > maxVal {
						newValue = newValue % (maxVal + 1)
					} else if newValue < 0 {
						// For negative values, wrap to positive
						newValue = (maxVal + 1) - ((-newValue - 1) % (maxVal + 1))
					}
				}

				// Write new value to data (LSB first)
				for j := 0; j < op.bits; j++ {
					currentBitOffset := bitOffset + int64(j)
					currentByteIndex := int(currentBitOffset) / 8
					currentBitIndex := uint(currentBitOffset) % 8
					bitValue := (newValue >> uint(j)) & 1
					if bitValue == 1 {
						data[currentByteIndex] |= (1 << currentBitIndex)
					} else {
						data[currentByteIndex] &^= (1 << currentBitIndex)
					}
				}
				results = append(results, newValue)
			}

			// Update TTL if needed (compute remaining TTL before any deletion)
			if len(data) > originalLen {
				typeKey := TypeOfKeyGet(key)
				if item, err := txn.Get(typeKey); err == nil {
					val, err := item.ValueCopy(nil)
					if err != nil {
						return err
					}
					if string(val) == KeyTypeString {
						strKey := s.stringKey(key)
						if strItem, err := txn.Get([]byte(strKey)); err == nil {
							if expiresAt := strItem.ExpiresAt(); expiresAt > 0 {
								nowUnix := uint64(time.Now().Unix())
								if expiresAt > nowUnix*100 {
									// 纳秒格式
									nowNano := uint64(time.Now().UnixNano())
									if expiresAt > nowNano {
										remainingTTL = time.Duration(expiresAt - nowNano)
									}
								} else {
									// 秒格式
									if expiresAt > nowUnix {
										// #nosec G115 - expiresAt is within int64 range
										remainingTTL = time.Duration(expiresAt-nowUnix) * time.Second
									}
								}
							}
						}
					}
				}
			}
		}

		// Save the updated data (preserve TTL if there was one)
		if len(data) > 0 {
			strKey := s.stringKey(key)
			if err := txn.Set(TypeOfKeyGet(key), []byte(KeyTypeString)); err != nil {
				return err
			}
			if remainingTTL > 0 {
				if err := txn.Delete([]byte(strKey)); err != nil {
					return err
				}
				e := badger.NewEntry([]byte(strKey), data).WithTTL(remainingTTL)
				if err := txn.SetEntry(e); err != nil {
					return err
				}
			} else {
				if err := txn.Set([]byte(strKey), data); err != nil {
					return err
				}
			}
		}

		return nil
	}, 30)

	return results, err
}

// StringEntry 批量写入的一条记录
type StringEntry struct {
	Key   string
	Value string
	TTL   time.Duration
}

// SetStringBatch 批量写入多个字符串键值对
// 用于 RDB 加载等场景，减少事务数
func (s *BotreonStore) SetStringBatch(entries []StringEntry) error {
	if err := s.checkErrorInjector("SetStringBatch"); err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}

	if err := s.retryUpdate(func(txn *badger.Txn) error {
		for _, e := range entries {
			if err := txn.Set(TypeOfKeyGet(e.Key), []byte(KeyTypeString)); err != nil {
				return err
			}
			strKey := s.stringKey(e.Key)
			if err := s.setEntryWithCompression(txn, []byte(strKey), []byte(e.Value), e.TTL); err != nil {
				return err
			}
		}
		return nil
	}, 30); err != nil {
		return err
	}

	// 写入成功后清除缓存
	if s.readCache != nil {
		for _, e := range entries {
			s.readCache.Delete(e.Key)
		}
	}
	return nil
}
