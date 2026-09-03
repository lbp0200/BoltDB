package store

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/lbp0200/BoltDB/internal/helper"

	"github.com/dgraph-io/badger/v4"
)

// ErrWrongType is returned when operation is attempted on key holding wrong type
var ErrWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")

// ErrMemberNotFound is returned when member is not found in sorted set
var ErrMemberNotFound = errors.New("member not found")

// 修改 HSet 维护计数器
func (s *BotreonStore) HSet(key, field string, value interface{}) error {
	if err := s.checkErrorInjector("HSet"); err != nil {
		return err
	}
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)
	logFuncTag := "BotreonStoreHSet"
	// 将值转换为字符串（与Redis一致，Hash值都是字符串）
	var bValue []byte
	switch v := value.(type) {
	case string:
		bValue = []byte(v)
	case []byte:
		bValue = v
	case int, int8, int16, int32, int64:
		bValue = []byte(fmt.Sprintf("%d", v))
	case uint, uint8, uint16, uint32, uint64:
		bValue = []byte(fmt.Sprintf("%d", v))
	case float32, float64:
		bValue = []byte(fmt.Sprintf("%g", v))
	case bool:
		if v {
			bValue = []byte("1")
		} else {
			bValue = []byte("0")
		}
	default:
		// 对于其他类型，使用gob编码（向后兼容）
		var err error
		bValue, err = helper.InterfaceToBytes(value)
		if err != nil {
			return fmt.Errorf("%s,%v", logFuncTag, err)
		}
	}
	hkey := s.hashKey(key, field)
	typeKey := TypeOfKeyGet(key)

	return s.retryUpdate(func(txn *badger.Txn) error {
		exists := false
		if _, err := txn.Get(hkey); err == nil {
			exists = true
		}

		item, err := txn.Get(typeKey)
		if err == nil {
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			keyType := string(val)
			if keyType != "" && keyType != KeyTypeHash {
				return ErrWrongType
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}

		if err := txn.Set(typeKey, []byte(KeyTypeHash)); err != nil {
			return err
		}

		if err := s.setValueWithCompression(txn, hkey, bValue); err != nil {
			return err
		}

		countKey := s.hashCountKey(key)
		var currentCount uint64
		countItem, err := txn.Get(countKey)
		if err == nil {
			val, err := countItem.ValueCopy(nil)
			if err != nil {
				return fmt.Errorf("HSet: failed to get count value: %v", err)
			}
			currentCount = helper.BytesToUint64(val)
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}

		if !exists {
			currentCount++
		}
		return txn.Set(countKey, helper.Uint64ToBytes(currentCount))
	}, 30, encodePropagateCommand([]byte("HSET"), []byte(key)))
}
func (s *BotreonStore) HGet(key, field string) ([]byte, error) {
	if err := s.checkErrorInjector("HGet"); err != nil {
		return nil, err
	}
	// Check key type before retrieving field
	typeKey := TypeOfKeyGet(key)
	var keyType string
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(typeKey)
		if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		if err == nil {
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			keyType = string(val)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if keyType != "" && keyType != KeyTypeHash {
		return nil, ErrWrongType
	}

	hkey := s.hashKey(key, field)
	var val []byte
	err = s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(hkey)
		if err != nil {
			return err
		}
		val, err = s.getValueWithDecompression(item)
		return err
	})
	return val, err
}

func (s *BotreonStore) hashKey(key, field string) []byte {
	return []byte(fmt.Sprintf("%s:%s:%s", KeyTypeHash, key, field))
}

// hashCountKey 方法用于生成哈希表计数器键
func (s *BotreonStore) hashCountKey(key string) []byte {
	return []byte(fmt.Sprintf("%s:%s:__count__", KeyTypeHash, key))
}

// HDel 实现 Redis HDEL 命令
func (s *BotreonStore) HDel(key string, fields ...string) (int, error) {
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)
	deletedCount := 0
	err := s.retryUpdate(func(txn *badger.Txn) error {
		deletedCount = 0 // reset each attempt; stale value must not survive conflict retry
		// Check if key already exists with a different type
		typeKey := TypeOfKeyGet(key)
		typeItem, typeErr := txn.Get(typeKey)
		if typeErr == nil {
			typeVal, err := typeItem.ValueCopy(nil)
			if err != nil {
				return err
			}
			keyType := string(typeVal)
			if keyType != "" && keyType != KeyTypeHash {
				return ErrWrongType
			}
		} else if !errors.Is(typeErr, badger.ErrKeyNotFound) {
			return typeErr
		}

		countKey := s.hashCountKey(key)
		var currentCount uint64
		countItem, err := txn.Get(countKey)
		if err == nil {
			val, err := countItem.ValueCopy(nil)
			if err != nil {
				return fmt.Errorf("HDel: failed to get count value: %v", err)
			}
			currentCount = helper.BytesToUint64(val)
		}

		for _, field := range fields {
			hkey := s.hashKey(key, field)
			// 检查是否存在
			_, err := txn.Get(hkey)
			if err == nil {
				// 存在则删除
				if err := txn.Delete(hkey); err != nil {
					return err
				}
				deletedCount++
				if currentCount > 0 {
					currentCount--
				}
			}
		}

		// 更新计数器（即使计数器为0也要保留）
		if deletedCount > 0 {
			return txn.Set(countKey, helper.Uint64ToBytes(currentCount))
		}
		return nil
	}, 30, encodePropagateCommand([]byte("HDEL"), []byte(key)))
	return deletedCount, err
}

// HLen 实现 Redis HLEN 命令
func (s *BotreonStore) HLen(key string) (uint64, error) {
	var count uint64
	err := s.db.View(func(txn *badger.Txn) error {
		if err := checkKeyType(txn, key, KeyTypeHash); err != nil {
			return err
		}
		countKey := s.hashCountKey(key)
		item, err := txn.Get(countKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			count = 0
		} else if err != nil {
			return err
		} else {
			val, err := item.ValueCopy(nil)
			if err != nil {
				return fmt.Errorf("HLen: failed to get count value: %v", err)
			}
			count = helper.BytesToUint64(val)
		}
		return nil
	})
	return count, err
}

// HGetAll 实现 Redis HGETALL 命令
func (s *BotreonStore) HGetAll(key string) (map[string][]byte, error) {
	result := make(map[string][]byte)
	prefix := fmt.Sprintf("%s:%s:", KeyTypeHash, key)
	err := s.db.View(func(txn *badger.Txn) error {
		if err := checkKeyType(txn, key, KeyTypeHash); err != nil {
			return err
		}
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()
		prefixBytes := []byte(prefix)
		for iter.Seek(prefixBytes); iter.Valid(); iter.Next() {
			k := iter.Item().Key()
			kStr := string(k)
			if !strings.HasPrefix(kStr, prefix) {
				break
			}
			// 提取字段名
			_, field := splitHashKey(k)
			if field == "__count__" {
				continue
			}
			item := iter.Item()
			val, err := s.getValueWithDecompression(item)
			if err != nil {
				return err
			}
			result[field] = val
		}
		return nil
	})
	return result, err
}

// splitHashKey 从哈希键中解析出key和field
// 键格式: HASH:key:field
// 例如: HASH:user:1:name -> key="user:1", field="name"
func splitHashKey(key []byte) (string, string) {
	keyStr := string(key)
	// 去掉前缀 "HASH:"
	prefix := KeyTypeHash + ":"
	if !strings.HasPrefix(keyStr, prefix) {
		return "", ""
	}
	// 获取剩余部分: "user:1:name"
	remainder := keyStr[len(prefix):]
	// 找到最后一个冒号，前面是key，后面是field
	lastColon := strings.LastIndex(remainder, ":")
	if lastColon == -1 {
		return "", ""
	}
	hashKey := remainder[:lastColon]
	field := remainder[lastColon+1:]
	return hashKey, field
}

// getAllHashFields 获取哈希表中的所有字段
func (s *BotreonStore) getAllHashFields(txn *badger.Txn, key string) ([]string, error) {
	var fields []string
	prefix := fmt.Sprintf("%s:%s:", KeyTypeHash, key)
	prefixBytes := []byte(prefix)
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()

	for iter.Seek(prefixBytes); iter.Valid(); iter.Next() {
		k := iter.Item().Key()
		kStr := string(k)
		if !strings.HasPrefix(kStr, prefix) {
			break
		}
		_, field := splitHashKey(k)
		if field != "__count__" {
			fields = append(fields, field)
		}
	}
	return fields, nil
}

// HExists 实现 Redis HEXISTS 命令，检查字段是否存在
func (s *BotreonStore) HExists(key, field string) (bool, error) {
	exists := false
	err := s.db.View(func(txn *badger.Txn) error {
		if err := checkKeyType(txn, key, KeyTypeHash); err != nil {
			return err
		}
		hkey := s.hashKey(key, field)
		_, err := txn.Get(hkey)
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

// HKeys 实现 Redis HKEYS 命令，获取所有字段名
func (s *BotreonStore) HKeys(key string) ([]string, error) {
	var fields []string
	err := s.db.View(func(txn *badger.Txn) error {
		if err := checkKeyType(txn, key, KeyTypeHash); err != nil {
			return err
		}
		var err error
		fields, err = s.getAllHashFields(txn, key)
		return err
	})
	return fields, err
}

// HVals 实现 Redis HVALS 命令，获取所有字段值
func (s *BotreonStore) HVals(key string) ([][]byte, error) {
	var values [][]byte
	prefix := fmt.Sprintf("%s:%s:", KeyTypeHash, key)
	err := s.db.View(func(txn *badger.Txn) error {
		if err := checkKeyType(txn, key, KeyTypeHash); err != nil {
			return err
		}
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()
		prefixBytes := []byte(prefix)
		for iter.Seek(prefixBytes); iter.Valid(); iter.Next() {
			k := iter.Item().Key()
			kStr := string(k)
			if !strings.HasPrefix(kStr, prefix) {
				break
			}
			_, field := splitHashKey(k)
			if field == "__count__" {
				continue
			}
			item := iter.Item()
			val, err := s.getValueWithDecompression(item)
			if err != nil {
				return err
			}
			values = append(values, val)
		}
		return nil
	})
	return values, err
}

// HMSet 实现 Redis HMSET 命令，批量设置多个字段
func (s *BotreonStore) HMSet(key string, fieldValues map[string]interface{}) error {
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)
	typeKey := TypeOfKeyGet(key)
	return s.retryUpdate(func(txn *badger.Txn) error {
		if err := txn.Set(typeKey, []byte(KeyTypeHash)); err != nil {
			return err
		}

		countKey := s.hashCountKey(key)
		var currentCount uint64
		countItem, err := txn.Get(countKey)
		if err == nil {
			val, err := countItem.ValueCopy(nil)
			if err != nil {
				return fmt.Errorf("HMSet: failed to get count value: %v", err)
			}
			currentCount = helper.BytesToUint64(val)
		}

		newFields := 0
		for field, value := range fieldValues {
			// 将值转换为字符串（与Redis一致）
			var bValue []byte
			switch v := value.(type) {
			case string:
				bValue = []byte(v)
			case []byte:
				bValue = v
			case int, int8, int16, int32, int64:
				bValue = []byte(fmt.Sprintf("%d", v))
			case uint, uint8, uint16, uint32, uint64:
				bValue = []byte(fmt.Sprintf("%d", v))
			case float32, float64:
				bValue = []byte(fmt.Sprintf("%g", v))
			case bool:
				if v {
					bValue = []byte("1")
				} else {
					bValue = []byte("0")
				}
			default:
				// 对于其他类型，使用gob编码（向后兼容）
				var err error
				bValue, err = helper.InterfaceToBytes(value)
				if err != nil {
					return fmt.Errorf("HMSet: failed to convert value for field %s: %v", field, err)
				}
			}
			hkey := s.hashKey(key, field)

			// 检查字段是否存在
			exists := false
			if _, err := txn.Get(hkey); err == nil {
				exists = true
			}

			// 写入字段值（带压缩）
			if err := s.setValueWithCompression(txn, hkey, bValue); err != nil {
				return err
			}

			if !exists {
				newFields++
			}
		}

		// 更新计数器
		if newFields > 0 {
			currentCount += uint64(newFields)
			return txn.Set(countKey, helper.Uint64ToBytes(currentCount))
		}
		return nil
	}, 30, encodePropagateCommand([]byte("HMSET"), []byte(key)))
}

// HMGet 实现 Redis HMGET 命令，批量获取多个字段值
func (s *BotreonStore) HMGet(key string, fields ...string) ([][]byte, error) {
	values := make([][]byte, len(fields))
	err := s.db.View(func(txn *badger.Txn) error {
		for i, field := range fields {
			hkey := s.hashKey(key, field)
			item, err := txn.Get(hkey)
			if errors.Is(err, badger.ErrKeyNotFound) {
				values[i] = nil
				continue
			}
			if err != nil {
				return err
			}
			val, err := s.getValueWithDecompression(item)
			if err != nil {
				return err
			}
			values[i] = val
		}
		return nil
	})
	return values, err
}

// HSetNX 实现 Redis HSETNX 命令，仅当字段不存在时设置
func (s *BotreonStore) HSetNX(key, field string, value interface{}) (bool, error) {
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)
	success := false
	typeKey := TypeOfKeyGet(key)
	// 将值转换为字符串（与Redis一致）
	var bValue []byte
	switch v := value.(type) {
	case string:
		bValue = []byte(v)
	case []byte:
		bValue = v
	case int, int8, int16, int32, int64:
		bValue = []byte(fmt.Sprintf("%d", v))
	case uint, uint8, uint16, uint32, uint64:
		bValue = []byte(fmt.Sprintf("%d", v))
	case float32, float64:
		bValue = []byte(fmt.Sprintf("%g", v))
	case bool:
		if v {
			bValue = []byte("1")
		} else {
			bValue = []byte("0")
		}
	default:
		// 对于其他类型，使用gob编码（向后兼容）
		var err error
		bValue, err = helper.InterfaceToBytes(value)
		if err != nil {
			return false, fmt.Errorf("HSetNX: failed to convert value: %v", err)
		}
	}
	hkey := s.hashKey(key, field)
	err := s.retryUpdate(func(txn *badger.Txn) error {
		success = false // reset each attempt; stale value must not survive conflict retry
		// 检查字段是否存在
		_, getErr := txn.Get(hkey)
		if getErr == nil {
			// 字段已存在，不设置
			return nil
		}
		if !errors.Is(getErr, badger.ErrKeyNotFound) {
			return getErr
		}

		// 字段不存在，设置它（带压缩）
		if err := txn.Set(typeKey, []byte(KeyTypeHash)); err != nil {
			return err
		}
		if err := s.setValueWithCompression(txn, hkey, bValue); err != nil {
			return err
		}

		// 更新计数器
		countKey := s.hashCountKey(key)
		var currentCount uint64
		countItem, err := txn.Get(countKey)
		if err == nil {
			val, err := countItem.ValueCopy(nil)
			if err != nil {
				return fmt.Errorf("HSetNX: failed to get count value: %v", err)
			}
			currentCount = helper.BytesToUint64(val)
		}
		currentCount++
		if err := txn.Set(countKey, helper.Uint64ToBytes(currentCount)); err != nil {
			return err
		}

		success = true
		return nil
	}, 30, encodePropagateCommand([]byte("HSETNX"), []byte(key)))
	return success, err
}

// HIncrBy 实现 Redis HINCRBY 命令，将字段值增加整数
func (s *BotreonStore) HIncrBy(key, field string, increment int64) (int64, error) {
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)

	var result int64
	typeKey := TypeOfKeyGet(key)
	err := s.retryUpdate(func(txn *badger.Txn) error {
		result = 0 // reset each attempt; stale value must not survive conflict retry
		// Check if key already exists with a different type
		typeItem, typeErr := txn.Get(typeKey)
		if typeErr == nil {
			typeVal, err := typeItem.ValueCopy(nil)
			if err != nil {
				return err
			}
			keyType := string(typeVal)
			if keyType != "" && keyType != KeyTypeHash {
				return ErrWrongType
			}
		} else if !errors.Is(typeErr, badger.ErrKeyNotFound) {
			return typeErr
		}

		hkey := s.hashKey(key, field)
		var currentValue int64
		fieldExists := false

		// 获取当前值
		item, err := txn.Get(hkey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			currentValue = 0
		} else if err != nil {
			return err
		} else {
			fieldExists = true
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			// 尝试解析为整数（支持字符串格式的整数）
			intVal, err := strconv.ParseInt(string(val), 10, 64)
			if err != nil {
				return fmt.Errorf("HIncrBy: value is not an integer or out of range")
			}
			currentValue = intVal
		}

		// 计算新值
		result = currentValue + increment

		// 保存新值（存储为字符串，与Redis一致，带压缩）
		bValue := []byte(strconv.FormatInt(result, 10))
		if err := s.setValueWithCompression(txn, hkey, bValue); err != nil {
			return err
		}

		// 更新计数器（如果字段是新创建的）
		if !fieldExists {
			countKey := s.hashCountKey(key)
			var currentCount uint64
			countItem, err := txn.Get(countKey)
			if err == nil {
				val, err := countItem.ValueCopy(nil)
				if err != nil {
					return fmt.Errorf("HIncrBy: failed to get count value: %v", err)
				}
				currentCount = helper.BytesToUint64(val)
			}
			currentCount++
			if err := txn.Set(countKey, helper.Uint64ToBytes(currentCount)); err != nil {
				return err
			}
		}

		return nil
	}, 30, encodePropagateCommand([]byte("HINCRBY"), []byte(key)))
	return result, err
}

// HIncrByFloat 实现 Redis HINCRBYFLOAT 命令，将字段值增加浮点数
func (s *BotreonStore) HIncrByFloat(key, field string, increment float64) (float64, error) {
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)

	var result float64
	typeKey := TypeOfKeyGet(key)
	err := s.retryUpdate(func(txn *badger.Txn) error {
		result = 0 // reset each attempt; stale value must not survive conflict retry
		// Check if key already exists with a different type
		typeItem, typeErr := txn.Get(typeKey)
		if typeErr == nil {
			typeVal, err := typeItem.ValueCopy(nil)
			if err != nil {
				return err
			}
			keyType := string(typeVal)
			if keyType != "" && keyType != KeyTypeHash {
				return ErrWrongType
			}
		} else if !errors.Is(typeErr, badger.ErrKeyNotFound) {
			return typeErr
		}

		hkey := s.hashKey(key, field)
		var currentValue float64
		fieldExists := false

		// 获取当前值
		item, err := txn.Get(hkey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			currentValue = 0
		} else if err != nil {
			return err
		} else {
			fieldExists = true
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			// 尝试解析为浮点数（支持字符串格式的浮点数）
			floatVal, err := strconv.ParseFloat(string(val), 64)
			if err != nil {
				return fmt.Errorf("HIncrByFloat: value is not a valid float")
			}
			currentValue = floatVal
		}

		// 计算新值
		result = currentValue + increment

		// 保存新值（存储为字符串，与Redis一致，带压缩）
		bValue := []byte(strconv.FormatFloat(result, 'f', -1, 64))
		if err := s.setValueWithCompression(txn, hkey, bValue); err != nil {
			return err
		}

		// 更新计数器（如果字段是新创建的）
		if !fieldExists {
			countKey := s.hashCountKey(key)
			var currentCount uint64
			countItem, err := txn.Get(countKey)
			if err == nil {
				val, err := countItem.ValueCopy(nil)
				if err != nil {
					return fmt.Errorf("HIncrByFloat: failed to get count value: %v", err)
				}
				currentCount = helper.BytesToUint64(val)
			}
			currentCount++
			if err := txn.Set(countKey, helper.Uint64ToBytes(currentCount)); err != nil {
				return err
			}
		}

		return nil
	}, 30, encodePropagateCommand([]byte("HINCRBYFLOAT"), []byte(key)))
	return result, err
}

// HStrLen 实现 Redis HSTRLEN 命令，获取字段值的字符串长度
func (s *BotreonStore) HStrLen(key, field string) (int, error) {
	var length int
	err := s.db.View(func(txn *badger.Txn) error {
		if err := checkKeyType(txn, key, KeyTypeHash); err != nil {
			return err
		}
		hkey := s.hashKey(key, field)
		item, err := txn.Get(hkey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			length = 0
			return nil
		}
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		length = len(val)
		return nil
	})
	return length, err
}

// HRandField 实现 Redis HRANDFIELD 命令，随机获取哈希表中的字段
// count: 要返回的字段数量，正数表示不重复，负数表示可以重复
// withValues: 是否同时返回字段值
func (s *BotreonStore) HRandField(key string, count int, withValues bool) ([]string, []string, error) {
	var fields []string
	var values []string
	err := s.db.View(func(txn *badger.Txn) error {
		if count >= 0 && count != 0 {
			// Count > 0: distinct via reservoir sampling; count == 0: single random
			// Use streaming iteration with reservoir sampling for count > 0
			return s.hRandFieldReservoir(txn, key, count, withValues, &fields, &values)
		}
		if count < 0 {
			// count < 0: allow repeats. Load only field names (not values) when !withValues.
			realCount := -count
			allFields := s.hGetAllFieldNames(txn, key, withValues)
			if len(allFields) == 0 {
				return nil
			}
			for i := 0; i < realCount; i++ {
				idx := int(randomFloat64() * float64(len(allFields)))
				fields = append(fields, allFields[idx].Field)
				if withValues {
					values = append(values, string(allFields[idx].Value))
				}
			}
			return nil
		}
		// count == 0: return all fields (with values)
		allFields, err := s.hGetAllFields(txn, key)
		if err != nil {
			return err
		}
		for _, f := range allFields {
			fields = append(fields, f.Field)
			if withValues {
				values = append(values, string(f.Value))
			}
		}
		return nil
	})
	return fields, values, err
}

// hRandFieldReservoir uses reservoir sampling to select random fields without loading all into memory.
func (s *BotreonStore) hRandFieldReservoir(txn *badger.Txn, key string, count int, withValues bool, fields *[]string, values *[]string) error {
	prefix := s.hashKey(key, "")
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.PrefetchValues = false

	it := txn.NewIterator(opts)
	defer it.Close()

	if count == 0 {
		// Single random field: reservoir of size 1
		var foundField string
		var foundValue []byte
		hasValue := false
		i := 0
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			field, val, ok := s.parseHashIteratorKey(it, prefix)
			if !ok {
				continue
			}
			if !hasValue {
				foundField = field
				foundValue = val
				hasValue = true
			} else {
				j := randomIntn(i + 1)
				if j == 0 {
					foundField = field
					foundValue = val
				}
			}
			i++
		}
		if hasValue {
			*fields = append(*fields, foundField)
			if withValues {
				*values = append(*values, string(foundValue))
			}
		}
		return nil
	}

	// count > 0: distinct via Algorithm R reservoir sampling
	reservoir := make([]hashField, 0, count)
	i := 0
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		field, val, ok := s.parseHashIteratorKey(it, prefix)
		if !ok {
			continue
		}
		if i < count {
			reservoir = append(reservoir, hashField{Field: field, Value: val})
		} else {
			j := randomIntn(i + 1)
			if j < count {
				reservoir[j] = hashField{Field: field, Value: val}
			}
		}
		i++
	}

	// If we found fewer fields than count, return all
	if len(reservoir) < count {
		count = len(reservoir)
	}
	for j := 0; j < count; j++ {
		*fields = append(*fields, reservoir[j].Field)
		if withValues {
			*values = append(*values, string(reservoir[j].Value))
		}
	}
	return nil
}

// parseHashIteratorKey extracts field and value from a hash iterator item.
func (s *BotreonStore) parseHashIteratorKey(it *badger.Iterator, prefix []byte) (string, []byte, bool) {
	key := it.Item().Key()
	remainder := string(key[len(prefix):])
	if remainder == "__count__" {
		return "", nil, false
	}
	val, err := it.Item().ValueCopy(nil)
	if err != nil {
		return "", nil, false
	}
	return remainder, val, true
}

// hGetAllFields 获取哈希表中的所有字段（内部方法）
func (s *BotreonStore) hGetAllFields(txn *badger.Txn, key string) ([]hashField, error) {
	var fields []hashField
	prefix := s.hashKey(key, "")
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.PrefetchValues = false

	it := txn.NewIterator(opts)
	defer it.Close()

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		k := item.Key()
		// key格式: hash:myhash:fieldname
		fieldName := string(k[len(prefix):])
		// 跳过内部元数据键
		if fieldName == "__count__" {
			continue
		}
		var fieldValue []byte
		err := item.Value(func(val []byte) error {
			fieldValue = val
			return nil
		})
		if err != nil {
			return nil, err
		}
		fields = append(fields, hashField{Field: fieldName, Value: fieldValue})
	}
	return fields, nil
}

// hGetAllFieldNames 只加载 field 名（和可选的 value），不强制读全部 value。
// 用于 HRANDFIELD count < 0 路径，避免 !withValues 时无意义地加载 value。
func (s *BotreonStore) hGetAllFieldNames(txn *badger.Txn, key string, withValues bool) []hashField {
	var fields []hashField
	prefix := s.hashKey(key, "")
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.PrefetchValues = withValues // 只有当需要 value 时才 prefetch

	it := txn.NewIterator(opts)
	defer it.Close()

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		k := item.Key()
		fieldName := string(k[len(prefix):])
		if fieldName == "__count__" {
			continue
		}
		if withValues {
			var fieldValue []byte
			_ = item.Value(func(val []byte) error {
				fieldValue = val
				return nil
			})
			fields = append(fields, hashField{Field: fieldName, Value: fieldValue})
		} else {
			fields = append(fields, hashField{Field: fieldName})
		}
	}
	return fields
}

// hashField 哈希字段结构
type hashField struct {
	Field string
	Value []byte
}

// HScanResult HSCAN结果
type HScanResult struct {
	Cursor uint64
	Fields map[string][]byte
}

// HScan 实现 Redis HSCAN 命令，增量迭代哈希表的字段
func (s *BotreonStore) HScan(key string, cursor uint64, pattern string, count int) (HScanResult, error) {
	var result HScanResult
	result.Cursor = 0
	result.Fields = make(map[string][]byte)

	if count <= 0 {
		count = 10
	}

	seekKey := s.scanBookmarkLookup(cursor)
	s.scanBookmarkRelease(cursor)

	err := s.db.View(func(txn *badger.Txn) error {
		typeKey := TypeOfKeyGet(key)
		item, err := txn.Get(typeKey)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return nil
			}
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		if string(val) != "" && string(val) != KeyTypeHash {
			return ErrWrongType
		}

		prefix := []byte(fmt.Sprintf("%s:%s:", KeyTypeHash, key))
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = true

		iter := txn.NewIterator(opts)
		defer iter.Close()

		if seekKey != nil {
			iter.Seek(seekKey)
		} else {
			iter.Seek(prefix)
		}

		collected := 0
		var lastKey []byte
		for iter.ValidForPrefix(prefix) && collected < count {
			item := iter.Item()
			keyBytes := item.KeyCopy(nil)
			fieldName := string(keyBytes[len(prefix):])

			if fieldName == "__count__" {
				lastKey = keyBytes
				iter.Next()
				continue
			}

			if pattern == "" || pattern == "*" || matchPattern(fieldName, pattern) {
				fieldVal, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				result.Fields[fieldName] = fieldVal
				collected++
			}

			lastKey = keyBytes
			iter.Next()
		}

		if iter.ValidForPrefix(prefix) {
			result.Cursor = s.scanBookmarkStore(lastKey)
		} else {
			result.Cursor = 0
		}

		return nil
	})

	if errors.Is(err, ErrWrongType) {
		return result, err
	}
	return result, nil
}

// HRandMemberResult represents a field-value pair from HRANDMEMBER
type HRandMemberResult struct {
	Field string
	Value []byte
}

// HRandMember 实现 Redis HRANDMEMBER 命令
// If count > 0: returns up to count distinct field-value pairs (no repeats).
// If count < 0: returns -count field-value pairs, allowing repeats.
// If count == 0: returns 1 random field-value pair.
// 使用蓄水池采样（与 HRandField 同策略）避免全量加载到内存。
func (s *BotreonStore) HRandMember(key string, count int) ([]HRandMemberResult, error) {
	var result []HRandMemberResult
	prefix := fmt.Sprintf("%s:%s:", KeyTypeHash, key)

	// type check
	typeKey := TypeOfKeyGet(key)
	if err := s.db.View(func(txn *badger.Txn) error {
		typeItem, err := txn.Get(typeKey)
		if err == nil {
			typeVal, err := typeItem.ValueCopy(nil)
			if err != nil {
				return err
			}
			keyType := string(typeVal)
			if keyType != "" && keyType != KeyTypeHash {
				return ErrWrongType
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}

	if count >= 0 {
		// count == 0: single random; count > 0: distinct via reservoir sampling
		err := s.db.View(func(txn *badger.Txn) error {
			prefixBytes := []byte(prefix)
			opts := badger.DefaultIteratorOptions
			opts.Prefix = prefixBytes
			opts.PrefetchValues = false

			it := txn.NewIterator(opts)
			defer it.Close()

			n := count
			if n == 0 {
				n = 1
			}
			reservoir := make([]HRandMemberResult, 0, n)
			i := 0
			for it.Seek(prefixBytes); it.ValidForPrefix(prefixBytes); it.Next() {
				k := it.Item().Key()
				_, field := splitHashKey(k)
				if field == "__count__" {
					continue
				}
				val, vErr := s.getValueWithDecompression(it.Item())
				if vErr != nil {
					return vErr
				}
				if i < n {
					reservoir = append(reservoir, HRandMemberResult{Field: field, Value: val})
				} else {
					j := randomIntn(i + 1)
					if j < n {
						reservoir[j] = HRandMemberResult{Field: field, Value: val}
					}
				}
				i++
			}
			// If we found fewer fields than requested, return all
			if n > len(reservoir) || count == 0 {
				result = reservoir[:min(len(reservoir), n)]
			} else {
				// Re-shuffle the reservoir to avoid positional bias
				randomShuffle(len(reservoir), func(i, j int) {
					reservoir[i], reservoir[j] = reservoir[j], reservoir[i]
				})
				result = reservoir
			}
			return nil
		})
		return result, err
	}

	// count < 0: allow repeats. Load all field names (no value load) then pick.
	err := s.db.View(func(txn *badger.Txn) error {
		prefixBytes := []byte(prefix)
		var allFields []string
		var allValues [][]byte

		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefixBytes
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefixBytes); it.ValidForPrefix(prefixBytes); it.Next() {
			k := it.Item().Key()
			_, field := splitHashKey(k)
			if field == "__count__" {
				continue
			}
			val, vErr := s.getValueWithDecompression(it.Item())
			if vErr != nil {
				return vErr
			}
			allFields = append(allFields, field)
			allValues = append(allValues, val)
		}

		if len(allFields) == 0 {
			return nil
		}

		n := -count
		result = make([]HRandMemberResult, n)
		for i := 0; i < n; i++ {
			idx := randomIntn(len(allFields))
			result[i] = HRandMemberResult{Field: allFields[idx], Value: allValues[idx]}
		}
		return nil
	})
	return result, err
}
