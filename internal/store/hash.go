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

// 哈希操作
//func (s *BotreonStore) HSet(key, field string, value interface{}) error {
//	logFuncTag := "BotreonStoreHSet"
//	baggerTypeKey := TypeOfKeyGet(key)
//	bValue, err := helper.InterfaceToBytes(value)
//	if err != nil {
//		return fmt.Errorf("%s,%v", logFuncTag, err)
//	}
//	hkey := s.hashKey(key, field)
//	return s.db.Update(func(txn *badger.Txn) error {
//		err := txn.Set(hkey, bValue)
//		if err != nil {
//			return err
//		}
//		return txn.Set(baggerTypeKey, []byte(KeyTypeHash))
//	})
//}

// 修改 HSet 维护计数器
func (s *BotreonStore) HSet(key, field string, value interface{}) error {
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

	// 先在View事务中获取当前计数
	var currentCount uint64
	err := s.db.View(func(txn *badger.Txn) error {
		countKey := s.hashCountKey(key)
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
		return nil
	})
	if err != nil {
		return err
	}

	// 然后在Update事务中写入
	return s.db.Update(func(txn *badger.Txn) error {
		// 检查字段是否存在
		exists := false
		if _, err := txn.Get(hkey); err == nil {
			exists = true
		}

		// Check if key already exists with a different type
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

		// 写入字段值（带压缩）
		if err := s.setValueWithCompression(txn, hkey, bValue); err != nil {
			return err
		}

		// 更新计数器
		countKey := s.hashCountKey(key)
		if !exists {
			currentCount++
		}
		return txn.Set(countKey, helper.Uint64ToBytes(currentCount))
	})
}
func (s *BotreonStore) HGet(key, field string) ([]byte, error) {
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
	deletedCount := 0
	err := s.db.Update(func(txn *badger.Txn) error {
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
	})
	return deletedCount, err
}

// HLen 实现 Redis HLEN 命令
func (s *BotreonStore) HLen(key string) (uint64, error) {
	var count uint64
	err := s.db.View(func(txn *badger.Txn) error {
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
	typeKey := TypeOfKeyGet(key)
	return s.db.Update(func(txn *badger.Txn) error {
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
	})
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
	err := s.db.Update(func(txn *badger.Txn) error {
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
	})
	return success, err
}

// HIncrBy 实现 Redis HINCRBY 命令，将字段值增加整数
func (s *BotreonStore) HIncrBy(key, field string, increment int64) (int64, error) {
	var result int64
	typeKey := TypeOfKeyGet(key)
	err := s.db.Update(func(txn *badger.Txn) error {
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
	})
	return result, err
}

// HIncrByFloat 实现 Redis HINCRBYFLOAT 命令，将字段值增加浮点数
func (s *BotreonStore) HIncrByFloat(key, field string, increment float64) (float64, error) {
	var result float64
	typeKey := TypeOfKeyGet(key)
	err := s.db.Update(func(txn *badger.Txn) error {
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
	})
	return result, err
}

// HStrLen 实现 Redis HSTRLEN 命令，获取字段值的字符串长度
func (s *BotreonStore) HStrLen(key, field string) (int, error) {
	var length int
	err := s.db.View(func(txn *badger.Txn) error {
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
		// 获取所有字段
		allFields, err := s.hGetAllFields(txn, key)
		if err != nil {
			return err
		}
		if len(allFields) == 0 {
			return nil
		}

		// 根据 count 和 withValues 处理
		if count > 0 && count < len(allFields) {
			// 需要随机选择 count 个不重复的字段
			selected := make(map[int]bool)
			for len(selected) < count {
				idx := int(randomFloat64() * float64(len(allFields)))
				if !selected[idx] {
					selected[idx] = true
					fields = append(fields, allFields[idx].Field)
					if withValues {
						values = append(values, string(allFields[idx].Value))
					}
				}
			}
		} else if count < 0 {
			// 需要随机选择 -count 个字段（可以重复）
			realCount := -count
			if realCount > len(allFields) {
				realCount = len(allFields)
			}
			for i := 0; i < realCount; i++ {
				idx := int(randomFloat64() * float64(len(allFields)))
				fields = append(fields, allFields[idx].Field)
				if withValues {
					values = append(values, string(allFields[idx].Value))
				}
			}
		} else {
			// count >= len(allFields)，返回所有字段
			for _, f := range allFields {
				fields = append(fields, f.Field)
				if withValues {
					values = append(values, string(f.Value))
				}
			}
		}
		return nil
	})
	return fields, values, err
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

// hashField 哈希字段结构
type hashField struct {
	Field string
	Value []byte
}
