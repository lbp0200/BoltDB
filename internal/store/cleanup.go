package store

import (
	"errors"
	"fmt"
	"strings"

	"github.com/dgraph-io/badger/v4"
)

// NextStartup 在服务启动时执行，恢复数据状态
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
		_ = cleanupOrphanedStreamData(txn)
		_ = cleanupOrphanedHLLData(txn)
		_ = cleanupOrphanedGeoData(txn)
		_ = cleanupOrphanedJSONData(txn)
		_ = cleanupOrphanedTSData(txn)
		_ = cleanupOrphanedGeoZSetData(txn)

		return nil
	}, 30)
}

// checkDataExists 检查键对应的数据是否存在
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
	case KeyTypeStream:
		// Stream检查meta键
		metaKey := []byte("stream:" + key + ":meta")
		_, err := txn.Get(metaKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return false, nil
		}
		return err == nil, err
	case KeyTypeHyperLogLog:
		hllKey := []byte("hll:" + key)
		_, err := txn.Get(hllKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return false, nil
		}
		return err == nil, err
	case KeyTypeGeo:
		// Geo检查meta键
		metaKey := []byte("geo:" + key + ":meta")
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

// cleanupOrphanedStreamData 清理没有TYPE_键的Stream数据
func cleanupOrphanedStreamData(txn *badger.Txn) error {
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()

	prefix := []byte("stream:")
	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		item := iter.Item()
		keyBytes := item.KeyCopy(nil)
		keyStr := string(keyBytes)

		// 格式: stream:key:meta, stream:key:data:id, stream:key:groups, stream:key:group:...
		parts := strings.SplitN(keyStr, ":", 3)
		if len(parts) < 2 {
			continue
		}
		key := parts[1]

		// 检查TYPE_键是否存在
		typeKey := TypeOfKeyGet(key)
		_, err := txn.Get(typeKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			// 删除整个stream的数据
			streamPrefix := []byte("stream:" + key + ":")
			if err := deleteByPrefix(txn, streamPrefix); err != nil {
				continue
			}
		}
	}
	return nil
}

// cleanupOrphanedHLLData 清理没有TYPE_键的HyperLogLog数据
func cleanupOrphanedHLLData(txn *badger.Txn) error {
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()

	prefix := []byte("hll:")
	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		item := iter.Item()
		keyBytes := item.KeyCopy(nil)
		keyStr := string(keyBytes)

		// 格式: hll:key
		key := strings.TrimPrefix(keyStr, "hll:")

		// 检查TYPE_键是否存在
		typeKey := TypeOfKeyGet(key)
		_, err := txn.Get(typeKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			if err := txn.Delete(keyBytes); err != nil {
				continue
			}
		}
	}
	return nil
}

// cleanupOrphanedGeoData 清理没有TYPE_键的Geo数据
func cleanupOrphanedGeoData(txn *badger.Txn) error {
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()

	prefix := []byte("geo:")
	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		item := iter.Item()
		keyBytes := item.KeyCopy(nil)
		keyStr := string(keyBytes)

		// 格式: geo:key:meta, geo:key:index:member, geo:key:members:..., geo:key:hash:...
		parts := strings.SplitN(keyStr, ":", 3)
		if len(parts) < 2 {
			continue
		}
		key := parts[1]

		// 检查TYPE_键是否存在
		typeKey := TypeOfKeyGet(key)
		_, err := txn.Get(typeKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			// 删除整个geo的数据
			geoPrefix := []byte("geo:" + key + ":")
			if err := deleteByPrefix(txn, geoPrefix); err != nil {
				continue
			}
		}
	}
	return nil
}

// cleanupOrphanedJSONData 清理没有TYPE_键的 JSON 数据（JSON:<key>）
func cleanupOrphanedJSONData(txn *badger.Txn) error {
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()

	prefix := []byte("JSON:")
	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		item := iter.Item()
		keyBytes := item.KeyCopy(nil)
		key := string(keyBytes[len("JSON:"):])
		typeKey := TypeOfKeyGet(key)
		_, err := txn.Get(typeKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			if err := txn.Delete(keyBytes); err != nil {
				continue
			}
		}
	}
	return nil
}

// cleanupOrphanedTSData 清理没有TYPE_键的 TimeSeries 数据（TS:<key>:meta / TS:<key>:data:*）
func cleanupOrphanedTSData(txn *badger.Txn) error {
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()

	prefix := []byte("TS:")
	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		item := iter.Item()
		keyBytes := item.KeyCopy(nil)
		keyStr := string(keyBytes)
		// 跳过聚合规则键 TS:rule:...（无 TYPE_，不应被清理）
		if strings.HasPrefix(keyStr, "TS:rule:") {
			continue
		}
		rest := strings.TrimPrefix(keyStr, "TS:")
		// TS:<key>:meta 或 TS:<key>:data:*
		sep := strings.Index(rest, ":")
		if sep < 0 {
			continue
		}
		key := rest[:sep]
		typeKey := TypeOfKeyGet(key)
		_, err := txn.Get(typeKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			tsPrefix := []byte(fmt.Sprintf("%s%s:", prefixTS, key))
			if err := deleteByPrefix(txn, tsPrefix); err != nil {
				continue
			}
		}
	}
	return nil
}

// cleanupOrphanedGeoZSetData 清理 GEO 残留的 zset 前缀（zset:<key>:* 但 TYPE_ 已不存在）
// GEO 会同时写入 geo:<key>:* 与 zset:<key>:*，若仅清理 geo: 前缀会残留 zset 孤儿。
func cleanupOrphanedGeoZSetData(txn *badger.Txn) error {
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()

	prefix := []byte(prefixKeySortedSetBytes)
	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		item := iter.Item()
		keyBytes := item.KeyCopy(nil)
		keyStr := string(keyBytes)
		parts := strings.SplitN(keyStr, ":", 3)
		if len(parts) < 2 {
			continue
		}
		key := parts[1]
		typeKey := TypeOfKeyGet(key)
		_, err := txn.Get(typeKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			zsetPrefix := []byte(fmt.Sprintf("%s%s:", prefixKeySortedSetBytes, key))
			if err := deleteByPrefix(txn, zsetPrefix); err != nil {
				continue
			}
		}
	}
	return nil
}
