package store

import (
	"errors"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/lbp0200/BoltDB/internal/logger"
)

// getKeyValueKey 根据键类型获取值键（用于 TTL/Expire 探测）
// 注意：Stream/Geo/HLL 的 TTL 探测需返回其 meta/数据键；未覆盖时
// TTL/Expire 会因 unknown key type 直接报错，导致 EXPIRE 返回错误
// 而非 0（Redis 语义：键存在但 TTL 不支持应按无 TTL 处理）。
func (s *BotreonStore) getKeyValueKey(key string, keyType string) ([]byte, error) {
	switch keyType {
	case KeyTypeString:
		return []byte(s.stringKey(key)), nil
	case KeyTypeList:
		// List的主键是length键
		return []byte(s.listKey(key, "length")), nil
	case KeyTypeHash:
		// Hash的主键是count键（与 hashCountKey 一致：HASH:key:__count__）
		return s.hashCountKey(key), nil
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
	case KeyTypeStream:
		return streamKey(key), nil
	case KeyTypeGeo:
		return geoKey(key), nil
	case KeyTypeHyperLogLog:
		return []byte(fmt.Sprintf("%s%s", HyperLogLogPrefix, key)), nil
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
	raw, err := s.RawType(key)
	if err != nil {
		return "none", err
	}
	if raw == "none" {
		return "none", nil
	}
	switch raw {
	case KeyTypeString:
		return "string", nil
	case KeyTypeList:
		return "list", nil
	case KeyTypeHash:
		return "hash", nil
	case KeyTypeSet:
		return "set", nil
	case KeyTypeSortedSet:
		return "zset", nil
	case KeyTypeJSON:
		return "json", nil
	case KeyTypeTimeSeries:
		return "ts", nil
	case KeyTypeStream:
		return "stream", nil
	case KeyTypeHyperLogLog:
		return "string", nil // HyperLogLog 内部存储为 string
	case KeyTypeGeo:
		return "zset", nil // Geo 底层为 zset+hash，TYPE 兼容返回 zset
	default:
		return "none", nil
	}
}

// RawType 返回内部 TYPE_ 键的原始值（不做 Redis 别名映射），用于 COPY 等需精确区分 zset/geo 的场景
func (s *BotreonStore) RawType(key string) (string, error) {
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
		return nil
	})
	return keyType, err
}

// EXPIRE 实现 Redis EXPIRE 命令，设置键的过期时间（秒）
func (s *BotreonStore) Expire(key string, seconds int) (bool, error) {
	if err := s.checkErrorInjector("Expire"); err != nil {
		return false, err
	}
	if seconds <= 0 {
		// Redis 语义：非正 TTL 立即删除键（EXPIRE key 0/负数）。
		// 返回 Del 是否删除了 key：key 存在→true(1)，不存在→false(0)。
		// 注意：Del 自身已取 key 锁——此处不得在锁内调用（RWMutex 不可重入）。
		deleted, err := s.Del(key)
		if err != nil {
			logger.Logger.Warn().Err(err).Str("key", key).Msg("Expire: error deleting key with non-positive TTL")
			return false, err
		}
		return deleted > 0, nil
	}
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)
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

		// 直接设置TTL：计算过期时间戳（秒 — 与 BadgerDB WithTTL 格式一致）
		expiresAt := uint64(time.Now().Unix()) + uint64(seconds)
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
		// Redis 语义：past 时间戳立即删除键，key 存在返回 true(1)，不存在返回 false(0)
		deleted, err := s.Del(key)
		if err != nil {
			logger.Logger.Warn().Err(err).Str("key", key).Msg("ExpireAt: error deleting expired key")
			return false, err
		}
		return deleted > 0, nil
	}
	return s.Expire(key, int(ttl))
}

// PEXPIRE 实现 Redis PEXPIRE 命令，设置键的过期时间（毫秒）
func (s *BotreonStore) PExpire(key string, milliseconds int64) (bool, error) {
	if milliseconds <= 0 {
		// Redis 语义：非正 TTL 立即删除键（PEXPIRE key 0/负数）。
		// 返回 Del 是否删除了 key：key 存在→true(1)，不存在→false(0)。
		// 注意：Del 自身已取 key 锁——此处不得在锁内调用（RWMutex 不可重入）。
		deleted, err := s.Del(key)
		if err != nil {
			logger.Logger.Warn().Err(err).Str("key", key).Msg("PExpire: error deleting key with non-positive TTL")
			return false, err
		}
		return deleted > 0, nil
	}
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)
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

		// 直接设置TTL：计算过期时间戳（秒 — 与 BadgerDB WithTTL 格式一致）
		// 毫秒精度：将毫秒转换为秒，向上取整，确保不会因为截断而少算
		expiresAt := uint64(time.Now().Unix()) + uint64(milliseconds)/1000
		if milliseconds%1000 != 0 {
			expiresAt++ // 向上取整，确保不会因为截断而提前过期
		}
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
		// Redis 语义：past 毫秒时间戳立即删除键，key 存在返回 true(1)，不存在返回 false(0)
		deleted, err := s.Del(key)
		if err != nil {
			logger.Logger.Warn().Err(err).Str("key", key).Msg("PExpireAt: error deleting expired key")
			return false, err
		}
		return deleted > 0, nil
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

		// ExpiresAt 返回的是 Unix 时间戳（秒），由以下方式写入：
		//   - Expire() / ExpireAt() / PExpire() / PExpireAt() 统一写入秒
		//   - WithTTL() / SetWithTTL() / RDB loader 写入秒
		expiresAt := valueItem.ExpiresAt()
		if expiresAt == 0 {
			ttl = -1 // -1表示键存在但没有设置过期时间
			return nil
		}

		nowUnix := uint64(time.Now().Unix())
		ttl = int64(expiresAt - nowUnix)
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

		// ExpiresAt 返回的是 Unix 时间戳（秒），由以下方式写入：
		//   - Expire() / ExpireAt() / PExpire() / PExpireAt() 统一写入秒
		//   - WithTTL() / SetWithTTL() / RDB loader 写入秒
		expiresAt := valueItem.ExpiresAt()
		if expiresAt == 0 {
			ttl = -1 // -1表示键存在但没有设置过期时间
			return nil
		}

		nowUnix := uint64(time.Now().Unix())
		ttl = int64(expiresAt-nowUnix) * 1000 // 秒 → 毫秒
		if ttl < 0 {
			ttl = -2 // 已过期
		}
		return nil
	})
	return ttl, err
}

// ExpireTime 实现 Redis EXPIRETIME 命令，返回键过期时间的Unix时间戳（秒）
// 返回 -1 表示键存在但没有设置过期时间，-2 表示键不存在
func (s *BotreonStore) ExpireTime(key string) (int64, error) {
	return s.computeAbsoluteExpiry(key, time.Second)
}

// PExpireTime 实现 Redis PEXPIRETIME 命令，返回键过期时间的Unix时间戳（毫秒）
// 返回 -1 表示键存在但没有设置过期时间，-2 表示键不存在
func (s *BotreonStore) PExpireTime(key string) (int64, error) {
	return s.computeAbsoluteExpiry(key, time.Millisecond)
}

// computeAbsoluteExpiry 是 ExpireTime/PExpireTime 的公共实现
func (s *BotreonStore) computeAbsoluteExpiry(key string, unit time.Duration) (int64, error) {
	var result int64 = -2
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

		valueKey, err := s.getKeyValueKey(key, keyType)
		if err != nil {
			return err
		}

		valueItem, err := txn.Get(valueKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			result = -2
			return nil
		}
		if err != nil {
			return err
		}

		expiresAt := valueItem.ExpiresAt()
		if expiresAt == 0 {
			result = -1 // 键存在但没有设置过期时间
			return nil
		}

		// #nosec G115 - expiresAt is a valid Unix timestamp within int64 range
		// expiresAt 始终是秒级 Unix 时间戳
		switch unit {
		case time.Millisecond:
			result = int64(expiresAt) * 1000
		default:
			result = int64(expiresAt)
		}
		return nil
	})
	return result, err
}

// PERSIST 实现 Redis PERSIST 命令，移除键的过期时间
func (s *BotreonStore) Persist(key string) (bool, error) {
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)
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
	case KeyTypeSortedSet:
		return "skiplist", nil // 有序集合使用 skiplist 编码
	case KeyTypeStream:
		return "stream", nil // 流使用 stream 编码
	case KeyTypeHyperLogLog:
		return "string", nil // HyperLogLog 内部使用 string 编码
	case KeyTypeJSON:
		return "string", nil // JSON 内部使用 string 编码
	case KeyTypeTimeSeries:
		return "string", nil // TimeSeries 内部使用 string 编码
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

// Time 实现 Redis TIME 命令，返回服务器当前时间
func (s *BotreonStore) Time() (int64, int64, error) {
	now := time.Now()
	sec := now.Unix()
	usec := int64(now.Nanosecond() / 1000)
	return sec, usec, nil
}
