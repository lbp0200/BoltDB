package replication

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc64"
	"io"
	"math"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/lbp0200/BoltDB/internal/logger"
	"github.com/lbp0200/BoltDB/internal/store"
)

const (
	RDBMagicString = "REDIS"
	RDBVersion     = "0009" // Redis RDB version 9
)

// RDBEncoder RDB编码器
type RDBEncoder struct {
	buf *bytes.Buffer
}

// NewRDBEncoder 创建新的RDB编码器
func NewRDBEncoder() *RDBEncoder {
	enc := &RDBEncoder{
		buf: &bytes.Buffer{},
	}
	enc.writeHeader()
	return enc
}

// writeHeader 写入RDB文件头
func (enc *RDBEncoder) writeHeader() {
	enc.buf.WriteString(RDBMagicString)
	enc.buf.WriteString(RDBVersion)
}

// WriteDatabaseSelector 写入数据库选择器
func (enc *RDBEncoder) WriteDatabaseSelector(dbNum int) {
	enc.buf.WriteByte(0xFE) // FE = database selector
	// #nosec G115 - dbNum is a small positive integer (database index)
	enc.writeLength(uint64(dbNum))
}

// WriteKeyValue 写入键值对
func (enc *RDBEncoder) WriteKeyValue(key string, value interface{}, keyType string, ttl int64) error {
	// 如果有TTL，写入过期时间
	if ttl > 0 {
		now := time.Now().Unix()
		expireTime := now + ttl
		enc.buf.WriteByte(0xFD) // FD = expire time in seconds
		// #nosec G115 - expireTime is a valid Unix timestamp within uint32 range
		_ = binary.Write(enc.buf, binary.LittleEndian, uint32(expireTime))
	}

	// 写入值类型
	var typeByte byte
	switch keyType {
	case store.KeyTypeString:
		typeByte = 0 // STRING
	case store.KeyTypeList:
		typeByte = 1 // LIST
	case store.KeyTypeSet:
		typeByte = 2 // SET
	case store.KeyTypeHash:
		typeByte = 3 // HASH
	case store.KeyTypeSortedSet:
		typeByte = 4 // ZSET
	default:
		return fmt.Errorf("unknown key type: %s", keyType)
	}

	enc.buf.WriteByte(typeByte)

	// 写入键
	enc.writeString(key)

	// 写入值
	switch v := value.(type) {
	case string:
		enc.writeString(v)
	case []string:
		// List
		enc.writeLength(uint64(len(v)))
		for _, item := range v {
			enc.writeString(item)
		}
	case map[string][]byte:
		// Hash
		enc.writeLength(uint64(len(v)))
		for field, val := range v {
			enc.writeString(field)
			enc.writeBytes(val)
		}
	default:
		return fmt.Errorf("unsupported value type")
	}

	return nil
}

// WriteStringKeyValue 写入字符串键值对
func (enc *RDBEncoder) WriteStringKeyValue(key, value string, ttl int64) error {
	if ttl > 0 {
		now := time.Now().Unix()
		expireTime := now + ttl
		enc.buf.WriteByte(0xFD)
		// #nosec G115 - expireTime is a valid Unix timestamp within uint32 range
		_ = binary.Write(enc.buf, binary.LittleEndian, uint32(expireTime))
	}

	enc.buf.WriteByte(0) // STRING type
	enc.writeString(key)
	enc.writeString(value)
	return nil
}

// WriteListKeyValue 写入列表键值对
func (enc *RDBEncoder) WriteListKeyValue(key string, values []string, ttl int64) error {
	if ttl > 0 {
		now := time.Now().Unix()
		expireTime := now + ttl
		enc.buf.WriteByte(0xFD)
		// #nosec G115 - expireTime is a valid Unix timestamp within uint32 range
		_ = binary.Write(enc.buf, binary.LittleEndian, uint32(expireTime))
	}

	enc.buf.WriteByte(1) // LIST type
	enc.writeString(key)
	enc.writeLength(uint64(len(values)))
	for _, v := range values {
		enc.writeString(v)
	}
	return nil
}

// WriteHashKeyValue 写入哈希键值对
func (enc *RDBEncoder) WriteHashKeyValue(key string, fields map[string][]byte, ttl int64) error {
	if ttl > 0 {
		now := time.Now().Unix()
		expireTime := now + ttl
		enc.buf.WriteByte(0xFD)
		// #nosec G115 - expireTime is a valid Unix timestamp within uint32 range
		_ = binary.Write(enc.buf, binary.LittleEndian, uint32(expireTime))
	}

	enc.buf.WriteByte(3) // HASH type
	enc.writeString(key)
	enc.writeLength(uint64(len(fields)))
	for field, value := range fields {
		enc.writeString(field)
		enc.writeBytes(value)
	}
	return nil
}

// WriteSetKeyValue 写入集合键值对
func (enc *RDBEncoder) WriteSetKeyValue(key string, members []string, ttl int64) error {
	if ttl > 0 {
		now := time.Now().Unix()
		expireTime := now + ttl
		enc.buf.WriteByte(0xFD)
		// #nosec G115 - expireTime is a valid Unix timestamp within uint32 range
		_ = binary.Write(enc.buf, binary.LittleEndian, uint32(expireTime))
	}

	enc.buf.WriteByte(2) // SET type
	enc.writeString(key)
	enc.writeLength(uint64(len(members)))
	for _, member := range members {
		enc.writeString(member)
	}
	return nil
}

// WriteSortedSetKeyValue 写入有序集合键值对
func (enc *RDBEncoder) WriteSortedSetKeyValue(key string, members []*store.ZSetMember, ttl int64) error {
	if ttl > 0 {
		now := time.Now().Unix()
		expireTime := now + ttl
		enc.buf.WriteByte(0xFD)
		// #nosec G115 - expireTime is a valid Unix timestamp within uint32 range
		_ = binary.Write(enc.buf, binary.LittleEndian, uint32(expireTime))
	}

	enc.buf.WriteByte(4) // ZSET type
	enc.writeString(key)
	enc.writeLength(uint64(len(members)))
	for _, m := range members {
		enc.writeString(m.Member)
		scoreBytes := []byte(fmt.Sprintf("%.10g", m.Score))
		enc.writeBytes(scoreBytes)
	}
	return nil
}

// WriteFooter 写入RDB文件尾
func (enc *RDBEncoder) WriteFooter() {
	enc.buf.WriteByte(0xFF) // FF = end of RDB file
	hash := crc64.New(crc64.MakeTable(crc64.ECMA))
	hash.Write(enc.buf.Bytes())
	enc.buf.Write(hash.Sum(nil))
}

// Bytes 获取编码后的字节
func (enc *RDBEncoder) Bytes() []byte {
	return enc.buf.Bytes()
}

// WriteTo 写入到Writer
func (enc *RDBEncoder) WriteTo(w io.Writer) (int64, error) {
	n, err := enc.buf.WriteTo(w)
	return n, err
}

// writeString 写入字符串（长度编码）
func (enc *RDBEncoder) writeString(s string) {
	enc.writeLength(uint64(len(s)))
	enc.buf.WriteString(s)
}

// writeBytes 写入字节数组
func (enc *RDBEncoder) writeBytes(b []byte) {
	enc.writeLength(uint64(len(b)))
	enc.buf.Write(b)
}

// writeLength 写入长度（使用Redis长度编码）
func (enc *RDBEncoder) writeLength(length uint64) {
	if length < 0x40 {
		// 6位长度
		enc.buf.WriteByte(byte(length))
	} else if length < 0x4000 {
		// 14位长度
		enc.buf.WriteByte(byte((length >> 8) | 0x40))
		enc.buf.WriteByte(byte(length & 0xFF))
	} else {
		// 32位长度
		enc.buf.WriteByte(0x80)
		// #nosec G115 - length is bounded by practical RDB size limits
		_ = binary.Write(enc.buf, binary.LittleEndian, uint32(length))
	}
}

// GenerateRDB 生成RDB快照
// 使用单个一致性视图事务，消除 TYPE_ 扫描与 value 读取之间的 TOCTOU。
// PrefetchValues: false 避免全库 value 预取导致的内存压力。
func GenerateRDB(s *store.BotreonStore) ([]byte, error) {
	rdb, _, err := GenerateRDBWithOffset(s, nil)
	return rdb, err
}

// GenerateRDBWithOffset 生成RDB快照并返回快照对应的复制偏移量。
//
// snapshotOffset 在 badger View 事务内、所有数据读取完成后捕获。
// 这确保了 snapshotOffset 对应的偏移量至少包含了 RDB 中所有可见的写入，
// 同时不包含 RDB 中不可见的写入。将 FULLRESYNC 的 offset 设为该值后，
// backlog (snapshotOffset → currentOffset) 只覆盖快照中不存在的写入，
// 最小化重复应用窗口（通常 0-1 个命令）。
//
// offsetFn 如果为 nil，则不捕获偏移量（等效于 GenerateRDB）。
func GenerateRDBWithOffset(s *store.BotreonStore, offsetFn func() int64) ([]byte, int64, error) {
	enc := NewRDBEncoder()
	var snapshotOffset int64

	// 选择数据库0
	enc.WriteDatabaseSelector(0)

	// 遍历所有键，在同一个 View 事务中完成全部读取。
	// snapshotOffset 在 View 内部、所有读完成后捕获。
	err := s.GetDB().View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // 显式读取，不预取任何值
		iter := txn.NewIterator(opts)
		defer iter.Close()

		prefix := []byte("TYPE_")
		for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
			item := iter.Item()
			keyBytes := item.KeyCopy(nil)
			key := string(keyBytes[len(prefix):])

			typeVal, err := item.ValueCopy(nil)
			if err != nil {
				logger.Logger.Warn().Str("key", key).Err(err).Msg("获取键类型失败")
				continue
			}
			keyType := string(typeVal)

			ttl := readTTLFromValueTxn(txn, key, keyType)

			// 根据类型获取值并写入（同一 txn 内完成）
			switch keyType {
			case store.KeyTypeString:
				value, err := readStringInTxn(txn, key)
				if err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("获取字符串值失败")
					continue
				}
				_ = enc.WriteStringKeyValue(key, value, ttl)

			case store.KeyTypeList:
				values, err := readListInTxn(txn, key)
				if err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("获取列表值失败")
					continue
				}
				_ = enc.WriteListKeyValue(key, values, ttl)

			case store.KeyTypeHash:
				fields, err := readHashInTxn(txn, key)
				if err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("获取哈希值失败")
					continue
				}
				_ = enc.WriteHashKeyValue(key, fields, ttl)

			case store.KeyTypeSet:
				members, err := readSetInTxn(txn, key)
				if err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("获取集合值失败")
					continue
				}
				_ = enc.WriteSetKeyValue(key, members, ttl)

			case store.KeyTypeSortedSet:
				members, err := readZSetInTxn(txn, key)
				if err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("获取有序集合值失败")
					continue
				}
				if ttl > 0 {
					now := time.Now().Unix()
					expireTime := now + ttl
					enc.buf.WriteByte(0xFD)
					// #nosec G115 - expireTime is a valid Unix timestamp within uint32 range
					_ = binary.Write(enc.buf, binary.LittleEndian, uint32(expireTime))
				}
				enc.buf.WriteByte(4) // ZSET type
				enc.writeString(key)
				enc.writeLength(uint64(len(members)))
				for _, m := range members {
					enc.writeString(m.Member)
					scoreBytes := []byte(fmt.Sprintf("%.10g", m.Score))
					enc.writeBytes(scoreBytes)
				}

			default:
				logger.Logger.Debug().Str("key", key).Str("type", keyType).Msg("跳过不支持的RDB键类型")
			}
		}

		// 在所有数据读取完成后、同一 View 事务内捕获偏移量。
		// 此时所有 RDB 中可见写入的 offset 已递增（写入 -> badger commit -> offset incr
		// 在同一 handler goroutine 中顺序执行，且在 View 开始前已完成）。
		if offsetFn != nil {
			snapshotOffset = offsetFn()
		}
		return nil
	})

	if err != nil {
		return nil, 0, fmt.Errorf("生成RDB失败: %w", err)
	}

	enc.WriteFooter()
	return enc.Bytes(), snapshotOffset, nil
}

// readTTLFromValueTxn 从事务中读取值键的 TTL（BadgerDB 的 ExpiresAt 存储在 entry 头部）
func readTTLFromValueTxn(txn *badger.Txn, key, keyType string) int64 {
	var valueKey []byte
	switch keyType {
	case store.KeyTypeString:
		valueKey = []byte("STRING:" + key)
	case store.KeyTypeList:
		valueKey = []byte("LIST:" + key + ":length")
	case store.KeyTypeHash:
		valueKey = []byte("HASH:" + key + ":__count__")
	case store.KeyTypeSet:
		valueKey = []byte("SET:" + key + ":count")
	case store.KeyTypeSortedSet:
		valueKey = []byte("zset:" + key + ":meta")
	}
	if valueKey == nil {
		return 0
	}
	valItem, err := txn.Get(valueKey)
	if err != nil {
		return 0
	}
	if expiresAt := valItem.ExpiresAt(); expiresAt > 0 {
		expireTime := time.Unix(0, int64(expiresAt))
		now := time.Now()
		if expireTime.After(now) {
			return int64(expireTime.Sub(now).Seconds())
		}
	}
	return 0
}

// readStringInTxn 从事务中读取字符串值（解压缩后）
func readStringInTxn(txn *badger.Txn, key string) (string, error) {
	item, err := txn.Get([]byte("STRING:" + key))
	if err != nil {
		return "", err
	}
	raw, err := item.ValueCopy(nil)
	if err != nil {
		return "", err
	}
	decoded, err := store.DecompressData(raw)
	if err != nil {
		return "", err
	}
	return string(decoded), nil
}

// readListInTxn 从事务中读取列表的全部元素
func readListInTxn(txn *badger.Txn, key string) ([]string, error) {
	lengthKey := []byte("LIST:" + key + ":length")
	startKey := []byte("LIST:" + key + ":start")

	lengthItem, err := txn.Get(lengthKey)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, err
	}
	lengthBytes, err := lengthItem.ValueCopy(nil)
	if err != nil {
		return nil, err
	}
	length := int64(binary.BigEndian.Uint64(lengthBytes))
	if length == 0 {
		return nil, nil
	}

	startItem, err := txn.Get(startKey)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, err
	}
	startBytes, err := startItem.ValueCopy(nil)
	if err != nil {
		return nil, err
	}
	currentNodeID := string(startBytes)

	result := make([]string, 0, length)
	visited := make(map[string]bool)
	for int64(len(result)) < length {
		if visited[currentNodeID] {
			break
		}
		visited[currentNodeID] = true

		nodeKey := []byte("LIST:" + key + ":" + currentNodeID)
		valItem, err := txn.Get(nodeKey)
		if err != nil {
			return nil, err
		}
		raw, err := valItem.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		decoded, err := store.DecompressData(raw)
		if err != nil {
			return nil, err
		}
		result = append(result, string(decoded))

		// 最后一个节点可能没有 :next 键（链表非完全闭环）
		nextKey := []byte("LIST:" + key + ":" + currentNodeID + ":next")
		nextItem, err := txn.Get(nextKey)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				break
			}
			return nil, err
		}
		nextBytes, err := nextItem.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		currentNodeID = string(nextBytes)
	}
	return result, nil
}

// readHashInTxn 从事务中读取哈希的全部字段和值
func readHashInTxn(txn *badger.Txn, key string) (map[string][]byte, error) {
	prefix := []byte("HASH:" + key + ":")
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	defer it.Close()

	countKey := []byte("HASH:" + key + ":__count__")
	result := make(map[string][]byte)
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		k := it.Item().Key()
		if bytes.Equal(k, countKey) {
			continue
		}
		field := string(k[len(prefix):])
		raw, err := it.Item().ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		decoded, err := store.DecompressData(raw)
		if err != nil {
			return nil, err
		}
		result[field] = decoded
	}
	return result, nil
}

// readSetInTxn 从事务中读取集合的全部成员
func readSetInTxn(txn *badger.Txn, key string) ([]string, error) {
	prefix := []byte("SET:" + key + ":member:")
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	defer it.Close()

	var result []string
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		member := string(it.Item().Key()[len(prefix):])
		result = append(result, member)
	}
	return result, nil
}

// readZSetInTxn 从事务中读取有序集合的全部成员
func readZSetInTxn(txn *badger.Txn, key string) ([]store.ZSetMember, error) {
	prefix := []byte("zset:" + key + ":data:")
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = true
	opts.PrefetchSize = 100
	it := txn.NewIterator(opts)
	defer it.Close()

	var result []store.ZSetMember
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		member := string(item.Key()[len(prefix):])
		scoreBytes, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		score := math.Float64frombits(binary.BigEndian.Uint64(scoreBytes))
		result = append(result, store.ZSetMember{Member: member, Score: score})
	}
	return result, nil
}
