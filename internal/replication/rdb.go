package replication

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc64"
	"io"
	"math"

	"github.com/dgraph-io/badger/v4"
	"github.com/lbp0200/BoltDB/internal/logger"
	"github.com/lbp0200/BoltDB/internal/store"
)

const (
	RDBMagicString = "REDIS"
	RDBVersion     = "0009" // Redis RDB version 9

	// RDB type bytes (BoltDB subset; not full Redis type map)
	rdbTypeString           byte = 0
	rdbTypeList             byte = 1
	rdbTypeSet              byte = 2
	rdbTypeHash             byte = 3
	rdbTypeZSet             byte = 4
	rdbTypeStream           byte = 5 // entries only (legacy)
	rdbTypeGeo              byte = 6
	rdbTypeJSON             byte = 7
	rdbTypeTimeSeries       byte = 8
	rdbTypeHLL              byte = 9
	rdbTypeStreamWithGroups byte = 15 // entries + consumer groups/PEL
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

// WriteKeyValue 写入键值对（expireTimeUnix = 0 表示无过期）
func (enc *RDBEncoder) WriteKeyValue(key string, value interface{}, keyType string, expireTimeUnix int64) error {
	// 如果有TTL，写入过期时间
	if expireTimeUnix > 0 {
		enc.buf.WriteByte(0xFD) // FD = expire time in seconds
		// #nosec G115 - expireTimeUnix is a valid Unix timestamp within uint32 range
		_ = binary.Write(enc.buf, binary.LittleEndian, uint32(expireTimeUnix))
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
	case store.KeyTypeStream:
		typeByte = 5 // STREAM
	case store.KeyTypeGeo:
		typeByte = 6 // GEO
	case store.KeyTypeJSON:
		typeByte = 7 // JSON
	case store.KeyTypeTimeSeries:
		typeByte = 8 // TIMESERIES
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
	case []store.GeoMember:
		// GEO
		enc.writeLength(uint64(len(v)))
		for _, m := range v {
			enc.writeString(m.Member)
			_ = binary.Write(enc.buf, binary.LittleEndian, m.Lat)
			_ = binary.Write(enc.buf, binary.LittleEndian, m.Lon)
		}
	case []store.TimeSeriesDataPoint:
		// TIMESERIES
		enc.writeLength(uint64(len(v)))
		for _, p := range v {
			_ = binary.Write(enc.buf, binary.LittleEndian, p.Timestamp)
			_ = binary.Write(enc.buf, binary.LittleEndian, p.Value)
		}
	default:
		return fmt.Errorf("unsupported value type")
	}

	return nil
}

// WriteStringKeyValue 写入字符串键值对（expireTimeUnix = 0 表示无过期）
func (enc *RDBEncoder) WriteStringKeyValue(key, value string, expireTimeUnix int64) error {
	if expireTimeUnix > 0 {
		enc.buf.WriteByte(0xFD)
		// #nosec G115 - expireTimeUnix is a valid Unix timestamp within uint32 range
		_ = binary.Write(enc.buf, binary.LittleEndian, uint32(expireTimeUnix))
	}

	enc.buf.WriteByte(0) // STRING type
	enc.writeString(key)
	enc.writeString(value)
	return nil
}

// WriteListKeyValue 写入列表键值对（expireTimeUnix = 0 表示无过期）
func (enc *RDBEncoder) WriteListKeyValue(key string, values []string, expireTimeUnix int64) error {
	if expireTimeUnix > 0 {
		enc.buf.WriteByte(0xFD)
		// #nosec G115 - expireTimeUnix is a valid Unix timestamp within uint32 range
		_ = binary.Write(enc.buf, binary.LittleEndian, uint32(expireTimeUnix))
	}

	enc.buf.WriteByte(1) // LIST type
	enc.writeString(key)
	enc.writeLength(uint64(len(values)))
	for _, v := range values {
		enc.writeString(v)
	}
	return nil
}

// WriteHashKeyValue 写入哈希键值对（expireTimeUnix = 0 表示无过期）
func (enc *RDBEncoder) WriteHashKeyValue(key string, fields map[string][]byte, expireTimeUnix int64) error {
	if expireTimeUnix > 0 {
		enc.buf.WriteByte(0xFD)
		// #nosec G115 - expireTimeUnix is a valid Unix timestamp within uint32 range
		_ = binary.Write(enc.buf, binary.LittleEndian, uint32(expireTimeUnix))
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

// WriteSetKeyValue 写入集合键值对（expireTimeUnix = 0 表示无过期）
func (enc *RDBEncoder) WriteSetKeyValue(key string, members []string, expireTimeUnix int64) error {
	if expireTimeUnix > 0 {
		enc.buf.WriteByte(0xFD)
		// #nosec G115 - expireTimeUnix is a valid Unix timestamp within uint32 range
		_ = binary.Write(enc.buf, binary.LittleEndian, uint32(expireTimeUnix))
	}

	enc.buf.WriteByte(2) // SET type
	enc.writeString(key)
	enc.writeLength(uint64(len(members)))
	for _, member := range members {
		enc.writeString(member)
	}
	return nil
}

// WriteJSONKeyValue 写入 JSON 键值对
func (enc *RDBEncoder) WriteJSONKeyValue(key, value string) error {
	return enc.WriteJSONKeyValueWithTTL(key, value, 0)
}

// WriteJSONKeyValueWithTTL 写入 JSON 键值对（附带 TTL 时写入 0xFD 过期头）
func (enc *RDBEncoder) WriteJSONKeyValueWithTTL(key, value string, expireTimeUnix int64) error {
	if expireTimeUnix > 0 {
		enc.buf.WriteByte(0xFD)
		// #nosec G115
		_ = binary.Write(enc.buf, binary.LittleEndian, uint32(expireTimeUnix))
	}
	enc.buf.WriteByte(7) // JSON type
	enc.writeString(key)
	enc.writeString(value)
	return nil
}

// WriteTimeSeriesKeyValue 写入 time series 键值对
func (enc *RDBEncoder) WriteTimeSeriesKeyValue(key string, points []store.TimeSeriesDataPoint) error {
	return enc.WriteTimeSeriesKeyValueWithTTL(key, points, 0)
}

// WriteTimeSeriesKeyValueWithTTL 写入 time series 键值对（附带 TTL）
func (enc *RDBEncoder) WriteTimeSeriesKeyValueWithTTL(key string, points []store.TimeSeriesDataPoint, expireTimeUnix int64) error {
	if expireTimeUnix > 0 {
		enc.buf.WriteByte(0xFD)
		// #nosec G115
		_ = binary.Write(enc.buf, binary.LittleEndian, uint32(expireTimeUnix))
	}
	enc.buf.WriteByte(8) // TIMESERIES type
	enc.writeString(key)
	enc.writeLength(uint64(len(points)))
	for _, p := range points {
		_ = binary.Write(enc.buf, binary.LittleEndian, p.Timestamp)
		_ = binary.Write(enc.buf, binary.LittleEndian, p.Value)
	}
	return nil
}

// WriteGeoKeyValue 写入 geo 键值对（expireTimeUnix = 0 表示无过期）
func (enc *RDBEncoder) WriteGeoKeyValue(key string, members []store.GeoMember, expireTimeUnix int64) error {
	if expireTimeUnix > 0 {
		enc.buf.WriteByte(0xFD)
		// #nosec G115 - expireTimeUnix is a valid Unix timestamp within uint32 range
		_ = binary.Write(enc.buf, binary.LittleEndian, uint32(expireTimeUnix))
	}

	enc.buf.WriteByte(6) // GEO type
	enc.writeString(key)
	enc.writeLength(uint64(len(members)))
	for _, m := range members {
		enc.writeString(m.Member)
		_ = binary.Write(enc.buf, binary.LittleEndian, m.Lat)
		_ = binary.Write(enc.buf, binary.LittleEndian, m.Lon)
	}
	return nil
}

// WriteHLLKeyValue 写入 HyperLogLog 键值对
func (enc *RDBEncoder) WriteHLLKeyValue(key string, data []byte) error {
	return enc.WriteHLLKeyValueWithTTL(key, data, 0)
}

// WriteHLLKeyValueWithTTL 写入 HLL 键值对（附带 TTL）
func (enc *RDBEncoder) WriteHLLKeyValueWithTTL(key string, data []byte, expireTimeUnix int64) error {
	if expireTimeUnix > 0 {
		enc.buf.WriteByte(0xFD)
		// #nosec G115
		_ = binary.Write(enc.buf, binary.LittleEndian, uint32(expireTimeUnix))
	}
	enc.buf.WriteByte(9) // HLL type
	enc.writeString(key)
	enc.writeBytes(data) // encoding byte + registers
	return nil
}

// WriteStreamKeyValue 写入 stream 键值对（条目 + consumer groups/PEL）。
// 使用 type 15 (rdbTypeStreamWithGroups)。旧 type 5 仅条目格式仍可由 loader 读取。
func (enc *RDBEncoder) WriteStreamKeyValue(key string, entries []store.StreamEntry, groups []store.StreamGroup) error {
	return enc.WriteStreamKeyValueWithTTL(key, entries, groups, 0)
}

// WriteStreamKeyValueWithTTL 写入 stream 键值对（附带 TTL）
func (enc *RDBEncoder) WriteStreamKeyValueWithTTL(key string, entries []store.StreamEntry, groups []store.StreamGroup, expireTimeUnix int64) error {
	if expireTimeUnix > 0 {
		enc.buf.WriteByte(0xFD)
		// #nosec G115
		_ = binary.Write(enc.buf, binary.LittleEndian, uint32(expireTimeUnix))
	}
	enc.buf.WriteByte(rdbTypeStreamWithGroups)
	enc.writeString(key)

	// Write metadata: length, firstID (ts-seq), lastID (ts-seq) 用于重建
	enc.writeLength(uint64(len(entries)))
	if len(entries) > 0 {
		enc.writeString(entries[0].ID)
		enc.writeString(entries[len(entries)-1].ID)
	} else {
		enc.writeString("0-0")
		enc.writeString("0-0")
	}

	// Write entries
	enc.writeLength(uint64(len(entries)))
	for _, entry := range entries {
		enc.writeString(entry.ID)
		enc.writeLength(uint64(len(entry.Fields)))
		for k, v := range entry.Fields {
			enc.writeString(k)
			enc.writeString(v)
		}
	}

	// Consumer groups + PEL
	enc.writeLength(uint64(len(groups)))
	for i := range groups {
		g := &groups[i]
		enc.writeString(g.Name)
		enc.writeString(g.LastDeliveredID)
		// consumers
		var consumers []*store.StreamConsumer
		for _, c := range g.Consumers {
			if c != nil {
				consumers = append(consumers, c)
			}
		}
		enc.writeLength(uint64(len(consumers)))
		for _, c := range consumers {
			enc.writeString(c.Name)
			_ = binary.Write(enc.buf, binary.LittleEndian, c.LastSeen)
		}
		// pending
		var pending []*store.StreamPendingEntry
		for _, p := range g.Pending {
			if p != nil {
				pending = append(pending, p)
			}
		}
		enc.writeLength(uint64(len(pending)))
		for _, p := range pending {
			enc.writeString(p.ID)
			enc.writeString(p.Consumer)
			_ = binary.Write(enc.buf, binary.LittleEndian, p.DeliveryCount)
			_ = binary.Write(enc.buf, binary.LittleEndian, p.LastDelivery)
		}
	}
	return nil
}

// WriteSortedSetKeyValue 写入有序集合键值对（expireTimeUnix = 0 表示无过期）
func (enc *RDBEncoder) WriteSortedSetKeyValue(key string, members []store.ZSetMember, expireTimeUnix int64) error {
	if expireTimeUnix > 0 {
		enc.buf.WriteByte(0xFD)
		// #nosec G115 - expireTimeUnix is a valid Unix timestamp within uint32 range
		_ = binary.Write(enc.buf, binary.LittleEndian, uint32(expireTimeUnix))
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

// GenerateRDBWithSnapshotLock 在调用方已持有 SnapshotMu 写锁的前提下
// 生成 RDB。与 replication_handler 中"先取 offset 再开 View"的绑定配合，
// 消除 offset 捕获与 View 开启之间的子窗口。
// 注意：它不消除重复窗口——repl offset 在 PropagateCommand()（写锁之外）
// 才赋值，已提交未传播的写入仍会同时进入 RDB 与 backlog。
// 详见 docs/failures/snapshot-inconsistency.md §4。BGSAVE 等不绑定偏移的
// 路径仍用普通 GenerateRDB。
func GenerateRDBWithSnapshotLock(s *store.BotreonStore) ([]byte, error) {
	// 调用方已持写锁，这里直接复用 RDB 逻辑，不再额外加锁。
	return GenerateRDB(s)
}

// GenerateRDBWithOffset 生成RDB快照并返回快照对应的复制偏移量。
//
// snapshotOffset 在 badger View 事务内、所有数据读取完成后捕获（通过 offsetFn）。
// 注意：这是 View 内的一个时间点，不一定是 MVCC 快照的实际边界。
//
// 正确的 FULLRESYNC 用法：调用者在调用 GenerateRDB 之前应单独捕获
// GetMasterReplOffset()。这是安全的，因为所有写入的序关系是：
//
//	store.Set() (badger commit) → PropagateCommand() (offset 递增)
//
// 因此任何 offset < preViewSnapshotOffset 的写入在 badger View 开启前
// 已提交，一定在 MVCC 快照中可见。backlog 从 preViewSnapshotOffset 开始。
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
				if err := enc.WriteStringKeyValue(key, value, ttl); err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("写入字符串值到RDB失败")
				}

			case store.KeyTypeList:
				values, err := readListInTxn(txn, key)
				if err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("获取列表值失败")
					continue
				}
				if err := enc.WriteListKeyValue(key, values, ttl); err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("写入列表值到RDB失败")
				}

			case store.KeyTypeHash:
				fields, err := readHashInTxn(txn, key)
				if err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("获取哈希值失败")
					continue
				}
				if err := enc.WriteHashKeyValue(key, fields, ttl); err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("写入哈希值到RDB失败")
				}

			case store.KeyTypeSet:
				members, err := readSetInTxn(txn, key)
				if err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("获取集合值失败")
					continue
				}
				if err := enc.WriteSetKeyValue(key, members, ttl); err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("写入集合值到RDB失败")
				}

			case store.KeyTypeSortedSet:
				members, err := readZSetInTxn(txn, key)
				if err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("获取有序集合值失败")
					continue
				}
				if err := enc.WriteSortedSetKeyValue(key, members, ttl); err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("写入有序集合值到RDB失败")
				}

			case store.KeyTypeStream:
				entries, err := readStreamInTxn(txn, key)
				if err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("获取stream值失败")
					continue
				}
				groups, gErr := readStreamGroupsInTxn(txn, key)
				if gErr != nil {
					logger.Logger.Warn().Str("key", key).Err(gErr).Msg("获取stream groups失败")
					groups = nil
				}
				if err := enc.WriteStreamKeyValueWithTTL(key, entries, groups, ttl); err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("写入stream值到RDB失败")
				}

			case store.KeyTypeJSON:
				value, err := readJSONInTxn(txn, key)
				if err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("获取JSON值失败")
					continue
				}
				if err := enc.WriteJSONKeyValueWithTTL(key, value, ttl); err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("写入JSON值到RDB失败")
				}

			case store.KeyTypeTimeSeries:
				points, err := readTimeSeriesInTxn(txn, key)
				if err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("获取time series值失败")
					continue
				}
				if err := enc.WriteTimeSeriesKeyValueWithTTL(key, points, ttl); err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("写入time series值到RDB失败")
				}

			case store.KeyTypeGeo:
				members, err := readGeoInTxn(txn, key)
				if err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("获取geo值失败")
					continue
				}
				if err := enc.WriteGeoKeyValue(key, members, ttl); err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("写入geo值到RDB失败")
				}

			case store.KeyTypeHyperLogLog:
				data, err := readHLLInTxn(txn, key)
				if err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("获取HLL值失败")
					continue
				}
				if err := enc.WriteHLLKeyValueWithTTL(key, data, ttl); err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("写入HLL值到RDB失败")
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

// readTTLFromValueTxn 从事务中读取值键的绝对过期时间（Unix 秒时间戳）。
// 返回 0 表示无过期或已过期。返回正值 = 应写入 RDB 的过期时间戳。
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
	case store.KeyTypeGeo:
		valueKey = []byte("geo:" + key + ":meta")
	case store.KeyTypeJSON:
		valueKey = []byte("JSON:" + key)
	case store.KeyTypeTimeSeries:
		valueKey = []byte("TS:" + key + ":meta")
	case store.KeyTypeHyperLogLog:
		valueKey = []byte(store.HyperLogLogPrefix + key)
	case store.KeyTypeStream:
		valueKey = []byte("stream:" + key + ":meta")
	}
	if valueKey == nil {
		return 0
	}
	valItem, err := txn.Get(valueKey)
	if err != nil {
		return 0
	}
	if expiresAt := valItem.ExpiresAt(); expiresAt > 0 {
		// ExpiresAt 是秒级 Unix 时间戳（统一格式，由 Expire/PExpire/WithTTL 写入）
		return int64(expiresAt)
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
		score := store.DecodeScore(scoreBytes)
		result = append(result, store.ZSetMember{Member: member, Score: score})
	}
	return result, nil
}

// readJSONInTxn 从事务中读取 JSON 值
func readJSONInTxn(txn *badger.Txn, key string) (string, error) {
	item, err := txn.Get([]byte("JSON:" + key))
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

// readTimeSeriesInTxn 从事务中读取 time series 的全部数据点
func readTimeSeriesInTxn(txn *badger.Txn, key string) ([]store.TimeSeriesDataPoint, error) {
	prefix := []byte("TS:" + key + ":data:")
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = true
	opts.PrefetchSize = 100
	it := txn.NewIterator(opts)
	defer it.Close()

	var result []store.TimeSeriesDataPoint
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		raw, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		if len(raw) < 16 {
			continue
		}
		timestamp := int64(binary.BigEndian.Uint64(raw[:8]))
		value := math.Float64frombits(binary.BigEndian.Uint64(raw[8:16]))
		result = append(result, store.TimeSeriesDataPoint{Timestamp: timestamp, Value: value})
	}
	return result, nil
}

// readHLLInTxn 从事务中读取 HyperLogLog 原始字节
func readHLLInTxn(txn *badger.Txn, key string) ([]byte, error) {
	hllKey := []byte(store.HyperLogLogPrefix + key)
	item, err := txn.Get(hllKey)
	if err != nil {
		return nil, err
	}
	return item.ValueCopy(nil)
}

// readGeoInTxn 从事务中读取 geo 的全部成员 (member, lat, lon)
func readGeoInTxn(txn *badger.Txn, key string) ([]store.GeoMember, error) {
	prefix := []byte("geo:" + key + ":index:")
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = true
	opts.PrefetchSize = 100
	it := txn.NewIterator(opts)
	defer it.Close()

	var result []store.GeoMember
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		member := string(item.Key()[len(prefix):])
		raw, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		hash := binary.BigEndian.Uint64(raw)
		lat, lon := store.DecodeGeoHash(hash)
		result = append(result, store.GeoMember{Member: member, Lat: lat, Lon: lon})
	}
	return result, nil
}

// readStreamInTxn 从事务中读取 stream 的全部条目
func readStreamInTxn(txn *badger.Txn, key string) ([]store.StreamEntry, error) {
	prefix := []byte("stream:" + key + ":data:")
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = true
	opts.PrefetchSize = 100
	it := txn.NewIterator(opts)
	defer it.Close()

	var entries []store.StreamEntry
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		id := string(bytes.TrimPrefix(item.Key(), prefix))
		raw, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}

		var fields map[string]string
		if err := json.Unmarshal(raw, &fields); err != nil {
			logger.Logger.Warn().Str("key", key).Str("id", id).Err(err).Msg("解码stream条目失败，跳过")
			continue
		}

		entries = append(entries, store.StreamEntry{
			ID:     id,
			Fields: fields,
		})
	}
	return entries, nil
}

// readStreamGroupsInTxn loads consumer groups (with consumers + PEL) for a stream key.
func readStreamGroupsInTxn(txn *badger.Txn, key string) ([]store.StreamGroup, error) {
	prefix := []byte("stream:" + key + ":groups:")
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = true
	it := txn.NewIterator(opts)
	defer it.Close()

	var groups []store.StreamGroup
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		raw, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		var g store.StreamGroup
		if err := json.Unmarshal(raw, &g); err != nil {
			logger.Logger.Warn().Str("key", key).Err(err).Msg("解码stream group失败，跳过")
			continue
		}
		if g.Name == "" {
			// fallback: group name from key suffix stream:key:groups:NAME
			full := string(item.Key())
			if i := bytes.LastIndexByte(item.Key(), ':'); i >= 0 {
				g.Name = full[i+1:]
			}
		}
		groups = append(groups, g)
	}
	return groups, nil
}
