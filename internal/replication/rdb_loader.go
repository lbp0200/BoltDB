package replication

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc64"
	"io"
	"strconv"
	"time"

	"github.com/lbp0200/BoltDB/internal/logger"
	"github.com/lbp0200/BoltDB/internal/store"
)

// batchSize RDB 加载时每个事务最多写入的字符串条目数
const rdbBatchSize = 1000

// RDBDecoder RDB解码器
type RDBDecoder struct {
	buf      *bytes.Buffer
	version  string
	origData []byte // 原始数据副本，用于 CRC64 校验
}

// NewRDBDecoder 创建新的RDB解码器
func NewRDBDecoder(data []byte) *RDBDecoder {
	return &RDBDecoder{
		buf:      bytes.NewBuffer(data),
		origData: data,
	}
}

// DecodeHeader 解码RDB文件头
func (dec *RDBDecoder) DecodeHeader() error {
	// 读取magic string
	magic := make([]byte, 5)
	if _, err := dec.buf.Read(magic); err != nil {
		return fmt.Errorf("failed to read RDB magic: %w", err)
	}
	if string(magic) != RDBMagicString {
		return fmt.Errorf("invalid RDB magic: %s", string(magic))
	}

	// 读取版本
	version := make([]byte, 4)
	if _, err := dec.buf.Read(version); err != nil {
		return fmt.Errorf("failed to read RDB version: %w", err)
	}
	dec.version = string(version)

	return nil
}

// readLength 读取长度编码
func (dec *RDBDecoder) readLength() (uint64, error) {
	if dec.buf.Len() == 0 {
		return 0, fmt.Errorf("unexpected end of buffer")
	}

	b := dec.buf.Next(1)[0]
	if b&0xC0 == 0 {
		// 6位长度 (00xxxxxx)
		return uint64(b & 0x3F), nil
	} else if b&0x80 == 0 {
		// 14位长度 (01xxxxxx)
		if dec.buf.Len() < 1 {
			return 0, fmt.Errorf("unexpected end of buffer")
		}
		b2 := dec.buf.Next(1)[0]
		return uint64(((uint64(b) & 0x3F) << 8) | uint64(b2)), nil
	} else {
		// 32位长度 (10xxxxxx)
		if dec.buf.Len() < 4 {
			return 0, fmt.Errorf("unexpected end of buffer")
		}
		var length uint32
		if err := binary.Read(dec.buf, binary.LittleEndian, &length); err != nil {
			return 0, err
		}
		return uint64(length), nil
	}
}

// readString 读取字符串（长度编码）
func (dec *RDBDecoder) readString() (string, error) {
	length, err := dec.readLength()
	if err != nil {
		return "", err
	}
	if dec.buf.Len() < int(length) {
		return "", fmt.Errorf("unexpected end of buffer")
	}
	return string(dec.buf.Next(int(length))), nil
}

// readBytes 读取字节数组
func (dec *RDBDecoder) readBytes() ([]byte, error) {
	length, err := dec.readLength()
	if err != nil {
		return nil, err
	}
	if dec.buf.Len() < int(length) {
		return nil, fmt.Errorf("unexpected end of buffer")
	}
	return dec.buf.Next(int(length)), nil
}

// readExpireTime 读取过期时间
func (dec *RDBDecoder) readExpireTime() (int64, error) {
	if dec.buf.Len() == 0 {
		return 0, fmt.Errorf("unexpected end of buffer")
	}

	expireType := dec.buf.Next(1)[0]
	switch expireType {
	case 0xFC:
		// 毫秒精度
		var ms int64
		if err := binary.Read(dec.buf, binary.LittleEndian, &ms); err != nil {
			return 0, err
		}
		return ms, nil
	case 0xFD:
		// 秒精度
		var sec int32
		if err := binary.Read(dec.buf, binary.LittleEndian, &sec); err != nil {
			return 0, err
		}
		return int64(sec), nil
	}
	// 不是过期时间，将字节放回
	if err := dec.buf.UnreadByte(); err != nil {
		logger.Logger.Warn().Err(err).Msg("Failed to unread byte")
	}
	return 0, nil
}

// LoadRDB 加载RDB数据到存储
func (rm *ReplicationManager) LoadRDB(data []byte) error {
	if err := rm.store.FlushDB(); err != nil {
		return fmt.Errorf("failed to flush old data before RDB load: %w", err)
	}

	dec := NewRDBDecoder(data)

	// 解码头部
	if err := dec.DecodeHeader(); err != nil {
		return fmt.Errorf("failed to decode RDB header: %w", err)
	}

	return loadRDBEntries(dec, rm.store)
}

// LoadRDBWithStore 使用指定存储加载RDB数据（用于从节点同步）
func LoadRDBWithStore(data []byte, s *store.BotreonStore) error {
	dec := NewRDBDecoder(data)

	// 解码头部
	if err := dec.DecodeHeader(); err != nil {
		return fmt.Errorf("failed to decode RDB header: %w", err)
	}

	return loadRDBEntries(dec, s)
}

// loadRDBEntries 遍历 RDB decoder 并将条目写入 store
// 字符串类型使用 batch 写入减少事务数
func loadRDBEntries(dec *RDBDecoder, s *store.BotreonStore) error {
	logger.Logger.Info().Str("version", dec.version).Msg("开始加载RDB数据")

	var entries []store.StringEntry

	// 刷新字符串批量缓冲区
	flushStrings := func() error {
		if len(entries) == 0 {
			return nil
		}
		if err := s.SetStringBatch(entries); err != nil {
			logger.Logger.Warn().Int("count", len(entries)).Err(err).Msg("批量写入字符串失败")
		}
		entries = entries[:0]
		return nil
	}

	// 遍历所有键值对
	for dec.buf.Len() > 0 {
		// 检查是否到达文件尾
		if dec.buf.Len() > 0 {
			remaining := dec.buf.Bytes()
			if len(remaining) > 0 && remaining[0] == 0xFF {
				break
			}
		}

		// 读取过期时间
		expireTime, _ := dec.readExpireTime()
		var ttl time.Duration
		if expireTime > 0 {
			if expireTime > 0xFFFFFFFF {
				// 毫秒精度
				now := time.Now().UnixMilli()
				if expireTime > now {
					ttl = time.Duration(expireTime-now) * time.Millisecond
				}
			} else {
				// 秒精度
				expireAt := time.Unix(int64(expireTime), 0)
				if expireAt.After(time.Now()) {
					ttl = time.Until(expireAt)
				}
			}
		}

		// 读取类型
		if dec.buf.Len() == 0 {
			break
		}
		typeByte, _ := dec.buf.ReadByte()

		// 读取键
		key, err := dec.readString()
		if err != nil {
			logger.Logger.Warn().Err(err).Msg("读取RDB键失败，跳过")
			continue
		}

		// 遇到非字符串类型时，先刷新字符串缓冲区
		// 这样复杂类型和字符串类型不会混在同一个事务中
		if typeByte != 0 && len(entries) > 0 {
			fsErr := flushStrings()
			if fsErr != nil {
				logger.Logger.Warn().Err(fsErr).Msg("刷新字符串批量缓冲区失败")
			}
		}

		// 根据类型读取值
		switch typeByte {
		case 0: // STRING
			value, err := dec.readString()
			if err != nil {
				logger.Logger.Warn().Str("key", key).Err(err).Msg("读取字符串值失败，跳过")
				continue
			}
			entries = append(entries, store.StringEntry{
				Key:   key,
				Value: value,
				TTL:   ttl,
			})
			if len(entries) >= rdbBatchSize {
				fsErr := flushStrings()
				if fsErr != nil {
					logger.Logger.Warn().Err(fsErr).Msg("刷新字符串批量缓冲区失败")
				}
			}

		case 1: // LIST
			length, err := dec.readLength()
			if err != nil {
				logger.Logger.Warn().Str("key", key).Err(err).Msg("读取列表长度失败，跳过")
				continue
			}
			values := make([]string, 0, length)
			for i := uint64(0); i < length; i++ {
				val, err := dec.readString()
				if err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("读取列表元素失败，跳过")
					continue
				}
				values = append(values, val)
			}
			for _, v := range values {
				if _, err := s.RPush(key, v); err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("存储列表值失败")
				}
			}
			if ttl > 0 {
				if _, err := s.PExpire(key, int64(ttl.Milliseconds())); err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("RDB: 设置列表TTL失败")
				}
			}

		case 2: // SET
			length, err := dec.readLength()
			if err != nil {
				logger.Logger.Warn().Str("key", key).Err(err).Msg("读取集合长度失败，跳过")
				continue
			}
			for i := uint64(0); i < length; i++ {
				member, err := dec.readString()
				if err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("读取集合元素失败，跳过")
					continue
				}
				if _, err := s.SAdd(key, member); err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("存储集合值失败")
				}
			}
			if ttl > 0 {
				if _, err := s.PExpire(key, int64(ttl.Milliseconds())); err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("RDB: 设置集合TTL失败")
				}
			}

		case 3: // HASH
			length, err := dec.readLength()
			if err != nil {
				logger.Logger.Warn().Str("key", key).Err(err).Msg("读取哈希长度失败，跳过")
				continue
			}
			for i := uint64(0); i < length; i++ {
				field, err := dec.readString()
				if err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("读取哈希字段失败，跳过")
					continue
				}
				value, err := dec.readString()
				if err != nil {
					logger.Logger.Warn().Str("key", key).Str("field", field).Err(err).Msg("读取哈希值失败，跳过")
					continue
				}
				if err := s.HSet(key, field, value); err != nil {
					logger.Logger.Warn().Str("key", key).Str("field", field).Err(err).Msg("存储哈希值失败")
				}
			}
			if ttl > 0 {
				if _, err := s.PExpire(key, int64(ttl.Milliseconds())); err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("RDB: 设置哈希TTL失败")
				}
			}

		case 4: // ZSET (SortedSet)
			length, err := dec.readLength()
			if err != nil {
				logger.Logger.Warn().Str("key", key).Err(err).Msg("读取有序集合长度失败，跳过")
				continue
			}
			members := make([]store.ZSetMember, 0, length)
			for i := uint64(0); i < length; i++ {
				member, err := dec.readString()
				if err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("读取有序集合成员失败，跳过")
					continue
				}
				scoreBytes, err := dec.readBytes()
				if err != nil {
					logger.Logger.Warn().Str("key", key).Str("member", member).Err(err).Msg("读取有序集合分数失败，跳过")
					continue
				}
				score, err := strconv.ParseFloat(string(scoreBytes), 64)
				if err != nil {
					logger.Logger.Warn().Str("key", key).Str("member", member).Err(err).Msg("解析有序集合分数失败，跳过")
					continue
				}
				members = append(members, store.ZSetMember{Member: member, Score: score})
			}
			if len(members) > 0 {
				if err := s.ZAdd(key, members); err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("存储有序集合值失败")
				}
			}
			if ttl > 0 {
				if _, err := s.PExpire(key, int64(ttl.Milliseconds())); err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("RDB: 设置有序集合TTL失败")
				}
			}

		case 5, 15: // STREAM (5=entries only; 15=entries+groups/PEL)
			_, err := dec.readLength() // total length (skip, use numEntries below)
			if err != nil {
				logger.Logger.Warn().Str("key", key).Err(err).Msg("读取stream长度失败，跳过")
				continue
			}
			_, err = dec.readString()
			if err != nil {
				logger.Logger.Warn().Str("key", key).Err(err).Msg("读取stream firstID失败，跳过")
				continue
			}
			_, err = dec.readString()
			if err != nil {
				logger.Logger.Warn().Str("key", key).Err(err).Msg("读取stream lastID失败，跳过")
				continue
			}
			numEntries, err := dec.readLength()
			if err != nil {
				logger.Logger.Warn().Str("key", key).Err(err).Msg("读取stream条目数失败，跳过")
				continue
			}
			for i := uint64(0); i < numEntries; i++ {
				entryID, err := dec.readString()
				if err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("读取stream条目ID失败，跳过")
					continue
				}
				numFields, err := dec.readLength()
				if err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("读取stream字段数失败，跳过")
					continue
				}
				fields := make(map[string]string)
				for j := uint64(0); j < numFields; j++ {
					fieldName, err := dec.readString()
					if err != nil {
						logger.Logger.Warn().Str("key", key).Err(err).Msg("读取stream字段名失败，跳过")
						continue
					}
					fieldValue, err := dec.readString()
					if err != nil {
						logger.Logger.Warn().Str("key", key).Str("field", fieldName).Err(err).Msg("读取stream字段值失败，跳过")
						continue
					}
					fields[fieldName] = fieldValue
				}
				if _, err := s.XAdd(key, store.StreamXAddOptions{}, entryID, fields); err != nil {
					logger.Logger.Warn().Str("key", key).Str("id", entryID).Err(err).Msg("RDB加载stream条目失败")
				}
			}
			// type 15: consumer groups + PEL
			if typeByte == 15 {
				numGroups, gErr := dec.readLength()
				if gErr != nil {
					logger.Logger.Warn().Str("key", key).Err(gErr).Msg("读取stream groups数失败")
					continue
				}
				for gi := uint64(0); gi < numGroups; gi++ {
					gName, err := dec.readString()
					if err != nil {
						logger.Logger.Warn().Str("key", key).Err(err).Msg("读取group name失败")
						break
					}
					lastID, err := dec.readString()
					if err != nil {
						logger.Logger.Warn().Str("key", key).Err(err).Msg("读取group lastDeliveredID失败")
						break
					}
					group := &store.StreamGroup{
						Name:            gName,
						LastDeliveredID: lastID,
						Consumers:       make(map[string]*store.StreamConsumer),
						Pending:         make(map[string]*store.StreamPendingEntry),
					}
					nCons, err := dec.readLength()
					if err != nil {
						logger.Logger.Warn().Str("key", key).Err(err).Msg("读取group consumers失败")
						break
					}
					for ci := uint64(0); ci < nCons; ci++ {
						cName, err := dec.readString()
						if err != nil {
							break
						}
						var lastSeen int64
						if err := binary.Read(dec.buf, binary.LittleEndian, &lastSeen); err != nil {
							break
						}
						group.Consumers[cName] = &store.StreamConsumer{Name: cName, LastSeen: lastSeen}
					}
					nPend, err := dec.readLength()
					if err != nil {
						logger.Logger.Warn().Str("key", key).Err(err).Msg("读取group pending失败")
						break
					}
					for pi := uint64(0); pi < nPend; pi++ {
						pID, err := dec.readString()
						if err != nil {
							break
						}
						pCons, err := dec.readString()
						if err != nil {
							break
						}
						var dCount, lastDel int64
						if err := binary.Read(dec.buf, binary.LittleEndian, &dCount); err != nil {
							break
						}
						if err := binary.Read(dec.buf, binary.LittleEndian, &lastDel); err != nil {
							break
						}
						group.Pending[pID] = &store.StreamPendingEntry{
							ID:            pID,
							Consumer:      pCons,
							DeliveryCount: dCount,
							LastDelivery:  lastDel,
						}
					}
					if err := s.XGroupRestore(key, group); err != nil {
						logger.Logger.Warn().Str("key", key).Str("group", gName).Err(err).Msg("RDB恢复stream group失败")
					}
				}
			}

		case 6: // GEO
			length, err := dec.readLength()
			if err != nil {
				logger.Logger.Warn().Str("key", key).Err(err).Msg("读取geo长度失败，跳过")
				continue
			}
			var geoMembers []store.GeoMember
			for i := uint64(0); i < length; i++ {
				member, err := dec.readString()
				if err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("读取geo成员失败，跳过")
					continue
				}
				var lat, lon float64
				if err := binary.Read(dec.buf, binary.LittleEndian, &lat); err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("读取geo纬度失败，跳过")
					continue
				}
				if err := binary.Read(dec.buf, binary.LittleEndian, &lon); err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("读取geo经度失败，跳过")
					continue
				}
				geoMembers = append(geoMembers, store.GeoMember{Member: member, Lat: lat, Lon: lon})
			}
			if len(geoMembers) > 0 {
				if _, err := s.GeoAdd(key, geoMembers); err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("存储geo值失败")
				}
			}
			if ttl > 0 {
				if _, err := s.PExpire(key, int64(ttl.Milliseconds())); err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("RDB: 设置geo TTL失败")
				}
			}

		case 7: // JSON
			value, err := dec.readString()
			if err != nil {
				logger.Logger.Warn().Str("key", key).Err(err).Msg("读取JSON值失败，跳过")
				continue
			}
			if _, err := s.JSONSet(key, "$", value, false, false); err != nil {
				logger.Logger.Warn().Str("key", key).Err(err).Msg("存储JSON值失败")
			}

		case 9: // HLL
			data, err := dec.readBytes()
			if err != nil {
				logger.Logger.Warn().Str("key", key).Err(err).Msg("读取HLL数据失败，跳过")
				continue
			}
			if err := s.RestoreHLL(key, data); err != nil {
				logger.Logger.Warn().Str("key", key).Err(err).Msg("存储HLL值失败")
			}

		case 8: // TIMESERIES
			length, err := dec.readLength()
			if err != nil {
				logger.Logger.Warn().Str("key", key).Err(err).Msg("读取time series长度失败，跳过")
				continue
			}
			for i := uint64(0); i < length; i++ {
				var timestamp int64
				var value float64
				if err := binary.Read(dec.buf, binary.LittleEndian, &timestamp); err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("读取time series时间戳失败，跳过")
					continue
				}
				if err := binary.Read(dec.buf, binary.LittleEndian, &value); err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("读取time series值失败，跳过")
					continue
				}
				opts := store.TSAddOptions{}
				if _, err := s.TSAdd(key, timestamp, value, opts); err != nil {
					logger.Logger.Warn().Str("key", key).Int64("ts", timestamp).Err(err).Msg("RDB加载time series数据点失败")
				}
			}

		case 0xFE: // DATABASE SELECTOR
			// 忽略数据库选择器，我们只使用数据库0
			logger.Logger.Debug().Msg("跳过数据库选择器")

		default:
			logger.Logger.Warn().Uint8("type", typeByte).Str("key", key).Msg("未知的RDB数据类型，跳过")
			return nil
		}
	}

	// 最后刷新一次
	fsErr := flushStrings()
	if fsErr != nil {
		logger.Logger.Warn().Err(fsErr).Msg("最终刷新字符串批量缓冲区失败")
	}

	// CRC64 校验：验证 RDB 尾部 8 字节校验和
	const crc64Size = 8

	// 先消费 0xFF 结束符（loop 只检查未消费）
	if dec.buf.Len() == 0 {
		return fmt.Errorf("RDB file truncated: missing 0xFF end-of-file marker")
	}
	marker, err := dec.buf.ReadByte()
	if err != nil {
		return fmt.Errorf("failed to read end-of-file marker: %w", err)
	}
	if marker != 0xFF {
		return fmt.Errorf("invalid RDB end-of-file marker: expected 0xFF, got 0x%02x", marker)
	}

	// 剩余字节应是 8 字节 CRC64
	if dec.buf.Len() < crc64Size {
		return fmt.Errorf("RDB file truncated: expected 8-byte CRC64 checksum, got %d bytes remaining", dec.buf.Len())
	}
	if dec.buf.Len() > crc64Size {
		logger.Logger.Warn().Int("extra_bytes", dec.buf.Len()-crc64Size).Msg("RDB has trailing data beyond CRC64 checksum")
	}

	storedCRC := make([]byte, crc64Size)
	if _, err := io.ReadFull(dec.buf, storedCRC); err != nil {
		return fmt.Errorf("failed to read CRC64 checksum: %w", err)
	}

	// CRC 覆盖整个 RDB 文件（magic + version + 条目 + 0xFF 结束符），不含校验和自身
	totalLen := len(dec.origData)
	if totalLen <= crc64Size {
		return fmt.Errorf("RDB data too short: %d bytes", totalLen)
	}
	hash := crc64.New(crc64.MakeTable(crc64.ECMA))
	hash.Write(dec.origData[:totalLen-crc64Size])
	expectedCRC := hash.Sum(nil)

	if !bytes.Equal(storedCRC, expectedCRC) {
		return fmt.Errorf("RDB CRC64 checksum mismatch: stored %x, computed %x", storedCRC, expectedCRC)
	}

	logger.Logger.Info().Msg("RDB数据加载完成，CRC64校验通过")
	return nil
}
