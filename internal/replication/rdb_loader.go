package replication

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"time"

	"github.com/lbp0200/BoltDB/internal/logger"
	"github.com/lbp0200/BoltDB/internal/store"
)

// RDBDecoder RDB解码器
type RDBDecoder struct {
	buf     *bytes.Buffer
	version string
}

// NewRDBDecoder 创建新的RDB解码器
func NewRDBDecoder(data []byte) *RDBDecoder {
	return &RDBDecoder{
		buf: bytes.NewBuffer(data),
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
	if b&0x80 == 0 {
		// 6位长度
		return uint64(b & 0x3F), nil
	} else if b&0x40 == 0 {
		// 14位长度
		if dec.buf.Len() < 1 {
			return 0, fmt.Errorf("unexpected end of buffer")
		}
		b2 := dec.buf.Next(1)[0]
		return uint64(((uint64(b) & 0x3F) << 8) | uint64(b2)), nil
	} else {
		// 32位长度
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

	logger.Logger.Info().Str("version", dec.version).Msg("开始加载RDB数据")

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

		// 根据类型读取值
		switch typeByte {
		case 0: // STRING
			value, err := dec.readString()
			if err != nil {
				logger.Logger.Warn().Str("key", key).Err(err).Msg("读取字符串值失败，跳过")
				continue
			}
			if ttl > 0 {
				_ = rm.store.SetWithTTL(key, value, ttl)
			} else {
				if err := rm.store.Set(key, value); err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("存储字符串值失败")
					continue
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
				if _, err := rm.store.RPush(key, v); err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("存储列表值失败")
				}
			}
			if ttl > 0 {
				_, _ = rm.store.PExpire(key, int64(ttl.Milliseconds()))
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
				if _, err := rm.store.SAdd(key, member); err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("存储集合值失败")
				}
			}
			if ttl > 0 {
				_, _ = rm.store.PExpire(key, int64(ttl.Milliseconds()))
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
				if err := rm.store.HSet(key, field, value); err != nil {
					logger.Logger.Warn().Str("key", key).Str("field", field).Err(err).Msg("存储哈希值失败")
				}
			}
			if ttl > 0 {
				_, _ = rm.store.PExpire(key, int64(ttl.Milliseconds()))
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
				if err := rm.store.ZAdd(key, members); err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("存储有序集合值失败")
				}
			}
			if ttl > 0 {
				_, _ = rm.store.PExpire(key, int64(ttl.Milliseconds()))
			}

		case 0xFE: // DATABASE SELECTOR
			// 忽略数据库选择器，我们只使用数据库0
			logger.Logger.Debug().Msg("跳过数据库选择器")

		default:
			logger.Logger.Warn().Uint8("type", typeByte).Str("key", key).Msg("未知的RDB数据类型，跳过")
			return nil
		}
	}

	logger.Logger.Info().Msg("RDB数据加载完成")
	return nil
}

// LoadRDBWithStore 使用指定存储加载RDB数据（用于从节点同步）
func LoadRDBWithStore(data []byte, s *store.BotreonStore) error {
	dec := NewRDBDecoder(data)

	// 解码头部
	if err := dec.DecodeHeader(); err != nil {
		return fmt.Errorf("failed to decode RDB header: %w", err)
	}

	logger.Logger.Info().Str("version", dec.version).Msg("开始加载RDB数据")

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

		// 根据类型读取值
		switch typeByte {
		case 0: // STRING
			value, err := dec.readString()
			if err != nil {
				logger.Logger.Warn().Str("key", key).Err(err).Msg("读取字符串值失败，跳过")
				continue
			}
			if ttl > 0 {
				_ = s.SetWithTTL(key, value, ttl)
			} else {
				if err := s.Set(key, value); err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("存储字符串值失败")
					continue
				}
			}

		case 1: // LIST
			length, err := dec.readLength()
			if err != nil {
				logger.Logger.Warn().Str("key", key).Err(err).Msg("读取列表长度失败，跳过")
				continue
			}
			for i := uint64(0); i < length; i++ {
				val, err := dec.readString()
				if err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("读取列表元素失败，跳过")
					continue
				}
				if _, err := s.RPush(key, val); err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("存储列表值失败")
				}
			}
			if ttl > 0 {
				_, _ = s.PExpire(key, int64(ttl.Milliseconds()))
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
				_, _ = s.PExpire(key, int64(ttl.Milliseconds()))
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
				_, _ = s.PExpire(key, int64(ttl.Milliseconds()))
			}

		case 4: // ZSET
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
				_, _ = s.PExpire(key, int64(ttl.Milliseconds()))
			}

		case 0xFE:
			logger.Logger.Debug().Msg("跳过数据库选择器")

		default:
			logger.Logger.Warn().Uint8("type", typeByte).Str("key", key).Msg("未知的RDB数据类型，跳过")
			return nil
		}
	}

	logger.Logger.Info().Msg("RDB数据加载完成")
	return nil
}
