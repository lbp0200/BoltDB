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
)

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

		// 获取 TTL（毫秒）— 从值键的 ExpiresAt 读取（需处理双格式）
		var ttl int64 = 0
		valueKey, err := s.getKeyValueKey(key, keyType)
		if err == nil {
			if valItem, err := txn.Get(valueKey); err == nil {
				if expiresAt := valItem.ExpiresAt(); expiresAt > 0 {
					nowUnix := uint64(time.Now().Unix())
					// 秒格式
					remaining := (int64(expiresAt) - int64(nowUnix)) * 1_000_000_000
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
	case 0xFC: // 毫秒
		if buf.Len() < 8 {
			return 0, false
		}
		var expireMS uint64
		if err := binary.Read(buf, binary.LittleEndian, &expireMS); err != nil {
			return 0, false
		}
		return int64(expireMS), true
	case 0xFD: // 秒
		if buf.Len() < 4 {
			return 0, false
		}
		var expireSec uint32
		if err := binary.Read(buf, binary.LittleEndian, &expireSec); err != nil {
			return 0, false
		}
		return int64(expireSec) * 1000, true
	default:
		// 不是过期时间标记，回退
		return 0, false
	}
}

// Restore 实现 Redis RESTORE 命令，使用 RDB 格式反序列化键值
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

// restoreLegacy 处理旧版序列化格式（向后兼容）
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

// getListData 获取列表的所有元素
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
