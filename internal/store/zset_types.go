package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
)

const (
	prefixKeySortedSetBytes = "zset:"
	sortedSetIndex          = ":index:"
	sortedSetData           = ":data:"
	UnderScore              = "_"
	KeyTypeSortedSet        = "zset"
)

// ZSetMember 定义排序集成员结构体
type ZSetMember struct {
	Member string
	Score  float64
}

// ZSetsMetaValue 定义元数据结构体，存储成员数量和版本
//
// Design note on Version (uint32):
//   - Version is bumped on every ZADD/ZINCRBY/ZREM that modifies the sorted set.
//   - uint32 wraps at ~4.3 billion. After wrap, a new index key may collide with
//     an old index key from a previous version. In practice this requires ~4.3B
//     mutations on a single sorted set, which is virtually impossible under normal
//     workloads. If such scale is anticipated, consider migrating to uint64.
type ZSetsMetaValue struct {
	Card    int64
	Version uint32
}

// encodeScore 优化分数编码，确保负分数正确排序
func encodeScore(score float64) []byte {
	bits := math.Float64bits(score)
	b := make([]byte, 8)
	if score >= 0 {
		bits = bits ^ 0x8000000000000000
	} else {
		bits = ^bits
	}
	binary.BigEndian.PutUint64(b, bits)
	return b
}

// decodeScore 从字节解码回 float64
func decodeScore(b []byte) float64 {
	bits := binary.BigEndian.Uint64(b)
	if bits&0x8000000000000000 == 0 {
		bits = ^bits
	} else {
		bits = bits ^ 0x8000000000000000
	}
	return math.Float64frombits(bits)
}

// DecodeScore 是 decodeScore 的公开导出版本，供 replication 包使用。
// 将 encodeScore 编码的字节还原为 float64 分数值。
func DecodeScore(b []byte) float64 {
	return decodeScore(b)
}

// encodeMeta 编码元数据
func encodeMeta(meta ZSetsMetaValue) []byte {
	b := make([]byte, 12)
	// #nosec G115 - Card is bounded by practical sorted set size limits
	binary.BigEndian.PutUint64(b[:8], uint64(meta.Card))
	binary.BigEndian.PutUint32(b[8:], meta.Version)
	return b
}

// decodeMeta 解码元数据
func decodeMeta(b []byte) (ZSetsMetaValue, error) {
	if len(b) != 12 {
		return ZSetsMetaValue{}, errors.New("invalid meta data")
	}
	// #nosec G115 - card is bounded by practical sorted set size limits
	card := int64(binary.BigEndian.Uint64(b[:8]))
	version := binary.BigEndian.Uint32(b[8:])
	return ZSetsMetaValue{Card: card, Version: version}, nil
}

func sortedSetKeyMeta(zSetName string) []byte {
	return keyBadgerGet(prefixKeySortedSetBytes, []byte(zSetName+":meta"))
}

func sortedSetKeyIndex(zSetName string, score float64, member string, version uint32) []byte {
	key := []byte(zSetName + sortedSetIndex)
	key = append(key, encodeScore(score)...)
	key = append(key, []byte(":"+member+":")...)
	key = append(key, encodeVersion(version)...)
	return keyBadgerGet(prefixKeySortedSetBytes, key)
}

func sortedSetKeyMember(zSetName, member string) []byte {
	return keyBadgerGet(prefixKeySortedSetBytes, []byte(zSetName+sortedSetData+member))
}

func encodeVersion(version uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, version)
	return b
}

// encodeDataValue packs score (8 bytes, sorted-ordered) + version (4 bytes) into
// a 12-byte value. The score bytes are the same as encodeScore (IEEE 754 bit-flip
// for correct byte-order sorting). This allows reading both score and version from
// a single key lookup, eliminating the need to read the meta key for version.
func encodeDataValue(score float64, version uint32) []byte {
	b := make([]byte, 12)
	copy(b[:8], encodeScore(score))
	binary.BigEndian.PutUint32(b[8:], version)
	return b
}

// decodeDataValue unpacks score + version from a data value.
// Backward compatible: accepts both old 8-byte (score-only, version=0)
// and new 12-byte (score+version) formats.
func decodeDataValue(val []byte) (float64, uint32) {
	if len(val) >= 12 {
		score := decodeScore(val[:8])
		version := binary.BigEndian.Uint32(val[8:])
		return score, version
	}
	// Legacy 8-byte format: score only, assume version 0
	if len(val) >= 8 {
		score := decodeScore(val[:8])
		return score, 0
	}
	return 0, 0
}

// parseZSetIndexKey extracts score, member, version from a zset index key.
// Key format: zset:<name>:index:<8 byte score>:<member>:<4 byte version>
// This handles zSet names containing ':' unlike bytes.Split-based parsing.
func parseZSetIndexKey(key, prefix []byte) (score float64, member string, version uint32, ok bool) {
	if !bytes.HasPrefix(key, prefix) {
		return 0, "", 0, false
	}
	remaining := key[len(prefix):]
	if len(remaining) < 14 {
		return 0, "", 0, false
	}
	score = decodeScore(remaining[:8])
	member = string(remaining[9 : len(remaining)-5])
	version = binary.BigEndian.Uint32(remaining[len(remaining)-4:])
	return score, member, version, true
}

func keyBadgerGet(prefix string, key []byte) []byte {
	return append([]byte(prefix), key...)
}
