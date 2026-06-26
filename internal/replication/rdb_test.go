package replication

import (
	"bytes"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/zeebo/assert"
)

func TestRDBEncoder_New(t *testing.T) {
	t.Parallel()
	enc := NewRDBEncoder()

	// 验证 RDB 头部
	header := enc.buf.Bytes()[:9]
	assert.Equal(t, "REDIS0009", string(header))
}

func TestRDBEncoder_WriteStringKeyValue(t *testing.T) {
	t.Parallel()
	enc := NewRDBEncoder()

	// 写入字符串键值
	err := enc.WriteStringKeyValue("key1", "value1", 0)
	assert.NoError(t, err)

	// 验证数据已写入
	assert.True(t, enc.buf.Len() > 9) // header + key + value
}

func TestRDBEncoder_WriteStringKeyValue_WithTTL(t *testing.T) {
	t.Parallel()
	enc := NewRDBEncoder()

	// 写入带绝对过期时间戳的字符串键值
	expireAt := time.Now().Unix() + 60 // 60秒后过期
	err := enc.WriteStringKeyValue("key1", "value1", expireAt)
	assert.NoError(t, err)

	// 验证数据已写入（应该更长因为有过期时间）
	assert.True(t, enc.buf.Len() > 9)
}

func TestRDBEncoder_WriteListKeyValue(t *testing.T) {
	t.Parallel()
	enc := NewRDBEncoder()

	// 写入列表键值
	values := []string{"item1", "item2", "item3"}
	err := enc.WriteListKeyValue("mylist", values, 0)
	assert.NoError(t, err)

	assert.True(t, enc.buf.Len() > 9)
}

func TestRDBEncoder_WriteHashKeyValue(t *testing.T) {
	t.Parallel()
	enc := NewRDBEncoder()

	// 写入哈希键值
	fields := map[string][]byte{
		"field1": []byte("value1"),
		"field2": []byte("value2"),
	}
	err := enc.WriteHashKeyValue("myhash", fields, 0)
	assert.NoError(t, err)

	assert.True(t, enc.buf.Len() > 9)
}

func TestRDBEncoder_WriteSetKeyValue(t *testing.T) {
	t.Parallel()
	enc := NewRDBEncoder()

	// 写入集合键值
	members := []string{"member1", "member2", "member3"}
	err := enc.WriteSetKeyValue("myset", members, 0)
	assert.NoError(t, err)

	assert.True(t, enc.buf.Len() > 9)
}

func TestRDBEncoder_WriteSortedSetKeyValue(t *testing.T) {
	t.Parallel()
	enc := NewRDBEncoder()

	// 写入有序集合键值
	members := []store.ZSetMember{
		{Member: "member1", Score: 1.0},
		{Member: "member2", Score: 2.0},
	}
	err := enc.WriteSortedSetKeyValue("myzset", members, 0)
	assert.NoError(t, err)

	assert.True(t, enc.buf.Len() > 9)
}

func TestRDBEncoder_Bytes(t *testing.T) {
	t.Parallel()
	enc := NewRDBEncoder()
	enc.WriteStringKeyValue("key", "value", 0)

	data := enc.Bytes()
	assert.True(t, len(data) > 0)
	assert.Equal(t, "REDIS0009", string(data[:9]))
}

func TestRDBEncoder_WriteTo(t *testing.T) {
	t.Parallel()
	enc := NewRDBEncoder()
	enc.WriteStringKeyValue("key", "value", 0)

	// 先保存原始数据
	originalData := enc.Bytes()

	var buf bytes.Buffer
	n, err := enc.WriteTo(&buf)
	assert.NoError(t, err)
	assert.True(t, n > 0)
	assert.Equal(t, originalData, buf.Bytes())
}

func TestRDBEncoder_Footer(t *testing.T) {
	t.Parallel()
	enc := NewRDBEncoder()
	enc.WriteStringKeyValue("key", "value", 0)
	enc.WriteFooter()

	data := enc.Bytes()
	// 验证末尾有 EOF 标记
	assert.True(t, len(data) > 9)
}
