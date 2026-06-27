package store

import (
	"testing"

	"github.com/zeebo/assert"
)

// TestSetAndGet 测试 SET 和 GET 命令
func TestSetAndGet(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Test SET
	err := store.Set("key1", "value1")
	assert.NoError(t, err)

	// Test GET
	value, err := store.Get("key1")
	assert.NoError(t, err)
	assert.Equal(t, "value1", value)

	// Test GET non-existent key
	value, err = store.Get("nonexistent")
	assert.Error(t, err)
	assert.Equal(t, "", value)
}

// TestSetEX 测试 SETEX 命令
func TestSetEX(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	err := store.SetEX("key1", "value1", 1)
	assert.NoError(t, err)

	// 验证值已设置（不测试过期，因为Badger的TTL清理是异步的）
	value, err := store.Get("key1")
	assert.NoError(t, err)
	assert.Equal(t, "value1", value)
}

// TestPSETEX 测试 PSETEX 命令
func TestPSETEX(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// 使用较长的TTL以确保在测试期间不会过期
	err := store.PSETEX("key1", "value1", 10000)
	assert.NoError(t, err)

	// 验证值已设置（不测试过期，因为Badger的TTL清理是异步的）
	value, err := store.Get("key1")
	assert.NoError(t, err)
	assert.Equal(t, "value1", value)
}

// TestSetNX 测试 SETNX 命令
func TestSetNX(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// 第一次设置应该成功
	success, err := store.SetNX("key1", "value1")
	assert.NoError(t, err)
	assert.True(t, success)

	// 验证值已设置
	value, err := store.Get("key1")
	assert.NoError(t, err)
	assert.Equal(t, "value1", value)

	// 第二次设置应该失败（键已存在）
	success, err = store.SetNX("key1", "value2")
	assert.NoError(t, err)
	assert.False(t, success)

	// 验证值未改变
	value, err = store.Get("key1")
	assert.NoError(t, err)
	assert.Equal(t, "value1", value)
}

// TestGetSet 测试 GETSET 命令
func TestGetSet(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// 设置初始值
	err := store.Set("key1", "oldvalue")
	assert.NoError(t, err)

	// GETSET 应该返回旧值并设置新值
	oldValue, err := store.GetSet("key1", "newvalue")
	assert.NoError(t, err)
	assert.Equal(t, "oldvalue", oldValue)

	// 验证新值已设置
	value, err := store.Get("key1")
	assert.NoError(t, err)
	assert.Equal(t, "newvalue", value)

	// 对不存在的键使用 GETSET
	oldValue, err = store.GetSet("key2", "value2")
	assert.NoError(t, err)
	assert.Equal(t, "", oldValue) // 不存在的键返回空字符串

	value, err = store.Get("key2")
	assert.NoError(t, err)
	assert.Equal(t, "value2", value)
}

// TestMGet 测试 MGET 命令
func TestMGet(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// 设置多个键
	err := store.Set("key1", "value1")
	assert.NoError(t, err)
	err = store.Set("key2", "value2")
	assert.NoError(t, err)

	// 获取多个键
	values, err := store.MGet("key1", "key2", "nonexistent")
	assert.NoError(t, err)
	assert.Equal(t, 3, len(values))
	assert.Equal(t, "value1", values[0])
	assert.Equal(t, "value2", values[1])
	assert.Equal(t, "", values[2]) // 不存在的键返回空字符串
}

// TestMGetWrongType tests that MGet returns ErrWrongType when a key is not a string
func TestMGetWrongType(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Create a hash key
	err := store.HSet("myhash", "field1", "value1")
	assert.NoError(t, err)

	// Create a string key
	err = store.Set("mystring", "value1")
	assert.NoError(t, err)

	// MGet mixing string and non-string keys should return ErrWrongType
	_, err = store.MGet("mystring", "myhash")
	assert.Error(t, err)
	assert.Equal(t, ErrWrongType, err)

	// MGet with only non-existent keys should not error
	values, err := store.MGet("nonexistent1", "nonexistent2")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(values))
	assert.Equal(t, "", values[0])
	assert.Equal(t, "", values[1])
}

// TestMSet 测试 MSET 命令
func TestMSet(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// 设置多个键值对
	err := store.MSet("key1", "value1", "key2", "value2", "key3", "value3")
	assert.NoError(t, err)

	// 验证所有值
	value1, _ := store.Get("key1")
	assert.Equal(t, "value1", value1)
	value2, _ := store.Get("key2")
	assert.Equal(t, "value2", value2)
	value3, _ := store.Get("key3")
	assert.Equal(t, "value3", value3)
}

// TestMSetNX 测试 MSETNX 命令
func TestMSetNX(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// 第一次设置应该成功（所有键都不存在）
	success, err := store.MSetNX("key1", "value1", "key2", "value2")
	assert.NoError(t, err)
	assert.True(t, success)

	// 验证值已设置
	value1, _ := store.Get("key1")
	assert.Equal(t, "value1", value1)
	value2, _ := store.Get("key2")
	assert.Equal(t, "value2", value2)

	// 第二次设置应该失败（key1已存在）
	success, err = store.MSetNX("key1", "newvalue1", "key3", "value3")
	assert.NoError(t, err)
	assert.False(t, success)

	// 验证key1未改变，key3未设置
	value1, _ = store.Get("key1")
	assert.Equal(t, "value1", value1) // 未改变
	value3, _ := store.Get("key3")
	assert.Equal(t, "", value3) // 未设置
}

// TestINCR 测试 INCR 命令
func TestINCR(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// 对不存在的键执行 INCR
	value, err := store.INCR("counter")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), value)

	// 再次 INCR
	value, err = store.INCR("counter")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), value)

	// 验证值
	strValue, err := store.Get("counter")
	assert.NoError(t, err)
	assert.Equal(t, "2", strValue)
}

// TestINCRBY 测试 INCRBY 命令
func TestINCRBY(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// 设置初始值
	err := store.Set("counter", "10")
	assert.NoError(t, err)

	// INCRBY
	value, err := store.INCRBY("counter", 5)
	assert.NoError(t, err)
	assert.Equal(t, int64(15), value)

	// 再次 INCRBY
	value, err = store.INCRBY("counter", -3)
	assert.NoError(t, err)
	assert.Equal(t, int64(12), value)
}

// TestDECR 测试 DECR 命令
func TestDECR(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// 设置初始值
	err := store.Set("counter", "10")
	assert.NoError(t, err)

	// DECR
	value, err := store.DECR("counter")
	assert.NoError(t, err)
	assert.Equal(t, int64(9), value)

	// 再次 DECR
	value, err = store.DECR("counter")
	assert.NoError(t, err)
	assert.Equal(t, int64(8), value)
}

// TestDECRBY 测试 DECRBY 命令
func TestDECRBY(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// 设置初始值
	err := store.Set("counter", "10")
	assert.NoError(t, err)

	// DECRBY
	value, err := store.DECRBY("counter", 3)
	assert.NoError(t, err)
	assert.Equal(t, int64(7), value)

	// 再次 DECRBY
	value, err = store.DECRBY("counter", 5)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), value)
}

// TestINCRBYFLOAT 测试 INCRBYFLOAT 命令
func TestINCRBYFLOAT(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// 设置初始值
	err := store.Set("float", "10.5")
	assert.NoError(t, err)

	// INCRBYFLOAT
	value, err := store.INCRBYFLOAT("float", 2.3)
	assert.NoError(t, err)
	// 验证浮点数（允许小的误差）
	if value < 12.79 || value > 12.81 {
		t.Errorf("expected value around 12.8, got %f", value)
	}

	// 再次 INCRBYFLOAT
	value, err = store.INCRBYFLOAT("float", -1.2)
	assert.NoError(t, err)
	// 验证浮点数（允许小的误差）
	if value < 11.59 || value > 11.61 {
		t.Errorf("expected value around 11.6, got %f", value)
	}
}

// TestAPPEND 测试 APPEND 命令
func TestAPPEND(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// 设置初始值
	err := store.Set("key1", "Hello")
	assert.NoError(t, err)

	// APPEND
	length, err := store.APPEND("key1", " World")
	assert.NoError(t, err)
	assert.Equal(t, 11, length)

	// 验证值
	value, err := store.Get("key1")
	assert.NoError(t, err)
	assert.Equal(t, "Hello World", value)

	// 对不存在的键执行 APPEND
	length, err = store.APPEND("key2", "test")
	assert.NoError(t, err)
	assert.Equal(t, 4, length)

	value, err = store.Get("key2")
	assert.NoError(t, err)
	assert.Equal(t, "test", value)
}

// TestStrLen 测试 STRLEN 命令
func TestStrLen(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// 设置值
	err := store.Set("key1", "Hello")
	assert.NoError(t, err)

	// STRLEN
	length, err := store.StrLen("key1")
	assert.NoError(t, err)
	assert.Equal(t, 5, length)

	// 不存在的键
	length, err = store.StrLen("nonexistent")
	assert.NoError(t, err)
	assert.Equal(t, 0, length)
}

// TestGetRange 测试 GETRANGE 命令
func TestGetRange(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// 设置值
	err := store.Set("key1", "Hello World")
	assert.NoError(t, err)

	// GETRANGE 正常范围
	value, err := store.GetRange("key1", 0, 4)
	assert.NoError(t, err)
	assert.Equal(t, "Hello", value)

	// GETRANGE 负数索引
	value, err = store.GetRange("key1", -5, -1)
	assert.NoError(t, err)
	assert.Equal(t, "World", value)

	// GETRANGE 超出范围
	value, err = store.GetRange("key1", 0, 100)
	assert.NoError(t, err)
	assert.Equal(t, "Hello World", value)

	// 不存在的键
	value, err = store.GetRange("nonexistent", 0, 4)
	assert.NoError(t, err)
	assert.Equal(t, "", value)
}

// TestSetRange 测试 SETRANGE 命令
func TestSetRange(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// 设置初始值
	err := store.Set("key1", "Hello World")
	assert.NoError(t, err)

	// SETRANGE
	length, err := store.SetRange("key1", 6, "Redis")
	assert.NoError(t, err)
	assert.Equal(t, 11, length)

	// 验证值
	value, err := store.Get("key1")
	assert.NoError(t, err)
	assert.Equal(t, "Hello Redis", value)

	// SETRANGE 超出范围（应该扩展）
	length, err = store.SetRange("key2", 5, "test")
	assert.NoError(t, err)
	assert.Equal(t, 9, length)

	value, err = store.Get("key2")
	assert.NoError(t, err)
	assert.Equal(t, "\x00\x00\x00\x00\x00test", value)
}

// TestGetBit 测试 GETBIT 命令
func TestGetBit(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// 设置值 "A" = 0x41 = 01000001
	err := store.Set("key1", "A")
	assert.NoError(t, err)

	// GETBIT
	bit, err := store.GetBit("key1", 0)
	assert.NoError(t, err)
	assert.Equal(t, 0, bit) // 第一位是0

	bit, err = store.GetBit("key1", 1)
	assert.NoError(t, err)
	assert.Equal(t, 1, bit) // 第二位是1

	// 超出范围的位
	bit, err = store.GetBit("key1", 100)
	assert.NoError(t, err)
	assert.Equal(t, 0, bit) // 返回0

	// 不存在的键
	bit, err = store.GetBit("nonexistent", 0)
	assert.NoError(t, err)
	assert.Equal(t, 0, bit)
}

// TestSetBit 测试 SETBIT 命令
func TestSetBit(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// 设置值 "A" = 0x41 = 01000001
	err := store.Set("key1", "A")
	assert.NoError(t, err)

	// SETBIT 获取旧值并设置新值
	oldBit, err := store.SetBit("key1", 0, 1)
	assert.NoError(t, err)
	assert.Equal(t, 0, oldBit) // 旧值是0

	// 验证值已改变（应该是 0xC1 = 11000001 = "Á"）
	value, err := store.Get("key1")
	assert.NoError(t, err)
	assert.Equal(t, "\xC1", value)

	// SETBIT 超出范围（应该扩展）
	oldBit, err = store.SetBit("key2", 10, 1)
	assert.NoError(t, err)
	assert.Equal(t, 0, oldBit) // 新位，旧值是0

	// 验证扩展 — bit 10 = byte 1, offset 2 from MSB (big-endian) = 0x20
	value, err = store.Get("key2")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(value)) // 恰好 2 字节
	assert.Equal(t, byte(0x00), value[0])
	assert.Equal(t, byte(0x20), value[1]) // byte 1 bit 2 from MSB
}

// TestBitCount 测试 BITCOUNT 命令
func TestBitCount(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// 设置值 "A" = 0x41 = 01000001 (2个1)
	err := store.Set("key1", "A")
	assert.NoError(t, err)

	// BITCOUNT 整个字符串
	count, err := store.BitCount("key1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 2, count)

	// BITCOUNT 指定范围
	count, err = store.BitCount("key1", 0, 0)
	assert.NoError(t, err)
	assert.Equal(t, 2, count)

	// 不存在的键
	count, err = store.BitCount("nonexistent", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 0, count)
}

// TestBitOp 测试 BITOP 命令
func TestBitOp(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// 设置值
	err := store.Set("key1", "\x01") // 00000001
	assert.NoError(t, err)
	err = store.Set("key2", "\x02") // 00000010
	assert.NoError(t, err)

	// BITOP AND
	length, err := store.BitOp("AND", "dest", "key1", "key2")
	assert.NoError(t, err)
	assert.Equal(t, 1, length)

	value, err := store.Get("dest")
	assert.NoError(t, err)
	assert.Equal(t, "\x00", value) // 00000001 AND 00000010 = 00000000

	// BITOP OR
	length, err = store.BitOp("OR", "dest", "key1", "key2")
	assert.NoError(t, err)
	assert.Equal(t, 1, length)

	value, err = store.Get("dest")
	assert.NoError(t, err)
	assert.Equal(t, "\x03", value) // 00000001 OR 00000010 = 00000011

	// BITOP XOR
	length, err = store.BitOp("XOR", "dest", "key1", "key2")
	assert.NoError(t, err)
	assert.Equal(t, 1, length)

	value, err = store.Get("dest")
	assert.NoError(t, err)
	assert.Equal(t, "\x03", value) // 00000001 XOR 00000010 = 00000011

	// BITOP NOT
	length, err = store.BitOp("NOT", "dest", "key1")
	assert.NoError(t, err)
	assert.Equal(t, 1, length)

	value, err = store.Get("dest")
	assert.NoError(t, err)
	assert.Equal(t, "\xFE", value) // NOT 00000001 = 11111110
}

// TestBitPos 测试 BITPOS 命令
func TestBitPos(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// 设置值 "\x00\xFF" = 00000000 11111111
	err := store.Set("key1", "\x00\xFF")
	assert.NoError(t, err)

	// BITPOS 查找第一个1
	pos, err := store.BitPos("key1", 1, 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 8, pos) // 第8位（第二个字节的第一位）

	// BITPOS 查找第一个0
	pos, err = store.BitPos("key1", 0, 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 0, pos) // 第0位

	// 不存在的键
	pos, err = store.BitPos("nonexistent", 1, 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, -1, pos) // 未找到
}

// TestStringEdgeCases 测试边界情况
func TestStringEdgeCases(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// 空字符串
	err := store.Set("empty", "")
	assert.NoError(t, err)
	value, err := store.Get("empty")
	assert.NoError(t, err)
	assert.Equal(t, "", value)

	// 长字符串
	longStr := string(make([]byte, 1000))
	err = store.Set("long", longStr)
	assert.NoError(t, err)
	length, err := store.StrLen("long")
	assert.NoError(t, err)
	assert.Equal(t, 1000, length)

	// 特殊字符
	err = store.Set("special", "\x00\x01\x02\xFF")
	assert.NoError(t, err)
	value, err = store.Get("special")
	assert.NoError(t, err)
	assert.Equal(t, "\x00\x01\x02\xFF", value)
}

// TestBitLen 测试 BITLEN 命令
func TestBitLen(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Test with value
	err := store.Set("bits", "hi") // 'h' = 0x68 = 01101000, 'i' = 0x69 = 01101001
	assert.NoError(t, err)

	length, err := store.BitLen("bits")
	assert.NoError(t, err)
	assert.Equal(t, 16, length) // 2 bytes = 16 bits

	// Test non-existent key (should return 0)
	length, _ = store.BitLen("nonexistent")
	assert.Equal(t, 0, length)
}

// TestBitFieldDebug 测试 BITFIELD 调试
func TestBitFieldDebug(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// 测试 SET 后立即 GET
	_, err := store.BitField("testbyte", []string{"SET", "u8", "0", "65"})
	assert.NoError(t, err)
	t.Logf("SET u8 0 = 65 completed")

	// GET 应该返回之前存储的值（SET 返回旧值）
	getResult, err := store.BitField("testbyte", []string{"GET", "u8", "0"})
	assert.NoError(t, err)
	got, ok := getResult[0].(int64)
	assert.True(t, ok)
	t.Logf("GET u8 0 after SET 65 = %d (expected 65)", got)
	assert.Equal(t, int64(65), got)

	// 用 Get 验证实际存储的字节
	val, err := store.Get("testbyte")
	assert.NoError(t, err)
	if len(val) > 0 {
		t.Logf("Get returns byte: 0x%02x (%d)", val[0], val[0])
		assert.Equal(t, uint8(0x41), val[0])
	}

	// 测试用 Set 设置字符串 "A" 然后 BitField 读取
	err = store.Set("testbyte2", "A") // 0x41 = 65
	assert.NoError(t, err)

	val2, err := store.Get("testbyte2")
	assert.NoError(t, err)
	t.Logf("Set 'A' then Get: 0x%02x", val2[0])

	result2, err := store.BitField("testbyte2", []string{"GET", "u8", "0"})
	assert.NoError(t, err)
	got2, ok := result2[0].(int64)
	assert.True(t, ok)
	t.Logf("BitField GET u8 0 = %d (expected 65 = 0x41)", got2)
	assert.Equal(t, int64(65), got2)

	// 测试有符号数
	// 127 = 0x7F = 01111111 (bit 7 = 0, 正数)
	_, err = store.BitField("signed1", []string{"SET", "i8", "0", "127"})
	assert.NoError(t, err)
	result3, err := store.BitField("signed1", []string{"GET", "i8", "0"})
	assert.NoError(t, err)
	got3, ok := result3[0].(int64)
	assert.True(t, ok)
	t.Logf("GET i8 0 of 127 = %d (expected 127, positive)", got3)
	assert.Equal(t, int64(127), got3)

	// -128 = 0x80 = 10000000 (bit 7 = 1, 负数)
	_, err = store.BitField("signed2", []string{"SET", "i8", "0", "18446744073709551488"}) // -128 as unsigned
	// Use INCRBY to wrap to -128
	_, err = store.BitField("signed3", []string{"SET", "u8", "0", "128"})
	assert.NoError(t, err)
	result4, err := store.BitField("signed3", []string{"GET", "i8", "0"})
	assert.NoError(t, err)
	got4, ok := result4[0].(int64)
	assert.True(t, ok)
	t.Logf("GET i8 0 of byte 128 (0x80) = %d (expected -128, negative)", got4)
	assert.Equal(t, int64(-128), got4)
}

// TestBitField 测试 BITFIELD 命令
func TestBitField(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Test GET operation on non-existent key — should return 0
	result, err := store.BitField("key", []string{"GET", "u8", "0"})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(result))
	got, ok := result[0].(int64)
	assert.True(t, ok)
	assert.Equal(t, int64(0), got)

	// Test SET operation — returns old value (0 for new key)
	result, err = store.BitField("key", []string{"SET", "u8", "0", "255"})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(result))
	got, ok = result[0].(int64)
	assert.True(t, ok)
	assert.Equal(t, int64(0), got)

	// Verify the SET worked by GET — should return 255
	result, err = store.BitField("key", []string{"GET", "u8", "0"})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(result))
	got, ok = result[0].(int64)
	assert.True(t, ok)
	assert.Equal(t, int64(255), got)

	// Test INCRBY operation — 255 + 1 wraps around for u8
	result, err = store.BitField("key", []string{"INCRBY", "u8", "0", "1"})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(result))

	// Test with signed integers
	result, err = store.BitField("signed", []string{"SET", "i8", "0", "127"})
	assert.NoError(t, err)
	got, ok = result[0].(int64)
	assert.True(t, ok)
	assert.Equal(t, int64(0), got)

	result, err = store.BitField("signed", []string{"GET", "i8", "0"})
	assert.NoError(t, err)
	got, ok = result[0].(int64)
	assert.True(t, ok)
	assert.Equal(t, int64(127), got)

	// Test multiple operations — SET returns old, GET returns current
	result, err = store.BitField("multi", []string{"SET", "u8", "0", "10", "GET", "u8", "0"})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(result))
	// First result: old value of byte 0 (0 for new key)
	gotOld, ok := result[0].(int64)
	assert.True(t, ok)
	assert.Equal(t, int64(0), gotOld)
	// Second result: current value after SET (10)
	gotNew, ok := result[1].(int64)
	assert.True(t, ok)
	assert.Equal(t, int64(10), gotNew)
}

// TestBitFieldOverflow 测试 BITFIELD 溢出情况
func TestBitFieldOverflow(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Test with 16-bit
	err := store.Set("key16", "ab")
	assert.NoError(t, err)

	// GET u16 at bit offset 8 — reads bits 8-23 (byte 1 value + beyond)
	// "ab" = 0x61 0x62, so byte 1 = 0x62 = 98
	result, err := store.BitField("key16", []string{"GET", "u16", "8"})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(result))
	got, ok := result[0].(int64)
	assert.True(t, ok)
	assert.Equal(t, int64(98), got) // byte 1 = 'b' = 0x62 = 98
}

// TestSetWrongType tests that SET returns ErrWrongType when key exists with different type
func TestSetWrongType(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Create a hash key first
	err := store.HSet("myhash", "field1", "value1")
	assert.NoError(t, err)

	// Verify it's a hash
	keyType, err := store.Type("myhash")
	assert.NoError(t, err)
	assert.Equal(t, "hash", keyType)

	// Try to SET on the hash key - should return ErrWrongType
	err = store.Set("myhash", "value")
	assert.Error(t, err)
	assert.Equal(t, ErrWrongType, err)

	// Create a list key
	_, err = store.LPush("mylist", "value1")
	assert.NoError(t, err)

	// Verify it's a list
	keyType, err = store.Type("mylist")
	assert.NoError(t, err)
	assert.Equal(t, "list", keyType)

	// Try to SET on the list key - should return ErrWrongType
	err = store.Set("mylist", "value")
	assert.Error(t, err)
	assert.Equal(t, ErrWrongType, err)

	// Create a set key
	_, err = store.SAdd("myset", "member1")
	assert.NoError(t, err)

	// Verify it's a set
	keyType, err = store.Type("myset")
	assert.NoError(t, err)
	assert.Equal(t, "set", keyType)

	// Try to SET on the set key - should return ErrWrongType
	err = store.Set("myset", "value")
	assert.Error(t, err)
	assert.Equal(t, ErrWrongType, err)

	// Create a zset key
	err = store.ZAdd("myzset", []ZSetMember{{Member: "member1", Score: 1.0}})
	assert.NoError(t, err)

	// Verify it's a zset
	keyType, err = store.Type("myzset")
	assert.NoError(t, err)
	assert.Equal(t, "zset", keyType)

	// Try to SET on the zset key - should return ErrWrongType
	err = store.Set("myzset", "value")
	assert.Error(t, err)
	assert.Equal(t, ErrWrongType, err)

	// SET on non-existent key should work fine
	err = store.Set("newstring", "value")
	assert.NoError(t, err)

	// SET on existing string key should also work fine
	err = store.Set("newstring", "newvalue")
	assert.NoError(t, err)
}
