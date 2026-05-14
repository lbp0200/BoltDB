package integration

import (
	"context"
	"testing"

	"github.com/zeebo/assert"
)

// TestBitSetGet 测试 SETBIT 和 GETBIT 命令
func TestBitSetGet(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// SETBIT - 设置位
	err := sharedClient.SetBit(ctx, "bitkey", 7, 1).Err()
	assert.NoError(t, err)

	// GETBIT - 获取位
	bit, err := sharedClient.GetBit(ctx, "bitkey", 7).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), bit)

	// 获取未设置的位
	bit, err = sharedClient.GetBit(ctx, "bitkey", 0).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), bit)

	// 设置位为0
	err = sharedClient.SetBit(ctx, "bitkey", 7, 0).Err()
	assert.NoError(t, err)

	bit, err = sharedClient.GetBit(ctx, "bitkey", 7).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), bit)
}

// TestBitCount 测试 BITCOUNT 命令
func TestBitCount(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 设置一个字符串，其二进制表示中有几个位被设置
	// "f" = 0x66 = 01100110，有4个位被设置
	_ = sharedClient.Set(ctx, "bitcountkey", "f", 0).Err()

	count, err := sharedClient.BitCount(ctx, "bitcountkey", nil).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(4), count)

	// 指定范围
	result, err := sharedClient.Do(ctx, "BITCOUNT", "bitcountkey", "0", "0").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(4), result)
}

// TestBitOp 测试 BITOP 命令
func TestBitOp(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 准备测试数据
	// "a" = 0x61 = 01100001
	// "b" = 0x62 = 01100010
	_ = sharedClient.Set(ctx, "bitop1", "a", 0).Err()
	_ = sharedClient.Set(ctx, "bitop2", "b", 0).Err()

	// BITAND - 按位与
	result, err := sharedClient.Do(ctx, "BITOP", "AND", "bitandresult", "bitop1", "bitop2").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), result)

	val, _ := sharedClient.Get(ctx, "bitandresult").Result()
	// 0x61 & 0x62 = 0x60 = "`"
	assert.Equal(t, "`", val)

	// BITOR - 按位或
	result, err = sharedClient.Do(ctx, "BITOP", "OR", "bitorresult", "bitop1", "bitop2").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), result)

	val, _ = sharedClient.Get(ctx, "bitorresult").Result()
	// 0x61 | 0x62 = 0x63 = "c"
	assert.Equal(t, "c", val)

	// BITXOR - 按位异或
	result, err = sharedClient.Do(ctx, "BITOP", "XOR", "bitxorresult", "bitop1", "bitop2").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), result)

	val, _ = sharedClient.Get(ctx, "bitxorresult").Result()
	// 0x61 ^ 0x62 = 0x03
	assert.Equal(t, "\x03", val)

	// BITNOT - 按位取反
	_ = sharedClient.Set(ctx, "bitnotkey", "\x00", 0).Err()
	result, err = sharedClient.Do(ctx, "BITOP", "NOT", "bitnotresult", "bitnotkey").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), result)

	val, _ = sharedClient.Get(ctx, "bitnotresult").Result()
	// ~0x00 = 0xFF
	assert.Equal(t, "\xff", val)
}

// TestBitField 测试 BITFIELD 命令
func TestBitField(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// BITFIELD mykey SET u8 0 255
	result, err := sharedClient.Do(ctx, "BITFIELD", "bitfieldkey", "SET", "u8", "0", "255").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result)

	// GET u8 0
	result, err = sharedClient.Do(ctx, "BITFIELD", "bitfieldkey", "GET", "u8", "0").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(255), result)

	// BITFIELD mykey INCRBY u8 0 1
	result, err = sharedClient.Do(ctx, "BITFIELD", "bitfieldkey", "INCRBY", "u8", "0", "1").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result) // 溢出回绕

	// 多操作
	result, err = sharedClient.Do(ctx, "BITFIELD", "bitfieldkey2", "SET", "i8", "8", "100", "GET", "u8", "8").Result()
	assert.NoError(t, err)
	// 返回数组 [old_value, new_value]
	arr, ok := result.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr))
}

// TestBitFieldIncr 测试 BITFIELD INCR 命令
func TestBitFieldIncr(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 设置初始值
	_, err := sharedClient.Do(ctx, "BITFIELD", "incrkey", "SET", "i8", "0", "10").Result()
	assert.NoError(t, err)

	// INCRBY i8 0 5 - 增加5
	result, err := sharedClient.Do(ctx, "BITFIELD", "incrkey", "INCRBY", "i8", "0", "5").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(15), result)

	// INCRBY i8 0 -10 - 减少10
	result, err = sharedClient.Do(ctx, "BITFIELD", "incrkey", "INCRBY", "i8", "0", "-10").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(5), result)

	// INCRBY i8 0 200 - 溢出测试 (5 + 200 = 205, i8范围-128到127，溢出)
	result, err = sharedClient.Do(ctx, "BITFIELD", "incrkey", "INCRBY", "i8", "0", "200").Result()
	assert.NoError(t, err)
	// 205 - 256 = -51
	assert.Equal(t, int64(-51), result)
}

// TestBitPos 测试 BITPOS 命令
func TestBitPos(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// "\x00\x00\x00" - 所有位都是0
	_ = sharedClient.Set(ctx, "bitposkey", "\x00\x00\x00", 0).Err()

	// 查找第一个设置为1的位 (应该返回-1或超出范围)
	result, err := sharedClient.Do(ctx, "BITPOS", "bitposkey", "1").Result()
	assert.NoError(t, err)
	// 超出范围时返回-1
	assert.Equal(t, int64(-1), result)

	// "\xff\x00\x00" - 第一个字节全是1，后两个是0
	_ = sharedClient.Set(ctx, "bitposkey2", "\xff\x00\x00", 0).Err()

	// 查找第一个0位，应该在第8位
	result, err = sharedClient.Do(ctx, "BITPOS", "bitposkey2", "0").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(8), result)

	// "\x80\x00\x00" - 只有第一位是1
	_ = sharedClient.Set(ctx, "bitposkey3", "\x80\x00\x00", 0).Err()

	result, err = sharedClient.Do(ctx, "BITPOS", "bitposkey3", "1").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result)
}

// TestBitLen 测试 BITLEN 命令
func TestBitLen(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// "hello" = 5字节 = 40位
	_ = sharedClient.Set(ctx, "bitlenkey", "hello", 0).Err()

	result, err := sharedClient.Do(ctx, "BITLEN", "bitlenkey").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(40), result)

	// 空字符串
	_ = sharedClient.Set(ctx, "bitlenkey2", "", 0).Err()
	result, err = sharedClient.Do(ctx, "BITLEN", "bitlenkey2").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result)
}
