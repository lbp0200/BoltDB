package store

import (
	"testing"

	"github.com/zeebo/assert"
)

// TestLPush 测试 LPUSH 命令
func TestLPush(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	key := "mylist"

	// 单个元素
	n, err := store.LPush(key, "world")
	assert.NoError(t, err)
	assert.Equal(t, 1, n)

	// 多个元素
	n, err = store.LPush(key, "hello", "test")
	assert.NoError(t, err)
	assert.Equal(t, 3, n) // 返回操作后列表的长度

	// 验证长度
	length, err := store.LLen(key)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), length)

	// 验证顺序（LPUSH是头部插入，所以最后插入的在最前面）
	val, err := store.LIndex(key, 0)
	assert.NoError(t, err)
	assert.Equal(t, "test", val)
}

// TestRPush 测试 RPUSH 命令
func TestRPush(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	key := "mylist"

	// 单个元素
	n, err := store.RPush(key, "hello")
	assert.NoError(t, err)
	assert.Equal(t, 1, n)

	// 多个元素
	n, err = store.RPush(key, "world", "test")
	assert.NoError(t, err)
	assert.Equal(t, 3, n) // 返回操作后列表的长度

	// 验证长度
	length, err := store.LLen(key)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), length)

	// 验证顺序（RPUSH是尾部插入）
	val, err := store.LIndex(key, 0)
	assert.NoError(t, err)
	assert.Equal(t, "hello", val)
	val, err = store.LIndex(key, 2)
	assert.NoError(t, err)
	assert.Equal(t, "test", val)
}

// TestLPop 测试 LPOP 命令
func TestLPop(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	key := "mylist"

	// 设置初始值
	_, _ = store.LPush(key, "world", "hello")

	// LPOP
	val, err := store.LPop(key)
	assert.NoError(t, err)
	assert.Equal(t, "hello", val) // LPUSH后，hello在头部

	// 验证长度
	length, err := store.LLen(key)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), length)

	// 再次LPOP
	val, err = store.LPop(key)
	assert.NoError(t, err)
	assert.Equal(t, "world", val)

	// 空列表LPOP
	val, err = store.LPop(key)
	assert.NoError(t, err)
	assert.Equal(t, "", val)
}

// TestRPop 测试 RPOP 命令
func TestRPop(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	key := "mylist"

	// 设置初始值
	_, _ = store.LPush(key, "world", "hello")

	// RPOP
	val, err := store.RPop(key)
	assert.NoError(t, err)
	assert.Equal(t, "world", val) // LPUSH后，world在尾部

	// 验证长度
	length, err := store.LLen(key)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), length)

	// 再次RPOP
	val, err = store.RPop(key)
	assert.NoError(t, err)
	assert.Equal(t, "hello", val)

	// 空列表RPOP
	val, err = store.RPop(key)
	assert.NoError(t, err)
	assert.Equal(t, "", val)
}

// TestLLen 测试 LLEN 命令
func TestLLen(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	key := "mylist"

	// 空列表
	length, err := store.LLen(key)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), length)

	// 添加元素
	_, _ = store.LPush(key, "a", "b", "c")
	length, err = store.LLen(key)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), length)
}

// TestLIndex 测试 LINDEX 命令
func TestLIndex(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	key := "mylist"

	// 设置值
	_, _ = store.RPush(key, "a", "b", "c")

	// 正常索引
	val, err := store.LIndex(key, 0)
	assert.NoError(t, err)
	assert.Equal(t, "a", val)

	val, err = store.LIndex(key, 1)
	assert.NoError(t, err)
	assert.Equal(t, "b", val)

	val, err = store.LIndex(key, 2)
	assert.NoError(t, err)
	assert.Equal(t, "c", val)

	// 负数索引
	val, err = store.LIndex(key, -1)
	assert.NoError(t, err)
	assert.Equal(t, "c", val)

	val, err = store.LIndex(key, -2)
	assert.NoError(t, err)
	assert.Equal(t, "b", val)

	// 超出范围
	val, err = store.LIndex(key, 10)
	assert.NoError(t, err)
	assert.Equal(t, "", val)
}

// TestLRange 测试 LRANGE 命令
func TestLRange(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	key := "mylist"

	// 设置值
	_, _ = store.RPush(key, "a", "b", "c", "d", "e")

	// 正常范围
	values, err := store.LRange(key, 0, 2)
	assert.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c"}, values)

	// 负数索引
	values, err = store.LRange(key, -3, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"c", "d", "e"}, values)

	// 整个列表
	values, err = store.LRange(key, 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c", "d", "e"}, values)

	// 超出范围
	values, err = store.LRange(key, 10, 20)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(values)) // 空切片
}

// TestLSet 测试 LSET 命令
func TestLSet(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	key := "mylist"

	// 设置值
	_, _ = store.RPush(key, "a", "b", "c")

	// 设置索引0
	err := store.LSet(key, 0, "x")
	assert.NoError(t, err)
	val, _ := store.LIndex(key, 0)
	assert.Equal(t, "x", val)

	// 设置负数索引
	err = store.LSet(key, -1, "z")
	assert.NoError(t, err)
	val, _ = store.LIndex(key, 2)
	assert.Equal(t, "z", val)
}

// TestLTrim 测试 LTRIM 命令
func TestLTrim(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	key := "mylist"

	// 设置值
	_, _ = store.RPush(key, "a", "b", "c", "d", "e")

	// 修剪列表
	err := store.LTrim(key, 1, 3)
	assert.NoError(t, err)

	// 验证结果
	values, err := store.LRange(key, 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"b", "c", "d"}, values)

	// 修剪为空
	err = store.LTrim(key, 10, 20)
	assert.NoError(t, err)
	length, _ := store.LLen(key)
	assert.Equal(t, uint64(0), length)
}

// TestLInsert 测试 LINSERT 命令
func TestLInsert(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	key := "mylist"

	// 设置值
	_, _ = store.RPush(key, "a", "b", "c")

	// BEFORE插入
	count, err := store.LInsert(key, "BEFORE", "b", "x")
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	values, _ := store.LRange(key, 0, -1)
	assert.Equal(t, []string{"a", "x", "b", "c"}, values)

	// AFTER插入
	count, err = store.LInsert(key, "AFTER", "b", "y")
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	values, _ = store.LRange(key, 0, -1)
	assert.Equal(t, []string{"a", "x", "b", "y", "c"}, values)

	// pivot不存在
	count, err = store.LInsert(key, "BEFORE", "z", "w")
	assert.NoError(t, err)
	assert.Equal(t, 0, count)
}

// TestLRem 测试 LREM 命令
func TestLRem(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	key := "mylist"

	// 设置值
	_, _ = store.RPush(key, "a", "b", "a", "c", "a", "d")

	// 删除第一个a
	count, err := store.LRem(key, 1, "a")
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	values, _ := store.LRange(key, 0, -1)
	assert.Equal(t, []string{"b", "a", "c", "a", "d"}, values)

	// 删除所有a
	count, err = store.LRem(key, 0, "a")
	assert.NoError(t, err)
	assert.Equal(t, 2, count)

	values, _ = store.LRange(key, 0, -1)
	assert.Equal(t, []string{"b", "c", "d"}, values)

	// 从尾部删除
	_, _ = store.RPush(key, "x", "y", "x")
	count, err = store.LRem(key, -1, "x")
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	values, _ = store.LRange(key, 0, -1)
	assert.Equal(t, []string{"b", "c", "d", "x", "y"}, values)
}

// TestRPopLPush 测试 RPOPLPUSH 命令
func TestRPopLPush(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	source := "source"
	dest := "dest"

	// 设置源列表
	store.RPush(source, "a", "b", "c")

	// RPOPLPUSH
	val, err := store.RPopLPush(source, dest)
	assert.NoError(t, err)
	assert.Equal(t, "c", val)

	// 验证源列表
	values, _ := store.LRange(source, 0, -1)
	assert.Equal(t, []string{"a", "b"}, values)

	// 验证目标列表
	values, _ = store.LRange(dest, 0, -1)
	assert.Equal(t, []string{"c"}, values)

	// 再次RPOPLPUSH
	val, err = store.RPopLPush(source, dest)
	assert.NoError(t, err)
	assert.Equal(t, "b", val)

	values, _ = store.LRange(dest, 0, -1)
	assert.Equal(t, []string{"b", "c"}, values)
}

// TestListEdgeCases 测试边界情况
func TestListEdgeCases(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	key := "mylist"

	// 空列表操作
	val, _ := store.LPop(key)
	assert.Equal(t, "", val)
	val, _ = store.RPop(key)
	assert.Equal(t, "", val)
	length, _ := store.LLen(key)
	assert.Equal(t, uint64(0), length)

	// 单个元素
	_, _ = store.LPush(key, "single")
	val, _ = store.LPop(key)
	assert.Equal(t, "single", val)
	length, _ = store.LLen(key)
	assert.Equal(t, uint64(0), length)

	// 大量元素
	for i := 0; i < 100; i++ {
		_, _ = store.RPush(key, "item")
	}
	length, _ = store.LLen(key)
	assert.Equal(t, uint64(100), length)
}

func TestLPUSHX(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	key := "test_lpushx"

	// 键不存在，应该返回0
	count, err := store.LPUSHX(key, "value1")
	assert.NoError(t, err)
	assert.Equal(t, 0, count)

	// 创建列表
	store.LPush(key, "existing")

	// 键存在，应该成功
	count, err = store.LPUSHX(key, "value1", "value2")
	assert.NoError(t, err)
	assert.Equal(t, 3, count)

	// 验证值
	val, _ := store.LIndex(key, 0)
	assert.Equal(t, "value2", val)
}

func TestRPUSHX(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	key := "test_rpushx"

	// 键不存在，应该返回0
	count, err := store.RPUSHX(key, "value1")
	assert.NoError(t, err)
	assert.Equal(t, 0, count)

	// 创建列表
	store.RPush(key, "existing")

	// 键存在，应该成功
	count, err = store.RPUSHX(key, "value1", "value2")
	assert.NoError(t, err)
	assert.Equal(t, 3, count)

	// 验证值
	val, _ := store.LIndex(key, 2)
	assert.Equal(t, "value2", val)
}

func TestBLPOP(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	// 测试非空列表
	store.LPush("list1", "value1")
	key, value, err := store.BLPOP([]string{"list1", "list2"}, 0)
	assert.NoError(t, err)
	assert.Equal(t, "list1", key)
	assert.Equal(t, "value1", value)

	// 测试空列表
	key, value, err = store.BLPOP([]string{"empty_list"}, 0)
	assert.NoError(t, err)
	assert.Equal(t, "", key)
	assert.Equal(t, "", value)
}

func TestBRPOP(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	// 测试非空列表
	store.RPush("list1", "value1")
	key, value, err := store.BRPOP([]string{"list1", "list2"}, 0)
	assert.NoError(t, err)
	assert.Equal(t, "list1", key)
	assert.Equal(t, "value1", value)

	// 测试空列表
	key, value, err = store.BRPOP([]string{"empty_list"}, 0)
	assert.NoError(t, err)
	assert.Equal(t, "", key)
	assert.Equal(t, "", value)
}

func TestBRPOPLPUSH(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	// 测试非空列表
	store.RPush("source", "value1")
	value, err := store.BRPOPLPUSH("source", "dest", 0)
	assert.NoError(t, err)
	assert.Equal(t, "value1", value)

	// 验证值已移动
	val, _ := store.LIndex("dest", 0)
	assert.Equal(t, "value1", val)
}

// TestLPos 测试 LPOS 命令
func TestLPos(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	// Create a list with duplicate values
	store.RPush("mylist", "a", "b", "c", "b", "a")

	// Test basic LPOS
	positions, err := store.LPos("mylist", "b", 0, 0, 0)
	assert.NoError(t, err)
	assert.True(t, len(positions) > 0)
	assert.Equal(t, int64(1), positions[0])

	// Test LPOS with RANK
	positions, err = store.LPos("mylist", "b", 2, 1, 0)
	assert.NoError(t, err)
	assert.True(t, len(positions) > 0)
	assert.Equal(t, int64(3), positions[0])

	// Test LPOS for non-existent element - returns empty positions, not error
	positions, err = store.LPos("mylist", "nonexistent", 0, 0, 0)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(positions))

	// Test LPOS for non-existent key - returns empty positions, not error
	positions, err = store.LPos("nonexistent", "value", 0, 0, 0)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(positions))
}

// TestLMove 测试 LMOVE 命令
func TestLMove(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	// Create source list
	store.RPush("source", "a", "b", "c")

	// Test LMOVE LEFT to RIGHT
	value, err := store.LMove("source", "dest", "LEFT", "RIGHT")
	assert.NoError(t, err)
	assert.Equal(t, "a", value)

	// Verify
	val, _ := store.LIndex("dest", 0)
	assert.Equal(t, "a", val)

	// Test LMOVE RIGHT to LEFT
	store.RPush("source2", "x", "y", "z")
	value, err = store.LMove("source2", "dest2", "RIGHT", "LEFT")
	assert.NoError(t, err)
	assert.Equal(t, "z", value)

	// Verify
	val, _ = store.LIndex("dest2", 0)
	assert.Equal(t, "z", val)
}

// TestBLMove 测试 BLMOVE 命令
func TestBLMove(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	// Test BLMOVE with existing element
	store.RPush("source", "value1")
	value, err := store.BLMove("source", "dest", "LEFT", "RIGHT", 1)
	assert.NoError(t, err)
	assert.Equal(t, "value1", value)

	// Verify
	val, _ := store.LIndex("dest", 0)
	assert.Equal(t, "value1", val)
}
