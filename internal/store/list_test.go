package store

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/zeebo/assert"
)

// TestLPush 测试 LPUSH 命令
func TestLPush(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

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
	t.Parallel()
	store := setupTestStore(t)

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
	t.Parallel()
	store := setupTestStore(t)

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
	t.Parallel()
	store := setupTestStore(t)

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
	t.Parallel()
	store := setupTestStore(t)

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
	t.Parallel()
	store := setupTestStore(t)

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
	t.Parallel()
	store := setupTestStore(t)

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
	t.Parallel()
	store := setupTestStore(t)

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
	t.Parallel()
	store := setupTestStore(t)

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
	t.Parallel()
	store := setupTestStore(t)

	key := "mylist"

	// 设置值
	_, _ = store.RPush(key, "a", "b", "c")

	// BEFORE插入
	count, err := store.LInsert(key, "BEFORE", "b", "x")
	assert.NoError(t, err)
	// LINSERT returns new list length after successful insert
	assert.Equal(t, 4, count)

	values, _ := store.LRange(key, 0, -1)
	assert.Equal(t, []string{"a", "x", "b", "c"}, values)

	// AFTER插入
	count, err = store.LInsert(key, "AFTER", "b", "y")
	assert.NoError(t, err)
	// LINSERT returns new list length after successful insert
	assert.Equal(t, 5, count)

	values, _ = store.LRange(key, 0, -1)
	assert.Equal(t, []string{"a", "x", "b", "y", "c"}, values)

	// pivot不存在
	count, err = store.LInsert(key, "BEFORE", "z", "w")
	assert.NoError(t, err)
	// LINSERT returns -1 when pivot is not found
	assert.Equal(t, -1, count)
}

// TestLRem 测试 LREM 命令
func TestLRem(t *testing.T) {
	t.Parallel()
	// Not parallel — avoids BadgerDB contention with other store tests
	store := setupTestStore(t)

	key := "mylist"

	// 设置值
	n, err := store.RPush(key, "a", "b", "a", "c", "a", "d")
	assert.NoError(t, err)
	assert.Equal(t, 6, n)

	// 删除第一个a
	count, err := store.LRem(key, 1, "a")
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	values, err := store.LRange(key, 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"b", "a", "c", "a", "d"}, values)

	// 删除所有a
	count, err = store.LRem(key, 0, "a")
	assert.NoError(t, err)
	assert.Equal(t, 2, count)

	values, err = store.LRange(key, 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"b", "c", "d"}, values)

	// 从尾部删除
	n, err = store.RPush(key, "x", "y", "x")
	assert.NoError(t, err)
	assert.Equal(t, 6, n)
	count, err = store.LRem(key, -1, "x")
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	values, err = store.LRange(key, 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"b", "c", "d", "x", "y"}, values)
}

// TestRPopLPush 测试 RPOPLPUSH 命令
func TestRPopLPush(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

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
	t.Parallel()
	store := setupTestStore(t)

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

	// 验证首尾元素内容
	head, _ := store.LIndex(key, 0)
	assert.Equal(t, "item", head)
	tail, _ := store.LIndex(key, 99)
	assert.Equal(t, "item", tail)
}

func TestLPUSHX(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

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
	t.Parallel()
	store := setupTestStore(t)

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
	t.Parallel()
	store := setupTestStore(t)

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
	t.Parallel()
	store := setupTestStore(t)

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
	t.Parallel()
	store := setupTestStore(t)

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
	t.Parallel()
	store := setupTestStore(t)

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
	t.Parallel()
	store := setupTestStore(t)

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
	t.Parallel()
	store := setupTestStore(t)

	// Test BLMOVE with existing element
	store.RPush("source", "value1")
	value, err := store.BLMove("source", "dest", "LEFT", "RIGHT", 1)
	assert.NoError(t, err)
	assert.Equal(t, "value1", value)

	// Verify
	val, _ := store.LIndex("dest", 0)
	assert.Equal(t, "value1", val)
}

func TestBLPOPBlockingRace(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Stress the TOCTOU race: start BLPOP before data exists, then LPush.
	// With the fix (register-then-recheck), BLPOP should never miss data.
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("race_list_%d", i)
		done := make(chan struct{})
		go func() {
			k, v, err := store.BLPOPBlocking(context.Background(), []string{key}, 2000)
			assert.NoError(t, err)
			assert.Equal(t, key, k)
			assert.Equal(t, "value", v)
			close(done)
		}()

		// Small jitter to encourage the race window
		time.Sleep(time.Duration(i%5) * time.Microsecond)
		_, err := store.LPush(key, "value")
		assert.NoError(t, err)

		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatalf("BLPOPBlocking missed data on iteration %d (race condition)", i)
		}
	}
}

func TestBRPOPBlockingRace(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("race_rlist_%d", i)
		done := make(chan struct{})
		go func() {
			k, v, err := store.BRPOPBlocking(context.Background(), []string{key}, 2000)
			assert.NoError(t, err)
			assert.Equal(t, key, k)
			assert.Equal(t, "value", v)
			close(done)
		}()

		time.Sleep(time.Duration(i%5) * time.Microsecond)
		_, err := store.RPush(key, "value")
		assert.NoError(t, err)

		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatalf("BRPOPBlocking missed data on iteration %d (race condition)", i)
		}
	}
}

func TestBLPOPBlockingMultipleKeysRace(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Multiple keys, only one gets data after registration
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("multi_blpop_%d", i)
		done := make(chan struct{})
		go func() {
			k, v, err := store.BLPOPBlocking(context.Background(), []string{"nobody", key, "nobody2"}, 2000)
			assert.NoError(t, err)
			assert.Equal(t, key, k)
			assert.Equal(t, "data", v)
			close(done)
		}()

		time.Sleep(time.Duration(i%3) * 10 * time.Microsecond)
		_, err := store.LPush(key, "data")
		assert.NoError(t, err)

		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatalf("BLPOPBlocking multi-key missed data on iteration %d", i)
		}
	}
}

func TestBLPOPBlockingConcurrentPushers(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	const numPushers = 10

	// Multiple goroutines push to the same key; BLPOP should get exactly one value
	key := "concurrent_push_key"

	done := make(chan struct{})
	go func() {
		k, v, err := store.BLPOPBlocking(context.Background(), []string{key}, 3000)
		assert.NoError(t, err)
		assert.Equal(t, key, k)
		assert.NotEqual(t, "", v)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)

	var wg sync.WaitGroup
	for i := 0; i < numPushers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, err := store.LPush(key, fmt.Sprintf("val_%d", idx))
			assert.NoError(t, err)
		}(i)
	}
	wg.Wait()

	select {
	case <-done:
	case <-time.After(4 * time.Second):
		t.Fatal("BLPOPBlocking concurrent pushers timed out")
	}
}

func TestBLPOPBlockingAlreadyHasData(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Data exists before BLPOP - should return immediately via the
	// re-check path, not via the channel notification
	_, err := store.LPush("existing", "hello")
	assert.NoError(t, err)

	k, v, err := store.BLPOPBlocking(context.Background(), []string{"existing"}, 2000)
	assert.NoError(t, err)
	assert.Equal(t, "existing", k)
	assert.Equal(t, "hello", v)
}

func TestBLPOPBlockingUnregisterCleanup(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// When timeout triggers, the channel should be properly cleaned up
	// Register internally, then let it timeout
	k, v, err := store.BLPOPBlocking(context.Background(), []string{"ghost"}, 1000)
	assert.NoError(t, err)
	assert.Equal(t, "", k)
	assert.Equal(t, "", v)

	// After timeout, no dangling channels should remain for "ghost"
	store.blockingMu.RLock()
	_, exists := store.blockingPopChans["ghost"]
	store.blockingMu.RUnlock()
	assert.False(t, exists)

	// Push to the key afterwards - BLPOP should work normally
	_, err = store.LPush("ghost", "after_timeout")
	assert.NoError(t, err)

	k, v, err = store.BLPOPBlocking(context.Background(), []string{"ghost"}, 1000)
	assert.NoError(t, err)
	assert.Equal(t, "ghost", k)
	assert.Equal(t, "after_timeout", v)
}

// TestListWrongType tests that LPush/RPush returns ErrWrongType when key exists with different type
func TestListWrongType(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Create a hash key first
	err := store.HSet("myhash", "field1", "value1")
	assert.NoError(t, err)

	// Verify it's a hash
	keyType, err := store.Type("myhash")
	assert.NoError(t, err)
	assert.Equal(t, "hash", keyType)

	// Try to LPush to the hash key - should return ErrWrongType
	_, err = store.LPush("myhash", "value")
	assert.Error(t, err)
	assert.Equal(t, ErrWrongType, err)

	// Try to RPush to the hash key - should return ErrWrongType
	_, err = store.RPush("myhash", "value")
	assert.Error(t, err)
	assert.Equal(t, ErrWrongType, err)

	// Create a string key
	err = store.Set("mystring", "value")
	assert.NoError(t, err)

	// Verify it's a string
	keyType, err = store.Type("mystring")
	assert.NoError(t, err)
	assert.Equal(t, "string", keyType)

	// Try to LPush to the string key - should return ErrWrongType
	_, err = store.LPush("mystring", "value")
	assert.Error(t, err)
	assert.Equal(t, ErrWrongType, err)

	// Try to RPush to the string key - should return ErrWrongType
	_, err = store.RPush("mystring", "value")
	assert.Error(t, err)
	assert.Equal(t, ErrWrongType, err)

	// Create a sorted set key
	err = store.ZAdd("myzset", []ZSetMember{{Member: "member1", Score: 1.0}})
	assert.NoError(t, err)

	// Verify it's a zset
	keyType, err = store.Type("myzset")
	assert.NoError(t, err)
	assert.Equal(t, "zset", keyType)

	// Try to LPush to the zset key - should return ErrWrongType
	_, err = store.LPush("myzset", "value")
	assert.Error(t, err)
	assert.Equal(t, ErrWrongType, err)

	// Try to RPush to the zset key - should return ErrWrongType
	_, err = store.RPush("myzset", "value")
	assert.Error(t, err)
	assert.Equal(t, ErrWrongType, err)

	// Create a set key
	_, err = store.SAdd("myset", "member1")
	assert.NoError(t, err)

	// Verify it's a set
	keyType, err = store.Type("myset")
	assert.NoError(t, err)
	assert.Equal(t, "set", keyType)

	// Try to LPush to the set key - should return ErrWrongType
	_, err = store.LPush("myset", "value")
	assert.Error(t, err)
	assert.Equal(t, ErrWrongType, err)

	// Try to RPush to the set key - should return ErrWrongType
	_, err = store.RPush("myset", "value")
	assert.Error(t, err)
	assert.Equal(t, ErrWrongType, err)
}

func TestRegisterBlockingPop(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	ch := make(chan BlockingResult, 1)
	store.registerBlockingPop("test_key", ch)

	store.blockingMu.RLock()
	chans, exists := store.blockingPopChans["test_key"]
	store.blockingMu.RUnlock()
	assert.True(t, exists)
	assert.Equal(t, 1, len(chans))
	assert.Equal(t, ch, chans[0])
}

func TestRegisterBlockingPop_MultipleChannels(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	ch1 := make(chan BlockingResult, 1)
	ch2 := make(chan BlockingResult, 1)
	store.registerBlockingPop("multi_key", ch1)
	store.registerBlockingPop("multi_key", ch2)

	store.blockingMu.RLock()
	chans, exists := store.blockingPopChans["multi_key"]
	store.blockingMu.RUnlock()
	assert.True(t, exists)
	assert.Equal(t, 2, len(chans))
	assert.Equal(t, ch1, chans[0])
	assert.Equal(t, ch2, chans[1])
}

func TestBRPOPLPUSHBlocking_WithData(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	store.RPush("src", "value1")
	store.RPush("src", "value2")

	value, err := store.BRPOPLPUSHBlocking(context.Background(), "src", "dst", 0)
	assert.NoError(t, err)
	assert.Equal(t, "value2", value)

	dstVal, _ := store.LIndex("dst", 0)
	assert.Equal(t, "value2", dstVal)
}

func TestBRPOPLPUSHBlocking_EmptyList(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// timeout 0 now means "block forever" (redis semantics), so a cancelled
	// context provides the immediate-return path for an empty source.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	value, err := store.BRPOPLPUSHBlocking(ctx, "empty_src", "dst", 0)
	assert.NoError(t, err)
	assert.Equal(t, "", value)
}

func TestBRPOPLPUSHBlocking_ConcurrentPush(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	done := make(chan struct{})
	go func() {
		value, err := store.BRPOPLPUSHBlocking(context.Background(), "concurrent_src", "concurrent_dst", 3000)
		assert.NoError(t, err)
		assert.Equal(t, "pushed_data", value)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	_, err := store.RPush("concurrent_src", "pushed_data")
	assert.NoError(t, err)

	select {
	case <-done:
	case <-time.After(4 * time.Second):
		t.Fatal("BRPOPLPUSHBlocking timed out waiting for concurrent push")
	}
}

func TestBRPOPLPUSHBlocking_ContextCancel(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	value, err := store.BRPOPLPUSHBlocking(ctx, "cancel_src", "cancel_dst", 10000)
	assert.NoError(t, err)
	assert.Equal(t, "", value)
}

func TestBLMoveBlocking_WithData(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	store.RPush("src", "value1")
	value, err := store.BLMoveBlocking(context.Background(), "src", "dst", "LEFT", "RIGHT", 0)
	assert.NoError(t, err)
	assert.Equal(t, "value1", value)

	dstVal, _ := store.LIndex("dst", 0)
	assert.Equal(t, "value1", dstVal)
}

func TestBLMoveBlocking_EmptyList(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	value, err := store.BLMoveBlocking(ctx, "empty_src", "dst", "LEFT", "RIGHT", 0)
	assert.NoError(t, err)
	assert.Equal(t, "", value)
}

func TestBLMoveBlocking_ConcurrentPush(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	done := make(chan struct{})
	go func() {
		value, err := store.BLMoveBlocking(context.Background(), "push_src", "push_dst", "RIGHT", "LEFT", 3000)
		assert.NoError(t, err)
		assert.Equal(t, "new_value", value)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	_, err := store.RPush("push_src", "new_value")
	assert.NoError(t, err)

	select {
	case <-done:
	case <-time.After(4 * time.Second):
		t.Fatal("BLMoveBlocking timed out waiting for concurrent push")
	}
}

func TestBLMoveBlocking_ContextCancel(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	value, err := store.BLMoveBlocking(ctx, "cancel_src", "cancel_dst", "LEFT", "RIGHT", 10)
	assert.NoError(t, err)
	assert.Equal(t, "", value)
}
