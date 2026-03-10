package store

import (
	"fmt"
	"testing"

	"github.com/zeebo/assert"
)

func TestSAdd(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key := "myset"

	// 添加单个成员
	added, err := store.SAdd(key, "apple")
	assert.NoError(t, err)
	assert.Equal(t, 1, added)

	// 添加多个成员
	added, err = store.SAdd(key, "banana", "cherry")
	assert.NoError(t, err)
	assert.Equal(t, 2, added)

	// 添加重复成员（不应增加计数）
	added, err = store.SAdd(key, "apple")
	assert.NoError(t, err)
	assert.Equal(t, 0, added)

	// 验证集合大小
	count, _ := store.SCard(key)
	assert.Equal(t, uint64(3), count)
}

func TestSRem(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key := "myset"

	// 准备数据
	store.SAdd(key, "apple", "banana", "cherry")

	// 删除单个成员
	removed, err := store.SRem(key, "banana")
	assert.NoError(t, err)
	assert.Equal(t, 1, removed)

	// 删除不存在的成员
	removed, err = store.SRem(key, "nonexistent")
	assert.NoError(t, err)
	assert.Equal(t, 0, removed)

	// 删除多个成员
	removed, err = store.SRem(key, "apple", "cherry")
	assert.NoError(t, err)
	assert.Equal(t, 2, removed)

	// 验证集合为空
	count, _ := store.SCard(key)
	assert.Equal(t, uint64(0), count)
}

func TestSCard(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key := "myset"

	// 空集合
	count, err := store.SCard(key)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), count)

	// 添加成员后
	store.SAdd(key, "apple", "banana", "cherry")
	count, err = store.SCard(key)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), count)
}

func TestSIsMember(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key := "myset"

	// 准备数据
	_, _ = store.SAdd(key, "apple", "banana")

	// 检查存在的成员
	exists, err := store.SIsMember(key, "apple")
	assert.NoError(t, err)
	assert.True(t, exists)

	// 检查不存在的成员
	exists, err = store.SIsMember(key, "cherry")
	assert.NoError(t, err)
	assert.False(t, exists)

	// 检查空集合
	exists, err = store.SIsMember("nonexistent", "apple")
	assert.NoError(t, err)
	assert.False(t, exists)
}

func TestSMembers(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key := "myset"

	// 空集合
	members, err := store.SMembers(key)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(members))

	// 添加成员
	store.SAdd(key, "apple", "banana", "cherry")
	members, err = store.SMembers(key)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(members))

	// 验证成员（顺序可能不同）
	memberMap := make(map[string]bool)
	for _, m := range members {
		memberMap[m] = true
	}
	assert.True(t, memberMap["apple"])
	assert.True(t, memberMap["banana"])
	assert.True(t, memberMap["cherry"])
}

func TestSPop(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key := "myset"

	// 空集合
	member, err := store.SPop(key)
	assert.NoError(t, err)
	assert.Equal(t, "", member)

	// 添加成员
	store.SAdd(key, "apple", "banana", "cherry")

	// 弹出成员
	member, err = store.SPop(key)
	assert.NoError(t, err)
	assert.NotEqual(t, "", member)

	// 验证成员被删除
	exists, _ := store.SIsMember(key, member)
	assert.False(t, exists)

	// 验证集合大小减少
	count, _ := store.SCard(key)
	assert.Equal(t, uint64(2), count)
}

func TestSPopN(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key := "myset"

	// 添加成员
	store.SAdd(key, "apple", "banana", "cherry", "date")

	// 弹出2个成员
	members, err := store.SPopN(key, 2)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(members))

	// 验证成员被删除
	for _, m := range members {
		exists, _ := store.SIsMember(key, m)
		assert.False(t, exists)
	}

	// 验证集合大小
	count, _ := store.SCard(key)
	assert.Equal(t, uint64(2), count)

	// 弹出超过集合大小的数量
	members, err = store.SPopN(key, 10)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(members))

	// 验证集合为空
	count, _ = store.SCard(key)
	assert.Equal(t, uint64(0), count)
}

func TestSRandMember(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key := "myset"

	// 空集合
	member, err := store.SRandMember(key)
	assert.NoError(t, err)
	assert.Equal(t, "", member)

	// 添加成员
	store.SAdd(key, "apple", "banana", "cherry")

	// 随机获取成员（不删除）
	member, err = store.SRandMember(key)
	assert.NoError(t, err)
	assert.NotEqual(t, "", member)

	// 验证成员仍在集合中
	exists, _ := store.SIsMember(key, member)
	assert.True(t, exists)

	// 验证集合大小不变
	count, _ := store.SCard(key)
	assert.Equal(t, uint64(3), count)
}

func TestSRandMemberN(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key := "myset"

	// 添加成员
	store.SAdd(key, "apple", "banana", "cherry")

	// 获取2个随机成员（不重复）
	members, err := store.SRandMemberN(key, 2)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(members))

	// 验证成员仍在集合中
	for _, m := range members {
		exists, _ := store.SIsMember(key, m)
		assert.True(t, exists)
	}

	// 获取超过集合大小的数量
	members, err = store.SRandMemberN(key, 10)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(members)) // 应该返回所有成员

	// 负数count（允许重复）
	members, err = store.SRandMemberN(key, -5)
	assert.NoError(t, err)
	assert.Equal(t, 5, len(members))
}

func TestSMove(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	source := "source"
	dest := "dest"

	// 准备数据
	store.SAdd(source, "apple", "banana")

	// 移动成员
	moved, err := store.SMove(source, dest, "apple")
	assert.NoError(t, err)
	assert.True(t, moved)

	// 验证成员从源集合删除
	exists, _ := store.SIsMember(source, "apple")
	assert.False(t, exists)

	// 验证成员添加到目标集合
	exists, _ = store.SIsMember(dest, "apple")
	assert.True(t, exists)

	// 移动不存在的成员
	moved, err = store.SMove(source, dest, "nonexistent")
	assert.NoError(t, err)
	assert.False(t, moved)

	// 移动已存在的成员（应该成功，但不重复添加）
	store.SAdd(source, "cherry")
	moved, err = store.SMove(source, dest, "cherry")
	assert.NoError(t, err)
	assert.True(t, moved)

	// 再次移动同一个成员（源集合已没有）
	moved, err = store.SMove(source, dest, "cherry")
	assert.NoError(t, err)
	assert.False(t, moved)
}

func TestSInter(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key1 := "set1"
	key2 := "set2"
	key3 := "set3"

	// 准备数据
	store.SAdd(key1, "apple", "banana", "cherry")
	store.SAdd(key2, "banana", "cherry", "date")
	store.SAdd(key3, "cherry", "date", "elderberry")

	// 计算两个集合的交集
	result, err := store.SInter(key1, key2)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(result))

	// 验证结果
	resultMap := make(map[string]bool)
	for _, m := range result {
		resultMap[m] = true
	}
	assert.True(t, resultMap["banana"])
	assert.True(t, resultMap["cherry"])

	// 计算三个集合的交集
	result, err = store.SInter(key1, key2, key3)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, "cherry", result[0])

	// 空交集
	store.SAdd("empty", "x")
	result, err = store.SInter(key1, "empty")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(result))

	// 空参数
	result, err = store.SInter()
	assert.NoError(t, err)
	assert.Equal(t, 0, len(result))
}

func TestSUnion(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key1 := "set1"
	key2 := "set2"
	key3 := "set3"

	// 准备数据
	store.SAdd(key1, "apple", "banana")
	store.SAdd(key2, "banana", "cherry")
	store.SAdd(key3, "date")

	// 计算两个集合的并集
	result, err := store.SUnion(key1, key2)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(result))

	// 验证结果（去重）
	resultMap := make(map[string]bool)
	for _, m := range result {
		resultMap[m] = true
	}
	assert.True(t, resultMap["apple"])
	assert.True(t, resultMap["banana"])
	assert.True(t, resultMap["cherry"])

	// 计算三个集合的并集
	result, err = store.SUnion(key1, key2, key3)
	assert.NoError(t, err)
	assert.Equal(t, 4, len(result))

	// 空参数
	result, err = store.SUnion()
	assert.NoError(t, err)
	assert.Equal(t, 0, len(result))
}

func TestSDiff(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key1 := "set1"
	key2 := "set2"
	key3 := "set3"

	// 准备数据
	store.SAdd(key1, "apple", "banana", "cherry")
	store.SAdd(key2, "banana", "cherry")
	store.SAdd(key3, "cherry")

	// 计算差集
	result, err := store.SDiff(key1, key2)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, "apple", result[0])

	// 计算多个集合的差集
	result, err = store.SDiff(key1, key2, key3)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, "apple", result[0])

	// 空差集
	result, err = store.SDiff(key2, key1)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(result))

	// 空参数
	result, err = store.SDiff()
	assert.NoError(t, err)
	assert.Equal(t, 0, len(result))
}

func TestSInterStore(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key1 := "set1"
	key2 := "set2"
	dest := "dest"

	// 准备数据
	store.SAdd(key1, "apple", "banana", "cherry")
	store.SAdd(key2, "banana", "cherry", "date")

	// 计算交集并存储
	count, err := store.SInterStore(dest, key1, key2)
	assert.NoError(t, err)
	assert.Equal(t, 2, count)

	// 验证目标集合
	members, _ := store.SMembers(dest)
	assert.Equal(t, 2, len(members))
	memberMap := make(map[string]bool)
	for _, m := range members {
		memberMap[m] = true
	}
	assert.True(t, memberMap["banana"])
	assert.True(t, memberMap["cherry"])

	// 覆盖现有集合
	store.SAdd(dest, "old")
	count, err = store.SInterStore(dest, key1, key2)
	assert.NoError(t, err)
	assert.Equal(t, 2, count)

	// 验证旧成员被删除
	exists, _ := store.SIsMember(dest, "old")
	assert.False(t, exists)
}

func TestSUnionStore(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key1 := "set1"
	key2 := "set2"
	dest := "dest"

	// 准备数据
	store.SAdd(key1, "apple", "banana")
	store.SAdd(key2, "banana", "cherry")

	// 计算并集并存储
	count, err := store.SUnionStore(dest, key1, key2)
	assert.NoError(t, err)
	assert.Equal(t, 3, count)

	// 验证目标集合
	members, _ := store.SMembers(dest)
	assert.Equal(t, 3, len(members))
	memberMap := make(map[string]bool)
	for _, m := range members {
		memberMap[m] = true
	}
	assert.True(t, memberMap["apple"])
	assert.True(t, memberMap["banana"])
	assert.True(t, memberMap["cherry"])

	// 覆盖现有集合
	store.SAdd(dest, "old")
	count, err = store.SUnionStore(dest, key1, key2)
	assert.NoError(t, err)
	assert.Equal(t, 3, count)

	// 验证旧成员被删除
	exists, _ := store.SIsMember(dest, "old")
	assert.False(t, exists)
}

func TestSDiffStore(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key1 := "set1"
	key2 := "set2"
	dest := "dest"

	// 准备数据
	store.SAdd(key1, "apple", "banana", "cherry")
	store.SAdd(key2, "banana", "cherry")

	// 计算差集并存储
	count, err := store.SDiffStore(dest, key1, key2)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	// 验证目标集合
	members, _ := store.SMembers(dest)
	assert.Equal(t, 1, len(members))
	assert.Equal(t, "apple", members[0])

	// 覆盖现有集合
	store.SAdd(dest, "old")
	count, err = store.SDiffStore(dest, key1, key2)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	// 验证旧成员被删除
	exists, _ := store.SIsMember(dest, "old")
	assert.False(t, exists)
}

func TestSetEdgeCases(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	// 测试空集合操作
	count, _ := store.SCard("empty")
	assert.Equal(t, uint64(0), count)

	members, _ := store.SMembers("empty")
	assert.Equal(t, 0, len(members))

	// 测试单个成员集合
	store.SAdd("single", "one")
	count, _ = store.SCard("single")
	assert.Equal(t, uint64(1), count)

	// 测试大量成员
	key := "large"
	for i := 0; i < 100; i++ {
		store.SAdd(key, string(rune('a'+i)))
	}
	count, _ = store.SCard(key)
	assert.Equal(t, uint64(100), count)

	// 测试特殊字符
	store.SAdd("special", "a:b", "c,d", "e f")
	members, _ = store.SMembers("special")
	assert.Equal(t, 3, len(members))
}

// TestSMIsMember 测试 SMISMEMBER 命令
func TestSMIsMember(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	// Create a set
	store.SAdd("myset", "a", "b", "c")

	// Test SMIsMember with existing members
	results, err := store.SMIsMember("myset", "a", "b", "d")
	assert.NoError(t, err)
	assert.Equal(t, 3, len(results))
	assert.Equal(t, int64(1), results[0]) // "a" exists
	assert.Equal(t, int64(1), results[1]) // "b" exists
	assert.Equal(t, int64(0), results[2]) // "d" does not exist

	// Test with non-existent key
	results, err = store.SMIsMember("nonexistent", "a")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), results[0])
}

// TestSInterCard 测试 SINTERCARD 命令
func TestSInterCard(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	// Create sets
	store.SAdd("set1", "a", "b", "c")
	store.SAdd("set2", "b", "c", "d")

	// Test SInterCard
	count, err := store.SInterCard("set1", "set2")
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), count) // b, c

	// Test with no overlap
	store.SAdd("set3", "x", "y", "z")
	count, err = store.SInterCard("set1", "set3")
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), count)

	// Test with non-existent key
	count, err = store.SInterCard("nonexistent", "set1")
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), count)
}

// TestSScan 测试 SSCAN 命令
func TestSScan(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	// Create a set with many members
	for i := 0; i < 100; i++ {
		store.SAdd("myset", fmt.Sprintf("member%d", i))
	}

	// Test SScan - should return some members
	result, err := store.SScan("myset", 0, "*member5*", 10)
	assert.NoError(t, err)
	// Result contains cursor and members
	_ = result

	// Test with non-existent key
	result, err = store.SScan("nonexistent", 0, "*", 10)
	assert.NoError(t, err)
	_ = result
}
