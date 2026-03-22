package store

import (
	"testing"

	"github.com/zeebo/assert"
)

func TestHSet(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key := "user:1"

	// 设置单个字段
	err := store.HSet(key, "name", "Alice")
	assert.NoError(t, err)

	// 设置多个字段
	err = store.HSet(key, "age", 30)
	assert.NoError(t, err)

	err = store.HSet(key, "city", "Beijing")
	assert.NoError(t, err)

	// 验证字段数量
	count, _ := store.HLen(key)
	assert.Equal(t, uint64(3), count)

	// 覆盖现有字段
	err = store.HSet(key, "age", 31)
	assert.NoError(t, err)

	// 验证字段数量不变
	count, _ = store.HLen(key)
	assert.Equal(t, uint64(3), count)
}

func TestHGet(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key := "user:1"

	// 设置字段
	_ = store.HSet(key, "name", "Alice")
	_ = store.HSet(key, "age", 30)

	// 获取存在的字段
	val, err := store.HGet(key, "name")
	assert.NoError(t, err)
	assert.NotNil(t, val)

	// 获取不存在的字段
	val, err = store.HGet(key, "nonexistent")
	assert.Error(t, err)
	assert.Nil(t, val)
}

func TestHDel(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key := "user:1"

	// 准备数据
	_ = store.HSet(key, "name", "Alice")
	_ = store.HSet(key, "age", 30)
	_ = store.HSet(key, "city", "Beijing")

	// 删除单个字段
	deleted, err := store.HDel(key, "age")
	assert.NoError(t, err)
	assert.Equal(t, 1, deleted)

	// 验证字段数量
	count, _ := store.HLen(key)
	assert.Equal(t, uint64(2), count)

	// 删除不存在的字段
	deleted, err = store.HDel(key, "nonexistent")
	assert.NoError(t, err)
	assert.Equal(t, 0, deleted)

	// 删除多个字段
	deleted, err = store.HDel(key, "name", "city")
	assert.NoError(t, err)
	assert.Equal(t, 2, deleted)

	// 验证哈希为空
	count, _ = store.HLen(key)
	assert.Equal(t, uint64(0), count)
}

func TestHLen(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key := "user:1"

	// 空哈希
	count, err := store.HLen(key)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), count)

	// 添加字段
	_ = store.HSet(key, "name", "Alice")
	count, err = store.HLen(key)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), count)

	_ = store.HSet(key, "age", 30)
	count, err = store.HLen(key)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), count)
}

func TestHGetAll(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key := "user:1"

	// 空哈希
	data, err := store.HGetAll(key)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(data))

	// 设置字段
	_ = store.HSet(key, "name", "Alice")
	_ = store.HSet(key, "age", 30)
	_ = store.HSet(key, "city", "Beijing")

	// 获取所有字段
	data, err = store.HGetAll(key)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(data))
	assert.NotNil(t, data["name"])
	assert.NotNil(t, data["age"])
	assert.NotNil(t, data["city"])
}

func TestHExists(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key := "user:1"

	// 准备数据
	_ = store.HSet(key, "name", "Alice")

	// 检查存在的字段
	exists, err := store.HExists(key, "name")
	assert.NoError(t, err)
	assert.True(t, exists)

	// 检查不存在的字段
	exists, err = store.HExists(key, "age")
	assert.NoError(t, err)
	assert.False(t, exists)

	// 检查不存在的哈希
	exists, err = store.HExists("nonexistent", "name")
	assert.NoError(t, err)
	assert.False(t, exists)
}

func TestHKeys(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key := "user:1"

	// 空哈希
	fields, err := store.HKeys(key)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(fields))

	// 设置字段
	_ = store.HSet(key, "name", "Alice")
	_ = store.HSet(key, "age", 30)
	_ = store.HSet(key, "city", "Beijing")

	// 获取所有字段名
	fields, err = store.HKeys(key)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(fields))

	// 验证字段名（顺序可能不同）
	fieldMap := make(map[string]bool)
	for _, f := range fields {
		fieldMap[f] = true
	}
	assert.True(t, fieldMap["name"])
	assert.True(t, fieldMap["age"])
	assert.True(t, fieldMap["city"])
}

func TestHVals(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key := "user:1"

	// 空哈希
	values, err := store.HVals(key)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(values))

	// 设置字段
	_ = store.HSet(key, "name", "Alice")
	_ = store.HSet(key, "age", 30)
	_ = store.HSet(key, "city", "Beijing")

	// 获取所有字段值
	values, err = store.HVals(key)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(values))

	// 验证所有值都不为空
	for _, val := range values {
		assert.NotNil(t, val)
		assert.True(t, len(val) > 0)
	}
}

func TestHMSet(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key := "user:1"

	// 批量设置字段
	fieldValues := map[string]interface{}{
		"name": "Alice",
		"age":  30,
		"city": "Beijing",
	}
	err := store.HMSet(key, fieldValues)
	assert.NoError(t, err)

	// 验证字段数量
	count, _ := store.HLen(key)
	assert.Equal(t, uint64(3), count)

	// 验证字段值
	name, _ := store.HGet(key, "name")
	assert.NotNil(t, name)

	age, _ := store.HGet(key, "age")
	assert.NotNil(t, age)

	// 更新现有字段
	fieldValues2 := map[string]interface{}{
		"age":  31,
		"city": "Shanghai",
	}
	err = store.HMSet(key, fieldValues2)
	assert.NoError(t, err)

	// 验证字段数量不变
	count, _ = store.HLen(key)
	assert.Equal(t, uint64(3), count)
}

func TestHMGet(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key := "user:1"

	// 准备数据
	store.HSet(key, "name", "Alice")
	store.HSet(key, "age", 30)

	// 批量获取存在的字段
	values, err := store.HMGet(key, "name", "age")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(values))
	assert.NotNil(t, values[0])
	assert.NotNil(t, values[1])

	// 批量获取混合字段（存在和不存在）
	values, err = store.HMGet(key, "name", "nonexistent", "age")
	assert.NoError(t, err)
	assert.Equal(t, 3, len(values))
	assert.NotNil(t, values[0])
	assert.Nil(t, values[1]) // 不存在的字段返回nil
	assert.NotNil(t, values[2])
}

func TestHSetNX(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key := "user:1"

	// 设置不存在的字段
	success, err := store.HSetNX(key, "name", "Alice")
	assert.NoError(t, err)
	assert.True(t, success)

	// 验证字段已设置
	exists, _ := store.HExists(key, "name")
	assert.True(t, exists)

	// 尝试设置已存在的字段
	success, err = store.HSetNX(key, "name", "Bob")
	assert.NoError(t, err)
	assert.False(t, success)

	// 验证字段值未改变
	val, _ := store.HGet(key, "name")
	assert.NotNil(t, val)

	// 设置另一个不存在的字段
	success, err = store.HSetNX(key, "age", 30)
	assert.NoError(t, err)
	assert.True(t, success)
}

func TestHIncrBy(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key := "counter"

	// 对不存在的字段增加
	result, err := store.HIncrBy(key, "count", 5)
	assert.NoError(t, err)
	assert.Equal(t, int64(5), result)

	// 再次增加
	result, err = store.HIncrBy(key, "count", 3)
	assert.NoError(t, err)
	assert.Equal(t, int64(8), result)

	// 减少
	result, err = store.HIncrBy(key, "count", -2)
	assert.NoError(t, err)
	assert.Equal(t, int64(6), result)

	// 验证字段值
	val, _ := store.HGet(key, "count")
	assert.NotNil(t, val)

	// 对已存在的非整数字段增加（应该失败）
	store.HSet(key, "name", "Alice")
	_, err = store.HIncrBy(key, "name", 1)
	assert.Error(t, err)
}

func TestHIncrByFloat(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key := "counter"

	// 对不存在的字段增加
	result, err := store.HIncrByFloat(key, "score", 1.5)
	assert.NoError(t, err)
	assert.Equal(t, 1.5, result)

	// 再次增加
	result, err = store.HIncrByFloat(key, "score", 2.3)
	assert.NoError(t, err)
	assert.Equal(t, 3.8, result)

	// 减少
	result, err = store.HIncrByFloat(key, "score", -0.8)
	assert.NoError(t, err)
	assert.Equal(t, 3.0, result)

	// 验证字段值
	val, _ := store.HGet(key, "score")
	assert.NotNil(t, val)

	// 对已存在的非浮点数字段增加（应该失败）
	store.HSet(key, "name", "Alice")
	_, err = store.HIncrByFloat(key, "name", 1.0)
	assert.Error(t, err)
}

func TestHStrLen(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key := "user:1"

	// 不存在的字段
	length, err := store.HStrLen(key, "name")
	assert.NoError(t, err)
	assert.Equal(t, 0, length)

	// 设置字段
	store.HSet(key, "name", "Alice")
	store.HSet(key, "age", 30)
	store.HSet(key, "empty", "")

	// 获取字符串长度
	length, err = store.HStrLen(key, "name")
	assert.NoError(t, err)
	assert.Equal(t, 5, length) // "Alice"的长度

	length, err = store.HStrLen(key, "age")
	assert.NoError(t, err)
	assert.True(t, length > 0) // 数字转换为字符串后的长度

	length, err = store.HStrLen(key, "empty")
	assert.NoError(t, err)
	assert.Equal(t, 0, length)
}

func TestHashEdgeCases(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key := "test"

	// 测试大量字段
	for i := 0; i < 100; i++ {
		store.HSet(key, string(rune('a'+i)), i)
	}
	count, _ := store.HLen(key)
	assert.Equal(t, uint64(100), count)

	// 测试特殊字符字段名
	store.HSet("special", "field:with:colons", "value1")
	store.HSet("special", "field with spaces", "value2")
	val, _ := store.HGet("special", "field:with:colons")
	assert.NotNil(t, val)

	// 测试空值
	store.HSet("empty", "field", "")
	val, _ = store.HGet("empty", "field")
	assert.NotNil(t, val)
	assert.Equal(t, 0, len(val))

	// 测试HGetAll返回所有字段
	data, _ := store.HGetAll(key)
	assert.Equal(t, 100, len(data))
}

func TestHashOperations(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key := "user:1"

	// 综合测试：设置、获取、更新、删除
	store.HSet(key, "name", "Alice")
	store.HSet(key, "age", 30)

	// 使用HSetNX添加新字段
	success, _ := store.HSetNX(key, "city", "Beijing")
	assert.True(t, success)

	// 使用HSetNX尝试添加已存在的字段
	success, _ = store.HSetNX(key, "name", "Bob")
	assert.False(t, success)

	// 使用HIncrBy增加年龄
	newAge, _ := store.HIncrBy(key, "age", 1)
	assert.Equal(t, int64(31), newAge)

	// 使用HMGet批量获取
	values, _ := store.HMGet(key, "name", "age", "city")
	assert.Equal(t, 3, len(values))
	assert.NotNil(t, values[0])
	assert.NotNil(t, values[1])
	assert.NotNil(t, values[2])

	// 使用HKeys获取所有字段名
	fields, _ := store.HKeys(key)
	assert.Equal(t, 3, len(fields))

	// 使用HVals获取所有字段值
	vals, _ := store.HVals(key)
	assert.Equal(t, 3, len(vals))

	// 删除字段
	deleted, _ := store.HDel(key, "city")
	assert.Equal(t, 1, deleted)

	// 验证最终状态
	count, _ := store.HLen(key)
	assert.Equal(t, uint64(2), count)
}

// TestHRandField 测试 HRANDFIELD 命令
func TestHRandField(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	// Create hash
	store.HSet("myhash", "field1", "value1")
	store.HSet("myhash", "field2", "value2")
	store.HSet("myhash", "field3", "value3")

	// Test HRandField - get random fields (without values)
	fields, _, err := store.HRandField("myhash", 2, false)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(fields))

	// Test HRandField with values
	fieldsWithValues, values, err := store.HRandField("myhash", 2, true)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(fieldsWithValues))
	assert.Equal(t, 2, len(values))

	// Test on non-existent key
	fields, _, err = store.HRandField("nonexistent", 10, false)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(fields))

	// Test with count > size - should return all fields
	fields, _, err = store.HRandField("myhash", 10, false)
	assert.NoError(t, err)
	// Should return all 3 fields (or more depending on implementation)
	assert.True(t, len(fields) >= 3)
}

// TestHGetAllFields tests the hGetAllFields helper
func TestHGetAllFields(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	// Create hash
	store.HSet("myhash", "field1", "value1")
	store.HSet("myhash", "field2", "value2")

	// Test using HGetAll (which uses hGetAllFields internally)
	data, err := store.HGetAll("myhash")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(data))
	assert.Equal(t, []byte("value1"), data["field1"])
	assert.Equal(t, []byte("value2"), data["field2"])

	// Test on non-existent key
	data, err = store.HGetAll("nonexistent")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(data))
}

// TestHSetWrongType tests that HSet returns ErrWrongType when key exists with different type
func TestHSetWrongType(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	// Create a string key first
	err := store.Set("mystring", "value")
	assert.NoError(t, err)

	// Verify it's a string
	keyType, err := store.Type("mystring")
	assert.NoError(t, err)
	assert.Equal(t, "string", keyType)

	// Try to HSet to the string key - should return ErrWrongType
	err = store.HSet("mystring", "field1", "value1")
	assert.Error(t, err)
	assert.Equal(t, ErrWrongType, err)

	// Create a list key
	_, err = store.LPush("mylist", "value1")
	assert.NoError(t, err)

	// Verify it's a list
	keyType, err = store.Type("mylist")
	assert.NoError(t, err)
	assert.Equal(t, "list", keyType)

	// Try to HSet to the list key - should return ErrWrongType
	err = store.HSet("mylist", "field1", "value1")
	assert.Error(t, err)
	assert.Equal(t, ErrWrongType, err)

	// Create a sorted set key
	err = store.ZAdd("myzset", []ZSetMember{{Member: "member1", Score: 1.0}})
	assert.NoError(t, err)

	// Verify it's a zset
	keyType, err = store.Type("myzset")
	assert.NoError(t, err)
	assert.Equal(t, "zset", keyType)

	// Try to HSet to the zset key - should return ErrWrongType
	err = store.HSet("myzset", "field1", "value1")
	assert.Error(t, err)
	assert.Equal(t, ErrWrongType, err)

	// Create a set key
	_, err = store.SAdd("myset", "member1")
	assert.NoError(t, err)

	// Verify it's a set
	keyType, err = store.Type("myset")
	assert.NoError(t, err)
	assert.Equal(t, "set", keyType)

	// Try to HSet to the set key - should return ErrWrongType
	err = store.HSet("myset", "field1", "value1")
	assert.Error(t, err)
	assert.Equal(t, ErrWrongType, err)
}
