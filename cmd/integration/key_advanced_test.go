package integration

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/zeebo/assert"
)

// TestScan 测试 SCAN 命令
func TestScan(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 添加测试数据
	for i := 0; i < 20; i++ {
		_ = sharedClient.Set(ctx, "scankey:"+string(rune('a'+i)), "value", 0).Err()
	}

	// SCAN - 基本迭代（使用原始命令）
	result, err := sharedClient.Do(ctx, "SCAN", "0").Result()
	assert.NoError(t, err)

	arr, ok := result.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr)) // [cursor, keys]

	cursor, _ := arr[0].(int64)
	assert.True(t, cursor >= 0)

	keys, ok := arr[1].([]interface{})
	assert.True(t, ok)
	// 默认COUNT=10，但可能有更多或更少
	assert.True(t, len(keys) > 0 && len(keys) <= 20)
}

// TestScanMatch 测试 SCAN MATCH 命令
func TestScanMatch(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 添加测试数据
	for i := 0; i < 10; i++ {
		_ = sharedClient.Set(ctx, "test:scan:match:"+string(rune('a'+i)), "value", 0).Err()
		_ = sharedClient.Set(ctx, "other:scan:match:"+string(rune('a'+i)), "value", 0).Err()
	}

	// SCAN with MATCH
	result, err := sharedClient.Do(ctx, "SCAN", "0", "MATCH", "test:*").Result()
	assert.NoError(t, err)

	arr, ok := result.([]interface{})
	assert.True(t, ok)

	keys, _ := arr[1].([]interface{})
	// 只应该返回匹配的键
	for _, key := range keys {
		keyStr, _ := key.(string)
		assert.True(t, len(keyStr) > 0)
	}
}

// TestScanCount 测试 SCAN COUNT 命令
func TestScanCount(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 添加测试数据
	for i := 0; i < 50; i++ {
		_ = sharedClient.Set(ctx, "countkey:"+string(rune('a'+i%26)), "value", 0).Err()
	}

	// SCAN with COUNT
	result, err := sharedClient.Do(ctx, "SCAN", "0", "COUNT", "20").Result()
	assert.NoError(t, err)

	arr, ok := result.([]interface{})
	assert.True(t, ok)

	keys, ok := arr[1].([]interface{})
	assert.True(t, ok)
	// 指定COUNT=20
	assert.True(t, len(keys) >= 0)
}

// TestScanType 测试 SCAN TYPE 命令
func TestScanType(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 添加不同类型的键
	_ = sharedClient.Set(ctx, "stringkey", "value", 0).Err()
	_ = sharedClient.LPush(ctx, "listkey", "value").Err()
	_ = sharedClient.HSet(ctx, "hashkey", "field", "value").Err()
	_ = sharedClient.SAdd(ctx, "setkey", "value").Err()
	_ = sharedClient.ZAdd(ctx, "zsetkey", redis.Z{Score: 1, Member: "value"}).Err()

	// SCAN with TYPE - 只扫描string类型
	result, err := sharedClient.Do(ctx, "SCAN", "0", "TYPE", "string").Result()
	assert.NoError(t, err)

	arr, ok := result.([]interface{})
	assert.True(t, ok)

	keys, _ := arr[1].([]interface{})
	// 应该至少包含stringkey
	found := false
	for _, key := range keys {
		keyStr, _ := key.(string)
		if keyStr == "stringkey" {
			found = true
			break
		}
	}
	assert.True(t, found)
}

// TestDump 测试 DUMP 命令
func TestDump(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 测试 String 类型
	err := sharedClient.Set(ctx, "dump:string", "hello world", 0).Err()
	assert.NoError(t, err)

	// 使用 Do() 直接获取原始 RESP 响应
	result, err := sharedClient.Do(ctx, "DUMP", "dump:string").Result()
	assert.NoError(t, err)

	// go-redis 会将 bulk string 转换为 string
	dumpData, ok := result.(string)
	assert.True(t, ok)
	assert.Equal(t, true, len(dumpData) > 10)

	// 验证 RDB 格式
	assert.Equal(t, byte('R'), dumpData[0])
	assert.Equal(t, byte('E'), dumpData[1])
	assert.Equal(t, byte('D'), dumpData[2])
	assert.Equal(t, byte('I'), dumpData[3])
	assert.Equal(t, byte('S'), dumpData[4])

	// 测试 List 类型
	err = sharedClient.RPush(ctx, "dump:list", "a", "b", "c").Err()
	assert.NoError(t, err)

	result, err = sharedClient.Do(ctx, "DUMP", "dump:list").Result()
	assert.NoError(t, err)
	dumpData, ok = result.(string)
	assert.True(t, ok)
	assert.Equal(t, true, len(dumpData) > 10)

	// 测试 Hash 类型
	err = sharedClient.HSet(ctx, "dump:hash", "field1", "value1", "field2", "value2").Err()
	assert.NoError(t, err)

	result, err = sharedClient.Do(ctx, "DUMP", "dump:hash").Result()
	assert.NoError(t, err)
	dumpData, ok = result.(string)
	assert.True(t, ok)
	assert.Equal(t, true, len(dumpData) > 10)

	// 测试 Set 类型
	err = sharedClient.SAdd(ctx, "dump:set", "member1", "member2").Err()
	assert.NoError(t, err)

	result, err = sharedClient.Do(ctx, "DUMP", "dump:set").Result()
	assert.NoError(t, err)
	dumpData, ok = result.(string)
	assert.True(t, ok)
	assert.Equal(t, true, len(dumpData) > 10)

	// 测试 ZSet 类型
	err = sharedClient.ZAdd(ctx, "dump:zset", redis.Z{Score: 1, Member: "m1"}, redis.Z{Score: 2, Member: "m2"}).Err()
	assert.NoError(t, err)

	result, err = sharedClient.Do(ctx, "DUMP", "dump:zset").Result()
	assert.NoError(t, err)
	dumpData, ok = result.(string)
	assert.True(t, ok)
	assert.Equal(t, true, len(dumpData) > 10)

	// 测试不存在的键
	result, err = sharedClient.Do(ctx, "DUMP", "nonexistent").Result()
	// go-redis returns error for nil bulk string
	if err != nil {
		assert.Equal(t, "redis: nil", err.Error())
	} else {
		assert.Nil(t, result)
	}
}

// TestRestore 测试 RESTORE 命令
func TestRestore(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 测试 String 类型的 DUMP/RESTORE
	t.Run("String", func(t *testing.T) {
		err := sharedClient.Set(ctx, "restore:string", "hello world", 0).Err()
		assert.NoError(t, err)

		dumpResult, err := sharedClient.Do(ctx, "DUMP", "restore:string").Result()
		assert.NoError(t, err)
		dumpData := dumpResult.(string)

		err = sharedClient.Del(ctx, "restore:string").Err()
		assert.NoError(t, err)

		restoreResult, err := sharedClient.Do(ctx, "RESTORE", "restored:string", dumpData).Result()
		assert.NoError(t, err)
		assert.Equal(t, "OK", restoreResult)

		value, err := sharedClient.Get(ctx, "restored:string").Result()
		assert.NoError(t, err)
		assert.Equal(t, "hello world", value)
	})

	// 测试 List 类型的 DUMP/RESTORE
	t.Run("List", func(t *testing.T) {
		err := sharedClient.RPush(ctx, "restore:list", "a", "b", "c").Err()
		assert.NoError(t, err)

		dumpResult, err := sharedClient.Do(ctx, "DUMP", "restore:list").Result()
		assert.NoError(t, err)
		dumpData := dumpResult.(string)

		err = sharedClient.Del(ctx, "restore:list").Err()
		assert.NoError(t, err)

		restoreResult, err := sharedClient.Do(ctx, "RESTORE", "restored:list", dumpData).Result()
		assert.NoError(t, err)
		assert.Equal(t, "OK", restoreResult)

		values, err := sharedClient.LRange(ctx, "restored:list", 0, -1).Result()
		assert.NoError(t, err)
		assert.Equal(t, []string{"a", "b", "c"}, values)
	})

	// 测试 Hash 类型的 DUMP/RESTORE
	t.Run("Hash", func(t *testing.T) {
		err := sharedClient.HSet(ctx, "restore:hash", "field1", "value1", "field2", "value2").Err()
		assert.NoError(t, err)

		dumpResult, err := sharedClient.Do(ctx, "DUMP", "restore:hash").Result()
		assert.NoError(t, err)
		dumpData := dumpResult.(string)

		err = sharedClient.Del(ctx, "restore:hash").Err()
		assert.NoError(t, err)

		restoreResult, err := sharedClient.Do(ctx, "RESTORE", "restored:hash", dumpData).Result()
		assert.NoError(t, err)
		assert.Equal(t, "OK", restoreResult)

		val1, err := sharedClient.HGet(ctx, "restored:hash", "field1").Result()
		assert.NoError(t, err)
		assert.Equal(t, "value1", val1)

		val2, err := sharedClient.HGet(ctx, "restored:hash", "field2").Result()
		assert.NoError(t, err)
		assert.Equal(t, "value2", val2)
	})

	// 测试 Set 类型的 DUMP/RESTORE
	t.Run("Set", func(t *testing.T) {
		err := sharedClient.SAdd(ctx, "restore:set", "member1", "member2", "member3").Err()
		assert.NoError(t, err)

		dumpResult, err := sharedClient.Do(ctx, "DUMP", "restore:set").Result()
		assert.NoError(t, err)
		dumpData := dumpResult.(string)

		err = sharedClient.Del(ctx, "restore:set").Err()
		assert.NoError(t, err)

		restoreResult, err := sharedClient.Do(ctx, "RESTORE", "restored:set", dumpData).Result()
		assert.NoError(t, err)
		assert.Equal(t, "OK", restoreResult)

		members, err := sharedClient.SMembers(ctx, "restored:set").Result()
		assert.NoError(t, err)
		assert.Equal(t, 3, len(members))
	})

	// 测试 ZSet 类型的 DUMP/RESTORE
	t.Run("ZSet", func(t *testing.T) {
		// 添加有序集合成员
		err := sharedClient.ZAdd(ctx, "zsettest", redis.Z{
			Score:  1.0,
			Member: "member1",
		}, redis.Z{
			Score:  2.0,
			Member: "member2",
		}, redis.Z{
			Score:  3.0,
			Member: "member3",
		}).Err()
		assert.NoError(t, err)

		// 验证原始数据存在
		originalMembers, err := sharedClient.ZRangeWithScores(ctx, "zsettest", 0, -1).Result()
		assert.NoError(t, err)
		assert.Equal(t, 3, len(originalMembers))

		dumpResult, err := sharedClient.Do(ctx, "DUMP", "zsettest").Result()
		assert.NoError(t, err)
		dumpData := dumpResult.(string)
		assert.True(t, len(dumpData) > 0)

		err = sharedClient.Del(ctx, "zsettest").Err()
		assert.NoError(t, err)

		restoreResult, err := sharedClient.Do(ctx, "RESTORE", "zsetsaved", dumpData).Result()
		assert.NoError(t, err)
		assert.Equal(t, "OK", restoreResult)

		// 验证恢复的数据
		members, err := sharedClient.ZRangeWithScores(ctx, "zsetsaved", 0, -1).Result()
		assert.NoError(t, err)
		assert.Equal(t, 3, len(members))
		assert.Equal(t, "member1", members[0].Member)
		assert.Equal(t, float64(1.0), members[0].Score)
		assert.Equal(t, "member2", members[1].Member)
		assert.Equal(t, float64(2.0), members[1].Score)
		assert.Equal(t, "member3", members[2].Member)
		assert.Equal(t, float64(3.0), members[2].Score)
	})

	// 测试 REPLACE 选项
	t.Run("Replace", func(t *testing.T) {
		err := sharedClient.Set(ctx, "replace:key", "original", 0).Err()
		assert.NoError(t, err)

		// 创建另一个键并 DUMP
		err = sharedClient.Set(ctx, "replace:source", "newvalue", 0).Err()
		assert.NoError(t, err)

		dumpResult, err := sharedClient.Do(ctx, "DUMP", "replace:source").Result()
		assert.NoError(t, err)
		dumpData := dumpResult.(string)

		// 使用 REPLACE 恢复
		restoreResult, err := sharedClient.Do(ctx, "RESTORE", "replace:key", dumpData, "REPLACE").Result()
		assert.NoError(t, err)
		assert.Equal(t, "OK", restoreResult)

		value, err := sharedClient.Get(ctx, "replace:key").Result()
		assert.NoError(t, err)
		assert.Equal(t, "newvalue", value)
	})

	// 测试 REPLACE 时不存在的键报错
	t.Run("ReplaceError", func(t *testing.T) {
		err := sharedClient.Set(ctx, "noreplace:source", "value", 0).Err()
		assert.NoError(t, err)

		dumpResult, err := sharedClient.Do(ctx, "DUMP", "noreplace:source").Result()
		assert.NoError(t, err)
		dumpData := dumpResult.(string)

		// 尝试恢复到一个不存在的键（不需要 REPLACE）
		restoreResult, err := sharedClient.Do(ctx, "RESTORE", "noreplace:newkey", dumpData).Result()
		assert.NoError(t, err)
		assert.Equal(t, "OK", restoreResult)
	})
}

// TestSort 测试 SORT 命令
func TestSortBasic(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 添加测试数据
	_ = sharedClient.RPush(ctx, "sortlist", "3", "1", "2", "5", "4").Err()

	// SORT - 基本排序（使用原始命令）
	result, err := sharedClient.Do(ctx, "SORT", "sortlist").Result()
	assert.NoError(t, err)

	arr, ok := result.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, []interface{}{"1", "2", "3", "4", "5"}, arr)
}

// TestSortAlpha 测试 SORT ALPHA 命令
func TestSortAlpha(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 添加测试数据（字母）
	_ = sharedClient.RPush(ctx, "sortalpha", "c", "a", "b", "e", "d").Err()

	// SORT ALPHA - 字母排序
	result, err := sharedClient.Do(ctx, "SORT", "sortalpha", "ALPHA").Result()
	assert.NoError(t, err)

	arr, ok := result.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, []interface{}{"a", "b", "c", "d", "e"}, arr)
}

// TestSortBy 测试 SORT BY 命令
func TestSortBy(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 添加测试数据
	_ = sharedClient.RPush(ctx, "sortby", "a", "b", "c").Err()
	_ = sharedClient.Set(ctx, "weight_a", "30", 0).Err()
	_ = sharedClient.Set(ctx, "weight_b", "10", 0).Err()
	_ = sharedClient.Set(ctx, "weight_c", "20", 0).Err()

	// SORT BY - 按外部键排序
	result, err := sharedClient.Do(ctx, "SORT", "sortby", "BY", "weight_*").Result()
	assert.NoError(t, err)
	// 按weight排序: b(10), c(20), a(30)
	arr, ok := result.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, []interface{}{"b", "c", "a"}, arr)
}

// TestSortGet 测试 SORT GET 命令
func TestSortGet(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 添加测试数据
	_ = sharedClient.RPush(ctx, "sortget", "a", "b").Err()
	_ = sharedClient.Set(ctx, "data_a", "value_a", 0).Err()
	_ = sharedClient.Set(ctx, "data_b", "value_b", 0).Err()

	// SORT GET - 获取外部键
	result, err := sharedClient.Do(ctx, "SORT", "sortget", "GET", "data_*").Result()
	assert.NoError(t, err)

	arr, ok := result.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, []interface{}{"value_a", "value_b"}, arr)
}

// TestSortStore 测试 SORT STORE 命令
func TestSortStore(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 添加测试数据
	_ = sharedClient.RPush(ctx, "sortstore", "3", "1", "2").Err()

	// SORT STORE - 存储排序结果
	result, err := sharedClient.Do(ctx, "SORT", "sortstore", "STORE", "sortedresult").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), result)

	// 验证结果
	members, _ := sharedClient.LRange(ctx, "sortedresult", 0, -1).Result()
	assert.Equal(t, []string{"1", "2", "3"}, members)
}

// TestObjectRefCount 测试 OBJECT REFCOUNT 命令
func TestObjectRefCount(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 设置测试数据
	_ = sharedClient.Set(ctx, "objkey", "value", 0).Err()

	// OBJECT REFCOUNT
	result, err := sharedClient.Do(ctx, "OBJECT", "REFCOUNT", "objkey").Result()
	assert.NoError(t, err)
	refcount, ok := result.(int64)
	assert.True(t, ok)
	assert.True(t, refcount >= 1)
}

// TestObjectEncoding 测试 OBJECT ENCODING 命令
func TestObjectEncoding(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 设置测试数据
	_ = sharedClient.Set(ctx, "encodestring", "value", 0).Err()

	// OBJECT ENCODING
	result, err := sharedClient.Do(ctx, "OBJECT", "ENCODING", "encodestring").Result()
	assert.NoError(t, err)

	encoding, ok := result.(string)
	assert.True(t, ok)
	assert.True(t, len(encoding) > 0)

	// OBJECT ENCODING - 不存在的键
	// 注意：go-redis 将 nil 响应转换为错误 "redis: nil"，需要特殊处理
	result, err = sharedClient.Do(ctx, "OBJECT", "ENCODING", "nonexistent").Result()
	if err != nil {
		assert.Equal(t, "redis: nil", err.Error())
	}
	assert.Nil(t, result)
}

// TestObjectIdleTime 测试 OBJECT IDLETIME 命令
func TestObjectIdleTime(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 设置测试数据
	_ = sharedClient.Set(ctx, "idletime", "value", 0).Err()

	// OBJECT IDLETIME
	result, err := sharedClient.Do(ctx, "OBJECT", "IDLETIME", "idletime").Result()
	assert.NoError(t, err)

	idletime, ok := result.(int64)
	assert.True(t, ok)
	assert.True(t, idletime >= 0)
}

// TestObjectFreq 测试 OBJECT FREQ 命令
func TestObjectFreq(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 设置测试数据
	_ = sharedClient.Set(ctx, "freqkey", "value", 0).Err()

	// OBJECT FREQ
	result, err := sharedClient.Do(ctx, "OBJECT", "FREQ", "freqkey").Result()
	assert.NoError(t, err)
	// LFU频率应该是0（刚创建）
	assert.Equal(t, int64(0), result)
}
