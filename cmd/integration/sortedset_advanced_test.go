package integration

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/zeebo/assert"
)

// TestZRangeByScore 测试 ZRANGEBYSCORE 命令
func TestZRangeByScore(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 准备测试数据
	assert.NoError(t, sharedClient.ZAdd(ctx, "zrangescore", redis.Z{Score: 10, Member: "a"}, redis.Z{Score: 20, Member: "b"}, redis.Z{Score: 30, Member: "c"}).Err())

	// ZRANGEBYSCORE - 按分数范围获取
	members, err := sharedClient.ZRangeByScore(ctx, "zrangescore", &redis.ZRangeBy{
		Min: "10",
		Max: "25",
	}).Result()
	assert.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, members)

	// ZRANGEBYSCORE - 不包含边界
	members, err = sharedClient.ZRangeByScore(ctx, "zrangescore", &redis.ZRangeBy{
		Min: "(10",
		Max: "(25",
	}).Result()
	assert.NoError(t, err)
	assert.Equal(t, []string{"b"}, members) // exclusive (10, 25) returns only b (score 20)

	// ZRANGEBYSCORE - 负无穷到某值
	members, err = sharedClient.ZRangeByScore(ctx, "zrangescore", &redis.ZRangeBy{
		Min: "-inf",
		Max: "15",
	}).Result()
	assert.NoError(t, err)
	assert.Equal(t, []string{"a"}, members)
}

// TestZRevRangeByScore 测试 ZREVRANGEBYSCORE 命令
func TestZRevRangeByScore(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 准备测试数据
	assert.NoError(t, sharedClient.ZAdd(ctx, "zrevscore", redis.Z{Score: 10, Member: "a"}, redis.Z{Score: 20, Member: "b"}, redis.Z{Score: 30, Member: "c"}).Err())

	// ZREVRANGEBYSCORE - 逆序按分数范围获取
	members, err := sharedClient.ZRevRangeByScore(ctx, "zrevscore", &redis.ZRangeBy{
		Min: "10",
		Max: "30",
	}).Result()
	assert.NoError(t, err)
	assert.Equal(t, []string{"c", "b", "a"}, members)

	// ZREVRANGEBYSCORE - 限制数量
	members, err = sharedClient.ZRevRangeByScore(ctx, "zrevscore", &redis.ZRangeBy{
		Min:    "10",
		Max:    "30",
		Offset: 0,
		Count:  2,
	}).Result()
	assert.NoError(t, err)
	assert.Equal(t, []string{"c", "b"}, members)
}

// TestZRemRangeByRank 测试 ZREMRANGEBYRANK 命令
func TestZRemRangeByRank(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 准备测试数据
	assert.NoError(t, sharedClient.ZAdd(ctx, "zremrank", redis.Z{Score: 10, Member: "a"}, redis.Z{Score: 20, Member: "b"}, redis.Z{Score: 30, Member: "c"}).Err())

	// ZREMRANGEBYRANK - 按排名删除 (删除排名0到1，即a和b)
	count, err := sharedClient.ZRemRangeByRank(ctx, "zremrank", 0, 1).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)

	// 验证剩余元素
	members, _ := sharedClient.ZRange(ctx, "zremrank", 0, -1).Result()
	assert.Equal(t, []string{"c"}, members)
}

// TestZRemRangeByScore 测试 ZREMRANGEBYSCORE 命令
func TestZRemRangeByScore(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 准备测试数据
	assert.NoError(t, sharedClient.ZAdd(ctx, "zremscore", redis.Z{Score: 10, Member: "a"}, redis.Z{Score: 20, Member: "b"}, redis.Z{Score: 30, Member: "c"}).Err())

	// ZREMRANGEBYSCORE - 按分数范围删除
	count, err := sharedClient.ZRemRangeByScore(ctx, "zremscore", "10", "20").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)

	// 验证剩余元素
	members, _ := sharedClient.ZRange(ctx, "zremscore", 0, -1).Result()
	assert.Equal(t, []string{"c"}, members)

	// ZREMRANGEBYSCORE - 不包含边界
	assert.NoError(t, sharedClient.ZAdd(ctx, "zremscore2", redis.Z{Score: 10, Member: "a"}, redis.Z{Score: 20, Member: "b"}, redis.Z{Score: 30, Member: "c"}).Err())
	count, err = sharedClient.ZRemRangeByScore(ctx, "zremscore2", "(10", "(30").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count) // 只有b被删除
}

// TestZPopMaxMin 测试 ZPOPMAX 和 ZPOPMIN 命令
func TestZPopMaxMin(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 准备测试数据
	assert.NoError(t, sharedClient.ZAdd(ctx, "zpopmaxmin", redis.Z{Score: 10, Member: "a"}, redis.Z{Score: 20, Member: "b"}, redis.Z{Score: 30, Member: "c"}).Err())

	// ZPOPMAX - 弹出最大值的成员
	result, err := sharedClient.ZPopMax(ctx, "zpopmaxmin").Result()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, "c", result[0].Member)
	assert.Equal(t, float64(30), result[0].Score)

	// ZPOPMIN - 弹出最小值的成员
	result, err = sharedClient.ZPopMin(ctx, "zpopmaxmin").Result()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, "a", result[0].Member)
	assert.Equal(t, float64(10), result[0].Score)

	// 验证剩余元素
	members, _ := sharedClient.ZRange(ctx, "zpopmaxmin", 0, -1).Result()
	assert.Equal(t, []string{"b"}, members)
}

// TestBZPopMaxMin 测试 BZPOPMAX 和 BZPOPMIN 命令
func TestBZPopMaxMin(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 准备测试数据
	assert.NoError(t, sharedClient.ZAdd(ctx, "bzpop", redis.Z{Score: 10, Member: "a"}).Err())

	// BZPOPMIN - 阻塞弹出最小值
	result, err := sharedClient.BZPopMin(ctx, 0, "bzpop").Result()
	assert.NoError(t, err)
	assert.Equal(t, "bzpop", result.Key)
	assert.Equal(t, "a", result.Member)
	assert.Equal(t, float64(10), result.Score)

	// BZPOPMAX - 阻塞弹出最大值
	assert.NoError(t, sharedClient.ZAdd(ctx, "bzpop2", redis.Z{Score: 100, Member: "max"}).Err())
	result, err = sharedClient.BZPopMax(ctx, 0, "bzpop2").Result()
	assert.NoError(t, err)
	assert.Equal(t, "bzpop2", result.Key)
	assert.Equal(t, "max", result.Member)
	assert.Equal(t, float64(100), result.Score)
}

// TestZLex 测试 ZLEXCOUNT 和 ZRANGEBYLEX 命令
func TestZLex(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 准备测试数据 (相同分数，按字典序排序)
	assert.NoError(t, sharedClient.ZAdd(ctx, "zlex", redis.Z{Score: 0, Member: "a"}, redis.Z{Score: 0, Member: "b"}, redis.Z{Score: 0, Member: "c"}, redis.Z{Score: 0, Member: "d"}).Err())

	// ZLEXCOUNT - 字典范围计数
	count, err := sharedClient.ZLexCount(ctx, "zlex", "[b", "[d").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), count) // b, c, d

	// ZRANGEBYLEX - 按字典范围获取（使用原始命令）
	result, err := sharedClient.Do(ctx, "ZRANGEBYLEX", "zlex", "[b", "[c").Result()
	assert.NoError(t, err)

	arr, ok := result.([]interface{})
	assert.True(t, ok)
	// Convert []interface{} to []string
	strArr := make([]string, len(arr))
	for i, v := range arr {
		strArr[i] = v.(string)
	}
	assert.Equal(t, []string{"b", "c"}, strArr)

	// ZRANGEBYLEX - 开区间
	result, err = sharedClient.Do(ctx, "ZRANGEBYLEX", "zlex", "(b", "(d").Result()
	assert.NoError(t, err)

	arr, ok = result.([]interface{})
	assert.True(t, ok)
	strArr = make([]string, len(arr))
	for i, v := range arr {
		strArr[i] = v.(string)
	}
	assert.Equal(t, []string{"c"}, strArr)
}

// TestZRevRangeByLex 测试 ZREVRANGEBYLEX 命令
func TestZRevRangeByLex(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 准备测试数据 (相同分数)
	assert.NoError(t, sharedClient.ZAdd(ctx, "zrevlex", redis.Z{Score: 0, Member: "a"}, redis.Z{Score: 0, Member: "b"}, redis.Z{Score: 0, Member: "c"}, redis.Z{Score: 0, Member: "d"}).Err())

	// ZREVRANGEBYLEX - 逆序按字典范围获取
	result, err := sharedClient.Do(ctx, "ZREVRANGEBYLEX", "zrevlex", "[d", "[b").Result()
	assert.NoError(t, err)

	arr, ok := result.([]interface{})
	assert.True(t, ok)
	// 逆序应该返回 d, c, b
	assert.Equal(t, 3, len(arr))
	// Verify values
	strArr := make([]string, len(arr))
	for i, v := range arr {
		strArr[i] = v.(string)
	}
	assert.Equal(t, []string{"d", "c", "b"}, strArr)
}

// TestZRemRangeByLex 测试 ZREMRANGEBYLEX 命令
func TestZRemRangeByLex(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 准备测试数据
	assert.NoError(t, sharedClient.ZAdd(ctx, "zremlex", redis.Z{Score: 0, Member: "a"}, redis.Z{Score: 0, Member: "b"}, redis.Z{Score: 0, Member: "c"}, redis.Z{Score: 0, Member: "d"}).Err())

	// ZREMRANGEBYLEX - 按字典范围删除
	count, err := sharedClient.Do(ctx, "ZREMRANGEBYLEX", "zremlex", "[b", "[c").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)

	// 验证剩余元素
	members, _ := sharedClient.ZRange(ctx, "zremlex", 0, -1).Result()
	assert.Equal(t, []string{"a", "d"}, members)
}

// TestZScan 测试 ZSCAN 命令
func TestZScan(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 准备测试数据
	assert.NoError(t, sharedClient.ZAdd(ctx, "zscan", redis.Z{Score: 10, Member: "a"}, redis.Z{Score: 20, Member: "b"}, redis.Z{Score: 30, Member: "c"}).Err())

	// ZSCAN - 迭代扫描
	result, err := sharedClient.Do(ctx, "ZSCAN", "zscan", "0").Result()
	assert.NoError(t, err)

	arr, ok := result.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr)) // [cursor, [members...]]
	cursor, _ := arr[0].(int64)
	assert.Equal(t, int64(0), cursor)

	members, ok := arr[1].([]interface{})
	assert.True(t, ok)
	// 应该有3个成员 * 2 (member + score) = 6个元素
	assert.Equal(t, 6, len(members))

	// ZSCAN with MATCH
	result, err = sharedClient.Do(ctx, "ZSCAN", "zscan", "0", "MATCH", "a*").Result()
	assert.NoError(t, err)

	arr, ok = result.([]interface{})
	assert.True(t, ok)
	members, ok = arr[1].([]interface{})
	assert.True(t, ok)
	// 应该只有a
	assert.Equal(t, 2, len(members))
}

// TestZRangeWithScores 测试 ZRANGE WITHSCORES 选项
func TestZRangeWithScores(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 准备测试数据
	assert.NoError(t, sharedClient.ZAdd(ctx, "zrangews", redis.Z{Score: 10, Member: "a"}, redis.Z{Score: 20, Member: "b"}, redis.Z{Score: 30, Member: "c"}).Err())

	// ZRANGE WITHSCORES
	result, err := sharedClient.ZRangeWithScores(ctx, "zrangews", 0, -1).Result()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(result))

	assert.Equal(t, "a", result[0].Member)
	assert.Equal(t, float64(10), result[0].Score)

	assert.Equal(t, "b", result[1].Member)
	assert.Equal(t, float64(20), result[1].Score)

	assert.Equal(t, "c", result[2].Member)
	assert.Equal(t, float64(30), result[2].Score)
}

// TestZRangeByRankWithScores 测试 ZRANGEBYRANK WITHSCORES
func TestZRangeByRankWithScores(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 准备测试数据
	assert.NoError(t, sharedClient.ZAdd(ctx, "zrankws", redis.Z{Score: 10, Member: "a"}, redis.Z{Score: 20, Member: "b"}, redis.Z{Score: 30, Member: "c"}).Err())

	// ZRANGEBYRANK with WITHSCORES - 需要用原始命令
	result, err := sharedClient.Do(ctx, "ZRANGE", "zrankws", "0", "1", "WITHSCORES").Result()
	assert.NoError(t, err)

	arr, ok := result.([]interface{})
	assert.True(t, ok)
	// [member1, score1, member2, score2]
	assert.Equal(t, 4, len(arr))
	assert.Equal(t, "a", arr[0])
	assert.Equal(t, "10", arr[1]) // Scores are returned as strings in RESP
	assert.Equal(t, "b", arr[2])
	assert.Equal(t, "20", arr[3])
}
