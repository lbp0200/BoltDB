package integration

import (
	"context"
	"testing"

	"github.com/zeebo/assert"
)

// TestPFAdd 测试 PFADD 命令
func TestPFAdd(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// PFADD - 添加元素
	result, err := sharedClient.Do(ctx, "PFADD", "hllkey", "a", "b", "c").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), result) // 返回1表示基数改变

	// 添加重复元素
	result, err = sharedClient.Do(ctx, "PFADD", "hllkey", "a", "b").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result) // 返回0表示基数未改变

	// PFADD - 不存在的键
	result, err = sharedClient.Do(ctx, "PFADD", "newkey", "x", "y", "z").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), result)
}

// TestPFCount 测试 PFCOUNT 命令
func TestPFCount(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 添加测试数据
	assert.NoError(t, sharedClient.Do(ctx, "PFADD", "hllcount", "a", "b", "c", "d", "e").Err())

	// PFCOUNT - 获取基数估计
	result, err := sharedClient.Do(ctx, "PFCOUNT", "hllcount").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(5), result)

	// 添加更多元素
	assert.NoError(t, sharedClient.Do(ctx, "PFADD", "hllcount", "f", "g", "h", "i", "j").Err())

	result, err = sharedClient.Do(ctx, "PFCOUNT", "hllcount").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(10), result)

	// PFCOUNT - 不存在的键
	result, err = sharedClient.Do(ctx, "PFCOUNT", "nonexistent").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result)
}

// TestPFCountMultiple 测试 PFCOUNT 多键命令
func TestPFCountMultiple(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 创建两个HyperLogLog
	assert.NoError(t, sharedClient.Do(ctx, "PFADD", "hll1", "a", "b", "c").Err())
	assert.NoError(t, sharedClient.Do(ctx, "PFADD", "hll2", "d", "e", "f").Err())

	// PFCOUNT 多键 - 合并基数
	result, err := sharedClient.Do(ctx, "PFCOUNT", "hll1", "hll2").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(6), result)

	// PFCOUNT 多键 - 有重复元素
	_ = sharedClient.Do(ctx, "PFADD", "hll2", "a", "b").Err() // 添加重复元素
	result, err = sharedClient.Do(ctx, "PFCOUNT", "hll1", "hll2").Result()
	assert.NoError(t, err)
	// 重复元素不应该增加唯一计数
	assert.Equal(t, int64(6), result)
}

// TestPFMerge 测试 PFMERGE 命令
func TestPFMerge(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 创建两个HyperLogLog
	assert.NoError(t, sharedClient.Do(ctx, "PFADD", "hllsource1", "a", "b", "c").Err())
	assert.NoError(t, sharedClient.Do(ctx, "PFADD", "hllsource2", "d", "e", "f").Err())

	// PFMERGE - 合并到目标
	result, err := sharedClient.Do(ctx, "PFMERGE", "hlldest", "hllsource1", "hllsource2").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// 验证合并后的基数
	count, err := sharedClient.Do(ctx, "PFCOUNT", "hlldest").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(6), count)

	// PFMERGE - 包含重复元素
	assert.NoError(t, sharedClient.Do(ctx, "PFADD", "hllsource3", "a", "b", "c", "d", "e").Err())
	result, err = sharedClient.Do(ctx, "PFMERGE", "hllmerge2", "hllsource1", "hllsource3").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	count, err = sharedClient.Do(ctx, "PFCOUNT", "hllmerge2").Result()
	assert.NoError(t, err)
	// 合并后应该还是5个唯一元素
	assert.Equal(t, int64(5), count)
}

// TestPFAddUnique 测试 HyperLogLog 唯一性
func TestPFAddUnique(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 添加大量元素
	for i := 0; i < 1000; i++ {
		_, err := sharedClient.Do(ctx, "PFADD", "hllunique", i).Result()
		if err != nil {
			t.Fatalf("PFADD failed: %v", err)
		}
	}

	// 验证基数接近1000（HyperLogLog是估计值）
	result, err := sharedClient.Do(ctx, "PFCOUNT", "hllunique").Result()
	assert.NoError(t, err)
	count, ok := result.(int64)
	assert.True(t, ok)
	// HyperLogLog有误差，但1000个元素应该在合理范围内
	assert.True(t, count >= 900 && count <= 1100)
}

// TestPFInfo 测试 PFINFO 命令
func TestPFInfo(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 创建HyperLogLog
	assert.NoError(t, sharedClient.Do(ctx, "PFADD", "hllinfo", "a", "b", "c").Err())

	// PFINFO - 获取信息
	result, err := sharedClient.Do(ctx, "PFINFO", "hllinfo").Result()
	assert.NoError(t, err)

	arr, ok := result.([]interface{})
	assert.True(t, ok)
	// 应该返回 key-value 对
	assert.True(t, len(arr) >= 2)

	// 查找特定字段
	found := false
	for i := 0; i < len(arr)-1; i += 2 {
		key, _ := arr[i].(string)
		if key == "encoding" {
			found = true
			break
		}
	}
	assert.True(t, found)
}
