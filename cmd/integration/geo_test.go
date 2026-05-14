package integration

import (
	"context"
	"testing"

	"github.com/zeebo/assert"
)

// TestGeoAdd 测试 GEOADD 命令
func TestGeoAdd(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// GEOADD - 添加地理位置
	// 北京: 116.40, 39.90
	// 上海: 121.47, 31.23
	// 广州: 113.26, 23.12
	result, err := sharedClient.Do(ctx, "GEOADD", "mygeo", "116.40", "39.90", "beijing", "121.47", "31.23", "shanghai").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), result)

	// 添加重复位置
	result, err = sharedClient.Do(ctx, "GEOADD", "mygeo", "116.40", "39.90", "beijing").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result)
}

// TestGeoPos 测试 GEOPOS 命令
func TestGeoPos(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 添加测试数据
	_, err := sharedClient.Do(ctx, "GEOADD", "mygeopos", "116.40", "39.90", "beijing", "121.47", "31.23", "shanghai").Result()
	assert.NoError(t, err)

	// GEOPOS - 获取位置（使用原始命令）
	result, err := sharedClient.Do(ctx, "GEOPOS", "mygeopos", "beijing", "shanghai").Result()
	assert.NoError(t, err)

	arr, ok := result.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr))
}

// TestGeoHash 测试 GEOHASH 命令
func TestGeoHash(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 添加测试数据
	_ = sharedClient.Do(ctx, "GEOADD", "mygeohash", "116.40", "39.90", "beijing").Err()

	// GEOHASH - 获取geohash
	result, err := sharedClient.Do(ctx, "GEOHASH", "mygeohash", "beijing").Result()
	assert.NoError(t, err)

	arr, ok := result.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 1, len(arr))

	hash, ok := arr[0].(string)
	assert.True(t, ok)
	// 北京的geohash应该是有效的
	assert.True(t, len(hash) > 0)
}

// TestGeoDist 测试 GEODIST 命令
func TestGeoDist(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 添加测试数据
	_, err := sharedClient.Do(ctx, "GEOADD", "mydist", "116.40", "39.90", "beijing", "121.47", "31.23", "shanghai").Result()
	assert.NoError(t, err)

	// GEODIST - 计算距离（默认米）
	dist, err := sharedClient.Do(ctx, "GEODIST", "mydist", "beijing", "shanghai").Result()
	assert.NoError(t, err)

	// 验证返回了距离值
	assert.True(t, dist != nil)
}

// TestGeoSearch 测试 GEOSEARCH 命令
func TestGeoSearch(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 添加测试数据
	_, err := sharedClient.Do(ctx, "GEOADD", "mysearch", "116.40", "39.90", "beijing").Result()
	assert.NoError(t, err)
	_, err = sharedClient.Do(ctx, "GEOADD", "mysearch", "121.47", "31.23", "shanghai").Result()
	assert.NoError(t, err)

	// GEOSEARCH - 按圆形区域搜索
	result, err := sharedClient.Do(ctx, "GEOSEARCH", "mysearch", "FROMLONLAT", "116.40", "39.90", "BYRADIUS", "500", "km").Result()
	assert.NoError(t, err)

	// 验证返回了结果
	assert.True(t, result != nil)
}

// TestGeoSearchStore 测试 GEOSEARCHSTORE 命令
func TestGeoSearchStore(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 添加测试数据
	_, err := sharedClient.Do(ctx, "GEOADD", "searchstore", "116.40", "39.90", "beijing").Result()
	assert.NoError(t, err)
	_, err = sharedClient.Do(ctx, "GEOADD", "searchstore", "121.47", "31.23", "shanghai").Result()
	assert.NoError(t, err)

	// GEOSEARCHSTORE - 搜索并存储结果
	result, err := sharedClient.Do(ctx, "GEOSEARCHSTORE", "resultstore", "searchstore", "FROMLONLAT", "116.40", "39.90", "BYRADIUS", "2000", "km").Result()
	assert.NoError(t, err)

	// 验证返回了结果
	assert.True(t, result != nil)
}
