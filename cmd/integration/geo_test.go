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

	// GEOPOS - 获取位置
	result, err := sharedClient.Do(ctx, "GEOPOS", "mygeopos", "beijing", "shanghai").Result()
	assert.NoError(t, err)

	arr, ok := result.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr))

	// Validate nested [lon, lat] structure for each coordinate
	for _, elem := range arr {
		coord, ok := elem.([]interface{})
		assert.True(t, ok)
		assert.Equal(t, 2, len(coord))
		lon, ok := coord[0].(string)
		assert.True(t, ok)
		lat, ok := coord[1].(string)
		assert.True(t, ok)
		assert.True(t, len(lon) > 0)
		assert.True(t, len(lat) > 0)
	}

	// Verify non-existent member returns nil
	result, err = sharedClient.Do(ctx, "GEOPOS", "mygeopos", "nonexistent").Result()
	assert.NoError(t, err)
	nilArr, ok := result.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 1, len(nilArr))
	assert.Nil(t, nilArr[0])
}

// TestGeoHash 测试 GEOHASH 命令
func TestGeoHash(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 添加测试数据
	assert.NoError(t, sharedClient.Do(ctx, "GEOADD", "mygeohash", "116.40", "39.90", "beijing").Err())

	// GEOHASH - 获取geohash
	result, err := sharedClient.Do(ctx, "GEOHASH", "mygeohash", "beijing").Result()
	assert.NoError(t, err)

	arr, ok := result.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 1, len(arr))

	hash, ok := arr[0].(string)
	assert.True(t, ok)
	// 北京的geohash应该是有效的
	assert.True(t, len(hash) > 0) // geohash is a non-empty string
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
	assert.True(t, result != nil)
}

// TestGeoSearchWithModifiers 测试 GEOSEARCH WITHCOORD/WITHDIST/WITHHASH
func TestGeoSearchWithModifiers(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 添加测试数据
	_, err := sharedClient.Do(ctx, "GEOADD", "searchmod", "116.40", "39.90", "beijing").Result()
	assert.NoError(t, err)
	_, err = sharedClient.Do(ctx, "GEOADD", "searchmod", "121.47", "31.23", "shanghai").Result()
	assert.NoError(t, err)

	// WITHCOORD — 返回 nested [member, [lon, lat]]
	result, err := sharedClient.Do(ctx, "GEOSEARCH", "searchmod", "FROMLONLAT", "116.40", "39.90", "BYRADIUS", "2000", "km", "WITHCOORD").Result()
	assert.NoError(t, err)
	arr := result.([]interface{})
	assert.Equal(t, 2, len(arr)) // 2 cities within 2000km
	for _, elem := range arr {
		entry := elem.([]interface{})
		assert.Equal(t, 2, len(entry)) // WITHCOORD: [member, [lon, lat]]
		coord := entry[1].([]interface{})
		assert.Equal(t, 2, len(coord))
	}

	// WITHDIST — 返回 nested [member, dist]
	result, err = sharedClient.Do(ctx, "GEOSEARCH", "searchmod", "FROMLONLAT", "116.40", "39.90", "BYRADIUS", "2000", "km", "WITHDIST").Result()
	assert.NoError(t, err)
	arr = result.([]interface{})
	assert.Equal(t, 2, len(arr))
	for _, elem := range arr {
		entry := elem.([]interface{})
		assert.Equal(t, 2, len(entry)) // WITHDIST: [member, dist]
	}

	// WITHHASH — 返回 nested [member, hash]
	result, err = sharedClient.Do(ctx, "GEOSEARCH", "searchmod", "FROMLONLAT", "116.40", "39.90", "BYRADIUS", "2000", "km", "WITHHASH").Result()
	assert.NoError(t, err)
	arr = result.([]interface{})
	assert.Equal(t, 2, len(arr))
	for _, elem := range arr {
		entry := elem.([]interface{})
		assert.Equal(t, 2, len(entry)) // WITHHASH: [member, hash]
	}

	// WITHDIST WITHCOORD — 返回 nested [member, dist, [lon, lat]]
	result, err = sharedClient.Do(ctx, "GEOSEARCH", "searchmod", "FROMLONLAT", "116.40", "39.90", "BYRADIUS", "2000", "km", "WITHDIST", "WITHCOORD").Result()
	assert.NoError(t, err)
	arr = result.([]interface{})
	assert.Equal(t, 2, len(arr))
	for _, elem := range arr {
		entry := elem.([]interface{})
		assert.Equal(t, 3, len(entry)) // WITHDIST+WITHCOORD: [member, dist, [lon, lat]]
		coord := entry[2].([]interface{})
		assert.Equal(t, 2, len(coord))
	}
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
