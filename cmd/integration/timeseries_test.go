package integration

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/zeebo/assert"
)

func TestTimeSeries(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// Test TS.CREATE
	err := sharedClient.Do(ctx, "TS.CREATE", "temperature").Err()
	assert.NoError(t, err)

	// Verify type is "ts"
	keyType, err := sharedClient.Type(ctx, "temperature").Result()
	assert.NoError(t, err)
	assert.Equal(t, "ts", keyType)

	// Test TS.CREATE with options
	err = sharedClient.Do(ctx, "TS.CREATE", "humidity", "RETENTION", 3600000, "ENCODING", "compressed").Err()
	assert.NoError(t, err)

	// Test TS.ADD
	timestamp1 := time.Now().UnixNano() / int64(time.Millisecond)
	result, err := sharedClient.Do(ctx, "TS.ADD", "temperature", timestamp1, 25.5).Result()
	assert.NoError(t, err)
	assert.Equal(t, timestamp1, result)

	// Add more data points
	timestamp2 := timestamp1 + 1000
	result, err = sharedClient.Do(ctx, "TS.ADD", "temperature", timestamp2, 26.0).Result()
	assert.NoError(t, err)
	assert.Equal(t, timestamp2, result)

	timestamp3 := timestamp1 + 2000
	result, err = sharedClient.Do(ctx, "TS.ADD", "temperature", timestamp3, 27.5).Result()
	assert.NoError(t, err)
	assert.Equal(t, timestamp3, result)

	// Test TS.GET
	getResult, err := sharedClient.Do(ctx, "TS.GET", "temperature").Result()
	assert.NoError(t, err)
	// Should return array [timestamp, value]
	arr, ok := getResult.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr))
	// Handle both string and int64 responses
	switch v := arr[0].(type) {
	case int64:
		assert.Equal(t, timestamp3, v)
	case string:
		parsed, _ := strconv.ParseInt(v, 10, 64)
		assert.Equal(t, timestamp3, parsed)
	}

	// Test TS.RANGE
	rangeResult, err := sharedClient.Do(ctx, "TS.RANGE", "temperature", "-", "+").Result()
	assert.NoError(t, err)
	rangeArr, ok := rangeResult.([]interface{})
	assert.True(t, ok)
	// Each data point is 2 elements (timestamp, value), so 3 data points = 6 elements
	assert.Equal(t, 6, len(rangeArr))

	// Test TS.RANGE with count
	rangeResult, err = sharedClient.Do(ctx, "TS.RANGE", "temperature", "-", "+", "COUNT", 2).Result()
	assert.NoError(t, err)
	rangeArr, ok = rangeResult.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 4, len(rangeArr)) // 2 data points * 2 elements

	// Test TS.RANGE with specific range
	rangeResult, err = sharedClient.Do(ctx, "TS.RANGE", "temperature", strconv.FormatInt(timestamp1, 10), strconv.FormatInt(timestamp2, 10)).Result()
	assert.NoError(t, err)
	rangeArr, ok = rangeResult.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 4, len(rangeArr)) // 2 data points * 2 elements

	// Test TS.INFO
	infoResult, err := sharedClient.Do(ctx, "TS.INFO", "temperature").Result()
	assert.NoError(t, err)
	infoArr, ok := infoResult.([]interface{})
	assert.True(t, ok)
	assert.True(t, len(infoArr) >= 2)

	// Test TS.LEN
	length, err := sharedClient.Do(ctx, "TS.LEN", "temperature").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), length)

	// Test TS.DEL
	deleted, err := sharedClient.Do(ctx, "TS.DEL", "temperature", timestamp1, timestamp2).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), deleted)

	// Verify deletion
	length, err = sharedClient.Do(ctx, "TS.LEN", "temperature").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), length)

	// Test TSMGET
	// Create another time series
	sharedClient.Do(ctx, "TS.ADD", "humidity", timestamp1, 60.0)
	sharedClient.Do(ctx, "TS.ADD", "humidity", timestamp2, 65.0)

	mgetResult, err := sharedClient.Do(ctx, "TS.MGET", "*", "temperature", "humidity").Result()
	assert.NoError(t, err)
	mgetArr, ok := mgetResult.([]interface{})
	assert.True(t, ok)
	// TSMGET returns array of [timestamp, value] for each key
	// 2 keys = 2 pairs = 4 elements total
	assert.Equal(t, 4, len(mgetArr))

	// Test TS.ADD with ON_DUPLICATE
	timestamp4 := timestamp1 + 3000
	sharedClient.Do(ctx, "TS.ADD", "testdup", timestamp4, 10.0)
	// Add duplicate with skip
	result, err = sharedClient.Do(ctx, "TS.ADD", "testdup", timestamp4, 20.0, "ON_DUPLICATE", "skip").Result()
	assert.NoError(t, err)

	// Verify value wasn't changed
	getResult, err = sharedClient.Do(ctx, "TS.GET", "testdup").Result()
	assert.NoError(t, err)
	arr, ok = getResult.([]interface{})
	assert.True(t, ok)
	// Value may be string, int64 or float64
	val := arr[1]
	switch v := val.(type) {
	case float64:
		assert.Equal(t, float64(10.0), v)
	case int64:
		assert.Equal(t, int64(10), v)
	case string:
		parsed, _ := strconv.ParseFloat(v, 64)
		assert.Equal(t, float64(10.0), parsed)
	}

	// Test error cases
	// Create existing key
	err = sharedClient.Do(ctx, "TS.CREATE", "temperature").Err()
	assert.Error(t, err)

	// Key not found
	_, err = sharedClient.Do(ctx, "TS.GET", "nonexistent").Result()
	assert.Error(t, err)
}

func TestTimeSeriesWithAutoTimestamp(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// Test TS.ADD with auto timestamp (*)
	result, err := sharedClient.Do(ctx, "TS.ADD", "metric", "*", 100.0).Result()
	assert.NoError(t, err)
	assert.NotEqual(t, nil, result)

	// Verify data was added
	length, err := sharedClient.Do(ctx, "TS.LEN", "metric").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), length)
}

func TestTimeSeriesDuplicatePolicy(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	timestamp := time.Now().UnixNano() / int64(time.Millisecond)

	// Add initial value
	sharedClient.Do(ctx, "TS.ADD", "ts1", timestamp, 10.0)

	// Test ON_DUPLICATE block
	err := sharedClient.Do(ctx, "TS.ADD", "ts1", timestamp, 20.0, "ON_DUPLICATE", "block").Err()
	assert.Error(t, err)

	// Test ON_DUPLICATE update
	_, err = sharedClient.Do(ctx, "TS.ADD", "ts1", timestamp, 30.0, "ON_DUPLICATE", "update").Result()
	assert.NoError(t, err)

	// Verify value was updated
	getResult, err := sharedClient.Do(ctx, "TS.GET", "ts1").Result()
	assert.NoError(t, err)
	arr := getResult.([]interface{})
	// Value may be string, int64 or float64 depending on how Redis returns it
	val := arr[1]
	var match bool
	switch v := val.(type) {
	case float64:
		match = v == 30.0
	case int64:
		match = v == 30
	case string:
		parsed, _ := strconv.ParseFloat(v, 64)
		match = parsed == 30.0
	}
	assert.True(t, match)
}
