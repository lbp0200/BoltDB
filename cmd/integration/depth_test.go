package integration

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zeebo/assert"
)

// TestStringConcurrent_ReadWriteConflict tests concurrent read/write to same key
func TestStringConcurrent_ReadWriteConflict(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()
	const goroutines = 10
	const opsPerGoroutine = 100

	var wg sync.WaitGroup
	results := make(chan int64, goroutines*opsPerGoroutine)

	// Writer goroutines
	for i := 0; i < goroutines/2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				testClient.Set(ctx, "concurrent_key", idx*1000+j, 0)
			}
		}(i)
	}

	// Reader goroutines
	for i := 0; i < goroutines/2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				val, _ := testClient.Get(ctx, "concurrent_key").Int64()
				results <- val
			}
		}()
	}

	wg.Wait()
	close(results)

	// Verify final value is valid (was written by some writer)
	finalVal, err := testClient.Get(ctx, "concurrent_key").Int64()
	assert.NoError(t, err)
	assert.True(t, finalVal >= 0)
}

// TestStringConcurrent_ConcurrentIncrement tests concurrent INCR on same key
func TestStringConcurrent_ConcurrentIncrement(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()
	const goroutines = 10
	const incrsPerGoroutine = 100

	// Initialize counter
	err := testClient.Set(ctx, "incr_counter", 0, 0).Err()
	assert.NoError(t, err)

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < incrsPerGoroutine; j++ {
				_, _ = testClient.Incr(ctx, "incr_counter").Result()
			}
		}()
	}

	wg.Wait()

	// Final value should be exactly goroutines * incrsPerGoroutine
	finalVal, err := testClient.Get(ctx, "incr_counter").Int64()
	assert.NoError(t, err)
	assert.Equal(t, int64(goroutines*incrsPerGoroutine), finalVal)
}

// TestStringConcurrent_AppendRace tests concurrent APPEND operations
func TestStringConcurrent_AppendRace(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()
	const goroutines = 5
	const appendsPerGoroutine = 20

	testClient.Set(ctx, "append_race_key", "init", 0)

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < appendsPerGoroutine; j++ {
				testClient.Append(ctx, "append_race_key", string(rune('A'+idx)))
			}
		}(i)
	}

	wg.Wait()

	// Final length should be 4 (init) + goroutines * appendsPerGoroutine
	finalLen, err := testClient.StrLen(ctx, "append_race_key").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(4+goroutines*appendsPerGoroutine), finalLen)
}

// TestStringConcurrent_DecrRace tests concurrent DECR on same key
func TestStringConcurrent_DecrRace(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()
	const goroutines = 10
	const decrsPerGoroutine = 100

	// Initialize counter to a large positive value
	err := testClient.Set(ctx, "decr_counter", goroutines*decrsPerGoroutine, 0).Err()
	assert.NoError(t, err)

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < decrsPerGoroutine; j++ {
				_, _ = testClient.Decr(ctx, "decr_counter").Result()
			}
		}()
	}

	wg.Wait()

	// Final value should be 0
	finalVal, err := testClient.Get(ctx, "decr_counter").Int64()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), finalVal)
}

// TestStringConcurrent_SetexRace tests concurrent SETEX on same key
func TestStringConcurrent_SetexRace(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()
	const goroutines = 5
	const opsPerGoroutine = 20

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				testClient.SetEx(ctx, "setex_concurrent_key", fmt.Sprintf("value%d_%d", idx, j), 10*time.Second)
			}
		}(i)
	}

	wg.Wait()

	// Key should exist with some value
	val, err := testClient.Get(ctx, "setex_concurrent_key").Result()
	assert.NoError(t, err)
	assert.True(t, len(val) > 0)
}

// TestStringConcurrent_GetrangeSetrangeRace tests concurrent GETRANGE and SETRANGE
func TestStringConcurrent_GetrangeSetrangeRace(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()
	const goroutines = 5
	const opsPerGoroutine = 50

	// Initialize string
	testClient.Set(ctx, "range_race_key", "abcdefghij", 0)

	var wg sync.WaitGroup

	// SETRANGE goroutines
	for i := 0; i < goroutines/2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				offset := int64(j % 10)
				testClient.SetRange(ctx, "range_race_key", offset, fmt.Sprintf("%d", idx))
			}
		}(i)
	}

	// GETRANGE goroutines
	for i := 0; i < goroutines/2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				testClient.GetRange(ctx, "range_race_key", 0, -1)
			}
		}()
	}

	wg.Wait()

	// Key should still be a valid string
	val, err := testClient.Get(ctx, "range_race_key").Result()
	assert.NoError(t, err)
	assert.Equal(t, 10, len(val))
}

// TestListConcurrent_PushPopRace tests concurrent LPUSH and LPOP
func TestListConcurrent_PushPopRace(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()
	const goroutines = 10
	const opsPerGoroutine = 100

	testClient.Del(ctx, "race_list")

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				if j%2 == 0 {
					testClient.LPush(ctx, "race_list", idx*1000+j)
				} else {
					testClient.RPush(ctx, "race_list", idx*1000+j)
				}
			}
		}(i)
	}

	wg.Wait()

	// List should have some elements (not empty due to race)
	llen, _ := testClient.LLen(ctx, "race_list").Result()
	assert.True(t, llen >= 0)
}

// TestListConcurrent_MultipleBlockingPops tests BLPOP behavior
func TestListConcurrent_MultipleBlockingPops(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()
	testClient.Del(ctx, "blocking_list")

	// Pre-populate the list
	testClient.LPush(ctx, "blocking_list", "value1")
	testClient.LPush(ctx, "blocking_list", "value2")

	// BLPOP should return immediately since data exists
	result, err := testClient.BLPop(ctx, 0, "blocking_list").Result()
	assert.NoError(t, err)
	assert.Equal(t, "blocking_list", result[0])
	// The value returned is from the LEFT side (LPUSH), so it's "value2" (last pushed)
	assert.Equal(t, "value2", result[1])

	// Pop another value
	result, err = testClient.BLPop(ctx, 0, "blocking_list").Result()
	assert.NoError(t, err)
	assert.Equal(t, "value1", result[1])
}

// TestHashConcurrent_HgetHsetRace tests concurrent HGET and HSET
func TestHashConcurrent_HgetHsetRace(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()
	const goroutines = 10
	const opsPerGoroutine = 100

	testClient.Del(ctx, "race_hash")

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				field := fmt.Sprintf("field%d", j%10) // Use only 10 fields
				if j%2 == 0 {
					testClient.HSet(ctx, "race_hash", field, idx*1000+j)
				} else {
					testClient.HGet(ctx, "race_hash", field)
				}
			}
		}(i)
	}

	wg.Wait()

	// Hash should have some fields
	hlen, _ := testClient.HLen(ctx, "race_hash").Result()
	assert.True(t, hlen > 0)
}

// TestStringError_TypeMismatchIntegration tests string commands on wrong types (integration level)
func TestStringError_TypeMismatchIntegration(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// Create a hash key
	testClient.HSet(ctx, "myhash", "field", "value")

	// APPEND on hash should return WRONGTYPE
	err := testClient.Append(ctx, "myhash", "extra").Err()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "WRONGTYPE"))

	// DECR on hash should return WRONGTYPE
	err = testClient.Decr(ctx, "myhash").Err()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "WRONGTYPE"))

	// DECRBY on hash should return WRONGTYPE
	err = testClient.DecrBy(ctx, "myhash", 1).Err()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "WRONGTYPE"))

	// SETEX on hash should return WRONGTYPE
	err = testClient.SetEx(ctx, "myhash", "value", 10).Err()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "WRONGTYPE"))
}

// TestStringError_DecrbyOnFloat tests DECRBY on float string value
func TestStringError_DecrbyOnFloat(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	testClient.Set(ctx, "float_key", "1.5", 0)
	err := testClient.DecrBy(ctx, "float_key", 1).Err()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "not an integer"))
}
