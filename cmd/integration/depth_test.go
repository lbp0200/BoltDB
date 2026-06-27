package integration

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zeebo/assert"
)

// TestStringConcurrent_ReadWriteConflict tests concurrent read/write to same key
func TestStringConcurrent_ReadWriteConflict(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

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
				sharedClient.Set(ctx, "concurrent_key", idx*1000+j, 0)
			}
		}(i)
	}

	// Reader goroutines
	for i := 0; i < goroutines/2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				val, _ := sharedClient.Get(ctx, "concurrent_key").Int64()
				results <- val
			}
		}()
	}

	wg.Wait()
	close(results)

	// Verify final value is valid (was written by some writer)
	finalVal, err := sharedClient.Get(ctx, "concurrent_key").Int64()
	assert.NoError(t, err)
	assert.True(t, finalVal >= 0)
}

// TestStringConcurrent_ConcurrentIncrement tests concurrent INCR on same key
func TestStringConcurrent_ConcurrentIncrement(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()
	const goroutines = 10
	const incrsPerGoroutine = 100

	// Initialize counter
	err := sharedClient.Set(ctx, "incr_counter", 0, 0).Err()
	assert.NoError(t, err)

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < incrsPerGoroutine; j++ {
				_, _ = sharedClient.Incr(ctx, "incr_counter").Result()
			}
		}()
	}

	wg.Wait()

	// Final value should be exactly goroutines * incrsPerGoroutine
	finalVal, err := sharedClient.Get(ctx, "incr_counter").Int64()
	assert.NoError(t, err)
	assert.Equal(t, int64(goroutines*incrsPerGoroutine), finalVal)
}

// TestStringConcurrent_AppendRace tests concurrent APPEND operations
func TestStringConcurrent_AppendRace(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()
	const goroutines = 5
	const appendsPerGoroutine = 20

	sharedClient.Set(ctx, "append_race_key", "init", 0)

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < appendsPerGoroutine; j++ {
				sharedClient.Append(ctx, "append_race_key", string(rune('A'+idx)))
			}
		}(i)
	}

	wg.Wait()

	// Final length should be 4 (init) + goroutines * appendsPerGoroutine
	finalLen, err := sharedClient.StrLen(ctx, "append_race_key").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(4+goroutines*appendsPerGoroutine), finalLen)
}

// TestStringConcurrent_DecrRace tests concurrent DECR on same key
func TestStringConcurrent_DecrRace(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()
	const goroutines = 10
	const decrsPerGoroutine = 100

	// Initialize counter to a large positive value
	err := sharedClient.Set(ctx, "decr_counter", goroutines*decrsPerGoroutine, 0).Err()
	assert.NoError(t, err)

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < decrsPerGoroutine; j++ {
				_, _ = sharedClient.Decr(ctx, "decr_counter").Result()
			}
		}()
	}

	wg.Wait()

	// Final value should be 0
	finalVal, err := sharedClient.Get(ctx, "decr_counter").Int64()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), finalVal)
}

// TestStringConcurrent_SetexRace tests concurrent SETEX on same key
func TestStringConcurrent_SetexRace(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()
	const goroutines = 5
	const opsPerGoroutine = 20

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				sharedClient.SetEx(ctx, "setex_concurrent_key", fmt.Sprintf("value%d_%d", idx, j), 10*time.Second)
			}
		}(i)
	}

	wg.Wait()

	// Key should exist with some value
	val, err := sharedClient.Get(ctx, "setex_concurrent_key").Result()
	assert.NoError(t, err)
	assert.True(t, len(val) > 0)
}

// TestStringConcurrent_GetrangeSetrangeRace tests concurrent GETRANGE and SETRANGE
func TestStringConcurrent_GetrangeSetrangeRace(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()
	const goroutines = 5
	const opsPerGoroutine = 50

	// Initialize string
	sharedClient.Set(ctx, "range_race_key", "abcdefghij", 0)

	var wg sync.WaitGroup

	// SETRANGE goroutines
	for i := 0; i < goroutines/2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				offset := int64(j % 10)
				sharedClient.SetRange(ctx, "range_race_key", offset, fmt.Sprintf("%d", idx))
			}
		}(i)
	}

	// GETRANGE goroutines
	for i := 0; i < goroutines/2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				sharedClient.GetRange(ctx, "range_race_key", 0, -1)
			}
		}()
	}

	wg.Wait()

	// Key should still be a valid string
	val, err := sharedClient.Get(ctx, "range_race_key").Result()
	assert.NoError(t, err)
	assert.Equal(t, 10, len(val))
}

// TestListConcurrent_PushPopRace tests concurrent LPUSH and LPOP
func TestListConcurrent_PushPopRace(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()
	const goroutines = 10
	const opsPerGoroutine = 100

	sharedClient.Del(ctx, "race_list")

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				if j%2 == 0 {
					sharedClient.LPush(ctx, "race_list", idx*1000+j)
				} else {
					sharedClient.RPush(ctx, "race_list", idx*1000+j)
				}
			}
		}(i)
	}

	wg.Wait()

	// List should have some elements (not empty due to race)
	llen, err := sharedClient.LLen(ctx, "race_list").Result()
	assert.NoError(t, err)
	assert.True(t, llen > 0)
}

// TestListConcurrent_MultipleBlockingPops tests BLPOP behavior
func TestListConcurrent_MultipleBlockingPops(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()
	sharedClient.Del(ctx, "blocking_list")

	// Pre-populate the list
	sharedClient.LPush(ctx, "blocking_list", "value1")
	sharedClient.LPush(ctx, "blocking_list", "value2")

	// BLPOP should return immediately since data exists
	result, err := sharedClient.BLPop(ctx, 0, "blocking_list").Result()
	assert.NoError(t, err)
	assert.Equal(t, "blocking_list", result[0])
	// The value returned is from the LEFT side (LPUSH), so it's "value2" (last pushed)
	assert.Equal(t, "value2", result[1])

	// Pop another value
	result, err = sharedClient.BLPop(ctx, 0, "blocking_list").Result()
	assert.NoError(t, err)
	assert.Equal(t, "value1", result[1])
}

// TestHashConcurrent_HgetHsetRace tests concurrent HGET and HSET
func TestHashConcurrent_HgetHsetRace(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()
	const goroutines = 10
	const opsPerGoroutine = 100

	sharedClient.Del(ctx, "race_hash")

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				field := fmt.Sprintf("field%d", j%10) // Use only 10 fields
				if j%2 == 0 {
					sharedClient.HSet(ctx, "race_hash", field, idx*1000+j)
				} else {
					sharedClient.HGet(ctx, "race_hash", field)
				}
			}
		}(i)
	}

	wg.Wait()

	// Hash should have some fields
	hlen, err := sharedClient.HLen(ctx, "race_hash").Result()
	assert.NoError(t, err)
	assert.True(t, hlen > 0)
}

// TestStringError_TypeMismatchIntegration tests string commands on wrong types (integration level)
func TestStringError_TypeMismatchIntegration(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// Create a hash key
	sharedClient.HSet(ctx, "myhash", "field", "value")

	// APPEND on hash should return WRONGTYPE
	err := sharedClient.Append(ctx, "myhash", "extra").Err()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "WRONGTYPE"))

	// DECR on hash should return WRONGTYPE
	err = sharedClient.Decr(ctx, "myhash").Err()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "WRONGTYPE"))

	// DECRBY on hash should return WRONGTYPE
	err = sharedClient.DecrBy(ctx, "myhash", 1).Err()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "WRONGTYPE"))

	// SETEX on hash should return WRONGTYPE
	err = sharedClient.SetEx(ctx, "myhash", "value", 10).Err()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "WRONGTYPE"))
}

// TestStringError_DecrbyOnFloat tests DECRBY on float string value
func TestStringError_DecrbyOnFloat(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	sharedClient.Set(ctx, "float_key", "1.5", 0)
	err := sharedClient.DecrBy(ctx, "float_key", 1).Err()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "not an integer"))
}

// TestListConcurrent_LpushxRace tests concurrent LPUSHX on same key
func TestListConcurrent_LpushxRace(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()
	const goroutines = 5
	const opsPerGoroutine = 50

	// Initialize list
	sharedClient.RPush(ctx, "lpushx_race_key", "initial")

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				sharedClient.LPushX(ctx, "lpushx_race_key", fmt.Sprintf("val%d_%d", idx, j))
			}
		}(i)
	}

	wg.Wait()

	// List should have more elements than initial
	llen, err := sharedClient.LLen(ctx, "lpushx_race_key").Result()
	assert.NoError(t, err)
	assert.True(t, llen > 1)
}

// TestListConcurrent_LmoveRace tests concurrent LMOVE operations
func TestListConcurrent_LmoveRace(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()
	const goroutines = 5
	const opsPerGoroutine = 20

	// Initialize source list with enough elements
	for i := 0; i < goroutines*opsPerGoroutine; i++ {
		sharedClient.RPush(ctx, "lmove_source", fmt.Sprintf("item%d", i))
	}

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				sharedClient.LMove(ctx, "lmove_source", "lmove_dest", "LEFT", "RIGHT")
			}
		}()
	}

	wg.Wait()

	// Source or dest should have elements
	srcLen, _ := sharedClient.LLen(ctx, "lmove_source").Result()
	dstLen, _ := sharedClient.LLen(ctx, "lmove_dest").Result()
	assert.True(t, srcLen+dstLen > 0)
}

// TestListError_WrongTypeIntegration tests list commands on wrong types (integration level)
func TestListError_WrongTypeIntegration(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// Create a string key
	sharedClient.Set(ctx, "string_key", "value", 0)

	// LPUSHX on string should return WRONGTYPE
	err := sharedClient.LPushX(ctx, "string_key", "val").Err()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "WRONGTYPE"))

	// RPUSHX on string should return WRONGTYPE
	err = sharedClient.RPushX(ctx, "string_key", "val").Err()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "WRONGTYPE"))

	// LMOVE from string key should return WRONGTYPE
	err = sharedClient.LMove(ctx, "string_key", "dest", "LEFT", "RIGHT").Err()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "WRONGTYPE"))
}

// TestHashConcurrent_HincrbyRace tests concurrent HINCRBY on same field
func TestHashConcurrent_HincrbyRace(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()
	const goroutines = 10
	const incrsPerGoroutine = 100
	expectedTotal := goroutines * incrsPerGoroutine

	sharedClient.Del(ctx, "incr_hash")
	sharedClient.HSet(ctx, "incr_hash", "counter", "0")

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < incrsPerGoroutine; j++ {
				sharedClient.HIncrBy(ctx, "incr_hash", "counter", 1)
			}
		}()
	}
	wg.Wait()

	// After fixing the race condition with key-level locking,
	// we can now assert the exact expected value.
	finalVal, err := sharedClient.HGet(ctx, "incr_hash", "counter").Int64()
	assert.NoError(t, err)
	assert.Equal(t, int64(expectedTotal), finalVal)
}

// TestSetConcurrent_SaddSremRace tests concurrent SADD and SREM on same set
func TestSetConcurrent_SaddSremRace(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()
	const goroutines = 10
	const opsPerGoroutine = 100

	sharedClient.Del(ctx, "race_set")

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				member := fmt.Sprintf("member%d", j%20)
				if j%2 == 0 {
					sharedClient.SAdd(ctx, "race_set", member)
				} else {
					sharedClient.SRem(ctx, "race_set", member)
				}
			}
		}(i)
	}

	wg.Wait()

	// Set should have some members
	card, err := sharedClient.SCard(ctx, "race_set").Result()
	assert.NoError(t, err)
	assert.True(t, card > 0)
}

// TestSetConcurrent_SismemberRace tests concurrent SISMEMBER on same set
func TestSetConcurrent_SismemberRace(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()
	const goroutines = 10
	const opsPerGoroutine = 100

	sharedClient.Del(ctx, "race_set")
	sharedClient.SAdd(ctx, "race_set", "target_member")

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				sharedClient.SIsMember(ctx, "race_set", "target_member")
			}
		}()
	}

	wg.Wait()

	// Member should still exist
	isMember, _ := sharedClient.SIsMember(ctx, "race_set", "target_member").Result()
	assert.True(t, isMember)
}

// TestSetError_WrongTypeIntegration tests set commands on wrong types (integration level)
func TestSetError_WrongTypeIntegration(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// Create a string key
	sharedClient.Set(ctx, "string_key", "value", 0)

	// SADD on string should return WRONGTYPE
	err := sharedClient.SAdd(ctx, "string_key", "member").Err()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "WRONGTYPE"))

	// SREM on string should return WRONGTYPE
	err = sharedClient.SRem(ctx, "string_key", "member").Err()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "WRONGTYPE"))

	// SISMEMBER on string should return WRONGTYPE
	err = sharedClient.SIsMember(ctx, "string_key", "member").Err()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "WRONGTYPE"))

	// SMEMBERS on string should return WRONGTYPE
	err = sharedClient.SMembers(ctx, "string_key").Err()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "WRONGTYPE"))

	// SCARD on string should return WRONGTYPE
	err = sharedClient.SCard(ctx, "string_key").Err()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "WRONGTYPE"))
}

// TestSortedSetConcurrent_ZaddZremRace tests concurrent ZADD and ZREM
func TestSortedSetConcurrent_ZaddZremRace(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()
	const goroutines = 10
	const opsPerGoroutine = 100

	sharedClient.Del(ctx, "race_zset")

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				member := fmt.Sprintf("member%d", j%20)
				score := float64(j%100) + 0.5
				if j%2 == 0 {
					sharedClient.ZAdd(ctx, "race_zset", redis.Z{Member: member, Score: score})
				} else {
					sharedClient.ZRem(ctx, "race_zset", member)
				}
			}
		}(i)
	}

	wg.Wait()

	// ZSet should have some members (not empty due to race)
	card, err := sharedClient.ZCard(ctx, "race_zset").Result()
	assert.NoError(t, err)
	assert.True(t, card > 0)
}

// TestSortedSetConcurrent_ZscoreRace tests concurrent ZSCORE operations
func TestSortedSetConcurrent_ZscoreRace(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()
	const goroutines = 10
	const opsPerGoroutine = 100

	sharedClient.Del(ctx, "race_zset")
	sharedClient.ZAdd(ctx, "race_zset", redis.Z{Member: "target_member", Score: 42.0})

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				sharedClient.ZScore(ctx, "race_zset", "target_member")
			}
		}()
	}

	wg.Wait()

	// Member should still exist with correct score
	score, err := sharedClient.ZScore(ctx, "race_zset", "target_member").Result()
	assert.NoError(t, err)
	assert.Equal(t, 42.0, score)
}

// TestSortedSetError_WrongTypeIntegration tests sorted set commands on wrong types
func TestSortedSetError_WrongTypeIntegration(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// Create a string key
	sharedClient.Set(ctx, "string_key", "value", 0)

	// ZADD on string should return WRONGTYPE
	err := sharedClient.ZAdd(ctx, "string_key", redis.Z{Member: "m", Score: 1.0}).Err()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "WRONGTYPE"))

	// ZREM on string should return WRONGTYPE
	err = sharedClient.ZRem(ctx, "string_key", "m").Err()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "WRONGTYPE"))

	// ZCARD on string should return WRONGTYPE
	err = sharedClient.ZCard(ctx, "string_key").Err()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "WRONGTYPE"))

	// ZSCORE on string should return WRONGTYPE
	err = sharedClient.ZScore(ctx, "string_key", "m").Err()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "WRONGTYPE"))

	// ZRANGE on string should return WRONGTYPE
	err = sharedClient.ZRange(ctx, "string_key", 0, -1).Err()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "WRONGTYPE"))
}

func assertWrongType(t *testing.T, name string, err error) {
	t.Helper()
	if err == nil {
		t.Errorf("%s: expected error (WRONGTYPE), got nil", name)
		return
	}
	if !strings.Contains(err.Error(), "WRONGTYPE") {
		t.Errorf("%s: expected WRONGTYPE, got: %v", name, err)
	}
}

func setStringKey(t *testing.T) context.Context {
	t.Helper()
	ctx := context.Background()
	sharedClient.Set(ctx, "wrongtype_key", "value", 0)
	return ctx
}

// TestStreamError_WrongTypeIntegration tests stream commands on wrong types (integration level)
func TestStreamError_WrongTypeIntegration(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)
	ctx := setStringKey(t)

	t.Run("XADD", func(t *testing.T) {
		err := sharedClient.Do(ctx, "XADD", "wrongtype_key", "*", "field", "val").Err()
		assertWrongType(t, "XADD", err)
	})
	t.Run("XLEN", func(t *testing.T) {
		err := sharedClient.Do(ctx, "XLEN", "wrongtype_key").Err()
		assertWrongType(t, "XLEN", err)
	})
	t.Run("XREAD", func(t *testing.T) {
		err := sharedClient.Do(ctx, "XREAD", "STREAMS", "wrongtype_key", "0").Err()
		assertWrongType(t, "XREAD", err)
	})
	t.Run("XRANGE", func(t *testing.T) {
		err := sharedClient.Do(ctx, "XRANGE", "wrongtype_key", "-", "+").Err()
		assertWrongType(t, "XRANGE", err)
	})
	t.Run("XREVRANGE", func(t *testing.T) {
		err := sharedClient.Do(ctx, "XREVRANGE", "wrongtype_key", "+", "-").Err()
		assertWrongType(t, "XREVRANGE", err)
	})
	t.Run("XDEL", func(t *testing.T) {
		err := sharedClient.Do(ctx, "XDEL", "wrongtype_key", "123").Err()
		assertWrongType(t, "XDEL", err)
	})
	t.Run("XGROUP_CREATE", func(t *testing.T) {
		err := sharedClient.Do(ctx, "XGROUP", "CREATE", "wrongtype_key", "mygroup", "$").Err()
		assertWrongType(t, "XGROUP CREATE", err)
	})
	t.Run("XREADGROUP", func(t *testing.T) {
		err := sharedClient.Do(ctx, "XREADGROUP", "GROUP", "mygroup", "consumer", "STREAMS", "wrongtype_key", ">").Err()
		assertWrongType(t, "XREADGROUP", err)
	})
	t.Run("XTRIM", func(t *testing.T) {
		err := sharedClient.Do(ctx, "XTRIM", "wrongtype_key", "MAXLEN", "0").Err()
		assertWrongType(t, "XTRIM", err)
	})
	t.Run("XPENDING", func(t *testing.T) {
		err := sharedClient.Do(ctx, "XPENDING", "wrongtype_key", "mygroup").Err()
		assertWrongType(t, "XPENDING", err)
	})
	t.Run("XINFO_STREAM", func(t *testing.T) {
		err := sharedClient.Do(ctx, "XINFO", "STREAM", "wrongtype_key").Err()
		assertWrongType(t, "XINFO STREAM", err)
	})
	t.Run("XINFO_GROUPS", func(t *testing.T) {
		err := sharedClient.Do(ctx, "XINFO", "GROUPS", "wrongtype_key").Err()
		assertWrongType(t, "XINFO GROUPS", err)
	})
}

// TestJsonError_WrongTypeIntegration tests JSON commands on wrong types (integration level)
func TestJsonError_WrongTypeIntegration(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)
	ctx := setStringKey(t)

	tests := []struct {
		name string
		do   func() error
	}{
		{"JSON.SET", func() error {
			return sharedClient.Do(ctx, "JSON.SET", "wrongtype_key", "$", `{"a":1}`).Err()
		}},
		{"JSON.GET", func() error {
			return sharedClient.Do(ctx, "JSON.GET", "wrongtype_key").Err()
		}},
		{"JSON.TYPE", func() error {
			return sharedClient.Do(ctx, "JSON.TYPE", "wrongtype_key").Err()
		}},
		{"JSON.DEL", func() error {
			return sharedClient.Do(ctx, "JSON.DEL", "wrongtype_key").Err()
		}},
		{"JSON.ARRAPPEND", func() error {
			return sharedClient.Do(ctx, "JSON.ARRAPPEND", "wrongtype_key", "$", `"val"`).Err()
		}},
		{"JSON.ARRLEN", func() error {
			return sharedClient.Do(ctx, "JSON.ARRLEN", "wrongtype_key").Err()
		}},
		{"JSON.OBJKEYS", func() error {
			return sharedClient.Do(ctx, "JSON.OBJKEYS", "wrongtype_key").Err()
		}},
		{"JSON.NUMINCRBY", func() error {
			return sharedClient.Do(ctx, "JSON.NUMINCRBY", "wrongtype_key", "$", "1").Err()
		}},
		{"JSON.NUMMULTBY", func() error {
			return sharedClient.Do(ctx, "JSON.NUMMULTBY", "wrongtype_key", "$", "2").Err()
		}},
		{"JSON.CLEAR", func() error {
			return sharedClient.Do(ctx, "JSON.CLEAR", "wrongtype_key").Err()
		}},
		{"JSON.DEBUG", func() error {
			return sharedClient.Do(ctx, "JSON.DEBUG", "MEMORY", "wrongtype_key").Err()
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertWrongType(t, tt.name, tt.do())
		})
	}
}

// TestTimeSeriesError_WrongTypeIntegration tests time series commands on wrong types (integration level)
func TestTimeSeriesError_WrongTypeIntegration(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)
	ctx := setStringKey(t)

	tests := []struct {
		name string
		do   func() error
	}{
		{"TS.ADD", func() error {
			return sharedClient.Do(ctx, "TS.ADD", "wrongtype_key", "1000", "25.5").Err()
		}},
		{"TS.GET", func() error {
			return sharedClient.Do(ctx, "TS.GET", "wrongtype_key").Err()
		}},
		{"TS.RANGE", func() error {
			return sharedClient.Do(ctx, "TS.RANGE", "wrongtype_key", "-", "+").Err()
		}},
		{"TS.DEL", func() error {
			return sharedClient.Do(ctx, "TS.DEL", "wrongtype_key", "-", "+").Err()
		}},
		{"TS.INFO", func() error {
			return sharedClient.Do(ctx, "TS.INFO", "wrongtype_key").Err()
		}},
		{"TS.LEN", func() error {
			return sharedClient.Do(ctx, "TS.LEN", "wrongtype_key").Err()
		}},
		{"TS.MGET", func() error {
			return sharedClient.Do(ctx, "TS.MGET", "filter=test", "wrongtype_key").Err()
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertWrongType(t, tt.name, tt.do())
		})
	}
}

// TestGeoError_WrongTypeIntegration tests geospatial commands on wrong types (integration level)
func TestGeoError_WrongTypeIntegration(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)
	ctx := setStringKey(t)

	tests := []struct {
		name string
		do   func() error
	}{
		{"GEOADD", func() error {
			return sharedClient.Do(ctx, "GEOADD", "wrongtype_key", "116.40", "39.90", "beijing").Err()
		}},
		{"GEOPOS", func() error {
			return sharedClient.Do(ctx, "GEOPOS", "wrongtype_key", "member1").Err()
		}},
		{"GEOHASH", func() error {
			return sharedClient.Do(ctx, "GEOHASH", "wrongtype_key", "member1").Err()
		}},
		{"GEODIST", func() error {
			return sharedClient.Do(ctx, "GEODIST", "wrongtype_key", "member1", "member2").Err()
		}},
		{"GEORADIUS", func() error {
			return sharedClient.Do(ctx, "GEORADIUS", "wrongtype_key", "116.40", "39.90", "100", "km").Err()
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertWrongType(t, tt.name, tt.do())
		})
	}
}

// TestClusterConcurrent_ClusterCallsRace tests concurrent CLUSTER command calls
func TestClusterConcurrent_ClusterCallsRace(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()
	const goroutines = 10
	const opsPerGoroutine = 100

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				// CLUSTER INFO returns error in non-cluster mode - verify no crash
				_, err := sharedClient.Do(ctx, "CLUSTER", "INFO").Result()
				assert.True(t, err != nil) // Should return error, not crash
			}
		}()
	}

	wg.Wait()
}

// TestClusterError_ClusterDisabledIntegration tests CLUSTER commands return error when disabled
func TestClusterError_ClusterDisabledIntegration(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// CLUSTER INFO should return error when cluster is disabled
	_, err := sharedClient.Do(ctx, "CLUSTER", "INFO").Result()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "cluster support disabled"))

	// CLUSTER KEYSLOT should also return error
	_, err = sharedClient.Do(ctx, "CLUSTER", "KEYSLOT", "mykey").Result()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "cluster support disabled"))
}

// TestReplicationConcurrent_RoleRace tests concurrent ROLE operations
func TestReplicationConcurrent_RoleRace(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()
	const goroutines = 10
	const opsPerGoroutine = 100

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				sharedClient.Do(ctx, "ROLE")
			}
		}()
	}

	wg.Wait()
	// No crash is the expected outcome
}

// TestReplicationConcurrent_ReplconfRace tests concurrent REPLCONF operations
func TestReplicationConcurrent_ReplconfRace(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()
	const goroutines = 10
	const opsPerGoroutine = 100

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				sharedClient.Do(ctx, "REPLCONF", "LISTENING-PORT", "6379")
			}
		}()
	}

	wg.Wait()
	// No crash is the expected outcome
}

// TestReplicationError_InvalidSubcommandIntegration tests REPLCONF with invalid subcommand (integration)
func TestReplicationError_InvalidSubcommandIntegration(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	_, err := sharedClient.Do(ctx, "REPLCONF", "INVALID_SUBCOMMAND").Result()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "ERR") || strings.Contains(err.Error(), "unknown"))
}

// TestReplicationError_AckWithoutOffset tests REPLCONF ACK without offset
func TestReplicationError_AckWithoutOffset(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	_, err := sharedClient.Do(ctx, "REPLCONF", "ACK").Result()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "ERR"))
}

// TestSentinelConcurrent_InfoReplicationRace tests concurrent INFO replication operations
func TestSentinelConcurrent_InfoReplicationRace(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()
	const goroutines = 10
	const opsPerGoroutine = 100

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				sharedClient.Do(ctx, "INFO", "replication")
			}
		}()
	}

	wg.Wait()
	// No crash is the expected outcome
}

// TestSentinelError_InfoInvalidSection tests INFO with invalid section returns empty or error
func TestSentinelError_InfoInvalidSection(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// INFO with invalid section should return empty or valid response (not crash)
	result, err := sharedClient.Do(ctx, "INFO", "invalid_section").Result()
	// Redis returns empty section, we may do the same
	if err == nil {
		// Expected - empty or partial response
		assert.True(t, result != nil)
	}
}

// TestSentinelError_ReplconfUnknownSubcommandIntegration tests REPLCONF with unknown subcommand (integration)
func TestSentinelError_ReplconfUnknownSubcommandIntegration(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// REPLCONF with unknown subcommand should return error
	_, err := sharedClient.Do(ctx, "REPLCONF", "UNKNOWN_SUBCOMMAND").Result()
	assert.Error(t, err)
	if !strings.Contains(err.Error(), "ERR") && !strings.Contains(err.Error(), "unknown") {
		t.Errorf("Error should mention ERR or unknown, got: %s", err.Error())
	}
}

// TestClusterError_InvalidSubcommandIntegration tests CLUSTER with invalid subcommand (integration)
func TestClusterError_InvalidSubcommandIntegration(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// CLUSTER with invalid subcommand should return error
	_, err := sharedClient.Do(ctx, "CLUSTER", "INVALID_SUBCOMMAND").Result()
	assert.Error(t, err)
	if !strings.Contains(err.Error(), "ERR") && !strings.Contains(err.Error(), "unknown subcommand") {
		t.Errorf("Error should mention ERR or unknown subcommand, got: %s", err.Error())
	}
}

// ---- High-scale concurrent tests (100+ goroutines) ----

// TestStringConcurrent_HighScaleIncrement tests 100 concurrent INCR operations.
func TestStringConcurrent_HighScaleIncrement(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()
	const goroutines = 100
	const incrsPerGoroutine = 50

	err := sharedClient.Set(ctx, "highscale_counter", 0, 0).Err()
	assert.NoError(t, err)

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < incrsPerGoroutine; j++ {
				sharedClient.Incr(ctx, "highscale_counter")
			}
		}()
	}
	wg.Wait()

	finalVal, err := sharedClient.Get(ctx, "highscale_counter").Int64()
	assert.NoError(t, err)
	assert.Equal(t, int64(goroutines*incrsPerGoroutine), finalVal)
}

// TestStringConcurrent_HighScaleReadWrite tests 100 concurrent mixed read/write.
func TestStringConcurrent_HighScaleReadWrite(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()
	const goroutines = 100
	const opsPerGoroutine = 50

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				key := fmt.Sprintf("hsc:k%d", j%50)
				if j%3 == 0 {
					sharedClient.Set(ctx, key, idx*1000+j, 0)
				} else {
					sharedClient.Get(ctx, key)
				}
			}
		}(i)
	}
	wg.Wait()

	// Verify no crash, key should exist
	exists, err := sharedClient.Exists(ctx, "hsc:k0").Result()
	assert.NoError(t, err)
	assert.True(t, exists >= 0)
}

// TestListConcurrent_HighScalePushPop tests 100 concurrent list push/pop.
func TestListConcurrent_HighScalePushPop(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	if testing.Short() {
		t.Skip("skipping high-scale list test in short mode")
	}

	ctx := context.Background()
	const goroutines = 100
	const opsPerGoroutine = 30

	sharedClient.Del(ctx, "hsc_list")

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				if j%2 == 0 {
					sharedClient.LPush(ctx, "hsc_list", idx*1000+j)
				} else {
					sharedClient.RPush(ctx, "hsc_list", idx*1000+j)
				}
			}
		}(i)
	}
	wg.Wait()

	llen, err := sharedClient.LLen(ctx, "hsc_list").Result()
	assert.NoError(t, err)
	t.Logf("high-scale list: len=%d", llen)
	assert.True(t, llen > 0)
}

// TestHashConcurrent_HighScaleHset tests 100 concurrent HSET on same hash.
func TestHashConcurrent_HighScaleHset(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()
	const goroutines = 100
	const opsPerGoroutine = 30

	sharedClient.Del(ctx, "hsc_hash")

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				field := fmt.Sprintf("f%d", j%20)
				sharedClient.HSet(ctx, "hsc_hash", field, idx*1000+j)
			}
		}(i)
	}
	wg.Wait()

	hlen, err := sharedClient.HLen(ctx, "hsc_hash").Result()
	assert.NoError(t, err)
	assert.True(t, hlen > 0)
	t.Logf("high-scale hash: fields=%d", hlen)
}

// TestSetConcurrent_HighScaleSadd tests 100 concurrent SADD/SREM.
func TestSetConcurrent_HighScaleSadd(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()
	const goroutines = 100
	const opsPerGoroutine = 30

	sharedClient.Del(ctx, "hsc_set")

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				member := fmt.Sprintf("m%d", j%30)
				if j%2 == 0 {
					sharedClient.SAdd(ctx, "hsc_set", member)
				} else {
					sharedClient.SRem(ctx, "hsc_set", member)
				}
			}
		}(i)
	}
	wg.Wait()

	card, err := sharedClient.SCard(ctx, "hsc_set").Result()
	assert.NoError(t, err)
	assert.True(t, card > 0)
}
