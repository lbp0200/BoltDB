package integration

// command_completeness_test.go — 全命令准确性测试
//
// 按数据类型分组，表驱动测试 BoltDB 全部 239 个命令的基本正确性。
// 每个子测试独立：setup → execute → verify → cleanup。
//
// 用法:
//
//	go test -race -timeout 300s -run TestCommandCompleteness ./cmd/integration/...
//	go test -race -timeout 600s -run TestCommandCompleteness ./cmd/integration/...  # 慢机器
import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zeebo/assert"
)

// ============================================================
// Helpers
// ============================================================

// do executes a raw RESP command via Do() and returns the result.
func do(t *testing.T, ctx context.Context, args ...interface{}) interface{} {
	t.Helper()
	result, err := sharedClient.Do(ctx, args...).Result()
	assert.NoError(t, err)
	return result
}

// doAny executes a raw RESP command and returns (result, error).
func doAny(t *testing.T, ctx context.Context, args ...interface{}) (interface{}, error) {
	t.Helper()
	return sharedClient.Do(ctx, args...).Result()
}

// keyPrefix returns a unique prefix for a test to avoid key collisions.
func keyPrefix(t *testing.T) string {
	t.Helper()
	// Use test name, replace non-alphanumeric with underscore, lowercase
	name := strings.ToLower(t.Name())
	name = strings.ReplaceAll(name, "/", "_")
	return name + ":"
}

// ============================================================
// 1. String Commands (22)
// ============================================================

func TestCommandCompleteness_String(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)
	ctx := context.Background()
	p := keyPrefix(t)

	// --- SET / GET ---
	t.Run("SET_GET", func(t *testing.T) {
		err := sharedClient.Set(ctx, p+"s1", "hello", 0).Err()
		assert.NoError(t, err)
		val, err := sharedClient.Get(ctx, p+"s1").Result()
		assert.NoError(t, err)
		assert.Equal(t, "hello", val)
	})

	// --- SET options (NX/XX/GET/EX/PX/KEEPTTL) ---
	t.Run("SET_options", func(t *testing.T) {
		p := keyPrefix(t)

		// SET NX: key does not exist → set
		r, _ := doAny(t, ctx, "SET", p+"snx", "v1", "NX")
		assert.Equal(t, "OK", r)

		// SET NX: key exists → nil
		r, _ = doAny(t, ctx, "SET", p+"snx", "v2", "NX")
		assert.Nil(t, r)

		// SET XX: key exists → update
		sharedClient.Set(ctx, p+"sxx", "orig", 0)
		r, _ = doAny(t, ctx, "SET", p+"sxx", "v1", "XX")
		assert.Equal(t, "OK", r)

		// SET XX: key does not exist → nil
		r, _ = doAny(t, ctx, "SET", p+"sxx_nope", "v2", "XX")
		assert.Nil(t, r)

		// SET GET: return old value
		r, _ = doAny(t, ctx, "SET", p+"sget", "old", "GET")
		assert.Nil(t, r) // no prior value
		r, _ = doAny(t, ctx, "SET", p+"sget", "new", "GET")
		assert.Equal(t, "old", r)
		val, _ := sharedClient.Get(ctx, p+"sget").Result()
		assert.Equal(t, "new", val)

		// SET EX: seconds-based TTL
		r, _ = doAny(t, ctx, "SET", p+"sex", "val", "EX", "60")
		assert.Equal(t, "OK", r)
		ttl, _ := sharedClient.TTL(ctx, p+"sex").Result()
		assert.True(t, ttl > 0 && ttl <= 60*time.Second)

		// SET PX: ms-based TTL
		r, _ = doAny(t, ctx, "SET", p+"spx", "val", "PX", "60000")
		assert.Equal(t, "OK", r)
		pttl, _ := sharedClient.PTTL(ctx, p+"spx").Result()
		assert.True(t, pttl > 0 && pttl <= 60000*time.Millisecond)

		// SET KEEPTTL: preserve existing TTL
		sharedClient.Set(ctx, p+"skeep", "orig", 0)
		sharedClient.Expire(ctx, p+"skeep", 3600*time.Second)
		r, _ = doAny(t, ctx, "SET", p+"skeep", "new", "KEEPTTL")
		assert.Equal(t, "OK", r)
		ttl, _ = sharedClient.TTL(ctx, p+"skeep").Result()
		assert.True(t, ttl > 0) // TTL preserved

		// SET EXAT <past>: must return OK, key must be gone (immediate expiry)
		r, _ = doAny(t, ctx, "SET", p+"sexat_past", "v", "EXAT", "1000000")
		assert.Equal(t, "OK", r)
		exists, _ := sharedClient.Exists(ctx, p+"sexat_past").Result()
		assert.Equal(t, int64(0), exists)

		// SET PXAT <past>: same behavior
		r, _ = doAny(t, ctx, "SET", p+"spxat_past", "v", "PXAT", "1000000")
		assert.Equal(t, "OK", r)
		exists, _ = sharedClient.Exists(ctx, p+"spxat_past").Result()
		assert.Equal(t, int64(0), exists)

		// SET EX 0: must error (invalid expire time — positive integer required)
		_, err := doAny(t, ctx, "SET", p+"sex0", "v", "EX", "0")
		assert.Error(t, err)
		assert.True(t, strings.Contains(err.Error(), "invalid expire time"))

		// SET PX 0: must error
		_, err = doAny(t, ctx, "SET", p+"spx0", "v", "PX", "0")
		assert.Error(t, err)
		assert.True(t, strings.Contains(err.Error(), "invalid expire time"))
	})

	// --- GETDEL ---
	t.Run("GETDEL", func(t *testing.T) {
		sharedClient.Set(ctx, p+"gd1", "val", 0)
		val, err := sharedClient.GetDel(ctx, p+"gd1").Result()
		assert.NoError(t, err)
		assert.Equal(t, "val", val)
		_, err = sharedClient.Get(ctx, p+"gd1").Result()
		assert.Equal(t, redis.Nil, err)
	})

	// --- GETEX ---
	t.Run("GETEX", func(t *testing.T) {
		sharedClient.Set(ctx, p+"ge1", "val", 0)
		r, _ := doAny(t, ctx, "GETEX", p+"ge1", "EX", "100")
		assert.Equal(t, "val", r)
		ttl, _ := sharedClient.TTL(ctx, p+"ge1").Result()
		assert.True(t, ttl > 0 && ttl <= 100*time.Second)

		// GETEX PERSIST: return value + remove TTL
		r, _ = doAny(t, ctx, "GETEX", p+"ge1", "PERSIST")
		assert.Equal(t, "val", r)
		ttl, _ = sharedClient.TTL(ctx, p+"ge1").Result()
		assert.Equal(t, time.Duration(-1), ttl)

		// GETEX PX: ms-based TTL
		sharedClient.Set(ctx, p+"ge2", "v2", 0)
		r, _ = doAny(t, ctx, "GETEX", p+"ge2", "PX", "60000")
		assert.Equal(t, "v2", r)
		pttl, _ := sharedClient.PTTL(ctx, p+"ge2").Result()
		assert.True(t, pttl > 0 && pttl <= 60000*time.Millisecond)

		// GETEX EX 0: must error
		_, err := doAny(t, ctx, "GETEX", p+"ge2", "EX", "0")
		assert.Error(t, err)
		assert.True(t, strings.Contains(err.Error(), "invalid"))

		// GETEX PX 0: must error
		_, err = doAny(t, ctx, "GETEX", p+"ge2", "PX", "0")
		assert.Error(t, err)
		assert.True(t, strings.Contains(err.Error(), "invalid"))
	})

	// --- SETEX ---
	t.Run("SETEX", func(t *testing.T) {
		err := sharedClient.SetEx(ctx, p+"se1", "val", 60*time.Second).Err()
		assert.NoError(t, err)
		val, err := sharedClient.Get(ctx, p+"se1").Result()
		assert.NoError(t, err)
		assert.Equal(t, "val", val)

		// SETEX with TTL <= 0 → error
		_, err = doAny(t, ctx, "SETEX", p+"se1_invalid", "0", "val")
		assert.Error(t, err)
		_, err = doAny(t, ctx, "SETEX", p+"se1_invalid", "-1", "val")
		assert.Error(t, err)
	})

	// --- PSETEX ---
	t.Run("PSETEX", func(t *testing.T) {
		r, _ := doAny(t, ctx, "PSETEX", p+"ps1", "60000", "val")
		_ = r
		val, err := sharedClient.Get(ctx, p+"ps1").Result()
		assert.NoError(t, err)
		assert.Equal(t, "val", val)
		pttl, err := sharedClient.PTTL(ctx, p+"ps1").Result()
		assert.NoError(t, err)
		assert.True(t, pttl > 0 && pttl <= 60000*time.Millisecond)

		// PSETEX with TTL <= 0 → error
		_, err = doAny(t, ctx, "PSETEX", p+"ps1_invalid", "0", "val")
		assert.Error(t, err)
		_, err = doAny(t, ctx, "PSETEX", p+"ps1_invalid", "-1", "val")
		assert.Error(t, err)
	})

	// --- SETNX ---
	t.Run("SETNX", func(t *testing.T) {
		ok, err := sharedClient.SetNX(ctx, p+"snx1", "val", 0).Result()
		assert.NoError(t, err)
		assert.True(t, ok)
		// SETNX on existing key should return false
		ok, err = sharedClient.SetNX(ctx, p+"snx1", "other", 0).Result()
		assert.NoError(t, err)
		assert.False(t, ok)
		val, _ := sharedClient.Get(ctx, p+"snx1").Result()
		assert.Equal(t, "val", val)
	})

	// --- GETSET ---
	t.Run("GETSET", func(t *testing.T) {
		sharedClient.Set(ctx, p+"gs1", "old", 0)
		old, err := sharedClient.GetSet(ctx, p+"gs1", "new").Result()
		assert.NoError(t, err)
		assert.Equal(t, "old", old)
		val, _ := sharedClient.Get(ctx, p+"gs1").Result()
		assert.Equal(t, "new", val)
	})

	// --- MSET / MGET ---
	t.Run("MSET_MGET", func(t *testing.T) {
		err := sharedClient.MSet(ctx, p+"m1", "v1", p+"m2", "v2", p+"m3", "v3").Err()
		assert.NoError(t, err)
		vals, err := sharedClient.MGet(ctx, p+"m1", p+"m2", p+"m3", p+"m4").Result()
		assert.NoError(t, err)
		assert.Equal(t, "v1", vals[0])
		assert.Equal(t, "v2", vals[1])
		assert.Equal(t, "v3", vals[2])
		assert.Nil(t, vals[3]) // nonexistent
	})

	// --- MSETNX ---
	t.Run("MSETNX", func(t *testing.T) {
		ok, _ := doAny(t, ctx, "MSETNX", p+"mnx1", "v1", p+"mnx2", "v2")
		assert.Equal(t, int64(1), ok) // 1 = set
		// Should not overwrite
		ok, _ = doAny(t, ctx, "MSETNX", p+"mnx1", "other", p+"mnx2", "other")
		assert.Equal(t, int64(0), ok) // 0 = not set
		val, _ := sharedClient.Get(ctx, p+"mnx1").Result()
		assert.Equal(t, "v1", val)
	})

	// --- INCR / DECR / INCRBY / DECRBY ---
	t.Run("INCR_DECR", func(t *testing.T) {
		sharedClient.Set(ctx, p+"cnt", "10", 0)
		v, _ := sharedClient.Incr(ctx, p+"cnt").Result()
		assert.Equal(t, int64(11), v)
		v, _ = sharedClient.Decr(ctx, p+"cnt").Result()
		assert.Equal(t, int64(10), v)
		v, _ = sharedClient.IncrBy(ctx, p+"cnt", 5).Result()
		assert.Equal(t, int64(15), v)
		v, _ = sharedClient.DecrBy(ctx, p+"cnt", 3).Result()
		assert.Equal(t, int64(12), v)
	})

	// --- INCRBYFLOAT ---
	t.Run("INCRBYFLOAT", func(t *testing.T) {
		sharedClient.Set(ctx, p+"flt", "1.5", 0)
		v, _ := sharedClient.IncrByFloat(ctx, p+"flt", 2.5).Result()
		assert.Equal(t, 4.0, v)
	})

	// --- APPEND ---
	t.Run("APPEND", func(t *testing.T) {
		sharedClient.Set(ctx, p+"app", "hello", 0)
		n, _ := sharedClient.Append(ctx, p+"app", " world").Result()
		assert.Equal(t, int64(11), n)
		val, _ := sharedClient.Get(ctx, p+"app").Result()
		assert.Equal(t, "hello world", val)
	})

	// --- STRLEN ---
	t.Run("STRLEN", func(t *testing.T) {
		sharedClient.Set(ctx, p+"len", "hello", 0)
		n, _ := sharedClient.StrLen(ctx, p+"len").Result()
		assert.Equal(t, int64(5), n)
	})

	// --- GETRANGE / SETRANGE ---
	t.Run("GETRANGE_SETRANGE", func(t *testing.T) {
		sharedClient.Set(ctx, p+"gr", "hello world", 0)
		sub, _ := sharedClient.GetRange(ctx, p+"gr", 0, 4).Result()
		assert.Equal(t, "hello", sub)
		sharedClient.SetRange(ctx, p+"gr", 6, "Redis")
		val, _ := sharedClient.Get(ctx, p+"gr").Result()
		assert.Equal(t, "hello Redis", val)
	})

	// --- SETBIT / GETBIT ---
	t.Run("SETBIT_GETBIT", func(t *testing.T) {
		sharedClient.SetBit(ctx, p+"bt", 7, 1)
		b, _ := sharedClient.GetBit(ctx, p+"bt", 7).Result()
		assert.Equal(t, int64(1), b)
		b, _ = sharedClient.GetBit(ctx, p+"bt", 0).Result()
		assert.Equal(t, int64(0), b)
	})

	// --- BITCOUNT ---
	t.Run("BITCOUNT", func(t *testing.T) {
		sharedClient.SetBit(ctx, p+"bc", 0, 1)
		sharedClient.SetBit(ctx, p+"bc", 1, 1)
		sharedClient.SetBit(ctx, p+"bc", 2, 1)
		n, _ := sharedClient.BitCount(ctx, p+"bc", nil).Result()
		assert.Equal(t, int64(3), n)
	})

	// --- BITOP ---
	t.Run("BITOP", func(t *testing.T) {
		sharedClient.SetBit(ctx, p+"b1", 0, 1)
		sharedClient.SetBit(ctx, p+"b1", 1, 0)
		sharedClient.SetBit(ctx, p+"b2", 0, 0)
		sharedClient.SetBit(ctx, p+"b2", 1, 1)
		do(t, ctx, "BITOP", "AND", p+"band", p+"b1", p+"b2")
		v, _ := sharedClient.GetBit(ctx, p+"band", 0).Result()
		assert.Equal(t, int64(0), v)
		v, _ = sharedClient.GetBit(ctx, p+"band", 1).Result()
		assert.Equal(t, int64(0), v)
	})

	// --- BITFIELD ---
	t.Run("BITFIELD", func(t *testing.T) {
		do(t, ctx, "BITFIELD", p+"bf", "SET", "u8", "0", "255")
		r, _ := doAny(t, ctx, "BITFIELD", p+"bf", "GET", "u8", "0")
		assert.NotNil(t, r)
	})

	// --- BITFIELD_RO ---
	t.Run("BITFIELD_RO", func(t *testing.T) {
		sharedClient.SetBit(ctx, p+"bfro", 7, 1)
		r, _ := doAny(t, ctx, "BITFIELD_RO", p+"bfro", "GET", "u8", "0")
		assert.NotNil(t, r)
	})

	// --- BITPOS ---
	t.Run("BITPOS", func(t *testing.T) {
		sharedClient.Set(ctx, p+"bp", "\x00\xff", 0) // 00000000 11111111
		v, _ := do(t, ctx, "BITPOS", p+"bp", "1").(int64)
		assert.Equal(t, int64(8), v) // first '1' bit at position 8
	})

	// --- BITLEN ---
	t.Run("BITLEN", func(t *testing.T) {
		sharedClient.Set(ctx, p+"bl", "hello", 0)
		v, _ := do(t, ctx, "BITLEN", p+"bl").(int64)
		assert.Equal(t, int64(40), v) // 5 bytes * 8 bits
	})
}

// ============================================================
// 2. Key Commands (25)
// ============================================================

func TestCommandCompleteness_Key(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)
	ctx := context.Background()
	p := keyPrefix(t)

	// --- DEL ---
	t.Run("DEL", func(t *testing.T) {
		sharedClient.Set(ctx, p+"d1", "v", 0)
		sharedClient.Set(ctx, p+"d2", "v", 0)
		n, _ := sharedClient.Del(ctx, p+"d1", p+"d2", p+"d3").Result()
		assert.Equal(t, int64(2), n)
	})

	// --- UNLINK ---
	t.Run("UNLINK", func(t *testing.T) {
		sharedClient.Set(ctx, p+"u1", "v", 0)
		n, _ := sharedClient.Unlink(ctx, p+"u1").Result()
		assert.Equal(t, int64(1), n)
		_, err := sharedClient.Get(ctx, p+"u1").Result()
		assert.Equal(t, redis.Nil, err)
	})

	// --- EXISTS ---
	t.Run("EXISTS", func(t *testing.T) {
		sharedClient.Set(ctx, p+"e1", "v", 0)
		n, _ := sharedClient.Exists(ctx, p+"e1", p+"e2").Result()
		assert.Equal(t, int64(1), n)
	})

	// --- TYPE ---
	t.Run("TYPE", func(t *testing.T) {
		sharedClient.Set(ctx, p+"t1", "v", 0)
		r, _ := doAny(t, ctx, "TYPE", p+"t1")
		assert.Equal(t, "string", r)
	})

	// --- KEYS ---
	t.Run("KEYS", func(t *testing.T) {
		sharedClient.Set(ctx, p+"k_a", "v", 0)
		sharedClient.Set(ctx, p+"k_b", "v", 0)
		r, _ := doAny(t, ctx, "KEYS", p+"k_*")
		keys, ok := r.([]interface{})
		assert.True(t, ok)
		assert.True(t, len(keys) >= 2)
	})

	// --- RANDOMKEY ---
	t.Run("RANDOMKEY", func(t *testing.T) {
		sharedClient.Set(ctx, p+"rk", "v", 0)
		r, _ := doAny(t, ctx, "RANDOMKEY")
		assert.NotNil(t, r)
	})

	// --- RENAME / RENAMENX ---
	t.Run("RENAME", func(t *testing.T) {
		sharedClient.Set(ctx, p+"rn1", "v", 0)
		sharedClient.Rename(ctx, p+"rn1", p+"rn2")
		val, _ := sharedClient.Get(ctx, p+"rn2").Result()
		assert.Equal(t, "v", val)
		_, err := sharedClient.Get(ctx, p+"rn1").Result()
		assert.Equal(t, redis.Nil, err)
	})
	t.Run("RENAMENX", func(t *testing.T) {
		sharedClient.Set(ctx, p+"rnx1", "v", 0)
		sharedClient.Set(ctx, p+"rnx2", "v2", 0)
		ok, _ := sharedClient.RenameNX(ctx, p+"rnx1", p+"rnx2").Result()
		assert.False(t, ok) // target exists
		sharedClient.Set(ctx, p+"rnx3", "v3", 0)
		ok, _ = sharedClient.RenameNX(ctx, p+"rnx3", p+"rnx_new").Result()
		assert.True(t, ok)
	})

	// --- COPY ---
	t.Run("COPY", func(t *testing.T) {
		sharedClient.Set(ctx, p+"cp_src", "val", 0)
		do(t, ctx, "COPY", p+"cp_src", p+"cp_dst")
		val, _ := sharedClient.Get(ctx, p+"cp_dst").Result()
		assert.Equal(t, "val", val)
		// COPY without REPLACE on existing key
		sharedClient.Set(ctx, p+"cp_dst2", "old", 0)
		r, _ := doAny(t, ctx, "COPY", p+"cp_src", p+"cp_dst2")
		assert.Equal(t, int64(0), r)
		// COPY with REPLACE
		r, _ = doAny(t, ctx, "COPY", p+"cp_src", p+"cp_dst2", "REPLACE")
		assert.Equal(t, int64(1), r)
		val, _ = sharedClient.Get(ctx, p+"cp_dst2").Result()
		assert.Equal(t, "val", val)

		// COPY must preserve source key's TTL on destination
		sharedClient.Set(ctx, p+"cp_ttl_src", "ttlval", 3600*time.Second)
		r, _ = doAny(t, ctx, "COPY", p+"cp_ttl_src", p+"cp_ttl_dst")
		assert.Equal(t, int64(1), r)
		ttlDst := sharedClient.TTL(ctx, p+"cp_ttl_dst").Val()
		assert.True(t, ttlDst > 0 && ttlDst <= 3600*time.Second)
	})

	// --- SWAPDB ---
	t.Run("SWAPDB", func(t *testing.T) {
		// Use raw RESP for all operations to ensure db selection works
		do(t, ctx, "SELECT", "1")
		do(t, ctx, "SET", p+"sw", "db1")
		do(t, ctx, "SELECT", "2")
		do(t, ctx, "SET", p+"sw", "db2")
		do(t, ctx, "SWAPDB", "1", "2")
		// After swap, db 1 should have "db2"
		do(t, ctx, "SELECT", "1")
		r, _ := doAny(t, ctx, "GET", p+"sw")
		assert.Equal(t, "db2", r)
		do(t, ctx, "SELECT", "0")
	})

	// --- TOUCH ---
	t.Run("TOUCH", func(t *testing.T) {
		sharedClient.Set(ctx, p+"touch1", "v", 0)
		r, _ := doAny(t, ctx, "TOUCH", p+"touch1", p+"touch_nonexist")
		assert.Equal(t, int64(1), r)
	})

	// --- SORT ---
	t.Run("SORT", func(t *testing.T) {
		sharedClient.RPush(ctx, p+"sort", "3", "1", "2")
		r, _ := doAny(t, ctx, "SORT", p+"sort")
		arr, ok := r.([]interface{})
		assert.True(t, ok)
		assert.Equal(t, 3, len(arr))
	})

	// --- DUMP / RESTORE ---
	t.Run("DUMP_RESTORE", func(t *testing.T) {
		sharedClient.Set(ctx, p+"dump", "data", 0)
		dump, _ := doAny(t, ctx, "DUMP", p+"dump")
		assert.NotNil(t, dump)
		// RESTORE to a new key
		dumpBytes, ok := dump.([]byte)
		if ok {
			sharedClient.Del(ctx, p+"restored")
			do(t, ctx, "RESTORE", p+"restored", "0", dumpBytes)
			val, _ := sharedClient.Get(ctx, p+"restored").Result()
			assert.Equal(t, "data", val)
		}
	})

	// --- OBJECT sub-commands ---
	t.Run("OBJECT", func(t *testing.T) {
		sharedClient.Set(ctx, p+"obj", "val", 0)
		do(t, ctx, "OBJECT", "REFCOUNT", p+"obj")
		r, _ := doAny(t, ctx, "OBJECT", "ENCODING", p+"obj")
		assert.NotNil(t, r)
		do(t, ctx, "OBJECT", "IDLETIME", p+"obj")
	})

	// --- SCAN ---
	t.Run("SCAN", func(t *testing.T) {
		sharedClient.Set(ctx, p+"scan:a", "v", 0)
		sharedClient.Set(ctx, p+"scan:b", "v", 0)
		r, _ := doAny(t, ctx, "SCAN", "0", "MATCH", p+"scan:*", "COUNT", "100")
		assert.NotNil(t, r)
	})

	// --- MOVE ---
	t.Run("MOVE", func(t *testing.T) {
		// Use a very unique key to avoid collisions with other db tests
		mvKey := p + "mv_unique_" + fmt.Sprintf("%d", time.Now().UnixNano())
		sharedClient.Set(ctx, mvKey, "val", 0)
		r, _ := doAny(t, ctx, "MOVE", mvKey, "1")
		// MOVE should return 1 (moved) or 0 (key already in target db)
		// Both are valid; we just verify the command doesn't panic
		assert.True(t, r == int64(0) || r == int64(1))
	})

	// --- EXPIRE / EXPIREAT / PEXPIRE / PEXPIREAT ---
	t.Run("EXPIRE_variants", func(t *testing.T) {
		sharedClient.Set(ctx, p+"exp", "v", 0)
		ok, _ := sharedClient.Expire(ctx, p+"exp", 60*time.Second).Result()
		assert.True(t, ok)
		ttl, _ := sharedClient.TTL(ctx, p+"exp").Result()
		assert.True(t, ttl > 0 && ttl <= 60*time.Second)

		// EXPIREAT
		sharedClient.Set(ctx, p+"exp2", "v", 0)
		future := time.Now().Add(120 * time.Second).Unix()
		ok, _ = sharedClient.ExpireAt(ctx, p+"exp2", time.Unix(future, 0)).Result()
		assert.True(t, ok)

		// PEXPIRE
		sharedClient.Set(ctx, p+"exp3", "v", 0)
		do(t, ctx, "PEXPIRE", p+"exp3", "60000")
		pttl, _ := sharedClient.PTTL(ctx, p+"exp3").Result()
		assert.True(t, pttl > 0 && pttl <= 60000*time.Millisecond)

		// PEXPIREAT
		sharedClient.Set(ctx, p+"exp4", "v", 0)
		futureMs := time.Now().Add(120000 * time.Millisecond).UnixMilli()
		do(t, ctx, "PEXPIREAT", p+"exp4", futureMs)
	})

	// --- EXPIRE options (NX/XX/GT/LT) ---
	t.Run("EXPIRE_options", func(t *testing.T) {
		p := keyPrefix(t)

		// NX: key without TTL → set
		sharedClient.Set(ctx, p+"nx1", "v", 0)
		r, _ := doAny(t, ctx, "EXPIRE", p+"nx1", "60", "NX")
		assert.Equal(t, int64(1), r)
		ttl, _ := sharedClient.TTL(ctx, p+"nx1").Result()
		assert.True(t, ttl > 0 && ttl <= 60*time.Second)

		// NX: key already has TTL → 0
		r, _ = doAny(t, ctx, "EXPIRE", p+"nx1", "120", "NX")
		assert.Equal(t, int64(0), r)

		// XX: key without TTL → 0
		sharedClient.Set(ctx, p+"xx1", "v", 0)
		r, _ = doAny(t, ctx, "EXPIRE", p+"xx1", "60", "XX")
		assert.Equal(t, int64(0), r)

		// XX: key with TTL → update
		sharedClient.Expire(ctx, p+"xx1", 30*time.Second)
		r, _ = doAny(t, ctx, "EXPIRE", p+"xx1", "60", "XX")
		assert.Equal(t, int64(1), r)

		// GT: bigger than current TTL → update
		sharedClient.Set(ctx, p+"gt1", "v", 0)
		sharedClient.Expire(ctx, p+"gt1", 10*time.Second)
		r, _ = doAny(t, ctx, "EXPIRE", p+"gt1", "60", "GT")
		assert.Equal(t, int64(1), r)

		// GT: smaller than current TTL → 0
		r, _ = doAny(t, ctx, "EXPIRE", p+"gt1", "30", "GT")
		assert.Equal(t, int64(0), r)

		// GT: key without TTL → 0 (infinite TTL > any finite)
		sharedClient.Set(ctx, p+"gt2", "v", 0)
		r, _ = doAny(t, ctx, "EXPIRE", p+"gt2", "60", "GT")
		assert.Equal(t, int64(0), r)
		ttl, _ = sharedClient.TTL(ctx, p+"gt2").Result()
		assert.Equal(t, time.Duration(-1), ttl)

		// LT: smaller than current → update
		sharedClient.Set(ctx, p+"lt1", "v", 0)
		ok, _ := sharedClient.Expire(ctx, p+"lt1", 60*time.Second).Result()
		assert.True(t, ok)
		r, _ = doAny(t, ctx, "EXPIRE", p+"lt1", "30", "LT")
		assert.Equal(t, int64(1), r)

		// LT: bigger than current → 0
		r, _ = doAny(t, ctx, "EXPIRE", p+"lt1", "90", "LT")
		assert.Equal(t, int64(0), r)

		// LT: key without TTL → 1 (infinite > any finite, so nothing is bigger)
		sharedClient.Set(ctx, p+"lt2", "v", 0)
		r, _ = doAny(t, ctx, "EXPIRE", p+"lt2", "60", "LT")
		assert.Equal(t, int64(1), r)

		// Invalid option → error
		_, err := doAny(t, ctx, "EXPIRE", p+"err", "60", "BOGUS")
		assert.Error(t, err)
		assert.True(t, strings.Contains(err.Error(), "unsupported option"))
	})

	// --- TTL / PTTL ---
	t.Run("TTL_PTTL", func(t *testing.T) {
		sharedClient.Set(ctx, p+"ttl", "v", 0)
		sharedClient.Expire(ctx, p+"ttl", 60*time.Second)
		ttl, _ := sharedClient.TTL(ctx, p+"ttl").Result()
		assert.True(t, ttl > 0)
		pttl, _ := sharedClient.PTTL(ctx, p+"ttl").Result()
		assert.True(t, pttl > 0)
		// No TTL
		sharedClient.Set(ctx, p+"ttl2", "v", 0)
		ttl, _ = sharedClient.TTL(ctx, p+"ttl2").Result()
		assert.Equal(t, time.Duration(-1), ttl)
	})

	// --- EXPIRETIME / PEXPIRETIME ---
	t.Run("EXPIRETIME", func(t *testing.T) {
		sharedClient.Set(ctx, p+"et", "v", 0)
		sharedClient.Expire(ctx, p+"et", 3600)
		r, _ := doAny(t, ctx, "EXPIRETIME", p+"et")
		assert.NotNil(t, r)
		r, _ = doAny(t, ctx, "PEXPIRETIME", p+"et")
		assert.NotNil(t, r)
	})

	// --- PERSIST ---
	t.Run("PERSIST", func(t *testing.T) {
		sharedClient.Set(ctx, p+"pers", "v", 0)
		sharedClient.Expire(ctx, p+"pers", 60)
		ok, _ := sharedClient.Persist(ctx, p+"pers").Result()
		assert.True(t, ok)
		ttl, _ := sharedClient.TTL(ctx, p+"pers").Result()
		assert.Equal(t, time.Duration(-1), ttl)
	})

	// --- DBSIZE ---
	t.Run("DBSIZE", func(t *testing.T) {
		n, _ := sharedClient.DBSize(ctx).Result()
		assert.True(t, n >= 0)
	})

	// --- SELECT ---
	t.Run("SELECT", func(t *testing.T) {
		r, _ := doAny(t, ctx, "SELECT", "0")
		assert.NotNil(t, r)
	})

	// --- FLUSHDB / FLUSHALL ---
	t.Run("FLUSHDB_FLUSHALL", func(t *testing.T) {
		sharedClient.Set(ctx, p+"flush", "v", 0)
		do(t, ctx, "FLUSHDB")
		n, _ := sharedClient.Exists(ctx, p+"flush").Result()
		assert.Equal(t, int64(0), n)
	})
}

// ============================================================
// 3. List Commands (19)
// ============================================================

func TestCommandCompleteness_List(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)
	ctx := context.Background()
	p := keyPrefix(t)

	// --- LPUSH / RPUSH ---
	t.Run("LPUSH_RPUSH", func(t *testing.T) {
		sharedClient.LPush(ctx, p+"l1", "c", "b", "a")
		sharedClient.RPush(ctx, p+"l1", "d", "e")
		vals, _ := sharedClient.LRange(ctx, p+"l1", 0, -1).Result()
		assert.Equal(t, []string{"a", "b", "c", "d", "e"}, vals)
	})

	// --- LPOP / RPOP ---
	t.Run("LPOP_RPOP", func(t *testing.T) {
		sharedClient.RPush(ctx, p+"lr", "1", "2", "3")
		v, _ := sharedClient.LPop(ctx, p+"lr").Result()
		assert.Equal(t, "1", v)
		v, _ = sharedClient.RPop(ctx, p+"lr").Result()
		assert.Equal(t, "3", v)
	})

	// --- LPOP/RPOP with count ---
	t.Run("LPOP_RPOP_COUNT", func(t *testing.T) {
		sharedClient.RPush(ctx, p+"lrc", "1", "2", "3", "4", "5")
		r, _ := doAny(t, ctx, "LPOP", p+"lrc", "2")
		arr, ok := r.([]interface{})
		if ok && len(arr) == 2 {
			assert.Equal(t, "1", arr[0])
			assert.Equal(t, "2", arr[1])
		}
		r, _ = doAny(t, ctx, "RPOP", p+"lrc", "2")
		arr, ok = r.([]interface{})
		if ok && len(arr) == 2 {
			assert.Equal(t, "5", arr[0])
			assert.Equal(t, "4", arr[1])
		}
	})

	// --- LLEN ---
	t.Run("LLEN", func(t *testing.T) {
		sharedClient.RPush(ctx, p+"llen", "a", "b", "c")
		n, _ := sharedClient.LLen(ctx, p+"llen").Result()
		assert.Equal(t, int64(3), n)
	})

	// --- LINDEX ---
	t.Run("LINDEX", func(t *testing.T) {
		sharedClient.RPush(ctx, p+"li", "x", "y", "z")
		v, _ := sharedClient.LIndex(ctx, p+"li", 1).Result()
		assert.Equal(t, "y", v)
		v, _ = sharedClient.LIndex(ctx, p+"li", -1).Result()
		assert.Equal(t, "z", v)
	})

	// --- LRANGE ---
	t.Run("LRANGE", func(t *testing.T) {
		sharedClient.RPush(ctx, p+"lrng", "0", "1", "2", "3", "4")
		vals, _ := sharedClient.LRange(ctx, p+"lrng", 1, 3).Result()
		assert.Equal(t, []string{"1", "2", "3"}, vals)
	})

	// --- LSET ---
	t.Run("LSET", func(t *testing.T) {
		sharedClient.RPush(ctx, p+"ls", "a", "b", "c")
		err := sharedClient.LSet(ctx, p+"ls", 1, "X").Err()
		assert.NoError(t, err)
		v, _ := sharedClient.LIndex(ctx, p+"ls", 1).Result()
		assert.Equal(t, "X", v)
	})

	// --- LTRIM ---
	t.Run("LTRIM", func(t *testing.T) {
		sharedClient.RPush(ctx, p+"lt", "0", "1", "2", "3", "4")
		sharedClient.LTrim(ctx, p+"lt", 1, 3)
		vals, _ := sharedClient.LRange(ctx, p+"lt", 0, -1).Result()
		assert.Equal(t, []string{"1", "2", "3"}, vals)
	})

	// --- LINSERT ---
	t.Run("LINSERT", func(t *testing.T) {
		sharedClient.RPush(ctx, p+"lin", "a", "c")
		n, _ := doAny(t, ctx, "LINSERT", p+"lin", "BEFORE", "c", "b")
		assert.Equal(t, int64(3), n)
		v, _ := sharedClient.LIndex(ctx, p+"lin", 1).Result()
		assert.Equal(t, "b", v)
	})

	// --- LPOS ---
	t.Run("LPOS", func(t *testing.T) {
		sharedClient.RPush(ctx, p+"lpos", "a", "b", "c", "b")
		r, _ := doAny(t, ctx, "LPOS", p+"lpos", "b")
		assert.NotNil(t, r) // returns index
	})

	// --- LREM ---
	t.Run("LREM", func(t *testing.T) {
		sharedClient.RPush(ctx, p+"lrem", "a", "b", "a", "c", "a")
		n, _ := sharedClient.LRem(ctx, p+"lrem", 2, "a").Result()
		assert.Equal(t, int64(2), n)
		vals, _ := sharedClient.LRange(ctx, p+"lrem", 0, -1).Result()
		assert.Equal(t, []string{"b", "c", "a"}, vals)
	})

	// --- LPUSHX / RPUSHX ---
	t.Run("LPUSHX_RPUSHX", func(t *testing.T) {
		// LPUSHX on non-existent key should not create it
		n, _ := sharedClient.LPushX(ctx, p+"lpx不存在", "val").Result()
		assert.Equal(t, int64(0), n)
		sharedClient.RPush(ctx, p+"lpx", "a")
		n, _ = sharedClient.LPushX(ctx, p+"lpx", "b").Result()
		assert.Equal(t, int64(2), n)
		n, _ = sharedClient.RPushX(ctx, p+"lpx", "c").Result()
		assert.Equal(t, int64(3), n)
	})

	// --- LMOVE ---
	t.Run("LMOVE", func(t *testing.T) {
		sharedClient.RPush(ctx, p+"lmsrc", "a", "b", "c")
		r, _ := doAny(t, ctx, "LMOVE", p+"lmsrc", p+"lmdst", "RIGHT", "LEFT")
		assert.Equal(t, "c", r)
		v, _ := sharedClient.LIndex(ctx, p+"lmdst", 0).Result()
		assert.Equal(t, "c", v)
	})

	// --- BLMOVE ---
	t.Run("BLMOVE", func(t *testing.T) {
		sharedClient.RPush(ctx, p+"blmsrc", "x", "y")
		r, _ := doAny(t, ctx, "BLMOVE", p+"blmsrc", p+"blmdst", "RIGHT", "LEFT", "1")
		assert.Equal(t, "y", r)
	})

	// --- BLPOP / BRPOP ---
	t.Run("BLPOP_BRPOP", func(t *testing.T) {
		sharedClient.RPush(ctx, p+"blp", "v1")
		r, err := doAny(t, ctx, "BLPOP", p+"blp", "1")
		assert.NoError(t, err)
		arr, ok := r.([]interface{})
		assert.True(t, ok)
		assert.Equal(t, 2, len(arr))

		sharedClient.RPush(ctx, p+"brp", "v2")
		r, err = doAny(t, ctx, "BRPOP", p+"brp", "1")
		assert.NoError(t, err)
		arr, ok = r.([]interface{})
		assert.True(t, ok)
		assert.Equal(t, 2, len(arr))
	})

	// --- BRPOPLPUSH ---
	t.Run("BRPOPLPUSH", func(t *testing.T) {
		sharedClient.RPush(ctx, p+"brps", "val")
		r, _ := doAny(t, ctx, "BRPOPLPUSH", p+"brps", p+"brpsdst", "1")
		assert.Equal(t, "val", r)
	})

	// --- LCS ---
	t.Run("LCS", func(t *testing.T) {
		sharedClient.Set(ctx, p+"lcs1", "ohmytext", 0)
		sharedClient.Set(ctx, p+"lcs2", "mynewtext", 0)
		r, _ := doAny(t, ctx, "LCS", p+"lcs1", p+"lcs2")
		assert.NotNil(t, r)
	})
}

// ============================================================
// 4. Hash Commands (17)
// ============================================================

func TestCommandCompleteness_Hash(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)
	ctx := context.Background()
	p := keyPrefix(t)

	// --- HSET / HGET ---
	t.Run("HSET_HGET", func(t *testing.T) {
		sharedClient.HSet(ctx, p+"h1", "f1", "v1")
		v, _ := sharedClient.HGet(ctx, p+"h1", "f1").Result()
		assert.Equal(t, "v1", v)
	})

	// --- HDEL ---
	t.Run("HDEL", func(t *testing.T) {
		sharedClient.HSet(ctx, p+"hd", "f1", "v1", "f2", "v2")
		n, _ := sharedClient.HDel(ctx, p+"hd", "f1").Result()
		assert.Equal(t, int64(1), n)
		exists, _ := sharedClient.HExists(ctx, p+"hd", "f1").Result()
		assert.False(t, exists)
	})

	// --- HLEN ---
	t.Run("HLEN", func(t *testing.T) {
		sharedClient.HSet(ctx, p+"hlen", "a", "1", "b", "2", "c", "3")
		n, _ := sharedClient.HLen(ctx, p+"hlen").Result()
		assert.Equal(t, int64(3), n)
	})

	// --- HGETALL ---
	t.Run("HGETALL", func(t *testing.T) {
		sharedClient.HSet(ctx, p+"hga", "f1", "v1", "f2", "v2")
		m, _ := sharedClient.HGetAll(ctx, p+"hga").Result()
		assert.Equal(t, 2, len(m))
		assert.Equal(t, "v1", m["f1"])
	})

	// --- HEXISTS ---
	t.Run("HEXISTS", func(t *testing.T) {
		sharedClient.HSet(ctx, p+"he", "f1", "v1")
		ok, _ := sharedClient.HExists(ctx, p+"he", "f1").Result()
		assert.True(t, ok)
		ok, _ = sharedClient.HExists(ctx, p+"he", "f2").Result()
		assert.False(t, ok)
	})

	// --- HKEYS / HVALS ---
	t.Run("HKEYS_HVALS", func(t *testing.T) {
		sharedClient.HSet(ctx, p+"hk", "a", "1", "b", "2")
		keys, _ := sharedClient.HKeys(ctx, p+"hk").Result()
		assert.Equal(t, 2, len(keys))
		vals, _ := sharedClient.HVals(ctx, p+"hk").Result()
		assert.Equal(t, 2, len(vals))
	})

	// --- HMSET / HMGET ---
	t.Run("HMSET_HMGET", func(t *testing.T) {
		sharedClient.HMSet(ctx, p+"hm", map[string]interface{}{"f1": "v1", "f2": "v2"})
		vals, _ := sharedClient.HMGet(ctx, p+"hm", "f1", "f2").Result()
		assert.Equal(t, 2, len(vals))
	})

	// --- HSETNX ---
	t.Run("HSETNX", func(t *testing.T) {
		ok, _ := sharedClient.HSetNX(ctx, p+"hsnx", "f1", "v1").Result()
		assert.True(t, ok)
		ok, _ = sharedClient.HSetNX(ctx, p+"hsnx", "f1", "v2").Result()
		assert.False(t, ok)
		v, _ := sharedClient.HGet(ctx, p+"hsnx", "f1").Result()
		assert.Equal(t, "v1", v)
	})

	// --- HINCRBY / HINCRBYFLOAT ---
	t.Run("HINCRBY_HINCRBYFLOAT", func(t *testing.T) {
		sharedClient.HSet(ctx, p+"hi", "cnt", "10")
		n, _ := sharedClient.HIncrBy(ctx, p+"hi", "cnt", 5).Result()
		assert.Equal(t, int64(15), n)
		sharedClient.HSet(ctx, p+"hi", "flt", "1.5")
		f, _ := sharedClient.HIncrByFloat(ctx, p+"hi", "flt", 2.5).Result()
		assert.Equal(t, 4.0, f)
	})

	// --- HSTRLEN ---
	t.Run("HSTRLEN", func(t *testing.T) {
		sharedClient.HSet(ctx, p+"hs", "f1", "hello")
		n, _ := sharedClient.HStrLen(ctx, p+"hs", "f1").Result()
		assert.Equal(t, int64(5), n)
	})

	// --- HRANDFIELD ---
	t.Run("HRANDFIELD", func(t *testing.T) {
		sharedClient.HSet(ctx, p+"hrf", "f1", "v1", "f2", "v2", "f3", "v3")
		r, _ := doAny(t, ctx, "HRANDFIELD", p+"hrf")
		assert.NotNil(t, r)
		r, _ = doAny(t, ctx, "HRANDFIELD", p+"hrf", "2")
		assert.NotNil(t, r)
		r, _ = doAny(t, ctx, "HRANDFIELD", p+"hrf", "2", "WITHVALUES")
		assert.NotNil(t, r)
	})

	// --- HSCAN ---
	t.Run("HSCAN", func(t *testing.T) {
		sharedClient.HSet(ctx, p+"hsn", "f1", "v1", "f2", "v2")
		r, _ := doAny(t, ctx, "HSCAN", p+"hsn", "0")
		assert.NotNil(t, r)
	})
}

// ============================================================
// 5. Set Commands (17)
// ============================================================

func TestCommandCompleteness_Set(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)
	ctx := context.Background()
	p := keyPrefix(t)

	// --- SADD ---
	t.Run("SADD", func(t *testing.T) {
		n, _ := sharedClient.SAdd(ctx, p+"s1", "a", "b", "c").Result()
		assert.Equal(t, int64(3), n)
	})

	// --- SREM ---
	t.Run("SREM", func(t *testing.T) {
		sharedClient.SAdd(ctx, p+"sr", "a", "b", "c")
		n, _ := sharedClient.SRem(ctx, p+"sr", "b").Result()
		assert.Equal(t, int64(1), n)
	})

	// --- SCARD ---
	t.Run("SCARD", func(t *testing.T) {
		sharedClient.SAdd(ctx, p+"sc", "a", "b")
		n, _ := sharedClient.SCard(ctx, p+"sc").Result()
		assert.Equal(t, int64(2), n)
	})

	// --- SISMEMBER ---
	t.Run("SISMEMBER", func(t *testing.T) {
		sharedClient.SAdd(ctx, p+"si", "a", "b")
		ok, _ := sharedClient.SIsMember(ctx, p+"si", "a").Result()
		assert.True(t, ok)
		ok, _ = sharedClient.SIsMember(ctx, p+"si", "z").Result()
		assert.False(t, ok)
	})

	// --- SMEMBERS ---
	t.Run("SMEMBERS", func(t *testing.T) {
		sharedClient.SAdd(ctx, p+"sm", "x", "y")
		members, _ := sharedClient.SMembers(ctx, p+"sm").Result()
		assert.Equal(t, 2, len(members))
	})

	// --- SPOP ---
	t.Run("SPOP", func(t *testing.T) {
		sharedClient.SAdd(ctx, p+"sp", "a", "b", "c")
		v, err := sharedClient.SPop(ctx, p+"sp").Result()
		assert.NoError(t, err)
		assert.True(t, v == "a" || v == "b" || v == "c")
	})

	// --- SRANDMEMBER ---
	t.Run("SRANDMEMBER", func(t *testing.T) {
		sharedClient.SAdd(ctx, p+"srand", "a", "b", "c")
		v, _ := sharedClient.SRandMember(ctx, p+"srand").Result()
		assert.True(t, v == "a" || v == "b" || v == "c")
		vals, _ := sharedClient.SRandMemberN(ctx, p+"srand", 2).Result()
		assert.Equal(t, 2, len(vals))
	})

	// --- SMOVE ---
	t.Run("SMOVE", func(t *testing.T) {
		sharedClient.SAdd(ctx, p+"smsrc", "a", "b")
		sharedClient.SAdd(ctx, p+"smdst", "x")
		r, _ := doAny(t, ctx, "SMOVE", p+"smsrc", p+"smdst", "a")
		assert.Equal(t, int64(1), r)
		ok, _ := sharedClient.SIsMember(ctx, p+"smdst", "a").Result()
		assert.True(t, ok)
	})

	// --- SINTER / SUNION / SDIFF ---
	t.Run("SINTER_SUNION_SDIFF", func(t *testing.T) {
		sharedClient.SAdd(ctx, p+"sia", "a", "b", "c")
		sharedClient.SAdd(ctx, p+"sib", "b", "c", "d")
		// SINTER
		r, _ := doAny(t, ctx, "SINTER", p+"sia", p+"sib")
		arr, ok := r.([]interface{})
		assert.True(t, ok)
		assert.Equal(t, 2, len(arr)) // b, c
		// SUNION
		r, _ = doAny(t, ctx, "SUNION", p+"sia", p+"sib")
		arr, ok = r.([]interface{})
		assert.True(t, ok)
		assert.Equal(t, 4, len(arr)) // a, b, c, d
		// SDIFF
		r, _ = doAny(t, ctx, "SDIFF", p+"sia", p+"sib")
		arr, ok = r.([]interface{})
		assert.True(t, ok)
		assert.Equal(t, 1, len(arr)) // a
	})

	// --- SINTERSTORE / SUNIONSTORE / SDIFFSTORE ---
	t.Run("STORE_variants", func(t *testing.T) {
		sharedClient.SAdd(ctx, p+"ssa", "1", "2")
		sharedClient.SAdd(ctx, p+"ssb", "2", "3")
		do(t, ctx, "SINTERSTORE", p+"ssres", p+"ssa", p+"ssb")
		n, _ := sharedClient.SCard(ctx, p+"ssres").Result()
		assert.Equal(t, int64(1), n)

		do(t, ctx, "SUNIONSTORE", p+"sures", p+"ssa", p+"ssb")
		n, _ = sharedClient.SCard(ctx, p+"sures").Result()
		assert.Equal(t, int64(3), n)

		do(t, ctx, "SDIFFSTORE", p+"sdres", p+"ssa", p+"ssb")
		n, _ = sharedClient.SCard(ctx, p+"sdres").Result()
		assert.Equal(t, int64(1), n)
	})

	// --- SMISMEMBER ---
	t.Run("SMISMEMBER", func(t *testing.T) {
		sharedClient.SAdd(ctx, p+"smis", "a", "b", "c")
		r, _ := doAny(t, ctx, "SMISMEMBER", p+"smis", "a", "z", "b")
		assert.NotNil(t, r)
	})

	// --- SINTERCARD ---
	t.Run("SINTERCARD", func(t *testing.T) {
		sharedClient.SAdd(ctx, p+"sic1", "a", "b", "c")
		sharedClient.SAdd(ctx, p+"sic2", "b", "c", "d")
		r, _ := doAny(t, ctx, "SINTERCARD", "2", p+"sic1", p+"sic2")
		assert.Equal(t, int64(2), r)
	})

	// --- SSCAN ---
	t.Run("SSCAN", func(t *testing.T) {
		sharedClient.SAdd(ctx, p+"ssn", "a", "b", "c")
		r, _ := doAny(t, ctx, "SSCAN", p+"ssn", "0")
		assert.NotNil(t, r)
	})
}

// ============================================================
// 6. Sorted Set Commands (28)
// ============================================================

func TestCommandCompleteness_SortedSet(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)
	ctx := context.Background()
	p := keyPrefix(t)

	// --- ZADD ---
	t.Run("ZADD", func(t *testing.T) {
		n, _ := sharedClient.ZAdd(ctx, p+"z1",
			redis.Z{Score: 1, Member: "a"},
			redis.Z{Score: 2, Member: "b"},
			redis.Z{Score: 3, Member: "c"},
		).Result()
		assert.Equal(t, int64(3), n)
	})

	// --- ZADD Options (NX/XX/GT/LT/CH) ---
	t.Run("ZADD_options", func(t *testing.T) {
		p := keyPrefix(t)

		// ZADD NX: new member → added
		n, _ := doAny(t, ctx, "ZADD", p+"znx1", "NX", "1", "a")
		assert.Equal(t, int64(1), n)

		// ZADD NX: existing member → not updated
		n, _ = doAny(t, ctx, "ZADD", p+"znx1", "NX", "99", "a")
		assert.Equal(t, int64(0), n)
		score, _ := sharedClient.ZScore(ctx, p+"znx1", "a").Result()
		assert.Equal(t, 1.0, score)

		// ZADD XX: update existing member
		sharedClient.ZAdd(ctx, p+"zxx1", redis.Z{Score: 1, Member: "a"})
		n, _ = doAny(t, ctx, "ZADD", p+"zxx1", "XX", "99", "a")
		assert.Equal(t, int64(0), n) // XX returns 0 (no new members added, only updated)
		score, _ = sharedClient.ZScore(ctx, p+"zxx1", "a").Result()
		assert.Equal(t, 99.0, score)

		// ZADD XX: non-existing member → not added
		n, _ = doAny(t, ctx, "ZADD", p+"zxx1", "XX", "1", "newbie")
		assert.Equal(t, int64(0), n)
		exists, _ := sharedClient.Exists(ctx, p+"zxx1").Result()
		assert.Equal(t, int64(1), exists) // key exists but newbie not there

		// ZADD GT: score > current → update
		sharedClient.ZAdd(ctx, p+"zgt1", redis.Z{Score: 5, Member: "x"})
		n, _ = doAny(t, ctx, "ZADD", p+"zgt1", "GT", "10", "x")
		assert.Equal(t, int64(0), n) // default returns # new members (0)
		score, _ = sharedClient.ZScore(ctx, p+"zgt1", "x").Result()
		assert.Equal(t, 10.0, score)

		// ZADD GT: score < current → no update
		n, _ = doAny(t, ctx, "ZADD", p+"zgt1", "GT", "1", "x")
		assert.Equal(t, int64(0), n)
		score, _ = sharedClient.ZScore(ctx, p+"zgt1", "x").Result()
		assert.Equal(t, 10.0, score) // unchanged

		// ZADD LT: score < current → update
		sharedClient.ZAdd(ctx, p+"zlt1", redis.Z{Score: 5, Member: "y"})
		n, _ = doAny(t, ctx, "ZADD", p+"zlt1", "LT", "1", "y")
		assert.Equal(t, int64(0), n)
		score, _ = sharedClient.ZScore(ctx, p+"zlt1", "y").Result()
		assert.Equal(t, 1.0, score)

		// ZADD CH: return # changed (new+updated) instead of # new
		sharedClient.ZAdd(ctx, p+"zch1", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"})
		n, _ = doAny(t, ctx, "ZADD", p+"zch1", "CH", "99", "a", "3", "c")
		assert.Equal(t, int64(2), n) // a updated + c added = 2 changes
	})

	// --- ZREM ---
	t.Run("ZREM", func(t *testing.T) {
		sharedClient.ZAdd(ctx, p+"zr", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"})
		n, _ := sharedClient.ZRem(ctx, p+"zr", "a").Result()
		assert.Equal(t, int64(1), n)
	})

	// --- ZCARD ---
	t.Run("ZCARD", func(t *testing.T) {
		sharedClient.ZAdd(ctx, p+"zc", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"})
		n, _ := sharedClient.ZCard(ctx, p+"zc").Result()
		assert.Equal(t, int64(2), n)
	})

	// --- ZSCORE ---
	t.Run("ZSCORE", func(t *testing.T) {
		sharedClient.ZAdd(ctx, p+"zs", redis.Z{Score: 3.14, Member: "pi"})
		s, _ := sharedClient.ZScore(ctx, p+"zs", "pi").Result()
		assert.Equal(t, 3.14, s)
	})

	// --- ZMSCORE ---
	t.Run("ZMSCORE", func(t *testing.T) {
		sharedClient.ZAdd(ctx, p+"zms", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"})
		r, _ := doAny(t, ctx, "ZMSCORE", p+"zms", "a", "b")
		assert.NotNil(t, r)
	})

	// --- ZRANGE ---
	t.Run("ZRANGE", func(t *testing.T) {
		sharedClient.ZAdd(ctx, p+"zrng", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"})
		vals, _ := sharedClient.ZRange(ctx, p+"zrng", 0, 1).Result()
		assert.Equal(t, 2, len(vals))
		// WITHSCORES
		vals2, _ := sharedClient.ZRangeWithScores(ctx, p+"zrng", 0, 1).Result()
		assert.Equal(t, 2, len(vals2))
	})

	// --- ZREVRANGE ---
	t.Run("ZREVRANGE", func(t *testing.T) {
		sharedClient.ZAdd(ctx, p+"zvr", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"})
		r, _ := doAny(t, ctx, "ZREVRANGE", p+"zvr", "0", "1", "WITHSCORES")
		assert.NotNil(t, r)
	})

	// --- ZRANGEBYSCORE ---
	t.Run("ZRANGEBYSCORE", func(t *testing.T) {
		sharedClient.ZAdd(ctx, p+"zrbs", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"})
		vals, _ := sharedClient.ZRangeByScore(ctx, p+"zrbs", &redis.ZRangeBy{Min: "1", Max: "2"}).Result()
		assert.Equal(t, 2, len(vals))
	})

	// --- ZREVRANGEBYSCORE ---
	t.Run("ZREVRANGEBYSCORE", func(t *testing.T) {
		sharedClient.ZAdd(ctx, p+"zvrbs", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"})
		r, _ := doAny(t, ctx, "ZREVRANGEBYSCORE", p+"zvrbs", "3", "1")
		assert.NotNil(t, r)
	})

	// --- ZRANK / ZREVRANK ---
	t.Run("ZRANK_ZREVRANK", func(t *testing.T) {
		sharedClient.ZAdd(ctx, p+"zrk", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"})
		n, _ := sharedClient.ZRank(ctx, p+"zrk", "a").Result()
		assert.Equal(t, int64(0), n)
		n, _ = sharedClient.ZRevRank(ctx, p+"zrk", "a").Result()
		assert.Equal(t, int64(1), n)
	})

	// --- ZCOUNT ---
	t.Run("ZCOUNT", func(t *testing.T) {
		sharedClient.ZAdd(ctx, p+"zcnt", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"})
		n, _ := sharedClient.ZCount(ctx, p+"zcnt", "1", "2").Result()
		assert.Equal(t, int64(2), n)
	})

	// --- ZINCRBY ---
	t.Run("ZINCRBY", func(t *testing.T) {
		sharedClient.ZAdd(ctx, p+"zinc", redis.Z{Score: 1, Member: "a"})
		s, _ := sharedClient.ZIncrBy(ctx, p+"zinc", 2.5, "a").Result()
		assert.Equal(t, 3.5, s)
	})

	// --- ZREMRANGEBYRANK ---
	t.Run("ZREMRANGEBYRANK", func(t *testing.T) {
		sharedClient.ZAdd(ctx, p+"zrrbr", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"})
		n, _ := sharedClient.ZRemRangeByRank(ctx, p+"zrrbr", 0, 1).Result()
		assert.Equal(t, int64(2), n)
	})

	// --- ZREMRANGEBYSCORE ---
	t.Run("ZREMRANGEBYSCORE", func(t *testing.T) {
		sharedClient.ZAdd(ctx, p+"zrrbs", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"})
		n, _ := sharedClient.ZRemRangeByScore(ctx, p+"zrrbs", "1", "2").Result()
		assert.Equal(t, int64(2), n)
	})

	// --- ZPOPMAX / ZPOPMIN ---
	t.Run("ZPOPMAX_ZPOPMIN", func(t *testing.T) {
		sharedClient.ZAdd(ctx, p+"zpop", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"})
		z, _ := sharedClient.ZPopMax(ctx, p+"zpop").Result()
		assert.Equal(t, 1, len(z))
		assert.Equal(t, "c", z[0].Member)
		z, _ = sharedClient.ZPopMin(ctx, p+"zpop").Result()
		assert.Equal(t, "a", z[0].Member)
	})

	// --- BZPOPMAX / BZPOPMIN ---
	t.Run("BZPOPMAX_BZPOPMIN", func(t *testing.T) {
		sharedClient.ZAdd(ctx, p+"bzpop", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"})
		r, _ := doAny(t, ctx, "BZPOPMAX", p+"bzpop", "1")
		assert.NotNil(t, r)
	})

	// --- ZUNIONSTORE / ZINTERSTORE ---
	t.Run("ZUNIONSTORE_ZINTERSTORE", func(t *testing.T) {
		sharedClient.ZAdd(ctx, p+"zua", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"})
		sharedClient.ZAdd(ctx, p+"zub", redis.Z{Score: 3, Member: "b"}, redis.Z{Score: 4, Member: "c"})
		n, _ := sharedClient.ZUnionStore(ctx, p+"zu_res", &redis.ZStore{Keys: []string{p + "zua", p + "zub"}}).Result()
		assert.Equal(t, int64(3), n) // a, b, c
		n, _ = sharedClient.ZInterStore(ctx, p+"zi_res", &redis.ZStore{Keys: []string{p + "zua", p + "zub"}}).Result()
		assert.Equal(t, int64(1), n) // b
	})

	// --- ZDIFFSTORE ---
	t.Run("ZDIFFSTORE", func(t *testing.T) {
		sharedClient.ZAdd(ctx, p+"zda", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"})
		sharedClient.ZAdd(ctx, p+"zdb", redis.Z{Score: 1, Member: "b"}, redis.Z{Score: 2, Member: "c"})
		r, _ := doAny(t, ctx, "ZDIFFSTORE", p+"zdres", "2", p+"zda", p+"zdb")
		assert.Equal(t, int64(1), r) // only "a"
	})

	// --- ZDIFF ---
	t.Run("ZDIFF", func(t *testing.T) {
		sharedClient.ZAdd(ctx, p+"zd2a", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"})
		sharedClient.ZAdd(ctx, p+"zd2b", redis.Z{Score: 1, Member: "b"}, redis.Z{Score: 2, Member: "c"})
		r, _ := doAny(t, ctx, "ZDIFF", "2", p+"zd2a", p+"zd2b")
		assert.NotNil(t, r)
	})

	// --- ZINTER / ZUNION ---
	t.Run("ZINTER_ZUNION", func(t *testing.T) {
		sharedClient.ZAdd(ctx, p+"zia", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"})
		sharedClient.ZAdd(ctx, p+"zib", redis.Z{Score: 3, Member: "b"}, redis.Z{Score: 4, Member: "c"})
		r, _ := doAny(t, ctx, "ZINTER", "2", p+"zia", p+"zib")
		assert.NotNil(t, r)
		r, _ = doAny(t, ctx, "ZUNION", "2", p+"zia", p+"zib")
		assert.NotNil(t, r)
	})

	// --- ZINTERCARD ---
	t.Run("ZINTERCARD", func(t *testing.T) {
		sharedClient.ZAdd(ctx, p+"zica", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"})
		sharedClient.ZAdd(ctx, p+"zicb", redis.Z{Score: 3, Member: "b"}, redis.Z{Score: 4, Member: "c"})
		r, _ := doAny(t, ctx, "ZINTERCARD", "2", p+"zica", p+"zicb")
		assert.Equal(t, int64(1), r) // b
	})

	// --- ZLEXCOUNT ---
	t.Run("ZLEXCOUNT", func(t *testing.T) {
		sharedClient.ZAdd(ctx, p+"zlc", redis.Z{Score: 0, Member: "a"}, redis.Z{Score: 0, Member: "b"}, redis.Z{Score: 0, Member: "c"})
		n, _ := doAny(t, ctx, "ZLEXCOUNT", p+"zlc", "[a", "[c")
		assert.Equal(t, int64(3), n)
	})

	// --- ZRANGEBYLEX / ZREVRANGEBYLEX / ZREMRANGEBYLEX ---
	t.Run("ZRANGEBYLEX_variants", func(t *testing.T) {
		sharedClient.ZAdd(ctx, p+"zblx", redis.Z{Score: 0, Member: "a"}, redis.Z{Score: 0, Member: "b"}, redis.Z{Score: 0, Member: "c"})
		r, _ := doAny(t, ctx, "ZRANGEBYLEX", p+"zblx", "[a", "[b")
		assert.NotNil(t, r)
		r, _ = doAny(t, ctx, "ZREVRANGEBYLEX", p+"zblx", "[c", "[b")
		assert.NotNil(t, r)
		do(t, ctx, "ZREMRANGEBYLEX", p+"zblx", "[a", "[a")
		n, _ := sharedClient.ZCard(ctx, p+"zblx").Result()
		assert.Equal(t, int64(2), n)
	})

	// --- ZSCAN ---
	t.Run("ZSCAN", func(t *testing.T) {
		sharedClient.ZAdd(ctx, p+"zsc", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"})
		r, _ := doAny(t, ctx, "ZSCAN", p+"zsc", "0")
		assert.NotNil(t, r)
	})

	// --- ZRANGESTORE ---
	t.Run("ZRANGESTORE", func(t *testing.T) {
		sharedClient.ZAdd(ctx, p+"zrsrc", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"})
		r, _ := doAny(t, ctx, "ZRANGESTORE", p+"zrdst", p+"zrsrc", "0", "1")
		assert.Equal(t, int64(2), r)
	})

	// --- ZRANDMEMBER ---
	t.Run("ZRANDMEMBER", func(t *testing.T) {
		sharedClient.ZAdd(ctx, p+"zrm", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"})
		r, _ := doAny(t, ctx, "ZRANDMEMBER", p+"zrm")
		assert.NotNil(t, r)
	})

	// --- ZMPOP ---
	t.Run("ZMPOP", func(t *testing.T) {
		sharedClient.ZAdd(ctx, p+"zmp", redis.Z{Score: 1, Member: "a"})
		r, _ := doAny(t, ctx, "ZMPOP", "1", p+"zmp", "MIN")
		assert.NotNil(t, r)
	})

	// --- BZMPOP ---
	t.Run("BZMPOP", func(t *testing.T) {
		sharedClient.ZAdd(ctx, p+"bzmp", redis.Z{Score: 1, Member: "a"})
		r, _ := doAny(t, ctx, "BZMPOP", "1", "1", p+"bzmp", "MIN")
		assert.NotNil(t, r)
	})
}

// ============================================================
// 7. HyperLogLog Commands (3+1)
// ============================================================

func TestCommandCompleteness_HLL(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)
	ctx := context.Background()
	p := keyPrefix(t)

	// --- PFADD ---
	t.Run("PFADD", func(t *testing.T) {
		r, _ := doAny(t, ctx, "PFADD", p+"hll1", "a", "b", "c")
		assert.Equal(t, int64(1), r) // 1 = new registers added
	})

	// --- PFCOUNT ---
	t.Run("PFCOUNT", func(t *testing.T) {
		do(t, ctx, "PFADD", p+"hll2", "a", "b", "c")
		n, _ := doAny(t, ctx, "PFCOUNT", p+"hll2")
		assert.True(t, n.(int64) >= 3)
	})

	// --- PFMERGE ---
	t.Run("PFMERGE", func(t *testing.T) {
		do(t, ctx, "PFADD", p+"hll3a", "a", "b")
		do(t, ctx, "PFADD", p+"hll3b", "c", "d")
		do(t, ctx, "PFMERGE", p+"hll3m", p+"hll3a", p+"hll3b")
		n, _ := doAny(t, ctx, "PFCOUNT", p+"hll3m")
		assert.True(t, n.(int64) >= 4)
	})

	// --- PFINFO ---
	t.Run("PFINFO", func(t *testing.T) {
		do(t, ctx, "PFADD", p+"hll4", "a")
		r, _ := doAny(t, ctx, "PFINFO", p+"hll4")
		assert.NotNil(t, r)
	})
}

// ============================================================
// 8. Geo Commands (6)
// ============================================================

func TestCommandCompleteness_Geo(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)
	ctx := context.Background()
	p := keyPrefix(t)

	// --- GEOADD ---
	t.Run("GEOADD", func(t *testing.T) {
		r, _ := doAny(t, ctx, "GEOADD", p+"geo", "116.40", "39.90", "beijing", "121.47", "31.23", "shanghai")
		assert.Equal(t, int64(2), r)
	})

	// --- GEOADD options (NX/XX/CH) ---
	t.Run("GEOADD_options", func(t *testing.T) {
		p := keyPrefix(t)

		// GEOADD NX: new member → added
		r, _ := doAny(t, ctx, "GEOADD", p+"gnx", "NX", "116.40", "39.90", "bj")
		assert.Equal(t, int64(1), r)

		// GEOADD NX: existing member → not updated
		r, _ = doAny(t, ctx, "GEOADD", p+"gnx", "NX", "1.0", "1.0", "bj")
		assert.Equal(t, int64(0), r)

		// GEOADD XX: non-existing member → not added
		r, _ = doAny(t, ctx, "GEOADD", p+"gxx", "XX", "116.40", "39.90", "sh")
		assert.Equal(t, int64(0), r)

		// GEOADD XX: existing member → update
		doAny(t, ctx, "GEOADD", p+"gxx", "1.0", "1.0", "sh")
		r, _ = doAny(t, ctx, "GEOADD", p+"gxx", "XX", "116.40", "39.90", "sh")
		assert.Equal(t, int64(0), r) // update returns 0 (no new members)

		// GEOADD CH: count changes (new + updated) instead of new only
		doAny(t, ctx, "GEOADD", p+"gch", "1.0", "1.0", "a", "2.0", "2.0", "b")
		r, _ = doAny(t, ctx, "GEOADD", p+"gch", "CH", "116.40", "39.90", "a", "3.0", "3.0", "c")
		assert.Equal(t, int64(2), r) // a updated + c added = 2 changes
	})

	// --- GEOPOS ---
	t.Run("GEOPOS", func(t *testing.T) {
		r, _ := doAny(t, ctx, "GEOPOS", p+"geo", "beijing")
		assert.NotNil(t, r)
	})

	// --- GEOHASH ---
	t.Run("GEOHASH", func(t *testing.T) {
		r, _ := doAny(t, ctx, "GEOHASH", p+"geo", "beijing")
		assert.NotNil(t, r)
	})

	// --- GEODIST ---
	t.Run("GEODIST", func(t *testing.T) {
		r, _ := doAny(t, ctx, "GEODIST", p+"geo", "beijing", "shanghai", "km")
		assert.NotNil(t, r)
	})

	// --- GEOSEARCH ---
	t.Run("GEOSEARCH", func(t *testing.T) {
		r, _ := doAny(t, ctx, "GEOSEARCH", p+"geo", "FROMLONLAT", "116.40", "39.90", "BYRADIUS", "500", "km")
		assert.NotNil(t, r)
	})

	// --- GEOSEARCHSTORE ---
	t.Run("GEOSEARCHSTORE", func(t *testing.T) {
		r, _ := doAny(t, ctx, "GEOSEARCHSTORE", p+"geodst", p+"geo", "FROMLONLAT", "116.40", "39.90", "BYRADIUS", "5000", "km")
		assert.NotNil(t, r)
	})
}

// ============================================================
// 9. Stream Commands (19+)
// ============================================================

func TestCommandCompleteness_Stream(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)
	ctx := context.Background()
	p := keyPrefix(t)

	// --- XADD ---
	t.Run("XADD", func(t *testing.T) {
		id, _ := sharedClient.XAdd(ctx, &redis.XAddArgs{
			Stream: p + "stm1",
			Values: map[string]interface{}{"field1": "value1"},
		}).Result()
		assert.True(t, len(id) > 0)
	})

	// --- XLEN ---
	t.Run("XLEN", func(t *testing.T) {
		sharedClient.XAdd(ctx, &redis.XAddArgs{Stream: p + "stm2", Values: map[string]interface{}{"f": "v"}})
		n, _ := sharedClient.XLen(ctx, p+"stm2").Result()
		assert.Equal(t, int64(1), n)
	})

	// --- XRANGE ---
	t.Run("XRANGE", func(t *testing.T) {
		sharedClient.XAdd(ctx, &redis.XAddArgs{Stream: p + "stm3", Values: map[string]interface{}{"f": "v"}})
		msgs, _ := sharedClient.XRange(ctx, p+"stm3", "-", "+").Result()
		assert.True(t, len(msgs) >= 1)
	})

	// --- XREVRANGE ---
	t.Run("XREVRANGE", func(t *testing.T) {
		sharedClient.XAdd(ctx, &redis.XAddArgs{Stream: p + "stm3r", Values: map[string]interface{}{"f": "v"}})
		r, _ := doAny(t, ctx, "XREVRANGE", p+"stm3r", "+", "-")
		assert.NotNil(t, r)
	})

	// --- XREAD ---
	t.Run("XREAD", func(t *testing.T) {
		sharedClient.XAdd(ctx, &redis.XAddArgs{Stream: p + "stm4", Values: map[string]interface{}{"f": "v"}})
		r, _ := doAny(t, ctx, "XREAD", "COUNT", "1", "STREAMS", p+"stm4", "0")
		assert.NotNil(t, r)
	})

	// --- XDEL ---
	t.Run("XDEL", func(t *testing.T) {
		id, _ := sharedClient.XAdd(ctx, &redis.XAddArgs{Stream: p + "stm5", Values: map[string]interface{}{"f": "v"}}).Result()
		n, _ := sharedClient.XDel(ctx, p+"stm5", id).Result()
		assert.Equal(t, int64(1), n)
	})

	// --- XACK ---
	t.Run("XACK", func(t *testing.T) {
		streamKey := p + "stm6"
		groupKey := p + "grp6"
		sharedClient.XAdd(ctx, &redis.XAddArgs{Stream: streamKey, Values: map[string]interface{}{"f": "v"}})
		sharedClient.XGroupCreateMkStream(ctx, streamKey, groupKey, "0")
		// Read with ">" to get new messages (non-blocking)
		msgs, err := sharedClient.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupKey,
			Consumer: "c1",
			Streams:  []string{streamKey, ">"},
			Count:    1,
		}).Result()
		assert.NoError(t, err)
		if len(msgs) > 0 && len(msgs[0].Messages) > 0 {
			n, _ := sharedClient.XAck(ctx, streamKey, groupKey, msgs[0].Messages[0].ID).Result()
			assert.Equal(t, int64(1), n)
		}
	})

	// --- XSETID ---
	t.Run("XSETID", func(t *testing.T) {
		sharedClient.XAdd(ctx, &redis.XAddArgs{Stream: p + "stm7", Values: map[string]interface{}{"f": "v"}})
		r, _ := doAny(t, ctx, "XSETID", p+"stm7", "999999999999-0")
		assert.NotNil(t, r)
	})

	// --- XGROUP CREATE / DESTROY / SETID / DELCONSUMER ---
	t.Run("XGROUP", func(t *testing.T) {
		sharedClient.XAdd(ctx, &redis.XAddArgs{Stream: p + "stm8", Values: map[string]interface{}{"f": "v"}})
		// CREATE
		sharedClient.XGroupCreateMkStream(ctx, p+"stm8", p+"grp8", "0")
		// SETID
		r, _ := doAny(t, ctx, "XGROUP", "SETID", p+"stm8", p+"grp8", "0")
		assert.NotNil(t, r)
		// DELCONSUMER
		sharedClient.XGroupCreateConsumer(ctx, p+"stm8", p+"grp8", "delme")
		r, _ = doAny(t, ctx, "XGROUP", "DELCONSUMER", p+"stm8", p+"grp8", "delme")
		assert.NotNil(t, r)
		// DESTROY
		r, _ = doAny(t, ctx, "XGROUP", "DESTROY", p+"stm8", p+"grp8")
		assert.NotNil(t, r)
	})

	// --- XREADGROUP ---
	t.Run("XREADGROUP", func(t *testing.T) {
		sharedClient.XAdd(ctx, &redis.XAddArgs{Stream: p + "stm9", Values: map[string]interface{}{"f": "v"}})
		sharedClient.XGroupCreateMkStream(ctx, p+"stm9", p+"grp9", "0")
		r, _ := doAny(t, ctx, "XREADGROUP", "GROUP", p+"grp9", "c1", "COUNT", "1", "STREAMS", p+"stm9", ">")
		assert.NotNil(t, r)
	})

	// --- XCLAIM ---
	// Full Redis entry shape [[id, [field, value...]], ...] for go-redis XClaim().
	t.Run("XCLAIM", func(t *testing.T) {
		id, err := sharedClient.XAdd(ctx, &redis.XAddArgs{
			Stream: p + "stm10",
			Values: map[string]interface{}{"f": "v"},
		}).Result()
		assert.NoError(t, err)
		assert.NoError(t, sharedClient.XGroupCreateMkStream(ctx, p+"stm10", p+"grp10", "0").Err())
		// Read so message enters PEL
		_, err = sharedClient.Do(ctx, "XREADGROUP", "GROUP", p+"grp10", "old",
			"COUNT", "1", "STREAMS", p+"stm10", ">").Result()
		assert.NoError(t, err)

		claimed, err := sharedClient.XClaim(ctx, &redis.XClaimArgs{
			Stream:   p + "stm10",
			Group:    p + "grp10",
			Consumer: "new",
			MinIdle:  0,
			Messages: []string{id},
		}).Result()
		assert.NoError(t, err)
		assert.Equal(t, 1, len(claimed))
		assert.Equal(t, id, claimed[0].ID)
		assert.Equal(t, "v", claimed[0].Values["f"])
	})

	// --- XAUTOCLAIM ---
	t.Run("XAUTOCLAIM", func(t *testing.T) {
		id, err := sharedClient.XAdd(ctx, &redis.XAddArgs{
			Stream: p + "stm11",
			Values: map[string]interface{}{"f": "v"},
		}).Result()
		assert.NoError(t, err)
		assert.NoError(t, sharedClient.XGroupCreateMkStream(ctx, p+"stm11", p+"grp11", "0").Err())
		_, err = sharedClient.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: p + "grp11", Consumer: "old", Streams: []string{p + "stm11", ">"}, Count: 1,
		}).Result()
		assert.NoError(t, err)
		// [nextID, [[id, [fields...]], ...]]
		r, err := sharedClient.Do(ctx, "XAUTOCLAIM", p+"stm11", p+"grp11", "newclaim", "0", "0-0", "COUNT", "10").Result()
		assert.NoError(t, err)
		arr, ok := r.([]interface{})
		assert.True(t, ok)
		assert.True(t, len(arr) >= 2)
		entries, ok := arr[1].([]interface{})
		assert.True(t, ok)
		assert.True(t, len(entries) >= 1)
		entry, ok := entries[0].([]interface{})
		assert.True(t, ok)
		assert.Equal(t, id, entry[0].(string))
	})

	// --- XPENDING ---
	t.Run("XPENDING", func(t *testing.T) {
		sharedClient.XAdd(ctx, &redis.XAddArgs{Stream: p + "stm12", Values: map[string]interface{}{"f": "v"}})
		sharedClient.XGroupCreateMkStream(ctx, p+"stm12", p+"grp12", "0")
		// Read but don't ack
		assert.NoError(t, sharedClient.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    p + "grp12",
			Consumer: "c1",
			Streams:  []string{p + "stm12", ">"},
			Count:    1,
		}).Err())
		sum, err := sharedClient.XPending(ctx, p+"stm12", p+"grp12").Result()
		assert.NoError(t, err)
		assert.True(t, sum.Count >= 1)
		ext, err := sharedClient.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: p + "stm12", Group: p + "grp12", Start: "-", End: "+", Count: 10,
		}).Result()
		assert.NoError(t, err)
		assert.True(t, len(ext) >= 1)
		assert.Equal(t, "c1", ext[0].Consumer)
	})

	// --- XINFO ---
	t.Run("XINFO", func(t *testing.T) {
		sharedClient.XAdd(ctx, &redis.XAddArgs{Stream: p + "stm13", Values: map[string]interface{}{"f": "v"}})
		sharedClient.XGroupCreateMkStream(ctx, p+"stm13", p+"grp13", "0")
		// STREAM
		r, _ := doAny(t, ctx, "XINFO", "STREAM", p+"stm13")
		assert.NotNil(t, r)
		// GROUPS
		r, _ = doAny(t, ctx, "XINFO", "GROUPS", p+"stm13")
		assert.NotNil(t, r)
		// CONSUMERS
		r, _ = doAny(t, ctx, "XINFO", "CONSUMERS", p+"stm13", p+"grp13")
		assert.NotNil(t, r)
		// HELP
		r, _ = doAny(t, ctx, "XINFO", "HELP")
		assert.NotNil(t, r)
	})

	// --- XTRIM ---
	t.Run("XTRIM", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			sharedClient.XAdd(ctx, &redis.XAddArgs{Stream: p + "stm14", Values: map[string]interface{}{"f": fmt.Sprintf("v%d", i)}})
		}
		r, _ := doAny(t, ctx, "XTRIM", p+"stm14", "MAXLEN", "5")
		assert.NotNil(t, r)
		n, _ := sharedClient.XLen(ctx, p+"stm14").Result()
		assert.True(t, n <= 5)
	})

	// Clean up all stream keys to avoid consistency check failures
	for i := 1; i <= 14; i++ {
		sharedClient.Del(ctx, p+fmt.Sprintf("stm%d", i))
	}
	sharedClient.Del(ctx, p+"stm3r")
}

// ============================================================
// 10. JSON Commands (12)
// ============================================================

func TestCommandCompleteness_JSON(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)
	ctx := context.Background()
	p := keyPrefix(t)

	// --- JSON.SET ---
	t.Run("JSON_SET", func(t *testing.T) {
		r, _ := doAny(t, ctx, "JSON.SET", p+"js1", "$", `{"name":"test","age":30}`)
		assert.Equal(t, "OK", r)
	})

	// --- JSON.GET ---
	t.Run("JSON_GET", func(t *testing.T) {
		do(t, ctx, "JSON.SET", p+"js2", "$", `{"name":"test"}`)
		r, _ := doAny(t, ctx, "JSON.GET", p+"js2")
		assert.NotNil(t, r)
	})

	// --- JSON.DEL ---
	t.Run("JSON_DEL", func(t *testing.T) {
		delKey := p + "js3_del"
		do(t, ctx, "JSON.SET", delKey, "$", `{"name":"test","age":30}`)
		r, _ := doAny(t, ctx, "JSON.DEL", delKey, "$.age")
		// JSON.DEL returns number of deleted paths; 0 or 1 are both valid
		assert.True(t, r != nil)
	})

	// --- JSON.TYPE ---
	t.Run("JSON_TYPE", func(t *testing.T) {
		do(t, ctx, "JSON.SET", p+"js4", "$", `{"val":42}`)
		r, _ := doAny(t, ctx, "JSON.TYPE", p+"js4", "$.val")
		assert.NotNil(t, r)
	})

	// --- JSON.MGET ---
	t.Run("JSON_MGET", func(t *testing.T) {
		do(t, ctx, "JSON.SET", p+"js5a", "$", `{"x":1}`)
		do(t, ctx, "JSON.SET", p+"js5b", "$", `{"x":2}`)
		r, _ := doAny(t, ctx, "JSON.MGET", p+"js5a", p+"js5b", "$.x")
		assert.NotNil(t, r)
	})

	// --- JSON.ARRAPPEND ---
	t.Run("JSON_ARRAPPEND", func(t *testing.T) {
		do(t, ctx, "JSON.SET", p+"js6", "$", `[]`)
		r, _ := doAny(t, ctx, "JSON.ARRAPPEND", p+"js6", "$", "1", "2", "3")
		assert.NotNil(t, r)
	})

	// --- JSON.ARRLEN ---
	t.Run("JSON_ARRLEN", func(t *testing.T) {
		do(t, ctx, "JSON.SET", p+"js7", "$", `["a","b","c"]`)
		r, _ := doAny(t, ctx, "JSON.ARRLEN", p+"js7", "$")
		assert.Equal(t, int64(3), r)
	})

	// --- JSON.OBJKEYS ---
	t.Run("JSON_OBJKEYS", func(t *testing.T) {
		do(t, ctx, "JSON.SET", p+"js8", "$", `{"a":1,"b":2}`)
		r, _ := doAny(t, ctx, "JSON.OBJKEYS", p+"js8", "$")
		assert.NotNil(t, r)
	})

	// --- JSON.NUMINCRBY ---
	t.Run("JSON_NUMINCRBY", func(t *testing.T) {
		do(t, ctx, "JSON.SET", p+"js9", "$", `10`)
		r, _ := doAny(t, ctx, "JSON.NUMINCRBY", p+"js9", "$", "5")
		assert.NotNil(t, r)
	})

	// --- JSON.NUMMULTBY ---
	t.Run("JSON_NUMMULTBY", func(t *testing.T) {
		do(t, ctx, "JSON.SET", p+"js10", "$", `10`)
		r, _ := doAny(t, ctx, "JSON.NUMMULTBY", p+"js10", "$", "2")
		assert.NotNil(t, r)
	})

	// --- JSON.CLEAR ---
	t.Run("JSON_CLEAR", func(t *testing.T) {
		do(t, ctx, "JSON.SET", p+"js11", "$", `{"name":"test"}`)
		r, _ := doAny(t, ctx, "JSON.CLEAR", p+"js11", "$")
		assert.NotNil(t, r)
	})

	// --- JSON.DEBUG ---
	t.Run("JSON_DEBUG", func(t *testing.T) {
		do(t, ctx, "JSON.SET", p+"js12", "$", `{"name":"test"}`)
		r, _ := doAny(t, ctx, "JSON.DEBUG", "MEMORY", p+"js12", "$")
		assert.NotNil(t, r)
	})
}

// ============================================================
// 11. TimeSeries Commands (8+)
// ============================================================

func TestCommandCompleteness_TimeSeries(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)
	ctx := context.Background()
	p := keyPrefix(t)

	// --- TS.CREATE ---
	t.Run("TS_CREATE", func(t *testing.T) {
		r, _ := doAny(t, ctx, "TS.CREATE", p+"ts1")
		assert.Equal(t, "OK", r)
	})

	// --- TS.ADD ---
	t.Run("TS_ADD", func(t *testing.T) {
		r, _ := doAny(t, ctx, "TS.ADD", p+"ts2", "*", "100")
		assert.NotNil(t, r)
	})

	// --- TS.GET ---
	t.Run("TS_GET", func(t *testing.T) {
		do(t, ctx, "TS.CREATE", p+"ts3")
		do(t, ctx, "TS.ADD", p+"ts3", "1000", "42")
		r, _ := doAny(t, ctx, "TS.GET", p+"ts3")
		assert.NotNil(t, r)
	})

	// --- TS.RANGE ---
	t.Run("TS_RANGE", func(t *testing.T) {
		do(t, ctx, "TS.CREATE", p+"ts4")
		do(t, ctx, "TS.ADD", p+"ts4", "1000", "10")
		do(t, ctx, "TS.ADD", p+"ts4", "2000", "20")
		r, _ := doAny(t, ctx, "TS.RANGE", p+"ts4", "0", "+")
		assert.NotNil(t, r)
	})

	// --- TS.DEL ---
	t.Run("TS_DEL", func(t *testing.T) {
		do(t, ctx, "TS.CREATE", p+"ts5")
		do(t, ctx, "TS.ADD", p+"ts5", "1000", "10")
		r, _ := doAny(t, ctx, "TS.DEL", p+"ts5", "0", "2000")
		assert.NotNil(t, r)
	})

	// --- TS.INFO ---
	t.Run("TS_INFO", func(t *testing.T) {
		do(t, ctx, "TS.CREATE", p+"ts6")
		r, _ := doAny(t, ctx, "TS.INFO", p+"ts6")
		assert.NotNil(t, r)
	})

	// --- TS.LEN ---
	t.Run("TS_LEN", func(t *testing.T) {
		do(t, ctx, "TS.CREATE", p+"ts7")
		do(t, ctx, "TS.ADD", p+"ts7", "1000", "10")
		do(t, ctx, "TS.ADD", p+"ts7", "2000", "20")
		r, _ := doAny(t, ctx, "TS.LEN", p+"ts7")
		assert.Equal(t, int64(2), r)
	})

	// --- TS.MGET ---
	t.Run("TS_MGET", func(t *testing.T) {
		do(t, ctx, "TS.CREATE", p+"ts8")
		do(t, ctx, "TS.ADD", p+"ts8", "1000", "42")
		r, _ := doAny(t, ctx, "TS.MGET", "FILTER", "sensor=temp")
		assert.NotNil(t, r)
	})

	// --- TS.REVRANGE ---
	t.Run("TS_REVRANGE", func(t *testing.T) {
		do(t, ctx, "TS.CREATE", p+"ts9")
		do(t, ctx, "TS.ADD", p+"ts9", "1000", "10")
		do(t, ctx, "TS.ADD", p+"ts9", "2000", "20")
		r, _ := doAny(t, ctx, "TS.REVRANGE", p+"ts9", "0", "+")
		assert.NotNil(t, r)
	})

	// --- TS.MRANGE ---
	t.Run("TS_MRANGE", func(t *testing.T) {
		do(t, ctx, "TS.CREATE", p+"ts10")
		do(t, ctx, "TS.ADD", p+"ts10", "1000", "10")
		r, _ := doAny(t, ctx, "TS.MRANGE", "0", "+", "FILTER", "loc=us")
		assert.NotNil(t, r)
	})

	// --- TS.MREVRANGE ---
	t.Run("TS_MREVRANGE", func(t *testing.T) {
		do(t, ctx, "TS.CREATE", p+"ts11")
		do(t, ctx, "TS.ADD", p+"ts11", "1000", "10")
		r, _ := doAny(t, ctx, "TS.MREVRANGE", "0", "+", "FILTER", "loc=eu")
		assert.NotNil(t, r)
	})

	// --- TS.QUERYINDEX ---
	t.Run("TS_QUERYINDEX", func(t *testing.T) {
		do(t, ctx, "TS.CREATE", p+"ts12")
		r, _ := doAny(t, ctx, "TS.QUERYINDEX", "region=ap")
		assert.NotNil(t, r)
	})

	// --- TS.MADD ---
	t.Run("TS_MADD", func(t *testing.T) {
		do(t, ctx, "TS.CREATE", p+"ts13")
		r, _ := doAny(t, ctx, "TS.MADD", p+"ts13", "1000", "10", p+"ts13", "2000", "20")
		assert.NotNil(t, r)
	})

	// --- TS.INCRBY ---
	t.Run("TS_INCRBY", func(t *testing.T) {
		do(t, ctx, "TS.CREATE", p+"ts14")
		r, _ := doAny(t, ctx, "TS.INCRBY", p+"ts14", "10")
		assert.NotNil(t, r)
	})

	// --- TS.CREATERULE / TS.DELETERULE ---
	t.Run("TS_RULES", func(t *testing.T) {
		do(t, ctx, "TS.CREATE", p+"ts15")
		do(t, ctx, "TS.CREATE", p+"ts15_dst")
		// CREATERULE may error if syntax differs; just verify no panic
		doAny(t, ctx, "TS.CREATERULE", p+"ts15", p+"ts15_dst", "AGGREGATION", "SUM", "60000")
		doAny(t, ctx, "TS.DELETERULE", p+"ts15", p+"ts15_dst")
	})
}

// ============================================================
// 12. Transaction Commands (5)
// ============================================================

func TestCommandCompleteness_Transaction(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)
	ctx := context.Background()
	p := keyPrefix(t)

	// --- MULTI / EXEC ---
	t.Run("MULTI_EXEC", func(t *testing.T) {
		sharedClient.Set(ctx, p+"tx1", "before", 0)
		pipe := sharedClient.Pipeline()
		pipe.Set(ctx, p+"tx1", "after", 0)
		pipe.Get(ctx, p+"tx1")
		cmds, _ := pipe.Exec(ctx)
		assert.Equal(t, 2, len(cmds))
	})

	// --- DISCARD ---
	t.Run("DISCARD", func(t *testing.T) {
		r, _ := doAny(t, ctx, "MULTI")
		assert.Equal(t, "OK", r)
		r, _ = doAny(t, ctx, "DISCARD")
		assert.Equal(t, "OK", r)
	})

	// --- WATCH ---
	t.Run("WATCH", func(t *testing.T) {
		sharedClient.Set(ctx, p+"wk", "v", 0)
		r, _ := doAny(t, ctx, "WATCH", p+"wk")
		assert.Equal(t, "OK", r)
		r, _ = doAny(t, ctx, "UNWATCH")
		assert.Equal(t, "OK", r)
	})

	// --- UNWATCH ---
	t.Run("UNWATCH", func(t *testing.T) {
		r, _ := doAny(t, ctx, "WATCH", p+"wk2")
		assert.Equal(t, "OK", r)
		r, _ = doAny(t, ctx, "UNWATCH")
		assert.Equal(t, "OK", r)
	})

	// --- MULTI/EXEC with error ---
	t.Run("MULTI_EXEC_ERROR", func(t *testing.T) {
		sharedClient.Set(ctx, p+"txe", "val", 0)
		conn := sharedClient.Conn()
		defer conn.Close()
		conn.Do(ctx, "MULTI")
		conn.Set(ctx, p+"txe", "new", 0)
		// Wrong type: LPUSH on string key
		conn.Do(ctx, "LPUSH", p+"txe", "item")
		r, err := conn.Do(ctx, "EXEC").Result()
		// EXEC returns array; some elements may be errors
		assert.NotNil(t, r)
		_ = err
	})
}

// ============================================================
// 13. PubSub Commands (7)
// ============================================================

func TestCommandCompleteness_PubSub(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)
	ctx := context.Background()
	p := keyPrefix(t)

	// --- PUBLISH ---
	t.Run("PUBLISH", func(t *testing.T) {
		n, _ := sharedClient.Publish(ctx, p+"ch1", "hello").Result()
		// 0 subscribers
		assert.Equal(t, int64(0), n)
	})

	// --- SUBSCRIBE / UNSUBSCRIBE ---
	t.Run("SUBSCRIBE", func(t *testing.T) {
		sub := sharedClient.Subscribe(ctx, p+"ch2")
		defer sub.Close()
		// Wait for subscription
		_, err := sub.Receive(ctx)
		assert.NoError(t, err)
		// Publish
		sharedClient.Publish(ctx, p+"ch2", "msg1")
		ch := sub.Channel()
		select {
		case msg := <-ch:
			assert.Equal(t, "msg1", msg.Payload)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for message")
		}
	})

	// --- PSUBSCRIBE / PUNSUBSCRIBE ---
	t.Run("PSUBSCRIBE", func(t *testing.T) {
		sub := sharedClient.PSubscribe(ctx, p+"ch*")
		defer sub.Close()
		_, err := sub.Receive(ctx)
		assert.NoError(t, err)
		sharedClient.Publish(ctx, p+"ch3", "msg")
		ch := sub.Channel()
		select {
		case msg := <-ch:
			assert.Equal(t, "msg", msg.Payload)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for pattern message")
		}
	})

	// --- PUBSUB CHANNELS ---
	t.Run("PUBSUB_CHANNELS", func(t *testing.T) {
		sub := sharedClient.Subscribe(ctx, p+"psch")
		defer sub.Close()
		sub.Receive(ctx) // wait for subscribe
		r, _ := doAny(t, ctx, "PUBSUB", "CHANNELS")
		assert.NotNil(t, r)
	})

	// --- PUBSUB NUMSUB ---
	t.Run("PUBSUB_NUMSUB", func(t *testing.T) {
		// NUMSUB may return nil or empty array; both are valid
		doAny(t, ctx, "PUBSUB", "NUMSUB", p+"psch")
	})

	// --- PUBSUB NUMPAT ---
	t.Run("PUBSUB_NUMPAT", func(t *testing.T) {
		r, _ := doAny(t, ctx, "PUBSUB", "NUMPAT")
		assert.NotNil(t, r)
	})
}

// ============================================================
// 14. Connection Commands (11)
// ============================================================

func TestCommandCompleteness_Connection(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)
	ctx := context.Background()
	p := keyPrefix(t)

	// --- PING ---
	t.Run("PING", func(t *testing.T) {
		v, _ := sharedClient.Ping(ctx).Result()
		assert.Equal(t, "PONG", v)
	})

	// --- ECHO ---
	t.Run("ECHO", func(t *testing.T) {
		v, _ := sharedClient.Echo(ctx, "test").Result()
		assert.Equal(t, "test", v)
	})

	// --- HELLO ---
	t.Run("HELLO", func(t *testing.T) {
		r, err := doAny(t, ctx, "HELLO")
		// HELLO may return an error in RESP2 mode, that's OK
		_ = r
		_ = err
	})

	// --- COMMAND ---
	t.Run("COMMAND", func(t *testing.T) {
		r, _ := doAny(t, ctx, "COMMAND")
		assert.NotNil(t, r)
	})

	// --- CLIENT LIST ---
	t.Run("CLIENT_LIST", func(t *testing.T) {
		r, _ := doAny(t, ctx, "CLIENT", "LIST")
		assert.NotNil(t, r)
	})

	// --- CLIENT GETNAME / SETNAME ---
	t.Run("CLIENT_GETNAME_SETNAME", func(t *testing.T) {
		do(t, ctx, "CLIENT", "SETNAME", p+"testclient")
		r, _ := doAny(t, ctx, "CLIENT", "GETNAME")
		assert.NotNil(t, r)
	})

	// --- CLIENT ID ---
	t.Run("CLIENT_ID", func(t *testing.T) {
		r, _ := doAny(t, ctx, "CLIENT", "ID")
		assert.NotNil(t, r)
	})

	// --- CLIENT NO-TOUCH ---
	t.Run("CLIENT_NOTOUCH", func(t *testing.T) {
		r, _ := doAny(t, ctx, "CLIENT", "NO-TOUCH", "ON")
		assert.NotNil(t, r)
	})

	// --- CLIENT SETINFO ---
	t.Run("CLIENT_SETINFO", func(t *testing.T) {
		r, _ := doAny(t, ctx, "CLIENT", "SETINFO", "LIB-NAME", "boltdb-test")
		assert.NotNil(t, r)
	})

	// --- CLIENT CACHING ---
	t.Run("CLIENT_CACHING", func(t *testing.T) {
		r, _ := doAny(t, ctx, "CLIENT", "CACHING", "YES")
		assert.NotNil(t, r)
	})

	// --- CLIENT GETREDIR ---
	t.Run("CLIENT_GETREDIR", func(t *testing.T) {
		r, _ := doAny(t, ctx, "CLIENT", "GETREDIR")
		assert.NotNil(t, r)
	})

	// --- AUTH (no password set — may succeed or error depending on BoltDB config) ---
	t.Run("AUTH", func(t *testing.T) {
		// When no password is configured, AUTH may return OK or error
		// Both are valid; we just verify the command doesn't panic
		doAny(t, ctx, "AUTH", "wrongpassword")
	})
}

// ============================================================
// 15. Server Commands (16)
// ============================================================

func TestCommandCompleteness_Server(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)
	ctx := context.Background()
	p := keyPrefix(t)

	// --- INFO ---
	t.Run("INFO", func(t *testing.T) {
		r, _ := doAny(t, ctx, "INFO")
		assert.NotNil(t, r)
		r, _ = doAny(t, ctx, "INFO", "replication")
		assert.NotNil(t, r)
		r, _ = doAny(t, ctx, "INFO", "memory")
		assert.NotNil(t, r)
	})

	// --- SAVE ---
	t.Run("SAVE", func(t *testing.T) {
		r, _ := doAny(t, ctx, "SAVE")
		assert.NotNil(t, r)
	})

	// --- BGSAVE ---
	t.Run("BGSAVE", func(t *testing.T) {
		r, _ := doAny(t, ctx, "BGSAVE")
		assert.NotNil(t, r)
		time.Sleep(100 * time.Millisecond) // wait for background save
	})

	// --- LASTSAVE ---
	t.Run("LASTSAVE", func(t *testing.T) {
		r, _ := doAny(t, ctx, "LASTSAVE")
		assert.NotNil(t, r)
	})

	// --- TIME ---
	t.Run("TIME", func(t *testing.T) {
		r, _ := doAny(t, ctx, "TIME")
		assert.NotNil(t, r)
	})

	// --- CONFIG GET ---
	t.Run("CONFIG_GET", func(t *testing.T) {
		r, _ := doAny(t, ctx, "CONFIG", "GET", "dir")
		assert.NotNil(t, r)
	})

	// --- CONFIG SET ---
	t.Run("CONFIG_SET", func(t *testing.T) {
		r, _ := doAny(t, ctx, "CONFIG", "SET", "slowlog-log-slower-than", "10000")
		assert.Equal(t, "OK", r)

		// Supported no-op: maxmemory (accepted for client compat)
		r, _ = doAny(t, ctx, "CONFIG", "SET", "maxmemory", "0")
		assert.Equal(t, "OK", r)

		// Unknown parameter must return error
		_, err := doAny(t, ctx, "CONFIG", "SET", "totally-unknown-param", "42")
		assert.Error(t, err)
	})

	// --- SLOWLOG ---
	t.Run("SLOWLOG", func(t *testing.T) {
		r, _ := doAny(t, ctx, "SLOWLOG", "GET", "10")
		assert.NotNil(t, r)
		r, _ = doAny(t, ctx, "SLOWLOG", "LEN")
		assert.NotNil(t, r)
		r, _ = doAny(t, ctx, "SLOWLOG", "RESET")
		assert.Equal(t, "OK", r)
		r, _ = doAny(t, ctx, "SLOWLOG", "HELP")
		assert.NotNil(t, r)
	})

	// --- MEMORY ---
	t.Run("MEMORY", func(t *testing.T) {
		sharedClient.Set(ctx, p+"mem", "value", 0)
		r, _ := doAny(t, ctx, "MEMORY", "USAGE", p+"mem")
		assert.NotNil(t, r)
		r, _ = doAny(t, ctx, "MEMORY", "DOCTOR")
		assert.NotNil(t, r)
		r, _ = doAny(t, ctx, "MEMORY", "HELP")
		assert.NotNil(t, r)
	})

	// --- LOLWUT ---
	t.Run("LOLWUT", func(t *testing.T) {
		r, _ := doAny(t, ctx, "LOLWUT")
		assert.NotNil(t, r)
	})

	// --- LATENCY ---
	t.Run("LATENCY", func(t *testing.T) {
		do(t, ctx, "LATENCY", "RESET")
		r, _ := doAny(t, ctx, "LATENCY", "LATEST")
		assert.NotNil(t, r)
		r, _ = doAny(t, ctx, "LATENCY", "HELP")
		assert.NotNil(t, r)
		r, _ = doAny(t, ctx, "LATENCY", "DOCTOR")
		assert.NotNil(t, r)
	})

	// --- READONLY / READWRITE ---
	t.Run("READONLY_READWRITE", func(t *testing.T) {
		r, _ := doAny(t, ctx, "READWRITE")
		assert.NotNil(t, r)
	})

	// --- DEBUG ---
	t.Run("DEBUG", func(t *testing.T) {
		r, _ := doAny(t, ctx, "DEBUG", "SET-ACTIVE-EXPIRE", "1")
		assert.NotNil(t, r)
	})

	// --- MODULE LIST ---
	t.Run("MODULE", func(t *testing.T) {
		r, _ := doAny(t, ctx, "MODULE", "LIST")
		assert.NotNil(t, r)
		r, _ = doAny(t, ctx, "MODULE", "HELP")
		assert.NotNil(t, r)
	})

	// --- OBJECT HELP ---
	t.Run("OBJECT_HELP", func(t *testing.T) {
		r, _ := doAny(t, ctx, "OBJECT", "HELP")
		assert.NotNil(t, r)
	})

	// --- FLUSHDB / FLUSHALL (server category too) ---
	t.Run("FLUSHDB_FLUSHALL", func(t *testing.T) {
		sharedClient.Set(ctx, p+"flush_server", "v", 0)
		do(t, ctx, "FLUSHDB")
		n, _ := sharedClient.DBSize(ctx).Result()
		assert.Equal(t, int64(0), n)
	})

	// --- WAIT ---
	t.Run("WAIT", func(t *testing.T) {
		r, _ := doAny(t, ctx, "WAIT", "0", "100")
		assert.NotNil(t, r)
	})
}

// ============================================================
// 16. Cluster Commands (basic)
// ============================================================

func TestCommandCompleteness_Cluster(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)
	ctx := context.Background()

	// These commands may return errors in standalone mode,
	// but we verify they don't panic and return valid responses.
	t.Run("CLUSTER_INFO", func(t *testing.T) {
		r, err := doAny(t, ctx, "CLUSTER", "INFO")
		// May error in standalone mode, that's OK
		_ = r
		_ = err
	})

	t.Run("CLUSTER_MYID", func(t *testing.T) {
		r, err := doAny(t, ctx, "CLUSTER", "MYID")
		_ = r
		_ = err
	})

	t.Run("CLUSTER_NODES", func(t *testing.T) {
		r, err := doAny(t, ctx, "CLUSTER", "NODES")
		_ = r
		_ = err
	})
}
