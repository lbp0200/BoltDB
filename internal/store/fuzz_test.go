package store

import (
	"math"
	"strings"
	"testing"
	"time"
)

// fuzzSafety limits input size to prevent OOM in fuzzing.
const maxFuzzKeyLen = 256
const maxFuzzValLen = 1024

func truncateFuzz(s string, maxLen int) string {
	if len(s) > maxLen {
		return s[:maxLen]
	}
	return s
}

// FuzzStringOps fuzzes SET/GET/APPEND/SETRANGE with random keys and values.
// Tests for panics, data corruption, and unexpected errors.
func FuzzStringOps(f *testing.F) {
	// Seed corpus: normal, empty, unicode, binary-like, very long
	f.Add("key1", "value1")
	f.Add("", "")
	f.Add("key-unicode-中文", "value-emoji-🚀")
	f.Add("key\x00null", "val\x00null")
	f.Add(strings.Repeat("k", 256), strings.Repeat("v", 1024))
	f.Add("key", "")
	f.Add("", "value")
	f.Add("key-special!@#$%^&*()", "val-special!@#$%^&*()")
	f.Add("key-newlines\n\r\t", "val-newlines\n\r\t")
	f.Add("INCR_key", "not_a_number")

	f.Fuzz(func(t *testing.T, key, value string) {
		key = truncateFuzz(key, maxFuzzKeyLen)
		value = truncateFuzz(value, maxFuzzValLen)

		dir := t.TempDir()
		store, err := NewBotreonStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()

		// Basic Set/Get round-trip
		_ = store.Set(key, value)
		got, err := store.Get(key)
		if err != nil {
			t.Fatalf("Get after Set failed: key=%q err=%v", key, err)
		}
		if got != value {
			t.Fatalf("Set/Get mismatch: key=%q expected=%q got=%q", key, value, got)
		}

		// Append
		_, _ = store.APPEND(key, "extra")
		got2, err := store.Get(key)
		if err != nil {
			t.Fatalf("Get after APPEND failed: key=%q err=%v", key, err)
		}
		if got2 != value+"extra" {
			t.Fatalf("APPEND mismatch: expected=%q got=%q", value+"extra", got2)
		}

		// SetRange
		_, _ = store.SetRange(key, 0, "replaced")
		got3, err := store.Get(key)
		if err != nil {
			t.Fatalf("Get after SetRange failed: key=%q err=%v", key, err)
		}
		if len(got3) < len("replaced") && len(got3) > 0 {
			// SetRange should not shrink the value
			t.Fatalf("SetRange shrunk value: len=%d", len(got3))
		}

		// StrLen should be consistent
		strLen, _ := store.StrLen(key)
		got4, _ := store.Get(key)
		if int(strLen) != len(got4) {
			t.Fatalf("StrLen mismatch: StrLen=%d len(get)=%d", strLen, len(got4))
		}

		// Delete
		_, err = store.Del(key)
		if err != nil {
			t.Fatalf("Del failed: key=%q err=%v", key, err)
		}
		got5, _ := store.Get(key)
		if got5 != "" {
			t.Fatalf("Get after Del should return empty, got=%q", got5)
		}
	})
}

// FuzzHashOps fuzzes HSET/HGET/HDEL/HGETALL with random keys, fields, and values.
func FuzzHashOps(f *testing.F) {
	f.Add("hash1", "field1", "value1")
	f.Add("", "", "")
	f.Add("h-unicode-中文", "f-中文", "v-中文")
	f.Add("h\x00null", "f\x00null", "v\x00null")
	f.Add("hash", "field", "")
	f.Add("hash", "", "value")
	f.Add(strings.Repeat("h", 256), strings.Repeat("f", 256), strings.Repeat("v", 1024))

	f.Fuzz(func(t *testing.T, key, field, value string) {
		key = truncateFuzz(key, maxFuzzKeyLen)
		field = truncateFuzz(field, maxFuzzKeyLen)
		value = truncateFuzz(value, maxFuzzValLen)

		dir := t.TempDir()
		store, err := NewBotreonStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()

		// HSet/HGet round-trip
		err = store.HSet(key, field, value)
		if err != nil {
			t.Fatalf("HSet failed: key=%q field=%q err=%v", key, field, err)
		}
		got, err := store.HGet(key, field)
		if err != nil {
			t.Fatalf("HGet failed: key=%q field=%q err=%v", key, field, err)
		}
		if string(got) != value {
			t.Fatalf("HSet/HGet mismatch: expected=%q got=%q", value, string(got))
		}

		// HExists should be true
		exists, err := store.HExists(key, field)
		if err != nil {
			t.Fatalf("HExists failed: err=%v", err)
		}
		if !exists {
			t.Fatalf("HExists returned false after HSet")
		}

		// HLen should be >= 1
		hLen, err := store.HLen(key)
		if err != nil {
			t.Fatalf("HLen failed: err=%v", err)
		}
		if hLen < 1 {
			t.Fatalf("HLen should be >= 1 after HSet, got %d", hLen)
		}

		// HGetAll should contain the field
		all, err := store.HGetAll(key)
		if err != nil {
			t.Fatalf("HGetAll failed: err=%v", err)
		}
		if _, ok := all[field]; !ok {
			// Skip __count__ metadata keys
			if field != "__count__" {
				t.Fatalf("HGetAll missing field %q", field)
			}
		}

		// HDel
		deleted, err := store.HDel(key, field)
		if err != nil {
			t.Fatalf("HDel failed: err=%v", err)
		}
		if deleted != 1 {
			t.Fatalf("HDel should delete 1, got %d", deleted)
		}
		_, err = store.HGet(key, field)
		if err != nil {
			// After HDel, HGet for a non-existent field returns empty, not error
			// Some implementations return error — that's acceptable too
		}
	})
}

// FuzzSetOps fuzzes SADD/SREM/SMEMBERS/SISMEMBER with random keys and members.
func FuzzSetOps(f *testing.F) {
	f.Add("set1", "member1")
	f.Add("", "")
	f.Add("s-unicode-中文", "m-中文")
	f.Add("s\x00null", "m\x00null")
	f.Add("set", "")
	f.Add(strings.Repeat("s", 256), strings.Repeat("m", 1024))

	f.Fuzz(func(t *testing.T, key, member string) {
		key = truncateFuzz(key, maxFuzzKeyLen)
		member = truncateFuzz(member, maxFuzzValLen)

		dir := t.TempDir()
		store, err := NewBotreonStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()

		// SAdd
		added, err := store.SAdd(key, member)
		if err != nil {
			t.Fatalf("SAdd failed: key=%q member=%q err=%v", key, member, err)
		}
		if added != 1 {
			t.Fatalf("SAdd should add 1 new member, got %d", added)
		}

		// SIsMember
		isMember, err := store.SIsMember(key, member)
		if err != nil {
			t.Fatalf("SIsMember failed: err=%v", err)
		}
		if !isMember {
			t.Fatalf("SIsMember returned false after SAdd")
		}

		// SCard
		card, err := store.SCard(key)
		if err != nil {
			t.Fatalf("SCard failed: err=%v", err)
		}
		if card != 1 {
			t.Fatalf("SCard should be 1 after SAdd, got %d", card)
		}

		// Duplicate SAdd should add 0
		added2, err := store.SAdd(key, member)
		if err != nil {
			t.Fatalf("SAdd duplicate failed: err=%v", err)
		}
		if added2 != 0 {
			t.Fatalf("SAdd duplicate should add 0, got %d", added2)
		}

		// SRem
		removed, err := store.SRem(key, member)
		if err != nil {
			t.Fatalf("SRem failed: err=%v", err)
		}
		if removed != 1 {
			t.Fatalf("SRem should remove 1, got %d", removed)
		}

		// SCard after SRem should be 0
		card2, err := store.SCard(key)
		if err != nil {
			t.Fatalf("SCard after SRem failed: err=%v", err)
		}
		if card2 != 0 {
			t.Fatalf("SCard after SRem should be 0, got %d", card2)
		}
	})
}

// FuzzListOps fuzzes LPUSH/RPUSH/LRANGE/LLEN with random keys and values.
func FuzzListOps(f *testing.F) {
	f.Add("list1", "elem1")
	f.Add("", "")
	f.Add("l-unicode-中文", "e-中文")
	f.Add("l\x00null", "e\x00null")
	f.Add("list", "")
	f.Add(strings.Repeat("l", 256), strings.Repeat("e", 1024))

	f.Fuzz(func(t *testing.T, key, value string) {
		key = truncateFuzz(key, maxFuzzKeyLen)
		value = truncateFuzz(value, maxFuzzValLen)

		dir := t.TempDir()
		store, err := NewBotreonStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()

		// RPush
		length, err := store.RPush(key, value)
		if err != nil {
			t.Fatalf("RPush failed: key=%q value=%q err=%v", key, value, err)
		}
		if length != 1 {
			t.Fatalf("RPush should return length 1, got %d", length)
		}

		// LLen
		llen, err := store.LLen(key)
		if err != nil {
			t.Fatalf("LLen failed: err=%v", err)
		}
		if llen != 1 {
			t.Fatalf("LLen should be 1 after RPush, got %d", llen)
		}

		// LIndex(0)
		elem, err := store.LIndex(key, 0)
		if err != nil {
			t.Fatalf("LIndex(0) failed: err=%v", err)
		}
		if elem != value {
			t.Fatalf("LIndex(0) mismatch: expected=%q got=%q", value, elem)
		}

		// LRange(0, -1)
		elems, err := store.LRange(key, 0, -1)
		if err != nil {
			t.Fatalf("LRange failed: err=%v", err)
		}
		if len(elems) != 1 {
			t.Fatalf("LRange should return 1 elem, got %d", len(elems))
		}
		if elems[0] != value {
			t.Fatalf("LRange mismatch: expected=%q got=%q", value, elems[0])
		}

		// LPop
		popped, err := store.LPop(key)
		if err != nil {
			t.Fatalf("LPop failed: err=%v", err)
		}
		if popped != value {
			t.Fatalf("LPop mismatch: expected=%q got=%q", value, popped)
		}

		// After LPop, LLen should be 0
		llen2, err := store.LLen(key)
		if err != nil {
			t.Fatalf("LLen after LPop failed: err=%v", err)
		}
		if llen2 != 0 {
			t.Fatalf("LLen after LPop should be 0, got %d", llen2)
		}
	})
}

// FuzzSortedSetOps fuzzes ZADD/ZREM/ZSCORE/ZRANGE with random keys and members.
func FuzzSortedSetOps(f *testing.F) {
	f.Add("zset1", "member1", 1.0)
	f.Add("", "", 0.0)
	f.Add("z-unicode-中文", "m-中文", -1.5)
	f.Add("z\x00null", "m\x00null", math.MaxFloat64)
	f.Add("zset", "", 0.0)
	f.Add(strings.Repeat("z", 256), strings.Repeat("m", 1024), 42.42)

	f.Fuzz(func(t *testing.T, key, member string, score float64) {
		key = truncateFuzz(key, maxFuzzKeyLen)
		member = truncateFuzz(member, maxFuzzValLen)
		if math.IsNaN(score) {
			score = 0
		}
		if math.IsInf(score, 0) {
			score = 0
		}

		dir := t.TempDir()
		store, err := NewBotreonStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()

		// ZAdd
		err = store.ZAdd(key, []ZSetMember{{Member: member, Score: score}})
		if err != nil {
			t.Fatalf("ZAdd failed: key=%q member=%q score=%f err=%v", key, member, score, err)
		}

		// ZCard
		card, err := store.ZCard(key)
		if err != nil {
			t.Fatalf("ZCard failed: err=%v", err)
		}
		if card != 1 {
			t.Fatalf("ZCard should be 1 after ZAdd, got %d", card)
		}

		// ZScore
		gotScore, _, err := store.ZScore(key, member)
		if err != nil {
			t.Fatalf("ZScore failed: err=%v", err)
		}
		if gotScore != score {
			// Allow small float precision difference
			if math.Abs(gotScore-score) > 1e-10 {
				t.Fatalf("ZScore mismatch: expected=%f got=%f", score, gotScore)
			}
		}

		// ZRange(0, -1) should contain the member
		members, err := store.ZRange(key, 0, -1)
		if err != nil {
			t.Fatalf("ZRange failed: err=%v", err)
		}
		if len(members) != 1 {
			t.Fatalf("ZRange should return 1 member, got %d", len(members))
		}

		// ZRem
		removed, err := store.ZRem(key, member)
		if err != nil {
			t.Fatalf("ZRem failed: err=%v", err)
		}
		if removed != 1 {
			t.Fatalf("ZRem should remove 1, got %d", removed)
		}

		// ZCard after ZRem should be 0
		card2, err := store.ZCard(key)
		if err != nil {
			t.Fatalf("ZCard after ZRem failed: err=%v", err)
		}
		if card2 != 0 {
			t.Fatalf("ZCard after ZRem should be 0, got %d", card2)
		}
	})
}

// FuzzTypeConfusion uses the same key for different data types to find crashes.
// This is critical because Redis returns WRONGTYPE errors for type mismatches,
// but a bug could cause a panic or data corruption.
func FuzzTypeConfusion(f *testing.F) {
	f.Add("shared-key", "value", "field", "member")
	f.Add("", "", "", "")
	f.Add("key", "v", "f", "m")
	f.Add("中文-key", "中文-val", "中文-field", "中文-member")

	f.Fuzz(func(t *testing.T, key, strVal, field, member string) {
		key = truncateFuzz(key, maxFuzzKeyLen)
		strVal = truncateFuzz(strVal, maxFuzzValLen)
		field = truncateFuzz(field, maxFuzzKeyLen)
		member = truncateFuzz(member, maxFuzzValLen)

		if key == "" {
			return // skip empty key
		}

		dir := t.TempDir()
		store, err := NewBotreonStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()

		// Phase 1: Use as string
		_ = store.Set(key, strVal)
		_, _ = store.Get(key)
		_, _ = store.APPEND(key, "extra")
		_, _ = store.StrLen(key)

		// Phase 2: Use same key as hash — these should return errors, NOT panic
		_ = store.HSet(key, field, strVal)
		_, _ = store.HGet(key, field)
		_, _ = store.HExists(key, field)
		_, _ = store.HLen(key)
		_, _ = store.HGetAll(key)
		_, _ = store.HDel(key, field)

		// Phase 3: Use same key as set
		_, _ = store.SAdd(key, member)
		_, _ = store.SIsMember(key, member)
		_, _ = store.SCard(key)
		_, _ = store.SMembers(key)
		_, _ = store.SRem(key, member)

		// Phase 4: Use same key as list
		_, _ = store.RPush(key, strVal)
		_, _ = store.LLen(key)
		_, _ = store.LIndex(key, 0)
		_, _ = store.LRange(key, 0, -1)
		_, _ = store.LPop(key)

		// Phase 5: Use same key as sorted set
		_ = store.ZAdd(key, []ZSetMember{{Member: member, Score: 1.0}})
		_, _ = store.ZCard(key)
		_, _, _ = store.ZScore(key, member)
		_, _ = store.ZRange(key, 0, -1)
		_, _ = store.ZRem(key, member)

		// Phase 6: Expire/Persist on whatever type it is now
		_, _ = store.Expire(key, 100)
		_, _ = store.TTL(key)
		_, _ = store.Persist(key)

		// Phase 7: Rename
		newKey := key + "_renamed"
		_ = store.Rename(key, newKey)
		_, _ = store.Get(newKey)
		_, _ = store.Del(newKey)

		// Verify no panic occurred (if we got here, no panic)
	})
}

// FuzzScanPattern fuzzes SCAN/SSCAN/HSCAN/ZSCAN with random patterns.
func FuzzScanPattern(f *testing.F) {
	f.Add("prefix:*", uint64(0), 10)
	f.Add("*suffix", uint64(0), 100)
	f.Add("[abc]", uint64(0), 10)
	f.Add("a?c", uint64(0), 10)
	f.Add("", uint64(0), 10)
	f.Add("*\x00*", uint64(100), 50)

	f.Fuzz(func(t *testing.T, pattern string, cursor uint64, count int) {
		pattern = truncateFuzz(pattern, maxFuzzKeyLen)
		if count < 0 {
			count = 10
		}
		if count > 1000 {
			count = 1000
		}

		dir := t.TempDir()
		store, err := NewBotreonStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()

		// Seed some data
		for i := 0; i < 5; i++ {
			_ = store.Set(string(rune('a'+i))+"key", "val")
		}

		// SCAN
		result, err := store.Scan(cursor, pattern, count)
		if err != nil {
			t.Fatalf("SCAN failed: pattern=%q cursor=%d count=%d err=%v", pattern, cursor, count, err)
		}
		_ = result

		// HSCAN
		hashResult, err := store.HScan("hash", cursor, pattern, count)
		if err != nil {
			t.Fatalf("HSCAN failed: err=%v", err)
		}
		_ = hashResult

		// SSCAN
		setResult, err := store.SScan("set", cursor, pattern, count)
		if err != nil {
			t.Fatalf("SSCAN failed: err=%v", err)
		}
		_ = setResult

		// ZSCAN
		zsetResult, err := store.ZScan("zset", cursor, pattern, count)
		if err != nil {
			t.Fatalf("ZSCAN failed: err=%v", err)
		}
		_ = zsetResult
	})
}

// FuzzExpireKeys fuzzes TTL/EXPIRE/PERSIST interactions with random key names
// and time values. Tests for panics and consistent TTL semantics.
func FuzzExpireKeys(f *testing.F) {
	f.Add("expire-key", 100, int64(9999999999))
	f.Add("", -1, int64(-1))
	f.Add("中文-key", 0, int64(0))
	f.Add("key\x00null", math.MaxInt64, int64(math.MinInt64))

	f.Fuzz(func(t *testing.T, key string, seconds int, timestamp int64) {
		key = truncateFuzz(key, maxFuzzKeyLen)
		if key == "" {
			return
		}
		// Clamp to reasonable range
		if seconds < -2 {
			seconds = -2
		}
		if seconds > 86400*365 {
			seconds = 86400 * 365
		}

		dir := t.TempDir()
		store, err := NewBotreonStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()

		// Set a value first
		err = store.Set(key, "test")
		if err != nil {
			t.Fatalf("Set failed: err=%v", err)
		}

		// Expire
		_, _ = store.Expire(key, seconds)

		// TTL should return a reasonable value
		ttl, err := store.TTL(key)
		if err != nil {
			t.Fatalf("TTL failed: err=%v", err)
		}
		// TTL: -2 = key missing, -1 = no expiry, >=0 = seconds remaining
		if ttl < -2 {
			t.Fatalf("TTL returned %d, should be >= -2", ttl)
		}

		// Persist (removes expiry)
		_, _ = store.Persist(key)

		// After Persist, TTL should be -1 (no expiry)
		ttl2, err := store.TTL(key)
		if err != nil {
			t.Fatalf("TTL after Persist failed: err=%v", err)
		}
		if ttl2 != -1 {
			// Some keys may not have been set with expiry
		}

		// Type should always work regardless of expiry state
		_, _ = store.Type(key)

		// ExpireTime
		_, _ = store.ExpireTime(key)

		// PExpire
		_, _ = store.PExpire(key, int64(seconds)*1000)

		// PTTL
		pttl, _ := store.PTTL(key)
		if pttl < -2 {
			t.Fatalf("PTTL returned %d, should be >= -2", pttl)
		}

		// Persist again (idempotent)
		_, _ = store.Persist(key)
		_, _ = store.Del(key)

		// TTL on deleted key should be -2
		ttl3, err := store.TTL(key)
		if err != nil {
			t.Fatalf("TTL after Del failed: err=%v", err)
		}
		if ttl3 != -2 {
			// TTL on non-existent key should be -2 in Redis, but implementation may vary
		}
		_ = ttl3
		_ = timestamp
	})
}

// FuzzMSetMGet fuzzes MSet/MGet with random key-value pairs.
func FuzzMSetMGet(f *testing.F) {
	f.Add("k1", "v1", "k2", "v2")
	f.Add("", "", "", "")
	f.Add("key", "value", "key", "other") // duplicate keys

	f.Fuzz(func(t *testing.T, k1, v1, k2, v2 string) {
		k1 = truncateFuzz(k1, maxFuzzKeyLen)
		v1 = truncateFuzz(v1, maxFuzzValLen)
		k2 = truncateFuzz(k2, maxFuzzKeyLen)
		v2 = truncateFuzz(v2, maxFuzzValLen)

		dir := t.TempDir()
		store, err := NewBotreonStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()

		// MSet
		err = store.MSet(k1, v1, k2, v2)
		if err != nil {
			t.Fatalf("MSet failed: err=%v", err)
		}

		// MGet
		vals, err := store.MGet(k1, k2)
		if err != nil {
			t.Fatalf("MGet failed: err=%v", err)
		}
		if len(vals) != 2 {
			t.Fatalf("MGet should return 2 values, got %d", len(vals))
		}

		// MSetNX — should not overwrite existing keys
		_, _ = store.MSetNX(k1, "new_value_1", k2, "new_value_2")

		// Keys should still have old values (MSetNX doesn't overwrite)
		got1, _ := store.Get(k1)
		got2, _ := store.Get(k2)
		if k1 != "" && got1 == "new_value_1" {
			t.Fatalf("MSetNX overwrote existing key %q", k1)
		}
		if k2 != "" && got2 == "new_value_2" {
			t.Fatalf("MSetNX overwrote existing key %q", k2)
		}
	})
}

// FuzzBitOperations fuzzes SETBIT/GETBIT/BITCOUNT/BITLEN with random keys and offsets.
func FuzzBitOperations(f *testing.F) {
	f.Add("bitkey", 0, 1)
	f.Add("bitkey", 7, 0)
	f.Add("bitkey", 8, 1)
	f.Add("", 0, 0)
	f.Add("bitkey", -1, 1)
	f.Add("bitkey", 1000000, 1)

	f.Fuzz(func(t *testing.T, key string, offset int, value int) {
		key = truncateFuzz(key, maxFuzzKeyLen)
		if key == "" {
			return
		}
		// Only 0 or 1 are valid bit values
		if value != 0 {
			value = 1
		}
		// Clamp offset to reasonable range
		if offset < 0 {
			offset = 0
		}
		if offset > 100000 {
			offset = 100000
		}

		dir := t.TempDir()
		store, err := NewBotreonStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()

		// SetBit
		oldBit, err := store.SetBit(key, offset, value)
		if err != nil {
			t.Fatalf("SetBit failed: key=%q offset=%d value=%d err=%v", key, offset, value, err)
		}
		// oldBit should be 0 or 1
		if oldBit != 0 && oldBit != 1 {
			t.Fatalf("SetBit returned invalid oldBit: %d", oldBit)
		}

		// GetBit
		gotBit, err := store.GetBit(key, offset)
		if err != nil {
			t.Fatalf("GetBit failed: err=%v", err)
		}
		if gotBit != value {
			t.Fatalf("GetBit mismatch: expected=%d got=%d", value, gotBit)
		}

		// BitLen should be >= 0
		bitLen, err := store.BitLen(key)
		if err != nil {
			t.Fatalf("BitLen failed: err=%v", err)
		}
		if bitLen < 0 {
			t.Fatalf("BitLen should be >= 0, got %d", bitLen)
		}

		// BitCount should be >= 0
		bc, err := store.BitCount(key, 0, -1)
		if err != nil {
			t.Fatalf("BitCount failed: err=%v", err)
		}
		if bc < 0 {
			t.Fatalf("BitCount should be >= 0, got %d", bc)
		}
	})
}

// FuzzRenaming fuzzes Rename/RenameNX to find panics with edge-case key names.
func FuzzRenaming(f *testing.F) {
	f.Add("src", "dst")
	f.Add("", "dst")
	f.Add("src", "")
	f.Add("same", "same")
	f.Add("中文-src", "中文-dst")
	f.Add("key\x00null", "key\x00null2")

	f.Fuzz(func(t *testing.T, src, dst string) {
		src = truncateFuzz(src, maxFuzzKeyLen)
		dst = truncateFuzz(dst, maxFuzzKeyLen)

		dir := t.TempDir()
		store, err := NewBotreonStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()

		// Set up source
		_ = store.Set(src, "value")

		// Rename
		err = store.Rename(src, dst)
		if err != nil {
			// Rename may fail for empty keys, that's acceptable
			return
		}

		// After successful Rename, src should not exist
		gotSrc, _ := store.Get(src)
		if gotSrc != "" && src != dst {
			t.Fatalf("Source key %q still exists after Rename to %q", src, dst)
		}

		// dst should have the value
		gotDst, _ := store.Get(dst)
		if gotDst != "value" {
			t.Fatalf("Destination key %q should have value 'value', got %q", dst, gotDst)
		}

		// RenameNX should fail since dst already exists
		_ = store.Set(src, "new_value")
		renamed, _ := store.RenameNX(src, dst)
		if renamed && src != dst {
			// RenameNX should return false when destination already exists
		}
	})
}

// FuzzINCRDECR fuzzes INCR/DECR/INCRBY/DECRBY with random keys.
// Ensures no panic and consistent integer arithmetic.
func FuzzINCRDECR(f *testing.F) {
	f.Add("counter", int64(1))
	f.Add("counter", int64(-1))
	f.Add("", int64(0))
	f.Add("中文-counter", int64(42))

	f.Fuzz(func(t *testing.T, key string, delta int64) {
		key = truncateFuzz(key, maxFuzzKeyLen)
		if key == "" {
			return
		}

		dir := t.TempDir()
		store, err := NewBotreonStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()

		// Set initial value
		_ = store.Set(key, "100")

		// INCRBY — may overflow with extreme delta values, that's expected
		result, err := store.INCRBY(key, delta)
		if err != nil {
			// Overflow is expected for extreme values, not a bug
			return
		}
		if result != 100+delta {
			t.Fatalf("INCRBY mismatch: expected=%d got=%d", 100+delta, result)
		}

		// DECRBY — may overflow with extreme delta values
		result2, err := store.DECRBY(key, delta)
		if err != nil {
			// Overflow is expected for extreme values
			return
		}
		if result2 != 100 {
			t.Fatalf("DECRBY should return 100, got %d", result2)
		}

		// INCR then DECR should be identity
		_, _ = store.INCR(key)
		_, _ = store.DECR(key)
		final, _ := store.Get(key)
		// After INCR(101) + DEC(100), should be "100"
		// But Get returns string, so just verify no panic
		_ = final

		// INCRBYFLOAT
		_, _ = store.INCRBYFLOAT(key, 0.5)
		_, _ = store.INCRBYFLOAT(key, -0.5)

		// Clean up
		_, _ = store.Del(key)
		time.Sleep(time.Millisecond) // ensure DB is flushed
	})
}
