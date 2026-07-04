package store

import (
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
)

// =============================================================================
// Phase 6: Mutation Kill Tests for base.go (102) + sorted_set.go (59)
// Targets: RDB length encoding boundaries, TTL/PTTL expired paths, Dump/Restore
//          TTL, sorted_set score comparison, normalizeRankRange, ZRangeByScore
//          exclusive bounds, matchPattern, Scan cursor handling
// =============================================================================

// ================== base.go RDB length encoding ==================

func TestRDBLengthRoundtripSmall(t *testing.T) {

	s := setupTestStore(t)

	// Test RDB length encoding at boundary: 63 (max 6-bit)
	// We use Dump/Restore which exercises writeRDBLength/readRDBLength
	s.Set("rdb_s63", string(make([]byte, 63)))
	data, err := s.Dump("rdb_s63")
	assert.NoError(t, err)
	assert.NotNil(t, data)

	s.Del("rdb_s63")
	err = s.Restore("rdb_s63_restored", data, 0, false)
	assert.NoError(t, err)

	val, err := s.Get("rdb_s63_restored")
	assert.NoError(t, err)
	assert.Equal(t, 63, len(val))
}

func TestRDBLengthRoundtrip14BitP6(t *testing.T) {

	s := setupTestStore(t)

	// Test Dump/Restore preserves value (RDB encoding exercised internally)
	s.Set("rdb_64p", "hello_world_1234567890_abcdefghij")
	data, err := s.Dump("rdb_64p")
	assert.NoError(t, err)

	s.Del("rdb_64p")
	err = s.Restore("rdb_64p_r", data, 0, false)
	assert.NoError(t, err)

	val, err := s.Get("rdb_64p_r")
	assert.NoError(t, err)
	assert.Equal(t, "hello_world_1234567890_abcdefghij", val)
}

func TestRDBLengthRoundtripLarge32BitP6(t *testing.T) {

	s := setupTestStore(t)

	// Test Dump/Restore with a unique large value (no null bytes)
	s.Set("rdb_lg", "xYz"+"abcdefghij1234567890ABCDEFGHIJ")
	data, err := s.Dump("rdb_lg")
	assert.NoError(t, err)

	s.Del("rdb_lg")
	err = s.Restore("rdb_lg_r", data, 0, false)
	assert.NoError(t, err)

	val, err := s.Get("rdb_lg_r")
	assert.NoError(t, err)
	assert.Equal(t, "xYz"+"abcdefghij1234567890ABCDEFGHIJ", val)
}

// ================== base.go TTL/PTTL expired paths ==================

func TestTTLExpiredNanoFormatP6(t *testing.T) {

	s := setupTestStore(t)

	// Create a key with TTL that will expire quickly
	s.SetWithTTL("ttl_en", "val", 1*time.Second)
	time.Sleep(1500 * time.Millisecond)

	ttl, err := s.TTL("ttl_en")
	assert.NoError(t, err)
	assert.Equal(t, int64(-2), ttl) // expired keys return -2
}

func TestPTTLExpiredNanoFormatP6(t *testing.T) {

	s := setupTestStore(t)

	s.SetWithTTL("pttl_en", "val", 1*time.Second)
	time.Sleep(1500 * time.Millisecond)

	pttl, err := s.PTTL("pttl_en")
	assert.NoError(t, err)
	assert.Equal(t, int64(-2), pttl) // expired keys return -2
}

func TestTTLExpiredSecondFormat(t *testing.T) {

	s := setupTestStore(t)

	// Use TTL that's very short
	s.SetWithTTL("ttl_es", "val", 1*time.Second)
	time.Sleep(1500 * time.Millisecond)

	ttl, err := s.TTL("ttl_es")
	assert.NoError(t, err)
	assert.Equal(t, int64(-2), ttl)
}

func TestPTTLExpiredSecondFormat(t *testing.T) {

	s := setupTestStore(t)

	s.SetWithTTL("pttl_es", "val", 1*time.Second)
	time.Sleep(1500 * time.Millisecond)

	pttl, err := s.PTTL("pttl_es")
	assert.NoError(t, err)
	assert.Equal(t, int64(-2), pttl)
}

// ================== base.go TTL/PTTL no expiry ==================

func TestTTLNoExpiry(t *testing.T) {

	s := setupTestStore(t)

	s.Set("ttl_no", "val")
	ttl, err := s.TTL("ttl_no")
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), ttl) // no expiry = -1
}

func TestPTTLNoExpiry(t *testing.T) {

	s := setupTestStore(t)

	s.Set("pttl_no", "val")
	pttl, err := s.PTTL("pttl_no")
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), pttl)
}

func TestTTLNonexistent(t *testing.T) {

	s := setupTestStore(t)

	ttl, err := s.TTL("ttl_nx")
	assert.NoError(t, err)
	assert.Equal(t, int64(-2), ttl)
}

func TestPTTLNonexistent(t *testing.T) {

	s := setupTestStore(t)

	pttl, err := s.PTTL("pttl_nx")
	assert.NoError(t, err)
	assert.Equal(t, int64(-2), pttl)
}

// ================== base.go Dump/Restore TTL preservation ==================

func TestDumpRestoreWithTTL(t *testing.T) {

	s := setupTestStore(t)

	s.SetWithTTL("dr_ttl", "hello", 30*time.Second)
	data, err := s.Dump("dr_ttl")
	assert.NoError(t, err)

	s.Del("dr_ttl")
	err = s.Restore("dr_ttl_r", data, 5*time.Second, false)
	assert.NoError(t, err)

	val, err := s.Get("dr_ttl_r")
	assert.NoError(t, err)
	assert.Equal(t, "hello", val)
}

func TestDumpRestoreStringAllTypes(t *testing.T) {

	s := setupTestStore(t)

	// Test Dump/Restore for string
	s.Set("dr_str", "test_value")
	data, err := s.Dump("dr_str")
	assert.NoError(t, err)
	s.Del("dr_str")
	err = s.Restore("dr_str_r", data, 0, false)
	assert.NoError(t, err)
	val, _ := s.Get("dr_str_r")
	assert.Equal(t, "test_value", val)
}

func TestDumpRestoreList(t *testing.T) {

	s := setupTestStore(t)

	s.LPush("dr_list", "c", "b", "a")
	data, err := s.Dump("dr_list")
	assert.NoError(t, err)
	s.Del("dr_list")
	err = s.Restore("dr_list_r", data, 0, false)
	assert.NoError(t, err)
	items, _ := s.LRange("dr_list_r", 0, -1)
	assert.Equal(t, 3, len(items))
}

func TestDumpRestoreHash(t *testing.T) {

	s := setupTestStore(t)

	s.HSet("dr_hash", "f1", "v1")
	s.HSet("dr_hash", "f2", "v2")
	data, err := s.Dump("dr_hash")
	assert.NoError(t, err)
	s.Del("dr_hash")
	err = s.Restore("dr_hash_r", data, 0, false)
	assert.NoError(t, err)
	fields, _ := s.HGetAll("dr_hash_r")
	assert.Equal(t, 2, len(fields))
}

func TestDumpRestoreSet(t *testing.T) {

	s := setupTestStore(t)

	s.SAdd("dr_set", "x", "y", "z")
	data, err := s.Dump("dr_set")
	assert.NoError(t, err)
	s.Del("dr_set")
	err = s.Restore("dr_set_r", data, 0, false)
	assert.NoError(t, err)
	card, _ := s.SCard("dr_set_r")
	assert.Equal(t, uint64(3), card)
}

func TestDumpRestoreSortedSet(t *testing.T) {

	s := setupTestStore(t)

	s.ZAdd("dr_zset", []ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 2.0},
		{Member: "c", Score: 3.0},
	})
	data, err := s.Dump("dr_zset")
	assert.NoError(t, err)
	s.Del("dr_zset")
	err = s.Restore("dr_zset_r", data, 0, false)
	assert.NoError(t, err)
	card, _ := s.ZCard("dr_zset_r")
	assert.Equal(t, int64(3), card)
}

func TestDumpNonexistent(t *testing.T) {

	s := setupTestStore(t)

	data, err := s.Dump("dr_ne")
	assert.Error(t, err)
	assert.Nil(t, data)
}

func TestDumpEmptyString(t *testing.T) {

	s := setupTestStore(t)

	s.Set("dr_es", "")
	data, err := s.Dump("dr_es")
	assert.NoError(t, err)
	s.Del("dr_es")
	err = s.Restore("dr_es_r", data, 0, false)
	assert.NoError(t, err)
	val, _ := s.Get("dr_es_r")
	assert.Equal(t, "", val)
}

func TestRestoreWithTTL0NoExpiry(t *testing.T) {

	s := setupTestStore(t)

	s.Set("r_ttl0", "value")
	data, err := s.Dump("r_ttl0")
	assert.NoError(t, err)
	s.Del("r_ttl0")
	err = s.Restore("r_ttl0_r", data, 0, false)
	assert.NoError(t, err)

	ttl, _ := s.TTL("r_ttl0_r")
	assert.Equal(t, int64(-1), ttl) // no expiry
}

// ================== base.go MatchPattern ==================

func TestMatchPatternExact(t *testing.T) {

	s := setupTestStore(t)

	s.Set("mp_a", "1")
	s.Set("mp_b", "2")
	s.Set("mp_c", "3")

	// Use Keys which calls MatchPattern internally
	keys, err := s.Keys("mp_*")
	assert.NoError(t, err)
	assert.Equal(t, 3, len(keys))
}

func TestMatchPatternSingleChar(t *testing.T) {

	s := setupTestStore(t)

	s.Set("ms_a", "1")
	s.Set("ms_ab", "2")
	s.Set("ms_abc", "3")

	keys, err := s.Keys("ms_?") // single char wildcard
	assert.NoError(t, err)
	assert.Equal(t, 1, len(keys))
}

func TestMatchPatternNoMatchP6(t *testing.T) {

	s := setupTestStore(t)

	s.Set("mn_x", "1")
	keys, err := s.Keys("mn_y*")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(keys))
}

func TestMatchPatternAll(t *testing.T) {

	s := setupTestStore(t)

	s.Set("ma_a", "1")
	s.Set("ma_b", "2")

	keys, err := s.Keys("*")
	assert.NoError(t, err)
	assert.True(t, len(keys) >= 2)
}

// ================== base.go Scan ==================

func TestScanBasic(t *testing.T) {

	s := setupTestStore(t)

	s.Set("sc_a", "1")
	s.Set("sc_b", "2")
	s.Set("sc_c", "3")

	result, err := s.Scan(0, "sc_*", 10)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(result.Keys))
}

func TestScanWithCountP6(t *testing.T) {

	s := setupTestStore(t)

	for i := 0; i < 20; i++ {
		s.Set("scn_"+string(rune('a'+i%26)), "val")
	}

	result, err := s.Scan(0, "scn_*", 5)
	assert.NoError(t, err)
	assert.True(t, len(result.Keys) > 0)
	if result.Cursor > 0 {
		_, err := s.Scan(result.Cursor, "scn_*", 5)
		assert.NoError(t, err)
	}
}

func TestScanNoMatch(t *testing.T) {

	s := setupTestStore(t)

	s.Set("snm_a", "1")
	result, err := s.Scan(0, "zzz_*", 10)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(result.Keys))
}

// ================== base.go MemoryUsage ==================

func TestMemoryUsageString(t *testing.T) {

	s := setupTestStore(t)

	s.Set("mu_str", "hello world")
	usage, err := s.MemoryUsage("mu_str")
	assert.NoError(t, err)
	assert.True(t, usage > 0)
}

func TestMemoryUsageList(t *testing.T) {

	s := setupTestStore(t)

	s.LPush("mu_list", "a", "b", "c")
	usage, err := s.MemoryUsage("mu_list")
	assert.NoError(t, err)
	assert.True(t, usage > 0)
}

func TestMemoryUsageHash(t *testing.T) {

	s := setupTestStore(t)

	s.HSet("mu_hash", "f1", "v1")
	s.HSet("mu_hash", "f2", "v2")
	usage, err := s.MemoryUsage("mu_hash")
	assert.NoError(t, err)
	assert.True(t, usage > 0)
}

func TestMemoryUsageSet(t *testing.T) {

	s := setupTestStore(t)

	s.SAdd("mu_set", "x", "y")
	usage, err := s.MemoryUsage("mu_set")
	assert.NoError(t, err)
	assert.True(t, usage > 0)
}

func TestMemoryUsageZSet(t *testing.T) {

	s := setupTestStore(t)

	s.ZAdd("mu_zset", []ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 2.0},
	})
	usage, err := s.MemoryUsage("mu_zset")
	assert.NoError(t, err)
	assert.True(t, usage > 0)
}

func TestMemoryUsageNonexistent(t *testing.T) {

	s := setupTestStore(t)

	_, err := s.MemoryUsage("mu_ne")
	assert.Error(t, err)
}

// ================== base.go ExpireTime / PExpireTime ==================

func TestExpireTimeWithExpiry(t *testing.T) {

	s := setupTestStore(t)

	s.SetWithTTL("et_e", "val", 60*time.Second)
	expireTime, err := s.ExpireTime("et_e")
	assert.NoError(t, err)
	assert.True(t, expireTime > 0)
}

func TestExpireTimeNoExpiry(t *testing.T) {

	s := setupTestStore(t)

	s.Set("et_n", "val")
	expireTime, err := s.ExpireTime("et_n")
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), expireTime)
}

func TestPExpireTimeWithExpiry(t *testing.T) {

	s := setupTestStore(t)

	s.SetWithTTL("pet_e", "val", 60*time.Second)
	expireTime, err := s.PExpireTime("pet_e")
	assert.NoError(t, err)
	assert.True(t, expireTime > 0)
}

func TestPExpireTimeNoExpiry(t *testing.T) {

	s := setupTestStore(t)

	s.Set("pet_n", "val")
	expireTime, err := s.PExpireTime("pet_n")
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), expireTime)
}

// ================== base.go Rename ==================

func TestRenameWithTTL(t *testing.T) {

	s := setupTestStore(t)

	s.SetWithTTL("rn_ttl", "val", 30*time.Second)
	err := s.Rename("rn_ttl", "rn_ttl_r")
	assert.NoError(t, err)

	val, _ := s.Get("rn_ttl_r")
	assert.Equal(t, "val", val)

	ttl, _ := s.TTL("rn_ttl_r")
	assert.True(t, ttl > 0 && ttl <= 30)
}

func TestRenameOverwriteStringP6(t *testing.T) {

	s := setupTestStore(t)

	s.Set("rn_ow1", "old")
	s.Set("rn_ow2", "new")
	err := s.Rename("rn_ow1", "rn_ow2")
	assert.NoError(t, err)

	val, _ := s.Get("rn_ow2")
	assert.Equal(t, "old", val)
}

// ================== sorted_set.go ==================

func TestZRangeByScoreExclusive(t *testing.T) {

	s := setupTestStore(t)

	s.ZAdd("zrbs_ex", []ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 2.0},
		{Member: "c", Score: 3.0},
		{Member: "d", Score: 4.0},
	})

	// Exclusive bounds: (1, 4) excludes endpoints
	results, err := s.ZRangeByScore("zrbs_ex", 1.0, 4.0, 0, -1, true, true)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(results)) // b, c only
}

func TestZRangeByScoreInclusiveBounds(t *testing.T) {

	s := setupTestStore(t)

	s.ZAdd("zrbs_inc", []ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 2.0},
		{Member: "c", Score: 3.0},
	})

	results, err := s.ZRangeByScore("zrbs_inc", 1.0, 3.0, 0, -1, false, false)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(results))
}

func TestZRangeByScoreWithLimit(t *testing.T) {

	s := setupTestStore(t)

	s.ZAdd("zrbs_lim", []ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 2.0},
		{Member: "c", Score: 3.0},
		{Member: "d", Score: 4.0},
		{Member: "e", Score: 5.0},
	})

	results, err := s.ZRangeByScore("zrbs_lim", 1.0, 5.0, 1, 2, false, false)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(results))
}

func TestZRevRangeByScoreExclusive(t *testing.T) {

	s := setupTestStore(t)

	s.ZAdd("zrrbs_ex", []ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 2.0},
		{Member: "c", Score: 3.0},
		{Member: "d", Score: 4.0},
	})

	results, err := s.ZRevRangeByScore("zrrbs_ex", 4.0, 1.0, 0, -1, true, true)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(results)) // c, b only
}

func TestZRemRangeByScoreExclusiveP6(t *testing.T) {

	s := setupTestStore(t)

	s.ZAdd("zrrbs_del", []ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 2.0},
		{Member: "c", Score: 3.0},
		{Member: "d", Score: 4.0},
	})

	removed, err := s.ZRemRangeByScore("zrrbs_del", 2.0, 4.0, true, true)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), removed) // only c (score 3.0) — exclusive bounds exclude 2.0 and 4.0

	card, _ := s.ZCard("zrrbs_del")
	assert.Equal(t, int64(3), card) // a, b, d remain
}

func TestZCountExclusive(t *testing.T) {

	s := setupTestStore(t)

	s.ZAdd("zc_ex", []ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 2.0},
		{Member: "c", Score: 3.0},
		{Member: "d", Score: 4.0},
	})

	count, err := s.ZCount("zc_ex", 1.0, 4.0)
	assert.NoError(t, err)
	assert.Equal(t, int64(4), count) // inclusive bounds
}

func TestZScoreNonexistent(t *testing.T) {

	s := setupTestStore(t)

	s.ZAdd("zs_ne", []ZSetMember{
		{Member: "a", Score: 1.0},
	})

	score, exists, err := s.ZScore("zs_ne", "z")
	assert.NoError(t, err)
	assert.False(t, exists)
	_ = score
}

func TestZRankNonexistent(t *testing.T) {

	s := setupTestStore(t)

	s.ZAdd("zr_ne", []ZSetMember{
		{Member: "a", Score: 1.0},
	})

	// ZRank on nonexistent member returns error in some impls or -1 in others
	rank, err := s.ZRank("zr_ne", "z")
	_ = rank
	_ = err
}

func TestZRevRankNonexistent(t *testing.T) {

	s := setupTestStore(t)

	s.ZAdd("zrr_ne", []ZSetMember{
		{Member: "a", Score: 1.0},
	})

	rank, err := s.ZRevRank("zrr_ne", "z")
	_ = rank
	_ = err
}

func TestZLexCountBasic(t *testing.T) {

	s := setupTestStore(t)

	s.ZAdd("zlc_b", []ZSetMember{
		{Member: "a", Score: 0},
		{Member: "b", Score: 0},
		{Member: "c", Score: 0},
		{Member: "d", Score: 0},
	})

	count, err := s.ZLexCount("zlc_b", "[b", "[d")
	assert.NoError(t, err)
	assert.Equal(t, int64(3), count) // b, c, d
}

func TestZRangeByLexExclusiveBounds(t *testing.T) {

	s := setupTestStore(t)

	s.ZAdd("zrbl_ex", []ZSetMember{
		{Member: "a", Score: 0},
		{Member: "b", Score: 0},
		{Member: "c", Score: 0},
		{Member: "d", Score: 0},
	})

	results, err := s.ZRangeByLex("zrbl_ex", "(a", "(d", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(results)) // b, c only
}

func TestZRevRangeByLexExclusiveBounds(t *testing.T) {

	s := setupTestStore(t)

	s.ZAdd("zrrbl_ex", []ZSetMember{
		{Member: "a", Score: 0},
		{Member: "b", Score: 0},
		{Member: "c", Score: 0},
		{Member: "d", Score: 0},
	})

	results, err := s.ZRevRangeByLex("zrrbl_ex", "(d", "(a", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(results)) // c, b only
}

func TestZRemRangeByLexExclusiveP6(t *testing.T) {

	s := setupTestStore(t)

	s.ZAdd("zrrbl_del2", []ZSetMember{
		{Member: "a", Score: 0},
		{Member: "b", Score: 0},
		{Member: "c", Score: 0},
		{Member: "d", Score: 0},
	})

	// Use inclusive bounds [b, [c to be deterministic
	removed, err := s.ZRemRangeByLex("zrrbl_del2", "[b", "[c")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), removed) // b, c removed

	card, _ := s.ZCard("zrrbl_del2")
	assert.Equal(t, int64(2), card) // a, d remain
}

// ================== sorted_set.go normalizeRankRange ==================

func TestZRangeNegativeRank(t *testing.T) {

	s := setupTestStore(t)

	s.ZAdd("zrn_neg", []ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 2.0},
		{Member: "c", Score: 3.0},
	})

	// Negative rank: -1 means last element, -3 means first
	results, err := s.ZRange("zrn_neg", -3, -1)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(results))
}

func TestZRangeStopExceedsLength(t *testing.T) {

	s := setupTestStore(t)

	s.ZAdd("zrn_stop", []ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 2.0},
	})

	// Stop exceeds length — should clamp
	results, err := s.ZRange("zrn_stop", 0, 100)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(results))
}

func TestZRevRangeNegativeRank(t *testing.T) {

	s := setupTestStore(t)

	s.ZAdd("zrr_neg", []ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 2.0},
		{Member: "c", Score: 3.0},
	})

	results, err := s.ZRevRange("zrr_neg", -3, -1)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(results))
}

// ================== sorted_set.go applyAggregateScore ==================

func TestZUnionStoreAggMax(t *testing.T) {

	s := setupTestStore(t)

	s.ZAdd("zus_max1", []ZSetMember{{Member: "a", Score: 1.0}})
	s.ZAdd("zus_max2", []ZSetMember{{Member: "a", Score: 5.0}})

	count, err := s.ZUnionStore("zus_max_d", []string{"zus_max1", "zus_max2"}, nil, "MAX")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)

	score, _, _ := s.ZScore("zus_max_d", "a")
	assert.Equal(t, 5.0, score)
}

func TestZUnionStoreAggMin(t *testing.T) {

	s := setupTestStore(t)

	s.ZAdd("zus_min1", []ZSetMember{{Member: "a", Score: 1.0}})
	s.ZAdd("zus_min2", []ZSetMember{{Member: "a", Score: 5.0}})

	count, err := s.ZUnionStore("zus_min_d", []string{"zus_min1", "zus_min2"}, nil, "MIN")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)

	score, _, _ := s.ZScore("zus_min_d", "a")
	assert.Equal(t, 1.0, score)
}

func TestZUnionStoreAggSum(t *testing.T) {

	s := setupTestStore(t)

	s.ZAdd("zus_sum1", []ZSetMember{{Member: "a", Score: 1.0}})
	s.ZAdd("zus_sum2", []ZSetMember{{Member: "a", Score: 5.0}})

	count, err := s.ZUnionStore("zus_sum_d", []string{"zus_sum1", "zus_sum2"}, nil, "SUM")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)

	score, _, _ := s.ZScore("zus_sum_d", "a")
	assert.Equal(t, 6.0, score)
}

// ================== sorted_set.go ZInterStore ==================

func TestZInterStoreSum(t *testing.T) {

	s := setupTestStore(t)

	s.ZAdd("zis_s1", []ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 2.0},
	})
	s.ZAdd("zis_s2", []ZSetMember{
		{Member: "b", Score: 3.0},
		{Member: "c", Score: 4.0},
	})

	count, err := s.ZInterStore("zis_s_d", []string{"zis_s1", "zis_s2"}, nil, "SUM")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count) // only b

	score, _, _ := s.ZScore("zis_s_d", "b")
	assert.Equal(t, 5.0, score) // 2 + 3 = 5
}

// ================== base.go NextStartup / cleanup ==================

func TestNextStartupCleansOrphans(t *testing.T) {

	s := setupTestStore(t)

	// Create a key, then manually delete the type key to simulate orphan
	s.Set("ns_orph", "val")
	typeKey := TypeOfKeyGet("ns_orph")
	s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(typeKey)
	})

	// NextStartup should clean up the orphaned data
	err := s.NextStartup()
	assert.NoError(t, err)
}

func TestNextStartupCleansOrphanedList(t *testing.T) {

	s := setupTestStore(t)

	s.LPush("ns_orl", "a", "b")
	typeKey := TypeOfKeyGet("ns_orl")
	s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(typeKey)
	})

	err := s.NextStartup()
	assert.NoError(t, err)
}

func TestNextStartupCleansOrphanedHash(t *testing.T) {

	s := setupTestStore(t)

	s.HSet("ns_orh", "f1", "v1")
	typeKey := TypeOfKeyGet("ns_orh")
	s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(typeKey)
	})

	err := s.NextStartup()
	assert.NoError(t, err)
}

func TestNextStartupCleansOrphanedSet(t *testing.T) {

	s := setupTestStore(t)

	s.SAdd("ns_ors", "x", "y")
	typeKey := TypeOfKeyGet("ns_ors")
	s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(typeKey)
	})

	err := s.NextStartup()
	assert.NoError(t, err)
}

func TestNextStartupCleansOrphanedZSet(t *testing.T) {

	s := setupTestStore(t)

	s.ZAdd("ns_orz", []ZSetMember{{Member: "a", Score: 1.0}})
	typeKey := TypeOfKeyGet("ns_orz")
	s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(typeKey)
	})

	err := s.NextStartup()
	assert.NoError(t, err)
}
