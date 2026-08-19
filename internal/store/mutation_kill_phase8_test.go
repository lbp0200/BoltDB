package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// =============================================================================
// Phase 8: Final push to 90% mcover — targeting remaining NOT COVERED
// sorted_set.go: memberInLexRange, normalizeRankRange, ZRank/ZRevRank
// define.go: init, config, edge cases
// base.go: remaining TTL/PTTL paths, MatchPattern
// =============================================================================

// ================== sorted_set.go memberInLexRange boundary cases ==================

func TestZRangeByLexMaxPlus(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.ZAdd("zrbl_p", []ZSetMember{
		{Member: "a", Score: 0},
		{Member: "b", Score: 0},
		{Member: "c", Score: 0},
	})

	// max = "+" means unbounded
	results, err := s.ZRangeByLex("zrbl_p", "-", "+", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(results))
}

func TestZRangeByLexMaxExclusive(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.ZAdd("zrbl_me", []ZSetMember{
		{Member: "a", Score: 0},
		{Member: "b", Score: 0},
		{Member: "c", Score: 0},
	})

	// max = "(c" means exclusive
	results, err := s.ZRangeByLex("zrbl_me", "-", "(c", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(results)) // a, b only
}

func TestZRangeByLexMaxInclusive(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.ZAdd("zrbl_mi", []ZSetMember{
		{Member: "a", Score: 0},
		{Member: "b", Score: 0},
		{Member: "c", Score: 0},
	})

	// max = "[c" means inclusive
	results, err := s.ZRangeByLex("zrbl_mi", "-", "[c", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(results)) // a, b, c
}

func TestZRangeByLexMaxPlain(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.ZAdd("zrbl_mp", []ZSetMember{
		{Member: "a", Score: 0},
		{Member: "b", Score: 0},
		{Member: "c", Score: 0},
	})

	// max = "c" (plain, inclusive)
	results, err := s.ZRangeByLex("zrbl_mp", "-", "c", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(results)) // a, b, c
}

// ================== sorted_set.go ZRank / ZRevRank ==================

func TestZRankBasic(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.ZAdd("zr_b", []ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 2.0},
		{Member: "c", Score: 3.0},
	})

	rank, err := s.ZRank("zr_b", "b")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), rank)
}

func TestZRankFirst(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.ZAdd("zr_f", []ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 2.0},
	})

	rank, err := s.ZRank("zr_f", "a")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), rank)
}

func TestZRankLast(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.ZAdd("zr_l", []ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 2.0},
		{Member: "c", Score: 3.0},
	})

	rank, err := s.ZRank("zr_l", "c")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), rank)
}

func TestZRevRankBasic(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.ZAdd("zrr_b", []ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 2.0},
		{Member: "c", Score: 3.0},
	})

	rank, err := s.ZRevRank("zrr_b", "a")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), rank) // reverse: c=0, b=1, a=2
}

func TestZRevRankFirst(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.ZAdd("zrr_f", []ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 2.0},
		{Member: "c", Score: 3.0},
	})

	rank, err := s.ZRevRank("zrr_f", "c")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), rank) // highest score = rank 0 in rev
}

// ================== sorted_set.go ZScore ==================

func TestZScoreExists(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.ZAdd("zs_e", []ZSetMember{
		{Member: "a", Score: 42.0},
	})

	score, exists, err := s.ZScore("zs_e", "a")
	assert.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, 42.0, score)
}

func TestZScoreNonExist(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.ZAdd("zs_ne", []ZSetMember{
		{Member: "a", Score: 1.0},
	})

	score, exists, err := s.ZScore("zs_ne", "z")
	assert.NoError(t, err)
	assert.False(t, exists)
	_ = score
}

// ================== sorted_set.go ZRange boundary ==================

func TestZRangeStartExceedsTotal(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.ZAdd("zr_se", []ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 2.0},
	})

	// start > stop after normalization → empty
	results, err := s.ZRange("zr_se", 10, 5)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(results))
}

func TestZRangeBothNegativeBeyond(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.ZAdd("zr_bn", []ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 2.0},
	})

	// Both beyond total: -100, -100 → start=0, stop=-98 → start > stop → empty
	results, err := s.ZRange("zr_bn", -100, -100)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(results))
}

func TestZRangeEmptySet(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	results, err := s.ZRange("zr_empty", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(results))
}

func TestZRevRangeStopBeyond(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.ZAdd("zrr_sb", []ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 2.0},
	})

	results, err := s.ZRevRange("zrr_sb", 0, 100)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(results))
}

// ================== sorted_set.go ZSetDel ==================

func TestZSetDelP8(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.ZAdd("zsd_p8", []ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 2.0},
	})

	err := s.ZSetDel("zsd_p8")
	assert.NoError(t, err)

	exists, _ := s.Exists("zsd_p8")
	assert.False(t, exists)
}

// ================== sorted_set.go ZMPop ==================

func TestZMPopMin(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.ZAdd("zmp_min", []ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 3.0},
		{Member: "c", Score: 2.0},
	})

	key, members, err := s.ZMPop([]string{"zmp_min"}, "MIN", 1)
	assert.NoError(t, err)
	assert.Equal(t, "zmp_min", key)
	assert.Equal(t, 1, len(members))
	assert.Equal(t, "a", members[0].Member)
}

func TestZMPopMaxP8(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.ZAdd("zmp_max", []ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 3.0},
		{Member: "c", Score: 2.0},
	})

	key, members, err := s.ZMPop([]string{"zmp_max"}, "MAX", 1)
	assert.NoError(t, err)
	assert.Equal(t, "zmp_max", key)
	assert.Equal(t, 1, len(members))
	assert.Equal(t, "b", members[0].Member)
}

// ================== sorted_set.go ZInterCard ==================

func TestZInterCardNoOverlap(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.ZAdd("zic_no", []ZSetMember{
		{Member: "a", Score: 1.0},
	})
	s.ZAdd("zic_no2", []ZSetMember{
		{Member: "b", Score: 2.0},
	})

	count, err := s.ZInterCard([]string{"zic_no", "zic_no2"}, 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

func TestZInterCardAllOverlap(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.ZAdd("zic_all", []ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 2.0},
	})
	s.ZAdd("zic_all2", []ZSetMember{
		{Member: "a", Score: 3.0},
		{Member: "b", Score: 4.0},
	})

	count, err := s.ZInterCard([]string{"zic_all", "zic_all2"}, 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)
}

// ================== define.go additional ==================

func TestGetDBNotNil(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	db := s.GetDB()
	assert.NotNil(t, db)
}

func TestCheckP8(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	err := s.Check()
	assert.NoError(t, err)
}

// ================== base.go remaining ==================

func TestObjectEncodingString(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.Set("oe_s", "hello")
	enc, err := s.ObjectEncoding("oe_s")
	assert.NoError(t, err)
	assert.NotEmpty(t, enc)
}

func TestObjectRefCount(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.Set("or_s", "hello")
	count, err := s.ObjectRefCount("or_s")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)
}

func TestObjectIdleTimeP8(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.Set("oi_s", "hello")
	idle, err := s.ObjectIdleTime("oi_s")
	assert.NoError(t, err)
	assert.True(t, idle >= 0)
}

// ================== timeseries.go TSIncrBy edge cases ==================

func TestTSIncrByNoExistingTS(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.TSCreate("tsi_ne", TSCreateOptions{})
	s.TSAdd("tsi_ne", 1000, 10.0, TSAddOptions{})

	// Increment at non-existing timestamp (2000 doesn't exist)
	ts, err := s.TSIncrBy("tsi_ne", 2000, 5.0, TSAddOptions{})
	assert.NoError(t, err)
	assert.Equal(t, int64(2000), ts)
}

func TestLMPopMultipleKeys(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.LPush("lmpk_a", "a1", "a2")
	s.LPush("lmpk_b", "b1")

	key, values, err := s.LMPop([]string{"lmpk_a", "lmpk_b"}, "LEFT", 1)
	assert.NoError(t, err)
	assert.True(t, key == "lmpk_a" || key == "lmpk_b")
	assert.Equal(t, 1, len(values))
}

func TestLMPopRightMultipleElements(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.LPush("lmpr_a", "d", "c", "b", "a")
	key, values, err := s.LMPop([]string{"lmpr_a"}, "RIGHT", 2)
	assert.NoError(t, err)
	assert.Equal(t, "lmpr_a", key)
	assert.Equal(t, 2, len(values))
}

// ================== list.go LMove edge cases ==================

func TestLMoveSameKey(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.LPush("lmss", "b", "a")
	val, err := s.LMove("lmss", "lmss", "LEFT", "RIGHT")
	assert.NoError(t, err)
	assert.Equal(t, "a", val)
}

// ================== list.go LTrim edge cases ==================

func TestLTrimToEmpty(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.LPush("ltm_e", "c", "b", "a")
	err := s.LTrim("ltm_e", 5, 10) // beyond range → empty
	assert.NoError(t, err)

	data, _ := s.LRange("ltm_e", 0, -1)
	assert.Equal(t, 0, len(data))
}

// ================== hash.go HScan additional ==================

func TestHScanPatternMatch(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.HSet("hsp_a", "field1", "val1")
	s.HSet("hsp_a", "field2", "val2")
	s.HSet("hsp_a", "other1", "val3")

	result, err := s.HScan("hsp_a", 0, "field*", 10)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(result.Fields))
}

// ================== set.go additional ==================

func TestSInterCardBasicP8(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.SAdd("sic_a_p8", "a", "b")
	s.SAdd("sic_b_p8", "b", "c")

	count, err := s.SInterCard("sic_a_p8", "sic_b_p8")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)
}

func TestSMoveP8(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.SAdd("smv_src_p8", "a", "b")
	s.SAdd("smv_dst_p8", "c")

	ok, err := s.SMove("smv_src_p8", "smv_dst_p8", "a")
	assert.NoError(t, err)
	assert.True(t, ok)

	exists, _ := s.SIsMember("smv_src_p8", "a")
	assert.False(t, exists)
	exists, _ = s.SIsMember("smv_dst_p8", "a")
	assert.True(t, exists)
}

func TestSMoveNonExistent(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.SAdd("smv_ne_p8", "a")
	s.SAdd("smv_ne_d", "b")

	ok, err := s.SMove("smv_ne_p8", "smv_ne_d", "z")
	assert.NoError(t, err)
	assert.False(t, ok)
}
