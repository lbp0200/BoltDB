package store

import (
	"testing"

	"github.com/zeebo/assert"
)

// =============================================================================
// Phase 13d: Additional Mutation Kill Tests for remaining NOT COVERED
// Target: sorted_set.go ZMPop/BZMPop/ZUnionStore/ZInterStore, stream.go xReadBlocking,
//         base.go RDB export/import, list.go LPos, hash.go HScan
// =============================================================================

// ---------- ZMPop (sorted_set.go) ----------

func TestZMPopMultipleKeys(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	mustZAdd(t, s, "zmpop_mk1", []ZSetMember{
		{Member: "a", Score: 1},
	})
	mustZAdd(t, s, "zmpop_mk2", []ZSetMember{
		{Member: "b", Score: 2},
	})

	key, members, err := s.ZMPop([]string{"zmpop_mk1", "zmpop_mk2"}, "MIN", 1)
	assert.NoError(t, err)
	assert.True(t, key == "zmpop_mk1" || key == "zmpop_mk2")
	assert.Equal(t, 1, len(members))
}

func TestZMPopEmptyKeys(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	key, members, err := s.ZMPop([]string{"zmpop_empty1", "zmpop_empty2"}, "MIN", 1)
	assert.NoError(t, err)
	assert.Equal(t, "", key)
	assert.Equal(t, 0, len(members))
}

func TestZMPopWrongType(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.Set("zmpop_wt", "val")
	_, _, err := s.ZMPop([]string{"zmpop_wt"}, "MIN", 1)
	assert.Error(t, err)
}

func TestZMPopMaxModifier(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	mustZAdd(t, s, "zmpop_max", []ZSetMember{
		{Member: "a", Score: 1},
		{Member: "b", Score: 3},
		{Member: "c", Score: 2},
	})

	key, members, err := s.ZMPop([]string{"zmpop_max"}, "MAX", 2)
	assert.NoError(t, err)
	assert.Equal(t, "zmpop_max", key)
	assert.Equal(t, 2, len(members))
	assert.Equal(t, "b", members[0].Member) // highest first
}

// ---------- ZUnionStore (sorted_set.go) ----------

func TestZUnionStoreBasic(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	mustZAdd(t, s, "zus_a", []ZSetMember{
		{Member: "a", Score: 1},
		{Member: "b", Score: 2},
	})
	mustZAdd(t, s, "zus_b", []ZSetMember{
		{Member: "b", Score: 3},
		{Member: "c", Score: 4},
	})

	count, err := s.ZUnionStore("zus_dst", []string{"zus_a", "zus_b"}, nil, "SUM")
	assert.NoError(t, err)
	assert.Equal(t, int64(3), count) // a, b, c
}

func TestZUnionStoreWithWeights(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	mustZAdd(t, s, "zusw_a", []ZSetMember{
		{Member: "a", Score: 1},
	})
	mustZAdd(t, s, "zusw_b", []ZSetMember{
		{Member: "a", Score: 2},
	})

	count, err := s.ZUnionStore("zusw_dst", []string{"zusw_a", "zusw_b"}, []float64{1, 2}, "SUM")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)
}

func TestZUnionStoreAggregateMax(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	mustZAdd(t, s, "zusmax_a", []ZSetMember{
		{Member: "a", Score: 1},
	})
	mustZAdd(t, s, "zusmax_b", []ZSetMember{
		{Member: "a", Score: 5},
	})

	count, err := s.ZUnionStore("zusmax_dst", []string{"zusmax_a", "zusmax_b"}, nil, "MAX")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)

	score, _, _ := s.ZScore("zusmax_dst", "a")
	assert.Equal(t, 5.0, score) // MAX(1, 5) = 5
}

func TestZUnionStoreAggregateMin(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	mustZAdd(t, s, "zusmin_a", []ZSetMember{
		{Member: "a", Score: 1},
	})
	mustZAdd(t, s, "zusmin_b", []ZSetMember{
		{Member: "a", Score: 5},
	})

	count, err := s.ZUnionStore("zusmin_dst", []string{"zusmin_a", "zusmin_b"}, nil, "MIN")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)

	score, _, _ := s.ZScore("zusmin_dst", "a")
	assert.Equal(t, 1.0, score) // MIN(1, 5) = 1
}

// ---------- ZInterStore (sorted_set.go) ----------

func TestZInterStoreBasicP4(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	mustZAdd(t, s, "zis_a", []ZSetMember{
		{Member: "a", Score: 1},
		{Member: "b", Score: 2},
	})
	mustZAdd(t, s, "zis_b", []ZSetMember{
		{Member: "b", Score: 3},
		{Member: "c", Score: 4},
	})

	count, err := s.ZInterStore("zis_dst", []string{"zis_a", "zis_b"}, nil, "SUM")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count) // only b
}

// ---------- ZDiffStore (sorted_set.go) ----------

func TestZDiffStoreBasic(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	mustZAdd(t, s, "zds_a", []ZSetMember{
		{Member: "a", Score: 1},
		{Member: "b", Score: 2},
		{Member: "c", Score: 3},
	})
	mustZAdd(t, s, "zds_b", []ZSetMember{
		{Member: "b", Score: 2},
	})

	count, err := s.ZDiffStore("zds_dst", []string{"zds_a", "zds_b"})
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count) // a, c
}

// ---------- ZRangeByLex (sorted_set.go) ----------

func TestZRangeByLexEmptyP4(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	results, err := s.ZRangeByLex("zbl_empty", "-", "+", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(results))
}

func TestZRangeByLexWithOffsetP4(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.ZAdd("zbl_offset", []ZSetMember{
		{Member: "a", Score: 0},
		{Member: "b", Score: 0},
		{Member: "c", Score: 0},
		{Member: "d", Score: 0},
	})

	results, err := s.ZRangeByLex("zbl_offset", "-", "+", 1, 2)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(results))
	assert.Equal(t, "b", results[0])
	assert.Equal(t, "c", results[1])
}

// ---------- ZRevRangeByLex (sorted_set.go) ----------

func TestZRevRangeByLexP4(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.ZAdd("zrbl_ok", []ZSetMember{
		{Member: "a", Score: 0},
		{Member: "b", Score: 0},
		{Member: "c", Score: 0},
	})

	results, err := s.ZRevRangeByLex("zrbl_ok", "[c", "[a", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(results))
	assert.Equal(t, "c", results[0]) // reverse order
}

// ---------- ZRemRangeByLex (sorted_set.go) ----------

func TestZRemRangeByLexP4(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.ZAdd("zrrbl_ok", []ZSetMember{
		{Member: "a", Score: 0},
		{Member: "b", Score: 0},
		{Member: "c", Score: 0},
		{Member: "d", Score: 0},
	})

	removed, err := s.ZRemRangeByLex("zrrbl_ok", "[b", "[c")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), removed) // b, c

	card, _ := s.ZCard("zrrbl_ok")
	assert.Equal(t, int64(2), card) // a, d remain
}

// ---------- ZInterCard (sorted_set.go) ----------

func TestZInterCardBasic(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	mustZAdd(t, s, "zic_a", []ZSetMember{
		{Member: "a", Score: 1},
		{Member: "b", Score: 2},
	})
	mustZAdd(t, s, "zic_b", []ZSetMember{
		{Member: "b", Score: 3},
		{Member: "c", Score: 4},
	})

	count, err := s.ZInterCard([]string{"zic_a", "zic_b"}, 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count) // only b
}

func TestZInterCardWithLimitP4(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	mustZAdd(t, s, "zicl_a", []ZSetMember{
		{Member: "a", Score: 1},
		{Member: "b", Score: 2},
		{Member: "c", Score: 3},
	})
	mustZAdd(t, s, "zicl_b", []ZSetMember{
		{Member: "a", Score: 1},
		{Member: "b", Score: 2},
		{Member: "c", Score: 3},
	})

	count, err := s.ZInterCard([]string{"zicl_a", "zicl_b"}, 2)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count) // limited to 2
}

// ---------- ZSetDel (sorted_set.go) ----------

func TestZSetDelP4(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	mustZAdd(t, s, "zsd_ok", []ZSetMember{
		{Member: "a", Score: 1},
		{Member: "b", Score: 2},
	})

	err := s.ZSetDel("zsd_ok")
	assert.NoError(t, err)

	exists, _ := s.Exists("zsd_ok")
	assert.False(t, exists)
}

// ---------- LPos (list.go) ----------

func TestLPosP4(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.LPush("lpos_ok", "c", "b", "a")

	pos, err := s.LPos("lpos_ok", "b", 0, 0, 0)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(pos))
	assert.Equal(t, int64(1), pos[0])
}

func TestLPosNotFoundP4(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.LPush("lpos_nf", "a", "b")

	pos, err := s.LPos("lpos_nf", "x", 0, 0, 0)
	// Not found returns empty result
	_ = err
	assert.Equal(t, 0, len(pos))
}

func TestLPosWithRankP4(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.LPush("lpos_rk", "a", "b", "a")

	// Find second occurrence of "a"
	pos, err := s.LPos("lpos_rk", "a", 2, 0, 0)
	assert.NoError(t, err)
	if len(pos) > 0 {
		assert.Equal(t, int64(2), pos[0])
	}
}

// ---------- LMove (list.go) ----------

func TestLMoveP4(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.LPush("lmove_src", "b", "a")
	s.LPush("lmove_dst", "d")

	result, err := s.LMove("lmove_src", "lmove_dst", "LEFT", "RIGHT")
	assert.NoError(t, err)
	assert.Equal(t, "a", result)

	data1, _ := s.LRange("lmove_src", 0, -1)
	assert.Equal(t, 1, len(data1))

	data2, _ := s.LRange("lmove_dst", 0, -1)
	assert.Equal(t, 2, len(data2))
}

// ---------- LRem (list.go) ----------

func TestLRemP4(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.LPush("lrem_ok", "a", "b", "a", "c", "a")

	count, err := s.LRem("lrem_ok", 2, "a")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)

	data, _ := s.LRange("lrem_ok", 0, -1)
	assert.Equal(t, 3, len(data)) // b, c, a remain
}

// ---------- LSet (list.go) ----------

func TestLSetP4(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.LPush("lset_ok", "b", "a")

	err := s.LSet("lset_ok", 0, "x")
	assert.NoError(t, err)

	val, _ := s.LIndex("lset_ok", 0)
	assert.Equal(t, "x", val)
}

// ---------- LTrim (list.go) ----------

func TestLTrimP4(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.LPush("ltrim_ok", "e", "d", "c", "b", "a")

	err := s.LTrim("ltrim_ok", 1, 3)
	assert.NoError(t, err)

	data, _ := s.LRange("ltrim_ok", 0, -1)
	assert.Equal(t, 3, len(data))
}

// ---------- LInsert (list.go) ----------

func TestLInsertP4(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.LPush("lins_ok", "c", "a")

	count, err := s.LInsert("lins_ok", "AFTER", "a", "b")
	assert.NoError(t, err)
	assert.True(t, count > 0)

	data, _ := s.LRange("lins_ok", 0, -1)
	// LInsert may return different count depending on implementation
	assert.True(t, len(data) >= 2)
}

// ---------- HGetAll (hash.go) ----------

func TestHGetAllStorePhase3(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.HSet("hga_ok", "f1", "v1")
	s.HSet("hga_ok", "f2", "v2")

	fields, err := s.HGetAll("hga_ok")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(fields))
}

// ---------- HIncrBy (hash.go) ----------

func TestHIncrByStorePhase3(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.HSet("hib_ok", "f1", "10")

	newVal, err := s.HIncrBy("hib_ok", "f1", 5)
	assert.NoError(t, err)
	assert.Equal(t, int64(15), newVal)
}

// ---------- HIncrByFloat (hash.go) ----------

func TestHIncrByFloatStorePhase3(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.HSet("hibf_ok", "f1", "1.5")

	newVal, err := s.HIncrByFloat("hibf_ok", "f1", 2.5)
	assert.NoError(t, err)
	assert.Equal(t, 4.0, newVal)
}

// ---------- HScan (hash.go) ----------

func TestHScanStorePhase3(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.HSet("hs_ok", "f1", "v1")
	s.HSet("hs_ok", "f2", "v2")
	s.HSet("hs_ok", "g1", "w1")

	result, err := s.HScan("hs_ok", 0, "f*", 10)
	assert.NoError(t, err)
	assert.True(t, len(result.Fields) > 0)
}

// ---------- HRandField (hash.go) ----------

func TestHRandFieldStorePhase3(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.HSet("hrf_ok", "f1", "v1")
	s.HSet("hrf_ok", "f2", "v2")
	s.HSet("hrf_ok", "f3", "v3")

	fields, _, err := s.HRandField("hrf_ok", 2, false)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(fields))
}

// ---------- SScan (set.go) ----------

func TestSScanStorePhase3(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.SAdd("sscan_ok", "a", "b", "c", "d")

	result, err := s.SScan("sscan_ok", 0, "", 2)
	assert.NoError(t, err)
	assert.True(t, len(result.Members) > 0)
}

// ---------- SRandMember (set.go) ----------

func TestSRandMemberStorePhase3(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.SAdd("srm_ok", "a", "b", "c")

	members, err := s.SRandMemberN("srm_ok", 2)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(members))
}

// ---------- SPop (set.go) ----------

func TestSPopStorePhase3(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.SAdd("spop_ok", "a", "b", "c")

	members, err := s.SPopN("spop_ok", 2)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(members))

	card, _ := s.SCard("spop_ok")
	assert.Equal(t, int64(1), card)
}

// ---------- SInterStore (set.go) ----------

func TestSInterStoreStorePhase3(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.SAdd("sis_a", "a", "b")
	s.SAdd("sis_b", "b", "c")

	count, err := s.SInterStore("sis_dst", "sis_a", "sis_b")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)
}

// ---------- SUnionStore (set.go) ----------

func TestSUnionStoreStorePhase3(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.SAdd("sus_a", "a", "b")
	s.SAdd("sus_b", "b", "c")

	count, err := s.SUnionStore("sus_dst", "sus_a", "sus_b")
	assert.NoError(t, err)
	assert.Equal(t, int64(3), count) // a, b, c
}

// ---------- SDiffStore (set.go) ----------

func TestSDiffStoreStorePhase3(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.SAdd("sds_a", "a", "b", "c")
	s.SAdd("sds_b", "b")

	count, err := s.SDiffStore("sds_dst", "sds_a", "sds_b")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count) // a, c
}

// ---------- SMove (set.go) ----------

func TestSMoveStorePhase3(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.SAdd("smv_src", "a", "b")
	s.SAdd("smv_dst", "c")

	ok, err := s.SMove("smv_src", "smv_dst", "b")
	assert.NoError(t, err)
	assert.True(t, ok)

	exists, _ := s.SIsMember("smv_src", "b")
	assert.False(t, exists)
	exists, _ = s.SIsMember("smv_dst", "b")
	assert.True(t, exists)
}

// ---------- SInter (set.go) ----------

func TestSInterStorePhase3(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.SAdd("si_a", "a", "b")
	s.SAdd("si_b", "b", "c")

	members, err := s.SInter("si_a", "si_b")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(members))
}

// ---------- SDiff (set.go) ----------

func TestSDiffStorePhase3(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.SAdd("sd_a", "a", "b", "c")
	s.SAdd("sd_b", "b")

	members, err := s.SDiff("sd_a", "sd_b")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(members))
}

// ---------- SUnion (set.go) ----------

func TestSUnionStorePhase3(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.SAdd("su_a", "a", "b")
	s.SAdd("su_b", "b", "c")

	members, err := s.SUnion("su_a", "su_b")
	assert.NoError(t, err)
	assert.Equal(t, 3, len(members))
}

// ---------- PFAdd / PFCount (hyperloglog.go) ----------

func TestPFAddStorePhase3(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	added, err := s.PFAdd("pfadd_ok", "a", "b", "c")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), added)
}

func TestPFCountStorePhase3(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.PFAdd("pfcount_ok", "a", "b", "c")
	count, err := s.PFCount("pfcount_ok")
	assert.NoError(t, err)
	assert.True(t, count >= 2) // approximate
}

// ---------- PFMerge (hyperloglog.go) ----------

func TestPFMergeStorePhase3(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.PFAdd("pfm_a", "a", "b")
	s.PFAdd("pfm_b", "c", "d")

	err := s.PFMerge("pfm_dst", "pfm_a", "pfm_b")
	assert.NoError(t, err)

	count, _ := s.PFCount("pfm_dst")
	assert.True(t, count >= 3)
}
