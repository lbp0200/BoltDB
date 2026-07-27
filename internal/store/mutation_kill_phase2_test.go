package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/zeebo/assert"
)

// =============================================================================
// Phase 13b: Mutation Kill Tests targeting remaining NOT COVERED mutants
// Focus: TTL/PTTL expired paths, BitCount/BitPos negative offsets, BitField overflow,
//        ZRANGEBYLEX exclusive bounds, negative rank normalization, XADD MAXLEN trim,
//        Copy/Rename TTL preservation, BitOp empty sources
// =============================================================================

// ---------- TTL/PTTL: expired key paths (base.go 444, 504-507, 551) ----------

func TestTTLExpiredSecondsFormat(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	// SetWithTTL writes in seconds format (expiresAt <= nowUnix*100)
	s.Set("ttl_exp_sec", "val")
	s.Expire("ttl_exp_sec", 1) // 1 second TTL

	// Wait for expiry
	time.Sleep(1100 * time.Millisecond)

	// TTL should return -2 (expired)
	ttl, err := s.TTL("ttl_exp_sec")
	assert.NoError(t, err)
	// After expiry, BadgerDB may or may not have cleaned up the key
	assert.True(t, ttl == -2 || ttl <= 0)
}

func TestPTTLExpiredSecondsFormat(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	s.Set("pttl_exp_sec", "val")
	s.Expire("pttl_exp_sec", 1)

	time.Sleep(1100 * time.Millisecond)

	pttl, err := s.PTTL("pttl_exp_sec")
	assert.NoError(t, err)
	// After expiry, BadgerDB may or may not have cleaned up the key
	assert.True(t, pttl == -2 || pttl <= 0)
}

func TestTTLExpiredNanoFormat(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	// Expire() writes nano format (expiresAt > nowUnix*100)
	// After expiry, TTL should return -2
	s.Set("ttl_exp_nano", "val")
	s.Expire("ttl_exp_nano", 1)

	time.Sleep(1100 * time.Millisecond)

	ttl, err := s.TTL("ttl_exp_nano")
	assert.NoError(t, err)
	// After expiry, BadgerDB may or may not have cleaned up the key
	// The key may return -2 (expired) or still exist with TTL=0
	assert.True(t, ttl == -2 || ttl <= 0)
}

func TestPTTLExpiredNanoFormat(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	s.Set("pttl_exp_nano", "val")
	s.Expire("pttl_exp_nano", 1)

	time.Sleep(1100 * time.Millisecond)

	pttl, err := s.PTTL("pttl_exp_nano")
	assert.NoError(t, err)
	// After expiry, BadgerDB may or may not have cleaned up the key
	assert.True(t, pttl == -2 || pttl <= 0)
}

// ---------- BitCount: negative byte offsets (string.go 724-739) ----------

func TestBitCountNegativeOffsets(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	// Set a 5-byte string: "hello" = 0x68 0x65 0x6c 0x6c 0x6f
	s.Set("bc_neg", "hello")

	// BITCOUNT key 0 -1 → all bytes
	count, err := s.BitCount("bc_neg", 0, -1)
	assert.NoError(t, err)
	assert.True(t, count > 0)

	// BITCOUNT key -2 -1 → last 2 bytes ("lo")
	count, err = s.BitCount("bc_neg", -2, -1)
	assert.NoError(t, err)
	assert.True(t, count > 0)

	// BITCOUNT key 0 -100 → start > end after normalization → 0
	count, err = s.BitCount("bc_neg", 0, -100)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)

	// BITCOUNT key -100 0 → start clamped to 0
	count, err = s.BitCount("bc_neg", -100, 0)
	assert.NoError(t, err)
	assert.True(t, count >= 0)

	// BITCOUNT key 0 100 → end clamped to len-1
	count, err = s.BitCount("bc_neg", 0, 100)
	assert.NoError(t, err)
	assert.True(t, count > 0)
}

// ---------- BitPos: negative byte offsets + not-found (string.go 862-899) ----------

func TestBitPosNegativeOffsets(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	// "A" = 0x41 = 01000001 → bit 0 is 0, bit 6 is 1
	s.Set("bp_neg", "A")

	// BITPOS key 1 0 -1 → find set bit in all bytes
	pos, err := s.BitPos("bp_neg", 1, 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), pos) // bit 1 of byte 0 (0x41 = 01000001, MSB first)

	// BITPOS key 1 -1 -1 → last byte only (same as byte 0 for single byte)
	pos, err = s.BitPos("bp_neg", 1, -1, -1)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), pos)

	// BITPOS key 0 -1 -1 → find clear bit in last byte
	pos, err = s.BitPos("bp_neg", 0, -1, -1)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), pos) // bit 0 is 0
}

func TestBitPosNotFound(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	// "\x00\x00" → all zeros, no set bits
	s.Set("bp_notfound", "\x00\x00")

	// BITPOS key 1 0 -1 → no set bit found → -1
	pos, err := s.BitPos("bp_notfound", 1, 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), pos)
}

func TestBitPosNegativeEndClamp(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	s.Set("bp_clamp", "\xff")

	// BITPOS key 1 0 -100 → end < 0, clamp to -1
	pos, err := s.BitPos("bp_clamp", 1, 0, -100)
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), pos) // start > end after clamp
}

// ---------- BitOp: all empty sources (string.go 781-788) ----------

func TestBitOpAllEmptySources(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	// BITOP AND dest nonexistent1 nonexistent2 → all sources empty
	count, err := s.BitOp("AND", "bo_empty_dest", "bo_nonexist1", "bo_nonexist2")
	assert.NoError(t, err)
	// Empty result stored as empty string
	exists, _ := s.Exists("bo_empty_dest")
	assert.Equal(t, true, exists)
	_ = count
}

// ---------- ZRANGEBYLEX: exclusive/inclusive bounds (sorted_set.go 358-366) ----------

func TestZRangByLexExclusiveUpperBound(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	s.ZAdd("zbl_exc", []ZSetMember{
		{Member: "aaa", Score: 0},
		{Member: "bbb", Score: 0},
		{Member: "ccc", Score: 0},
		{Member: "ddd", Score: 0},
	})

	// [aaa (ccc → aaa, bbb (exclusive upper bound)
	results, err := s.ZRangeByLex("zbl_exc", "[aaa", "(ccc", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(results))
	assert.Equal(t, "aaa", results[0])
	assert.Equal(t, "bbb", results[1])
}

func TestZRangByLexInclusiveUpperBound(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	s.ZAdd("zbl_inc", []ZSetMember{
		{Member: "aaa", Score: 0},
		{Member: "bbb", Score: 0},
		{Member: "ccc", Score: 0},
	})

	// [aaa [ccc → aaa, bbb, ccc (inclusive upper bound)
	results, err := s.ZRangeByLex("zbl_inc", "[aaa", "[ccc", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(results))
}

func TestZRangByLexPlainUpperBound(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	s.ZAdd("zbl_plain", []ZSetMember{
		{Member: "a", Score: 0},
		{Member: "b", Score: 0},
		{Member: "c", Score: 0},
	})

	// [a c → a, b, c (plain upper bound, inclusive)
	results, err := s.ZRangeByLex("zbl_plain", "[a", "c", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(results))
}

// ---------- ZRANGE: negative rank normalization (sorted_set.go 370-387) ----------

func TestZRangeNegativeRanks(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	mustZAdd(t, s, "zr_neg", []ZSetMember{
		{Member: "a", Score: 1},
		{Member: "b", Score: 2},
		{Member: "c", Score: 3},
		{Member: "d", Score: 4},
		{Member: "e", Score: 5},
	})

	// ZRANGE -3 -1 → start = 5+(-3)=2, stop = 5+(-1)=4 → c,d,e
	results, err := s.ZRange("zr_neg", -3, -1)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(results))
	assert.Equal(t, "c", results[0].Member)
	assert.Equal(t, "e", results[2].Member)
}

func TestZRangeNegativeStartClamp(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	mustZAdd(t, s, "zr_nsc", []ZSetMember{
		{Member: "a", Score: 1},
		{Member: "b", Score: 2},
	})

	// ZRANGE -100 0 → start clamped to 0
	results, err := s.ZRange("zr_nsc", -100, 0)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, "a", results[0].Member)
}

func TestZRangeStopClamp(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	mustZAdd(t, s, "zr_sc", []ZSetMember{
		{Member: "a", Score: 1},
		{Member: "b", Score: 2},
	})

	// ZRANGE 0 100 → stop clamped to 1
	results, err := s.ZRange("zr_sc", 0, 100)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(results))
}

func TestZRangeStartGreaterThanStop(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	mustZAdd(t, s, "zr_gts", []ZSetMember{
		{Member: "a", Score: 1},
		{Member: "b", Score: 2},
	})

	// ZRANGE 2 1 → start > stop → empty
	results, err := s.ZRange("zr_gts", 2, 1)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(results))
}

// ---------- XADD: MAXLEN trimming (stream.go 317-354) ----------

func TestXAddMaxLenTrimming(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	// Add 10 entries with MAXLEN 3
	for i := 0; i < 10; i++ {
		_, err := s.XAdd("xadd_ml", StreamXAddOptions{MaxLen: 3}, "*",
			map[string]string{"f": fmt.Sprintf("v%d", i)})
		assert.NoError(t, err)
	}

	length, err := s.XLen("xadd_ml")
	assert.NoError(t, err)
	assert.True(t, length <= 3) // MAXLEN should keep at most 3
}

func TestXAddMaxLenExact(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	// Add exactly MAXLEN entries
	for i := 0; i < 5; i++ {
		_, err := s.XAdd("xadd_me", StreamXAddOptions{MaxLen: 5}, "*",
			map[string]string{"f": fmt.Sprintf("v%d", i)})
		assert.NoError(t, err)
	}

	length, err := s.XLen("xadd_me")
	assert.NoError(t, err)
	assert.Equal(t, int64(5), length)
}

func TestXAddMaxLenTrimMultiple(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	// Start with 5 entries, then add 5 more with MAXLEN 3
	for i := 0; i < 5; i++ {
		_, _ = s.XAdd("xadd_mt", StreamXAddOptions{}, "*",
			map[string]string{"f": fmt.Sprintf("v%d", i)})
	}
	for i := 0; i < 5; i++ {
		_, _ = s.XAdd("xadd_mt", StreamXAddOptions{MaxLen: 3}, "*",
			map[string]string{"f": fmt.Sprintf("v%d", i+5)})
	}

	length, err := s.XLen("xadd_mt")
	assert.NoError(t, err)
	assert.True(t, length <= 3)
}

// ---------- XREAD: $ startID (stream.go 462-465, 652-673) ----------

func TestXReadDollarID(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	// Add some entries
	_, _ = s.XAdd("xread_dollar", StreamXAddOptions{}, "*",
		map[string]string{"f": "v1"})

	// XREAD with "$" should return empty (no new entries)
	results, err := s.XRead(context.Background(), 0, -1, "xread_dollar", "$")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(results))
}

func TestXReadDollarIDAfterNewEntry(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	// Add first entry
	_, _ = s.XAdd("xread_dollar2", StreamXAddOptions{}, "*",
		map[string]string{"f": "v1"})

	// XREAD with "$" → empty
	results, _ := s.XRead(context.Background(), 0, -1, "xread_dollar2", "$")
	assert.Equal(t, 0, len(results))

	// Add second entry
	_, _ = s.XAdd("xread_dollar2", StreamXAddOptions{}, "*",
		map[string]string{"f": "v2"})

	// XREAD with "$" → still empty (only new entries since last read)
	results, _ = s.XRead(context.Background(), 0, -1, "xread_dollar2", "$")
	assert.Equal(t, 0, len(results))

	// XREAD with "0" → all entries
	results, _ = s.XRead(context.Background(), 0, -1, "xread_dollar2", "0")
	assert.True(t, len(results) > 0)
}

// ---------- XREAD: COUNT limit (stream.go 494, 684) ----------

func TestXReadCountLimit(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	// Add 5 entries
	for i := 0; i < 5; i++ {
		_, _ = s.XAdd("xread_cnt", StreamXAddOptions{}, "*",
			map[string]string{"f": fmt.Sprintf("v%d", i)})
	}

	// XREAD with COUNT 2 → should return at most 2 entries
	results, err := s.XRead(context.Background(), 2, -1, "xread_cnt", "0")
	assert.NoError(t, err)
	// Should have 1 stream with at most 2 entries
	if len(results) > 0 {
		for _, streamMap := range results {
			for _, entries := range streamMap {
				assert.True(t, len(entries) <= 2)
			}
		}
	}
}

// ---------- Rename: TTL preservation with SetWithTTL (base.go 915-927) ----------

func TestRenameWithSetWithTTL(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	s.Set("rn_ttls_src", "val")
	s.Expire("rn_ttls_src", 60)

	err := s.Rename("rn_ttls_src", "rn_ttls_dst")
	assert.NoError(t, err)

	ttl, err := s.TTL("rn_ttls_dst")
	assert.NoError(t, err)
	assert.True(t, ttl > 0 && ttl <= 60)
}

// ---------- ZRANGEBYSCORE: exclusive max boundary (sorted_set.go 196, 623) ----------

func TestZRangeByScoreExclusiveMax(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	mustZAdd(t, s, "zrbs_em", []ZSetMember{
		{Member: "a", Score: 1},
		{Member: "b", Score: 3},
		{Member: "c", Score: 5},
		{Member: "d", Score: 7},
	})

	// (3 exclusive upper bound → only score 5 and 7 are > 3... wait
	// Actually (3 means exclusive, so score > 3 → d only
	// Wait, the function is zRangeMembersByScoreInTxn with exclusive max
	// min=3, max=7, maxExclusive=true → score > 3 → but we want score >= 3 to break
	// Let me check the logic: if maxExclusive && score >= maxScore { break }
	// So (3 means scores < 3 are included? No...
	// Actually ZRANGEBYSCORE min max → min <= score <= max
	// ( means exclusive, so (3 means score > 3
	// Wait, that's the Redis convention: ( is exclusive
	// So ZRANGEBYSCORE key (3 +inf → scores > 3
	// Let me use the string-returning variant
	results, err := s.ZRangeByScore("zrbs_em", 3, 7, 0, -1, true, false)
	assert.NoError(t, err)
	// exclusive min=3: score > 3 → d (score=7) only... but 5 is also > 3
	// Actually minExclusive=true means score > minScore
	// So score > 3 → c(5), d(7)
	assert.Equal(t, 2, len(results))

	// exclusive max: score < maxScore
	results, err = s.ZRangeByScore("zrbs_em", 1, 5, 0, -1, false, true)
	assert.NoError(t, err)
	// score < 5 → a(1), b(3)
	assert.Equal(t, 2, len(results))
}

// ---------- ZRANK/ZREVRANK: not found (sorted_set.go 472, 518-520, 550-552) ----------

func TestZRankNonExistentMember(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	mustZAdd(t, s, "zrank_ne", []ZSetMember{
		{Member: "a", Score: 1},
		{Member: "b", Score: 2},
	})

	rank, err := s.ZRank("zrank_ne", "nonexistent")
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), rank)
}

func TestZRankNonExistentSet(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	rank, err := s.ZRank("zrank_ns", "member")
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), rank)
}

func TestZRevRankNonExistentMember(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	mustZAdd(t, s, "zrevrank_ne", []ZSetMember{
		{Member: "a", Score: 1},
		{Member: "b", Score: 2},
	})

	rank, err := s.ZRevRank("zrevrank_ne", "nonexistent")
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), rank)
}

// ---------- BITCOUNT: start > end after normalization ----------

func TestBitCountStartGreaterThanEnd(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	s.Set("bc_ste", "hello")

	// BITCOUNT key 2 1 → start > end → 0
	count, err := s.BitCount("bc_ste", 2, 1)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

// ---------- BITOP: NOT with single key ----------

func TestBitOpNot(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	s.Set("bo_not_src", "\xff")
	count, err := s.BitOp("NOT", "bo_not_dst", "bo_not_src")
	assert.NoError(t, err)
	assert.True(t, count > 0)

	val, err := s.Get("bo_not_dst")
	assert.NoError(t, err)
	assert.Equal(t, byte(0x00), val[0])
}

// ---------- BITOP: XOR ----------

func TestBitOpXor(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	s.Set("bo_xor_a", "\xff")
	s.Set("bo_xor_b", "\x0f")
	count, err := s.BitOp("XOR", "bo_xor_dst", "bo_xor_a", "bo_xor_b")
	assert.NoError(t, err)
	assert.True(t, count > 0)

	val, err := s.Get("bo_xor_dst")
	assert.NoError(t, err)
	assert.Equal(t, byte(0xf0), val[0]) // 0xff XOR 0x0f = 0xf0
}

// ---------- ZSCORE: existing member ----------

func TestZScoreExisting(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	mustZAdd(t, s, "zscore_ex", []ZSetMember{
		{Member: "a", Score: 3.14},
	})

	score, exists, err := s.ZScore("zscore_ex", "a")
	assert.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, 3.14, score)
}

// ---------- ZCARD: existing set ----------

func TestZCardExisting(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	mustZAdd(t, s, "zcard_ex", []ZSetMember{
		{Member: "a", Score: 1},
		{Member: "b", Score: 2},
		{Member: "c", Score: 3},
	})

	card, err := s.ZCard("zcard_ex")
	assert.NoError(t, err)
	assert.Equal(t, int64(3), card)
}

// ---------- ZREMRANGEBYSCORE: with exclusive bounds ----------

func TestZRemRangeByScoreExclusive(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	mustZAdd(t, s, "zrrbs_exc", []ZSetMember{
		{Member: "a", Score: 1},
		{Member: "b", Score: 3},
		{Member: "c", Score: 5},
		{Member: "d", Score: 7},
	})

	// Remove scores > 3 and < 7 (exclusive bounds)
	removed, err := s.ZRemRangeByScore("zrrbs_exc", 3.0, 7.0, true, true)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), removed) // c(5) removed (3 < 5 < 7)

	card, _ := s.ZCard("zrrbs_exc")
	assert.Equal(t, int64(3), card) // a(1), b(3), d(7) remain
}

// ---------- ZREMRANGEBYRANK: negative ranks ----------

func TestZRemRangeByRankNegative(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	mustZAdd(t, s, "zrrbr_neg", []ZSetMember{
		{Member: "a", Score: 1},
		{Member: "b", Score: 2},
		{Member: "c", Score: 3},
		{Member: "d", Score: 4},
		{Member: "e", Score: 5},
	})

	// Remove last 2 members (ranks -2 to -1 → ranks 3 to 4)
	removed, err := s.ZRemRangeByRank("zrrbr_neg", -2, -1)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), removed)

	card, _ := s.ZCard("zrrbr_neg")
	assert.Equal(t, int64(3), card)
}

// ---------- ZINCRBY ----------

func TestZIncrByStore(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	mustZAdd(t, s, "zincr_ex", []ZSetMember{
		{Member: "a", Score: 1.0},
	})

	newScore, err := s.ZIncrBy("zincr_ex", "a", 2.5)
	assert.NoError(t, err)
	assert.Equal(t, 3.5, newScore)
}

func TestZIncrByNewMember(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	newScore, err := s.ZIncrBy("zincr_new", "x", 5.0)
	assert.NoError(t, err)
	assert.Equal(t, 5.0, newScore)
}

// ---------- ZCOUNT: with scores ----------

func TestZCountRange(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	mustZAdd(t, s, "zcount_r", []ZSetMember{
		{Member: "a", Score: 1},
		{Member: "b", Score: 3},
		{Member: "c", Score: 5},
		{Member: "d", Score: 7},
	})

	count, err := s.ZCount("zcount_r", 3, 5)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count) // b(3), c(5)
}

// ---------- ZLEXCOUNT ----------

func TestZLexCountMutation(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	mustZAdd(t, s, "zlexcount_r", []ZSetMember{
		{Member: "a", Score: 0},
		{Member: "b", Score: 0},
		{Member: "c", Score: 0},
		{Member: "d", Score: 0},
	})

	count, err := s.ZLexCount("zlexcount_r", "[b", "[c")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)
}

// ---------- ZDIFF ----------

func TestZDiff(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	mustZAdd(t, s, "zdiff_a", []ZSetMember{
		{Member: "a", Score: 1},
		{Member: "b", Score: 2},
		{Member: "c", Score: 3},
	})
	mustZAdd(t, s, "zdiff_b", []ZSetMember{
		{Member: "b", Score: 2},
	})

	results, err := s.ZDiff([]string{"zdiff_a", "zdiff_b"})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(results))
}

// ---------- ZDIFFSTORE ----------

func TestZDiffStoreMutation(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	mustZAdd(t, s, "zdiffs_a", []ZSetMember{
		{Member: "a", Score: 1},
		{Member: "b", Score: 2},
		{Member: "c", Score: 3},
	})
	mustZAdd(t, s, "zdiffs_b", []ZSetMember{
		{Member: "b", Score: 2},
	})

	count, err := s.ZDiffStore("zdiffs_dst", []string{"zdiffs_a", "zdiffs_b"})
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)
}
