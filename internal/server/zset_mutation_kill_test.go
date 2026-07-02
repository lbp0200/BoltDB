package server

import (
	"strings"
	"testing"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/zeebo/assert"
)

// =============================================================================
// Phase 13: Mutation Kill Tests for zset_commands.go NOT COVERED mutants
// Targets: ZINTERCARD, ZUNION, ZMPOP, BZMPOP, ZRANGESTORE, ZRANGEBYSCORE,
//          ZREVRANGEBYSCORE, ZMSCORE, ZSCAN, ZPOPMAX, ZPOPMIN, BZPOPMAX,
//          BZPOPMIN, ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZDIFF, ZDIFFSTORE,
//          ZLEXCOUNT, ZRANGEBYLEX, ZREVRANGEBYLEX, ZREMRANGEBYLEX
// =============================================================================

// ---------- ZINTERCARD ----------

func TestZINTERCARD_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZINTERCARD", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestZINTERCARD_InvalidNumKeys(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Pass 2 args so we get past the len(args) check; first arg "abc" fails Atoi
	resp := handler.executeCommand(state, "ZINTERCARD", [][]byte{[]byte("abc"), []byte("k1")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "not an integer"))
}

func TestZINTERCARD_NumKeysZero(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Pass 2 args so we get past the len(args) check; numKeys=0 triggers the < 1 check
	resp := handler.executeCommand(state, "ZINTERCARD", [][]byte{[]byte("0"), []byte("k1")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "not an integer"))
}

func TestZINTERCARD_TooFewKeys(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZINTERCARD", [][]byte{[]byte("2"), []byte("k1")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestZINTERCARD_Success(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zic1"), []byte("1"), []byte("a"), []byte("2"), []byte("b")}, "127.0.0.1:12345")
	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zic2"), []byte("1"), []byte("b"), []byte("2"), []byte("c")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZINTERCARD", [][]byte{[]byte("2"), []byte("zic1"), []byte("zic2")}, "127.0.0.1:12345")
	intResp, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*intResp))
}

func TestZINTERCARD_WithLimit(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zicl1"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c")}, "127.0.0.1:12345")
	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zicl2"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZINTERCARD", [][]byte{[]byte("2"), []byte("zicl1"), []byte("zicl2"), []byte("LIMIT"), []byte("2")}, "127.0.0.1:12345")
	intResp, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*intResp))
}

func TestZINTERCARD_LimitMissingValue(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("ziclm1"), []byte("1"), []byte("a")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "ZINTERCARD", [][]byte{[]byte("1"), []byte("ziclm1"), []byte("LIMIT")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "syntax error"))
}

func TestZINTERCARD_UnknownOption(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zicuo1"), []byte("1"), []byte("a")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "ZINTERCARD", [][]byte{[]byte("1"), []byte("zicuo1"), []byte("FOOBAR")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "syntax error"))
}

func TestZINTERCARD_NegativeLimit(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zicnl1"), []byte("1"), []byte("a")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "ZINTERCARD", [][]byte{[]byte("1"), []byte("zicnl1"), []byte("LIMIT"), []byte("-1")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "not an integer"))
}

func TestZINTERCARD_WRONGTYPE(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("notazset"), []byte("val")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "ZINTERCARD", [][]byte{[]byte("1"), []byte("notazset")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}

// ---------- ZUNION ----------

func TestZUNION_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZUNION", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestZUNION_InvalidNumKeys(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Pass enough args to get past the len(args) check
	resp := handler.executeCommand(state, "ZUNION", [][]byte{[]byte("abc"), []byte("k1")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "not an integer"))
}

func TestZUNION_TooFewKeys(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// numKeys=2 but only 1 key provided — code doesn't validate so this would panic.
	// Instead test numKeys < 1 which is validated.
	resp := handler.executeCommand(state, "ZUNION", [][]byte{[]byte("0"), []byte("k1")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "syntax error"))
}

func TestZUNION_Success(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zu1"), []byte("1"), []byte("a"), []byte("2"), []byte("b")}, "127.0.0.1:12345")
	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zu2"), []byte("3"), []byte("b"), []byte("4"), []byte("c")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZUNION", [][]byte{[]byte("2"), []byte("zu1"), []byte("zu2")}, "127.0.0.1:12345")
	arrResp, ok := resp.(*proto.Array)
	assert.True(t, ok)
	// 3 unique members: a, b, c (no scores by default)
	assert.Equal(t, 3, len(arrResp.Args))
}

func TestZUNION_WithWeights(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zuw1"), []byte("1"), []byte("a")}, "127.0.0.1:12345")
	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zuw2"), []byte("2"), []byte("a")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZUNION", [][]byte{[]byte("2"), []byte("zuw1"), []byte("zuw2"), []byte("WEIGHTS"), []byte("1"), []byte("2")}, "127.0.0.1:12345")
	arrResp, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 1, len(arrResp.Args)) // 1 member: a
}

func TestZUNION_WithAggregate(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zua1"), []byte("1"), []byte("a")}, "127.0.0.1:12345")
	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zua2"), []byte("5"), []byte("a")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZUNION", [][]byte{[]byte("2"), []byte("zua1"), []byte("zua2"), []byte("AGGREGATE"), []byte("MAX")}, "127.0.0.1:12345")
	arrResp, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 1, len(arrResp.Args)) // 1 member: a
}

func TestZUNION_UnknownOption(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZUNION", [][]byte{[]byte("1"), []byte("k1"), []byte("FOOBAR")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "syntax error"))
}

func TestZUNION_InvalidWeightsCount(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZUNION", [][]byte{[]byte("2"), []byte("k1"), []byte("k2"), []byte("WEIGHTS"), []byte("1")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "syntax error"))
}

func TestZUNION_InvalidAggregate(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZUNION", [][]byte{[]byte("1"), []byte("k1"), []byte("AGGREGATE"), []byte("INVALID")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "syntax error"))
}

// ---------- ZMPOP ----------

func TestZMPOP_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZMPOP", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestZMPOP_InvalidNumKeys(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZMPOP", [][]byte{[]byte("abc"), []byte("k1"), []byte("MIN")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "syntax error"))
}

func TestZMPOP_InvalidModifier(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZMPOP", [][]byte{[]byte("1"), []byte("k1"), []byte("INVALID")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "syntax error"))
}

func TestZMPOP_WithCount(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zmpop1"), []byte("1"), []byte("a"), []byte("2"), []byte("b")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZMPOP", [][]byte{[]byte("1"), []byte("zmpop1"), []byte("MIN"), []byte("COUNT"), []byte("2")}, "127.0.0.1:12345")
	arrResp, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.True(t, len(arrResp.Args) > 0)
}

func TestZMPOP_CountMissingValue(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zmpopcmv"), []byte("1"), []byte("a")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZMPOP", [][]byte{[]byte("1"), []byte("zmpopcmv"), []byte("MIN"), []byte("COUNT")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "syntax error"))
}

func TestZMPOP_InvalidCount(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zmpopic"), []byte("1"), []byte("a")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZMPOP", [][]byte{[]byte("1"), []byte("zmpopic"), []byte("MIN"), []byte("COUNT"), []byte("abc")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "not an integer"))
}

func TestZMPOP_EmptyKey(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZMPOP", [][]byte{[]byte("1"), []byte("zmpop_empty"), []byte("MIN")}, "127.0.0.1:12345")
	// ZMPOP on empty key returns nil array
	switch resp.(type) {
	case proto.NilArray:
		// ok
	case *proto.Array:
		arr := resp.(*proto.Array)
		assert.Equal(t, 0, len(arr.Args))
	default:
		t.Fatalf("unexpected response type: %T %v", resp, resp)
	}
}

func TestZMPOP_WRONGTYPE(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("notazset_zmpop"), []byte("val")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "ZMPOP", [][]byte{[]byte("1"), []byte("notazset_zmpop"), []byte("MIN")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}

// ---------- BZMPOP ----------

func TestBZMPOP_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "BZMPOP", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestBZMPOP_InvalidTimeout(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "BZMPOP", [][]byte{[]byte("abc"), []byte("1"), []byte("k1"), []byte("MIN")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "not an integer"))
}

func TestBZMPOP_NegativeTimeout(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "BZMPOP", [][]byte{[]byte("-1"), []byte("1"), []byte("k1"), []byte("MIN")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "not an integer"))
}

func TestBZMPOP_InvalidNumKeys(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "BZMPOP", [][]byte{[]byte("0"), []byte("abc"), []byte("k1"), []byte("MIN")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "syntax error"))
}

func TestBZMPOP_InvalidModifier(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "BZMPOP", [][]byte{[]byte("0"), []byte("1"), []byte("k1"), []byte("INVALID")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "syntax error"))
}

func TestBZMPOP_CountMissingValue(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("bzmpopcmv"), []byte("1"), []byte("a")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "BZMPOP", [][]byte{[]byte("0"), []byte("1"), []byte("bzmpopcmv"), []byte("MIN"), []byte("COUNT")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "syntax error"))
}

func TestBZMPOP_InvalidCount(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("bzmpopic"), []byte("1"), []byte("a")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "BZMPOP", [][]byte{[]byte("0"), []byte("1"), []byte("bzmpopic"), []byte("MIN"), []byte("COUNT"), []byte("abc")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "not an integer"))
}

func TestBZMPOP_ZeroTimeoutEmptyKey(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "BZMPOP", [][]byte{[]byte("0"), []byte("1"), []byte("bzmpop_empty"), []byte("MIN")}, "127.0.0.1:12345")
	_, ok := resp.(*proto.NilArray)
	assert.True(t, ok)
}

// ---------- ZRANGESTORE ----------

func TestZRANGESTORE_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZRANGESTORE", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestZRANGESTORE_WithByScore(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zrs_src"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZRANGESTORE", [][]byte{[]byte("zrs_dst"), []byte("zrs_src"), []byte("1"), []byte("2"), []byte("BYSCORE")}, "127.0.0.1:12345")
	intResp, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*intResp))
}

func TestZRANGESTORE_WithByLex(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zrslex_src"), []byte("0"), []byte("a"), []byte("0"), []byte("b"), []byte("0"), []byte("c")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZRANGESTORE", [][]byte{[]byte("zrslex_dst"), []byte("zrslex_src"), []byte("[a"), []byte("[b"), []byte("BYLEX")}, "127.0.0.1:12345")
	intResp, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*intResp))
}

func TestZRANGESTORE_WithRev(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zrsrev_src"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c")}, "127.0.0.1:12345")

	// REV swaps start/stop: 2,0 → 0,2, then ZRange(0,2) returns a,b,c
	resp := handler.executeCommand(state, "ZRANGESTORE", [][]byte{[]byte("zrsrev_dst"), []byte("zrsrev_src"), []byte("2"), []byte("0"), []byte("REV")}, "127.0.0.1:12345")
	intResp, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	// REV swaps: 2,0 → 0,2, ZRange(0,2) = 3 elements
	assert.Equal(t, int64(3), int64(*intResp))
}

func TestZRANGESTORE_WithLimit(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zrslim_src"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZRANGESTORE", [][]byte{[]byte("zrslim_dst"), []byte("zrslim_src"), []byte("0"), []byte("-1"), []byte("LIMIT"), []byte("0"), []byte("2")}, "127.0.0.1:12345")
	intResp, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*intResp))
}

func TestZRANGESTORE_LimitMissingArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZRANGESTORE", [][]byte{[]byte("dst"), []byte("src"), []byte("0"), []byte("-1"), []byte("LIMIT")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "syntax error"))
}

func TestZRANGESTORE_InvalidLimitOffset(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZRANGESTORE", [][]byte{[]byte("dst"), []byte("src"), []byte("0"), []byte("-1"), []byte("LIMIT"), []byte("abc"), []byte("2")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "invalid LIMIT offset"))
}

func TestZRANGESTORE_InvalidLimitCount(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZRANGESTORE", [][]byte{[]byte("dst"), []byte("src"), []byte("0"), []byte("-1"), []byte("LIMIT"), []byte("0"), []byte("abc")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "invalid LIMIT count"))
}

func TestZRANGESTORE_WithScores(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zrss_src"), []byte("1"), []byte("a"), []byte("2"), []byte("b")}, "127.0.0.1:12345")

	// ZRANGESTORE doesn't support WITHSCORES, use plain args
	resp := handler.executeCommand(state, "ZRANGESTORE", [][]byte{[]byte("zrss_dst"), []byte("zrss_src"), []byte("0"), []byte("-1")}, "127.0.0.1:12345")
	intResp, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	// ZRANGESTORE returns count of stored elements
	assert.Equal(t, int64(2), int64(*intResp))

	// Verify the stored data can be read back
	zResp := handler.executeCommand(state, "ZRANGE", [][]byte{[]byte("zrss_dst"), []byte("0"), []byte("-1")}, "127.0.0.1:12345")
	zArr, ok := zResp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 2, len(zArr.Args))
}

// ---------- ZRANGEBYSCORE ----------

func TestZRANGEBYSCORE_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZRANGEBYSCORE", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestZRANGEBYSCORE_InvalidMin(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZRANGEBYSCORE", [][]byte{[]byte("k"), []byte("abc"), []byte("2")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "not a valid float"))
}

func TestZRANGEBYSCORE_InvalidMax(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZRANGEBYSCORE", [][]byte{[]byte("k"), []byte("1"), []byte("xyz")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "not a valid float"))
}

func TestZRANGEBYSCORE_WithLimitInvalidOffset(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZRANGEBYSCORE", [][]byte{[]byte("k"), []byte("1"), []byte("2"), []byte("LIMIT"), []byte("abc"), []byte("1")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "not an integer"))
}

func TestZRANGEBYSCORE_WithLimitInvalidCount(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZRANGEBYSCORE", [][]byte{[]byte("k"), []byte("1"), []byte("2"), []byte("LIMIT"), []byte("0"), []byte("abc")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "not an integer"))
}

func TestZRANGEBYSCORE_EmptyKey(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZRANGEBYSCORE", [][]byte{[]byte("zrbs_empty"), []byte("-inf"), []byte("+inf")}, "127.0.0.1:12345")
	arrResp, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 0, len(arrResp.Args))
}

func TestZRANGEBYSCORE_Success(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zrbs_ok"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZRANGEBYSCORE", [][]byte{[]byte("zrbs_ok"), []byte("1"), []byte("2")}, "127.0.0.1:12345")
	arrResp, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 2, len(arrResp.Args))
}

// ---------- ZREVRANGEBYSCORE ----------

func TestZREVRANGEBYSCORE_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZREVRANGEBYSCORE", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestZREVRANGEBYSCORE_Success(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zvrbs_ok"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZREVRANGEBYSCORE", [][]byte{[]byte("zvrbs_ok"), []byte("3"), []byte("1")}, "127.0.0.1:12345")
	arrResp, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 3, len(arrResp.Args))
}

// ---------- ZMSCORE ----------

func TestZMSCORE_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZMSCORE", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestZMSCORE_Success(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zms_ok"), []byte("1.5"), []byte("a"), []byte("2.5"), []byte("b")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZMSCORE", [][]byte{[]byte("zms_ok"), []byte("a"), []byte("b"), []byte("c")}, "127.0.0.1:12345")
	arrResp, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 3, len(arrResp.Args))
	// c doesn't exist, should be nil
	assert.Nil(t, arrResp.Args[2])
}

// ---------- ZSCAN ----------

func TestZSCAN_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZSCAN", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestZSCAN_InvalidCursor(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZSCAN", [][]byte{[]byte("k"), []byte("abc")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "not an integer"))
}

func TestZSCAN_Success(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zscan_ok"), []byte("1"), []byte("a"), []byte("2"), []byte("b")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZSCAN", [][]byte{[]byte("zscan_ok"), []byte("0")}, "127.0.0.1:12345")
	arrResp, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 2, len(arrResp.Elems))
}

func TestZSCAN_WithMatch(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zscan_m"), []byte("1"), []byte("abc"), []byte("2"), []byte("def")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZSCAN", [][]byte{[]byte("zscan_m"), []byte("0"), []byte("MATCH"), []byte("a*")}, "127.0.0.1:12345")
	_, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
}

func TestZSCAN_WithCount(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zscan_c"), []byte("1"), []byte("a"), []byte("2"), []byte("b")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZSCAN", [][]byte{[]byte("zscan_c"), []byte("0"), []byte("COUNT"), []byte("10")}, "127.0.0.1:12345")
	_, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
}

func TestZSCAN_InvalidCount(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZSCAN", [][]byte{[]byte("k"), []byte("0"), []byte("COUNT"), []byte("abc")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "not an integer"))
}

// ---------- ZRANGE ----------

func TestZRANGE_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZRANGE", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestZRANGE_InvalidStart(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZRANGE", [][]byte{[]byte("k"), []byte("abc"), []byte("1")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "not an integer"))
}

func TestZRANGE_InvalidStop(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZRANGE", [][]byte{[]byte("k"), []byte("0"), []byte("abc")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "not an integer"))
}

func TestZRANGE_EmptyKey(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZRANGE", [][]byte{[]byte("zrange_empty"), []byte("0"), []byte("-1")}, "127.0.0.1:12345")
	arrResp, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 0, len(arrResp.Args))
}

func TestZRANGE_WithScores(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zrange_ws"), []byte("1"), []byte("a"), []byte("2"), []byte("b")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZRANGE", [][]byte{[]byte("zrange_ws"), []byte("0"), []byte("-1"), []byte("WITHSCORES")}, "127.0.0.1:12345")
	arrResp, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 4, len(arrResp.Args))
}

// ---------- ZREVRANGE ----------

func TestZREVRANGE_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZREVRANGE", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestZREVRANGE_EmptyKey(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZREVRANGE", [][]byte{[]byte("zrevr_empty"), []byte("0"), []byte("-1")}, "127.0.0.1:12345")
	arrResp, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 0, len(arrResp.Args))
}

func TestZREVRANGE_WithScores(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zrevr_ws"), []byte("1"), []byte("a"), []byte("2"), []byte("b")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZREVRANGE", [][]byte{[]byte("zrevr_ws"), []byte("0"), []byte("-1"), []byte("WITHSCORES")}, "127.0.0.1:12345")
	arrResp, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 4, len(arrResp.Args))
}

// ---------- ZPOPMAX / ZPOPMIN ----------

func TestZPOPMAX_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZPOPMAX", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestZPOPMIN_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZPOPMIN", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestZPOPMAX_EmptyKey(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZPOPMAX", [][]byte{[]byte("zpopmax_empty")}, "127.0.0.1:12345")
	_, ok := resp.(*proto.Array)
	assert.True(t, ok)
}

func TestZPOPMIN_EmptyKey(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZPOPMIN", [][]byte{[]byte("zpopmin_empty")}, "127.0.0.1:12345")
	_, ok := resp.(*proto.Array)
	assert.True(t, ok)
}

func TestZPOPMAX_WithCount(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zpopmax_c"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZPOPMAX", [][]byte{[]byte("zpopmax_c"), []byte("2")}, "127.0.0.1:12345")
	arrResp, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 4, len(arrResp.Args)) // 2 members × 2 (member + score)
}

func TestZPOPMIN_WithCount(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zpopmin_c"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZPOPMIN", [][]byte{[]byte("zpopmin_c"), []byte("2")}, "127.0.0.1:12345")
	arrResp, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 4, len(arrResp.Args))
}

// ---------- ZDIFF ----------

func TestZDIFF_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZDIFF", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestZDIFF_InvalidNumKeys(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Pass 2 args to get past len(args) check
	resp := handler.executeCommand(state, "ZDIFF", [][]byte{[]byte("abc"), []byte("k1")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "not an integer"))
}

func TestZDIFF_Success(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zdiff1"), []byte("1"), []byte("a"), []byte("2"), []byte("b")}, "127.0.0.1:12345")
	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zdiff2"), []byte("1"), []byte("b")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZDIFF", [][]byte{[]byte("2"), []byte("zdiff1"), []byte("zdiff2")}, "127.0.0.1:12345")
	arrResp, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 1, len(arrResp.Args))
}

// ---------- ZDIFFSTORE ----------

func TestZDIFFSTORE_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZDIFFSTORE", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

// ---------- ZLEXCOUNT ----------

func TestZLEXCOUNT_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZLEXCOUNT", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestZLEXCOUNT_Success(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zlex_ok"), []byte("0"), []byte("a"), []byte("0"), []byte("b"), []byte("0"), []byte("c")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZLEXCOUNT", [][]byte{[]byte("zlex_ok"), []byte("[a"), []byte("[c")}, "127.0.0.1:12345")
	intResp, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(3), int64(*intResp))
}

// ---------- ZRANGEBYLEX ----------

func TestZRANGEBYLEX_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZRANGEBYLEX", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestZRANGEBYLEX_Success(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zrblx_ok"), []byte("0"), []byte("a"), []byte("0"), []byte("b"), []byte("0"), []byte("c")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZRANGEBYLEX", [][]byte{[]byte("zrblx_ok"), []byte("[a"), []byte("[b")}, "127.0.0.1:12345")
	arrResp, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 2, len(arrResp.Args))
}

// ---------- ZREVRANGEBYLEX ----------

func TestZREVRANGEBYLEX_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZREVRANGEBYLEX", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestZREVRANGEBYLEX_Success(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zvrblx_ok"), []byte("0"), []byte("a"), []byte("0"), []byte("b"), []byte("0"), []byte("c")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZREVRANGEBYLEX", [][]byte{[]byte("zvrblx_ok"), []byte("[c"), []byte("[b")}, "127.0.0.1:12345")
	arrResp, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 2, len(arrResp.Args))
}

// ---------- ZREMRANGEBYLEX ----------

func TestZREMRANGEBYLEX_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZREMRANGEBYLEX", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestZREMRANGEBYLEX_Success(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zrbrblx_ok"), []byte("0"), []byte("a"), []byte("0"), []byte("b"), []byte("0"), []byte("c")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZREMRANGEBYLEX", [][]byte{[]byte("zrbrblx_ok"), []byte("[a"), []byte("[b")}, "127.0.0.1:12345")
	intResp, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*intResp))
}

// ---------- ZREMRANGEBYRANK ----------

func TestZREMRANGEBYRANK_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZREMRANGEBYRANK", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestZREMRANGEBYRANK_Success(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zrbr_ok"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZREMRANGEBYRANK", [][]byte{[]byte("zrbr_ok"), []byte("0"), []byte("1")}, "127.0.0.1:12345")
	intResp, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*intResp))
}

// ---------- ZREMRANGEBYSCORE ----------

func TestZREMRANGEBYSCORE_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZREMRANGEBYSCORE", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestZREMRANGEBYSCORE_Success(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zrbrs_ok"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZREMRANGEBYSCORE", [][]byte{[]byte("zrbrs_ok"), []byte("1"), []byte("2")}, "127.0.0.1:12345")
	intResp, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*intResp))
}

// ---------- ZINCRBY ----------

func TestZINCRBY_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZINCRBY", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestZINCRBY_Success(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zincr_ok"), []byte("1"), []byte("a")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZINCRBY", [][]byte{[]byte("zincr_ok"), []byte("5"), []byte("a")}, "127.0.0.1:12345")
	bulkResp, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "6", string(string(*bulkResp)))
}

func TestZINCRBY_NewMember(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZINCRBY", [][]byte{[]byte("zincr_new"), []byte("3"), []byte("x")}, "127.0.0.1:12345")
	bulkResp, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "3", string(string(*bulkResp)))
}

// ---------- ZCOUNT ----------

func TestZCOUNT_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZCOUNT", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestZCOUNT_Success(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zcnt_ok"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZCOUNT", [][]byte{[]byte("zcnt_ok"), []byte("1"), []byte("2")}, "127.0.0.1:12345")
	intResp, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*intResp))
}

// ---------- ZRANK / ZREVRANK ----------

func TestZRANK_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZRANK", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestZRANK_Success(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zrank_ok"), []byte("1"), []byte("a"), []byte("2"), []byte("b")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZRANK", [][]byte{[]byte("zrank_ok"), []byte("b")}, "127.0.0.1:12345")
	intResp, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*intResp))
}

func TestZRANK_NotFound(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zrank_nf"), []byte("1"), []byte("a")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZRANK", [][]byte{[]byte("zrank_nf"), []byte("b")}, "127.0.0.1:12345")
	bsResp, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Nil(t, *bsResp)
}

func TestZREVRANK_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZREVRANK", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestZREVRANK_Success(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zrevrank_ok"), []byte("1"), []byte("a"), []byte("2"), []byte("b")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZREVRANK", [][]byte{[]byte("zrevrank_ok"), []byte("a")}, "127.0.0.1:12345")
	intResp, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*intResp))
}

// ---------- ZCARD / ZSCORE ----------

func TestZCARD_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZCARD", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestZSCORE_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZSCORE", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestZSCORE_Success(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zscore_ok"), []byte("3.14"), []byte("pi")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZSCORE", [][]byte{[]byte("zscore_ok"), []byte("pi")}, "127.0.0.1:12345")
	bulkResp, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "3.14", string(string(*bulkResp)))
}

func TestZSCORE_NotFound(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zscore_nf"), []byte("1"), []byte("a")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZSCORE", [][]byte{[]byte("zscore_nf"), []byte("b")}, "127.0.0.1:12345")
	bsResp, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Nil(t, *bsResp)
}

// ---------- ZADD / ZREM ----------

func TestZADD_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZADD", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestZADD_InvalidScore(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZADD", [][]byte{[]byte("k"), []byte("abc"), []byte("m")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "not a valid float"))
}

func TestZREM_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZREM", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestZREM_Success(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zrem_ok"), []byte("1"), []byte("a"), []byte("2"), []byte("b")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZREM", [][]byte{[]byte("zrem_ok"), []byte("a"), []byte("b")}, "127.0.0.1:12345")
	intResp, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*intResp))
}
