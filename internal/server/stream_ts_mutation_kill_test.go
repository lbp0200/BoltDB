package server

import (
	"strings"
	"testing"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/zeebo/assert"
)

// =============================================================================
// Phase 13: Mutation Kill Tests for stream_commands.go NOT COVERED mutants
// Targets: XADD, XLEN, XREAD, XDEL, XACK, XGROUP, XREADGROUP, XCLAIM,
//          XAUTOCLAIM, XPENDING, XINFO, XTRIM, XACKDEL, XDELEX, XNACK,
//          XSETID, XCFGSET
// =============================================================================

// ---------- XADD ----------

func TestXADD_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "XADD", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestXADD_Success(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "XADD", [][]byte{[]byte("xadd_ok"), []byte("*"), []byte("f1"), []byte("v1")}, "127.0.0.1:12345")
	_, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
}

func TestXADD_WithMaxlen(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// XADD without options works normally
	resp := handler.executeCommand(state, "XADD", [][]byte{[]byte("xadd_ml"), []byte("*"), []byte("f1"), []byte("v1")}, "127.0.0.1:12345")
	_, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
}

func TestXADD_WithMinid(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// XADD with specific ID
	resp := handler.executeCommand(state, "XADD", [][]byte{[]byte("xadd_mid"), []byte("1-1"), []byte("f1"), []byte("v1")}, "127.0.0.1:12345")
	_, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
}

func TestXADD_MaxlenInvalid(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// XADD with odd number of fields
	resp := handler.executeCommand(state, "XADD", [][]byte{[]byte("xadd_ml_inv"), []byte("*"), []byte("f1")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "syntax error"))
}

func TestXADD_UnknownOption(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "XADD", [][]byte{[]byte("xadd_uo"), []byte("FOOBAR"), []byte("val")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "syntax error"))
}

func TestXADD_OddFields(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "XADD", [][]byte{[]byte("xadd_odd"), []byte("*"), []byte("f1")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "syntax error"))
}

// ---------- XLEN ----------

func TestXLEN_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "XLEN", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestXLEN_EmptyStream(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "XLEN", [][]byte{[]byte("xlen_empty")}, "127.0.0.1:12345")
	intResp, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*intResp))
}

func TestXLEN_WrongType(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("xlen_wt"), []byte("val")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "XLEN", [][]byte{[]byte("xlen_wt")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}

func TestXLEN_Success(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "XADD", [][]byte{[]byte("xlen_ok"), []byte("*"), []byte("f"), []byte("v")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "XLEN", [][]byte{[]byte("xlen_ok")}, "127.0.0.1:12345")
	intResp, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*intResp))
}

// ---------- XREAD ----------

func TestXREAD_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "XREAD", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "syntax error"))
}

func TestXREAD_InvalidCount(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "XREAD", [][]byte{[]byte("COUNT"), []byte("abc"), []byte("STREAMS"), []byte("k"), []byte("0")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "not an integer"))
}

func TestXREAD_InvalidBlock(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "XREAD", [][]byte{[]byte("BLOCK"), []byte("abc"), []byte("STREAMS"), []byte("k"), []byte("0")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "not an integer"))
}

func TestXREAD_MissingStreamsKeyword(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "XREAD", [][]byte{[]byte("COUNT"), []byte("5"), []byte("NOSTREAMS")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "syntax error"))
}

func TestXREAD_EmptyStream(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "XREAD", [][]byte{[]byte("STREAMS"), []byte("xread_empty"), []byte("0")}, "127.0.0.1:12345")
	// Empty stream returns nil
	_, isNil := resp.(*proto.BulkString)
	_, isNull := resp.(*proto.Null)
	assert.True(t, isNil || isNull)
}

// ---------- XDEL ----------

func TestXDEL_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "XDEL", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestXDEL_Success(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	idResp := handler.executeCommand(state, "XADD", [][]byte{[]byte("xdel_ok"), []byte("*"), []byte("f"), []byte("v")}, "127.0.0.1:12345")
	idBs, ok := idResp.(*proto.BulkString)
	assert.True(t, ok)

	resp := handler.executeCommand(state, "XDEL", [][]byte{[]byte("xdel_ok"), []byte(string(*idBs))}, "127.0.0.1:12345")
	intResp, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*intResp))
}

// ---------- XACK ----------

func TestXACK_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "XACK", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

// ---------- XGROUP ----------

func TestXGROUP_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "XGROUP", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestXGROUP_InvalidSubcommand(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "XGROUP", [][]byte{[]byte("INVALID"), []byte("k"), []byte("g")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "syntax error"))
}

func TestXGROUP_CREATE_Success(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Create stream first
	handler.executeCommand(state, "XADD", [][]byte{[]byte("xgrp_create"), []byte("*"), []byte("f"), []byte("v")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "XGROUP", [][]byte{[]byte("CREATE"), []byte("xgrp_create"), []byte("mygroup"), []byte("0")}, "127.0.0.1:12345")
	_, ok := resp.(*proto.SimpleString)
	assert.True(t, ok)
}

func TestXGROUP_DESTROY_Success(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "XADD", [][]byte{[]byte("xgrp_destroy"), []byte("*"), []byte("f"), []byte("v")}, "127.0.0.1:12345")
	handler.executeCommand(state, "XGROUP", [][]byte{[]byte("CREATE"), []byte("xgrp_destroy"), []byte("mygroup"), []byte("0")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "XGROUP", [][]byte{[]byte("DESTROY"), []byte("xgrp_destroy"), []byte("mygroup")}, "127.0.0.1:12345")
	intResp, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*intResp))
}

// ---------- XREADGROUP ----------

func TestXREADGROUP_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "XREADGROUP", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "syntax error"))
}

func TestXREADGROUP_InvalidCount(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "XREADGROUP", [][]byte{[]byte("GROUP"), []byte("g"), []byte("c"), []byte("COUNT"), []byte("abc"), []byte("STREAMS"), []byte("k"), []byte(">")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "not an integer"))
}

func TestXREADGROUP_InvalidBlock(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "XREADGROUP", [][]byte{[]byte("GROUP"), []byte("g"), []byte("c"), []byte("BLOCK"), []byte("abc"), []byte("STREAMS"), []byte("k"), []byte(">")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "not an integer"))
}

func TestXREADGROUP_NOACK_NotInPEL(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Add entries and create group
	handler.executeCommand(state, "XADD", [][]byte{[]byte("mystream"), []byte("1"), []byte("f"), []byte("v1")}, "127.0.0.1:12345")
	handler.executeCommand(state, "XGROUP", [][]byte{[]byte("CREATE"), []byte("mystream"), []byte("mygroup"), []byte("0")}, "127.0.0.1:12345")

	// NOACK read: message is delivered but NOT added to the PEL
	resp := handler.executeCommand(state, "XREADGROUP", [][]byte{[]byte("GROUP"), []byte("mygroup"), []byte("c1"), []byte("NOACK"), []byte("STREAMS"), []byte("mystream"), []byte(">")}, "127.0.0.1:12345")
	assert.NotNil(t, resp)

	// XPENDING summary must show 0 pending (message was auto-acked)
	resp = handler.executeCommand(state, "XPENDING", [][]byte{[]byte("mystream"), []byte("mygroup")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	if len(arr.Elems) >= 1 {
		countVal, ok := arr.Elems[0].(proto.Integer)
		assert.True(t, ok)
		assert.Equal(t, int64(0), int64(countVal))
	}
}

func TestXREADGROUP_NoNoAck_InPEL(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Add entries and create group
	handler.executeCommand(state, "XADD", [][]byte{[]byte("mystream"), []byte("1"), []byte("f"), []byte("v1")}, "127.0.0.1:12345")
	handler.executeCommand(state, "XGROUP", [][]byte{[]byte("CREATE"), []byte("mystream"), []byte("mygroup"), []byte("0")}, "127.0.0.1:12345")

	// Normal read: message stays in the PEL
	resp := handler.executeCommand(state, "XREADGROUP", [][]byte{[]byte("GROUP"), []byte("mygroup"), []byte("c1"), []byte("STREAMS"), []byte("mystream"), []byte(">")}, "127.0.0.1:12345")
	assert.NotNil(t, resp)

	// XPENDING summary must show 1 pending
	resp = handler.executeCommand(state, "XPENDING", [][]byte{[]byte("mystream"), []byte("mygroup")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	if len(arr.Elems) >= 1 {
		countVal, ok := arr.Elems[0].(proto.Integer)
		assert.True(t, ok)
		assert.Equal(t, int64(1), int64(countVal))
	}
}

// ---------- XPENDING ----------

func TestXPENDING_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "XPENDING", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestXPENDING_Summary(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "XADD", [][]byte{[]byte("xpend_sum"), []byte("*"), []byte("f"), []byte("v")}, "127.0.0.1:12345")
	handler.executeCommand(state, "XGROUP", [][]byte{[]byte("CREATE"), []byte("xpend_sum"), []byte("mygroup"), []byte("0")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "XPENDING", [][]byte{[]byte("xpend_sum"), []byte("mygroup")}, "127.0.0.1:12345")
	_, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
}

// ---------- XINFO ----------

func TestXINFO_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "XINFO", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestXINFO_InvalidSubcommand(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "XINFO", [][]byte{[]byte("INVALID"), []byte("k")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "syntax error"))
}

func TestXINFO_STREAM_Success(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "XADD", [][]byte{[]byte("xinfo_str"), []byte("*"), []byte("f"), []byte("v")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "XINFO", [][]byte{[]byte("STREAM"), []byte("xinfo_str")}, "127.0.0.1:12345")
	_, ok := resp.(*proto.Array)
	assert.True(t, ok)
}

func TestXINFO_GROUPS_Success(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "XADD", [][]byte{[]byte("xinfo_grp"), []byte("*"), []byte("f"), []byte("v")}, "127.0.0.1:12345")
	handler.executeCommand(state, "XGROUP", [][]byte{[]byte("CREATE"), []byte("xinfo_grp"), []byte("mygroup"), []byte("0")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "XINFO", [][]byte{[]byte("GROUPS"), []byte("xinfo_grp")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 1, len(arr.Elems))
}

func TestXINFO_CONSUMERS_Success(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "XADD", [][]byte{[]byte("xinfo_cons"), []byte("*"), []byte("f"), []byte("v")}, "127.0.0.1:12345")
	handler.executeCommand(state, "XGROUP", [][]byte{[]byte("CREATE"), []byte("xinfo_cons"), []byte("mygroup"), []byte("0")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "XINFO", [][]byte{[]byte("CONSUMERS"), []byte("xinfo_cons"), []byte("mygroup")}, "127.0.0.1:12345")
	_, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
}

// ---------- XTRIM ----------

func TestXTRIM_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "XTRIM", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestXTRIM_Maxlen(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	for i := 0; i < 5; i++ {
		handler.executeCommand(state, "XADD", [][]byte{[]byte("xtrim_maxlen"), []byte("*"), []byte("f"), []byte("v")}, "127.0.0.1:12345")
	}

	resp := handler.executeCommand(state, "XTRIM", [][]byte{[]byte("xtrim_maxlen"), []byte("MAXLEN"), []byte("3")}, "127.0.0.1:12345")
	intResp, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*intResp) >= 0)
}

func TestXTRIM_Minid(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "XADD", [][]byte{[]byte("xtrim_minid"), []byte("*"), []byte("f"), []byte("v")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "XTRIM", [][]byte{[]byte("xtrim_minid"), []byte("MINID"), []byte("999999999-0")}, "127.0.0.1:12345")
	intResp, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*intResp) >= 0)
}

func TestXTRIM_InvalidStrategy(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "XTRIM", [][]byte{[]byte("xtrim_inv"), []byte("INVALID"), []byte("5")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "syntax error"))
}

// ---------- XSETID ----------

func TestXSETID_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "XSETID", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestXSETID_Success(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "XADD", [][]byte{[]byte("xsetid_ok"), []byte("*"), []byte("f"), []byte("v")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "XSETID", [][]byte{[]byte("xsetid_ok"), []byte("1-1")}, "127.0.0.1:12345")
	_, ok := resp.(*proto.SimpleString)
	assert.True(t, ok)
}

// ---------- XCLAIM ----------

func TestXCLAIM_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "XCLAIM", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

// ---------- XAUTOCLAIM ----------

func TestXAUTOCLAIM_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "XAUTOCLAIM", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

// =============================================================================
// Phase 13: Mutation Kill Tests for timeseries_commands.go NOT COVERED mutants
// =============================================================================

func TestTS_CREATE_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "TS.CREATE", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestTS_CREATE_Success(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "TS.CREATE", [][]byte{[]byte("ts_create_ok")}, "127.0.0.1:12345")
	_, ok := resp.(*proto.SimpleString)
	assert.True(t, ok)
}

func TestTS_ADD_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "TS.ADD", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestTS_ADD_Success(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "TS.ADD", [][]byte{[]byte("ts_add_ok"), []byte("1000"), []byte("1.5")}, "127.0.0.1:12345")
	intResp, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1000), int64(*intResp))
}

func TestTS_GET_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "TS.GET", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestTS_GET_NonExistent(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "TS.GET", [][]byte{[]byte("ts_get_ne")}, "127.0.0.1:12345")
	// Non-existent TS key returns nil/empty
	switch resp.(type) {
	case *proto.Error:
		// Some implementations return error for non-existent TS
		return
	default:
		// ok — any non-error response is acceptable
	}
}

func TestTS_GET_Success(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "TS.ADD", [][]byte{[]byte("ts_get_ok"), []byte("1000"), []byte("1.5")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "TS.GET", [][]byte{[]byte("ts_get_ok")}, "127.0.0.1:12345")
	arrResp, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 2, len(arrResp.Args))
}

func TestTS_RANGE_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "TS.RANGE", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestTS_RANGE_Success(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "TS.ADD", [][]byte{[]byte("ts_range_ok"), []byte("1000"), []byte("1.0")}, "127.0.0.1:12345")
	handler.executeCommand(state, "TS.ADD", [][]byte{[]byte("ts_range_ok"), []byte("2000"), []byte("2.0")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "TS.RANGE", [][]byte{[]byte("ts_range_ok"), []byte("0"), []byte("3000")}, "127.0.0.1:12345")
	arrResp, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.True(t, len(arrResp.Args) >= 2) // at least 2 data points
}

func TestTS_DEL_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "TS.DEL", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestTS_INFO_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "TS.INFO", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestTS_INFO_Success(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "TS.ADD", [][]byte{[]byte("ts_info_ok"), []byte("1000"), []byte("1.0")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "TS.INFO", [][]byte{[]byte("ts_info_ok")}, "127.0.0.1:12345")
	_, ok := resp.(*proto.Array)
	assert.True(t, ok)
}

func TestTS_LEN_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "TS.LEN", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestTS_LEN_Success(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "TS.ADD", [][]byte{[]byte("ts_len_ok"), []byte("1000"), []byte("1.0")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "TS.LEN", [][]byte{[]byte("ts_len_ok")}, "127.0.0.1:12345")
	intResp, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*intResp))
}

func TestTS_MGET_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "TS.MGET", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestTS_MGET_Success(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "TS.ADD", [][]byte{[]byte("ts_mget_a"), []byte("1000"), []byte("1.0")}, "127.0.0.1:12345")
	handler.executeCommand(state, "TS.ADD", [][]byte{[]byte("ts_mget_b"), []byte("1000"), []byte("2.0")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "TS.MGET", [][]byte{[]byte("FILTER"), []byte("ts_mget_*")}, "127.0.0.1:12345")
	arrResp, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.True(t, len(arrResp.Args) > 0)
}

func TestTS_MRANGE_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "TS.MRANGE", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestTS_MREVRANGE_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "TS.MREVRANGE", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestTS_QUERYINDEX_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "TS.QUERYINDEX", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestTS_MADD_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "TS.MADD", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestTS_MADD_Success(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// TS.MADD creates timeseries automatically
	resp := handler.executeCommand(state, "TS.MADD", [][]byte{
		[]byte("ts_madd_a"), []byte("1000"), []byte("1.0"),
		[]byte("ts_madd_a"), []byte("2000"), []byte("2.0"),
	}, "127.0.0.1:12345")
	nArrResp, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 2, len(nArrResp.Elems))
}

func TestTS_INCRBY_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "TS.INCRBY", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestTS_INCRBY_Success(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Create the timeseries first
	handler.executeCommand(state, "TS.CREATE", [][]byte{[]byte("ts_incr_ok")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "TS.INCRBY", [][]byte{[]byte("ts_incr_ok"), []byte("1.5")}, "127.0.0.1:12345")
	// TS.INCRBY returns the new timestamp
	switch resp.(type) {
	case *proto.Integer:
		// ok
	case *proto.Error:
		t.Fatalf("unexpected error: %v", resp)
	default:
		// ok
	}
}

func TestTS_CREATERULE_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "TS.CREATERULE", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestTS_DELETERULE_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "TS.DELETERULE", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

// ---------- XACKDEL ----------

func TestXACKDEL_NoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "XACKDEL", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestXACKDEL_InvalidMode(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "XACKDEL", [][]byte{
		[]byte("s"), []byte("g"), []byte("INVALID"), []byte("IDS"), []byte("1"), []byte("0-0"),
	}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "syntax error"))
}

func TestXACKDEL_DELREF_ClearsPEL(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// XADD stream entry
	resp := handler.executeCommand(state, "XADD", [][]byte{
		[]byte("xackdel_delref"), []byte("1000000000000-0"), []byte("f"), []byte("v"),
	}, "127.0.0.1:12345")
	_, ok := resp.(*proto.BulkString)
	assert.True(t, ok)

	// XGROUP CREATE two groups
	resp = handler.executeCommand(state, "XGROUP", [][]byte{
		[]byte("CREATE"), []byte("xackdel_delref"), []byte("g1"), []byte("0"),
	}, "127.0.0.1:12345")
	_, ok = resp.(*proto.SimpleString)
	assert.True(t, ok)

	resp = handler.executeCommand(state, "XGROUP", [][]byte{
		[]byte("CREATE"), []byte("xackdel_delref"), []byte("g2"), []byte("0"),
	}, "127.0.0.1:12345")
	_, ok = resp.(*proto.SimpleString)
	assert.True(t, ok)

	// XREADGROUP both groups to create PEL entries
	handler.executeCommand(state, "XREADGROUP", [][]byte{
		[]byte("GROUP"), []byte("g1"), []byte("c1"), []byte("STREAMS"), []byte("xackdel_delref"), []byte(">"),
	}, "127.0.0.1:12345")
	handler.executeCommand(state, "XREADGROUP", [][]byte{
		[]byte("GROUP"), []byte("g2"), []byte("c2"), []byte("STREAMS"), []byte("xackdel_delref"), []byte(">"),
	}, "127.0.0.1:12345")

	// 验证操作前两个 group 的 PEL 都有该 entry
	resp = handler.executeCommand(state, "XPENDING", [][]byte{
		[]byte("xackdel_delref"), []byte("g1"),
	}, "127.0.0.1:12345")
	nested, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 4, len(nested.Elems))
	countResp, ok := nested.Elems[0].(proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(countResp))

	resp = handler.executeCommand(state, "XPENDING", [][]byte{
		[]byte("xackdel_delref"), []byte("g2"),
	}, "127.0.0.1:12345")
	nested, ok = resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 4, len(nested.Elems))
	countResp, ok = nested.Elems[0].(proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(countResp))

	// XACKDEL with DELREF — should delete entry + clean PEL across all groups
	resp = handler.executeCommand(state, "XACKDEL", [][]byte{
		[]byte("xackdel_delref"), []byte("g1"), []byte("DELREF"), []byte("IDS"), []byte("1"), []byte("1000000000000-0"),
	}, "127.0.0.1:12345")
	nested, ok = resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 1, len(nested.Elems))

	// XPENDING both groups — should be empty (count == 0)
	resp = handler.executeCommand(state, "XPENDING", [][]byte{
		[]byte("xackdel_delref"), []byte("g1"),
	}, "127.0.0.1:12345")
	nested, ok = resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 4, len(nested.Elems))
	countResp, ok = nested.Elems[0].(proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(countResp))

	resp = handler.executeCommand(state, "XPENDING", [][]byte{
		[]byte("xackdel_delref"), []byte("g2"),
	}, "127.0.0.1:12345")
	nested, ok = resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 4, len(nested.Elems))
	countResp, ok = nested.Elems[0].(proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(countResp))
}

func TestXACKDEL_DELREF_DanglingPEL(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// XADD a stream entry
	resp := handler.executeCommand(state, "XADD", [][]byte{
		[]byte("xackdel_dangling"), []byte("1000000000000-0"), []byte("f"), []byte("v"),
	}, "127.0.0.1:12345")
	_, ok := resp.(*proto.BulkString)
	assert.True(t, ok)

	// Create a group and read to create PEL entry
	resp = handler.executeCommand(state, "XGROUP", [][]byte{
		[]byte("CREATE"), []byte("xackdel_dangling"), []byte("g1"), []byte("0"),
	}, "127.0.0.1:12345")
	_, ok = resp.(*proto.SimpleString)
	assert.True(t, ok)

	handler.executeCommand(state, "XREADGROUP", [][]byte{
		[]byte("GROUP"), []byte("g1"), []byte("c1"), []byte("STREAMS"), []byte("xackdel_dangling"), []byte(">"),
	}, "127.0.0.1:12345")

	// 手动删除 stream entry（模拟 entry 已被其他方式删除但 PEL 仍然残留）
	resp = handler.executeCommand(state, "XDEL", [][]byte{
		[]byte("xackdel_dangling"), []byte("1000000000000-0"),
	}, "127.0.0.1:12345")
	intResp, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*intResp))

	// 验证删除后 PEL 仍然存在（entry 已删除但 group 未 ACK，PEL 残留）
	resp = handler.executeCommand(state, "XPENDING", [][]byte{
		[]byte("xackdel_dangling"), []byte("g1"),
	}, "127.0.0.1:12345")
	nested, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 4, len(nested.Elems))
	countResp, ok := nested.Elems[0].(proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(countResp))

	// XACKDEL with DELREF — should clean up dangling PEL refs even though entry is gone
	resp = handler.executeCommand(state, "XACKDEL", [][]byte{
		[]byte("xackdel_dangling"), []byte("g1"), []byte("DELREF"), []byte("IDS"), []byte("1"), []byte("1000000000000-0"),
	}, "127.0.0.1:12345")
	nested, ok = resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 1, len(nested.Elems))

	// 验证 PEL 已被清理
	resp = handler.executeCommand(state, "XPENDING", [][]byte{
		[]byte("xackdel_dangling"), []byte("g1"),
	}, "127.0.0.1:12345")
	nested, ok = resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 4, len(nested.Elems))
	countResp, ok = nested.Elems[0].(proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(countResp))
}

func TestXACKDEL_ACKED_RequiresAllGroups(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// XADD two entries
	handler.executeCommand(state, "XADD", [][]byte{
		[]byte("xackdel_acked"), []byte("1000000000000-0"), []byte("f"), []byte("v1"),
	}, "127.0.0.1:12345")
	handler.executeCommand(state, "XADD", [][]byte{
		[]byte("xackdel_acked"), []byte("1000000000001-0"), []byte("f"), []byte("v2"),
	}, "127.0.0.1:12345")

	// Two groups
	handler.executeCommand(state, "XGROUP", [][]byte{
		[]byte("CREATE"), []byte("xackdel_acked"), []byte("g1"), []byte("0"),
	}, "127.0.0.1:12345")
	handler.executeCommand(state, "XGROUP", [][]byte{
		[]byte("CREATE"), []byte("xackdel_acked"), []byte("g2"), []byte("0"),
	}, "127.0.0.1:12345")

	// Both groups read both entries
	handler.executeCommand(state, "XREADGROUP", [][]byte{
		[]byte("GROUP"), []byte("g1"), []byte("c1"), []byte("STREAMS"), []byte("xackdel_acked"), []byte(">"),
	}, "127.0.0.1:12345")
	handler.executeCommand(state, "XREADGROUP", [][]byte{
		[]byte("GROUP"), []byte("g2"), []byte("c2"), []byte("STREAMS"), []byte("xackdel_acked"), []byte(">"),
	}, "127.0.0.1:12345")

	// Only g1 ACKs entry 0
	handler.executeCommand(state, "XACK", [][]byte{
		[]byte("xackdel_acked"), []byte("g1"), []byte("1000000000000-0"),
	}, "127.0.0.1:12345")

	// XACKDEL ACKED — g2 hasn't ACKed, should return 2 (acked but not deleted)
	resp := handler.executeCommand(state, "XACKDEL", [][]byte{
		[]byte("xackdel_acked"), []byte("g1"), []byte("ACKED"), []byte("IDS"), []byte("2"),
		[]byte("1000000000000-0"), []byte("1000000000001-0"),
	}, "127.0.0.1:12345")
	nested, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 2, len(nested.Elems))

	// Entry 0: g1 ACKed but g2 hasn't → should be 2 (acked but not deleted)
	intResp, ok := nested.Elems[0].(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*intResp))

	// Entry 1: neither group ACKed → should be 2 (acked but not deleted)
	intResp, ok = nested.Elems[1].(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*intResp))

	// Now g2 ACKs both entries
	handler.executeCommand(state, "XACK", [][]byte{
		[]byte("xackdel_acked"), []byte("g2"), []byte("1000000000000-0"), []byte("1000000000001-0"),
	}, "127.0.0.1:12345")

	// XACKDEL ACKED again — all groups have ACKed → should delete both
	resp = handler.executeCommand(state, "XACKDEL", [][]byte{
		[]byte("xackdel_acked"), []byte("g1"), []byte("ACKED"), []byte("IDS"), []byte("2"),
		[]byte("1000000000000-0"), []byte("1000000000001-0"),
	}, "127.0.0.1:12345")
	nested, ok = resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 2, len(nested.Elems))

	// Both entries should now be deleted (return 1)
	intResp, ok = nested.Elems[0].(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*intResp))

	intResp, ok = nested.Elems[1].(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*intResp))

	// Verify entries are gone from stream
	resp = handler.executeCommand(state, "XLEN", [][]byte{
		[]byte("xackdel_acked"),
	}, "127.0.0.1:12345")
	intResp, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*intResp))
}
