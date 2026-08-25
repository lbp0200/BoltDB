package server

import (
	"sync/atomic"
	"testing"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/zeebo/assert"
)

// keyspaceStats is a point-in-time snapshot of the INFO stats counters.
type keyspaceStats struct {
	hits   int64
	misses int64
}

func snapshotKeyspaceStats(h *Handler) keyspaceStats {
	return keyspaceStats{
		hits:   atomic.LoadInt64(&h.keyspaceHits),
		misses: atomic.LoadInt64(&h.keyspaceMisses),
	}
}

func assertKeyspaceDelta(t *testing.T, h *Handler, before keyspaceStats, wantHits, wantMisses int64) {
	t.Helper()
	after := snapshotKeyspaceStats(h)
	gotHits := after.hits - before.hits
	gotMisses := after.misses - before.misses
	if gotHits != wantHits {
		t.Errorf("keyspace_hits delta: got %d, want %d", gotHits, wantHits)
	}
	if gotMisses != wantMisses {
		t.Errorf("keyspace_misses delta: got %d, want %d", gotMisses, wantMisses)
	}
}

func execCmd(t *testing.T, h *Handler, state *connState, cmd string, args ...string) proto.RESP {
	t.Helper()
	bargs := make([][]byte, len(args))
	for i, a := range args {
		bargs[i] = []byte(a)
	}
	return h.executeCommand(state, cmd, bargs, "127.0.0.1:12345")
}

// TestKeyspaceStats_ReadCommands covers the commands whose lookup semantics
// were differentially verified against redis-server 8.2.1 (INFO stats deltas).
func TestKeyspaceStats_ReadCommands(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	t.Run("GETRANGE and SUBSTR alias", func(t *testing.T) {
		execCmd(t, handler, state, "SET", "k", "hello")
		before := snapshotKeyspaceStats(handler)
		execCmd(t, handler, state, "GETRANGE", "k", "0", "2")
		execCmd(t, handler, state, "SUBSTR", "k", "1", "2")
		execCmd(t, handler, state, "GETRANGE", "missing", "0", "2")
		assertKeyspaceDelta(t, handler, before, 2, 1)
	})

	t.Run("GETRANGE syntax error records nothing", func(t *testing.T) {
		before := snapshotKeyspaceStats(handler)
		execCmd(t, handler, state, "GETRANGE", "k", "bad", "2")
		assertKeyspaceDelta(t, handler, before, 0, 0)
	})

	t.Run("GETDEL GETEX GETSET count source key lookup", func(t *testing.T) {
		execCmd(t, handler, state, "SET", "gd", "v")
		execCmd(t, handler, state, "SET", "gex", "v")
		execCmd(t, handler, state, "SET", "gs", "v")
		before := snapshotKeyspaceStats(handler)
		execCmd(t, handler, state, "GETDEL", "gd")
		execCmd(t, handler, state, "GETDEL", "gd") // now missing
		execCmd(t, handler, state, "GETEX", "gex", "PERSIST")
		execCmd(t, handler, state, "GETSET", "gs", "n")
		assertKeyspaceDelta(t, handler, before, 3, 1)
	})

	t.Run("HSTRLEN hit even when field missing", func(t *testing.T) {
		execCmd(t, handler, state, "HSET", "h", "f", "v")
		before := snapshotKeyspaceStats(handler)
		execCmd(t, handler, state, "HSTRLEN", "h", "f")
		execCmd(t, handler, state, "HSTRLEN", "h", "nofield")
		execCmd(t, handler, state, "HSTRLEN", "nokey", "f")
		assertKeyspaceDelta(t, handler, before, 2, 1)
	})

	t.Run("HRANDFIELD", func(t *testing.T) {
		before := snapshotKeyspaceStats(handler)
		execCmd(t, handler, state, "HRANDFIELD", "nokey")
		assertKeyspaceDelta(t, handler, before, 0, 1)
	})

	t.Run("LPOS", func(t *testing.T) {
		execCmd(t, handler, state, "RPUSH", "l", "a", "b")
		before := snapshotKeyspaceStats(handler)
		execCmd(t, handler, state, "LPOS", "l", "a")
		execCmd(t, handler, state, "LPOS", "nokey", "a")
		assertKeyspaceDelta(t, handler, before, 1, 1)
	})

	t.Run("LCS counts one lookup per key", func(t *testing.T) {
		execCmd(t, handler, state, "SET", "lcs1", "abc")
		before := snapshotKeyspaceStats(handler)
		execCmd(t, handler, state, "LCS", "lcs1", "lcs2") // +1hit +1miss
		execCmd(t, handler, state, "LCS", "lcs1", "lcs1") // +2hits
		assertKeyspaceDelta(t, handler, before, 3, 1)
	})

	t.Run("SINTER counts per key", func(t *testing.T) {
		execCmd(t, handler, state, "SADD", "s1", "a")
		before := snapshotKeyspaceStats(handler)
		execCmd(t, handler, state, "SINTER", "s1", "s2", "s3") // +1hit +2miss
		assertKeyspaceDelta(t, handler, before, 1, 2)
	})

	t.Run("ZMSCORE single key lookup for whole command", func(t *testing.T) {
		execCmd(t, handler, state, "ZADD", "z", "1", "m1", "2", "m2")
		before := snapshotKeyspaceStats(handler)
		execCmd(t, handler, state, "ZMSCORE", "z", "m1", "m2", "gone")
		assertKeyspaceDelta(t, handler, before, 1, 0)
	})

	t.Run("ZRANGEBYSCORE ZRANDMEMBER ZDIFF", func(t *testing.T) {
		execCmd(t, handler, state, "ZADD", "zs", "1", "m")
		before := snapshotKeyspaceStats(handler)
		execCmd(t, handler, state, "ZRANGEBYSCORE", "zs", "0", "5")
		execCmd(t, handler, state, "ZRANGEBYSCORE", "none", "0", "5")
		execCmd(t, handler, state, "ZRANDMEMBER", "zs")
		execCmd(t, handler, state, "ZRANDMEMBER", "none")
		execCmd(t, handler, state, "ZDIFF", "1", "zs")
		assertKeyspaceDelta(t, handler, before, 3, 2)
	})

	t.Run("PFCOUNT counts per key", func(t *testing.T) {
		execCmd(t, handler, state, "PFADD", "pf1", "a")
		before := snapshotKeyspaceStats(handler)
		execCmd(t, handler, state, "PFCOUNT", "pf1", "pf2", "pf3") // +1hit +2miss
		assertKeyspaceDelta(t, handler, before, 1, 2)
	})

	t.Run("TOUCH counts per key", func(t *testing.T) {
		execCmd(t, handler, state, "SET", "t1", "1")
		before := snapshotKeyspaceStats(handler)
		resp := execCmd(t, handler, state, "TOUCH", "t1", "t2", "t3")
		integer, ok := resp.(*proto.Integer)
		assert.True(t, ok)
		assert.Equal(t, int64(1), int64(*integer))
		assertKeyspaceDelta(t, handler, before, 1, 2)
	})

	t.Run("SORT_RO counts input key", func(t *testing.T) {
		execCmd(t, handler, state, "RPUSH", "sl", "3", "1", "2")
		before := snapshotKeyspaceStats(handler)
		execCmd(t, handler, state, "SORT_RO", "sl")
		execCmd(t, handler, state, "SORT_RO", "nokey")
		assertKeyspaceDelta(t, handler, before, 1, 1)
	})

	t.Run("BITOP counts sources only not dest", func(t *testing.T) {
		execCmd(t, handler, state, "SET", "b1", "aa")
		before := snapshotKeyspaceStats(handler)
		execCmd(t, handler, state, "BITOP", "AND", "bdest", "b1", "b2") // +1hit +1miss
		assertKeyspaceDelta(t, handler, before, 1, 1)
	})

	t.Run("COPY counts source key", func(t *testing.T) {
		execCmd(t, handler, state, "SET", "csrc", "v")
		before := snapshotKeyspaceStats(handler)
		execCmd(t, handler, state, "COPY", "csrc", "cdst")   // +1hit
		execCmd(t, handler, state, "COPY", "nocopy", "cdst") // +1miss
		assertKeyspaceDelta(t, handler, before, 1, 1)
	})

	t.Run("plain SET records nothing but SET GET does", func(t *testing.T) {
		execCmd(t, handler, state, "SET", "sg", "v")
		before := snapshotKeyspaceStats(handler)
		execCmd(t, handler, state, "SET", "sg", "v2")
		assertKeyspaceDelta(t, handler, before, 0, 0)

		before = snapshotKeyspaceStats(handler)
		execCmd(t, handler, state, "SET", "sg", "v3", "GET") // old value exists
		assertKeyspaceDelta(t, handler, before, 1, 0)

		before = snapshotKeyspaceStats(handler)
		execCmd(t, handler, state, "SET", "sgfresh", "v", "GET") // missing
		assertKeyspaceDelta(t, handler, before, 0, 1)
	})
}

// TestKeyspaceStats_XREAD_DoubleLookup replicates redis-server 8.2.1 behavior:
// XREAD/XREADGROUP record TWO lookups per stream key on the success path but a
// single one when the command errors (e.g. NOGROUP).
func TestKeyspaceStats_XREAD_DoubleLookup(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	t.Run("XREAD success counts twice per stream", func(t *testing.T) {
		execCmd(t, handler, state, "XADD", "st", "*", "f", "v")
		before := snapshotKeyspaceStats(handler)
		resp := execCmd(t, handler, state, "XREAD", "STREAMS", "st", "0")
		_, isNull := resp.(*proto.BulkString)
		_ = isNull
		assertKeyspaceDelta(t, handler, before, 2, 0)
	})

	t.Run("XREAD missing stream still counts twice", func(t *testing.T) {
		before := snapshotKeyspaceStats(handler)
		execCmd(t, handler, state, "XREAD", "STREAMS", "nostream", "0")
		assertKeyspaceDelta(t, handler, before, 0, 2)
	})

	t.Run("XREADGROUP success counts twice", func(t *testing.T) {
		execCmd(t, handler, state, "XADD", "grst", "*", "f", "v")
		execCmd(t, handler, state, "XGROUP", "CREATE", "grst", "g1", "0")
		before := snapshotKeyspaceStats(handler)
		execCmd(t, handler, state, "XREADGROUP", "GROUP", "g1", "c1", "STREAMS", "grst", ">")
		assertKeyspaceDelta(t, handler, before, 2, 0)
	})

	t.Run("XREADGROUP existing stream missing group counts one hit", func(t *testing.T) {
		// redis looks up the key (hit) and then fails group resolution.
		before := snapshotKeyspaceStats(handler)
		resp := execCmd(t, handler, state, "XREADGROUP", "GROUP", "nosuchgroup", "c1", "STREAMS", "grst", ">")
		errResp, ok := resp.(*proto.Error)
		if !ok {
			t.Fatalf("expected NOGROUP error reply, got %T", resp)
		}
		assert.Equal(t, "NOGROUP No such key 'grst' or consumer group 'nosuchgroup' in XREADGROUP with GROUP option", string(*errResp))
		assertKeyspaceDelta(t, handler, before, 1, 0)
	})

	t.Run("XREADGROUP missing stream errors NOGROUP and counts one miss", func(t *testing.T) {
		before := snapshotKeyspaceStats(handler)
		resp := execCmd(t, handler, state, "XREADGROUP", "GROUP", "g2", "c1", "STREAMS", "nostreamgrp", ">")
		_, ok := resp.(*proto.Error)
		if !ok {
			t.Fatalf("expected NOGROUP error reply, got %T", resp)
		}
		assertKeyspaceDelta(t, handler, before, 0, 1)
	})
}

// TestKeyspaceStats_XCLAIM_NOGROUP verifies the XCLAIM NOGROUP error surface
// (previously the missing-group case silently returned an empty result).
func TestKeyspaceStats_XCLAIM_NOGROUP(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	before := snapshotKeyspaceStats(handler)
	resp := execCmd(t, handler, state, "XCLAIM", "nostream", "nogroup", "c1", "0", "1-1")
	errResp, ok := resp.(*proto.Error)
	if !ok {
		t.Fatalf("expected NOGROUP error reply, got %T", resp)
	}
	assert.Equal(t, "NOGROUP No such key 'nostream' or consumer group 'nogroup'", string(*errResp))
	assertKeyspaceDelta(t, handler, before, 0, 1)
}
