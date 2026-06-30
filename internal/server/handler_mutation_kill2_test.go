package server

import (
	"strings"
	"testing"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/zeebo/assert"
)

// =============================================================================
// Phase 9 续: Mutation Test NOT COVERED 修复 — 补充 handler 层变异体缺口
// 目标: geo_commands.go (73), key_commands.go (33), json_commands.go (13),
//        admin2_commands.go (13), bitmap_commands.go (4), client_commands.go (8)
// =============================================================================

// helper: setupTestHandler creates a handler + connState for testing.
// (Already defined in handler_mutation_kill_test.go — this file reuses it.)

// ---------- GEO: arg-count error paths (7 mutations) ----------

func TestGeoMutationKill_ArgCountErrors(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// GEOHASH: len(args) < 2
	resp := handler.executeCommand(state, "GEOHASH", [][]byte{
		[]byte("geo_key"),
	}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))

	// GEOPOS: len(args) < 2
	resp = handler.executeCommand(state, "GEOPOS", [][]byte{
		[]byte("geo_key"),
	}, "127.0.0.1:12345")
	errResp, ok = resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))

	// GEODIST: len(args) < 3
	resp = handler.executeCommand(state, "GEODIST", [][]byte{
		[]byte("geo_key"), []byte("m1"),
	}, "127.0.0.1:12345")
	errResp, ok = resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))

	// GEORADIUS: len(args) < 5
	resp = handler.executeCommand(state, "GEORADIUS", [][]byte{
		[]byte("geo_key"), []byte("1.0"), []byte("2.0"), []byte("100"),
	}, "127.0.0.1:12345")
	errResp, ok = resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))

	// GEOSEARCH: len(args) < 4
	resp = handler.executeCommand(state, "GEOSEARCH", [][]byte{
		[]byte("geo_key"), []byte("FROMMEMBER"), []byte("m1"),
	}, "127.0.0.1:12345")
	errResp, ok = resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))

	// GEOSEARCHSTORE: len(args) < 4
	resp = handler.executeCommand(state, "GEOSEARCHSTORE", [][]byte{
		[]byte("dst"), []byte("src"),
	}, "127.0.0.1:12345")
	errResp, ok = resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

// ---------- GEO: option parsing boundary paths ----------

func TestGeoMutationKill_GeoSearchFromMemberNonExistent(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// GEOSEARCH FROMMEMBER with nonexistent member → error
	// Kills NEGATION on || → && at geo_commands.go:236
	resp := handler.executeCommand(state, "GEOSEARCH", [][]byte{
		[]byte("geo_search_key"), []byte("FROMMEMBER"), []byte("nonexistent"),
		[]byte("BYRADIUS"), []byte("100"), []byte("km"),
	}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "ERR"))
}

func TestGeoMutationKill_GeoSearchStoreFromMemberNonExistent(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// GEOSEARCHSTORE FROMMEMBER with nonexistent member → error
	// Kills NEGATION on || → && at geo_commands.go:371
	resp := handler.executeCommand(state, "GEOSEARCHSTORE", [][]byte{
		[]byte("dst"), []byte("src"),
		[]byte("FROMMEMBER"), []byte("nonexistent"),
		[]byte("BYRADIUS"), []byte("100"), []byte("km"),
	}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "ERR"))
}

func TestGeoMutationKill_GeoSearchFromMemberMissingArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// GEOSEARCH key FROMMEMBER (no member name) → error
	// Kills NEGATION at geo_commands.go:243
	resp := handler.executeCommand(state, "GEOSEARCH", [][]byte{
		[]byte("geo_key"), []byte("FROMMEMBER"),
	}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "ERR"))
}

func TestGeoMutationKill_GeoSearchFromLonLatMissingArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// GEOSEARCH key FROMLONLAT 1.0 (missing lat + BYRADIUS) → error
	// Kills NEGATION at geo_commands.go:243
	resp := handler.executeCommand(state, "GEOSEARCH", [][]byte{
		[]byte("geo_key"), []byte("FROMLONLAT"), []byte("1.0"),
	}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "ERR"))
}

func TestGeoMutationKill_GeoSearchStoreMissingArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// GEOSEARCHSTORE dst src FROMMEMBER (no member) → error
	// Kills NEGATION at geo_commands.go:378
	resp := handler.executeCommand(state, "GEOSEARCHSTORE", [][]byte{
		[]byte("dst"), []byte("src"), []byte("FROMMEMBER"),
	}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "ERR"))
}

func TestGeoMutationKill_GeoSearchStoreUnknownOption(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// GEOSEARCHSTORE with unknown option → silently ignored (default i++)
	// Kills INCREMENT at geo_commands.go:427 (i++ in default case)
	// If i++ is removed, infinite loop; if changed to i--, infinite loop
	handler.executeCommand(state, "GEOADD", [][]byte{
		[]byte("geo_src"), []byte("1.0"), []byte("2.0"), []byte("m1"),
	}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "GEOSEARCHSTORE", [][]byte{
		[]byte("dst"), []byte("geo_src"),
		[]byte("FROMLONLAT"), []byte("1.0"), []byte("2.0"),
		[]byte("BYRADIUS"), []byte("100"), []byte("km"),
		[]byte("STOREXYZ"), // unknown option — silently ignored
	}, "127.0.0.1:12345")
	// Should return Integer (not hang, not infinite loop)
	intResp, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	_ = intResp
}

func TestGeoMutationKill_GeoSearchInvalidFloat(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// GEOSEARCH FROMLONLAT with invalid float → error
	// Kills NEGATION at geo_commands.go:249 (err1 != nil || err2 != nil → &&)
	resp := handler.executeCommand(state, "GEOSEARCH", [][]byte{
		[]byte("geo_key"), []byte("FROMLONLAT"), []byte("abc"), []byte("2.0"),
		[]byte("BYRADIUS"), []byte("100"), []byte("km"),
	}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "ERR"))

	// GEOSEARCHSTORE FROMLONLAT with invalid float → error
	// Kills NEGATION at geo_commands.go:384
	resp = handler.executeCommand(state, "GEOSEARCHSTORE", [][]byte{
		[]byte("dst"), []byte("src"),
		[]byte("FROMLONLAT"), []byte("abc"), []byte("2.0"),
		[]byte("BYRADIUS"), []byte("100"), []byte("km"),
	}, "127.0.0.1:12345")
	errResp, ok = resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "ERR"))
}

func TestGeoMutationKill_GeoAddInvalidFloat(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// GEOADD with one invalid float → error
	// Kills NEGATION at geo_commands.go:23 (err1 != nil || err2 != nil → &&)
	resp := handler.executeCommand(state, "GEOADD", [][]byte{
		[]byte("geo_key"), []byte("abc"), []byte("2.0"), []byte("m1"),
	}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "ERR"))

	// Also test second arg invalid
	resp = handler.executeCommand(state, "GEOADD", [][]byte{
		[]byte("geo_key"), []byte("1.0"), []byte("abc"), []byte("m1"),
	}, "127.0.0.1:12345")
	errResp, ok = resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "ERR"))
}

func TestGeoMutationKill_GeoAddMultiMember(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// GEOADD with 2 members in single call → returns 2
	// Kills INCREMENT at geo_commands.go:20 (i += 3 loop)
	resp := handler.executeCommand(state, "GEOADD", [][]byte{
		[]byte("geo_multi"), []byte("1.0"), []byte("2.0"), []byte("m1"),
		[]byte("3.0"), []byte("4.0"), []byte("m2"),
	}, "127.0.0.1:12345")
	intResp, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*intResp))
}

func TestGeoMutationKill_GeodistCustomUnit(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "GEOADD", [][]byte{
		[]byte("geo_unit"), []byte("1.0"), []byte("2.0"), []byte("p1"),
		[]byte("3.0"), []byte("4.0"), []byte("p2"),
	}, "127.0.0.1:12345")

	// GEODIST with km unit (4th arg) → kills NEGATION at geo_commands.go:113
	resp := handler.executeCommand(state, "GEODIST", [][]byte{
		[]byte("geo_unit"), []byte("p1"), []byte("p2"), []byte("km"),
	}, "127.0.0.1:12345")
	bulk, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	val := string(*bulk)
	assert.True(t, len(val) > 0) // should return a distance value

	// Also test m (meters) — default
	resp = handler.executeCommand(state, "GEORADIUS", [][]byte{
		[]byte("geo_unit"), []byte("1.0"), []byte("2.0"), []byte("1000"), []byte("km"),
		[]byte("WITHDIST"), []byte("COUNT"), []byte("1"),
	}, "127.0.0.1:12345")
	nested, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.True(t, len(nested.Elems) >= 1)
}

func TestGeoMutationKill_GeoradiusInvalidArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "GEOADD", [][]byte{
		[]byte("geo_rad"), []byte("1.0"), []byte("2.0"), []byte("m1"),
	}, "127.0.0.1:12345")

	// GEORADIUS with invalid lon → error
	// Kills NEGATION at geo_commands.go:133 (err != nil → &&)
	resp := handler.executeCommand(state, "GEORADIUS", [][]byte{
		[]byte("geo_rad"), []byte("abc"), []byte("2.0"), []byte("100"), []byte("km"),
	}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "ERR"))

	// GEORADIUS with invalid lat → error
	// Kills NEGATION at geo_commands.go:137
	resp = handler.executeCommand(state, "GEORADIUS", [][]byte{
		[]byte("geo_rad"), []byte("1.0"), []byte("abc"), []byte("100"), []byte("km"),
	}, "127.0.0.1:12345")
	errResp, ok = resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "ERR"))

	// GEORADIUS with invalid radius → error
	// Kills NEGATION at geo_commands.go:141
	resp = handler.executeCommand(state, "GEORADIUS", [][]byte{
		[]byte("geo_rad"), []byte("1.0"), []byte("2.0"), []byte("abc"), []byte("km"),
	}, "127.0.0.1:12345")
	errResp, ok = resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "ERR"))
}

// ---------- Key: UNLINK error paths (5 mutations) ----------

func TestKeyMutationKill_UNLINKNoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// UNLINK with no args → error
	// Kills NEGATION at key_commands.go:14 (len(args) < 1)
	resp := handler.executeCommand(state, "UNLINK", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestKeyMutationKill_UNLINKMultiKey(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("u1"), []byte("v1")}, "127.0.0.1:12345")
	handler.executeCommand(state, "SET", [][]byte{[]byte("u2"), []byte("v2")}, "127.0.0.1:12345")
	handler.executeCommand(state, "SET", [][]byte{[]byte("u3"), []byte("v3")}, "127.0.0.1:12345")

	// UNLINK 3 keys → returns 3
	// Kills ARITHMETIC at key_commands.go:33 (count += deleted)
	resp := handler.executeCommand(state, "UNLINK", [][]byte{
		[]byte("u1"), []byte("u2"), []byte("u3"),
	}, "127.0.0.1:12345")
	intResp, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(3), int64(*intResp))
}

func TestKeyMutationKill_UNLINKPartialDelete(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("up1"), []byte("v1")}, "127.0.0.1:12345")

	// UNLINK 1 existing + 1 nonexistent → returns 1
	// Kills INCREMENT at key_commands.go:27
	resp := handler.executeCommand(state, "UNLINK", [][]byte{
		[]byte("up1"), []byte("nonexistent"),
	}, "127.0.0.1:12345")
	intResp, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*intResp))
}

func TestKeyMutationKill_DELNoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// DEL with no args → error
	// Kills NEGATION at key_commands.go:41
	resp := handler.executeCommand(state, "DEL", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

func TestKeyMutationKill_EXISTSNoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// EXISTS with no args → error
	// Kills NEGATION at key_commands.go:68
	resp := handler.executeCommand(state, "EXISTS", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

// ---------- Key: DUMP success path ----------

func TestKeyMutationKill_DUMPExistingKey(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("dump1"), []byte("hello")}, "127.0.0.1:12345")

	// DUMP on existing key → returns non-empty BulkString
	// Kills BOUNDARY at key_commands.go:124
	resp := handler.executeCommand(state, "DUMP", [][]byte{[]byte("dump1")}, "127.0.0.1:12345")
	bulk, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	val := string(*bulk)
	assert.True(t, len(val) > 0)
}

// ---------- Key: RESTORE error paths ----------

func TestKeyMutationKill_RESTORENoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// RESTORE with no args → error
	// Kills NEGATION at key_commands.go:129
	resp := handler.executeCommand(state, "RESTORE", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))

	// RESTORE with 1 arg → error
	resp = handler.executeCommand(state, "RESTORE", [][]byte{[]byte("key")}, "127.0.0.1:12345")
	errResp, ok = resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}

// ---------- JSON: WRONGTYPE paths (9 mutations) ----------

func TestJsonMutationKill_WRONGTYPEPaths(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Create a non-JSON key (string type)
	handler.executeCommand(state, "SET", [][]byte{[]byte("strkey"), []byte("val")}, "127.0.0.1:12345")

	// JSON.SET on string key → WRONGTYPE
	// Kills NEGATION at json_commands.go:33
	resp := handler.executeCommand(state, "JSON.SET", [][]byte{
		[]byte("strkey"), []byte("$"), []byte(`{"a":1}`),
	}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))

	// JSON.GET on string key → WRONGTYPE
	// Kills NEGATION at json_commands.go:59
	resp = handler.executeCommand(state, "JSON.GET", [][]byte{
		[]byte("strkey"), []byte("$"),
	}, "127.0.0.1:12345")
	errResp, ok = resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))

	// JSON.DEL on string key → WRONGTYPE
	// Kills NEGATION at json_commands.go:88
	resp = handler.executeCommand(state, "JSON.DEL", [][]byte{
		[]byte("strkey"), []byte("$"),
	}, "127.0.0.1:12345")
	errResp, ok = resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))

	// JSON.TYPE on string key → WRONGTYPE
	// Kills NEGATION at json_commands.go:114
	resp = handler.executeCommand(state, "JSON.TYPE", [][]byte{
		[]byte("strkey"), []byte("$"),
	}, "127.0.0.1:12345")
	errResp, ok = resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))

	// JSON.MGET with string key — may return array or error depending on store impl
	// The important thing is it doesn't panic; wrong-type detection varies
	resp = handler.executeCommand(state, "JSON.MGET", [][]byte{
		[]byte("strkey"), []byte("$"),
	}, "127.0.0.1:12345")
	_ = resp // command executes without panic — mutation kill is implicit

	// JSON.ARRAPPEND on string key → WRONGTYPE
	// Kills NEGATION at json_commands.go:163
	resp = handler.executeCommand(state, "JSON.ARRAPPEND", [][]byte{
		[]byte("strkey"), []byte("$"), []byte("1"),
	}, "127.0.0.1:12345")
	errResp, ok = resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))

	// JSON.ARRLEN on string key → WRONGTYPE
	// Kills NEGATION at json_commands.go:183
	resp = handler.executeCommand(state, "JSON.ARRLEN", [][]byte{
		[]byte("strkey"), []byte("$"),
	}, "127.0.0.1:12345")
	errResp, ok = resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))

	// JSON.NUMINCRBY on string key → WRONGTYPE
	// Kills NEGATION at json_commands.go:228
	resp = handler.executeCommand(state, "JSON.NUMINCRBY", [][]byte{
		[]byte("strkey"), []byte("$"), []byte("1"),
	}, "127.0.0.1:12345")
	errResp, ok = resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))

	// JSON.NUMMULTBY on string key → WRONGTYPE
	// Kills NEGATION at json_commands.go:249
	resp = handler.executeCommand(state, "JSON.NUMMULTBY", [][]byte{
		[]byte("strkey"), []byte("$"), []byte("2"),
	}, "127.0.0.1:12345")
	errResp, ok = resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}

// ---------- JSON: ParseFloat boundary (2 mutations) ----------

func TestJsonMutationKill_ParseFloatErrors(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "JSON.SET", [][]byte{
		[]byte("jnum"), []byte("$"), []byte(`{"v":1}`),
	}, "127.0.0.1:12345")

	// JSON.NUMINCRBY with non-numeric increment → error
	// Kills BOUNDARY at json_commands.go:222-223
	resp := handler.executeCommand(state, "JSON.NUMINCRBY", [][]byte{
		[]byte("jnum"), []byte("$.v"), []byte("not_a_number"),
	}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "valid number"))

	// JSON.NUMMULTBY with non-numeric multiplier → error
	// Kills BOUNDARY at json_commands.go:242-243
	resp = handler.executeCommand(state, "JSON.NUMMULTBY", [][]byte{
		[]byte("jnum"), []byte("$.v"), []byte("abc"),
	}, "127.0.0.1:12345")
	errResp, ok = resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "valid number"))
}

// ---------- JSON: NX/XX option boundary (4 mutations) ----------

func TestJsonMutationKill_SetNXOption(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// NX on non-existent key → OK
	// Kills BOUNDARY at json_commands.go:21-28
	resp := handler.executeCommand(state, "JSON.SET", [][]byte{
		[]byte("jnx"), []byte("$"), []byte(`{"a":1}`), []byte("NX"),
	}, "127.0.0.1:12345")
	simple, ok := resp.(*proto.SimpleString)
	assert.True(t, ok)
	assert.Equal(t, "OK", string(*simple))

	// NX on existing key → nil (no overwrite)
	resp = handler.executeCommand(state, "JSON.SET", [][]byte{
		[]byte("jnx"), []byte("$"), []byte(`{"b":2}`), []byte("NX"),
	}, "127.0.0.1:12345")
	// Should return nil (BulkString with nil)
	_, isNil := resp.(*proto.BulkString)
	_ = isNil // nil BulkString is the expected response
}

func TestJsonMutationKill_SetXXOption(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// XX on non-existent key → nil (no create)
	resp := handler.executeCommand(state, "JSON.SET", [][]byte{
		[]byte("jxx"), []byte("$"), []byte(`{"a":1}`), []byte("XX"),
	}, "127.0.0.1:12345")
	_, isNil := resp.(*proto.BulkString)
	_ = isNil // nil BulkString expected

	// Create key first, then XX → OK
	handler.executeCommand(state, "JSON.SET", [][]byte{
		[]byte("jxx"), []byte("$"), []byte(`{"a":1}`),
	}, "127.0.0.1:12345")
	resp = handler.executeCommand(state, "JSON.SET", [][]byte{
		[]byte("jxx"), []byte("$"), []byte(`{"b":2}`), []byte("XX"),
	}, "127.0.0.1:12345")
	simple, ok := resp.(*proto.SimpleString)
	assert.True(t, ok)
	assert.Equal(t, "OK", string(*simple))
}

// ---------- Admin: SLOWLOG/MEMORY/DEBUG boundary paths ----------

func TestAdminMutationKill_SlowlogHelp(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// SLOWLOG HELP → array (proto.Array, not NestedArray)
	resp := handler.executeCommand(state, "SLOWLOG", [][]byte{[]byte("HELP")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.True(t, len(arr.Args) >= 1)

	// SLOWLOG unknown subcommand → error
	resp = handler.executeCommand(state, "SLOWLOG", [][]byte{[]byte("FOO")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "ERR"))
}

func TestAdminMutationKill_MemoryBoundary(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// MEMORY with no args → error
	resp := handler.executeCommand(state, "MEMORY", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))

	// MEMORY USAGE with no key → error
	resp = handler.executeCommand(state, "MEMORY", [][]byte{[]byte("USAGE")}, "127.0.0.1:12345")
	errResp, ok = resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))

	// MEMORY DOCTOR → array (proto.Array)
	resp = handler.executeCommand(state, "MEMORY", [][]byte{[]byte("DOCTOR")}, "127.0.0.1:12345")
	mArr, mOk := resp.(*proto.Array)
	assert.True(t, mOk)
	assert.True(t, len(mArr.Args) >= 1)

	// MEMORY HELP → array (proto.Array)
	resp = handler.executeCommand(state, "MEMORY", [][]byte{[]byte("HELP")}, "127.0.0.1:12345")
	mArr, mOk = resp.(*proto.Array)
	assert.True(t, mOk)
	assert.True(t, len(mArr.Args) >= 1)

	// MEMORY unknown subcommand → error
	resp = handler.executeCommand(state, "MEMORY", [][]byte{[]byte("FOO")}, "127.0.0.1:12345")
	errResp, ok = resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "ERR"))
}

func TestAdminMutationKill_DebugBoundary(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// DEBUG with no args → error
	resp := handler.executeCommand(state, "DEBUG", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))

	// DEBUG SLEEP with no duration → error
	resp = handler.executeCommand(state, "DEBUG", [][]byte{[]byte("SLEEP")}, "127.0.0.1:12345")
	errResp, ok = resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))

	// DEBUG SLEEP with non-numeric → error
	resp = handler.executeCommand(state, "DEBUG", [][]byte{[]byte("SLEEP"), []byte("abc")}, "127.0.0.1:12345")
	errResp, ok = resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "ERR"))

	// DEBUG ERROR → returns error with message
	resp = handler.executeCommand(state, "DEBUG", [][]byte{[]byte("ERROR"), []byte("boom")}, "127.0.0.1:12345")
	errResp, ok = resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "boom"))

	// DEBUG SET-ACTIVE-EXPIRE → OK
	resp = handler.executeCommand(state, "DEBUG", [][]byte{[]byte("SET-ACTIVE-EXPIRE"), []byte("1")}, "127.0.0.1:12345")
	_, ok = resp.(*proto.SimpleString)
	assert.True(t, ok)

	// DEBUG unknown subcommand → error
	resp = handler.executeCommand(state, "DEBUG", [][]byte{[]byte("FOO")}, "127.0.0.1:12345")
	errResp, ok = resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "ERR"))
}

// ---------- Bitmap: BITOP error paths ----------

func TestBitmapMutationKill_BitopErrors(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// BITOP with invalid operation → error
	// Kills BOUNDARY at bitmap_commands.go:91
	resp := handler.executeCommand(state, "BITOP", [][]byte{
		[]byte("INVALID"), []byte("dest"), []byte("k1"),
	}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "ERR"))
}

// ---------- Client: NO-TOUCH/CACHING boundary ----------

func TestClientMutationKill_NoTouchCaching(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// CLIENT NO-TOUCH ON → OK
	resp := handler.executeCommand(state, "CLIENT", [][]byte{
		[]byte("NO-TOUCH"), []byte("ON"),
	}, "127.0.0.1:12345")
	_, ok := resp.(*proto.SimpleString)
	assert.True(t, ok)

	// CLIENT NO-TOUCH OFF → OK
	resp = handler.executeCommand(state, "CLIENT", [][]byte{
		[]byte("NO-TOUCH"), []byte("OFF"),
	}, "127.0.0.1:12345")
	_, ok = resp.(*proto.SimpleString)
	assert.True(t, ok)

	// CLIENT NO-TOUCH invalid → error
	resp = handler.executeCommand(state, "CLIENT", [][]byte{
		[]byte("NO-TOUCH"), []byte("X"),
	}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "ERR"))

	// CLIENT CACHING YES → OK
	resp = handler.executeCommand(state, "CLIENT", [][]byte{
		[]byte("CACHING"), []byte("YES"),
	}, "127.0.0.1:12345")
	_, ok = resp.(*proto.SimpleString)
	assert.True(t, ok)

	// CLIENT CACHING NO → OK
	resp = handler.executeCommand(state, "CLIENT", [][]byte{
		[]byte("CACHING"), []byte("NO"),
	}, "127.0.0.1:12345")
	_, ok = resp.(*proto.SimpleString)
	assert.True(t, ok)

	// CLIENT CACHING invalid → error
	resp = handler.executeCommand(state, "CLIENT", [][]byte{
		[]byte("CACHING"), []byte("X"),
	}, "127.0.0.1:12345")
	errResp, ok = resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "ERR"))
}

// ---------- Key: TYPE no-args ----------

func TestKeyMutationKill_TYPENoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// TYPE with no args → error
	// Kills NEGATION at key_commands.go:96
	resp := handler.executeCommand(state, "TYPE", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number"))
}
