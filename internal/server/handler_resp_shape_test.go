package server

import (
	"testing"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/zeebo/assert"
)

func shapeNestedArray(t *testing.T, resp proto.RESP, minElems int) *proto.NestedArray {
	t.Helper()
	na, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.True(t, len(na.Elems) >= minElems)
	return na
}

func shapeBulkString(t *testing.T, resp proto.RESP, minLen int) string {
	t.Helper()
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.True(t, len(*bs) >= minLen)
	return string(*bs)
}

func shapeIsNilBulk(t *testing.T, resp proto.RESP) {
	t.Helper()
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.True(t, bs == nil || *bs == nil)
}

func shapeInteger(t *testing.T, resp proto.RESP) int64 {
	t.Helper()
	iv, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	return int64(*iv)
}

// TestRESPShape_SCAN verifies SCAN returns [cursor, [keys...]]
// Note: SCAN uses NewScanResponse (RawString), not NestedArray directly.
// We verify the string representation contains the expected structure.
func TestRESPShape_SCAN(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("sk1"), []byte("v1")}, "127.0.0.1:12345")
	handler.executeCommand(state, "SET", [][]byte{[]byte("sk2"), []byte("v2")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "SCAN", [][]byte{[]byte("0")}, "127.0.0.1:12345")
	raw, ok := resp.(proto.RawString)
	assert.True(t, ok)
	// RawString format: *2\r\n$<cursor>\r\n<cursor>\r\n*<n>\r\n...
	rawStr := string(raw)
	assert.True(t, len(rawStr) > 0)
	// First line should be *2 (outer array with cursor + keys)
	assert.Equal(t, "*2", rawStr[:2])
}

// TestRESPShape_SSCAN verifies SSCAN returns [cursor, [members...]]
func TestRESPShape_SSCAN(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SADD", [][]byte{[]byte("sset"), []byte("a"), []byte("b")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "SSCAN", [][]byte{[]byte("sset"), []byte("0")}, "127.0.0.1:12345")
	na := shapeNestedArray(t, resp, 2)
	// Element 0: cursor
	shapeBulkString(t, na.Elems[0], 1)
	// Element 1: array of members
	membersArr, ok := na.Elems[1].(*proto.NestedArray)
	assert.True(t, ok)
	assert.True(t, len(membersArr.Elems) >= 1)
}

// TestRESPShape_HSCAN verifies HSCAN returns [cursor, [field, val, field, val...]]
func TestRESPShape_HSCAN(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "HSET", [][]byte{[]byte("hkey"), []byte("f1"), []byte("v1"), []byte("f2"), []byte("v2")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "HSCAN", [][]byte{[]byte("hkey"), []byte("0")}, "127.0.0.1:12345")
	na := shapeNestedArray(t, resp, 2)
	// Element 0: cursor
	shapeBulkString(t, na.Elems[0], 1)
	// Element 1: array of field-value pairs
	fvArr, ok := na.Elems[1].(*proto.NestedArray)
	assert.True(t, ok)
	assert.True(t, len(fvArr.Elems) >= 2)

	// Each pair should be bulk strings
	shapeBulkString(t, fvArr.Elems[0], 1)
	shapeBulkString(t, fvArr.Elems[1], 1)
}

// TestRESPShape_ZSCAN verifies ZSCAN returns [cursor, [member, score...]]
func TestRESPShape_ZSCAN(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zskey"), []byte("1.0"), []byte("a"), []byte("2.0"), []byte("b")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZSCAN", [][]byte{[]byte("zskey"), []byte("0")}, "127.0.0.1:12345")
	na := shapeNestedArray(t, resp, 2)
	// Element 0: cursor as Integer
	_, ok := na.Elems[0].(proto.Integer)
	assert.True(t, ok)
	// Element 1: members as Array of [member, score, ...]
	membersArr, ok := na.Elems[1].(*proto.Array)
	assert.True(t, ok)
	assert.True(t, len(membersArr.Args) >= 2)
}

// TestRESPShape_GEOPOS verifies GEOPOS returns [ [lon, lat], ... ] or nil
func TestRESPShape_GEOPOS(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "GEOADD", [][]byte{[]byte("gpos"), []byte("116.40"), []byte("39.90"), []byte("bj")}, "127.0.0.1:12345")

	// Found member
	resp := handler.executeCommand(state, "GEOPOS", [][]byte{[]byte("gpos"), []byte("bj")}, "127.0.0.1:12345")
	na := shapeNestedArray(t, resp, 1)
	// Each element is either [lon, lat] or nil
	coord, ok := na.Elems[0].(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 2, len(coord.Elems))
	shapeBulkString(t, coord.Elems[0], 1) // lon
	shapeBulkString(t, coord.Elems[1], 1) // lat

	// Non-existent member returns nil
	resp = handler.executeCommand(state, "GEOPOS", [][]byte{[]byte("gpos"), []byte("nonexist")}, "127.0.0.1:12345")
	na = shapeNestedArray(t, resp, 1)
	shapeIsNilBulk(t, na.Elems[0])
}

// TestRESPShape_GEOSEARCH_WITHCOORD verifies GEOSEARCH WITHCOORD returns [member, [lon, lat]]
func TestRESPShape_GEOSEARCH_WITHCOORD(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "GEOADD", [][]byte{[]byte("gsk"), []byte("-122.4194"), []byte("37.7749"), []byte("SF")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "GEOSEARCH", [][]byte{[]byte("gsk"), []byte("FROMLONLAT"), []byte("-122.4194"), []byte("37.7749"), []byte("BYRADIUS"), []byte("100"), []byte("km"), []byte("WITHCOORD")}, "127.0.0.1:12345")
	na := shapeNestedArray(t, resp, 1)
	// Each element is [member, [lon, lat]]
	entry, ok := na.Elems[0].(*proto.NestedArray)
	assert.True(t, ok)
	assert.True(t, len(entry.Elems) >= 2)
	shapeBulkString(t, entry.Elems[0], 1) // member name
	coord, ok := entry.Elems[1].(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 2, len(coord.Elems))
}

// TestRESPShape_GEORADIUS_WITHCOORD verifies GEORADIUS WITHCOORD returns [member, [lon, lat]]
func TestRESPShape_GEORADIUS_WITHCOORD(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "GEOADD", [][]byte{[]byte("grk"), []byte("-122.4194"), []byte("37.7749"), []byte("SF")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "GEORADIUS", [][]byte{[]byte("grk"), []byte("-122.4194"), []byte("37.7749"), []byte("100"), []byte("km"), []byte("WITHCOORD")}, "127.0.0.1:12345")
	na := shapeNestedArray(t, resp, 1)
	entry, ok := na.Elems[0].(*proto.NestedArray)
	assert.True(t, ok)
	assert.True(t, len(entry.Elems) >= 2)
	shapeBulkString(t, entry.Elems[0], 1)
	coord, ok := entry.Elems[1].(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 2, len(coord.Elems))
}

// TestRESPShape_XINFO_CONSUMERS verifies XINFO CONSUMERS returns per-consumer NestedArrays
func TestRESPShape_XINFO_CONSUMERS(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Create stream + group + consumer
	id := handler.executeCommand(state, "XADD", [][]byte{[]byte("xistream"), []byte("1-0"), []byte("f"), []byte("v")}, "127.0.0.1:12345").(*proto.BulkString)
	_ = id
	handler.executeCommand(state, "XGROUP", [][]byte{[]byte("CREATE"), []byte("xistream"), []byte("g1"), []byte("0")}, "127.0.0.1:12345")
	handler.executeCommand(state, "XREADGROUP", [][]byte{[]byte("GROUP"), []byte("g1"), []byte("c1"), []byte("STREAMS"), []byte("xistream"), []byte(">")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "XINFO", [][]byte{[]byte("CONSUMERS"), []byte("xistream"), []byte("g1")}, "127.0.0.1:12345")
	na := shapeNestedArray(t, resp, 1)
	// Each consumer is a NestedArray of key-value pairs
	consumerArr, ok := na.Elems[0].(*proto.NestedArray)
	assert.True(t, ok)
	assert.True(t, len(consumerArr.Elems) >= 2)
	shapeBulkString(t, consumerArr.Elems[0], 1) // "name"
	shapeBulkString(t, consumerArr.Elems[1], 1) // consumer name
}

// TestRESPShape_XAUTOCLAIM verifies XAUTOCLAIM returns [nextID, [entries...]]
func TestRESPShape_XAUTOCLAIM(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "XADD", [][]byte{[]byte("xack"), []byte("1-0"), []byte("f1"), []byte("v1")}, "127.0.0.1:12345")
	handler.executeCommand(state, "XGROUP", [][]byte{[]byte("CREATE"), []byte("xack"), []byte("gg"), []byte("0")}, "127.0.0.1:12345")
	handler.executeCommand(state, "XREADGROUP", [][]byte{[]byte("GROUP"), []byte("gg"), []byte("c1"), []byte("STREAMS"), []byte("xack"), []byte(">")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "XAUTOCLAIM", [][]byte{[]byte("xack"), []byte("gg"), []byte("c2"), []byte("0"), []byte("0-0"), []byte("COUNT"), []byte("10")}, "127.0.0.1:12345")
	// Should be [nextID, [entries...]]
	na := shapeNestedArray(t, resp, 2)
	// Element 0: nextID
	shapeBulkString(t, na.Elems[0], 1)
	// Element 1: array of entries
	entriesArr, ok := na.Elems[1].(*proto.NestedArray)
	assert.True(t, ok)
	assert.True(t, len(entriesArr.Elems) >= 1)
	// Each entry is [id, [fields...]]
	entry, ok := entriesArr.Elems[0].(*proto.NestedArray)
	assert.True(t, ok)
	assert.True(t, len(entry.Elems) >= 2)
	shapeBulkString(t, entry.Elems[0], 1) // id
	fields, ok := entry.Elems[1].(*proto.NestedArray)
	assert.True(t, ok)
	assert.True(t, len(fields.Elems) >= 2) // at least one field pair
}

// TestRESPShape_XAUTOCLAIM_JUSTID verifies XAUTOCLAIM JUSTID returns [nextID, [id, id, ...]]
func TestRESPShape_XAUTOCLAIM_JUSTID(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "XADD", [][]byte{[]byte("xajid"), []byte("1-0"), []byte("f1"), []byte("v1")}, "127.0.0.1:12345")
	handler.executeCommand(state, "XGROUP", [][]byte{[]byte("CREATE"), []byte("xajid"), []byte("gg"), []byte("0")}, "127.0.0.1:12345")
	handler.executeCommand(state, "XREADGROUP", [][]byte{[]byte("GROUP"), []byte("gg"), []byte("c1"), []byte("STREAMS"), []byte("xajid"), []byte(">")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "XAUTOCLAIM", [][]byte{[]byte("xajid"), []byte("gg"), []byte("c2"), []byte("0"), []byte("0-0"), []byte("COUNT"), []byte("10"), []byte("JUSTID")}, "127.0.0.1:12345")
	na := shapeNestedArray(t, resp, 2)
	shapeBulkString(t, na.Elems[0], 1)
	entriesArr, ok := na.Elems[1].(*proto.NestedArray)
	assert.True(t, ok)
	assert.True(t, len(entriesArr.Elems) >= 1)
	// With JUSTID, entries are bulk strings, not sub-arrays
	shapeBulkString(t, entriesArr.Elems[0], 1)
}

// TestRESPShape_XINFO_GROUPS verifies XINFO GROUPS returns per-group NestedArrays
func TestRESPShape_XINFO_GROUPS(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "XADD", [][]byte{[]byte("xig"), []byte("1-0"), []byte("f"), []byte("v")}, "127.0.0.1:12345")
	handler.executeCommand(state, "XGROUP", [][]byte{[]byte("CREATE"), []byte("xig"), []byte("g1"), []byte("0")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "XINFO", [][]byte{[]byte("GROUPS"), []byte("xig")}, "127.0.0.1:12345")
	na := shapeNestedArray(t, resp, 1)
	groupArr, ok := na.Elems[0].(*proto.NestedArray)
	assert.True(t, ok)
	assert.True(t, len(groupArr.Elems) >= 2)
}

// TestRESPShape_GEOPOS_FlatNoModifier verifies GEORADIUS/GEOSEARCH without
// modifiers returns flat member-name array (not nested)
func TestRESPShape_GEORADIUS_FlatNoModifier(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "GEOADD", [][]byte{[]byte("gfnm"), []byte("-122.4194"), []byte("37.7749"), []byte("SF")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "GEORADIUS", [][]byte{[]byte("gfnm"), []byte("-122.4194"), []byte("37.7749"), []byte("100"), []byte("km")}, "127.0.0.1:12345")
	// Without modifiers — flat Array of member names
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.True(t, len(arr.Args) >= 1)
	assert.Equal(t, "SF", string(arr.Args[0]))
}

// TestRESPShape_XREAD verifies XREAD returns array of [streamKey, [entries...]]
func TestRESPShape_XREAD(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "XADD", [][]byte{[]byte("xrstream"), []byte("1-0"), []byte("f1"), []byte("v1")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "XREAD", [][]byte{[]byte("STREAMS"), []byte("xrstream"), []byte("0-0")}, "127.0.0.1:12345")
	na := shapeNestedArray(t, resp, 1)
	// Each stream: [key, [entries...]]
	streamArr, ok := na.Elems[0].(*proto.NestedArray)
	assert.True(t, ok)
	assert.True(t, len(streamArr.Elems) == 2)
	shapeBulkString(t, streamArr.Elems[0], 1) // stream key
	entriesArr, ok := streamArr.Elems[1].(*proto.NestedArray)
	assert.True(t, ok)
	assert.True(t, len(entriesArr.Elems) >= 1)
	// Each entry: [id, [fields...]]
	entry, ok := entriesArr.Elems[0].(*proto.NestedArray)
	assert.True(t, ok)
	assert.True(t, len(entry.Elems) == 2)
	shapeBulkString(t, entry.Elems[0], 1) // id
	fieldsArr, ok := entry.Elems[1].(*proto.NestedArray)
	assert.True(t, ok)
	assert.True(t, len(fieldsArr.Elems) >= 2)
}

// TestRESPShape_XRANGE verifies XRANGE returns [ [id, [fields...]], ... ]
func TestRESPShape_XRANGE(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "XADD", [][]byte{[]byte("xrk"), []byte("1-0"), []byte("f1"), []byte("v1")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "XRANGE", [][]byte{[]byte("xrk"), []byte("-"), []byte("+")}, "127.0.0.1:12345")
	na := shapeNestedArray(t, resp, 1)
	entry, ok := na.Elems[0].(*proto.NestedArray)
	assert.True(t, ok)
	assert.True(t, len(entry.Elems) == 2)
	shapeBulkString(t, entry.Elems[0], 1)
	fieldsArr, ok := entry.Elems[1].(*proto.NestedArray)
	assert.True(t, ok)
	assert.True(t, len(fieldsArr.Elems) >= 2)
}

// TestRESPShape_ExistsKey verifies EXISTS returns Integer
func TestRESPShape_ExistsKey(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("ek1"), []byte("v")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "EXISTS", [][]byte{[]byte("ek1")}, "127.0.0.1:12345")
	shapeInteger(t, resp)
}

// TestRESPShape_Del verifies DEL returns Integer count
func TestRESPShape_Del(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("dk1"), []byte("v")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "DEL", [][]byte{[]byte("dk1")}, "127.0.0.1:12345")
	shapeInteger(t, resp)
}

// TestRESPShape_CLUSTER_SLOTS tests the CLUSTER SLOTS response shape
func TestRESPShape_CLUSTER_SLOTS(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping cluster test in short mode")
	}
	// CLUSTER SLOTS needs a real cluster setup
	// This test verifies the [][]interface{} → RESP conversion
	// by calling the handler via a server with cluster mode

	// The cluster package requires node configuration that's hard to set up
	// in a unit test. Shape validation happens in cluster integration tests.
	t.Skip("CLUSTER SLOTS requires multi-node cluster setup")
}

// TestRESPShape_CommandInfo verifies COMMAND returns per-command NestedArrays
func TestRESPShape_CommandInfo(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "COMMAND", [][]byte{}, "127.0.0.1:12345")
	na := shapeNestedArray(t, resp, 1)
	// Each command is a NestedArray
	_, ok := na.Elems[0].(*proto.NestedArray)
	assert.True(t, ok)
}

// TestRESPShape_CommandCount verifies COMMAND COUNT returns Integer
func TestRESPShape_CommandCount(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "COMMAND", [][]byte{[]byte("COUNT")}, "127.0.0.1:12345")
	shapeInteger(t, resp)
}

// TestRESPShape_XREADGROUP verifies XREADGROUP returns same shape as XREAD
func TestRESPShape_XREADGROUP(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "XADD", [][]byte{[]byte("xrgk"), []byte("1-0"), []byte("f1"), []byte("v1")}, "127.0.0.1:12345")
	handler.executeCommand(state, "XGROUP", [][]byte{[]byte("CREATE"), []byte("xrgk"), []byte("gg"), []byte("0")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "XREADGROUP", [][]byte{[]byte("GROUP"), []byte("gg"), []byte("c1"), []byte("STREAMS"), []byte("xrgk"), []byte(">")}, "127.0.0.1:12345")
	na := shapeNestedArray(t, resp, 1)
	streamArr, ok := na.Elems[0].(*proto.NestedArray)
	assert.True(t, ok)
	assert.True(t, len(streamArr.Elems) == 2)
	shapeBulkString(t, streamArr.Elems[0], 1)
	entriesArr, ok := streamArr.Elems[1].(*proto.NestedArray)
	assert.True(t, ok)
	assert.True(t, len(entriesArr.Elems) >= 1)
	entry, ok := entriesArr.Elems[0].(*proto.NestedArray)
	assert.True(t, ok)
	assert.True(t, len(entry.Elems) == 2)
}

// TestRESPShape_XPENDING tests XPENDING returns summary + per-entry arrays
func TestRESPShape_XPENDING(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "XADD", [][]byte{[]byte("xpk"), []byte("1-0"), []byte("f"), []byte("v")}, "127.0.0.1:12345")
	handler.executeCommand(state, "XGROUP", [][]byte{[]byte("CREATE"), []byte("xpk"), []byte("gg"), []byte("0")}, "127.0.0.1:12345")
	handler.executeCommand(state, "XREADGROUP", [][]byte{[]byte("GROUP"), []byte("gg"), []byte("c1"), []byte("STREAMS"), []byte("xpk"), []byte(">")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "XPENDING", [][]byte{[]byte("xpk"), []byte("gg")}, "127.0.0.1:12345")
	na := shapeNestedArray(t, resp, 4)
	// Element 0: count as Integer (value, not pointer)
	_, ok := na.Elems[0].(proto.Integer)
	assert.True(t, ok)
}

// TestRESPShape_XINFO_STREAM verifies XINFO STREAM returns flat key-value array
func TestRESPShape_XINFO_STREAM(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "XADD", [][]byte{[]byte("xisk"), []byte("1-0"), []byte("f"), []byte("v")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "XINFO", [][]byte{[]byte("STREAM"), []byte("xisk")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.True(t, len(arr.Args) >= 4)
}

// TestRESPShape_SetReturnsOk verifies SET returns SimpleString("OK")
func TestRESPShape_SetReturnsOk(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()
	resp := handler.executeCommand(state, "SET", [][]byte{[]byte("ork"), []byte("v")}, "127.0.0.1:12345")
	ss, ok := resp.(*proto.SimpleString)
	assert.True(t, ok)
	assert.Equal(t, "OK", string(*ss))
}

// TestRESPShape_HSetReturnsInteger verifies HSET returns Integer count
func TestRESPShape_HSetReturnsInteger(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()
	resp := handler.executeCommand(state, "HSET", [][]byte{[]byte("ohk"), []byte("f"), []byte("v")}, "127.0.0.1:12345")
	_, ok := resp.(*proto.Integer)
	assert.True(t, ok)
}

// TestRESPShape_SAddReturnsInteger verifies SADD returns Integer count
func TestRESPShape_SAddReturnsInteger(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()
	resp := handler.executeCommand(state, "SADD", [][]byte{[]byte("osk"), []byte("a")}, "127.0.0.1:12345")
	_, ok := resp.(*proto.Integer)
	assert.True(t, ok)
}

// TestRESPShape_WrongType verifies type errors return proper WRONGTYPE Error
func TestRESPShape_WrongType(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "LPUSH", [][]byte{[]byte("wtk"), []byte("a")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "GET", [][]byte{[]byte("wtk")}, "127.0.0.1:12345")
	errStr, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, len(string(*errStr)) > 0)
}

func TestRESPShape_GetNonExistent(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()
	resp := handler.executeCommand(state, "GET", [][]byte{[]byte("nonexist")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.True(t, bs == nil || *bs == nil)
}

func TestRESPShape_TypeNonExistent(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()
	resp := handler.executeCommand(state, "TYPE", [][]byte{[]byte("nonexist")}, "127.0.0.1:12345")
	ss, ok := resp.(*proto.SimpleString)
	assert.True(t, ok)
	assert.Equal(t, "none", string(*ss))
}


