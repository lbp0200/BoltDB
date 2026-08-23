package server

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/cluster"
	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/zeebo/assert"
)

func shapeNestedArray(t *testing.T, resp proto.RESP, minElems int) *proto.NestedArray {
	t.Helper()
	na, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.True(t, len(na.Elems) >= minElems)
	return na
}

func shapeArray(t *testing.T, resp proto.RESP, minElems int) *proto.Array {
	t.Helper()
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.True(t, len(arr.Args) >= minElems)
	return arr
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
	assert.Equal(t, 2, len(membersArr.Elems))
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
	assert.Equal(t, 4, len(fvArr.Elems))

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
	assert.Equal(t, 4, len(membersArr.Args))
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
	assert.Equal(t, 2, len(entry.Elems))
	shapeBulkString(t, entry.Elems[0], 1) // member name
	coord, ok := entry.Elems[1].(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 2, len(coord.Elems))
}

// TestRESPShape_GEORADIUSBYMEMBER verifies GEORADIUSBYMEMBER returns a flat
// array of member names without modifiers (same shape as GEORADIUS).
func TestRESPShape_GEORADIUSBYMEMBER(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "GEOADD", [][]byte{[]byte("grm"), []byte("-122.4194"), []byte("37.7749"), []byte("SF")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "GEORADIUSBYMEMBER", [][]byte{[]byte("grm"), []byte("SF"), []byte("100"), []byte("km")}, "127.0.0.1:12345")
	arr := shapeArray(t, resp, 1)
	assert.Equal(t, "SF", string(arr.Args[0]))
}

// TestRESPShape_GEORADIUS_RO verifies GEORADIUS_RO returns the same shapes as
// GEORADIUS (flat array / nested array with modifiers).
func TestRESPShape_GEORADIUS_RO(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "GEOADD", [][]byte{[]byte("gro"), []byte("-122.4194"), []byte("37.7749"), []byte("SF")}, "127.0.0.1:12345")

	// Flat: array of bulk strings
	resp := handler.executeCommand(state, "GEORADIUS_RO", [][]byte{[]byte("gro"), []byte("-122.4194"), []byte("37.7749"), []byte("100"), []byte("km")}, "127.0.0.1:12345")
	shapeArray(t, resp, 1)

	// WITHCOORD: nested array of [member, [lon, lat]]
	resp = handler.executeCommand(state, "GEORADIUS_RO", [][]byte{[]byte("gro"), []byte("-122.4194"), []byte("37.7749"), []byte("100"), []byte("km"), []byte("WITHCOORD")}, "127.0.0.1:12345")
	na := shapeNestedArray(t, resp, 1)
	entry, ok := na.Elems[0].(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 2, len(entry.Elems))
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
	assert.Equal(t, 2, len(entry.Elems))
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
	assert.Equal(t, "1-0", string(*id))
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
	assert.Equal(t, 1, len(entriesArr.Elems))
	// Each entry is [id, [fields...]]
	entry, ok := entriesArr.Elems[0].(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 2, len(entry.Elems))
	shapeBulkString(t, entry.Elems[0], 1) // id
	fields, ok := entry.Elems[1].(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 2, len(fields.Elems)) // 1 field pair
}

// TestRESPShape_XCLAIM verifies XCLAIM returns [[id, [field, value...]], ...]
func TestRESPShape_XCLAIM(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "XADD", [][]byte{[]byte("xclaimk"), []byte("1-0"), []byte("f1"), []byte("v1")}, "127.0.0.1:12345")
	handler.executeCommand(state, "XGROUP", [][]byte{[]byte("CREATE"), []byte("xclaimk"), []byte("gg"), []byte("0")}, "127.0.0.1:12345")
	handler.executeCommand(state, "XREADGROUP", [][]byte{[]byte("GROUP"), []byte("gg"), []byte("c1"), []byte("STREAMS"), []byte("xclaimk"), []byte(">")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "XCLAIM", [][]byte{[]byte("xclaimk"), []byte("gg"), []byte("c2"), []byte("0"), []byte("1-0")}, "127.0.0.1:12345")
	na := shapeNestedArray(t, resp, 1)
	entry, ok := na.Elems[0].(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 2, len(entry.Elems))
	assert.Equal(t, "1-0", shapeBulkString(t, entry.Elems[0], 1))
	fields, ok := entry.Elems[1].(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 2, len(fields.Elems))
}

// TestRESPShape_XCLAIM_JUSTID verifies XCLAIM JUSTID returns flat bulk ID array
func TestRESPShape_XCLAIM_JUSTID(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "XADD", [][]byte{[]byte("xclaimjid"), []byte("1-0"), []byte("f1"), []byte("v1")}, "127.0.0.1:12345")
	handler.executeCommand(state, "XGROUP", [][]byte{[]byte("CREATE"), []byte("xclaimjid"), []byte("gg"), []byte("0")}, "127.0.0.1:12345")
	handler.executeCommand(state, "XREADGROUP", [][]byte{[]byte("GROUP"), []byte("gg"), []byte("c1"), []byte("STREAMS"), []byte("xclaimjid"), []byte(">")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "XCLAIM", [][]byte{[]byte("xclaimjid"), []byte("gg"), []byte("c2"), []byte("0"), []byte("1-0"), []byte("JUSTID")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 1, len(arr.Args))
	assert.Equal(t, "1-0", string(arr.Args[0]))
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
	assert.Equal(t, 1, len(entriesArr.Elems))
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
	assert.Equal(t, 1, len(arr.Args))
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
	assert.Equal(t, 1, len(entriesArr.Elems))
	// Each entry: [id, [fields...]]
	entry, ok := entriesArr.Elems[0].(*proto.NestedArray)
	assert.True(t, ok)
	assert.True(t, len(entry.Elems) == 2)
	shapeBulkString(t, entry.Elems[0], 1) // id
	fieldsArr, ok := entry.Elems[1].(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 2, len(fieldsArr.Elems))
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
	assert.Equal(t, 2, len(fieldsArr.Elems))
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

// TestRESPShape_CLUSTER_SLOTS tests the CLUSTER SLOTS response shape.
// Single-node cluster is sufficient — we ADDSLOTS and verify NestedArray depth.
func TestRESPShape_CLUSTER_SLOTS(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping cluster test in short mode")
	}

	handler, state := setupTestClusterHandler(t)
	defer handler.Db.Close()

	// Add some slots to populate the slots response
	clusterCmd := cluster.NewClusterCommands(handler.Cluster)
	_, err := clusterCmd.HandleCommand([]string{"ADDSLOTS", "0", "1", "2", "3", "4"})
	assert.NoError(t, err)

	// CLUSTER SLOTS with single-node returns:
	// NestedArray [
	//   NestedArray [BulkString("0"), BulkString("4"), NestedArray [BulkString(ip), BulkString(port), BulkString(nodeID)]]
	// ]
	resp := handler.executeCommand(state, "CLUSTER", [][]byte{[]byte("SLOTS")}, "127.0.0.1:12345")

	outer := shapeNestedArray(t, resp, 1)

	// Each element is a slot range entry: [start, end, [host, port, id]]
	for i, entry := range outer.Elems {
		slotEntry, ok := entry.(*proto.NestedArray)
		if !ok {
			t.Fatalf("slot entry %d should be NestedArray (got %T)", i, entry)
		}
		if len(slotEntry.Elems) < 3 {
			t.Fatalf("slot entry %d should have >=3 elements (got %d)", i, len(slotEntry.Elems))
		}

		// First two elements are Integer (start/end slot numbers) — matches
		// real Redis CLUSTER SLOTS (integer since Redis 6, not BulkString).
		_, okStart := slotEntry.Elems[0].(*proto.Integer)
		if !okStart {
			t.Fatalf("slot entry %d element 0 (start) should be Integer (got %T)", i, slotEntry.Elems[0])
		}
		_, okEnd := slotEntry.Elems[1].(*proto.Integer)
		if !okEnd {
			t.Fatalf("slot entry %d element 1 (end) should be Integer (got %T)", i, slotEntry.Elems[1])
		}

		// Third element is NestedArray: [host, port, nodeID]
		nodeInfo, ok := slotEntry.Elems[2].(*proto.NestedArray)
		if !ok {
			t.Fatalf("slot entry %d element 2 should be NestedArray (got %T)", i, slotEntry.Elems[2])
		}
		// Node info is [ip (BulkString), port (Integer), nodeID (BulkString)]
		// — matches real Redis CLUSTER SLOTS.
		if len(nodeInfo.Elems) != 3 {
			t.Fatalf("slot entry %d node info should have 3 elements (got %d)", i, len(nodeInfo.Elems))
		}
		if _, ok := nodeInfo.Elems[0].(*proto.BulkString); !ok {
			t.Fatalf("slot entry %d node info element 0 (ip) should be BulkString (got %T)", i, nodeInfo.Elems[0])
		}
		if _, ok := nodeInfo.Elems[1].(*proto.Integer); !ok {
			t.Fatalf("slot entry %d node info element 1 (port) should be Integer (got %T)", i, nodeInfo.Elems[1])
		}
		if _, ok := nodeInfo.Elems[2].(*proto.BulkString); !ok {
			t.Fatalf("slot entry %d node info element 2 (id) should be BulkString (got %T)", i, nodeInfo.Elems[2])
		}
	}
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
	assert.Equal(t, 1, len(entriesArr.Elems))
	entry, ok := entriesArr.Elems[0].(*proto.NestedArray)
	assert.True(t, ok)
	assert.True(t, len(entry.Elems) == 2)
}

// TestRESPShape_XPENDING tests XPENDING summary [count, min, max, [[consumer, n]...]]
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
	// Element 3: consumers [[name, count], ...]
	cons, ok := na.Elems[3].(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 1, len(cons.Elems))
}

// TestRESPShape_XPENDING_Extended tests detail form [[id, consumer, idle, deliveries], ...]
func TestRESPShape_XPENDING_Extended(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "XADD", [][]byte{[]byte("xpke"), []byte("1-0"), []byte("f"), []byte("v")}, "127.0.0.1:12345")
	handler.executeCommand(state, "XGROUP", [][]byte{[]byte("CREATE"), []byte("xpke"), []byte("gg"), []byte("0")}, "127.0.0.1:12345")
	handler.executeCommand(state, "XREADGROUP", [][]byte{[]byte("GROUP"), []byte("gg"), []byte("c1"), []byte("STREAMS"), []byte("xpke"), []byte(">")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "XPENDING", [][]byte{[]byte("xpke"), []byte("gg"), []byte("-"), []byte("+"), []byte("10")}, "127.0.0.1:12345")
	na := shapeNestedArray(t, resp, 1)
	entry, ok := na.Elems[0].(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 4, len(entry.Elems))
	assert.Equal(t, "1-0", shapeBulkString(t, entry.Elems[0], 1))
	assert.Equal(t, "c1", shapeBulkString(t, entry.Elems[1], 1))
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
	assert.True(t, strings.Contains(string(*errStr), "WRONGTYPE"))
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

func TestRESPShape_LMPOP(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "RPUSH", [][]byte{[]byte("lmshp"), []byte("a"), []byte("b")}, "127.0.0.1:12345")

	// LMPOP returns NestedArray[key, [elements...]]
	resp := handler.executeCommand(state, "LMPOP", [][]byte{[]byte("1"), []byte("lmshp"), []byte("LEFT"), []byte("COUNT"), []byte("2")}, "127.0.0.1:12345")
	na := shapeNestedArray(t, resp, 2)
	shapeBulkString(t, na.Elems[0], 1) // key name
	innerArr, ok := na.Elems[1].(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 2, len(innerArr.Args))

	// Non-existent key returns NilArray
	resp = handler.executeCommand(state, "LMPOP", [][]byte{[]byte("1"), []byte("nosuchkey"), []byte("LEFT")}, "127.0.0.1:12345")
	_, ok = resp.(proto.NilArray)
	assert.True(t, ok)
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

func TestRESPShape_HELLO(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()
	resp := handler.executeCommand(state, "HELLO", nil, "127.0.0.1:12345")
	na := shapeNestedArray(t, resp, 12)
	assert.Equal(t, "server", string(*na.Elems[0].(*proto.BulkString)))
	assert.Equal(t, "boltdb", string(*na.Elems[1].(*proto.BulkString)))
	assert.Equal(t, "version", string(*na.Elems[2].(*proto.BulkString)))
	assert.Equal(t, "proto", string(*na.Elems[4].(*proto.BulkString)))
	assert.Equal(t, int64(2), int64(*na.Elems[5].(*proto.Integer)))
	assert.Equal(t, "role", string(*na.Elems[10].(*proto.BulkString)))
	modules, ok := na.Elems[13].(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 0, len(modules.Elems))

	// HELLO 3 should return Map (RESP3 handshake)
	handler2, state2 := setupTestHandler(t)
	defer handler2.Db.Close()
	resp = handler2.executeCommand(state2, "HELLO", [][]byte{[]byte("3")}, "127.0.0.1:12345")
	m, ok := resp.(*proto.Map)
	assert.True(t, ok)
	assert.Equal(t, 14, len(m.Elems)) // same field count as NestedArray
	assert.Equal(t, "server", string(*m.Elems[0].(*proto.BulkString)))
	assert.Equal(t, "proto", string(*m.Elems[4].(*proto.BulkString)))
	assert.Equal(t, int64(3), int64(*m.Elems[5].(*proto.Integer)))

	// State should be updated to RESP3
	assert.Equal(t, 3, state2.respVersion)

	// HELLO with invalid proto version
	resp = handler.executeCommand(state, "HELLO", [][]byte{[]byte("1")}, "127.0.0.1:12345")
	_, ok = resp.(*proto.Error)
	assert.True(t, ok)
}

// setupRESP3 helper: creates handler + state, executes HELLO 3, returns RESP3-mode state
func setupRESP3(t *testing.T) (*Handler, *connState) {
	t.Helper()
	h, s := setupTestHandler(t)
	resp := h.executeCommand(s, "HELLO", [][]byte{[]byte("3")}, "127.0.0.1:12345")
	m, ok := resp.(*proto.Map)
	assert.True(t, ok)
	assert.Equal(t, 14, len(m.Elems))
	assert.Equal(t, 3, s.respVersion)
	return h, s
}

func shapeNull(t *testing.T, resp proto.RESP) {
	t.Helper()
	_, ok := resp.(*proto.Null)
	assert.True(t, ok)
}

func TestRESP3Shape_GetMissingKeyReturnsNull(t *testing.T) {
	t.Parallel()
	handler, state := setupRESP3(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "GET", [][]byte{[]byte("nonexistent")}, "127.0.0.1:12345")
	shapeNull(t, resp)
}

func TestRESP3Shape_GetExistingReturnsBulkString(t *testing.T) {
	t.Parallel()
	handler, state := setupRESP3(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("rk1"), []byte("v1")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "GET", [][]byte{[]byte("rk1")}, "127.0.0.1:12345")
	shapeBulkString(t, resp, 1)
}

func TestRESP3Shape_ZScoreMissingReturnsNull(t *testing.T) {
	t.Parallel()
	handler, state := setupRESP3(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zs2"), []byte("1.0"), []byte("m1")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "ZSCORE", [][]byte{[]byte("zs2"), []byte("nonexistent")}, "127.0.0.1:12345")
	shapeNull(t, resp)
}

func TestRESP3Shape_GetDelMissingReturnsNull(t *testing.T) {
	t.Parallel()
	handler, state := setupRESP3(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "GETDEL", [][]byte{[]byte("nonexistent")}, "127.0.0.1:12345")
	shapeNull(t, resp)
}

func TestRESP3Shape_GetExMissingReturnsNull(t *testing.T) {
	t.Parallel()
	handler, state := setupRESP3(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "GETEX", [][]byte{[]byte("nonexistent")}, "127.0.0.1:12345")
	shapeNull(t, resp)
}

func TestRESP3Shape_HELLO2BackToRESP2(t *testing.T) {
	t.Parallel()
	handler, state := setupRESP3(t)
	defer handler.Db.Close()

	// Switching back to RESP2
	resp := handler.executeCommand(state, "HELLO", [][]byte{[]byte("2")}, "127.0.0.1:12345")
	na, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.True(t, len(na.Elems) >= 12)
	assert.Equal(t, 2, state.respVersion)

	// After switching back, EXISTS should return Integer (not Boolean)
	resp = handler.executeCommand(state, "EXISTS", [][]byte{[]byte("nonexistent")}, "127.0.0.1:12345")
	shapeInteger(t, resp)
}

func shapeEmptyArray(t *testing.T, resp proto.RESP) {
	t.Helper()
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 0, len(arr.Args))
}

func shapeArrayWithNils(t *testing.T, resp proto.RESP, count int) {
	t.Helper()
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, count, len(arr.Args))
}

func TestRESP3Shape_HGetMissingReturnsNull(t *testing.T) {
	t.Parallel()
	handler, state := setupRESP3(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "HGET", [][]byte{[]byte("nosuchkey"), []byte("f")}, "127.0.0.1:12345")
	shapeNull(t, resp)
}

func TestRESP3Shape_LIndexMissingReturnsNull(t *testing.T) {
	t.Parallel()
	handler, state := setupRESP3(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "LINDEX", [][]byte{[]byte("nosuchkey"), []byte("0")}, "127.0.0.1:12345")
	shapeNull(t, resp)

	handler.executeCommand(state, "LPUSH", [][]byte{[]byte("lk"), []byte("a")}, "127.0.0.1:12345")
	resp = handler.executeCommand(state, "LINDEX", [][]byte{[]byte("lk"), []byte("5")}, "127.0.0.1:12345")
	shapeNull(t, resp)
}

func TestRESP3Shape_ZRankMissingReturnsNull(t *testing.T) {
	t.Parallel()
	handler, state := setupRESP3(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZRANK", [][]byte{[]byte("nosuchkey"), []byte("m")}, "127.0.0.1:12345")
	shapeNull(t, resp)
}

func TestRESP3Shape_ZRevRankMissingReturnsNull(t *testing.T) {
	t.Parallel()
	handler, state := setupRESP3(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZREVRANK", [][]byte{[]byte("nosuchkey"), []byte("m")}, "127.0.0.1:12345")
	shapeNull(t, resp)
}

func TestRESP3Shape_LPopMissingReturnsNull(t *testing.T) {
	t.Parallel()
	handler, state := setupRESP3(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "LPOP", [][]byte{[]byte("nosuchkey")}, "127.0.0.1:12345")
	shapeNull(t, resp)
}

func TestRESP3Shape_RPopMissingReturnsNull(t *testing.T) {
	t.Parallel()
	handler, state := setupRESP3(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "RPOP", [][]byte{[]byte("nosuchkey")}, "127.0.0.1:12345")
	shapeNull(t, resp)
}

func TestRESP3Shape_SRandMemberMissingReturnsNull(t *testing.T) {
	t.Parallel()
	handler, state := setupRESP3(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "SRANDMEMBER", [][]byte{[]byte("nosuchkey")}, "127.0.0.1:12345")
	shapeNull(t, resp)
}

func TestSPopNEmptyReturnsEmptyArray(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "SPOP", [][]byte{[]byte("nosuchkey"), []byte("2")}, "127.0.0.1:12345")
	shapeEmptyArray(t, resp)
}

func TestSPopNEmptyReturnsEmptyArray_RESP3(t *testing.T) {
	t.Parallel()
	handler, state := setupRESP3(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "SPOP", [][]byte{[]byte("nosuchkey"), []byte("2")}, "127.0.0.1:12345")
	shapeEmptyArray(t, resp)
}

func TestSPopSingleMissingReturnsNull_RESP3(t *testing.T) {
	t.Parallel()
	handler, state := setupRESP3(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "SPOP", [][]byte{[]byte("nosuchkey")}, "127.0.0.1:12345")
	shapeNull(t, resp)
}

func TestSPopSingleMissingReturnsNil_RESP2(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "SPOP", [][]byte{[]byte("nosuchkey")}, "127.0.0.1:12345")
	shapeIsNilBulk(t, resp)
}

func TestRESP3Shape_LPosMissingReturnsNull(t *testing.T) {
	t.Parallel()
	handler, state := setupRESP3(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "LPOS", [][]byte{[]byte("nosuchkey"), []byte("x")}, "127.0.0.1:12345")
	shapeNull(t, resp)
}

func TestRESP3Shape_RandomKeyMissingReturnsNull(t *testing.T) {
	t.Parallel()
	handler, state := setupRESP3(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "RANDOMKEY", [][]byte{}, "127.0.0.1:12345")
	shapeNull(t, resp)
}

func TestRESP3Shape_DumpMissingReturnsNull(t *testing.T) {
	t.Parallel()
	handler, state := setupRESP3(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "DUMP", [][]byte{[]byte("nosuchkey")}, "127.0.0.1:12345")
	shapeNull(t, resp)
}

func TestRESP3Shape_MemoryUsageMissingReturnsNull(t *testing.T) {
	t.Parallel()
	handler, state := setupRESP3(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "MEMORY", [][]byte{[]byte("USAGE"), []byte("nosuchkey")}, "127.0.0.1:12345")
	shapeNull(t, resp)
}

func TestRESP3Shape_BLMoveMissingReturnsNull(t *testing.T) {
	t.Parallel()
	handler, state := setupRESP3(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "BLMOVE", [][]byte{[]byte("nosuchkey"), []byte("dst"), []byte("LEFT"), []byte("LEFT"), []byte("0")}, "127.0.0.1:12345")
	shapeNull(t, resp)
}

func TestRESP3Shape_RPopLPushMissingReturnsNull(t *testing.T) {
	t.Parallel()
	handler, state := setupRESP3(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "RPOPLPUSH", [][]byte{[]byte("nosuchkey"), []byte("dst")}, "127.0.0.1:12345")
	shapeNull(t, resp)
}

func TestRESP3Shape_LMoveMissingReturnsNull(t *testing.T) {
	t.Parallel()
	handler, state := setupRESP3(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "LMOVE", [][]byte{[]byte("nosuchkey"), []byte("dst"), []byte("LEFT"), []byte("LEFT")}, "127.0.0.1:12345")
	shapeNull(t, resp)
}

func TestRESP3Shape_BRPopLPushMissingReturnsNull(t *testing.T) {
	t.Parallel()
	handler, state := setupRESP3(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "BRPOPLPUSH", [][]byte{[]byte("nosuchkey"), []byte("dst"), []byte("0")}, "127.0.0.1:12345")
	shapeNull(t, resp)
}

func TestRESP3Shape_ObjectRefCountMissingReturnsNull(t *testing.T) {
	t.Parallel()
	handler, state := setupRESP3(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "OBJECT", [][]byte{[]byte("REFCOUNT"), []byte("nosuchkey")}, "127.0.0.1:12345")
	shapeNull(t, resp)
}

func TestRESP3Shape_ObjectEncodingMissingReturnsNull(t *testing.T) {
	t.Parallel()
	handler, state := setupRESP3(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "OBJECT", [][]byte{[]byte("ENCODING"), []byte("nosuchkey")}, "127.0.0.1:12345")
	shapeNull(t, resp)
}

func TestSPopNInTransactionReturnsEmptyArray(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "MULTI", [][]byte{}, "127.0.0.1:12345")
	handler.executeCommand(state, "SPOP", [][]byte{[]byte("nosuchkey"), []byte("2")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "EXEC", [][]byte{}, "127.0.0.1:12345")
	na, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 1, len(na.Elems))
	arr, ok := na.Elems[0].(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 0, len(arr.Args))
}

func TestSPopSingleInTransactionMissingReturnsNil(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "MULTI", [][]byte{}, "127.0.0.1:12345")
	handler.executeCommand(state, "SPOP", [][]byte{[]byte("nosuchkey")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "EXEC", [][]byte{}, "127.0.0.1:12345")
	na, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 1, len(na.Elems))
	shapeIsNilBulk(t, na.Elems[0])
}

func TestSPopSingleInTransactionMissingReturnsNull_RESP3(t *testing.T) {
	t.Parallel()
	handler, state := setupRESP3(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "MULTI", [][]byte{}, "127.0.0.1:12345")
	handler.executeCommand(state, "SPOP", [][]byte{[]byte("nosuchkey")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "EXEC", [][]byte{}, "127.0.0.1:12345")
	na, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 1, len(na.Elems))
	shapeNull(t, na.Elems[0])
}

func TestRESPShape_CLUSTER_COUNTKEYSINSLOT(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping cluster test in short mode")
	}

	handler, state := setupTestClusterHandler(t)
	defer handler.Db.Close()

	// Insert keys and compute their CRC16 slot
	keys := []string{"cnt:key1", "cnt:key2", "cnt:key3", "other:key"}
	for _, k := range keys {
		handler.executeCommand(state, "SET", [][]byte{[]byte(k), []byte("v")}, "127.0.0.1:12345")
	}

	// Verify COUNTKEYSINSLOT returns a non-negative integer for any valid slot
	for _, k := range keys {
		slot := cluster.Slot(k)
		resp := handler.executeCommand(state, "CLUSTER", [][]byte{[]byte("COUNTKEYSINSLOT"), []byte(fmt.Sprintf("%d", slot))}, "127.0.0.1:12345")
		count := shapeInteger(t, resp)
		if count < 0 {
			t.Fatalf("COUNTKEYSINSLOT for slot %d (key %s) returned %d", slot, k, count)
		}
	}

	// COUNTKEYSINSLOT with slot that has data should return >=1
	slot0 := cluster.Slot(keys[0])
	resp := handler.executeCommand(state, "CLUSTER", [][]byte{[]byte("COUNTKEYSINSLOT"), []byte(fmt.Sprintf("%d", slot0))}, "127.0.0.1:12345")
	count := shapeInteger(t, resp)
	if count < 1 {
		t.Fatalf("expected >=1 keys in slot %d, got %d", slot0, count)
	}

	// COUNTKEYSINSLOT with invalid slot number should return error
	errResp := handler.executeCommand(state, "CLUSTER", [][]byte{[]byte("COUNTKEYSINSLOT"), []byte("99999")}, "127.0.0.1:12345")
	_, ok := errResp.(*proto.Error)
	if !ok {
		t.Fatalf("COUNTKEYSINSLOT with invalid slot should return Error, got %T", errResp)
	}
}

func TestRESPShape_ASKING(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// ASKING returns SimpleString("OK")
	resp := handler.executeCommand(state, "ASKING", nil, "127.0.0.1:12345")
	ss, ok := resp.(*proto.SimpleString)
	assert.True(t, ok)
	assert.Equal(t, "OK", string(*ss))

	assert.True(t, state.clusterAsking)
}

// TestRESPShape_ExpireTime verifies EXPIRETIME returns Integer
func TestRESPShape_ExpireTime(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Key with no expiry → -1
	resp := handler.executeCommand(state, "EXPIRETIME", [][]byte{[]byte("noexp")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(-2), int64(*integer)) // key doesn't exist

	handler.executeCommand(state, "SET", [][]byte{[]byte("etkey"), []byte("v")}, "127.0.0.1:12345")
	resp = handler.executeCommand(state, "EXPIRETIME", [][]byte{[]byte("etkey")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(-1), int64(*integer)) // exists, no TTL

	handler.executeCommand(state, "EXPIRE", [][]byte{[]byte("etkey"), []byte("10000")}, "127.0.0.1:12345")
	resp = handler.executeCommand(state, "EXPIRETIME", [][]byte{[]byte("etkey")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*integer) > time.Now().Unix()-1) // positive timestamp
}

// TestRESPShape_PExpireTime verifies PEXPIRETIME returns Integer (milliseconds)
func TestRESPShape_PExpireTime(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("petkey"), []byte("v")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "PEXPIRETIME", [][]byte{[]byte("petkey")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(-1), int64(*integer)) // exists, no TTL

	handler.executeCommand(state, "PEXPIRE", [][]byte{[]byte("petkey"), []byte("10000000")}, "127.0.0.1:12345")
	resp = handler.executeCommand(state, "PEXPIRETIME", [][]byte{[]byte("petkey")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*integer) > time.Now().UnixMilli()-1) // positive ms timestamp
}

// TestRESPShape_ACL_Whoami verifies ACL WHOAMI returns BulkString
func TestRESPShape_ACL_Whoami(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ACL", [][]byte{[]byte("WHOAMI")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "default", string(*bs))
}

// TestRESPShape_ACL_List verifies ACL LIST returns Array of rules
func TestRESPShape_ACL_List(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ACL", [][]byte{[]byte("LIST")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.True(t, len(arr.Args) >= 1)
}

// TestRESPShape_ACL_Cat verifies ACL CAT returns Array of categories
func TestRESPShape_ACL_Cat(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ACL", [][]byte{[]byte("CAT")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.True(t, len(arr.Args) >= 4)
}

// TestRESPShape_LCS verifies LCS returns BulkString
func TestRESPShape_LCS(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("lcs1"), []byte("hello world")}, "127.0.0.1:12345")
	handler.executeCommand(state, "SET", [][]byte{[]byte("lcs2"), []byte("hello there")}, "127.0.0.1:12345")

	// Without modifiers — BulkString
	resp := handler.executeCommand(state, "LCS", [][]byte{[]byte("lcs1"), []byte("lcs2")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "hello r", string(*bs)) // LCS of "hello world" and "hello there"

	// With LEN — Integer
	resp = handler.executeCommand(state, "LCS", [][]byte{[]byte("lcs1"), []byte("lcs2"), []byte("LEN")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(7), int64(*integer)) // "hello r" = 7 chars
}

// TestRESPShape_ZINTER verifies ZINTER returns Array
func TestRESPShape_ZINTER(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zint1"), []byte("1"), []byte("a"), []byte("2"), []byte("b")}, "127.0.0.1:12345")
	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zint2"), []byte("1"), []byte("a"), []byte("3"), []byte("c")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZINTER", [][]byte{[]byte("2"), []byte("zint1"), []byte("zint2")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 1, len(arr.Args))
	// WITHSCORES returns alternating member/score
	resp = handler.executeCommand(state, "ZINTER", [][]byte{[]byte("2"), []byte("zint1"), []byte("zint2"), []byte("WITHSCORES")}, "127.0.0.1:12345")
	arr, ok = resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr.Args)) // 1 member + 1 score
}

// TestRESPShape_ZUNION verifies ZUNION returns Array
func TestRESPShape_ZUNION(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zun1"), []byte("1"), []byte("a"), []byte("2"), []byte("b")}, "127.0.0.1:12345")
	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zun2"), []byte("1"), []byte("a"), []byte("3"), []byte("c")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZUNION", [][]byte{[]byte("2"), []byte("zun1"), []byte("zun2")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 3, len(arr.Args)) // a, b, c = 3 members
}

func setupTestClusterHandler(t *testing.T) (*Handler, *connState) {
	t.Helper()
	dbPath := t.TempDir()
	db, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)

	c, err := cluster.NewCluster(db, "", "127.0.0.1:6337", context.Background())
	assert.NoError(t, err)

	handler := &Handler{
		Db:      db,
		Cluster: c,
		conns:   make(map[*connState]*connMeta),
	}
	state := &connState{}

	return handler, state
}
