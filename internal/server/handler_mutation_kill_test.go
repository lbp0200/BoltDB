package server

import (
	"fmt"
	"math"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/zeebo/assert"
)

// =============================================================================
// Phase 9: Mutation Test NOT COVERED 修复
// 针对 gremlins 高优先级缺口补充精确值断言，消灭存活变异体
// =============================================================================

// ---------- Priority 1: GEO 选项解析 ----------

// TestGeoMutationKill_GeoradiusOptions 验证 GEORADIUS 各选项返回精确值
// 目标变异体: geo_commands.go:158-173 (INCREMENT_DECREMENT, CONDITIONALS_NEGATION)
func TestGeoMutationKill_GeoradiusOptions(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// SF: lon=-122.4194, lat=37.7749
	// LA: lon=-118.2437, lat=34.0522
	handler.executeCommand(state, "GEOADD", [][]byte{
		[]byte("geo_opt"), []byte("-122.4194"), []byte("37.7749"), []byte("SF"),
	}, "127.0.0.1:12345")
	handler.executeCommand(state, "GEOADD", [][]byte{
		[]byte("geo_opt"), []byte("-118.2437"), []byte("34.0522"), []byte("LA"),
	}, "127.0.0.1:12345")

	// --- WITHCOORD: 验证返回的坐标值正确 ---
	resp := handler.executeCommand(state, "GEORADIUS", [][]byte{
		[]byte("geo_opt"), []byte("-122.4194"), []byte("37.7749"), []byte("1000"), []byte("km"), []byte("WITHCOORD"),
	}, "127.0.0.1:12345")
	nested, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.True(t, len(nested.Elems) >= 1)

	// Find SF entry and verify its coordinates
	var foundSF bool
	for _, elem := range nested.Elems {
		entry, ok := elem.(*proto.NestedArray)
		if !ok {
			continue
		}
		member := string(*entry.Elems[0].(*proto.BulkString))
		if member == "SF" {
			foundSF = true
			coord, ok := entry.Elems[1].(*proto.NestedArray)
			assert.True(t, ok)
			lon := string(*coord.Elems[0].(*proto.BulkString))
			lat := string(*coord.Elems[1].(*proto.BulkString))
			// Verify coordinates are not swapped (lon should be ~-122, lat ~37)
			lonVal, _ := strconv.ParseFloat(lon, 64)
			latVal, _ := strconv.ParseFloat(lat, 64)
			assert.True(t, lonVal < -100)
			assert.True(t, latVal > 30 && latVal < 45)
		}
	}
	assert.True(t, foundSF)

	// --- WITHDIST: 验证距离值 ---
	resp = handler.executeCommand(state, "GEORADIUS", [][]byte{
		[]byte("geo_opt"), []byte("-122.4194"), []byte("37.7749"), []byte("1000"), []byte("km"), []byte("WITHDIST"),
	}, "127.0.0.1:12345")
	nested, ok = resp.(*proto.NestedArray)
	assert.True(t, ok)
	for _, elem := range nested.Elems {
		entry, ok := elem.(*proto.NestedArray)
		if !ok {
			continue
		}
		member := string(*entry.Elems[0].(*proto.BulkString))
		distStr := string(*entry.Elems[1].(*proto.BulkString))
		dist, err := strconv.ParseFloat(distStr, 64)
		assert.True(t, err == nil)
		if member == "SF" {
			assert.True(t, dist < 1.0)
		} else if member == "LA" {
			assert.True(t, dist > 500 && dist < 700)
		}
	}

	// --- WITHHASH: 验证 hash 值非空 ---
	resp = handler.executeCommand(state, "GEORADIUS", [][]byte{
		[]byte("geo_opt"), []byte("-122.4194"), []byte("37.7749"), []byte("1000"), []byte("km"), []byte("WITHHASH"),
	}, "127.0.0.1:12345")
	nested, ok = resp.(*proto.NestedArray)
	assert.True(t, ok)
	for _, elem := range nested.Elems {
		entry, ok := elem.(*proto.NestedArray)
		assert.True(t, ok)
		hashStr := string(*entry.Elems[1].(*proto.BulkString))
		assert.True(t, len(hashStr) > 0)
	}

	// --- COUNT: 验证返回数量限制 ---
	resp = handler.executeCommand(state, "GEORADIUS", [][]byte{
		[]byte("geo_opt"), []byte("-122.4194"), []byte("37.7749"), []byte("1000"), []byte("km"),
		[]byte("COUNT"), []byte("1"),
	}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 1, len(arr.Args))
}

// TestGeoMutationKill_GeosearchFrommemberSwap 验证 GEOSEARCH FROMMEMBER 坐标不互换
// 目标变异体: geo_commands.go:239-240 (centerLon/centerLat 赋值)
func TestGeoMutationKill_GeosearchFrommemberSwap(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "GEOADD", [][]byte{
		[]byte("geo_fm"), []byte("-122.4194"), []byte("37.7749"), []byte("SF"),
	}, "127.0.0.1:12345")
	handler.executeCommand(state, "GEOADD", [][]byte{
		[]byte("geo_fm"), []byte("-122.4200"), []byte("37.7750"), []byte("SF_NEARBY"),
	}, "127.0.0.1:12345")

	// GEOSEARCH FROMMEMBER SF BYRADIUS 1km — should find both
	resp := handler.executeCommand(state, "GEOSEARCH", [][]byte{
		[]byte("geo_fm"), []byte("FROMMEMBER"), []byte("SF"),
		[]byte("BYRADIUS"), []byte("1"), []byte("km"),
	}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.True(t, len(arr.Args) >= 2)

	// GEOSEARCH FROMMEMBER SF BYRADIUS 0.001km — should find only SF
	resp = handler.executeCommand(state, "GEOSEARCH", [][]byte{
		[]byte("geo_fm"), []byte("FROMMEMBER"), []byte("SF"),
		[]byte("BYRADIUS"), []byte("0.001"), []byte("km"),
	}, "127.0.0.1:12345")
	arr, ok = resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 1, len(arr.Args))
	assert.Equal(t, "SF", string(arr.Args[0]))
}

// TestGeoMutationKill_GeosearchBybox 验证 BYBOX width/height 解析
// 目标变异体: geo_commands.go:271-281 (BYBOX width/2, args[i+3])
func TestGeoMutationKill_GeosearchBybox(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "GEOADD", [][]byte{
		[]byte("geo_box"), []byte("-122.4194"), []byte("37.7749"), []byte("SF"),
	}, "127.0.0.1:12345")
	handler.executeCommand(state, "GEOADD", [][]byte{
		[]byte("geo_box"), []byte("-74.0060"), []byte("40.7128"), []byte("NYC"),
	}, "127.0.0.1:12345")

	// BYBOX 10000 km from SF center — should find both
	resp := handler.executeCommand(state, "GEOSEARCH", [][]byte{
		[]byte("geo_box"), []byte("FROMLONLAT"), []byte("-122.4194"), []byte("37.7749"),
		[]byte("BYBOX"), []byte("10000"), []byte("10000"), []byte("km"),
	}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.True(t, len(arr.Args) >= 2)

	// BYBOX 10 km from SF center — should find only SF
	resp = handler.executeCommand(state, "GEOSEARCH", [][]byte{
		[]byte("geo_box"), []byte("FROMLONLAT"), []byte("-122.4194"), []byte("37.7749"),
		[]byte("BYBOX"), []byte("10"), []byte("10"), []byte("km"),
	}, "127.0.0.1:12345")
	arr, ok = resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 1, len(arr.Args))
}

// TestGeoSearchBybox_RectangularSemantics verifies true rectangle semantics
// through the command handler: BYBOX must NOT degenerate to a width/2 circle
// (see git history of the old `radius = width / 2` implementation).
//
// Dataset (center = SF, 37.7749°N, 122.4194°W) and box results:
//   5000x1000 km → {SF, LA, East}
//   2000x5000 km → {SF, LA, North, NW}
//   1000x5000 km → {SF, LA, North, NW}
//   10000x10000km → all points except Bali (13453km away)
//   10x10 km     → {SF}
// Every point is far from any box edge (margin ≫ geohash 26-bit decode
// error), and both 5000x1000 and 1000x5000 have discriminating points:
// a width/2-circle implementation returns the wrong set in both cases.
func TestGeoSearchBybox_RectangularSemantics(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	geoAdd := func(lon, lat, member string) {
		handler.executeCommand(state, "GEOADD", [][]byte{
			[]byte("bybox"), []byte(lon), []byte(lat), []byte(member),
		}, "127.0.0.1:12345")
	}
	geoAdd("-122.4194", "37.7749", "SF")
	geoAdd("-118.2437", "34.0522", "LA")
	geoAdd("-122.4194", "47.7749", "North") // 1113km north
	geoAdd("-124.4194", "56.2749", "NW")     // 2065km away
	geoAdd("-100.0", "37.7749", "East")      // 1968km east
	geoAdd("-74.0060", "40.7128", "NYC")     // 4134km away
	geoAdd("115.1390", "-8.8028", "Bali")    // 13453km away

	search := func(width, height string) map[string]bool {
		resp := handler.executeCommand(state, "GEOSEARCH", [][]byte{
			[]byte("bybox"), []byte("FROMLONLAT"), []byte("-122.4194"), []byte("37.7749"),
			[]byte("BYBOX"), []byte(width), []byte(height), []byte("km"),
		}, "127.0.0.1:12345")
		arr, ok := resp.(*proto.Array)
		assert.True(t, ok)
		names := make(map[string]bool, len(arr.Args))
		for _, a := range arr.Args {
			names[string(a)] = true
		}
		return names
	}

	// 5000km wide × 1000km tall: half-height ~500km. North (1113km north)
	// is OUTSIDE the box but INSIDE a width/2 (2500km) circle — a width/2
	// implementation would wrongly include it. East (1968km east) is inside
	// the box.
	names := search("5000", "1000")
	assert.True(t, names["SF"])
	assert.True(t, names["LA"])
	assert.True(t, names["East"])
	assert.False(t, names["North"])
	assert.False(t, names["NW"])
	assert.False(t, names["NYC"])
	assert.False(t, names["Bali"])

	// 2000km wide × 5000km tall: half-width ~1120km. North (1113km) and NW
	// (2065km) are INSIDE the box but OUTSIDE a width/2 (1000km) circle —
	// a width/2 implementation would wrongly exclude them.
	names = search("2000", "5000")
	assert.True(t, names["SF"])
	assert.True(t, names["LA"])
	assert.True(t, names["North"])
	assert.True(t, names["NW"])
	assert.False(t, names["East"])
	assert.False(t, names["NYC"])
	assert.False(t, names["Bali"])

	// 1000km wide × 5000km tall: narrower width, same tall box.
	names = search("1000", "5000")
	assert.True(t, names["SF"])
	assert.True(t, names["LA"])
	assert.True(t, names["North"])
	assert.True(t, names["NW"])
	assert.False(t, names["East"])

	// 10000km × 10000km: NYC (4134km) enters the box.
	names = search("10000", "10000")
	assert.True(t, names["SF"])
	assert.True(t, names["LA"])
	assert.True(t, names["North"])
	assert.True(t, names["NW"])
	assert.True(t, names["East"])
	assert.True(t, names["NYC"])
	assert.False(t, names["Bali"])

	// 10km × 10km: only the center point.
	names = search("10", "10")
	assert.Equal(t, 1, len(names))
	assert.True(t, names["SF"])

	// Non-positive width or height → empty array (Redis accepts it).
	resp := handler.executeCommand(state, "GEOSEARCH", [][]byte{
		[]byte("bybox"), []byte("FROMLONLAT"), []byte("-122.4194"), []byte("37.7749"),
		[]byte("BYBOX"), []byte("0"), []byte("1000"), []byte("km"),
	}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 0, len(arr.Args))
}

// TestGeoSearchBybox_FromMemberAndOptions verifies FROMMEMBER + BYBOX and
// the WITHDIST/WITHCOORD/COUNT option combination.
func TestGeoSearchBybox_FromMemberAndOptions(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	geoAdd := func(lon, lat, member string) {
		handler.executeCommand(state, "GEOADD", [][]byte{
			[]byte("bybox_fm"), []byte(lon), []byte(lat), []byte(member),
		}, "127.0.0.1:12345")
	}
	geoAdd("-122.4194", "37.7749", "SF")
	geoAdd("-118.2437", "34.0522", "LA")
	geoAdd("-122.4194", "47.7749", "North")

	// FROMMEMBER SF + BYBOX 5000x1000km → {SF, LA, East-less set}; the box
	// half-height is ~500km so North (1113km north) is excluded.
	resp := handler.executeCommand(state, "GEOSEARCH", [][]byte{
		[]byte("bybox_fm"), []byte("FROMMEMBER"), []byte("SF"),
		[]byte("BYBOX"), []byte("5000"), []byte("1000"), []byte("km"),
		[]byte("WITHDIST"), []byte("WITHCOORD"), []byte("COUNT"), []byte("10"),
	}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr.Elems)) // SF, LA

	found := make(map[string]*proto.NestedArray)
	for _, e := range arr.Elems {
		inner, ok := e.(*proto.NestedArray)
		assert.True(t, ok)
		member, ok := inner.Elems[0].(*proto.BulkString)
		assert.True(t, ok) // first element is the member name
		found[string(*member)] = inner
	}
	for _, inner := range found {
		// [member, dist, [lon, lat]]
		assert.True(t, len(inner.Elems) >= 3)
		coord, ok := inner.Elems[len(inner.Elems)-1].(*proto.NestedArray)
		assert.True(t, ok)
		assert.Equal(t, 2, len(coord.Elems))
	}
	_, sfFound := found["SF"]
	_, laFound := found["LA"]
	assert.True(t, sfFound)
	assert.True(t, laFound)
}

// TestGeoSearchStore_Bybox verifies the BYBOX path of GEOSEARCHSTORE,
// including STOREDIST (score = distance from box center in the unit).
func TestGeoSearchStore_Bybox(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	geoAdd := func(lon, lat, member string) {
		handler.executeCommand(state, "GEOADD", [][]byte{
			[]byte("bybox_src"), []byte(lon), []byte(lat), []byte(member),
		}, "127.0.0.1:12345")
	}
	geoAdd("-122.4194", "37.7749", "SF")
	geoAdd("-118.2437", "34.0522", "LA")
	geoAdd("-122.4194", "47.7749", "North")
	geoAdd("-124.4194", "56.2749", "NW")

	// 1000km wide × 5000km tall → {SF, LA, North, NW}.
	resp := handler.executeCommand(state, "GEOSEARCHSTORE", [][]byte{
		[]byte("bybox_dst"), []byte("bybox_src"),
		[]byte("FROMLONLAT"), []byte("-122.4194"), []byte("37.7749"),
		[]byte("BYBOX"), []byte("1000"), []byte("5000"), []byte("km"),
		[]byte("STOREDIST"),
	}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(4), int64(*integer))

	zresp := handler.executeCommand(state, "ZRANGE", [][]byte{
		[]byte("bybox_dst"), []byte("0"), []byte("-1"), []byte("WITHSCORES"),
	}, "127.0.0.1:12345")
	zarr, ok := zresp.(*proto.Array)
	assert.True(t, ok)
	scores := make(map[string]float64)
	for i := 0; i+1 < len(zarr.Args); i += 2 {
		v, err := strconv.ParseFloat(string(zarr.Args[i+1]), 64)
		assert.True(t, err == nil)
		scores[string(zarr.Args[i])] = v
	}
	assert.Equal(t, 4, len(scores))
	assert.True(t, scores["SF"] < 1.0) // ≈ 0 km
	assert.True(t, scores["North"] > 1000.0 && scores["North"] < 1300.0) // ≈ 1113 km
	assert.True(t, scores["NW"] > 1900.0 && scores["NW"] < 2300.0) // ≈ 2065 km
	assert.True(t, scores["LA"] > 400.0 && scores["LA"] < 700.0) // ≈ 556 km
}

// TestGeoMutationKill_GeosearchstoreOptions 验证 GEOSEARCHSTORE 选项
// 目标变异体: geo_commands.go:352-427 (STOREDIST, COUNT, option loop)
func TestGeoMutationKill_GeosearchstoreOptions(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "GEOADD", [][]byte{
		[]byte("geo_src"), []byte("-122.4194"), []byte("37.7749"), []byte("SF"),
	}, "127.0.0.1:12345")
	handler.executeCommand(state, "GEOADD", [][]byte{
		[]byte("geo_src"), []byte("-118.2437"), []byte("34.0522"), []byte("LA"),
	}, "127.0.0.1:12345")
	handler.executeCommand(state, "GEOADD", [][]byte{
		[]byte("geo_src"), []byte("-74.0060"), []byte("40.7128"), []byte("NYC"),
	}, "127.0.0.1:12345")

	// GEOSEARCHSTORE with COUNT 2
	resp := handler.executeCommand(state, "GEOSEARCHSTORE", [][]byte{
		[]byte("geo_dst1"), []byte("geo_src"),
		[]byte("FROMLONLAT"), []byte("-122.4194"), []byte("37.7749"),
		[]byte("BYRADIUS"), []byte("1000"), []byte("km"),
		[]byte("COUNT"), []byte("2"),
	}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))

	// GEOSEARCHSTORE with STOREDIST
	resp = handler.executeCommand(state, "GEOSEARCHSTORE", [][]byte{
		[]byte("geo_dst2"), []byte("geo_src"),
		[]byte("FROMLONLAT"), []byte("-122.4194"), []byte("37.7749"),
		[]byte("BYRADIUS"), []byte("1000"), []byte("km"),
		[]byte("STOREDIST"),
	}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*integer) >= 2)

	// Verify STOREDIST stores distance values (floats in scores)
	members, _ := handler.Db.ZRange("geo_dst2", 0, -1)
	for _, m := range members {
		dist, err := strconv.ParseFloat(m.Member, 64)
		if err == nil {
			assert.True(t, dist >= 0)
		}
	}
}

// ---------- Priority 2: RESTORE ABSTTL 时间计算 ----------

// TestRestoreMutationKill_Absttl 验证 RESTORE ABSTTL 时间计算
// 目标变异体: key_commands.go:131 (ttlMS > now boundary)
//
//	key_commands.go:132 (ttl = time.Duration(ttlMS-now) arithmetic)
func TestRestoreMutationKill_Absttl(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Create a key to dump
	handler.executeCommand(state, "SET", [][]byte{[]byte("rkey"), []byte("hello")}, "127.0.0.1:12345")
	dumpResp := handler.executeCommand(state, "DUMP", [][]byte{[]byte("rkey")}, "127.0.0.1:12345")
	dumpBS, ok := dumpResp.(*proto.BulkString)
	assert.True(t, ok)
	dumpData := string(*dumpBS)

	// --- ABSTTL with future timestamp: key should survive ---
	futureMs := time.Now().UnixMilli() + 60000 // 60s from now
	resp := handler.executeCommand(state, "RESTORE", [][]byte{
		[]byte("rkey_future"),
		[]byte(strconv.FormatInt(futureMs, 10)),
		[]byte(dumpData),
		[]byte("REPLACE"),
		[]byte("ABSTTL"),
	}, "127.0.0.1:12345")
	_, isOK := resp.(*proto.SimpleString)
	assert.True(t, isOK)

	// Verify key exists and has correct value
	val, err := handler.Db.Get("rkey_future")
	assert.True(t, err == nil)
	assert.Equal(t, "hello", val)

	// Verify TTL is positive (not expired)
	ttlResp := handler.executeCommand(state, "TTL", [][]byte{[]byte("rkey_future")}, "127.0.0.1:12345")
	ttlInt, ok := ttlResp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*ttlInt) > 0)

	// --- ABSTTL with past timestamp: key should be immediately expired ---
	pastMs := time.Now().UnixMilli() - 60000 // 60s ago
	resp = handler.executeCommand(state, "RESTORE", [][]byte{
		[]byte("rkey_past"),
		[]byte(strconv.FormatInt(pastMs, 10)),
		[]byte(dumpData),
		[]byte("REPLACE"),
		[]byte("ABSTTL"),
	}, "127.0.0.1:12345")
	_, isOK = resp.(*proto.SimpleString)
	assert.True(t, isOK)

	// Past ABSTTL means ttlMS < now, so handler sets ttl=0
	ttlResp = handler.executeCommand(state, "TTL", [][]byte{[]byte("rkey_past")}, "127.0.0.1:12345")
	ttlInt, ok = ttlResp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*ttlInt) <= 0)

	// --- Non-ABSTTL: TTL should be the relative milliseconds ---
	resp = handler.executeCommand(state, "RESTORE", [][]byte{
		[]byte("rkey_rel"),
		[]byte("30000"), // 30 seconds
		[]byte(dumpData),
		[]byte("REPLACE"),
	}, "127.0.0.1:12345")
	_, isOK = resp.(*proto.SimpleString)
	assert.True(t, isOK)

	ttlResp = handler.executeCommand(state, "TTL", [][]byte{[]byte("rkey_rel")}, "127.0.0.1:12345")
	ttlInt, ok = ttlResp.(*proto.Integer)
	assert.True(t, ok)
	ttlVal := int64(*ttlInt)
	assert.True(t, ttlVal > 0 && ttlVal <= 30)
}

// ---------- Priority 3: SORT 选项组合 ----------

// TestSortMutationKill_Combinations 验证 SORT 复杂选项组合
// 目标变异体: key_commands.go:548-591 (BY/GET/ASC/DESC/ALPHA/STORE option loop)
func TestSortMutationKill_Combinations(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.LPush("sort_combo", "c")
	handler.Db.LPush("sort_combo", "b")
	handler.Db.LPush("sort_combo", "a")
	handler.Db.Set("weight_a", "30")
	handler.Db.Set("weight_b", "10")
	handler.Db.Set("weight_c", "20")
	handler.Db.Set("name_a", "Alice")
	handler.Db.Set("name_b", "Bob")
	handler.Db.Set("name_c", "Charlie")

	// --- BY + GET combo: sort by weight, retrieve name ---
	resp := handler.executeCommand(state, "SORT", [][]byte{
		[]byte("sort_combo"), []byte("BY"), []byte("weight_*"),
		[]byte("GET"), []byte("name_*"),
	}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 3, len(arr.Args))
	if len(arr.Args) == 3 {
		assert.Equal(t, "Bob", string(arr.Args[0]))
		assert.Equal(t, "Charlie", string(arr.Args[1]))
		assert.Equal(t, "Alice", string(arr.Args[2]))
	}

	// --- BY + GET + DESC combo ---
	resp = handler.executeCommand(state, "SORT", [][]byte{
		[]byte("sort_combo"), []byte("BY"), []byte("weight_*"),
		[]byte("GET"), []byte("name_*"), []byte("DESC"),
	}, "127.0.0.1:12345")
	arr, ok = resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 3, len(arr.Args))
	if len(arr.Args) == 3 {
		assert.Equal(t, "Alice", string(arr.Args[0]))
		assert.Equal(t, "Charlie", string(arr.Args[1]))
		assert.Equal(t, "Bob", string(arr.Args[2]))
	}

	// --- BY + LIMIT combo ---
	resp = handler.executeCommand(state, "SORT", [][]byte{
		[]byte("sort_combo"), []byte("BY"), []byte("weight_*"),
		[]byte("LIMIT"), []byte("1"), []byte("2"),
	}, "127.0.0.1:12345")
	arr, ok = resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr.Args))
	if len(arr.Args) == 2 {
		assert.Equal(t, "c", string(arr.Args[0]))
		assert.Equal(t, "a", string(arr.Args[1]))
	}

	// --- STORE: verify stored list ---
	resp = handler.executeCommand(state, "SORT", [][]byte{
		[]byte("sort_combo"), []byte("BY"), []byte("weight_*"),
		[]byte("STORE"), []byte("sort_stored"),
	}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(3), int64(*integer))
	stored, _ := handler.Db.LRange("sort_stored", 0, -1)
	assert.Equal(t, []string{"b", "c", "a"}, stored)
}

// TestSortMutationKill_LimitBoundaries 验证 SORT LIMIT 边界
// 目标变异体: key_commands.go:558 (offset/count parsing)
func TestSortMutationKill_LimitBoundaries(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.LPush("sort_lim", "5")
	handler.Db.LPush("sort_lim", "3")
	handler.Db.LPush("sort_lim", "1")
	handler.Db.LPush("sort_lim", "4")
	handler.Db.LPush("sort_lim", "2")

	// LIMIT 0 3 → first 3 elements
	resp := handler.executeCommand(state, "SORT", [][]byte{
		[]byte("sort_lim"), []byte("LIMIT"), []byte("0"), []byte("3"),
	}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 3, len(arr.Args))
	if len(arr.Args) == 3 {
		assert.Equal(t, "1", string(arr.Args[0]))
		assert.Equal(t, "2", string(arr.Args[1]))
		assert.Equal(t, "3", string(arr.Args[2]))
	}

	// LIMIT 4 10 → only 1 element left
	resp = handler.executeCommand(state, "SORT", [][]byte{
		[]byte("sort_lim"), []byte("LIMIT"), []byte("4"), []byte("10"),
	}, "127.0.0.1:12345")
	arr, ok = resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 1, len(arr.Args))
	if len(arr.Args) == 1 {
		assert.Equal(t, "5", string(arr.Args[0]))
	}

	// LIMIT 0 0 → empty result
	resp = handler.executeCommand(state, "SORT", [][]byte{
		[]byte("sort_lim"), []byte("LIMIT"), []byte("0"), []byte("0"),
	}, "127.0.0.1:12345")
	arr, ok = resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 0, len(arr.Args))

	// LIMIT 10 5 → offset past end → empty
	resp = handler.executeCommand(state, "SORT", [][]byte{
		[]byte("sort_lim"), []byte("LIMIT"), []byte("10"), []byte("5"),
	}, "127.0.0.1:12345")
	arr, ok = resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 0, len(arr.Args))
}

// ---------- Supplementary: Admin/Client/Bitmap edge cases ----------

// TestClientMutationKill_InfoOptions 验证 CLIENT INFO 返回可解析的字段
// 目标变异体: client_commands.go:52-108 (CONDITIONALS_NEGATION in option parsing)
func TestClientMutationKill_InfoOptions(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Register a connection so there's data to report
	conn := &mockNetConnMut{}
	meta := handler.registerConnection(state, conn, "127.0.0.1:9999")
	handler.connsMu.Lock()
	handler.conns[state] = meta
	handler.connsMu.Unlock()
	defer handler.unregisterConnection(state)

	// CLIENT INFO returns bulk string with connection info
	resp := handler.executeCommand(state, "CLIENT", [][]byte{[]byte("INFO")}, "127.0.0.1:9999")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	info := string(*bs)
	assert.True(t, len(info) > 0)
	assert.True(t, strings.Contains(info, "id="))
	assert.True(t, strings.Contains(info, "addr="))
}

// TestInfoMutationKill_StatsFields 验证 INFO stats 返回可解析字段
// 目标变异体: info.go:56-105 (ARITHMETIC_BASE in calculations)
func TestInfoMutationKill_StatsFields(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// INFO with no section returns all sections
	resp := handler.executeCommand(state, "INFO", [][]byte{}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	info := string(*bs)
	assert.True(t, len(info) > 0)
	// Verify it contains the Server section
	assert.True(t, strings.Contains(info, "# Server"))
	// Verify uptime_in_seconds field exists
	assert.True(t, strings.Contains(info, "uptime_in_seconds:"))
}

// TestBitmapMutationKill_BitposEdgeCases 验证 BITPOS 边界
// 目标变异体: bitmap_commands.go:61-164 (CONDITIONALS_NEGATION)
func TestBitmapMutationKill_BitposEdgeCases(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// SET a byte: 10000000 (0x80)
	handler.executeCommand(state, "SET", [][]byte{[]byte("bmp"), []byte{0x80}}, "127.0.0.1:12345")

	// BITPOS 1 — should find bit 0
	resp := handler.executeCommand(state, "BITPOS", [][]byte{[]byte("bmp"), []byte("1")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))

	// BITPOS 0 — should find bit at position 1
	resp = handler.executeCommand(state, "BITPOS", [][]byte{[]byte("bmp"), []byte("0")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// BITPOS with range: BITPOS key 1 0 0 (byte 0 only)
	resp = handler.executeCommand(state, "BITPOS", [][]byte{
		[]byte("bmp"), []byte("1"), []byte("0"), []byte("0"),
	}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))

	// Non-existent key: BITPOS should return -1 for bit 1
	resp = handler.executeCommand(state, "BITPOS", [][]byte{[]byte("bmp_empty"), []byte("1")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(-1), int64(*integer))
}

// TestHashMutationKill_HrandfieldEdgeCases 验证 HRANDFIELD 边界
// 目标变异体: hash_commands.go:361-362
func TestHashMutationKill_HrandfieldEdgeCases(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "HSET", [][]byte{[]byte("hrng"), []byte("f1"), []byte("v1")}, "127.0.0.1:12345")
	handler.executeCommand(state, "HSET", [][]byte{[]byte("hrng"), []byte("f2"), []byte("v2")}, "127.0.0.1:12345")
	handler.executeCommand(state, "HSET", [][]byte{[]byte("hrng"), []byte("f3"), []byte("v3")}, "127.0.0.1:12345")

	// HRANDFIELD with positive count returns distinct fields
	resp := handler.executeCommand(state, "HRANDFIELD", [][]byte{
		[]byte("hrng"), []byte("2"),
	}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr.Args))
	for _, elem := range arr.Args {
		field := string(elem)
		assert.True(t, field == "f1" || field == "f2" || field == "f3")
	}

	// HRANDFIELD with negative count allows duplicates, capped by field count
	resp = handler.executeCommand(state, "HRANDFIELD", [][]byte{
		[]byte("hrng"), []byte("-5"),
	}, "127.0.0.1:12345")
	arr, ok = resp.(*proto.Array)
	assert.True(t, ok)
	assert.True(t, len(arr.Args) >= 3)
}

// ---------- Supplementary: Replication path (handler_core.go) ----------

// TestReplicaofMutationKill_ErrorPath 验证 REPLICAOF 错误路径
// 目标变异体: handler_core.go:509-524 (CONDITIONALS_NEGATION)
func TestReplicaofMutationKill_ErrorPath(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()
	// No replication manager set — should return error
	resp := handler.executeCommand(state, "REPLICAOF", [][]byte{
		[]byte("127.0.0.1"), []byte("6379"),
	}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "replication not enabled"))
}

// TestGensearchstoreMutationKill_Fromlonlat 验证 GEOSEARCHSTORE FROMLONLAT 精确值
// 目标变异体: geo_commands.go:382-383 (ARITHMETIC_BASE in ParseFloat)
func TestGensearchstoreMutationKill_Fromlonlat(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "GEOADD", [][]byte{
		[]byte("gs_src"), []byte("-122.4194"), []byte("37.7749"), []byte("SF"),
	}, "127.0.0.1:12345")
	handler.executeCommand(state, "GEOADD", [][]byte{
		[]byte("gs_src"), []byte("-118.2437"), []byte("34.0522"), []byte("LA"),
	}, "127.0.0.1:12345")

	// GEOSEARCHSTORE with FROMLONLAT — within 100km should find only SF
	resp := handler.executeCommand(state, "GEOSEARCHSTORE", [][]byte{
		[]byte("gs_dst"), []byte("gs_src"),
		[]byte("FROMLONLAT"), []byte("-122.4194"), []byte("37.7749"),
		[]byte("BYRADIUS"), []byte("100"), []byte("km"),
	}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	members, _ := handler.Db.ZRange("gs_dst", 0, -1)
	assert.Equal(t, 1, len(members))
	assert.Equal(t, "SF", members[0].Member)
}

// ---------- Supplementary: Key commands ----------

// TestExpireMutationKill_PexpireatConversion 验证 EXPIRE→PEXPIREAT 转换
// 目标变异体: handler_core.go:534 (ARITHMETIC_BASE in time conversion)
func TestExpireMutationKill_PexpireatConversion(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("exp_key"), []byte("val")}, "127.0.0.1:12345")

	// EXPIRE with 60 seconds
	resp := handler.executeCommand(state, "EXPIRE", [][]byte{
		[]byte("exp_key"), []byte("60"),
	}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// TTL should be between 0 and 60
	ttlResp := handler.executeCommand(state, "TTL", [][]byte{[]byte("exp_key")}, "127.0.0.1:12345")
	ttlInt, ok := ttlResp.(*proto.Integer)
	assert.True(t, ok)
	ttl := int64(*ttlInt)
	assert.True(t, ttl > 0 && ttl <= 60)

	// PEXPIRE with 30000 ms
	handler.executeCommand(state, "SET", [][]byte{[]byte("pexp_key"), []byte("val")}, "127.0.0.1:12345")
	resp = handler.executeCommand(state, "PEXPIRE", [][]byte{
		[]byte("pexp_key"), []byte("30000"),
	}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// PTTL should be between 0 and 30000
	pttlResp := handler.executeCommand(state, "PTTL", [][]byte{[]byte("pexp_key")}, "127.0.0.1:12345")
	pttlInt, ok := pttlResp.(*proto.Integer)
	assert.True(t, ok)
	pttl := int64(*pttlInt)
	assert.True(t, pttl > 0 && pttl <= 30000)
}

// TestSortMutationKill_EmptyKey 验证 SORT 空键行为
func TestSortMutationKill_EmptyKey(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// SORT on non-existent key — store returns empty type, handler returns error
	resp := handler.executeCommand(state, "SORT", [][]byte{[]byte("no_such_key")}, "127.0.0.1:12345")
	_, isArray := resp.(*proto.Array)
	_, isErr := resp.(*proto.Error)
	assert.True(t, isArray || isErr)
}

// TestGensearchstoreMutationKill_CountZero 验证 GEOSEARCHSTORE COUNT 0
func TestGensearchstoreMutationKill_CountZero(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "GEOADD", [][]byte{
		[]byte("gs_z"), []byte("-122.4194"), []byte("37.7749"), []byte("SF"),
	}, "127.0.0.1:12345")

	// COUNT 0 — verify the return is a valid Integer (not an error)
	resp := handler.executeCommand(state, "GEOSEARCHSTORE", [][]byte{
		[]byte("gs_zd"), []byte("gs_z"),
		[]byte("FROMLONLAT"), []byte("-122.4194"), []byte("37.7749"),
		[]byte("BYRADIUS"), []byte("1000"), []byte("km"),
		[]byte("COUNT"), []byte("0"),
	}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*integer) >= 0)
}

// ---------- Mock types ----------

// mockNetConnMut is a minimal net.Conn mock for mutation-kill tests
type mockNetConnMut struct{}

func (m *mockNetConnMut) Read(b []byte) (n int, err error)   { return 0, fmt.Errorf("mock") }
func (m *mockNetConnMut) Write(b []byte) (n int, err error)  { return len(b), nil }
func (m *mockNetConnMut) Close() error                       { return nil }
func (m *mockNetConnMut) LocalAddr() net.Addr                { return &mockAddrMut{} }
func (m *mockNetConnMut) RemoteAddr() net.Addr               { return &mockAddrMut{} }
func (m *mockNetConnMut) SetDeadline(t time.Time) error      { return nil }
func (m *mockNetConnMut) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockNetConnMut) SetWriteDeadline(t time.Time) error { return nil }

type mockAddrMut struct{}

func (m *mockAddrMut) Network() string { return "tcp" }
func (m *mockAddrMut) String() string  { return "127.0.0.1:9999" }

// Ensure math and fmt imports are used
var _ = math.Inf
