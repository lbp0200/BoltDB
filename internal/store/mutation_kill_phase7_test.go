package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// =============================================================================
// Phase 7: Mutation Kill Tests targeting remaining NOT COVERED areas
// string.go BitPos/BitField/BitLen, list.go additional paths,
// timeseries.go additional paths, define.go additional paths
// =============================================================================

// ================== string.go BitPos ==================

func TestBitPosEmptyStringBit1(t *testing.T) {

	s := setupTestStore(t)

	s.Set("bp_e1", "")
	pos, err := s.BitPos("bp_e1", 1, 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, -1, pos) // no 1-bits in empty string
}

func TestBitPosEmptyStringBit0(t *testing.T) {

	s := setupTestStore(t)

	s.Set("bp_e0", "")
	pos, err := s.BitPos("bp_e0", 0, 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 0, pos) // first 0-bit at position 0
}

func TestBitPosAllZeros(t *testing.T) {

	s := setupTestStore(t)

	s.Set("bp_az", "\x00\x00\x00")
	pos, err := s.BitPos("bp_az", 1, 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, -1, pos) // no 1-bits found
}

func TestBitPosAllOnes(t *testing.T) {

	s := setupTestStore(t)

	s.Set("bp_a1", "\xff\xff")
	pos, err := s.BitPos("bp_a1", 0, 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, -1, pos) // no 0-bits found
}

func TestBitPosNegativeStart(t *testing.T) {

	s := setupTestStore(t)

	s.Set("bp_ns", "\x80\x00") // byte 0: 10000000, byte 1: 00000000
	// Negative start: -1 means last byte (byte 1)
	pos, err := s.BitPos("bp_ns", 1, -1, -1)
	assert.NoError(t, err)
	// Byte 1 is all zeros, no 1-bit found in byte 1
	assert.Equal(t, -1, pos)
}

func TestBitPosStartGreaterThanLen(t *testing.T) {

	s := setupTestStore(t)

	s.Set("bp_sg", "\x00")
	pos, err := s.BitPos("bp_sg", 0, 100, 100)
	assert.NoError(t, err)
	assert.Equal(t, -1, pos)
}

func TestBitPosEndExceedsLen(t *testing.T) {

	s := setupTestStore(t)

	s.Set("bp_ee", "\x80") // bit 0 is set
	pos, err := s.BitPos("bp_ee", 1, 0, 100)
	assert.NoError(t, err)
	assert.Equal(t, 0, pos)
}

func TestBitPosStartGreaterThanEnd(t *testing.T) {

	s := setupTestStore(t)

	s.Set("bp_se", "\xff")
	pos, err := s.BitPos("bp_se", 0, 5, 2) // start > end
	assert.NoError(t, err)
	assert.Equal(t, -1, pos)
}

// ================== string.go BitLen ==================

func TestBitLenBasic(t *testing.T) {

	s := setupTestStore(t)

	s.Set("bl_b", "hello")
	length, err := s.BitLen("bl_b")
	assert.NoError(t, err)
	assert.Equal(t, 40, length) // 5 bytes * 8
}

func TestBitLenEmpty(t *testing.T) {

	s := setupTestStore(t)

	s.Set("bl_e", "")
	length, err := s.BitLen("bl_e")
	assert.NoError(t, err)
	assert.Equal(t, 0, length)
}

func TestBitLenSingleByte(t *testing.T) {

	s := setupTestStore(t)

	s.Set("bl_s", "A")
	length, err := s.BitLen("bl_s")
	assert.NoError(t, err)
	assert.Equal(t, 8, length)
}

// ================== string.go BitField ==================

func TestBitFieldGetBasic(t *testing.T) {

	s := setupTestStore(t)

	s.Set("bf_gb", "\xff\x00")
	results, err := s.BitField("bf_gb", []string{"GET", "u8", "0"})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, int64(255), results[0].(int64))
}

func TestBitFieldSetBasic(t *testing.T) {

	s := setupTestStore(t)

	s.Set("bf_sb", "\x00\x00")
	results, err := s.BitField("bf_sb", []string{"SET", "u8", "0", "42"})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(results))
}

func TestBitFieldIncrbySignedOverflow(t *testing.T) {

	s := setupTestStore(t)

	s.Set("bf_so", "\x00\x00\x00\x00")
	// INCRBY i8 0 127 → max for i8
	results, err := s.BitField("bf_so", []string{"INCRBY", "i8", "0", "127"})
	assert.NoError(t, err)
	assert.Equal(t, int64(127), results[0].(int64))

	// INCRBY i8 0 1 → overflow wraps to -128
	results, err = s.BitField("bf_so", []string{"INCRBY", "i8", "0", "1"})
	assert.NoError(t, err)
	assert.Equal(t, int64(-128), results[0].(int64))
}

func TestBitFieldIncrbyUnsignedOverflow(t *testing.T) {

	s := setupTestStore(t)

	s.Set("bf_uo", "\x00\x00\x00\x00")
	// INCRBY u4 0 15 → max for u4
	results, err := s.BitField("bf_uo", []string{"INCRBY", "u4", "0", "15"})
	assert.NoError(t, err)
	assert.Equal(t, int64(15), results[0].(int64))

	// INCRBY u4 0 1 → wraps to 0
	results, err = s.BitField("bf_uo", []string{"INCRBY", "u4", "0", "1"})
	assert.NoError(t, err)
	assert.Equal(t, int64(0), results[0].(int64))
}

func TestBitFieldIncrbyUnsignedNegative(t *testing.T) {

	s := setupTestStore(t)

	s.Set("bf_un", "\x00\x00\x00\x00")
	// Start from 10, dec by 3 → should wrap
	results, err := s.BitField("bf_un", []string{"SET", "u4", "0", "10"})
	assert.NoError(t, err)
	_ = results

	results, err = s.BitField("bf_un", []string{"INCRBY", "u4", "0", "-3"})
	assert.NoError(t, err)
	// Result should be 7 (10 - 3)
	assert.Equal(t, int64(7), results[0].(int64))
}

func TestBitFieldMultipleOps(t *testing.T) {

	s := setupTestStore(t)

	s.Set("bf_mo", "\x00\x00\x00\x00")
	results, err := s.BitField("bf_mo", []string{
		"SET", "u8", "0", "100",
		"GET", "u8", "0",
	})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(results))
}

func TestBitField64Bits(t *testing.T) {

	s := setupTestStore(t)

	s.Set("bf_64", "\x00\x00\x00\x00\x00\x00\x00\x00")
	// Use a small value that won't overflow
	results, err := s.BitField("bf_64", []string{"SET", "u64", "0", "42"})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(results))

	results2, err := s.BitField("bf_64", []string{"GET", "u64", "0"})
	assert.NoError(t, err)
	assert.Equal(t, int64(42), results2[0].(int64))
}

// ================== list.go additional paths ==================

func TestLMPopLeft(t *testing.T) {

	s := setupTestStore(t)

	s.LPush("lmp_l", "c", "b", "a")

	key, values, err := s.LMPop([]string{"lmp_l"}, "LEFT", 2)
	assert.NoError(t, err)
	assert.Equal(t, "lmp_l", key)
	assert.Equal(t, 2, len(values))
	assert.Equal(t, "a", values[0]) // LEFT = from head
}

func TestLMPopRight(t *testing.T) {

	s := setupTestStore(t)

	s.LPush("lmp_r", "c", "b", "a")

	key, values, err := s.LMPop([]string{"lmp_r"}, "RIGHT", 2)
	assert.NoError(t, err)
	assert.Equal(t, "lmp_r", key)
	assert.Equal(t, 2, len(values))
	assert.Equal(t, "c", values[0]) // RIGHT = from tail
}

func TestLMPopSingleElement(t *testing.T) {

	s := setupTestStore(t)

	s.LPush("lmp_s", "a")
	key, values, err := s.LMPop([]string{"lmp_s"}, "LEFT", 10)
	assert.NoError(t, err)
	assert.Equal(t, "lmp_s", key)
	assert.Equal(t, 1, len(values))
}

func TestLMPopEmpty(t *testing.T) {

	s := setupTestStore(t)

	key, values, err := s.LMPop([]string{"lmp_e"}, "LEFT", 1)
	assert.NoError(t, err)
	assert.Equal(t, "", key)
	assert.Equal(t, 0, len(values))
}

// ================== timeseries.go additional ==================

func TestTSRangeEmptyKeyP7(t *testing.T) {

	s := setupTestStore(t)

	_, err := s.TSRange("ts_ne", "0", "100", 0)
	assert.NoError(t, err) // returns empty on non-existent key
}

func TestTSRevRangeEmptyKeyP7(t *testing.T) {

	s := setupTestStore(t)

	_, err := s.TSRevRange("ts_ne_rev", "0", "100", 0)
	assert.NoError(t, err) // returns empty on non-existent key
}

func TestTSDelNonexistent(t *testing.T) {

	s := setupTestStore(t)

	_, err := s.TSDel("ts_del_ne", "0", "100")
	assert.Error(t, err)
}

func TestTSMGetMixed(t *testing.T) {

	s := setupTestStore(t)

	s.TSCreate("tsmg_a", TSCreateOptions{})
	s.TSAdd("tsmg_a", 1000, 1.0, TSAddOptions{})

	points, err := s.TSMGet("", "tsmg_a", "tsmg_nonexist")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(points))
	assert.NotNil(t, points[0])
	assert.Nil(t, points[1]) // nonexistent
}

func TestTSQueryIndex(t *testing.T) {

	s := setupTestStore(t)

	s.TSCreate("tsqi_a", TSCreateOptions{})
	s.TSCreate("tsqi_b", TSCreateOptions{})

	keys, err := s.TSQueryIndex([]string{"test=value"})
	assert.NoError(t, err)
	assert.True(t, len(keys) >= 1)
}

func TestTSQueryIndexEmpty(t *testing.T) {

	s := setupTestStore(t)

	keys, err := s.TSQueryIndex([]string{})
	assert.NoError(t, err)
	// returns all TS keys (if any)
	_ = keys
}

// ================== define.go additional ==================

func TestCheckOnFreshDB(t *testing.T) {

	s := setupTestStore(t)

	err := s.Check()
	assert.NoError(t, err)
}

func TestFlushDBOnEmpty(t *testing.T) {

	s := setupTestStore(t)

	err := s.FlushDB()
	assert.NoError(t, err)
}

func TestFlushDBWithData(t *testing.T) {

	s := setupTestStore(t)

	s.Set("fdb_a", "1")
	s.Set("fdb_b", "2")
	err := s.FlushDB()
	assert.NoError(t, err)

	exists, _ := s.Exists("fdb_a")
	assert.False(t, exists)
}

func TestClearAllData(t *testing.T) {

	s := setupTestStore(t)

	s.Set("cad_a", "1")
	s.LPush("cad_b", "x")
	s.HSet("cad_c", "f", "v")

	s.ClearAllData()

	exists, _ := s.Exists("cad_a")
	assert.False(t, exists)
}

// ================== base.go additional ==================

func TestDelGeoKeyP7(t *testing.T) {

	s := setupTestStore(t)

	s.GeoAdd("del_geo", []GeoMember{
		{Member: "m1", Lon: 1.0, Lat: 2.0},
	})

	exists, _ := s.Exists("del_geo")
	assert.True(t, exists)

	s.Del("del_geo")
	exists, _ = s.Exists("del_geo")
	assert.False(t, exists)
}

func TestDelStreamKeyP7(t *testing.T) {

	s := setupTestStore(t)

	s.XAdd("del_stream", StreamXAddOptions{}, "*", map[string]string{"f": "v"})

	exists, _ := s.Exists("del_stream")
	assert.True(t, exists)

	s.Del("del_stream")
	exists, _ = s.Exists("del_stream")
	assert.False(t, exists)
}

func TestDelHLLKeyP7(t *testing.T) {

	s := setupTestStore(t)

	s.PFAdd("del_hll", "a", "b")

	exists, _ := s.Exists("del_hll")
	assert.True(t, exists)

	s.Del("del_hll")
	exists, _ = s.Exists("del_hll")
	assert.False(t, exists)
}

func TestTypeOfKeyGetStream(t *testing.T) {

	s := setupTestStore(t)

	s.XAdd("tok_s", StreamXAddOptions{}, "*", map[string]string{"k": "v"})
	key := TypeOfKeyGet("tok_s")
	assert.NotNil(t, key)
}

func TestTypeOfKeyGetHLL(t *testing.T) {

	s := setupTestStore(t)

	s.PFAdd("tok_h", "a")
	key := TypeOfKeyGet("tok_h")
	assert.NotNil(t, key)
}

func TestTypeOfKeyGetGeo(t *testing.T) {

	s := setupTestStore(t)

	s.GeoAdd("tok_g", []GeoMember{
		{Member: "m1", Lon: 1.0, Lat: 2.0},
	})
	key := TypeOfKeyGet("tok_g")
	assert.NotNil(t, key)
}

// ================== hash.go additional ==================

func TestHScanCountZero(t *testing.T) {

	s := setupTestStore(t)

	s.HSet("hs_z", "f1", "v1")
	s.HSet("hs_z", "f2", "v2")

	// count <= 0 should default to 10
	result, err := s.HScan("hs_z", 0, "f*", 0)
	assert.NoError(t, err)
	assert.True(t, len(result.Fields) > 0)
}

func TestHScanWithNonMatchingPattern(t *testing.T) {

	s := setupTestStore(t)

	s.HSet("hs_nm2", "f1", "v1")
	result, err := s.HScan("hs_nm2", 0, "g*", 10)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(result.Fields))
}

// ================== geospatial.go additional ==================

func TestExpandBoundingBoxNoClampP7(t *testing.T) {

	minLat, maxLat, minLon, maxLon := expandBoundingBox(0, 0, 0, 0, 1000)
	assert.True(t, minLat < 0)
	assert.True(t, maxLat > 0)
	assert.True(t, minLon < 0)
	assert.True(t, maxLon > 0)
}

func TestFormatGeoDistanceMP7(t *testing.T) {

	d := formatGeoDistance(500, "M")
	assert.Equal(t, 500.0, d)
}

func TestFormatGeoDistanceEmptyUnit(t *testing.T) {

	d := formatGeoDistance(500, "")
	assert.Equal(t, 500.0, d)
}

func TestConvertGeoRadiusMeters(t *testing.T) {

	m := convertGeoRadiusToMeters(1, "M")
	assert.Equal(t, 1.0, m)
}

func TestConvertGeoRadiusEmptyUnit(t *testing.T) {

	m := convertGeoRadiusToMeters(5, "")
	assert.Equal(t, 5.0, m)
}
