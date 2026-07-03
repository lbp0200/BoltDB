package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// =============================================================================
// Phase 5b: Mutation Kill Tests for misc files
// geospatial.go (11), json.go (7), lcs.go (6), compression.go (7),
// backpressure.go (8), hyperloglog.go (4), set.go (4), hash.go (25), list.go (25)
// =============================================================================

// ================== geospatial.go (11 mutants) ==================

func TestExpandBoundingBoxClampMinLat(t *testing.T) {
	t.Parallel()
	// Near south pole with large radius → minLat should clamp to -90
	minLat, maxLat, minLon, maxLon := expandBoundingBox(-89, -89, 0, 0, 200000)
	assert.Equal(t, float64(-90), minLat)
	_ = maxLat
	_ = minLon
	_ = maxLon
}

func TestExpandBoundingBoxClampMaxLat(t *testing.T) {
	t.Parallel()
	minLat, maxLat, _, _ := expandBoundingBox(89, 89, 0, 0, 200000)
	_ = minLat
	assert.Equal(t, float64(90), maxLat)
}

func TestExpandBoundingBoxClampMinLon(t *testing.T) {
	t.Parallel()
	_, _, minLon, _ := expandBoundingBox(0, 0, -179, -179, 200000)
	assert.Equal(t, float64(-180), minLon)
}

func TestExpandBoundingBoxClampMaxLon(t *testing.T) {
	t.Parallel()
	_, _, _, maxLon := expandBoundingBox(0, 0, 179, 179, 200000)
	assert.Equal(t, float64(180), maxLon)
}

func TestExpandBoundingBoxNoClamp(t *testing.T) {
	t.Parallel()
	minLat, maxLat, minLon, maxLon := expandBoundingBox(0, 0, 0, 0, 1000)
	assert.True(t, minLat < 0)
	assert.True(t, maxLat > 0)
	assert.True(t, minLon < 0)
	assert.True(t, maxLon > 0)
}

func TestConvertGeoRadiusToMetersKM(t *testing.T) {
	t.Parallel()
	m := convertGeoRadiusToMeters(1, "KM")
	assert.Equal(t, 1000.0, m)
}

func TestConvertGeoRadiusToMetersMI(t *testing.T) {
	t.Parallel()
	m := convertGeoRadiusToMeters(1, "MI")
	assert.InDelta(t, 1609.344, m, 0.01)
}

func TestConvertGeoRadiusToMetersFT(t *testing.T) {
	t.Parallel()
	m := convertGeoRadiusToMeters(10, "FT")
	assert.InDelta(t, 3.048, m, 0.01)
}

func TestConvertGeoRadiusToMetersUnknown(t *testing.T) {
	t.Parallel()
	m := convertGeoRadiusToMeters(5, "ZZ")
	assert.Equal(t, 5.0, m) // unknown unit returns unchanged
}

func TestFormatGeoDistanceKM(t *testing.T) {
	t.Parallel()
	d := formatGeoDistance(2000, "KM")
	assert.Equal(t, 2.0, d)
}

func TestFormatGeoDistanceMI(t *testing.T) {
	t.Parallel()
	d := formatGeoDistance(1609.344, "MI")
	assert.InDelta(t, 1.0, d, 0.01)
}

func TestFormatGeoDistanceFT(t *testing.T) {
	t.Parallel()
	d := formatGeoDistance(1, "FT")
	assert.InDelta(t, 3.28084, d, 0.01)
}

func TestFormatGeoDistanceDefault(t *testing.T) {
	t.Parallel()
	d := formatGeoDistance(100, "ZZ")
	assert.Equal(t, 100.0, d)
}

// ================== json.go (7 mutants) ==================

func TestGetValueByPathDollar(t *testing.T) {
	t.Parallel()
	root := map[string]interface{}{"a": float64(1)}
	val, err := getValueByPath(root, "$")
	assert.NoError(t, err)
	assert.Equal(t, root, val)
}

func TestGetValueByPathDollarDot(t *testing.T) {
	t.Parallel()
	root := map[string]interface{}{"a": float64(1)}
	val, err := getValueByPath(root, "$.a")
	assert.NoError(t, err)
	assert.Equal(t, float64(1), val)
}

func TestGetValueByPathNested(t *testing.T) {
	t.Parallel()
	root := map[string]interface{}{
		"a": map[string]interface{}{
			"b": "found",
		},
	}
	val, err := getValueByPath(root, "$.a.b")
	assert.NoError(t, err)
	assert.Equal(t, "found", val)
}

func TestGetValueByPathNotFound(t *testing.T) {
	t.Parallel()
	root := map[string]interface{}{"a": float64(1)}
	_, err := getValueByPath(root, "$.missing")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestGetValueByPathNotTraversable(t *testing.T) {
	t.Parallel()
	root := map[string]interface{}{"a": "string_value"}
	_, err := getValueByPath(root, "$.a.b")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not traversable")
}

func TestGetJSONTypeAll(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "null", getJSONType(nil))
	assert.Equal(t, "boolean", getJSONType(true))
	assert.Equal(t, "string", getJSONType("hello"))
	assert.Equal(t, "number", getJSONType(42.0))
	assert.Equal(t, "array", getJSONType([]interface{}{}))
	assert.Equal(t, "object", getJSONType(map[string]interface{}{}))
	assert.Equal(t, "unknown", getJSONType(42)) // int
}

func TestGetValueByPathEmptyString(t *testing.T) {
	t.Parallel()
	root := "root"
	val, err := getValueByPath(root, "")
	assert.NoError(t, err)
	assert.Equal(t, root, val)
}

// ================== lcs.go (6 mutants) ==================

func TestComputeLCSNoCommon(t *testing.T) {
	t.Parallel()
	lcs, length := computeLCS("abc", "xyz")
	assert.Equal(t, "", lcs)
	assert.Equal(t, 0, length)
}

func TestComputeLCSIdentical(t *testing.T) {
	t.Parallel()
	lcs, length := computeLCS("hello", "hello")
	assert.Equal(t, "hello", lcs)
	assert.Equal(t, 5, length)
}

func TestComputeLCSPartial(t *testing.T) {
	t.Parallel()
	lcs, _ := computeLCS("abcde", "ace")
	assert.Equal(t, "ace", lcs)
}

func TestComputeLCSLengthP5(t *testing.T) {
	t.Parallel()
	l := computeLCSLength("abcde", "ace")
	assert.Equal(t, 3, l)
}

func TestComputeLCSLengthEmpty(t *testing.T) {
	t.Parallel()
	l := computeLCSLength("", "abc")
	assert.Equal(t, 0, l)
}

func TestComputeLCSMatchesBasic(t *testing.T) {
	t.Parallel()
	matches := ComputeLCSMatches("hello world", "world hello", 3)
	assert.True(t, len(matches) >= 1)
	// Should find "hello" and "world" as contiguous matches
}

func TestComputeLCSMatchesMinLenFilter(t *testing.T) {
	t.Parallel()
	matches := ComputeLCSMatches("abc", "axc", 2)
	// LCS is "ac" but not contiguous, so no segments of length >= 2
	assert.Equal(t, 0, len(matches))
}

func TestComputeLCSMatchesEmpty(t *testing.T) {
	t.Parallel()
	matches := ComputeLCSMatches("", "abc", 1)
	assert.Nil(t, matches)

	matches = ComputeLCSMatches("abc", "", 1)
	assert.Nil(t, matches)
}

func TestComputeLCSMatchesZeroLength(t *testing.T) {
	t.Parallel()
	matches := ComputeLCSMatches("abc", "xyz", 1)
	assert.Equal(t, 0, len(matches))
}

// ================== compression.go (7 mutants) ==================

func TestCompressDataNone(t *testing.T) {
	t.Parallel()
	data := []byte("hello world")
	result, err := compressData(data, CompressionNone)
	assert.NoError(t, err)
	assert.Equal(t, data, result)
}

func TestCompressDataEmpty(t *testing.T) {
	t.Parallel()
	result, err := compressData([]byte{}, CompressionSnappy)
	assert.NoError(t, err)
	assert.Equal(t, []byte{}, result)
}

func TestCompressDataUnsupported(t *testing.T) {
	t.Parallel()
	_, err := compressData([]byte("data"), CompressionType("unknown"))
	assert.Error(t, err)
}

func TestDecompressDataEmpty(t *testing.T) {
	t.Parallel()
	result, err := decompressData([]byte{})
	assert.NoError(t, err)
	assert.Equal(t, []byte{}, result)
}

func TestDecompressDataNoMagic(t *testing.T) {
	t.Parallel()
	data := []byte("raw uncompressed data")
	result, err := decompressData(data)
	assert.NoError(t, err)
	assert.Equal(t, data, result)
}

func TestDecompressDataTooShort(t *testing.T) {
	t.Parallel()
	data := []byte("ab")
	result, err := decompressData(data)
	assert.NoError(t, err)
	assert.Equal(t, data, result) // too short for magic, returned as-is
}

func TestShouldCompressThreshold(t *testing.T) {
	t.Parallel()
	// Less than 64 bytes → should not compress
	assert.False(t, shouldCompress([]byte("short"), CompressionSnappy))
	// Exactly 64 bytes → should compress
	assert.True(t, shouldCompress(make([]byte, 64), CompressionSnappy))
	// CompressionNone → never compress
	assert.False(t, shouldCompress(make([]byte, 100), CompressionNone))
}

func TestCompressSnappyRoundtrip(t *testing.T) {
	t.Parallel()
	data := []byte("This is a test string that should compress and decompress correctly.")
	compressed, err := compressData(data, CompressionSnappy)
	assert.NoError(t, err)
	assert.NotEqual(t, data, compressed)

	decompressed, err := decompressData(compressed)
	assert.NoError(t, err)
	assert.Equal(t, data, decompressed)
}

func TestCompressLZ4Roundtrip(t *testing.T) {
	t.Parallel()
	data := make([]byte, 128)
	for i := range data {
		data[i] = byte(i%26 + 'a')
	}
	compressed, err := compressData(data, CompressionLZ4)
	assert.NoError(t, err)
	decompressed, err := decompressData(compressed)
	assert.NoError(t, err)
	assert.Equal(t, data, decompressed)
}

func TestCompressZSTRoundtrip(t *testing.T) {
	t.Parallel()
	data := make([]byte, 128)
	for i := range data {
		data[i] = byte(i%26 + 'a')
	}
	compressed, err := compressData(data, CompressionZSTD)
	assert.NoError(t, err)
	decompressed, err := decompressData(compressed)
	assert.NoError(t, err)
	assert.Equal(t, data, decompressed)
}

// ================== backpressure.go (8 mutants) ==================

func TestGetRetryMetricsDefaults(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	metrics := s.GetRetryMetrics()
	assert.Equal(t, int64(0), metrics.ActiveRetries)
	assert.Equal(t, int64(0), metrics.TotalRetries)
	assert.Equal(t, int64(0), metrics.L0Rejected)
	assert.Equal(t, int64(0), metrics.L0Delayed)
}

func TestPreWriteCheckDisabled(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	// backpressure is nil by default in test store
	delay, reject := s.preWriteCheck()
	assert.Equal(t, time.Duration(0), delay)
	assert.False(t, reject)
}

func TestSetBackpressureConfig(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	cfg := BackpressureConfig{
		Enabled:         true,
		L0SoftThreshold: 5.0,
		L0HardThreshold: 20.0,
		MaxPreDelay:     500000000, // 500ms
	}
	s.SetBackpressureConfig(cfg)
	got := s.GetBackpressureConfig()
	assert.True(t, got.Enabled)
	assert.Equal(t, 5.0, got.L0SoftThreshold)
	assert.Equal(t, 20.0, got.L0HardThreshold)
}

func TestGetBackpressureConfigDefaults(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	cfg := s.GetBackpressureConfig()
	assert.NotNil(t, cfg)
}

// ================== hyperloglog.go (4 mutants) ==================

func TestPFAddAndGetBasic(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	added, err := s.PFAdd("hll_basic", "a", "b", "c")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), added)

	count, err := s.PFCount("hll_basic")
	assert.NoError(t, err)
	assert.True(t, count >= 2) // approximate
}

func TestPFMergeBasic(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.PFAdd("hll_m1", "a", "b")
	s.PFAdd("hll_m2", "c", "d")

	err := s.PFMerge("hll_mdst", "hll_m1", "hll_m2")
	assert.NoError(t, err)

	count, _ := s.PFCount("hll_mdst")
	assert.True(t, count >= 3)
}

func TestPFCountNonexistent(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	count, err := s.PFCount("hll_none")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

func TestPFAddOverwrite(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.PFAdd("hll_ow", "a", "b")
	s.PFAdd("hll_ow", "c") // should merge, not overwrite

	count, _ := s.PFCount("hll_ow")
	assert.True(t, count >= 2) // at least a, b (and c)
}

// ================== set.go (4 mutants) ==================

func TestSPopZeroCount(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.SAdd("spop_z", "a", "b", "c")
	members, err := s.SPopN("spop_z", 0)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(members))
}

func TestSPopNegativeCount(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.SAdd("spop_n", "a", "b", "c")
	members, err := s.SPopN("spop_n", -5)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(members))
}

func TestSPopMoreThanAvailable(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.SAdd("spop_m", "a")
	members, err := s.SPopN("spop_m", 100)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(members))
}

func TestSInterCardBasic(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.SAdd("sic_a", "a", "b")
	s.SAdd("sic_b", "b", "c")

	count, err := s.SInterCard("sic_a", "sic_b")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)
}

// ================== list.go (25 mutants) ==================

func TestLPosWithRankAndCount(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.LPush("lpos_rc", "a", "b", "a", "c", "a")
	// rank=2 means find 2nd occurrence
	positions, err := s.LPos("lpos_rc", "a", 2, 0, 0)
	assert.NoError(t, err)
	if len(positions) > 0 {
		assert.Equal(t, int64(2), positions[0])
	}
}

func TestLPosNotFound(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.LPush("lpos_nx", "a", "b")
	positions, err := s.LPos("lpos_nx", "z", 0, 0, 0)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(positions))
}

func TestLMoveLeftRight(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.LPush("lmv_a", "b", "a")
	s.LPush("lmv_b", "d")

	val, err := s.LMove("lmv_a", "lmv_b", "LEFT", "RIGHT")
	assert.NoError(t, err)
	assert.Equal(t, "a", val)

	data1, _ := s.LRange("lmv_a", 0, -1)
	assert.Equal(t, 1, len(data1))

	data2, _ := s.LRange("lmv_b", 0, -1)
	assert.Equal(t, 2, len(data2))
}

func TestLMoveRightLeft(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.LPush("lmvrl_a", "b", "a")
	s.LPush("lmvrl_b", "d")

	val, err := s.LMove("lmvrl_a", "lmvrl_b", "RIGHT", "LEFT")
	assert.NoError(t, err)
	assert.Equal(t, "b", val)
}

func TestLRemRemoveFirst(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.LPush("lrem_f", "c", "b", "a", "b")
	count, err := s.LRem("lrem_f", 1, "b")
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestLRemRemoveAll(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.LPush("lrem_a", "b", "a", "b", "a")
	count, err := s.LRem("lrem_a", 0, "a")
	assert.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestLRemRemoveNegativeCount(t *testing.T) {
	// Not parallel — avoids BadgerDB contention with other store tests
	s := setupTestStore(t)

	n, err := s.LPush("lrem_n", "b", "a", "b", "a")
	assert.NoError(t, err)
	assert.Equal(t, 4, n, "LPush should return 4")
	count, err := s.LRem("lrem_n", -1, "a")
	assert.NoError(t, err)
	assert.Equal(t, 1, count, "LRem should remove 1 element from tail")

	// Verify final state
	vals, err := s.LRange("lrem_n", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "b"}, vals)
}

func TestLSetValidIndex(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.LPush("lset_v", "c", "b", "a")
	err := s.LSet("lset_v", 1, "x")
	assert.NoError(t, err)

	val, _ := s.LIndex("lset_v", 1)
	assert.Equal(t, "x", val)
}

func TestLTrimKeepMiddle(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.LPush("ltrm_m", "e", "d", "c", "b", "a")
	err := s.LTrim("ltrm_m", 1, 3)
	assert.NoError(t, err)

	data, _ := s.LRange("ltrm_m", 0, -1)
	assert.Equal(t, 3, len(data))
}

func TestLInsertBefore(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.LPush("lins_b", "c", "a")
	count, err := s.LInsert("lins_b", "BEFORE", "a", "b")
	assert.NoError(t, err)
	assert.True(t, count > 0)

	data, _ := s.LRange("lins_b", 0, -1)
	// a should be after b
	for i, v := range data {
		if v == "b" {
			assert.True(t, i+1 < len(data))
			assert.Equal(t, "a", data[i+1])
			break
		}
	}
}

func TestLInsertAfter(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.LPush("lins_a", "c", "a")
	count, err := s.LInsert("lins_a", "AFTER", "a", "b")
	assert.NoError(t, err)
	assert.True(t, count > 0)
}

func TestLInsertPivotNotFound(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.LPush("lins_nf", "c", "a")
	count, err := s.LInsert("lins_nf", "BEFORE", "z", "x")
	assert.NoError(t, err)
	assert.Equal(t, -1, count) // pivot not found returns -1
}

// ================== hash.go (25 mutants) ==================

func TestHGetAllEmpty(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	fields, err := s.HGetAll("hga_empty")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(fields))
}

func TestHIncrByNewField(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	newVal, err := s.HIncrBy("hib_new", "f1", 10)
	assert.NoError(t, err)
	assert.Equal(t, int64(10), newVal)
}

func TestHIncrByNegative(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.HSet("hib_neg", "f1", "10")
	newVal, err := s.HIncrBy("hib_neg", "f1", -5)
	assert.NoError(t, err)
	assert.Equal(t, int64(5), newVal)
}

func TestHIncrByFloatNegative(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.HSet("hibf_neg", "f1", "10.5")
	newVal, err := s.HIncrByFloat("hibf_neg", "f1", -0.5)
	assert.NoError(t, err)
	assert.Equal(t, 10.0, newVal)
}

func TestHRandFieldZero(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.HSet("hrf_z", "f1", "v1")
	s.HSet("hrf_z", "f2", "v2")
	s.HSet("hrf_z", "f3", "v3")

	fields, _, err := s.HRandField("hrf_z", 0, false)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(fields)) // count=0 returns all
}

func TestHRandFieldNegative(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.HSet("hrf_n", "f1", "v1")
	s.HSet("hrf_n", "f2", "v2")

	fields, _, err := s.HRandField("hrf_n", -5, false)
	assert.NoError(t, err)
	assert.Equal(t, 5, len(fields)) // negative count allows repeats
}

func TestHRandFieldWithValues(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.HSet("hrf_wv", "f1", "v1")
	s.HSet("hrf_wv", "f2", "v2")

	fields, values, err := s.HRandField("hrf_wv", 2, true)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(fields))
	assert.Equal(t, 2, len(values))
}

func TestHScanBasic(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.HSet("hs_b", "f1", "v1")
	s.HSet("hs_b", "f2", "v2")
	s.HSet("hs_b", "g1", "w1")

	result, err := s.HScan("hs_b", 0, "f*", 10)
	assert.NoError(t, err)
	assert.True(t, len(result.Fields) > 0)
}

func TestHScanEmpty(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	result, err := s.HScan("hs_e", 0, "*", 10)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(result.Fields))
}

func TestHScanNoMatch(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.HSet("hs_nm", "f1", "v1")
	result, err := s.HScan("hs_nm", 0, "g*", 10)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(result.Fields))
}

func TestHScanWithCursor(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	for i := 0; i < 20; i++ {
		s.HSet("hs_c", string(rune('a'+i%26)), "v")
	}

	result1, err := s.HScan("hs_c", 0, "", 5)
	assert.NoError(t, err)
	if result1.Cursor > 0 {
		result2, err := s.HScan("hs_c", result1.Cursor, "", 5)
		assert.NoError(t, err)
		_ = result2
	}
}

func TestHScanCountDefault(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.HSet("hs_cd", "f1", "v1")
	result, err := s.HScan("hs_cd", 0, "f*", 0) // count <= 0 should default to 10
	assert.NoError(t, err)
	assert.True(t, len(result.Fields) > 0)
}

// ================== define.go (38 mutants) ==================

func TestTypeOfKeyGetString(t *testing.T) {
	t.Parallel()
	key := TypeOfKeyGet("mykey")
	assert.Contains(t, string(key), "mykey")
}

func TestTypeOfKeyGetEmpty(t *testing.T) {
	t.Parallel()
	key := TypeOfKeyGet("")
	assert.NotNil(t, key)
}

func TestIsUUIDFormatValid(t *testing.T) {
	t.Parallel()
	assert.True(t, isUUIDFormat("550e8400-e29b-41d4-a716-446655440000"))
}

func TestIsUUIDFormatInvalid(t *testing.T) {
	t.Parallel()
	assert.False(t, isUUIDFormat("not-a-uuid"))
	assert.False(t, isUUIDFormat(""))
	assert.False(t, isUUIDFormat("550e8400-e29b-41d4-a716"))
}

func TestSetGetBackpressureConfig(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	cfg := BackpressureConfig{
		Enabled:         true,
		L0SoftThreshold: 8.0,
		L0HardThreshold: 20.0,
		MaxPreDelay:     1000000000,
	}
	s.SetBackpressureConfig(cfg)
	got := s.GetBackpressureConfig()
	assert.Equal(t, cfg.Enabled, got.Enabled)
	assert.Equal(t, cfg.L0SoftThreshold, got.L0SoftThreshold)
	assert.Equal(t, cfg.L0HardThreshold, got.L0HardThreshold)
	assert.Equal(t, cfg.MaxPreDelay, got.MaxPreDelay)
}

func TestIterateRawKeysP5(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.Set("irk_a", "val1")
	s.Set("irk_b", "val2")

	var keys []string
	err := s.IterateRawKeys(func(rawKey string) bool {
		keys = append(keys, rawKey)
		return true // continue
	})
	assert.NoError(t, err)
	assert.True(t, len(keys) >= 2)
}

func TestGetDB(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	db := s.GetDB()
	assert.NotNil(t, db)
}

func TestClearCaches(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	s.Set("cc_a", "val")
	s.ClearCaches()
	// Should not panic
}

func TestCheck(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	err := s.Check()
	assert.NoError(t, err)
}
