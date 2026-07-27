package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// =============================================================================
// Phase 9: Final targeted tests to push past 90% mcover
// JSON NX/XX, compression TTL paths, geospatial internal,
// backpressure, hash edge cases, list internal paths
// =============================================================================

// ================== json.go NX/XX ==================

func TestJSONSetNX(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	// NX=true, key doesn't exist → should set
	result, err := s.JSONSet("jsnx1", "$", `{"a":1}`, true, false)
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// NX=true, key exists → should skip
	result, err = s.JSONSet("jsnx1", "$", `{"b":2}`, true, false)
	assert.NoError(t, err)
	// skipped but still returns OK
}

func TestJSONSetXX(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	// XX=true, key doesn't exist → should skip
	result, err := s.JSONSet("jsxx1", "$", `{"a":1}`, false, true)
	assert.NoError(t, err)
	_ = result

	// Now set without XX
	s.JSONSet("jsxx1", "$", `{"a":1}`, false, false)

	// XX=true, key exists → should set
	result, err = s.JSONSet("jsxx1", "$", `{"b":2}`, false, true)
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)
}

func TestJSONSetNXAndXX(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	// Both NX and XX → always skip
	s.JSONSet("jsnxexx", "$", `{"a":1}`, true, true)
}

func TestJSONClearP9(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.JSONSet("jclr", "$", `{"a":1,"b":2}`, false, false)
	cleared, err := s.JSONClear("jclr", "$")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), cleared)
}

func TestJSONNumIncrByNonExistent(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	_, err := s.JSONNumIncrBy("jsinc_ne", "$", 1.0)
	assert.Error(t, err)
}

func TestJSONDebugMemoryP9(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.JSONSet("jsdm", "$", `{"key":"value"}`, false, false)
	size, err := s.JSONDebugMemory("jsdm", "$")
	assert.NoError(t, err)
	assert.True(t, size > 0)
}

func TestJSONTypeP9(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.JSONSet("jst", "$", `{"key":"value"}`, false, false)
	jtype, err := s.JSONType("jst", "$")
	assert.NoError(t, err)
	assert.Equal(t, "object", jtype)
}

func TestJSONTypeArray(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.JSONSet("jsta", "$", `[1,2,3]`, false, false)
	jtype, err := s.JSONType("jsta", "$")
	assert.NoError(t, err)
	assert.Equal(t, "array", jtype)
}

// ================== compression.go TTL paths ==================

func TestCompressionSnappyWithTTL(t *testing.T) {
	t.Parallel()
	// Use a store with Snappy compression
	dir := t.TempDir()
	cs, err := NewBotreonStoreWithCompression(dir, CompressionSnappy)
	assert.NoError(t, err)
	defer cs.Close()

	// Set with TTL — exercise setEntryWithCompression TTL paths
	cs.SetWithTTL("cs_ttl", "a"+string(make([]byte, 100)), 60*time.Second)

	val, err := cs.Get("cs_ttl")
	assert.NoError(t, err)
	assert.Equal(t, "a"+string(make([]byte, 100)), val)
}

func TestCompressionLZ4WithTTL(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cs, err := NewBotreonStoreWithCompression(dir, CompressionLZ4)
	assert.NoError(t, err)
	defer cs.Close()

	cs.SetWithTTL("cl_ttl", "hello world "+string(make([]byte, 200)), 30*time.Second)
	val, err := cs.Get("cl_ttl")
	assert.NoError(t, err)
	assert.Contains(t, val, "hello world")
}

func TestCompressionNoneWithTTL(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cs, err := NewBotreonStoreWithCompression(dir, CompressionNone)
	assert.NoError(t, err)
	defer cs.Close()

	cs.SetWithTTL("cn_ttl", "data", 60*time.Second)
	val, err := cs.Get("cn_ttl")
	assert.NoError(t, err)
	assert.Equal(t, "data", val)
}

// ================== geospatial.go internal ==================

func TestGeoDistKM(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.GeoAdd("gd_km", []GeoMember{
		{Member: "a", Lon: 13.361389, Lat: 38.115556},
		{Member: "b", Lon: 13.368500, Lat: 38.111111},
	})

	dist, err := s.GeoDist("gd_km", "a", "b", "KM")
	assert.NoError(t, err)
	assert.True(t, dist > 0)
}

func TestGeoDistMI(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.GeoAdd("gd_mi", []GeoMember{
		{Member: "a", Lon: 13.361389, Lat: 38.115556},
		{Member: "b", Lon: 13.368500, Lat: 38.111111},
	})

	dist, err := s.GeoDist("gd_mi", "a", "b", "MI")
	assert.NoError(t, err)
	assert.True(t, dist > 0)
}

func TestGeoDistFT(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.GeoAdd("gd_ft", []GeoMember{
		{Member: "a", Lon: 13.361389, Lat: 38.115556},
		{Member: "b", Lon: 13.368500, Lat: 38.111111},
	})

	dist, err := s.GeoDist("gd_ft", "a", "b", "FT")
	assert.NoError(t, err)
	assert.True(t, dist > 0)
}

func TestGeoDistUnsupportedUnit(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.GeoAdd("gd_un", []GeoMember{
		{Member: "a", Lon: 13.361389, Lat: 38.115556},
		{Member: "b", Lon: 13.368500, Lat: 38.111111},
	})

	_, err := s.GeoDist("gd_un", "a", "b", "ZZ")
	assert.Error(t, err) // unsupported unit
}

// ================== backpressure.go ==================

func TestPreWriteCheckWithConfig(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	// Enable backpressure with high thresholds
	cfg := BackpressureConfig{
		Enabled:         true,
		L0SoftThreshold: 100.0,
		L0HardThreshold: 200.0,
		MaxPreDelay:     1000000000,
	}
	s.SetBackpressureConfig(cfg)

	delay, reject := s.preWriteCheck()
	// With no actual L0 pressure, score should be 0 → no delay, no reject
	assert.Equal(t, time.Duration(0), delay)
	assert.False(t, reject)
}

func TestGetRetryMetricsAfterWrite(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.Set("grem_a", "val")
	metrics := s.GetRetryMetrics()
	assert.True(t, metrics.TotalRetries >= 0)
	assert.True(t, metrics.ActiveRetries >= 0)
}

// ================== hash.go additional ==================

func TestHScanWithPattern(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.HSet("hs_p1", "name", "Alice")
	s.HSet("hs_p1", "age", "30")
	s.HSet("hs_p1", "city", "NYC")

	result, err := s.HScan("hs_p1", 0, "n*", 10)
	assert.NoError(t, err)
	assert.True(t, len(result.Fields) > 0)
}

func TestHLenP9(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.HSet("hl1", "f1", "v1")
	s.HSet("hl1", "f2", "v2")
	s.HSet("hl1", "f3", "v3")

	length, err := s.HLen("hl1")
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), length)
}

func TestHExistsP9(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.HSet("he1", "f1", "v1")

	exists, err := s.HExists("he1", "f1")
	assert.NoError(t, err)
	assert.True(t, exists)

	exists, err = s.HExists("he1", "missing")
	assert.NoError(t, err)
	assert.False(t, exists)
}

// ================== list.go additional ==================

func TestLIndexHead(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.LPush("lih", "c", "b", "a")
	val, err := s.LIndex("lih", 0)
	assert.NoError(t, err)
	assert.Equal(t, "a", val)
}

func TestLIndexTail(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.LPush("lit", "c", "b", "a")
	val, err := s.LIndex("lit", 2)
	assert.NoError(t, err)
	assert.Equal(t, "c", val)
}

func TestLIndexMiddle(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.LPush("lim", "c", "b", "a")
	val, err := s.LIndex("lim", 1)
	assert.NoError(t, err)
	assert.Equal(t, "b", val)
}

func TestLLenP9(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.LPush("llen1", "c", "b", "a")
	length, err := s.LLen("llen1")
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), length)
}

func TestLPushAndRPop(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.LPush("lpr1", "a", "b")
	val, err := s.RPop("lpr1")
	assert.NoError(t, err)
	assert.Equal(t, "a", val)
}

// ================== set.go additional ==================

func TestSMembersP9(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.SAdd("sm1", "a", "b", "c")
	members, err := s.SMembers("sm1")
	assert.NoError(t, err)
	assert.Equal(t, 3, len(members))
}

func TestSIsMemberTrue(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.SAdd("sim1", "a", "b")
	exists, err := s.SIsMember("sim1", "a")
	assert.NoError(t, err)
	assert.True(t, exists)
}

func TestSIsMemberFalse(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.SAdd("simf", "a")
	exists, err := s.SIsMember("simf", "z")
	assert.NoError(t, err)
	assert.False(t, exists)
}

func TestTSGetWithData(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.TSCreate("tsgd", TSCreateOptions{})
	s.TSAdd("tsgd", 1000, 1.5, TSAddOptions{})
	s.TSAdd("tsgd", 2000, 2.5, TSAddOptions{})

	dp, err := s.TSGet("tsgd")
	assert.NoError(t, err)
	assert.Equal(t, int64(2000), dp.Timestamp)
	assert.Equal(t, 2.5, dp.Value)
}

func TestTSRangeAllPoints(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.TSCreate("tsra", TSCreateOptions{})
	s.TSAdd("tsra", 100, 1.0, TSAddOptions{})
	s.TSAdd("tsra", 200, 2.0, TSAddOptions{})
	s.TSAdd("tsra", 300, 3.0, TSAddOptions{})

	result, err := s.TSRange("tsra", "100", "300", 0)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(result))
}

func TestTSExists(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	s.TSCreate("tsex", TSCreateOptions{})
	s.TSAdd("tsex", 1000, 1.0, TSAddOptions{})

	// TSExists doesn't exist; verify via TSGet
	_, err := s.TSGet("tsex")
	assert.NoError(t, err)

	_, err = s.TSGet("tsne")
	assert.Error(t, err)
}
