package replication

import (
	"bytes"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/zeebo/assert"
)

// TestLoadRDB_Valid tests LoadRDB with valid RDB data
func TestLoadRDB_Valid(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// First generate valid RDB data
	rdbData, err := GenerateRDB(testStore)
	assert.NoError(t, err)
	assert.True(t, len(rdbData) >= 9) // valid RDB = "REDIS0009" header (9 bytes)

	// Load the RDB data - use LoadRDBWithStore since LoadRDB is not exported
	err = LoadRDBWithStore(rdbData, testStore)
	assert.NoError(t, err)
}

// TestLoadRDB_WithStringData tests LoadRDB with string data
func TestLoadRDB_WithStringData(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// Add string data
	testStore.Set("stringkey", "stringvalue")

	// Generate RDB
	rdbData, err := GenerateRDB(testStore)
	assert.NoError(t, err)

	// Create a new store and load RDB
	testStore2 := setupTestStore(t)
	defer testStore2.Close()

	err = LoadRDBWithStore(rdbData, testStore2)
	assert.NoError(t, err)

	// Verify data was loaded
	val, err := testStore2.Get("stringkey")
	assert.NoError(t, err)
	assert.Equal(t, "stringvalue", val)
}

// TestLoadRDB_WithListData tests LoadRDB with list data
func TestLoadRDB_WithListData(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// Add list data
	testStore.RPush("listkey", "value1", "value2", "value3")

	// Generate RDB
	rdbData, err := GenerateRDB(testStore)
	assert.NoError(t, err)

	// Create a new store and load RDB
	testStore2 := setupTestStore(t)
	defer testStore2.Close()

	err = LoadRDBWithStore(rdbData, testStore2)
	assert.NoError(t, err)

	// Verify data was loaded
	len, err := testStore2.LLen("listkey")
	assert.NoError(t, err)
	assert.Equal(t, int64(3), len)
}

// TestLoadRDB_WithHashData tests LoadRDB with hash data
func TestLoadRDB_WithHashData(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// Add hash data
	testStore.HSet("hashkey", "field1", "value1")
	testStore.HSet("hashkey", "field2", "value2")

	// Generate RDB
	rdbData, err := GenerateRDB(testStore)
	assert.NoError(t, err)

	// Create a new store and load RDB
	testStore2 := setupTestStore(t)
	defer testStore2.Close()

	err = LoadRDBWithStore(rdbData, testStore2)
	assert.NoError(t, err)

	// Verify data was loaded
	val, err := testStore2.HGet("hashkey", "field1")
	assert.NoError(t, err)
	assert.Equal(t, []byte("value1"), val)
}

// TestLoadRDB_WithSetData tests LoadRDB with set data
func TestLoadRDB_WithSetData(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// Add set data
	testStore.SAdd("setkey", "member1", "member2", "member3")

	// Generate RDB
	rdbData, err := GenerateRDB(testStore)
	assert.NoError(t, err)

	// Create a new store and load RDB
	testStore2 := setupTestStore(t)
	defer testStore2.Close()

	err = LoadRDBWithStore(rdbData, testStore2)
	assert.NoError(t, err)

	// Verify data was loaded
	count, err := testStore2.SCard("setkey")
	assert.NoError(t, err)
	assert.Equal(t, int64(3), count)
}

// TestLoadRDB_WithSortedSetData tests LoadRDB with sorted set data
func TestLoadRDB_WithSortedSetData(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// Add sorted set data
	testStore.ZAdd("zsetkey", []store.ZSetMember{
		{Member: "member1", Score: 1.0},
		{Member: "member2", Score: 2.0},
	})

	// Generate RDB
	rdbData, err := GenerateRDB(testStore)
	assert.NoError(t, err)

	// Create a new store and load RDB
	testStore2 := setupTestStore(t)
	defer testStore2.Close()

	err = LoadRDBWithStore(rdbData, testStore2)
	assert.NoError(t, err)

	// Verify data was loaded
	count, err := testStore2.ZCard("zsetkey")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), int64(count))
}

// TestLoadRDB_MultipleDataTypes tests LoadRDB with multiple data types
func TestLoadRDB_MultipleDataTypes(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// Add multiple data types
	testStore.Set("stringkey", "stringvalue")
	testStore.RPush("listkey", "value1")
	testStore.HSet("hashkey", "field1", "value1")
	testStore.SAdd("setkey", "member1")
	testStore.ZAdd("zsetkey", []store.ZSetMember{{Member: "member1", Score: 1.0}})

	// Generate RDB
	rdbData, err := GenerateRDB(testStore)
	assert.NoError(t, err)

	// Create a new store and load RDB
	testStore2 := setupTestStore(t)
	defer testStore2.Close()

	err = LoadRDBWithStore(rdbData, testStore2)
	assert.NoError(t, err)

	// Verify all data was loaded
	_, err = testStore2.Get("stringkey")
	assert.NoError(t, err)

	_, err = testStore2.HGet("hashkey", "field1")
	assert.NoError(t, err)

	count, _ := testStore2.SCard("setkey")
	assert.Equal(t, int64(1), count)

	zcount, _ := testStore2.ZCard("zsetkey")
	assert.Equal(t, int64(1), int64(zcount))
}

// TestLoadRDB_WithHLLData tests RDB roundtrip for HyperLogLog
func TestLoadRDB_WithHLLData(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	_, err := testStore.PFAdd("hllkey", "a", "b", "c", "d", "e")
	assert.NoError(t, err)
	count, err := testStore.PFCount("hllkey")
	assert.NoError(t, err)
	assert.Equal(t, int64(5), count)

	_, err = testStore.PFAdd("hllkey2", "x", "y", "z")
	assert.NoError(t, err)

	// Generate RDB
	rdbData, err := GenerateRDB(testStore)
	assert.NoError(t, err)
	t.Logf("RDB data size: %d", len(rdbData))
	if len(rdbData) > 80 {
		t.Logf("RDB start hex: %x", rdbData[:80])
		t.Logf("RDB end hex: %x", rdbData[len(rdbData)-20:])
	}

	// Create a new store and load RDB
	testStore2 := setupTestStore(t)
	defer testStore2.Close()

	err = LoadRDBWithStore(rdbData, testStore2)
	assert.NoError(t, err)

	// Verify HLL data was restored
	count2, err := testStore2.PFCount("hllkey")
	assert.NoError(t, err)
	assert.Equal(t, int64(5), count2)

	count3, err := testStore2.PFCount("hllkey2")
	assert.NoError(t, err)
	assert.Equal(t, int64(3), count3)
}

// TestLoadRDB_EmptyData tests LoadRDB with empty data
func TestLoadRDB_EmptyData(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// Empty RDB data - should cause error
	err := LoadRDBWithStore([]byte{}, testStore)
	// Empty RDB causes error
	assert.True(t, err != nil)
}

// TestLoadRDB_InvalidHeader tests LoadRDB with invalid header
func TestLoadRDB_InvalidHeader(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// Invalid RDB data (not starting with REDIS)
	err := LoadRDBWithStore([]byte("INVALID"), testStore)
	// Should return error
	assert.True(t, err != nil)
}

// TestLoadRDB_TruncatedData tests LoadRDB with truncated data
func TestLoadRDB_TruncatedData(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// Truncated RDB data - may or may not error depending on implementation
	err := LoadRDBWithStore([]byte("REDIS0009"), testStore)
	// Just verify it doesn't panic — error is acceptable for truncated data
	_ = err
}

// TestWriteKeyValue tests WriteKeyValue function
func TestWriteKeyValue(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// Add some data
	testStore.Set("testkey", "testvalue")

	// Generate RDB should include this key
	rdbData, err := GenerateRDB(testStore)
	assert.NoError(t, err)
	assert.True(t, len(rdbData) >= 9) // valid RDB = "REDIS0009" header (9 bytes)

	// Verify RDB contains the key
	assert.True(t, bytes.Contains(rdbData, []byte("testkey")))
	assert.True(t, bytes.Contains(rdbData, []byte("testvalue")))
}

// TestWriteListKeyValue tests WriteListKeyValue function
func TestWriteListKeyValue(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// Add list data
	testStore.RPush("mylist", "a", "b", "c")

	// Generate RDB
	rdbData, err := GenerateRDB(testStore)
	assert.NoError(t, err)

	// Verify RDB contains list data
	assert.True(t, bytes.Contains(rdbData, []byte("mylist")))
}

// TestWriteHashKeyValue tests WriteHashKeyValue function
func TestWriteHashKeyValue(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// Add hash data
	testStore.HSet("myhash", "field1", "value1")

	// Generate RDB
	rdbData, err := GenerateRDB(testStore)
	assert.NoError(t, err)

	// Verify RDB contains hash data
	assert.True(t, bytes.Contains(rdbData, []byte("myhash")))
}

// TestWriteSetKeyValue tests WriteSetKeyValue function
func TestWriteSetKeyValue(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// Add set data
	testStore.SAdd("myset", "member1", "member2")

	// Generate RDB
	rdbData, err := GenerateRDB(testStore)
	assert.NoError(t, err)

	// Verify RDB contains set data
	assert.True(t, bytes.Contains(rdbData, []byte("myset")))
}

// TestWriteSortedSetKeyValue tests WriteSortedSetKeyValue function
func TestWriteSortedSetKeyValue(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// Add sorted set data
	testStore.ZAdd("myzset", []store.ZSetMember{
		{Member: "member1", Score: 1.5},
		{Member: "member2", Score: 2.5},
	})

	// Generate RDB
	rdbData, err := GenerateRDB(testStore)
	assert.NoError(t, err)

	// Verify RDB contains zset data
	assert.True(t, bytes.Contains(rdbData, []byte("myzset")))
}

// TestGenerateRDB_MultipleKeys tests GenerateRDB with multiple keys
func TestGenerateRDB_MultipleKeys(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// Add multiple keys
	for i := 0; i < 10; i++ {
		testStore.Set(string(rune('a'+i)), string(rune('0'+i)))
	}

	// Generate RDB
	rdbData, err := GenerateRDB(testStore)
	assert.NoError(t, err)
	// Just verify it generated some data
	assert.True(t, len(rdbData) >= 9) // valid RDB = "REDIS0009" header (9 bytes)
}

// TestLoadRDB_Overwrite tests LoadRDB overwrites existing data
func TestLoadRDB_Overwrite(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// Set initial value
	testStore.Set("key", "original")

	// Generate RDB
	rdbData, err := GenerateRDB(testStore)
	assert.NoError(t, err)

	// Create new store with different data
	testStore2 := setupTestStore(t)
	defer testStore2.Close()
	testStore2.Set("key", "different")

	// Load RDB - should overwrite
	err = LoadRDBWithStore(rdbData, testStore2)
	assert.NoError(t, err)

	// Verify original value was loaded
	val, err := testStore2.Get("key")
	assert.NoError(t, err)
	assert.Equal(t, "original", val)
}

// TestWriteKeyValue_StringType tests WriteKeyValue with string type
func TestWriteKeyValue_StringType(t *testing.T) {
	t.Parallel()
	enc := NewRDBEncoder()
	err := enc.WriteKeyValue("testkey", "testvalue", store.KeyTypeString, 0)
	assert.NoError(t, err)

	rdbData := enc.Bytes()
	assert.True(t, len(rdbData) >= 9) // valid RDB = "REDIS0009" header (9 bytes)
}

// TestWriteKeyValue_WithTTL tests WriteKeyValue with TTL
func TestWriteKeyValue_WithTTL(t *testing.T) {
	t.Parallel()
	enc := NewRDBEncoder()
	expireAt := time.Now().Unix() + 60
	err := enc.WriteKeyValue("testkey", "testvalue", store.KeyTypeString, expireAt)
	assert.NoError(t, err)

	rdbData := enc.Bytes()
	assert.True(t, len(rdbData) >= 9) // valid RDB = "REDIS0009" header (9 bytes)
}

// TestWriteKeyValue_ListType tests WriteKeyValue with list type
func TestWriteKeyValue_ListType(t *testing.T) {
	t.Parallel()
	enc := NewRDBEncoder()
	err := enc.WriteKeyValue("testkey", []string{"a", "b", "c"}, store.KeyTypeList, 0)
	assert.NoError(t, err)

	rdbData := enc.Bytes()
	assert.True(t, len(rdbData) >= 9) // valid RDB = "REDIS0009" header (9 bytes)
}

// TestWriteKeyValue_SetType tests WriteKeyValue with set type
func TestWriteKeyValue_SetType(t *testing.T) {
	t.Parallel()
	enc := NewRDBEncoder()
	err := enc.WriteKeyValue("testkey", []string{"a", "b", "c"}, store.KeyTypeSet, 0)
	assert.NoError(t, err)

	rdbData := enc.Bytes()
	assert.True(t, len(rdbData) >= 9) // valid RDB = "REDIS0009" header (9 bytes)
}

// TestWriteKeyValue_HashType tests WriteKeyValue with hash type
func TestWriteKeyValue_HashType(t *testing.T) {
	t.Parallel()
	enc := NewRDBEncoder()
	err := enc.WriteKeyValue("testkey", map[string][]byte{"field1": []byte("value1")}, store.KeyTypeHash, 0)
	assert.NoError(t, err)

	rdbData := enc.Bytes()
	assert.True(t, len(rdbData) >= 9) // valid RDB = "REDIS0009" header (9 bytes)
}

// TestWriteKeyValue_ZSetType tests WriteKeyValue with sorted set type
// Note: WriteKeyValue writes the value as a string even for ZSET type (no error)
func TestWriteKeyValue_ZSetType(t *testing.T) {
	t.Parallel()
	enc := NewRDBEncoder()
	err := enc.WriteKeyValue("testkey", "dummy", store.KeyTypeSortedSet, 0)
	// No error is returned - it just writes the string value
	assert.NoError(t, err)

	rdbData := enc.Bytes()
	assert.True(t, len(rdbData) >= 9) // valid RDB = "REDIS0009" header (9 bytes)
}

// TestWriteKeyValue_LargeList tests WriteKeyValue with large list (triggers 14-bit and 32-bit length encoding)
func TestWriteKeyValue_LargeList(t *testing.T) {
	t.Parallel()
	enc := NewRDBEncoder()
	// Create a list with 100+ elements to trigger 14-bit length encoding
	values := make([]string, 150)
	for i := range values {
		values[i] = string(rune(i % 256))
	}
	err := enc.WriteKeyValue("largekey", values, store.KeyTypeList, 0)
	assert.NoError(t, err)

	rdbData := enc.Bytes()
	assert.True(t, len(rdbData) >= 9) // valid RDB = "REDIS0009" header (9 bytes)
}

// TestWriteKeyValue_LargeHash tests WriteKeyValue with large hash
func TestWriteKeyValue_LargeHash(t *testing.T) {
	t.Parallel()
	enc := NewRDBEncoder()
	// Create a hash with 100+ fields to trigger 14-bit length encoding
	fields := make(map[string][]byte, 150)
	for i := 0; i < 150; i++ {
		fields[string(rune(i))] = []byte("value")
	}
	err := enc.WriteKeyValue("largehash", fields, store.KeyTypeHash, 0)
	assert.NoError(t, err)

	rdbData := enc.Bytes()
	assert.True(t, len(rdbData) >= 9) // valid RDB = "REDIS0009" header (9 bytes)
}

// TestWriteKeyValue_LargeSet tests WriteKeyValue with large set
func TestWriteKeyValue_LargeSet(t *testing.T) {
	t.Parallel()
	enc := NewRDBEncoder()
	// Create a set with 100+ members to trigger 14-bit length encoding
	members := make([]string, 150)
	for i := range members {
		members[i] = string(rune(i))
	}
	err := enc.WriteKeyValue("largetset", members, store.KeyTypeSet, 0)
	assert.NoError(t, err)

	rdbData := enc.Bytes()
	assert.True(t, len(rdbData) >= 9) // valid RDB = "REDIS0009" header (9 bytes)
}

// TestLoadRDB_WithGeoData tests RDB round-trip for GEO data
func TestLoadRDB_WithGeoData(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	_, err := testStore.GeoAdd("mygeo", []store.GeoMember{
		{Member: "Paris", Lat: 48.8566, Lon: 2.3521},
		{Member: "London", Lat: 51.5074, Lon: -0.1278},
	})
	assert.NoError(t, err)

	rdbData, err := GenerateRDB(testStore)
	assert.NoError(t, err)

	testStore2 := setupTestStore(t)
	defer testStore2.Close()

	err = LoadRDBWithStore(rdbData, testStore2)
	assert.NoError(t, err)

	positions, err := testStore2.GeoPos("mygeo", "Paris", "London")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(positions))
}

// TestLoadRDB_WithJSONData tests RDB round-trip for JSON data
func TestLoadRDB_WithJSONData(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	_, err := testStore.JSONSet("myjson", "$", `{"name":"test","value":42}`, false, false)
	assert.NoError(t, err)

	rdbData, err := GenerateRDB(testStore)
	assert.NoError(t, err)

	testStore2 := setupTestStore(t)
	defer testStore2.Close()

	err = LoadRDBWithStore(rdbData, testStore2)
	assert.NoError(t, err)

	vals, err := testStore2.JSONGet("myjson", "$")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(vals))
}

// TestLoadRDB_WithTimeSeriesData tests RDB round-trip for TimeSeries data
func TestLoadRDB_WithTimeSeriesData(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	_, err := testStore.TSAdd("myts", 1000, 42.5, store.TSAddOptions{})
	assert.NoError(t, err)
	_, err = testStore.TSAdd("myts", 2000, 43.5, store.TSAddOptions{})
	assert.NoError(t, err)

	rdbData, err := GenerateRDB(testStore)
	assert.NoError(t, err)

	testStore2 := setupTestStore(t)
	defer testStore2.Close()

	err = LoadRDBWithStore(rdbData, testStore2)
	assert.NoError(t, err)

	point, err := testStore2.TSGet("myts")
	assert.NoError(t, err)
	assert.NotNil(t, point)
	assert.Equal(t, float64(43.5), point.Value)
}

// TestLoadRDB_WithStreamData tests RDB round-trip for Stream data
func TestLoadRDB_WithStreamData(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	_, err := testStore.XAdd("mystream", store.StreamXAddOptions{}, "1000000000000-0", map[string]string{"field1": "value1", "field2": "value2"})
	assert.NoError(t, err)
	_, err = testStore.XAdd("mystream", store.StreamXAddOptions{}, "1000000000001-0", map[string]string{"field3": "value3"})
	assert.NoError(t, err)

	rdbData, err := GenerateRDB(testStore)
	assert.NoError(t, err)

	testStore2 := setupTestStore(t)
	defer testStore2.Close()

	err = LoadRDBWithStore(rdbData, testStore2)
	assert.NoError(t, err)

	entries, err := testStore2.XRange("mystream", "-", "+", 0)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(entries))
	assert.Equal(t, "1000000000000-0", entries[0].ID)
}

// TestLoadRDB_StreamWithConsumerGroups verifies type-15 RDB restores XGROUP + PEL.
func TestLoadRDB_StreamWithConsumerGroups(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	_, err := testStore.XAdd("sgstream", store.StreamXAddOptions{}, "2000000000000-0", map[string]string{"f": "v"})
	assert.NoError(t, err)
	assert.NoError(t, testStore.XGroupCreate("sgstream", "g1", "0"))

	// Deliver to create PEL (stream key only; ">" is RESP-level stream ID, not a store key)
	_, err = testStore.XReadGroup(nil, "g1", "c1", 10, 0, "sgstream")
	assert.NoError(t, err)

	groups, err := testStore.XInfoGroups("sgstream")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(groups))
	assert.Equal(t, "g1", groups[0].Name)

	pending, err := testStore.XPending("sgstream", "g1")
	assert.NoError(t, err)
	assert.True(t, len(pending) >= 1)

	rdbData, err := GenerateRDB(testStore)
	assert.NoError(t, err)

	testStore2 := setupTestStore(t)
	defer testStore2.Close()
	assert.NoError(t, LoadRDBWithStore(rdbData, testStore2))

	groups2, err := testStore2.XInfoGroups("sgstream")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(groups2))
	assert.Equal(t, "g1", groups2[0].Name)
	assert.Equal(t, groups[0].LastDeliveredID, groups2[0].LastDeliveredID)

	pending2, err := testStore2.XPending("sgstream", "g1")
	assert.NoError(t, err)
	assert.Equal(t, len(pending), len(pending2))

	entries, err := testStore2.XRange("sgstream", "-", "+", 0)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(entries))
}

// TestDecodeLatLonBits tests decodeLatLonBits function directly
func TestDecodeLatLonBits(t *testing.T) {
	t.Parallel()

	// All zeros → converges toward min
	result := store.DecodeLatLonBits(0, -90, 90, 26)
	if !(result > -90 && result < -89) {
		t.Errorf("expected near -90, got %f", result)
	}

	// All ones → converges toward max
	result = store.DecodeLatLonBits(0x3FFFFFF, -90, 90, 26)
	if !(result > 89 && result < 90) {
		t.Errorf("expected near 90, got %f", result)
	}

	// Single bit at position 25 (MSB of 26) → positive
	result = store.DecodeLatLonBits(0x2000000, -90, 90, 26)
	if !(result > 0) {
		t.Errorf("expected positive, got %f", result)
	}

	// 1-bit encoding: 0 → -90, 1 → 90
	result = store.DecodeLatLonBits(0, -180, 180, 1)
	assert.Equal(t, float64(-90), result)
	result = store.DecodeLatLonBits(1, -180, 180, 1)
	assert.Equal(t, float64(90), result)
}
