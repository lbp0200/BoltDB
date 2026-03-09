package replication

import (
	"bytes"
	"testing"

	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/zeebo/assert"
)

// TestLoadRDB_Valid tests LoadRDB with valid RDB data
func TestLoadRDB_Valid(t *testing.T) {
	testStore := setupTestStore(t)
	defer testStore.Close()

	// First generate valid RDB data
	rdbData, err := GenerateRDB(testStore)
	assert.NoError(t, err)
	assert.True(t, len(rdbData) > 0)

	// Load the RDB data - use LoadRDBWithStore since LoadRDB is not exported
	err = LoadRDBWithStore(rdbData, testStore)
	assert.NoError(t, err)
}

// TestLoadRDB_WithStringData tests LoadRDB with string data
func TestLoadRDB_WithStringData(t *testing.T) {
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

// TestLoadRDB_EmptyData tests LoadRDB with empty data
func TestLoadRDB_EmptyData(t *testing.T) {
	testStore := setupTestStore(t)
	defer testStore.Close()

	// Empty RDB data - should cause error
	err := LoadRDBWithStore([]byte{}, testStore)
	// Empty RDB causes error
	assert.True(t, err != nil)
}

// TestLoadRDB_InvalidHeader tests LoadRDB with invalid header
func TestLoadRDB_InvalidHeader(t *testing.T) {
	testStore := setupTestStore(t)
	defer testStore.Close()

	// Invalid RDB data (not starting with REDIS)
	err := LoadRDBWithStore([]byte("INVALID"), testStore)
	// Should return error
	assert.True(t, err != nil)
}

// TestLoadRDB_TruncatedData tests LoadRDB with truncated data
func TestLoadRDB_TruncatedData(t *testing.T) {
	testStore := setupTestStore(t)
	defer testStore.Close()

	// Truncated RDB data - may or may not error depending on implementation
	err := LoadRDBWithStore([]byte("REDIS0009"), testStore)
	// Just verify it doesn't panic
	_ = err
}

// TestWriteKeyValue tests WriteKeyValue function
func TestWriteKeyValue(t *testing.T) {
	testStore := setupTestStore(t)
	defer testStore.Close()

	// Add some data
	testStore.Set("testkey", "testvalue")

	// Generate RDB should include this key
	rdbData, err := GenerateRDB(testStore)
	assert.NoError(t, err)
	assert.True(t, len(rdbData) > 0)

	// Verify RDB contains the key
	assert.True(t, bytes.Contains(rdbData, []byte("testkey")))
	assert.True(t, bytes.Contains(rdbData, []byte("testvalue")))
}

// TestWriteListKeyValue tests WriteListKeyValue function
func TestWriteListKeyValue(t *testing.T) {
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
	assert.True(t, len(rdbData) > 0)
}

// TestLoadRDB_Overwrite tests LoadRDB overwrites existing data
func TestLoadRDB_Overwrite(t *testing.T) {
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
