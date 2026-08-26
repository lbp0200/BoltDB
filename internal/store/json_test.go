package store

import (
	"testing"
)

func TestJSONSet(t *testing.T) {
	t.Parallel()
	db := setupTestStore(t)
	var err error

	// Test JSON.SET with simple object
	result, _, err := db.JSONSet("user:1", "$", `{"name":"John","age":30}`, false, false)
	if err != nil {
		t.Fatalf("JSON.SET failed: %v", err)
	}
	if result != "OK" {
		t.Errorf("Expected 'OK', got '%s'", result)
	}

	// Verify type
	keyType, err := db.Type("user:1")
	if err != nil {
		t.Fatalf("TYPE failed: %v", err)
	}
	if keyType != "json" {
		t.Errorf("Expected type 'json', got '%s'", keyType)
	}

	// Test JSON.SET with NX (should not update existing key)
	result, _, err = db.JSONSet("user:1", "$", `{"name":"Jane"}`, true, false)
	if err != nil {
		t.Fatalf("JSON.SET NX failed: %v", err)
	}
	if result != "OK" {
		t.Errorf("Expected 'OK', got '%s'", result)
	}

	// Test JSON.SET with XX (should update existing key)
	result, _, err = db.JSONSet("user:1", "$", `{"name":"Jane","city":"NYC"}`, false, true)
	if err != nil {
		t.Fatalf("JSON.SET XX failed: %v", err)
	}
	if result != "OK" {
		t.Errorf("Expected 'OK', got '%s'", result)
	}
}

func TestJSONGet(t *testing.T) {
	t.Parallel()
	db := setupTestStore(t)
	var err error

	// Set up test data
	_, _, err = db.JSONSet("user:1", "$", `{"name":"John","age":30}`, false, false)
	if err != nil {
		t.Fatalf("JSON.SET failed: %v", err)
	}

	// Test JSON.GET
	result, err := db.JSONGet("user:1")
	if err != nil {
		t.Fatalf("JSON.GET failed: %v", err)
	}
	if len(result) != 1 {
		t.Errorf("Expected 1 result, got %d", len(result))
	}
}

func TestJSONDel(t *testing.T) {
	t.Parallel()
	db := setupTestStore(t)
	var err error

	// Set up test data
	_, _, err = db.JSONSet("user:1", "$", `{"name":"John","age":30}`, false, false)
	if err != nil {
		t.Fatalf("JSON.SET failed: %v", err)
	}

	// Test JSON.DEL
	count, err := db.JSONDel("user:1")
	if err != nil {
		t.Fatalf("JSON.DEL failed: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 deleted, got %d", count)
	}

	// Verify key is deleted
	exists, err := db.Exists("user:1")
	if err != nil {
		t.Fatalf("EXISTS failed: %v", err)
	}
	if exists {
		t.Errorf("Key should be deleted")
	}
}

func TestJSONType(t *testing.T) {
	t.Parallel()
	db := setupTestStore(t)
	var err error

	// Set up test data
	_, _, err = db.JSONSet("user:1", "$", `{"name":"John","age":30}`, false, false)
	if err != nil {
		t.Fatalf("JSON.SET failed: %v", err)
	}

	// Test JSON.TYPE
	result, err := db.JSONType("user:1", "$")
	if err != nil {
		t.Fatalf("JSON.TYPE failed: %v", err)
	}
	if result != "object" {
		t.Errorf("Expected 'object', got '%s'", result)
	}
}

func TestJSONArrAppend(t *testing.T) {
	t.Parallel()
	db := setupTestStore(t)
	var err error

	// Set up array
	_, _, err = db.JSONSet("arr", "$", `[]`, false, false)
	if err != nil {
		t.Fatalf("JSON.SET failed: %v", err)
	}

	// Test JSON.ARRAPPEND
	count, err := db.JSONArrAppend("arr", "$", `"1"`, `"2"`, `"3"`)
	if err != nil {
		t.Fatalf("JSON.ARRAPPEND failed: %v", err)
	}
	if count != 3 {
		t.Errorf("Expected 3, got %d", count)
	}

	// Verify array length
	length, err := db.JSONArrLen("arr", "$")
	if err != nil {
		t.Fatalf("JSON.ARRLEN failed: %v", err)
	}
	if length != 3 {
		t.Errorf("Expected 3, got %d", length)
	}
}

func TestJSONObjKeys(t *testing.T) {
	t.Parallel()
	db := setupTestStore(t)
	var err error

	// Set up object
	_, _, err = db.JSONSet("user:1", "$", `{"name":"John","age":30,"city":"NYC"}`, false, false)
	if err != nil {
		t.Fatalf("JSON.SET failed: %v", err)
	}

	// Test JSON.OBJKEYS
	keys, err := db.JSONObjKeys("user:1", "$")
	if err != nil {
		t.Fatalf("JSON.OBJKEYS failed: %v", err)
	}
	if len(keys) != 3 {
		t.Errorf("Expected 3 keys, got %d", len(keys))
	}
}

func TestJSONNumIncrBy(t *testing.T) {
	t.Parallel()
	db := setupTestStore(t)
	var err error

	// Set up number
	_, _, err = db.JSONSet("counter", "$", `10`, false, false)
	if err != nil {
		t.Fatalf("JSON.SET failed: %v", err)
	}

	// Test JSON.NUMINCRBY
	result, err := db.JSONNumIncrBy("counter", "$", 5)
	if err != nil {
		t.Fatalf("JSON.NUMINCRBY failed: %v", err)
	}
	if result != 15 {
		t.Errorf("Expected 15, got %f", result)
	}
}

func TestJSONNumMultBy(t *testing.T) {
	t.Parallel()
	db := setupTestStore(t)
	var err error

	// Set up number
	_, _, err = db.JSONSet("counter", "$", `10`, false, false)
	if err != nil {
		t.Fatalf("JSON.SET failed: %v", err)
	}

	// Test JSON.NUMMULTBY
	result, err := db.JSONNumMultBy("counter", "$", 2)
	if err != nil {
		t.Fatalf("JSON.NUMMULTBY failed: %v", err)
	}
	if result != 20 {
		t.Errorf("Expected 20, got %f", result)
	}
}

func TestJSONClear(t *testing.T) {
	t.Parallel()
	db := setupTestStore(t)
	var err error

	// Set up object
	_, _, err = db.JSONSet("user:1", "$", `{"name":"John","age":30}`, false, false)
	if err != nil {
		t.Fatalf("JSON.SET failed: %v", err)
	}

	// Test JSON.CLEAR
	count, err := db.JSONClear("user:1", "$")
	if err != nil {
		t.Fatalf("JSON.CLEAR failed: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1, got %d", count)
	}
}

func TestJSONDebugMemory(t *testing.T) {
	t.Parallel()
	db := setupTestStore(t)
	var err error

	// Set up object
	_, _, err = db.JSONSet("user:1", "$", `{"name":"John"}`, false, false)
	if err != nil {
		t.Fatalf("JSON.SET failed: %v", err)
	}

	// Test JSON.DEBUG MEMORY with root path
	memory, err := db.JSONDebugMemory("user:1", "$")
	if err != nil {
		t.Fatalf("JSON.DEBUG MEMORY failed: %v", err)
	}
	if memory <= 0 {
		t.Errorf("Expected positive memory, got %d", memory)
	}
}

func TestJSONDebugMemoryPath(t *testing.T) {
	t.Parallel()
	db := setupTestStore(t)

	// Set up nested object
	_, _, err := db.JSONSet("user:2", "$", `{"name":"Alice","age":25,"address":{"city":"NYC","zip":"10001"}}`, false, false)
	if err != nil {
		t.Fatalf("JSON.SET failed: %v", err)
	}

	// Test with sub-path - name field
	nameMemory, err := db.JSONDebugMemory("user:2", "$.name")
	if err != nil {
		t.Fatalf("JSON.DEBUG MEMORY with path failed: %v", err)
	}
	if nameMemory <= 0 {
		t.Errorf("Expected positive memory for name field, got %d", nameMemory)
	}

	// name is a string "Alice" (7 bytes including quotes), should be smaller than full object
	fullMemory, err := db.JSONDebugMemory("user:2", "$")
	if err != nil {
		t.Fatalf("JSON.DEBUG MEMORY full failed: %v", err)
	}
	if nameMemory >= fullMemory {
		t.Errorf("Expected name field memory (%d) < full memory (%d)", nameMemory, fullMemory)
	}

	// Non-existent path should return error
	_, err = db.JSONDebugMemory("user:2", "$.nonexistent")
	if err == nil {
		t.Error("Expected error for non-existent path")
	}
}

func TestJSONMGet(t *testing.T) {
	t.Parallel()
	db := setupTestStore(t)
	var err error

	// Set up test data
	_, _, err = db.JSONSet("user:1", "$", `{"name":"John"}`, false, false)
	if err != nil {
		t.Fatalf("JSON.SET failed: %v", err)
	}
	_, _, err = db.JSONSet("user:2", "$", `{"name":"Jane"}`, false, false)
	if err != nil {
		t.Fatalf("JSON.SET failed: %v", err)
	}

	// Test JSON.MGET
	result, err := db.JSONMGet("$", "user:1", "user:2")
	if err != nil {
		t.Fatalf("JSON.MGET failed: %v", err)
	}
	if len(result) != 2 {
		t.Errorf("Expected 2 results, got %d", len(result))
	}
}

// Test for WRONGTYPE error
func TestJSONSetWrongType(t *testing.T) {
	t.Parallel()
	db := setupTestStore(t)
	var err error

	// Set a string value first
	err = db.Set("mykey", "value")
	if err != nil {
		t.Fatalf("SET failed: %v", err)
	}

	// Verify the type is "string"
	keyType, err := db.Type("mykey")
	if err != nil {
		t.Fatalf("TYPE failed: %v", err)
	}
	if keyType != "string" {
		t.Errorf("Expected type 'string', got '%s'", keyType)
	}

	// Try to set JSON on the same key - should return ErrWrongType
	_, _, err = db.JSONSet("mykey", "$", `{"name":"John"}`, false, false)
	if err != ErrWrongType {
		t.Errorf("Expected ErrWrongType, got %v", err)
	}
}

// Test for key not found scenarios
func TestJSONKeyNotFound(t *testing.T) {
	t.Parallel()
	db := setupTestStore(t)
	var err error

	// Test JSON.GET on non-existent key
	_, err = db.JSONGet("nonexistent")
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound, got %v", err)
	}

	// Test JSON.DEL on non-existent key
	count, err := db.JSONDel("nonexistent")
	if err != nil {
		t.Errorf("Expected nil error, got %v", err)
	}
	if count != 0 {
		t.Errorf("Expected 0 deleted, got %d", count)
	}

	// Test JSON.TYPE on non-existent key
	_, err = db.JSONType("nonexistent", "$")
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound, got %v", err)
	}
}

// Benchmark for JSON operations
func BenchmarkJSONSet(b *testing.B) {
	dir := b.TempDir()
	db, err := NewBotreonStore(dir)
	if err != nil {
		b.Fatalf("Failed to create store: %v", err)
	}
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = db.JSONSet("key", "$", `{"name":"John","age":30,"city":"NYC"}`, false, false)
	}
}

func BenchmarkJSONGet(b *testing.B) {
	dir := b.TempDir()
	db, err := NewBotreonStore(dir)
	if err != nil {
		b.Fatalf("Failed to create store: %v", err)
	}
	defer db.Close()

	_, _, _ = db.JSONSet("key", "$", `{"name":"John","age":30,"city":"NYC"}`, false, false)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = db.JSONGet("key")
	}
}
