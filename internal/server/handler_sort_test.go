package server

import (
	"strings"
	"testing"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/zeebo/assert"
)

// TestSORTOrdering_ListASC verifies SORT on list returns ascending numeric order.
func TestSORTOrdering_ListASC(t *testing.T) {
		t.Parallel()
		handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "LPUSH", [][]byte{[]byte("lst"), []byte("9"), []byte("3"), []byte("7"), []byte("1"), []byte("5")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "SORT", [][]byte{[]byte("lst")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	got := make([]string, len(arr.Args))
	for i, a := range arr.Args {
		got[i] = string(a)
	}
	assert.Equal(t, []string{"1", "3", "5", "7", "9"}, got)
}

// TestSORTOrdering_ListDESC verifies SORT DESC returns descending numeric order.
func TestSORTOrdering_ListDESC(t *testing.T) {
		t.Parallel()
		handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "LPUSH", [][]byte{[]byte("lst"), []byte("9"), []byte("3"), []byte("7"), []byte("1"), []byte("5")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "SORT", [][]byte{[]byte("lst"), []byte("DESC")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	got := make([]string, len(arr.Args))
	for i, a := range arr.Args {
		got[i] = string(a)
	}
	assert.Equal(t, []string{"9", "7", "5", "3", "1"}, got)
}

// TestSORTOrdering_Alpha verifies SORT ALPHA returns lexicographic order.
func TestSORTOrdering_Alpha(t *testing.T) {
		t.Parallel()
		handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "LPUSH", [][]byte{[]byte("lst"), []byte("banana"), []byte("apple"), []byte("cherry"), []byte("date")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "SORT", [][]byte{[]byte("lst"), []byte("ALPHA")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	got := make([]string, len(arr.Args))
	for i, a := range arr.Args {
		got[i] = string(a)
	}
	assert.Equal(t, []string{"apple", "banana", "cherry", "date"}, got)
}

// TestSORTOrdering_AlphaDESC verifies SORT ALPHA DESC returns reverse lexicographic order.
func TestSORTOrdering_AlphaDESC(t *testing.T) {
		t.Parallel()
		handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "LPUSH", [][]byte{[]byte("lst"), []byte("banana"), []byte("apple"), []byte("cherry"), []byte("date")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "SORT", [][]byte{[]byte("lst"), []byte("ALPHA"), []byte("DESC")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	got := make([]string, len(arr.Args))
	for i, a := range arr.Args {
		got[i] = string(a)
	}
	assert.Equal(t, []string{"date", "cherry", "banana", "apple"}, got)
}

// TestSORTOrdering_SingleElement verifies SORT on a single element returns it.
func TestSORTOrdering_SingleElement(t *testing.T) {
		t.Parallel()
		handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "LPUSH", [][]byte{[]byte("lst"), []byte("42")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "SORT", [][]byte{[]byte("lst")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 1, len(arr.Args))
	assert.Equal(t, "42", string(arr.Args[0]))
}

// TestSORTOrdering_EmptyList verifies SORT on non-existent key returns error.
func TestSORTOrdering_EmptyList(t *testing.T) {
		t.Parallel()
		handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "SORT", [][]byte{[]byte("nonexistent")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "ERR"))
}

// TestSORTOrdering_StringKey verifies SORT on string key returns the single value.
func TestSORTOrdering_StringKey(t *testing.T) {
		t.Parallel()
		handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("strkey"), []byte("hello")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "SORT", [][]byte{[]byte("strkey")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 1, len(arr.Args))
	assert.Equal(t, "hello", string(arr.Args[0]))
}

// TestSORTOrdering_SetKey verifies SORT on set returns sorted members.
func TestSORTOrdering_SetKey(t *testing.T) {
		t.Parallel()
		handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SADD", [][]byte{[]byte("myset"), []byte("z"), []byte("a"), []byte("m")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "SORT", [][]byte{[]byte("myset"), []byte("ALPHA")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	got := make([]string, len(arr.Args))
	for i, a := range arr.Args {
		got[i] = string(a)
	}
	assert.Equal(t, []string{"a", "m", "z"}, got)
}

// TestSORTOrdering_ZSetByScore verifies SORT on zset sorts by score (numeric).
func TestSORTOrdering_ZSetByScore(t *testing.T) {
		t.Parallel()
		handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("myzset"), []byte("3.5"), []byte("alpha"), []byte("1.0"), []byte("gamma"), []byte("2.0"), []byte("beta")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "SORT", [][]byte{[]byte("myzset")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	got := make([]string, len(arr.Args))
	for i, a := range arr.Args {
		got[i] = string(a)
	}
	// SORT on zset returns member names ordered by score ascending
	assert.Equal(t, []string{"gamma", "beta", "alpha"}, got)
}

// TestSORTOrdering_ZSetByScoreDESC verifies SORT DESC on zset sorts by score descending.
func TestSORTOrdering_ZSetByScoreDESC(t *testing.T) {
		t.Parallel()
		handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("myzset"), []byte("3.5"), []byte("alpha"), []byte("1.0"), []byte("gamma"), []byte("2.0"), []byte("beta")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "SORT", [][]byte{[]byte("myzset"), []byte("DESC")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	got := make([]string, len(arr.Args))
	for i, a := range arr.Args {
		got[i] = string(a)
	}
	assert.Equal(t, []string{"alpha", "beta", "gamma"}, got)
}

// TestSORTOrdering_DuplicateValues verifies duplicate values are all present and sorted.
func TestSORTOrdering_DuplicateValues(t *testing.T) {
		t.Parallel()
		handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "LPUSH", [][]byte{[]byte("lst"), []byte("5"), []byte("3"), []byte("5"), []byte("1"), []byte("3")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "SORT", [][]byte{[]byte("lst")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	got := make([]string, len(arr.Args))
	for i, a := range arr.Args {
		got[i] = string(a)
	}
	assert.Equal(t, []string{"1", "3", "3", "5", "5"}, got)
}

// TestSORTOrdering_AlphaDuplicateValues verifies ALPHA sort with duplicates.
func TestSORTOrdering_AlphaDuplicateValues(t *testing.T) {
		t.Parallel()
		handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "LPUSH", [][]byte{[]byte("lst"), []byte("c"), []byte("a"), []byte("b"), []byte("a"), []byte("c")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "SORT", [][]byte{[]byte("lst"), []byte("ALPHA")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	got := make([]string, len(arr.Args))
	for i, a := range arr.Args {
		got[i] = string(a)
	}
	assert.Equal(t, []string{"a", "a", "b", "c", "c"}, got)
}

// TestSORTOrdering_Limit verifies SORT LIMIT correctly slices sorted output.
func TestSORTOrdering_Limit(t *testing.T) {
		t.Parallel()
		handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "LPUSH", [][]byte{[]byte("lst"), []byte("9"), []byte("3"), []byte("7"), []byte("1"), []byte("5")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "SORT", [][]byte{[]byte("lst"), []byte("LIMIT"), []byte("1"), []byte("3")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	got := make([]string, len(arr.Args))
	for i, a := range arr.Args {
		got[i] = string(a)
	}
	// sorted: [1,3,5,7,9], LIMIT 1 3 → [3,5,7]
	assert.Equal(t, []string{"3", "5", "7"}, got)
}

// TestSORTOrdering_BY verifies SORT BY sorts using external key values.
func TestSORTOrdering_BY(t *testing.T) {
		t.Parallel()
		handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "LPUSH", [][]byte{[]byte("lst"), []byte("a"), []byte("b"), []byte("c")}, "127.0.0.1:12345")
	handler.executeCommand(state, "SET", [][]byte{[]byte("weight_a"), []byte("30")}, "127.0.0.1:12345")
	handler.executeCommand(state, "SET", [][]byte{[]byte("weight_b"), []byte("10")}, "127.0.0.1:12345")
	handler.executeCommand(state, "SET", [][]byte{[]byte("weight_c"), []byte("20")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "SORT", [][]byte{[]byte("lst"), []byte("BY"), []byte("weight_*")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	got := make([]string, len(arr.Args))
	for i, a := range arr.Args {
		got[i] = string(a)
	}
	// sorted by weight: b(10), c(20), a(30)
	assert.Equal(t, []string{"b", "c", "a"}, got)
}

// TestSORTOrdering_BYDESC verifies SORT BY DESC reverses external key sort.
func TestSORTOrdering_BYDESC(t *testing.T) {
		t.Parallel()
		handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "LPUSH", [][]byte{[]byte("lst"), []byte("a"), []byte("b"), []byte("c")}, "127.0.0.1:12345")
	handler.executeCommand(state, "SET", [][]byte{[]byte("weight_a"), []byte("30")}, "127.0.0.1:12345")
	handler.executeCommand(state, "SET", [][]byte{[]byte("weight_b"), []byte("10")}, "127.0.0.1:12345")
	handler.executeCommand(state, "SET", [][]byte{[]byte("weight_c"), []byte("20")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "SORT", [][]byte{[]byte("lst"), []byte("BY"), []byte("weight_*"), []byte("DESC")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	got := make([]string, len(arr.Args))
	for i, a := range arr.Args {
		got[i] = string(a)
	}
	// sorted by weight DESC: a(30), c(20), b(10)
	assert.Equal(t, []string{"a", "c", "b"}, got)
}

// TestSORTOrdering_GET verifies SORT GET retrieves external keys after sort.
func TestSORTOrdering_GET(t *testing.T) {
		t.Parallel()
		handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "LPUSH", [][]byte{[]byte("lst"), []byte("a"), []byte("b")}, "127.0.0.1:12345")
	handler.executeCommand(state, "SET", [][]byte{[]byte("obj_a"), []byte("apple")}, "127.0.0.1:12345")
	handler.executeCommand(state, "SET", [][]byte{[]byte("obj_b"), []byte("banana")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "SORT", [][]byte{[]byte("lst"), []byte("ALPHA"), []byte("GET"), []byte("obj_*")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	got := make([]string, len(arr.Args))
	for i, a := range arr.Args {
		got[i] = string(a)
	}
	// sorted by alpha: a, b → GET obj_a, obj_b
	assert.Equal(t, []string{"apple", "banana"}, got)
}

// TestSORTOrdering_Store verifies SORT STORE writes sorted result as a list and returns count.
func TestSORTOrdering_Store(t *testing.T) {
		t.Parallel()
		handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "LPUSH", [][]byte{[]byte("lst"), []byte("9"), []byte("3"), []byte("7"), []byte("1"), []byte("5")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "SORT", [][]byte{[]byte("lst"), []byte("STORE"), []byte("dst")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(5), int64(*integer))

	// Verify stored list order
	resp2 := handler.executeCommand(state, "LRANGE", [][]byte{[]byte("dst"), []byte("0"), []byte("-1")}, "127.0.0.1:12345")
	arr, ok := resp2.(*proto.Array)
	assert.True(t, ok)
	got := make([]string, len(arr.Args))
	for i, a := range arr.Args {
		got[i] = string(a)
	}
	assert.Equal(t, []string{"1", "3", "5", "7", "9"}, got)
}

// TestSORTOrdering_WrongType verifies SORT on hash key returns error.
func TestSORTOrdering_WrongType(t *testing.T) {
		t.Parallel()
		handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "HSET", [][]byte{[]byte("h"), []byte("f1"), []byte("v1")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "SORT", [][]byte{[]byte("h")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "ERR"))
}

// TestSORTOrdering_BYWithNonExistentKey verifies SORT BY with non-existent weight keys
// returns an error (the Get for the weight key fails).
func TestSORTOrdering_BYWithNonExistentKey(t *testing.T) {
		t.Parallel()
		handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "LPUSH", [][]byte{[]byte("lst"), []byte("x"), []byte("y")}, "127.0.0.1:12345")
	// weight_x and weight_y do not exist; Get returns ErrKeyNotFound.
	resp := handler.executeCommand(state, "SORT", [][]byte{[]byte("lst"), []byte("BY"), []byte("nonexist_*")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "ERR"))
}
