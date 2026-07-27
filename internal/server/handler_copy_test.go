package server

import (
	"testing"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/zeebo/assert"
)

// TestGetResponseType tests getResponseType function
func TestGetResponseType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		resp     proto.RESP
		expected string
	}{
		{
			name:     "SimpleString",
			resp:     proto.NewSimpleString("OK"),
			expected: "SimpleString",
		},
		{
			name:     "BulkString",
			resp:     proto.NewBulkString([]byte("value")),
			expected: "BulkString",
		},
		{
			name:     "Error",
			resp:     proto.Error("error message"),
			expected: "Error",
		},
		{
			name:     "Integer",
			resp:     proto.Integer(42),
			expected: "Integer",
		},
		{
			name:     "Array",
			resp:     &proto.Array{Args: [][]byte{[]byte("a"), []byte("b")}},
			expected: "Array",
		},
		{
			name:     "Nil",
			resp:     nil,
			expected: "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getResponseType(tt.resp)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestBoolToInt tests boolToInt function
func TestBoolToInt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    bool
		expected int
	}{
		{"true", true, 1},
		{"false", false, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := boolToInt(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestCopyList tests copyList function
func TestCopyList(t *testing.T) {
	t.Parallel()

	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()

	// Setup source list
	handler.Db.LPush("srclist", "value1")
	handler.Db.LPush("srclist", "value2")

	// Test copy
	result := handler.copyList("srclist", "dstlist")
	assert.True(t, result)

	// Verify copy
	length, _ := handler.Db.LLen("dstlist")
	assert.Equal(t, int64(2), length)
}

// TestCopyListEmpty tests copyList with empty source
func TestCopyListEmpty(t *testing.T) {
	t.Parallel()

	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()

	// Test copy empty list
	result := handler.copyList("emptysrc", "emptydst")
	assert.True(t, result)
}

// TestCopyHash tests copyHash function
func TestCopyHash(t *testing.T) {
	t.Parallel()

	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()

	// Setup source hash
	handler.Db.HSet("srchash", "field1", "value1")
	handler.Db.HSet("srchash", "field2", "value2")

	// Test copy
	result := handler.copyHash("srchash", "dsthash")
	assert.True(t, result)

	// Verify copy
	length, _ := handler.Db.HLen("dsthash")
	assert.Equal(t, int64(2), length)
}

// TestCopyHashEmpty tests copyHash with empty source
func TestCopyHashEmpty(t *testing.T) {
	t.Parallel()

	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()

	// Test copy empty hash
	result := handler.copyHash("emptysrc", "emptydst")
	assert.True(t, result)
}

// TestCopySet tests copySet function
func TestCopySet(t *testing.T) {
	t.Parallel()

	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()

	// Setup source set
	handler.Db.SAdd("srcset", "member1")
	handler.Db.SAdd("srcset", "member2")

	// Test copy
	result := handler.copySet("srcset", "dstset")
	assert.True(t, result)

	// Verify copy
	card, _ := handler.Db.SCard("dstset")
	assert.Equal(t, int64(2), card)
}

// TestCopySetEmpty tests copySet with empty source
func TestCopySetEmpty(t *testing.T) {
	t.Parallel()

	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()

	// Test copy empty set
	result := handler.copySet("emptysrc", "emptydst")
	assert.True(t, result)
}

// TestCopySortedSet tests copySortedSet function
func TestCopySortedSet(t *testing.T) {
	t.Parallel()

	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()

	// Setup source sorted set
	handler.Db.ZAdd("srczset", []store.ZSetMember{{Member: "member1", Score: 1.0}, {Member: "member2", Score: 2.0}})

	// Test copy
	result := handler.copySortedSet("srczset", "dstzset")
	assert.True(t, result)

	// Verify copy
	card, _ := handler.Db.ZCard("dstzset")
	assert.Equal(t, int64(2), card)
}

// TestCopySortedSetEmpty tests copySortedSet with empty source
func TestCopySortedSetEmpty(t *testing.T) {
	t.Parallel()

	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()

	// Test copy empty sorted set
	result := handler.copySortedSet("emptysrc", "emptydst")
	assert.True(t, result)
}
