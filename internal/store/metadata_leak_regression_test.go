package store

import (
	"fmt"
	"testing"

	"github.com/zeebo/assert"
)

// TestHGetAll_NoMetadataLeakage is a regression test for a bug where
// hGetAllFields returned __count__ metadata key as a field.
func TestHGetAll_NoMetadataLeakage(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := NewBotreonStore(dir)
	assert.NoError(t, err)
	defer store.Close()

	// Create a hash with known fields
	assert.NoError(t, store.HSet("myhash", "field1", "value1"))
	assert.NoError(t, store.HSet("myhash", "field2", "value2"))
	assert.NoError(t, store.HSet("myhash", "field3", "value3"))

	// HGetAll must return exactly 3 fields, no __count__ metadata
	all, err := store.HGetAll("myhash")
	assert.NoError(t, err)
	assert.Equal(t, 3, len(all))

	// Verify no metadata keys leaked
	for field := range all {
		assert.True(t, field != "__count__")
	}

	// Verify actual values
	assert.Equal(t, "value1", string(all["field1"]))
	assert.Equal(t, "value2", string(all["field2"]))
	assert.Equal(t, "value3", string(all["field3"]))
}

// TestHGetAll_EmptyHash tests HGetAll on an empty hash.
func TestHGetAll_EmptyHash(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := NewBotreonStore(dir)
	assert.NoError(t, err)
	defer store.Close()

	all, err := store.HGetAll("nonexistent")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(all))
}

// TestHGetAll_SingleField tests HGetAll with exactly one field.
func TestHGetAll_SingleField(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := NewBotreonStore(dir)
	assert.NoError(t, err)
	defer store.Close()

	assert.NoError(t, store.HSet("h", "only", "field"))
	all, err := store.HGetAll("h")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(all))
	assert.Equal(t, "field", string(all["only"]))
}

// TestHGetAll_DeleteAndReAdd tests HGetAll after deleting and re-adding fields.
func TestHGetAll_DeleteAndReAdd(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := NewBotreonStore(dir)
	assert.NoError(t, err)
	defer store.Close()

	assert.NoError(t, store.HSet("h", "f1", "v1"))
	assert.NoError(t, store.HSet("h", "f2", "v2"))

	deleted, err := store.HDel("h", "f1")
	assert.NoError(t, err)
	assert.Equal(t, 1, deleted)

	all, err := store.HGetAll("h")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(all))
	_, ok := all["f1"]
	assert.True(t, !ok)
	assert.Equal(t, "v2", string(all["f2"]))

	assert.NoError(t, store.HSet("h", "f1", "v1_new"))

	all2, err := store.HGetAll("h")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(all2))
	assert.Equal(t, "v1_new", string(all2["f1"]))
	assert.Equal(t, "v2", string(all2["f2"]))
}

// TestHGetAll_ManyFields tests HGetAll with many fields.
func TestHGetAll_ManyFields(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := NewBotreonStore(dir)
	assert.NoError(t, err)
	defer store.Close()

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("field_%03d", i)
		val := fmt.Sprintf("value_%03d", i)
		assert.NoError(t, store.HSet("big", key, val))
	}

	all, err := store.HGetAll("big")
	assert.NoError(t, err)
	assert.Equal(t, 100, len(all))

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("field_%03d", i)
		val := fmt.Sprintf("value_%03d", i)
		assert.Equal(t, val, string(all[key]))
	}
}

// TestHScan_NoMetadataLeakage verifies HSCAN doesn't return __count__ metadata.
func TestHScan_NoMetadataLeakage(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := NewBotreonStore(dir)
	assert.NoError(t, err)
	defer store.Close()

	assert.NoError(t, store.HSet("h", "f1", "v1"))
	assert.NoError(t, store.HSet("h", "f2", "v2"))

	result, err := store.HScan("h", 0, "*", 100)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(result.Fields))

	for field := range result.Fields {
		assert.True(t, field != "__count__")
	}
}

// TestZScan_NoMetadataLeakage verifies ZSCAN doesn't return metadata.
func TestZScan_NoMetadataLeakage(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := NewBotreonStore(dir)
	assert.NoError(t, err)
	defer store.Close()

	assert.NoError(t, store.ZAdd("z", []ZSetMember{{Member: "a", Score: 1.0}}))
	assert.NoError(t, store.ZAdd("z", []ZSetMember{{Member: "b", Score: 2.0}}))

	result, err := store.ZScan("z", 0, "*", 100)
	assert.NoError(t, err)
	// ZScan returns at least 2 members for {a, b}
	assert.True(t, len(result.Members) >= 2)

	for _, m := range result.Members {
		assert.True(t, m.Member != "__count__")
	}
}

// TestSScan_NoMetadataLeakage verifies SSCAN doesn't return metadata.
func TestSScan_NoMetadataLeakage(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := NewBotreonStore(dir)
	assert.NoError(t, err)
	defer store.Close()

	_, _ = store.SAdd("s", "a", "b", "c")

	result, err := store.SScan("s", 0, "*", 100)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(result.Members))

	for _, m := range result.Members {
		assert.True(t, m != "__count__" && m != "meta")
	}
}
