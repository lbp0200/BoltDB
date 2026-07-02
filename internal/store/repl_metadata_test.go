package store

import (
	"testing"

	"github.com/zeebo/assert"
)

// TestReplMetadataPersistence verifies that replId and masterReplOffset
// survive store close+reopen (F1a/F1b persistence via BadgerDB).
func TestReplMetadataPersistence(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Phase 1: create store, save metadata, close
	s1, err := NewBadgerStore(dir)
	assert.NoError(t, err)

	err = s1.SaveReplID("test-repl-id-12345")
	assert.NoError(t, err)

	err = s1.SaveMasterReplOffset(98765)
	assert.NoError(t, err)

	err = s1.Close()
	assert.NoError(t, err)

	// Phase 2: reopen same directory, verify metadata persists
	s2, err := NewBadgerStore(dir)
	assert.NoError(t, err)
	defer s2.Close()

	loadedID, err := s2.LoadReplID()
	assert.NoError(t, err)
	assert.Equal(t, "test-repl-id-12345", loadedID)

	loadedOffset, err := s2.LoadMasterReplOffset()
	assert.NoError(t, err)
	assert.Equal(t, int64(98765), loadedOffset)
}

// TestReplMetadataPersistence_EmptyStore verifies that LoadReplID and
// LoadMasterReplOffset return zero values on a fresh store (no prior save).
func TestReplMetadataPersistence_EmptyStore(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	defer s.Close()

	id, err := s.LoadReplID()
	assert.NoError(t, err)
	assert.Equal(t, "", id)

	offset, err := s.LoadMasterReplOffset()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), offset)
}
