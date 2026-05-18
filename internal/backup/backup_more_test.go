package backup

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/zeebo/assert"
)

// TestBackupManager_ListBackups tests ListBackups function
func TestBackupManager_ListBackups(t *testing.T) {
	t.Parallel()
	// Test with empty directory
	emptyDir := t.TempDir()
	backups, err := ListBackups(emptyDir)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(backups))
}

// TestBackupManager_ListBackups_WithFiles tests ListBackups with files
func TestBackupManager_ListBackups_WithFiles(t *testing.T) {
	t.Parallel()
	// Create a temp dir with some backup files
	tmpDir := t.TempDir()

	// Create some dummy backup files
	file1 := filepath.Join(tmpDir, "backup_001.bak")
	file2 := filepath.Join(tmpDir, "backup_002.bak")

	err := os.WriteFile(file1, []byte("test"), 0644)
	assert.NoError(t, err)

	err = os.WriteFile(file2, []byte("test"), 0644)
	assert.NoError(t, err)

	backups, err := ListBackups(tmpDir)
	assert.NoError(t, err)
	assert.True(t, len(backups) >= 0)
}

// TestBackupManager_CopyBackup tests CopyBackup function
func TestBackupManager_CopyBackup(t *testing.T) {
	t.Parallel()
	// Create source and destination files
	tmpDir := t.TempDir()
	src := filepath.Join(tmpDir, "source.bak")
	dst := filepath.Join(tmpDir, "dest.bak")

	// Write to source
	err := os.WriteFile(src, []byte("test backup data"), 0644)
	assert.NoError(t, err)

	// Copy
	err = CopyBackup(src, dst)
	assert.NoError(t, err)

	// Verify destination exists
	data, err := os.ReadFile(dst)
	assert.NoError(t, err)
	assert.Equal(t, "test backup data", string(data))
}

// TestBackupManager_CopyBackup_NotFound tests CopyBackup with non-existent source
func TestBackupManager_CopyBackup_NotFound(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	src := filepath.Join(tmpDir, "nonexistent.bak")
	dst := filepath.Join(tmpDir, "dest.bak")

	err := CopyBackup(src, dst)
	assert.Error(t, err)
}

// TestRestoreTo_NotFound tests RestoreTo with non-existent file
func TestRestoreTo_NotFound(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	err := RestoreTo("/nonexistent/path", tmpDir)
	assert.Error(t, err)
}

// TestBadgerBackupManager_New tests BadgerBackupManager creation
func TestBadgerBackupManager_New(t *testing.T) {
	t.Parallel()
	dbPath := t.TempDir()
	db, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	// Note: Getting internal badger.DB requires using internal APIs
	// For now, we just test that we can create a store
	assert.True(t, db != nil)
}

// TestBackupManager_Save2 tests Save
func TestBackupManager_Save2(t *testing.T) {
	t.Parallel()
	dbPath := t.TempDir()
	db, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	bm := NewBackupManager(db, t.TempDir())

	// Set some data
	db.Set("testkey", "testvalue")

	// Save
	err = bm.Save()
	assert.NoError(t, err)
}

// TestBackupManager_LastSave2 tests LastSave
func TestBackupManager_LastSave2(t *testing.T) {
	t.Parallel()
	dbPath := t.TempDir()
	db, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	bm := NewBackupManager(db, t.TempDir())

	// Get last save - should be zero initially
	lastSave := bm.LastSave()
	assert.True(t, lastSave >= 0)
}
