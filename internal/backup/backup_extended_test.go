package backup

import (
	"context"
	"testing"

	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/zeebo/assert"
)

// TestRDBBackupManager_New tests RDBBackupManager creation
func TestRDBBackupManager_New(t *testing.T) {
	t.Parallel()
	dbPath := t.TempDir()
	db, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	rbm := NewRDBBackupManager(db)
	assert.True(t, rbm != nil)
	assert.True(t, rbm.store != nil)
}

// TestListRDBBackups tests ListRDBBackups function
func TestListRDBBackups(t *testing.T) {
	t.Parallel()
	// Test with empty directory
	emptyDir := t.TempDir()
	backups, err := ListRDBBackups(emptyDir)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(backups))
}

// TestRestoreManager_BadgerRestore tests RestoreFromBadger with non-existent file
func TestRestoreManager_BadgerRestore(t *testing.T) {
	t.Parallel()
	dbPath := t.TempDir()
	db, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	rm := NewRestoreManager(db)
	// Non-existent file should return error
	err = rm.RestoreFromBadger("/non/existent/path")
	assert.Error(t, err)
}

// TestRestoreManager_RDBRestore tests RestoreFromRDB with non-existent file
func TestRestoreManager_RDBRestore(t *testing.T) {
	t.Parallel()
	dbPath := t.TempDir()
	db, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	rm := NewRestoreManager(db)
	// Non-existent file should return error
	err = rm.RestoreFromRDB("/non/existent/path")
	assert.Error(t, err)
}

// TestRestoreManager_PathRestore tests RestoreFromPath with non-existent path
func TestRestoreManager_PathRestore(t *testing.T) {
	t.Parallel()
	dbPath := t.TempDir()
	db, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	rm := NewRestoreManager(db)
	// Non-existent path should return error
	err = rm.RestoreFromPath("/non/existent/path")
	assert.Error(t, err)
}

// TestBackupManager_BGSave tests BGSave
func TestBackupManager_BGSave(t *testing.T) {
	t.Parallel()
	dbPath := t.TempDir()
	db, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	bm := NewBackupManager(db, t.TempDir())

	// Set some data
	db.Set("key1", "value1")

	err = bm.BGSave(context.Background())
	assert.NoError(t, err)
	bm.Wait()
}

// TestBackupManager_BackupBadger tests BackupBadger
func TestBackupManager_BackupBadger(t *testing.T) {
	t.Parallel()
	dbPath := t.TempDir()
	db, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	bm := NewBackupManager(db, t.TempDir())

	// Set some data
	db.Set("key1", "value1")

	_, err = bm.BackupBadger()
	assert.NoError(t, err)
}

// TestBackupManager_BackupRDB tests BackupRDB
func TestBackupManager_BackupRDB(t *testing.T) {
	t.Parallel()
	dbPath := t.TempDir()
	db, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	bm := NewBackupManager(db, t.TempDir())

	// Set some data
	db.Set("key1", "value1")

	_, err = bm.BackupRDB()
	assert.NoError(t, err)
}

// TestBackupManager_BackupBoth tests BackupBoth
func TestBackupManager_BackupBoth(t *testing.T) {
	t.Parallel()
	dbPath := t.TempDir()
	db, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	bm := NewBackupManager(db, t.TempDir())

	// Set some data
	db.Set("key1", "value1")

	_, err = bm.BackupBoth()
	assert.NoError(t, err)
}

// TestRDBBackupManager_Backup tests RDBBackupManager Backup
func TestRDBBackupManager_Backup(t *testing.T) {
	t.Parallel()
	dbPath := t.TempDir()
	db, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	rbm := NewRDBBackupManager(db)

	// Set some data
	db.Set("key1", "value1")

	_, err = rbm.Backup(t.TempDir())
	assert.NoError(t, err)
}

// TestRDBBackupManager_BackupWithCompression tests BackupWithCompression
func TestRDBBackupManager_BackupWithCompression(t *testing.T) {
	t.Parallel()
	dbPath := t.TempDir()
	db, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	rbm := NewRDBBackupManager(db)

	// Set some data
	db.Set("key1", "value1")

	_, err = rbm.BackupWithCompression(t.TempDir())
	assert.NoError(t, err)
}

// TestRDBBackupManager_GetBackupInfo tests GetBackupInfo
func TestRDBBackupManager_GetBackupInfo(t *testing.T) {
	t.Parallel()
	dbPath := t.TempDir()
	db, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	rbm := NewRDBBackupManager(db)

	// GetBackupInfo for non-existent file
	_, err = rbm.GetBackupInfo("/non/existent/file")
	assert.Error(t, err)
}
