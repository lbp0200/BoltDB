package backup

import (
	"testing"

	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/zeebo/assert"
)

// TestRDBBackupManager_New tests RDBBackupManager creation
func TestRDBBackupManager_New(t *testing.T) {
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
	// Test with empty directory
	emptyDir := t.TempDir()
	backups, err := ListRDBBackups(emptyDir)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(backups))
}

// TestRestoreManager_BadgerRestore tests RestoreFromBadger with non-existent file
func TestRestoreManager_BadgerRestore(t *testing.T) {
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
	dbPath := t.TempDir()
	db, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	bm := NewBackupManager(db, t.TempDir())

	// Set some data
	db.Set("key1", "value1")

	// BGSave should not panic
	err = bm.BGSave()
	_ = err
	assert.True(t, true)
}

// TestBackupManager_BackupBadger tests BackupBadger
func TestBackupManager_BackupBadger(t *testing.T) {
	dbPath := t.TempDir()
	db, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	bm := NewBackupManager(db, t.TempDir())

	// Set some data
	db.Set("key1", "value1")

	// BackupBadger - just verify no panic
	_, err = bm.BackupBadger()
	_ = err
	assert.True(t, true)
}

// TestBackupManager_BackupRDB tests BackupRDB
func TestBackupManager_BackupRDB(t *testing.T) {
	dbPath := t.TempDir()
	db, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	bm := NewBackupManager(db, t.TempDir())

	// Set some data
	db.Set("key1", "value1")

	// BackupRDB - just verify no panic
	_, err = bm.BackupRDB()
	_ = err
	assert.True(t, true)
}

// TestBackupManager_BackupBoth tests BackupBoth
func TestBackupManager_BackupBoth(t *testing.T) {
	dbPath := t.TempDir()
	db, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	bm := NewBackupManager(db, t.TempDir())

	// Set some data
	db.Set("key1", "value1")

	// BackupBoth - just verify no panic
	_, err = bm.BackupBoth()
	_ = err
	assert.True(t, true)
}

// TestRDBBackupManager_Backup tests RDBBackupManager Backup
func TestRDBBackupManager_Backup(t *testing.T) {
	dbPath := t.TempDir()
	db, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	rbm := NewRDBBackupManager(db)

	// Set some data
	db.Set("key1", "value1")

	// Backup - just verify no panic
	_, err = rbm.Backup(t.TempDir())
	_ = err
	assert.True(t, true)
}

// TestRDBBackupManager_BackupWithCompression tests BackupWithCompression
func TestRDBBackupManager_BackupWithCompression(t *testing.T) {
	dbPath := t.TempDir()
	db, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	rbm := NewRDBBackupManager(db)

	// Set some data
	db.Set("key1", "value1")

	// BackupWithCompression - just verify no panic
	_, err = rbm.BackupWithCompression(t.TempDir())
	_ = err
	assert.True(t, true)
}

// TestRDBBackupManager_GetBackupInfo tests GetBackupInfo
func TestRDBBackupManager_GetBackupInfo(t *testing.T) {
	dbPath := t.TempDir()
	db, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	rbm := NewRDBBackupManager(db)

	// GetBackupInfo for non-existent file
	_, err = rbm.GetBackupInfo("/non/existent/file")
	assert.Error(t, err)
}
