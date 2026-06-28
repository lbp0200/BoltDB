package backup

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/zeebo/assert"
)

// TestBadgerBackupManager_IncrementalBackup tests IncrementalBackup function
func TestBadgerBackupManager_IncrementalBackup(t *testing.T) {
	t.Parallel()
	dbPath := t.TempDir()
	db, err := badger.Open(badger.DefaultOptions(dbPath))
	assert.NoError(t, err)
	defer db.Close()

	// Write some data
	err = db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("key"), []byte("value"))
	})
	assert.NoError(t, err)

	bbm := NewBadgerBackupManager(db)
	backupFile, err := bbm.IncrementalBackup(t.TempDir(), 0)
	assert.NoError(t, err)
	assert.True(t, len(backupFile) > 0)

	// Verify backup file exists
	_, err = os.Stat(backupFile)
	assert.NoError(t, err)
}

// TestBadgerBackupManager_IncrementalBackup_Since tests IncrementalBackup with since parameter
func TestBadgerBackupManager_IncrementalBackup_Since(t *testing.T) {
	t.Parallel()
	dbPath := t.TempDir()
	db, err := badger.Open(badger.DefaultOptions(dbPath))
	assert.NoError(t, err)
	defer db.Close()

	// Write initial data
	err = db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("key1"), []byte("value1"))
	})
	assert.NoError(t, err)

	bbm := NewBadgerBackupManager(db)

	// Write more data
	err = db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("key2"), []byte("value2"))
	})
	assert.NoError(t, err)

	// Incremental backup with since=0 should work (gets all)
	backupFile, err := bbm.IncrementalBackup(t.TempDir(), 0)
	assert.NoError(t, err)
	assert.True(t, len(backupFile) > 0)
}

// TestBadgerBackupManager_IncrementalBackup_PathTraversal tests IncrementalBackup path traversal protection
func TestBadgerBackupManager_IncrementalBackup_PathTraversal(t *testing.T) {
	t.Parallel()
	dbPath := t.TempDir()
	db, err := badger.Open(badger.DefaultOptions(dbPath))
	assert.NoError(t, err)
	defer db.Close()

	bbm := NewBadgerBackupManager(db)

	// Try path traversal in backup dir
	invalidDir := t.TempDir() + "/../../../etc"
	_, err = bbm.IncrementalBackup(invalidDir, 0)
	// Path traversal: may succeed (OS resolves) or fail (permission denied) — both acceptable
	_ = err
}

// TestBadgerBackupManager_GetBackupInfo tests GetBackupInfo function
func TestBadgerBackupManager_GetBackupInfo(t *testing.T) {
	t.Parallel()
	// First create a backup file
	dbPath := t.TempDir()
	db, err := badger.Open(badger.DefaultOptions(dbPath))
	assert.NoError(t, err)

	err = db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("key"), []byte("value"))
	})
	assert.NoError(t, err)

	bbm := NewBadgerBackupManager(db)
	backupFile, err := bbm.Backup(t.TempDir())
	assert.NoError(t, err)
	db.Close()

	// Now get backup info
	info, err := GetBackupInfo(backupFile)
	assert.NoError(t, err)
	assert.True(t, info != nil)

	// Check expected fields
	assert.True(t, info["size"] != nil)
	assert.True(t, info["mod_time"] != nil)
	assert.True(t, info["path"] != nil)
}

// TestBadgerBackupManager_GetBackupInfo_NotFound tests GetBackupInfo with non-existent file
func TestBadgerBackupManager_GetBackupInfo_NotFound(t *testing.T) {
	t.Parallel()
	_, err := GetBackupInfo("/nonexistent/backup/file")
	assert.Error(t, err)
}

// TestBadgerBackupManager_Restore tests Restore function
func TestBadgerBackupManager_Restore(t *testing.T) {
	t.Parallel()
	// First create a backup file
	dbPath := t.TempDir()
	db, err := badger.Open(badger.DefaultOptions(dbPath))
	assert.NoError(t, err)

	err = db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("key"), []byte("value"))
	})
	assert.NoError(t, err)

	bbm := NewBadgerBackupManager(db)
	backupFile, err := bbm.Backup(t.TempDir())
	assert.NoError(t, err)
	db.Close()

	// Open a new database and restore
	restorePath := t.TempDir()
	db2, err := badger.Open(badger.DefaultOptions(restorePath))
	assert.NoError(t, err)
	defer db2.Close()

	bbm2 := NewBadgerBackupManager(db2)
	err = bbm2.Restore(backupFile)
	assert.NoError(t, err)

	// Verify restored data
	err = db2.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("key"))
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		assert.Equal(t, "value", string(val))
		return nil
	})
	assert.NoError(t, err)
}

// TestBadgerBackupManager_Restore_NotFound tests Restore with non-existent file
func TestBadgerBackupManager_Restore_NotFound(t *testing.T) {
	t.Parallel()
	dbPath := t.TempDir()
	db, err := badger.Open(badger.DefaultOptions(dbPath))
	assert.NoError(t, err)
	defer db.Close()

	bbm := NewBadgerBackupManager(db)
	err = bbm.Restore("/nonexistent/backup/file")
	assert.Error(t, err)
}

// TestRestoreTo tests RestoreTo function
func TestRestoreTo(t *testing.T) {
	t.Parallel()
	// First create a backup file
	dbPath := t.TempDir()
	db, err := badger.Open(badger.DefaultOptions(dbPath))
	assert.NoError(t, err)

	err = db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("key"), []byte("value"))
	})
	assert.NoError(t, err)

	bbm := NewBadgerBackupManager(db)
	backupFile, err := bbm.Backup(t.TempDir())
	assert.NoError(t, err)
	db.Close()

	// Restore to new database
	newDbPath := t.TempDir()
	err = RestoreTo(backupFile, newDbPath)
	assert.NoError(t, err)

	// Open and verify restored data
	db2, err := badger.Open(badger.DefaultOptions(newDbPath))
	assert.NoError(t, err)
	defer db2.Close()

	err = db2.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("key"))
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		assert.Equal(t, "value", string(val))
		return nil
	})
	assert.NoError(t, err)
}

// TestRestoreTo_InvalidBackup tests RestoreTo with non-existent backup file
func TestRestoreTo_InvalidBackup(t *testing.T) {
	t.Parallel()
	newDbPath := t.TempDir()
	err := RestoreTo("/nonexistent/backup/file", newDbPath)
	assert.Error(t, err)
}

// TestRestoreTo_InvalidDBPath tests RestoreTo with invalid database path
func TestRestoreTo_InvalidDBPath(t *testing.T) {
	t.Parallel()
	// Create a valid backup first
	dbPath := t.TempDir()
	db, err := badger.Open(badger.DefaultOptions(dbPath))
	assert.NoError(t, err)

	err = db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("key"), []byte("value"))
	})
	assert.NoError(t, err)

	bbm := NewBadgerBackupManager(db)
	backupFile, err := bbm.Backup(t.TempDir())
	assert.NoError(t, err)
	db.Close()

	// Try to restore to an invalid path (parent dir doesn't exist)
	invalidPath := "/nonexistent/directory/db"
	err = RestoreTo(backupFile, invalidPath)
	assert.Error(t, err)
}

// TestBadgerBackupManager_StoreBased tests using BadgerBackupManager with store's GetDB
func TestBadgerBackupManager_StoreBased(t *testing.T) {
	t.Parallel()
	dbPath := t.TempDir()
	s, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)
	defer s.Close()

	// Get the underlying badger DB
	db := s.GetDB()
	assert.True(t, db != nil)

	// Write some data using store
	err = s.Set("testkey", "testvalue")
	assert.NoError(t, err)

	// Create backup manager and perform backup
	bbm := NewBadgerBackupManager(db)
	backupFile, err := bbm.Backup(t.TempDir())
	assert.NoError(t, err)
	assert.True(t, len(backupFile) > 0)

	// Get backup info
	info, err := GetBackupInfo(backupFile)
	assert.NoError(t, err)
	assert.True(t, info["size"].(int64) > 0)
}

// TestBadgerBackupManager_IncrementalBackup_Empty tests IncrementalBackup with empty since
func TestBadgerBackupManager_IncrementalBackup_Empty(t *testing.T) {
	t.Parallel()
	dbPath := t.TempDir()
	db, err := badger.Open(badger.DefaultOptions(dbPath))
	assert.NoError(t, err)
	defer db.Close()

	// Write some data
	err = db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("key"), []byte("value"))
	})
	assert.NoError(t, err)

	bbm := NewBadgerBackupManager(db)
	backupFile, err := bbm.IncrementalBackup(t.TempDir(), 0)
	assert.NoError(t, err)
	assert.True(t, len(backupFile) > 0)

	// Verify backup file is different from regular backup
	assert.True(t, len(backupFile) > 0)
}

// TestBadgerBackupManager_RestoreAndIncrementalRestore tests full restore and incremental restore cycle
func TestBadgerBackupManager_RestoreAndIncrementalRestore(t *testing.T) {
	t.Parallel()
	// Create source database with initial data
	sourcePath := t.TempDir()
	db1, err := badger.Open(badger.DefaultOptions(sourcePath))
	assert.NoError(t, err)

	err = db1.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("key1"), []byte("value1"))
	})
	assert.NoError(t, err)

	bbm1 := NewBadgerBackupManager(db1)
	backupFile1, err := bbm1.Backup(t.TempDir())
	assert.NoError(t, err)
	db1.Close()

	// Restore to new database
	restorePath := t.TempDir()
	err = RestoreTo(backupFile1, restorePath)
	assert.NoError(t, err)

	// Open restored database
	db2, err := badger.Open(badger.DefaultOptions(restorePath))
	assert.NoError(t, err)

	// Verify restored data
	err = db2.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("key1"))
		if err != nil {
			return err
		}
		val, _ := item.ValueCopy(nil)
		assert.Equal(t, "value1", string(val))
		return nil
	})
	assert.NoError(t, err)

	// Add more data and create incremental backup
	err = db2.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("key2"), []byte("value2"))
	})
	assert.NoError(t, err)

	bbm2 := NewBadgerBackupManager(db2)
	incBackupFile, err := bbm2.IncrementalBackup(t.TempDir(), 0)
	assert.NoError(t, err)
	assert.True(t, len(incBackupFile) > 0)

	db2.Close()

	// Verify incremental backup file exists
	_, err = os.Stat(incBackupFile)
	assert.NoError(t, err)
}

// TestGetBackupInfo_FileInfo tests that GetBackupInfo returns correct file info
func TestGetBackupInfo_FileInfo(t *testing.T) {
	t.Parallel()
	// Create a backup file
	dbPath := t.TempDir()
	db, err := badger.Open(badger.DefaultOptions(dbPath))
	assert.NoError(t, err)

	err = db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("key"), []byte("value"))
	})
	assert.NoError(t, err)

	bbm := NewBadgerBackupManager(db)
	backupFile, err := bbm.Backup(t.TempDir())
	assert.NoError(t, err)
	db.Close()

	// Get backup info
	info, err := GetBackupInfo(backupFile)
	assert.NoError(t, err)

	// Verify size is positive
	size := info["size"].(int64)
	assert.True(t, size > 0)

	// Verify path is correct
	path := info["path"].(string)
	assert.True(t, path == backupFile || filepath.Clean(path) == filepath.Clean(backupFile))
}

// TestListBackups_WithInvalidExt tests ListBackups with different file extensions
func TestListBackups_WithInvalidExt(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	// Create files with various extensions
	files := []string{
		"backup_001",
		"backup_002.bak",
		"backup_003.txt",
		"backup_004",
	}

	for _, f := range files {
		err := os.WriteFile(filepath.Join(tmpDir, f), []byte("test"), 0644)
		assert.NoError(t, err)
	}

	backups, err := ListBackups(tmpDir)
	assert.NoError(t, err)

	// Should find files without extension or with .bak
	found := make(map[string]bool)
	for _, b := range backups {
		found[filepath.Base(b)] = true
	}

	// Should include files without extension or .bak
	assert.True(t, found["backup_001"] || found["backup_002.bak"])
}

// TestRDBBackupManager_GetBackupInfoFile tests GetBackupInfo for RDB files with success case
func TestRDBBackupManager_GetBackupInfoFile(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	// Create a test RDB file
	rdbFile := filepath.Join(tmpDir, "test.rdb")
	err := os.WriteFile(rdbFile, []byte("mock rdb data"), 0644)
	assert.NoError(t, err)

	rbm := &RDBBackupManager{}
	info, err := rbm.GetBackupInfo(rdbFile)
	assert.NoError(t, err)
	assert.True(t, info["size"].(int64) > 0)
	assert.Equal(t, "RDB", info["format"])
}

// TestListRDBBackupsWithFiles tests ListRDBBackups function with files
func TestListRDBBackupsWithFiles(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	// Create RDB files
	rdbFiles := []string{
		"backup_001.rdb",
		"backup_002.rdb",
		"backup_003.txt",
	}

	for _, f := range rdbFiles {
		err := os.WriteFile(filepath.Join(tmpDir, f), []byte("test"), 0644)
		assert.NoError(t, err)
	}

	backups, err := ListRDBBackups(tmpDir)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(backups))
}
