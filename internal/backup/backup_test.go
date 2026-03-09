package backup

import (
	"testing"

	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/zeebo/assert"
)

func setupTestStore(t *testing.T) *store.BotreonStore {
	t.Helper()
	dbPath := t.TempDir()
	s, err := store.NewBadgerStore(dbPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("failed to close store: %v", err)
		}
	})
	return s
}

func TestBackupManager_New(t *testing.T) {
	testStore := setupTestStore(t)
	bm := NewBackupManager(testStore, t.TempDir())

	assert.NotEqual(t, nil, bm)
}

func TestBackupManager_LastSave(t *testing.T) {
	testStore := setupTestStore(t)
	bm := NewBackupManager(testStore, t.TempDir())

	// 初始 LastSave 应该是 0
	lastSave := bm.LastSave()
	assert.Equal(t, int64(0), lastSave)
}

func TestBackupManager_Save(t *testing.T) {
	testStore := setupTestStore(t)
	bm := NewBackupManager(testStore, t.TempDir())

	// 先设置一些数据
	err := testStore.Set("key1", "value1")
	assert.NoError(t, err)

	// 保存
	err = bm.Save()
	assert.NoError(t, err)

	// LastSave 应该更新
	assert.True(t, bm.LastSave() > 0)
}

func TestRestoreManager_New(t *testing.T) {
	testStore := setupTestStore(t)
	rm := NewRestoreManager(testStore)

	assert.NotEqual(t, nil, rm)
}

func TestRestoreManager_RestoreFromPath_NotFound(t *testing.T) {
	testStore := setupTestStore(t)
	rm := NewRestoreManager(testStore)

	// 恢复不存在的文件应该报错
	err := rm.RestoreFromPath("/nonexistent/path")
	assert.Error(t, err)
}
