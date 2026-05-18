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
	t.Parallel()
	testStore := setupTestStore(t)
	bm := NewBackupManager(testStore, t.TempDir())

	assert.NotEqual(t, nil, bm)
}

func TestBackupManager_LastSave(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	bm := NewBackupManager(testStore, t.TempDir())

	// 初始 LastSave 应该是 0
	lastSave := bm.LastSave()
	assert.Equal(t, int64(0), lastSave)
}

func TestBackupManager_Save(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewRestoreManager(testStore)

	assert.NotEqual(t, nil, rm)
}

func TestRestoreManager_RestoreFromPath_NotFound(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewRestoreManager(testStore)

	// 恢复不存在的文件应该报错
	err := rm.RestoreFromPath("/nonexistent/path")
	assert.Error(t, err)
}

// TestRDBBackupRestore_RoundTrip 测试RDB备份-恢复完整往返
func TestRDBBackupRestore_RoundTrip(t *testing.T) {
	t.Parallel()
	// Step 1: 创建源库并写入数据
	sourceStore := setupTestStore(t)

	err := sourceStore.Set("stringkey", "value1")
	assert.NoError(t, err)

	err = sourceStore.HSet("hashkey", "field1", "val1")
	assert.NoError(t, err)

	_, err = sourceStore.LPush("listkey", "b", "a")
	assert.NoError(t, err)

	_, err = sourceStore.SAdd("setkey", "member1")
	assert.NoError(t, err)

	err = sourceStore.ZAdd("zsetkey", []store.ZSetMember{{Member: "one", Score: 1.0}})
	assert.NoError(t, err)

	// Step 2: RDB 备份
	rbm := NewRDBBackupManager(sourceStore)
	backupFile, err := rbm.Backup(t.TempDir())
	assert.NoError(t, err)

	// Step 3: 关闭源库并创建目标库
	sourceStore.Close()

	destStore := setupTestStore(t)

	// Step 4: 从 RDB 文件恢复到目标库
	rm := NewRestoreManager(destStore)
	err = rm.RestoreFromRDB(backupFile)
	assert.NoError(t, err)

	// Step 5: 验证数据完整
	val, err := destStore.Get("stringkey")
	assert.NoError(t, err)
	assert.Equal(t, "value1", val)

	hval, err := destStore.HGet("hashkey", "field1")
	assert.NoError(t, err)
	assert.Equal(t, "val1", string(hval))

	lvals, err := destStore.LRange("listkey", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(lvals))
	assert.Equal(t, "a", lvals[0]) // LPush: b, a → LRANGE: a, b

	smembers, err := destStore.SMembers("setkey")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(smembers))
	assert.Equal(t, "member1", smembers[0])

	zvals, err := destStore.ZRange("zsetkey", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(zvals))
	assert.Equal(t, "one", zvals[0].Member)
}

// TestRDBBackupCompressRestore_RoundTrip 测试压缩备份-恢复往返
func TestRDBBackupCompressRestore_RoundTrip(t *testing.T) {
	t.Parallel()
	sourceStore := setupTestStore(t)

	// 写入不同类型数据
	assert.NoError(t, sourceStore.Set("k1", "v1"))
	assert.NoError(t, sourceStore.Set("k2", "v2"))

	// 压缩备份
	rbm := NewRDBBackupManager(sourceStore)
	backupFile, err := rbm.BackupWithCompression(t.TempDir())
	assert.NoError(t, err)

	sourceStore.Close()

	// 恢复
	destStore := setupTestStore(t)
	rm := NewRestoreManager(destStore)
	err = rm.RestoreFromRDB(backupFile)
	assert.NoError(t, err)

	// 验证
	val, err := destStore.Get("k1")
	assert.NoError(t, err)
	assert.Equal(t, "v1", val)

	val, err = destStore.Get("k2")
	assert.NoError(t, err)
	assert.Equal(t, "v2", val)
}
