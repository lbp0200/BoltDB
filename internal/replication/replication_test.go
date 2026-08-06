package replication

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

func TestReplicationManager_New(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)

	assert.True(t, rm.IsMaster())
	assert.False(t, rm.IsSlave())
	assert.Equal(t, int64(0), rm.GetMasterReplOffset())
	assert.NotEqual(t, "", rm.GetReplicationID())
}

func TestReplicationManager_Role(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)

	// 默认是 master
	assert.Equal(t, RoleMaster, rm.GetRole())

	// 设置为 slave
	rm.SetRole(RoleSlave)
	assert.Equal(t, RoleSlave, rm.GetRole())
	assert.True(t, rm.IsSlave())
	assert.False(t, rm.IsMaster())

	// 切回 master
	rm.SetRole(RoleMaster)
	assert.Equal(t, RoleMaster, rm.GetRole())
}

func TestReplicationManager_ReplOffset(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)

	// 初始 offset 是 0
	assert.Equal(t, int64(0), rm.GetMasterReplOffset())

	// 设置 offset
	rm.SetMasterReplOffset(100)
	assert.Equal(t, int64(100), rm.GetMasterReplOffset())

	// 增加 offset
	rm.IncrementReplOffset(50)
	assert.Equal(t, int64(150), rm.GetMasterReplOffset())
}

func TestReplicationManager_MasterAddr(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)

	// 初始为空
	assert.Equal(t, "", rm.GetMasterAddr())

	// 设置主节点地址
	rm.SetMasterAddr("127.0.0.1:6379")
	assert.Equal(t, "127.0.0.1:6379", rm.GetMasterAddr())
}

func TestReplicationManager_SlaveManagement(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)

	// 初始没有从节点
	assert.Equal(t, 0, rm.GetSlaveCount())

	// 创建一个 mock slave 连接
	// 由于 SlaveConnection 需要 net.Conn，我们这里测试其行为
	// 需要通过内部方式测试

	// 测试 GetSlaves 返回空切片
	slaves := rm.GetSlaves()
	assert.Equal(t, 0, len(slaves))
}

func TestReplicationManager_Backlog(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)

	backlog := rm.GetBacklog()
	assert.NotEqual(t, nil, backlog)
	assert.Equal(t, int64(1024*1024), backlog.GetSize()) // 1MB
}

func TestReplicationManager_GenerateReplId(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)

	replId := rm.GetReplicationID()
	assert.NotEqual(t, "", replId)
	assert.Equal(t, 40, len(replId)) // 40 字符的十六进制
}

func TestReplicationManager_PropagateCommand(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)

	// PropagateCommand 现在总是记录到 backlog 并更新 offset，
	// 确保断连期间的写操作不会丢失
	cmd := [][]byte{[]byte("SET"), []byte("key"), []byte("value")}
	rm.PropagateCommand(cmd)

	// offset 应该增加了（即使没有从节点）
	cmdBytes := serializeCommand(cmd)
	t.Logf("offset after propagate (no slaves): %d", rm.GetMasterReplOffset())
	assert.Equal(t, int64(len(cmdBytes)), rm.GetMasterReplOffset())

	// backlog 应该包含命令
	backlog := rm.GetBacklog()
	data, err := backlog.GetRange(0, int64(len(cmdBytes)))
	assert.NoError(t, err)
	assert.Equal(t, cmdBytes, data)
}

func TestReplicationManager_WALTruncateTriggered(t *testing.T) {
	// Not parallel — WAL I/O
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	rm.SetBacklogSize(64 * 1024)

	dir := t.TempDir()
	wal, err := NewBacklogWAL(dir)
	assert.NoError(t, err)
	defer wal.Close()
	rm.SetBacklogWAL(wal)

	// Write enough commands to exceed walTruncateFactor × backlog size
	// (128KB threshold, 27-byte commands → 10K commands ≈ 270KB).
	const n = 10000
	for i := 0; i < n; i++ {
		rm.PropagateCommand([][]byte{[]byte("SET"), []byte("k"), []byte("v")})
	}

	// File must be bounded: truncated back to roughly the live window
	// instead of holding all n commands (which would be ~270KB+).
	sz, err := wal.GetFileSize()
	assert.NoError(t, err)
	if sz > 2*64*1024+4096 {
		t.Fatalf("WAL size %d not bounded by threshold", sz)
	}

	// Replay must reconstruct exactly the same offset as the live backlog.
	// Flush first: entries still in the 64KB write buffer are not on disk yet.
	err = wal.Flush()
	assert.NoError(t, err)
	replayed := NewReplicationBacklog(64 * 1024)
	err = wal.Replay(replayed)
	assert.NoError(t, err)
	assert.Equal(t, rm.GetBacklog().GetCurrentOffset(), replayed.GetCurrentOffset())
}

func TestReplicationManager_MultipleSlaveIds(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)

	// 测试复制ID生成的唯一性
	replId1 := rm.GetReplicationID()

	// 创建新的 manager（使用不同的 store 实例避免数据竞争）
	testStore2 := setupTestStore(t)
	rm2 := NewReplicationManager(testStore2)
	replId2 := rm2.GetReplicationID()

	// 两个 ID 应该不同（概率上几乎必然）
	assert.NotEqual(t, replId1, replId2)
}
