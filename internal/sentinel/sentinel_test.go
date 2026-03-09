package sentinel

import (
	"testing"
	"time"

	"github.com/zeebo/assert"
)

func TestSentinel_New(t *testing.T) {
	s := NewSentinel(2, 30*time.Second)

	assert.NotEqual(t, nil, s)
	assert.NotEqual(t, "", s.GetRunID())
	assert.Equal(t, int64(0), s.GetConfigEpoch())
}

func TestSentinel_GenerateRunID(t *testing.T) {
	// 测试 RunID 生成
	s := NewSentinel(2, 30*time.Second)
	runID := s.GetRunID()

	assert.NotEqual(t, "", runID)
	assert.True(t, len(runID) > 0)
}

func TestSentinel_AddMaster(t *testing.T) {
	s := NewSentinel(2, 30*time.Second)

	// 添加主节点
	err := s.AddMaster("mymaster", "127.0.0.1:6379", 2)
	assert.NoError(t, err)

	// 获取主节点
	master := s.GetMaster("mymaster")
	assert.NotEqual(t, nil, master)
	// master 存在即可，具体字段未导出
}

func TestSentinel_AddMaster_Duplicate(t *testing.T) {
	s := NewSentinel(2, 30*time.Second)

	// 添加主节点
	err := s.AddMaster("mymaster", "127.0.0.1:6379", 2)
	assert.NoError(t, err)

	// 重复添加应该返回错误
	err = s.AddMaster("mymaster", "127.0.0.1:6380", 2)
	assert.Error(t, err)
}

func TestSentinel_RemoveMaster(t *testing.T) {
	s := NewSentinel(2, 30*time.Second)

	// 添加主节点
	err := s.AddMaster("mymaster", "127.0.0.1:6379", 2)
	assert.NoError(t, err)

	// 移除主节点
	err = s.RemoveMaster("mymaster")
	assert.NoError(t, err)

	// 验证已移除
	master := s.GetMaster("mymaster")
	assert.True(t, master == nil)
}

func TestSentinel_RemoveMaster_NotFound(t *testing.T) {
	s := NewSentinel(2, 30*time.Second)

	// 移除不存在的主节点应该返回错误
	err := s.RemoveMaster("nonexistent")
	assert.Error(t, err)
}

func TestSentinel_GetAllMasters(t *testing.T) {
	s := NewSentinel(2, 30*time.Second)

	// 初始为空
	masters := s.GetAllMasters()
	assert.Equal(t, 0, len(masters))

	// 添加主节点
	s.AddMaster("master1", "127.0.0.1:6379", 2)
	s.AddMaster("master2", "127.0.0.1:6380", 2)

	masters = s.GetAllMasters()
	assert.Equal(t, 2, len(masters))
}

func TestSentinel_AddSentinel(t *testing.T) {
	s := NewSentinel(2, 30*time.Second)

	// 添加哨兵地址（这个只是记录，不建立连接）
	s.AddSentinel("127.0.0.1:26379")

	// 测试通过，不崩溃即可
}

func TestSentinel_ConfigEpoch(t *testing.T) {
	s := NewSentinel(2, 30*time.Second)

	// 初始为 0
	assert.Equal(t, int64(0), s.GetConfigEpoch())

	// 增加
	epoch := s.IncrementConfigEpoch()
	assert.Equal(t, int64(1), epoch)

	// 再次增加
	epoch = s.IncrementConfigEpoch()
	assert.Equal(t, int64(2), epoch)
}

func TestSentinel_Stop(t *testing.T) {
	s := NewSentinel(2, 30*time.Second)

	// 添加主节点
	s.AddMaster("mymaster", "127.0.0.1:6379", 2)

	// 停止 - 验证不 panic
	s.Stop()
}
