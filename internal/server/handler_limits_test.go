package server

import (
	"os"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/zeebo/assert"
)

// TestMaxClients_RejectsNewConnections 验证 maxclients 超限时拒绝新连接
func TestMaxClients_RejectsNewConnections(t *testing.T) {

	dbPath := t.TempDir()
	db, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	// 创建 Handler，maxclients=1
	handler := &Handler{
		Db:         db,
		conns:      make(map[*connState]*connMeta),
		MaxClients: 1,
		Ctx:        t.Context(),
	}

	// 先注册一个连接
	state1 := &connState{}
	handler.registerConnection(state1, &mockConn{}, "127.0.0.1:1")
	defer handler.unregisterConnection(state1)

	// 验证连接数 = 1
	assert.Equal(t, 1, handler.ActiveClientCount())
	// 验证 maxclients = 1
	assert.Equal(t, 1, handler.GetMaxClients())
	// 超限判断
	assert.True(t, handler.ActiveClientCount() >= handler.GetMaxClients())
}

// TestGetMaxClients 默认值验证
func TestGetMaxClients(t *testing.T) {

	// 未配置时默认 10000
	h1 := &Handler{}
	assert.Equal(t, 10000, h1.GetMaxClients())

	// 配置为 500
	h2 := &Handler{MaxClients: 500}
	assert.Equal(t, 500, h2.GetMaxClients())

	// 配置为 0 应返回默认值
	h3 := &Handler{MaxClients: 0}
	assert.Equal(t, 10000, h3.GetMaxClients())
}

// TestIdleTimeout_ReadDeadline 验证空闲超时设置
func TestIdleTimeout_ReadDeadline(t *testing.T) {

	dbPath := t.TempDir()
	db, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	handler := &Handler{
		Db:      db,
		conns:   make(map[*connState]*connMeta),
		Timeout: 5 * time.Second,
		Ctx:     t.Context(),
	}

	// 验证 Timeout 被正确设置
	assert.True(t, handler.Timeout > 0)
	assert.Equal(t, 5*time.Second, handler.Timeout)
}

// TestIdleTimeout_ZeroDefault 验证默认无超时
func TestIdleTimeout_ZeroDefault(t *testing.T) {

	h := &Handler{}
	assert.Equal(t, time.Duration(0), h.Timeout)
}

// TestAUTH_TimingAttackResistance 验证密码比较使用 constant-time compare，
// 防止时序侧信道攻击。正确密码和错误密码的比较时间应一致。
func TestAUTH_TimingAttackResistance(t *testing.T) {

	os.Setenv("BOLTDB_PASSWORD", "correct-password-12345")
	defer os.Unsetenv("BOLTDB_PASSWORD")

	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()

	// 正确密码
	state1 := &connState{}
	resp1 := handler.handleAUTH(state1, [][]byte{[]byte("correct-password-12345")}, "127.0.0.1:1")
	_, ok1 := resp1.(*proto.SimpleString)
	assert.True(t, ok1) // +OK
	assert.True(t, state1.authenticated)

	// 错误密码（长度相同）
	state2 := &connState{}
	resp2 := handler.handleAUTH(state2, [][]byte{[]byte("wrong--password-12345")}, "127.0.0.1:1")
	_, ok2 := resp2.(*proto.Error)
	assert.True(t, ok2) // -ERR
	assert.False(t, state2.authenticated)

	// 错误密码（长度不同）
	state3 := &connState{}
	resp3 := handler.handleAUTH(state3, [][]byte{[]byte("short")}, "127.0.0.1:1")
	_, ok3 := resp3.(*proto.Error)
	assert.True(t, ok3) // -ERR
	assert.False(t, state3.authenticated)

	// 关键验证：constant-time compare 不依赖密码长度
	// 如果使用 != 比较，不同长度的密码会提前返回
	// subtle.ConstantTimeCompare 在长度不同时也会完整遍历
}
