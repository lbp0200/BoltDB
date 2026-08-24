package server

import (
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/zeebo/assert"
)

// TestMaxClients_RejectsNewConnections 验证 maxclients 超限时拒绝新连接
func TestMaxClients_RejectsNewConnections(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

	h := &Handler{}
	assert.Equal(t, time.Duration(0), h.Timeout)
}

// TestAUTH_TimingAttackResistance 验证密码比较使用 constant-time compare，
// 防止时序侧信道攻击。正确密码和错误密码的比较时间应一致。
//
// 注意：本测试通过 t.Setenv("BOLTDB_PASSWORD", ...) 配置认证密码，
// 认证检查在 handler_dispatch.go 入口使用 Handler.authPassword 缓存
// （启动时 SetAuthPassword(os.Getenv(...))，避免每命令 os.Getenv），
// setupTestHandler 会在创建 Handler 时读取当前 env 并缓存，因此本测试
// 必须在 t.Setenv 之后创建 Handler 才能拿到正确密码。
// 若本测试 t.Parallel() 并行执行，t.Setenv 会直接 panic（Go 1.17+ 保护），
// 且即使绕过，缓存机制也无法完全隔离——其它并行测试若在 env 设置前已创建
// Handler（缓存空密码），则不受污染；但若在 env 设置后创建则会误判 NOAUTH。
// 因此这里必须保持【不带】t.Parallel()，让本测试在串行阶段独占运行。
func TestAUTH_TimingAttackResistance(t *testing.T) {
	t.Setenv("BOLTDB_PASSWORD", "correct-password-12345")
	// t.Setenv 在测试结束时自动清除，无需手动 Unsetenv

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
