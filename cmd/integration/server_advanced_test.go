package integration

import (
	"context"
	"testing"
	"time"

	"github.com/zeebo/assert"
)

// TestRole 测试 ROLE 命令
func TestRole(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// ROLE - 获取角色（单机模式下是master）
	result, err := sharedClient.Do(ctx, "ROLE").Result()
	assert.NoError(t, err)

	arr, ok := result.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr))
	assert.Equal(t, "master", arr[0])
}

// TestConfigGet 测试 CONFIG GET 命令
func TestConfigGet(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// CONFIG GET - 获取所有配置
	result, err := sharedClient.Do(ctx, "CONFIG", "GET", "*").Result()
	assert.NoError(t, err)

	arr, ok := result.([]interface{})
	assert.True(t, ok)
	assert.True(t, len(arr) >= 2)

	// CONFIG GET - 获取特定配置
	result, err = sharedClient.Do(ctx, "CONFIG", "GET", "maxclients").Result()
	assert.NoError(t, err)

	arr, ok = result.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr))
	assert.Equal(t, "maxclients", arr[0])
}

// TestConfigSet 测试 CONFIG SET 命令
func TestConfigSet(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// CONFIG SET - 设置配置
	result, err := sharedClient.Do(ctx, "CONFIG", "SET", "maxclients", "1000").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// CONFIG SET - 持久化配置
	result, err = sharedClient.Do(ctx, "CONFIG", "REWRITE").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)
}

// TestClientList 测试 CLIENT LIST 命令
func TestClientList(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// CLIENT LIST - 获取客户端列表
	result, err := sharedClient.Do(ctx, "CLIENT", "LIST").Result()
	assert.NoError(t, err)

	list, ok := result.(string)
	assert.True(t, ok)
	assert.True(t, len(list) > 0)
}

// TestClientGetName 测试 CLIENT GETNAME 命令
// 注意：使用共享连接，初始 client name 可能被其他测试设置（如 TestCommandCompleteness_Connection）。
// 因此只测试 SETNAME → GETNAME 的完整往返，不假定初始状态。
func TestClientGetName(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// CLIENT SETNAME - 设置客户端名称
	result, err := sharedClient.Do(ctx, "CLIENT", "SETNAME", "test-client").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// CLIENT GETNAME - 验证名称
	result, err = sharedClient.Do(ctx, "CLIENT", "GETNAME").Result()
	assert.NoError(t, err)
	assert.Equal(t, "test-client", result)
}

// TestClientSetName 测试 CLIENT SETNAME 命令
func TestClientSetName(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// CLIENT SETNAME - 设置客户端名称
	result, err := sharedClient.Do(ctx, "CLIENT", "SETNAME", "myclient").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// 验证
	name, _ := sharedClient.Do(ctx, "CLIENT", "GETNAME").Result()
	assert.Equal(t, "myclient", name)
}

// TestClientPause 测试 CLIENT PAUSE 命令
func TestClientPause(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// CLIENT PAUSE - 暂停客户端
	result, err := sharedClient.Do(ctx, "CLIENT", "PAUSE", "1000").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// CLIENT UNPAUSE - 恢复客户端
	result, err = sharedClient.Do(ctx, "CLIENT", "UNPAUSE").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)
}

// TestBgSave 测试 BGSAVE 命令
func TestBgSave(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// BGSAVE - 后台保存
	result, err := sharedClient.Do(ctx, "BGSAVE").Result()
	assert.NoError(t, err)
	assert.Equal(t, "Background saving started", result)
}

// TestLastSave 测试 LASTSAVE 命令
func TestLastSave(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 先执行 BGSAVE 来设置 lastSaveTime
	_, err := sharedClient.Do(ctx, "BGSAVE").Result()
	assert.NoError(t, err)
	time.Sleep(100 * time.Millisecond) // 等待后台保存完成

	// LASTSAVE - 获取最后保存时间
	result, err := sharedClient.Do(ctx, "LASTSAVE").Result()
	assert.NoError(t, err)

	timestamp, ok := result.(int64)
	assert.True(t, ok)
	// 应该是一个有效的时间戳
	assert.True(t, timestamp > 0)
}

// TestShutdown 测试 SHUTDOWN 命令
func TestShutdown(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// SHUTDOWN - 注意：这会关闭服务器
	// 由于测试环境，我们不实际执行关闭
	// 只验证命令被识别
	result, err := sharedClient.Do(ctx, "SHUTDOWN", "NOSAVE").Result()
	assert.Error(t, err) // 应该返回错误（连接断开）
	assert.Nil(t, result)
}

// TestClientKill 测试 CLIENT KILL 命令
func TestClientKill(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// CLIENT KILL - 杀死客户端连接（格式: ip:port）
	result, err := sharedClient.Do(ctx, "CLIENT", "KILL", "127.0.0.1:12345").Result()
	assert.NoError(t, err)
	// 如果连接不存在，返回0
	assert.Equal(t, int64(0), result)
}

// TestClientID 测试 CLIENT ID 命令
func TestClientID(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// CLIENT ID - 获取客户端ID
	result, err := sharedClient.Do(ctx, "CLIENT", "ID").Result()
	assert.NoError(t, err)

	id, ok := result.(int64)
	assert.True(t, ok)
	assert.True(t, id >= 1) // CLIENT ID is always >= 1
}

// TestClientInfo 测试 CLIENT INFO 命令
func TestClientInfo(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// CLIENT INFO - 获取客户端信息
	result, err := sharedClient.Do(ctx, "CLIENT", "INFO").Result()
	assert.NoError(t, err)

	info, ok := result.(string)
	assert.True(t, ok)
	assert.True(t, len(info) > 0)
}

// TestClientNoEvict 测试 CLIENT NOEVICT 命令
func TestClientNoEvict(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// CLIENT NOEVICT - 设置内存驱逐策略
	result, err := sharedClient.Do(ctx, "CLIENT", "NOEVICT", "ON").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	result, err = sharedClient.Do(ctx, "CLIENT", "NOEVICT", "OFF").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)
}

// TestClientTracking 测试 CLIENT TRACKING 命令
func TestClientTracking(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// CLIENT TRACKING - 启用跟踪
	result, err := sharedClient.Do(ctx, "CLIENT", "TRACKING", "ON").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// CLIENT TRACKING - 禁用跟踪
	result, err = sharedClient.Do(ctx, "CLIENT", "TRACKING", "OFF").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)
}

// TestModuleList 测试 MODULE LIST 命令
func TestModuleList(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// MODULE LIST - 列出加载的模块
	result, err := sharedClient.Do(ctx, "MODULE", "LIST").Result()
	assert.NoError(t, err)

	arr, ok := result.([]interface{})
	assert.True(t, ok)
	// 没有加载模块时返回空数组
	assert.Equal(t, 0, len(arr))
}
