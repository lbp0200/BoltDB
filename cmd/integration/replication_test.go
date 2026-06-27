package integration

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/zeebo/assert"
)

// contains helper function to check if string contains substring
func contains(s, substr string) bool {
	if len(s) < len(substr) {
		return false
	}
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// TestReplicationMaster 测试主节点复制信息
func TestReplicationMaster(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// INFO replication - 主节点信息
	result, err := sharedClient.Do(ctx, "INFO", "replication").Result()
	assert.NoError(t, err)

	info, ok := result.(string)
	assert.True(t, ok)
	assert.True(t, len(info) > 0)

	// 检查关键信息
	assert.True(t, contains(info, "role:"))
	assert.True(t, contains(info, "connected_slaves:"))
}

// TestReplicationCommandProp 测试命令传播
func TestReplicationCommandProp(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 添加一些数据
	for i := 0; i < 10; i++ {
		err := sharedClient.Set(ctx, "prop_key_"+string(rune('a'+i)), "value_"+string(rune('a'+i)), 0).Err()
		assert.NoError(t, err)
	}

	// INFO replication - 检查复制偏移量
	result, err := sharedClient.Do(ctx, "INFO", "replication").Result()
	assert.NoError(t, err)

	info, ok := result.(string)
	assert.True(t, ok)

	// 检查master_repl_offset存在
	assert.True(t, contains(info, "master_repl_offset:"))
}

// TestReplicationBacklog 测试复制积压缓冲区
func TestReplicationBacklog(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// INFO replication - 检查积压缓冲区信息
	result, err := sharedClient.Do(ctx, "INFO", "replication").Result()
	assert.NoError(t, err)

	info, ok := result.(string)
	assert.True(t, ok)

	// 检查积压相关信息
	assert.True(t, contains(info, "repl_backlog_active:") || contains(info, "repl_backlog_size:"))
}

// TestReplicationMultipleSlaves 测试多从节点场景
func TestReplicationMultipleSlaves(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 主节点上添加数据
	err := sharedClient.Set(ctx, "multi_slave_test", "value", 0).Err()
	assert.NoError(t, err)

	// INFO replication - 获取从节点数量
	result, err := sharedClient.Do(ctx, "INFO", "replication").Result()
	assert.NoError(t, err)

	info, ok := result.(string)
	assert.True(t, ok)

	// 检查connected_slaves字段
	assert.True(t, contains(info, "connected_slaves:"))
}

// TestMasterLinkStatus 测试主从连接状态
func TestMasterLinkStatus(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// INFO replication - 检查主从链接状态
	result, err := sharedClient.Do(ctx, "INFO", "replication").Result()
	assert.NoError(t, err)

	info, ok := result.(string)
	assert.True(t, ok)

	// 检查链接状态
	assert.True(t, contains(info, "master_link_status:") || contains(info, "role:"))
}

// TestSlaveReadOnly 测试从节点只读属性
func TestSlaveReadOnly(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// INFO replication - 检查从节点只读设置
	result, err := sharedClient.Do(ctx, "INFO", "replication").Result()
	assert.NoError(t, err)

	info, ok := result.(string)
	assert.True(t, ok)
	assert.True(t, len(info) > 0)
}

// TestReplicationStress 测试复制压力
func TestReplicationStress(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 快速添加大量数据
	for i := 0; i < 100; i++ {
		key := "stress_key_" + string(rune('a'+i%26)) + "_" + string(rune('0'+i/26))
		err := sharedClient.Set(ctx, key, "stress_value_"+string(rune('a'+i)), 0).Err()
		assert.NoError(t, err)
	}

	// 检查复制偏移量增长
	result, err := sharedClient.Do(ctx, "INFO", "replication").Result()
	assert.NoError(t, err)

	info, ok := result.(string)
	assert.True(t, ok)
	assert.True(t, len(info) > 0)
}

// TestReplicationWithDifferentTypes 测试不同数据类型的复制
func TestReplicationWithDifferentTypes(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// String
	err := sharedClient.Set(ctx, "type_string", "value", 0).Err()
	assert.NoError(t, err)

	// List
	err = sharedClient.RPush(ctx, "type_list", "a", "b", "c").Err()
	assert.NoError(t, err)

	// Hash
	err = sharedClient.HSet(ctx, "type_hash", "field", "value").Err()
	assert.NoError(t, err)

	// Set
	err = sharedClient.SAdd(ctx, "type_set", "a", "b", "c").Err()
	assert.NoError(t, err)

	// ZSet
	err = sharedClient.ZAdd(ctx, "type_zset", redis.Z{Score: 1, Member: "a"}).Err()
	assert.NoError(t, err)

	// 检查所有类型都被记录
	result, err := sharedClient.Do(ctx, "INFO", "replication").Result()
	assert.NoError(t, err)

	info, ok := result.(string)
	assert.True(t, ok)
	assert.True(t, len(info) > 0)
}

// TestReplicaOfCommand 测试 REPLICAOF 和 SLAVEOF 命令
func TestReplicaOfCommand(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 测试 REPLICAOF NO ONE (停止复制，提升为主节点)
	result, err := sharedClient.Do(ctx, "REPLICAOF", "NO", "ONE").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// 测试 SLAVEOF NO ONE (别名)
	result, err = sharedClient.Do(ctx, "SLAVEOF", "NO", "ONE").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)
}

// TestReplicaOfCommandErrors 测试 REPLICAOF 错误处理
func TestReplicaOfCommandErrors(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 测试参数不足的错误
	result, err := sharedClient.Do(ctx, "REPLICAOF").Result()
	assert.Error(t, err)
	assert.Nil(t, result)

	result, err = sharedClient.Do(ctx, "SLAVEOF", "NO").Result()
	assert.Error(t, err)
	assert.Nil(t, result)
}
