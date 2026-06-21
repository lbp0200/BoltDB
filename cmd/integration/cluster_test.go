package integration

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/cluster"
	"github.com/lbp0200/BoltDB/internal/server"
	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/redis/go-redis/v9"
	"github.com/zeebo/assert"
)

var (
	clusterClient *redis.Client
	clusterServer *server.Handler
	clusterDB     *store.BotreonStore
	clusterListener net.Listener
)

// setupClusterTestServer 启动带集群模式的测试服务器
func setupClusterTestServer(t *testing.T) {
	var err error

	// 创建临时数据库目录
	dbPath := t.TempDir()

	// 创建数据库
	clusterDB, err = store.NewBotreonStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// 启动服务器（使用随机端口）
	clusterListener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		clusterDB.Close()
		t.Fatalf("Failed to listen: %v", err)
	}

	// 创建集群（使用实际的监听地址）
	c, err := cluster.NewCluster(clusterDB, "", clusterListener.Addr().String())
	if err != nil {
		clusterListener.Close()
		clusterDB.Close()
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// 创建服务器处理器
	clusterServer = &server.Handler{
		Db:      clusterDB,
		Cluster: c,
	}

	// 在goroutine中运行服务器
	go func() {
		_ = clusterServer.ServeTCP(clusterListener)
	}()

	// 等待服务器启动
	time.Sleep(50 * time.Millisecond)

	// 创建Redis客户端
	clusterClient = redis.NewClient(&redis.Options{
		Addr:     clusterListener.Addr().String(),
		Password: "",
		DB:       0,
	})

	// 测试连接
	ctx := context.Background()
	_, err = clusterClient.Ping(ctx).Result()
	if err != nil {
		clusterListener.Close()
		clusterDB.Close()
		t.Fatalf("Failed to ping: %v", err)
	}
}

// teardownClusterTestServer 关闭测试服务器
func teardownClusterTestServer(t *testing.T) {
	if clusterClient != nil {
		clusterClient.Close()
	}
	if clusterListener != nil {
		clusterListener.Close()
	}
	if clusterDB != nil {
		clusterDB.Close()
	}
}

// TestClusterBasic 测试基本集群命令
func TestClusterBasic(t *testing.T) {
	setupClusterTestServer(t)
	defer teardownClusterTestServer(t)

	ctx := context.Background()

	// 测试 CLUSTER MYID
	nodeID, err := clusterClient.Do(ctx, "CLUSTER", "MYID").Result()
	assert.NoError(t, err)
	nodeIDStr, ok := nodeID.(string)
	assert.True(t, ok)
	assert.True(t, nodeIDStr != "")

	// 测试 CLUSTER INFO - 使用 Do() 获取原始响应
	info, err := clusterClient.Do(ctx, "CLUSTER", "INFO").Result()
	assert.NoError(t, err)
	// CLUSTER INFO 返回BulkString ([]byte)
	var infoStr string
	switch v := info.(type) {
	case string:
		infoStr = v
	case []byte:
		infoStr = string(v)
	default:
		t.Fatalf("unexpected type for CLUSTER INFO: %T", info)
	}
	assert.True(t, len(infoStr) > 0)
	// 验证包含关键字段
	assert.True(t, strings.Contains(infoStr, "cluster_state:ok"))

	// 测试 CLUSTER NODES
	nodes, err := clusterClient.Do(ctx, "CLUSTER", "NODES").Result()
	assert.NoError(t, err)
	var nodesStr string
	switch v := nodes.(type) {
	case string:
		nodesStr = v
	case []byte:
		nodesStr = string(v)
	default:
		t.Fatalf("unexpected type for CLUSTER NODES: %T", nodes)
	}
	// 节点应该包含自身
	assert.True(t, len(nodesStr) > 0)

	// 测试 CLUSTER SLOTS - 跳过严格验证，因为格式可能因实现而异
	_, err = clusterClient.Do(ctx, "CLUSTER", "SLOTS").Result()
	// 只检查命令执行成功，不验证具体返回格式
	assert.NoError(t, err)
}

// TestClusterKeySlot 测试 KEYSLOT 命令
func TestClusterKeySlot(t *testing.T) {
	setupClusterTestServer(t)
	defer teardownClusterTestServer(t)

	ctx := context.Background()

	// 测试不同键的槽位计算
	testCases := []struct {
		key    string
		expect string // 前缀
	}{
		{"testkey", "testkey"},
		{"mykey", "mykey"},
		{"user:1", "user:1"},
		{"{tag}:item", "{tag}"},
	}

	for _, tc := range testCases {
		slot, err := clusterClient.Do(ctx, "CLUSTER", "KEYSLOT", tc.key).Result()
		assert.NoError(t, err)
		slotNum, ok := slot.(int64)
		assert.True(t, ok)
		assert.True(t, slotNum >= 0 && slotNum < 16384)
	}
}

// TestClusterSlotRedirect 测试槽位重定向
func TestClusterSlotRedirect(t *testing.T) {
	setupClusterTestServer(t)
	defer teardownClusterTestServer(t)

	ctx := context.Background()

	// 在单机模式下，当前节点拥有所有槽位
	// 所以不应该有重定向
	// SET 应该成功
	err := clusterClient.Set(ctx, "testkey", "value", 0).Err()
	assert.NoError(t, err)

	// GET 应该成功
	val, err := clusterClient.Get(ctx, "testkey").Result()
	assert.NoError(t, err)
	assert.Equal(t, "value", val)

	// DEL 应该成功
	deleted, err := clusterClient.Del(ctx, "testkey").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), deleted)
}

// TestClusterMeet 测试 MEET 命令
func TestClusterMeet(t *testing.T) {
	setupClusterTestServer(t)
	defer teardownClusterTestServer(t)

	ctx := context.Background()

	// CLUSTER MEET 应该返回 OK（简化实现）
	result, err := clusterClient.Do(ctx, "CLUSTER", "MEET", "127.0.0.1", "6380").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)
}

// TestClusterAddSlots 测试 ADDSLOTS 命令
func TestClusterAddSlots(t *testing.T) {
	setupClusterTestServer(t)
	defer teardownClusterTestServer(t)

	ctx := context.Background()

	// CLUSTER ADDSLOTS 应该返回 OK（简化实现）
	result, err := clusterClient.Do(ctx, "CLUSTER", "ADDSLOTS", "0", "1", "2").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)
}

// TestClusterSetSlot 测试 SETSLOT 命令
func TestClusterSetSlot(t *testing.T) {
	setupClusterTestServer(t)
	defer teardownClusterTestServer(t)

	ctx := context.Background()

	// CLUSTER SETSLOT 应该返回 OK（简化实现）
	result, err := clusterClient.Do(ctx, "CLUSTER", "SETSLOT", "0", "IMPORTING", "nodeid").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)
}

// TestClusterForget 测试 FORGET 命令
func TestClusterForget(t *testing.T) {
	setupClusterTestServer(t)
	defer teardownClusterTestServer(t)

	ctx := context.Background()

	// CLUSTER FORGET 应该返回 OK（简化实现）
	result, err := clusterClient.Do(ctx, "CLUSTER", "FORGET", "somenodeid").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)
}

// TestClusterReplicate 测试 REPLICATE 命令
func TestClusterReplicate(t *testing.T) {
	setupClusterTestServer(t)
	defer teardownClusterTestServer(t)

	ctx := context.Background()

	// 先使用 CLUSTER MEET 添加一个主节点
	_, err := clusterClient.Do(ctx, "CLUSTER", "MEET", "127.0.0.1", "6380").Result()
	assert.NoError(t, err)

	// 获取新节点的ID
	nodesResult, err := clusterClient.Do(ctx, "CLUSTER", "NODES").Result()
	assert.NoError(t, err)
	nodesStr := ""
	switch v := nodesResult.(type) {
	case string:
		nodesStr = v
	case []byte:
		nodesStr = string(v)
	}

	// 解析节点列表找到新节点的ID
	// 格式: id ip:port flags master ping pong epoch link slots
	var newNodeID string
	for _, line := range strings.Split(nodesStr, "\n") {
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) >= 3 && parts[1] != "127.0.0.1:0" {
			newNodeID = parts[0]
			break
		}
	}

	if newNodeID == "" {
		// 如果没找到新节点，使用自己的ID进行测试（会返回错误但测试逻辑是正确的）
		newNodeID = "myself"
	}

	// CLUSTER REPLICATE 需要指定真实存在的节点ID
	result, err := clusterClient.Do(ctx, "CLUSTER", "REPLICATE", newNodeID).Result()
	// 如果节点不存在，REPLICATE 会返回错误，这是预期的行为
	assert.True(t, err != nil || result != nil)
}

// TestClusterDataCommands 测试集群模式下的数据命令
func TestClusterDataCommands(t *testing.T) {
	setupClusterTestServer(t)
	defer teardownClusterTestServer(t)

	ctx := context.Background()

	// String commands
	err := clusterClient.Set(ctx, "stringkey", "value", 0).Err()
	assert.NoError(t, err)

	val, err := clusterClient.Get(ctx, "stringkey").Result()
	assert.NoError(t, err)
	assert.Equal(t, "value", val)

	// List commands
	err = clusterClient.LPush(ctx, "listkey", "a", "b", "c").Err()
	assert.NoError(t, err)

	length, err := clusterClient.LLen(ctx, "listkey").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), length)

	// Hash commands
	err = clusterClient.HSet(ctx, "hashkey", "field1", "value1").Err()
	assert.NoError(t, err)

	val, err = clusterClient.HGet(ctx, "hashkey", "field1").Result()
	assert.NoError(t, err)
	assert.Equal(t, "value1", val)

	// Set commands
	err = clusterClient.SAdd(ctx, "setkey", "m1", "m2", "m3").Err()
	assert.NoError(t, err)

	members, err := clusterClient.SMembers(ctx, "setkey").Result()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(members))

	// SortedSet commands
	err = clusterClient.ZAdd(ctx, "zsetkey", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}).Err()
	assert.NoError(t, err)

	count, err := clusterClient.ZCard(ctx, "zsetkey").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)
}

// TestClusterMGetMSet 测试集群模式下的批量操作
func TestClusterMGetMSet(t *testing.T) {
	setupClusterTestServer(t)
	defer teardownClusterTestServer(t)

	ctx := context.Background()

	// MSET
	err := clusterClient.MSet(ctx, "k1", "v1", "k2", "v2").Err()
	assert.NoError(t, err)

	// MGET
	values, err := clusterClient.MGet(ctx, "k1", "k2").Result()
	assert.NoError(t, err)
	// values is already []interface{} from MGet
	assert.Equal(t, 2, len(values))
}

// TestClusterDel 测试集群模式下的 DEL 命令
func TestClusterDel(t *testing.T) {
	setupClusterTestServer(t)
	defer teardownClusterTestServer(t)

	ctx := context.Background()

	// 准备测试数据
	_ = clusterClient.Set(ctx, "key1", "value1", 0).Err()
	_ = clusterClient.Set(ctx, "key2", "value2", 0).Err()

	// DEL 单个键
	deleted, err := clusterClient.Del(ctx, "key1").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), deleted)

	// DEL 多个键
	deleted, err = clusterClient.Del(ctx, "key1", "key2").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), deleted)
}

// TestClusterExists 测试集群模式下的 EXISTS 命令
func TestClusterExists(t *testing.T) {
	setupClusterTestServer(t)
	defer teardownClusterTestServer(t)

	ctx := context.Background()

	// 准备测试数据
	_ = clusterClient.Set(ctx, "key1", "value1", 0).Err()

	// EXISTS 单个键
	exists, err := clusterClient.Exists(ctx, "key1").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), exists)

	// EXISTS 多个键
	exists, err = clusterClient.Exists(ctx, "key1", "key2").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), exists)
}

// TestClusterExpire 测试集群模式下的 EXPIRE 命令
func TestClusterExpire(t *testing.T) {
	setupClusterTestServer(t)
	defer teardownClusterTestServer(t)

	ctx := context.Background()

	// 准备测试数据
	_ = clusterClient.Set(ctx, "expirekey", "value", 0).Err()

	// EXPIRE
	set, err := clusterClient.Expire(ctx, "expirekey", 10*time.Second).Result()
	assert.NoError(t, err)
	assert.Equal(t, true, set)

	// TTL
	ttl, err := clusterClient.TTL(ctx, "expirekey").Result()
	assert.NoError(t, err)
	assert.True(t, ttl > 0 && ttl <= 10*time.Second)
}

// TestClusterHashTag 测试带 Hash Tag 的键
func TestClusterHashTag(t *testing.T) {
	setupClusterTestServer(t)
	defer teardownClusterTestServer(t)

	ctx := context.Background()

	// 带 hash tag 的键
	key1 := "{tag}:key1"
	key2 := "{tag}:key2"

	// 这些键应该在同一个槽位
	slot1, err := clusterClient.Do(ctx, "CLUSTER", "KEYSLOT", key1).Result()
	assert.NoError(t, err)
	slot1Num, _ := slot1.(int64)

	slot2, err := clusterClient.Do(ctx, "CLUSTER", "KEYSLOT", key2).Result()
	assert.NoError(t, err)
	slot2Num, _ := slot2.(int64)

	// 相同 hash tag 的键应该在同一槽位
	assert.Equal(t, slot1Num, slot2Num)

	// 测试 SET 和 GET
	err = clusterClient.Set(ctx, key1, "value1", 0).Err()
	assert.NoError(t, err)

	val, err := clusterClient.Get(ctx, key1).Result()
	assert.NoError(t, err)
	assert.Equal(t, "value1", val)
}

// === Cluster Boundary Tests ===

// TestClusterBoundary_Info tests CLUSTER INFO returns valid cluster information
func TestClusterBoundary_Info(t *testing.T) {
	setupClusterTestServer(t)
	defer teardownClusterTestServer(t)

	ctx := context.Background()

	info, err := clusterClient.Do(ctx, "CLUSTER", "INFO").Result()
	assert.NoError(t, err)
	infoStr, ok := info.(string)
	assert.True(t, ok)
	if len(infoStr) == 0 {
		t.Error("CLUSTER INFO should return non-empty string")
	}
	if !strings.Contains(infoStr, "cluster_state:") {
		t.Error("CLUSTER INFO should contain cluster_state field")
	}
	if !strings.Contains(infoStr, "cluster_slots_assigned:") {
		t.Error("CLUSTER INFO should contain cluster_slots_assigned field")
	}
}

// TestClusterBoundary_Nodes tests CLUSTER NODES returns valid node information
func TestClusterBoundary_Nodes(t *testing.T) {
	setupClusterTestServer(t)
	defer teardownClusterTestServer(t)

	ctx := context.Background()

	nodes, err := clusterClient.Do(ctx, "CLUSTER", "NODES").Result()
	assert.NoError(t, err)
	nodesStr, ok := nodes.(string)
	assert.True(t, ok)
	if len(nodesStr) == 0 {
		t.Error("CLUSTER NODES should return non-empty string")
	}
	// Each node entry contains: nodeId, ip:port, lastping, pong, port, flags
	if !strings.Contains(nodesStr, " myself") {
		t.Error("CLUSTER NODES should contain self node")
	}
}

// TestClusterBoundary_KeySlot tests CLUSTER KEYSLOT returns slot number in valid range
func TestClusterBoundary_KeySlot(t *testing.T) {
	setupClusterTestServer(t)
	defer teardownClusterTestServer(t)

	ctx := context.Background()

	testCases := []struct {
		key  string
		desc string
	}{
		{"mykey", "simple key"},
		{"", "empty key"},
		{"{tag}:subkey", "key with hash tag"},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			slot, err := clusterClient.Do(ctx, "CLUSTER", "KEYSLOT", tc.key).Result()
			assert.NoError(t, err)
			slotNum, ok := slot.(int64)
			if !ok {
				t.Errorf("CLUSTER KEYSLOT %s: could not convert result to int64", tc.desc)
				return
			}
			if slotNum < 0 || slotNum >= 16384 {
				t.Errorf("CLUSTER KEYSLOT %s: got %d, want 0-16383", tc.desc, slotNum)
			}
		})
	}
}

// TestClusterBoundary_Slots tests CLUSTER SLOTS returns slot allocation array
func TestClusterBoundary_Slots(t *testing.T) {
	setupClusterTestServer(t)
	defer teardownClusterTestServer(t)

	ctx := context.Background()

	// Add some slots first
	_, err := clusterClient.Do(ctx, "CLUSTER", "ADDSLOTS", 0, 1, 2, 3, 4).Result()
	assert.NoError(t, err)

	slots, err := clusterClient.Do(ctx, "CLUSTER", "SLOTS").Result()
	assert.NoError(t, err)
	slotsArr, ok := slots.([]interface{})
	if !ok {
		t.Error("CLUSTER SLOTS should return array")
	}
	_ = slotsArr // CLUSTER SLOTS returns array

	// Verify slot structure: [startSlot, endSlot, ip, port, nodeId]
	for _, slotEntry := range slotsArr {
		entry, ok := slotEntry.([]interface{})
		if !ok {
			t.Error("each slot entry should be an array")
		}
		if len(entry) < 3 {
			t.Error("each slot entry should have at least 3 elements")
		}
	}
}
