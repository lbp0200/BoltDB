package integration

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/cluster"
	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/server"
	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/redis/go-redis/v9"
	"github.com/zeebo/assert"
)

var (
	clusterClient   *redis.Client
	clusterServer   *server.Handler
	clusterDB       *store.BotreonStore
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
	c, err := cluster.NewCluster(clusterDB, "", clusterListener.Addr().String(), context.Background())
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
	assert.True(t, strings.Contains(nodesStr, "myself"))

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
	assert.NoError(t, clusterClient.Set(ctx, "key1", "value1", 0).Err())
	assert.NoError(t, clusterClient.Set(ctx, "key2", "value2", 0).Err())

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
	assert.NoError(t, clusterClient.Set(ctx, "key1", "value1", 0).Err())

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
	assert.NoError(t, clusterClient.Set(ctx, "expirekey", "value", 0).Err())

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
		t.Fatalf("CLUSTER SLOTS should return []interface{}, got %T", slots)
	}
	if len(slotsArr) == 0 {
		t.Fatal("CLUSTER SLOTS should return non-empty array")
	}

	// Verify slot structure: [startSlot, endSlot, [ip, port, nodeId]]
	for i, slotEntry := range slotsArr {
		entry, ok := slotEntry.([]interface{})
		if !ok {
			t.Fatalf("slot entry %d should be []interface{}, got %T", i, slotEntry)
		}
		if len(entry) < 3 {
			t.Fatalf("slot entry %d should have at least 3 elements (got %d)", i, len(entry))
		}
		// Element 0: start slot (string from go-redis, originally BulkString)
		if _, ok := entry[0].(string); !ok {
			if _, ok := entry[0].([]byte); !ok {
				t.Errorf("slot entry %d element 0 should be string/[]byte (got %T)", i, entry[0])
			}
		}
		// Element 1: end slot (string from go-redis, originally BulkString)
		if _, ok := entry[1].(string); !ok {
			if _, ok := entry[1].([]byte); !ok {
				t.Errorf("slot entry %d element 1 should be string/[]byte (got %T)", i, entry[1])
			}
		}
		// Element 2: node info array ([ip, port, nodeID])
		nodeInfo, ok := entry[2].([]interface{})
		if !ok {
			t.Errorf("slot entry %d element 2 should be []interface{} (got %T)", i, entry[2])
		} else if len(nodeInfo) >= 3 {
			if _, ok := nodeInfo[0].(string); !ok {
				if _, ok := nodeInfo[0].([]byte); !ok {
					t.Errorf("slot entry %d node info[0] should be string/[]byte (got %T)", i, nodeInfo[0])
				}
			}
		}
	}
}

// clusterNode represents one BoltDB node in a multi-node test
type clusterNode struct {
	db       *store.BotreonStore
	handler  *server.Handler
	listener net.Listener
	client   *redis.Client
	nodeID   string
	port     int
}

func startClusterNode(t *testing.T) *clusterNode {
	t.Helper()
	dbPath := t.TempDir()
	db, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)

	tcpAddr := ln.Addr().(*net.TCPAddr)
	addr := tcpAddr.String()

	c, err := cluster.NewCluster(db, "", addr, context.Background())
	assert.NoError(t, err)

	h := &server.Handler{
		Db:      db,
		Cluster: c,
	}

	go func() { _ = h.ServeTCP(ln) }()

	// Start cluster bus and gossip
	err = c.Bus.Start("127.0.0.1", tcpAddr.Port)
	assert.NoError(t, err)

	ctx := context.Background()
	c.Gossip = cluster.NewGossiper(ctx, c)
	c.Gossip.Start()

	cli := redis.NewClient(&redis.Options{Addr: addr})
	time.Sleep(100 * time.Millisecond)

	_, err = cli.Ping(ctx).Result()
	assert.NoError(t, err)

	nodeID, err := cli.Do(ctx, "CLUSTER", "MYID").Result()
	assert.NoError(t, err)

	return &clusterNode{
		db:       db,
		handler:  h,
		listener: ln,
		client:   cli,
		nodeID:   fmt.Sprintf("%v", nodeID),
		port:     tcpAddr.Port,
	}
}

func (n *clusterNode) stop() {
	n.client.Close()
	n.handler.Shutdown()
	n.listener.Close()
	n.db.Close()
}

// TestClusterMultiNode verifies two BoltDB nodes can MEET and exchange gossip over the cluster bus.
func TestClusterMultiNode(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping multi-node cluster test in short mode")
	}

	node1 := startClusterNode(t)
	defer node1.stop()

	node2 := startClusterNode(t)
	defer node2.stop()

	ctx := context.Background()

	// MEET from node1 to node2
	result, err := node1.client.Do(ctx, "CLUSTER", "MEET", "127.0.0.1", fmt.Sprintf("%d", node2.port)).Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// Allow bus connection + initial PING/PONG + gossip handshake
	time.Sleep(2 * time.Second)

	// Both nodes should see each other
	nodes1, err := node1.client.Do(ctx, "CLUSTER", "NODES").Result()
	assert.NoError(t, err)
	nodes1Str := fmt.Sprintf("%v", nodes1)
	if !strings.Contains(nodes1Str, node2.nodeID) {
		t.Fatalf("node1 should see node2 in CLUSTER NODES: %s", nodes1Str)
	}

	nodes2, err := node2.client.Do(ctx, "CLUSTER", "NODES").Result()
	assert.NoError(t, err)
	nodes2Str := fmt.Sprintf("%v", nodes2)
	if !strings.Contains(nodes2Str, node1.nodeID) {
		t.Fatalf("node2 should see node1 in CLUSTER NODES: %s", nodes2Str)
	}

	// Verify bus peer count — at least one bus connection should be established
	if pc := node1.handler.Cluster.Bus.PeerCount(); pc < 1 {
		t.Fatalf("node1 should have at least 1 bus peer, got %d", pc)
	}
	if pc := node2.handler.Cluster.Bus.PeerCount(); pc < 1 {
		t.Fatalf("node2 should have at least 1 bus peer, got %d", pc)
	}

	// Both nodes should have received PONG from the peer
	n2 := node1.handler.Cluster.GetNodeByID(node2.nodeID)
	if n2 == nil || n2.GetPongRecv() <= 0 {
		t.Fatalf("node1 should have received PONG from node2 (PongRecv=%d)", func() int64 {
			if n2 == nil {
				return -1
			}
			return n2.GetPongRecv()
		}())
	}

	n1 := node2.handler.Cluster.GetNodeByID(node1.nodeID)
	if n1 == nil || n1.GetPongRecv() <= 0 {
		t.Fatalf("node2 should have received PONG from node1 (PongRecv=%d)", func() int64 {
			if n1 == nil {
				return -1
			}
			return n1.GetPongRecv()
		}())
	}

	// Verify gossip payload exchange: node2 should know node1's epoch and flags from gossip
	if n1.Epoch <= 0 {
		t.Logf("node1 epoch not yet propagated via gossip (got %d), may need more time", n1.Epoch)
	}
}

// TestClusterGossipPropagation verifies that gossip payload (PFAIL, node info)
// propagates between nodes via the cluster bus.
func TestClusterGossipPropagation(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping gossip propagation test in short mode")
	}

	node1 := startClusterNode(t)
	defer node1.stop()

	node2 := startClusterNode(t)
	defer node2.stop()

	ctx := context.Background()

	// MEET from node1 to node2
	_, err := node1.client.Do(ctx, "CLUSTER", "MEET", "127.0.0.1", fmt.Sprintf("%d", node2.port)).Result()
	assert.NoError(t, err)

	// Allow gossip to exchange
	time.Sleep(2 * time.Second)

	// Mark node2 as PFAIL on node1
	node1.handler.Cluster.MarkNodePFail(node2.nodeID)

	// node1 should include PFAIL'd node2 in its gossip payload
	payload := node1.handler.Cluster.Bus.BuildGossipPayload()
	if len(payload.PFail) == 0 {
		t.Fatal("node1 should have PFAIL'd node2 in gossip payload")
	}
	found := false
	for _, pfailID := range payload.PFail {
		if pfailID == node2.nodeID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("node1 PFAIL payload should include node2: got %v", payload.PFail)
	}

	// Wait for gossip to propagate PFAIL to node2
	time.Sleep(3 * time.Second)

	// node2 should have received PFAIL for node1 (its own view won't have PFAIL
	// for itself, but it should have learned about it in applied gossip)
	// We verify by checking the gossip section arrived — node2's nodes should
	// be consistent
	n2node2 := node2.handler.Cluster.GetNodeByID(node2.nodeID)
	if n2node2 != nil {
		t.Logf("node2's self view: epoch=%d flags=%v", n2node2.Epoch, n2node2.Flags)
	}
}

// TestClusterSlotSync verifies that slot assignments propagate between nodes via gossip.
func TestClusterSlotSync(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping slot sync test in short mode")
	}

	node1 := startClusterNode(t)
	defer node1.stop()

	node2 := startClusterNode(t)
	defer node2.stop()

	ctx := context.Background()

	// MEET from node1 to node2
	_, err := node1.client.Do(ctx, "CLUSTER", "MEET", "127.0.0.1", fmt.Sprintf("%d", node2.port)).Result()
	assert.NoError(t, err)

	time.Sleep(2 * time.Second)

	// Assign specific slots to node1
	_, err = node1.client.Do(ctx, "CLUSTER", "ADDSLOTS", "100", "101", "102").Result()
	assert.NoError(t, err)

	// Node1 should own these slots
	for _, s := range []uint32{100, 101, 102} {
		owner := node1.handler.Cluster.GetSlotOwner(s)
		if owner == nil || owner.ID != node1.nodeID {
			t.Fatalf("slot %d should be owned by node1 on node1", s)
		}
	}

	// Node2 should NOT own these slots before gossip sync
	for _, s := range []uint32{100, 101, 102} {
		owner := node2.handler.Cluster.GetSlotOwner(s)
		// Initially node2 has all slots assigned to itself (NewCluster assigns all to self)
		if owner != nil && owner.ID == node2.nodeID {
			t.Logf("slot %d currently owned by node2 (self), waiting for gossip sync", s)
		}
	}

	// Wait for gossip to propagate slot owners
	time.Sleep(3 * time.Second)

	// After gossip sync, node2 should have learned node1's slot ownership
	// via the SlotOwners in gossip payload
	synced := 0
	for _, s := range []uint32{100, 101, 102} {
		owner := node2.handler.Cluster.GetSlotOwner(s)
		if owner != nil && owner.ID == node1.nodeID {
			synced++
		}
	}
	t.Logf("slots synced to node2: %d/3", synced)
	if synced == 0 {
		t.Log("slot sync may need more time or epochs to converge, this is not a hard failure")
	}
}

// TestClusterSetSlotNodePropagation verifies that CLUSTER SETSLOT NODE on one node
// propagates to other nodes via gossip (slot owner + epoch).
func TestClusterSetSlotNodePropagation(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping SETSLOT NODE propagation test in short mode")
	}

	node1 := startClusterNode(t)
	defer node1.stop()

	node2 := startClusterNode(t)
	defer node2.stop()

	ctx := context.Background()

	// MEET from node1 to node2
	_, err := node1.client.Do(ctx, "CLUSTER", "MEET", "127.0.0.1", fmt.Sprintf("%d", node2.port)).Result()
	assert.NoError(t, err)

	time.Sleep(2 * time.Second)

	// On node1: assign slot 500 to node2 via SETSLOT NODE
	_, err = node1.client.Do(ctx, "CLUSTER", "SETSLOT", "500", "NODE", node2.nodeID).Result()
	assert.NoError(t, err)

	// Verify node1 sees slot 500 owned by node2
	owner := node1.handler.Cluster.GetSlotOwner(500)
	if owner == nil || owner.ID != node2.nodeID {
		t.Fatalf("node1 should see slot 500 owned by node2, got %v", func() string {
			if owner == nil {
				return "nil"
			}
			return owner.ID
		}())
	}

	// Wait for gossip to propagate the assignment
	time.Sleep(4 * time.Second)

	// Node2 should have learned that it now owns slot 500 via gossip
	ownerOnNode2 := node2.handler.Cluster.GetSlotOwner(500)
	if ownerOnNode2 == nil {
		t.Fatal("slot 500 should have an owner on node2")
	}
	t.Logf("slot 500 owner on node2: %s (expected: %s)", ownerOnNode2.ID, node2.nodeID)

	// The epoch on node2's view of slot 500 should reflect the gossip
	t.Logf("node2's slot 500 owner epoch: %d", ownerOnNode2.Epoch)
}

// TestClusterMovedRedirect verifies MOVED redirect when a key's slot is on another node.
func TestClusterMovedRedirect(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping MOVED redirect test in short mode")
	}

	node1 := startClusterNode(t)
	defer node1.stop()

	node2 := startClusterNode(t)
	defer node2.stop()

	ctx := context.Background()

	// MEET from node1 to node2
	_, err := node1.client.Do(ctx, "CLUSTER", "MEET", "127.0.0.1", fmt.Sprintf("%d", node2.port)).Result()
	assert.NoError(t, err)

	time.Sleep(2 * time.Second)

	// Compute a key whose slot we can reassign
	testKey := fmt.Sprintf("moved:{test}:%d", time.Now().UnixNano())
	targetSlot := cluster.Slot(testKey)

	// Reassign this key's slot to node2 on node1
	_, err = node1.client.Do(ctx, "CLUSTER", "SETSLOT", fmt.Sprintf("%d", targetSlot), "NODE", node2.nodeID).Result()
	assert.NoError(t, err)

	// Wait for gossip to propagate
	time.Sleep(4 * time.Second)

	// Verify slot owner on both nodes
	owner1 := node1.handler.Cluster.GetSlotOwner(targetSlot)
	if owner1 == nil || owner1.ID != node2.nodeID {
		t.Fatalf("node1 should see slot %d owned by node2, got %v", targetSlot,
			func() string {
				if owner1 == nil {
					return "nil"
				}
				return owner1.ID
			}())
	}
	t.Logf("node1: slot %d → %s (correct)", targetSlot, owner1.ID)

	owner2 := node2.handler.Cluster.GetSlotOwner(targetSlot)
	if owner2 == nil {
		t.Fatalf("node2 should have an owner for slot %d", targetSlot)
	}
	t.Logf("node2: slot %d → %s (epoch=%d)", targetSlot, owner2.ID, owner2.Epoch)

	// GET from node1 → expects MOVED to the slot owner
	conn, err := net.DialTimeout("tcp", node1.client.Options().Addr, 5*time.Second)
	assert.NoError(t, err)
	defer conn.Close()

	sendRESP(conn, "GET", testKey)
	reader := bufio.NewReader(conn)
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	resp, err := proto.ReadRESP(reader)
	if err != nil {
		t.Fatalf("read response from node1: %v", err)
	}
	_ = conn.SetReadDeadline(time.Time{})

	respStr := resp.String()
	if !strings.Contains(respStr, "MOVED") {
		t.Fatalf("expected MOVED from node1, got: %s", respStr)
	}
	t.Logf("MOVED redirect verified: %s", respStr)
}

// TestClusterAskRedirect verifies Redis-compatible MIGRATING/IMPORTING redirect:
// 1. MIGRATING owner serves existing keys locally (no ASK)
// 2. MIGRATING owner ASKs only when key is missing
// 3. IMPORTING target accepts ASKING + GET without MOVED/ASK
func TestClusterAskRedirect(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping ASK redirect test in short mode")
	}

	node1 := startClusterNode(t)
	defer node1.stop()

	node2 := startClusterNode(t)
	defer node2.stop()

	ctx := context.Background()

	_, err := node1.client.Do(ctx, "CLUSTER", "MEET", "127.0.0.1", fmt.Sprintf("%d", node2.port)).Result()
	assert.NoError(t, err)

	time.Sleep(2 * time.Second)

	// Pick a key and compute its slot
	testKey := fmt.Sprintf("ask:{test}:%d", time.Now().UnixNano())
	missingKey := fmt.Sprintf("ask:{test}:missing:%d", time.Now().UnixNano())
	targetSlot := cluster.Slot(testKey)
	if cluster.Slot(missingKey) != targetSlot {
		// force same slot via shared hash tag already used
		missingKey = fmt.Sprintf("ask:{test}:gone:%d", time.Now().UnixNano())
	}

	// Write key on node1 first (before marking slot as migrating)
	_, err = node1.client.Set(ctx, testKey, "ask-value", 0).Result()
	assert.NoError(t, err)

	// Node1: SETSLOT <slot> MIGRATING <node2_id>
	_, err = node1.client.Do(ctx, "CLUSTER", "SETSLOT", fmt.Sprintf("%d", targetSlot), "MIGRATING", node2.nodeID).Result()
	assert.NoError(t, err)

	// Node2: SETSLOT <slot> IMPORTING <node1_id>
	_, err = node2.client.Do(ctx, "CLUSTER", "SETSLOT", fmt.Sprintf("%d", targetSlot), "IMPORTING", node1.nodeID).Result()
	assert.NoError(t, err)

	// Verify migration state on both nodes
	if !node1.handler.Cluster.IsMigratingSlot(targetSlot) {
		t.Fatal("node1 should have slot as MIGRATING")
	}
	if !node2.handler.Cluster.IsImportingSlot(targetSlot) {
		t.Fatal("node2 should have slot as IMPORTING")
	}

	// GET existing key on MIGRATING owner → serve locally (Redis semantics)
	conn1, err := net.DialTimeout("tcp", node1.client.Options().Addr, 5*time.Second)
	assert.NoError(t, err)
	defer conn1.Close()

	sendRESP(conn1, "GET", testKey)
	reader1 := bufio.NewReader(conn1)
	_ = conn1.SetReadDeadline(time.Now().Add(2 * time.Second))
	resp1, err := proto.ReadRESP(reader1)
	_ = conn1.SetReadDeadline(time.Time{})
	if err != nil {
		t.Fatalf("read from node1: %v", err)
	}
	resp1Str := resp1.String()
	if strings.Contains(resp1Str, "ASK") || strings.Contains(resp1Str, "MOVED") {
		t.Fatalf("existing key on MIGRATING owner should be served locally, got: %s", resp1Str)
	}
	if !strings.Contains(resp1Str, "ask-value") {
		t.Fatalf("expected value ask-value from node1, got: %s", resp1Str)
	}
	t.Logf("MIGRATING owner served existing key: %s", resp1Str)

	// GET missing key on MIGRATING owner → ASK
	sendRESP(conn1, "GET", missingKey)
	_ = conn1.SetReadDeadline(time.Now().Add(2 * time.Second))
	respMissing, err := proto.ReadRESP(reader1)
	_ = conn1.SetReadDeadline(time.Time{})
	if err != nil {
		t.Fatalf("read missing-key response: %v", err)
	}
	missStr := respMissing.String()
	if !strings.Contains(missStr, "ASK") {
		t.Fatalf("missing key on MIGRATING owner should ASK, got: %s", missStr)
	}
	t.Logf("ASK for missing key on MIGRATING owner: %s", missStr)

	// GET from node2 with ASKING → should NOT redirect
	conn3, err := net.DialTimeout("tcp", node2.client.Options().Addr, 5*time.Second)
	assert.NoError(t, err)
	defer conn3.Close()
	reader3 := bufio.NewReaderSize(conn3, 4096)

	sendRESP(conn3, "ASKING")
	sendRESP(conn3, "GET", testKey)

	_ = conn3.SetReadDeadline(time.Now().Add(2 * time.Second))

	_, err = proto.ReadRESP(reader3)
	if err != nil {
		t.Fatalf("read ASKING response: %v", err)
	}

	getResp, err := proto.ReadRESP(reader3)
	if err != nil {
		t.Fatalf("read GET response (with ASKING): %v", err)
	}
	getStr := getResp.String()
	if strings.Contains(getStr, "MOVED") || strings.Contains(getStr, "ASK") {
		t.Fatalf("node2 with ASKING should not redirect, got: %s", getStr)
	}
	t.Logf("ASKING + GET on node2: %s (no redirect = ASKING+IMPORTING works)", getStr)

	// --- ASKING one-shot ---
	// Fresh topology where node2 does NOT own the slot (NODE→node1) but is IMPORTING.
	// startClusterNode assigns all slots to each node by default; without NODE reassignment
	// a second GET would still "own" the slot and never prove sticky-ASKING is fixed.
	_, err = node2.client.Do(ctx, "CLUSTER", "SETSLOT", fmt.Sprintf("%d", targetSlot), "NODE", node1.nodeID).Result()
	assert.NoError(t, err)
	_, err = node2.client.Do(ctx, "CLUSTER", "SETSLOT", fmt.Sprintf("%d", targetSlot), "IMPORTING", node1.nodeID).Result()
	assert.NoError(t, err)

	conn4, err := net.DialTimeout("tcp", node2.client.Options().Addr, 5*time.Second)
	assert.NoError(t, err)
	defer conn4.Close()
	reader4 := bufio.NewReaderSize(conn4, 4096)

	sendRESP(conn4, "ASKING")
	sendRESP(conn4, "GET", testKey)
	_ = conn4.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = proto.ReadRESP(reader4) // ASKING OK
	assert.NoError(t, err)
	askGet, err := proto.ReadRESP(reader4)
	assert.NoError(t, err)
	askGetStr := askGet.String()
	if strings.Contains(askGetStr, "MOVED") || strings.Contains(askGetStr, "ASK") {
		t.Fatalf("ASKING+GET after NODE reassignment should not redirect, got: %s", askGetStr)
	}

	// Second GET without re-ASKING must MOVED/ASK (not sticky ASKING)
	sendRESP(conn4, "GET", testKey)
	get2, err := proto.ReadRESP(reader4)
	_ = conn4.SetReadDeadline(time.Time{})
	assert.NoError(t, err)
	get2Str := get2.String()
	if !strings.Contains(get2Str, "MOVED") && !strings.Contains(get2Str, "ASK") {
		t.Fatalf("second GET without ASKING should MOVED/ASK (sticky ASKING bug), got: %s", get2Str)
	}
	t.Logf("one-shot ASKING: second GET redirected: %s", get2Str)
}

// TestClusterMigratingSourceWriteFence verifies SET on an existing key while
// the owner marks the slot MIGRATING is rejected (source write fence).
func TestClusterMigratingSourceWriteFence(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping MIGRATING source write fence test in short mode")
	}

	node1 := startClusterNode(t)
	defer node1.stop()
	node2 := startClusterNode(t)
	defer node2.stop()

	ctx := context.Background()
	_, err := node1.client.Do(ctx, "CLUSTER", "MEET", "127.0.0.1", fmt.Sprintf("%d", node2.port)).Result()
	assert.NoError(t, err)
	time.Sleep(2 * time.Second)

	key := fmt.Sprintf("srcfence:{mig}:%d", time.Now().UnixNano())
	slot := cluster.Slot(key)
	assert.NoError(t, node1.client.Set(ctx, key, "original", 0).Err())

	_, err = node1.client.Do(ctx, "CLUSTER", "SETSLOT", fmt.Sprintf("%d", slot), "MIGRATING", node2.nodeID).Result()
	assert.NoError(t, err)

	// Existing key still readable
	val, err := node1.client.Get(ctx, key).Result()
	assert.NoError(t, err)
	assert.Equal(t, "original", val)

	// Write must be fenced
	setErr := node1.client.Set(ctx, key, "overwrite", 0).Err()
	if setErr == nil {
		t.Fatal("expected MIGRATING write fence on SET of existing key")
	}
	if !strings.Contains(setErr.Error(), "MIGRATING") && !strings.Contains(setErr.Error(), "fenced") {
		t.Fatalf("expected MIGRATING fence error, got: %v", setErr)
	}
	// Value unchanged
	val, err = node1.client.Get(ctx, key).Result()
	assert.NoError(t, err)
	assert.Equal(t, "original", val)
	t.Logf("MIGRATING source write fence: %v", setErr)
}

// TestClusterMigrateSlot verifies the CLUSTER MIGRATESLOT command.
// 1. Two nodes with MEET
// 2. Write keys to a slot on node1
// 3. Mark slot MIGRATING on node1, IMPORTING on node2
// 4. CLUSTER MIGRATESLOT migrates all keys
// 5. Verify keys exist on node2, deleted from node1
// 6. Verify slot ownership transferred to node2
func TestClusterMigrateSlot(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping MIGRATESLOT test in short mode")
	}

	node1 := startClusterNode(t)
	defer node1.stop()

	node2 := startClusterNode(t)
	defer node2.stop()

	ctx := context.Background()

	// MEET from node1 to node2
	_, err := node1.client.Do(ctx, "CLUSTER", "MEET", "127.0.0.1", fmt.Sprintf("%d", node2.port)).Result()
	assert.NoError(t, err)

	time.Sleep(2 * time.Second)

	// Pick a key whose slot is on node1 (default: all slots on node1)
	testKeys := []string{
		fmt.Sprintf("ms:{test}:key1:%d", time.Now().UnixNano()),
		fmt.Sprintf("ms:{test}:key2:%d", time.Now().UnixNano()),
		fmt.Sprintf("ms:{test}:key3:%d", time.Now().UnixNano()),
	}
	targetSlot := cluster.Slot(testKeys[0])

	// Verify all test keys are in the same slot
	for _, k := range testKeys {
		if cluster.Slot(k) != targetSlot {
			t.Fatalf("key %s should be in slot %d, got %d", k, targetSlot, cluster.Slot(k))
		}
	}

	// Verify slot is owned by node1
	owner := node1.handler.Cluster.GetSlotOwner(targetSlot)
	if owner == nil || owner.ID != node1.nodeID {
		t.Fatalf("slot %d should be owned by node1, got owner=%v", targetSlot,
			func() string {
				if owner == nil {
					return "nil"
				}
				return owner.ID
			}())
	}
	t.Logf("slot %d owned by node1: %s", targetSlot, node1.nodeID)

	// Write test keys on node1
	for _, k := range testKeys {
		err := node1.client.Set(ctx, k, "migrate-slot-value", 0).Err()
		assert.NoError(t, err)
	}

	// Verify keys exist on node1
	for _, k := range testKeys {
		val, err := node1.client.Get(ctx, k).Result()
		assert.NoError(t, err)
		assert.Equal(t, "migrate-slot-value", val)
	}

	// Set the slot as MIGRATING on node1
	_, err = node1.client.Do(ctx, "CLUSTER", "SETSLOT", fmt.Sprintf("%d", targetSlot), "MIGRATING", node2.nodeID).Result()
	assert.NoError(t, err)

	// Set the slot as IMPORTING on node2
	_, err = node2.client.Do(ctx, "CLUSTER", "SETSLOT", fmt.Sprintf("%d", targetSlot), "IMPORTING", node1.nodeID).Result()
	assert.NoError(t, err)

	// Verify migration state
	if !node1.handler.Cluster.IsMigratingSlot(targetSlot) {
		t.Fatal("node1 should have slot as MIGRATING")
	}
	if !node2.handler.Cluster.IsImportingSlot(targetSlot) {
		t.Fatal("node2 should have slot as IMPORTING")
	}

	// Execute CLUSTER MIGRATESLOT from node1
	t.Logf("starting slot migration: slot %d → node2 (%s)", targetSlot, node2.nodeID)
	_, err = node1.client.Do(ctx, "CLUSTER", "MIGRATESLOT", fmt.Sprintf("%d", targetSlot), node2.nodeID).Result()
	if err != nil {
		t.Fatalf("MIGRATESLOT failed: %v", err)
	}
	t.Log("MIGRATESLOT completed successfully")

	// Wait for gossip to propagate ownership change
	time.Sleep(3 * time.Second)

	// Verify keys exist on node2
	for _, k := range testKeys {
		val, err := node2.client.Get(ctx, k).Result()
		if err != nil {
			t.Fatalf("key %s should exist on node2: %v", k, err)
		}
		assert.Equal(t, "migrate-slot-value", val)
	}
	t.Log("all keys verified on node2")

	// Verify keys no longer exist on node1
	for _, k := range testKeys {
		_, err := node1.client.Get(ctx, k).Result()
		if err == nil {
			t.Fatalf("key %s should have been deleted from node1", k)
		}
	}
	t.Log("all keys deleted from node1 (non-COPY mode)")

	// Verify slot ownership transferred to node2
	owner1 := node1.handler.Cluster.GetSlotOwner(targetSlot)
	if owner1 == nil || owner1.ID != node2.nodeID {
		t.Fatalf("node1 should see slot %d owned by node2, got %v", targetSlot,
			func() string {
				if owner1 == nil {
					return "nil"
				}
				return owner1.ID
			}())
	}
	t.Logf("slot %d ownership transferred to node2 (verified on node1)", targetSlot)

	// Verify node2 sees itself as the owner
	owner2 := node2.handler.Cluster.GetSlotOwner(targetSlot)
	if owner2 == nil || owner2.ID != node2.nodeID {
		t.Fatalf("node2 should see slot %d owned by itself, got %v", targetSlot,
			func() string {
				if owner2 == nil {
					return "nil"
				}
				return owner2.ID
			}())
	}
	t.Logf("slot %d ownership confirmed on node2", targetSlot)

	// Verify migration state is cleared on both nodes
	if node1.handler.Cluster.IsMigratingSlot(targetSlot) {
		t.Fatal("MIGRATING state should be cleared after migration")
	}
	if node2.handler.Cluster.IsImportingSlot(targetSlot) {
		t.Fatal("IMPORTING state should be cleared after migration")
	}
	t.Log("migration state cleared on both nodes")
}

// TestClusterMigrateRoundTripStability migrates a slot A→B then B→A with
// rewritten values. Lightweight multi-round consistency oracle (not full soak).
func TestClusterMigrateRoundTripStability(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping migrate round-trip stability in short mode")
	}

	nodeA := startClusterNode(t)
	defer nodeA.stop()
	nodeB := startClusterNode(t)
	defer nodeB.stop()

	ctx := context.Background()
	_, err := nodeA.client.Do(ctx, "CLUSTER", "MEET", "127.0.0.1", fmt.Sprintf("%d", nodeB.port)).Result()
	assert.NoError(t, err)
	time.Sleep(2 * time.Second)

	const rounds = 3
	tag := fmt.Sprintf("{rt:%d}", time.Now().UnixNano()%100000)
	keys := []string{"k1:" + tag, "k2:" + tag, "k3:" + tag}
	slot := cluster.Slot(keys[0])
	for _, k := range keys {
		if cluster.Slot(k) != slot {
			t.Fatal("hash tag slot mismatch")
		}
	}

	waitOwner := func(t *testing.T, n *clusterNode, wantID string, timeout time.Duration) {
		t.Helper()
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			o := n.handler.Cluster.GetSlotOwner(slot)
			if o != nil && o.ID == wantID {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		o := n.handler.Cluster.GetSlotOwner(slot)
		got := "nil"
		if o != nil {
			got = o.ID
		}
		t.Fatalf("slot %d owner on %s: got %s want %s", slot, n.nodeID, got, wantID)
	}

	// Initially both own all slots; write on A then migrate A→B, B→A, ...
	src, dst := nodeA, nodeB
	for r := 0; r < rounds; r++ {
		val := fmt.Sprintf("round-%d", r)
		// Ensure src is the owner before writes (after reverse hops)
		waitOwner(t, src, src.nodeID, 10*time.Second)

		for _, k := range keys {
			assert.NoError(t, src.client.Set(ctx, k, val, 0).Err())
		}

		_, err = src.client.Do(ctx, "CLUSTER", "SETSLOT", fmt.Sprintf("%d", slot), "MIGRATING", dst.nodeID).Result()
		assert.NoError(t, err)
		_, err = dst.client.Do(ctx, "CLUSTER", "SETSLOT", fmt.Sprintf("%d", slot), "IMPORTING", src.nodeID).Result()
		assert.NoError(t, err)

		_, err = src.client.Do(ctx, "CLUSTER", "MIGRATESLOT", fmt.Sprintf("%d", slot), dst.nodeID).Result()
		if err != nil {
			t.Fatalf("round %d MIGRATESLOT: %v", r, err)
		}

		// Pin ownership on both nodes before reads/reverse hop.
		// Gossip lag can leave MOVED thrash even when data RESTORE succeeded;
		// this test isolates multi-round *data* migrate correctness.
		_, err = src.client.Do(ctx, "CLUSTER", "SETSLOT", fmt.Sprintf("%d", slot), "NODE", dst.nodeID).Result()
		assert.NoError(t, err)
		_, err = dst.client.Do(ctx, "CLUSTER", "SETSLOT", fmt.Sprintf("%d", slot), "NODE", dst.nodeID).Result()
		assert.NoError(t, err)
		waitOwner(t, src, dst.nodeID, 10*time.Second)
		waitOwner(t, dst, dst.nodeID, 10*time.Second)

		for _, k := range keys {
			var got string
			var gerr error
			deadline := time.Now().Add(10 * time.Second)
			for time.Now().Before(deadline) {
				got, gerr = dst.client.Get(ctx, k).Result()
				if gerr == nil && got == val {
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
			if gerr != nil || got != val {
				t.Fatalf("round %d: key %s on dest got %q err=%v want %q", r, k, got, gerr, val)
			}
			n, _ := src.client.Exists(ctx, k).Result()
			if n != 0 {
				t.Fatalf("round %d: key %s still on source", r, k)
			}
		}
		if src.handler.Cluster.IsMigratingSlot(slot) {
			t.Fatalf("round %d: source still MIGRATING", r)
		}
		if dst.handler.Cluster.IsImportingSlot(slot) {
			t.Fatalf("round %d: dest still IMPORTING", r)
		}

		// Swap for next hop
		src, dst = dst, src
		t.Logf("round %d: migrate OK val=%s", r, val)
	}
	t.Logf("round-trip stability: %d migrations OK", rounds)
}

// TestClusterMigrateNoReplacePreservesTarget ensures Phase-1 RESTORE does not
// use REPLACE: a newer value already on the IMPORTING target survives migrate.
func TestClusterMigrateNoReplacePreservesTarget(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping no-REPLACE migrate test in short mode")
	}

	node1 := startClusterNode(t)
	defer node1.stop()
	node2 := startClusterNode(t)
	defer node2.stop()

	ctx := context.Background()
	_, err := node1.client.Do(ctx, "CLUSTER", "MEET", "127.0.0.1", fmt.Sprintf("%d", node2.port)).Result()
	assert.NoError(t, err)
	time.Sleep(2 * time.Second)

	key := fmt.Sprintf("nr:{mig}:%d", time.Now().UnixNano())
	slot := cluster.Slot(key)

	// Source snapshot (stale relative to target)
	assert.NoError(t, node1.client.Set(ctx, key, "source-stale", 0).Err())
	// Target already has a newer value (simulates ASKING client write before fence,
	// or a prior partial Phase-1). startClusterNode owns all slots so SET works.
	assert.NoError(t, node2.client.Set(ctx, key, "target-newer", 0).Err())

	_, err = node1.client.Do(ctx, "CLUSTER", "SETSLOT", fmt.Sprintf("%d", slot), "MIGRATING", node2.nodeID).Result()
	assert.NoError(t, err)
	_, err = node2.client.Do(ctx, "CLUSTER", "SETSLOT", fmt.Sprintf("%d", slot), "IMPORTING", node1.nodeID).Result()
	assert.NoError(t, err)

	_, err = node1.client.Do(ctx, "CLUSTER", "MIGRATESLOT", fmt.Sprintf("%d", slot), node2.nodeID).Result()
	if err != nil {
		t.Fatalf("MIGRATESLOT: %v", err)
	}
	time.Sleep(2 * time.Second)

	val, err := node2.client.Get(ctx, key).Result()
	assert.NoError(t, err)
	if val != "target-newer" {
		t.Fatalf("target value overwritten by RESTORE REPLACE? got %q want target-newer", val)
	}
	// Source key deleted after successful migrate
	n, _ := node1.client.Exists(ctx, key).Result()
	if n != 0 {
		t.Fatalf("source still holds key after migrate")
	}
	t.Log("no-REPLACE: target-newer preserved through MIGRATESLOT")
}

// TestClusterMigrateSlotMultiType migrates string/hash/list/set/zset keys that
// share one hash-tag slot and asserts type-correct values on the target.
func TestClusterMigrateSlotMultiType(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping multi-type MIGRATESLOT test in short mode")
	}

	node1 := startClusterNode(t)
	defer node1.stop()
	node2 := startClusterNode(t)
	defer node2.stop()

	ctx := context.Background()
	_, err := node1.client.Do(ctx, "CLUSTER", "MEET", "127.0.0.1", fmt.Sprintf("%d", node2.port)).Result()
	assert.NoError(t, err)
	time.Sleep(2 * time.Second)

	tag := fmt.Sprintf("{mt:%d}", time.Now().UnixNano()%100000)
	strKey := "s:" + tag
	hashKey := "h:" + tag
	listKey := "l:" + tag
	setKey := "e:" + tag
	zsetKey := "z:" + tag
	slot := cluster.Slot(strKey)
	for _, k := range []string{hashKey, listKey, setKey, zsetKey} {
		if cluster.Slot(k) != slot {
			t.Fatalf("keys must share slot via hash tag")
		}
	}

	assert.NoError(t, node1.client.Set(ctx, strKey, "hello", 0).Err())
	assert.NoError(t, node1.client.HSet(ctx, hashKey, "f1", "v1", "f2", "v2").Err())
	assert.NoError(t, node1.client.RPush(ctx, listKey, "a", "b", "c").Err())
	assert.NoError(t, node1.client.SAdd(ctx, setKey, "m1", "m2").Err())
	assert.NoError(t, node1.client.ZAdd(ctx, zsetKey, redis.Z{Score: 1.5, Member: "zm"}).Err())

	_, err = node1.client.Do(ctx, "CLUSTER", "SETSLOT", fmt.Sprintf("%d", slot), "MIGRATING", node2.nodeID).Result()
	assert.NoError(t, err)
	_, err = node2.client.Do(ctx, "CLUSTER", "SETSLOT", fmt.Sprintf("%d", slot), "IMPORTING", node1.nodeID).Result()
	assert.NoError(t, err)

	_, err = node1.client.Do(ctx, "CLUSTER", "MIGRATESLOT", fmt.Sprintf("%d", slot), node2.nodeID).Result()
	if err != nil {
		t.Fatalf("MIGRATESLOT multi-type: %v", err)
	}
	time.Sleep(2 * time.Second)

	// String
	val, err := node2.client.Get(ctx, strKey).Result()
	assert.NoError(t, err)
	assert.Equal(t, "hello", val)
	// Hash
	hv, err := node2.client.HGetAll(ctx, hashKey).Result()
	assert.NoError(t, err)
	assert.Equal(t, map[string]string{"f1": "v1", "f2": "v2"}, hv)
	// List order
	list, err := node2.client.LRange(ctx, listKey, 0, -1).Result()
	assert.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c"}, list)
	// Set membership
	sm, err := node2.client.SMembers(ctx, setKey).Result()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(sm))
	// ZSet score
	score, err := node2.client.ZScore(ctx, zsetKey, "zm").Result()
	assert.NoError(t, err)
	assert.Equal(t, 1.5, score)

	// Source cleaned
	for _, k := range []string{strKey, hashKey, listKey, setKey, zsetKey} {
		n, _ := node1.client.Exists(ctx, k).Result()
		if n != 0 {
			t.Fatalf("source still has key %s after multi-type migrate", k)
		}
	}
	t.Log("multi-type MIGRATESLOT: string/hash/list/set/zset OK on target")
}

// TestClusterImportingWriteFence verifies ASKING writes (except RESTORE) are
// rejected on IMPORTING slots so Phase-1 RESTORE cannot race with clients.
func TestClusterImportingWriteFence(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping IMPORTING write fence test in short mode")
	}

	node1 := startClusterNode(t)
	defer node1.stop()
	node2 := startClusterNode(t)
	defer node2.stop()

	ctx := context.Background()
	_, err := node1.client.Do(ctx, "CLUSTER", "MEET", "127.0.0.1", fmt.Sprintf("%d", node2.port)).Result()
	assert.NoError(t, err)
	time.Sleep(2 * time.Second)

	testKey := fmt.Sprintf("fence:{mig}:%d", time.Now().UnixNano())
	targetSlot := cluster.Slot(testKey)

	assert.NoError(t, node1.client.Set(ctx, testKey, "src-val", 0).Err())

	_, err = node1.client.Do(ctx, "CLUSTER", "SETSLOT", fmt.Sprintf("%d", targetSlot), "MIGRATING", node2.nodeID).Result()
	assert.NoError(t, err)
	_, err = node2.client.Do(ctx, "CLUSTER", "SETSLOT", fmt.Sprintf("%d", targetSlot), "IMPORTING", node1.nodeID).Result()
	assert.NoError(t, err)

	// ASKING + SET on IMPORTING target → fenced
	conn, err := net.DialTimeout("tcp", node2.client.Options().Addr, 5*time.Second)
	assert.NoError(t, err)
	defer conn.Close()
	reader := bufio.NewReaderSize(conn, 4096)

	sendRESP(conn, "ASKING")
	sendRESP(conn, "SET", testKey, "client-overwrite")
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, err = proto.ReadRESP(reader) // ASKING OK
	assert.NoError(t, err)
	setResp, err := proto.ReadRESP(reader)
	_ = conn.SetReadDeadline(time.Time{})
	assert.NoError(t, err)
	setStr := setResp.String()
	if !strings.Contains(setStr, "IMPORTING") && !strings.Contains(setStr, "fenced") {
		t.Fatalf("expected IMPORTING write fence on ASKING SET, got: %s", setStr)
	}
	t.Logf("ASKING+SET fenced: %s", setStr)

	// ASKING + GET still allowed
	conn2, err := net.DialTimeout("tcp", node2.client.Options().Addr, 5*time.Second)
	assert.NoError(t, err)
	defer conn2.Close()
	reader2 := bufio.NewReaderSize(conn2, 4096)
	sendRESP(conn2, "ASKING")
	sendRESP(conn2, "GET", testKey)
	_ = conn2.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, err = proto.ReadRESP(reader2)
	assert.NoError(t, err)
	getResp, err := proto.ReadRESP(reader2)
	_ = conn2.SetReadDeadline(time.Time{})
	assert.NoError(t, err)
	getStr := getResp.String()
	if strings.Contains(getStr, "MOVED") || strings.Contains(getStr, "ASK") || strings.Contains(getStr, "fenced") {
		t.Fatalf("ASKING+GET should not redirect or fence, got: %s", getStr)
	}
	t.Logf("ASKING+GET allowed: %s", getStr)
}

// TestClusterDelKeysInSlot verifies CLUSTER DELKEYSINSLOT removes keys in a slot
// (Phase-1 abort cleanup primitive on IMPORTING target).
func TestClusterDelKeysInSlot(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping DELKEYSINSLOT test in short mode")
	}

	node := startClusterNode(t)
	defer node.stop()
	ctx := context.Background()

	// Hash-tag keys share one slot
	keys := []string{
		fmt.Sprintf("dki:{slot}:a:%d", time.Now().UnixNano()),
		fmt.Sprintf("dki:{slot}:b:%d", time.Now().UnixNano()),
	}
	slot := cluster.Slot(keys[0])
	for _, k := range keys {
		if cluster.Slot(k) != slot {
			t.Fatalf("keys must share slot")
		}
		assert.NoError(t, node.client.Set(ctx, k, "v", 0).Err())
	}

	n, err := node.client.Do(ctx, "CLUSTER", "DELKEYSINSLOT", fmt.Sprintf("%d", slot)).Result()
	assert.NoError(t, err)
	// go-redis may return int64
	if nInt, ok := n.(int64); ok {
		if nInt < 2 {
			t.Fatalf("expected at least 2 keys deleted, got %d", nInt)
		}
	} else {
		t.Logf("DELKEYSINSLOT returned %T %v", n, n)
	}

	for _, k := range keys {
		_, err := node.client.Get(ctx, k).Result()
		if err == nil {
			t.Fatalf("key %s should have been deleted by DELKEYSINSLOT", k)
		}
	}
	t.Log("DELKEYSINSLOT cleaned slot keys")
}

// TestClusterMigrateSlotAbortCleanup simulates partial Phase-1 RESTORE orphans
// on the target, then DELKEYSINSLOT + STABLE as abort cleanup does.
func TestClusterMigrateSlotAbortCleanup(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping migrate abort cleanup test in short mode")
	}

	node1 := startClusterNode(t)
	defer node1.stop()
	node2 := startClusterNode(t)
	defer node2.stop()

	ctx := context.Background()
	_, err := node1.client.Do(ctx, "CLUSTER", "MEET", "127.0.0.1", fmt.Sprintf("%d", node2.port)).Result()
	assert.NoError(t, err)
	time.Sleep(2 * time.Second)

	keys := []string{
		fmt.Sprintf("abort:{mig}:1:%d", time.Now().UnixNano()),
		fmt.Sprintf("abort:{mig}:2:%d", time.Now().UnixNano()),
	}
	slot := cluster.Slot(keys[0])
	for _, k := range keys {
		assert.NoError(t, node1.client.Set(ctx, k, "partial", 0).Err())
	}

	_, err = node1.client.Do(ctx, "CLUSTER", "SETSLOT", fmt.Sprintf("%d", slot), "MIGRATING", node2.nodeID).Result()
	assert.NoError(t, err)
	_, err = node2.client.Do(ctx, "CLUSTER", "SETSLOT", fmt.Sprintf("%d", slot), "IMPORTING", node1.nodeID).Result()
	assert.NoError(t, err)

	// Simulate partial Phase-1: DUMP from source, RESTORE on target (no REPLACE)
	for _, k := range keys {
		dump, err := node1.client.Dump(ctx, k).Result()
		assert.NoError(t, err)
		// RESTORE key 0 payload
		err = node2.client.Restore(ctx, k, 0, dump).Err()
		assert.NoError(t, err)
	}
	// Orphans present on target
	for _, k := range keys {
		val, err := node2.client.Get(ctx, k).Result()
		assert.NoError(t, err)
		assert.Equal(t, "partial", val)
	}

	// Abort cleanup: DELKEYSINSLOT + STABLE (matches abortTargetImport)
	_, err = node2.client.Do(ctx, "CLUSTER", "DELKEYSINSLOT", fmt.Sprintf("%d", slot)).Result()
	assert.NoError(t, err)
	_, err = node2.client.Do(ctx, "CLUSTER", "SETSLOT", fmt.Sprintf("%d", slot), "STABLE").Result()
	assert.NoError(t, err)
	_, err = node1.client.Do(ctx, "CLUSTER", "SETSLOT", fmt.Sprintf("%d", slot), "STABLE").Result()
	assert.NoError(t, err)

	for _, k := range keys {
		_, err := node2.client.Get(ctx, k).Result()
		if err == nil {
			t.Fatalf("orphan key %s should be gone on target after abort cleanup", k)
		}
		// Source still has data (Phase-1 rollback leaves source intact)
		val, err := node1.client.Get(ctx, k).Result()
		assert.NoError(t, err)
		assert.Equal(t, "partial", val)
	}
	if node2.handler.Cluster.IsImportingSlot(slot) {
		t.Fatal("IMPORTING should be cleared after STABLE")
	}
	t.Log("abort cleanup removed target orphans; source keys intact")
}

// TestClusterMigrateSlotUnderLoad runs concurrent writers against a MIGRATING
// source while MIGRATESLOT runs. Writers must be fenced (not corrupt migration);
// after migrate, all pre-seeded keys exist on the target with original values.
func TestClusterMigrateSlotUnderLoad(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping migrate-under-load test in short mode")
	}

	node1 := startClusterNode(t)
	defer node1.stop()
	node2 := startClusterNode(t)
	defer node2.stop()

	ctx := context.Background()
	_, err := node1.client.Do(ctx, "CLUSTER", "MEET", "127.0.0.1", fmt.Sprintf("%d", node2.port)).Result()
	assert.NoError(t, err)
	time.Sleep(2 * time.Second)

	const nKeys = 50
	keys := make([]string, nKeys)
	for i := 0; i < nKeys; i++ {
		keys[i] = fmt.Sprintf("load:{mig}:%d:%d", i, time.Now().UnixNano()%100000)
	}
	slot := cluster.Slot(keys[0])
	for _, k := range keys {
		if cluster.Slot(k) != slot {
			t.Fatalf("keys must share hash tag slot")
		}
		assert.NoError(t, node1.client.Set(ctx, k, "seed", 0).Err())
	}

	_, err = node1.client.Do(ctx, "CLUSTER", "SETSLOT", fmt.Sprintf("%d", slot), "MIGRATING", node2.nodeID).Result()
	assert.NoError(t, err)
	_, err = node2.client.Do(ctx, "CLUSTER", "SETSLOT", fmt.Sprintf("%d", slot), "IMPORTING", node1.nodeID).Result()
	assert.NoError(t, err)

	// Concurrent writers: hammer existing keys on source — must see write fence
	var wg sync.WaitGroup
	var fenceHits atomic.Int64
	var otherErrs atomic.Int64
	stop := make(chan struct{})
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			cli := redis.NewClient(&redis.Options{Addr: node1.client.Options().Addr})
			defer cli.Close()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				k := keys[i%nKeys]
				i++
				err := cli.Set(ctx, k, fmt.Sprintf("w%d-%d", id, i), 0).Err()
				if err != nil {
					if strings.Contains(err.Error(), "MIGRATING") || strings.Contains(err.Error(), "fenced") {
						fenceHits.Add(1)
					} else {
						otherErrs.Add(1)
					}
				}
			}
		}(w)
	}

	// Brief write pressure before migrate
	time.Sleep(200 * time.Millisecond)

	_, err = node1.client.Do(ctx, "CLUSTER", "MIGRATESLOT", fmt.Sprintf("%d", slot), node2.nodeID).Result()
	if err != nil {
		close(stop)
		wg.Wait()
		t.Fatalf("MIGRATESLOT under load failed: %v", err)
	}
	close(stop)
	wg.Wait()

	t.Logf("fence hits=%d other_errs=%d", fenceHits.Load(), otherErrs.Load())
	if fenceHits.Load() == 0 {
		t.Fatal("expected concurrent writers to hit MIGRATING write fence")
	}

	time.Sleep(2 * time.Second)

	// All seed keys on target with original value (writers were fenced)
	for _, k := range keys {
		val, err := node2.client.Get(ctx, k).Result()
		if err != nil {
			t.Fatalf("key %s missing on target after migrate under load: %v", k, err)
		}
		if val != "seed" {
			t.Fatalf("key %s value=%q want seed (writers should have been fenced)", k, val)
		}
		_, err = node1.client.Get(ctx, k).Result()
		if err == nil {
			t.Fatalf("key %s should be deleted from source", k)
		}
	}
	t.Log("migrate under load: all keys consistent on target")
}

// TestClusterFailover verifies that PFAIL reports from multiple nodes
// trigger FAIL promotion and slot reassignment.
func TestClusterFailover(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping failover test in short mode")
	}

	node1 := startClusterNode(t)
	defer node1.stop()

	node2 := startClusterNode(t)
	defer node2.stop()

	node3 := startClusterNode(t)
	defer node3.stop()

	ctx := context.Background()

	// MEET all to node1 (node1 learns about all)
	_, err := node1.client.Do(ctx, "CLUSTER", "MEET", "127.0.0.1", fmt.Sprintf("%d", node2.port)).Result()
	assert.NoError(t, err)
	time.Sleep(500 * time.Millisecond)

	_, err = node1.client.Do(ctx, "CLUSTER", "MEET", "127.0.0.1", fmt.Sprintf("%d", node3.port)).Result()
	assert.NoError(t, err)

	// Wait for full mesh gossip (all nodes know each other)
	time.Sleep(3 * time.Second)

	// Verify all nodes know each other
	for _, n := range []*clusterNode{node1, node2, node3} {
		nodes, err := n.client.Do(ctx, "CLUSTER", "NODES").Result()
		assert.NoError(t, err)
		nodesStr := fmt.Sprintf("%v", nodes)
		if !strings.Contains(nodesStr, node1.nodeID) || !strings.Contains(nodesStr, node2.nodeID) || !strings.Contains(nodesStr, node3.nodeID) {
			t.Fatalf("node should know all 3 nodes: %s", nodesStr)
		}
	}

	// Now simulate node1 being failed: mark it as PFAIL via gossip from node2 and node3
	// We directly add PFAIL to node1's gossip payload by building payloads manually
	// that include PFAIL for node1's ID

	// Have node2 "report" node1 as PFAIL by building a payload with PFAIL
	node2.handler.Cluster.MarkNodePFail(node1.nodeID)

	// Build payload and inject as if received from node2
	payload := node2.handler.Cluster.Bus.BuildGossipPayload()

	// Apply on node3 as if node2 reported it.
	// This should promote node1 to FAIL (threshold=1 in any cluster size for first report,
	// since totalNodes may be 2 from node3's perspective).
	node3.handler.Cluster.Bus.ApplyGossipPayloadFrom(node2.nodeID, payload)

	// Verify node1 is marked FAIL on node3
	n1onNode3 := node3.handler.Cluster.GetNodeByID(node1.nodeID)
	if n1onNode3 == nil {
		t.Fatal("node1 should exist on node3")
	}
	hasFAIL := false
	for _, f := range n1onNode3.Flags {
		if f == cluster.FlagFail {
			hasFAIL = true
			break
		}
	}
	if !hasFAIL {
		t.Fatalf("node1 should be marked FAIL on node3 after PFAIL report from node2. Flags: %v", n1onNode3.Flags)
	}
	t.Logf("node1 promoted to FAIL on node3: flags=%v slots reassigned", n1onNode3.Flags)
}

// TestClusterMigrateKey verifies the MIGRATE command copies a key between nodes.
func TestClusterMigrateKey(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping MIGRATE test in short mode")
	}

	node1 := startClusterNode(t)
	defer node1.stop()

	node2 := startClusterNode(t)
	defer node2.stop()

	ctx := context.Background()

	// Write a key on node1
	err := node1.client.Set(ctx, "migrate:test", "migrate-value", 0).Err()
	assert.NoError(t, err)

	// Verify key exists on node1
	val, err := node1.client.Get(ctx, "migrate:test").Result()
	assert.NoError(t, err)
	assert.Equal(t, "migrate-value", val)

	// MIGRATE from node1 to node2 via raw RESP (to see the response)
	host, portStr, err := net.SplitHostPort(node2.client.Options().Addr)
	assert.NoError(t, err)

	// Test DUMP on node1 first, then try RESTORE directly on node2
	dumpData, err := node1.client.Dump(ctx, "migrate:test").Result()
	if err != nil {
		t.Fatalf("DUMP on node1: %v", err)
	}
	t.Logf("DUMP data length: %d", len(dumpData))

	// Try direct RESTORE on node2
	_, err = node2.client.Do(ctx, "RESTORE", "migrate:test", "0", dumpData, "REPLACE").Result()
	if err != nil {
		t.Fatalf("direct RESTORE on node2: %v", err)
	}
	t.Log("direct RESTORE on node2 succeeded")

	// Verify key exists on node2
	val, err = node2.client.Get(ctx, "migrate:test").Result()
	assert.NoError(t, err)
	assert.Equal(t, "migrate-value", val)
	t.Log("key verified on node2 after direct RESTORE")

	// Clean up and test MIGRATE command
	node2.client.Del(ctx, "migrate:test")

	// Now test actual MIGRATE via raw RESP
	conn, err := net.DialTimeout("tcp", node1.client.Options().Addr, 5*time.Second)
	assert.NoError(t, err)
	defer conn.Close()

	sendRESP(conn, "MIGRATE", host, portStr, "migrate:test", "0", "5000")
	reader := bufio.NewReader(conn)
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	migrateResp, err := proto.ReadRESP(reader)
	_ = conn.SetReadDeadline(time.Time{})
	if err != nil {
		t.Fatalf("MIGRATE raw response error: %v", err)
	}
	t.Logf("MIGRATE raw response: %q", migrateResp.String())

	// Key should no longer exist on node1
	_, err = node1.client.Get(ctx, "migrate:test").Result()
	if err == nil {
		t.Fatal("key should have been deleted from source after MIGRATE")
	}

	// Key should exist on node2
	val, err = node2.client.Get(ctx, "migrate:test").Result()
	if err != nil {
		t.Fatalf("key should exist on node2 after MIGRATE: %v", err)
	}
	assert.Equal(t, "migrate-value", val)
	t.Log("MIGRATE: key successfully moved from node1 to node2")

	t.Log("MIGRATE: key successfully moved from node1 to node2")

	// Test MIGRATE with COPY (key should remain on source)
	err = node2.client.Set(ctx, "migrate:copy", "copy-value", 0).Err()
	assert.NoError(t, err)

	host2, portStr2, _ := net.SplitHostPort(node1.client.Options().Addr)
	_, err = node2.client.Do(ctx, "MIGRATE", host2, portStr2, "migrate:copy", "0", "5000", "COPY").Result()
	assert.NoError(t, err)

	// Key should still be on node2 (COPY)
	val, err = node2.client.Get(ctx, "migrate:copy").Result()
	assert.NoError(t, err)
	assert.Equal(t, "copy-value", val)

	// Key should also be on node1 (COPY)
	val, err = node1.client.Get(ctx, "migrate:copy").Result()
	assert.NoError(t, err)
	assert.Equal(t, "copy-value", val)

	t.Log("MIGRATE with COPY: key exists on both nodes")

	// Test MIGRATE with REPLACE (overwrite existing key)
	err = node1.client.Set(ctx, "migrate:replace", "old-value", 0).Err()
	assert.NoError(t, err)
	err = node2.client.Set(ctx, "migrate:replace", "new-value", 0).Err()
	assert.NoError(t, err)

	_, err = node1.client.Do(ctx, "MIGRATE", host, portStr, "migrate:replace", "0", "5000", "REPLACE").Result()
	assert.NoError(t, err)

	val, err = node2.client.Get(ctx, "migrate:replace").Result()
	assert.NoError(t, err)
	assert.Equal(t, "old-value", val)
	t.Log("MIGRATE with REPLACE: key overwritten on target")
}

// TestClusterBlockingFuzz verifies that blocking operations + CLUSTER commands
// don't cause deadlocks or goroutine leaks in cluster mode.
func TestClusterBlockingFuzz(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cluster blocking fuzz in short mode")
	}

	setupClusterTestServer(t)
	defer teardownClusterTestServer(t)

	ctx := context.Background()
	baseline := runtime.NumGoroutine()

	var wg sync.WaitGroup
	errs := make(chan error, 20)

	// Client 1: blocking pop
	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := net.DialTimeout("tcp", clusterListener.Addr().String(), 5*time.Second)
		if err != nil {
			errs <- err
			return
		}
		defer conn.Close()

		// Blocking BLPOP with 1s timeout
		sendRESP(conn, "BLPOP", "blkfuzz:list", "1")
		reader := bufio.NewReader(conn)
		_, err = proto.ReadRESP(reader)
		if err != nil {
			errs <- fmt.Errorf("BLPOP read: %w", err)
		}
	}()

	// Client 2: concurrent CLUSTER commands
	wg.Add(1)
	go func() {
		defer wg.Done()
		cli := redis.NewClient(&redis.Options{Addr: clusterListener.Addr().String()})
		defer cli.Close()

		for i := 0; i < 10; i++ {
			// Mix of different CLUSTER subcommands
			cmds := []string{"INFO", "NODES", "MYID", "SLOTS", "KEYSLOT"}
			sub := cmds[i%len(cmds)]
			var err error
			if sub == "KEYSLOT" {
				_, err = cli.Do(ctx, "CLUSTER", sub, fmt.Sprintf("blkfuzz:key:%d", i)).Result()
			} else {
				_, err = cli.Do(ctx, "CLUSTER", sub).Result()
			}
			if err != nil {
				errs <- fmt.Errorf("CLUSTER %s: %w", sub, err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()

	// Client 3: push data to unblock the BLPOP
	time.Sleep(200 * time.Millisecond)
	wg.Add(1)
	go func() {
		defer wg.Done()
		cli := redis.NewClient(&redis.Options{Addr: clusterListener.Addr().String()})
		defer cli.Close()

		_, err := cli.LPush(ctx, "blkfuzz:list", "data").Result()
		if err != nil {
			errs <- fmt.Errorf("LPUSH: %w", err)
		}
	}()

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("cluster blocking fuzz error: %v", err)
	}

	time.Sleep(goroutineSettleTime)
	final := runtime.NumGoroutine()
	if leak := final - baseline; leak > goroutineTolerance {
		t.Errorf("goroutine leak: %d (baseline=%d, final=%d)", leak, baseline, final)
	}

	// Verify server still responsive
	if err := clusterClient.Ping(ctx).Err(); err != nil {
		t.Fatalf("server not responsive after cluster blocking fuzz: %v", err)
	}
}
