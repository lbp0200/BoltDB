package integration

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/backup"
	"github.com/lbp0200/BoltDB/internal/replication"
	"github.com/lbp0200/BoltDB/internal/server"
	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/redis/go-redis/v9"
	"github.com/zeebo/assert"
)

// containsString checks if a string is present in a slice
func containsString(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// setupMasterSlaveServer 创建一个主从复制测试环境
func setupMasterSlaveServer(t *testing.T) (masterClient, slaveClient *redis.Client, cleanup func()) {
	var err error

	// 创建主节点
	masterDBPath := t.TempDir()
	masterDB, err := store.NewBotreonStore(masterDBPath)
	if err != nil {
		t.Fatalf("Failed to create master store: %v", err)
	}

	masterPubsubMgr := store.NewPubSubManager()
	masterBackupMgr := backup.NewBackupManager(masterDB, masterDBPath+"/backup")
	masterReplMgr := replication.NewReplicationManager(masterDB)

	masterHandler := &server.Handler{
		Db:          masterDB,
		Replication: masterReplMgr,
		Backup:      masterBackupMgr,
		PubSub:      masterPubsubMgr,
	}

	masterListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		masterDB.Close()
		t.Fatalf("Failed to listen on master: %v", err)
	}

	go func() {
		_ = masterHandler.ServeTCP(masterListener)
	}()

	time.Sleep(50 * time.Millisecond)

	masterClient = redis.NewClient(&redis.Options{
		Addr:     masterListener.Addr().String(),
		Password: "",
		DB:       0,
	})

	ctx := context.Background()
	_, err = masterClient.Ping(ctx).Result()
	if err != nil {
		masterListener.Close()
		masterDB.Close()
		t.Fatalf("Failed to ping master: %v", err)
	}

	// 创建从节点
	slaveDBPath := t.TempDir()
	slaveDB, err := store.NewBotreonStore(slaveDBPath)
	if err != nil {
		masterListener.Close()
		masterDB.Close()
		t.Fatalf("Failed to create slave store: %v", err)
	}

	slavePubsubMgr := store.NewPubSubManager()
	slaveBackupMgr := backup.NewBackupManager(slaveDB, slaveDBPath+"/backup")
	slaveReplMgr := replication.NewReplicationManager(slaveDB)

	slaveHandler := &server.Handler{
		Db:          slaveDB,
		Replication: slaveReplMgr,
		Backup:      slaveBackupMgr,
		PubSub:      slavePubsubMgr,
	}

	slaveListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		masterListener.Close()
		masterDB.Close()
		slaveDB.Close()
		t.Fatalf("Failed to listen on slave: %v", err)
	}

	go func() {
		_ = slaveHandler.ServeTCP(slaveListener)
	}()

	time.Sleep(50 * time.Millisecond)

	slaveClient = redis.NewClient(&redis.Options{
		Addr:     slaveListener.Addr().String(),
		Password: "",
		DB:       0,
	})

	_, err = slaveClient.Ping(ctx).Result()
	if err != nil {
		masterListener.Close()
		masterDB.Close()
		slaveListener.Close()
		slaveDB.Close()
		t.Fatalf("Failed to ping slave: %v", err)
	}

	// 启动从节点复制
	err = replication.StartSlaveReplication(slaveReplMgr, slaveDB, masterListener.Addr().String())
	if err != nil {
		masterListener.Close()
		masterDB.Close()
		slaveListener.Close()
		slaveDB.Close()
		t.Fatalf("Failed to start slave replication: %v", err)
	}

	// 等待复制建立
	time.Sleep(100 * time.Millisecond)

	cleanup = func() {
		slaveClient.Close()
		masterClient.Close()
		slaveListener.Close()
		masterListener.Close()
		masterBackupMgr.Wait()
		slaveBackupMgr.Wait()
		slaveDB.Close()
		masterDB.Close()
	}

	return masterClient, slaveClient, cleanup
}

// TestReplicationMasterSlaveBasic 测试基本的主从复制
func TestReplicationMasterSlaveBasic(t *testing.T) {
	masterClient, slaveClient, cleanup := setupMasterSlaveServer(t)
	defer cleanup()

	ctx := context.Background()

	// 在主节点写入数据
	err := masterClient.Set(ctx, "test_key", "test_value", 0).Err()
	assert.NoError(t, err)

	// 等待复制
	time.Sleep(200 * time.Millisecond)

	// 在从节点读取数据
	val, err := slaveClient.Get(ctx, "test_key").Result()
	assert.NoError(t, err)
	assert.Equal(t, "test_value", val)
}

// TestReplicationMasterSlaveMultipleKeys 测试多个键的复制
func TestReplicationMasterSlaveMultipleKeys(t *testing.T) {
	masterClient, slaveClient, cleanup := setupMasterSlaveServer(t)
	defer cleanup()

	ctx := context.Background()

	// 在主节点写入多个键
	for i := 0; i < 10; i++ {
		key := "key_" + string(rune('a'+i))
		err := masterClient.Set(ctx, key, "value_"+string(rune('a'+i)), 0).Err()
		assert.NoError(t, err)
	}

	// 等待复制
	time.Sleep(200 * time.Millisecond)

	// 在从节点验证所有键
	for i := 0; i < 10; i++ {
		key := "key_" + string(rune('a'+i))
		val, err := slaveClient.Get(ctx, key).Result()
		assert.NoError(t, err)
		assert.Equal(t, "value_"+string(rune('a'+i)), val)
	}
}

// TestReplicationMasterSlaveCounter 测试计数器复制
func TestReplicationMasterSlaveCounter(t *testing.T) {
	masterClient, slaveClient, cleanup := setupMasterSlaveServer(t)
	defer cleanup()

	ctx := context.Background()

	// 在主节点执行INCR
	val, err := masterClient.Incr(ctx, "counter").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), val)

	val, err = masterClient.Incr(ctx, "counter").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), val)

	val, err = masterClient.IncrBy(ctx, "counter", 5).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(7), val)

	// 等待复制
	time.Sleep(200 * time.Millisecond)

	// 在从节点验证 - 使用原始命令确保正确解析
	result, err := slaveClient.Do(ctx, "GET", "counter").Result()
	assert.NoError(t, err)
	counterVal, ok := result.(string)
	assert.True(t, ok)
	assert.Equal(t, "7", counterVal)
}

// TestReplicationMasterSlaveList 测试列表复制
func TestReplicationMasterSlaveList(t *testing.T) {
	masterClient, slaveClient, cleanup := setupMasterSlaveServer(t)
	defer cleanup()

	ctx := context.Background()

	// 在主节点创建列表
	err := masterClient.RPush(ctx, "test_list", "a", "b", "c").Err()
	assert.NoError(t, err)

	// 等待复制
	time.Sleep(200 * time.Millisecond)

	// 在从节点验证列表
	length, err := slaveClient.LLen(ctx, "test_list").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), length)

	items, err := slaveClient.LRange(ctx, "test_list", 0, -1).Result()
	assert.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c"}, items)
}

// TestReplicationMasterSlaveHash 测试哈希复制
func TestReplicationMasterSlaveHash(t *testing.T) {
	masterClient, slaveClient, cleanup := setupMasterSlaveServer(t)
	defer cleanup()

	ctx := context.Background()

	// 在主节点创建哈希 - 使用HMSET命令
	err := masterClient.Do(ctx, "HMSET", "test_hash", "field1", "value1", "field2", "value2").Err()
	assert.NoError(t, err)

	// 等待复制 - HMSET可能发送多个HSET命令，需要更长的等待时间
	time.Sleep(500 * time.Millisecond)

	// 在从节点验证哈希
	all, err := slaveClient.HGetAll(ctx, "test_hash").Result()
	assert.NoError(t, err)
	// 验证至少有2个字段（可能是HMSET拆分成多个HSET）
	if len(all) < 2 {
		t.Errorf("expected at least 2 fields, got %d: %v", len(all), all)
	}
	assert.Equal(t, "value1", all["field1"])
	assert.Equal(t, "value2", all["field2"])
}

// TestReplicationMasterSlaveSet 测试集合复制
func TestReplicationMasterSlaveSet(t *testing.T) {
	masterClient, slaveClient, cleanup := setupMasterSlaveServer(t)
	defer cleanup()

	ctx := context.Background()

	// 在主节点创建集合
	err := masterClient.SAdd(ctx, "test_set", "a", "b", "c").Err()
	assert.NoError(t, err)

	// 等待复制
	time.Sleep(200 * time.Millisecond)

	// 在从节点验证集合
	members, err := slaveClient.SMembers(ctx, "test_set").Result()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(members))

	// 验证所有成员存在
	assert.True(t, containsString(members, "a"))
	assert.True(t, containsString(members, "b"))
	assert.True(t, containsString(members, "c"))
}

// TestReplicationMasterSlaveZSet 测试有序集合复制
func TestReplicationMasterSlaveZSet(t *testing.T) {
	masterClient, slaveClient, cleanup := setupMasterSlaveServer(t)
	defer cleanup()

	ctx := context.Background()

	// 在主节点创建有序集合
	err := masterClient.ZAdd(ctx, "test_zset",
		redis.Z{Score: 1, Member: "a"},
		redis.Z{Score: 2, Member: "b"},
		redis.Z{Score: 3, Member: "c"},
	).Err()
	assert.NoError(t, err)

	// 等待复制
	time.Sleep(200 * time.Millisecond)

	// 在从节点验证有序集合
	members, err := slaveClient.ZRange(ctx, "test_zset", 0, -1).Result()
	assert.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c"}, members)

	// 验证分数
	score, err := slaveClient.ZScore(ctx, "test_zset", "b").Result()
	assert.NoError(t, err)
	assert.Equal(t, float64(2), score)
}

// TestReplicationMasterSlaveDEL 测试删除复制
func TestReplicationMasterSlaveDEL(t *testing.T) {
	masterClient, slaveClient, cleanup := setupMasterSlaveServer(t)
	defer cleanup()

	ctx := context.Background()

	// 在主节点创建键
	err := masterClient.Set(ctx, "to_delete", "value", 0).Err()
	assert.NoError(t, err)

	// 等待复制
	time.Sleep(200 * time.Millisecond)

	// 确认从节点有这个键
	val, err := slaveClient.Get(ctx, "to_delete").Result()
	assert.NoError(t, err)
	assert.Equal(t, "value", val)

	// 在主节点删除
	err = masterClient.Del(ctx, "to_delete").Err()
	assert.NoError(t, err)

	// 等待复制
	time.Sleep(200 * time.Millisecond)

	// 在从节点验证已删除
	_, err = slaveClient.Get(ctx, "to_delete").Result()
	assert.Equal(t, redis.Nil, err)
}

// TestReplicationMasterSlaveInfo 测试复制信息
func TestReplicationMasterSlaveInfo(t *testing.T) {
	masterClient, slaveClient, cleanup := setupMasterSlaveServer(t)
	defer cleanup()

	ctx := context.Background()

	// 在主节点写入一些数据
	_ = masterClient.Set(ctx, "info_test", "value", 0).Err()

	// 等待复制
	time.Sleep(200 * time.Millisecond)

	// 检查主节点的INFO replication
	result, err := masterClient.Do(ctx, "INFO", "replication").Result()
	assert.NoError(t, err)
	info, ok := result.(string)
	assert.True(t, ok)

	// 主节点应该有role:master
	assert.True(t, contains(info, "role:master"))
	// 主节点应该显示有1个从节点
	assert.True(t, contains(info, "connected_slaves:1"))

	// 检查从节点的INFO replication
	result, err = slaveClient.Do(ctx, "INFO", "replication").Result()
	assert.NoError(t, err)
	info, ok = result.(string)
	assert.True(t, ok)

	// 从节点应该有role:slave
	assert.True(t, contains(info, "role:slave"))
}

// TestReplicationMasterSlaveRole 测试ROLE命令
func TestReplicationMasterSlaveRole(t *testing.T) {
	masterClient, slaveClient, cleanup := setupMasterSlaveServer(t)
	defer cleanup()

	ctx := context.Background()

	// 检查主节点ROLE
	result, err := masterClient.Do(ctx, "ROLE").Result()
	assert.NoError(t, err)
	arr, ok := result.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, "master", arr[0])

	// 检查从节点ROLE
	result, err = slaveClient.Do(ctx, "ROLE").Result()
	assert.NoError(t, err)
	arr, ok = result.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, "slave", arr[0])
}

// TestPropagation_SET 测试 SET 命令传播
func TestPropagation_SET(t *testing.T) {
	master, slave, cleanup := setupMasterSlaveServer(t)
	defer cleanup()

	ctx := context.Background()
	err := master.Set(ctx, "set_prop", "hello", 0).Err()
	assert.NoError(t, err)
	time.Sleep(200 * time.Millisecond)

	val, err := slave.Get(ctx, "set_prop").Result()
	assert.NoError(t, err)
	assert.Equal(t, "hello", val)
}

// TestPropagation_SETEX 测试 SETEX 命令传播
func TestPropagation_SETEX(t *testing.T) {
	master, slave, cleanup := setupMasterSlaveServer(t)
	defer cleanup()

	ctx := context.Background()
	err := master.Do(ctx, "SETEX", "setex_prop", 100, "ttl_val").Err()
	assert.NoError(t, err)
	time.Sleep(200 * time.Millisecond)

	val, err := slave.Get(ctx, "setex_prop").Result()
	assert.NoError(t, err)
	assert.Equal(t, "ttl_val", val)
}

// TestPropagation_EXPIRE 测试 EXPIRE 传播
func TestPropagation_EXPIRE(t *testing.T) {
	master, slave, cleanup := setupMasterSlaveServer(t)
	defer cleanup()

	ctx := context.Background()
	_, err := master.Set(ctx, "exp_prop", "val", 0).Result()
	if err != nil {
		t.Fatalf("master SET failed: %v", err)
	}
	time.Sleep(300 * time.Millisecond)

	val, err := slave.Get(ctx, "exp_prop").Result()
	if err != nil {
		t.Fatalf("slave Get after SET failed: %v", err)
	}
	if val != "val" {
		t.Fatalf("slave value mismatch: got %s", val)
	}

	err = master.Do(ctx, "EXPIRE", "exp_prop", 999).Err()
	if err != nil {
		t.Fatalf("master EXPIRE failed: %v", err)
	}
	time.Sleep(300 * time.Millisecond)

	// Verify slave has the key after SET
	val, err = slave.Get(ctx, "exp_prop").Result()
	if err != nil {
		t.Fatalf("slave GET after SET failed: %v", err)
	}
	t.Logf("slave value after SET: %s", val)

	// Send EXPIRE via raw command
	err = master.Do(ctx, "EXPIRE", "exp_prop", 999).Err()
	if err != nil {
		t.Fatalf("master EXPIRE failed: %v", err)
	}
	time.Sleep(300 * time.Millisecond)

	// Check TTL on slave directly
	rawTTL, err := slave.Do(ctx, "TTL", "exp_prop").Int()
	if err != nil {
		t.Fatalf("slave TTL command failed: %v", err)
	}
	t.Logf("slave raw TTL: %d", rawTTL)
	if rawTTL <= 0 {
		t.Errorf("slave should have TTL > 0, got %d", rawTTL)
	}
}

// TestPropagation_DEL 测试 DEL 传播
func TestPropagation_DEL(t *testing.T) {
	master, slave, cleanup := setupMasterSlaveServer(t)
	defer cleanup()

	ctx := context.Background()
	master.Set(ctx, "del_prop_a", "val1", 0)
	master.Set(ctx, "del_prop_b", "val2", 0)
	time.Sleep(100 * time.Millisecond)

	master.Del(ctx, "del_prop_a", "del_prop_b")
	time.Sleep(200 * time.Millisecond)

	_, err := slave.Get(ctx, "del_prop_a").Result()
	if err == nil {
		t.Error("deleted key should not exist on slave")
	}
	_, err = slave.Get(ctx, "del_prop_b").Result()
	if err == nil {
		t.Error("deleted key should not exist on slave")
	}
}

// TestPropagation_RENAME 测试 RENAME 传播
func TestPropagation_RENAME(t *testing.T) {
	master, slave, cleanup := setupMasterSlaveServer(t)
	defer cleanup()

	ctx := context.Background()
	master.Set(ctx, "rename_src", "rename_val", 0)
	time.Sleep(100 * time.Millisecond)

	master.Rename(ctx, "rename_src", "rename_dst")
	time.Sleep(200 * time.Millisecond)

	_, err := slave.Get(ctx, "rename_src").Result()
	if err == nil {
		t.Error("old key should not exist on slave")
	}

	val, err := slave.Get(ctx, "rename_dst").Result()
	assert.NoError(t, err)
	assert.Equal(t, "rename_val", val)
}

// TestPropagation_MULTIEXEC 测试事务传播
func TestPropagation_MULTIEXEC(t *testing.T) {
	master, slave, cleanup := setupMasterSlaveServer(t)
	defer cleanup()

	ctx := context.Background()

	pipe := master.TxPipeline()
	pipe.Set(ctx, "multi_a", "1", 0)
	pipe.Set(ctx, "multi_b", "2", 0)
	_, err := pipe.Exec(ctx)
	assert.NoError(t, err)
	time.Sleep(300 * time.Millisecond)

	valA, err := slave.Get(ctx, "multi_a").Result()
	assert.NoError(t, err)
	assert.Equal(t, "1", valA)

	valB, err := slave.Get(ctx, "multi_b").Result()
	assert.NoError(t, err)
	assert.Equal(t, "2", valB)
}

// TestPropagation_LPUSH 测试 LPUSH 传播
func TestPropagation_LPUSH(t *testing.T) {
	master, slave, cleanup := setupMasterSlaveServer(t)
	defer cleanup()

	ctx := context.Background()
	n, err := master.LPush(ctx, "lpush_prop", "c", "b", "a").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), n)
	time.Sleep(200 * time.Millisecond)

	items, err := slave.LRange(ctx, "lpush_prop", 0, -1).Result()
	assert.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c"}, items)
}

// TestPropagation_SADD 测试 SADD 传播
func TestPropagation_SADD(t *testing.T) {
	master, slave, cleanup := setupMasterSlaveServer(t)
	defer cleanup()

	ctx := context.Background()
	master.SAdd(ctx, "sadd_prop", "m1", "m2", "m3")
	time.Sleep(200 * time.Millisecond)

	count, err := slave.SCard(ctx, "sadd_prop").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), count)
}

// TestPropagation_ZADD 测试 ZADD 传播
func TestPropagation_ZADD(t *testing.T) {
	master, slave, cleanup := setupMasterSlaveServer(t)
	defer cleanup()

	ctx := context.Background()
	master.ZAdd(ctx, "zadd_prop", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"})
	time.Sleep(200 * time.Millisecond)

	count, err := slave.ZCard(ctx, "zadd_prop").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)
}

// TestPropagation_PERSIST 测试 PERSIST 传播
func TestPropagation_PERSIST(t *testing.T) {
	master, slave, cleanup := setupMasterSlaveServer(t)
	defer cleanup()

	ctx := context.Background()
	master.Set(ctx, "persist_prop", "val", 100*time.Second)
	time.Sleep(100 * time.Millisecond)

	master.Persist(ctx, "persist_prop")
	time.Sleep(200 * time.Millisecond)

	ttl, err := slave.TTL(ctx, "persist_prop").Result()
	assert.NoError(t, err)
	if ttl != -1 {
		t.Errorf("persisted key should have no TTL on slave, got %v", ttl)
	}
}

// TestPropagation_HSET 测试 HSET 传播
func TestPropagation_HSET(t *testing.T) {
	master, slave, cleanup := setupMasterSlaveServer(t)
	defer cleanup()

	ctx := context.Background()
	master.HSet(ctx, "hset_prop", "f1", "v1", "f2", "v2")
	time.Sleep(200 * time.Millisecond)

	v1, err := slave.HGet(ctx, "hset_prop", "f1").Result()
	if err != nil {
		t.Fatalf("slave HGet f1 failed: %v", err)
	}
	assert.Equal(t, "v1", v1)

	v2, err := slave.HGet(ctx, "hset_prop", "f2").Result()
	if err != nil {
		t.Fatalf("slave HGet f2 failed: %v", err)
	}
	assert.Equal(t, "v2", v2)
}

// TestPropagation_LPOP 测试 LPOP 传播
func TestPropagation_LPOP(t *testing.T) {
	master, slave, cleanup := setupMasterSlaveServer(t)
	defer cleanup()

	ctx := context.Background()
	master.LPush(ctx, "lpop_prop", "x", "y", "z")
	time.Sleep(100 * time.Millisecond)

	val, err := master.LPop(ctx, "lpop_prop").Result()
	assert.NoError(t, err)
	assert.Equal(t, "z", val)
	time.Sleep(200 * time.Millisecond)

	items, err := slave.LRange(ctx, "lpop_prop", 0, -1).Result()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(items))
}

// TestPropagation_ReadOnly 测试只读命令不传播（验证不破坏复制状态）
func TestPropagation_ReadOnly(t *testing.T) {
	master, slave, cleanup := setupMasterSlaveServer(t)
	defer cleanup()

	ctx := context.Background()
	master.Set(ctx, "ro_key", "base", 0)
	time.Sleep(100 * time.Millisecond)

	_, err := master.Get(ctx, "ro_key").Result()
	assert.NoError(t, err)
	time.Sleep(200 * time.Millisecond)

	val, err := slave.Get(ctx, "ro_key").Result()
	assert.NoError(t, err)
	assert.Equal(t, "base", val)
}

// TestReplicationChaos_Load 测试复制在持续写入下的稳定性
func TestReplicationChaos_Load(t *testing.T) {
	master, slave, cleanup := setupMasterSlaveServer(t)
	defer cleanup()

	ctx := context.Background()

	time.Sleep(500 * time.Millisecond)
	baseline := runtime.NumGoroutine()
	t.Logf("baseline goroutines: %d", baseline)

	totalKeys := 300
	written := make(map[string]string, totalKeys)

	// 持续写入并验证
	for i := 0; i < totalKeys; i++ {
		key := fmt.Sprintf("chaos:load:%d", i)
		val := fmt.Sprintf("v:%d", i)
		err := master.Set(ctx, key, val, 0).Err()
		assert.NoError(t, err)
		written[key] = val

		if i%50 == 49 {
			time.Sleep(300 * time.Millisecond)
			for k, expected := range written {
				val, err := slave.Get(ctx, k).Result()
				assert.NoError(t, err)
				assert.Equal(t, expected, val)
			}
		}
	}

	time.Sleep(500 * time.Millisecond)
	final := runtime.NumGoroutine()
	leak := final - baseline
	if leak > 20 {
		t.Errorf("goroutine leak after load: %d (baseline=%d, final=%d)", leak, baseline, final)
	}
	t.Logf("final goroutines: %d (baseline=%d, leak=%d)", final, baseline, leak)
}

// TestReplicationChaos_RapidWrite 测试高速写入下的 slave 同步正确性
func TestReplicationChaos_RapidWrite(t *testing.T) {
	master, slave, cleanup := setupMasterSlaveServer(t)
	defer cleanup()

	ctx := context.Background()

	time.Sleep(500 * time.Millisecond)
	baseline := runtime.NumGoroutine()
	t.Logf("baseline goroutines: %d", baseline)

	// 多类型高速写入
	n := 50
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("chaos:rapid:%d", i)
		keys[i] = key

		switch i % 5 {
		case 0:
			master.Set(ctx, key, fmt.Sprintf("str:%d", i), 0)
		case 1:
			master.LPush(ctx, key, fmt.Sprintf("list:%d:a", i), fmt.Sprintf("list:%d:b", i))
		case 2:
			master.SAdd(ctx, key, fmt.Sprintf("set:%d:a", i), fmt.Sprintf("set:%d:b", i))
		case 3:
			master.HSet(ctx, key, fmt.Sprintf("field:%d", i), fmt.Sprintf("hash:%d", i))
		case 4:
			master.ZAdd(ctx, key, redis.Z{Score: float64(i), Member: fmt.Sprintf("zset:%d", i)})
		}
	}

	time.Sleep(1 * time.Second)

	// 验证每种类型的同步
	for i, key := range keys {
		switch i % 5 {
		case 0:
			val, err := slave.Get(ctx, key).Result()
			assert.NoError(t, err)
			assert.Equal(t, fmt.Sprintf("str:%d", i), val)
		case 1:
			vals, err := slave.LRange(ctx, key, 0, -1).Result()
			assert.NoError(t, err)
			assert.Equal(t, 2, len(vals))
		case 2:
			n, err := slave.SCard(ctx, key).Result()
			assert.NoError(t, err)
			assert.Equal(t, int64(2), n)
		case 3:
			val, err := slave.HGet(ctx, key, fmt.Sprintf("field:%d", i)).Result()
			assert.NoError(t, err)
			assert.Equal(t, fmt.Sprintf("hash:%d", i), val)
		case 4:
			score, err := slave.ZScore(ctx, key, fmt.Sprintf("zset:%d", i)).Result()
			assert.NoError(t, err)
			assert.Equal(t, float64(i), score)
		}
	}

	time.Sleep(500 * time.Millisecond)
	final := runtime.NumGoroutine()
	leak := final - baseline
	if leak > 20 {
		t.Errorf("goroutine leak after rapid write: %d (baseline=%d, final=%d)", leak, baseline, final)
	}
	t.Logf("final goroutines: %d (baseline=%d, leak=%d)", final, baseline, leak)
}

// TestReplicationChaos_SlowClient 测试慢客户端场景（输出缓冲区限制）
func TestReplicationChaos_SlowClient(t *testing.T) {
	master, _, cleanup := setupMasterSlaveServer(t)
	defer cleanup()

	ctx := context.Background()

	// 大量写入，SlaveReconnector 会不断接收数据
	// 不设 OutputBufferLimit，验证默认行为正常
	for i := 0; i < 500; i++ {
		err := master.Set(ctx, fmt.Sprintf("chaos:slow:%d", i), fmt.Sprintf("v:%d", i), 0).Err()
		assert.NoError(t, err)
	}

	time.Sleep(1 * time.Second)

	// INFO 命令应该正常返回
	info, err := master.Info(ctx, "replication").Result()
	assert.NoError(t, err)
	assert.True(t, strings.Contains(info, "role:master"))
}
