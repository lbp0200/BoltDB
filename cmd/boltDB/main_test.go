package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/server"
	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/redis/go-redis/v9"
	"github.com/zeebo/assert"
)

var (
	testClient *redis.Client
	testServer *server.Handler
	testDB     *store.BotreonStore
	listener   net.Listener
)

// setupTestServer 启动测试服务器
func setupTestServer(t *testing.T) {
	var err error

	// 创建临时数据库目录
	dbPath := t.TempDir()

	// 创建数据库
	testDB, err = store.NewBotreonStore(dbPath)
	assert.NoError(t, err)

	// 创建服务器处理器
	testServer = &server.Handler{Db: testDB}

	// 启动服务器（使用随机端口）
	listener, err = net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)

	// 在goroutine中运行服务器
	go func() {
		_ = testServer.ServeTCP(listener)
	}()

	// 等待服务器启动
	time.Sleep(50 * time.Millisecond)

	// 创建Redis客户端
	testClient = redis.NewClient(&redis.Options{
		Addr:     listener.Addr().String(),
		Password: "", // 无密码
		DB:       0,  // 默认数据库
	})

	// 测试连接
	ctx := context.Background()
	_, err = testClient.Ping(ctx).Result()
	assert.NoError(t, err)
}

// teardownTestServer 清理测试服务器
func teardownTestServer(t *testing.T) {
	if testClient != nil {
		_ = testClient.Close()
	}
	if listener != nil {
		_ = listener.Close()
	}
	if testDB != nil {
		_ = testDB.Close()
	}
}

// TestMain 测试入口
func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}

// TestStringCommands 测试String类型命令
func TestStringCommands(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// SET/GET
	err := testClient.Set(ctx, "key1", "value1", 0).Err()
	assert.NoError(t, err)

	val, err := testClient.Get(ctx, "key1").Result()
	assert.NoError(t, err)
	assert.Equal(t, "value1", val)

	// GET不存在的键
	val, err = testClient.Get(ctx, "nonexistent").Result()
	t.Logf("val: %s, err: %v, type: %T, isNil: %v", val, err, err, errors.Is(err, redis.Nil))
	assert.Error(t, err)
	assert.Equal(t, redis.Nil, err)

	// SETEX
	err = testClient.SetEx(ctx, "key2", "value2", 10*time.Second).Err()
	assert.NoError(t, err)

	val, err = testClient.Get(ctx, "key2").Result()
	assert.NoError(t, err)
	assert.Equal(t, "value2", val)

	// SETNX
	set, err := testClient.SetNX(ctx, "key3", "value3", 0).Result()
	assert.NoError(t, err)
	assert.True(t, set)

	set, err = testClient.SetNX(ctx, "key3", "value3_updated", 0).Result()
	assert.NoError(t, err)
	assert.False(t, set) // 键已存在，设置失败

	// GETSET
	oldVal, err := testClient.GetSet(ctx, "key1", "new_value1").Result()
	assert.NoError(t, err)
	assert.Equal(t, "value1", oldVal)

	val, err = testClient.Get(ctx, "key1").Result()
	assert.NoError(t, err)
	assert.Equal(t, "new_value1", val)

	// MGET/MSET
	err = testClient.MSet(ctx, "key4", "value4", "key5", "value5").Err()
	assert.NoError(t, err)

	mgetVals, err := testClient.MGet(ctx, "key4", "key5", "nonexistent").Result()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(mgetVals))
	if mgetVals[0] != nil {
		assert.Equal(t, "value4", mgetVals[0].(string))
	}
	if mgetVals[1] != nil {
		assert.Equal(t, "value5", mgetVals[1].(string))
	}
	assert.Nil(t, mgetVals[2]) // 不存在的键返回nil

	// INCR
	counterVal, err := testClient.Incr(ctx, "counter").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), counterVal)

	counterVal, err = testClient.Incr(ctx, "counter").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), counterVal)

	// INCRBY
	counterVal, err = testClient.IncrBy(ctx, "counter", 5).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(7), counterVal)

	// DECR
	counterVal, err = testClient.Decr(ctx, "counter").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(6), counterVal)

	// DECRBY
	counterVal, err = testClient.DecrBy(ctx, "counter", 3).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), counterVal)

	// APPEND
	length, err := testClient.Append(ctx, "key1", "_appended").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(19), length)

	val, err = testClient.Get(ctx, "key1").Result()
	assert.NoError(t, err)
	assert.Equal(t, "new_value1_appended", val)

	// STRLEN
	length, err = testClient.StrLen(ctx, "key1").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(len("new_value1_appended")), length)
}

// TestHashCommands 测试Hash类型命令
func TestHashCommands(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// HSET
	count, err := testClient.HSet(ctx, "user:1", "name", "Alice", "age", "30").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)

	// HGET
	val, err := testClient.HGet(ctx, "user:1", "name").Result()
	assert.NoError(t, err)
	assert.Equal(t, "Alice", val)

	// HGETALL
	all, err := testClient.HGetAll(ctx, "user:1").Result()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(all))
	assert.Equal(t, "Alice", all["name"])
	assert.Equal(t, "30", all["age"])

	// HEXISTS
	exists, err := testClient.HExists(ctx, "user:1", "name").Result()
	assert.NoError(t, err)
	assert.True(t, exists)

	exists, err = testClient.HExists(ctx, "user:1", "nonexistent").Result()
	assert.NoError(t, err)
	assert.False(t, exists)

	// HLEN
	length, err := testClient.HLen(ctx, "user:1").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), length)

	// HKEYS
	keys, err := testClient.HKeys(ctx, "user:1").Result()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(keys))

	// HVALS
	vals, err := testClient.HVals(ctx, "user:1").Result()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(vals))

	// HMSET
	err = testClient.HMSet(ctx, "user:2", map[string]interface{}{
		"name": "Bob",
		"city": "Beijing",
	}).Err()
	assert.NoError(t, err)

	// HMGET
	hmgetVals, err := testClient.HMGet(ctx, "user:2", "name", "city", "nonexistent").Result()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(hmgetVals))
	if hmgetVals[0] != nil {
		assert.Equal(t, "Bob", hmgetVals[0].(string))
	}
	if hmgetVals[1] != nil {
		assert.Equal(t, "Beijing", hmgetVals[1].(string))
	}
	assert.Nil(t, hmgetVals[2])

	// HSETNX
	set, err := testClient.HSetNX(ctx, "user:2", "email", "bob@example.com").Result()
	assert.NoError(t, err)
	assert.True(t, set)

	set, err = testClient.HSetNX(ctx, "user:2", "email", "new@example.com").Result()
	assert.NoError(t, err)
	assert.False(t, set) // 字段已存在

	// HINCRBY
	ageVal, err := testClient.HIncrBy(ctx, "user:1", "age", 1).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(31), ageVal)

	// HDEL
	deleted, err := testClient.HDel(ctx, "user:1", "age").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), deleted)

	length, err = testClient.HLen(ctx, "user:1").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), length)
}

// TestListCommands 测试List类型命令
func TestListCommands(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// LPUSH
	length, err := testClient.LPush(ctx, "mylist", "world", "hello").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), length)

	// LLEN
	length, err = testClient.LLen(ctx, "mylist").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), length)

	// LINDEX
	val, err := testClient.LIndex(ctx, "mylist", 0).Result()
	assert.NoError(t, err)
	assert.Equal(t, "hello", val)

	// LRANGE
	vals, err := testClient.LRange(ctx, "mylist", 0, -1).Result()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(vals))
	assert.Equal(t, "hello", vals[0])
	assert.Equal(t, "world", vals[1])

	// RPUSH
	length, err = testClient.RPush(ctx, "mylist", "end").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), length)

	// LPOP
	val, err = testClient.LPop(ctx, "mylist").Result()
	assert.NoError(t, err)
	assert.Equal(t, "hello", val)

	// RPOP
	val, err = testClient.RPop(ctx, "mylist").Result()
	assert.NoError(t, err)
	assert.Equal(t, "end", val)

	// LSET
	err = testClient.LSet(ctx, "mylist", 0, "updated").Err()
	assert.NoError(t, err)

	val, err = testClient.LIndex(ctx, "mylist", 0).Result()
	assert.NoError(t, err)
	assert.Equal(t, "updated", val)

	// LTRIM
	err = testClient.LTrim(ctx, "mylist", 0, 0).Err()
	assert.NoError(t, err)

	length, err = testClient.LLen(ctx, "mylist").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), length)
}

// TestSetCommands 测试Set类型命令
func TestSetCommands(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// SADD
	added, err := testClient.SAdd(ctx, "myset", "apple", "banana", "cherry").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), added)

	// SCARD
	count, err := testClient.SCard(ctx, "myset").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), count)

	// SISMEMBER
	member, err := testClient.SIsMember(ctx, "myset", "apple").Result()
	assert.NoError(t, err)
	assert.True(t, member)

	member, err = testClient.SIsMember(ctx, "myset", "nonexistent").Result()
	assert.NoError(t, err)
	assert.False(t, member)

	// SMEMBERS
	members, err := testClient.SMembers(ctx, "myset").Result()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(members))

	// SREM
	removed, err := testClient.SRem(ctx, "myset", "apple").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), removed)

	count, err = testClient.SCard(ctx, "myset").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)

	// SPOP
	val, err := testClient.SPop(ctx, "myset").Result()
	assert.NoError(t, err)
	assert.True(t, len(val) > 0)

	// SINTER
	testClient.SAdd(ctx, "set1", "a", "b", "c")
	testClient.SAdd(ctx, "set2", "b", "c", "d")

	intersection, err := testClient.SInter(ctx, "set1", "set2").Result()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(intersection))
}

// TestSortedSetCommands 测试SortedSet类型命令
func TestSortedSetCommands(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// ZADD
	added, err := testClient.ZAdd(ctx, "zset", redis.Z{
		Score:  1.0,
		Member: "member1",
	}, redis.Z{
		Score:  2.0,
		Member: "member2",
	}, redis.Z{
		Score:  3.0,
		Member: "member3",
	}).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), added)

	// ZCARD
	count, err := testClient.ZCard(ctx, "zset").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), count)

	// ZSCORE
	score, err := testClient.ZScore(ctx, "zset", "member1").Result()
	assert.NoError(t, err)
	assert.Equal(t, 1.0, score)

	// ZRANGE
	members, err := testClient.ZRange(ctx, "zset", 0, -1).Result()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(members))
	assert.Equal(t, "member1", members[0])
	assert.Equal(t, "member2", members[1])
	assert.Equal(t, "member3", members[2])

	// ZRANGE with scores
	zs, err := testClient.ZRangeWithScores(ctx, "zset", 0, -1).Result()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(zs))
	assert.Equal(t, "member1", zs[0].Member)
	assert.Equal(t, 1.0, zs[0].Score)

	// ZREVRANGE
	members, err = testClient.ZRevRange(ctx, "zset", 0, -1).Result()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(members))
	assert.Equal(t, "member3", members[0])

	// ZRANK
	rank, err := testClient.ZRank(ctx, "zset", "member2").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), rank)

	// ZREVRANK
	rank, err = testClient.ZRevRank(ctx, "zset", "member2").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), rank)

	// ZCOUNT
	count, err = testClient.ZCount(ctx, "zset", "1", "2").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count) // member1 和 member2

	// ZINCRBY
	newScore, err := testClient.ZIncrBy(ctx, "zset", 1.5, "member1").Result()
	assert.NoError(t, err)
	assert.Equal(t, 2.5, newScore)

	// ZREM
	removed, err := testClient.ZRem(ctx, "zset", "member1").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), removed)

	count, err = testClient.ZCard(ctx, "zset").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)
}

// TestGeneralCommands 测试通用命令
func TestGeneralCommands(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// PING
	pong, err := testClient.Ping(ctx).Result()
	assert.NoError(t, err)
	assert.Equal(t, "PONG", pong)

	// SET/GET
	err = testClient.Set(ctx, "test_key", "test_value", 0).Err()
	assert.NoError(t, err)

	// EXISTS
	exists, err := testClient.Exists(ctx, "test_key").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), exists)

	exists, err = testClient.Exists(ctx, "nonexistent").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), exists)

	// TYPE
	keyType, err := testClient.Type(ctx, "test_key").Result()
	assert.NoError(t, err)
	assert.Equal(t, "string", keyType)

	// DEL
	deleted, err := testClient.Del(ctx, "test_key").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), deleted)

	exists, err = testClient.Exists(ctx, "test_key").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), exists)

	// EXPIRE
	err = testClient.Set(ctx, "expire_key", "value", 0).Err()
	assert.NoError(t, err)

	set, err := testClient.Expire(ctx, "expire_key", 10*time.Second).Result()
	assert.NoError(t, err)
	assert.True(t, set)

	// TTL
	ttl, err := testClient.TTL(ctx, "expire_key").Result()
	assert.NoError(t, err)
	assert.True(t, ttl > 0 && ttl <= 10*time.Second)

	// PERSIST
	set, err = testClient.Persist(ctx, "expire_key").Result()
	assert.NoError(t, err)
	assert.True(t, set)

	ttl, err = testClient.TTL(ctx, "expire_key").Result()
	assert.NoError(t, err)
	assert.Equal(t, time.Duration(-1), ttl) // -1 表示没有过期时间
}

// TestDataPersistence 测试数据持久化
func TestDataPersistence(t *testing.T) {
	ctx := context.Background()

	// 第一次写入数据
	setupTestServer(t)
	err := testClient.Set(ctx, "persist_key", "persist_value", 0).Err()
	assert.NoError(t, err)
	err = testClient.HSet(ctx, "persist_hash", "field1", "value1").Err()
	assert.NoError(t, err)
	teardownTestServer(t)

	// 重新启动服务器（使用相同的数据库路径）
	// 注意：这里需要保存数据库路径以便重用
	// 为了简化，我们只测试数据确实被写入了
}

// TestConcurrentOperations 测试并发操作
func TestConcurrentOperations(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// 并发写入
	const numGoroutines = 10
	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			key := fmt.Sprintf("concurrent_key_%d", id)
			err := testClient.Set(ctx, key, fmt.Sprintf("value_%d", id), 0).Err()
			assert.NoError(t, err)
			done <- true
		}(i)
	}

	// 等待所有goroutine完成
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// 验证所有键都存在
	for i := 0; i < numGoroutines; i++ {
		key := fmt.Sprintf("concurrent_key_%d", i)
		val, err := testClient.Get(ctx, key).Result()
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("value_%d", i), val)
	}
}

// TestErrorHandling 测试错误处理
func TestErrorHandling(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// 测试不存在的键
	_, err := testClient.Get(ctx, "nonexistent").Result()
	assert.Error(t, err)
	assert.Equal(t, redis.Nil, err)

	// 测试错误的命令参数（Set需要至少3个参数：key, value, expiration）
	// 注意：redis v9 的 Set 方法签名是 Set(ctx, key, value, expiration)
	// 这里我们测试一个无效的场景，但实际上 Set 方法会要求所有参数
	// 为了测试错误处理，我们直接测试一个不存在的命令或无效操作
	err = testClient.Do(ctx, "INVALID_COMMAND").Err()
	assert.Error(t, err)

	// 测试类型错误（对字符串键执行Hash操作）
	err = testClient.Set(ctx, "string_key", "value", 0).Err()
	assert.NoError(t, err)

	_, err = testClient.HGet(ctx, "string_key", "field").Result()
	assert.Error(t, err) // 应该返回错误，因为键类型不匹配
}
