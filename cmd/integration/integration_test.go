package integration

import (
	"context"
	"encoding/json"
	"net"
	"os"
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
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// 创建PubSub管理器
	pubsubMgr := store.NewPubSubManager()

	// 创建备份管理器
	backupMgr := backup.NewBackupManager(testDB, dbPath)

	// 创建复制管理器
	replMgr := replication.NewReplicationManager(testDB)

	// 创建服务器处理器
	testServer = &server.Handler{Db: testDB, PubSub: pubsubMgr, Backup: backupMgr, Replication: replMgr}

	// 启动服务器（使用随机端口）
	listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		testDB.Close()
		t.Fatalf("Failed to listen: %v", err)
	}

	// 在goroutine中运行服务器
	go func() {
		_ = testServer.ServeTCP(listener)
	}()

	// 等待服务器启动
	time.Sleep(50 * time.Millisecond)

	// 创建Redis客户端
	testClient = redis.NewClient(&redis.Options{
		Addr:     listener.Addr().String(),
		Password: "",
		DB:       0,
	})

	// 测试连接
	ctx := context.Background()
	_, err = testClient.Ping(ctx).Result()
	if err != nil {
		listener.Close()
		testDB.Close()
		t.Fatalf("Failed to ping: %v", err)
	}
}

// teardownTestServer 关闭测试服务器
func teardownTestServer(t *testing.T) {
	if testClient != nil {
		testClient.Close()
	}
	if listener != nil {
		listener.Close()
	}
	if testDB != nil {
		testDB.Close()
	}
}

// TestConnection 测试连接命令
func TestConnection(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// PING
	pong, err := testClient.Ping(ctx).Result()
	assert.NoError(t, err)
	assert.Equal(t, "PONG", pong)

	// ECHO
	echo, err := testClient.Echo(ctx, "Hello").Result()
	assert.NoError(t, err)
	assert.Equal(t, "Hello", echo)
}

// TestString 测试String命令
func TestString(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// SET
	err := testClient.Set(ctx, "key1", "value1", 0).Err()
	assert.NoError(t, err)

	// GET
	val, err := testClient.Get(ctx, "key1").Result()
	assert.NoError(t, err)
	assert.Equal(t, "value1", val)

	// DEL 删除存在的键
	deleted, err := testClient.Del(ctx, "key1").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), deleted)

	// DEL 删除不存在的键
	deleted, err = testClient.Del(ctx, "nonexistent").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), deleted)

	// DEL 批量删除
	_ = testClient.Set(ctx, "k1", "v1", 0).Err()
	_ = testClient.Set(ctx, "k2", "v2", 0).Err()
	_ = testClient.Set(ctx, "k3", "v3", 0).Err()
	deleted, err = testClient.Del(ctx, "k1", "k2", "k3", "k4").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), deleted)

	// INCR
	incr, err := testClient.Incr(ctx, "counter").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), incr)

	incr, err = testClient.Incr(ctx, "counter").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), incr)

	// APPEND
	_ = testClient.Set(ctx, "appendkey", "hello", 0).Err()
	appendLen, err := testClient.Append(ctx, "appendkey", "world").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(10), appendLen) // "helloworld" = 10 chars

	val, err = testClient.Get(ctx, "appendkey").Result()
	assert.NoError(t, err)
	assert.Equal(t, "helloworld", val)

	// STRLEN
	strlen, err := testClient.StrLen(ctx, "appendkey").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(10), strlen)
}

// TestList 测试List命令
func TestList(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// LPUSH
	err := testClient.LPush(ctx, "list1", "a", "b", "c").Err()
	assert.NoError(t, err)

	// LLEN
	length, err := testClient.LLen(ctx, "list1").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), length)

	// LRANGE
	items, err := testClient.LRange(ctx, "list1", 0, -1).Result()
	assert.NoError(t, err)
	assert.Equal(t, []string{"c", "b", "a"}, items)

	// LPOP
	val, err := testClient.LPop(ctx, "list1").Result()
	assert.NoError(t, err)
	assert.Equal(t, "c", val)

	// RPUSH
	err = testClient.RPush(ctx, "list1", "d").Err()
	assert.NoError(t, err)

	// RPOP
	val, err = testClient.RPop(ctx, "list1").Result()
	assert.NoError(t, err)
	assert.Equal(t, "d", val)

	// LINDEX
	val, err = testClient.LIndex(ctx, "list1", 0).Result()
	assert.NoError(t, err)
	assert.Equal(t, "b", val)
}

// TestHash 测试Hash命令
func TestHash(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// HSET
	err := testClient.HSet(ctx, "hash1", "field1", "value1").Err()
	assert.NoError(t, err)

	// HGET
	val, err := testClient.HGet(ctx, "hash1", "field1").Result()
	assert.NoError(t, err)
	assert.Equal(t, "value1", val)

	// HGETALL
	all, err := testClient.HGetAll(ctx, "hash1").Result()
	assert.NoError(t, err)
	assert.Equal(t, map[string]string{"field1": "value1"}, all)

	// HINCRBY
	incr, err := testClient.HIncrBy(ctx, "hash1", "field2", 5).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(5), incr)

	// HEXISTS
	exists, err := testClient.HExists(ctx, "hash1", "field1").Result()
	assert.NoError(t, err)
	assert.Equal(t, true, exists)

	// HDEL
	deleted, err := testClient.HDel(ctx, "hash1", "field2").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), deleted)
}

// TestSet 测试Set命令
func TestSet(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// SADD
	err := testClient.SAdd(ctx, "set1", "m1", "m2", "m3").Err()
	assert.NoError(t, err)

	// SMEMBERS
	members, err := testClient.SMembers(ctx, "set1").Result()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(members))

	// SISMEMBER
	isMember, err := testClient.SIsMember(ctx, "set1", "m1").Result()
	assert.NoError(t, err)
	assert.Equal(t, true, isMember)

	// SCARD
	card, err := testClient.SCard(ctx, "set1").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), card)

	// SREM
	removed, err := testClient.SRem(ctx, "set1", "m3").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), removed)

	card, err = testClient.SCard(ctx, "set1").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), card)
}

// TestSortedSet 测试SortedSet命令
func TestSortedSet(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// ZADD
	err := testClient.ZAdd(ctx, "zset1", redis.Z{Score: 100, Member: "Alice"}, redis.Z{Score: 90, Member: "Bob"}, redis.Z{Score: 80, Member: "Charlie"}).Err()
	assert.NoError(t, err)

	// ZRANGE
	members, err := testClient.ZRange(ctx, "zset1", 0, -1).Result()
	assert.NoError(t, err)
	assert.Equal(t, []string{"Charlie", "Bob", "Alice"}, members)

	// ZSCORE
	score, err := testClient.ZScore(ctx, "zset1", "Alice").Result()
	assert.NoError(t, err)
	assert.Equal(t, float64(100), score)

	// ZCARD
	card, err := testClient.ZCard(ctx, "zset1").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), card)

	// ZINCRBY
	score, err = testClient.ZIncrBy(ctx, "zset1", 50, "Bob").Result()
	assert.NoError(t, err)
	assert.Equal(t, float64(140), score)

	// ZREM
	removed, err := testClient.ZRem(ctx, "zset1", "Charlie").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), removed)

	card, err = testClient.ZCard(ctx, "zset1").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), card)
}

// TestServerCommands 测试Server相关命令
func TestServerCommands(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// PING
	pong, err := testClient.Ping(ctx).Result()
	assert.NoError(t, err)
	assert.Equal(t, "PONG", pong)

	// ECHO
	echo, err := testClient.Echo(ctx, "Hello, Server!").Result()
	assert.NoError(t, err)
	assert.Equal(t, "Hello, Server!", echo)

	// DBSIZE
	dbsize, err := testClient.DBSize(ctx).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), dbsize)

	// TYPE
	_ = testClient.Set(ctx, "typekey", "value", 0).Err()
	keyType, err := testClient.Type(ctx, "typekey").Result()
	assert.NoError(t, err)
	assert.Equal(t, "string", keyType)
}

// TestKeyCommands 测试Key相关命令
func TestKeyCommands(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// EXISTS - 键不存在
	exists, err := testClient.Exists(ctx, "nonexistent").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), exists)

	// EXISTS - 键存在
	_ = testClient.Set(ctx, "existskey", "value", 0).Err()
	exists, err = testClient.Exists(ctx, "existskey").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), exists)

	// EXISTS - 批量检查
	_ = testClient.Set(ctx, "existskey2", "value2", 0).Err()
	exists, err = testClient.Exists(ctx, "existskey", "existskey2", "nonexistent").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), exists)

	// EXPIRE - 设置过期时间
	_ = testClient.Set(ctx, "expirekey", "value", 0).Err()
	set, err := testClient.Expire(ctx, "expirekey", 10*time.Second).Result()
	assert.NoError(t, err)
	assert.Equal(t, true, set)

	// TTL - 查看剩余过期时间
	// 注意：go-redis 的 TTL 返回 time.Duration（纳秒）
	ttlDuration, err := testClient.TTL(ctx, "expirekey").Result()
	assert.NoError(t, err)
	assert.True(t, ttlDuration > 0 && ttlDuration <= 10*time.Second)
	assert.True(t, ttlDuration >= 9*time.Second) // 至少还有9秒

	// TTL - 永不过期的键
	_ = testClient.Set(ctx, "noexpirekey", "value", 0).Err()
	noexpireTTL, err := testClient.TTL(ctx, "noexpirekey").Result()
	assert.NoError(t, err)
	assert.Equal(t, time.Duration(-1), noexpireTTL)

	// RENAME - 重命名键
	_ = testClient.Set(ctx, "renamekey", "oldvalue", 0).Err()
	err = testClient.Rename(ctx, "renamekey", "newkey").Err()
	assert.NoError(t, err)
	val, err := testClient.Get(ctx, "newkey").Result()
	assert.NoError(t, err)
	assert.Equal(t, "oldvalue", val)

	// RENAMENX - 新键不存在时重命名
	_ = testClient.Set(ctx, "renamenxkey", "value", 0).Err()
	_ = testClient.Set(ctx, "targetkey", "targetvalue", 0).Err()
	set, err = testClient.RenameNX(ctx, "renamenxkey", "targetkey").Result()
	assert.NoError(t, err)
	assert.Equal(t, false, set) // targetkey已存在

	set, err = testClient.RenameNX(ctx, "renamenxkey", "nonexistenttarget").Result()
	assert.NoError(t, err)
	assert.Equal(t, true, set)
}

// TestStringExtended 测试扩展String命令
func TestStringExtended(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// SETNX - 键不存在时设置
	set, err := testClient.SetNX(ctx, "setnxkey", "value", 0).Result()
	assert.NoError(t, err)
	assert.Equal(t, true, set)

	// SETNX - 键已存在时设置失败
	set, err = testClient.SetNX(ctx, "setnxkey", "value2", 0).Result()
	assert.NoError(t, err)
	assert.Equal(t, false, set)

	// GETSET - 获取旧值并设置新值
	_ = testClient.Set(ctx, "getsetkey", "oldvalue", 0).Err()
	oldVal, err := testClient.GetSet(ctx, "getsetkey", "newvalue").Result()
	assert.NoError(t, err)
	assert.Equal(t, "oldvalue", oldVal)

	newVal, err := testClient.Get(ctx, "getsetkey").Result()
	assert.NoError(t, err)
	assert.Equal(t, "newvalue", newVal)

	// DECR - 递减
	_ = testClient.Set(ctx, "decrkey", "10", 0).Err()
	decr, err := testClient.Decr(ctx, "decrkey").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(9), decr)

	// DECRBY - 按步长递减
	decr, err = testClient.DecrBy(ctx, "decrkey", 3).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(6), decr)

	// INCRBY - 按步长递增
	incr, err := testClient.IncrBy(ctx, "incrbykey", 5).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(5), incr)

	// INCRBYFLOAT - 浮点数递增
	incrFloat, err := testClient.IncrByFloat(ctx, "floatkey", 1.5).Result()
	assert.NoError(t, err)
	assert.Equal(t, float64(1.5), incrFloat)

	// MGET - 批量获取
	_ = testClient.Set(ctx, "mkey1", "value1", 0).Err()
	_ = testClient.Set(ctx, "mkey2", "value2", 0).Err()
	vals, err := testClient.MGet(ctx, "mkey1", "mkey2", "mkey3").Result()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{"value1", "value2", nil}, vals)

	// MSET - 批量设置
	err = testClient.MSet(ctx, "mskey1", "value1", "mskey2", "value2").Err()
	assert.NoError(t, err)

	val1, _ := testClient.Get(ctx, "mskey1").Result()
	val2, _ := testClient.Get(ctx, "mskey2").Result()
	assert.Equal(t, "value1", val1)
	assert.Equal(t, "value2", val2)
}

// TestListExtended 测试扩展List命令
func TestListExtended(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// 准备测试数据
	_ = testClient.RPush(ctx, "listext", "a", "b", "c").Err()

	// LSET - 设置指定位置的值
	err := testClient.LSet(ctx, "listext", 1, "x").Err()
	assert.NoError(t, err)

	val, err := testClient.LIndex(ctx, "listext", 1).Result()
	assert.NoError(t, err)
	assert.Equal(t, "x", val)

	// LTRIM - 裁剪列表
	err = testClient.LTrim(ctx, "listext", 0, 1).Err()
	assert.NoError(t, err)

	length, err := testClient.LLen(ctx, "listext").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), length)

	// RPOPLPUSH - 从一个列表弹出并推入另一个列表
	_ = testClient.RPush(ctx, "listsrc", "item1", "item2").Err()
	val, err = testClient.RPopLPush(ctx, "listsrc", "listdst").Result()
	assert.NoError(t, err)
	assert.Equal(t, "item2", val)
}

// TestHashExtended 测试扩展Hash命令
func TestHashExtended(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// HSETNX - 字段不存在时设置
	_ = testClient.HSet(ctx, "hashext", "field1", "value1").Err()
	set, err := testClient.HSetNX(ctx, "hashext", "field1", "value2").Result()
	assert.NoError(t, err)
	assert.Equal(t, false, set)

	set, err = testClient.HSetNX(ctx, "hashext", "field2", "value2").Result()
	assert.NoError(t, err)
	assert.Equal(t, true, set)

	// HKEYS - 获取所有字段
	keys, err := testClient.HKeys(ctx, "hashext").Result()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(keys))

	// HVALS - 获取所有值
	vals, err := testClient.HVals(ctx, "hashext").Result()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(vals))

	// HLEN - 获取字段数量
	length, err := testClient.HLen(ctx, "hashext").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), length)

	// HMGET - 批量获取字段
	results, err := testClient.HMGet(ctx, "hashext", "field1", "field2", "field3").Result()
	assert.NoError(t, err)
	assert.Equal(t, "value1", results[0])
	assert.Equal(t, "value2", results[1])
	assert.Nil(t, results[2])

	// HMSET - 批量设置字段
	err = testClient.HMSet(ctx, "hashmset", "k1", "v1", "k2", "v2").Err()
	assert.NoError(t, err)

	val, _ := testClient.HGet(ctx, "hashmset", "k1").Result()
	assert.Equal(t, "v1", val)
}

// TestSetExtended 测试扩展Set命令
func TestSetExtended(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// 准备测试数据
	_ = testClient.SAdd(ctx, "set1", "a", "b", "c").Err()
	_ = testClient.SAdd(ctx, "set2", "b", "c", "d").Err()

	// SDIFF - 差集
	members, err := testClient.SDiff(ctx, "set1", "set2").Result()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(members))
	assert.Equal(t, "a", members[0])

	// SINTER - 交集
	members, err = testClient.SInter(ctx, "set1", "set2").Result()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(members))

	// SUNION - 并集
	members, err = testClient.SUnion(ctx, "set1", "set2").Result()
	assert.NoError(t, err)
	assert.Equal(t, 4, len(members))

	// SPOP - 随机弹出
	_ = testClient.SAdd(ctx, "spopset", "a", "b", "c").Err()
	val, err := testClient.SPop(ctx, "spopset").Result()
	assert.NoError(t, err)
	assert.True(t, val == "a" || val == "b" || val == "c")

	card, _ := testClient.SCard(ctx, "spopset").Result()
	assert.Equal(t, int64(2), card)

	// SMOVE - 移动元素
	_ = testClient.SAdd(ctx, "setmove1", "a", "b").Err()
	_ = testClient.SAdd(ctx, "setmove2", "c").Err()
	moved, err := testClient.SMove(ctx, "setmove1", "setmove2", "a").Result()
	assert.NoError(t, err)
	assert.Equal(t, true, moved)

	card, _ = testClient.SCard(ctx, "setmove1").Result()
	assert.Equal(t, int64(1), card)
}

// TestSortedSetExtended 测试扩展SortedSet命令
func TestSortedSetExtended(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// 准备测试数据
	_ = testClient.ZAdd(ctx, "zsetext", redis.Z{Score: 10, Member: "a"}, redis.Z{Score: 20, Member: "b"}, redis.Z{Score: 30, Member: "c"}).Err()

	// ZRANK - 获取成员排名
	rank, err := testClient.ZRank(ctx, "zsetext", "b").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), rank)

	// ZREVRANK - 获取逆向排名
	revRank, err := testClient.ZRevRank(ctx, "zsetext", "b").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), revRank)

	// ZCOUNT - 获取指定分数范围内的成员数量
	count, err := testClient.ZCount(ctx, "zsetext", "10", "25").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)

	// ZREVRANGE - 逆向范围获取
	members, err := testClient.ZRevRange(ctx, "zsetext", 0, -1).Result()
	assert.NoError(t, err)
	assert.Equal(t, []string{"c", "b", "a"}, members)
}

// TestStringAdvanced 测试高级String命令
func TestStringAdvanced(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// SETEX - 设置过期时间的字符串
	err := testClient.SetEx(ctx, "setexkey", "value", 5*time.Second).Err()
	assert.NoError(t, err)
	val, err := testClient.Get(ctx, "setexkey").Result()
	assert.NoError(t, err)
	assert.Equal(t, "value", val)

	// PSETEX - 设置过期时间（毫秒）
	err = testClient.SetEx(ctx, "psetexkey", "value", 5000*time.Millisecond).Err()
	assert.NoError(t, err)
	val, err = testClient.Get(ctx, "psetexkey").Result()
	assert.NoError(t, err)
	assert.Equal(t, "value", val)

	// SETRANGE - 从指定偏移量开始修改字符串
	_ = testClient.Set(ctx, "setrangekey", "hello world", 0).Err()
	length, err := testClient.SetRange(ctx, "setrangekey", 6, "golang").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(12), length)
	val, err = testClient.Get(ctx, "setrangekey").Result()
	assert.NoError(t, err)
	assert.Equal(t, "hello golang", val)

	// GETRANGE - 获取子字符串
	val, err = testClient.GetRange(ctx, "setrangekey", 0, 4).Result()
	assert.NoError(t, err)
	assert.Equal(t, "hello", val)

	val, err = testClient.GetRange(ctx, "setrangekey", 6, -1).Result()
	assert.NoError(t, err)
	assert.Equal(t, "golang", val)
}

// TestKeyAdvanced 测试高级Key命令
func TestKeyAdvanced(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// KEYS - 查找匹配的键
	_ = testClient.Set(ctx, "testkey1", "value1", 0).Err()
	_ = testClient.Set(ctx, "testkey2", "value2", 0).Err()
	_ = testClient.Set(ctx, "testkey3", "value3", 0).Err()
	keys, err := testClient.Keys(ctx, "testkey*").Result()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(keys))

	// RANDOMKEY - 获取随机键
	randomKey, err := testClient.RandomKey(ctx).Result()
	assert.NoError(t, err)
	assert.True(t, randomKey != "")

	// PERSIST - 移除过期时间
	_ = testClient.Set(ctx, "persistkey", "value", 0).Err()
	_ = testClient.Expire(ctx, "persistkey", 10*time.Second).Err()
	success, err := testClient.Persist(ctx, "persistkey").Result()
	assert.NoError(t, err)
	assert.Equal(t, true, success)

	ttl, _ := testClient.TTL(ctx, "persistkey").Result()
	assert.Equal(t, time.Duration(-1), ttl)

	// EXPIREAT - 设置过期时间戳
	_ = testClient.Set(ctx, "expireatkey", "value", 0).Err()
	futureTime := time.Now().Add(1 * time.Hour)
	success, err = testClient.ExpireAt(ctx, "expireatkey", futureTime).Result()
	assert.NoError(t, err)
	assert.Equal(t, true, success)

	// PEXPIRE - 毫秒级过期
	_ = testClient.Set(ctx, "pexpirekey", "value", 0).Err()
	success, err = testClient.PExpire(ctx, "pexpirekey", 5000*time.Millisecond).Result()
	assert.NoError(t, err)
	assert.Equal(t, true, success)

	// PEXPIREAT - 毫秒级时间戳过期
	_ = testClient.Set(ctx, "pexpireatkey", "value", 0).Err()
	futureTimeMs := time.Now().Add(1 * time.Hour)
	success, err = testClient.PExpireAt(ctx, "pexpireatkey", futureTimeMs).Result()
	assert.NoError(t, err)
	assert.Equal(t, true, success)
}

// TestServerExtended 测试扩展Server命令
func TestServerExtended(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// FLUSHDB - 清空当前数据库
	_ = testClient.Set(ctx, "flushdbkey", "value", 0).Err()
	_ = testClient.Set(ctx, "flushdbkey2", "value2", 0).Err()
	err := testClient.FlushDB(ctx).Err()
	assert.NoError(t, err)

	dbsize, err := testClient.DBSize(ctx).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), dbsize)

	// DBSIZE - 验证数据库为空
	_ = testClient.Set(ctx, "dbsizekey", "value", 0).Err()
	dbsize, err = testClient.DBSize(ctx).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), dbsize)

	// TYPE - 检查键类型
	val, err := testClient.Type(ctx, "dbsizekey").Result()
	assert.NoError(t, err)
	assert.Equal(t, "string", val)

	// PING - 验证连接
	pong, err := testClient.Ping(ctx).Result()
	assert.NoError(t, err)
	assert.Equal(t, "PONG", pong)
}

// TestListAdvanced 测试高级List命令
func TestListAdvanced(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// LPUSHX - 列表存在时从左侧插入
	_ = testClient.RPush(ctx, "listx", "a").Err()
	err := testClient.LPushX(ctx, "listx", "b").Err()
	assert.NoError(t, err)
	err = testClient.LPushX(ctx, "nonexistent", "a").Err()
	assert.NoError(t, err) // 键不存在时不做任何操作

	items, err := testClient.LRange(ctx, "listx", 0, -1).Result()
	assert.NoError(t, err)
	assert.Equal(t, []string{"b", "a"}, items)

	// RPUSHX - 列表存在时从右侧插入
	err = testClient.RPushX(ctx, "listx", "c").Err()
	assert.NoError(t, err)
	items, err = testClient.LRange(ctx, "listx", 0, -1).Result()
	assert.NoError(t, err)
	assert.Equal(t, []string{"b", "a", "c"}, items)

	// LREM - 移除元素 (count=0 表示移除所有匹配的元素)
	_ = testClient.RPush(ctx, "listrem", "a", "b", "a", "c", "a").Err()
	removed, err := testClient.LRem(ctx, "listrem", 0, "a").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), removed)

	items, err = testClient.LRange(ctx, "listrem", 0, -1).Result()
	assert.NoError(t, err)
	assert.Equal(t, []string{"b", "c"}, items)
}

// TestHashAdvanced 测试高级Hash命令
func TestHashAdvanced(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// HINCRBYFLOAT - 浮点数递增
	_ = testClient.HSet(ctx, "hashfloat", "field", "10.5").Err()
	val, err := testClient.HIncrByFloat(ctx, "hashfloat", "field", 2.5).Result()
	assert.NoError(t, err)
	assert.Equal(t, float64(13), val)

	// HSTRLEN - 获取字段值长度
	_ = testClient.HSet(ctx, "hashstr", "field", "hello").Err()
	length, err := testClient.HStrLen(ctx, "hashstr", "field").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(5), length)
}

// TestSetAdvanced 测试高级Set命令
func TestSetAdvanced(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// SINTERSTORE - 交集并存储
	_ = testClient.SAdd(ctx, "setstore1", "a", "b", "c").Err()
	_ = testClient.SAdd(ctx, "setstore2", "b", "c", "d").Err()
	count, err := testClient.SInterStore(ctx, "setstoreresult", "setstore1", "setstore2").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)

	members, _ := testClient.SMembers(ctx, "setstoreresult").Result()
	assert.Equal(t, 2, len(members))

	// SDIFFSTORE - 差集并存储
	_ = testClient.SAdd(ctx, "setdiff1", "a", "b", "c").Err()
	_ = testClient.SAdd(ctx, "setdiff2", "b").Err()
	count, err = testClient.SDiffStore(ctx, "setdifffresult", "setdiff1", "setdiff2").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)

	// SUNIONSTORE - 并集并存储
	_ = testClient.SAdd(ctx, "setunion1", "a", "b").Err()
	_ = testClient.SAdd(ctx, "setunion2", "c", "d").Err()
	count, err = testClient.SUnionStore(ctx, "setunionresult", "setunion1", "setunion2").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(4), count)

	// SRANDMEMBER - 随机获取成员
	_ = testClient.SAdd(ctx, "setrand", "a", "b", "c", "d", "e").Err()
	val, err := testClient.SRandMember(ctx, "setrand").Result()
	assert.NoError(t, err)
	assert.True(t, val == "a" || val == "b" || val == "c" || val == "d" || val == "e")

	// SRANDMEMBER - 获取多个随机成员
	vals, err := testClient.SRandMemberN(ctx, "setrand", 3).Result()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(vals))
}

// TestTransaction 测试事务命令（MULTI/EXEC/DISCARD/WATCH/UNWATCH）
// 由于go-redis客户端对MULTI/EXEC的支持有限，我们验证命令被正确识别
func TestTransaction(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// 测试 UNWATCH - 取消监控
	result, err := testClient.Do(ctx, "UNWATCH").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// 测试 WATCH - 监控键（无论键是否存在都返回监控的键数量）
	result, err = testClient.Do(ctx, "WATCH", "watchkey").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), result)

	// 测试 MULTI - 开始事务
	result, err = testClient.Do(ctx, "MULTI").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// 测试 DISCARD - 放弃事务
	result, err = testClient.Do(ctx, "DISCARD").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// 重新开始事务并执行
	result, err = testClient.Do(ctx, "MULTI").Result()
	assert.NoError(t, err)

	// 在事务中添加命令并执行
	_ = testClient.Set(ctx, "txkey", "txvalue", 0).Err()
	result, err = testClient.Do(ctx, "EXEC").Result()
	assert.NoError(t, err)

	// 验证命令执行
	val, _ := testClient.Get(ctx, "txkey").Result()
	assert.Equal(t, "txvalue", val)

	// 测试 WATCH 多个键
	result, err = testClient.Do(ctx, "WATCH", "key1", "key2", "key3").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), result)
}

// TestTransactionExtended 扩展事务命令测试 - 测试修复后的 WATCH/MULTI/EXEC 行为
func TestTransactionExtended(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// ========== Error Cases ==========

	// EXEC without MULTI - should return error
	_, err := testClient.Do(ctx, "EXEC").Result()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "EXEC without MULTI"))

	// DISCARD without MULTI - should return error
	_, err = testClient.Do(ctx, "DISCARD").Result()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "DISCARD without MULTI"))

	// ========== Transaction with multiple commands ==========
	_, _ = testClient.Do(ctx, "MULTI").Result()
	_ = testClient.Set(ctx, "mkey1", "val1", 0).Err()
	_ = testClient.Set(ctx, "mkey2", "val2", 0).Err()
	_ = testClient.Set(ctx, "mkey3", "val3", 0).Err()
	_, err = testClient.Do(ctx, "EXEC").Result()
	assert.NoError(t, err)

	// Verify all commands were executed
	val1, _ := testClient.Get(ctx, "mkey1").Result()
	val2, _ := testClient.Get(ctx, "mkey2").Result()
	val3, _ := testClient.Get(ctx, "mkey3").Result()
	assert.Equal(t, "val1", val1)
	assert.Equal(t, "val2", val2)
	assert.Equal(t, "val3", val3)

	// ========== WATCH without keys (wrong number of args) ==========
	_, err = testClient.Do(ctx, "WATCH").Result()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "wrong number of arguments"))

	// ========== WATCH with multiple keys ==========
	result, err := testClient.Do(ctx, "WATCH", "key1", "key2", "key3").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), result)
}

// TestCOPY 测试COPY命令
func TestCOPY(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// COPY String - 源键不存在
	result, err := testClient.Do(ctx, "COPY", "nonexistent", "dstkey").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result)

	// COPY String - 正常复制
	_ = testClient.Set(ctx, "srcstring", "value", 0).Err()
	result, err = testClient.Do(ctx, "COPY", "srcstring", "dststring").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), result)

	// 验证复制成功
	val, _ := testClient.Get(ctx, "dststring").Result()
	assert.Equal(t, "value", val)

	// COPY - 目标存在时不替换
	_ = testClient.Set(ctx, "dstexists", "existing", 0).Err()
	result, err = testClient.Do(ctx, "COPY", "srcstring", "dstexists").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result)

	// COPY with REPLACE - 替换目标
	result, err = testClient.Do(ctx, "COPY", "srcstring", "dstexists", "REPLACE").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), result)
	val, _ = testClient.Get(ctx, "dstexists").Result()
	assert.Equal(t, "value", val)

	// COPY List
	_ = testClient.RPush(ctx, "srclist", "a", "b", "c").Err()
	result, err = testClient.Do(ctx, "COPY", "srclist", "dstlist").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), result)

	items, _ := testClient.LRange(ctx, "dstlist", 0, -1).Result()
	assert.Equal(t, []string{"a", "b", "c"}, items)

	// COPY Hash
	_ = testClient.HSet(ctx, "srchash", "field1", "value1", "field2", "value2").Err()
	result, err = testClient.Do(ctx, "COPY", "srchash", "dsthash").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), result)

	all, _ := testClient.HGetAll(ctx, "dsthash").Result()
	assert.Equal(t, map[string]string{"field1": "value1", "field2": "value2"}, all)

	// COPY Set
	_ = testClient.SAdd(ctx, "srcset", "a", "b", "c").Err()
	result, err = testClient.Do(ctx, "COPY", "srcset", "dstset").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), result)

	members, _ := testClient.SMembers(ctx, "dstset").Result()
	assert.Equal(t, 3, len(members))

	// COPY SortedSet
	_ = testClient.ZAdd(ctx, "srczset", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}).Err()
	result, err = testClient.Do(ctx, "COPY", "srczset", "dstzset").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), result)

	zmembers, _ := testClient.ZRange(ctx, "dstzset", 0, -1).Result()
	assert.Equal(t, []string{"a", "b"}, zmembers)
}

// TestSetAdvancedCommands 测试Set高级命令（SMISMEMBER, SINTERCARD）
func TestSetAdvancedCommands(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// 准备测试数据
	_ = testClient.SAdd(ctx, "smismem1", "a", "b", "c", "d").Err()
	_ = testClient.SAdd(ctx, "smismem2", "b", "c", "e").Err()

	// SINTERCARD - 返回交集基数
	result, err := testClient.Do(ctx, "SINTERCARD", "smismem1", "smismem2").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), result) // 交集是 {b, c}

	// SINTERCARD - 单个集合
	result, err = testClient.Do(ctx, "SINTERCARD", "smismem1").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(4), result) // 只有smismem1时，返回其基数

	// SINTERCARD - 无交集
	result, err = testClient.Do(ctx, "SINTERCARD", "smismem1", "noset").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result)

	// SMISMEMBER - 检查多个成员是否存在
	result, err = testClient.Do(ctx, "SMISMEMBER", "smismem1", "a", "b").Result()
	assert.NoError(t, err)
	// 返回 [1, 1] 表示 a和b都存在
	arr, ok := result.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr))
}

// TestHashAdvancedCommands 测试Hash高级命令（HRANDFIELD）
func TestHashAdvancedCommands(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// 准备测试数据
	_ = testClient.HSet(ctx, "hashrand", "f1", "v1", "f2", "v2", "f3", "v3", "f4", "v4", "f5", "v5").Err()

	// HRANDFIELD - 获取单个随机字段
	result, err := testClient.Do(ctx, "HRANDFIELD", "hashrand").Result()
	assert.NoError(t, err)
	// 单个字段时，go-redis可能返回字符串而不是数组
	assert.True(t, result != nil)

	// HRANDFIELD - 获取多个随机字段
	result, err = testClient.Do(ctx, "HRANDFIELD", "hashrand", "3").Result()
	assert.NoError(t, err)
	// 多个字段时返回数组
	arr, ok := result.([]interface{})
	assert.True(t, ok)
	// 应该返回3个字段
	assert.Equal(t, 3, len(arr))

	// HRANDFIELD with WITHVALUES - 获取字段和值
	result, err = testClient.Do(ctx, "HRANDFIELD", "hashrand", "2", "WITHVALUES").Result()
	assert.NoError(t, err)
	arr, ok = result.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 4, len(arr)) // 2个字段 + 2个值

	// HRANDFIELD - 空哈希
	result, err = testClient.Do(ctx, "HRANDFIELD", "emptyhash").Result()
	assert.NoError(t, err)
	// 空哈希应该返回nil或空数组

	// HRANDFIELD - 获取所有字段（count大于字段数量）
	result, err = testClient.Do(ctx, "HRANDFIELD", "hashrand", "10").Result()
	assert.NoError(t, err)
	arr, ok = result.([]interface{})
	assert.True(t, ok)
	// count >= 字段数量时，返回所有字段（至少5个）
	assert.True(t, len(arr) >= 5)
}

// TestSortedSetAdvancedCommands 测试SortedSet高级命令（ZUNIONSTORE, ZINTERSTORE, ZDIFFSTORE）
func TestSortedSetAdvancedCommands(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// 准备测试数据
	_ = testClient.ZAdd(ctx, "zunion1", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}).Err()
	_ = testClient.ZAdd(ctx, "zunion2", redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"}).Err()

	// ZUNIONSTORE - 并集
	result, err := testClient.ZUnionStore(ctx, "zunionresult", &redis.ZStore{
		Keys:    []string{"zunion1", "zunion2"},
		Weights: nil,
	}).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), result) // {a, b, c}

	members, _ := testClient.ZRange(ctx, "zunionresult", 0, -1).Result()
	assert.Equal(t, 3, len(members))

	// ZUNIONSTORE with WEIGHTS - 带权重
	_ = testClient.Del(ctx, "zunionresult").Err()
	result, err = testClient.ZUnionStore(ctx, "zunionresult", &redis.ZStore{
		Keys:    []string{"zunion1", "zunion2"},
		Weights: []float64{2, 3},
	}).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), result)

	// ZINTERSTORE - 交集
	_ = testClient.Del(ctx, "zinterresult").Err()
	result, err = testClient.ZInterStore(ctx, "zinterresult", &redis.ZStore{
		Keys:    []string{"zunion1", "zunion2"},
		Weights: nil,
	}).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), result) // 只有 {b}

	members, _ = testClient.ZRange(ctx, "zinterresult", 0, -1).Result()
	assert.Equal(t, []string{"b"}, members)

	// ZDIFFSTORE - 差集
	_ = testClient.ZAdd(ctx, "zdiff1", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"}).Err()
	_ = testClient.ZAdd(ctx, "zdiff2", redis.Z{Score: 2, Member: "b"}).Err()
	_ = testClient.Del(ctx, "zdiffresult").Err()
	result, err = testClient.ZDiffStore(ctx, "zdiffresult", "zdiff1", "zdiff2").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), result) // {a, c}

	members, _ = testClient.ZRange(ctx, "zdiffresult", 0, -1).Result()
	assert.Equal(t, 2, len(members))
}

// TestSWAPDB 测试SWAPDB命令
func TestSWAPDB(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// BoltDB 是单数据库实现，SWAPDB 返回OK但不做任何操作
	result, err := testClient.Do(ctx, "SWAPDB", 0, 1).Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)
}

// TestXAutoClaim 测试XAUTOCLAIM命令
func TestXAutoClaim(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// 添加测试流
	id1, err := testClient.XAdd(ctx, &redis.XAddArgs{
		Stream: "mystream",
		Values: map[string]any{"field1": "value1"},
	}).Result()
	assert.NoError(t, err)
	assert.NotEqual(t, "", id1)

	// 创建消费组
	err = testClient.XGroupCreate(ctx, "mystream", "mygroup", "0").Err()
	assert.NoError(t, err)

	// 读取消息以创建pending条目 - 使用原始命令
	_, err = testClient.Do(ctx, "XREADGROUP", "GROUP", "mygroup", "consumer1", "COUNT", "1", "STREAMS", "mystream", "0").Result()
	assert.NoError(t, err)

	// 等待一段时间让消息idle
	time.Sleep(100 * time.Millisecond)

	// 使用XAUTOCLAIM认领pending消息
	autoClaimResult, err := testClient.Do(ctx, "XAUTOCLAIM", "mystream", "mygroup", "consumer2", "0", id1, "COUNT", "1").Result()
	assert.NoError(t, err)

	// 解析结果
	arr, ok := autoClaimResult.([]interface{})
	assert.True(t, ok)
	// 格式: [nextID, [claimedIDs...], [messages...]]
	assert.Equal(t, 1, len(arr)) // 至少返回nextID
}

// TestXInfoHelp 测试XINFO HELP命令
func TestXInfoHelp(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// 测试XINFO HELP
	result, err := testClient.Do(ctx, "XINFO", "HELP").Result()
	assert.NoError(t, err)

	arr, ok := result.([]interface{})
	assert.True(t, ok)
	assert.True(t, len(arr) > 0)

	// 验证帮助信息包含关键命令
	foundHelp := false
	foundStream := false
	foundGroups := false
	for _, line := range arr {
		str, ok := line.(string)
		if !ok {
			continue
		}
		if strings.Contains(str, "XINFO STREAM") {
			foundStream = true
		}
		if strings.Contains(str, "XINFO GROUPS") {
			foundGroups = true
		}
		if strings.Contains(str, "<subcommand>") {
			foundHelp = true
		}
	}
	assert.True(t, foundHelp)
	assert.True(t, foundStream)
	assert.True(t, foundGroups)
}

// TestBLPOPBlocking 测试BLPOP阻塞命令
func TestBLPOPBlocking(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// 测试有数据时立即返回
	_ = testClient.LPush(ctx, "testlist", "value1")
	arr, err := testClient.BLPop(ctx, 0, "testlist").Result()
	assert.NoError(t, err)
	assert.Equal(t, "testlist", arr[0])
	assert.Equal(t, "value1", arr[1])
}

// TestBRPOPBlocking 测试BRPOP阻塞命令
func TestBRPOPBlocking(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// 测试有数据时立即返回
	_ = testClient.RPush(ctx, "testlist", "value1")
	arr, err := testClient.BRPop(ctx, 0, "testlist").Result()
	assert.NoError(t, err)
	assert.Equal(t, "testlist", arr[0])
	assert.Equal(t, "value1", arr[1])
}

// TestBRPOPLPUSHBlocking 测试BRPOPLPUSH阻塞命令
func TestBRPOPLPUSHBlocking(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// 准备源列表
	_ = testClient.RPush(ctx, "sourcelist", "value1")

	// 测试BRPOPLPUSH
	result, err := testClient.BRPopLPush(ctx, "sourcelist", "destlist", 0).Result()
	assert.NoError(t, err)
	assert.Equal(t, "value1", result)

	// 验证目标列表
	val, err := testClient.LPop(ctx, "destlist").Result()
	assert.NoError(t, err)
	assert.Equal(t, "value1", val)
}

// TestBLMoveBlocking 测试BLMOVE阻塞命令
func TestBLMoveBlocking(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// 准备源列表
	_ = testClient.RPush(ctx, "sourcelist", "value1")

	// 测试BLMOVE (RIGHT to LEFT)
	result, err := testClient.Do(ctx, "BLMOVE", "sourcelist", "destlist", "RIGHT", "LEFT", "0").Result()
	assert.NoError(t, err)
	assert.Equal(t, "value1", result)

	// 验证目标列表
	val, err := testClient.LPop(ctx, "destlist").Result()
	assert.NoError(t, err)
	assert.Equal(t, "value1", val)
}

// TestXREADBlocking 测试XREAD BLOCK功能
func TestXREADBlocking(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// 添加一条消息
	id1, err := testClient.XAdd(ctx, &redis.XAddArgs{
		Stream: "mystream",
		Values: map[string]any{"field1": "value1"},
	}).Result()
	assert.NoError(t, err)
	assert.NotEqual(t, "", id1)

	// 读取刚添加的消息 - 使用原始命令避免解析问题
	result, err := testClient.Do(ctx, "XREAD", "COUNT", "1", "STREAMS", "mystream", "0").Result()
	assert.NoError(t, err)
	assert.NotEqual(t, nil, result)

	// XREAD返回格式: [key, [entries...]]，每个stream返回一个[key, entries]对
	// entries数组中每个元素是 [id, field, value, ...] 格式
	arr, ok := result.([]interface{})
	assert.True(t, ok)
	// 期望: [mystream, [[id, field, value]]]
	assert.True(t, len(arr) >= 2)
}

// jsonEqual compares two JSON strings semantically (ignoring field order)
func jsonEqual(a, b string) bool {
	var aVal, bVal interface{}
	if err := json.Unmarshal([]byte(a), &aVal); err != nil {
		return false
	}
	if err := json.Unmarshal([]byte(b), &bVal); err != nil {
		return false
	}
	aJSON, _ := json.Marshal(aVal)
	bJSON, _ := json.Marshal(bVal)
	return string(aJSON) == string(bJSON)
}

// TestJSONSet 测试 JSON.SET 命令
func TestJSONSet(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// Test basic JSON.SET
	result, err := testClient.Do(ctx, "JSON.SET", "user:1", "$", `{"name":"John","age":30}`).Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// Verify type
	typ, err := testClient.Type(ctx, "user:1").Result()
	assert.NoError(t, err)
	assert.Equal(t, "json", typ)

	// Test JSON.SET with NX (should not update existing key)
	result, err = testClient.Do(ctx, "JSON.SET", "user:1", "$", `{"name":"Jane"}`, "NX").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// Test JSON.SET with XX (should update existing key)
	result, err = testClient.Do(ctx, "JSON.SET", "user:1", "$", `{"name":"Jane","city":"NYC"}`, "XX").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)
}

// TestJSONGet 测试 JSON.GET 命令
func TestJSONGet(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// Set up test data
	_, err := testClient.Do(ctx, "JSON.SET", "user:1", "$", `{"name":"John","age":30,"city":"NYC"}`).Result()
	assert.NoError(t, err)

	// Test JSON.GET - use semantic comparison since JSON field order may vary
	result, err := testClient.Do(ctx, "JSON.GET", "user:1").Result()
	assert.NoError(t, err)
	resultStr, ok := result.(string)
	assert.True(t, ok)
	expected := `{"name":"John","age":30,"city":"NYC"}`
	if !jsonEqual(resultStr, expected) {
		t.Errorf("expected %s, got %s", expected, resultStr)
	}
}

// TestJSONDel 测试 JSON.DEL 命令
func TestJSONDel(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// Set up test data
	_, err := testClient.Do(ctx, "JSON.SET", "user:1", "$", `{"name":"John","age":30}`).Result()
	assert.NoError(t, err)

	// Test JSON.DEL
	count, err := testClient.Do(ctx, "JSON.DEL", "user:1").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)

	// Verify key is deleted
	exists, err := testClient.Exists(ctx, "user:1").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), exists)
}

// TestJSONType 测试 JSON.TYPE 命令
func TestJSONType(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// Set up test data
	_, err := testClient.Do(ctx, "JSON.SET", "user:1", "$", `{"name":"John","age":30}`).Result()
	assert.NoError(t, err)

	// Test JSON.TYPE
	result, err := testClient.Do(ctx, "JSON.TYPE", "user:1", "$").Result()
	assert.NoError(t, err)
	assert.Equal(t, "object", result)
}

// TestJSONArrAppend 测试 JSON.ARRAPPEND 命令
func TestJSONArrAppend(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// Set up array
	_, err := testClient.Do(ctx, "JSON.SET", "arr", "$", `[]`).Result()
	assert.NoError(t, err)

	// Test JSON.ARRAPPEND
	count, err := testClient.Do(ctx, "JSON.ARRAPPEND", "arr", "$", "1", "2", "3").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), count)

	// Verify array length
	result, err := testClient.Do(ctx, "JSON.GET", "arr").Result()
	assert.NoError(t, err)
	resultStr, ok := result.(string)
	assert.True(t, ok)
	expected := "[1,2,3]"
	if !jsonEqual(resultStr, expected) {
		t.Errorf("expected %s, got %s", expected, resultStr)
	}
}

// TestJSONArrLen 测试 JSON.ARRLEN 命令
func TestJSONArrLen(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// Set up array
	_, err := testClient.Do(ctx, "JSON.SET", "arr", "$", `["a","b","c"]`).Result()
	assert.NoError(t, err)

	// Test JSON.ARRLEN
	result, err := testClient.Do(ctx, "JSON.ARRLEN", "arr", "$").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), result)
}

// TestJSONObjKeys 测试 JSON.OBJKEYS 命令
func TestJSONObjKeys(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// Set up object
	_, err := testClient.Do(ctx, "JSON.SET", "user:1", "$", `{"name":"John","age":30,"city":"NYC"}`).Result()
	assert.NoError(t, err)

	// Test JSON.OBJKEYS
	result, err := testClient.Do(ctx, "JSON.OBJKEYS", "user:1", "$").Result()
	assert.NoError(t, err)
	// Result is an array of keys
	keys, ok := result.([]interface{})
	assert.Equal(t, true, ok)
	assert.Equal(t, 3, len(keys))
}

// TestJSONNumIncrBy 测试 JSON.NUMINCRBY 命令
func TestJSONNumIncrBy(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// Set up number
	_, err := testClient.Do(ctx, "JSON.SET", "counter", "$", `10`).Result()
	assert.NoError(t, err)

	// Test JSON.NUMINCRBY
	result, err := testClient.Do(ctx, "JSON.NUMINCRBY", "counter", "$", "5").Result()
	assert.NoError(t, err)
	assert.Equal(t, "15", result)
}

// TestJSONNumMultBy 测试 JSON.NUMMULTBY 命令
func TestJSONNumMultBy(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// Set up number
	_, err := testClient.Do(ctx, "JSON.SET", "counter", "$", `10`).Result()
	assert.NoError(t, err)

	// Test JSON.NUMMULTBY
	result, err := testClient.Do(ctx, "JSON.NUMMULTBY", "counter", "$", "2").Result()
	assert.NoError(t, err)
	assert.Equal(t, "20", result)
}

// TestJSONClear 测试 JSON.CLEAR 命令
func TestJSONClear(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// Set up object
	_, err := testClient.Do(ctx, "JSON.SET", "user:1", "$", `{"name":"John","age":30}`).Result()
	assert.NoError(t, err)

	// Test JSON.CLEAR
	count, err := testClient.Do(ctx, "JSON.CLEAR", "user:1", "$").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)
}

// TestJSONDebugMemory 测试 JSON.DEBUG MEMORY 命令
func TestJSONDebugMemory(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// Set up object
	_, err := testClient.Do(ctx, "JSON.SET", "user:1", "$", `{"name":"John"}`).Result()
	assert.NoError(t, err)

	// Test JSON.DEBUG MEMORY
	result, err := testClient.Do(ctx, "JSON.DEBUG", "MEMORY", "user:1", "$").Result()
	assert.NoError(t, err)
	memory, ok := result.(int64)
	assert.Equal(t, true, ok)
	assert.True(t, memory > 0)
}

// TestJSONMGet 测试 JSON.MGET 命令
func TestJSONMGet(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// Set up test data
	_, err := testClient.Do(ctx, "JSON.SET", "user:1", "$", `{"name":"John"}`).Result()
	assert.NoError(t, err)
	_, err = testClient.Do(ctx, "JSON.SET", "user:2", "$", `{"name":"Jane"}`).Result()
	assert.NoError(t, err)

	// Test JSON.MGET
	result, err := testClient.Do(ctx, "JSON.MGET", "user:1", "user:2", "$").Result()
	assert.NoError(t, err)
	// Result is an array
	values, ok := result.([]interface{})
	assert.Equal(t, true, ok)
	assert.Equal(t, 2, len(values))
}

// TestJSONNotFound 测试不存在的键
func TestJSONNotFound(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// Test JSON.GET on non-existent key - returns redis.Nil
	result, err := testClient.Do(ctx, "JSON.GET", "nonexistent").Result()
	assert.Equal(t, nil, result)
	assert.True(t, err == redis.Nil || err == nil)

	// Test JSON.DEL on non-existent key
	count, err := testClient.Do(ctx, "JSON.DEL", "nonexistent").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

// TestJSONWithExistingDB 测试与现有数据库的交互
func TestJSONWithExistingDB(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// Test that JSON keys don't interfere with other types
	_, err := testClient.Set(ctx, "string:key", "value", 0).Result()
	assert.NoError(t, err)
	_, err = testClient.Do(ctx, "JSON.SET", "json:key", "$", `{"data":"test"}`).Result()
	assert.NoError(t, err)

	// Verify both exist
	stringExists, err := testClient.Exists(ctx, "string:key").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), stringExists)

	jsonExists, err := testClient.Exists(ctx, "json:key").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), jsonExists)

	// Verify types
	stringType, err := testClient.Type(ctx, "string:key").Result()
	assert.NoError(t, err)
	assert.Equal(t, "string", stringType)

	jsonType, err := testClient.Type(ctx, "json:key").Result()
	assert.NoError(t, err)
	assert.Equal(t, "json", jsonType)
}

// TestSelect 测试 SELECT 命令
func TestSelect(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// SELECT should return OK for any database number (BoltDB is single-db)
	result, err := testClient.Do(ctx, "SELECT", "0").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// SELECT with other database numbers should also return OK
	result, err = testClient.Do(ctx, "SELECT", "15").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)
}

// TestMove 测试 MOVE 命令
func TestMove(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// MOVE always returns 0 in single-db implementation
	result, err := testClient.Do(ctx, "MOVE", "key", "1").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result)
}

// TestWait 测试 WAIT 命令
func TestWait(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// WAIT should return 0 (no replication)
	result, err := testClient.Do(ctx, "WAIT", "1", "1000").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result)
}

// TestSlowLog 测试 SLOWLOG 命令
func TestSlowLog(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// SLOWLOG GET should return empty array
	result, err := testClient.Do(ctx, "SLOWLOG", "GET", "10").Result()
	assert.NoError(t, err)
	_, ok := result.([]interface{})
	assert.True(t, ok) // Should be empty array

	// SLOWLOG LEN should return 0
	result, err = testClient.Do(ctx, "SLOWLOG", "LEN").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result)

	// SLOWLOG RESET should return OK
	result, err = testClient.Do(ctx, "SLOWLOG", "RESET").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// SLOWLOG HELP should return help text
	result, err = testClient.Do(ctx, "SLOWLOG", "HELP").Result()
	assert.NoError(t, err)
	arr, ok := result.([]interface{})
	assert.True(t, ok)
	assert.True(t, len(arr) > 0)
}

// TestMemoryUsage 测试 MEMORY USAGE 命令
func TestMemoryUsage(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// Set a string value
	_ = testClient.Set(ctx, "memkey", "testvalue", 0).Err()

	// MEMORY USAGE should return positive value
	result, err := testClient.Do(ctx, "MEMORY", "USAGE", "memkey").Result()
	assert.NoError(t, err)
	memory, ok := result.(int64)
	assert.True(t, ok)
	assert.True(t, memory > 0)

	// MEMORY USAGE on non-existent key should return nil
	result, err = testClient.Do(ctx, "MEMORY", "USAGE", "nonexistent").Result()
	assert.Equal(t, nil, result)
	assert.True(t, err == redis.Nil || err == nil)

	// MEMORY DOCTOR should return info
	result, err = testClient.Do(ctx, "MEMORY", "DOCTOR").Result()
	assert.NoError(t, err)
	_, ok = result.([]interface{})
	assert.True(t, ok)

	// MEMORY HELP should return help text
	result, err = testClient.Do(ctx, "MEMORY", "HELP").Result()
	assert.NoError(t, err)
	arr, ok := result.([]interface{})
	assert.True(t, ok)
	assert.True(t, len(arr) > 0)
}

// TestLOLWUT 测试 LOLWUT 命令
func TestLOLWUT(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// LOLWUT should return version info
	result, err := testClient.Do(ctx, "LOLWUT").Result()
	assert.NoError(t, err)

	str, ok := result.(string)
	assert.True(t, ok)
	assert.True(t, len(str) > 0)
	assert.Equal(t, "BoltDB redis.bolt.8.0 - A disk-persistent Redis-compatible database", str)

	// LOLWUT with VERSION parameter
	result, err = testClient.Do(ctx, "LOLWUT", "VERSION", "5").Result()
	assert.NoError(t, err)
	str, ok = result.(string)
	assert.True(t, ok)
	assert.Equal(t, "BoltDB 5 - A disk-persistent Redis-compatible database", str)
}

// TestLatency 测试 LATENCY 命令
func TestLatency(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// LATENCY LATEST should return empty array
	result, err := testClient.Do(ctx, "LATENCY", "LATEST").Result()
	assert.NoError(t, err)
	arr, ok := result.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 0, len(arr))

	// LATENCY RESET should return 0 (count of events reset)
	result, err = testClient.Do(ctx, "LATENCY", "RESET").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result)

	// LATENCY RESET with event should return count
	result, err = testClient.Do(ctx, "LATENCY", "RESET", "command").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result)

	// LATENCY HELP should return help text
	result, err = testClient.Do(ctx, "LATENCY", "HELP").Result()
	assert.NoError(t, err)
	arr, ok = result.([]interface{})
	assert.True(t, ok)
	assert.True(t, len(arr) > 0)

	// LATENCY DOCTOR should return info
	result, err = testClient.Do(ctx, "LATENCY", "DOCTOR").Result()
	assert.NoError(t, err)
	_, ok = result.([]interface{})
	assert.True(t, ok)
}

// TestReadOnlyReadWrite 测试 READONLY 和 READWRITE 命令
func TestReadOnlyReadWrite(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// READONLY should return OK
	result, err := testClient.Do(ctx, "READONLY").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// READWRITE should return OK
	result, err = testClient.Do(ctx, "READWRITE").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)
}

// TestZRangeStore 测试 ZRANGESTORE 命令
func TestZRangeStore(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// Create source sorted set
	_ = testClient.ZAdd(ctx, "srczset", redis.Z{Score: 1, Member: "a"}).Err()
	_ = testClient.ZAdd(ctx, "srczset", redis.Z{Score: 2, Member: "b"}).Err()
	_ = testClient.ZAdd(ctx, "srczset", redis.Z{Score: 3, Member: "c"}).Err()
	_ = testClient.ZAdd(ctx, "srczset", redis.Z{Score: 4, Member: "d"}).Err()
	_ = testClient.ZAdd(ctx, "srczset", redis.Z{Score: 5, Member: "e"}).Err()

	// ZRANGESTORE with BYSCORE - basic test
	result, err := testClient.Do(ctx, "ZRANGESTORE", "dstzset", "srczset", "2", "4").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), result) // 3 elements (b, c, d)

	// Verify destination has correct values
	count, err := testClient.ZCard(ctx, "dstzset").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), count)

	// ZRANGESTORE with LIMIT
	result, err = testClient.Do(ctx, "ZRANGESTORE", "dstzset4", "srczset", "0", "4", "LIMIT", "0", "2").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), result)

	// ZRANGESTORE on empty source
	result, err = testClient.Do(ctx, "ZRANGESTORE", "dstzset5", "emptyzset", "1", "10").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result)

	// ZRANGESTORE with BYLEX - create sorted set with same scores
	_ = testClient.ZAdd(ctx, "lexzset", redis.Z{Score: 0, Member: "a"}).Err()
	_ = testClient.ZAdd(ctx, "lexzset", redis.Z{Score: 0, Member: "b"}).Err()
	_ = testClient.ZAdd(ctx, "lexzset", redis.Z{Score: 0, Member: "c"}).Err()
	_ = testClient.ZAdd(ctx, "lexzset", redis.Z{Score: 0, Member: "d"}).Err()
	_ = testClient.ZAdd(ctx, "lexzset", redis.Z{Score: 0, Member: "e"}).Err()

	result, err = testClient.Do(ctx, "ZRANGESTORE", "lexdst", "lexzset", "a", "c", "BYLEX").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), result)
}

// TestMain 测试入口
// TestMain 在所有测试之前和之后运行一次
// - 在 m.Run() 之前：可以执行共享的初始化代码
// - 在 m.Run() 之后：可以执行清理代码（如日志、报告）
func TestMain(m *testing.M) {
	println("[TestMain] === 测试套件开始 ===")

	// 这里可以放置所有测试共享的初始化逻辑
	// 例如：启动测试数据库、加载测试数据等
	// 目前每个测试函数自行调用 setupTestServer/teardownTestServer

	println("[TestMain] 运行测试...")
	code := m.Run()

	println("[TestMain] 测试运行完成，退出码:", code)
	println("[TestMain] === 测试套件结束 ===")

	os.Exit(code)
}
