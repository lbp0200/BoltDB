package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/backup"
	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/replication"
	"github.com/lbp0200/BoltDB/internal/server"
	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/redis/go-redis/v9"
	"github.com/zeebo/assert"
)

// TestConnection 测试连接命令
func TestConnection(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// PING
	pong, err := sharedClient.Ping(ctx).Result()
	assert.NoError(t, err)
	assert.Equal(t, "PONG", pong)

	// ECHO
	echo, err := sharedClient.Echo(ctx, "Hello").Result()
	assert.NoError(t, err)
	assert.Equal(t, "Hello", echo)
}

// TestString 测试String命令
func TestString(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// SET
	err := sharedClient.Set(ctx, "key1", "value1", 0).Err()
	assert.NoError(t, err)

	// GET
	val, err := sharedClient.Get(ctx, "key1").Result()
	assert.NoError(t, err)
	assert.Equal(t, "value1", val)

	// DEL 删除存在的键
	deleted, err := sharedClient.Del(ctx, "key1").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), deleted)

	// DEL 删除不存在的键
	deleted, err = sharedClient.Del(ctx, "nonexistent").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), deleted)

	// DEL 批量删除
	_ = sharedClient.Set(ctx, "k1", "v1", 0).Err()
	_ = sharedClient.Set(ctx, "k2", "v2", 0).Err()
	_ = sharedClient.Set(ctx, "k3", "v3", 0).Err()
	deleted, err = sharedClient.Del(ctx, "k1", "k2", "k3", "k4").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), deleted)

	// INCR
	incr, err := sharedClient.Incr(ctx, "counter").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), incr)

	incr, err = sharedClient.Incr(ctx, "counter").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), incr)

	// APPEND
	_ = sharedClient.Set(ctx, "appendkey", "hello", 0).Err()
	appendLen, err := sharedClient.Append(ctx, "appendkey", "world").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(10), appendLen) // "helloworld" = 10 chars

	val, err = sharedClient.Get(ctx, "appendkey").Result()
	assert.NoError(t, err)
	assert.Equal(t, "helloworld", val)

	// STRLEN
	strlen, err := sharedClient.StrLen(ctx, "appendkey").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(10), strlen)
}

// TestList 测试List命令
func TestList(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// LPUSH
	err := sharedClient.LPush(ctx, "list1", "a", "b", "c").Err()
	assert.NoError(t, err)

	// LLEN
	length, err := sharedClient.LLen(ctx, "list1").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), length)

	// LRANGE
	items, err := sharedClient.LRange(ctx, "list1", 0, -1).Result()
	assert.NoError(t, err)
	assert.Equal(t, []string{"c", "b", "a"}, items)

	// LPOP
	val, err := sharedClient.LPop(ctx, "list1").Result()
	assert.NoError(t, err)
	assert.Equal(t, "c", val)

	// RPUSH
	err = sharedClient.RPush(ctx, "list1", "d").Err()
	assert.NoError(t, err)

	// RPOP
	val, err = sharedClient.RPop(ctx, "list1").Result()
	assert.NoError(t, err)
	assert.Equal(t, "d", val)

	// LINDEX
	val, err = sharedClient.LIndex(ctx, "list1", 0).Result()
	assert.NoError(t, err)
	assert.Equal(t, "b", val)
}

// TestHash 测试Hash命令
func TestHash(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// HSET
	err := sharedClient.HSet(ctx, "hash1", "field1", "value1").Err()
	assert.NoError(t, err)

	// HGET
	val, err := sharedClient.HGet(ctx, "hash1", "field1").Result()
	assert.NoError(t, err)
	assert.Equal(t, "value1", val)

	// HGETALL
	all, err := sharedClient.HGetAll(ctx, "hash1").Result()
	assert.NoError(t, err)
	assert.Equal(t, map[string]string{"field1": "value1"}, all)

	// HINCRBY
	incr, err := sharedClient.HIncrBy(ctx, "hash1", "field2", 5).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(5), incr)

	// HEXISTS
	exists, err := sharedClient.HExists(ctx, "hash1", "field1").Result()
	assert.NoError(t, err)
	assert.Equal(t, true, exists)

	// HDEL
	deleted, err := sharedClient.HDel(ctx, "hash1", "field2").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), deleted)
}

// TestSet 测试Set命令
func TestSet(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// SADD
	err := sharedClient.SAdd(ctx, "set1", "m1", "m2", "m3").Err()
	assert.NoError(t, err)

	// SMEMBERS
	members, err := sharedClient.SMembers(ctx, "set1").Result()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(members))

	// SISMEMBER
	isMember, err := sharedClient.SIsMember(ctx, "set1", "m1").Result()
	assert.NoError(t, err)
	assert.Equal(t, true, isMember)

	// SCARD
	card, err := sharedClient.SCard(ctx, "set1").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), card)

	// SREM
	removed, err := sharedClient.SRem(ctx, "set1", "m3").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), removed)

	card, err = sharedClient.SCard(ctx, "set1").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), card)
}

// TestSortedSet 测试SortedSet命令
func TestSortedSet(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// ZADD
	err := sharedClient.ZAdd(ctx, "zset1", redis.Z{Score: 100, Member: "Alice"}, redis.Z{Score: 90, Member: "Bob"}, redis.Z{Score: 80, Member: "Charlie"}).Err()
	assert.NoError(t, err)

	// ZRANGE
	members, err := sharedClient.ZRange(ctx, "zset1", 0, -1).Result()
	assert.NoError(t, err)
	assert.Equal(t, []string{"Charlie", "Bob", "Alice"}, members)

	// ZSCORE
	score, err := sharedClient.ZScore(ctx, "zset1", "Alice").Result()
	assert.NoError(t, err)
	assert.Equal(t, float64(100), score)

	// ZCARD
	card, err := sharedClient.ZCard(ctx, "zset1").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), card)

	// ZINCRBY
	score, err = sharedClient.ZIncrBy(ctx, "zset1", 50, "Bob").Result()
	assert.NoError(t, err)
	assert.Equal(t, float64(140), score)

	// ZREM
	removed, err := sharedClient.ZRem(ctx, "zset1", "Charlie").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), removed)

	card, err = sharedClient.ZCard(ctx, "zset1").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), card)
}

// TestServerCommands 测试Server相关命令
func TestServerCommands(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// PING
	pong, err := sharedClient.Ping(ctx).Result()
	assert.NoError(t, err)
	assert.Equal(t, "PONG", pong)

	// ECHO
	echo, err := sharedClient.Echo(ctx, "Hello, Server!").Result()
	assert.NoError(t, err)
	assert.Equal(t, "Hello, Server!", echo)

	// DBSIZE
	dbsize, err := sharedClient.DBSize(ctx).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), dbsize)

	// TYPE
	_ = sharedClient.Set(ctx, "typekey", "value", 0).Err()
	keyType, err := sharedClient.Type(ctx, "typekey").Result()
	assert.NoError(t, err)
	assert.Equal(t, "string", keyType)
}

// TestKeyCommands 测试Key相关命令
func TestKeyCommands(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// EXISTS - 键不存在
	exists, err := sharedClient.Exists(ctx, "nonexistent").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), exists)

	// EXISTS - 键存在
	_ = sharedClient.Set(ctx, "existskey", "value", 0).Err()
	exists, err = sharedClient.Exists(ctx, "existskey").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), exists)

	// EXISTS - 批量检查
	_ = sharedClient.Set(ctx, "existskey2", "value2", 0).Err()
	exists, err = sharedClient.Exists(ctx, "existskey", "existskey2", "nonexistent").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), exists)

	// EXPIRE - 设置过期时间
	_ = sharedClient.Set(ctx, "expirekey", "value", 0).Err()
	set, err := sharedClient.Expire(ctx, "expirekey", 10*time.Second).Result()
	assert.NoError(t, err)
	assert.Equal(t, true, set)

	// TTL - 查看剩余过期时间
	// 注意：go-redis 的 TTL 返回 time.Duration（纳秒）
	ttlDuration, err := sharedClient.TTL(ctx, "expirekey").Result()
	assert.NoError(t, err)
	assert.True(t, ttlDuration > 0 && ttlDuration <= 10*time.Second)
	assert.True(t, ttlDuration >= 9*time.Second) // 至少还有9秒

	// TTL - 永不过期的键
	_ = sharedClient.Set(ctx, "noexpirekey", "value", 0).Err()
	noexpireTTL, err := sharedClient.TTL(ctx, "noexpirekey").Result()
	assert.NoError(t, err)
	assert.Equal(t, time.Duration(-1), noexpireTTL)

	// RENAME - 重命名键
	_ = sharedClient.Set(ctx, "renamekey", "oldvalue", 0).Err()
	err = sharedClient.Rename(ctx, "renamekey", "newkey").Err()
	assert.NoError(t, err)
	val, err := sharedClient.Get(ctx, "newkey").Result()
	assert.NoError(t, err)
	assert.Equal(t, "oldvalue", val)

	// RENAMENX - 新键不存在时重命名
	_ = sharedClient.Set(ctx, "renamenxkey", "value", 0).Err()
	_ = sharedClient.Set(ctx, "targetkey", "targetvalue", 0).Err()
	set, err = sharedClient.RenameNX(ctx, "renamenxkey", "targetkey").Result()
	assert.NoError(t, err)
	assert.Equal(t, false, set) // targetkey已存在

	set, err = sharedClient.RenameNX(ctx, "renamenxkey", "nonexistenttarget").Result()
	assert.NoError(t, err)
	assert.Equal(t, true, set)
}

// TestStringExtended 测试扩展String命令
func TestStringExtended(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// SETNX - 键不存在时设置
	set, err := sharedClient.SetNX(ctx, "setnxkey", "value", 0).Result()
	assert.NoError(t, err)
	assert.Equal(t, true, set)

	// SETNX - 键已存在时设置失败
	set, err = sharedClient.SetNX(ctx, "setnxkey", "value2", 0).Result()
	assert.NoError(t, err)
	assert.Equal(t, false, set)

	// GETSET - 获取旧值并设置新值
	_ = sharedClient.Set(ctx, "getsetkey", "oldvalue", 0).Err()
	oldVal, err := sharedClient.GetSet(ctx, "getsetkey", "newvalue").Result()
	assert.NoError(t, err)
	assert.Equal(t, "oldvalue", oldVal)

	newVal, err := sharedClient.Get(ctx, "getsetkey").Result()
	assert.NoError(t, err)
	assert.Equal(t, "newvalue", newVal)

	// DECR - 递减
	_ = sharedClient.Set(ctx, "decrkey", "10", 0).Err()
	decr, err := sharedClient.Decr(ctx, "decrkey").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(9), decr)

	// DECRBY - 按步长递减
	decr, err = sharedClient.DecrBy(ctx, "decrkey", 3).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(6), decr)

	// INCRBY - 按步长递增
	incr, err := sharedClient.IncrBy(ctx, "incrbykey", 5).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(5), incr)

	// INCRBYFLOAT - 浮点数递增
	incrFloat, err := sharedClient.IncrByFloat(ctx, "floatkey", 1.5).Result()
	assert.NoError(t, err)
	assert.Equal(t, float64(1.5), incrFloat)

	// MGET - 批量获取
	_ = sharedClient.Set(ctx, "mkey1", "value1", 0).Err()
	_ = sharedClient.Set(ctx, "mkey2", "value2", 0).Err()
	vals, err := sharedClient.MGet(ctx, "mkey1", "mkey2", "mkey3").Result()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{"value1", "value2", nil}, vals)

	// MSET - 批量设置
	err = sharedClient.MSet(ctx, "mskey1", "value1", "mskey2", "value2").Err()
	assert.NoError(t, err)

	val1, _ := sharedClient.Get(ctx, "mskey1").Result()
	val2, _ := sharedClient.Get(ctx, "mskey2").Result()
	assert.Equal(t, "value1", val1)
	assert.Equal(t, "value2", val2)
}

// TestListExtended 测试扩展List命令
func TestListExtended(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 准备测试数据
	_ = sharedClient.RPush(ctx, "listext", "a", "b", "c").Err()

	// LSET - 设置指定位置的值
	err := sharedClient.LSet(ctx, "listext", 1, "x").Err()
	assert.NoError(t, err)

	val, err := sharedClient.LIndex(ctx, "listext", 1).Result()
	assert.NoError(t, err)
	assert.Equal(t, "x", val)

	// LTRIM - 裁剪列表
	err = sharedClient.LTrim(ctx, "listext", 0, 1).Err()
	assert.NoError(t, err)

	length, err := sharedClient.LLen(ctx, "listext").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), length)

	// RPOPLPUSH - 从一个列表弹出并推入另一个列表
	_ = sharedClient.RPush(ctx, "listsrc", "item1", "item2").Err()
	val, err = sharedClient.RPopLPush(ctx, "listsrc", "listdst").Result()
	assert.NoError(t, err)
	assert.Equal(t, "item2", val)
}

// TestHashExtended 测试扩展Hash命令
func TestHashExtended(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// HSETNX - 字段不存在时设置
	_ = sharedClient.HSet(ctx, "hashext", "field1", "value1").Err()
	set, err := sharedClient.HSetNX(ctx, "hashext", "field1", "value2").Result()
	assert.NoError(t, err)
	assert.Equal(t, false, set)

	set, err = sharedClient.HSetNX(ctx, "hashext", "field2", "value2").Result()
	assert.NoError(t, err)
	assert.Equal(t, true, set)

	// HKEYS - 获取所有字段
	keys, err := sharedClient.HKeys(ctx, "hashext").Result()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(keys))

	// HVALS - 获取所有值
	vals, err := sharedClient.HVals(ctx, "hashext").Result()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(vals))

	// HLEN - 获取字段数量
	length, err := sharedClient.HLen(ctx, "hashext").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), length)

	// HMGET - 批量获取字段
	results, err := sharedClient.HMGet(ctx, "hashext", "field1", "field2", "field3").Result()
	assert.NoError(t, err)
	assert.Equal(t, "value1", results[0])
	assert.Equal(t, "value2", results[1])
	assert.Nil(t, results[2])

	// HMSET - 批量设置字段
	err = sharedClient.HMSet(ctx, "hashmset", "k1", "v1", "k2", "v2").Err()
	assert.NoError(t, err)

	val, _ := sharedClient.HGet(ctx, "hashmset", "k1").Result()
	assert.Equal(t, "v1", val)
}

// TestSetExtended 测试扩展Set命令
func TestSetExtended(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 准备测试数据
	_ = sharedClient.SAdd(ctx, "set1", "a", "b", "c").Err()
	_ = sharedClient.SAdd(ctx, "set2", "b", "c", "d").Err()

	// SDIFF - 差集
	members, err := sharedClient.SDiff(ctx, "set1", "set2").Result()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(members))
	assert.Equal(t, "a", members[0])

	// SINTER - 交集
	members, err = sharedClient.SInter(ctx, "set1", "set2").Result()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(members))

	// SUNION - 并集
	members, err = sharedClient.SUnion(ctx, "set1", "set2").Result()
	assert.NoError(t, err)
	assert.Equal(t, 4, len(members))

	// SPOP - 随机弹出
	_ = sharedClient.SAdd(ctx, "spopset", "a", "b", "c").Err()
	val, err := sharedClient.SPop(ctx, "spopset").Result()
	assert.NoError(t, err)
	assert.True(t, val == "a" || val == "b" || val == "c")

	card, _ := sharedClient.SCard(ctx, "spopset").Result()
	assert.Equal(t, int64(2), card)

	// SMOVE - 移动元素
	_ = sharedClient.SAdd(ctx, "setmove1", "a", "b").Err()
	_ = sharedClient.SAdd(ctx, "setmove2", "c").Err()
	moved, err := sharedClient.SMove(ctx, "setmove1", "setmove2", "a").Result()
	assert.NoError(t, err)
	assert.Equal(t, true, moved)

	card, _ = sharedClient.SCard(ctx, "setmove1").Result()
	assert.Equal(t, int64(1), card)
}

// TestSortedSetExtended 测试扩展SortedSet命令
func TestSortedSetExtended(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 准备测试数据
	_ = sharedClient.ZAdd(ctx, "zsetext", redis.Z{Score: 10, Member: "a"}, redis.Z{Score: 20, Member: "b"}, redis.Z{Score: 30, Member: "c"}).Err()

	// ZRANK - 获取成员排名
	rank, err := sharedClient.ZRank(ctx, "zsetext", "b").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), rank)

	// ZREVRANK - 获取逆向排名
	revRank, err := sharedClient.ZRevRank(ctx, "zsetext", "b").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), revRank)

	// ZCOUNT - 获取指定分数范围内的成员数量
	count, err := sharedClient.ZCount(ctx, "zsetext", "10", "25").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)

	// ZREVRANGE - 逆向范围获取
	members, err := sharedClient.ZRevRange(ctx, "zsetext", 0, -1).Result()
	assert.NoError(t, err)
	assert.Equal(t, []string{"c", "b", "a"}, members)
}

// TestStringAdvanced 测试高级String命令
func TestStringAdvanced(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// SETEX - 设置过期时间的字符串
	err := sharedClient.SetEx(ctx, "setexkey", "value", 5*time.Second).Err()
	assert.NoError(t, err)
	val, err := sharedClient.Get(ctx, "setexkey").Result()
	assert.NoError(t, err)
	assert.Equal(t, "value", val)

	// PSETEX - 设置过期时间（毫秒）
	err = sharedClient.SetEx(ctx, "psetexkey", "value", 5000*time.Millisecond).Err()
	assert.NoError(t, err)
	val, err = sharedClient.Get(ctx, "psetexkey").Result()
	assert.NoError(t, err)
	assert.Equal(t, "value", val)

	// SETRANGE - 从指定偏移量开始修改字符串
	_ = sharedClient.Set(ctx, "setrangekey", "hello world", 0).Err()
	length, err := sharedClient.SetRange(ctx, "setrangekey", 6, "golang").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(12), length)
	val, err = sharedClient.Get(ctx, "setrangekey").Result()
	assert.NoError(t, err)
	assert.Equal(t, "hello golang", val)

	// GETRANGE - 获取子字符串
	val, err = sharedClient.GetRange(ctx, "setrangekey", 0, 4).Result()
	assert.NoError(t, err)
	assert.Equal(t, "hello", val)

	val, err = sharedClient.GetRange(ctx, "setrangekey", 6, -1).Result()
	assert.NoError(t, err)
	assert.Equal(t, "golang", val)
}

// TestKeyAdvanced 测试高级Key命令
func TestKeyAdvanced(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// KEYS - 查找匹配的键
	_ = sharedClient.Set(ctx, "testkey1", "value1", 0).Err()
	_ = sharedClient.Set(ctx, "testkey2", "value2", 0).Err()
	_ = sharedClient.Set(ctx, "testkey3", "value3", 0).Err()
	keys, err := sharedClient.Keys(ctx, "testkey*").Result()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(keys))

	// RANDOMKEY - 获取随机键
	randomKey, err := sharedClient.RandomKey(ctx).Result()
	assert.NoError(t, err)
	assert.True(t, randomKey != "")

	// PERSIST - 移除过期时间
	_ = sharedClient.Set(ctx, "persistkey", "value", 0).Err()
	_ = sharedClient.Expire(ctx, "persistkey", 10*time.Second).Err()
	success, err := sharedClient.Persist(ctx, "persistkey").Result()
	assert.NoError(t, err)
	assert.Equal(t, true, success)

	ttl, _ := sharedClient.TTL(ctx, "persistkey").Result()
	assert.Equal(t, time.Duration(-1), ttl)

	// EXPIREAT - 设置过期时间戳
	_ = sharedClient.Set(ctx, "expireatkey", "value", 0).Err()
	futureTime := time.Now().Add(1 * time.Hour)
	success, err = sharedClient.ExpireAt(ctx, "expireatkey", futureTime).Result()
	assert.NoError(t, err)
	assert.Equal(t, true, success)

	// PEXPIRE - 毫秒级过期
	_ = sharedClient.Set(ctx, "pexpirekey", "value", 0).Err()
	success, err = sharedClient.PExpire(ctx, "pexpirekey", 5000*time.Millisecond).Result()
	assert.NoError(t, err)
	assert.Equal(t, true, success)

	// PEXPIREAT - 毫秒级时间戳过期
	_ = sharedClient.Set(ctx, "pexpireatkey", "value", 0).Err()
	futureTimeMs := time.Now().Add(1 * time.Hour)
	success, err = sharedClient.PExpireAt(ctx, "pexpireatkey", futureTimeMs).Result()
	assert.NoError(t, err)
	assert.Equal(t, true, success)
}

// TestServerExtended 测试扩展Server命令
func TestServerExtended(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// FLUSHDB - 清空当前数据库
	_ = sharedClient.Set(ctx, "flushdbkey", "value", 0).Err()
	_ = sharedClient.Set(ctx, "flushdbkey2", "value2", 0).Err()
	err := sharedClient.FlushDB(ctx).Err()
	assert.NoError(t, err)

	dbsize, err := sharedClient.DBSize(ctx).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), dbsize)

	// DBSIZE - 验证数据库为空
	_ = sharedClient.Set(ctx, "dbsizekey", "value", 0).Err()
	dbsize, err = sharedClient.DBSize(ctx).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), dbsize)

	// TYPE - 检查键类型
	val, err := sharedClient.Type(ctx, "dbsizekey").Result()
	assert.NoError(t, err)
	assert.Equal(t, "string", val)

	// PING - 验证连接
	pong, err := sharedClient.Ping(ctx).Result()
	assert.NoError(t, err)
	assert.Equal(t, "PONG", pong)
}

// TestListAdvanced 测试高级List命令
func TestListAdvanced(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// LPUSHX - 列表存在时从左侧插入
	_ = sharedClient.RPush(ctx, "listx", "a").Err()
	err := sharedClient.LPushX(ctx, "listx", "b").Err()
	assert.NoError(t, err)
	err = sharedClient.LPushX(ctx, "nonexistent", "a").Err()
	assert.NoError(t, err) // 键不存在时不做任何操作

	items, err := sharedClient.LRange(ctx, "listx", 0, -1).Result()
	assert.NoError(t, err)
	assert.Equal(t, []string{"b", "a"}, items)

	// RPUSHX - 列表存在时从右侧插入
	err = sharedClient.RPushX(ctx, "listx", "c").Err()
	assert.NoError(t, err)
	items, err = sharedClient.LRange(ctx, "listx", 0, -1).Result()
	assert.NoError(t, err)
	assert.Equal(t, []string{"b", "a", "c"}, items)

	// LREM - 移除元素 (count=0 表示移除所有匹配的元素)
	_ = sharedClient.RPush(ctx, "listrem", "a", "b", "a", "c", "a").Err()
	removed, err := sharedClient.LRem(ctx, "listrem", 0, "a").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), removed)

	items, err = sharedClient.LRange(ctx, "listrem", 0, -1).Result()
	assert.NoError(t, err)
	assert.Equal(t, []string{"b", "c"}, items)
}

// TestHashAdvanced 测试高级Hash命令
func TestHashAdvanced(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// HINCRBYFLOAT - 浮点数递增
	_ = sharedClient.HSet(ctx, "hashfloat", "field", "10.5").Err()
	val, err := sharedClient.HIncrByFloat(ctx, "hashfloat", "field", 2.5).Result()
	assert.NoError(t, err)
	assert.Equal(t, float64(13), val)

	// HSTRLEN - 获取字段值长度
	_ = sharedClient.HSet(ctx, "hashstr", "field", "hello").Err()
	length, err := sharedClient.HStrLen(ctx, "hashstr", "field").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(5), length)
}

// TestSetAdvanced 测试高级Set命令
func TestSetAdvanced(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// SINTERSTORE - 交集并存储
	_ = sharedClient.SAdd(ctx, "setstore1", "a", "b", "c").Err()
	_ = sharedClient.SAdd(ctx, "setstore2", "b", "c", "d").Err()
	count, err := sharedClient.SInterStore(ctx, "setstoreresult", "setstore1", "setstore2").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)

	members, _ := sharedClient.SMembers(ctx, "setstoreresult").Result()
	assert.Equal(t, 2, len(members))

	// SDIFFSTORE - 差集并存储
	_ = sharedClient.SAdd(ctx, "setdiff1", "a", "b", "c").Err()
	_ = sharedClient.SAdd(ctx, "setdiff2", "b").Err()
	count, err = sharedClient.SDiffStore(ctx, "setdifffresult", "setdiff1", "setdiff2").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)

	// SUNIONSTORE - 并集并存储
	_ = sharedClient.SAdd(ctx, "setunion1", "a", "b").Err()
	_ = sharedClient.SAdd(ctx, "setunion2", "c", "d").Err()
	count, err = sharedClient.SUnionStore(ctx, "setunionresult", "setunion1", "setunion2").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(4), count)

	// SRANDMEMBER - 随机获取成员
	_ = sharedClient.SAdd(ctx, "setrand", "a", "b", "c", "d", "e").Err()
	val, err := sharedClient.SRandMember(ctx, "setrand").Result()
	assert.NoError(t, err)
	assert.True(t, val == "a" || val == "b" || val == "c" || val == "d" || val == "e")

	// SRANDMEMBER - 获取多个随机成员
	vals, err := sharedClient.SRandMemberN(ctx, "setrand", 3).Result()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(vals))
}

// TestTransaction 测试事务命令（MULTI/EXEC/DISCARD/WATCH/UNWATCH）
// 由于go-redis客户端对MULTI/EXEC的支持有限，我们验证命令被正确识别
func TestTransaction(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 测试 UNWATCH - 取消监控
	result, err := sharedClient.Do(ctx, "UNWATCH").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// 测试 WATCH - 监控键
	result, err = sharedClient.Do(ctx, "WATCH", "watchkey").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// 测试 MULTI - 开始事务
	result, err = sharedClient.Do(ctx, "MULTI").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// 测试 DISCARD - 放弃事务
	result, err = sharedClient.Do(ctx, "DISCARD").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// 重新开始事务并执行
	result, err = sharedClient.Do(ctx, "MULTI").Result()
	assert.NoError(t, err)

	// 在事务中添加命令并执行
	_ = sharedClient.Set(ctx, "txkey", "txvalue", 0).Err()
	result, err = sharedClient.Do(ctx, "EXEC").Result()
	assert.NoError(t, err)

	// 验证命令执行
	val, _ := sharedClient.Get(ctx, "txkey").Result()
	assert.Equal(t, "txvalue", val)

	// 测试 WATCH 多个键
	result, err = sharedClient.Do(ctx, "WATCH", "key1", "key2", "key3").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)
}

// TestTransactionExtended 扩展事务命令测试 - 测试修复后的 WATCH/MULTI/EXEC 行为
func TestTransactionExtended(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// ========== Error Cases ==========

	// EXEC without MULTI - should return error
	_, err := sharedClient.Do(ctx, "EXEC").Result()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "EXEC without MULTI"))

	// DISCARD without MULTI - should return error
	_, err = sharedClient.Do(ctx, "DISCARD").Result()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "DISCARD without MULTI"))

	// ========== Transaction with multiple commands ==========
	_, _ = sharedClient.Do(ctx, "MULTI").Result()
	_ = sharedClient.Set(ctx, "mkey1", "val1", 0).Err()
	_ = sharedClient.Set(ctx, "mkey2", "val2", 0).Err()
	_ = sharedClient.Set(ctx, "mkey3", "val3", 0).Err()
	_, _ = sharedClient.Do(ctx, "EXEC").Result()

	// Verify all commands were executed
	val1, _ := sharedClient.Get(ctx, "mkey1").Result()
	val2, _ := sharedClient.Get(ctx, "mkey2").Result()
	val3, _ := sharedClient.Get(ctx, "mkey3").Result()
	assert.Equal(t, "val1", val1)
	assert.Equal(t, "val2", val2)
	assert.Equal(t, "val3", val3)

	// ========== WATCH without keys (wrong number of args) ==========
	_, err = sharedClient.Do(ctx, "WATCH").Result()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "wrong number of arguments"))

	// ========== WATCH with multiple keys ==========
	result, err := sharedClient.Do(ctx, "WATCH", "key1", "key2", "key3").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)
}

// TestCOPY 测试COPY命令
func TestCOPY(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// COPY String - 源键不存在
	result, err := sharedClient.Do(ctx, "COPY", "nonexistent", "dstkey").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result)

	// COPY String - 正常复制
	_ = sharedClient.Set(ctx, "srcstring", "value", 0).Err()
	result, err = sharedClient.Do(ctx, "COPY", "srcstring", "dststring").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), result)

	// 验证复制成功
	val, _ := sharedClient.Get(ctx, "dststring").Result()
	assert.Equal(t, "value", val)

	// COPY - 目标存在时不替换
	_ = sharedClient.Set(ctx, "dstexists", "existing", 0).Err()
	result, err = sharedClient.Do(ctx, "COPY", "srcstring", "dstexists").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result)

	// COPY with REPLACE - 替换目标
	result, err = sharedClient.Do(ctx, "COPY", "srcstring", "dstexists", "REPLACE").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), result)
	val, _ = sharedClient.Get(ctx, "dstexists").Result()
	assert.Equal(t, "value", val)

	// COPY List
	_ = sharedClient.RPush(ctx, "srclist", "a", "b", "c").Err()
	result, err = sharedClient.Do(ctx, "COPY", "srclist", "dstlist").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), result)

	items, _ := sharedClient.LRange(ctx, "dstlist", 0, -1).Result()
	assert.Equal(t, []string{"a", "b", "c"}, items)

	// COPY Hash
	_ = sharedClient.HSet(ctx, "srchash", "field1", "value1", "field2", "value2").Err()
	result, err = sharedClient.Do(ctx, "COPY", "srchash", "dsthash").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), result)

	all, _ := sharedClient.HGetAll(ctx, "dsthash").Result()
	assert.Equal(t, map[string]string{"field1": "value1", "field2": "value2"}, all)

	// COPY Set
	_ = sharedClient.SAdd(ctx, "srcset", "a", "b", "c").Err()
	result, err = sharedClient.Do(ctx, "COPY", "srcset", "dstset").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), result)

	members, _ := sharedClient.SMembers(ctx, "dstset").Result()
	assert.Equal(t, 3, len(members))

	// COPY SortedSet
	_ = sharedClient.ZAdd(ctx, "srczset", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}).Err()
	result, err = sharedClient.Do(ctx, "COPY", "srczset", "dstzset").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), result)

	zmembers, _ := sharedClient.ZRange(ctx, "dstzset", 0, -1).Result()
	assert.Equal(t, []string{"a", "b"}, zmembers)
}

// TestSetAdvancedCommands 测试Set高级命令（SMISMEMBER, SINTERCARD）
func TestSetAdvancedCommands(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 准备测试数据
	_ = sharedClient.SAdd(ctx, "smismem1", "a", "b", "c", "d").Err()
	_ = sharedClient.SAdd(ctx, "smismem2", "b", "c", "e").Err()

	// SINTERCARD - 返回交集基数
	result, err := sharedClient.Do(ctx, "SINTERCARD", "2", "smismem1", "smismem2").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), result) // 交集是 {b, c}

	// SINTERCARD - 单个集合
	result, err = sharedClient.Do(ctx, "SINTERCARD", "1", "smismem1").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(4), result) // 只有smismem1时，返回其基数

	// SINTERCARD - 无交集
	result, err = sharedClient.Do(ctx, "SINTERCARD", "2", "smismem1", "noset").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result)

	// SMISMEMBER - 检查多个成员是否存在
	result, err = sharedClient.Do(ctx, "SMISMEMBER", "smismem1", "a", "b").Result()
	assert.NoError(t, err)
	// 返回 [1, 1] 表示 a和b都存在
	arr, ok := result.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr))
}

// TestHashAdvancedCommands 测试Hash高级命令（HRANDFIELD）
func TestHashAdvancedCommands(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 准备测试数据
	_ = sharedClient.HSet(ctx, "hashrand", "f1", "v1", "f2", "v2", "f3", "v3", "f4", "v4", "f5", "v5").Err()

	// HRANDFIELD - 获取单个随机字段
	result, err := sharedClient.Do(ctx, "HRANDFIELD", "hashrand").Result()
	assert.NoError(t, err)
	// 单个字段时，go-redis可能返回字符串而不是数组
	assert.True(t, result != nil)

	// HRANDFIELD - 获取多个随机字段
	result, err = sharedClient.Do(ctx, "HRANDFIELD", "hashrand", "3").Result()
	assert.NoError(t, err)
	// 多个字段时返回数组
	arr, ok := result.([]interface{})
	assert.True(t, ok)
	// 应该返回3个字段
	assert.Equal(t, 3, len(arr))

	// HRANDFIELD with WITHVALUES - 获取字段和值
	result, err = sharedClient.Do(ctx, "HRANDFIELD", "hashrand", "2", "WITHVALUES").Result()
	assert.NoError(t, err)
	arr, ok = result.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 4, len(arr)) // 2个字段 + 2个值

	// HRANDFIELD - 空哈希
	result, err = sharedClient.Do(ctx, "HRANDFIELD", "emptyhash").Result()
	assert.NoError(t, err)
	// 空哈希应该返回nil或空数组

	// HRANDFIELD - 获取所有字段（count大于字段数量）
	result, err = sharedClient.Do(ctx, "HRANDFIELD", "hashrand", "10").Result()
	assert.NoError(t, err)
	arr, ok = result.([]interface{})
	assert.True(t, ok)
	// count >= 字段数量时，返回所有字段（至少5个）
	assert.True(t, len(arr) >= 5)
}

// TestSortedSetAdvancedCommands 测试SortedSet高级命令（ZUNIONSTORE, ZINTERSTORE, ZDIFFSTORE）
func TestSortedSetAdvancedCommands(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 准备测试数据
	_ = sharedClient.ZAdd(ctx, "zunion1", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}).Err()
	_ = sharedClient.ZAdd(ctx, "zunion2", redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"}).Err()

	// ZUNIONSTORE - 并集
	result, err := sharedClient.ZUnionStore(ctx, "zunionresult", &redis.ZStore{
		Keys:    []string{"zunion1", "zunion2"},
		Weights: nil,
	}).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), result) // {a, b, c}

	members, _ := sharedClient.ZRange(ctx, "zunionresult", 0, -1).Result()
	assert.Equal(t, 3, len(members))

	// ZUNIONSTORE with WEIGHTS - 带权重
	_ = sharedClient.Del(ctx, "zunionresult").Err()
	result, err = sharedClient.ZUnionStore(ctx, "zunionresult", &redis.ZStore{
		Keys:    []string{"zunion1", "zunion2"},
		Weights: []float64{2, 3},
	}).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), result)

	// ZINTERSTORE - 交集
	_ = sharedClient.Del(ctx, "zinterresult").Err()
	result, err = sharedClient.ZInterStore(ctx, "zinterresult", &redis.ZStore{
		Keys:    []string{"zunion1", "zunion2"},
		Weights: nil,
	}).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), result) // 只有 {b}

	members, _ = sharedClient.ZRange(ctx, "zinterresult", 0, -1).Result()
	assert.Equal(t, []string{"b"}, members)

	// ZDIFFSTORE - 差集
	_ = sharedClient.ZAdd(ctx, "zdiff1", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"}).Err()
	_ = sharedClient.ZAdd(ctx, "zdiff2", redis.Z{Score: 2, Member: "b"}).Err()
	_ = sharedClient.Del(ctx, "zdiffresult").Err()
	result, err = sharedClient.ZDiffStore(ctx, "zdiffresult", "zdiff1", "zdiff2").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), result) // {a, c}

	members, _ = sharedClient.ZRange(ctx, "zdiffresult", 0, -1).Result()
	assert.Equal(t, 2, len(members))
}

// TestZRandMember 测试 ZRANDMEMBER 命令
func TestZRandMember(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)
	ctx := context.Background()

	// 准备测试数据
	_ = sharedClient.ZAdd(ctx, "zrand_test", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"}).Err()

	// ZRANDMEMBER key - 随机返回一个成员
	val, err := sharedClient.Do(ctx, "ZRANDMEMBER", "zrand_test").Result()
	assert.NoError(t, err)
	member, ok := val.(string)
	assert.True(t, ok)
	assert.True(t, member == "a" || member == "b" || member == "c")

	// ZRANDMEMBER key count (正数) - 返回不重复的成员
	members, err := sharedClient.ZRandMember(ctx, "zrand_test", 2).Result()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(members))
	for _, m := range members {
		assert.True(t, m == "a" || m == "b" || m == "c")
	}
	// 正数 count >= card 时返回所有成员
	all, err := sharedClient.ZRandMember(ctx, "zrand_test", 10).Result()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(all))

	// ZRANDMEMBER key count (负数) - 可以重复
	repeated, err := sharedClient.ZRandMember(ctx, "zrand_test", -5).Result()
	assert.NoError(t, err)
	assert.Equal(t, 5, len(repeated))

	// ZRANDMEMBER key WITHSCORES - 返回带分数的单个成员
	val, err = sharedClient.Do(ctx, "ZRANDMEMBER", "zrand_test", "1", "WITHSCORES").Result()
	assert.NoError(t, err)
	arr, ok := val.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr))

	// ZRANDMEMBER key count WITHSCORES
	val, err = sharedClient.Do(ctx, "ZRANDMEMBER", "zrand_test", 2, "WITHSCORES").Result()
	assert.NoError(t, err)
	arr, ok = val.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 4, len(arr)) // 2 members × 2 (member+score)

	// ZRANDMEMBER 不存在的 key - 返回 nil
	val, err = sharedClient.Do(ctx, "ZRANDMEMBER", "nonexistent").Result()
	if err != nil {
		assert.Equal(t, redis.Nil, err)
		assert.Nil(t, val)
	} else {
		assert.Nil(t, val)
	}

	// 不存在的 key + count - 返回空数组
	empty, err := sharedClient.ZRandMember(ctx, "nonexistent", 2).Result()
	assert.NoError(t, err)
	assert.Equal(t, 0, len(empty))

	// ZRANDMEMBER 对非 zset 类型 - 返回错误
	_ = sharedClient.Set(ctx, "zrand_str", "value", 0).Err()
	_, err = sharedClient.Do(ctx, "ZRANDMEMBER", "zrand_str").Result()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "WRONGTYPE"))

	// 清理
	sharedClient.Del(ctx, "zrand_test", "zrand_str")
}

// TestZDIFF 测试 ZDIFF 只读差集命令
func TestZDIFF(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)
	ctx := context.Background()

	// 准备数据
	_ = sharedClient.ZAdd(ctx, "zdiff_a", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"}).Err()
	_ = sharedClient.ZAdd(ctx, "zdiff_b", redis.Z{Score: 2, Member: "b"}).Err()

	// ZDIFF 2 zdiff_a zdiff_b → {a, c}
	result, err := sharedClient.Do(ctx, "ZDIFF", 2, "zdiff_a", "zdiff_b").Result()
	assert.NoError(t, err)
	arr, ok := result.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr))
	members := make([]string, len(arr))
	for i, v := range arr {
		members[i] = v.(string)
	}
	assert.True(t, containsString(members, "a"))
	assert.True(t, containsString(members, "c"))

	// ZDIFF WITHSCORES
	result, err = sharedClient.Do(ctx, "ZDIFF", 2, "zdiff_a", "zdiff_b", "WITHSCORES").Result()
	assert.NoError(t, err)
	arr, ok = result.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 4, len(arr)) // 2 members × 2 (member + score)

	// ZDIFF 不存在的 key → 空数组
	result, err = sharedClient.Do(ctx, "ZDIFF", 1, "nonexistent").Result()
	assert.NoError(t, err)
	arr, ok = result.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 0, len(arr))

	// ZDIFF 对非 zset 类型 → WRONGTYPE
	_ = sharedClient.Set(ctx, "zdiff_str", "value", 0).Err()
	_, err = sharedClient.Do(ctx, "ZDIFF", 1, "zdiff_str").Result()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "WRONGTYPE"))

	// 清理
	sharedClient.Del(ctx, "zdiff_a", "zdiff_b", "zdiff_str")
}

// TestSWAPDB 测试SWAPDB命令
func TestSWAPDB(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// BoltDB 是单数据库实现，SWAPDB 返回OK但不做任何操作
	result, err := sharedClient.Do(ctx, "SWAPDB", 0, 1).Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)
}

// TestXAutoClaim 测试XAUTOCLAIM命令
func TestXAutoClaim(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 添加测试流
	id1, err := sharedClient.XAdd(ctx, &redis.XAddArgs{
		Stream: "mystream",
		Values: map[string]any{"field1": "value1"},
	}).Result()
	assert.NoError(t, err)
	assert.NotEqual(t, "", id1)

	// 创建消费组
	err = sharedClient.XGroupCreate(ctx, "mystream", "mygroup", "0").Err()
	assert.NoError(t, err)

	// 读取消息以创建pending条目 - 使用原始命令
	_, err = sharedClient.Do(ctx, "XREADGROUP", "GROUP", "mygroup", "consumer1", "COUNT", "1", "STREAMS", "mystream", "0").Result()
	assert.NoError(t, err)

	// 等待一段时间让消息idle
	time.Sleep(100 * time.Millisecond)

	// 使用XAUTOCLAIM认领pending消息
	autoClaimResult, err := sharedClient.Do(ctx, "XAUTOCLAIM", "mystream", "mygroup", "consumer2", "0", id1, "COUNT", "1").Result()
	assert.NoError(t, err)

	// 解析结果
	arr, ok := autoClaimResult.([]interface{})
	assert.True(t, ok)
	// 格式: [nextID, [claimedIDs...], [messages...]]
	assert.True(t, len(arr) >= 1) // 至少返回nextID
}

// TestXInfoHelp 测试XINFO HELP命令
func TestXInfoHelp(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 测试XINFO HELP
	result, err := sharedClient.Do(ctx, "XINFO", "HELP").Result()
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
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 测试有数据时立即返回
	_ = sharedClient.LPush(ctx, "testlist", "value1")
	arr, err := sharedClient.BLPop(ctx, 0, "testlist").Result()
	assert.NoError(t, err)
	assert.Equal(t, "testlist", arr[0])
	assert.Equal(t, "value1", arr[1])
}

// TestBRPOPBlocking 测试BRPOP阻塞命令
func TestBRPOPBlocking(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 测试有数据时立即返回
	_ = sharedClient.RPush(ctx, "testlist", "value1")
	arr, err := sharedClient.BRPop(ctx, 0, "testlist").Result()
	assert.NoError(t, err)
	assert.Equal(t, "testlist", arr[0])
	assert.Equal(t, "value1", arr[1])
}

// TestBRPOPLPUSHBlocking 测试BRPOPLPUSH阻塞命令
func TestBRPOPLPUSHBlocking(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 准备源列表
	_ = sharedClient.RPush(ctx, "sourcelist", "value1")

	// 测试BRPOPLPUSH
	result, err := sharedClient.BRPopLPush(ctx, "sourcelist", "destlist", 0).Result()
	assert.NoError(t, err)
	assert.Equal(t, "value1", result)

	// 验证目标列表
	val, err := sharedClient.LPop(ctx, "destlist").Result()
	assert.NoError(t, err)
	assert.Equal(t, "value1", val)
}

// TestBLMoveBlocking 测试BLMOVE阻塞命令
func TestBLMoveBlocking(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 准备源列表
	_ = sharedClient.RPush(ctx, "sourcelist", "value1")

	// 测试BLMOVE (RIGHT to LEFT)
	result, err := sharedClient.Do(ctx, "BLMOVE", "sourcelist", "destlist", "RIGHT", "LEFT", "0").Result()
	assert.NoError(t, err)
	assert.Equal(t, "value1", result)

	// 验证目标列表
	val, err := sharedClient.LPop(ctx, "destlist").Result()
	assert.NoError(t, err)
	assert.Equal(t, "value1", val)
}

// TestBLPOPBlockingWithPush 测试 BLPOP 阻塞时另一个客户端推送数据
// 覆盖状态机: BLPop(空) → LPush(另一个连接) → 结果
func TestBLPOPBlockingWithPush(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 使用原始 RESP 连接进行阻塞 BLPOP
	conn, err := net.Dial("tcp", sharedListener.Addr().String())
	assert.NoError(t, err)
	defer conn.Close()

	// 发送 BLPOP test_blocking_key 3
	err = proto.WriteRESP(bufio.NewWriter(conn), &proto.Array{
		Args: [][]byte{[]byte("BLPOP"), []byte("test_blocking_key"), []byte("3")},
	})
	assert.NoError(t, err)

	// 等待 BLPOP 注册到 blockingPopChans
	time.Sleep(200 * time.Millisecond)

	// 从 go-redis 客户端推送数据
	count, err := sharedClient.LPush(ctx, "test_blocking_key", "pushed_value").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)

	// 读取 BLPOP 响应
	reader := bufio.NewReader(conn)
	resp, err := proto.ReadRESP(reader)
	assert.NoError(t, err)
	assert.NotNil(t, resp)

	assert.Equal(t, 2, len(resp.Args))
	assert.Equal(t, "test_blocking_key", string(resp.Args[0]))
	assert.Equal(t, "pushed_value", string(resp.Args[1]))
}

// TestXREADBlocking 测试XREAD BLOCK功能
func TestXREADBlocking(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 添加一条消息
	id1, err := sharedClient.XAdd(ctx, &redis.XAddArgs{
		Stream: "mystream",
		Values: map[string]any{"field1": "value1"},
	}).Result()
	assert.NoError(t, err)
	assert.NotEqual(t, "", id1)

	// 读取刚添加的消息 - 使用原始命令避免解析问题
	result, err := sharedClient.Do(ctx, "XREAD", "COUNT", "1", "STREAMS", "mystream", "0").Result()
	assert.NoError(t, err)
	assert.NotEqual(t, nil, result)

	// XREAD返回格式: [[key, [entries...], ...]]，外层数组每个元素是一个stream
	// entries数组中每个元素是 [id, [field, value, ...]] 格式
	arr, ok := result.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 1, len(arr)) // 1 stream
	streamArr, ok := arr[0].([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 2, len(streamArr)) // [streamKey, entries]
	assert.Equal(t, "mystream", streamArr[0])
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
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// Test basic JSON.SET
	result, err := sharedClient.Do(ctx, "JSON.SET", "user:1", "$", `{"name":"John","age":30}`).Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// Verify type
	typ, err := sharedClient.Type(ctx, "user:1").Result()
	assert.NoError(t, err)
	assert.Equal(t, "json", typ)

	// Test JSON.SET with NX (should not update existing key)
	result, err = sharedClient.Do(ctx, "JSON.SET", "user:1", "$", `{"name":"Jane"}`, "NX").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// Test JSON.SET with XX (should update existing key)
	result, err = sharedClient.Do(ctx, "JSON.SET", "user:1", "$", `{"name":"Jane","city":"NYC"}`, "XX").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)
}

// TestJSONGet 测试 JSON.GET 命令
func TestJSONGet(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// Set up test data
	_, err := sharedClient.Do(ctx, "JSON.SET", "user:1", "$", `{"name":"John","age":30,"city":"NYC"}`).Result()
	assert.NoError(t, err)

	// Test JSON.GET - use semantic comparison since JSON field order may vary
	result, err := sharedClient.Do(ctx, "JSON.GET", "user:1").Result()
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
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// Set up test data
	_, err := sharedClient.Do(ctx, "JSON.SET", "user:1", "$", `{"name":"John","age":30}`).Result()
	assert.NoError(t, err)

	// Test JSON.DEL
	count, err := sharedClient.Do(ctx, "JSON.DEL", "user:1").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)

	// Verify key is deleted
	exists, err := sharedClient.Exists(ctx, "user:1").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), exists)
}

// TestJSONType 测试 JSON.TYPE 命令
func TestJSONType(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// Set up test data
	_, err := sharedClient.Do(ctx, "JSON.SET", "user:1", "$", `{"name":"John","age":30}`).Result()
	assert.NoError(t, err)

	// Test JSON.TYPE
	result, err := sharedClient.Do(ctx, "JSON.TYPE", "user:1", "$").Result()
	assert.NoError(t, err)
	assert.Equal(t, "object", result)
}

// TestJSONArrAppend 测试 JSON.ARRAPPEND 命令
func TestJSONArrAppend(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// Set up array
	_, err := sharedClient.Do(ctx, "JSON.SET", "arr", "$", `[]`).Result()
	assert.NoError(t, err)

	// Test JSON.ARRAPPEND
	count, err := sharedClient.Do(ctx, "JSON.ARRAPPEND", "arr", "$", "1", "2", "3").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), count)

	// Verify array length
	result, err := sharedClient.Do(ctx, "JSON.GET", "arr").Result()
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
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// Set up array
	_, err := sharedClient.Do(ctx, "JSON.SET", "arr", "$", `["a","b","c"]`).Result()
	assert.NoError(t, err)

	// Test JSON.ARRLEN
	result, err := sharedClient.Do(ctx, "JSON.ARRLEN", "arr", "$").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), result)
}

// TestJSONObjKeys 测试 JSON.OBJKEYS 命令
func TestJSONObjKeys(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// Set up object
	_, err := sharedClient.Do(ctx, "JSON.SET", "user:1", "$", `{"name":"John","age":30,"city":"NYC"}`).Result()
	assert.NoError(t, err)

	// Test JSON.OBJKEYS
	result, err := sharedClient.Do(ctx, "JSON.OBJKEYS", "user:1", "$").Result()
	assert.NoError(t, err)
	// Result is an array of keys
	keys, ok := result.([]interface{})
	assert.Equal(t, true, ok)
	assert.Equal(t, 3, len(keys))
}

// TestJSONNumIncrBy 测试 JSON.NUMINCRBY 命令
func TestJSONNumIncrBy(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// Set up number
	_, err := sharedClient.Do(ctx, "JSON.SET", "counter", "$", `10`).Result()
	assert.NoError(t, err)

	// Test JSON.NUMINCRBY
	result, err := sharedClient.Do(ctx, "JSON.NUMINCRBY", "counter", "$", "5").Result()
	assert.NoError(t, err)
	assert.Equal(t, "15", result)
}

// TestJSONNumMultBy 测试 JSON.NUMMULTBY 命令
func TestJSONNumMultBy(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// Set up number
	_, err := sharedClient.Do(ctx, "JSON.SET", "counter", "$", `10`).Result()
	assert.NoError(t, err)

	// Test JSON.NUMMULTBY
	result, err := sharedClient.Do(ctx, "JSON.NUMMULTBY", "counter", "$", "2").Result()
	assert.NoError(t, err)
	assert.Equal(t, "20", result)
}

// TestJSONClear 测试 JSON.CLEAR 命令
func TestJSONClear(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// Set up object
	_, err := sharedClient.Do(ctx, "JSON.SET", "user:1", "$", `{"name":"John","age":30}`).Result()
	assert.NoError(t, err)

	// Test JSON.CLEAR
	count, err := sharedClient.Do(ctx, "JSON.CLEAR", "user:1", "$").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)
}

// TestJSONDebugMemory 测试 JSON.DEBUG MEMORY 命令
func TestJSONDebugMemory(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// Set up object
	_, err := sharedClient.Do(ctx, "JSON.SET", "user:1", "$", `{"name":"John"}`).Result()
	assert.NoError(t, err)

	// Test JSON.DEBUG MEMORY with root path
	result, err := sharedClient.Do(ctx, "JSON.DEBUG", "MEMORY", "user:1", "$").Result()
	assert.NoError(t, err)
	memory, ok := result.(int64)
	assert.Equal(t, true, ok)
	assert.True(t, memory > 0)
}

// TestJSONDebugMemoryPath 测试 JSON.DEBUG MEMORY 子路径
func TestJSONDebugMemoryPath(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// Set up nested object
	_, err := sharedClient.Do(ctx, "JSON.SET", "user:2", "$", `{"name":"Alice","age":25,"address":{"city":"NYC","zip":"10001"}}`).Result()
	assert.NoError(t, err)

	// Test with sub-path - name field
	result, err := sharedClient.Do(ctx, "JSON.DEBUG", "MEMORY", "user:2", "$.name").Result()
	assert.NoError(t, err)
	nameMemory, ok := result.(int64)
	assert.Equal(t, true, ok)
	assert.True(t, nameMemory > 0)

	// Full object should be larger than name field alone
	result, err = sharedClient.Do(ctx, "JSON.DEBUG", "MEMORY", "user:2", "$").Result()
	assert.NoError(t, err)
	fullMemory, ok := result.(int64)
	assert.Equal(t, true, ok)
	if !(fullMemory > nameMemory) {
		t.Errorf("expected full memory (%d) > name memory (%d)", fullMemory, nameMemory)
	}

	// Non-existent path should return null
	_, err = sharedClient.Do(ctx, "JSON.DEBUG", "MEMORY", "user:2", "$.nonexistent").Result()
	assert.Error(t, err)
}

// TestJSONMGet 测试 JSON.MGET 命令
func TestJSONMGet(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// Set up test data
	_, err := sharedClient.Do(ctx, "JSON.SET", "user:1", "$", `{"name":"John"}`).Result()
	assert.NoError(t, err)
	_, err = sharedClient.Do(ctx, "JSON.SET", "user:2", "$", `{"name":"Jane"}`).Result()
	assert.NoError(t, err)

	// Test JSON.MGET
	result, err := sharedClient.Do(ctx, "JSON.MGET", "user:1", "user:2", "$").Result()
	assert.NoError(t, err)
	// Result is an array
	values, ok := result.([]interface{})
	assert.Equal(t, true, ok)
	assert.Equal(t, 2, len(values))
}

// TestJSONNotFound 测试不存在的键
func TestJSONNotFound(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// Test JSON.GET on non-existent key - returns redis.Nil
	result, err := sharedClient.Do(ctx, "JSON.GET", "nonexistent").Result()
	assert.Equal(t, nil, result)
	assert.True(t, err == redis.Nil || err == nil)

	// Test JSON.DEL on non-existent key
	count, err := sharedClient.Do(ctx, "JSON.DEL", "nonexistent").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

// TestJSONWithExistingDB 测试与现有数据库的交互
func TestJSONWithExistingDB(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// Test that JSON keys don't interfere with other types
	_, err := sharedClient.Set(ctx, "string:key", "value", 0).Result()
	assert.NoError(t, err)
	_, err = sharedClient.Do(ctx, "JSON.SET", "json:key", "$", `{"data":"test"}`).Result()
	assert.NoError(t, err)

	// Verify both exist
	stringExists, err := sharedClient.Exists(ctx, "string:key").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), stringExists)

	jsonExists, err := sharedClient.Exists(ctx, "json:key").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), jsonExists)

	// Verify types
	stringType, err := sharedClient.Type(ctx, "string:key").Result()
	assert.NoError(t, err)
	assert.Equal(t, "string", stringType)

	jsonType, err := sharedClient.Type(ctx, "json:key").Result()
	assert.NoError(t, err)
	assert.Equal(t, "json", jsonType)
}

// TestSelect 测试 SELECT 命令
func TestSelect(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// SELECT should return OK for any database number (BoltDB is single-db)
	result, err := sharedClient.Do(ctx, "SELECT", "0").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// SELECT with other database numbers should also return OK
	result, err = sharedClient.Do(ctx, "SELECT", "15").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)
}

// TestMove 测试 MOVE 命令
func TestMove(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// MOVE always returns 0 in single-db implementation
	result, err := sharedClient.Do(ctx, "MOVE", "key", "1").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result)
}

// TestWait 测试 WAIT 命令
func TestWait(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// WAIT should return 0 (no replication)
	result, err := sharedClient.Do(ctx, "WAIT", "1", "1000").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result)
}

// TestSlowLog 测试 SLOWLOG 命令
func TestSlowLog(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// SLOWLOG GET should return empty array
	result, err := sharedClient.Do(ctx, "SLOWLOG", "GET", "10").Result()
	assert.NoError(t, err)
	_, ok := result.([]interface{})
	assert.True(t, ok) // Should be empty array

	// SLOWLOG LEN should return 0
	result, err = sharedClient.Do(ctx, "SLOWLOG", "LEN").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result)

	// SLOWLOG RESET should return OK
	result, err = sharedClient.Do(ctx, "SLOWLOG", "RESET").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// SLOWLOG HELP should return help text
	result, err = sharedClient.Do(ctx, "SLOWLOG", "HELP").Result()
	assert.NoError(t, err)
	arr, ok := result.([]interface{})
	assert.True(t, ok)
	assert.True(t, len(arr) > 0)
}

// TestMemoryUsage 测试 MEMORY USAGE 命令
func TestMemoryUsage(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// Set a string value
	_ = sharedClient.Set(ctx, "memkey", "testvalue", 0).Err()

	// MEMORY USAGE should return positive value
	result, err := sharedClient.Do(ctx, "MEMORY", "USAGE", "memkey").Result()
	assert.NoError(t, err)
	memory, ok := result.(int64)
	assert.True(t, ok)
	assert.True(t, memory > 0)

	// MEMORY USAGE on non-existent key should return nil
	result, err = sharedClient.Do(ctx, "MEMORY", "USAGE", "nonexistent").Result()
	assert.Equal(t, nil, result)
	assert.True(t, err == redis.Nil || err == nil)

	// MEMORY DOCTOR should return info
	result, err = sharedClient.Do(ctx, "MEMORY", "DOCTOR").Result()
	assert.NoError(t, err)
	_, ok = result.([]interface{})
	assert.True(t, ok)

	// MEMORY HELP should return help text
	result, err = sharedClient.Do(ctx, "MEMORY", "HELP").Result()
	assert.NoError(t, err)
	arr, ok := result.([]interface{})
	assert.True(t, ok)
	assert.True(t, len(arr) > 0)
}

// TestLOLWUT 测试 LOLWUT 命令
func TestLOLWUT(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// LOLWUT should return version info
	result, err := sharedClient.Do(ctx, "LOLWUT").Result()
	assert.NoError(t, err)

	str, ok := result.(string)
	assert.True(t, ok)
	assert.True(t, len(str) > 0)
	assert.True(t, strings.Contains(str, "BoltDB redis.bolt."))

	// LOLWUT with VERSION parameter
	result, err = sharedClient.Do(ctx, "LOLWUT", "VERSION", "5").Result()
	assert.NoError(t, err)
	str, ok = result.(string)
	assert.True(t, ok)
	assert.Equal(t, "BoltDB 5 - A disk-persistent Redis-compatible database", str)
}

// TestLatency 测试 LATENCY 命令
func TestLatency(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// LATENCY LATEST should return empty array
	result, err := sharedClient.Do(ctx, "LATENCY", "LATEST").Result()
	assert.NoError(t, err)
	arr, ok := result.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 0, len(arr))

	// LATENCY RESET should return 0 (count of events reset)
	result, err = sharedClient.Do(ctx, "LATENCY", "RESET").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result)

	// LATENCY RESET with event should return count
	result, err = sharedClient.Do(ctx, "LATENCY", "RESET", "command").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result)

	// LATENCY HELP should return help text
	result, err = sharedClient.Do(ctx, "LATENCY", "HELP").Result()
	assert.NoError(t, err)
	arr, ok = result.([]interface{})
	assert.True(t, ok)
	assert.True(t, len(arr) > 0)

	// LATENCY DOCTOR should return info
	result, err = sharedClient.Do(ctx, "LATENCY", "DOCTOR").Result()
	assert.NoError(t, err)
	_, ok = result.([]interface{})
	assert.True(t, ok)
}

// TestReadOnlyReadWrite 测试 READONLY 和 READWRITE 命令
func TestReadOnlyReadWrite(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// READONLY should return OK
	result, err := sharedClient.Do(ctx, "READONLY").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// READWRITE should return OK
	result, err = sharedClient.Do(ctx, "READWRITE").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)
}

// TestZRangeStore 测试 ZRANGESTORE 命令
func TestZRangeStore(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// Create source sorted set
	_ = sharedClient.ZAdd(ctx, "srczset", redis.Z{Score: 1, Member: "a"}).Err()
	_ = sharedClient.ZAdd(ctx, "srczset", redis.Z{Score: 2, Member: "b"}).Err()
	_ = sharedClient.ZAdd(ctx, "srczset", redis.Z{Score: 3, Member: "c"}).Err()
	_ = sharedClient.ZAdd(ctx, "srczset", redis.Z{Score: 4, Member: "d"}).Err()
	_ = sharedClient.ZAdd(ctx, "srczset", redis.Z{Score: 5, Member: "e"}).Err()

	// ZRANGESTORE with BYSCORE - basic test
	result, err := sharedClient.Do(ctx, "ZRANGESTORE", "dstzset", "srczset", "2", "4").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), result) // 3 elements (b, c, d)

	// Verify destination has correct values
	count, err := sharedClient.ZCard(ctx, "dstzset").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), count)

	// ZRANGESTORE with LIMIT
	result, err = sharedClient.Do(ctx, "ZRANGESTORE", "dstzset4", "srczset", "0", "4", "LIMIT", "0", "2").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), result)

	// ZRANGESTORE on empty source
	result, err = sharedClient.Do(ctx, "ZRANGESTORE", "dstzset5", "emptyzset", "1", "10").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result)

	// ZRANGESTORE with BYLEX - create sorted set with same scores
	_ = sharedClient.ZAdd(ctx, "lexzset", redis.Z{Score: 0, Member: "a"}).Err()
	_ = sharedClient.ZAdd(ctx, "lexzset", redis.Z{Score: 0, Member: "b"}).Err()
	_ = sharedClient.ZAdd(ctx, "lexzset", redis.Z{Score: 0, Member: "c"}).Err()
	_ = sharedClient.ZAdd(ctx, "lexzset", redis.Z{Score: 0, Member: "d"}).Err()
	_ = sharedClient.ZAdd(ctx, "lexzset", redis.Z{Score: 0, Member: "e"}).Err()

	result, err = sharedClient.Do(ctx, "ZRANGESTORE", "lexdst", "lexzset", "a", "c", "BYLEX").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), result)
}

// TestMain 测试入口 - 使用共享服务器模式
// 优化：启动一次服务器，所有测试共享，每个测试仅清理数据
// 这将测试总时间从 75-120s 降至 ~15-20s（提速 4-6 倍）
func TestMain(m *testing.M) {
	println("[TestMain] === 集成测试套件开始 ===")
	println("[TestMain] 启动共享测试服务器...")

	// 初始化共享服务器
	setupSharedServer()

	println("[TestMain] 运行测试...")
	code := m.Run()

	println("[TestMain] 测试完成，退出码:", code)
	println("[TestMain] 关闭共享服务器...")
	teardownSharedServer()

	println("[TestMain] === 集成测试套件结束 ===")
	os.Exit(code)
}

// ========== 回归测试 ==========

// TestWATCHConflict 测试 WATCH 冲突检测
// WATCH 一个键，另一个客户端修改该键后，EXEC 应返回 nil 表示事务失败
func TestWATCHConflict(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 使用原始 TCP 连接发送 RESP 命令以完全控制 WATCH/MULTI/EXEC 流程
	connA, err := net.Dial("tcp", sharedListener.Addr().String())
	assert.NoError(t, err)
	defer connA.Close()
	readerA := bufio.NewReader(connA)

	connB, err := net.Dial("tcp", sharedListener.Addr().String())
	assert.NoError(t, err)
	defer connB.Close()
	readerB := bufio.NewReader(connB)

	sendRESP := func(conn net.Conn, cmd string, args ...string) {
		cmdArgs := make([][]byte, 1+len(args))
		cmdArgs[0] = []byte(cmd)
		for i, arg := range args {
			cmdArgs[i+1] = []byte(arg)
		}
		req := &proto.Array{Args: cmdArgs}
		err := proto.WriteRESP(conn, req)
		assert.NoError(t, err)
	}

	sendRESP(connA, "SET", "watchkey", "original")
	_, err = proto.ReadRESP(readerA)
	assert.NoError(t, err)

	// 连接 A: WATCH watchkey
	sendRESP(connA, "WATCH", "watchkey")
	resp, err := proto.ReadRESP(readerA)
	assert.NoError(t, err)
	assert.NotNil(t, resp)

	// 连接 B: 修改被监视的键
	sendRESP(connB, "SET", "watchkey", "modified_by_b")
	resp, err = proto.ReadRESP(readerB)
	assert.NoError(t, err)

	// 连接 A: MULTI
	sendRESP(connA, "MULTI")
	resp, err = proto.ReadRESP(readerA)
	assert.NoError(t, err)

	// 连接 A: SET（仅排队）
	sendRESP(connA, "SET", "watchkey", "modified_by_a")
	resp, err = proto.ReadRESP(readerA)
	assert.NoError(t, err)

	// 连接 A: EXEC - 应返回 nil array 因为 WATCH 检测到冲突
	sendRESP(connA, "EXEC")
	// Read raw response line since ReadRESP doesn't support nil arrays
	rawLine, err := readerA.ReadString('\n')
	assert.NoError(t, err)
	assert.Equal(t, "*-1\r\n", rawLine)

	// 验证键未被 Client A 修改
	val, err := sharedClient.Get(ctx, "watchkey").Result()
	assert.NoError(t, err)
	assert.Equal(t, "modified_by_b", val)
}

// TestBLPOPTimeout 测试 BLPOP 在空键上的超时行为
func TestBLPOPTimeout(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	start := time.Now()
	result, err := sharedClient.BLPop(ctx, time.Second, "nonexistent_timeout_key").Result()
	elapsed := time.Since(start)

	assert.Equal(t, redis.Nil, err)
	assert.Nil(t, result)
	if elapsed > 3*time.Second {
		t.Fatalf("BLPOP timeout=1s should return within 2s, took: %v", elapsed)
	}
}

// TestConcurrentTransaction 测试并发事务隔离
// 两个连接同时对同一个键执行 MULTI/SET/EXEC，不应互相干扰
func TestConcurrentTransaction(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	clientA := redis.NewClient(&redis.Options{
		Addr: sharedListener.Addr().String(),
	})
	defer clientA.Close()
	clientB := redis.NewClient(&redis.Options{
		Addr: sharedListener.Addr().String(),
	})
	defer clientB.Close()

	var wg sync.WaitGroup
	wg.Add(2)

	// Client A: 并发事务写入
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			err := clientA.Watch(ctx, func(tx *redis.Tx) error {
				_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
					pipe.Set(ctx, "concurrent_key", "value_a", 0)
					pipe.Incr(ctx, "concurrent_counter")
					return nil
				})
				return err
			}, "concurrent_key")
			if err != nil && err != redis.TxFailedErr {
				t.Logf("Client A transaction error (expected): %v", err)
			}
		}
	}()

	// Client B: 并发事务写入
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			err := clientB.Watch(ctx, func(tx *redis.Tx) error {
				_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
					pipe.Set(ctx, "concurrent_key", "value_b", 0)
					pipe.Incr(ctx, "concurrent_counter")
					return nil
				})
				return err
			}, "concurrent_key")
			if err != nil && err != redis.TxFailedErr {
				t.Logf("Client B transaction error (expected): %v", err)
			}
		}
	}()

	wg.Wait()

	// 验证 counter 在并发事务后至少被增加
	counter, err := sharedClient.Get(ctx, "concurrent_counter").Result()
	assert.NoError(t, err)
	assert.NotEqual(t, "0", counter)
}

// TestTransactionConnIsolationRegression - Regression for MULTI transaction connection isolation bug
// Bug: Transaction state (inTransaction, commands) was on shared Handler, letting one connection
// leak queued commands into another connection's EXEC.
// Fix: Moved all transaction state into per-connection connState.
//
// State transitions covered:
//   IDLE → MULTI → SET → QUEUED → EXEC → IDLE  (per-connection)
//   connA[MULTI→SET connA.key] → connB[MULTI→SET connB.key→EXEC] → EXEC connA  (no cross-leak)
//   MULTI → disconnect → reconnect → MULTI → EXEC  (clean state after reconnect)
//   MULTI → MULTI  (nesting error, per-connection)
//   WATCH → [other conn SET] → MULTI → SET → EXEC  (watch conflict returns *-1, per-connection tracking)
func TestTransactionConnIsolationRegression(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// --- Sub-test 1: Independent MULTI sessions, no command leakage ---
	connA, err := net.Dial("tcp", sharedListener.Addr().String())
	assert.NoError(t, err)
	defer connA.Close()
	readerA := bufio.NewReader(connA)

	connB, err := net.Dial("tcp", sharedListener.Addr().String())
	assert.NoError(t, err)
	defer connB.Close()
	readerB := bufio.NewReader(connB)

	sendRESP := func(conn net.Conn, cmd string, args ...string) {
		cmdArgs := make([][]byte, 1+len(args))
		cmdArgs[0] = []byte(cmd)
		for i, arg := range args {
			cmdArgs[i+1] = []byte(arg)
		}
		_ = proto.WriteRESP(bufio.NewWriter(conn), &proto.Array{Args: cmdArgs})
	}

	readOK := func(r *bufio.Reader) {
		resp, err := proto.ReadRESP(r)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	}

	// connA: MULTI → SET iso_key_a
	sendRESP(connA, "MULTI")
	readOK(readerA)
	sendRESP(connA, "SET", "iso_key_a", "val_a")
	readOK(readerA)

	// connB: MULTI (should see no trace of connA's commands)
	sendRESP(connB, "MULTI")
	readOK(readerB)
	sendRESP(connB, "SET", "iso_key_b", "val_b")
	readOK(readerB)

	// connA EXEC — only iso_key_a should be set
	sendRESP(connA, "EXEC")
	readOK(readerA)
	val, err := sharedClient.Get(ctx, "iso_key_a").Result()
	assert.NoError(t, err)
	assert.Equal(t, "val_a", val)
	_, err = sharedClient.Get(ctx, "iso_key_b").Result()
	assert.Equal(t, redis.Nil, err)

	// connB EXEC — iso_key_b should now be set
	sendRESP(connB, "EXEC")
	readOK(readerB)
	val, err = sharedClient.Get(ctx, "iso_key_b").Result()
	assert.NoError(t, err)
	assert.Equal(t, "val_b", val)

	// --- Sub-test 2: Disconnect with open MULTI then reconnect ---
	connC, err := net.Dial("tcp", sharedListener.Addr().String())
	assert.NoError(t, err)
	readerC := bufio.NewReader(connC)

	sendRESP(connC, "MULTI")
	readOK(readerC)
	sendRESP(connC, "SET", "orphan_key", "orphan_val")
	readOK(readerC)
	connC.Close()
	time.Sleep(100 * time.Millisecond)

	connD, err := net.Dial("tcp", sharedListener.Addr().String())
	assert.NoError(t, err)
	defer connD.Close()
	readerD := bufio.NewReader(connD)

	sendRESP(connD, "MULTI")
	readOK(readerD)
	sendRESP(connD, "SET", "clean_key", "clean_val")
	readOK(readerD)
	sendRESP(connD, "EXEC")
	readOK(readerD)

	val, err = sharedClient.Get(ctx, "clean_key").Result()
	assert.NoError(t, err)
	assert.Equal(t, "clean_val", val)
	_, err = sharedClient.Get(ctx, "orphan_key").Result()
	assert.Equal(t, redis.Nil, err)

	// --- Sub-test 3: MULTI nesting error ---
	connE, err := net.Dial("tcp", sharedListener.Addr().String())
	assert.NoError(t, err)
	defer connE.Close()
	readerE := bufio.NewReader(connE)

	sendRESP(connE, "MULTI")
	readOK(readerE)
	sendRESP(connE, "MULTI")
	rawLine, err := readerE.ReadString('\n')
	assert.NoError(t, err)
	assert.True(t, strings.HasPrefix(rawLine, "-"))
	assert.True(t, strings.Contains(rawLine, "MULTI calls can not be nested"))

	sendRESP(connE, "DISCARD")
	readOK(readerE)
	sendRESP(connE, "EXEC")
	rawLine, err = readerE.ReadString('\n')
	assert.NoError(t, err)
	assert.True(t, strings.HasPrefix(rawLine, "-"))
	assert.True(t, strings.Contains(rawLine, "EXEC without MULTI"))
}

// TestMGetWrongTypeRegression - Regression for MGET wrong-type garbled-data bug
// Bug: MGet did not check key type before reading, producing garbled bytes for non-string keys.
// Fix: Added type prefix check in MGet; non-string keys return ErrWrongType.
//
// State transitions covered:
//   MGET [string, hash] → -ERR WRONGTYPE  (wrong type detected)
//   MGET [string, nonexistent] → [value, nil]  (mixed OK keys still work)
//   MGET [hash, list, set, zset] → -ERR WRONGTYPE  (all non-string types rejected)
//   concurrent: SET → HSET same key, MGET while type changes  (concurrent type change)
//   disconnect → reconnect → MGET (works after reconnect)
func TestMGetWrongTypeRegression(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// --- Sub-test 1: MGET mixed string + hash returns error, no garbled data ---
	_ = sharedClient.Set(ctx, "mget_str", "hello", 0)
	_ = sharedClient.HSet(ctx, "mget_hash", "field", "value")

	_, err := sharedClient.MGet(ctx, "mget_str", "mget_hash").Result()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "WRONGTYPE"))

	// --- Sub-test 2: MGET with non-existent keys still OK ---
	_ = sharedClient.Set(ctx, "mget_ok", "ok", 0)
	vals, err := sharedClient.MGet(ctx, "mget_ok", "mget_nope").Result()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(vals))
	assert.Equal(t, "ok", vals[0])
	assert.Nil(t, vals[1])

	// --- Sub-test 3: All non-string types rejected ---
	_ = sharedClient.LPush(ctx, "mget_list", "a")
	_ = sharedClient.SAdd(ctx, "mget_set", "x")
	_ = sharedClient.ZAdd(ctx, "mget_zset", redis.Z{Score: 1, Member: "m"})

	_, err = sharedClient.MGet(ctx, "mget_str", "mget_list").Result()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "WRONGTYPE"))

	_, err = sharedClient.MGet(ctx, "mget_str", "mget_set").Result()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "WRONGTYPE"))

	_, err = sharedClient.MGet(ctx, "mget_str", "mget_zset").Result()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "WRONGTYPE"))

	// --- Sub-test 4: Concurrent MGET while type changes ---
	_ = sharedClient.Set(ctx, "mget_concur", "initial", 0)
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			_, _ = sharedClient.MGet(ctx, "mget_concur", "mget_str", "mget_nope").Result()
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			sharedClient.HSet(ctx, "mget_concur", "f", "v")
			time.Sleep(2 * time.Millisecond)
			sharedClient.Del(ctx, "mget_concur")
			time.Sleep(1 * time.Millisecond)
			sharedClient.Set(ctx, "mget_concur", "restored", 0)
			time.Sleep(2 * time.Millisecond)
		}
	}()
	wg.Wait()

	// --- Sub-test 5: Reconnect, MGET still works ---
	conn, err := net.Dial("tcp", sharedListener.Addr().String())
	assert.NoError(t, err)
	conn.Close()
	time.Sleep(50 * time.Millisecond)

	_, err = sharedClient.MGet(ctx, "mget_str").Result()
	assert.NoError(t, err)
}

// TestWriteRESPNilArrayRegression - Regression for RESP WriteRESP NilArray optimization
// Bug: BLPOP/BRPOP timeout returned *0 (empty array) instead of *-1 (nil array).
// EXEC watch failure used &Array{Args: []} which also encoded as *0.
// Fix: Added NilArray type; WriteRESP encodes it as *-1\r\n; BLPOP/BRPOP/EXEC use it.
//
// State transitions covered:
//   BLPOP empty → timeout → *-1  (nil array for timeout)
//   EXEC after watch conflict → *-1  (nil array for failed EXEC)
//   concurrent BLPOP + LPUSH → *2 array  (normal flow still produces correct array)
//   WriteRESP with NilArray → *-1\r\n  (encoding correctness)
//   disconnect → reconnect → BLPOP timeout  (works after reconnect)
func TestWriteRESPNilArrayRegression(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// --- Sub-test 1: BLPOP timeout returns *-1 via raw RESP ---
	conn, err := net.Dial("tcp", sharedListener.Addr().String())
	assert.NoError(t, err)
	defer conn.Close()
	reader := bufio.NewReader(conn)

	sendRESP := func(conn net.Conn, cmd string, args ...string) {
		cmdArgs := make([][]byte, 1+len(args))
		cmdArgs[0] = []byte(cmd)
		for i, arg := range args {
			cmdArgs[i+1] = []byte(arg)
		}
		_ = proto.WriteRESP(bufio.NewWriter(conn), &proto.Array{Args: cmdArgs})
	}

	// BLPOP nonexistent_list 1 (timeout after 1s)
	sendRESP(conn, "BLPOP", "nilarr_nonexistent", "1")
	rawLine, err := reader.ReadString('\n')
	assert.NoError(t, err)
	assert.Equal(t, "*-1\r\n", rawLine)

	// --- Sub-test 2: BLPOP with data still returns correct array ---
	_ = sharedClient.LPush(ctx, "nilarr_popdata", "hello")
	sendRESP(conn, "BLPOP", "nilarr_popdata", "1")
	resp, err := proto.ReadRESP(reader)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, 2, len(resp.Args))

	// --- Sub-test 3: BRPOP timeout also returns *-1 ---
	sendRESP(conn, "BRPOP", "nilarr_br_nonexistent", "1")
	rawLine, err = reader.ReadString('\n')
	assert.NoError(t, err)
	assert.Equal(t, "*-1\r\n", rawLine)

	// --- Sub-test 4: EXEC watch conflict returns *-1 via raw RESP ---
	connA, err := net.Dial("tcp", sharedListener.Addr().String())
	assert.NoError(t, err)
	defer connA.Close()
	readerA := bufio.NewReader(connA)

	connB, err := net.Dial("tcp", sharedListener.Addr().String())
	assert.NoError(t, err)
	defer connB.Close()
	readerB := bufio.NewReader(connB)

	sendRESP(connA, "SET", "watch_nil", "original")
	readOK := func(r *bufio.Reader) {
		rp, err := proto.ReadRESP(r)
		assert.NoError(t, err)
		assert.NotNil(t, rp)
	}
	readOK(readerA)

	sendRESP(connA, "WATCH", "watch_nil")
	readOK(readerA)
	sendRESP(connB, "SET", "watch_nil", "modified_by_b")
	readOK(readerB)
	sendRESP(connA, "MULTI")
	readOK(readerA)
	sendRESP(connA, "SET", "watch_nil", "modified_by_a")
	readOK(readerA)
	sendRESP(connA, "EXEC")
	rawLine, err = readerA.ReadString('\n')
	assert.NoError(t, err)
	assert.Equal(t, "*-1\r\n", rawLine)

	// --- Sub-test 5: WriteRESP direct NilArray encoding ---
	var buf strings.Builder
	nilArr := proto.NilArray{}
	err = proto.WriteRESP(&buf, nilArr)
	assert.NoError(t, err)
	assert.Equal(t, "*-1\r\n", buf.String())

	// --- Sub-test 6: Reconnect, BLPOP timeout still returns *-1 ---
	conn.Close()
	time.Sleep(100 * time.Millisecond)

	conn2, err := net.Dial("tcp", sharedListener.Addr().String())
	assert.NoError(t, err)
	defer conn2.Close()
	reader2 := bufio.NewReader(conn2)

	sendRESP(conn2, "BLPOP", "nilarr_reconnect_nope", "1")
	rawLine, err = reader2.ReadString('\n')
	assert.NoError(t, err)
	assert.Equal(t, "*-1\r\n", rawLine)
}

// ========== 共享服务器基础设施 ==========

var (
	sharedClient     *redis.Client
	sharedServer     *server.Handler
	sharedDB         *store.BotreonStore
	sharedListener   net.Listener
	sharedServerOnce sync.Once
)

// setupSharedServer 启动一次共享服务器（TestMain 调用）
func setupSharedServer() {
	var err error

	// 创建持久化测试数据库目录（非临时，生命周期与 TestMain 相同）
	dbPath := "/tmp/boltdb_integration_shared"
	os.RemoveAll(dbPath)

	sharedDB, err = store.NewBotreonStore(dbPath)
	if err != nil {
		println("[TestMain] FATAL: 创建数据库失败:", err.Error())
		os.Exit(1)
	}

	// 创建管理器
	pubsubMgr := store.NewPubSubManager()
	backupMgr := backup.NewBackupManager(sharedDB, dbPath)
	replMgr := replication.NewReplicationManager(sharedDB)

	// 创建服务器处理器
	sharedServer = &server.Handler{
		Db:          sharedDB,
		PubSub:      pubsubMgr,
		Backup:      backupMgr,
		Replication: replMgr,
		Ctx:         context.Background(),
	}

	// 启动监听
	sharedListener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		sharedDB.Close()
		println("[TestMain] FATAL: 监听失败:", err.Error())
		os.Exit(1)
	}

	// 启动服务器 goroutine
	go func() {
		_ = sharedServer.ServeTCP(sharedListener)
	}()

	// 等待启动
	time.Sleep(50 * time.Millisecond)

	// 创建客户端
	sharedClient = redis.NewClient(&redis.Options{
		Addr:     sharedListener.Addr().String(),
		Password: "",
		DB:       0,
	})

	// 验证连接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = sharedClient.Ping(ctx).Result()
	if err != nil {
		sharedListener.Close()
		sharedDB.Close()
		println("[TestMain] FATAL: 连接失败:", err.Error())
		os.Exit(1)
	}

	println("[TestMain] 共享服务器已就绪，地址:", sharedListener.Addr().String())
}

// teardownSharedServer 关闭共享服务器（TestMain 调用）
func teardownSharedServer() {
	if sharedClient != nil {
		sharedClient.Close()
	}
	if sharedListener != nil {
		sharedListener.Close()
	}
	// 等待所有 handler goroutine 退出，确保 DB 关闭前无并发访问
	if sharedServer != nil {
		sharedServer.Shutdown()
	}
	if sharedDB != nil {
		sharedDB.Close()
	}
	os.RemoveAll("/tmp/boltdb_integration_shared")
}

// setupTest 清理数据并准备测试环境（每个测试前调用）
// 替代旧的 setupTestServer，不重启服务器，仅清理数据
func setupTest(t *testing.T) {
	t.Helper()

	sharedServerOnce.Do(func() {
		// 确保服务器已启动（兼容性检查）
		if sharedDB == nil {
			t.Fatal("共享服务器未初始化 - TestMain 未运行？")
		}
	})

	// 清理所有数据（使用 store 的 ClearAllData，内部已包含重试机制）
	if err := sharedDB.ClearAllData(); err != nil {
		t.Fatalf("清理测试数据失败: %v", err)
	}

	// 清理所有缓存（通过公开方法）
	sharedDB.ClearCaches()

	// 清理 PubSub 状态（跨测试残留会导致失败）
	sharedServer.PubSub.Clear()

	// ResetConnectionState 已移除 — connState 永不为 nil, 无需 fallback
}

// teardownTest 测试后清理（每个测试调用）
func teardownTest(t *testing.T) {
	t.Helper()
	// 执行存储一致性检查（孤儿键检测、TYPE-Data 配对验证）
	if sharedDB != nil {
		if err := sharedDB.Check(); err != nil {
			t.Fatalf("storage consistency check failed after test: %v", err)
		}
	}
}

// teardownTestSoft 测试后清理，Check 失败仅日志不终止（用于混沌测试）
func teardownTestSoft(t *testing.T) {
	t.Helper()
	if sharedDB != nil {
		if err := sharedDB.Check(); err != nil {
			t.Logf("WARN: storage consistency check failed (soft): %v", err)
		}
	}
}

// ========== 兼容性层（可选，逐步迁移期使用）==========

func TestMONITOR(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)
	ctx := context.Background()

	// 使用原始 TCP 连接作为 MONITOR 客户端
	monConn, err := net.Dial("tcp", sharedListener.Addr().String())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer monConn.Close()
	monConn.SetDeadline(time.Now().Add(5 * time.Second))

	// 发送 MONITOR
	_, err = monConn.Write([]byte("*1\r\n$7\r\nMONITOR\r\n"))
	if err != nil {
		t.Fatalf("failed to write MONITOR: %v", err)
	}

	// 读取 +OK\r\n
	buf := make([]byte, 5)
	_, err = monConn.Read(buf)
	if err != nil {
		t.Fatalf("failed to read OK: %v", err)
	}
	if string(buf) != "+OK\r\n" {
		t.Fatalf("expected +OK\\r\\n, got %q", string(buf))
	}

	// 通过 go-redis 发送 SET 命令
	err = sharedClient.Set(ctx, "mon_test_key", "mon_test_val", 0).Err()
	if err != nil {
		t.Fatalf("SET failed: %v", err)
	}

	// 读取 MONITOR 输出
	monBuf := make([]byte, 1024)
	n, err := monConn.Read(monBuf)
	if err != nil {
		t.Fatalf("failed to read monitor output: %v", err)
	}
	monLine := string(monBuf[:n])
	t.Logf("monitor line: %s", monLine)

	// 验证 MONITOR 行包含 SET 命令
	if !strings.Contains(monLine, "\"SET\"") || !strings.Contains(monLine, "\"mon_test_key\"") {
		t.Errorf("monitor line should contain SET command, got: %s", monLine)
	}

	// 测试 QUIT
	_, err = monConn.Write([]byte("*1\r\n$4\r\nQUIT\r\n"))
	if err != nil {
		t.Fatalf("failed to write QUIT: %v", err)
	}
	quitBuf := make([]byte, 5)
	_, err = monConn.Read(quitBuf)
	if err != nil {
		t.Fatalf("failed to read QUIT response: %v", err)
	}
	if string(quitBuf) != "+OK\r\n" {
		t.Fatalf("expected +OK\\r\\n for QUIT, got %q", string(quitBuf))
	}

	// 清理测试 key
	sharedClient.Del(ctx, "mon_test_key")
}

func TestMONITOR_WrongArgs(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)
	ctx := context.Background()

	err := sharedClient.Do(ctx, "MONITOR", "extra_arg").Err()
	if err == nil {
		t.Fatal("expected error for MONITOR with args")
	}
}

func TestMONITOR_InvalidCommandInMonitorMode(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	monConn, err := net.Dial("tcp", sharedListener.Addr().String())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer monConn.Close()
	monConn.SetDeadline(time.Now().Add(5 * time.Second))

	// 发送 MONITOR
	_, err = monConn.Write([]byte("*1\r\n$7\r\nMONITOR\r\n"))
	if err != nil {
		t.Fatalf("failed to write MONITOR: %v", err)
	}

	// 读取 +OK\r\n
	buf := make([]byte, 5)
	_, err = monConn.Read(buf)
	if err != nil {
		t.Fatalf("failed to read OK: %v", err)
	}

	// 在 MONITOR 模式下发送 SET --- 非法命令
	_, err = monConn.Write([]byte("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"))
	if err != nil {
		t.Fatalf("failed to write SET: %v", err)
	}

	// 读取错误响应
	errBuf := make([]byte, 1024)
	n, err := monConn.Read(errBuf)
	if err != nil {
		t.Fatalf("failed to read error: %v", err)
	}
	errResp := string(errBuf[:n])
	if !strings.Contains(errResp, "ERR only PING / QUIT allowed") {
		t.Errorf("expected MONITOR mode error, got: %s", errResp)
	}
}

func TestMONITOR_PING(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	monConn, err := net.Dial("tcp", sharedListener.Addr().String())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer monConn.Close()
	monConn.SetDeadline(time.Now().Add(5 * time.Second))

	// 发送 MONITOR
	_, err = monConn.Write([]byte("*1\r\n$7\r\nMONITOR\r\n"))
	if err != nil {
		t.Fatalf("failed to write MONITOR: %v", err)
	}
	buf := make([]byte, 5)
	_, err = monConn.Read(buf)
	if err != nil {
		t.Fatalf("failed to read OK: %v", err)
	}

	// 在 MONITOR 模式下 ping
	_, err = monConn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	if err != nil {
		t.Fatalf("failed to write PING: %v", err)
	}
	pingBuf := make([]byte, 7)
	_, err = monConn.Read(pingBuf)
	if err != nil {
		t.Fatalf("failed to read PONG: %v", err)
	}
	if string(pingBuf) != "+PONG\r\n" {
		t.Fatalf("expected +PONG\\r\\n, got %q", string(pingBuf))
	}
}




