package server

import (
	"fmt"
	"sort"
	"strings"

	"github.com/lbp0200/BoltDB/internal/proto"
)

type commandFlag string

const (
	flagWrite    commandFlag = "write"
	flagReadonly commandFlag = "readonly"
	flagAdmin    commandFlag = "admin"
	flagPubSub   commandFlag = "pubsub"
	flagNoscript commandFlag = "noscript"
	flagDenyOOM  commandFlag = "denyoom"
)

type cmdInfo struct {
	name     string
	arity    int
	flags    []commandFlag
	firstKey int
	lastKey  int
	step     int
}

var commandRegistry []cmdInfo
var commandMap map[string]cmdInfo

func init() {
	commands := []cmdInfo{
		{name: "ACL", arity: -2, flags: []commandFlag{flagAdmin, flagNoscript, flagReadonly}},
		{name: "APPEND", arity: 3, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "ASKING", arity: 1, flags: []commandFlag{flagReadonly}},
		{name: "AUTH", arity: -2, flags: []commandFlag{flagNoscript, flagReadonly}},
		{name: "BGSAVE", arity: -1, flags: []commandFlag{flagAdmin}},
		{name: "BITCOUNT", arity: -2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "BITFIELD", arity: -2, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "BITFIELD_RO", arity: -2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "BITLEN", arity: 2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "BITOP", arity: -4, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 2, lastKey: -1, step: 1},
		{name: "BITPOS", arity: -3, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "BLMOVE", arity: 6, flags: []commandFlag{flagWrite, flagDenyOOM, flagNoscript}, firstKey: 1, lastKey: 2, step: 1},
		{name: "BLPOP", arity: -3, flags: []commandFlag{flagWrite, flagDenyOOM, flagNoscript}, firstKey: 1, lastKey: -2, step: 1},
		{name: "BRPOP", arity: -3, flags: []commandFlag{flagWrite, flagDenyOOM, flagNoscript}, firstKey: 1, lastKey: -2, step: 1},
		{name: "BLMPOP", arity: -5, flags: []commandFlag{flagWrite, flagDenyOOM, flagNoscript}},
		{name: "BRPOPLPUSH", arity: 4, flags: []commandFlag{flagWrite, flagDenyOOM, flagNoscript}, firstKey: 1, lastKey: 2, step: 1},
		{name: "BZPOPMAX", arity: -3, flags: []commandFlag{flagWrite, flagDenyOOM, flagNoscript}, firstKey: 1, lastKey: -2, step: 1},
		{name: "BZPOPMIN", arity: -3, flags: []commandFlag{flagWrite, flagDenyOOM, flagNoscript}, firstKey: 1, lastKey: -2, step: 1},
		{name: "CLIENT", arity: -2, flags: []commandFlag{flagAdmin, flagNoscript}},
		{name: "CLUSTER", arity: -2, flags: []commandFlag{flagAdmin, flagReadonly}},
		{name: "COMMAND", arity: -1, flags: []commandFlag{flagReadonly, flagNoscript}},
		{name: "CONFIG", arity: -2, flags: []commandFlag{flagAdmin, flagReadonly}},
		{name: "COPY", arity: -3, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 2, step: 1},
		{name: "DBSIZE", arity: 1, flags: []commandFlag{flagReadonly}},
		{name: "DEBUG", arity: -2, flags: []commandFlag{flagAdmin}},
		{name: "DECR", arity: 2, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "DECRBY", arity: 3, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "DEL", arity: -2, flags: []commandFlag{flagWrite}, firstKey: 1, lastKey: -1, step: 1},
		{name: "DISCARD", arity: 1, flags: []commandFlag{flagReadonly, flagNoscript}},
		{name: "DUMP", arity: 2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "ECHO", arity: 2, flags: []commandFlag{flagReadonly}},
		{name: "EXEC", arity: 1, flags: []commandFlag{flagNoscript, flagReadonly}},
		{name: "EXISTS", arity: -2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: -1, step: 1},
		{name: "EXPIRETIME", arity: 2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "EXPIRE", arity: -3, flags: []commandFlag{flagWrite}, firstKey: 1, lastKey: 1, step: 1},
		{name: "EXPIREAT", arity: -3, flags: []commandFlag{flagWrite}, firstKey: 1, lastKey: 1, step: 1},
		{name: "FLUSHALL", arity: -1, flags: []commandFlag{flagWrite, flagAdmin}},
		{name: "FLUSHDB", arity: -1, flags: []commandFlag{flagWrite, flagAdmin}},
		{name: "GEOADD", arity: -5, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "GEODIST", arity: -4, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "GEOHASH", arity: -2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "GEOPOS", arity: -2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "GEORADIUS", arity: -6, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "GEORADIUS_RO", arity: -6, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "GEORADIUSBYMEMBER", arity: -5, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "GEORADIUSBYMEMBER_RO", arity: -5, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "GEOSEARCH", arity: -7, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "GEOSEARCHSTORE", arity: -8, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 2, step: 1},
		{name: "GET", arity: 2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "GETBIT", arity: 3, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "GETDEL", arity: 2, flags: []commandFlag{flagWrite}, firstKey: 1, lastKey: 1, step: 1},
		{name: "GETEX", arity: -2, flags: []commandFlag{flagWrite}, firstKey: 1, lastKey: 1, step: 1},
		{name: "GETRANGE", arity: 4, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "SUBSTR", arity: 4, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "GETSET", arity: 3, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "HDEL", arity: -3, flags: []commandFlag{flagWrite}, firstKey: 1, lastKey: 1, step: 1},
		{name: "HELLO", arity: -1, flags: []commandFlag{flagReadonly}, firstKey: 0, lastKey: 0, step: 0},
		{name: "HEXISTS", arity: 3, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "HGET", arity: 3, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "HGETALL", arity: 2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "HINCRBY", arity: 4, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "HINCRBYFLOAT", arity: 4, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "HKEYS", arity: 2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "HLEN", arity: 2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "HMGET", arity: -3, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "HMSET", arity: -4, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "HRANDFIELD", arity: -2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "HRANDMEMBER", arity: -2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "HSCAN", arity: -3, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "HSET", arity: -4, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "HSETNX", arity: 4, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "HSTRLEN", arity: 3, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "HVALS", arity: 2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "INCR", arity: 2, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "INCRBY", arity: 3, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "INCRBYFLOAT", arity: 3, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "INFO", arity: -1, flags: []commandFlag{flagReadonly}},
		{name: "JSON.ARRAPPEND", arity: -4, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "JSON.ARRLEN", arity: -2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "JSON.CLEAR", arity: -2, flags: []commandFlag{flagWrite}, firstKey: 1, lastKey: 1, step: 1},
		{name: "JSON.DEBUG", arity: -3, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "JSON.DEL", arity: -2, flags: []commandFlag{flagWrite}, firstKey: 1, lastKey: 1, step: 1},
		{name: "JSON.GET", arity: -2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "JSON.MGET", arity: -3, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: -1, step: 1},
		{name: "JSON.NUMINCRBY", arity: 4, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "JSON.NUMMULTBY", arity: 4, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "JSON.OBJKEYS", arity: -2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "JSON.SET", arity: -4, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "JSON.TYPE", arity: -2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "KEYS", arity: 2, flags: []commandFlag{flagReadonly}},
		{name: "LASTSAVE", arity: 1, flags: []commandFlag{flagReadonly}},
		{name: "LATENCY", arity: -2, flags: []commandFlag{flagAdmin, flagReadonly}},
		{name: "LINDEX", arity: 3, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "LINSERT", arity: 5, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "LLEN", arity: 2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "LMOVE", arity: 5, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 2, step: 1},
		{name: "LOLWUT", arity: -1, flags: []commandFlag{flagReadonly}},
		{name: "LCS", arity: -3, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 2, step: 1},
		{name: "LPOP", arity: -2, flags: []commandFlag{flagWrite}, firstKey: 1, lastKey: 1, step: 1},
		{name: "LPOS", arity: -3, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "LPUSH", arity: -3, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "LPUSHX", arity: -3, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "LRANGE", arity: 4, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "LREM", arity: 4, flags: []commandFlag{flagWrite}, firstKey: 1, lastKey: 1, step: 1},
		{name: "LSET", arity: 4, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "LTRIM", arity: 4, flags: []commandFlag{flagWrite}, firstKey: 1, lastKey: 1, step: 1},
		{name: "MEMORY", arity: -2, flags: []commandFlag{flagReadonly}},
		{name: "MGET", arity: -2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: -1, step: 1},
		{name: "MIGRATE", arity: -6, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 3, lastKey: 3, step: 1},
		{name: "MODULE", arity: -2, flags: []commandFlag{flagAdmin, flagReadonly}},
		{name: "MONITOR", arity: 1, flags: []commandFlag{flagAdmin, flagNoscript}},
		{name: "MOVE", arity: 3, flags: []commandFlag{flagWrite}, firstKey: 1, lastKey: 1, step: 1},
		{name: "MSET", arity: -3, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: -1, step: 2},
		{name: "MSETNX", arity: -3, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: -1, step: 2},
		{name: "MULTI", arity: 1, flags: []commandFlag{flagReadonly, flagNoscript}},
		{name: "OBJECT", arity: -2, flags: []commandFlag{flagReadonly}},
		{name: "PERSIST", arity: 2, flags: []commandFlag{flagWrite}, firstKey: 1, lastKey: 1, step: 1},
		{name: "PEXPIRE", arity: -3, flags: []commandFlag{flagWrite}, firstKey: 1, lastKey: 1, step: 1},
		{name: "PEXPIREAT", arity: -3, flags: []commandFlag{flagWrite}, firstKey: 1, lastKey: 1, step: 1},
		{name: "PEXPIRETIME", arity: 2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "PFADD", arity: -2, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "PFCOUNT", arity: -2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: -1, step: 1},
		{name: "PFINFO", arity: 2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "PFMERGE", arity: -2, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: -1, step: 1},
		{name: "PING", arity: -1, flags: []commandFlag{flagReadonly}},
		{name: "PSETEX", arity: 4, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "PSUBSCRIBE", arity: -2, flags: []commandFlag{flagPubSub, flagNoscript, flagReadonly}},
		{name: "SSUBSCRIBE", arity: -2, flags: []commandFlag{flagPubSub, flagNoscript, flagReadonly}, firstKey: 1, lastKey: -1, step: 1},
		{name: "PTTL", arity: 2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "PUBLISH", arity: 3, flags: []commandFlag{flagPubSub, flagWrite}},
		{name: "SPUBLISH", arity: 3, flags: []commandFlag{flagPubSub, flagWrite}, firstKey: 1, lastKey: 1, step: 1},
		{name: "PUBSUB", arity: -2, flags: []commandFlag{flagPubSub, flagReadonly}},
		{name: "PUNSUBSCRIBE", arity: -1, flags: []commandFlag{flagPubSub, flagNoscript, flagReadonly}},
		{name: "SUNSUBSCRIBE", arity: -1, flags: []commandFlag{flagPubSub, flagNoscript, flagReadonly}, firstKey: 1, lastKey: -1, step: 1},
		{name: "QUIT", arity: -1, flags: []commandFlag{flagReadonly, flagNoscript}},
		{name: "RANDOMKEY", arity: 1, flags: []commandFlag{flagReadonly}},
		{name: "READONLY", arity: 1, flags: []commandFlag{flagReadonly}},
		{name: "READWRITE", arity: 1, flags: []commandFlag{flagReadonly}},
		{name: "RENAME", arity: 3, flags: []commandFlag{flagWrite}, firstKey: 1, lastKey: 2, step: 1},
		{name: "RENAMENX", arity: 3, flags: []commandFlag{flagWrite}, firstKey: 1, lastKey: 2, step: 1},
		{name: "REPLCONF", arity: -1, flags: []commandFlag{flagAdmin, flagReadonly}},
		{name: "REPLICAOF", arity: 3, flags: []commandFlag{flagAdmin}},
		{name: "RESTORE", arity: -4, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "ROLE", arity: 1, flags: []commandFlag{flagReadonly}},
		{name: "RPOP", arity: -2, flags: []commandFlag{flagWrite}, firstKey: 1, lastKey: 1, step: 1},
		{name: "RPOPLPUSH", arity: 3, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 2, step: 1},
		{name: "RPUSH", arity: -3, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "RPUSHX", arity: -3, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "SADD", arity: -3, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "SAVE", arity: 1, flags: []commandFlag{flagAdmin}},
		{name: "SCAN", arity: -2, flags: []commandFlag{flagReadonly}},
		{name: "SCARD", arity: 2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "SDIFF", arity: -2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: -1, step: 1},
		{name: "SDIFFSTORE", arity: -3, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: -1, step: 1},
		{name: "SELECT", arity: 2, flags: []commandFlag{flagReadonly, flagNoscript}},
		{name: "SET", arity: -3, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "SETBIT", arity: 4, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "SETEX", arity: 4, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "SETNX", arity: 3, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "SETRANGE", arity: 4, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "SHUTDOWN", arity: -1, flags: []commandFlag{flagAdmin}},
		{name: "SINTER", arity: -2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: -1, step: 1},
		{name: "SINTERCARD", arity: -3, flags: []commandFlag{flagReadonly}},
		{name: "SINTERSTORE", arity: -3, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: -1, step: 1},
		{name: "SISMEMBER", arity: 3, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "SLAVEOF", arity: 3, flags: []commandFlag{flagAdmin}},
		{name: "SLOWLOG", arity: -2, flags: []commandFlag{flagAdmin, flagReadonly}},
		{name: "SMEMBERS", arity: 2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "SMISMEMBER", arity: -3, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "SMOVE", arity: 4, flags: []commandFlag{flagWrite}, firstKey: 1, lastKey: 2, step: 1},
		{name: "SORT", arity: -2, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "SORT_RO", arity: -2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "SPOP", arity: -2, flags: []commandFlag{flagWrite}, firstKey: 1, lastKey: 1, step: 1},
		{name: "SRANDMEMBER", arity: -2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "SREM", arity: -3, flags: []commandFlag{flagWrite}, firstKey: 1, lastKey: 1, step: 1},
		{name: "SSCAN", arity: -3, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "STRLEN", arity: 2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "SUBSCRIBE", arity: -2, flags: []commandFlag{flagPubSub, flagNoscript, flagReadonly}},
		{name: "SUNION", arity: -2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: -1, step: 1},
		{name: "SUNIONSTORE", arity: -3, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: -1, step: 1},
		{name: "SWAPDB", arity: 3, flags: []commandFlag{flagWrite}},
		{name: "TIME", arity: 1, flags: []commandFlag{flagReadonly}},
		{name: "TOUCH", arity: -2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: -1, step: 1},
		{name: "TS.ADD", arity: -4, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "TS.CREATE", arity: -4, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "TS.DEL", arity: 3, flags: []commandFlag{flagWrite}, firstKey: 1, lastKey: 1, step: 1},
		{name: "TS.GET", arity: 2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "TS.INFO", arity: 2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "TS.LEN", arity: 2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "TS.MGET", arity: -3, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: -1, step: 1},
		{name: "TS.MADD", arity: -4, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: -1, step: 3},
		{name: "TS.MRANGE", arity: -5, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: -1, step: 1},
		{name: "TS.MREVRANGE", arity: -5, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: -1, step: 1},
		{name: "TS.QUERYINDEX", arity: -2, flags: []commandFlag{flagReadonly}, firstKey: 0, lastKey: 0, step: 0},
		{name: "TS.INCRBY", arity: -3, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "TS.CREATERULE", arity: 6, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 2, step: 1},
		{name: "TS.DELETERULE", arity: 6, flags: []commandFlag{flagWrite}, firstKey: 1, lastKey: 2, step: 1},
		{name: "TS.RANGE", arity: -5, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "TS.REVRANGE", arity: -5, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "TTL", arity: 2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "TYPE", arity: 2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "UNSUBSCRIBE", arity: -1, flags: []commandFlag{flagPubSub, flagNoscript, flagReadonly}},
		{name: "UNLINK", arity: -2, flags: []commandFlag{flagWrite}, firstKey: 1, lastKey: -1, step: 1},
		{name: "UNWATCH", arity: 1, flags: []commandFlag{flagReadonly, flagNoscript}},
		{name: "WAIT", arity: 3, flags: []commandFlag{flagReadonly, flagNoscript}},
		{name: "WATCH", arity: -2, flags: []commandFlag{flagReadonly, flagNoscript}, firstKey: 1, lastKey: -1, step: 1},
		{name: "XACK", arity: -4, flags: []commandFlag{flagWrite}, firstKey: 1, lastKey: 1, step: 1},
		{name: "XACKDEL", arity: -6, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "XADD", arity: -5, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "XAUTOCLAIM", arity: -6, flags: []commandFlag{flagWrite}, firstKey: 1, lastKey: 1, step: 1},
		{name: "XCLAIM", arity: -6, flags: []commandFlag{flagWrite}, firstKey: 1, lastKey: 1, step: 1},
		{name: "XCFGSET", arity: -3, flags: []commandFlag{flagWrite}, firstKey: 1, lastKey: 1, step: 1},
		{name: "XDEL", arity: -3, flags: []commandFlag{flagWrite}, firstKey: 1, lastKey: 1, step: 1},
		{name: "XDELEX", arity: -5, flags: []commandFlag{flagWrite}, firstKey: 1, lastKey: 1, step: 1},
		{name: "XGROUP", arity: -2, flags: []commandFlag{flagWrite}},
		{name: "XINFO", arity: -2, flags: []commandFlag{flagReadonly}},
		{name: "XLEN", arity: 2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "XNACK", arity: -4, flags: []commandFlag{flagWrite}, firstKey: 1, lastKey: 1, step: 1},
		{name: "XPENDING", arity: -3, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "XRANGE", arity: -4, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "XREAD", arity: -4, flags: []commandFlag{flagReadonly}},
		{name: "XREADGROUP", arity: -7, flags: []commandFlag{flagReadonly}},
		{name: "XREVRANGE", arity: -4, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "XSETID", arity: -3, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "XTRIM", arity: -4, flags: []commandFlag{flagWrite}, firstKey: 1, lastKey: 1, step: 1},
		{name: "ZADD", arity: -4, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "ZCARD", arity: 2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "ZCOUNT", arity: 4, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "ZDIFF", arity: -3, flags: []commandFlag{flagReadonly}},
		{name: "ZDIFFSTORE", arity: -4, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "ZINCRBY", arity: 4, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "ZINTER", arity: -3, flags: []commandFlag{flagReadonly}},
		{name: "ZINTERCARD", arity: -3, flags: []commandFlag{flagReadonly}},
		{name: "ZINTERSTORE", arity: -4, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
		{name: "ZLEXCOUNT", arity: 4, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "LMPOP", arity: -4, flags: []commandFlag{flagWrite, flagDenyOOM}},
		{name: "ZMPOP", arity: -4, flags: []commandFlag{flagWrite, flagDenyOOM}},
		{name: "BZMPOP", arity: -5, flags: []commandFlag{flagWrite, flagDenyOOM, flagNoscript}},
		{name: "ZMSCORE", arity: -3, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "ZPOPMAX", arity: -2, flags: []commandFlag{flagWrite}, firstKey: 1, lastKey: 1, step: 1},
		{name: "ZPOPMIN", arity: -2, flags: []commandFlag{flagWrite}, firstKey: 1, lastKey: 1, step: 1},
		{name: "ZRANDMEMBER", arity: -2, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "ZRANGE", arity: -4, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "ZRANGEBYLEX", arity: -4, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "ZRANGEBYSCORE", arity: -4, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "ZRANGESTORE", arity: -5, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 2, step: 1},
		{name: "ZRANK", arity: -3, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "ZREM", arity: -3, flags: []commandFlag{flagWrite}, firstKey: 1, lastKey: 1, step: 1},
		{name: "ZREMRANGEBYLEX", arity: 4, flags: []commandFlag{flagWrite}, firstKey: 1, lastKey: 1, step: 1},
		{name: "ZREMRANGEBYRANK", arity: 4, flags: []commandFlag{flagWrite}, firstKey: 1, lastKey: 1, step: 1},
		{name: "ZREMRANGEBYSCORE", arity: 4, flags: []commandFlag{flagWrite}, firstKey: 1, lastKey: 1, step: 1},
		{name: "ZREVRANGE", arity: -4, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "ZREVRANGEBYLEX", arity: -4, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "ZREVRANGEBYSCORE", arity: -4, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "ZREVRANK", arity: -3, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "ZSCAN", arity: -3, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "ZSCORE", arity: 3, flags: []commandFlag{flagReadonly}, firstKey: 1, lastKey: 1, step: 1},
		{name: "ZUNION", arity: -3, flags: []commandFlag{flagReadonly}},
		{name: "ZUNIONSTORE", arity: -4, flags: []commandFlag{flagWrite, flagDenyOOM}, firstKey: 1, lastKey: 1, step: 1},
	}

	commandRegistry = commands
	commandMap = make(map[string]cmdInfo, len(commands))
	for _, c := range commands {
		commandMap[c.name] = c
	}
	sort.Slice(commandRegistry, func(i, j int) bool {
		return commandRegistry[i].name < commandRegistry[j].name
	})
}

func buildCommandInfo(c cmdInfo) proto.RESP {
	nameResp := proto.NewBulkString([]byte(c.name))
	arityResp := proto.NewInteger(int64(c.arity))
	flags := make([][]byte, len(c.flags))
	for i, f := range c.flags {
		flags[i] = []byte(f)
	}
	flagsResp := &proto.Array{Args: flags}
	firstKeyResp := proto.NewInteger(int64(c.firstKey))
	lastKeyResp := proto.NewInteger(int64(c.lastKey))
	stepResp := proto.NewInteger(int64(c.step))
	return &proto.NestedArray{
		Elems: []proto.RESP{
			nameResp, arityResp, flagsResp,
			firstKeyResp, lastKeyResp, stepResp,
		},
	}
}

// buildCommandDoc 构建 COMMAND DOCS 格式的命令文档（Redis 8 语义）。
// 返回 doc 部分（[field, value, ...] 数组）；name 由 handleCommand 的
// DOCS 分支单独 append（输出为 [name, doc, name, doc, ...] 扁平数组）。
func buildCommandDoc(c cmdInfo) proto.RESP {
	flagArgs := make([][]byte, len(c.flags))
	for i, f := range c.flags {
		flagArgs[i] = []byte(f)
	}
	return &proto.NestedArray{
		Elems: []proto.RESP{
			proto.NewBulkString([]byte("summary")),
			proto.NewBulkString([]byte("")),
			proto.NewBulkString([]byte("since")),
			proto.NewBulkString([]byte("1.0.0")),
			proto.NewBulkString([]byte("group")),
			proto.NewBulkString([]byte(commandGroup(c))),
			proto.NewBulkString([]byte("arity")),
			proto.NewInteger(int64(c.arity)),
			proto.NewBulkString([]byte("flags")),
			&proto.Array{Args: flagArgs},
		},
	}
}

// commandGroup 根据 flags 推断命令所属组（Redis COMMAND DOCS 的 group 字段）。
func commandGroup(c cmdInfo) string {
	for _, f := range c.flags {
		switch f {
		case flagAdmin:
			return "server"
		case flagPubSub:
			return "pubsub"
		case flagWrite:
			return "generic"
		}
	}
	return "generic"
}

func handleCommand(args [][]byte) proto.RESP {
	if len(args) == 0 {
		infos := make([]proto.RESP, len(commandRegistry))
		for i, c := range commandRegistry {
			infos[i] = buildCommandInfo(c)
		}
		return &proto.NestedArray{Elems: infos}
	}

	sub := strings.ToUpper(string(args[0]))
	switch sub {
	case "COUNT":
		return proto.NewInteger(int64(len(commandRegistry)))

	case "DOCS":
		// COMMAND DOCS [command-name ...]：无参返回所有命令文档
		if len(args) == 1 {
			docs := make([]proto.RESP, 0, len(commandRegistry)*2)
			for _, c := range commandRegistry {
				docs = append(docs, proto.NewBulkString([]byte(c.name)), buildCommandDoc(c))
			}
			return &proto.NestedArray{Elems: docs}
		}
		docs := make([]proto.RESP, 0, len(args)*2)
		for _, nameBytes := range args[1:] {
			name := strings.ToUpper(string(nameBytes))
			if c, ok := commandMap[name]; ok {
				docs = append(docs, proto.NewBulkString([]byte(c.name)), buildCommandDoc(c))
			}
		}
		return &proto.NestedArray{Elems: docs}

	case "INFO":
		if len(args) == 1 {
			return proto.NilArray{}
		}
		infos := make([]proto.RESP, 0, len(args)-1)
		for _, nameBytes := range args[1:] {
			name := strings.ToUpper(string(nameBytes))
			if c, ok := commandMap[name]; ok {
				infos = append(infos, buildCommandInfo(c))
			} else {
				infos = append(infos, proto.NilArray{})
			}
		}
		return &proto.NestedArray{Elems: infos}

	case "GETKEYS":
		// COMMAND GETKEYS cmd arg... — 返回命令涉及的 key 参数
		if len(args) < 2 {
			return proto.NewError("ERR Unknown subcommand or wrong number of arguments for 'GETKEYS'. Try COMMAND HELP.")
		}
		cmdName := strings.ToUpper(string(args[1]))
		c, ok := commandMap[cmdName]
		if !ok {
			return proto.NewError("ERR Invalid command specified")
		}
		if c.firstKey == 0 {
			return proto.NewError("ERR The command has no key arguments")
		}
		cmdArgs := args[2:]
		first := c.firstKey
		last := c.lastKey
		if first < 0 {
			first = len(cmdArgs) + first + 1
		}
		if last < 0 {
			last = len(cmdArgs) + last + 1
		}
		keys := make([][]byte, 0, (last-first)/c.step+1)
		for i := first - 1; i < last && i < len(cmdArgs); i += c.step {
			if i >= 0 {
				keys = append(keys, cmdArgs[i])
			}
		}
		return &proto.Array{Args: keys}

	case "LIST":
		// COMMAND LIST [FILTERBY PATTERN pattern] — 返回命令名列表
		names := make([][]byte, 0, len(commandRegistry))
		for _, c := range commandRegistry {
			names = append(names, []byte(c.name))
		}
		return &proto.Array{Args: names}

	case "HELP":
		return &proto.Array{Args: [][]byte{
			[]byte("COMMAND <subcommand> [<arg> ...]. Subcommands are:"),
			[]byte("COUNT"),
			[]byte("DOCS [<command-name> ...]"),
			[]byte("GETKEYS <full-command>"),
			[]byte("HELP"),
			[]byte("INFO [<command-name> ...]"),
			[]byte("LIST"),
		}}

	default:
		return proto.NewError(fmt.Sprintf("ERR unknown subcommand '%s'", sub))
	}
}
