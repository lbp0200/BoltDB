package server

import (
	"fmt"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/replication"
)

// allDispatchCommands 包含 handler_dispatch.go switch 中所有已注册的命令。
// 用于启动时校验 isWriteCommand 一致性。
var allDispatchCommands = map[string]bool{
	"PING": true, "COMMAND": true, "QUIT": true, "ROLE": true, "ECHO": true,
	"ACL": true, "CLIENT": true, "AUTH": true, "HELLO": true,
	"SET": true, "GET": true, "GETDEL": true, "GETEX": true,
	"SETEX": true, "PSETEX": true, "SETNX": true, "GETSET": true,
	"MGET": true, "MSET": true, "MSETNX": true,
	"INCR": true, "INCRBY": true, "DECR": true, "DECRBY": true,
	"INCRBYFLOAT": true, "APPEND": true, "STRLEN": true,
	"SETBIT": true, "GETBIT": true, "BITCOUNT": true, "BITOP": true,
	"BITFIELD": true, "BITFIELD_RO": true, "BITPOS": true, "BITLEN": true,
	"GETRANGE": true, "SETRANGE": true,
	"UNLINK": true, "DEL": true, "EXISTS": true,
	"PFADD": true, "PFCOUNT": true, "PFMERGE": true, "PFINFO": true,
	"TYPE": true, "DUMP": true, "RESTORE": true, "OBJECT": true,
	"EXPIRE": true, "EXPIREAT": true, "PEXPIRE": true, "PEXPIREAT": true,
	"TTL": true, "PTTL": true, "EXPIRETIME": true, "PEXPIRETIME": true, "PERSIST": true,
	"RENAME": true, "RENAMENX": true, "COPY": true, "SWAPDB": true,
	"TOUCH": true, "SHUTDOWN": true,
	"KEYS": true, "SCAN": true, "RANDOMKEY": true,
	"LPUSH": true, "RPUSH": true, "LPOP": true, "RPOP": true,
	"LLEN": true, "LINDEX": true, "LRANGE": true, "LSET": true,
	"LTRIM": true, "LINSERT": true, "LPOS": true, "LCS": true, "LREM": true,
	"RPOPLPUSH": true, "LMOVE": true, "BLMOVE": true,
	"LPUSHX": true, "RPUSHX": true, "BLPOP": true, "BRPOP": true, "BRPOPLPUSH": true,
	"HSET": true, "HGET": true, "HDEL": true, "HLEN": true,
	"HGETALL": true, "HEXISTS": true, "HKEYS": true, "HVALS": true,
	"HMSET": true, "HMGET": true, "HSETNX": true,
	"HINCRBY": true, "HINCRBYFLOAT": true, "HSTRLEN": true,
	"HRANDFIELD": true, "HRANDMEMBER": true,
	"SADD": true, "SREM": true, "SCARD": true, "SISMEMBER": true,
	"SMEMBERS": true, "SPOP": true, "SRANDMEMBER": true, "SMOVE": true,
	"SINTER": true, "SUNION": true, "SDIFF": true, "SINTERSTORE": true,
	"SMISMEMBER": true, "SINTERCARD": true, "SUNIONSTORE": true, "SDIFFSTORE": true, "SSCAN": true,
	"HSCAN": true,
	"ZADD":  true, "ZREM": true, "ZREMRANGEBYRANK": true, "ZREMRANGEBYSCORE": true,
	"ZPOPMAX": true, "ZPOPMIN": true, "BZPOPMAX": true, "BZPOPMIN": true,
	"ZCARD": true, "ZSCORE": true, "ZRANK": true, "ZREVRANK": true,
	"ZCOUNT": true, "ZMSCORE": true,
	"ZRANGE": true, "ZREVRANGE": true, "ZRANGEBYSCORE": true, "ZREVRANGEBYSCORE": true,
	"ZINCRBY": true, "ZRANDMEMBER": true,
	"LMPOP": true, "ZMPOP": true, "BZMPOP": true,
	"ZUNIONSTORE": true, "ZINTERSTORE": true, "ZDIFFSTORE": true,
	"ZDIFF": true, "ZINTER": true, "ZINTERCARD": true, "ZUNION": true,
	"ZLEXCOUNT": true, "ZRANGEBYLEX": true, "ZREVRANGEBYLEX": true,
	"ZREMRANGEBYLEX": true, "ZSCAN": true,
	"ASKING": true, "CLUSTER": true, "CONFIG": true,
	"REPLICAOF": true, "SLAVEOF": true, "REPLCONF": true, "INFO": true,
	"SAVE": true, "BGSAVE": true, "LASTSAVE": true, "DBSIZE": true, "TIME": true,
	"FLUSHDB": true, "FLUSHALL": true, "SELECT": true, "MOVE": true,
	"WAIT": true, "SLOWLOG": true, "MEMORY": true, "MODULE": true,
	"LOLWUT": true, "LATENCY": true, "READONLY": true, "READWRITE": true,
	"ZRANGESTORE": true, "PUBLISH": true,
	"SUBSCRIBE": true, "PSUBSCRIBE": true, "UNSUBSCRIBE": true,
	"PUNSUBSCRIBE": true, "PUBSUB": true,
	"MULTI": true, "EXEC": true, "DISCARD": true, "WATCH": true, "UNWATCH": true,
	"GEOADD": true, "GEOPOS": true, "GEOHASH": true, "GEODIST": true,
	"GEORADIUS": true, "GEOSEARCH": true, "GEOSEARCHSTORE": true,
	"XADD": true, "XLEN": true, "XREAD": true, "XRANGE": true, "XREVRANGE": true,
	"XDEL": true, "XACK": true, "XACKDEL": true, "XDELEX": true,
	"XNACK": true, "XSETID": true, "XCFGSET": true, "XGROUP": true,
	"XREADGROUP": true, "XCLAIM": true, "XAUTOCLAIM": true,
	"XPENDING": true, "XINFO": true, "XTRIM": true,
	"SORT":     true,
	"JSON.SET": true, "JSON.GET": true, "JSON.DEL": true, "JSON.TYPE": true,
	"JSON.MGET": true, "JSON.ARRAPPEND": true, "JSON.ARRLEN": true,
	"JSON.OBJKEYS": true, "JSON.NUMINCRBY": true, "JSON.NUMMULTBY": true,
	"JSON.CLEAR": true, "JSON.DEBUG": true,
	"TS.CREATE": true, "TS.ADD": true, "TS.GET": true, "TS.RANGE": true,
	"TS.DEL": true, "TS.INFO": true, "TS.LEN": true,
	"TS.MGET": true, "TS.REVRANGE": true, "TS.MRANGE": true, "TS.MREVRANGE": true,
	"TS.QUERYINDEX": true, "TS.MADD": true, "TS.INCRBY": true,
	"TS.CREATERULE": true, "TS.DELETERULE": true,
	"MIGRATE": true, "DEBUG": true, "MONITOR": true,
}

// ValidateWriteCommandConsistency 启动时校验 isWriteCommand 与 dispatch switch 一致性。
// 如果 isWriteCommand 中的命令在 dispatch switch 中不存在，说明该命令永远不会被复制——panic。
func ValidateWriteCommandConsistency() error {
	writeCmds := getWriteCommandSet()
	var missing []string
	for cmd := range writeCmds {
		if !allDispatchCommands[cmd] {
			missing = append(missing, cmd)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("isWriteCommand contains commands not in dispatch switch (will never be replicated): %v", missing)
	}
	return nil
}

// isWriteCommand 检查是否是写命令
func isWriteCommand(cmd string) bool {
	return getWriteCommandSet()[cmd]
}

// shouldPropagateCommand reports whether a successful write should enter the
// replication stream via processRequest/EXEC.
//
// Excluded:
//   - control/admin commands that are never stream-replicated
//   - SPOP: handler canonicalizes to SREM of the actual members (single path)
//   - commands in replication.ReplicatedCommandsExcluded (no slave handler yet,
//     or external side effects) — propagating them causes FULLRESYNC thrash
func shouldPropagateCommand(cmd string) bool {
	switch cmd {
	case "REPLICAOF", "PSYNC", "REPLCONF", "SPOP":
		return false
	}
	if _, excluded := replication.IsReplicationExcluded(cmd); excluded {
		return false
	}
	return true
}

// isErrorResponse reports whether resp is a Redis error reply.
// Failed master writes must not enter the backlog (see processRequest).
func isErrorResponse(resp proto.RESP) bool {
	if resp == nil {
		return true
	}
	switch resp.(type) {
	case *proto.Error, proto.Error:
		return true
	default:
		return false
	}
}

// isPositiveIntegerResp reports whether resp is a RESP integer reply with value
// 1 (a "successfully wrote" signal). Conditional EXPIRE/PEXPIRE (NX/XX/GT/LT)
// return 0 when the condition rejects the write; in that case the master did
// NOT set a TTL and must not propagate the canonicalized PEXPIREAT (which would
// force the rejected absolute expiry onto the replica).
func isPositiveIntegerResp(resp proto.RESP) bool {
	if resp == nil {
		return false
	}
	if intResp, ok := resp.(*proto.Integer); ok {
		return int64(*intResp) == 1
	}
	return false
}

// getWriteCommandSet 返回写命令集合（与 isWriteCommand 相同）
func getWriteCommandSet() map[string]bool {
	return map[string]bool{
		"SET": true, "SETEX": true, "PSETEX": true, "SETNX": true,
		"GETSET": true, "MSET": true, "MSETNX": true,
		"INCR": true, "INCRBY": true, "DECR": true, "DECRBY": true,
		"INCRBYFLOAT": true, "APPEND": true, "SETRANGE": true, "SETBIT": true,
		"GETDEL": true, "GETEX": true,
		"BITOP": true, "BITFIELD": true,
		"COPY": true,
		"DEL":  true, "UNLINK": true, "EXPIRE": true, "EXPIREAT": true,
		"PEXPIRE": true, "PEXPIREAT": true, "PERSIST": true,
		"RENAME": true, "RENAMENX": true,
		"LPUSH": true, "RPUSH": true, "LPOP": true, "RPOP": true,
		"LSET": true, "LTRIM": true, "LINSERT": true, "LREM": true,
		"RPOPLPUSH": true, "LPUSHX": true, "RPUSHX": true,
		"LMOVE": true, "BLMOVE": true, "BRPOPLPUSH": true,
		"BLPOP": true, "BRPOP": true, "BLMPOP": true,
		"HSET": true, "HDEL": true, "HMSET": true, "HSETNX": true,
		"HINCRBY": true, "HINCRBYFLOAT": true,
		"SADD": true, "SREM": true, "SMOVE": true, "SPOP": true,
		"SINTERSTORE": true, "SUNIONSTORE": true, "SDIFFSTORE": true,
		"ZADD": true, "ZREM": true, "ZINCRBY": true,
		"ZPOPMAX": true, "ZPOPMIN": true, "ZMPOP": true, "BZMPOP": true, "LMPOP": true,
		"ZREMRANGEBYRANK": true, "ZREMRANGEBYSCORE": true, "ZREMRANGEBYLEX": true,
		"ZUNIONSTORE": true, "ZINTERSTORE": true, "ZDIFFSTORE": true,
		"ZRANGESTORE": true,
		"PFADD":       true, "PFMERGE": true,
		"GEOADD": true, "GEOSEARCHSTORE": true,
		"XADD": true, "XDEL": true, "XACK": true,
		"XCLAIM": true, "XGROUP": true, "XTRIM": true,
		// XREADGROUP mutates PEL / LastDeliveredID — must replicate or XCLAIM/XACK diverge
		"XREADGROUP": true,
		"XACKDEL":    true, "XDELEX": true, "XNACK": true, "XSETID": true, "XCFGSET": true,
		"SWAPDB": true, "MOVE": true,
		"PUBLISH":  true,
		"JSON.SET": true, "JSON.DEL": true, "JSON.ARRAPPEND": true,
		"JSON.NUMINCRBY": true, "JSON.NUMMULTBY": true, "JSON.CLEAR": true,
		"TS.CREATE": true, "TS.ADD": true, "TS.DEL": true,
		"TS.MADD": true, "TS.INCRBY": true, "TS.CREATERULE": true, "TS.DELETERULE": true,
		"RESTORE": true, "MIGRATE": true, "FLUSHDB": true, "FLUSHALL": true,
		"XAUTOCLAIM": true, "SORT": true,
		"BZPOPMAX": true, "BZPOPMIN": true,
	}
}
