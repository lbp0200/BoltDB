package replication

// ReplicatedCommands 记录了 executeReplicatedCommand 中可处理的写命令。
// 用于与 server.isWriteCommand 进行对称性校验。
//
// MIGRATE / PUBLISH 被有意排除 — 见 ReplicatedCommandsExcluded。
var ReplicatedCommands = map[string]bool{
	"APPEND":           true,
	"BITFIELD":         true,
	"BITOP":            true,
	"BLMOVE":           true,
	"BLPOP":            true,
	"BRPOP":            true,
	"BRPOPLPUSH":       true,
	"BZMPOP":           true,
	"BZPOPMAX":         true,
	"BZPOPMIN":         true,
	"COPY":             true,
	"DECR":             true,
	"DECRBY":           true, // 与 DECR 共用 case
	"DEL":              true,
	"UNLINK":           true, // 与 DEL 共用 case（语义等价）
	"EXPIRE":           true,
	"EXPIREAT":         true,
	"FLUSHDB":          true,
	"FLUSHALL":         true, // 与 FLUSHDB 共用 case
	"GEOADD":           true,
	"GEOSEARCHSTORE":   true,
	"GETDEL":           true,
	"GETEX":            true,
	"GETSET":           true,
	"HDEL":             true,
	"HINCRBY":          true,
	"HINCRBYFLOAT":     true,
	"HMSET":            true,
	"HSET":             true,
	"HSETNX":           true,
	"INCR":             true,
	"INCRBY":           true, // 与 INCR 共用 case
	"INCRBYFLOAT":      true,
	"JSON.ARRAPPEND":   true,
	"JSON.CLEAR":       true,
	"JSON.DEL":         true,
	"JSON.NUMINCRBY":   true,
	"JSON.NUMMULTBY":   true,
	"JSON.SET":         true,
	"LINSERT":          true,
	"LMOVE":            true,
	"LMPOP":            true,
	"LPOP":             true,
	"LPUSH":            true,
	"LPUSHX":           true,
	"LREM":             true,
	"LSET":             true,
	"LTRIM":            true,
	"MSET":             true,
	"MSETNX":           true, // 与 MSET 共用 case
	"PERSIST":          true,
	"PEXPIRE":          true,
	"PEXPIREAT":        true,
	"PFADD":            true,
	"PFMERGE":          true,
	"PSETEX":           true,
	"RENAME":           true,
	"RENAMENX":         true,
	"RESTORE":          true,
	"RPOP":             true,
	"RPOPLPUSH":        true,
	"RPUSH":            true,
	"RPUSHX":           true,
	"SADD":             true,
	"SDIFFSTORE":       true,
	"SET":              true,
	"SETBIT":           true,
	"SETEX":            true,
	"SETNX":            true,
	"SETRANGE":         true,
	"SINTERSTORE":      true,
	"SMOVE":            true,
	"SORT":             true,
	"SPOP":             true,
	"SREM":             true,
	"SUNIONSTORE":      true,
	"MOVE":             true, // single-DB no-op
	"SWAPDB":           true, // single-DB no-op
	"TS.ADD":           true,
	"TS.CREATE":        true,
	"TS.CREATERULE":    true,
	"TS.DEL":           true,
	"TS.DELETERULE":    true,
	"TS.INCRBY":        true,
	"TS.MADD":          true,
	"XACK":             true,
	"XACKDEL":          true,
	"XADD":             true,
	"XAUTOCLAIM":       true,
	"XCFGSET":          true, // no-op wire compat
	"XCLAIM":           true,
	"XDEL":             true,
	"XDELEX":           true,
	"XGROUP":           true,
	"XREADGROUP":       true, // mutates PEL; without this live XCLAIM/XACK diverge
	"XNACK":            true,
	"XSETID":           true,
	"XTRIM":            true,
	"ZADD":             true,
	"ZDIFFSTORE":       true,
	"ZINCRBY":          true,
	"ZINTERSTORE":      true,
	"ZMPOP":            true,
	"ZPOPMAX":          true,
	"ZPOPMIN":          true,
	"ZRANGESTORE":      true,
	"ZREM":             true,
	"ZREMRANGEBYLEX":   true,
	"ZREMRANGEBYRANK":  true,
	"ZREMRANGEBYSCORE": true,
	"ZUNIONSTORE":      true,
}

// ReplicatedCommandsExcluded 记录了属于 isWriteCommand 但有意不在
// executeReplicatedCommand 中处理的命令及原因。这些命令不应导致对称性测试失败。
//
// processRequest / EXEC 通过 shouldPropagateCommand → IsReplicationExcluded
// 阻止这些命令进入 backlog。
var ReplicatedCommandsExcluded = map[string]string{
	"MIGRATE": "外部副作用（RESTORE on target），不应由主节点传播",
	"PUBLISH": "handler 级操作（PubSub），executeReplicatedCommand 只有 store 无 PubSub 实例",
}

// ValidateReplicationMapping 校验 cmd 是否在 ReplicatedCommands 中。
func ValidateReplicationMapping(cmd string) bool {
	return ReplicatedCommands[cmd]
}

// IsReplicationExcluded 返回 cmd 是否被有意排除，以及排除原因。
func IsReplicationExcluded(cmd string) (reason string, excluded bool) {
	reason, excluded = ReplicatedCommandsExcluded[cmd]
	return
}
