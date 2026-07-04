package replication

// ReplicatedCommands 记录了 executeReplicatedCommand 中可处理的写命令。
// 用于与 server.isWriteCommand 进行对称性校验。
//
// 已知未实现但属于 isWriteCommand 的命令（TODO: 逐步补齐）：
//
//	BZMPOP, MOVE, SWAPDB, XACKDEL, XCFGSET, XDELEX, XNACK, XSETID,
//	TS.CREATERULE, TS.DELETERULE, TS.INCRBY, TS.MADD
//
// MIGRATE 被有意排除 — 它在目标节点上有外部副作用（RESTORE），
// 不应由主节点传播到从节点，见 internal/server/handler_core.go:599-601。
var ReplicatedCommands = map[string]bool{
	"APPEND":           true,
	"BITFIELD":         true,
	"BITOP":            true,
	"BLMOVE":           true,
	"BLPOP":            true,
	"BRPOP":            true,
	"BRPOPLPUSH":       true,
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
	"TS.ADD":           true,
	"TS.CREATE":        true,
	"TS.DEL":           true,
	"XACK":             true,
	"XADD":             true,
	"XAUTOCLAIM":       true,
	"XCLAIM":           true,
	"XDEL":             true,
	"XGROUP":           true,
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
var ReplicatedCommandsExcluded = map[string]string{
	"MIGRATE":       "外部副作用（RESTORE on target），不应由主节点传播",
	"PUBLISH":       "handler 级操作（PubSub），executeReplicatedCommand 只有 store 无 PubSub 实例",
	"BZMPOP":        "阻塞变体，复制路径不做阻塞操作（同 BZPOPMAX/BZPOPMIN 仅做非阻塞等价）",
	"MOVE":          "DB 级操作，复制语义不清晰",
	"SWAPDB":        "DB 级操作，复制语义不清晰",
	"XACKDEL":       "BoltDB 扩展命令，暂未实现复制",
	"XCFGSET":       "BoltDB 扩展命令，暂未实现复制",
	"XDELEX":        "BoltDB 扩展命令，暂未实现复制",
	"XNACK":         "BoltDB 扩展命令，暂未实现复制",
	"XSETID":        "BoltDB 扩展命令，暂未实现复制",
	"TS.CREATERULE": "BoltDB 扩展命令，暂未实现复制",
	"TS.DELETERULE": "BoltDB 扩展命令，暂未实现复制",
	"TS.INCRBY":     "BoltDB 扩展命令，暂未实现复制",
	"TS.MADD":       "BoltDB 扩展命令，暂未实现复制",
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
