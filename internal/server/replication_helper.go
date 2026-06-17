package server

// isWriteCommand 检查是否是写命令
func isWriteCommand(cmd string) bool {
	writeCommands := map[string]bool{
		"SET": true, "SETEX": true, "PSETEX": true, "SETNX": true,
		"GETSET": true, "MSET": true, "MSETNX": true,
		"INCR": true, "INCRBY": true, "DECR": true, "DECRBY": true,
		"INCRBYFLOAT": true, "APPEND": true, "SETRANGE": true, "SETBIT": true,
		"GETDEL": true, "GETEX": true,
		"BITOP": true, "BITFIELD": true,
		"COPY": true,
		"DEL":  true, "EXPIRE": true, "EXPIREAT": true,
		"PEXPIRE": true, "PEXPIREAT": true, "PERSIST": true,
		"RENAME": true, "RENAMENX": true,
		"LPUSH": true, "RPUSH": true, "LPOP": true, "RPOP": true,
		"LSET": true, "LTRIM": true, "LINSERT": true, "LREM": true,
		"RPOPLPUSH": true, "LPUSHX": true, "RPUSHX": true,
		"LMOVE": true, "BLMOVE": true, "BRPOPLPUSH": true,
		"BLPOP": true, "BRPOP": true,
		"HSET": true, "HDEL": true, "HMSET": true, "HSETNX": true,
		"HINCRBY": true, "HINCRBYFLOAT": true,
		"SADD": true, "SREM": true, "SMOVE": true,
		"SINTERSTORE": true, "SUNIONSTORE": true, "SDIFFSTORE": true,
		"ZADD": true, "ZREM": true, "ZINCRBY": true,
		"ZPOPMAX": true, "ZPOPMIN": true, "ZMPOP": true, "LMPOP": true,
		"ZREMRANGEBYRANK": true, "ZREMRANGEBYSCORE": true, "ZREMRANGEBYLEX": true,
		"ZUNIONSTORE": true, "ZINTERSTORE": true, "ZDIFFSTORE": true,
		"ZRANGESTORE": true,
		"PFADD":       true, "PFMERGE": true,
		// GEO commands
		"GEOADD": true, "GEOSEARCHSTORE": true,
		// Stream commands
		"XADD": true, "XDEL": true, "XACK": true,
		"XCLAIM": true, "XGROUP": true, "XTRIM": true,
		// JSON commands
		"JSON.SET": true, "JSON.DEL": true, "JSON.ARRAPPEND": true,
		"JSON.NUMINCRBY": true, "JSON.NUMMULTBY": true, "JSON.CLEAR": true,
		// TimeSeries commands
		"TS.CREATE": true, "TS.ADD": true, "TS.DEL": true,
		// P1.5 — 修复复制数据丢失（2026-06-15）
		"RESTORE":    true,
		"FLUSHDB":    true,
		"FLUSHALL":   true,
		"XAUTOCLAIM": true,
		"SORT":       true,
		// 2026-06-11: BZPOPMAX/BZPOPMIN 修复 — 阻塞式排序集合弹出未复制
		"BZPOPMAX": true,
		"BZPOPMIN": true,
	}
	return writeCommands[cmd]
}
