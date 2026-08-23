package server

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/lbp0200/BoltDB/internal/logger"
	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/replication"
)

// pauseExempt 报告命令是否在 CLIENT PAUSE 窗口内豁免（不被阻塞等待）。
// 连接/认证/管理类命令必须豁免，否则 CLIENT UNPAUSE 自身无法执行、
// 心跳和认证也会被卡死。
func (h *Handler) pauseExempt(cmd string) bool {
	switch cmd {
	case "CLIENT", "QUIT", "AUTH", "HELLO", "COMMAND", "PING", "SHUTDOWN":
		// SHUTDOWN 是管理命令：即使处于 CLIENT PAUSE 窗口也必须立即
		// 执行，否则暂停期间无法优雅关停服务器。
		return true
	}
	return false
}

// waitPauseWindow 阻塞当前命令直到 CLIENT PAUSE 窗口结束：
// pauseUntil 归零（CLIENT UNPAUSE 调用）或当前时间超过 pauseUntil
// （暂停超时自动恢复）。连接上下文取消（关闭/服务器关闭）时立即返回，
// 避免优雅关闭被暂停窗口拖住。轮询间隔 10ms 保证 UNPAUSE 及时生效。
func (h *Handler) waitPauseWindow(state *connState) {
	for {
		until := h.pauseUntil.Load()
		if until == 0 || time.Now().UnixMilli() >= until {
			return
		}
		select {
		case <-state.ctx.Done():
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (h *Handler) executeCommand(state *connState, cmd string, args [][]byte, remoteAddr string) proto.RESP {
	if state == nil {
		return proto.NewError("ERR internal error: nil connState")
	}

	// CLIENT PAUSE 窗口：暂停期间除豁免命令（CLIENT/QUIT/AUTH/HELLO/
	// COMMAND/PING 等）外，所有命令在入口等待直到 UNPAUSE 或暂停超时。
	// 这是 Redis CLIENT PAUSE 的语义：故障转移期间停止处理客户端命令。
	if !h.pauseExempt(cmd) {
		h.waitPauseWindow(state)
	}

	state.mu.Lock()
	// 如果配置了密码，检查是否已认证
	if password := os.Getenv("BOLTDB_PASSWORD"); password != "" && !state.authenticated {
		switch cmd {
		case "AUTH", "PING", "QUIT", "COMMAND", "HELLO":
			// 这些命令可以绕过认证
		default:
			state.mu.Unlock()
			return proto.NewError("NOAUTH Authentication required.")
		}
	}

	// 如果在事务中（且不是事务控制命令），将命令加入队列
	if state.inTransaction {
		switch cmd {
		case "MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH", "PING", "QUIT", "RESET":
			// 事务控制/连接命令不排队
		default:
			state.commands = append(state.commands, TransactionCommand{
				Command: cmd,
				Args:    args,
			})
			state.mu.Unlock()
			return proto.NewSimpleString("QUEUED")
		}
	}
	// Expose current command to redirect/fence helpers for the duration of dispatch.
	state.currentCmd = cmd
	state.mu.Unlock()
	defer func() {
		state.mu.Lock()
		state.currentCmd = ""
		state.mu.Unlock()
	}()

	switch cmd {
	// 连接命令
	case "PING":
		return proto.NewSimpleString("PONG")

	case "COMMAND":
		return handleCommand(args)

	case "QUIT":
		state.cancel()
		return proto.NewSimpleString("OK")

	case "ROLE":
		return h.handleROLE(state, args, remoteAddr)

	case "ECHO":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'ECHO' command")
		}
		return proto.NewBulkString(args[0])

	case "ACL":
		return handleACL(args)

	case "CLIENT":
		return h.handleCLIENT(state, args, remoteAddr)
	case "SET":
		return h.handleSET(state, args, remoteAddr)

	case "GET":
		return h.handleGET(state, args, remoteAddr)

	case "GETDEL":
		return h.handleGETDEL(state, args, remoteAddr)

	case "GETEX":
		return h.handleGETEX(state, args, remoteAddr)

	case "SETEX":
		return h.handleSETEX(state, args, remoteAddr)

	case "PSETEX":
		return h.handlePSETEX(state, args, remoteAddr)

	case "SETNX":
		return h.handleSETNX(state, args, remoteAddr)

	case "GETSET":
		return h.handleGETSET(state, args, remoteAddr)

	case "MGET":
		return h.handleMGET(state, args, remoteAddr)

	case "MSET":
		return h.handleMSET(state, args, remoteAddr)

	case "MSETNX":
		return h.handleMSETNX(state, args, remoteAddr)

	case "INCR":
		return h.handleINCR(state, args, remoteAddr)

	case "INCRBY":
		return h.handleINCRBY(state, args, remoteAddr)

	case "DECR":
		return h.handleDECR(state, args, remoteAddr)

	case "DECRBY":
		return h.handleDECRBY(state, args, remoteAddr)

	case "INCRBYFLOAT":
		return h.handleINCRBYFLOAT(state, args, remoteAddr)

	case "APPEND":
		return h.handleAPPEND(state, args, remoteAddr)

	case "STRLEN":
		return h.handleSTRLEN(state, args, remoteAddr)

	case "SETBIT":
		return h.handleSETBIT(state, args, remoteAddr)

	case "GETBIT":
		return h.handleGETBIT(state, args, remoteAddr)

	case "BITCOUNT":
		return h.handleBITCOUNT(state, args, remoteAddr)

	case "BITOP":
		return h.handleBITOP(state, args, remoteAddr)

	case "BITFIELD":
		return h.handleBITFIELD(state, args, remoteAddr)

	case "BITFIELD_RO":
		return h.handleBITFIELD_RO(state, args, remoteAddr)

	case "BITPOS":
		return h.handleBITPOS(state, args, remoteAddr)

	case "BITLEN":
		return h.handleBITLEN(state, args, remoteAddr)

	case "GETRANGE", "SUBSTR":
		return h.handleGETRANGE(state, args, remoteAddr)

	case "SETRANGE":
		return h.handleSETRANGE(state, args, remoteAddr)

	// 通用键管理命令
	case "UNLINK":
		return h.handleUNLINK(state, args, remoteAddr)

	case "DEL":
		return h.handleDEL(state, args, remoteAddr)

	case "EXISTS":
		return h.handleEXISTS(state, args, remoteAddr)

	case "PFADD":
		return h.handlePFADD(state, args, remoteAddr)

	case "PFCOUNT":
		return h.handlePFCOUNT(state, args, remoteAddr)

	case "PFMERGE":
		return h.handlePFMERGE(state, args, remoteAddr)

	case "PFINFO":
		return h.handlePFINFO(state, args, remoteAddr)

	case "TYPE":
		return h.handleTYPE(state, args, remoteAddr)

	case "DUMP":
		return h.handleDUMP(state, args, remoteAddr)

	case "RESTORE":
		return h.handleRESTORE(state, args, remoteAddr)

	case "RESTORE-ASKING":
		return h.handleRESTORE(state, args, remoteAddr)

	case "OBJECT":
		return h.handleOBJECT(state, args, remoteAddr)

	case "EXPIRE":
		return h.handleEXPIRE(state, args, remoteAddr)

	case "EXPIREAT":
		return h.handleEXPIREAT(state, args, remoteAddr)

	case "PEXPIRE":
		return h.handlePEXPIRE(state, args, remoteAddr)

	case "PEXPIREAT":
		return h.handlePEXPIREAT(state, args, remoteAddr)

	case "TTL":
		return h.handleTTL(state, args, remoteAddr)

	case "PTTL":
		return h.handlePTTL(state, args, remoteAddr)

	case "EXPIRETIME":
		return h.handleEXPIRETIME(state, args, remoteAddr)

	case "PEXPIRETIME":
		return h.handlePEXPIRETIME(state, args, remoteAddr)

	case "PERSIST":
		return h.handlePERSIST(state, args, remoteAddr)

	case "RENAME":
		return h.handleRENAME(state, args, remoteAddr)

	case "RENAMENX":
		return h.handleRENAMENX(state, args, remoteAddr)

	case "COPY":
		return h.handleCOPY(state, args, remoteAddr)

	case "SWAPDB":
		return h.handleSWAPDB(state, args, remoteAddr)

	case "TOUCH":
		return h.handleTOUCH(state, args, remoteAddr)

	case "SHUTDOWN":
		return h.handleSHUTDOWN(state, args, remoteAddr)

	case "KEYS":
		return h.handleKEYS(state, args, remoteAddr)

	case "SCAN":
		return h.handleSCAN(state, args, remoteAddr)

	case "RANDOMKEY":
		return h.handleRANDOMKEY(state, args, remoteAddr)
	case "LPUSH":
		return h.handleLPUSH(state, args, remoteAddr)

	case "RPUSH":
		return h.handleRPUSH(state, args, remoteAddr)

	case "LPOP":
		return h.handleLPOP(state, args, remoteAddr)

	case "RPOP":
		return h.handleRPOP(state, args, remoteAddr)

	case "LLEN":
		return h.handleLLEN(state, args, remoteAddr)

	case "LINDEX":
		return h.handleLINDEX(state, args, remoteAddr)

	case "LRANGE":
		return h.handleLRANGE(state, args, remoteAddr)

	case "LSET":
		return h.handleLSET(state, args, remoteAddr)

	case "LTRIM":
		return h.handleLTRIM(state, args, remoteAddr)

	case "LINSERT":
		return h.handleLINSERT(state, args, remoteAddr)

	case "LPOS":
		return h.handleLPOS(state, args, remoteAddr)

	case "LCS":
		return h.handleLCS(state, args, remoteAddr)

	case "LREM":
		return h.handleLREM(state, args, remoteAddr)

	case "RPOPLPUSH":
		return h.handleRPOPLPUSH(state, args, remoteAddr)

	case "LMOVE":
		return h.handleLMOVE(state, args, remoteAddr)

	case "BLMOVE":
		return h.handleBLMOVE(state, args, remoteAddr)

	case "LPUSHX":
		return h.handleLPUSHX(state, args, remoteAddr)

	case "RPUSHX":
		return h.handleRPUSHX(state, args, remoteAddr)

	case "BLPOP":
		return h.handleBLPOP(state, args, remoteAddr)

	case "BRPOP":
		return h.handleBRPOP(state, args, remoteAddr)

	case "BRPOPLPUSH":
		return h.handleBRPOPLPUSH(state, args, remoteAddr)

	case "BLMPOP":
		return h.handleBLMPOP(state, args, remoteAddr)
	case "HSET":
		return h.handleHSET(state, args, remoteAddr)

	case "HGET":
		return h.handleHGET(state, args, remoteAddr)

	case "HDEL":
		return h.handleHDEL(state, args, remoteAddr)

	case "HELLO":
		return h.handleHELLO(state, args, remoteAddr)

	case "HLEN":
		return h.handleHLEN(state, args, remoteAddr)

	case "HGETALL":
		return h.handleHGETALL(state, args, remoteAddr)

	case "HEXISTS":
		return h.handleHEXISTS(state, args, remoteAddr)

	case "HKEYS":
		return h.handleHKEYS(state, args, remoteAddr)

	case "HVALS":
		return h.handleHVALS(state, args, remoteAddr)

	case "HMSET":
		return h.handleHMSET(state, args, remoteAddr)

	case "HMGET":
		return h.handleHMGET(state, args, remoteAddr)

	case "HSETNX":
		return h.handleHSETNX(state, args, remoteAddr)

	case "HINCRBY":
		return h.handleHINCRBY(state, args, remoteAddr)

	case "HINCRBYFLOAT":
		return h.handleHINCRBYFLOAT(state, args, remoteAddr)

	case "HSTRLEN":
		return h.handleHSTRLEN(state, args, remoteAddr)

	case "HRANDFIELD":
		return h.handleHRANDFIELD(state, args, remoteAddr)
	case "SADD":
		return h.handleSADD(state, args, remoteAddr)

	case "SREM":
		return h.handleSREM(state, args, remoteAddr)

	case "SCARD":
		return h.handleSCARD(state, args, remoteAddr)

	case "SISMEMBER":
		return h.handleSISMEMBER(state, args, remoteAddr)

	case "SMEMBERS":
		return h.handleSMEMBERS(state, args, remoteAddr)

	case "SPOP":
		return h.handleSPOP(state, args, remoteAddr)

	case "SRANDMEMBER":
		return h.handleSRANDMEMBER(state, args, remoteAddr)

	case "SMOVE":
		return h.handleSMOVE(state, args, remoteAddr)

	case "SINTER":
		return h.handleSINTER(state, args, remoteAddr)

	case "SUNION":
		return h.handleSUNION(state, args, remoteAddr)

	case "SDIFF":
		return h.handleSDIFF(state, args, remoteAddr)

	case "SINTERSTORE":
		return h.handleSINTERSTORE(state, args, remoteAddr)

	case "SMISMEMBER":
		return h.handleSMISMEMBER(state, args, remoteAddr)

	case "SINTERCARD":
		return h.handleSINTERCARD(state, args, remoteAddr)

	case "SUNIONSTORE":
		return h.handleSUNIONSTORE(state, args, remoteAddr)

	case "SDIFFSTORE":
		return h.handleSDIFFSTORE(state, args, remoteAddr)

	case "SSCAN":
		return h.handleSSCAN(state, args, remoteAddr)
	case "HSCAN":
		return h.handleHSCAN(state, args, remoteAddr)
	case "ZADD":
		return h.handleZADD(state, args, remoteAddr)

	case "ZREM":
		return h.handleZREM(state, args, remoteAddr)

	case "ZREMRANGEBYRANK":
		return h.handleZREMRANGEBYRANK(state, args, remoteAddr)

	case "ZREMRANGEBYSCORE":
		return h.handleZREMRANGEBYSCORE(state, args, remoteAddr)

	case "ZPOPMAX":
		return h.handleZPOPMAX(state, args, remoteAddr)

	case "ZPOPMIN":
		return h.handleZPOPMIN(state, args, remoteAddr)

	case "BZPOPMAX":
		return h.handleBZPOPMAX(state, args, remoteAddr)

	case "BZPOPMIN":
		return h.handleBZPOPMIN(state, args, remoteAddr)

	case "ZCARD":
		return h.handleZCARD(state, args, remoteAddr)

	case "ZSCORE":
		return h.handleZSCORE(state, args, remoteAddr)

	case "ZRANK":
		return h.handleZRANK(state, args, remoteAddr)

	case "ZREVRANK":
		return h.handleZREVRANK(state, args, remoteAddr)

	case "ZCOUNT":
		return h.handleZCOUNT(state, args, remoteAddr)

	case "ZMSCORE":
		return h.handleZMSCORE(state, args, remoteAddr)

	case "ZRANGE":
		return h.handleZRANGE(state, args, remoteAddr)

	case "ZREVRANGE":
		return h.handleZREVRANGE(state, args, remoteAddr)

	case "ZRANGEBYSCORE":
		return h.handleZRANGEBYSCORE(state, args, remoteAddr)

	case "ZREVRANGEBYSCORE":
		return h.handleZREVRANGEBYSCORE(state, args, remoteAddr)

	case "ZINCRBY":
		return h.handleZINCRBY(state, args, remoteAddr)

	case "HRANDMEMBER":
		return h.handleHRANDMEMBER(state, args, remoteAddr)

	case "ZRANDMEMBER":
		return h.handleZRANDMEMBER(state, args, remoteAddr)

	case "LMPOP":
		return h.handleLMPOP(state, args, remoteAddr)

	case "ZMPOP":
		return h.handleZMPOP(state, args, remoteAddr)

	case "BZMPOP":
		return h.handleBZMPOP(state, args, remoteAddr)

	case "ZUNIONSTORE":
		return h.handleZUNIONSTORE(state, args, remoteAddr)

	case "ZINTERSTORE":
		return h.handleZINTERSTORE(state, args, remoteAddr)

	case "ZDIFFSTORE":
		return h.handleZDIFFSTORE(state, args, remoteAddr)

	case "ZDIFF":
		return h.handleZDIFF(state, args, remoteAddr)

	case "ZINTER":
		return h.handleZINTER(state, args, remoteAddr)

	case "ZINTERCARD":
		return h.handleZINTERCARD(state, args, remoteAddr)

	case "ZUNION":
		return h.handleZUNION(state, args, remoteAddr)

	case "ZLEXCOUNT":
		return h.handleZLEXCOUNT(state, args, remoteAddr)

	case "ZRANGEBYLEX":
		return h.handleZRANGEBYLEX(state, args, remoteAddr)

	case "ZREVRANGEBYLEX":
		return h.handleZREVRANGEBYLEX(state, args, remoteAddr)

	case "ZREMRANGEBYLEX":
		return h.handleZREMRANGEBYLEX(state, args, remoteAddr)

	case "ZSCAN":
		return h.handleZSCAN(state, args, remoteAddr)

	case "ASKING":
		state.clusterAsking = true
		logger.Logger.Debug().Msg("收到 ASKING 命令")
		return proto.OK

	// Cluster命令
	case "CLUSTER":
		return h.handleCLUSTER(state, args, remoteAddr)
	case "CONFIG":
		return h.handleCONFIG(state, args, remoteAddr)

	// 复制命令
	case "REPLICAOF", "SLAVEOF":
		if h.Replication == nil {
			return proto.NewError("ERR replication not enabled")
		}
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'REPLICAOF' command")
		}
		host := string(args[0])
		port := string(args[1])
		if host == "NO" && port == "ONE" {
			// 停止复制
			replication.StopSlaveReplication(h.Replication)
			return proto.OK
		}
		// 启动复制
		masterAddr := fmt.Sprintf("%s:%s", host, port)
		if err := replication.StartSlaveReplication(h.Replication, h.Db, masterAddr); err != nil {
			return wrapLogError(err)
		}
		return proto.OK

	// 注意：PSYNC 命令由 processRequest 中的 handlePSyncWithRDB 特殊处理
	// 这里不需要处理，master 节点会在收到 PSYNC 时直接发送 RDB 数据

	case "FAILOVER":
		return h.handleFAILOVER(state, args, remoteAddr)

	case "PSYNC", "SYNC":
		if h.Replication == nil {
			return proto.NewError("ERR replication not enabled")
		}
		return proto.NewError("ERR PSYNC is handled by the replication layer")

	case "REPLCONF":
		return h.handleREPLCONF(state, args, remoteAddr)

	case "INFO":
		section := ""
		if len(args) >= 1 {
			section = strings.ToUpper(string(args[0]))
		}
		info := h.buildInfoResponse(section)
		// 注意：Redis 8.2 的 INFO 在 RESP3 下仍是 bulk string
		// （实测 wire 确认，非 Map）——保持字符串返回。
		return proto.NewBulkString([]byte(info))

	// 备份命令
	case "SAVE":
		return h.handleSAVE(state, args, remoteAddr)

	case "BGSAVE":
		return h.handleBGSAVE(state, args, remoteAddr)

	case "BGREWRITEAOF":
		return h.handleBGREWRITEAOF(state, args, remoteAddr)

	case "RESET":
		return h.handleRESET(state, args, remoteAddr)

	case "WAITAOF":
		return h.handleWAITAOF(state, args, remoteAddr)

	case "LASTSAVE":
		return h.handleLASTSAVE(state, args, remoteAddr)

	case "DBSIZE":
		return h.handleDBSIZE(state, args, remoteAddr)

	case "TIME":
		return h.handleTIME(state, args, remoteAddr)

	case "FLUSHDB":
		return h.handleFLUSHDB(state, args, remoteAddr)

	case "FLUSHALL":
		return h.handleFLUSHALL(state, args, remoteAddr)

	case "SELECT":
		return h.handleSELECT(state, args, remoteAddr)

	case "MOVE":
		return h.handleMOVE(state, args, remoteAddr)

	case "WAIT":
		return h.handleWAIT(state, args, remoteAddr)

	case "SLOWLOG":
		return h.handleSLOWLOG(state, args, remoteAddr)

	case "MEMORY":
		return h.handleMEMORY(state, args, remoteAddr)
	case "MODULE":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'MODULE' command")
		}
		subCommand := strings.ToUpper(string(args[0]))
		switch subCommand {
		case "LIST":
			// Return empty array - no modules loaded
			return &proto.Array{Args: [][]byte{}}
		case "LOAD", "LOADEX":
			// BoltDB has no module system — same error Redis gives when
			// the module file cannot be loaded.
			return proto.NewError("ERR Error loading the extension. Please check the server logs.")
		case "UNLOAD":
			return proto.NewError("ERR Error unloading module: no such module with that name")
		case "HELP":
			return &proto.Array{Args: [][]byte{
				[]byte("MODULE LIST - list loaded modules"),
				[]byte("MODULE HELP - shows this help message"),
			}}
		default:
			return proto.NewError("ERR unknown subcommand for 'MODULE'")
		}

	// ==================== LOLWUT ====================
	case "LOLWUT":
		// LOLWUT [VERSION version] - Redis version sanity check
		version := "redis.bolt." + Version
		if len(args) > 0 && strings.ToUpper(string(args[0])) == "VERSION" && len(args) > 1 {
			version = string(args[1])
		}
		// Return a simple artistic pattern
		result := fmt.Sprintf("BoltDB %s - A disk-persistent Redis-compatible database", version)
		return proto.NewBulkString([]byte(result))

	// ==================== LATENCY ====================
	case "LATENCY":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'LATENCY' command")
		}
		subCmd := strings.ToUpper(string(args[0]))
		switch subCmd {
		case "LATEST":
			// LATENCY LATEST - return latest latency samples
			return &proto.Array{Args: [][]byte{}}
		case "RESET":
			// LATENCY RESET [EVENT ...] - reset latency data
			return proto.NewInteger(0)
		case "HELP":
			return &proto.Array{Args: [][]byte{
				[]byte("LATENCY LATEST - returns the latest latency samples"),
				[]byte("LATENCY RESET [EVENT ...] - reset latency data for events"),
				[]byte("LATENCY DOCTOR - analyzes latency issues"),
				[]byte("LATENCY HELP - shows this help message"),
			}}
		case "DOCTOR":
			// Return a diagnostic message
			return &proto.Array{Args: [][]byte{
				[]byte("Latency doctor report:"),
				[]byte("- No latency issues detected"),
				[]byte("- BoltDB uses BadgerDB for disk-based storage"),
				[]byte("- Expected latency < 5ms for SSD, < 50ms for HDD"),
			}}
		case "GRAPH":
			// LATENCY GRAPH <event> - ASCII graph; BoltDB records no
			// samples, so every event errors exactly like Redis does.
			event := "event"
			if len(args) >= 2 {
				event = string(args[1])
			}
			return proto.NewError(fmt.Sprintf("No samples available for event '%s'", event))
		case "HISTORY":
			// LATENCY HISTORY <event> - time series; no samples recorded.
			return &proto.Array{Args: [][]byte{}}
		case "HISTOGRAM":
			// LATENCY HISTOGRAM [event ...] - latency percentiles;
			// RESP3 wire is a Map (like Redis 8.2), RESP2 an array.
			// No samples recorded, so both are empty.
			if state.respVersion == 3 {
				return &proto.Map{Elems: []proto.RESP{}}
			}
			return &proto.Array{Args: [][]byte{}}
		default:
			return proto.NewError("ERR unknown subcommand for 'LATENCY'")
		}

	// ==================== READONLY ====================
	case "READONLY":
		// READONLY - enter read-only mode (for replicas in read-write splitting scenarios)
		// This is primarily used in Redis Cluster for replica nodes
		return proto.NewSimpleString("OK")

	// ==================== READWRITE ====================
	case "READWRITE":
		// READWRITE - exit read-only mode
		return proto.NewSimpleString("OK")

	// ==================== ZRANGESTORE ====================
	case "ZRANGESTORE":
		return h.handleZRANGESTORE(state, args, remoteAddr)
	case "PUBLISH":
		return h.handlePUBLISH(state, args, remoteAddr)

	case "SPUBLISH":
		return h.handleSPUBLISH(state, args, remoteAddr)

	case "SUBSCRIBE":
		return h.handleSUBSCRIBE(state, args, remoteAddr)

	case "SSUBSCRIBE":
		return h.handleSSUBSCRIBE(state, args, remoteAddr)

	case "PSUBSCRIBE":
		return h.handlePSUBSCRIBE(state, args, remoteAddr)

	case "UNSUBSCRIBE":
		return h.handleUNSUBSCRIBE(state, args, remoteAddr)

	case "SUNSUBSCRIBE":
		return h.handleSUNSUBSCRIBE(state, args, remoteAddr)

	case "PUNSUBSCRIBE":
		return h.handlePUNSUBSCRIBE(state, args, remoteAddr)

	case "PUBSUB":
		return h.handlePUBSUB(state, args, remoteAddr)
	case "MULTI":
		return h.handleMULTI(state, args, remoteAddr)

	case "EXEC":
		return h.handleEXEC(state, args, remoteAddr)

	case "DISCARD":
		return h.handleDISCARD(state, args, remoteAddr)

	case "WATCH":
		return h.handleWATCH(state, args, remoteAddr)

	case "UNWATCH":
		return h.handleUNWATCH(state, args, remoteAddr)
	case "GEOADD":
		return h.handleGEOADD(state, args, remoteAddr)

	case "GEOPOS":
		return h.handleGEOPOS(state, args, remoteAddr)

	case "GEOHASH":
		return h.handleGEOHASH(state, args, remoteAddr)

	case "GEODIST":
		return h.handleGEODIST(state, args, remoteAddr)

	case "GEORADIUS":
		return h.handleGEORADIUS(state, args, remoteAddr)

	case "GEORADIUS_RO":
		return h.handleGEORADIUS_RO(state, args, remoteAddr)

	case "GEORADIUSBYMEMBER":
		return h.handleGEORADIUSBYMEMBER(state, args, remoteAddr)

	case "GEORADIUSBYMEMBER_RO":
		return h.handleGEORADIUSBYMEMBER_RO(state, args, remoteAddr)

	case "GEOSEARCH":
		return h.handleGEOSEARCH(state, args, remoteAddr)

	case "GEOSEARCHSTORE":
		return h.handleGEOSEARCHSTORE(state, args, remoteAddr)

	// ==================== Stream commands ====================
	case "XADD":
		return h.handleXADD(state, args, remoteAddr)

	case "XLEN":
		return h.handleXLEN(state, args, remoteAddr)

	case "XREAD":
		return h.handleXREAD(state, args, remoteAddr)

	case "XRANGE":
		return h.handleXRANGE(state, args, remoteAddr)

	case "XREVRANGE":
		return h.handleXREVRANGE(state, args, remoteAddr)

	case "XDEL":
		return h.handleXDEL(state, args, remoteAddr)

	case "XACK":
		return h.handleXACK(state, args, remoteAddr)

	case "XACKDEL":
		return h.handleXACKDEL(state, args, remoteAddr)

	case "XDELEX":
		return h.handleXDELEX(state, args, remoteAddr)

	case "XNACK":
		return h.handleXNACK(state, args, remoteAddr)

	case "XSETID":
		return h.handleXSETID(state, args, remoteAddr)

	case "XCFGSET":
		return h.handleXCFGSET(state, args, remoteAddr)

	case "XGROUP":
		return h.handleXGROUP(state, args, remoteAddr)

	case "XREADGROUP":
		return h.handleXREADGROUP(state, args, remoteAddr)

	case "XCLAIM":
		return h.handleXCLAIM(state, args, remoteAddr)

	case "XAUTOCLAIM":
		return h.handleXAUTOCLAIM(state, args, remoteAddr)

	case "XPENDING":
		return h.handleXPENDING(state, args, remoteAddr)

	case "XINFO":
		return h.handleXINFO(state, args, remoteAddr)

	case "XTRIM":
		return h.handleXTRIM(state, args, remoteAddr)

	// ==================== SORT ====================
	case "SORT":
		return h.handleSORT(state, args, remoteAddr)

	case "SORT_RO":
		return h.handleSORT_RO(state, args, remoteAddr)
	case "AUTH":
		return h.handleAUTH(state, args, remoteAddr)
	case "JSON.SET":
		return h.handleJSON_SET(state, args, remoteAddr)

	case "JSON.GET":
		return h.handleJSON_GET(state, args, remoteAddr)

	case "JSON.DEL":
		return h.handleJSON_DEL(state, args, remoteAddr)

	case "JSON.TYPE":
		return h.handleJSON_TYPE(state, args, remoteAddr)

	case "JSON.MGET":
		return h.handleJSON_MGET(state, args, remoteAddr)

	case "JSON.ARRAPPEND":
		return h.handleJSON_ARRAPPEND(state, args, remoteAddr)

	case "JSON.ARRLEN":
		return h.handleJSON_ARRLEN(state, args, remoteAddr)

	case "JSON.OBJKEYS":
		return h.handleJSON_OBJKEYS(state, args, remoteAddr)

	case "JSON.NUMINCRBY":
		return h.handleJSON_NUMINCRBY(state, args, remoteAddr)

	case "JSON.NUMMULTBY":
		return h.handleJSON_NUMMULTBY(state, args, remoteAddr)

	case "JSON.CLEAR":
		return h.handleJSON_CLEAR(state, args, remoteAddr)

	case "JSON.DEBUG":
		return h.handleJSON_DEBUG(state, args, remoteAddr)
	case "TS.CREATE":
		return h.handleTS_CREATE(state, args, remoteAddr)

	case "TS.ADD":
		return h.handleTS_ADD(state, args, remoteAddr)

	case "TS.GET":
		return h.handleTS_GET(state, args, remoteAddr)

	case "TS.RANGE":
		return h.handleTS_RANGE(state, args, remoteAddr)

	case "TS.DEL":
		return h.handleTS_DEL(state, args, remoteAddr)

	case "TS.INFO":
		return h.handleTS_INFO(state, args, remoteAddr)

	case "TS.LEN":
		return h.handleTS_LEN(state, args, remoteAddr)

	case "TS.MGET":
		return h.handleTS_MGET(state, args, remoteAddr)

	case "TS.REVRANGE":
		return h.handleTS_REVRANGE(state, args, remoteAddr)

	case "TS.MRANGE":
		return h.handleTS_MRANGE(state, args, remoteAddr)

	case "TS.MREVRANGE":
		return h.handleTS_MREVRANGE(state, args, remoteAddr)

	case "TS.QUERYINDEX":
		return h.handleTS_QUERYINDEX(state, args, remoteAddr)

	case "TS.MADD":
		return h.handleTS_MADD(state, args, remoteAddr)

	case "TS.INCRBY":
		return h.handleTS_INCRBY(state, args, remoteAddr)

	case "TS.CREATERULE":
		return h.handleTS_CREATERULE(state, args, remoteAddr)

	case "TS.DELETERULE":
		return h.handleTS_DELETERULE(state, args, remoteAddr)

	case "MIGRATE":
		return h.handleMIGRATE(state, args, remoteAddr)

	case "DEBUG":
		return h.handleDEBUG(state, args, remoteAddr)

	case "MONITOR":
		if len(args) > 0 {
			return proto.NewError("ERR wrong number of arguments for 'MONITOR' command")
		}
		state.mu.Lock()
		state.monitoring = true
		state.mu.Unlock()
		h.registerMonitorClient(state)
		return proto.OK

	default:
		state.mu.Lock()
		inTx := state.inTransaction
		if inTx {
			state.commands = append(state.commands, TransactionCommand{
				Command: cmd,
				Args:    args,
			})
		}
		state.mu.Unlock()
		if inTx {
			return proto.NewSimpleString("QUEUED")
		}
		return proto.NewError(fmt.Sprintf("ERR unknown command '%s'", cmd))
	}
}
