package server

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/replication"
	"github.com/lbp0200/BoltDB/internal/store"
)

// handleROLE 实现 ROLE 命令
func (h *Handler) handleROLE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	// 返回角色信息，兼容 redis-sentinel
	// 格式: [master|slave|sentinel, master地址, 复制偏移量]
	// 对于主节点: ["master", "repl_offset"]
	// 对于从节点: ["slave", "master地址", master端口, 状态, 已同步偏移量]
	if h.Replication != nil {
		role := h.Replication.GetRole()
		if role == replication.RoleMaster {
			offset := h.Replication.GetMasterReplOffset()
			return &proto.Array{Args: [][]byte{
				[]byte(replication.RoleMaster),
				[]byte(strconv.FormatInt(offset, 10)),
			}}
		} else {
			// 从节点
			masterAddr := h.Replication.GetMasterAddr()
			masterHost := ""
			masterPort := "6379"
			if masterAddr != "" {
				parts := strings.Split(masterAddr, ":")
				if len(parts) >= 2 {
					masterHost = parts[0]
					masterPort = parts[1]
				} else if len(parts) == 1 {
					masterHost = parts[0]
				}
			}
			offset := h.Replication.GetMasterReplOffset()
			return &proto.Array{Args: [][]byte{
				[]byte(replication.RoleSlave),
				[]byte(masterHost),
				[]byte(masterPort),
				[]byte("connected"),
				[]byte(strconv.FormatInt(offset, 10)),
			}}
		}
	}
	// 默认主节点
	return &proto.Array{Args: [][]byte{
		[]byte(replication.RoleMaster),
		[]byte("0"),
	}}
}

// handleHELLO 实现 HELLO 命令
func (h *Handler) handleHELLO(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	protoLevel := 2
	if len(args) >= 1 {
		level, err := strconv.Atoi(string(args[0]))
		if err != nil || level < 2 || level > 3 {
			return proto.NewError("ERR Protocol version is not supported")
		}
		protoLevel = level
	}
	state.respVersion = protoLevel
	role := "master"
	if h.Replication != nil {
		role = h.Replication.GetRole()
	}
	mode := "standalone"
	if h.Cluster != nil {
		mode = "cluster"
	}
	id := int64(0)
	if state.clientInfo != nil {
		id = state.clientInfo.ID
	}
	elements := []proto.RESP{
		proto.NewBulkString([]byte("server")),
		proto.NewBulkString([]byte("boltdb")),
		proto.NewBulkString([]byte("version")),
		proto.NewBulkString([]byte(Version)),
		proto.NewBulkString([]byte("proto")),
		proto.NewInteger(int64(protoLevel)),
		proto.NewBulkString([]byte("id")),
		proto.NewInteger(id),
		proto.NewBulkString([]byte("mode")),
		proto.NewBulkString([]byte(mode)),
		proto.NewBulkString([]byte("role")),
		proto.NewBulkString([]byte(role)),
		proto.NewBulkString([]byte("modules")),
		&proto.NestedArray{Elems: []proto.RESP{}},
	}
	if protoLevel == 3 {
		return &proto.Map{Elems: elements}
	}
	return &proto.NestedArray{Elems: elements}
}

// handleSAVE 实现 SAVE 命令
func (h *Handler) handleSAVE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if h.Backup == nil {
		return proto.NewError("ERR backup not enabled")
	}
	if err := h.Backup.Save(); err != nil {
		return wrapLogError(err)
	}
	return proto.OK
}

// handleBGSAVE 实现 BGSAVE 命令
func (h *Handler) handleBGSAVE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if h.Backup == nil {
		return proto.NewError("ERR backup not enabled")
	}
	bgCtx := h.Ctx
	if bgCtx == nil {
		bgCtx = context.Background()
	}
	if err := h.Backup.BGSave(bgCtx); err != nil {
		return wrapLogError(err)
	}
	return proto.NewSimpleString("Background saving started")
}

// handleLASTSAVE 实现 LASTSAVE 命令
func (h *Handler) handleLASTSAVE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if h.Backup == nil {
		return proto.NewError("ERR backup not enabled")
	}
	lastSave := h.Backup.LastSave()
	return proto.NewInteger(lastSave)
}

// handleDBSIZE 实现 DBSIZE 命令
func (h *Handler) handleDBSIZE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	keys, err := h.Db.Keys("*")
	if err != nil {
		return wrapStoreError(err)
	}
	return proto.NewInteger(int64(len(keys)))
}

// handleTIME 实现 TIME 命令
func (h *Handler) handleTIME(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	sec, usec, err := h.Db.Time()
	if err != nil {
		return wrapStoreError(err)
	}
	return &proto.Array{Args: [][]byte{
		[]byte(fmt.Sprintf("%d", sec)),
		[]byte(fmt.Sprintf("%d", usec)),
	}}
}

// handleFLUSHDB 实现 FLUSHDB 命令
func (h *Handler) handleFLUSHDB(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	err := h.Db.FlushDB()
	if err != nil {
		return wrapStoreError(err)
	}
	return proto.OK
}

// handleFLUSHALL 实现 FLUSHALL 命令
func (h *Handler) handleFLUSHALL(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	err := h.Db.FlushDB()
	if err != nil {
		return wrapStoreError(err)
	}
	return proto.OK
}

// handleSELECT 实现 SELECT 命令
func (h *Handler) handleSELECT(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	// BoltDB is a single-database implementation
	// Always return OK regardless of the database number
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'SELECT' command")
	}
	return proto.OK
}

// handleMOVE 实现 MOVE 命令
func (h *Handler) handleMOVE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	// BoltDB is a single-database implementation
	// MOVE always returns 0 (key was not moved)
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'MOVE' command")
	}
	// #nosec G115 - result is always 0 for single-db implementation
	return proto.NewInteger(0)
}

// handleWAIT 实现 WAIT 命令：阻塞直到至少 numreplicas 个从节点的
// ReplAckOffset >= 当前 master offset，或超时。返回已确认的从节点数。
func (h *Handler) handleWAIT(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'WAIT' command")
	}
	numReplicas, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return proto.NewError("ERR numreplicas is not an integer or out of range")
	}
	timeoutMs, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return proto.NewError("ERR timeout is not an integer or out of range")
	}
	if numReplicas < 0 {
		numReplicas = 0
	}
	if timeoutMs < 0 {
		timeoutMs = 0
	}

	if h.Replication == nil || !h.Replication.IsMaster() {
		return proto.NewInteger(0)
	}

	targetOffset := h.Replication.GetMasterReplOffset()
	deadline := time.Now().Add(time.Duration(timeoutMs) * time.Millisecond)

	for {
		acked := 0
		for _, slave := range h.Replication.GetSlaves() {
			if slave.GetReplAckOffset() >= targetOffset {
				acked++
			}
		}
		if numReplicas == 0 || acked >= numReplicas {
			return proto.NewInteger(int64(acked))
		}
		if timeoutMs == 0 || time.Now().After(deadline) {
			return proto.NewInteger(int64(acked))
		}
		// Short poll; Redis blocks on slave ACKs — this is a bounded approximation.
		sleep := 10 * time.Millisecond
		if remaining := time.Until(deadline); remaining > 0 && remaining < sleep {
			sleep = remaining
		}
		time.Sleep(sleep)
	}
}

// handleSLOWLOG 实现 SLOWLOG 命令
func (h *Handler) handleSLOWLOG(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	// BoltDB does not implement slow query logging yet
	// Return empty list for all subcommands
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'SLOWLOG' command")
	}
	subCommand := strings.ToUpper(string(args[0]))
	switch subCommand {
	case "GET":
		// Return empty array for GET
		return &proto.Array{Args: [][]byte{}}
	case "LEN":
		// Return 0 (no slowlog entries)
		return proto.NewInteger(0)
	case "RESET":
		// Return OK for RESET
		return proto.OK
	case "HELP":
		return &proto.Array{Args: [][]byte{
			[]byte("SLOWLOG GET <count> - returns top <count> entries from the slowlog"),
			[]byte("SLOWLOG LEN - returns the length of the slowlog"),
			[]byte("SLOWLOG RESET - clears the slowlog"),
			[]byte("SLOWLOG HELP - shows this help message"),
		}}
	default:
		return proto.NewError("ERR unknown subcommand for 'SLOWLOG'")
	}
}

// handleMEMORY 实现 MEMORY 命令
func (h *Handler) handleMEMORY(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'MEMORY' command")
	}
	subCommand := strings.ToUpper(string(args[0]))
	switch subCommand {
	case "USAGE":
		// MEMORY USAGE key [SAMPLES count]
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'MEMORY USAGE' command")
		}
		key := string(args[1])
		// Estimate memory usage - use key type size approximation
		size, err := h.Db.MemoryUsage(key)
		if err != nil {
			if errors.Is(err, store.ErrKeyNotFound) {
				if state.respVersion == 3 {
					return &proto.Null{}
				}
				return proto.NewBulkString(nil)
			}
			return wrapStoreError(err)
		}
		return proto.NewInteger(size)
	case "DOCTOR":
		// Return basic memory info
		return &proto.Array{Args: [][]byte{
			[]byte("BoltDB uses BadgerDB for storage"),
			[]byte("Memory usage is managed by the underlying BadgerDB engine"),
		}}
	case "HELP":
		return &proto.Array{Args: [][]byte{
			[]byte("MEMORY USAGE key [SAMPLES count] - estimate memory usage of key"),
			[]byte("MEMORY DOCTOR - reports memory usage details"),
			[]byte("MEMORY HELP - shows this help message"),
		}}
	default:
		return proto.NewError("ERR unknown subcommand for 'MEMORY'")
	}

	// ==================== MODULE ====================
}

// handleAUTH 实现 AUTH 命令
func (h *Handler) handleAUTH(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	// 简化实现：检查密码
	// 支持环境变量 BOLTDB_PASSWORD
	password := os.Getenv("BOLTDB_PASSWORD")
	if password == "" {
		// 没有配置密码，任何密码都接受
		state.authenticated = true
		return proto.NewSimpleString("OK")
	}

	// 格式: AUTH password 或 AUTH username password
	var inputPassword string
	if len(args) >= 1 {
		inputPassword = string(args[0])
	}

	if subtle.ConstantTimeCompare([]byte(inputPassword), []byte(password)) == 1 {
		state.authenticated = true
		return proto.NewSimpleString("OK")
	}
	return proto.NewError("ERR invalid password")

	// ==================== JSON ====================
}

// handleDEBUG 实现 DEBUG 命令
func (h *Handler) handleDEBUG(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'DEBUG' command")
	}
	subcommand := strings.ToUpper(string(args[0]))
	switch subcommand {
	case "SLEEP":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'DEBUG SLEEP' command")
		}
		duration, err := strconv.ParseFloat(string(args[1]), 64)
		if err != nil || duration < 0 {
			return proto.NewError("ERR invalid sleep duration")
		}
		time.Sleep(time.Duration(duration * float64(time.Second)))
		return proto.OK
	case "OBJECT":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'DEBUG OBJECT' command")
		}
		key := string(args[1])
		keyType, err := h.Db.Type(key)
		if err != nil {
			return wrapStoreError(err)
		}
		ttl, err := h.Db.TTL(key)
		if err != nil {
			return wrapStoreError(err)
		}
		info := fmt.Sprintf("Key: %s; Type: %s; TTL: %ds", key, keyType, ttl)
		return proto.NewBulkString([]byte(info))
	case "SEGFAULT":
		return proto.NewError("ERR DEBUG SEGFAULT requested (simulated)")
	case "ERROR":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'DEBUG ERROR' command")
		}
		message := string(args[1])
		return proto.NewError(fmt.Sprintf("ERR %s", message))
	case "SET-ACTIVE-EXPIRE":
		// DEBUG SET-ACTIVE-EXPIRE <0|1> — enable/disable active expiration (testing only)
		return proto.OK
	default:
		return proto.NewError(fmt.Sprintf("ERR unknown DEBUG subcommand '%s'", subcommand))
	}
}
