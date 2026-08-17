package server

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/lbp0200/BoltDB/internal/logger"
	"github.com/lbp0200/BoltDB/internal/proto"
)

// handleCLIENT 实现 CLIENT 命令
func (h *Handler) handleCLIENT(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'CLIENT' command")
	}
	subcommand := strings.ToUpper(string(args[0]))
	switch subcommand {
	case "LIST":
		return h.clientListRESP()
	case "GETNAME":
		if state.clientInfo != nil && state.clientInfo.Name != "" {
			return proto.NewBulkString([]byte(state.clientInfo.Name))
		}
		if state.respVersion == 3 {
			return &proto.Null{}
		}
		nilResp := proto.NewBulkString(nil)
		return nilResp
	case "SETNAME":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'CLIENT SETNAME' command")
		}
		name := string(args[1])
		if state.clientInfo == nil {
			state.clientInfo = &ClientInfo{}
		}
		state.clientInfo.Name = name
		return proto.OK
	case "ID":
		if state.clientInfo != nil && state.clientInfo.ID > 0 {
			return proto.NewInteger(state.clientInfo.ID)
		}
		return proto.NewInteger(1)
	case "KILL":
		// CLIENT KILL TYPE <type> — kill by connection type
		if len(args) >= 3 && strings.ToUpper(string(args[1])) == "TYPE" {
			killType := strings.ToUpper(string(args[2]))
			var killed int
			switch killType {
			case "SLAVE":
				if h.Replication == nil {
					return proto.NewInteger(0)
				}
				slaves := h.Replication.GetSlaves()
				for _, slave := range slaves {
					if err := slave.Close(); err != nil {
						logger.Logger.Debug().Err(err).Str("slave_id", slave.ID).Msg("close slave on CLIENT KILL TYPE slave")
					}
					h.Replication.RemoveSlave(slave.ID)
					killed++
				}
			case "BLOCKING":
				h.connsMu.RLock()
				var targets []struct {
					state *connState
					conn  net.Conn
				}
				for s, m := range h.conns {
					if s.blocking.Load() {
						targets = append(targets, struct {
							state *connState
							conn  net.Conn
						}{s, m.conn})
					}
				}
				h.connsMu.RUnlock()
				for _, t := range targets {
					t.state.mu.Lock()
					if t.state.cancel != nil {
						t.state.cancel()
					}
					t.state.mu.Unlock()
					if t.conn != nil {
						_ = t.conn.Close()
					}
				}
				killed = len(targets)
			case "NORMAL":
				h.connsMu.RLock()
				var targets []struct {
					state *connState
					conn  net.Conn
				}
				for s, m := range h.conns {
					targets = append(targets, struct {
						state *connState
						conn  net.Conn
					}{s, m.conn})
				}
				h.connsMu.RUnlock()
				for _, t := range targets {
					t.state.mu.Lock()
					if t.state.cancel != nil {
						t.state.cancel()
					}
					t.state.mu.Unlock()
					if t.conn != nil {
						_ = t.conn.Close()
					}
				}
				killed = len(targets)
			default:
				return proto.NewError(fmt.Sprintf("ERR unsupported CLIENT KILL TYPE '%s'", killType))
			}
			return proto.NewInteger(int64(killed))
		}

		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'CLIENT KILL' command")
		}
		addr := string(args[1])
		if addr == "" {
			return proto.NewError("ERR Invalid address")
		}

		h.connsMu.RLock()
		var targetState *connState
		var targetConn net.Conn
		for s, m := range h.conns {
			if m.remoteAddr == addr {
				targetState = s
				targetConn = m.conn
				break
			}
		}
		h.connsMu.RUnlock()

		if targetState == nil {
			return proto.NewInteger(0)
		}

		targetState.mu.Lock()
		if targetState.cancel != nil {
			targetState.cancel()
		}
		targetState.mu.Unlock()
		if targetConn != nil {
			_ = targetConn.Close()
		}
		return proto.NewInteger(1)
	case "PAUSE":
		// CLIENT PAUSE <milliseconds> [WRITE|ALL]
		// 暂停所有客户端（除豁免命令外）的处理，用于故障转移窗口。
		// Redis 语义：暂停期间命令被阻塞直到超时或 CLIENT UNPAUSE。
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'CLIENT PAUSE' command")
		}
		ms, err := strconv.Atoi(string(args[1]))
		if err != nil || ms < 0 {
			return proto.NewError("ERR timeout is not an integer or out of range")
		}
		h.pauseUntil.Store(time.Now().Add(time.Duration(ms) * time.Millisecond).UnixMilli())
		return proto.OK
	case "UNPAUSE":
		h.pauseUntil.Store(0)
		return proto.OK
	case "INFO":
		addr := fmt.Sprintf("127.0.0.1:%d", h.Port)
		clientID := int64(0)
		fd := 0
		clientName := ""
		ci := state.clientInfo
		if ci != nil {
			if ci.Addr != "" {
				addr = ci.Addr
			}
			clientID = ci.ID
			fd = ci.FD
			clientName = ci.Name
		}
		clientAge := "0"
		idleTime := "0"
		flags := "N"
		db := "0"
		sub := "0"
		psub := "0"
		multi := "-1"
		keys := "0"
		libName := ""
		libVer := ""
		if ci != nil {
			libName = ci.LibName
			libVer = ci.LibVer
		}
		info := fmt.Sprintf("id=%d addr=%s fd=%d name=%s age=%s idle=%s flags=%s db=%s sub=%s psub=%s multi=%s cmd=client events=r oFlags= keys=%s lib-name=%s lib-ver=%s",
			clientID, addr, fd, clientName, clientAge, idleTime, flags, db, sub, psub, multi, keys, libName, libVer)
		return proto.NewBulkString([]byte(info))
	case "NOEVICT":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'CLIENT NOEVICT' command")
		}
		mode := strings.ToUpper(string(args[1]))
		if mode != "ON" && mode != "OFF" {
			return proto.NewError("ERR syntax error")
		}
		// 真实生效：ON 时输出缓冲区超限不主动断开（Redis 语义）
		state.noEvict.Store(mode == "ON")
		return proto.OK
	case "TRACKING":
		// CLIENT TRACKING ON|OFF [REDIRECT <id>]
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'CLIENT TRACKING' command")
		}
		mode := strings.ToUpper(string(args[1]))
		if mode != "ON" && mode != "OFF" {
			return proto.NewError("ERR syntax error")
		}
		state.tracking.Store(mode == "ON")
		// 可选 REDIRECT <id>：设置失效通知的重定向目标客户端
		for i := 2; i < len(args); i++ {
			opt := strings.ToUpper(string(args[i]))
			switch opt {
			case "REDIRECT":
				if i+1 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				id, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil || id < 0 {
					return proto.NewError("ERR value is not an integer or out of range")
				}
				state.trackingRedirect.Store(id)
				i++
			case "BCAST", "OPTIN", "OPTOUT", "NOLOOP":
				// 接受的附加选项（BoltDB 不推送失效通知，语义无差异）
			default:
				return proto.NewError("ERR syntax error")
			}
		}
		return proto.OK
	case "SETINFO":
		// CLIENT SETINFO [LIB-NAME name] [LIB-VER ver]
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'CLIENT SETINFO' command")
		}
		opt := strings.ToUpper(string(args[1]))
		if opt != "LIB-NAME" && opt != "LIB-VER" {
			return proto.NewError("ERR syntax error")
		}
		// 真实存储到连接状态，CLIENT INFO 中可见（Redis 语义）
		if state.clientInfo == nil {
			state.clientInfo = &ClientInfo{}
		}
		val := string(args[2])
		if opt == "LIB-NAME" {
			state.clientInfo.LibName = val
		} else {
			state.clientInfo.LibVer = val
		}
		return proto.OK
	case "NO-TOUCH":
		// CLIENT NO-TOUCH <ON|OFF>
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'CLIENT NO-TOUCH' command")
		}
		mode := strings.ToUpper(string(args[1]))
		if mode != "ON" && mode != "OFF" {
			return proto.NewError("ERR syntax error")
		}
		return proto.OK
	case "CACHING":
		// CLIENT CACHING <YES|NO>
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'CLIENT CACHING' command")
		}
		mode := strings.ToUpper(string(args[1]))
		if mode != "YES" && mode != "NO" {
			return proto.NewError("ERR syntax error")
		}
		return proto.OK
	case "GETREDIR":
		// CLIENT GETREDIR — 返回 tracking 失效通知的重定向客户端 ID
		// （0 = 无重定向；BoltDB 不推送失效通知，仅反映连接状态）。
		return proto.NewInteger(state.trackingRedirect.Load())
	default:
		return proto.NewError("ERR syntax error")
	}

	// String命令
}
