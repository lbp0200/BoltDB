package server

import (
	"fmt"
	"net"
	"strings"

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
		logger.Logger.Warn().Msg("CLIENT PAUSE is not implemented (no-op)")
		return proto.OK
	case "UNPAUSE":
		logger.Logger.Warn().Msg("CLIENT UNPAUSE is not implemented (no-op)")
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
		info := fmt.Sprintf("id=%d addr=%s fd=%d name=%s age=%s idle=%s flags=%s db=%s sub=%s psub=%s multi=%s cmd=client events=r oFlags= keys=%s",
			clientID, addr, fd, clientName, clientAge, idleTime, flags, db, sub, psub, multi, keys)
		return proto.NewBulkString([]byte(info))
	case "NOEVICT":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'CLIENT NOEVICT' command")
		}
		mode := strings.ToUpper(string(args[1]))
		if mode != "ON" && mode != "OFF" {
			return proto.NewError("ERR syntax error")
		}
		// noevict 模式（简化实现）
		return proto.OK
	case "TRACKING":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'CLIENT TRACKING' command")
		}
		mode := strings.ToUpper(string(args[1]))
		if mode != "ON" && mode != "OFF" {
			return proto.NewError("ERR syntax error")
		}
		// tracking 模式（简化实现）
		return proto.OK
	default:
		return proto.NewError("ERR syntax error")
	}

	// String命令
}
