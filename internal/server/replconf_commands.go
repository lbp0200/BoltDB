package server

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/lbp0200/BoltDB/internal/logger"
	"github.com/lbp0200/BoltDB/internal/proto"
)

// handleREPLCONF 实现 REPLCONF 命令
func (h *Handler) handleREPLCONF(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if h.Replication == nil {
		return proto.NewError("ERR replication not enabled")
	}
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'REPLCONF' command")
	}
	subcommand := strings.ToUpper(string(args[0]))
	switch subcommand {
	case "LISTENING-PORT":
		if len(args) >= 2 {
			port := string(args[1])
			logger.Logger.Debug().Str("remote_addr", remoteAddr).Str("port", port).Msg("从节点监听端口")
		}
		return proto.OK
	case "CAPA":
		if len(args) >= 2 {
			capa := string(args[1])
			logger.Logger.Debug().Str("remote_addr", remoteAddr).Str("capability", capa).Msg("从节点能力")
		}
		return proto.OK
	case "ACK":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'REPLCONF ACK' command")
		}
		offset, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return proto.NewError("ERR invalid offset")
		}
		if h.Replication.IsMaster() {
			slave := h.Replication.GetSlaveByAddr(remoteAddr)
			if slave != nil {
				slave.UpdateReplAck(offset)
				logger.Logger.Debug().
					Str("slave_id", slave.ID).
					Str("remote_addr", remoteAddr).
					Int64("ack_offset", offset).
					Msg("更新从节点ACK偏移量")
			}
		}
		return proto.OK
	case "GETACK":
		offset := h.Replication.GetBacklogCurrentOffset()
		return &proto.Array{Args: [][]byte{
			[]byte("REPLCONF"),
			[]byte("ACK"),
			[]byte(strconv.FormatInt(offset, 10)),
		}}
	case "SYNC":
		return proto.OK
	case "NOREPLY":
		return proto.OK
	default:
		return proto.NewError(fmt.Sprintf("ERR unknown subcommand '%s'", subcommand))
	}
}
