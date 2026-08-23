package server

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/replication"
)

// handleCONFIG 实现 CONFIG 命令
func (h *Handler) handleCONFIG(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'CONFIG' command")
	}
	subcommand := strings.ToUpper(string(args[0]))
	switch subcommand {
	case "GET":
		if len(args) == 1 || (len(args) >= 2 && string(args[1]) == "*") {
			backlogSize := replication.DefaultBacklogSize
			if h.Replication != nil && h.Replication.GetBacklog() != nil {
				backlogSize = h.Replication.GetBacklog().GetSize()
			}
			configs := []string{
				"save", "",
				"appendonly", "no",
				"maxmemory", "0",
				"maxmemory-policy", "noeviction",
				"backlog-size", strconv.FormatInt(backlogSize, 10),
			}
			results := make([][]byte, len(configs))
			for i, cfg := range configs {
				results[i] = []byte(cfg)
			}
			// RESP3: CONFIG GET must be a Map ('%') so clients return a dict.
			if state.respVersion == 3 {
				return h.configArrayToMap(results)
			}
			return &proto.Array{Args: results}
		} else if len(args) >= 2 {
			key := string(args[1])
			var value string
			switch strings.ToLower(key) {
			case "save":
				value = ""
			case "appendonly":
				value = "no"
			case "maxmemory":
				value = "0"
			case "maxmemory-policy":
				value = "noeviction"
			case "backlog-size":
				if h.Replication != nil {
					value = strconv.FormatInt(h.Replication.GetBacklog().GetSize(), 10)
				} else {
					value = strconv.FormatInt(replication.DefaultBacklogSize, 10)
				}
			default:
				value = ""
			}
			// RESP3: CONFIG GET must be a Map ('%') so clients return a dict.
			if state.respVersion == 3 {
				return h.configArrayToMap([][]byte{[]byte(key), []byte(value)})
			}
			return &proto.Array{Args: [][]byte{[]byte(key), []byte(value)}}
		} else {
			return proto.NewError("ERR wrong number of arguments for 'CONFIG GET' command")
		}
	case "SET":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'CONFIG SET' command")
		}
		param := strings.ToLower(string(args[1]))
		val := string(args[2])
		switch param {
		case "backlog-size":
			if h.Replication == nil {
				return proto.NewError("ERR no replication manager")
			}
			size, err := replication.ParseBacklogSize(val)
			if err != nil {
				return proto.NewError(fmt.Sprintf("ERR invalid backlog-size: %s", err))
			}
			h.Replication.SetBacklogSize(size)
		case "save", "appendonly", "maxmemory", "maxmemory-policy",
			"slowlog-log-slower-than", "slowlog-max-len":
			// Known no-op configs: accepted for client compatibility
			// (redis-py, redis-cli send these during init)
		default:
			return proto.NewError(fmt.Sprintf("ERR unsupported config parameter: %s", param))
		}
		return proto.OK
	case "REWRITE":
		return proto.OK
	default:
		return proto.NewError(fmt.Sprintf("ERR unknown subcommand '%s'", subcommand))
	}
}
