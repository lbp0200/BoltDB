package server

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/lbp0200/BoltDB/internal/proto"
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
			sl := h.ensureSlowlog()
			sl.mu.Lock()
			slowlogThreshold := sl.threshold
			slowlogMaxLen := sl.maxLen
			sl.mu.Unlock()
			configs := []string{
				"save", "",
				"appendonly", "no",
				"maxmemory", "0",
				"maxmemory-policy", "noeviction",
				"slowlog-log-slower-than", strconv.FormatInt(slowlogThreshold, 10),
				"slowlog-max-len", strconv.Itoa(slowlogMaxLen),
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
			case "slowlog-log-slower-than":
				value = strconv.FormatInt(h.ensureSlowlog().threshold, 10)
			case "slowlog-max-len":
				value = strconv.Itoa(h.ensureSlowlog().maxLen)
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
		case "slowlog-log-slower-than":
			us, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return proto.NewError("ERR value is not an integer or out of range")
			}
			h.ensureSlowlog().setThreshold(us)
		case "slowlog-max-len":
			n, err := strconv.Atoi(val)
			if err != nil {
				return proto.NewError("ERR value is not an integer or out of range")
			}
			h.ensureSlowlog().setMaxLen(n)
		case "backlog-size", "save", "appendonly", "maxmemory", "maxmemory-policy":
			// Known no-op configs: accepted for client compatibility.
			// backlog-size 曾控制复制环大小——S2 backlog 退役后无实际存储，保留为 no-op。
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
