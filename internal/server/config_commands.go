package server

import (
	"fmt"
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
			configs := []string{
				"save", "",
				"appendonly", "no",
				"maxmemory", "0",
				"maxmemory-policy", "noeviction",
			}
			results := make([][]byte, len(configs))
			for i, cfg := range configs {
				results[i] = []byte(cfg)
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
			default:
				value = ""
			}
			return &proto.Array{Args: [][]byte{[]byte(key), []byte(value)}}
		} else {
			return proto.NewError("ERR wrong number of arguments for 'CONFIG GET' command")
		}
	case "SET":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'CONFIG SET' command")
		}
		return proto.OK
	case "REWRITE":
		return proto.OK
	default:
		return proto.NewError(fmt.Sprintf("ERR unknown subcommand '%s'", subcommand))
	}
}
