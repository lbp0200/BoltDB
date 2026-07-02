package server

import (
	"fmt"
	"strings"

	"github.com/lbp0200/BoltDB/internal/cluster"
	"github.com/lbp0200/BoltDB/internal/proto"
)

// handleCLUSTER 实现 CLUSTER 命令
func (h *Handler) handleCLUSTER(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if h.Cluster == nil {
		return proto.NewError("ERR This instance has cluster support disabled")
	}
	if len(args) == 0 {
		return proto.NewError("ERR wrong number of arguments for 'CLUSTER' command")
	}
	clusterCmd := cluster.NewClusterCommands(h.Cluster)
	subcommandArgs := make([]string, len(args))
	for i, arg := range args {
		subcommandArgs[i] = string(arg)
	}
	result, err := clusterCmd.HandleCommand(subcommandArgs)
	if err != nil {
		return wrapLogError(err)
	}
	// 根据返回类型转换
	switch v := result.(type) {
	case string:
		// 使用 BulkString 以正确处理多行响应（如 CLUSTER INFO）
		return proto.NewBulkString([]byte(v))
	case int64:
		return proto.NewInteger(v)
	case []string:
		// 对于CLUSTER NODES，返回多行字符串
		return proto.NewBulkString([]byte(strings.Join(v, "\n")))
	case [][]interface{}:
		// 对于CLUSTER SLOTS，槽位信息
		// 格式：[[startSlot, endSlot, [host, port, nodeId]], ...]
		slotsResp := make([]proto.RESP, len(v))
		for i, slotEntry := range v {
			entry := make([]proto.RESP, len(slotEntry))
			for j, item := range slotEntry {
				if sub, ok := item.([]interface{}); ok {
					subEntry := make([]proto.RESP, len(sub))
					for k, subItem := range sub {
						subEntry[k] = proto.NewBulkString([]byte(fmt.Sprintf("%v", subItem)))
					}
					entry[j] = &proto.NestedArray{Elems: subEntry}
				} else {
					entry[j] = proto.NewBulkString([]byte(fmt.Sprintf("%v", item)))
				}
			}
			slotsResp[i] = &proto.NestedArray{Elems: entry}
		}
		return &proto.NestedArray{Elems: slotsResp}

	case []interface{}:
		entries := make([]proto.RESP, len(v))
		for i, item := range v {
			if sub, ok := item.([]interface{}); ok {
				subEntry := make([]proto.RESP, len(sub))
				for k, subItem := range sub {
					subEntry[k] = proto.NewBulkString([]byte(fmt.Sprintf("%v", subItem)))
				}
				entries[i] = &proto.NestedArray{Elems: subEntry}
			} else {
				entries[i] = proto.NewBulkString([]byte(fmt.Sprintf("%v", item)))
			}
		}
		return &proto.NestedArray{Elems: entries}

	default:
		return proto.NewSimpleString(fmt.Sprintf("%v", v))
	}

	// CONFIG 命令（用于 redis-benchmark 兼容性）
}
