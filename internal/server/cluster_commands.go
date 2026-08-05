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
		// 标准 Redis 格式：[[start(integer), end(integer), [host, port(integer), nodeId]], ...]
		// 注意：start/end/port 必须是 RESP integer，字符串会让严格客户端
		// （redis-benchmark --cluster）解析失败。
		slotsResp := make([]proto.RESP, len(v))
		for i, slotEntry := range v {
			entry := make([]proto.RESP, len(slotEntry))
			for j, item := range slotEntry {
				if sub, ok := item.([]interface{}); ok {
					subEntry := make([]proto.RESP, len(sub))
					for k, subItem := range sub {
						subEntry[k] = respValue(subItem)
					}
					entry[j] = &proto.NestedArray{Elems: subEntry}
				} else {
					entry[j] = respValue(item)
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
					subEntry[k] = respValue(subItem)
				}
				entries[i] = &proto.NestedArray{Elems: subEntry}
			} else {
				entries[i] = respValue(item)
			}
		}
		return &proto.NestedArray{Elems: entries}

	default:
		return proto.NewSimpleString(fmt.Sprintf("%v", v))
	}

	// CONFIG 命令（用于 redis-benchmark 兼容性）
}

// respValue 将 interface{} 编码为 RESP 值：int64 → integer，
// []interface{} → 嵌套数组（递归），其余 → bulk string。
func respValue(v interface{}) proto.RESP {
	switch t := v.(type) {
	case int64:
		return proto.NewInteger(t)
	case []interface{}:
		elems := make([]proto.RESP, len(t))
		for i, item := range t {
			elems[i] = respValue(item)
		}
		return &proto.NestedArray{Elems: elems}
	default:
		return proto.NewBulkString([]byte(fmt.Sprintf("%v", v)))
	}
}
