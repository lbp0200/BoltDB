package replication

import (
	"fmt"
	"strconv"
)

// FeedEntryCommand 是 log-key 增量流（S2 分级 2/3——a4 §10 附7 feed 传输设计）的
// wire 命令类型：master 将日志键条目以 REPLLOG 命令发送——从侧识别该命令类型后
// 拆出 ts + 全命令并 apply。
//
// wire 形态（flattened——与 RESP 自然对齐）：REPLLOG <ts> <cmd> <arg>...
//   - ts 为十进制字符串（日志键 ts——直接主侧 ts 域）；
//   - <cmd> <arg>... 为全命令参数（协议相位值为 backlog 事件对齐的既有全命令——
//     从侧 executeReplicatedCommand 直接消费该参数切片）。
//
// 定案理由：flattened（非嵌套 bulk）——从侧拆出 ts 后参数切片即命令本身
// （args[0] = 命令名——executeReplicatedCommand 无需重解析/重编码）；嵌套 bulk
// 需二次 RESP 解析（proto.ReadRESP）且主侧需整条重编码——无收益。

//nolint:unused // 预接线 wire 原语——仅测试使用（feed 协议相位 ② 集成后消费）
const feedEntryCommand = "REPLLOG"

// feedEntryArgs 构造 REPLLOG 命令参数：REPLLOG <ts> <cmd> <arg>...
// 主侧发送侧使用（按从侧请求 ts 增量扫描日志键后逐条组帧）。
//
//nolint:unused // 预接线 wire 原语——仅测试使用（feed 协议相位 ② 集成后消费）
func feedEntryArgs(ts uint64, command []string) []string {
	args := make([]string, 0, len(command)+2)
	args = append(args, feedEntryCommand, strconv.FormatUint(ts, 10))
	return append(args, command...)
}

// feedEntryParse 解析 REPLLOG 命令参数（从侧接收侧使用）：
// 返回 (ts, 全命令参数)——全命令参数切片可直接交 executeReplicatedCommand。
//
//nolint:unused // 预接线 wire 原语——仅测试使用（feed 协议相位 ② 集成后消费）
func feedEntryParse(args [][]byte) (uint64, []string, error) {
	if len(args) < 3 || string(args[0]) != feedEntryCommand {
		return 0, nil, fmt.Errorf("not a %s feed entry: %d args", feedEntryCommand, len(args))
	}
	ts, err := strconv.ParseUint(string(args[1]), 10, 64)
	if err != nil {
		return 0, nil, fmt.Errorf("invalid feed ts %q: %w", args[1], err)
	}
	cmd := make([]string, 0, len(args)-2)
	for _, a := range args[2:] {
		cmd = append(cmd, string(a))
	}
	return ts, cmd, nil
}
