package replication

import (
	"fmt"
	"strconv"
)

// parseCommandEvents 解析 RESP 命令流为逐命令参数切片（全参数——feed 值源对齐用）。
// 与 backlog 序列化（serializeCommand）互逆——命令事件 = backlog 全窗口字节的事件级
// 切分（a4 §10 附7 feed 传输设计——协议相位值源 = backlog 影子事件对齐的全命令）。
func parseCommandEvents(data []byte) [][]string {
	var out [][]string
	i := 0
	for i < len(data) {
		if data[i] != '*' {
			break
		}
		i++ // past '*'
		var nArgs int
		digits := i
		for i < len(data) && data[i] >= '0' && data[i] <= '9' {
			i++
		}
		for d := digits; d < i; d++ {
			nArgs = nArgs*10 + int(data[d]-'0')
		}
		if i >= len(data) || data[i] != '\r' || i+1 >= len(data) || data[i+1] != '\n' {
			break
		}
		i += 2 // past count line '\r\n'
		var args []string
		for a := 0; a < nArgs && i < len(data); a++ {
			if data[i] != '$' {
				break
			}
			i++ // past '$'
			for i < len(data) && data[i] != '\r' {
				i++
			}
			i += 2 // past length '\r\n'
			argStart := i
			for i < len(data) && (data[i] != '\r' || i+1 >= len(data) || data[i+1] != '\n') {
				i++
			}
			args = append(args, string(data[argStart:i]))
			i += 2 // past arg '\r\n'
		}
		if len(args) >= 1 {
			out = append(out, args)
		}
	}
	return out
}

// parseReplLogValue 解析 log 键值（D4 全重放命令——encodePropagateCommand/StringArgs
// 产物——RESP 编码）为命令参数——零对齐 feed 值源（无需 backlog 事件对齐——并发
// commit 序 vs append 序分叉问题消失——每个 log 键携带自身完整命令）。
func parseReplLogValue(logValue []byte) ([]string, error) {
	idArgs := parseCommandEvents(logValue)
	if len(idArgs) != 1 || len(idArgs[0]) < 2 {
		return nil, fmt.Errorf("unparseable repl log value %q", string(logValue))
	}
	return idArgs[0], nil
}

// FeedEntriesFrom 按请求 ts 增量构造 REPLLOG wire 条目（master 侧发送面的读取基础）：
// 值源 = log 键自身值（D4——全重放命令——零对齐——无 backlog 事件读取/对齐）——并发
// 写者下 commit 序 vs append 序分叉问题消失（D4 前该分叉导致错误关联——2026-09-04
// 规模验证 missing=2499——对齐硬化 528c236 为过渡安全网——D4 全族落地后退役）。
func (rm *ReplicationManager) FeedEntriesFrom(since uint64) ([][]string, error) {
	all, err := rm.store.ReplLogEntries()
	if err != nil {
		return nil, err
	}
	start := 0
	for start < len(all) && all[start].TS < since {
		start++
	}
	out := make([][]string, 0, len(all)-start)
	for i := start; i < len(all); i++ {
		args, err := parseReplLogValue(all[i].Value)
		if err != nil {
			return nil, fmt.Errorf("feed value parse at ts=%d: %w", all[i].TS, err)
		}
		out = append(out, feedEntryArgs(all[i].TS, args))
	}
	return out, nil
}

// FeedSlave 对处于 feed 模式的从侧增量发送 REPLLOG wire 条目（S2 backlog 退役首步——
// 实际流发送）：FeedEntriesFrom(feedSinceTS) 构造增量（log 键 ts 升序 + D4 全重放值
// 全命令——零对齐值源）→ 逐条序列化发送（REPLLOG 帧——SendCommand）→ 游标推进到
// 最后已发条目的 ts+1（ts 严格升序——下次增量续传）。非 feed 模式从侧返回 nil（走
// backlog 字节路径——PropagateCommand 分支）。
// 字节双轨说明：feed 帧（REPLLOG <ts> <cmd>...）字节与 backlog 命令字节不同——feed
// 模式从侧 lastOffset 允许漂移——停滞判据以 ts 形式为准（masterTS==0 旧主字节兜底
// 不受影响——feed 模式要求 ts 化主从）。全量扫描（O(n)）首步正确性优先——②/D4
// 部署时转 ReplLogEntriesFrom 增量 seek。
func (rm *ReplicationManager) FeedSlave(slave *SlaveConnection) error {
	if !slave.FeedIsEnabled() {
		return nil
	}
	since := slave.FeedSinceTS()
	entries, err := rm.FeedEntriesFrom(since)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}
	for _, args := range entries {
		argBytes := make([][]byte, len(args))
		for j, a := range args {
			argBytes[j] = []byte(a)
		}
		if err := slave.SendCommand(serializeCommand(argBytes), rm.GetMasterReplOffset()); err != nil {
			return err
		}
	}
	// 游标推进到最后已发条目的 ts+1（ts 在 wire 参数索引 1——跳过已发——增量续传）
	lastTS, err := strconv.ParseUint(entries[len(entries)-1][1], 10, 64)
	if err != nil {
		return fmt.Errorf("feed last ts parse: %w", err)
	}
	slave.FeedSetEnabled(true, lastTS+1)
	return nil
}
