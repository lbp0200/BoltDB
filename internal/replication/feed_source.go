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

// FeedEntriesFrom 按请求 ts 增量构造 REPLLOG wire 条目（master 侧发送面的读取基础）：
// 日志键（ReplLogEntries——ts 升序）与 backlog 命令事件按绝对位置 1:1 对齐（协议相位
// 值源 = backlog 影子对齐的全命令——backlog 保持权威——②/D4 部署前无需日志键全值）。
// 返回逐条 wire 参数（feedEntryArgs 形态——可直接序列化发送）。
func (rm *ReplicationManager) FeedEntriesFrom(since uint64) ([][]string, error) {
	all, err := rm.store.ReplLogEntries()
	if err != nil {
		return nil, err
	}
	backlog := rm.backlog
	cur := backlog.GetCurrentOffset()
	raw, err := backlog.GetRange(0, cur)
	if err != nil {
		return nil, err
	}
	events := parseCommandEvents(raw)
	if len(events) < len(all) {
		return nil, fmt.Errorf("backlog events %d < repl log entries %d (alignment broken)", len(events), len(all))
	}
	start := 0
	for start < len(all) && all[start].TS < since {
		start++
	}
	out := make([][]string, 0, len(all)-start)
	for i := start; i < len(all); i++ {
		out = append(out, feedEntryArgs(all[i].TS, events[i]))
	}
	return out, nil
}

// FeedSlave 对处于 feed 模式的从侧增量发送 REPLLOG wire 条目（S2 backlog 退役首步——
// 实际流发送）：FeedEntriesFrom(feedSinceTS) 构造增量（log 键 ts 升序 + backlog 对齐
// 全命令——backlog 值源）→ 逐条序列化发送（REPLLOG 帧——SendCommand）→ 游标推进到
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
