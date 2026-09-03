package replication

import (
	"fmt"
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
//
//nolint:unused // 预接线 sender——仅测试使用（feed 协议相位 ③ PSYNC-ts 集成后消费）
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
