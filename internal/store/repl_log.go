package store

import (
	"encoding/binary"
	"fmt"
)

// 复制传播日志键（S2——D 定案——kvrocks 式 log-in-commit，a4 §10 附6）：
// 每命令的传播日志条目与数据变更在**同一 commitTS 事务**写入（同 ts = 天然绑定——
// 无分发侧打标、无竞态）。日志键 = replLogPrefix + 8 字节大端 ts——键序即 ts 序
// （读侧前缀扫描即按 ts 排水——backlog/PSYNC/ACK 迁移 ts 记账的数据源）。

var replLogPrefix = []byte("REPLLOG_")

// replLogKey 构造传播日志键：前缀 + ts 大端（badger 键字节序 = ts 升序）。
func replLogKey(ts uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], ts)
	k := make([]byte, 0, len(replLogPrefix)+8)
	k = append(k, replLogPrefix...)
	return append(k, b[:]...)
}

// encodePropagateCommand 将命令参数编码为 RESP 数组字节（与复制层 serializeCommand
// 同格式——复制层日志键消费者可直接解码重放）。格式在消费者增量前保持冻结。
func encodePropagateCommand(args ...[]byte) []byte {
	var b []byte
	b = append(b, '*')
	b = append(b, fmt.Sprintf("%d", len(args))...)
	b = append(b, '\r', '\n')
	for _, a := range args {
		b = append(b, '$')
		b = append(b, fmt.Sprintf("%d", len(a))...)
		b = append(b, '\r', '\n')
		b = append(b, a...)
		b = append(b, '\r', '\n')
	}
	return b
}
