package replication

import (
	"fmt"
	"sort"

	"github.com/lbp0200/BoltDB/internal/store"
)

// ReplConversionTable 是 backlog 字节域 ↔ 日志键 ts 域的过渡期换算表
// （a4 §10 附7 定案——"非 ACK 判据的运行期依赖——过渡期验证锚 + 升级桥"）：
// 每 backlog 条目（事件）的起始字节偏移 ↔ 其命令日志键 ts 的双向映射——
// 由事件对齐构建（parseCommandEvents(backlog 字节) 事件序列 ↔ ReplLogEntries
// 日志键 ts 升序序列——D4 后日志键值为全重放命令——逐事件等价核验）。
// 用途：(i) 双轨一致性验证锚（守卫重写——ts 单调/重放 + 4 守卫以表核验）；
// (ii) 半升级窗口（部分从侧仍字节 offset）的 PSYNC 续传桥（offset ↔ ts 换算）。
// 运行期判据全 ts 化后（backlog 退役阶段 2），表降为测试工具。
type ReplConversionTable struct {
	// offsets[i] = 事件 i 的起始字节偏移（backlog 域——严格递增）
	offsets []int64
	// tss[i] = 事件 i 的命令日志键 ts（ts 域——严格递增）
	tss []uint64
	// cmds[i] = 事件 i 的全命令参数（backlog 事件——对齐核验保留）
	cmds [][]string
	// currentOffset = 建表时的 backlog 水位（GetCurrentOffset）
	currentOffset int64
}

// BuildReplConversionTable 构建换算表：backlog 全窗口字节（GetRange(0, cur)）
// → parseCommandEvents 事件序列；日志键 ts 序列（ReplLogEntries——D4 全重放值
// → parseReplLogValue 全命令）；逐事件对齐核验命令等价（双轨一致性——错位即返回
// 错误——验证锚语义：并发 commit/append 分叉在此现形）。
func BuildReplConversionTable(backlog *ReplicationBacklog, st *store.BotreonStore) (*ReplConversionTable, error) {
	cur := backlog.GetCurrentOffset()
	// 空 backlog（水位 0——无任何写入）：GetRange(0,0) 无可用窗口会报
	// "offset too new"——直接返回空表（0 事件 == 0 日志键——双轨一致）。
	if cur == 0 {
		return &ReplConversionTable{
			offsets:       []int64{},
			tss:           []uint64{},
			cmds:          [][]string{},
			currentOffset: 0,
		}, nil
	}
	raw, err := backlog.GetRange(0, cur)
	if err != nil {
		return nil, fmt.Errorf("conversion table: backlog range: %w", err)
	}
	events := parseCommandEvents(raw)
	entries, err := st.ReplLogEntries()
	if err != nil {
		return nil, fmt.Errorf("conversion table: repl log entries: %w", err)
	}
	if len(events) != len(entries) {
		return nil, fmt.Errorf("conversion table: event count mismatch: backlog=%d log-keys=%d (dual-track divergence)", len(events), len(entries))
	}

	t := &ReplConversionTable{
		offsets:       make([]int64, 0, len(events)),
		tss:           make([]uint64, 0, len(events)),
		cmds:          make([][]string, 0, len(events)),
		currentOffset: cur,
	}
	off := int64(0)
	for i, ev := range events {
		logCmd, lerr := parseReplLogValue(entries[i].Value)
		if lerr != nil {
			return nil, fmt.Errorf("conversion table: log value parse at idx=%d: %w", i, lerr)
		}
		// 事件对齐核验：backlog 命令 == 日志键全重放命令（D4 后两者同为全命令 RESP）
		if len(ev) != len(logCmd) {
			return nil, fmt.Errorf("conversion table: alignment mismatch at idx=%d: backlog cmd len=%d log-key len=%d", i, len(ev), len(logCmd))
		}
		for j := range logCmd {
			if ev[j] != logCmd[j] {
				return nil, fmt.Errorf("conversion table: alignment mismatch at idx=%d arg=%d: backlog=%q log-key=%q", i, j, ev[j], logCmd[j])
			}
		}
		t.offsets = append(t.offsets, off)
		t.tss = append(t.tss, entries[i].TS)
		t.cmds = append(t.cmds, ev)
		off += int64(len(serializeCommand(toByteArgs(ev))))
	}
	return t, nil
}

// toByteArgs 将字符串命令参数转为 [][]byte（serializeCommand 输入）。
func toByteArgs(args []string) [][]byte {
	out := make([][]byte, len(args))
	for i, a := range args {
		out[i] = []byte(a)
	}
	return out
}

// Count 返回换算表事件数（backlog 条目数 == 日志键数）。
func (t *ReplConversionTable) Count() int {
	if t == nil {
		return 0
	}
	return len(t.tss)
}

// CurrentOffset 返回建表时的 backlog 水位。
func (t *ReplConversionTable) CurrentOffset() int64 {
	if t == nil {
		return 0
	}
	return t.currentOffset
}

// OffsetAt / TSAt 按事件索引访问（i ∈ [0, Count())）。
func (t *ReplConversionTable) OffsetAt(i int) int64 {
	if t == nil || i < 0 || i >= len(t.offsets) {
		return 0
	}
	return t.offsets[i]
}

func (t *ReplConversionTable) TSAt(i int) uint64 {
	if t == nil || i < 0 || i >= len(t.tss) {
		return 0
	}
	return t.tss[i]
}

// OffsetToTS 换算：backlog 字节偏移 → 命令 ts。offset 落在事件 [i, i+1) 的字节
// 区间内（含事件起始点——半开区间语义）即映射到事件 i 的 ts；超出水位时回退到
// 最后一事件的 ts（升级桥语义——已由后续增量流覆盖）。
func (t *ReplConversionTable) OffsetToTS(offset int64) uint64 {
	if t == nil || len(t.tss) == 0 {
		return 0
	}
	i := sort.Search(len(t.offsets), func(i int) bool { return t.offsets[i] > offset })
	if i == 0 {
		return t.tss[0]
	}
	return t.tss[i-1]
}

// TSAtOffset 换算：backlog 字节偏移 → ts（OffsetToTS 别名——升级桥语义一致）。
func (t *ReplConversionTable) TSAtOffset(offset int64) uint64 {
	return t.OffsetToTS(offset)
}

// TSToOffset 换算：命令 ts → backlog 条目起始字节偏移（ts 不在表中时回退到
// 首个 >= 该 ts 的条目偏移——升级桥续传起点语义）。表空返回 0。
func (t *ReplConversionTable) TSToOffset(ts uint64) int64 {
	if t == nil || len(t.tss) == 0 {
		return 0
	}
	i := sort.Search(len(t.tss), func(i int) bool { return t.tss[i] >= ts })
	if i >= len(t.tss) {
		return t.offsets[len(t.offsets)-1]
	}
	return t.offsets[i]
}

// AlignCheck 双轨一致性核验（验证锚）：换算表事件数 vs 当前日志键数——不等即
// 双轨分叉（并发 commit/append 序错位——missing=2499 类根因的早期检测面）。
// 返回 (日志键数, 是否一致)。
func (t *ReplConversionTable) AlignCheck(st *store.BotreonStore) (int, bool) {
	if t == nil {
		return 0, false
	}
	entries, err := st.ReplLogEntries()
	if err != nil {
		return 0, false
	}
	return len(entries), len(entries) == len(t.tss)
}
