package replication

import (
	"bytes"
	"fmt"
	"testing"
)

// respKeys 从 RESP 数组流中提取每个命令的 (命令名, key)——事件级标识（SET 形态的
// key = 第 2 参）。log 键值当前为标识性形式（② 全重放形式延期——a4 §10 附7 D4），
// 因此探针在事件级比对（命令名 + key 序列），不做全字节比对。
func respKeys(data []byte) [][2]string {
	var out [][2]string
	i := 0
	for i < len(data) {
		if data[i] != '*' {
			break
		}
		i++ // past '*'
		// 解析数组参数个数 N
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
			for i < len(data) && !(data[i] == '\r' && i+1 < len(data) && data[i+1] == '\n') {
				i++
			}
			if a < 2 {
				args = append(args, string(data[argStart:i]))
			}
			i += 2 // past arg '\r\n'
		}
		if len(args) >= 2 {
			out = append(out, [2]string{args[0], args[1]})
		}
	}
	return out
}

// TestReplLogShadowDualWriteConsistency（读侧探针——§10 附7 分级-1）：
// 影子双写一致性的事件级比对——每写经 store 提交（→ 传播日志键 REPLLOG_）与
// PropagateCommand（→ backlog 字节流）双轨记录——验证：① 日志键条目数 == 传播命令数
// （1:1）；② 日志值事件序列（cmd, key）== backlog 命令事件序列（事件级一致）；③ 日志键
// ts 升序。注：log 值为标识性形式（② 全重放形式延期）——此处不做全字节比对。
func TestReplLogShadowDualWriteConsistency(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	rm := NewReplicationManager(s)
	defer rm.Stop()

	const n = 25
	var expected [][2]string
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("probe:key:%d", i)
		if err := s.Set(k, "v"); err != nil {
			t.Fatal(err)
		}
		rm.PropagateCommand([][]byte{[]byte("SET"), []byte(k), []byte("v")})
		expected = append(expected, [2]string{"SET", k})
	}

	// 读侧 A：传播日志键（ts 升序）
	entries, err := s.ReplLogEntries()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != n {
		t.Fatalf("repl log entries = %d, want %d", len(entries), n)
	}
	for i := 1; i < len(entries); i++ {
		if entries[i].TS <= entries[i-1].TS {
			t.Fatalf("log ts not ascending at %d: %d <= %d", i, entries[i].TS, entries[i-1].TS)
		}
	}
	var logValues []byte
	for _, e := range entries {
		logValues = append(logValues, e.Value...)
	}
	logEvents := respKeys(logValues)
	if len(logEvents) != n {
		t.Fatalf("log event count = %d, want %d", len(logEvents), n)
	}
	for i, ev := range logEvents {
		if ev != expected[i] {
			t.Fatalf("log event %d = %v, want %v", i, ev, expected[i])
		}
	}

	// 读侧 B：backlog 全窗口字节流——事件级与 log 键一致
	backlog := rm.backlog
	cur := backlog.GetCurrentOffset()
	backlogBytes, err := backlog.GetRange(0, cur)
	if err != nil {
		t.Fatal(err)
	}
	backlogEvents := respKeys(backlogBytes)
	if len(backlogEvents) != n {
		t.Fatalf("backlog event count = %d, want %d", len(backlogEvents), n)
	}
	for i, ev := range backlogEvents {
		if ev != expected[i] {
			t.Fatalf("backlog event %d = %v, want %v", i, ev, expected[i])
		}
	}
	if !bytes.Equal(logValues, backlogBytes) {
		t.Logf("shadow dual-write byte-level differs (expected: log values are identifying-form until the S2 form increment)")
	}
}
