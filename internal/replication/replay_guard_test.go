package replication

import (
	"fmt"
	"testing"
)

// TestTSReplayEquivalence 验证 ts 重放守卫（§10 附7 验证门槛——backlog 退役前的双轨
// 核验锚）：日志键回放（feed REPLLOG wire 事件序列——ts + 全命令）== 字节 backlog
// 回放（命令事件序列）事件级等价——每事件命令参数逐项一致 + 日志键 ts 严格升序。
func TestTSReplayEquivalence(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	rm := NewReplicationManager(s)
	defer rm.Stop()

	const n = 15
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("replay:key:%d", i)
		if err := s.Set(k, "v"); err != nil {
			t.Fatal(err)
		}
		rm.PropagateCommand([][]byte{[]byte("SET"), []byte(k), []byte("v")})
	}

	// 日志键回放：feed wire（ts + 全命令——backlog 事件对齐值源）
	wire, err := rm.FeedEntriesFrom(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(wire) != n {
		t.Fatalf("feed entries = %d, want %d", len(wire), n)
	}
	var logReplay [][]string
	var lastTS uint64
	for _, args := range wire {
		argBytes := make([][]byte, len(args))
		for j, a := range args {
			argBytes[j] = []byte(a)
		}
		ts, cmd, err := feedEntryParse(argBytes)
		if err != nil {
			t.Fatal(err)
		}
		if ts < lastTS {
			t.Fatalf("feed ts regression: %d < %d (ts must be ascending)", ts, lastTS)
		}
		lastTS = ts
		logReplay = append(logReplay, cmd)
	}

	// 字节 backlog 回放：命令事件序列
	raw, err := rm.backlog.GetRange(0, rm.backlog.GetCurrentOffset())
	if err != nil {
		t.Fatal(err)
	}
	backlogReplay := parseCommandEvents(raw)
	if len(backlogReplay) != n {
		t.Fatalf("backlog replay = %d, want %d", len(backlogReplay), n)
	}

	// 事件级等价：日志键回放命令 == backlog 回放命令（逐事件逐参数）
	for i := range logReplay {
		if len(logReplay[i]) != len(backlogReplay[i]) {
			t.Fatalf("event %d arg count: log %d != backlog %d", i, len(logReplay[i]), len(backlogReplay[i]))
		}
		for j := range logReplay[i] {
			if logReplay[i][j] != backlogReplay[i][j] {
				t.Fatalf("event %d arg %d: log %q != backlog %q", i, j, logReplay[i][j], backlogReplay[i][j])
			}
		}
	}

	// 换算表核验（a4 §10 附7——验证锚接入守卫）：事件对齐构建成功（上述等价性的
	// 强形式——offset↔ts 双向映射建表）+ AlignCheck 双轨一致（事件数 == 日志键数）。
	tbl, err := BuildReplConversionTable(rm.GetBacklog(), s)
	if err != nil {
		t.Fatalf("conversion table build failed in replay guard: %v", err)
	}
	if tbl.Count() != n {
		t.Fatalf("conversion table count = %d, want %d", tbl.Count(), n)
	}
	if cnt, ok := tbl.AlignCheck(s); !ok || cnt != n {
		t.Fatalf("conversion table AlignCheck = (%d, %v), want (%d, true)", cnt, ok, n)
	}
}
