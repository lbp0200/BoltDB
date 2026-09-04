package replication

import (
	"fmt"
	"testing"
)

// TestFeedEntriesFromAlignment 验证 master 侧 sender 的事件对齐与增量切片：
// 每写（Set → log 键 + PropagateCommand → backlog）后，FeedEntriesFrom 按请求 ts
// 返回 REPLLOG wire 条目——值与 backlog 全命令事件 1:1 对齐（协议相位值源——
// a4 §10 附7 feed 传输设计）。
func TestFeedEntriesFromAlignment(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	rm := NewReplicationManager(s)
	defer rm.Stop()

	const n = 10
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("feed:key:%d", i)
		if err := s.Set(k, "v"); err != nil {
			t.Fatal(err)
		}
		rm.PropagateCommand([][]byte{[]byte("SET"), []byte(k), []byte("v")})
	}

	entries, err := s.ReplLogEntries()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != n {
		t.Fatalf("log entries = %d, want %d", len(entries), n)
	}

	// 全量（since=0）：n 条 wire 条目——值 = log 键自身 D4 全重放命令（零对齐值源——
	// SET k v 含值参数——可 apply 的完整形式）
	wire, err := rm.FeedEntriesFrom(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(wire) != n {
		t.Fatalf("wire entries = %d, want %d", len(wire), n)
	}
	for i, args := range wire {
		argBytes := make([][]byte, len(args))
		for j, a := range args {
			argBytes[j] = []byte(a)
		}
		ts, cmd, err := feedEntryParse(argBytes)
		if err != nil {
			t.Fatal(err)
		}
		if ts != entries[i].TS {
			t.Fatalf("wire[%d] ts = %d, want %d", i, ts, entries[i].TS)
		}
		// 值 = backlog 全命令（SET k v——含值参数——可 apply 的完整形式）
		if len(cmd) != 3 || cmd[0] != "SET" || cmd[1] != fmt.Sprintf("feed:key:%d", i) || cmd[2] != "v" {
			t.Fatalf("wire[%d] cmd = %v, want [SET feed:key:%d v]", i, cmd, i)
		}
	}

	// 增量（since = 中点 ts）：仅后半段（含中点的 ts）
	midTS := entries[n/2].TS
	wireInc, err := rm.FeedEntriesFrom(midTS)
	if err != nil {
		t.Fatal(err)
	}
	if len(wireInc) != n-n/2 {
		t.Fatalf("incremental wire entries = %d, want %d", len(wireInc), n-n/2)
	}
	for i, args := range wireInc {
		argBytes := make([][]byte, len(args))
		for j, a := range args {
			argBytes[j] = []byte(a)
		}
		ts, _, err := feedEntryParse(argBytes)
		if err != nil {
			t.Fatal(err)
		}
		if ts != entries[n/2+i].TS {
			t.Fatalf("incremental wire[%d] ts = %d, want %d", i, ts, entries[n/2+i].TS)
		}
	}
}
