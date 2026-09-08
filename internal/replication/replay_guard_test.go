package replication

import (
	"fmt"
	"testing"
)

// TestTSReplayEquivalence 验证 ts 重放守卫（feed REPLLOG wire 事件序列的完整性）：
// 写入 n 条命令后，日志键回放（FeedEntriesFrom）返回 n 帧——每帧 ts 严格升序 +
// 命令参数与写入一一对应。环已退役——原"字节 backlog 回放等价 + 换算表 AlignCheck"
// 双轨核验锚失效；此守卫保留其核心不变量：feed wire 的 ts-ascending + 命令完整性。
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

	// 日志键回放：feed wire（ts + 全命令）
	wire, err := rm.FeedEntriesFrom(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(wire) != n {
		t.Fatalf("feed entries = %d, want %d", len(wire), n)
	}
	var lastTS uint64
	for i, args := range wire {
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
		want := []string{"SET", fmt.Sprintf("replay:key:%d", i), "v"}
		if len(cmd) != len(want) {
			t.Fatalf("event %d arg count: got %d want %d", i, len(cmd), len(want))
		}
		for j := range want {
			if cmd[j] != want[j] {
				t.Fatalf("event %d arg %d: got %q want %q", i, j, cmd[j], want[j])
			}
		}
	}
}
