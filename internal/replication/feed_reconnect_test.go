package replication

import (
	"fmt"
	"testing"
)

// TestFeedModeReconnectResume 验证 feed-mode 的无丢失重连（退役决策正确性前置）：
// 从侧断连后（lastAppliedTS = resumeTS 水位），重连经 PSYNC-ts 4 参（④——整数边界
// [logStartTS, currentTS]）→ CONTINUE（非 FULLRESYNC——无丢失）+ res.TS == resumeTS；
// 断连窗口的 feed 续传 = FeedEntriesFrom(resumeTS+1)（严格大于——已 apply 的不重发）
// 覆盖 gap（m 条——无丢失无重复）——与 backlog 断连窗口事件事件级等价（replay 守卫式
// 对齐核验——resume 点在事件索引 5——gap 从事件 6 起）。
func TestFeedModeReconnectResume(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	rm := NewReplicationManager(s)
	rm.SetRole(RoleMaster)
	rm.SetFeedLoop(true)
	defer rm.Stop()

	const n = 12
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("recon:k:%d", i)
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
	resumeTS := entries[len(entries)-1].TS // 断连时从侧已 apply 全部初始写入（末条 ts 水位）

	// 断连窗口写（从侧断开期间的 gap——6 条新写）
	const m = 6
	for i := n; i < n+m; i++ {
		k := fmt.Sprintf("recon:k:%d", i)
		if err := s.Set(k, "v"); err != nil {
			t.Fatal(err)
		}
		rm.PropagateCommand([][]byte{[]byte("SET"), []byte(k), []byte("v")})
	}

	// 重连：PSYNC-ts（从侧带 lastAppliedTS=resumeTS）——ts 模式边界判定
	replID := rm.GetReplicationID()
	res, err := HandlePSync(rm, replID, 0, resumeTS)
	if err != nil {
		t.Fatal(err)
	}
	if res.FullResync {
		t.Fatalf("resume ts=%d in-range should CONTINUE, got FULLRESYNC", resumeTS)
	}
	if res.TS != resumeTS {
		t.Fatalf("continue TS = %d, want %d", res.TS, resumeTS)
	}

	// 断连窗口的 feed 续传：严格大于 resumeTS（已 apply 的不重发）——覆盖 gap m 条
	wire, err := rm.FeedEntriesFrom(resumeTS + 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(wire) != m {
		t.Fatalf("gap feed entries = %d, want %d (disconnect window)", len(wire), m)
	}

	// 事件级等价：gap 的 feed 命令 == backlog 断连窗口事件（resume 点后对齐）
	raw, err := rm.backlog.GetRange(0, rm.backlog.GetCurrentOffset())
	if err != nil {
		t.Fatal(err)
	}
	events := parseCommandEvents(raw)
	if len(events) != n+m {
		t.Fatalf("backlog events = %d, want %d", len(events), n+m)
	}
	for i := range wire {
		argBytes := make([][]byte, len(wire[i]))
		for j, a := range wire[i] {
			argBytes[j] = []byte(a)
		}
		_, cmd, err := feedEntryParse(argBytes)
		if err != nil {
			t.Fatal(err)
		}
		want := events[n+i] // resume 点在事件索引 n-1（末条已 apply）——gap 从事件 n 起
		if len(cmd) != len(want) {
			t.Fatalf("gap entry %d cmd %v, want %v", i, cmd, want)
		}
		for j := range want {
			if cmd[j] != want[j] {
				t.Fatalf("gap entry %d arg %d: %q != %q (no-loss alignment)", i, j, cmd[j], want[j])
			}
		}
	}
}
