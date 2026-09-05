package replication

import (
	"fmt"
	"strings"
	"testing"

	"github.com/lbp0200/BoltDB/internal/store"
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

// TestVerifyFeedTSContinuity 验证 log 键 ts 空洞检测（KVrocks「迭代器离散即断开」
// 模式——2026-09-06）：跳变条目（中间 ts 缺失）→ 明确错误；连续条目 → nil。
// 纯函数单测（不依赖 db——无需真实空洞构造——首条边界不校验）。
func TestVerifyFeedTSContinuity(t *testing.T) {
	t.Parallel()
	entry := func(ts uint64) store.ReplLogEntry { return store.ReplLogEntry{TS: ts} }

	// 连续：nil
	if err := verifyFeedTSContinuity([]store.ReplLogEntry{entry(1), entry(2), entry(3)}); err != nil {
		t.Fatalf("continuous entries: unexpected error: %v", err)
	}
	// 空/单条：nil（无跳变可检）
	if err := verifyFeedTSContinuity(nil); err != nil {
		t.Fatalf("empty entries: unexpected error: %v", err)
	}
	if err := verifyFeedTSContinuity([]store.ReplLogEntry{entry(7)}); err != nil {
		t.Fatalf("single entry: unexpected error: %v", err)
	}
	// 首条边界不校验（since/logStartTS 合法 gap）：[3,4,5] 连续 → nil
	if err := verifyFeedTSContinuity([]store.ReplLogEntry{entry(3), entry(4), entry(5)}); err != nil {
		t.Fatalf("offset-continuous entries: unexpected error: %v", err)
	}

	// 空洞（中间缺失）：明确错误含 ts gap + 期望 ts（空洞 ts=2 缺失——报 ts=3 expected 2）
	err := verifyFeedTSContinuity([]store.ReplLogEntry{entry(1), entry(3), entry(4)})
	if err == nil {
		t.Fatal("gap entries: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "ts gap") {
		t.Fatalf("gap entries: unexpected error: %v", err)
	}
	if !strings.Contains(err.Error(), "expected 2") {
		t.Fatalf("gap entries: error missing expected ts: %v", err)
	}
}

// TestCheckFeedTSGap 验证从侧帧 ts 连续性检测（lost 定论修复——2026-09-06——
// KVrocks「迭代器离散即断开」模式从侧版）：prevTS==0（首帧）跳过；连续（ts==
// prevTS+1）通过；跳变（ts != prevTS+1）报错（含期望 ts——断开重连补发覆盖空洞）。
func TestCheckFeedTSGap(t *testing.T) {
	t.Parallel()
	// 首帧（prevTS==0——初始/字节模式）：跳过检查（logStartTS 边界合法 gap）
	if err := checkFeedTSGap(0, 3); err != nil {
		t.Fatalf("first frame (prevTS=0): unexpected error: %v", err)
	}
	// 连续（ts == prevTS+1）：通过
	if err := checkFeedTSGap(99, 100); err != nil {
		t.Fatalf("continuous frame: unexpected error: %v", err)
	}
	// 跳变（ts != prevTS+1）：报错（含期望 ts）
	err := checkFeedTSGap(99, 101)
	if err == nil {
		t.Fatal("gap frame: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "ts gap") {
		t.Fatalf("gap frame: unexpected error: %v", err)
	}
	if !strings.Contains(err.Error(), "expected 100") {
		t.Fatalf("gap frame: error missing expected ts: %v", err)
	}
}
