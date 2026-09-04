package replication

import (
	"strconv"
	"testing"
	"time"
)

// TestD4ZeroAlignmentProbe 验证零对齐 feed 值源可行性（D4 首批——string 族——a4 §10
// 附7 落点）：log 键值已升级为全重放命令（SET key value / SET key value PXAT <ms> /
// INCRBY key delta——encodePropagateCommand/StringArgs 产物）——parseCommandEvents
// 直接给出完整命令参数——无需 backlog 事件对齐（并发 commit 序 vs append 序分叉
// 问题的零对齐解法）——与 feedEntryParse 兼容（可直接组帧 REPLLOG wire）。
func TestD4ZeroAlignmentProbe(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	rm := NewReplicationManager(s)
	rm.SetRole(RoleMaster)
	defer rm.Stop()

	// D4 全重放写（string 族——无 TTL / PXAT 绝对 TTL / 数值增量）
	if err := s.Set("d4:set", "v1"); err != nil {
		t.Fatal(err)
	}
	rm.PropagateCommand([][]byte{[]byte("SET"), []byte("d4:set"), []byte("v1")})
	if err := s.SetWithTTL("d4:ttl", "v2", 100*time.Second); err != nil {
		t.Fatal(err)
	}
	rm.PropagateCommand([][]byte{[]byte("SET"), []byte("d4:ttl"), []byte("v2")})
	if _, err := s.INCRBY("d4:incr", 5); err != nil {
		t.Fatal(err)
	}
	rm.PropagateCommand([][]byte{[]byte("INCRBY"), []byte("d4:incr"), []byte("5")})

	entries, err := s.ReplLogEntries()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatalf("log entries = %d, want 3", len(entries))
	}

	// 每条 log 键值解析 = 完整命令（D4 零对齐值源——无需 backlog 事件）
	wantCommands := [][]string{
		{"SET", "d4:set", "v1"},
		{"SET", "d4:ttl", "v2", "PXAT"}, // 第 4 参 PXAT——第 5 参为绝对 deadline（ms——动态值）
		{"INCRBY", "d4:incr", "5"},
	}
	for i, e := range entries {
		idArgs := parseCommandEvents(e.Value)
		if len(idArgs) != 1 {
			t.Fatalf("entry %d: value not a single command: %q", i, string(e.Value))
		}
		cmd := idArgs[0]
		want := wantCommands[i]
		if len(cmd) < len(want) {
			t.Fatalf("entry %d cmd %v, want prefix %v (full replay value)", i, cmd, want)
		}
		for j := range want {
			if cmd[j] != want[j] {
				t.Fatalf("entry %d arg %d: %q != %q (full replay value)", i, j, cmd[j], want[j])
			}
		}
		// PXAT 条目：第 5 参 = 绝对 deadline（ms——正整数——数值校验）
		if len(want) > 3 && want[3] == "PXAT" {
			ms, err := strconv.ParseInt(cmd[4], 10, 64)
			if err != nil || ms <= 0 {
				t.Fatalf("entry %d PXAT deadline %q not a positive ms value", i, cmd[4])
			}
		}
	}

	// 零对齐构造可行性：log 值解析的命令可直接组帧（ts + 命令——feedEntryArgs 形态）
	for _, e := range entries {
		idArgs := parseCommandEvents(e.Value)
		if len(idArgs) != 1 || len(idArgs[0]) < 2 {
			t.Fatalf("log value %q not frameable (need command + key)", string(e.Value))
		}
	}
}
