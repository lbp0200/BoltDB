package replication

import (
	"fmt"
	"testing"
)

// TestFeedEntryRoundtrip 验证 REPLLOG wire 形态（flattened：REPLLOG <ts> <cmd>
// <arg>...）的构造与解析往返一致（a4 §10 附7 feed 传输设计——协议相位基础）。
func TestFeedEntryRoundtrip(t *testing.T) {
	t.Parallel()
	cases := []struct {
		ts      uint64
		command []string
	}{
		{1, []string{"SET", "k1", "v1"}},
		{42, []string{"HSET", "hash:key", "f1", "v1", "f2", "v2"}},
		{0, []string{"DEL", "gone"}},
		{18446744073709551615, []string{"SET", "max", "ts"}}, // 最大 uint64 ts
	}
	for _, c := range cases {
		name := fmt.Sprintf("ts=%d", c.ts)
		t.Run(name, func(t *testing.T) {
			args := feedEntryArgs(c.ts, c.command)
			if len(args) != len(c.command)+2 {
				t.Fatalf("feed entry args = %d, want %d", len(args), len(c.command)+2)
			}
			if args[0] != feedEntryCommand {
				t.Fatalf("feed entry cmd = %q, want %q", args[0], feedEntryCommand)
			}
			argBytes := make([][]byte, len(args))
			for i, a := range args {
				argBytes[i] = []byte(a)
			}
			ts, cmd, err := feedEntryParse(argBytes)
			if err != nil {
				t.Fatal(err)
			}
			if ts != c.ts {
				t.Fatalf("ts = %d, want %d", ts, c.ts)
			}
			if len(cmd) != len(c.command) {
				t.Fatalf("cmd len = %d, want %d", len(cmd), len(c.command))
			}
			for i := range cmd {
				if cmd[i] != c.command[i] {
					t.Fatalf("cmd[%d] = %q, want %q", i, cmd[i], c.command[i])
				}
			}
		})
	}
}

// TestFeedEntryParseInvalid 验证非法 REPLLOG 输入的拒绝路径。
func TestFeedEntryParseInvalid(t *testing.T) {
	t.Parallel()
	bad := [][][]byte{
		{[]byte("REPLLOG"), []byte("not-a-ts"), []byte("SET"), []byte("k")},
		{[]byte("SET"), []byte("1"), []byte("k"), []byte("v")},
		{[]byte("REPLLOG"), []byte("1")},
		{},
	}
	for _, args := range bad {
		if _, _, err := feedEntryParse(args); err == nil {
			t.Fatalf("expected error for args %q", args)
		}
	}
}
