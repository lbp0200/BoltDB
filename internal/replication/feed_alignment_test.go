package replication

import (
	"strings"
	"testing"
)

// TestParseReplLogValue 验证零对齐值源的确定性解析（D4——2026-09-04 全族落地后
// 对齐硬化退役）：log 键值（RESP 编码全重放命令）→ 完整命令参数；不可解析值 → 报错；
// 参数不足（len < 2）→ 报错。
func TestParseReplLogValue(t *testing.T) {
	t.Parallel()

	// 完整命令：SET recon:k:5 v1（D4 全重放值）→ 3 参命令
	cmd, err := parseReplLogValue(serializeCommand([][]byte{[]byte("SET"), []byte("recon:k:5"), []byte("v1")}))
	if err != nil {
		t.Fatalf("valid log value should parse: %v", err)
	}
	if len(cmd) != 3 || cmd[0] != "SET" || cmd[1] != "recon:k:5" || cmd[2] != "v1" {
		t.Fatalf("parsed cmd = %v, want [SET recon:k:5 v1]", cmd)
	}

	// 不可解析值（非 RESP 编码）→ 报错
	if _, err := parseReplLogValue([]byte("not-a-resp-value")); err == nil {
		t.Fatal("unparseable log value should error")
	} else if !strings.Contains(err.Error(), "unparseable") {
		t.Fatalf("unexpected error: %v", err)
	}

	// 参数不足（len < 2——无键）→ 报错
	if _, err := parseReplLogValue(serializeCommand([][]byte{[]byte("PING")})); err == nil {
		t.Fatal("short command should error")
	}
}
