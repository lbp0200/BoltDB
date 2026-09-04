package replication

import (
	"strings"
	"testing"
)

// TestVerifyFeedAlignment 验证对齐硬化的确定性检测（2026-09-04 规模验证 missing=2499
// 根因的即时修复）：log 键标识符值（RESP 编码命令名+键——encodePropagateCommand 产物）
// 与 backlog 事件命令+键一致 → nil；并发分叉（错误关联）→ 报错；不可解析值 → 报错。
func TestVerifyFeedAlignment(t *testing.T) {
	t.Parallel()

	// 一致：标识符（SET recon:k:5）vs 事件（SET recon:k:5 v1）——命令+键匹配
	idValue := serializeCommand([][]byte{[]byte("SET"), []byte("recon:k:5")})
	if err := verifyFeedAlignment(idValue, []string{"SET", "recon:k:5", "v1"}); err != nil {
		t.Fatalf("matching identifier should pass: %v", err)
	}

	// 并发分叉：标识符（SET recon:k:5）vs 事件（SET recon:k:7 v1）——键错位→检测
	diverged := serializeCommand([][]byte{[]byte("SET"), []byte("recon:k:5")})
	if err := verifyFeedAlignment(diverged, []string{"SET", "recon:k:7", "v1"}); err == nil {
		t.Fatal("misaligned event should be detected (key mismatch)")
	} else if !strings.Contains(err.Error(), "command/key mismatch") {
		t.Fatalf("unexpected error: %v", err)
	}

	// 命令错位：标识符（SET）vs 事件（DEL）——命令不匹配→检测
	cmdMismatch := serializeCommand([][]byte{[]byte("SET"), []byte("recon:k:5")})
	if err := verifyFeedAlignment(cmdMismatch, []string{"DEL", "recon:k:5"}); err == nil {
		t.Fatal("command mismatch should be detected")
	}

	// 不可解析值（非 RESP 编码）→ 报错
	if err := verifyFeedAlignment([]byte("not-a-resp-value"), []string{"SET", "k"}); err == nil {
		t.Fatal("unparseable identifier value should error")
	}

	// 事件参数不足（len < 2）→ 报错
	if err := verifyFeedAlignment(idValue, []string{"SET"}); err == nil {
		t.Fatal("short event should error")
	}
}
