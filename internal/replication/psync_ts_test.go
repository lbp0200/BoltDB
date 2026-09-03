package replication

import (
	"fmt"
	"testing"

	"github.com/zeebo/assert"
)

// TestPSyncTSRange 验证 PSYNC-ts 整数边界判定（④——a4 §10 附7 落点深化）：
// ts 模式（第 4 参 > 0）——ts ∈ [logStartTS, currentTS] 内 = CONTINUE（增量）；
// 越界 = FULLRESYNC；FULLRESYNC 结果携带主侧 currentTS。
func TestPSyncTSRange(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	rm := NewReplicationManager(s)
	rm.SetRole(RoleMaster)
	defer rm.Stop()

	const n = 10
	for i := 0; i < n; i++ {
		if err := s.Set(fmt.Sprintf("psyncts:key:%d", i), "v"); err != nil {
			t.Fatal(err)
		}
		rm.PropagateCommand([][]byte{[]byte("SET"), []byte(fmt.Sprintf("psyncts:key:%d", i)), []byte("v")})
	}

	replID := rm.GetReplicationID()
	entries, err := s.ReplLogEntries()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != n {
		t.Fatalf("log entries = %d, want %d", len(entries), n)
	}
	firstTS := entries[0].TS
	lastTS := entries[len(entries)-1].TS

	// 边界内（含两端）→ CONTINUE（增量同步）
	for _, ts := range []uint64{firstTS, lastTS} {
		res, err := HandlePSync(rm, replID, 0, ts)
		assert.NoError(t, err)
		assert.NotNil(t, res)
		if res.FullResync {
			t.Fatalf("ts=%d in range [%d,%d] should CONTINUE, got FULLRESYNC", ts, firstTS, lastTS)
		}
		if res.TS != ts {
			t.Fatalf("continue result TS = %d, want %d", res.TS, ts)
		}
	}

	// 越界（ts < logStartTS）→ FULLRESYNC
	res, err := HandlePSync(rm, replID, 0, firstTS-1)
	assert.NoError(t, err)
	assert.NotNil(t, res)
	if !res.FullResync {
		t.Fatalf("ts=%d below log start %d should FULLRESYNC", firstTS-1, firstTS)
	}
	if res.TS != lastTS {
		t.Fatalf("fullresync result TS = %d, want current ts %d", res.TS, lastTS)
	}

	// 字节模式（ts=0——旧从节点）→ 走原字节判定（replId 匹配 + offset 0 → CONTINUE）
	res0, err := HandlePSync(rm, replID, 0, 0)
	assert.NoError(t, err)
	assert.NotNil(t, res0)
	if res0.FullResync {
		t.Fatal("byte-mode (ts=0) with matching replId and offset 0 should CONTINUE")
	}
}
