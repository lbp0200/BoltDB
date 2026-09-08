package replication

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/zeebo/assert"
)

// TestCatchUpAndEnableSlaveTS_EmptyGapSetsReady 验证 ts 域 catch-up 空 gap：无写入时
// CatchUpAndEnableSlaveTS(slave, 0) → FeedSlave 读 [1, curTS] 为空 → 直接返回 nil——
// slave 翻 Ready，游标停在 resumeTS+1=1（无已发条目）。
func TestCatchUpAndEnableSlaveTS_EmptyGapSetsReady(t *testing.T) {
	t.Parallel()
	rm := NewReplicationManager(setupTestStore(t))
	defer rm.Stop()

	slave := NewSlaveConnection(newMockConn())
	rm.AddSlave(slave)

	if err := rm.CatchUpAndEnableSlaveTS(slave, 0); err != nil {
		t.Fatalf("empty-gap catch-up: %v", err)
	}
	assert.True(t, slave.IsReady())
	assert.Equal(t, uint64(1), slave.FeedSinceTS()) // resumeTS+1，无已发条目 → 游标停在 1
}

// TestCatchUpAndEnableSlaveTS_SendFailureReturnsErr 验证 ts 域 catch-up 发送失败：feed
// 写入失败时方法返回 error（供调用方摘除）。注意——Ready 在 propMu 内原子翻 true（早于
// feed，防与 live-push 交错），故失败不重置 Ready；从侧摘除是调用方责任（生产路径
// replication_handler.go 两处 catch-up 失败即 RemoveSlave）。此守卫断言：err 非空 + 从侧
// 仍在管理列表（方法自身不摘除）。
func TestCatchUpAndEnableSlaveTS_SendFailureReturnsErr(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	rm := NewReplicationManager(s)
	defer rm.Stop()

	if err := s.Set("k", "v"); err != nil {
		t.Fatal(err)
	}
	rm.PropagateCommand([][]byte{[]byte("SET"), []byte("k"), []byte("v")})
	assert.True(t, rm.GetMasterReplOffset() > 0)

	conn := newMockConn()
	conn.writeErr = fmt.Errorf("write boom")
	slave := NewSlaveConnection(conn)
	rm.AddSlave(slave)

	err := rm.CatchUpAndEnableSlaveTS(slave, 0)
	assert.True(t, err != nil)
	assert.Equal(t, 1, rm.GetSlaveCount()) // 方法不摘除——调用方责任
}

// TestCatchUpAndEnableSlaveTS_ConcurrentPropagateNoDupNoHole 验证 ts 域并发补发无重复
// 无空洞（lost 家族守卫）：预填 prefill 条 → 以"预填后 currentTS"为 resume 点做 ts catch-up
// （FeedSlave 读 [resumeTS+1, curTS]）→ 并发 writers 持续 PropagateCommand（feed-only，
// 全部经 FeedSlave 走共享 feedSinceTS 游标——单调推进 ⇒ 每条恰好一次）→ 补发窗口与 live
// push 的并集覆盖所有并发命令、无重复、无空洞。
func TestCatchUpAndEnableSlaveTS_ConcurrentPropagateNoDupNoHole(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	rm := NewReplicationManager(s)
	defer rm.Stop()

	const (
		prefill   = 20
		writers   = 8
		perWriter = 40
	)
	for i := 0; i < prefill; i++ {
		if err := s.Set(fmt.Sprintf("pre:%d", i), "1"); err != nil {
			t.Fatal(err)
		}
	}
	resumeTS, _ := s.ReplLogCurrentTS()
	assert.True(t, resumeTS > 0)

	conn := newMockConn()
	slave := NewSlaveConnection(conn)
	rm.AddSlave(slave)
	assert.False(t, slave.IsReady())

	var (
		wg      sync.WaitGroup
		nextKey atomic.Int64
	)
	wg.Add(writers)
	for w := 0; w < writers; w++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				n := nextKey.Add(1)
				key := fmt.Sprintf("live:%d", n)
				// 镜像生产路径：真实 store 写（推进 ts）→ PropagateCommand（feed 推给从侧）。
				if err := s.Set(key, "1"); err != nil {
					t.Error(err)
					return
				}
				rm.PropagateCommand([][]byte{
					[]byte("SET"),
					[]byte(key),
					[]byte("1"),
				})
			}
		}()
	}

	if err := rm.CatchUpAndEnableSlaveTS(slave, resumeTS); err != nil {
		t.Fatalf("catch-up: %v", err)
	}
	wg.Wait()

	assert.True(t, slave.IsReady())
	assert.Equal(t, int64(0), rm.GetReplSendDropCount())

	got := parseFeedCommands(t, conn.writeBuffer)
	want := writers * perWriter
	if len(got) != want {
		t.Fatalf("catch-up+live stream: got %d commands, want %d (dup or hole)", len(got), want)
	}
	seen := make(map[string]int, want)
	var lastTS uint64
	for _, f := range got {
		if f.ts < lastTS {
			t.Fatalf("feed ts regression: %d < %d (must be ascending)", f.ts, lastTS)
		}
		lastTS = f.ts
		if len(f.cmd) != 3 || f.cmd[0] != "SET" {
			t.Fatalf("unexpected command on slave stream: %v", f.cmd)
		}
		key := f.cmd[1]
		seen[key]++
		if seen[key] > 1 {
			t.Fatalf("command for %s delivered twice (gap-fill raced with live push)", key)
		}
	}
	if len(seen) != want {
		t.Fatalf("unique keys=%d want=%d", len(seen), want)
	}
}

// feedFrame 是一条已解析的 REPLLOG wire 帧（ts + 内层命令）。
type feedFrame struct {
	ts  uint64
	cmd []string
}

// parseFeedCommands 解析 feed wire 帧序列（每帧 [REPLLOG, ts, cmd...]），返回 (ts + 内层命令)。
func parseFeedCommands(t *testing.T, data []byte) []feedFrame {
	t.Helper()
	r := bufio.NewReader(bytes.NewReader(data))
	var out []feedFrame
	for {
		resp, err := proto.ReadRESP(r)
		if err != nil {
			if err == io.EOF {
				return out
			}
			t.Fatalf("parse slave stream after %d frames: %v", len(out), err)
		}
		args := make([]string, len(resp.Args))
		for i, a := range resp.Args {
			args[i] = string(a)
		}
		if len(args) < 3 || args[0] != feedEntryCommand {
			t.Fatalf("not a REPLLOG frame: %v", args)
		}
		ts, err := strconv.ParseUint(args[1], 10, 64)
		if err != nil {
			t.Fatalf("frame ts parse: %v", err)
		}
		out = append(out, feedFrame{ts: ts, cmd: args[2:]})
	}
}
