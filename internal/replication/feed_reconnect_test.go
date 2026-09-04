package replication

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"testing"

	"github.com/lbp0200/BoltDB/internal/proto"
)

// TestFeedModeReconnectResume 验证 feed-mode 重连的无丢失契约（S2 PSYNC-ts ④）：
// 从侧断连后（lastAppliedTS = resumeTS 水位），重连经 PSYNC-ts 4 参 → CONTINUE
// （res.TS == resumeTS——ts 域 catch-up 起点）——gap 由 CatchUpAndEnableSlaveTS
// 从 resumeTS+1 起经 FeedSlave 补发（log 键值源零对齐——不再走字节 SendBacklogData，
// byte 坐标错域问题结构性消除——见 9435523 根因记录）。
// 保留断连窗口 gap 可恢复性核验：FeedEntriesFrom(resumeTS+1)（严格大于——已 apply 的
// 不重发）覆盖 m 条 gap，与 backlog 断连窗口事件事件级等价（replay 守卫式对齐核验）。
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

	// 重连：PSYNC-ts（从侧带 lastAppliedTS=resumeTS）——ts 域判定 → CONTINUE
	replID := rm.GetReplicationID()
	res, err := HandlePSync(rm, replID, 0, resumeTS)
	if err != nil {
		t.Fatal(err)
	}
	if res.FullResync {
		t.Fatalf("resume ts=%d in-range should CONTINUE, got FULLRESYNC", resumeTS)
	}
	if res.TS != resumeTS {
		t.Fatalf("continue TS = %d, want resumeTS %d", res.TS, resumeTS)
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

// TestFeedModeReconnectTsCatchUp 验证 feed 模式重连的 **ts 域增量 catch-up**
// （CatchUpAndEnableSlaveTS——S2 分级-3 治本路径）：从侧断连后（lastAppliedTS =
// resumeTS），断连窗口新写 m 条 gap；重连经 HandlePSync → CONTINUE（res.TS ==
// resumeTS），然后 CatchUpAndEnableSlaveTS 从 resumeTS+1 起经 FeedSlave 同步补发
// REPLLOG 帧——从侧逐帧读到 [resumeTS+1, resumeTS+m]（严格升序——已 apply 的不
// 重发）——命令与 gap 写入一一对应（无丢失无重复）——catch-up 后从侧 Ready。
func TestFeedModeReconnectTsCatchUp(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	rm := NewReplicationManager(s)
	rm.SetRole(RoleMaster)
	rm.SetFeedLoop(true)
	defer rm.Stop()

	const n = 10
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("catchup:k:%d", i)
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
	resumeTS := entries[len(entries)-1].TS

	// 断连窗口写（gap——5 条）
	const m = 5
	for i := n; i < n+m; i++ {
		k := fmt.Sprintf("catchup:k:%d", i)
		if err := s.Set(k, "v"); err != nil {
			t.Fatal(err)
		}
		rm.PropagateCommand([][]byte{[]byte("SET"), []byte(k), []byte("v")})
	}

	// 重连判定：ts 域 → CONTINUE（res.TS == resumeTS）
	replID := rm.GetReplicationID()
	res, err := HandlePSync(rm, replID, 0, resumeTS)
	if err != nil {
		t.Fatal(err)
	}
	if res.FullResync {
		t.Fatalf("resume ts=%d in-range should CONTINUE, got FULLRESYNC", resumeTS)
	}
	if res.TS != resumeTS {
		t.Fatalf("continue TS = %d, want resumeTS %d", res.TS, resumeTS)
	}

	// 从侧连接 + ts 域 catch-up：同步补发 gap 帧
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	sc := NewSlaveConnection(server)
	rm.AddSlave(sc) // Ready=false——catch-up 期间 PropagateCommand 不 live-push

	type frame struct {
		ts  uint64
		cmd []string
	}
	frames := make(chan frame, m)
	errCh := make(chan error, 1)
	go func() {
		reader := bufio.NewReader(client)
		for i := 0; i < m; i++ {
			resp, err := proto.ReadRESP(reader)
			if err != nil {
				errCh <- fmt.Errorf("frame %d read: %w", i, err)
				return
			}
			args := make([]string, len(resp.Args))
			for j, a := range resp.Args {
				args[j] = string(a)
			}
			if len(args) < 3 || args[0] != feedEntryCommand {
				errCh <- fmt.Errorf("frame %d: not REPLLOG wire: %v", i, args)
				return
			}
			ts, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				errCh <- fmt.Errorf("frame %d ts parse: %w", i, err)
				return
			}
			frames <- frame{ts: ts, cmd: args[2:]}
		}
		errCh <- nil
	}()

	if err := rm.CatchUpAndEnableSlaveTS(sc, res.TS); err != nil {
		t.Fatalf("ts catch-up failed: %v", err)
	}
	if !sc.IsReady() {
		t.Fatal("slave should be Ready after ts catch-up")
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}

	// 帧核验：ts 从 resumeTS+1 严格升序——命令 == gap 写入（无丢失无重复）
	lastTS := resumeTS
	for i := 0; i < m; i++ {
		fr := <-frames
		wantTS := resumeTS + uint64(i) + 1
		if fr.ts != wantTS {
			t.Fatalf("frame %d ts = %d, want %d", i, fr.ts, wantTS)
		}
		if fr.ts <= lastTS {
			t.Fatalf("frame %d ts %d not strictly ascending (prev %d)", i, fr.ts, lastTS)
		}
		lastTS = fr.ts
		want := []string{"SET", fmt.Sprintf("catchup:k:%d", n+i), "v"}
		if len(fr.cmd) != len(want) {
			t.Fatalf("frame %d cmd %v, want %v", i, fr.cmd, want)
		}
		for j := range want {
			if fr.cmd[j] != want[j] {
				t.Fatalf("frame %d cmd[%d] %q, want %q", i, j, fr.cmd[j], want[j])
			}
		}
	}

	// 游标推进：feedSinceTS == resumeTS+m+1（后续 live-push 无缝续传——无重发）
	if got := sc.FeedSinceTS(); got != resumeTS+uint64(m)+1 {
		t.Fatalf("feedSinceTS after catch-up = %d, want %d", got, resumeTS+uint64(m)+1)
	}
}
