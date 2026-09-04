package replication

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"testing"

	"github.com/lbp0200/BoltDB/internal/proto"
)

// TestFeedLoopIncrementalSend 验证 feed-mode 从侧的增量流发送（S2 backlog 退役首步）：
// 写序列 → PropagateCommand 的 per-slave 循环对 feed-enabled 从侧走 FeedSlave（REPLLOG
// wire 增量——逐条组帧 + 游标推进 feedSinceTS = 最后已发 ts+1）——客户端逐帧读到
// REPLLOG <ts> <SET k v>——ts 严格升序——命令与写入一一对应——非 feed 从侧不受影响。
func TestFeedLoopIncrementalSend(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	rm := NewReplicationManager(s)
	defer rm.Stop()
	rm.SetRole(RoleMaster)
	rm.SetFeedLoop(true)

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	sc := NewSlaveConnection(server)
	sc.SetReady(true)
	sc.FeedSetEnabled(true, 0)
	rm.AddSlave(sc)

	const n = 8
	type frame struct {
		ts  uint64
		cmd []string
	}
	frames := make(chan frame, n)
	errCh := make(chan error, 1)
	go func() {
		reader := bufio.NewReader(client)
		for i := 0; i < n; i++ {
			resp, err := proto.ReadRESP(reader)
			if err != nil {
				errCh <- fmt.Errorf("frame %d read: %w", i, err)
				return
			}
			arr := resp
			args := make([]string, len(arr.Args))
			for j, a := range arr.Args {
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

	for i := 0; i < n; i++ {
		k := fmt.Sprintf("feed:k:%d", i)
		if err := s.Set(k, "v"); err != nil {
			t.Fatal(err)
		}
		rm.PropagateCommand([][]byte{[]byte("SET"), []byte(k), []byte("v")})
	}

	var lastTS uint64
	for i := 0; i < n; i++ {
		fr := <-frames
		if i > 0 && fr.ts <= lastTS {
			t.Fatalf("frame %d ts %d not ascending (prev %d)", i, fr.ts, lastTS)
		}
		lastTS = fr.ts
		want := []string{"SET", fmt.Sprintf("feed:k:%d", i), "v"}
		if len(fr.cmd) != len(want) {
			t.Fatalf("frame %d cmd %v, want %v", i, fr.cmd, want)
		}
		for j := range want {
			if fr.cmd[j] != want[j] {
				t.Fatalf("frame %d cmd[%d] %q, want %q", i, j, fr.cmd[j], want[j])
			}
		}
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	if got := sc.FeedSinceTS(); got != lastTS+1 {
		t.Fatalf("feed cursor = %d, want %d (last sent ts + 1)", got, lastTS+1)
	}
}
