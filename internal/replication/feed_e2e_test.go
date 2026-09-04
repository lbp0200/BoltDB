package replication

import (
	"bufio"
	"fmt"
	"net"
	"testing"
	"time"
)

// TestFeedModeEndToEnd 验证 feed-mode 的整链路闭环（S2 backlog 退役首步——实际流发送的
// 端到端验证）：master（SetFeedLoop + CatchUpAndEnableSlave 自动激活 feed-mode）→
// REPLLOG wire 增量 → 从侧 readCommandLoop（REPLLOG 分支 apply）→ 数据收敛 +
// lastAppliedTS 推进。同步 pipe：master 写阻塞至从侧读——循环结束后从侧必已全部
// apply——收敛断言确定性成立。
func TestFeedModeEndToEnd(t *testing.T) {
	t.Parallel()

	masterStore := setupTestStore(t)
	masterRM := NewReplicationManager(masterStore)
	defer masterRM.Stop()
	masterRM.SetRole(RoleMaster)
	masterRM.SetFeedLoop(true)

	slaveStore := setupTestStore(t)
	slaveRM := NewReplicationManager(slaveStore)
	defer slaveRM.Stop()
	slaveRM.SetRole(RoleSlave)

	serverEnd, clientEnd := net.Pipe()
	defer serverEnd.Close()
	defer clientEnd.Close()

	// 从侧 readCommandLoop 先启动（同步 pipe——master 侧写入会阻塞至从侧读取）
	mc := &MasterConnection{
		Addr:   "127.0.0.1:6337",
		Conn:   clientEnd,
		Reader: bufio.NewReader(clientEnd),
		Writer: bufio.NewWriter(clientEnd),
		stopCh: make(chan struct{}),
	}
	sr := NewSlaveReconnector(slaveRM, slaveStore, "127.0.0.1:6337")
	sr.state.Store(int32(SlaveConnected))
	done := make(chan error, 1)
	go func() { done <- sr.readCommandLoop(mc) }()

	// master 侧从连接 + feed-mode 自动激活（CatchUpAndEnableSlave——propMu 下
	// FeedSetEnabled(true, curTS+1)——空 backlog 时无 catch-up 字节）
	sc := NewSlaveConnection(serverEnd)
	masterRM.AddSlave(sc)
	if err := masterRM.CatchUpAndEnableSlave(sc, 0); err != nil {
		t.Fatalf("catch-up: %v", err)
	}
	if !sc.FeedIsEnabled() {
		t.Fatal("feed-mode not auto-activated (SetFeedLoop + CatchUpAndEnableSlave)")
	}

	// master 写序列（feed-mode：REPLLOG 增量——非 feed 字节路径关闭——避免双 apply）
	const n = 10
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("feed:e2e:%d", i)
		if err := masterStore.Set(k, "v"); err != nil {
			t.Fatal(err)
		}
		masterRM.PropagateCommand([][]byte{[]byte("SET"), []byte(k), []byte("v")})
	}

	// 收敛：轮询等待从侧 apply 完成（readCommandLoop 的 apply 异步于 pipe 读——
	// 同步 pipe 只保证帧被读走而非已 apply——-race 下时序放宽——最终收敛断言不变）。
	deadline := time.Now().Add(5 * time.Second)
	for {
		allOK := true
		for i := 0; i < n; i++ {
			v, err := slaveStore.Get(fmt.Sprintf("feed:e2e:%d", i))
			if err != nil || v != "v" {
				allOK = false
				break
			}
		}
		if allOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("slave convergence timeout (n=%d)", n)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// lastAppliedTS 轮询推进到最后 feed 条目的 ts（== master 当前 log 水位）
	masterCurTS, err := masterStore.ReplLogCurrentTS()
	if err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for sr.lastAppliedTS.Load() != masterCurTS {
		if time.Now().After(deadline) {
			t.Fatalf("lastAppliedTS = %d, want %d (master log water)", sr.lastAppliedTS.Load(), masterCurTS)
		}
		time.Sleep(20 * time.Millisecond)
	}

	close(sr.stopCh)
	clientEnd.Close()
	if err := <-done; err != nil {
		t.Fatalf("readCommandLoop: %v", err)
	}
}
