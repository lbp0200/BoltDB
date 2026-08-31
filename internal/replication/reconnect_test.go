package replication

import (
	"bufio"
	"bytes"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/zeebo/assert"
)

func TestNewSlaveReconnector(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	sr := NewSlaveReconnector(rm, testStore, "127.0.0.1:6379")
	assert.Equal(t, SlaveDisconnected, sr.GetState())
	assert.Equal(t, "127.0.0.1:6379", sr.GetMasterAddr())
}

func TestSlaveReconnector_StartStop(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	sr := NewSlaveReconnector(rm, testStore, "127.0.0.1:59999")
	sr.Start()
	time.Sleep(50 * time.Millisecond)

	// Should be in connecting or disconnected state
	state := sr.GetState()
	assert.True(t, state == SlaveConnecting || state == SlaveDisconnected)

	sr.Stop()
	assert.Equal(t, SlaveDisconnected, sr.GetState())
}

func TestReconnectConfig_Defaults(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 0, DefaultReconnectConfig.MaxRetries)
	assert.Equal(t, 1*time.Second, DefaultReconnectConfig.BaseBackoff)
	assert.Equal(t, 60*time.Second, DefaultReconnectConfig.MaxBackoff)
	assert.Equal(t, 30*time.Second, DefaultReconnectConfig.ResetAfter)
}

func TestSlaveReconnector_GoroutineLeak(t *testing.T) {
	t.Parallel()
	before := runtime.NumGoroutine()

	for i := 0; i < 5; i++ {
		testStore := setupTestStore(t)
		rm := NewReplicationManager(testStore)
		sr := NewSlaveReconnector(rm, testStore, "127.0.0.1:59999")
		sr.Start()
		time.Sleep(10 * time.Millisecond)
		sr.Stop()
		rm.Stop()
		testStore.CloseWithTimeout(store.CloseTimeout)
		// Let BadgerDB background goroutines settle before next iteration.
		runtime.Gosched()
		time.Sleep(20 * time.Millisecond)
	}

	// Give goroutines time to fully exit (BadgerDB compaction/GC goroutines
	// may not terminate immediately after Close returns).
	for i := 0; i < 20; i++ {
		runtime.GC()
		runtime.Gosched()
		time.Sleep(10 * time.Millisecond)
		after := runtime.NumGoroutine()
		if after-before <= 15 {
			return
		}
	}
	after := runtime.NumGoroutine()
	t.Errorf("goroutine leak suspected: before=%d after=%d leaked=%d", before, after, after-before)
}

// Shutdown invariant (AGENTS.md): after ReplicationManager.Stop() returns, no
// goroutine may touch the store again — db.Close() happens later in the same
// sequence (main.go: replMgr.Stop() → cancel() → handler.Shutdown() →
// backupMgr.Wait() → db.Close()).
//
// For a replica that means Stop() must end the SlaveReconnector: the loop only
// exits on its stopCh, it retries forever (DefaultReconnectConfig.MaxRetries == 0)
// with a 1s base backoff, and each attempt runs tryReplicate → LoadRDB /
// executeReplicatedCommand against the store. Stop() used to leave it running,
// so a shutdown replica kept dialling its master and could apply to a closed
// store. The existing reconnector tests hide this because they call sr.Stop()
// explicitly, which proves the reconnector *can* be stopped, not that shutdown
// stops it.
func TestReplicationManagerStopStopsSlaveReconnector(t *testing.T) {
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	// A port nobody listens on: every attempt fails fast, so the loop settles
	// into its backoff timer and the next attempt is deterministic (~1s later).
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	deadAddr := ln.Addr().String()
	assert.NoError(t, ln.Close())

	assert.NoError(t, StartSlaveReplication(rm, testStore, deadAddr))

	// Hold the reconnector itself: a correct Stop() clears rm.slaveReconnector,
	// after which rm.GetReconnectCount() would report 0 and prove nothing.
	rm.mu.RLock()
	sr := rm.slaveReconnector
	rm.mu.RUnlock()
	assert.True(t, sr != nil)

	// Wait for the first attempt so the loop is provably inside its backoff.
	seen := false
	for i := 0; i < 100 && !seen; i++ {
		if sr.GetReconnectCount() > 0 {
			seen = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	assert.True(t, seen)

	countAtStop := sr.GetReconnectCount()
	rm.Stop()

	// 3s > BaseBackoff(1s): with the reconnector still running the counter is
	// guaranteed to advance in this window, so a passing assertion here is not a
	// race lottery.
	time.Sleep(3 * time.Second)

	if after := sr.GetReconnectCount(); after != countAtStop {
		t.Errorf("SlaveReconnector kept running after ReplicationManager.Stop(): reconnect attempts %d → %d "+
			"(it retries forever, and each attempt can reach the store after db.Close)", countAtStop, after)
	}
	if state := sr.GetState(); state != SlaveDisconnected {
		t.Errorf("SlaveReconnector state after Stop() = %v, want SlaveDisconnected", state)
	}
}

func TestSlaveReconnector_GetLastOffset(t *testing.T) {
	t.Parallel()
	sr := &SlaveReconnector{}

	assert.Equal(t, int64(0), sr.GetLastOffset())

	sr.lastOffset.Store(12345)
	assert.Equal(t, int64(12345), sr.GetLastOffset())
}

func TestSlaveReconnector_GetReconnectCount(t *testing.T) {
	t.Parallel()
	sr := &SlaveReconnector{}

	assert.Equal(t, int64(0), sr.GetReconnectCount())

	sr.reconnectCount.Store(7)
	assert.Equal(t, int64(7), sr.GetReconnectCount())
}

func TestSlaveReconnector_writeRespToMaster(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	mc := &MasterConnection{
		Writer: bufio.NewWriter(&buf),
		stopCh: make(chan struct{}),
	}

	sr := &SlaveReconnector{}
	err := sr.writeRespToMaster(mc, []byte("+PONG\r\n"))
	assert.NoError(t, err)
	assert.Equal(t, "+PONG\r\n", buf.String())
}

func TestSlaveReconnector_sendHandshake(t *testing.T) {
	t.Parallel()
	serverEnd, clientEnd := net.Pipe()
	defer serverEnd.Close()
	defer clientEnd.Close()

	mc := &MasterConnection{
		Addr:   "127.0.0.1:6379",
		Conn:   clientEnd,
		Reader: bufio.NewReader(clientEnd),
		Writer: bufio.NewWriter(clientEnd),
		stopCh: make(chan struct{}),
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		br := bufio.NewReader(serverEnd)
		for i := 0; i < 4; i++ {
			_, err := proto.ReadRESP(br)
			if err != nil {
				return
			}
			_, err = serverEnd.Write([]byte("+OK\r\n"))
			if err != nil {
				return
			}
		}
	}()

	sr := &SlaveReconnector{}
	err := sr.sendHandshake(mc)
	assert.NoError(t, err)
	<-done
}

func TestSlaveReconnector_sendHandshake_ReadError(t *testing.T) {
	t.Parallel()
	serverEnd, clientEnd := net.Pipe()
	defer serverEnd.Close()
	defer clientEnd.Close()

	mc := &MasterConnection{
		Addr:   "127.0.0.1:6379",
		Conn:   clientEnd,
		Reader: bufio.NewReader(clientEnd),
		Writer: bufio.NewWriter(clientEnd),
		stopCh: make(chan struct{}),
	}

	// Close server end immediately to cause read error on first response
	serverEnd.Close()

	sr := &SlaveReconnector{}
	err := sr.sendHandshake(mc)
	assert.Error(t, err)
}

func TestSlaveReconnector_sendPSYNC_FullResync(t *testing.T) {
	t.Parallel()
	serverEnd, clientEnd := net.Pipe()
	defer serverEnd.Close()
	defer clientEnd.Close()

	mc := &MasterConnection{
		Addr:   "127.0.0.1:6379",
		Conn:   clientEnd,
		Reader: bufio.NewReader(clientEnd),
		Writer: bufio.NewWriter(clientEnd),
		stopCh: make(chan struct{}),
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		br := bufio.NewReader(serverEnd)
		_, err := proto.ReadRESP(br)
		if err != nil {
			return
		}
		_, err = serverEnd.Write([]byte("+FULLRESYNC abc123 100\r\n"))
		if err != nil {
			return
		}
	}()

	sr := &SlaveReconnector{}
	fullResync, err := sr.sendPSYNC(mc)
	assert.NoError(t, err)
	assert.True(t, fullResync)
	assert.Equal(t, "abc123", sr.lastReplId)
	assert.Equal(t, int64(100), sr.lastOffset.Load())
	<-done
}

func TestSlaveReconnector_sendPSYNC_Continue(t *testing.T) {
	t.Parallel()
	serverEnd, clientEnd := net.Pipe()
	defer serverEnd.Close()
	defer clientEnd.Close()

	mc := &MasterConnection{
		Addr:   "127.0.0.1:6379",
		Conn:   clientEnd,
		Reader: bufio.NewReader(clientEnd),
		Writer: bufio.NewWriter(clientEnd),
		stopCh: make(chan struct{}),
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		br := bufio.NewReader(serverEnd)
		_, err := proto.ReadRESP(br)
		if err != nil {
			return
		}
		_, err = serverEnd.Write([]byte("+CONTINUE def456\r\n"))
		if err != nil {
			return
		}
	}()

	sr := &SlaveReconnector{}
	fullResync, err := sr.sendPSYNC(mc)
	assert.NoError(t, err)
	assert.False(t, fullResync)
	<-done
}

func TestSlaveReconnector_sendPSYNC_UnexpectedResponse(t *testing.T) {
	t.Parallel()
	serverEnd, clientEnd := net.Pipe()
	defer serverEnd.Close()
	defer clientEnd.Close()

	mc := &MasterConnection{
		Addr:   "127.0.0.1:6379",
		Conn:   clientEnd,
		Reader: bufio.NewReader(clientEnd),
		Writer: bufio.NewWriter(clientEnd),
		stopCh: make(chan struct{}),
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		br := bufio.NewReader(serverEnd)
		_, err := proto.ReadRESP(br)
		if err != nil {
			return
		}
		_, err = serverEnd.Write([]byte("-ERR unknown command\r\n"))
		if err != nil {
			return
		}
	}()

	sr := &SlaveReconnector{}
	_, err := sr.sendPSYNC(mc)
	assert.Error(t, err)
	<-done
}

func TestSlaveReconnector_readCommandLoop_PING(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	serverEnd, clientEnd := net.Pipe()
	defer serverEnd.Close()
	defer clientEnd.Close()

	mc := &MasterConnection{
		Addr:   "127.0.0.1:6379",
		Conn:   clientEnd,
		Reader: bufio.NewReader(clientEnd),
		Writer: bufio.NewWriter(clientEnd),
		stopCh: make(chan struct{}),
	}

	sr := NewSlaveReconnector(rm, testStore, "127.0.0.1:6379")
	sr.state.Store(int32(SlaveConnected))

	done := make(chan error, 1)
	go func() {
		done <- sr.readCommandLoop(mc)
	}()

	_, err := serverEnd.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	assert.NoError(t, err)

	br := bufio.NewReader(serverEnd)
	resp, err := proto.ReadRESP(br)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(resp.Args))
	assert.Equal(t, "PONG", string(resp.Args[0]))

	close(sr.stopCh)
	clientEnd.Close()
	err = <-done
	assert.NoError(t, err)
}

func TestSlaveReconnector_readCommandLoop_REPLCONF_GETACK(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	serverEnd, clientEnd := net.Pipe()
	defer serverEnd.Close()
	defer clientEnd.Close()

	mc := &MasterConnection{
		Addr:   "127.0.0.1:6379",
		Conn:   clientEnd,
		Reader: bufio.NewReader(clientEnd),
		Writer: bufio.NewWriter(clientEnd),
		stopCh: make(chan struct{}),
	}

	sr := NewSlaveReconnector(rm, testStore, "127.0.0.1:6379")
	sr.state.Store(int32(SlaveConnected))

	done := make(chan error, 1)
	go func() {
		done <- sr.readCommandLoop(mc)
	}()

	_, err := serverEnd.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"))
	assert.NoError(t, err)

	br := bufio.NewReader(serverEnd)
	resp, err := proto.ReadRESP(br)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(resp.Args))
	assert.Equal(t, "REPLCONF", string(resp.Args[0]))
	assert.Equal(t, "ACK", string(resp.Args[1]))
	assert.Equal(t, "0", string(resp.Args[2]))

	close(sr.stopCh)
	clientEnd.Close()
	err = <-done
	assert.NoError(t, err)
}

func TestSlaveReconnector_readCommandLoop_SELECT(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	serverEnd, clientEnd := net.Pipe()
	defer serverEnd.Close()
	defer clientEnd.Close()

	mc := &MasterConnection{
		Addr:   "127.0.0.1:6379",
		Conn:   clientEnd,
		Reader: bufio.NewReader(clientEnd),
		Writer: bufio.NewWriter(clientEnd),
		stopCh: make(chan struct{}),
	}

	sr := NewSlaveReconnector(rm, testStore, "127.0.0.1:6379")
	sr.state.Store(int32(SlaveConnected))

	done := make(chan error, 1)
	go func() {
		done <- sr.readCommandLoop(mc)
	}()

	_, err := serverEnd.Write([]byte("*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n"))
	assert.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	close(sr.stopCh)
	clientEnd.Close()
	err = <-done
	assert.NoError(t, err)
}

func TestSlaveReconnector_readCommandLoop_SET(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	serverEnd, clientEnd := net.Pipe()
	defer serverEnd.Close()
	defer clientEnd.Close()

	mc := &MasterConnection{
		Addr:   "127.0.0.1:6379",
		Conn:   clientEnd,
		Reader: bufio.NewReader(clientEnd),
		Writer: bufio.NewWriter(clientEnd),
		stopCh: make(chan struct{}),
	}

	sr := NewSlaveReconnector(rm, testStore, "127.0.0.1:6379")
	sr.state.Store(int32(SlaveConnected))

	done := make(chan error, 1)
	go func() {
		done <- sr.readCommandLoop(mc)
	}()

	_, err := serverEnd.Write([]byte("*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$5\r\nmyval\r\n"))
	assert.NoError(t, err)

	for i := 0; i < 50; i++ {
		val, err := testStore.Get("mykey")
		if err == nil {
			assert.Equal(t, "myval", val)
			goto done
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for SET to be processed")

done:
	close(sr.stopCh)
	clientEnd.Close()
	err = <-done
	assert.NoError(t, err)
}

func TestSlaveReconnector_readCommandLoop_ReadError(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	serverEnd, clientEnd := net.Pipe()
	defer serverEnd.Close()
	defer clientEnd.Close()

	mc := &MasterConnection{
		Addr:   "127.0.0.1:6379",
		Conn:   clientEnd,
		Reader: bufio.NewReader(clientEnd),
		Writer: bufio.NewWriter(clientEnd),
		stopCh: make(chan struct{}),
	}

	sr := NewSlaveReconnector(rm, testStore, "127.0.0.1:6379")
	sr.state.Store(int32(SlaveConnected))

	serverEnd.Close()

	err := sr.readCommandLoop(mc)
	assert.Error(t, err)
}

func TestSlaveReconnector_readCommandLoop_StopCh(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	serverEnd, clientEnd := net.Pipe()
	defer serverEnd.Close()
	defer clientEnd.Close()

	mc := &MasterConnection{
		Addr:   "127.0.0.1:6379",
		Conn:   clientEnd,
		Reader: bufio.NewReader(clientEnd),
		Writer: bufio.NewWriter(clientEnd),
		stopCh: make(chan struct{}),
	}

	sr := NewSlaveReconnector(rm, testStore, "127.0.0.1:6379")
	sr.state.Store(int32(SlaveConnected))

	close(sr.stopCh)

	err := sr.readCommandLoop(mc)
	assert.NoError(t, err)
}

func TestSlaveReconnector_readCommandLoop_OffsetTracking(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	serverEnd, clientEnd := net.Pipe()
	defer serverEnd.Close()
	defer clientEnd.Close()

	mc := &MasterConnection{
		Addr:   "127.0.0.1:6379",
		Conn:   clientEnd,
		Reader: bufio.NewReader(clientEnd),
		Writer: bufio.NewWriter(clientEnd),
		stopCh: make(chan struct{}),
	}

	sr := NewSlaveReconnector(rm, testStore, "127.0.0.1:6379")
	sr.state.Store(int32(SlaveConnected))

	done := make(chan error, 1)
	go func() {
		done <- sr.readCommandLoop(mc)
	}()

	_, err := serverEnd.Write([]byte("*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n"))
	assert.NoError(t, err)

	var offset int64
	for i := 0; i < 50; i++ {
		offset = sr.lastOffset.Load()
		if offset > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	assert.True(t, offset > 0)

	_, err = serverEnd.Write([]byte("*3\r\n$3\r\nSET\r\n$2\r\nk2\r\n$2\r\nv2\r\n"))
	assert.NoError(t, err)

	var offset2 int64
	for i := 0; i < 50; i++ {
		offset2 = sr.lastOffset.Load()
		if offset2 > offset {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	assert.True(t, offset2 > offset)

	close(sr.stopCh)
	clientEnd.Close()
	err = <-done
	assert.NoError(t, err)
}

func TestSlaveReconnector_readCommandLoop_MultipleCommands(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	serverEnd, clientEnd := net.Pipe()
	defer serverEnd.Close()
	defer clientEnd.Close()

	mc := &MasterConnection{
		Addr:   "127.0.0.1:6379",
		Conn:   clientEnd,
		Reader: bufio.NewReader(clientEnd),
		Writer: bufio.NewWriter(clientEnd),
		stopCh: make(chan struct{}),
	}

	sr := NewSlaveReconnector(rm, testStore, "127.0.0.1:6379")
	sr.state.Store(int32(SlaveConnected))

	done := make(chan error, 1)
	go func() {
		done <- sr.readCommandLoop(mc)
	}()

	br := bufio.NewReader(serverEnd)

	_, err := serverEnd.Write([]byte("*3\r\n$3\r\nSET\r\n$1\r\na\r\n$3\r\nAAA\r\n"))
	assert.NoError(t, err)

	for i := 0; i < 50; i++ {
		if _, err := testStore.Get("a"); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	_, err = serverEnd.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	assert.NoError(t, err)

	resp, err := proto.ReadRESP(br)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(resp.Args))
	assert.Equal(t, "PONG", string(resp.Args[0]))

	_, err = serverEnd.Write([]byte("*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$3\r\nBBB\r\n"))
	assert.NoError(t, err)

	for i := 0; i < 50; i++ {
		if _, err := testStore.Get("b"); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	valA, _ := testStore.Get("a")
	assert.Equal(t, "AAA", valA)
	valB, _ := testStore.Get("b")
	assert.Equal(t, "BBB", valB)

	close(sr.stopCh)
	clientEnd.Close()
	err = <-done
	assert.NoError(t, err)
}

func TestSlaveReconnector_readCommandLoop_ApplySkipIncrementsCount(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	serverEnd, clientEnd := net.Pipe()
	defer serverEnd.Close()
	defer clientEnd.Close()

	mc := &MasterConnection{
		Addr:   "127.0.0.1:6379",
		Conn:   clientEnd,
		Reader: bufio.NewReader(clientEnd),
		Writer: bufio.NewWriter(clientEnd),
		stopCh: make(chan struct{}),
	}

	sr := NewSlaveReconnector(rm, testStore, "127.0.0.1:6379")
	sr.state.Store(int32(SlaveConnected))

	done := make(chan error, 1)
	go func() {
		done <- sr.readCommandLoop(mc)
	}()

	// JSON.CLEAR on a missing key returns store.ErrKeyNotFound, which
	// isTransientReplicationError treats as skippable: offset advances,
	// store is unchanged, applySkipCount increments.
	skipCmd := [][]byte{[]byte("JSON.CLEAR"), []byte("nosuch")}
	skipBytes := serializeCommand(skipCmd)
	_, err := serverEnd.Write(skipBytes)
	assert.NoError(t, err)

	for i := 0; i < 50; i++ {
		if rm.GetReplApplySkipCount() == 1 && sr.lastOffset.Load() == int64(len(skipBytes)) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	assert.Equal(t, int64(1), rm.GetReplApplySkipCount())
	assert.Equal(t, int64(len(skipBytes)), sr.lastOffset.Load())

	setCmd := [][]byte{[]byte("SET"), []byte("after-skip"), []byte("ok")}
	setBytes := serializeCommand(setCmd)
	_, err = serverEnd.Write(setBytes)
	assert.NoError(t, err)

	for i := 0; i < 50; i++ {
		if _, gerr := testStore.Get("after-skip"); gerr == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	val, gerr := testStore.Get("after-skip")
	assert.NoError(t, gerr)
	assert.Equal(t, "ok", val)
	assert.Equal(t, int64(1), rm.GetReplApplySkipCount())
	assert.Equal(t, int64(len(skipBytes)+len(setBytes)), sr.lastOffset.Load())

	close(sr.stopCh)
	clientEnd.Close()
	err = <-done
	assert.NoError(t, err)
}

func TestSlaveReconnector_sendPSYNC_WithReplId(t *testing.T) {
	t.Parallel()
	serverEnd, clientEnd := net.Pipe()
	defer serverEnd.Close()
	defer clientEnd.Close()

	mc := &MasterConnection{
		Addr:   "127.0.0.1:6379",
		Conn:   clientEnd,
		Reader: bufio.NewReader(clientEnd),
		Writer: bufio.NewWriter(clientEnd),
		stopCh: make(chan struct{}),
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		br := bufio.NewReader(serverEnd)
		_, err := proto.ReadRESP(br)
		if err != nil {
			return
		}
		_, err = serverEnd.Write([]byte("+CONTINUE existing-replid\r\n"))
		if err != nil {
			return
		}
	}()

	sr := &SlaveReconnector{
		lastReplId: "existing-replid",
	}
	sr.lastOffset.Store(500)
	fullResync, err := sr.sendPSYNC(mc)
	assert.NoError(t, err)
	assert.False(t, fullResync)
	assert.Equal(t, "existing-replid", sr.lastReplId)
	<-done
}

// TestSlaveReconnector_readCommandLoop_StallForcesReconnect 守卫 §1c 自愈机制：
// 主节点通告的 offset 高于已应用 offset、且数据流空闲超过 stallTimeout 时，
// readCommandLoop 必须返回错误（触发重连，PSYNC CONTINUE 重放缺失尾巴）。
func TestSlaveReconnector_readCommandLoop_StallForcesReconnect(t *testing.T) {
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	serverEnd, clientEnd := net.Pipe()
	defer serverEnd.Close()
	defer clientEnd.Close()

	mc := &MasterConnection{
		Addr:   "127.0.0.1:6379",
		Conn:   clientEnd,
		Reader: bufio.NewReader(clientEnd),
		Writer: bufio.NewWriter(clientEnd),
		stopCh: make(chan struct{}),
	}

	sr := NewSlaveReconnector(rm, testStore, "127.0.0.1:6379")
	sr.stallTimeout = 200 * time.Millisecond
	sr.state.Store(int32(SlaveConnected))

	// 排水 goroutine：消费从节点发出的 REPLCONF GETACK（net.Pipe 同步写，
	// 对端不读会阻塞 GETACK 发送器）。测试体末尾显式 serverEnd.Close()
	// 解除其阻塞后等待退出（不能依赖 defer 顺序：LIFO 下 Close 会晚于等待）。
	drainDone := make(chan struct{})
	go func() {
		buf := make([]byte, 512)
		for {
			if _, err := serverEnd.Read(buf); err != nil {
				close(drainDone)
				return
			}
		}
	}()

	done := make(chan error, 1)
	go func() {
		done <- sr.readCommandLoop(mc)
	}()

	// 喂一条数据命令（推进 lastOffset、记录 lastDataTime）
	_, err := serverEnd.Write([]byte("*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n"))
	assert.NoError(t, err)

	var offset int64
	for i := 0; i < 50; i++ {
		offset = sr.lastOffset.Load()
		if offset > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	assert.True(t, offset > 0)

	// 先模拟收敛（ACK 通告 offset == lastOffset），武装停滞检测：
	// 追赶排水期的空闲间隙不判停滞，只有"收敛后主节点水位前进且数据流
	// 空闲超过 stallTimeout"才判定尾巴投递缺口。
	_, err = serverEnd.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$2\r\n29\r\n"))
	assert.NoError(t, err)

	// 空闲超过 stallTimeout 后收到主节点更高 offset 通告 → 判定停滞
	time.Sleep(250 * time.Millisecond)
	_, err = serverEnd.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$7\r\n9999999\r\n"))
	assert.NoError(t, err)

	select {
	case err := <-done:
		assert.Error(t, err)
		assert.True(t, strings.Contains(err.Error(), "replication stalled"))
	case <-time.After(3 * time.Second):
		t.Fatal("readCommandLoop did not force reconnect on stall")
	}

	close(sr.stopCh)
	serverEnd.Close()
	select {
	case <-drainDone:
	case <-time.After(2 * time.Second):
		t.Fatal("drain goroutine did not exit")
	}
}

// TestSlaveReconnector_readCommandLoop_NoStallWhenDataFlows 负例：数据流仍在
// 到达时，即使主节点通告更高 offset 也不得误判停滞（避免正常追赶期重连风暴）。
func TestSlaveReconnector_readCommandLoop_NoStallWhenDataFlows(t *testing.T) {
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	serverEnd, clientEnd := net.Pipe()
	defer serverEnd.Close()
	defer clientEnd.Close()

	mc := &MasterConnection{
		Addr:   "127.0.0.1:6379",
		Conn:   clientEnd,
		Reader: bufio.NewReader(clientEnd),
		Writer: bufio.NewWriter(clientEnd),
		stopCh: make(chan struct{}),
	}

	sr := NewSlaveReconnector(rm, testStore, "127.0.0.1:6379")
	sr.stallTimeout = 2 * time.Second // 足够大：只要 ACK 紧跟数据到达即不触发
	sr.state.Store(int32(SlaveConnected))

	drainDone := make(chan struct{})
	go func() {
		buf := make([]byte, 512)
		for {
			if _, err := serverEnd.Read(buf); err != nil {
				close(drainDone)
				return
			}
		}
	}()

	done := make(chan error, 1)
	go func() {
		done <- sr.readCommandLoop(mc)
	}()

	_, err := serverEnd.Write([]byte("*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n"))
	assert.NoError(t, err)

	var offset int64
	for i := 0; i < 50; i++ {
		offset = sr.lastOffset.Load()
		if offset > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	assert.True(t, offset > 0)

	// 数据刚到达即收到更高 offset 通告 → 不触发停滞
	_, err = serverEnd.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$7\r\n9999999\r\n"))
	assert.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	close(sr.stopCh)
	clientEnd.Close()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("readCommandLoop did not return on stop")
	}

	serverEnd.Close()
	select {
	case <-drainDone:
	case <-time.After(2 * time.Second):
		t.Fatal("drain goroutine did not exit")
	}
}
