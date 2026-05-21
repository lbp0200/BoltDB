package integration

import (
	"bufio"
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/store"
)

// opcodes for state-machine chaos fuzzing
const (
	fsmPING = iota
	fsmSET
	fsmGET
	fsmMULTI
	fsmEXEC
	fsmDISCARD
	fsmWATCH
	fsmUNWATCH
	fsmSUBSCRIBE
	fsmUNSUBSCRIBE
	fsmPUBLISH
	fsmBLPOP
	fsmBRPOP
	fsmBLMOVE
	fsmBZPOPMAX
	fsmXREAD
	fsmXREADBLOCK
	fsmCLIENTKILL
	fsmQUIT
	fsmDEL
	fsmLPUSH
	fsmRPUSH
	fsmSADD
	fsmZADD
	fsmHSET
	fsmINCR
	fsmEXPIRE
	fsmMONITOR
	fsmWAIT
	fsmCount
)

var fsmOpNames = map[byte]string{
	fsmPING:        "PING",
	fsmSET:         "SET",
	fsmGET:         "GET",
	fsmMULTI:       "MULTI",
	fsmEXEC:        "EXEC",
	fsmDISCARD:     "DISCARD",
	fsmWATCH:       "WATCH",
	fsmUNWATCH:     "UNWATCH",
	fsmSUBSCRIBE:   "SUBSCRIBE",
	fsmUNSUBSCRIBE: "UNSUBSCRIBE",
	fsmPUBLISH:     "PUBLISH",
	fsmBLPOP:       "BLPOP",
	fsmBRPOP:       "BRPOP",
	fsmBLMOVE:      "BLMOVE",
	fsmBZPOPMAX:    "BZPOPMAX",
	fsmXREAD:       "XREAD",
	fsmXREADBLOCK:  "XREAD BLOCK",
	fsmCLIENTKILL:  "CLIENT KILL",
	fsmQUIT:        "QUIT",
	fsmDEL:         "DEL",
	fsmLPUSH:       "LPUSH",
	fsmRPUSH:       "RPUSH",
	fsmSADD:        "SADD",
	fsmZADD:        "ZADD",
	fsmHSET:        "HSET",
	fsmINCR:        "INCR",
	fsmEXPIRE:      "EXPIRE",
	fsmMONITOR:     "MONITOR",
	fsmWAIT:        "WAIT",
}

// TestFuzzServerStateMachineChaos runs aggressive state-transition sequences
// mixing SUBSCRIBE, BLPOP, XREAD BLOCK, CLIENT KILL, MONITOR, QUIT, and normal ops.
// It intentionally sends invalid sequences to test error recovery paths.
// Detects: goroutine leak, stuck connections, server deadlock, orphaned subscribers.
//
// Usage:
//
//	go test -race -timeout 60s ./cmd/integration/ -run TestFuzzServerStateMachineChaos
//	FSM_ITERATIONS=500 go test -race -timeout 120s ./cmd/integration/ -run TestFuzzServerStateMachineChaos
func TestFuzzServerStateMachineChaos(t *testing.T) {
	iterations := getFSMIterations()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	t.Logf("fsm-chaos: iterations=%d", iterations)
	t.Logf("fsm-chaos: set FSM_ITERATIONS env to change (default=200)")

	for seq := 0; seq < iterations; seq++ {
		setupTest(t)

		if t.Failed() {
			return
		}

		testFSMSequence(t, rng, seq)
		teardownTest(t)
	}
}

func getFSMIterations() int {
	s := os.Getenv("FSM_ITERATIONS")
	if s == "" {
		return 50
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return 50
	}
	if n > 5000 {
		return 5000
	}
	return n
}

func testFSMSequence(t *testing.T, rng *rand.Rand, seq int) {
	t.Helper()

	baseline := runtime.NumGoroutine()

	conn, err := net.DialTimeout("tcp", sharedListener.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatalf("seq %d: dial: %v", seq, err)
	}
	defer conn.Close()
	reader := bufio.NewReaderSize(conn, 4096)

	// Generate sequence length: 5-30 ops
	seqLen := rng.Intn(26) + 5
	ops := make([]byte, seqLen)
	for i := range ops {
		ops[i] = byte(rng.Intn(fsmCount))
	}

	var blockingOpsCount int
	var connClosed bool

	for _, op := range ops {
		if connClosed {
			break
		}

		if isConnClosed(conn) {
			connClosed = true
			break
		}

		if op == fsmBLPOP || op == fsmBRPOP || op == fsmBLMOVE || op == fsmBZPOPMAX || op == fsmXREADBLOCK {
			blockingOpsCount++
		}

		executed := executeFSMOp(t, conn, reader, op, rng)
		if !executed {
			continue
		}

		if op == fsmQUIT || op == fsmCLIENTKILL {
			// QUIT closes the connection from server side
			// CLIENT KILL TYPE normal may kill this connection
			connClosed = true
			break
		}

		// brief deadline to avoid hanging on blocking ops
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		drainRESP(reader)
		conn.SetReadDeadline(time.Time{})
	}

	// close the connection
	if !isConnClosed(conn) {
		conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		drainRESP(reader)
		conn.SetReadDeadline(time.Time{})
	}
	conn.Close()

	// give goroutines time to settle
	time.Sleep(goroutineSettleTime)

	// check goroutine leak
	final := runtime.NumGoroutine()
	if leak := final - baseline; leak > goroutineTolerance {
		t.Errorf("seq %d (len=%d, blocking=%d): goroutine leak: %d (baseline=%d, final=%d)",
			seq, len(ops), blockingOpsCount, leak, baseline, final)
	}

	// verify server still responsive (no deadlock)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := sharedClient.Ping(ctx).Err(); err != nil {
		t.Fatalf("seq %d: server not responsive: %v", seq, err)
	}

	// verify store consistency
	if sharedDB != nil {
		if err := sharedDB.Check(); err != nil {
			t.Fatalf("seq %d: store consistency check failed: %v", seq, err)
		}
	}

}

func executeFSMOp(t *testing.T, conn net.Conn, reader *bufio.Reader, op byte, rng *rand.Rand) bool {
	switch op {
	case fsmPING:
		sendRESP(conn, "PING")
		return true
	case fsmSET:
		sendRESP(conn, "SET", fsmRandKey(rng), fsmRandString(rng, 16))
		return true
	case fsmGET:
		sendRESP(conn, "GET", fsmRandKey(rng))
		return true
	case fsmMULTI:
		sendRESP(conn, "MULTI")
		return true
	case fsmEXEC:
		sendRESP(conn, "EXEC")
		return true
	case fsmDISCARD:
		sendRESP(conn, "DISCARD")
		return true
	case fsmWATCH:
		sendRESP(conn, "WATCH", fsmRandKey(rng))
		return true
	case fsmUNWATCH:
		sendRESP(conn, "UNWATCH")
		return true
	case fsmSUBSCRIBE:
		ch := fsmRandString(rng, 8)
		sendRESP(conn, "SUBSCRIBE", ch)
		return true
	case fsmUNSUBSCRIBE:
		ch := fsmRandString(rng, 8)
		sendRESP(conn, "UNSUBSCRIBE", ch)
		return true
	case fsmPUBLISH:
		ch := fsmRandString(rng, 8)
		sendRESP(conn, "PUBLISH", ch, fsmRandString(rng, 16))
		return true
	case fsmBLPOP:
		sendRESP(conn, "BLPOP", fsmRandKey(rng), "1")
		return true
	case fsmBRPOP:
		sendRESP(conn, "BRPOP", fsmRandKey(rng), "1")
		return true
	case fsmBLMOVE:
		sendRESP(conn, "BLMOVE", fsmRandKey(rng), fsmRandKey(rng), "LEFT", "LEFT", "1")
		return true
	case fsmBZPOPMAX:
		key := fsmRandKey(rng)
		sendRESP(conn, "BZPOPMAX", key, "1")
		return true
	case fsmXREAD:
		sendRESP(conn, "XREAD", "STREAMS", fsmRandKey(rng), "0")
		return true
	case fsmXREADBLOCK:
		sendRESP(conn, "XREAD", "BLOCK", "1000", "STREAMS", fsmRandKey(rng), "$")
		return true
	case fsmCLIENTKILL:
		sendRESP(conn, "CLIENT", "KILL", "TYPE", "normal")
		return true
	case fsmQUIT:
		sendRESP(conn, "QUIT")
		return true
	case fsmDEL:
		sendRESP(conn, "DEL", fsmRandKey(rng))
		return true
	case fsmLPUSH:
		sendRESP(conn, "LPUSH", fsmRandKey(rng), fsmRandString(rng, 16))
		return true
	case fsmRPUSH:
		sendRESP(conn, "RPUSH", fsmRandKey(rng), fsmRandString(rng, 16))
		return true
	case fsmSADD:
		sendRESP(conn, "SADD", fsmRandKey(rng), fsmRandString(rng, 8))
		return true
	case fsmZADD:
		sendRESP(conn, "ZADD", fsmRandKey(rng), fmt.Sprintf("%d", rng.Intn(1000)), fsmRandString(rng, 8))
		return true
	case fsmHSET:
		sendRESP(conn, "HSET", fsmRandKey(rng), fsmRandString(rng, 8), fsmRandString(rng, 16))
		return true
	case fsmINCR:
		sendRESP(conn, "INCR", fsmRandKey(rng))
		return true
	case fsmEXPIRE:
		sendRESP(conn, "EXPIRE", fsmRandKey(rng), fmt.Sprintf("%d", rng.Intn(100)+1))
		return true
	case fsmMONITOR:
		sendRESP(conn, "MONITOR")
		return true
	case fsmWAIT:
		sendRESP(conn, "WAIT", "0", "0")
		return true
	default:
		return false
	}
}

const fsmRandLetters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_:"

func fsmRandString(rng *rand.Rand, n int) string {
	var b strings.Builder
	b.Grow(n)
	for i := 0; i < n; i++ {
		b.WriteByte(fsmRandLetters[rng.Intn(len(fsmRandLetters))])
	}
	return b.String()
}

func fsmRandKey(rng *rand.Rand) string {
	prefixes := []string{"fsm:", "st:", "k:", "l:", "s:", "z:", "h:", "str:", ""}
	prefix := prefixes[rng.Intn(len(prefixes))]
	return prefix + fsmRandString(rng, rng.Intn(8)+1)
}

// TestFuzzServerBlockingKill tests that blocking operations (BLPOP, BRPOP, BLMOVE,
// BZPOPMAX, XREAD BLOCK) can be interrupted by CLIENT KILL without leaking goroutines
// or leaving orphaned blocking registrations.
func TestFuzzServerBlockingKill(t *testing.T) {
	iterations := getFSMIterations()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	t.Logf("blocking-kill: iterations=%d", iterations)

	for seq := 0; seq < iterations; seq++ {
		setupTest(t)
		if t.Failed() {
			return
		}

		testBlockingKillSequence(t, rng, seq)
		teardownTest(t)
	}
}

func testBlockingKillSequence(t *testing.T, rng *rand.Rand, seq int) {
	t.Helper()

	baseline := runtime.NumGoroutine()

	// Open a dedicated connection for the blocking op
	blockConn, err := net.DialTimeout("tcp", sharedListener.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatalf("seq %d: block dial: %v", seq, err)
	}
	blockAddr := blockConn.RemoteAddr().String()

	// Run the blocking op (infinite timeout = 0)
	blockingOps := []string{"BLPOP", "BRPOP", "BZPOPMAX"}
	blockingOp := blockingOps[rng.Intn(len(blockingOps))]
	sendRESP(blockConn, blockingOp, fmt.Sprintf("fsm:kill:%d", seq), "0")

	// Wait for the blocking op to register
	time.Sleep(100 * time.Millisecond)

	// Kill the connection via CLIENT KILL by address
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = sharedClient.Do(ctx, "CLIENT", "KILL", "ADDR", blockAddr).Result()
	cancel()
	if err != nil {
		// If KILL failed, close directly
		blockConn.Close()
	} else {
		time.Sleep(200 * time.Millisecond)
	}

	blockConn.Close()

	time.Sleep(goroutineSettleTime)

	// Check goroutine leak
	final := runtime.NumGoroutine()
	if leak := final - baseline; leak > goroutineTolerance {
		t.Errorf("seq %d (%s): goroutine leak: %d (baseline=%d, final=%d)",
			seq, blockingOp, leak, baseline, final)
	}

	// Verify server responsive
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer pingCancel()
	if err := sharedClient.Ping(pingCtx).Err(); err != nil {
		t.Fatalf("seq %d: server not responsive: %v", seq, err)
	}

	// Verify store consistency
	if sharedDB != nil {
		if err := sharedDB.Check(); err != nil {
			t.Fatalf("seq %d: store consistency: %v", seq, err)
		}
	}
}

// TestFuzzServerSubscriberChaos tests that subscriber lifecycle transitions
// (SUBSCRIBE → disconnect, SUBSCRIBE → QUIT, SUBSCRIBE → CLIENT KILL,
// SUBSCRIBE → BLPOP → disconnect, MONITOR → BLPOP → CLIENT KILL)
// clean up properly without orphaned subscribers or goroutines.
func TestFuzzServerSubscriberChaos(t *testing.T) {
	iterations := getFSMIterations()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	t.Logf("subscriber-chaos: iterations=%d", iterations)

	for seq := 0; seq < iterations; seq++ {
		setupTest(t)
		if t.Failed() {
			return
		}

		testSubscriberChaosSequence(t, rng, seq)
		teardownTest(t)
	}
}

func testSubscriberChaosSequence(t *testing.T, rng *rand.Rand, seq int) {
	t.Helper()

	baseline := runtime.NumGoroutine()

	conn, err := net.DialTimeout("tcp", sharedListener.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatalf("seq %d: dial: %v", seq, err)
	}
	reader := bufio.NewReaderSize(conn, 1024)

	// Transition pattern: enter subscriber/monitor mode then send illegal ops
	switch rng.Intn(9) {
	case 0: // SUBSCRIBE → disconnect
		sendRESP(conn, "SUBSCRIBE", fmt.Sprintf("fsm:sub:%d", seq))
		readWithTimeout(reader, 200*time.Millisecond)
		conn.Close()

	case 1: // SUBSCRIBE → QUIT
		sendRESP(conn, "SUBSCRIBE", fmt.Sprintf("fsm:sub:%d", seq))
		readWithTimeout(reader, 200*time.Millisecond)
		sendRESP(conn, "QUIT")
		readWithTimeout(reader, 200*time.Millisecond)
		conn.Close()

	case 2: // SUBSCRIBE → BLPOP (illegal, should error)
		sendRESP(conn, "SUBSCRIBE", fmt.Sprintf("fsm:sub:%d", seq))
		readWithTimeout(reader, 200*time.Millisecond)
		sendRESP(conn, "BLPOP", "fsm:nokey", "1")
		readWithTimeout(reader, 200*time.Millisecond)
		conn.Close()

	case 3: // SUBSCRIBE → GET → MULTI (multi illegal in pubsub)
		sendRESP(conn, "SUBSCRIBE", fmt.Sprintf("fsm:sub:%d", seq))
		readWithTimeout(reader, 200*time.Millisecond)
		sendRESP(conn, "GET", "fsm:somekey")
		readWithTimeout(reader, 200*time.Millisecond)
		sendRESP(conn, "MULTI")
		readWithTimeout(reader, 200*time.Millisecond)
		conn.Close()

	case 4: // MONITOR → BLPOP (illegal)
		sendRESP(conn, "MONITOR")
		readWithTimeout(reader, 200*time.Millisecond)
		sendRESP(conn, "BLPOP", "fsm:nokey", "1")
		readWithTimeout(reader, 200*time.Millisecond)
		conn.Close()

	case 5: // SUBSCRIBE → CLIENT KILL (kill this connection)
		sendRESP(conn, "SUBSCRIBE", fmt.Sprintf("fsm:sub:%d", seq))
		readWithTimeout(reader, 200*time.Millisecond)
		addr := conn.RemoteAddr().String()
		conn.Close()

		killCtx, killCancel := context.WithTimeout(context.Background(), 3*time.Second)
		sharedClient.Do(killCtx, "CLIENT", "KILL", "ADDR", addr)
		killCancel()

	case 6: // MULTI → SUBSCRIBE → EXEC (illegal)
		sendRESP(conn, "MULTI")
		readWithTimeout(reader, 200*time.Millisecond)
		sendRESP(conn, "SUBSCRIBE", fmt.Sprintf("fsm:sub:%d", seq))
		readWithTimeout(reader, 200*time.Millisecond)
		sendRESP(conn, "EXEC")
		readWithTimeout(reader, 200*time.Millisecond)
		conn.Close()

	case 7: // MULTI → BLPOP → EXEC (blocking in multi)
		sendRESP(conn, "MULTI")
		readWithTimeout(reader, 200*time.Millisecond)
		sendRESP(conn, "BLPOP", "fsm:nokey", "10")
		readWithTimeout(reader, 200*time.Millisecond)
		sendRESP(conn, "EXEC")
		readWithTimeout(reader, 200*time.Millisecond)
		conn.Close()

	case 8: // PSUBSCRIBE → MONITOR → disconnect (stacked modes)
		sendRESP(conn, "PSUBSCRIBE", fmt.Sprintf("fsm:pat:*"))
		readWithTimeout(reader, 200*time.Millisecond)
		// PSUBSCRIBE should set subscriber, MONITOR as a command should fail
		sendRESP(conn, "MONITOR")
		readWithTimeout(reader, 200*time.Millisecond)
		conn.Close()
	}

	// Extra cleanup
	conn.Close()

	time.Sleep(goroutineSettleTime)

	// goroutine leak check
	final := runtime.NumGoroutine()
	if leak := final - baseline; leak > goroutineTolerance {
		t.Errorf("seq %d: goroutine leak: %d (baseline=%d, final=%d)", seq, leak, baseline, final)
	}

	// server responsiveness
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer pingCancel()
	if err := sharedClient.Ping(pingCtx).Err(); err != nil {
		t.Fatalf("seq %d: server not responsive: %v", seq, err)
	}

	// store consistency
	if sharedDB != nil {
		if err := sharedDB.Check(); err != nil {
			t.Fatalf("seq %d: store consistency: %v", seq, err)
		}
	}
}

// TestFuzzServerConcurrentStateChaos tests concurrent connections mixing
// blocking ops, pubsub, and transactions to detect deadlocks and stuck state.
// Uses the shared server with parallel clients doing conflicting state transitions.
func TestFuzzServerConcurrentStateChaos(t *testing.T) {
	iterations := getFSMIterations()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	t.Logf("concurrent-state-chaos: iterations=%d", iterations)

	for seq := 0; seq < iterations; seq++ {
		setupTest(t)
		if t.Failed() {
			return
		}

		testConcurrentStateChaos(t, rng, seq)
		teardownTest(t)
	}
}

func testConcurrentStateChaos(t *testing.T, rng *rand.Rand, seq int) {
	t.Helper()

	baseline := runtime.NumGoroutine()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	clients := rng.Intn(6) + 3

	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			crng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id) + int64(seq)*100))

			conn, err := net.DialTimeout("tcp", sharedListener.Addr().String(), 3*time.Second)
			if err != nil {
				return
			}
			defer conn.Close()
			creader := bufio.NewReaderSize(conn, 512)

			ops := crng.Intn(8) + 2
			for j := 0; j < ops; j++ {
				if ctx.Err() != nil {
					return
				}

				op := byte(crng.Intn(fsmCount))
				executed := executeFSMOp(t, conn, creader, op, crng)
				if !executed {
					continue
				}

				if op == fsmQUIT || op == fsmCLIENTKILL {
					break
				}

				conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
				drainRESP(creader)
				conn.SetReadDeadline(time.Time{})
			}
		}(i)
	}

	wg.Wait()
	cancel()

	time.Sleep(goroutineSettleTime)

	// goroutine leak
	final := runtime.NumGoroutine()
	if leak := final - baseline; leak > goroutineTolerance*2 {
		t.Errorf("seq %d (%d clients): goroutine leak: %d (baseline=%d, final=%d)",
			seq, clients, leak, baseline, final)
	}

	// server responsive
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()
	if err := sharedClient.Ping(pingCtx).Err(); err != nil {
		t.Fatalf("seq %d: server not responsive: %v", seq, err)
	}

	// store consistency
	if sharedDB != nil {
		if err := sharedDB.Check(); err != nil {
			t.Fatalf("seq %d: store consistency: %v", seq, err)
		}
	}
}

func readWithTimeout(reader *bufio.Reader, timeout time.Duration) {
	done := make(chan struct{}, 1)
	go func() {
		drainRESP(reader)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
	}
}

// ensure store is imported for consistency check references
var _ = store.CloseTimeout
