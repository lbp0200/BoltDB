package integration

import (
	"bufio"
	"context"
	"fmt"
	"math/rand"
	"net"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/proto"
)

// Opcodes for command sequence fuzzing.
const (
	opSET = iota
	opGET
	opMULTI
	opEXEC
	opDISCARD
	opWATCH
	opUNWATCH
	opSUBSCRIBE
	opUNSUBSCRIBE
	opPUBLISH
	opBLPOP
	opBZPOPMAX
	opXREAD
	opCLIENTKILL
	opQUIT
	opPING
	opDEL
	opLPUSH
	opSADD
	opZADD
	opCount
)

var opNames = map[byte]string{
	opSET:        "SET",
	opGET:        "GET",
	opMULTI:      "MULTI",
	opEXEC:       "EXEC",
	opDISCARD:    "DISCARD",
	opWATCH:      "WATCH",
	opUNWATCH:    "UNWATCH",
	opSUBSCRIBE:  "SUBSCRIBE",
	opUNSUBSCRIBE: "UNSUBSCRIBE",
	opPUBLISH:    "PUBLISH",
	opBLPOP:      "BLPOP",
	opBZPOPMAX:   "BZPOPMAX",
	opXREAD:      "XREAD",
	opCLIENTKILL: "CLIENT KILL",
	opQUIT:       "QUIT",
	opPING:       "PING",
	opDEL:        "DEL",
	opLPUSH:      "LPUSH",
	opSADD:       "SADD",
	opZADD:       "ZADD",
}

// FuzzServerCommandSequence fuzzes state-machine command sequences.
// Each fuzz input byte encodes an operation; the fuzzer naturally explores
// different sequences, detecting panics, goroutine leaks, and deadlocks.
func FuzzServerCommandSequence(f *testing.F) {
	seeds := [][]byte{
		// Transaction sequences
		{opMULTI, opSET, opEXEC},
		{opMULTI, opDISCARD},
		{opWATCH, opSET, opEXEC},
		{opWATCH, opUNWATCH, opSET, opEXEC},
		// PubSub sequences
		{opSUBSCRIBE, opPUBLISH, opUNSUBSCRIBE},
		{opSUBSCRIBE, opQUIT},
		{opSUBSCRIBE, opPING},
		// Blocking sequences
		{opBLPOP},
		{opLPUSH, opBLPOP},

		// Mixed sequences
		{opMULTI, opBLPOP, opEXEC},
		{opSUBSCRIBE, opWATCH},
		{opCLIENTKILL, opSET},
		{opQUIT, opSET},
		{opMULTI, opSUBSCRIBE, opEXEC},
		// Connection management
		{opPING, opPING, opPING},
		{opQUIT},
		{opCLIENTKILL},
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, ops []byte) {
		if len(ops) == 0 {
			return
		}

		setupTest(t)
		defer teardownTest(t)

		baseline := runtime.NumGoroutine()

		conn, err := net.DialTimeout("tcp", sharedListener.Addr().String(), 5*time.Second)
		if err != nil {
			return
		}
		defer conn.Close()
		reader := bufio.NewReaderSize(conn, 1024)

		inTx := false
		subscribed := false

		for _, op := range ops {
			opType := op % opCount
			// If subscribed and not a PubSub-allowed command, the server will error
			// but should not crash
			executed := executeFuzzOp(t, conn, reader, opType, &inTx, &subscribed)
			if !executed {
				continue
			}

			if isConnClosed(conn) {
				return
			}
		}

		// Cleanup: close properly if connection alive
		if !isConnClosed(conn) {
			sendRESP(conn, "QUIT")
			time.Sleep(100 * time.Millisecond)
		}
		conn.Close()

		// Let goroutines settle
		time.Sleep(goroutineSettleTime)
		finalGoroutines := runtime.NumGoroutine()

		// Report but don't fail on goroutine increase — fuzzer uses it to find issues
		if leak := finalGoroutines - baseline; leak > goroutineTolerance {
			t.Errorf("goroutine leak after %d ops: %d (baseline=%d, final=%d)",
				len(ops), leak, baseline, finalGoroutines)
		}

		// Verify server still alive
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := sharedClient.Ping(ctx).Err(); err != nil {
			t.Fatalf("server not responsive after fuzz: %v", err)
		}
	})
}

// FuzzServerRawBytes fuzzes raw RESP bytes sent directly to the server.
// Catches parser-level issues in the full server context, including
// interactions between pipeline parsing and connection state.
func FuzzServerRawBytes(f *testing.F) {
	seeds := [][]byte{
		[]byte("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"),
		[]byte("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"),
		[]byte("PING\r\n"),
		[]byte("*1\r\n$4\r\nPING\r\n"),
		[]byte("*1\r\n$4\r\nQUIT\r\n"),
		[]byte("*2\r\n$9\r\nSUBSCRIBE\r\n$3\r\nfoo\r\n"),
		[]byte("*5\r\n$4\r\nMULTI\r\n$3\r\nSET\r\n$3\r\nkey\r\n$1\r\na\r\n$4\r\nEXEC\r\n"),
		[]byte("*2\r\n$5\r\nBLPOP\r\n$4\r\nlist\r\n$1\r\n1\r\n"),
		[]byte("*3\r\n$6\r\nCLIENT\r\n$4\r\nKILL\r\n$4\r\nADDR\r\n$9\r\n0.0.0.0:0\r\n"),
		[]byte("*2\r\n$4\r\nXADD\r\n$5\r\nmystream\r\n$1\r\n*\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"),
		[]byte("*2\r\n$3\r\nGET\r\n$999999\r\n"),
		[]byte("*2\r\n$3\r\nGET\r\n$-1\r\n"),
		// Include parser-level edge cases (they also go through full server)
		[]byte("*3\r\n+OK\r\n"),
		[]byte("*2\r\n:1\r\n:2\r\n"),
		[]byte("   PING\r\n"),
		[]byte("PING\r\nPING\r\nPING\r\n"),
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) == 0 {
			return
		}

		setupTest(t)
		defer teardownTest(t)

		baseline := runtime.NumGoroutine()

		conn, err := net.DialTimeout("tcp", sharedListener.Addr().String(), 5*time.Second)
		if err != nil {
			return
		}
		defer conn.Close()

		conn.SetDeadline(time.Now().Add(3 * time.Second))
		if _, err := conn.Write(data); err != nil {
			return
		}

		// Try to drain responses
		reader := bufio.NewReaderSize(conn, 4096)
		for i := 0; i < 100; i++ {
			if _, err := proto.ReadRESP(reader); err != nil {
				break
			}
		}

		conn.SetDeadline(time.Time{})
		conn.Close()

		time.Sleep(goroutineSettleTime)
		finalGoroutines := runtime.NumGoroutine()
		if leak := finalGoroutines - baseline; leak > goroutineTolerance {
			t.Errorf("goroutine leak: %d (baseline=%d, final=%d)",
				leak, baseline, finalGoroutines)
		}

		// Verify server still alive
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := sharedClient.Ping(ctx).Err(); err != nil {
			t.Fatalf("server not responsive after fuzz: %v", err)
		}
	})
}

func executeFuzzOp(t *testing.T, conn net.Conn, reader *bufio.Reader, op byte, inTx *bool, subscribed *bool) bool {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	switch op {
	case opPING:
		sendRESP(conn, "PING")
		return true
	case opSET:
		key := randKey(rng)
		val := randString(rng, 16)
		sendRESP(conn, "SET", key, val)
		return true
	case opGET:
		key := randKey(rng)
		sendRESP(conn, "GET", key)
		return true
	case opMULTI:
		sendRESP(conn, "MULTI")
		*inTx = true
		return true
	case opEXEC:
		sendRESP(conn, "EXEC")
		*inTx = false
		return true
	case opDISCARD:
		sendRESP(conn, "DISCARD")
		*inTx = false
		return true
	case opWATCH:
		keys := make([]string, rng.Intn(3)+1)
		for i := range keys {
			keys[i] = randKey(rng)
		}
		args := []string{"WATCH"}
		args = append(args, keys...)
		sendRESP(conn, args[0], args[1:]...)
		return true
	case opUNWATCH:
		sendRESP(conn, "UNWATCH")
		return true
	case opSUBSCRIBE:
		ch := randString(rng, 8)
		sendRESP(conn, "SUBSCRIBE", ch)
		*subscribed = true
		return true
	case opUNSUBSCRIBE:
		if !*subscribed {
			sendRESP(conn, "PING")
			return true
		}
		ch := randString(rng, 8)
		sendRESP(conn, "UNSUBSCRIBE", ch)
		return true
	case opPUBLISH:
		ch := randString(rng, 8)
		val := randString(rng, 16)
		sendRESP(conn, "PUBLISH", ch, val)
		return true
	case opBLPOP:
		key := randKey(rng)
		sendRESP(conn, "BLPOP", key, "1")
		return true
	case opBZPOPMAX:
		key := randKey(rng)
		sendRESP(conn, "BZPOPMAX", key, "1")
		return true
	case opXREAD:
		sendRESP(conn, "XREAD", "BLOCK", "1000", "STREAMS", randKey(rng), "0")
		return true
	case opCLIENTKILL:
		sendRESP(conn, "CLIENT", "KILL", "TYPE", "normal")
		return true
	case opQUIT:
		sendRESP(conn, "QUIT")
		return true
	case opDEL:
		key := randKey(rng)
		sendRESP(conn, "DEL", key)
		return true
	case opLPUSH:
		key := randKey(rng)
		val := randString(rng, 16)
		sendRESP(conn, "LPUSH", key, val)
		return true
	case opSADD:
		key := randKey(rng)
		member := randString(rng, 8)
		sendRESP(conn, "SADD", key, member)
		return true
	case opZADD:
		key := randKey(rng)
		score := fmt.Sprintf("%d", rng.Intn(1000))
		member := randString(rng, 8)
		sendRESP(conn, "ZADD", key, score, member)
		return true
	default:
		return false
	}
}

const randKeyLetters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_:"

func randString(rng *rand.Rand, n int) string {
	var b strings.Builder
	b.Grow(n)
	for i := 0; i < n; i++ {
		b.WriteByte(randKeyLetters[rng.Intn(len(randKeyLetters))])
	}
	return b.String()
}

func randKey(rng *rand.Rand) string {
	prefixes := []string{"fuzz:", "test:", "k:", "key:", "list:", "set:", "zset:", "stream:", ""}
	prefix := prefixes[rng.Intn(len(prefixes))]
	return prefix + randString(rng, rng.Intn(12)+1)
}

func isConnClosed(conn net.Conn) bool {
	conn.SetReadDeadline(time.Now().Add(1 * time.Millisecond))
	n, err := conn.Read(make([]byte, 1))
	conn.SetReadDeadline(time.Time{})
	return n == 0 && err != nil
}

var _ sync.Locker
