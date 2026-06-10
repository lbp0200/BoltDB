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
	// v8.3 expansion
	opHSET
	opHGET
	opINCR
	opEXPIRE
	opRPUSH
	opRENAME
	opSMEMBERS
	opGEOADD
	opMONITOR
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
	opHSET:       "HSET",
	opHGET:       "HGET",
	opINCR:       "INCR",
	opEXPIRE:     "EXPIRE",
	opRPUSH:      "RPUSH",
	opRENAME:     "RENAME",
	opSMEMBERS:   "SMEMBERS",
	opGEOADD:     "GEOADD",
	opMONITOR:    "MONITOR",
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
		{opBZPOPMAX},
		{opZADD, opBZPOPMAX},
		{opXREAD},
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
		// v8.3 expansion — hash
		{opHSET, opHGET},
		{opHSET, opHSET, opHGET},
		// v8.3 expansion — counter + ttl
		{opINCR, opGET},
		{opSET, opEXPIRE, opGET},
		{opINCR, opEXPIRE, opGET},
		// v8.3 expansion — list
		{opRPUSH, opLPUSH, opBLPOP},
		{opRPUSH, opLPUSH, opBLPOP, opBLPOP},
		// v8.3 expansion — rename
		{opSET, opRENAME, opGET},
		{opSET, opRENAME, opDEL},
		// v8.3 expansion — set cross-type
		{opSADD, opSMEMBERS},
		// v8.3 expansion — geo
		{opGEOADD},
		// v8.3 expansion — monitor
		{opMONITOR, opQUIT},
		// State machine chaos expansions — mixed state transitions
		{opSUBSCRIBE, opBLPOP, opQUIT},
		{opSUBSCRIBE, opXREAD, opUNSUBSCRIBE},
		{opSUBSCRIBE, opCLIENTKILL},
		{opMONITOR, opBLPOP, opQUIT},
		{opMONITOR, opCLIENTKILL},
		{opWATCH, opSUBSCRIBE, opEXEC},
		{opMULTI, opSUBSCRIBE, opBLPOP, opEXEC},
		{opBLPOP, opCLIENTKILL},
		{opBLPOP, opMULTI, opEXEC},
		{opXREAD, opBLPOP, opCLIENTKILL},
		{opLPUSH, opBLPOP, opXREAD},
		{opRPUSH, opBLPOP, opBZPOPMAX},
		{opSADD, opDEL, opSUBSCRIBE, opCLIENTKILL},
		{opMONITOR, opSUBSCRIBE, opQUIT},
		{opSUBSCRIBE, opMONITOR, opBLPOP},
		{opSET, opEXPIRE, opGET, opSUBSCRIBE, opUNSUBSCRIBE},
		{opWATCH, opMULTI, opSET, opSUBSCRIBE, opEXEC},
		{opBLPOP, opBZPOPMAX, opXREAD, opQUIT},
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
	case opHSET:
		key := randKey(rng)
		field := randString(rng, 8)
		val := randString(rng, 16)
		sendRESP(conn, "HSET", key, field, val)
		return true
	case opHGET:
		key := randKey(rng)
		field := randString(rng, 8)
		sendRESP(conn, "HGET", key, field)
		return true
	case opINCR:
		key := randKey(rng)
		sendRESP(conn, "INCR", key)
		return true
	case opEXPIRE:
		key := randKey(rng)
		ttl := fmt.Sprintf("%d", rng.Intn(100)+1)
		sendRESP(conn, "EXPIRE", key, ttl)
		return true
	case opRPUSH:
		key := randKey(rng)
		val := randString(rng, 16)
		sendRESP(conn, "RPUSH", key, val)
		return true
	case opRENAME:
		key := randKey(rng)
		newKey := randKey(rng)
		sendRESP(conn, "RENAME", key, newKey)
		return true
	case opSMEMBERS:
		key := randKey(rng)
		sendRESP(conn, "SMEMBERS", key)
		return true
	case opGEOADD:
		key := randKey(rng)
		lng := fmt.Sprintf("%f", (rng.Float64()*360-180))
		lat := fmt.Sprintf("%f", (rng.Float64()*180-90))
		member := randString(rng, 8)
		sendRESP(conn, "GEOADD", key, lng, lat, member)
		return true
	case opMONITOR:
		sendRESP(conn, "MONITOR")
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

// FuzzServerPipeline fuzzes RESP pipeline bursts (multiple commands in one write).
// Tests parser robustness under pipelined command sequences.
func FuzzServerPipeline(f *testing.F) {
	seeds := [][]byte{
		[]byte("*1\r\n$4\r\nPING\r\n"),
		[]byte("*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n"),
		[]byte("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n*2\r\n$3\r\nSET\r\n$3\r\nkey\r\n$1\r\na\r\n"),
		[]byte("*3\r\n$3\r\nSET\r\n$3\r\na\r\n$1\r\nb\r\n*3\r\n$3\r\nSET\r\n$3\r\nc\r\n$1\r\nd\r\n*2\r\n$3\r\nGET\r\n$3\r\na\r\n"),
		[]byte("*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$2\r\nk\r\n$1\r\nv\r\n*1\r\n$4\r\nEXEC\r\n"),
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

		reader := bufio.NewReaderSize(conn, 4096)
		for i := 0; i < 200; i++ {
			if _, err := proto.ReadRESP(reader); err != nil {
				break
			}
		}

		conn.SetDeadline(time.Time{})
		conn.Close()
		time.Sleep(goroutineSettleTime)

		finalGoroutines := runtime.NumGoroutine()
		if leak := finalGoroutines - baseline; leak > goroutineTolerance {
			t.Errorf("goroutine leak after pipeline: %d (baseline=%d, final=%d)",
				leak, baseline, finalGoroutines)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := sharedClient.Ping(ctx).Err(); err != nil {
			t.Fatalf("server not responsive after pipeline fuzz: %v", err)
		}
	})
}

// FuzzServerConcurrent fuzzes concurrent operations from multiple connections.
// Tests race conditions, deadlocks, and state corruption under contention.
func FuzzServerConcurrent(f *testing.F) {
	seeds := [][]byte{
		{2, 0, 5},             // 2 clients, 5 ops each
		{3, 0, 3},             // 3 clients, 3 ops each
		{5, 1, 2},             // 5 clients, 2 ops each (shared key range)
		{10, 0, 1},            // 10 clients, 1 op each
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, params []byte) {
		if len(params) < 3 {
			return
		}

		numClients := int(params[0]) % 15
		if numClients < 2 {
			numClients = 2
		}
		sharedKeys := int(params[1]) % 2 // 0 = each client has own keys, 1 = shared key range
		opsPerClient := int(params[2]) % 8
		if opsPerClient < 1 {
			opsPerClient = 1
		}

		setupTest(t)
		defer teardownTest(t)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		var wg sync.WaitGroup

		for i := 0; i < numClients; i++ {
			wg.Add(1)
			go func(clientID int) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(clientID)))
				conn, err := net.DialTimeout("tcp", sharedListener.Addr().String(), 3*time.Second)
				if err != nil {
					return
				}
				defer conn.Close()
				reader := bufio.NewReaderSize(conn, 256)

				for j := 0; j < opsPerClient; j++ {
					if ctx.Err() != nil {
						return
					}

					var key string
					if sharedKeys == 1 {
						key = fmt.Sprintf("con:fuzz:%d", rng.Intn(5)) // shared: 5 keys
					} else {
						key = fmt.Sprintf("con:fuzz:%d_%d", clientID, j)
					}

					op := rng.Intn(6)
					switch op {
					case 0: // SET + GET contention
						sendRESP(conn, "SET", key, randString(rng, 8))
						_, _ = proto.ReadRESP(reader)
						sendRESP(conn, "GET", key)
						_, _ = proto.ReadRESP(reader)
					case 1: // INCR contention
						sendRESP(conn, "INCR", key)
						_, _ = proto.ReadRESP(reader)
					case 2: // LPUSH + LPOP
						sendRESP(conn, "LPUSH", key, randString(rng, 8))
						_, _ = proto.ReadRESP(reader)
					case 3: // DEL contention
						sendRESP(conn, "DEL", key)
						_, _ = proto.ReadRESP(reader)
					case 4: // WATCH + MULTI + EXEC
						sendRESP(conn, "WATCH", key)
						_, _ = proto.ReadRESP(reader)
						sendRESP(conn, "MULTI")
						_, _ = proto.ReadRESP(reader)
						sendRESP(conn, "SET", key, randString(rng, 8))
						_, _ = proto.ReadRESP(reader)
						sendRESP(conn, "EXEC")
						_, _ = proto.ReadRESP(reader)
					case 5: // HSET + HGET
						field := randString(rng, 4)
						sendRESP(conn, "HSET", key, field, randString(rng, 8))
						_, _ = proto.ReadRESP(reader)
					}
				}
			}(i)
		}

		wg.Wait()
		cancel()

		ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel2()
		if err := sharedClient.Ping(ctx2).Err(); err != nil {
			t.Fatalf("server not responsive after concurrent fuzz: %v", err)
		}
	})
}

var _ sync.Locker
