package integration

import (
	"bufio"
	"context"
	"math/rand"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/proto"
)

// sendRaw writes raw bytes to the connection without protocol framing.
func sendRaw(t *testing.T, conn net.Conn, data string) {
	t.Helper()
	_, err := conn.Write([]byte(data))
	if err != nil {
		t.Logf("sendRaw write: %v", err)
	}
}

// sendRespCmd sends a properly framed RESP command and returns without reading.
func sendRespCmd(t *testing.T, conn net.Conn, cmd string, args ...string) {
	t.Helper()
	cmdArgs := make([][]byte, 1+len(args))
	cmdArgs[0] = []byte(cmd)
	for i, arg := range args {
		cmdArgs[i+1] = []byte(arg)
	}
	if err := proto.WriteRESP(conn, &proto.Array{Args: cmdArgs}); err != nil {
		t.Logf("sendRespCmd: %v", err)
	}
}

// readRespOrNil attempts to read a RESP response; returns nil on error.
func readRespOrNil(t *testing.T, reader *bufio.Reader) proto.RESP {
	t.Helper()
	resp, err := proto.ReadRESP(reader)
	if err != nil {
		return nil
	}
	return resp
}

// dial connects to the server via raw TCP.
func dial(t *testing.T, addr string) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return conn, bufio.NewReader(conn)
}

// waitForServer blocks until a TCP connection succeeds or timeout.
func waitForServer(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("server at %s not reachable after %v", addr, timeout)
}

// ---------- Tests ----------

// TestConnectionInterrupt_DuringCommand verifies the server handles a client
// that disconnects immediately after sending a command (before reading response).
func TestConnectionInterrupt_DuringCommand(t *testing.T) {
	srv := StartIsolatedServer(t)
	baseline := baselineGoroutines(t)

	const iterations = 20
	for i := 0; i < iterations; i++ {
		conn, _ := dial(t, srv.Addr)
		// Send a valid SET command, then immediately close
		sendRespCmd(t, conn, "SET", "interrupt:key", "value")
		conn.Close()
		time.Sleep(50 * time.Millisecond)
	}

	time.Sleep(goroutineSettleTime)
	final := runtime.NumGoroutine()
	leak := final - baseline
	if leak > goroutineTolerance {
		t.Errorf("goroutine leak after interrupt-during-command: %d (baseline=%d, final=%d)",
			leak, baseline, final)
	}

	// Verify server is still functional after interrupt-during-command
	verifyServerLiveness(t, srv)
}

// TestConnectionInterrupt_DuringBigResponse disconnects while the server is
// sending a large response (1000-element LRANGE), testing write-side resilience.
func TestConnectionInterrupt_DuringBigResponse(t *testing.T) {
	srv := StartIsolatedServer(t)
	ctx := context.Background()

	// Populate a large list
	pipe := srv.Client.Pipeline()
	for i := 0; i < 1000; i++ {
		pipe.RPush(ctx, "biglist", "val")
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("pipeline: %v", err)
	}

	baseline := baselineGoroutines(t)

	const iterations = 10
	for i := 0; i < iterations; i++ {
		conn, reader := dial(t, srv.Addr)
		// Send LRANGE for 1000 elements
		sendRespCmd(t, conn, "LRANGE", "biglist", "0", "-1")

		// Read partial response then close
		buf := make([]byte, 4096)
		_ = reader /* ensure reader is used */
		n, _ := conn.Read(buf)
		t.Logf("iteration %d: read %d bytes before close", i, n)
		conn.Close()
		time.Sleep(50 * time.Millisecond)
	}

	time.Sleep(goroutineSettleTime)
	final := runtime.NumGoroutine()
	leak := final - baseline
	if leak > goroutineTolerance {
		t.Errorf("goroutine leak after big-response interrupt: %d (baseline=%d, final=%d)",
			leak, baseline, final)
	}

	// Verify list data is still intact after interrupted reads
	length, err := srv.Client.LLen(ctx, "biglist").Result()
	if err != nil {
		t.Fatalf("LLen after big-response interrupt: %v", err)
	}
	if length != 1000 {
		t.Errorf("LLen after interrupt = %d, want 1000 (data integrity check)", length)
	}
	verifyServerLiveness(t, srv)
}

// TestConnectionInterrupt_GarbageData sends random binary data that doesn't
// conform to RESP protocol, verifying the server rejects and closes gracefully.
func TestConnectionInterrupt_GarbageData(t *testing.T) {
	srv := StartIsolatedServer(t)
	baseline := baselineGoroutines(t)

	const iterations = 20
	for i := 0; i < iterations; i++ {
		conn, _ := dial(t, srv.Addr)
		// Send random garbage
		garbage := make([]byte, 64)
		for j := range garbage {
			garbage[j] = byte(rand.Intn(256))
		}
		conn.Write(garbage)
		conn.Close()
		time.Sleep(50 * time.Millisecond)
	}

	time.Sleep(goroutineSettleTime)
	final := runtime.NumGoroutine()
	leak := final - baseline
	if leak > goroutineTolerance {
		t.Errorf("goroutine leak after garbage data: %d (baseline=%d, final=%d)",
			leak, baseline, final)
	}

	// Verify server is still functional after garbage data
	verifyServerLiveness(t, srv)
}

// TestConnectionInterrupt_PartialRESP sends an incomplete RESP command
// (header says 3 args but only sends 1), then closes the connection.
func TestConnectionInterrupt_PartialRESP(t *testing.T) {
	srv := StartIsolatedServer(t)
	baseline := baselineGoroutines(t)

	const iterations = 20
	for i := 0; i < iterations; i++ {
		conn, _ := dial(t, srv.Addr)
		// Partial RESP: *3\r\n$3\r\nSET\r\n (only 1 of 3 args)
		sendRaw(t, conn, "*3\r\n$3\r\nSET\r\n")
		time.Sleep(20 * time.Millisecond)
		conn.Close()
		time.Sleep(50 * time.Millisecond)
	}

	time.Sleep(goroutineSettleTime)
	final := runtime.NumGoroutine()
	leak := final - baseline
	if leak > goroutineTolerance {
		t.Errorf("goroutine leak after partial RESP: %d (baseline=%d, final=%d)",
			leak, baseline, final)
	}

	// Verify server is still functional after partial RESP
	verifyServerLiveness(t, srv)
}

// TestConnectionInterrupt_PipelinedCommands sends multiple commands in one
// write (pipelining) then disconnects before reading any response.
func TestConnectionInterrupt_PipelinedCommands(t *testing.T) {
	srv := StartIsolatedServer(t)
	baseline := baselineGoroutines(t)

	const iterations = 20
	for i := 0; i < iterations; i++ {
		conn, _ := dial(t, srv.Addr)
		// Pipeline 5 commands
		for j := 0; j < 5; j++ {
			sendRespCmd(t, conn, "SET", "pipe:key", "val")
		}
		conn.Close()
		time.Sleep(50 * time.Millisecond)
	}

	time.Sleep(goroutineSettleTime)
	final := runtime.NumGoroutine()
	leak := final - baseline
	if leak > goroutineTolerance {
		t.Errorf("goroutine leak after pipelined interrupt: %d (baseline=%d, final=%d)",
			leak, baseline, final)
	}

	// Verify server is functional; pipelined SETs may or may not have persisted
	verifyServerLiveness(t, srv)
}

// TestConnectionInterrupt_RapidConnectDisconnect performs rapid connect/close
// cycles to stress the connection registration/unregistration path.
func TestConnectionInterrupt_RapidConnectDisconnect(t *testing.T) {
	srv := StartIsolatedServer(t)
	baseline := baselineGoroutines(t)

	const iterations = 50
	for i := 0; i < iterations; i++ {
		conn, err := net.DialTimeout("tcp", srv.Addr, time.Second)
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		conn.Close()
	}

	time.Sleep(goroutineSettleTime)
	final := runtime.NumGoroutine()
	leak := final - baseline
	if leak > goroutineTolerance {
		t.Errorf("goroutine leak after rapid connect/disconnect: %d (baseline=%d, final=%d)",
			leak, baseline, final)
	}

	// Verify server is functional after rapid connect/disconnect
	verifyServerLiveness(t, srv)
}

// TestConnectionInterrupt_ServerSurvivesAndServesNewClients verifies that after
// many interrupted connections, the server still correctly serves new requests.
func TestConnectionInterrupt_ServerSurvivesAndServesNewClients(t *testing.T) {
	srv := StartIsolatedServer(t)
	ctx := context.Background()

	// Phase 1: interrupt many connections
	for i := 0; i < 30; i++ {
		conn, _ := dial(t, srv.Addr)
		sendRespCmd(t, conn, "SET", "interrupted", "yes")
		conn.Close()
	}

	// Phase 2: verify server still works with go-redis client
	err := srv.Client.Set(ctx, "after:interrupt", "works", 0).Err()
	if err != nil {
		t.Fatalf("SET after interrupts: %v", err)
	}
	val, err := srv.Client.Get(ctx, "after:interrupt").Result()
	if err != nil {
		t.Fatalf("GET after interrupts: %v", err)
	}
	if val != "works" {
		t.Errorf("GET after interrupts = %q, want %q", val, "works")
	}
}

// TestConnectionInterrupt_ServerSurvivesAfterGarbageAndInterruption mix
// combines garbage data, partial RESP, and premature disconnects.
func TestConnectionInterrupt_ServerSurvivesAfterGarbageAndInterruption(t *testing.T) {
	srv := StartIsolatedServer(t)
	ctx := context.Background()

	// Garbage data
	for i := 0; i < 10; i++ {
		conn, _ := dial(t, srv.Addr)
		conn.Write([]byte{0xFF, 0xFE, 0x00, 0x01})
		conn.Close()
	}

	// Partial RESP
	for i := 0; i < 10; i++ {
		conn, _ := dial(t, srv.Addr)
		sendRaw(t, conn, "*2\r\n$3\r\nGET\r\n")
		conn.Close()
	}

	// Mid-response disconnect
	for i := 0; i < 10; i++ {
		conn, _ := dial(t, srv.Addr)
		sendRespCmd(t, conn, "SET", "key", strings.Repeat("v", 10000))
		// Read a little then close
		conn.Read(make([]byte, 100))
		conn.Close()
	}

	// Verify server is still functional
	err := srv.Client.Set(ctx, "survival:check", "ok", 0).Err()
	if err != nil {
		t.Fatalf("SET after mixed disruptions: %v", err)
	}
	val, err := srv.Client.Get(ctx, "survival:check").Result()
	if err != nil {
		t.Fatalf("GET after mixed disruptions: %v", err)
	}
	if val != "ok" {
		t.Errorf("GET = %q, want %q", val, "ok")
	}
}

// TestConnectionInterrupt_ConcurrentInterrupts sends commands from many
// goroutines simultaneously, each closing immediately after sending.
func TestConnectionInterrupt_ConcurrentInterrupts(t *testing.T) {
	srv := StartIsolatedServer(t)
	baseline := baselineGoroutines(t)

	const goroutines = 30
	done := make(chan struct{}, goroutines)
	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			defer func() { done <- struct{}{} }()
			conn, err := net.DialTimeout("tcp", srv.Addr, time.Second)
			if err != nil {
				t.Errorf("goroutine %d dial: %v", idx, err)
				return
			}
			sendRespCmd(t, conn, "SET", "conc:key", "val")
			conn.Close()
		}(i)
	}

	for i := 0; i < goroutines; i++ {
		<-done
	}

	time.Sleep(goroutineSettleTime)
	final := runtime.NumGoroutine()
	leak := final - baseline
	if leak > goroutineTolerance {
		t.Errorf("goroutine leak after concurrent interrupts: %d (baseline=%d, final=%d)",
			leak, baseline, final)
	}

	// Verify server is functional after concurrent interrupts
	verifyServerLiveness(t, srv)
}

// TestConnectionInterrupt_EmptyConnection opens a connection and immediately
// closes it without sending anything.
func TestConnectionInterrupt_EmptyConnection(t *testing.T) {
	srv := StartIsolatedServer(t)
	baseline := baselineGoroutines(t)

	const iterations = 50
	for i := 0; i < iterations; i++ {
		conn, err := net.DialTimeout("tcp", srv.Addr, time.Second)
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		conn.Close()
	}

	time.Sleep(goroutineSettleTime)
	final := runtime.NumGoroutine()
	leak := final - baseline
	if leak > goroutineTolerance {
		t.Errorf("goroutine leak after empty connections: %d (baseline=%d, final=%d)",
			leak, baseline, final)
	}

	// Verify server is functional after empty connections
	verifyServerLiveness(t, srv)
}

// TestConnectionInterrupt_BlankLineThenClose sends just \r\n (empty RESP line)
// which the parser should reject.
func TestConnectionInterrupt_BlankLineThenClose(t *testing.T) {
	srv := StartIsolatedServer(t)
	baseline := baselineGoroutines(t)

	const iterations = 20
	for i := 0; i < iterations; i++ {
		conn, _ := dial(t, srv.Addr)
		sendRaw(t, conn, "\r\n")
		conn.Close()
		time.Sleep(50 * time.Millisecond)
	}

	time.Sleep(goroutineSettleTime)
	final := runtime.NumGoroutine()
	leak := final - baseline
	if leak > goroutineTolerance {
		t.Errorf("goroutine leak after blank lines: %d (baseline=%d, final=%d)",
			leak, baseline, final)
	}

	// Verify server is functional after blank lines
	verifyServerLiveness(t, srv)
}

// TestConnectionInterrupt_MultipleCmdThenClose sends multiple valid commands
// (SET + GET + DEL), reads only the first response, then closes.
func TestConnectionInterrupt_MultipleCmdThenClose(t *testing.T) {
	srv := StartIsolatedServer(t)
	ctx := context.Background()
	baseline := baselineGoroutines(t)

	const iterations = 20
	for i := 0; i < iterations; i++ {
		conn, reader := dial(t, srv.Addr)
		sendRespCmd(t, conn, "SET", "multi:cmd", "v")
		sendRespCmd(t, conn, "GET", "multi:cmd")
		sendRespCmd(t, conn, "DEL", "multi:cmd")

		// Read only the first response
		_ = readPubSubResp(t, reader)
		conn.Close()
		time.Sleep(50 * time.Millisecond)
	}

	time.Sleep(goroutineSettleTime)
	final := runtime.NumGoroutine()
	leak := final - baseline
	if leak > goroutineTolerance {
		t.Errorf("goroutine leak after multi-cmd interrupt: %d (baseline=%d, final=%d)",
			leak, baseline, final)
	}

	// Verify server works
	err := srv.Client.Set(ctx, "after:multi", "ok", 0).Err()
	if err != nil {
		t.Fatalf("SET after multi-cmd interrupt: %v", err)
	}
}

// TestConnectionInterrupt_SlowReadDuringResponse sends a command that returns
// a large response, starts reading slowly (byte-by-byte), then gives up.
func TestConnectionInterrupt_SlowReadDuringResponse(t *testing.T) {
	srv := StartIsolatedServer(t)
	ctx := context.Background()

	// Populate a list
	pipe := srv.Client.Pipeline()
	for i := 0; i < 500; i++ {
		pipe.RPush(ctx, "slowlist", "data")
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("pipeline: %v", err)
	}

	baseline := baselineGoroutines(t)

	const iterations = 10
	for i := 0; i < iterations; i++ {
		conn, _ := dial(t, srv.Addr)
		sendRespCmd(t, conn, "LRANGE", "slowlist", "0", "-1")

		// Read byte-by-byte for a bit, then close
		buf := make([]byte, 1)
		for j := 0; j < 200; j++ {
			if _, err := conn.Read(buf); err != nil {
				break
			}
		}
		conn.Close()
		time.Sleep(50 * time.Millisecond)
	}

	time.Sleep(goroutineSettleTime)
	final := runtime.NumGoroutine()
	leak := final - baseline
	if leak > goroutineTolerance {
		t.Errorf("goroutine leak after slow read: %d (baseline=%d, final=%d)",
			leak, baseline, final)
	}

	// Verify list data is still intact after slow-read interruption
	length, err := srv.Client.LLen(ctx, "slowlist").Result()
	if err != nil {
		t.Fatalf("LLen after slow read: %v", err)
	}
	if length != 500 {
		t.Errorf("LLen after slow read = %d, want 500 (data integrity check)", length)
	}
	verifyServerLiveness(t, srv)
}

// TestConnectionInterrupt_MassiveConcurrentStress combines many goroutines
// doing connect → SET → close in parallel to stress the server.
func TestConnectionInterrupt_MassiveConcurrentStress(t *testing.T) {
	srv := StartIsolatedServer(t)
	ctx := context.Background()
	baseline := baselineGoroutines(t)

	const goroutines = 100
	done := make(chan struct{}, goroutines)
	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			defer func() { done <- struct{}{} }()
			conn, err := net.DialTimeout("tcp", srv.Addr, time.Second)
			if err != nil {
				return // server may throttle
			}
			_ = proto.WriteRESP(conn, &proto.Array{Args: [][]byte{
				[]byte("SET"),
				[]byte("stress"),
				[]byte("val"),
			}})
			// Random small delay before close
			time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
			conn.Close()
		}(i)
	}

	for i := 0; i < goroutines; i++ {
		<-done
	}

	time.Sleep(goroutineSettleTime)
	final := runtime.NumGoroutine()
	leak := final - baseline
	if leak > goroutineTolerance*2 { // higher tolerance for stress test
		t.Errorf("goroutine leak after massive stress: %d (baseline=%d, final=%d)",
			leak, baseline, final)
	}

	// Verify server is functional and data persisted after massive stress
	val, err := srv.Client.Get(ctx, "stress").Result()
	if err != nil {
		t.Fatalf("GET stress key after massive stress: %v", err)
	}
	if val != "val" {
		t.Errorf("stress key = %q, want %q", val, "val")
	}
	verifyServerLiveness(t, srv)
}
