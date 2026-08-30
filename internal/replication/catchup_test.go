package replication

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/zeebo/assert"
)

func TestCatchUpAndEnableSlave_EmptyGapSetsReady(t *testing.T) {
	t.Parallel()
	rm := NewReplicationManager(setupTestStore(t))
	defer rm.Stop()

	slave := NewSlaveConnection(newMockConn())
	rm.AddSlave(slave)

	start := rm.GetMasterReplOffset()
	if err := rm.CatchUpAndEnableSlave(slave, start); err != nil {
		t.Fatalf("empty-gap catch-up: %v", err)
	}
	assert.True(t, slave.IsReady())
	assert.Equal(t, start, slave.GetReplOffset())
}

func TestCatchUpAndEnableSlave_SendFailureLeavesNotReady(t *testing.T) {
	t.Parallel()
	rm := NewReplicationManager(setupTestStore(t))
	defer rm.Stop()

	rm.PropagateCommand([][]byte{[]byte("SET"), []byte("k"), []byte("v")})
	assert.True(t, rm.GetMasterReplOffset() > 0)

	conn := newMockConn()
	conn.writeErr = fmt.Errorf("write boom")
	slave := NewSlaveConnection(conn)
	rm.AddSlave(slave)

	err := rm.CatchUpAndEnableSlave(slave, 0)
	assert.True(t, err != nil)
	assert.False(t, slave.IsReady())
	assert.Equal(t, int64(0), slave.GetReplOffset())
	assert.Equal(t, 1, rm.GetSlaveCount())
}

func TestCatchUpAndEnableSlave_ConcurrentPropagateNoDupNoHole(t *testing.T) {
	t.Parallel()
	rm := NewReplicationManager(setupTestStore(t))
	defer rm.Stop()

	const (
		prefill   = 20
		writers   = 8
		perWriter = 40
	)
	for i := 0; i < prefill; i++ {
		rm.PropagateCommand([][]byte{[]byte("SET"), []byte(fmt.Sprintf("pre:%d", i)), []byte("1")})
	}
	startOffset := rm.GetMasterReplOffset()
	assert.True(t, startOffset > 0)

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
				rm.PropagateCommand([][]byte{
					[]byte("SET"),
					[]byte(fmt.Sprintf("live:%d", n)),
					[]byte("1"),
				})
			}
		}()
	}

	if err := rm.CatchUpAndEnableSlave(slave, startOffset); err != nil {
		t.Fatalf("catch-up: %v", err)
	}
	wg.Wait()

	assert.True(t, slave.IsReady())
	assert.Equal(t, int64(0), rm.GetReplSendDropCount())
	// SendCommand stores the command's start offset, not the backlog
	// watermark, so slave.GetReplOffset() may sit one command behind
	// GetMasterReplOffset() after a live push. Completeness is the stream.

	got := parseBacklogCommands(t, conn.writeBuffer)
	want := writers * perWriter
	if len(got) != want {
		t.Fatalf("catch-up+live stream: got %d commands, want %d (dup or hole)", len(got), want)
	}
	seen := make(map[string]int, want)
	for _, args := range got {
		if len(args) != 3 || string(args[0]) != "SET" {
			t.Fatalf("unexpected command on slave stream: %q", args)
		}
		key := string(args[1])
		seen[key]++
		if seen[key] > 1 {
			t.Fatalf("command for %s delivered twice (gap-fill raced with live push)", key)
		}
	}
	if len(seen) != want {
		t.Fatalf("unique keys=%d want=%d", len(seen), want)
	}
}

func parseBacklogCommands(t *testing.T, data []byte) [][][]byte {
	t.Helper()
	r := bufio.NewReader(bytes.NewReader(data))
	var out [][][]byte
	for {
		resp, err := proto.ReadRESP(r)
		if err != nil {
			if err == io.EOF {
				return out
			}
			t.Fatalf("parse slave stream after %d commands: %v", len(out), err)
		}
		out = append(out, resp.Args)
	}
}
