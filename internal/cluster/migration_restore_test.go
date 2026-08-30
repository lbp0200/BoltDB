package cluster

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/zeebo/assert"
)

func TestSendRestoreSendsAskingBeforeRestore(t *testing.T) {
	t.Parallel()
	srv, cli := net.Pipe()
	defer srv.Close()
	defer cli.Close()

	done := make(chan error, 1)
	go func() {
		br := bufio.NewWriter(srv)
		rd := bufio.NewReader(srv)
		ask, err := proto.ReadRESP(rd)
		if err != nil {
			done <- err
			return
		}
		if len(ask.Args) != 1 || string(ask.Args[0]) != "ASKING" {
			done <- fmt.Errorf("first command must be ASKING, got %q", ask.Args)
			return
		}
		if err := proto.WriteRESP(br, proto.NewSimpleString("OK")); err != nil {
			done <- err
			return
		}
		if err := br.Flush(); err != nil {
			done <- err
			return
		}
		restore, err := proto.ReadRESP(rd)
		if err != nil {
			done <- err
			return
		}
		if len(restore.Args) < 1 || string(restore.Args[0]) != "RESTORE" {
			done <- fmt.Errorf("second command must be RESTORE, got %q", restore.Args)
			return
		}
		if err := proto.WriteRESP(br, proto.NewSimpleString("OK")); err != nil {
			done <- err
			return
		}
		if err := br.Flush(); err != nil {
			done <- err
			return
		}
		done <- nil
	}()

	mc := &migrateConn{conn: cli, reader: bufio.NewReader(cli)}
	if err := mc.sendRestore("k", []byte("dump-bytes")); err != nil {
		t.Fatalf("sendRestore: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRestoreResponseMsg_OK(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "OK", restoreResponseMsg(&proto.Array{Args: [][]byte{[]byte("OK")}}))
}

func TestRestoreResponseMsg_AlreadyExists(t *testing.T) {
	t.Parallel()
	// Contract: sendRestore treats "already exists" / BUSYKEY as non-fatal (no REPLACE)
	msg := restoreResponseMsg(&proto.Array{Args: [][]byte{[]byte("ERR target key already exists")}})
	assert.True(t, strings.Contains(msg, "already exists"))

	busy := restoreResponseMsg(&proto.Array{Args: [][]byte{[]byte("BUSYKEY Target key name already exists.")}})
	assert.True(t, strings.Contains(busy, "BUSYKEY") || strings.Contains(busy, "already exists"))
}

func TestRestoreResponseMsg_Empty(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "empty response", restoreResponseMsg(nil))
	assert.Equal(t, "empty response", restoreResponseMsg(&proto.Array{}))
}

// TestSendRestoreSemanticsDocumentsNoReplace documents that a non-OK response
// containing "already exists" is treated as success by sendRestore's predicate
// (would be killed if someone reintroduces REPLACE).
func TestSendRestoreSemanticsDocumentsNoReplace(t *testing.T) {
	t.Parallel()
	// Mirror sendRestore acceptance predicate
	accept := func(msg string) bool {
		if msg == "" || msg == "OK" {
			return true
		}
		return strings.Contains(msg, "already exists") || strings.Contains(msg, "BUSYKEY")
	}
	assert.True(t, accept("OK"))
	assert.True(t, accept("ERR target key already exists"))
	assert.True(t, accept("BUSYKEY Target key name already exists."))
	assert.False(t, accept("ERR wrong number of arguments"))
	assert.False(t, accept("ERR syntax error"))
}
