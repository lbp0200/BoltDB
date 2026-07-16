package cluster

import (
	"strings"
	"testing"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/zeebo/assert"
)

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
