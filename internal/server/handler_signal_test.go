package server

import (
	"testing"

	"github.com/zeebo/assert"
)

// TestReplicationTakeoverSignal_String tests String method
func TestReplicationTakeoverSignal_String(t *testing.T) {
	sig := ReplicationTakeoverSignal{}
	str := sig.String()
	assert.Equal(t, "replication-takeover", str)
}

// TestReplicationTakeoverSignal_Error tests Error method
func TestReplicationTakeoverSignal_Error(t *testing.T) {
	sig := ReplicationTakeoverSignal{}
	err := sig.Error()
	assert.Equal(t, "replication takeover", err)
}

// TestReplicationTakeoverSignal_IsError tests IsError method
func TestReplicationTakeoverSignal_IsError(t *testing.T) {
	sig := ReplicationTakeoverSignal{}
	// IsError returns false
	assert.False(t, sig.IsError())
}
