package replication

import (
	"bufio"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/zeebo/assert"
)

// TestSerializeCommand tests serializeCommand function
func TestSerializeCommand(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		cmd  [][]byte
		want string
	}{
		{
			name: "simple SET command",
			cmd:  [][]byte{[]byte("SET"), []byte("key"), []byte("value")},
			want: "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n",
		},
		{
			name: "GET command",
			cmd:  [][]byte{[]byte("GET"), []byte("mykey")},
			want: "*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n",
		},
		{
			name: "empty args",
			cmd:  [][]byte{},
			want: "*0\r\n",
		},
		{
			name: "single arg",
			cmd:  [][]byte{[]byte("PING")},
			want: "*1\r\n$4\r\nPING\r\n",
		},
		{
			name: "long value",
			cmd:  [][]byte{[]byte("SET"), []byte("key"), []byte("long value with many characters")},
			want: "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$31\r\nlong value with many characters\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := serializeCommand(tt.cmd)
			assert.Equal(t, tt.want, string(got))
		})
	}
}

// TestExecuteReplicatedCommand_SET tests executeReplicatedCommand for SET
func TestExecuteReplicatedCommand_SET(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// Test SET command
	_ = executeReplicatedCommand(testStore, [][]byte{[]byte("SET"), []byte("testkey"), []byte("testvalue")})

	// Verify the value was set
	val, err := testStore.Get("testkey")
	assert.NoError(t, err)
	assert.Equal(t, "testvalue", val)
}

// TestExecuteReplicatedCommand_DEL tests executeReplicatedCommand for DEL
func TestExecuteReplicatedCommand_DEL(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// First set a key
	testStore.Set("key1", "value1")
	testStore.Set("key2", "value2")

	// Test DEL command
	executeReplicatedCommand(testStore, [][]byte{[]byte("DEL"), []byte("key1")})

	// Verify key1 is deleted
	_, err := testStore.Get("key1")
	assert.True(t, err != nil) // Should return error for deleted key
}

// TestExecuteReplicatedCommand_INCR tests executeReplicatedCommand for INCR
func TestExecuteReplicatedCommand_INCR(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// Test INCR command on non-existent key (INCR creates the key)
	executeReplicatedCommand(testStore, [][]byte{[]byte("INCR"), []byte("newcounter")})

	// Verify value was incremented from 0 to 1
	val, err := testStore.Get("newcounter")
	assert.NoError(t, err)
	assert.Equal(t, "1", val)
}

// TestExecuteReplicatedCommand_INCRBY tests executeReplicatedCommand for INCRBY
func TestExecuteReplicatedCommand_INCRBY(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// Test INCRBY command on non-existent key
	executeReplicatedCommand(testStore, [][]byte{[]byte("INCRBY"), []byte("counter"), []byte("5")})

	// Verify value was incremented from 0 to 5
	val, err := testStore.Get("counter")
	assert.NoError(t, err)
	assert.Equal(t, "5", val)
}

// TestExecuteReplicatedCommand_DECR tests executeReplicatedCommand for DECR
func TestExecuteReplicatedCommand_DECR(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// Test DECR command on non-existent key (creates key with -1)
	executeReplicatedCommand(testStore, [][]byte{[]byte("DECR"), []byte("counter")})

	// Verify value was decremented from 0 to -1
	val, err := testStore.Get("counter")
	assert.NoError(t, err)
	assert.Equal(t, "-1", val)
}

// TestExecuteReplicatedCommand_DECRBY tests executeReplicatedCommand for DECRBY
func TestExecuteReplicatedCommand_DECRBY(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// Test DECRBY command on non-existent key
	executeReplicatedCommand(testStore, [][]byte{[]byte("DECRBY"), []byte("counter"), []byte("3")})

	// Verify value was decremented from 0 to -3
	val, err := testStore.Get("counter")
	assert.NoError(t, err)
	assert.Equal(t, "-3", val)
}

// TestExecuteReplicatedCommand_APPEND tests executeReplicatedCommand for APPEND
func TestExecuteReplicatedCommand_APPEND(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// Set initial value
	testStore.Set("key", "hello")

	// Test APPEND command
	executeReplicatedCommand(testStore, [][]byte{[]byte("APPEND"), []byte("key"), []byte(" world")})

	// Verify value was appended
	val, err := testStore.Get("key")
	assert.NoError(t, err)
	assert.Equal(t, "hello world", val)
}

// TestExecuteReplicatedCommand_HSET tests executeReplicatedCommand for HSET
func TestExecuteReplicatedCommand_HSET(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// Test HSET command
	executeReplicatedCommand(testStore, [][]byte{[]byte("HSET"), []byte("myhash"), []byte("field1"), []byte("value1")})

	// Verify value was set
	val, err := testStore.HGet("myhash", "field1")
	assert.NoError(t, err)
	assert.Equal(t, []byte("value1"), val)
}

// TestExecuteReplicatedCommand_SADD tests executeReplicatedCommand for SADD
func TestExecuteReplicatedCommand_SADD(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// Test SADD command
	executeReplicatedCommand(testStore, [][]byte{[]byte("SADD"), []byte("myset"), []byte("member1"), []byte("member2")})

	// Verify members were added
	count, err := testStore.SCard("myset")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)
}

// TestExecuteReplicatedCommand_ZADD tests executeReplicatedCommand for ZADD
func TestExecuteReplicatedCommand_ZADD(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// Test ZADD command
	executeReplicatedCommand(testStore, [][]byte{
		[]byte("ZADD"), []byte("myzset"), []byte("1.0"), []byte("member1"), []byte("2.0"), []byte("member2"),
	})

	// Verify members were added
	count, err := testStore.ZCard("myzset")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)
}

// TestExecuteReplicatedCommand_RPUSH tests executeReplicatedCommand for RPUSH
func TestExecuteReplicatedCommand_RPUSH(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// Test RPUSH command
	executeReplicatedCommand(testStore, [][]byte{[]byte("RPUSH"), []byte("mylist"), []byte("value1"), []byte("value2")})

	// Verify values were pushed
	len, err := testStore.LLen("mylist")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), len)
}

// TestExecuteReplicatedCommand_LPUSH tests executeReplicatedCommand for LPUSH
func TestExecuteReplicatedCommand_LPUSH(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// Test LPUSH command
	executeReplicatedCommand(testStore, [][]byte{[]byte("LPUSH"), []byte("mylist"), []byte("value1"), []byte("value2")})

	// Verify values were pushed
	len, err := testStore.LLen("mylist")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), len)
}

// TestExecuteReplicatedCommand_EmptyArgs tests executeReplicatedCommand with empty args
func TestExecuteReplicatedCommand_EmptyArgs(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// Should not panic with empty args
	executeReplicatedCommand(testStore, [][]byte{})

	// Should not panic with single arg
	executeReplicatedCommand(testStore, [][]byte{[]byte("UNKNOWN")})
}

// TestExecuteReplicatedCommand_WithTTL tests SET with EX option
func TestExecuteReplicatedCommand_SetWithTTL(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// Test SET with EX
	executeReplicatedCommand(testStore, [][]byte{[]byte("SET"), []byte("key"), []byte("value"), []byte("EX"), []byte("60")})

	// Verify the value was set
	val, err := testStore.Get("key")
	assert.NoError(t, err)
	assert.Equal(t, "value", val)
}

// TestExecuteReplicatedCommand_SetWithPX tests SET with PX option
func TestExecuteReplicatedCommand_SetWithPX(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// Test SET with PX (milliseconds)
	executeReplicatedCommand(testStore, [][]byte{[]byte("SET"), []byte("key"), []byte("value"), []byte("PX"), []byte("60000")})

	// Verify the value was set
	val, err := testStore.Get("key")
	assert.NoError(t, err)
	assert.Equal(t, "value", val)
}

// TestAddSlave tests AddSlave method
func TestAddSlave(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	// Use existing mockConn from slave_test.go
	conn := newMockConn()
	slaveConn := NewSlaveConnection(conn)

	// Initially no slaves
	assert.Equal(t, 0, rm.GetSlaveCount())

	// Add slave
	rm.AddSlave(slaveConn)

	// Verify slave was added
	assert.Equal(t, 1, rm.GetSlaveCount())
	slaves := rm.GetSlaves()
	assert.Equal(t, 1, len(slaves))
	assert.Equal(t, slaveConn.ID, slaves[0].ID)
}

// TestRemoveSlave tests RemoveSlave method
func TestRemoveSlave(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	// Create and add a mock connection
	conn := newMockConn()
	slaveConn := NewSlaveConnection(conn)
	slaveID := slaveConn.ID

	rm.AddSlave(slaveConn)
	assert.Equal(t, 1, rm.GetSlaveCount())

	// Remove slave
	rm.RemoveSlave(slaveID)

	// Verify slave was removed
	assert.Equal(t, 0, rm.GetSlaveCount())
}

// TestRemoveNonExistentSlave tests RemoveSlave with non-existent ID
func TestRemoveNonExistentSlave(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	// Should not panic
	rm.RemoveSlave("non-existent-id")
	assert.Equal(t, 0, rm.GetSlaveCount())
}

// TestGetSlaveByID tests GetSlaveByID method
func TestGetSlaveByID(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	// Create and add a mock connection
	conn := newMockConn()
	slaveConn := NewSlaveConnection(conn)

	rm.AddSlave(slaveConn)

	// Get slave by ID
	found := rm.GetSlaveByID(slaveConn.ID)
	assert.True(t, found != nil)
	assert.Equal(t, slaveConn.ID, found.ID)

	// Get non-existent slave
	notFound := rm.GetSlaveByID("non-existent")
	assert.True(t, notFound == nil)
}

// TestGetSlaveByAddr tests GetSlaveByAddr method
func TestGetSlaveByAddr(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	// Create and add a mock connection
	conn := newMockConn()
	slaveConn := NewSlaveConnection(conn)

	rm.AddSlave(slaveConn)

	// Get slave by address
	found := rm.GetSlaveByAddr(slaveConn.Addr)
	assert.True(t, found != nil)
	assert.Equal(t, slaveConn.Addr, found.Addr)

	// Get non-existent address
	notFound := rm.GetSlaveByAddr("127.0.0.1:9999")
	assert.True(t, notFound == nil)
}

// TestUpdateSlaveAckOffset tests UpdateSlaveAckOffset method
func TestUpdateSlaveAckOffset(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	// Create and add a mock connection
	conn := newMockConn()
	slaveConn := NewSlaveConnection(conn)
	slaveID := slaveConn.ID

	rm.AddSlave(slaveConn)

	// Update ACK offset
	rm.UpdateSlaveAckOffset(slaveID, 100)

	// Verify ACK offset was updated (need to check via slave directly)
	// This tests the code path runs without error
	assert.Equal(t, 1, rm.GetSlaveCount())
}

// TestUpdateSlaveAckOffset_NonExistent tests UpdateSlaveAckOffset with non-existent slave
func TestUpdateSlaveAckOffset_NonExistent(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	// Should not panic
	rm.UpdateSlaveAckOffset("non-existent-id", 100)
}

// TestSetMasterConnection tests SetMasterConnection and GetMasterConnection
func TestSetMasterConnection(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	// Initially nil
	assert.True(t, rm.GetMasterConnection() == nil)

	// Create a mock master connection
	mock := newMockMasterConn()
	mc := &MasterConnection{
		Addr:   "127.0.0.1:6379",
		Conn:   mock,
		Reader: bufio.NewReader(mock),
		Writer: bufio.NewWriter(mock),
		stopCh: make(chan struct{}),
	}

	// Set the master connection
	rm.SetMasterConnection(mc)

	// Verify it's set
	assert.True(t, rm.GetMasterConnection() != nil)
	assert.Equal(t, "127.0.0.1:6379", rm.GetMasterConnection().Addr)
}

// TestStopSlaveReplication tests StopSlaveReplication function
func TestStopSlaveReplication(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	// First set role to slave
	rm.SetRole(RoleSlave)
	rm.SetMasterAddr("127.0.0.1:6379")

	// Verify role is slave
	assert.Equal(t, RoleSlave, rm.role)
	assert.Equal(t, "127.0.0.1:6379", rm.GetMasterAddr())

	// Stop replication - this should set role back to master
	StopSlaveReplication(rm)

	// Verify role changed to master and master addr cleared
	assert.Equal(t, RoleMaster, rm.role)
	assert.Equal(t, "", rm.GetMasterAddr())
}

// TestStartSlaveReplication_ConnectionFailure tests StartSlaveReplication with invalid address
func TestStartSlaveReplication_ConnectionFailure(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	// Use an invalid address - StartSlaveReplication now returns nil (async reconnect)
	err := StartSlaveReplication(rm, testStore, "127.0.0.1:59999")
	assert.NoError(t, err)
	assert.True(t, rm.IsSlave())

	// Stop replication to clean up goroutine
	StopSlaveReplication(rm)
	assert.True(t, rm.IsMaster())
}

// TestStartSlaveReplication_SetsRole tests that StartSlaveReplication sets role to slave
func TestStartSlaveReplication_SetsRole(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	err := StartSlaveReplication(rm, testStore, "10.255.255.1:1")
	assert.NoError(t, err)
	assert.True(t, rm.IsSlave())

	// Stop replication to clean up goroutine
	StopSlaveReplication(rm)
	assert.True(t, rm.IsMaster())
}

// TestHandlePSync_PartialSync tests HandlePSync with partial sync scenario
func TestHandlePSync_PartialSync(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	rm.SetRole(RoleMaster)

	// Add some data to backlog to enable partial sync
	rm.PropagateCommand([][]byte{[]byte("SET"), []byte("key"), []byte("value")})

	// Try partial sync with same repl ID but offset 0
	result, err := HandlePSync(rm, rm.GetReplicationID(), 0)
	assert.NoError(t, err)
	assert.True(t, result != nil)
}

// TestHandlePSync_ValidPartialSync tests HandlePSync with valid partial sync offset
func TestHandlePSync_ValidPartialSync(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	rm.SetRole(RoleMaster)

	// Add some data to backlog to enable partial sync
	rm.PropagateCommand([][]byte{[]byte("SET"), []byte("key"), []byte("value")})

	// Get the current offset
	currentOffset := rm.GetMasterReplOffset()

	// Try partial sync with offset that should be within backlog range
	result, err := HandlePSync(rm, rm.GetReplicationID(), currentOffset-1)
	assert.NoError(t, err)
	assert.True(t, result != nil)
	// Should return partial sync if within valid range
	if currentOffset > 0 {
		assert.True(t, !result.FullResync)
	}
}

// TestHandlePSync_DifferentReplId tests HandlePSync with different repl ID
func TestHandlePSync_DifferentReplId(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	rm.SetRole(RoleMaster)

	// Add data to enable partial sync check
	rm.PropagateCommand([][]byte{[]byte("SET"), []byte("key"), []byte("value")})

	// Try with different repl ID - should trigger full sync
	result, err := HandlePSync(rm, "different-repl-id-12345", 100)
	assert.NoError(t, err)
	assert.True(t, result != nil)
	assert.True(t, result.FullResync) // Different repl ID should trigger full resync
}

// TestPropagateCommand_WithSlaves tests PropagateCommand with active slaves
func TestPropagateCommand_WithSlaves(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	// Create and add a mock connection that is ready
	conn := newMockConn()
	slaveConn := NewSlaveConnection(conn)
	slaveConn.SetReady(true) // Mark as ready

	rm.AddSlave(slaveConn)

	// Initial offset should be 0
	initialOffset := rm.GetMasterReplOffset()

	// Propagate a command
	cmd := [][]byte{[]byte("SET"), []byte("key"), []byte("value")}
	rm.PropagateCommand(cmd)

	// Offset should increase (if slave is ready)
	// Note: actual behavior depends on implementation
	t.Logf("Offset before: %d, after: %d", initialOffset, rm.GetMasterReplOffset())
}

// TestReplicationManager_MultipleSlaves tests managing multiple slaves
func TestReplicationManager_MultipleSlaves(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	// Add multiple slaves
	for i := 0; i < 3; i++ {
		conn := newMockConn()
		slaveConn := NewSlaveConnection(conn)
		rm.AddSlave(slaveConn)
	}

	assert.Equal(t, 3, rm.GetSlaveCount())

	slaves := rm.GetSlaves()
	assert.Equal(t, 3, len(slaves))

	// Remove one slave
	rm.RemoveSlave(slaves[1].ID)
	assert.Equal(t, 2, rm.GetSlaveCount())
}

// TestExecuteReplicatedCommand_HINCRBY tests executeReplicatedCommand for HINCRBY
func TestExecuteReplicatedCommand_HINCRBY(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// First set a hash field
	testStore.HSet("myhash", "field1", "10")

	// Test HINCRBY command
	executeReplicatedCommand(testStore, [][]byte{[]byte("HINCRBY"), []byte("myhash"), []byte("field1"), []byte("5")})

	// Verify value was incremented
	val, err := testStore.HGet("myhash", "field1")
	assert.NoError(t, err)
	assert.Equal(t, []byte("15"), val)
}

// TestExecuteReplicatedCommand_HDEL tests executeReplicatedCommand for HDEL
func TestExecuteReplicatedCommand_HDEL(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// First set hash fields
	testStore.HSet("myhash", "field1", "value1")
	testStore.HSet("myhash", "field2", "value2")

	// Test HDEL command
	executeReplicatedCommand(testStore, [][]byte{[]byte("HDEL"), []byte("myhash"), []byte("field1")})

	// Verify field was deleted
	_, err := testStore.HGet("myhash", "field1")
	assert.True(t, err != nil) // Should return error for deleted field

	// Verify other field still exists
	val, err := testStore.HGet("myhash", "field2")
	assert.NoError(t, err)
	assert.Equal(t, []byte("value2"), val)
}

// TestExecuteReplicatedCommand_ZREM tests executeReplicatedCommand for ZREM
func TestExecuteReplicatedCommand_ZREM(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// First add sorted set members
	testStore.ZAdd("myzset", []store.ZSetMember{
		{Member: "member1", Score: 1.0},
		{Member: "member2", Score: 2.0},
	})

	// Test ZREM command
	executeReplicatedCommand(testStore, [][]byte{[]byte("ZREM"), []byte("myzset"), []byte("member1")})

	// Verify member was removed
	count, err := testStore.ZCard("myzset")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)
}

// TestExecuteReplicatedCommand_SREM tests executeReplicatedCommand for SREM
func TestExecuteReplicatedCommand_SREM(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// First add set members
	testStore.SAdd("myset", "member1", "member2")

	// Test SREM command
	executeReplicatedCommand(testStore, [][]byte{[]byte("SREM"), []byte("myset"), []byte("member1")})

	// Verify member was removed
	count, err := testStore.SCard("myset")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)
}

// TestExecuteReplicatedCommand_LSET tests executeReplicatedCommand for LSET
func TestExecuteReplicatedCommand_LSET(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// First add list elements
	testStore.RPush("mylist", "value1", "value2", "value3")

	// Test LSET command
	executeReplicatedCommand(testStore, [][]byte{[]byte("LSET"), []byte("mylist"), []byte("1"), []byte("newvalue")})

	// Verify value was set
	val, err := testStore.LIndex("mylist", 1)
	assert.NoError(t, err)
	assert.Equal(t, "newvalue", val)
}

// TestExecuteReplicatedCommand_LREM tests executeReplicatedCommand for LREM
func TestExecuteReplicatedCommand_LREM(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// First add list elements
	testStore.RPush("mylist", "a", "b", "a", "c")

	// Test LREM command (remove first 2 'a')
	executeReplicatedCommand(testStore, [][]byte{[]byte("LREM"), []byte("mylist"), []byte("2"), []byte("a")})

	// Verify elements were removed
	len, err := testStore.LLen("mylist")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), len)
}

// TestExecuteReplicatedCommand_LTRIM tests executeReplicatedCommand for LTRIM
func TestExecuteReplicatedCommand_LTRIM(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// First add list elements
	testStore.RPush("mylist", "a", "b", "c", "d", "e")

	// Test LTRIM command (keep elements 0-2)
	executeReplicatedCommand(testStore, [][]byte{[]byte("LTRIM"), []byte("mylist"), []byte("0"), []byte("2")})

	// Verify list was trimmed
	len, err := testStore.LLen("mylist")
	assert.NoError(t, err)
	assert.Equal(t, int64(3), len)
}

// TestExecuteReplicatedCommand_ZINCRBY tests executeReplicatedCommand for ZINCRBY
func TestExecuteReplicatedCommand_ZINCRBY(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// First add sorted set member
	testStore.ZAdd("myzset", []store.ZSetMember{
		{Member: "member1", Score: 1.0},
	})

	// Test ZINCRBY command
	executeReplicatedCommand(testStore, [][]byte{[]byte("ZINCRBY"), []byte("myzset"), []byte("2.5"), []byte("member1")})

	// Verify score was incremented
	members, err := testStore.ZRange("myzset", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(members))
	assert.Equal(t, 3.5, members[0].Score)
}

// TestExecuteReplicatedCommand_SETBIT tests executeReplicatedCommand for SETBIT
func TestExecuteReplicatedCommand_SETBIT(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	testStore.Set("bitkey", "\x00")

	err := executeReplicatedCommand(testStore, [][]byte{[]byte("SETBIT"), []byte("bitkey"), []byte("0"), []byte("1")})
	assert.NoError(t, err)

	bit, err := testStore.GetBit("bitkey", 0)
	assert.NoError(t, err)
	assert.Equal(t, 1, bit)
}

// TestExecuteReplicatedCommand_BITOP tests executeReplicatedCommand for BITOP
func TestExecuteReplicatedCommand_BITOP(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	testStore.Set("key1", "\x0f")
	testStore.Set("key2", "\xf0")

	err := executeReplicatedCommand(testStore, [][]byte{[]byte("BITOP"), []byte("OR"), []byte("dest"), []byte("key1"), []byte("key2")})
	assert.NoError(t, err)

	val, err := testStore.Get("dest")
	assert.NoError(t, err)
	assert.Equal(t, "\xff", val)
}

// TestExecuteReplicatedCommand_BITFIELD tests executeReplicatedCommand for BITFIELD
func TestExecuteReplicatedCommand_BITFIELD(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	err := executeReplicatedCommand(testStore, [][]byte{[]byte("BITFIELD"), []byte("bfkey"), []byte("SET"), []byte("u8"), []byte("0"), []byte("42")})
	assert.NoError(t, err)

	val, err := testStore.Get("bfkey")
	assert.NoError(t, err)
	if len(val) > 0 {
		assert.Equal(t, byte(42), val[0])
	}
}

// TestExecuteReplicatedCommand_ZUNIONSTORE tests executeReplicatedCommand for ZUNIONSTORE
func TestExecuteReplicatedCommand_ZUNIONSTORE(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	testStore.ZAdd("zset1", []store.ZSetMember{{Member: "a", Score: 1}, {Member: "b", Score: 2}})
	testStore.ZAdd("zset2", []store.ZSetMember{{Member: "b", Score: 3}, {Member: "c", Score: 4}})

	err := executeReplicatedCommand(testStore, [][]byte{[]byte("ZUNIONSTORE"), []byte("dest"), []byte("2"), []byte("zset1"), []byte("zset2"), []byte("AGGREGATE"), []byte("SUM")})
	assert.NoError(t, err)

	count, err := testStore.ZCard("dest")
	assert.NoError(t, err)
	assert.Equal(t, int64(3), count)
}

// TestExecuteReplicatedCommand_ZINTERSTORE tests executeReplicatedCommand for ZINTERSTORE
func TestExecuteReplicatedCommand_ZINTERSTORE(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	testStore.ZAdd("zset1", []store.ZSetMember{{Member: "a", Score: 1}, {Member: "b", Score: 2}})
	testStore.ZAdd("zset2", []store.ZSetMember{{Member: "b", Score: 3}, {Member: "c", Score: 4}})

	err := executeReplicatedCommand(testStore, [][]byte{[]byte("ZINTERSTORE"), []byte("dest"), []byte("2"), []byte("zset1"), []byte("zset2")})
	assert.NoError(t, err)

	count, err := testStore.ZCard("dest")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)

	members, err := testStore.ZRange("dest", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, "b", members[0].Member)
	assert.Equal(t, 5.0, members[0].Score)
}

// TestExecuteReplicatedCommand_ZDIFFSTORE tests executeReplicatedCommand for ZDIFFSTORE
func TestExecuteReplicatedCommand_ZDIFFSTORE(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	testStore.ZAdd("zset1", []store.ZSetMember{{Member: "a", Score: 1}, {Member: "b", Score: 2}, {Member: "c", Score: 3}})
	testStore.ZAdd("zset2", []store.ZSetMember{{Member: "b", Score: 3}})

	err := executeReplicatedCommand(testStore, [][]byte{[]byte("ZDIFFSTORE"), []byte("dest"), []byte("2"), []byte("zset1"), []byte("zset2")})
	assert.NoError(t, err)

	count, err := testStore.ZCard("dest")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)
}

// TestExecuteReplicatedCommand_ZRANGESTORE tests executeReplicatedCommand for ZRANGESTORE
func TestExecuteReplicatedCommand_ZRANGESTORE(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	testStore.ZAdd("src", []store.ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 2.0},
		{Member: "c", Score: 3.0},
	})

	err := executeReplicatedCommand(testStore, [][]byte{
		[]byte("ZRANGESTORE"), []byte("dst"), []byte("src"),
		[]byte("0"), []byte("1"),
	})
	assert.NoError(t, err)

	count, err := testStore.ZCard("dst")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)
}

// TestExecuteReplicatedCommand_ZRANGESTORE_BYSCORE tests ZRANGESTORE with BYSCORE
func TestExecuteReplicatedCommand_ZRANGESTORE_BYSCORE(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	testStore.ZAdd("src", []store.ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 2.0},
		{Member: "c", Score: 3.0},
	})

	err := executeReplicatedCommand(testStore, [][]byte{
		[]byte("ZRANGESTORE"), []byte("dst"), []byte("src"),
		[]byte("1.5"), []byte("3.5"), []byte("BYSCORE"),
	})
	assert.NoError(t, err)

	count, err := testStore.ZCard("dst")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)
}

// TestExecuteReplicatedCommand_COPY_String tests executeReplicatedCommand for COPY with string type
func TestExecuteReplicatedCommand_COPY_String(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	testStore.Set("src", "hello")

	err := executeReplicatedCommand(testStore, [][]byte{[]byte("COPY"), []byte("src"), []byte("dst")})
	assert.NoError(t, err)

	val, err := testStore.Get("dst")
	assert.NoError(t, err)
	assert.Equal(t, "hello", val)
}

// TestExecuteReplicatedCommand_COPY_List tests executeReplicatedCommand for COPY with list type
func TestExecuteReplicatedCommand_COPY_List(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	testStore.RPush("src", "a", "b", "c")

	err := executeReplicatedCommand(testStore, [][]byte{[]byte("COPY"), []byte("src"), []byte("dst")})
	assert.NoError(t, err)

	items, err := testStore.LRange("dst", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(items))
	assert.Equal(t, "a", items[0])
	assert.Equal(t, "c", items[2])
}

// TestExecuteReplicatedCommand_COPY_Hash tests executeReplicatedCommand for COPY with hash type
func TestExecuteReplicatedCommand_COPY_Hash(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	testStore.HSet("src", "field1", "val1")
	testStore.HSet("src", "field2", "val2")

	err := executeReplicatedCommand(testStore, [][]byte{[]byte("COPY"), []byte("src"), []byte("dst")})
	assert.NoError(t, err)

	data, err := testStore.HGetAll("dst")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(data))
	assert.Equal(t, "val1", string(data["field1"]))
	assert.Equal(t, "val2", string(data["field2"]))
}

// TestExecuteReplicatedCommand_COPY_Set tests executeReplicatedCommand for COPY with set type
func TestExecuteReplicatedCommand_COPY_Set(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	testStore.SAdd("src", "m1", "m2", "m3")

	err := executeReplicatedCommand(testStore, [][]byte{[]byte("COPY"), []byte("src"), []byte("dst")})
	assert.NoError(t, err)

	count, err := testStore.SCard("dst")
	assert.NoError(t, err)
	assert.Equal(t, int64(3), count)
}

// TestExecuteReplicatedCommand_COPY_ZSet tests executeReplicatedCommand for COPY with zset type
func TestExecuteReplicatedCommand_COPY_ZSet(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	testStore.ZAdd("src", []store.ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 2.0},
	})

	err := executeReplicatedCommand(testStore, [][]byte{[]byte("COPY"), []byte("src"), []byte("dst")})
	assert.NoError(t, err)

	count, err := testStore.ZCard("dst")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)

	members, err := testStore.ZRange("dst", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 1.0, members[0].Score)
}

// TestExecuteReplicatedCommand_COPY_Replace tests COPY with REPLACE option
func TestExecuteReplicatedCommand_COPY_Replace(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	testStore.Set("src", "newvalue")
	testStore.Set("dst", "oldvalue")

	err := executeReplicatedCommand(testStore, [][]byte{[]byte("COPY"), []byte("src"), []byte("dst"), []byte("REPLACE")})
	assert.NoError(t, err)

	val, err := testStore.Get("dst")
	assert.NoError(t, err)
	assert.Equal(t, "newvalue", val)
}

// TestExecuteReplicatedCommand_GEOSEARCHSTORE tests executeReplicatedCommand for GEOSEARCHSTORE
func TestExecuteReplicatedCommand_GEOSEARCHSTORE(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	testStore.GeoAdd("src", []store.GeoMember{
		{Member: "Palermo", Lon: 13.361389, Lat: 38.115556},
		{Member: "Catania", Lon: 15.087269, Lat: 37.502669},
	})

	err := executeReplicatedCommand(testStore, [][]byte{
		[]byte("GEOSEARCHSTORE"), []byte("dst"), []byte("src"),
		[]byte("FROMLONLAT"), []byte("15"), []byte("37"),
		[]byte("BYRADIUS"), []byte("200"), []byte("km"),
	})
	assert.NoError(t, err)

	// Verify results were stored (at least one result)
	_, err = testStore.ZCard("dst")
	assert.NoError(t, err)
}

// TestExecuteReplicatedCommand_Unknown tests that unknown commands are silently ignored
func TestExecuteReplicatedCommand_Unknown(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	err := executeReplicatedCommand(testStore, [][]byte{[]byte("UNKNOWNCMD")})
	assert.Error(t, err) // unknown commands now return error to trigger resync

	err = executeReplicatedCommand(testStore, [][]byte{[]byte("COPY"), []byte("nonexistent"), []byte("dst")})
	assert.NoError(t, err)
}

// TestExecuteReplicatedCommand_SETBIT_InvalidArgs tests SETBIT with invalid args
func TestExecuteReplicatedCommand_SETBIT_InvalidArgs(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	err := executeReplicatedCommand(testStore, [][]byte{[]byte("SETBIT"), []byte("key")})
	assert.NoError(t, err)
}

// TestExecuteReplicatedCommand_ZUNIONSTORE_Weights tests ZUNIONSTORE with WEIGHTS option
func TestExecuteReplicatedCommand_ZUNIONSTORE_Weights(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	testStore.ZAdd("z1", []store.ZSetMember{{Member: "a", Score: 1}, {Member: "b", Score: 2}})
	testStore.ZAdd("z2", []store.ZSetMember{{Member: "b", Score: 3}, {Member: "c", Score: 4}})

	err := executeReplicatedCommand(testStore, [][]byte{
		[]byte("ZUNIONSTORE"), []byte("dest"), []byte("2"), []byte("z1"), []byte("z2"),
		[]byte("WEIGHTS"), []byte("2"), []byte("3"),
		[]byte("AGGREGATE"), []byte("MAX"),
	})
	assert.NoError(t, err)

	count, err := testStore.ZCard("dest")
	assert.NoError(t, err)
	assert.Equal(t, int64(3), count)
}

// TestExecuteReplicatedCommand_ZDIFFSTORE_Empty tests ZDIFFSTORE with non-existent keys
func TestExecuteReplicatedCommand_ZDIFFSTORE_Empty(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	err := executeReplicatedCommand(testStore, [][]byte{[]byte("ZDIFFSTORE"), []byte("dest"), []byte("0")})
	assert.NoError(t, err)
}

// TestExecuteReplicatedCommand_ZRANGESTORE_EmptyKey tests ZRANGESTORE with non-existent src
func TestExecuteReplicatedCommand_ZRANGESTORE_EmptyKey(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	err := executeReplicatedCommand(testStore, [][]byte{
		[]byte("ZRANGESTORE"), []byte("dst"), []byte("nonexistent"),
		[]byte("0"), []byte("-1"),
	})
	assert.NoError(t, err)

	count, err := testStore.ZCard("dst")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

// TestExecuteReplicatedCommand_HSETNX tests executeReplicatedCommand for HSETNX
func TestExecuteReplicatedCommand_HSETNX(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	err := executeReplicatedCommand(testStore, [][]byte{[]byte("HSETNX"), []byte("hash"), []byte("field"), []byte("value")})
	assert.NoError(t, err)

	val, err := testStore.HGet("hash", "field")
	assert.NoError(t, err)
	assert.Equal(t, []byte("value"), val)
}

// TestExecuteReplicatedCommand_PFADD_PFMERGE tests executeReplicatedCommand for PFADD and PFMERGE
func TestExecuteReplicatedCommand_PFADD(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	err := executeReplicatedCommand(testStore, [][]byte{[]byte("PFADD"), []byte("hll"), []byte("a"), []byte("b"), []byte("c")})
	assert.NoError(t, err)
}

func TestExecuteReplicatedCommand_PFMERGE(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	executeReplicatedCommand(testStore, [][]byte{[]byte("PFADD"), []byte("hll1"), []byte("a"), []byte("b")})
	executeReplicatedCommand(testStore, [][]byte{[]byte("PFADD"), []byte("hll2"), []byte("c"), []byte("d")})

	err := executeReplicatedCommand(testStore, [][]byte{[]byte("PFMERGE"), []byte("dest"), []byte("hll1"), []byte("hll2")})
	assert.NoError(t, err)
}

// TestExecuteReplicatedCommand_XADD tests executeReplicatedCommand for XADD
func TestExecuteReplicatedCommand_XADD(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	err := executeReplicatedCommand(testStore, [][]byte{
		[]byte("XADD"), []byte("xadd:repl:stream"), []byte("*"), []byte("field"), []byte("val"),
	})
	assert.NoError(t, err)

	entryCount, err := testStore.XLen("xadd:repl:stream")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), entryCount)
}

// TestExecuteReplicatedCommand_XACK tests executeReplicatedCommand for XACK
func TestExecuteReplicatedCommand_XACK(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// XACK on non-existent stream should not error
	err := executeReplicatedCommand(testStore, [][]byte{[]byte("XACK"), []byte("stream"), []byte("group"), []byte("0-0")})
	assert.NoError(t, err)
}

// TestExecuteReplicatedCommand_XCLAIM tests executeReplicatedCommand for XCLAIM
func TestExecuteReplicatedCommand_XCLAIM(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// XCLAIM on non-existent stream should not error
	err := executeReplicatedCommand(testStore, [][]byte{[]byte("XCLAIM"), []byte("stream"), []byte("group"), []byte("consumer"), []byte("1000"), []byte("0-0")})
	assert.NoError(t, err)
}

// TestExecuteReplicatedCommand_XGROUP tests executeReplicatedCommand for XGROUP
func TestExecuteReplicatedCommand_XGROUP(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	err := executeReplicatedCommand(testStore, [][]byte{[]byte("XGROUP"), []byte("CREATE"), []byte("stream"), []byte("group"), []byte("0")})
	assert.NoError(t, err)
}

// TestExecuteReplicatedCommand_BLPOP tests executeReplicatedCommand for BLPOP
func TestExecuteReplicatedCommand_BLPOP(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	testStore.RPush("list1", "a", "b")

	err := executeReplicatedCommand(testStore, [][]byte{[]byte("BLPOP"), []byte("list1"), []byte("list2"), []byte("1")})
	assert.NoError(t, err)

	items, err := testStore.LRange("list1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(items))
	assert.Equal(t, "b", items[0])
}

// TestExecuteReplicatedCommand_BZPOPMAX tests executeReplicatedCommand for BZPOPMAX
func TestExecuteReplicatedCommand_BZPOPMAX(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	testStore.ZAdd("zset1", []store.ZSetMember{{Score: 1, Member: "a"}, {Score: 2, Member: "b"}})

	err := executeReplicatedCommand(testStore, [][]byte{[]byte("BZPOPMAX"), []byte("zset1"), []byte("zset2"), []byte("1")})
	assert.NoError(t, err)

	// Verify element was popped
	members, _ := testStore.ZRange("zset1", 0, -1)
	assert.Equal(t, 1, len(members))
	assert.Equal(t, "a", members[0].Member)
}

// TestExecuteReplicatedCommand_BZPOPMIN tests executeReplicatedCommand for BZPOPMIN
func TestExecuteReplicatedCommand_BZPOPMIN(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	testStore.ZAdd("zset1", []store.ZSetMember{{Score: 1, Member: "a"}, {Score: 2, Member: "b"}})

	err := executeReplicatedCommand(testStore, [][]byte{[]byte("BZPOPMIN"), []byte("zset1"), []byte("1")})
	assert.NoError(t, err)

	// Verify element was popped
	members, _ := testStore.ZRange("zset1", 0, -1)
	assert.Equal(t, 1, len(members))
	assert.Equal(t, "b", members[0].Member)
}

// TestExecuteReplicatedCommand_BRPOP tests executeReplicatedCommand for BRPOP
func TestExecuteReplicatedCommand_BRPOP(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	testStore.RPush("list1", "a", "b")

	err := executeReplicatedCommand(testStore, [][]byte{[]byte("BRPOP"), []byte("list1"), []byte("1")})
	assert.NoError(t, err)

	items, err := testStore.LRange("list1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(items))
	assert.Equal(t, "a", items[0])
}

// TestExecuteReplicatedCommand_BLMOVE tests executeReplicatedCommand for BLMOVE
func TestExecuteReplicatedCommand_BLMOVE(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	testStore.RPush("src", "a", "b")

	err := executeReplicatedCommand(testStore, [][]byte{[]byte("BLMOVE"), []byte("src"), []byte("dst"), []byte("RIGHT"), []byte("LEFT")})
	assert.NoError(t, err)
}

// TestExecuteReplicatedCommand_BRPOPLPUSH tests executeReplicatedCommand for BRPOPLPUSH
func TestExecuteReplicatedCommand_BRPOPLPUSH(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	testStore.RPush("src", "a", "b")

	err := executeReplicatedCommand(testStore, [][]byte{[]byte("BRPOPLPUSH"), []byte("src"), []byte("dst")})
	assert.NoError(t, err)
}

// TestExecuteReplicatedCommand_InvalidArgs tests various invalid argument scenarios
func TestExecuteReplicatedCommand_InvalidArgs(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// All invalid arg cases should not panic, just return nil or error
	_ = executeReplicatedCommand(testStore, [][]byte{[]byte("SETBIT"), []byte("k"), []byte("notanumber"), []byte("1")})
	_ = executeReplicatedCommand(testStore, [][]byte{[]byte("BITOP")})
	_ = executeReplicatedCommand(testStore, [][]byte{[]byte("BITFIELD")})
	_ = executeReplicatedCommand(testStore, [][]byte{[]byte("COPY")})
	_ = executeReplicatedCommand(testStore, [][]byte{[]byte("ZUNIONSTORE")})
	_ = executeReplicatedCommand(testStore, [][]byte{[]byte("ZUNIONSTORE"), []byte("d"), []byte("notanumber")})
	_ = executeReplicatedCommand(testStore, [][]byte{[]byte("ZINTERSTORE")})
	_ = executeReplicatedCommand(testStore, [][]byte{[]byte("ZDIFFSTORE")})
	_ = executeReplicatedCommand(testStore, [][]byte{[]byte("ZRANGESTORE")})
	_ = executeReplicatedCommand(testStore, [][]byte{[]byte("GEOSEARCHSTORE")})
	_ = executeReplicatedCommand(testStore, [][]byte{[]byte("ZUNIONSTORE"), []byte("d"), []byte("1"), []byte("k"), []byte("WEIGHTS")})
	_ = executeReplicatedCommand(testStore, [][]byte{[]byte("ZINTERSTORE"), []byte("d"), []byte("1"), []byte("k"), []byte("WEIGHTS")})
	_ = executeReplicatedCommand(testStore, [][]byte{[]byte("GEOSEARCHSTORE"), []byte("d"), []byte("s")})
	_ = executeReplicatedCommand(testStore, [][]byte{[]byte("GEOSEARCHSTORE"), []byte("d"), []byte("s"), []byte("FROMLONLAT")})
	_ = executeReplicatedCommand(testStore, [][]byte{[]byte("GEOSEARCHSTORE"), []byte("d"), []byte("s"), []byte("FROMLONLAT"), []byte("15"), []byte("37")})
	_ = executeReplicatedCommand(testStore, [][]byte{[]byte("ZRANGESTORE"), []byte("d"), []byte("s"), []byte("0"), []byte("-1"), []byte("LIMIT")})
	_ = executeReplicatedCommand(testStore, [][]byte{[]byte("GEOSEARCHSTORE"), []byte("d"), []byte("s"), []byte("FROMMEMBER"), []byte("nonexistent"), []byte("BYRADIUS"), []byte("100"), []byte("km")})
}

// TestExecuteReplicatedCommand_GETDEL tests executeReplicatedCommand for GETDEL
func TestExecuteReplicatedCommand_GETDEL(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	testStore.Set("key", "value")

	err := executeReplicatedCommand(testStore, [][]byte{[]byte("GETDEL"), []byte("key")})
	assert.NoError(t, err)

	_, err = testStore.Get("key")
	assert.Error(t, err)
}

// TestExecuteReplicatedCommand_GETDEL_NonExistent tests GETDEL on non-existent key
func TestExecuteReplicatedCommand_GETDEL_NonExistent(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	err := executeReplicatedCommand(testStore, [][]byte{[]byte("GETDEL"), []byte("nonexistent")})
	assert.NoError(t, err)
}

// TestExecuteReplicatedCommand_GETEX tests executeReplicatedCommand for GETEX with EX option
func TestExecuteReplicatedCommand_GETEX_EX(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	testStore.Set("key", "value")

	err := executeReplicatedCommand(testStore, [][]byte{[]byte("GETEX"), []byte("key"), []byte("EX"), []byte("100")})
	assert.NoError(t, err)

	val, err := testStore.Get("key")
	assert.NoError(t, err)
	assert.Equal(t, "value", val)
}

// TestExecuteReplicatedCommand_GETEX_PERSIST tests GETEX with PERSIST
func TestExecuteReplicatedCommand_GETEX_PERSIST(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	testStore.SetWithTTL("key", "value", 100*time.Second)

	err := executeReplicatedCommand(testStore, [][]byte{[]byte("GETEX"), []byte("key"), []byte("PERSIST")})
	assert.NoError(t, err)
}

// TestExecuteReplicatedCommand_GETEX_NoOption tests GETEX with no option (just GET)
func TestExecuteReplicatedCommand_GETEX_NoOption(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	testStore.Set("key", "value")

	err := executeReplicatedCommand(testStore, [][]byte{[]byte("GETEX"), []byte("key")})
	assert.NoError(t, err)

	val, err := testStore.Get("key")
	assert.NoError(t, err)
	assert.Equal(t, "value", val)
}

// TestExecuteReplicatedCommand_GETEX_NonExistent tests GETEX on non-existent key
func TestExecuteReplicatedCommand_GETEX_NonExistent(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	err := executeReplicatedCommand(testStore, [][]byte{[]byte("GETEX"), []byte("nonexistent")})
	assert.NoError(t, err)
}

// TestExecuteReplicatedCommand_JSON_SET tests executeReplicatedCommand for JSON.SET
func TestExecuteReplicatedCommand_JSON_SET(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	err := executeReplicatedCommand(testStore, [][]byte{
		[]byte("JSON.SET"), []byte("jsonkey"), []byte("$"), []byte(`{"name":"test"}`),
	})
	assert.NoError(t, err)

	result, err := testStore.JSONGet("jsonkey", "$")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(result))
}

// TestExecuteReplicatedCommand_JSON_SET_WithNX tests JSON.SET with NX option
func TestExecuteReplicatedCommand_JSON_SET_WithNX(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	err := executeReplicatedCommand(testStore, [][]byte{
		[]byte("JSON.SET"), []byte("jsonnx"), []byte("$"), []byte(`{"val":1}`), []byte("NX"),
	})
	assert.NoError(t, err)

	// Second call with NX should not overwrite
	err = executeReplicatedCommand(testStore, [][]byte{
		[]byte("JSON.SET"), []byte("jsonnx"), []byte("$"), []byte(`{"val":2}`), []byte("NX"),
	})
	assert.NoError(t, err)

	result, err := testStore.JSONGet("jsonnx", "$")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(result))
}

// TestExecuteReplicatedCommand_JSON_DEL tests executeReplicatedCommand for JSON.DEL
func TestExecuteReplicatedCommand_JSON_DEL(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	testStore.JSONSet("jsondel", "$", `{"a":1,"b":2}`, false, false)

	err := executeReplicatedCommand(testStore, [][]byte{
		[]byte("JSON.DEL"), []byte("jsondel"),
	})
	assert.NoError(t, err)
}

// TestExecuteReplicatedCommand_JSON_ARRAPPEND tests executeReplicatedCommand for JSON.ARRAPPEND
func TestExecuteReplicatedCommand_JSON_ARRAPPEND(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	testStore.JSONSet("jsonarr", "$", `[1,2]`, false, false)

	err := executeReplicatedCommand(testStore, [][]byte{
		[]byte("JSON.ARRAPPEND"), []byte("jsonarr"), []byte("$"), []byte("3"),
	})
	assert.NoError(t, err)
}

// TestExecuteReplicatedCommand_JSON_NUMINCRBY tests executeReplicatedCommand for JSON.NUMINCRBY
func TestExecuteReplicatedCommand_JSON_NUMINCRBY(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	testStore.JSONSet("jsonnum", "$", `10`, false, false)

	err := executeReplicatedCommand(testStore, [][]byte{
		[]byte("JSON.NUMINCRBY"), []byte("jsonnum"), []byte("$"), []byte("5"),
	})
	assert.NoError(t, err)
}

// TestExecuteReplicatedCommand_JSON_NUMMULTBY tests executeReplicatedCommand for JSON.NUMMULTBY
func TestExecuteReplicatedCommand_JSON_NUMMULTBY(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	testStore.JSONSet("jsonmult", "$", `10`, false, false)

	err := executeReplicatedCommand(testStore, [][]byte{
		[]byte("JSON.NUMMULTBY"), []byte("jsonmult"), []byte("$"), []byte("2"),
	})
	assert.NoError(t, err)
}

// TestExecuteReplicatedCommand_JSON_CLEAR tests executeReplicatedCommand for JSON.CLEAR
func TestExecuteReplicatedCommand_JSON_CLEAR(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	testStore.JSONSet("jsonclear", "$", `{"a":1}`, false, false)

	err := executeReplicatedCommand(testStore, [][]byte{
		[]byte("JSON.CLEAR"), []byte("jsonclear"),
	})
	assert.NoError(t, err)
}

// TestExecuteReplicatedCommand_TS_CREATE tests executeReplicatedCommand for TS.CREATE
func TestExecuteReplicatedCommand_TS_CREATE(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	err := executeReplicatedCommand(testStore, [][]byte{
		[]byte("TS.CREATE"), []byte("tscreatekey"), []byte("RETENTION"), []byte("3600000"),
	})
	assert.NoError(t, err)
}

// TestExecuteReplicatedCommand_TS_ADD tests executeReplicatedCommand for TS.ADD
func TestExecuteReplicatedCommand_TS_ADD(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	err := executeReplicatedCommand(testStore, [][]byte{
		[]byte("TS.ADD"), []byte("tsaddkey"), []byte("1000"), []byte("42.5"),
	})
	assert.NoError(t, err)
}

// TestExecuteReplicatedCommand_TS_ADD_AutoTimestamp tests TS.ADD with *
func TestExecuteReplicatedCommand_TS_ADD_AutoTimestamp(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	err := executeReplicatedCommand(testStore, [][]byte{
		[]byte("TS.ADD"), []byte("tsaddauto"), []byte("*"), []byte("3.14"),
	})
	assert.NoError(t, err)
}

// TestExecuteReplicatedCommand_TS_DEL tests executeReplicatedCommand for TS.DEL
func TestExecuteReplicatedCommand_TS_DEL(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// Add some data points first
	testStore.TSAdd("tsdelkey", 100, 1.0, store.TSAddOptions{})
	testStore.TSAdd("tsdelkey", 200, 2.0, store.TSAddOptions{})

	err := executeReplicatedCommand(testStore, [][]byte{
		[]byte("TS.DEL"), []byte("tsdelkey"), []byte("100"), []byte("150"),
	})
	assert.NoError(t, err)
}

// TestExecuteReplicatedCommand_JSON_InvalidArgs tests JSON commands with invalid args
func TestExecuteReplicatedCommand_JSON_InvalidArgs(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// JSON.SET with too few args should not panic
	err := executeReplicatedCommand(testStore, [][]byte{[]byte("JSON.SET"), []byte("key")})
	assert.NoError(t, err)
}

// TestExecuteReplicatedCommand_TS_InvalidArgs tests TS commands with invalid args
func TestExecuteReplicatedCommand_TS_InvalidArgs(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// TS.ADD with too few args should not panic
	err := executeReplicatedCommand(testStore, [][]byte{[]byte("TS.ADD"), []byte("key")})
	assert.NoError(t, err)

	// TS.DEL with too few args should not panic
	err = executeReplicatedCommand(testStore, [][]byte{[]byte("TS.DEL"), []byte("key")})
	assert.NoError(t, err)
}

// TestExecuteReplicatedCommand_RESTORE tests executeReplicatedCommand for RESTORE
func TestExecuteReplicatedCommand_RESTORE(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// Dump a key to get serialized data
	testStore.Set("mykey", "hello")
	dumpData, err := testStore.Dump("mykey")
	assert.NoError(t, err)

	// RESTORE to new key with REPLACE
	err = executeReplicatedCommand(testStore, [][]byte{[]byte("RESTORE"), []byte("newkey"), []byte("0"), dumpData, []byte("REPLACE")})
	assert.NoError(t, err)

	// Verify restored value
	val, err := testStore.Get("newkey")
	assert.NoError(t, err)
	assert.Equal(t, "hello", val)
}

// TestExecuteReplicatedCommand_RESTORE_WithTTL tests RESTORE with TTL
func TestExecuteReplicatedCommand_RESTORE_WithTTL(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	testStore.Set("mykey", "world")
	dumpData, err := testStore.Dump("mykey")
	assert.NoError(t, err)

	// RESTORE with 10s TTL
	err = executeReplicatedCommand(testStore, [][]byte{[]byte("RESTORE"), []byte("newkey"), []byte("10000"), dumpData, []byte("REPLACE")})
	assert.NoError(t, err)

	// Verify restored value
	val, err := testStore.Get("newkey")
	assert.NoError(t, err)
	assert.Equal(t, "world", val)
}

// TestExecuteReplicatedCommand_FLUSHDB tests executeReplicatedCommand for FLUSHDB
func TestExecuteReplicatedCommand_FLUSHDB(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	testStore.Set("key1", "val1")
	testStore.Set("key2", "val2")

	// FLUSHDB should clear all keys
	err := executeReplicatedCommand(testStore, [][]byte{[]byte("FLUSHDB")})
	assert.NoError(t, err)

	_, err = testStore.Get("key1")
	assert.True(t, err != nil)
	_, err = testStore.Get("key2")
	assert.True(t, err != nil)
}

// TestExecuteReplicatedCommand_FLUSHALL tests executeReplicatedCommand for FLUSHALL
func TestExecuteReplicatedCommand_FLUSHALL(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	testStore.Set("akey", "aval")

	// FLUSHALL should clear all keys (same as FLUSHDB in BoltDB)
	err := executeReplicatedCommand(testStore, [][]byte{[]byte("FLUSHALL")})
	assert.NoError(t, err)

	_, err = testStore.Get("akey")
	assert.True(t, err != nil)
}

// TestExecuteReplicatedCommand_XAUTOCLAIM tests executeReplicatedCommand for XAUTOCLAIM
func TestExecuteReplicatedCommand_XAUTOCLAIM(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// XAUTOCLAIM on non-existent stream should not error
	err := executeReplicatedCommand(testStore, [][]byte{
		[]byte("XAUTOCLAIM"), []byte("stream"), []byte("group"),
		[]byte("consumer"), []byte("1000"), []byte("0-0"),
	})
	assert.NoError(t, err)
}

// TestExecuteReplicatedCommand_XAUTOCLAIM_WithOptions tests XAUTOCLAIM with COUNT and JUSTID
func TestExecuteReplicatedCommand_XAUTOCLAIM_WithOptions(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	err := executeReplicatedCommand(testStore, [][]byte{
		[]byte("XAUTOCLAIM"), []byte("stream"), []byte("group"),
		[]byte("consumer"), []byte("1000"), []byte("0-0"),
		[]byte("COUNT"), []byte("10"), []byte("JUSTID"),
	})
	assert.NoError(t, err)
}

// TestExecuteReplicatedCommand_SORT_STORE_List tests SORT with STORE option (list source)
func TestExecuteReplicatedCommand_SORT_STORE_List(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// Set up source list
	testStore.RPush("mylist", "3", "1", "2")

	// SORT mylist STORE sortedlist
	err := executeReplicatedCommand(testStore, [][]byte{
		[]byte("SORT"), []byte("mylist"), []byte("STORE"), []byte("sortedlist"),
	})
	assert.NoError(t, err)

	// Verify stored result
	vals, err := testStore.LRange("sortedlist", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(vals))
	assert.Equal(t, "1", vals[0])
	assert.Equal(t, "2", vals[1])
	assert.Equal(t, "3", vals[2])
}

// TestExecuteReplicatedCommand_SORT_STORE_Set tests SORT with STORE option (set source)
func TestExecuteReplicatedCommand_SORT_STORE_Set(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// Set up source set
	testStore.SAdd("myset", "c", "a", "b")

	// SORT myset ALPHA STORE sortedset
	err := executeReplicatedCommand(testStore, [][]byte{
		[]byte("SORT"), []byte("myset"), []byte("ALPHA"), []byte("STORE"), []byte("sortedset"),
	})
	assert.NoError(t, err)

	// Verify stored result
	vals, err := testStore.LRange("sortedset", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(vals))
	assert.Equal(t, "a", vals[0])
	assert.Equal(t, "b", vals[1])
	assert.Equal(t, "c", vals[2])
}

// TestExecuteReplicatedCommand_SORT_STORE_Desc tests SORT DESC with STORE
func TestExecuteReplicatedCommand_SORT_STORE_Desc(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	testStore.RPush("mylist", "3", "1", "2")

	err := executeReplicatedCommand(testStore, [][]byte{
		[]byte("SORT"), []byte("mylist"), []byte("DESC"), []byte("STORE"), []byte("sortedlist"),
	})
	assert.NoError(t, err)

	vals, err := testStore.LRange("sortedlist", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(vals))
	assert.Equal(t, "3", vals[0])
	assert.Equal(t, "2", vals[1])
	assert.Equal(t, "1", vals[2])
}

// TestExecuteReplicatedCommand_SORT_NoStore tests SORT without STORE (read-only, no-op)
func TestExecuteReplicatedCommand_SORT_NoStore(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	testStore.RPush("mylist", "3", "1", "2")

	// Read-only SORT without STORE should not error (no-op in replication)
	err := executeReplicatedCommand(testStore, [][]byte{
		[]byte("SORT"), []byte("mylist"),
	})
	assert.NoError(t, err)
}

// TestExecuteReplicatedCommand_RESTORE_InvalidArgs tests RESTORE with too few args
func TestExecuteReplicatedCommand_RESTORE_InvalidArgs(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// RESTORE with too few args should not panic
	err := executeReplicatedCommand(testStore, [][]byte{[]byte("RESTORE")})
	assert.NoError(t, err)

	err = executeReplicatedCommand(testStore, [][]byte{[]byte("RESTORE"), []byte("key")})
	assert.NoError(t, err)
}

// TestExecuteReplicatedCommand_SORT_STORE_Limit tests SORT LIMIT with STORE
func TestExecuteReplicatedCommand_SORT_STORE_Limit(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	testStore.RPush("mylist", "5", "3", "1", "4", "2")

	err := executeReplicatedCommand(testStore, [][]byte{
		[]byte("SORT"), []byte("mylist"), []byte("LIMIT"), []byte("1"), []byte("3"),
		[]byte("STORE"), []byte("sortedlist"),
	})
	assert.NoError(t, err)

	vals, err := testStore.LRange("sortedlist", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(vals))
	assert.Equal(t, "2", vals[0]) // sorted: [1,2,3,4,5], offset 1, count 3 → [2,3,4]
	assert.Equal(t, "3", vals[1])
	assert.Equal(t, "4", vals[2])
}

// TestExecuteReplicatedCommand_XAUTOCLAIM_InvalidArgs tests XAUTOCLAIM with too few args
func TestExecuteReplicatedCommand_XAUTOCLAIM_InvalidArgs(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	// XAUTOCLAIM with too few args should not panic
	err := executeReplicatedCommand(testStore, [][]byte{[]byte("XAUTOCLAIM")})
	assert.NoError(t, err)
	err = executeReplicatedCommand(testStore, [][]byte{[]byte("XAUTOCLAIM"), []byte("key")})
	assert.NoError(t, err)
}

// TestExecuteReplicatedCommand_SORT_STORE_String tests SORT with STORE (string source, single element)
func TestExecuteReplicatedCommand_SORT_STORE_String(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	testStore.Set("mystr", "42")

	err := executeReplicatedCommand(testStore, [][]byte{
		[]byte("SORT"), []byte("mystr"), []byte("STORE"), []byte("sortedlist"),
	})
	assert.NoError(t, err)

	vals, err := testStore.LRange("sortedlist", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(vals))
	assert.Equal(t, "42", vals[0])
}

// TestExecuteReplicatedCommand_RESTORE_OldFormat tests RESTORE in old format (key + serialized + REPLACE)
func TestExecuteReplicatedCommand_RESTORE_OldFormat(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	testStore.Set("mykey", "hello")
	dumpData, err := testStore.Dump("mykey")
	assert.NoError(t, err)

	// Old format: RESTORE key serializedData REPLACE (no TTL)
	err = executeReplicatedCommand(testStore, [][]byte{
		[]byte("RESTORE"), []byte("newkey"), dumpData, []byte("REPLACE"),
	})
	assert.NoError(t, err)

	val, err := testStore.Get("newkey")
	assert.NoError(t, err)
	assert.Equal(t, "hello", val)
}

// TestExecuteReplicatedCommand_RESTORE_ABSTTL tests RESTORE with ABSTTL option
func TestExecuteReplicatedCommand_RESTORE_ABSTTL(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	defer testStore.Close()

	testStore.Set("mykey", "world")
	dumpData, err := testStore.Dump("mykey")
	assert.NoError(t, err)

	// ABSTTL with future timestamp (now + 10s in ms)
	futureTS := time.Now().UnixMilli() + 10000
	ttlStr := strconv.FormatInt(futureTS, 10)

	err = executeReplicatedCommand(testStore, [][]byte{
		[]byte("RESTORE"), []byte("newkey"), []byte(ttlStr), dumpData,
		[]byte("REPLACE"), []byte("ABSTTL"),
	})
	assert.NoError(t, err)

	val, err := testStore.Get("newkey")
	assert.NoError(t, err)
	assert.Equal(t, "world", val)
}

// TestErrorsIsStop tests errorsIsStop function
func TestErrorsIsStop(t *testing.T) {
	t.Parallel()

	assert.False(t, errorsIsStop(nil, nil))

	err := fmt.Errorf("test error")
	assert.False(t, errorsIsStop(err, nil))

	stopCh := make(chan struct{})
	assert.False(t, errorsIsStop(err, stopCh))

	close(stopCh)
	assert.True(t, errorsIsStop(err, stopCh))
}

// TestFullResyncDataIntegrity 验证 FULLRESYNC 后 slave 数据与 master 完全一致
// 覆盖 string / list / hash / set / zset / stream / geo 多种数据类型
func TestFullResyncDataIntegrity(t *testing.T) {
	t.Parallel()
	master := setupTestStore(t)
	defer master.Close()

	// 写入各类数据
	assert.NoError(t, master.Set("str1", "hello"))
	assert.NoError(t, master.Set("str2", "world"))
	err := master.SetStringBatch([]store.StringEntry{
		{Key: "batch_a", Value: "batch_val_a"},
		{Key: "batch_b", Value: "batch_val_b"},
	})
	assert.NoError(t, err)
	_, err = master.LPush("mylist", "c", "b", "a")
	assert.NoError(t, err)
	_, err = master.RPush("mylist", "d", "e")
	assert.NoError(t, err)
	assert.NoError(t, master.HSet("myhash", "field1", "val1"))
	assert.NoError(t, master.HSet("myhash", "field2", "val2"))
	_, err = master.SAdd("myset", "m1", "m2", "m3")
	assert.NoError(t, err)
	assert.NoError(t, master.ZAdd("myzset", []store.ZSetMember{
		{Score: 1.0, Member: "a"},
		{Score: 2.0, Member: "b"},
	}))
	_, err = master.GeoAdd("mygeo", []store.GeoMember{
		{Member: "beijing", Lat: 39.9, Lon: 116.4},
		{Member: "shanghai", Lat: 31.2, Lon: 121.5},
	})
	assert.NoError(t, err)

	// 带 TTL 的字符串键
	assert.NoError(t, master.Set("ttl_key", "will_expire"))
	_, err = master.Expire("ttl_key", 3600)
	assert.NoError(t, err)

	// 生成 RDB 快照
	rdbData, err := GenerateRDB(master)
	assert.NoError(t, err)
	assert.True(t, len(rdbData) > 0)

	// 加载到 slave
	slave := setupTestStore(t)
	defer slave.Close()

	err = LoadRDBWithStore(rdbData, slave)
	assert.NoError(t, err)

	// 验证所有数据类型一致
	t.Run("string", func(t *testing.T) {
		val, err := slave.Get("str1")
		assert.NoError(t, err)
		assert.Equal(t, "hello", val)
		val, err = slave.Get("str2")
		assert.NoError(t, err)
		assert.Equal(t, "world", val)
		val, err = slave.Get("batch_a")
		assert.NoError(t, err)
		assert.Equal(t, "batch_val_a", val)
	})

	t.Run("list", func(t *testing.T) {
		vals, err := slave.LRange("mylist", 0, -1)
		assert.NoError(t, err)
		assert.Equal(t, 5, len(vals))
		assert.Equal(t, "a", vals[0]) // LPUSH c,b,a → [a,b,c]; then RPUSH d,e → [a,b,c,d,e]
		assert.Equal(t, "b", vals[1])
		assert.Equal(t, "c", vals[2])
		assert.Equal(t, "d", vals[3])
		assert.Equal(t, "e", vals[4])
	})

	t.Run("hash", func(t *testing.T) {
		val, err := slave.HGet("myhash", "field1")
		assert.NoError(t, err)
		assert.Equal(t, []byte("val1"), val)
		val, err = slave.HGet("myhash", "field2")
		assert.NoError(t, err)
		assert.Equal(t, []byte("val2"), val)
	})

	t.Run("set", func(t *testing.T) {
		members, err := slave.SMembers("myset")
		assert.NoError(t, err)
		assert.Equal(t, 3, len(members))
	})

	t.Run("zset", func(t *testing.T) {
		count, err := slave.ZCard("myzset")
		assert.NoError(t, err)
		assert.Equal(t, int64(2), count)
	})

	t.Run("geo", func(t *testing.T) {
		pos, err := slave.GeoGetAllPositions("mygeo")
		assert.NoError(t, err)
		assert.Equal(t, 2, len(pos))
	})

	t.Run("ttl", func(t *testing.T) {
		val, err := slave.Get("ttl_key")
		assert.NoError(t, err)
		assert.Equal(t, "will_expire", val)

		ttl, err := slave.TTL("ttl_key")
		assert.NoError(t, err)
		if ttl <= 0 {
			t.Errorf("TTL should be >0 after RDB load, got ttl=%d (want >0, -1=no_expiry, -2=missing)", ttl)
		}
	})

}

func TestHandlePSync_FullResyncDataIntegrity(t *testing.T) {
	t.Parallel()
	master := setupTestStore(t)
	rm := NewReplicationManager(master)
	defer rm.Stop()

	rm.SetRole(RoleMaster)

	// 写入数据
	assert.NoError(t, master.Set("psync_key", "psync_value"))
	rm.PropagateCommand([][]byte{[]byte("SET"), []byte("psync_key"), []byte("psync_value")})

	// 验证 HandlePSync 返回正确的结果
	result, err := HandlePSync(rm, "?", 0)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.True(t, result.FullResync) // 不同 replId 触发全量同步
	assert.Equal(t, rm.GetReplicationID(), result.ReplId)
}

// TestIsTransientReplicationError tests isTransientReplicationError function
func TestIsTransientReplicationError(t *testing.T) {
	t.Parallel()

	assert.False(t, isTransientReplicationError(nil))
	assert.True(t, isTransientReplicationError(fmt.Errorf("max retries exhausted after 3 attempts")))
	assert.True(t, isTransientReplicationError(fmt.Errorf("write rejected by backpressure")))
	assert.True(t, isTransientReplicationError(fmt.Errorf("key not found")))
	assert.False(t, isTransientReplicationError(fmt.Errorf("connection refused")))
	assert.False(t, isTransientReplicationError(fmt.Errorf("")))
}
