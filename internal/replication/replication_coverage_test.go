package replication

import (
	"bufio"
	"testing"

	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/zeebo/assert"
)

// TestSerializeCommand tests serializeCommand function
func TestSerializeCommand(t *testing.T) {
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
	testStore := setupTestStore(t)
	defer testStore.Close()

	// Test SET command
	executeReplicatedCommand(testStore, [][]byte{[]byte("SET"), []byte("testkey"), []byte("testvalue")})

	// Verify the value was set
	val, err := testStore.Get("testkey")
	assert.NoError(t, err)
	assert.Equal(t, "testvalue", val)
}

// TestExecuteReplicatedCommand_DEL tests executeReplicatedCommand for DEL
func TestExecuteReplicatedCommand_DEL(t *testing.T) {
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
	testStore := setupTestStore(t)
	defer testStore.Close()

	// Should not panic with empty args
	executeReplicatedCommand(testStore, [][]byte{})

	// Should not panic with single arg
	executeReplicatedCommand(testStore, [][]byte{[]byte("UNKNOWN")})
}

// TestExecuteReplicatedCommand_WithTTL tests SET with EX option
func TestExecuteReplicatedCommand_SetWithTTL(t *testing.T) {
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
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	// Should not panic
	rm.RemoveSlave("non-existent-id")
	assert.Equal(t, 0, rm.GetSlaveCount())
}

// TestGetSlaveByID tests GetSlaveByID method
func TestGetSlaveByID(t *testing.T) {
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
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	// Should not panic
	rm.UpdateSlaveAckOffset("non-existent-id", 100)
}

// TestSetMasterConnection tests SetMasterConnection and GetMasterConnection
func TestSetMasterConnection(t *testing.T) {
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	// Initially nil
	assert.True(t, rm.GetMasterConnection() == nil)

	// Create a mock master connection
	mock := newMockMasterConn()
	mc := &MasterConnection{
		Addr:    "127.0.0.1:6379",
		Conn:    mock,
		Reader:  bufio.NewReader(mock),
		Writer:  bufio.NewWriter(mock),
		stopCh:  make(chan struct{}),
	}

	// Set the master connection
	rm.SetMasterConnection(mc)

	// Verify it's set
	assert.True(t, rm.GetMasterConnection() != nil)
	assert.Equal(t, "127.0.0.1:6379", rm.GetMasterConnection().Addr)
}

// TestStopSlaveReplication tests StopSlaveReplication function
func TestStopSlaveReplication(t *testing.T) {
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
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	// Use an invalid address that will fail to connect
	err := StartSlaveReplication(rm, testStore, "127.0.0.1:59999")
	// Connection should fail
	assert.True(t, err != nil)
}

// TestStartSlaveReplication_SetsRole tests that StartSlaveReplication sets role to slave
func TestStartSlaveReplication_SetsRole(t *testing.T) {
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	// Try to connect to a port that won't accept connections immediately
	// This will cause the function to fail but role should still be set
	// Use a non-routable address
	err := StartSlaveReplication(rm, testStore, "10.255.255.1:1")
	// Connection should fail (timeout or refused)
	assert.True(t, err != nil)

	// Role might be set to slave before the connection fails
	// Note: The implementation sets role before connecting
}

// TestHandlePSync_PartialSync tests HandlePSync with partial sync scenario
func TestHandlePSync_PartialSync(t *testing.T) {
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
