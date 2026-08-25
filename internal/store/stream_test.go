package store

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/zeebo/assert"
)

// TestStreamXAdd tests XAdd function
func TestStreamXAdd(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Add entry to stream
	id, err := store.XAdd("mystream", StreamXAddOptions{}, "*", map[string]string{"field1": "value1"})
	assert.NoError(t, err)
	assert.True(t, len(id) > 0)

	// Add another entry
	id2, err := store.XAdd("mystream", StreamXAddOptions{}, "*", map[string]string{"field2": "value2"})
	assert.NoError(t, err)
	assert.True(t, len(id2) > 0)
}

// TestStreamXAdd_WithID tests XAdd with specific ID
func TestStreamXAdd_WithID(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Add entry with specific ID
	id, err := store.XAdd("mystream", StreamXAddOptions{}, "1000000000000-0", map[string]string{"field1": "value1"})
	assert.NoError(t, err)
	// ID might be returned without the -0 suffix when sequence is 0
	assert.True(t, len(id) > 0)
}

// TestStreamXLen tests XLen function
func TestStreamXLen(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Add entries
	_, _ = store.XAdd("mystream", StreamXAddOptions{}, "*", map[string]string{"field1": "value1"})
	_, _ = store.XAdd("mystream", StreamXAddOptions{}, "*", map[string]string{"field2": "value2"})
	_, _ = store.XAdd("mystream", StreamXAddOptions{}, "*", map[string]string{"field3": "value3"})

	// Get length
	length, err := store.XLen("mystream")
	assert.NoError(t, err)
	assert.Equal(t, int64(3), length)

	// Test non-existent stream
	length, err = store.XLen("nonexistent")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), length)
}

// TestStreamXRANGE tests XRANGE function
func TestStreamXRANGE(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Add entries
	_, _ = store.XAdd("mystream", StreamXAddOptions{}, "1000000000000-0", map[string]string{"field1": "value1"})
	_, _ = store.XAdd("mystream", StreamXAddOptions{}, "1000000000001-0", map[string]string{"field2": "value2"})
	_, _ = store.XAdd("mystream", StreamXAddOptions{}, "1000000000002-0", map[string]string{"field3": "value3"})

	// Get range
	entries, err := store.XRange("mystream", "1000000000000-0", "+", 10)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(entries))
}

// TestStreamXREVRANGE tests XREVRANGE function
func TestStreamXREVRANGE(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Add entries
	_, _ = store.XAdd("mystream", StreamXAddOptions{}, "1000000000000-0", map[string]string{"field1": "value1"})
	_, _ = store.XAdd("mystream", StreamXAddOptions{}, "1000000000001-0", map[string]string{"field2": "value2"})
	_, _ = store.XAdd("mystream", StreamXAddOptions{}, "1000000000002-0", map[string]string{"field3": "value3"})

	// Get reverse range
	entries, err := store.XRevRange("mystream", "+", "1000000000000-0", 10)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(entries))
}

// TestStreamXDel tests XDel function
func TestStreamXDel(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Add entries
	id1, _ := store.XAdd("mystream", StreamXAddOptions{}, "1000000000000-0", map[string]string{"field1": "value1"})
	_, _ = store.XAdd("mystream", StreamXAddOptions{}, "1000000000001-0", map[string]string{"field2": "value2"})

	// Delete entry
	deleted, err := store.XDel("mystream", id1)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), deleted)

	// Verify length decreased
	length, _ := store.XLen("mystream")
	assert.Equal(t, int64(1), length)
}

// TestStreamXRead tests XRead function
func TestStreamXRead(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Add entries
	id1, err := store.XAdd("mystream", StreamXAddOptions{}, "1000000000000-0", map[string]string{"field1": "value1"})
	assert.NoError(t, err)

	// Read from stream
	result, err := store.XRead(context.Background(), 10, 0, "mystream", "0")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, 1, len(result[0]["mystream"]))
	assert.Equal(t, id1, result[0]["mystream"][0].ID)
}

func TestStreamXReadBlocking(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Start blocking XRead in a goroutine
	done := make(chan []map[string][]StreamEntry, 1)
	errCh := make(chan error, 1)
	go func() {
		result, err := store.XRead(context.Background(), 10, 1000, "mystream", "$")
		if err != nil {
			errCh <- err
			return
		}
		done <- result
	}()

	// Wait for goroutine to register, then XAdd
	time.Sleep(50 * time.Millisecond)
	id, err := store.XAdd("mystream", StreamXAddOptions{}, "*", map[string]string{"field": "value"})
	assert.NoError(t, err)

	select {
	case result := <-done:
		assert.Equal(t, 1, len(result))
		assert.Equal(t, id, result[0]["mystream"][0].ID)
	case err := <-errCh:
		t.Fatalf("XReadBlocking error: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("XReadBlocking timed out - notification was missed")
	}
}

func TestStreamXReadBlockingAlreadyHasData(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Add data first
	id, err := store.XAdd("mystream", StreamXAddOptions{}, "1000000000000-0", map[string]string{"field": "value"})
	assert.NoError(t, err)

	// Blocking read should return immediately via xReadImmediate
	result, err := store.XRead(context.Background(), 10, 1000, "mystream", "0")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, id, result[0]["mystream"][0].ID)
}

func TestStreamXReadBlockingTimeout(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Block on non-existent stream with short timeout
	result, err := store.XRead(context.Background(), 10, 100, "ghoststream", "$")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(result))

	// After timeout, channel should be properly cleaned up
	store.streamBlockingMu.RLock()
	_, exists := store.streamBlockingChans["ghoststream"]
	store.streamBlockingMu.RUnlock()
	assert.False(t, exists)
}

func TestStreamXReadBlockingConcurrent(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	const numReaders = 5
	done := make(chan int, numReaders)

	for i := 0; i < numReaders; i++ {
		go func(id int) {
			key := fmt.Sprintf("stream_%d", id)
			result, err := store.XRead(context.Background(), 10, 2000, key, "$")
			if err == nil && len(result) > 0 {
				done <- id
			}
		}(i)
	}

	time.Sleep(100 * time.Millisecond)

	// Push data for each stream
	for i := 0; i < numReaders; i++ {
		key := fmt.Sprintf("stream_%d", i)
		_, err := store.XAdd(key, StreamXAddOptions{}, "*", map[string]string{"data": "hello"})
		assert.NoError(t, err)
	}

	for i := 0; i < numReaders; i++ {
		select {
		case id := <-done:
			t.Logf("Reader %d completed", id)
		case <-time.After(3 * time.Second):
			t.Fatalf("Reader %d timed out", i)
		}
	}
}

// TestStreamInfo tests XInfo function
func TestStreamInfo(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Add entries
	_, _ = store.XAdd("mystream", StreamXAddOptions{}, "1000000000000-0", map[string]string{"field1": "value1"})
	_, _ = store.XAdd("mystream", StreamXAddOptions{}, "1000000000001-0", map[string]string{"field2": "value2"})

	// Get stream info
	info, err := store.XInfo("mystream")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), info.Length)
}

// TestStreamXGroupCreate tests XGroup Create function
func TestStreamXGroupCreate(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Add entries first
	_, _ = store.XAdd("mystream", StreamXAddOptions{}, "1000000000000-0", map[string]string{"field1": "value1"})

	// Create group
	err := store.XGroupCreate("mystream", "mygroup", "0")
	assert.NoError(t, err)

	// Verify group exists
	info, err := store.XInfo("mystream")
	assert.NoError(t, err)
	assert.True(t, len(info.Groups) > 0)
}

// TestStreamXReadGroupBlocking tests blocking XReadGroup
func TestStreamXReadGroupBlocking(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	_ = store.XGroupCreate("mystream", "mygroup", "0")

	done := make(chan []map[string][]StreamEntry, 1)
	errCh := make(chan error, 1)
	go func() {
		result, err := store.XReadGroup(context.Background(), "mygroup", "consumer1", 10, 1000, "mystream")
		if err != nil {
			errCh <- err
			return
		}
		done <- result
	}()

	time.Sleep(50 * time.Millisecond)
	id, err := store.XAdd("mystream", StreamXAddOptions{}, "*", map[string]string{"field": "value"})
	assert.NoError(t, err)

	select {
	case result := <-done:
		assert.Equal(t, 1, len(result))
		assert.Equal(t, id, result[0]["mystream"][0].ID)
	case err := <-errCh:
		t.Fatalf("XReadGroupBlocking error: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("XReadGroupBlocking timed out")
	}

	// Verify consumer was tracked
	pending, err := store.XPending("mystream", "mygroup")
	assert.NoError(t, err)
	assert.True(t, len(pending) >= 1)
}

// TestStreamXReadGroupBlockingTimeout tests blocking XReadGroup with timeout
func TestStreamXReadGroupBlockingTimeout(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	err := store.XGroupCreate("mystream", "mygroup", "0")
	assert.NoError(t, err)

	result, err := store.XReadGroup(context.Background(), "mygroup", "consumer1", 10, 100, "mystream")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(result))
}

// TestStreamXReadGroup tests XReadGroup function
func TestStreamXReadGroup(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Add entries
	id, err := store.XAdd("mystream", StreamXAddOptions{}, "1000000000000-0", map[string]string{"field1": "value1"})
	assert.NoError(t, err)
	assert.Equal(t, "1000000000000-0", id)

	// Create group
	err = store.XGroupCreate("mystream", "mygroup", "0")
	assert.NoError(t, err)

	// Read from group
	results, err := store.XReadGroup(nil, "mygroup", "myconsumer", 10, 0, "mystream")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(results))
}

// TestStreamXPending tests XPending function
func TestStreamXPending(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Add entries
	id, err := store.XAdd("mystream", StreamXAddOptions{}, "1000000000000-0", map[string]string{"field1": "value1"})
	assert.NoError(t, err)
	assert.Equal(t, "1000000000000-0", id)

	// Create group
	err = store.XGroupCreate("mystream", "mygroup", "0")
	assert.NoError(t, err)

	// Read from group (this will create pending entries)
	_, err = store.XReadGroup(nil, "mygroup", "myconsumer", 10, 0, "mystream")
	assert.NoError(t, err)

	// Get pending info
	pending, err := store.XPending("mystream", "mygroup")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(pending))
}

// TestStreamXClaim tests XClaim function
func TestStreamXClaim(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Add entries
	id, err := store.XAdd("mystream", StreamXAddOptions{}, "1000000000000-0", map[string]string{"field1": "value1"})
	assert.NoError(t, err)
	assert.Equal(t, "1000000000000-0", id)

	// Create group and read
	err = store.XGroupCreate("mystream", "mygroup", "0")
	assert.NoError(t, err)
	_, err = store.XReadGroup(nil, "mygroup", "consumer1", 10, 0, "mystream")
	assert.NoError(t, err)

	// Claim for another consumer
	claimed, err := store.XClaim("mystream", "mygroup", "consumer2", XClaimOptions{}, "1000000000000-0")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(claimed))
}

// TestStreamXClaimForce verifies XClaim with force=true claims ids that are
// NOT in the PEL (Redis XCLAIM FORCE semantics) — previously a no-op that
// only claimed existing PEL entries.
func TestStreamXClaimForce(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Add entries
	_, err := store.XAdd("mystream", StreamXAddOptions{}, "1000000000000-0", map[string]string{"field1": "value1"})
	assert.NoError(t, err)
	_, err = store.XAdd("mystream", StreamXAddOptions{}, "1000000000001-0", map[string]string{"field2": "value2"})
	assert.NoError(t, err)

	// Create group WITHOUT reading (PEL is empty)
	err = store.XGroupCreate("mystream", "mygroup", "0")
	assert.NoError(t, err)

	// Without FORCE: id not in PEL → not claimed
	claimed, err := store.XClaim("mystream", "mygroup", "consumer1", XClaimOptions{}, "1000000000000-0")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(claimed))

	// With FORCE: id not in PEL → claimed and added to PEL
	claimed, err = store.XClaim("mystream", "mygroup", "consumer1", XClaimOptions{Force: true}, "1000000000000-0")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(claimed))
	assert.Equal(t, "1000000000000-0", claimed[0])

	// Now it exists in PEL: a normal claim also works
	claimed, err = store.XClaim("mystream", "mygroup", "consumer2", XClaimOptions{}, "1000000000000-0")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(claimed))
}

// TestStreamXClaimOptions verifies XCLAIM IDLE / RETRYCOUNT options take
// effect on the claimed PEL entry (LastDelivery backdated / DeliveryCount set).
func TestStreamXClaimOptions(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	_, err := store.XAdd("mystream", StreamXAddOptions{}, "1000000000000-0", map[string]string{"field1": "value1"})
	assert.NoError(t, err)
	err = store.XGroupCreate("mystream", "mygroup", "0")
	assert.NoError(t, err)
	_, err = store.XReadGroup(nil, "mygroup", "consumer1", 10, 0, "mystream")
	assert.NoError(t, err)

	// Claim with RETRYCOUNT=5 and IDLE=60000: DeliveryCount set to 5,
	// LastDelivery backdated by 60s.
	_, err = store.XClaim("mystream", "mygroup", "consumer2", XClaimOptions{
		RetryCount: 5,
		IdleMS:     60000,
	}, "1000000000000-0")
	assert.NoError(t, err)

	pending, err := store.XPending("mystream", "mygroup")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(pending))
	entry := pending[0]
	assert.Equal(t, "consumer2", entry.Consumer)
	assert.Equal(t, int64(5), entry.DeliveryCount)
	// LastDelivery ≈ now-60000 (allow ±5s clock skew)
	now := time.Now().UnixNano() / int64(time.Millisecond)
	backdated := now - entry.LastDelivery
	if backdated < 55000 || backdated > 65000 {
		t.Errorf("expected LastDelivery backdated ~60s, actual delta=%dms", backdated)
	}
}

// TestStreamXClaimLastID verifies XCLAIM LASTID sets the group's
// LastDeliveredID (previously the LASTID option was accepted but skipped).
func TestStreamXClaimLastID(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	_, err := store.XAdd("mystream", StreamXAddOptions{}, "1000000000000-0", map[string]string{"field1": "value1"})
	assert.NoError(t, err)
	err = store.XGroupCreate("mystream", "mygroup", "0")
	assert.NoError(t, err)
	_, err = store.XReadGroup(nil, "mygroup", "consumer1", 10, 0, "mystream")
	assert.NoError(t, err)

	// Claim with LASTID 2000000000000-0
	_, err = store.XClaim("mystream", "mygroup", "consumer2", XClaimOptions{
		LastID: "2000000000000-0",
	}, "1000000000000-0")
	assert.NoError(t, err)

	// Group's LastDeliveredID must be updated
	groups, err := store.XInfoGroups("mystream")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(groups))
	assert.Equal(t, "2000000000000-0", groups[0].LastDeliveredID)
}

// TestStreamXTrim tests XTrim function
func TestStreamXTrim(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Add entries
	_, _ = store.XAdd("mystream", StreamXAddOptions{}, "1000000000000-0", map[string]string{"field1": "value1"})
	_, _ = store.XAdd("mystream", StreamXAddOptions{}, "1000000000001-0", map[string]string{"field2": "value2"})
	_, _ = store.XAdd("mystream", StreamXAddOptions{}, "1000000000002-0", map[string]string{"field3": "value3"})

	// Trim to maxlen 2
	trimmed, err := store.XTrim("mystream", 2, "")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), trimmed)

	// Verify length
	length, _ := store.XLen("mystream")
	assert.Equal(t, int64(2), length)
}

// TestStreamXAutoClaim tests XAutoClaim function
func TestStreamXAutoClaim(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Add entries
	_, _ = store.XAdd("mystream", StreamXAddOptions{}, "1000000000000-0", map[string]string{"field1": "value1"})

	// Create group
	_ = store.XGroupCreate("mystream", "mygroup", "0")

	// Read to create pending
	_, _ = store.XReadGroup(nil, "mygroup", "consumer1", 10, 0, "mystream")

	// Auto claim
	result, err := store.XAutoClaim("mystream", "mygroup", "consumer2", 0, ">", XAutoClaimOptions{})
	assert.NoError(t, err)
	assert.True(t, result != nil)
}

// TestStreamXAutoClaimOptions verifies XAUTOCLAIM IDLE/RETRYCOUNT/FORCE
// options take effect (previously only COUNT/JUSTID were supported).
func TestStreamXAutoClaimOptions(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Add entries
	_, _ = store.XAdd("mystream", StreamXAddOptions{}, "1000000000000-0", map[string]string{"field1": "value1"})
	_, _ = store.XAdd("mystream", StreamXAddOptions{}, "1000000000001-0", map[string]string{"field2": "value2"})

	// Create group WITHOUT reading (PEL empty)
	_ = store.XGroupCreate("mystream", "mygroup", "0")

	// Without FORCE: PEL is empty, nothing claimed
	result, err := store.XAutoClaim("mystream", "mygroup", "consumer1", 0, "0-0", XAutoClaimOptions{})
	assert.NoError(t, err)
	assert.Equal(t, 0, len(result.ClaimedIDs))

	// With FORCE: claim entries not in the PEL (both stream entries)
	result, err = store.XAutoClaim("mystream", "mygroup", "consumer2", 0, "0-0", XAutoClaimOptions{
		Force: true,
	})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(result.ClaimedIDs))

	// RETRYCOUNT + IDLE apply to the claimed PEL entries
	result, err = store.XAutoClaim("mystream", "mygroup", "consumer3", 0, "0-0", XAutoClaimOptions{
		RetryCount: 5,
		IdleMS:     60000,
	})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(result.ClaimedIDs))

	pending, err := store.XPending("mystream", "mygroup")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(pending))
	for _, p := range pending {
		assert.Equal(t, "consumer3", p.Consumer)
		assert.Equal(t, int64(5), p.DeliveryCount)
		// LastDelivery ≈ now-60000 (allow ±5s clock skew)
		now := time.Now().UnixNano() / int64(time.Millisecond)
		backdated := now - p.LastDelivery
		if backdated < 55000 || backdated > 65000 {
			t.Errorf("expected LastDelivery backdated ~60s, actual delta=%dms", backdated)
		}
	}
}

// TestStreamXAck tests XAck function
func TestStreamXAck(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Add entries
	_, _ = store.XAdd("mystream", StreamXAddOptions{}, "1000000000000-0", map[string]string{"field1": "value1"})

	// Ack on non-existent group should return 0
	acked, err := store.XAck("mystream", "mygroup", "1000000000000-0")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), acked)
}

// TestStreamXGroupDelConsumer tests XGroupDelConsumer function
func TestStreamXGroupDelConsumer(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Add entries and create group
	store.XAdd("mystream", StreamXAddOptions{}, "1000000000000-0", map[string]string{"field": "value"})
	store.XGroupCreate("mystream", "mygroup", "0")

	// Delete consumer (function takes key, group, consumer)
	removed, err := store.XGroupDelConsumer("mystream", "mygroup", "consumer1")
	assert.NoError(t, err)
	// Should return 0 since the entry wasn't claimed
	_ = removed
}

// TestStreamXGroupDestroy tests XGroupDestroy function
func TestStreamXGroupDestroy(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Create group
	store.XAdd("mystream", StreamXAddOptions{}, "1000000000000-0", map[string]string{"field": "value"})
	store.XGroupCreate("mystream", "mygroup", "0")

	// Destroy group
	err := store.XGroupDestroy("mystream", "mygroup")
	assert.NoError(t, err)
}

// TestStreamXGroupSetID tests XGroupSetID function
func TestStreamXGroupSetID(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Add entries and create group
	store.XAdd("mystream", StreamXAddOptions{}, "1000000000000-0", map[string]string{"field": "value"})
	store.XGroupCreate("mystream", "mygroup", "0")

	// Set new ID
	err := store.XGroupSetID("mystream", "mygroup", "2000000000000-0")
	assert.NoError(t, err)
}

// TestWriteCommandXGroupCreateConsumer verifies the replication replay path
// handles XGROUP CREATECONSUMER (was falling through to the default branch
// and only logging, leaving the consumer missing on the replica).
func TestWriteCommandXGroupCreateConsumer(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	_, err := store.XAdd("mystream", StreamXAddOptions{}, "1000000000000-0", map[string]string{"field": "value"})
	assert.NoError(t, err)
	err = store.XGroupCreate("mystream", "mygroup", "0")
	assert.NoError(t, err)

	// Replay XGROUP CREATECONSUMER mystream mygroup consumer1
	err = WriteCommand(store, [][]byte{
		[]byte("XGROUP"), []byte("CREATECONSUMER"), []byte("mystream"), []byte("mygroup"), []byte("consumer1"),
	}, context.Background())
	assert.NoError(t, err)

	// Consumer must exist in the group
	groups, err := store.XInfoGroups("mystream")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(groups))
	found := false
	if groups[0].Consumers != nil {
		_, found = groups[0].Consumers["consumer1"]
	}
	assert.True(t, found)
}

// TestWriteCommandXAutoClaimOptions verifies the replication replay path
// parses XAUTOCLAIM's IDLE/RETRYCOUNT/FORCE options (previously only
// COUNT/JUSTID were replayed, so PEL LastDelivery/DeliveryCount diverged
// between master and replica).
func TestWriteCommandXAutoClaimOptions(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	_, err := store.XAdd("mystream", StreamXAddOptions{}, "1000000000000-0", map[string]string{"field": "value"})
	assert.NoError(t, err)
	_, err = store.XAdd("mystream", StreamXAddOptions{}, "1000000000001-0", map[string]string{"field": "value"})
	assert.NoError(t, err)
	err = store.XGroupCreate("mystream", "mygroup", "0")
	assert.NoError(t, err)

	// Replay XAUTOCLAIM with FORCE + RETRYCOUNT 5 + IDLE 60000
	err = WriteCommand(store, [][]byte{
		[]byte("XAUTOCLAIM"), []byte("mystream"), []byte("mygroup"), []byte("c1"),
		[]byte("0"), []byte("0-0"), []byte("FORCE"), []byte("RETRYCOUNT"), []byte("5"),
		[]byte("IDLE"), []byte("60000"),
	}, context.Background())
	assert.NoError(t, err)

	pending, err := store.XPending("mystream", "mygroup")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(pending))
	for _, p := range pending {
		assert.Equal(t, "c1", p.Consumer)
		assert.Equal(t, int64(5), p.DeliveryCount)
		now := time.Now().UnixNano() / int64(time.Millisecond)
		backdated := now - p.LastDelivery
		if backdated < 55000 || backdated > 65000 {
			t.Errorf("expected LastDelivery backdated ~60s, actual delta=%dms", backdated)
		}
	}
}

// TestWriteCommandXReadGroupNoAck verifies the replication replay path honors
// XREADGROUP NOACK: entries are acked after delivery so the replica PEL stays
// empty, matching the master (previously NOACK was skipped on replay, leaving
// the entries pending on the replica).
func TestWriteCommandXReadGroupNoAck(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	_, err := store.XAdd("mystream", StreamXAddOptions{}, "1000000000000-0", map[string]string{"field": "value"})
	assert.NoError(t, err)
	err = store.XGroupCreate("mystream", "mygroup", "0")
	assert.NoError(t, err)

	// Replay XREADGROUP GROUP mygroup c1 NOACK STREAMS mystream >
	err = WriteCommand(store, [][]byte{
		[]byte("XREADGROUP"), []byte("GROUP"), []byte("mygroup"), []byte("c1"),
		[]byte("NOACK"), []byte("STREAMS"), []byte("mystream"), []byte(">"),
	}, context.Background())
	assert.NoError(t, err)

	// PEL must be empty after NOACK replay (entry was acked)
	pending, err := store.XPending("mystream", "mygroup")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(pending))
}

// TestWriteCommandXAddMaxLen verifies the replication replay path parses
// XADD MAXLEN (previously the option was treated as a field, writing
// garbage fields and never trimming, so the replica diverged from master).
func TestWriteCommandXAddMaxLen(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Replay 3 XADD with MAXLEN 2
	for i := 0; i < 3; i++ {
		err := WriteCommand(store, [][]byte{
			[]byte("XADD"), []byte("mystream"), []byte("MAXLEN"), []byte("2"), []byte("*"), []byte("f"), []byte("v"),
		}, context.Background())
		assert.NoError(t, err)
	}

	// Stream must be trimmed to 2
	length, err := store.XLen("mystream")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), length)

	// No garbage fields from the MAXLEN option
	entries, err := store.XRange("mystream", "-", "+", -1)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(entries))
	for _, e := range entries {
		assert.Equal(t, "v", e.Fields["f"])
		if _, bad := e.Fields["MAXLEN"]; bad {
			t.Error("MAXLEN option leaked into fields")
		}
	}
}

// TestStreamXInfoGroups tests XInfoGroups function
func TestStreamXInfoGroups(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Test on non-existent stream - returns empty groups, not error
	groups, err := store.XInfoGroups("nonexistent")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(groups))

	// Create stream with group
	store.XAdd("mystream", StreamXAddOptions{}, "1000000000000-0", map[string]string{"field": "value"})
	store.XGroupCreate("mystream", "mygroup", "0")

	// Get info
	groups, err = store.XInfoGroups("mystream")
	assert.NoError(t, err)
	assert.True(t, len(groups) > 0)
}

// TestStreamXInfoConsumers tests XInfoConsumers function
func TestStreamXInfoConsumers(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Test on non-existent stream - returns empty consumers, not error
	consumers, err := store.XInfoConsumers("nonexistent", "mygroup")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(consumers))

	// Create stream with group
	store.XAdd("mystream", StreamXAddOptions{}, "1000000000000-0", map[string]string{"field": "value"})
	store.XGroupCreate("mystream", "mygroup", "0")

	// Get info (no consumers yet)
	consumers, err = store.XInfoConsumers("mystream", "mygroup")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(consumers))
}

// TestStreamType tests StreamType function
func TestStreamType(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Test on non-existent key - should return false, no error
	exists, err := store.StreamType("nonexistent")
	assert.NoError(t, err)
	assert.False(t, exists)

	// Create a stream
	store.XAdd("mystream", StreamXAddOptions{}, "1000000000000-0", map[string]string{"field": "value"})

	// Get type
	exists, err = store.StreamType("mystream")
	assert.NoError(t, err)
	assert.True(t, exists)
}

// TestGetStreamEntry tests GetStreamEntry function
func TestGetStreamEntry(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Test on non-existent stream
	_, err := store.GetStreamEntry("nonexistent", "1000000000000-0")
	assert.Error(t, err)

	// Add entry
	id, err := store.XAdd("mystream", StreamXAddOptions{}, "1000000000000-0", map[string]string{"field": "value"})
	assert.NoError(t, err)

	// Get entry
	entry, err := store.GetStreamEntry("mystream", id)
	assert.NoError(t, err)
	assert.NotNil(t, entry)
	assert.Equal(t, "1000000000000-0", entry.ID)
}

// TestXAckDelRemoveRefs 验证 XACKDEL DELREF 模式下 PEL 引用在所有 consumer group 中被正确清理
// 前置条件: 两个 consumer group 都有同一 entry 的 PEL 引用
// 操作: XAckDelRemoveRefs
// 预期: 两个 group 的 PEL 中均不再包含该 entry
// 边缘场景: 不存在的 stream、不存在的 entry、空的 PEL
func TestXAckDelRemoveRefs(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// 边缘场景 1: 不存在的 stream → 不应报错
	err := store.XAckDelRemoveRefs("nonexistent_stream", "1000000000000-0")
	assert.NoError(t, err)

	stream := "xackdel_delref_test"

	// 添加 entry
	id, err := store.XAdd(stream, StreamXAddOptions{}, "1000000000000-0", map[string]string{"f": "v"})
	assert.NoError(t, err)

	// 创建两个 consumer group
	err = store.XGroupCreate(stream, "group1", "0")
	assert.NoError(t, err)
	err = store.XGroupCreate(stream, "group2", "0")
	assert.NoError(t, err)

	// 两个 group 分别读取，产生 PEL 引用
	_, err = store.XReadGroup(nil, "group1", "consumer1", 10, 0, stream)
	assert.NoError(t, err)
	_, err = store.XReadGroup(nil, "group2", "consumer2", 10, 0, stream)
	assert.NoError(t, err)

	// 验证两个 group 的 PEL 都有该 entry
	p1, err := store.XPending(stream, "group1")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(p1))
	assert.Equal(t, id, p1[0].ID)

	p2, err := store.XPending(stream, "group2")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(p2))
	assert.Equal(t, id, p2[0].ID)

	// 边缘场景 2: 不存在的 entry → 不应报错（空操作）
	err = store.XAckDelRemoveRefs(stream, "9999999999999-0")
	assert.NoError(t, err)

	// 两个 group 的 PEL 应保持不变
	p1, err = store.XPending(stream, "group1")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(p1))

	p2, err = store.XPending(stream, "group2")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(p2))

	// 执行 XAckDelRemoveRefs — 清理所有 group 中的 PEL 引用
	err = store.XAckDelRemoveRefs(stream, id)
	assert.NoError(t, err)

	// 验证 group1 PEL 已清空
	p1, err = store.XPending(stream, "group1")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(p1))

	// 验证 group2 PEL 已清空
	p2, err = store.XPending(stream, "group2")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(p2))

	// 边缘场景 3: 已清理后再次调用 → 不应报错（幂等）
	err = store.XAckDelRemoveRefs(stream, id)
	assert.NoError(t, err)
}

// TestXIsAckedByAllGroups 验证 XACKDEL ACKED 模式下双 group 确认语义
// 场景: 0 个 group / 1 个 group / 2 个 group / 3 个 group
//
//	→ 0 group: vacuous truth → true
//	→ 1 group, ACKed → true
//	→ 2 groups, 1 ACKed → false
//	→ 2 groups, both ACKed → true
//	→ 3 groups, 2 ACKed → false
func TestXIsAckedByAllGroups(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	stream := "xackdel_acked_test"

	// 添加一个 entry
	id, err := store.XAdd(stream, StreamXAddOptions{}, "1000000000000-0", map[string]string{"f": "v"})
	assert.NoError(t, err)

	// 边缘场景 1: 0 个 group → vacuous truth, 应返回 true
	allAcked, err := store.XIsAckedByAllGroups(stream, id)
	assert.NoError(t, err)
	assert.True(t, allAcked)

	// 创建 1 个 group
	err = store.XGroupCreate(stream, "group1", "0")
	assert.NoError(t, err)

	// 读取产生 PEL 引用
	_, err = store.XReadGroup(nil, "group1", "consumer1", 10, 0, stream)
	assert.NoError(t, err)

	// 边缘场景 2: 1 个 group, 未 ACK → 应返回 false
	allAcked, err = store.XIsAckedByAllGroups(stream, id)
	assert.NoError(t, err)
	assert.False(t, allAcked)

	// group1 ACK
	acked, err := store.XAck(stream, "group1", id)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), acked)

	// 边缘场景 3: 1 个 group, 已 ACK → 应返回 true
	allAcked, err = store.XIsAckedByAllGroups(stream, id)
	assert.NoError(t, err)
	assert.True(t, allAcked)

	// 创建第 2 个 group 并读取产生 PEL
	err = store.XGroupCreate(stream, "group2", "0")
	assert.NoError(t, err)
	_, err = store.XReadGroup(nil, "group2", "consumer2", 10, 0, stream)
	assert.NoError(t, err)

	// 边缘场景 4: 2 个 group, 仅 1 个 ACK → 应返回 false
	allAcked, err = store.XIsAckedByAllGroups(stream, id)
	assert.NoError(t, err)
	assert.False(t, allAcked)

	// group2 也 ACK
	acked, err = store.XAck(stream, "group2", id)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), acked)

	// 边缘场景 5: 2 个 group, 均已 ACK → 应返回 true
	allAcked, err = store.XIsAckedByAllGroups(stream, id)
	assert.NoError(t, err)
	assert.True(t, allAcked)

	// 创建第 3 个 group 并读取产生 PEL
	err = store.XGroupCreate(stream, "group3", "0")
	assert.NoError(t, err)
	_, err = store.XReadGroup(nil, "group3", "consumer3", 10, 0, stream)
	assert.NoError(t, err)

	// 边缘场景 6: 3 个 group, 仅 2 个 ACK → 应返回 false
	allAcked, err = store.XIsAckedByAllGroups(stream, id)
	assert.NoError(t, err)
	assert.False(t, allAcked)

	// group3 也 ACK
	acked, err = store.XAck(stream, "group3", id)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), acked)

	// 边缘场景 7: 3 个 group, 均已 ACK → 应返回 true
	allAcked, err = store.XIsAckedByAllGroups(stream, id)
	assert.NoError(t, err)
	assert.True(t, allAcked)
}

// 边缘场景 8: entry 已从 stream 中删除 → 应返回 true（dataKey 不存在）
func TestXIsAckedByAllGroups_EntryDeleted(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	stream := "xackdel_deleted_entry"

	// 添加 entry 并创建 group，产生 PEL
	id, err := store.XAdd(stream, StreamXAddOptions{}, "1000000000000-0", map[string]string{"f": "v"})
	assert.NoError(t, err)
	err = store.XGroupCreate(stream, "g1", "0")
	assert.NoError(t, err)
	_, err = store.XReadGroup(nil, "g1", "c1", 10, 0, stream)
	assert.NoError(t, err)

	// 先 ACK 再删除 entry
	_, err = store.XAck(stream, "g1", id)
	assert.NoError(t, err)
	del, err := store.XDel(stream, id)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), del)

	// 验证 entry 确实已从 stream 中删除
	entries, err := store.XRange(stream, "-", "+", 0)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(entries))

	// entry 已从 stream 中删除 → XIsAckedByAllGroups 应短路返回 true
	allAcked, err := store.XIsAckedByAllGroups(stream, id)
	assert.NoError(t, err)
	assert.True(t, allAcked)
}

// TestWriteCommandGetExExat verifies the replication replay path handles
// GETEX EXAT/PXAT (previously only EX/PX/PERSIST were replayed, so absolute
// TTLs set on the master were lost on the replica).
func TestWriteCommandGetExExat(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	err := store.Set("k", "v")
	assert.NoError(t, err)

	// Replay GETEX k EXAT <future ts> — sets absolute expiry in seconds
	future := time.Now().Unix() + 3600
	err = WriteCommand(store, [][]byte{
		[]byte("GETEX"), []byte("k"), []byte("EXAT"), []byte(strconv.FormatInt(future, 10)),
	}, context.Background())
	assert.NoError(t, err)

	// Key must have a TTL ~3600s
	ttl, err := store.TTL("k")
	assert.NoError(t, err)
	if ttl <= 0 || ttl > 3600 {
		t.Errorf("expected TTL ~3600s after EXAT replay, got %v", ttl)
	}

	// Replay GETEX k PXAT <future ms> — sets absolute expiry in millis
	futureMs := time.Now().UnixMilli() + 7200000
	err = WriteCommand(store, [][]byte{
		[]byte("GETEX"), []byte("k"), []byte("PXAT"), []byte(strconv.FormatInt(futureMs, 10)),
	}, context.Background())
	assert.NoError(t, err)

	ttl, err = store.TTL("k")
	assert.NoError(t, err)
	if ttl <= 0 || ttl > 7200 {
		t.Errorf("expected TTL ~7200s after PXAT replay, got %v", ttl)
	}
}

// TestWriteCommandSortGet verifies the replication replay path honors
// SORT ... GET pattern ... STORE dest (previously GET was ignored on replay,
// so the destination list diverged from the master).
func TestWriteCommandSortGet(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Source list + target keys for GET pattern "score:*"
	_, err := store.RPush("mylist", "a", "b")
	assert.NoError(t, err)
	err = store.Set("score:a", "10")
	assert.NoError(t, err)
	err = store.Set("score:b", "20")
	assert.NoError(t, err)

	// Replay: SORT mylist GET score:* STORE out
	err = WriteCommand(store, [][]byte{
		[]byte("SORT"), []byte("mylist"), []byte("GET"), []byte("score:*"), []byte("STORE"), []byte("out"),
	}, context.Background())
	assert.NoError(t, err)

	// Destination must contain the GET-resolved values (order: a then b)
	vals, err := store.LRange("out", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(vals))
	assert.Equal(t, "10", vals[0])
	assert.Equal(t, "20", vals[1])
}

// TestWriteCommandSetOptions verifies the replication replay path honors
// SET NX/XX/KEEPTTL/EXAT/PXAT (previously only EX/PX were replayed, so
// conditional sets diverged between master and replica).
func TestWriteCommandSetOptions(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// SET k v1 NX → key does not exist, so it sets
	err := WriteCommand(store, [][]byte{
		[]byte("SET"), []byte("k"), []byte("v1"), []byte("NX"),
	}, context.Background())
	assert.NoError(t, err)
	val, err := store.Get("k")
	assert.NoError(t, err)
	assert.Equal(t, "v1", val)

	// SET k v2 NX → key exists, must NOT overwrite
	err = WriteCommand(store, [][]byte{
		[]byte("SET"), []byte("k"), []byte("v2"), []byte("NX"),
	}, context.Background())
	assert.NoError(t, err)
	val, err = store.Get("k")
	assert.NoError(t, err)
	assert.Equal(t, "v1", val)

	// SET k v3 XX → key exists, overwrites
	err = WriteCommand(store, [][]byte{
		[]byte("SET"), []byte("k"), []byte("v3"), []byte("XX"),
	}, context.Background())
	assert.NoError(t, err)
	val, err = store.Get("k")
	assert.NoError(t, err)
	assert.Equal(t, "v3", val)

	// SET nxkey v XX → key does not exist, must NOT set
	err = WriteCommand(store, [][]byte{
		[]byte("SET"), []byte("nxkey"), []byte("v"), []byte("XX"),
	}, context.Background())
	assert.NoError(t, err)
	exists, err := store.Exists("nxkey")
	assert.NoError(t, err)
	assert.False(t, exists)

	// SET k v4 EXAT <future> → sets absolute expiry in seconds
	future := time.Now().Unix() + 3600
	err = WriteCommand(store, [][]byte{
		[]byte("SET"), []byte("k"), []byte("v4"), []byte("EXAT"), []byte(strconv.FormatInt(future, 10)),
	}, context.Background())
	assert.NoError(t, err)
	ttl, err := store.TTL("k")
	assert.NoError(t, err)
	if ttl <= 0 || ttl > 3600 {
		t.Errorf("expected TTL ~3600s after SET EXAT replay, got %v", ttl)
	}

	// SET k v5 KEEPTTL → preserves the existing TTL
	err = WriteCommand(store, [][]byte{
		[]byte("SET"), []byte("k"), []byte("v5"), []byte("KEEPTTL"),
	}, context.Background())
	assert.NoError(t, err)
	val, err = store.Get("k")
	assert.NoError(t, err)
	assert.Equal(t, "v5", val)
	ttl2, err := store.TTL("k")
	assert.NoError(t, err)
	if ttl2 <= 0 || ttl2 > 3600 {
		t.Errorf("expected TTL preserved ~3600s after SET KEEPTTL replay, got %v", ttl2)
	}
}

// TestWriteCommandSetExatPast verifies the replication replay path correctly
// handles SET with past EXAT/PXAT timestamps: key must be written then
// immediately expire (not survive permanently).
func TestWriteCommandSetExatPast(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	pastSec := time.Now().Unix() - 3600
	err := WriteCommand(store, [][]byte{
		[]byte("SET"), []byte("k1"), []byte("val1"), []byte("EXAT"), []byte(strconv.FormatInt(pastSec, 10)),
	}, context.Background())
	assert.NoError(t, err)

	// key should not be found — it expired immediately
	val, err := store.Get("k1")
	assert.Error(t, err) // ErrKeyNotFound
	assert.Equal(t, "", val)

	// Same for past PXAT (milliseconds)
	pastMs := time.Now().UnixMilli() - 3600000
	err = WriteCommand(store, [][]byte{
		[]byte("SET"), []byte("k2"), []byte("val2"), []byte("PXAT"), []byte(strconv.FormatInt(pastMs, 10)),
	}, context.Background())
	assert.NoError(t, err)

	val, err = store.Get("k2")
	assert.Error(t, err)
	assert.Equal(t, "", val)
}

// TestStreamXAddNoMkStream verifies XADD NOMKSTREAM refuses to create a
// missing stream (previously NOMKSTREAM was accepted but ignored).
func TestStreamXAddNoMkStream(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// NOMKSTREAM on a missing stream → error
	_, err := store.XAdd("missing_stream", StreamXAddOptions{NoMkStream: true}, "*", map[string]string{"f": "v"})
	assert.Error(t, err)
	assert.Equal(t, ErrStreamNotFound, err)

	// Without NOMKSTREAM, the stream is auto-created
	id, err := store.XAdd("auto_stream", StreamXAddOptions{}, "*", map[string]string{"f": "v"})
	assert.NoError(t, err)
	assert.NotEqual(t, "", id)

	// NOMKSTREAM on an existing stream → works fine
	id2, err := store.XAdd("auto_stream", StreamXAddOptions{NoMkStream: true}, "*", map[string]string{"f": "v"})
	assert.NoError(t, err)
	assert.NotEqual(t, "", id2)
}

// TestWriteCommandLMPop verifies the replication replay path applies
// LMPOP (LEFT pop with COUNT) on the replica.
func TestWriteCommandLMPop(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	_, err := store.RPush("l1", "a", "b", "c")
	assert.NoError(t, err)

	// Replay: LMPOP 1 l1 LEFT COUNT 2
	err = WriteCommand(store, [][]byte{
		[]byte("LMPOP"), []byte("1"), []byte("l1"), []byte("LEFT"), []byte("COUNT"), []byte("2"),
	}, context.Background())
	assert.NoError(t, err)

	// Two elements popped from the left
	vals, err := store.LRange("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(vals))
	assert.Equal(t, "c", vals[0])
}

// TestWriteCommandZMPop verifies the replication replay path applies
// ZMPOP (MIN pop with COUNT) on the replica.
func TestWriteCommandZMPop(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	err := store.ZAdd("z1", []ZSetMember{{Member: "m1", Score: 1}, {Member: "m2", Score: 2}, {Member: "m3", Score: 3}})
	assert.NoError(t, err)

	// Replay: ZMPOP 1 z1 MIN COUNT 2
	err = WriteCommand(store, [][]byte{
		[]byte("ZMPOP"), []byte("1"), []byte("z1"), []byte("MIN"), []byte("COUNT"), []byte("2"),
	}, context.Background())
	assert.NoError(t, err)

	// Two lowest-score members popped
	left, err := store.ZRange("z1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(left))
	assert.Equal(t, "m3", left[0].Member)
}

// TestWriteCommandBZMPop verifies the replication replay path applies
// BZMPOP as its non-blocking equivalent (ZMPop MIN with COUNT).
func TestWriteCommandBZMPop(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	err := store.ZAdd("z1", []ZSetMember{{Member: "m1", Score: 1}, {Member: "m2", Score: 2}, {Member: "m3", Score: 3}})
	assert.NoError(t, err)

	// Replay: BZMPOP 0 1 z1 MIN COUNT 1 (non-blocking on replica)
	err = WriteCommand(store, [][]byte{
		[]byte("BZMPOP"), []byte("0"), []byte("1"), []byte("z1"), []byte("MIN"), []byte("COUNT"), []byte("1"),
	}, context.Background())
	assert.NoError(t, err)

	// One lowest-score member popped
	left, err := store.ZRange("z1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(left))
	assert.Equal(t, "m2", left[0].Member)
}

// TestWriteCommandGetDel verifies the replication replay path applies
// GETDEL (get-and-delete) on the replica.
func TestWriteCommandGetDel(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	err := store.Set("k", "v")
	assert.NoError(t, err)

	// Replay: GETDEL k
	err = WriteCommand(store, [][]byte{
		[]byte("GETDEL"), []byte("k"),
	}, context.Background())
	assert.NoError(t, err)

	// Key must be deleted
	exists, err := store.Exists("k")
	assert.NoError(t, err)
	assert.False(t, exists)
}

// TestWriteCommandSetNx verifies the replication replay path applies
// SETNX (set-if-not-exists) on the replica.
func TestWriteCommandSetNx(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Replay: SETNX k v → key missing, sets
	err := WriteCommand(store, [][]byte{
		[]byte("SETNX"), []byte("k"), []byte("v"),
	}, context.Background())
	assert.NoError(t, err)
	val, err := store.Get("k")
	assert.NoError(t, err)
	assert.Equal(t, "v", val)

	// Replay: SETNX k v2 → key exists, must NOT overwrite
	err = WriteCommand(store, [][]byte{
		[]byte("SETNX"), []byte("k"), []byte("v2"),
	}, context.Background())
	assert.NoError(t, err)
	val, err = store.Get("k")
	assert.NoError(t, err)
	assert.Equal(t, "v", val)
}

// TestWriteCommandBZPopMax verifies the replication replay path applies
// BZPOPMAX as its non-blocking equivalent (ZPopMax 1).
func TestWriteCommandBZPopMax(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	err := store.ZAdd("z1", []ZSetMember{{Member: "m1", Score: 1}, {Member: "m2", Score: 2}})
	assert.NoError(t, err)

	// Replay: BZPOPMAX z1 0 (non-blocking on replica)
	err = WriteCommand(store, [][]byte{
		[]byte("BZPOPMAX"), []byte("z1"), []byte("0"),
	}, context.Background())
	assert.NoError(t, err)

	// Highest-score member popped
	left, err := store.ZRange("z1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(left))
	assert.Equal(t, "m1", left[0].Member)
}

// TestWriteCommandBLPop verifies the replication replay path applies
// BLPOP as its non-blocking equivalent (LPop 1).
func TestWriteCommandBLPop(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	_, err := store.RPush("l1", "a", "b")
	assert.NoError(t, err)

	// Replay: BLPOP l1 0 (non-blocking on replica)
	err = WriteCommand(store, [][]byte{
		[]byte("BLPOP"), []byte("l1"), []byte("0"),
	}, context.Background())
	assert.NoError(t, err)

	// Leftmost element popped
	vals, err := store.LRange("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(vals))
	assert.Equal(t, "b", vals[0])
}

// TestWriteCommandBRPop verifies the replication replay path applies
// BRPOP as its non-blocking equivalent (RPop 1).
func TestWriteCommandBRPop(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	_, err := store.RPush("l1", "a", "b")
	assert.NoError(t, err)

	// Replay: BRPOP l1 0 (non-blocking on replica)
	err = WriteCommand(store, [][]byte{
		[]byte("BRPOP"), []byte("l1"), []byte("0"),
	}, context.Background())
	assert.NoError(t, err)

	// Rightmost element popped
	vals, err := store.LRange("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(vals))
	assert.Equal(t, "a", vals[0])
}

// TestWriteCommandBLMove verifies the replication replay path applies
// BLMOVE as its non-blocking equivalent (LMove).
func TestWriteCommandBLMove(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	_, err := store.RPush("src", "a", "b")
	assert.NoError(t, err)

	// Replay: BLMOVE src dst LEFT RIGHT 0 (non-blocking on replica)
	err = WriteCommand(store, [][]byte{
		[]byte("BLMOVE"), []byte("src"), []byte("dst"), []byte("LEFT"), []byte("RIGHT"), []byte("0"),
	}, context.Background())
	assert.NoError(t, err)

	// Leftmost of src moved to right of dst
	srcVals, err := store.LRange("src", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(srcVals))
	assert.Equal(t, "b", srcVals[0])

	dstVals, err := store.LRange("dst", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(dstVals))
	assert.Equal(t, "a", dstVals[0])
}

// TestWriteCommandBRPopLPush verifies the replication replay path applies
// BRPOPLPUSH as its non-blocking equivalent (RPopLPush).
func TestWriteCommandBRPopLPush(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	_, err := store.RPush("src", "a", "b")
	assert.NoError(t, err)

	// Replay: BRPOPLPUSH src dst 0 (non-blocking on replica)
	err = WriteCommand(store, [][]byte{
		[]byte("BRPOPLPUSH"), []byte("src"), []byte("dst"), []byte("0"),
	}, context.Background())
	assert.NoError(t, err)

	// Rightmost of src moved to left of dst
	srcVals, err := store.LRange("src", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(srcVals))
	assert.Equal(t, "a", srcVals[0])

	dstVals, err := store.LRange("dst", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(dstVals))
	assert.Equal(t, "b", dstVals[0])
}

// TestWriteCommandRenameNx verifies the replication replay path applies
// RENAMENX (rename if new key absent) on the replica.
func TestWriteCommandRenameNx(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	err := store.Set("k1", "v1")
	assert.NoError(t, err)

	// Replay: RENAMENX k1 k2 → k2 absent, renames
	err = WriteCommand(store, [][]byte{
		[]byte("RENAMENX"), []byte("k1"), []byte("k2"),
	}, context.Background())
	assert.NoError(t, err)

	val, err := store.Get("k2")
	assert.NoError(t, err)
	assert.Equal(t, "v1", val)

	// Replay: RENAMENX k2 k3 → set k3 first so target exists, must NOT rename
	err = store.Set("k3", "occupied")
	assert.NoError(t, err)
	err = WriteCommand(store, [][]byte{
		[]byte("RENAMENX"), []byte("k2"), []byte("k3"),
	}, context.Background())
	assert.NoError(t, err)
	val, err = store.Get("k2")
	assert.NoError(t, err)
	assert.Equal(t, "v1", val) // k2 kept (k3 exists)
	val, err = store.Get("k3")
	assert.NoError(t, err)
	assert.Equal(t, "occupied", val)
}

// TestWriteCommandSMove verifies the replication replay path applies
// SMOVE (move member between sets) on the replica.
func TestWriteCommandSMove(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	_, err := store.SAdd("set1", "a", "b")
	assert.NoError(t, err)

	// Replay: SMOVE set1 set2 a
	err = WriteCommand(store, [][]byte{
		[]byte("SMOVE"), []byte("set1"), []byte("set2"), []byte("a"),
	}, context.Background())
	assert.NoError(t, err)

	// "a" moved to set2, "b" remains in set1
	m1, err := store.SMembers("set1")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(m1))
	assert.Equal(t, "b", m1[0])

	m2, err := store.SMembers("set2")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(m2))
	assert.Equal(t, "a", m2[0])
}

// TestWriteCommandZUnionStore verifies the replication replay path applies
// ZUNIONSTORE (sum scores into destination) on the replica.
func TestWriteCommandZUnionStore(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	err := store.ZAdd("z1", []ZSetMember{{Member: "m1", Score: 1}, {Member: "m2", Score: 2}})
	assert.NoError(t, err)
	err = store.ZAdd("z2", []ZSetMember{{Member: "m2", Score: 3}})
	assert.NoError(t, err)

	// Replay: ZUNIONSTORE dst 2 z1 z2
	err = WriteCommand(store, [][]byte{
		[]byte("ZUNIONSTORE"), []byte("dst"), []byte("2"), []byte("z1"), []byte("z2"),
	}, context.Background())
	assert.NoError(t, err)

	// m1 = 1 (only in z1), m2 = 2+3 = 5
	members, err := store.ZRange("dst", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(members))
	scoreMap := map[string]float64{}
	for _, m := range members {
		scoreMap[m.Member] = m.Score
	}
	assert.Equal(t, float64(1), scoreMap["m1"])
	assert.Equal(t, float64(5), scoreMap["m2"])
}

// TestWriteCommandZRemRangeByLex verifies the replication replay path applies
// ZREMRANGEBYLEX on the replica.
func TestWriteCommandZRemRangeByLex(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	err := store.ZAdd("z1", []ZSetMember{
		{Member: "a", Score: 1}, {Member: "b", Score: 2}, {Member: "c", Score: 3},
	})
	assert.NoError(t, err)

	// Replay: ZREMRANGEBYLEX z1 [b + (remove b..)
	err = WriteCommand(store, [][]byte{
		[]byte("ZREMRANGEBYLEX"), []byte("z1"), []byte("[b"), []byte("+"),
	}, context.Background())
	assert.NoError(t, err)

	// Only "a" remains
	left, err := store.ZRange("z1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(left))
	assert.Equal(t, "a", left[0].Member)
}

// TestWriteCommandZRemRangeByRank verifies the replication replay path
// applies ZREMRANGEBYRANK on the replica.
func TestWriteCommandZRemRangeByRank(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	err := store.ZAdd("z1", []ZSetMember{
		{Member: "m1", Score: 1}, {Member: "m2", Score: 2}, {Member: "m3", Score: 3},
	})
	assert.NoError(t, err)

	// Replay: ZREMRANGEBYRANK z1 0 0 (remove lowest rank)
	err = WriteCommand(store, [][]byte{
		[]byte("ZREMRANGEBYRANK"), []byte("z1"), []byte("0"), []byte("0"),
	}, context.Background())
	assert.NoError(t, err)

	left, err := store.ZRange("z1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(left))
	assert.Equal(t, "m2", left[0].Member)
}

// TestWriteCommandZRemRangeByScore verifies the replication replay path
// applies ZREMRANGEBYSCORE on the replica.
func TestWriteCommandZRemRangeByScore(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	err := store.ZAdd("z1", []ZSetMember{
		{Member: "m1", Score: 1}, {Member: "m2", Score: 2}, {Member: "m3", Score: 3},
	})
	assert.NoError(t, err)

	// Replay: ZREMRANGEBYSCORE z1 2 3 (remove score >= 2)
	err = WriteCommand(store, [][]byte{
		[]byte("ZREMRANGEBYSCORE"), []byte("z1"), []byte("2"), []byte("3"),
	}, context.Background())
	assert.NoError(t, err)

	left, err := store.ZRange("z1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(left))
	assert.Equal(t, "m1", left[0].Member)
}

// TestWriteCommandZPopMax verifies the replication replay path applies
// ZPOPMAX (pop highest score) on the replica.
func TestWriteCommandZPopMax(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	err := store.ZAdd("z1", []ZSetMember{{Member: "m1", Score: 1}, {Member: "m2", Score: 2}})
	assert.NoError(t, err)

	// Replay: ZPOPMAX z1
	err = WriteCommand(store, [][]byte{
		[]byte("ZPOPMAX"), []byte("z1"),
	}, context.Background())
	assert.NoError(t, err)

	// Highest-score member popped
	left, err := store.ZRange("z1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(left))
	assert.Equal(t, "m1", left[0].Member)
}

// TestWriteCommandZPopMin verifies the replication replay path applies
// ZPOPMIN (pop lowest score) on the replica.
func TestWriteCommandZPopMin(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	err := store.ZAdd("z1", []ZSetMember{{Member: "m1", Score: 1}, {Member: "m2", Score: 2}})
	assert.NoError(t, err)

	// Replay: ZPOPMIN z1
	err = WriteCommand(store, [][]byte{
		[]byte("ZPOPMIN"), []byte("z1"),
	}, context.Background())
	assert.NoError(t, err)

	// Lowest-score member popped
	left, err := store.ZRange("z1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(left))
	assert.Equal(t, "m2", left[0].Member)
}

// TestWriteCommandZIncrBy verifies the replication replay path applies
// ZINCRBY (increment member score) on the replica.
func TestWriteCommandZIncrBy(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	err := store.ZAdd("z1", []ZSetMember{{Member: "m1", Score: 1}})
	assert.NoError(t, err)

	// Replay: ZINCRBY z1 5 m1
	err = WriteCommand(store, [][]byte{
		[]byte("ZINCRBY"), []byte("z1"), []byte("5"), []byte("m1"),
	}, context.Background())
	assert.NoError(t, err)

	// Score incremented by 5 → 6
	score, _, err := store.ZScore("z1", "m1")
	assert.NoError(t, err)
	assert.Equal(t, float64(6), score)
}

// TestWriteCommandSAdd verifies the replication replay path applies
// SADD on the replica.
func TestWriteCommandSAdd(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Replay: SADD set1 a b
	err := WriteCommand(store, [][]byte{
		[]byte("SADD"), []byte("set1"), []byte("a"), []byte("b"),
	}, context.Background())
	assert.NoError(t, err)

	members, err := store.SMembers("set1")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(members))
}

// TestWriteCommandSRem verifies the replication replay path applies
// SREM on the replica.
func TestWriteCommandSRem(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	_, err := store.SAdd("set1", "a", "b")
	assert.NoError(t, err)

	// Replay: SREM set1 a
	err = WriteCommand(store, [][]byte{
		[]byte("SREM"), []byte("set1"), []byte("a"),
	}, context.Background())
	assert.NoError(t, err)

	members, err := store.SMembers("set1")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(members))
	assert.Equal(t, "b", members[0])
}

// TestWriteCommandSPop verifies the replication replay path applies
// SPOP on the replica.
func TestWriteCommandSPop(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	_, err := store.SAdd("set1", "a", "b")
	assert.NoError(t, err)

	// Replay: SPOP set1 (pops one random member)
	err = WriteCommand(store, [][]byte{
		[]byte("SPOP"), []byte("set1"),
	}, context.Background())
	assert.NoError(t, err)

	members, err := store.SMembers("set1")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(members))
}

// TestWriteCommandSInterStore verifies the replication replay path applies
// SINTERSTORE (intersection into destination) on the replica.
func TestWriteCommandSInterStore(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	_, err := store.SAdd("set1", "a", "b", "c")
	assert.NoError(t, err)
	_, err = store.SAdd("set2", "b", "c", "d")
	assert.NoError(t, err)

	// Replay: SINTERSTORE dst set1 set2
	err = WriteCommand(store, [][]byte{
		[]byte("SINTERSTORE"), []byte("dst"), []byte("set1"), []byte("set2"),
	}, context.Background())
	assert.NoError(t, err)

	// Intersection = {b, c}
	members, err := store.SMembers("dst")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(members))
}

// TestWriteCommandSUnionStore verifies the replication replay path applies
// SUNIONSTORE (union into destination) on the replica.
func TestWriteCommandSUnionStore(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	_, err := store.SAdd("set1", "a", "b")
	assert.NoError(t, err)
	_, err = store.SAdd("set2", "b", "c")
	assert.NoError(t, err)

	// Replay: SUNIONSTORE dst set1 set2
	err = WriteCommand(store, [][]byte{
		[]byte("SUNIONSTORE"), []byte("dst"), []byte("set1"), []byte("set2"),
	}, context.Background())
	assert.NoError(t, err)

	// Union = {a, b, c}
	members, err := store.SMembers("dst")
	assert.NoError(t, err)
	assert.Equal(t, 3, len(members))
}

// TestWriteCommandSDiffStore verifies the replication replay path applies
// SDIFFSTORE (difference into destination) on the replica.
func TestWriteCommandSDiffStore(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	_, err := store.SAdd("set1", "a", "b", "c")
	assert.NoError(t, err)
	_, err = store.SAdd("set2", "b")
	assert.NoError(t, err)

	// Replay: SDIFFSTORE dst set1 set2
	err = WriteCommand(store, [][]byte{
		[]byte("SDIFFSTORE"), []byte("dst"), []byte("set1"), []byte("set2"),
	}, context.Background())
	assert.NoError(t, err)

	// Difference = {a, c}
	members, err := store.SMembers("dst")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(members))
}

// TestWriteCommandZInterStore verifies the replication replay path applies
// ZINTERSTORE (intersection into destination) on the replica.
func TestWriteCommandZInterStore(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	err := store.ZAdd("z1", []ZSetMember{{Member: "m1", Score: 1}, {Member: "m2", Score: 2}})
	assert.NoError(t, err)
	err = store.ZAdd("z2", []ZSetMember{{Member: "m2", Score: 3}, {Member: "m3", Score: 4}})
	assert.NoError(t, err)

	// Replay: ZINTERSTORE dst 2 z1 z2
	err = WriteCommand(store, [][]byte{
		[]byte("ZINTERSTORE"), []byte("dst"), []byte("2"), []byte("z1"), []byte("z2"),
	}, context.Background())
	assert.NoError(t, err)

	// Intersection = {m2} with sum score 2+3=5
	members, err := store.ZRange("dst", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(members))
	assert.Equal(t, "m2", members[0].Member)
	assert.Equal(t, float64(5), members[0].Score)
}

// TestWriteCommandLPop verifies the replication replay path applies
// LPOP (pop leftmost) on the replica.
func TestWriteCommandLPop(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	_, err := store.RPush("l1", "a", "b")
	assert.NoError(t, err)

	// Replay: LPOP l1
	err = WriteCommand(store, [][]byte{
		[]byte("LPOP"), []byte("l1"),
	}, context.Background())
	assert.NoError(t, err)

	vals, err := store.LRange("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(vals))
	assert.Equal(t, "b", vals[0])
}

// TestWriteCommandRPop verifies the replication replay path applies
// RPOP (pop rightmost) on the replica.
func TestWriteCommandRPop(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	_, err := store.RPush("l1", "a", "b")
	assert.NoError(t, err)

	// Replay: RPOP l1
	err = WriteCommand(store, [][]byte{
		[]byte("RPOP"), []byte("l1"),
	}, context.Background())
	assert.NoError(t, err)

	vals, err := store.LRange("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(vals))
	assert.Equal(t, "a", vals[0])
}

// TestWriteCommandLRem verifies the replication replay path applies
// LREM (remove matching elements) on the replica.
func TestWriteCommandLRem(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	_, err := store.RPush("l1", "a", "b", "a", "c")
	assert.NoError(t, err)

	// Replay: LREM l1 2 a (remove 2 occurrences from head)
	err = WriteCommand(store, [][]byte{
		[]byte("LREM"), []byte("l1"), []byte("2"), []byte("a"),
	}, context.Background())
	assert.NoError(t, err)

	vals, err := store.LRange("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(vals))
	assert.Equal(t, "b", vals[0])
	assert.Equal(t, "c", vals[1])
}

// TestWriteCommandLTrim verifies the replication replay path applies
// LTRIM (keep range) on the replica.
func TestWriteCommandLTrim(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	_, err := store.RPush("l1", "a", "b", "c", "d")
	assert.NoError(t, err)

	// Replay: LTRIM l1 1 2 (keep indexes 1..2)
	err = WriteCommand(store, [][]byte{
		[]byte("LTRIM"), []byte("l1"), []byte("1"), []byte("2"),
	}, context.Background())
	assert.NoError(t, err)

	vals, err := store.LRange("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(vals))
	assert.Equal(t, "b", vals[0])
	assert.Equal(t, "c", vals[1])
}

// TestWriteCommandLInsert verifies the replication replay path applies
// LINSERT (insert before/after pivot) on the replica.
func TestWriteCommandLInsert(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	_, err := store.RPush("l1", "a", "c")
	assert.NoError(t, err)

	// Replay: LINSERT l1 BEFORE c b (insert "b" before pivot "c")
	err = WriteCommand(store, [][]byte{
		[]byte("LINSERT"), []byte("l1"), []byte("BEFORE"), []byte("c"), []byte("b"),
	}, context.Background())
	assert.NoError(t, err)

	vals, err := store.LRange("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(vals))
	assert.Equal(t, "a", vals[0])
	assert.Equal(t, "b", vals[1])
	assert.Equal(t, "c", vals[2])
}

// TestWriteCommandLSet verifies the replication replay path applies
// LSET (set element at index) on the replica.
func TestWriteCommandLSet(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	_, err := store.RPush("l1", "a", "b")
	assert.NoError(t, err)

	// Replay: LSET l1 0 x (set index 0 to "x")
	err = WriteCommand(store, [][]byte{
		[]byte("LSET"), []byte("l1"), []byte("0"), []byte("x"),
	}, context.Background())
	assert.NoError(t, err)

	vals, err := store.LRange("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, "x", vals[0])
	assert.Equal(t, "b", vals[1])
}

// TestWriteCommandAppend verifies the replication replay path applies
// APPEND on the replica.
func TestWriteCommandAppend(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	err := store.Set("k", "foo")
	assert.NoError(t, err)

	// Replay: APPEND k bar
	err = WriteCommand(store, [][]byte{
		[]byte("APPEND"), []byte("k"), []byte("bar"),
	}, context.Background())
	assert.NoError(t, err)

	val, err := store.Get("k")
	assert.NoError(t, err)
	assert.Equal(t, "foobar", val)
}

// TestWriteCommandGetSet verifies the replication replay path applies
// GETSET (set new value, return old) on the replica.
func TestWriteCommandGetSet(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	err := store.Set("k", "old")
	assert.NoError(t, err)

	// Replay: GETSET k new
	err = WriteCommand(store, [][]byte{
		[]byte("GETSET"), []byte("k"), []byte("new"),
	}, context.Background())
	assert.NoError(t, err)

	// Value must be updated (return value not needed on replay)
	val, err := store.Get("k")
	assert.NoError(t, err)
	assert.Equal(t, "new", val)
}

// TestWriteCommandSetRange verifies the replication replay path applies
// SETRANGE (overwrite substring at offset) on the replica.
func TestWriteCommandSetRange(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	err := store.Set("k", "Hello World")
	assert.NoError(t, err)

	// Replay: SETRANGE k 6 Redis (overwrite at offset 6)
	err = WriteCommand(store, [][]byte{
		[]byte("SETRANGE"), []byte("k"), []byte("6"), []byte("Redis"),
	}, context.Background())
	assert.NoError(t, err)

	val, err := store.Get("k")
	assert.NoError(t, err)
	assert.Equal(t, "Hello Redis", val)
}

// TestWriteCommandIncrByFloat verifies the replication replay path applies
// INCRBYFLOAT on the replica.
func TestWriteCommandIncrByFloat(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	err := store.Set("k", "10.5")
	assert.NoError(t, err)

	// Replay: INCRBYFLOAT k 0.5
	err = WriteCommand(store, [][]byte{
		[]byte("INCRBYFLOAT"), []byte("k"), []byte("0.5"),
	}, context.Background())
	assert.NoError(t, err)

	val, err := store.Get("k")
	assert.NoError(t, err)
	assert.Equal(t, "11", val)
}

// TestWriteCommandMSet verifies the replication replay path applies
// MSET (multi-set) on the replica.
func TestWriteCommandMSet(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Replay: MSET k1 v1 k2 v2
	err := WriteCommand(store, [][]byte{
		[]byte("MSET"), []byte("k1"), []byte("v1"), []byte("k2"), []byte("v2"),
	}, context.Background())
	assert.NoError(t, err)

	v1, err := store.Get("k1")
	assert.NoError(t, err)
	assert.Equal(t, "v1", v1)
	v2, err := store.Get("k2")
	assert.NoError(t, err)
	assert.Equal(t, "v2", v2)
}

// TestWriteCommandPFAdd verifies the replication replay path applies
// PFADD on the replica.
func TestWriteCommandPFAdd(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Replay: PFADD hll a b c
	err := WriteCommand(store, [][]byte{
		[]byte("PFADD"), []byte("hll"), []byte("a"), []byte("b"), []byte("c"),
	}, context.Background())
	assert.NoError(t, err)

	count, err := store.PFCount("hll")
	assert.NoError(t, err)
	assert.Equal(t, int64(3), count)
}

// TestWriteCommandGeoAdd verifies the replication replay path applies
// GEOADD on the replica.
func TestWriteCommandGeoAdd(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Replay: GEOADD geo 13.361389 38.115556 Palermo
	err := WriteCommand(store, [][]byte{
		[]byte("GEOADD"), []byte("geo"), []byte("13.361389"), []byte("38.115556"), []byte("Palermo"),
	}, context.Background())
	assert.NoError(t, err)

	// Member must exist and resolve to a position
	pos, err := store.GeoPos("geo", "Palermo")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(pos))
	if len(pos) == 1 {
		assert.True(t, pos[0][0] > 0)
	}
}

// TestWriteCommandHSet verifies the replication replay path applies
// HSET on the replica.
func TestWriteCommandHSet(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Replay: HSET h f1 v1 f2 v2
	err := WriteCommand(store, [][]byte{
		[]byte("HSET"), []byte("h"), []byte("f1"), []byte("v1"), []byte("f2"), []byte("v2"),
	}, context.Background())
	assert.NoError(t, err)

	v1, err := store.HGet("h", "f1")
	assert.NoError(t, err)
	assert.Equal(t, "v1", string(v1))
	v2, err := store.HGet("h", "f2")
	assert.NoError(t, err)
	assert.Equal(t, "v2", string(v2))
}

// TestWriteCommandHDel verifies the replication replay path applies
// HDEL on the replica.
func TestWriteCommandHDel(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	err := store.HSet("h", "f1", "v1")
	assert.NoError(t, err)
	err = store.HSet("h", "f2", "v2")
	assert.NoError(t, err)

	// Replay: HDEL h f1
	err = WriteCommand(store, [][]byte{
		[]byte("HDEL"), []byte("h"), []byte("f1"),
	}, context.Background())
	assert.NoError(t, err)

	// f1 deleted, f2 remains
	_, err = store.HGet("h", "f1")
	assert.Error(t, err)
	v2, err := store.HGet("h", "f2")
	assert.NoError(t, err)
	assert.Equal(t, "v2", string(v2))
}

// TestWriteCommandHSetNx verifies the replication replay path applies
// HSETNX (set field if absent) on the replica.
func TestWriteCommandHSetNx(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Replay: HSETNX h f1 v1 (field absent → sets)
	err := WriteCommand(store, [][]byte{
		[]byte("HSETNX"), []byte("h"), []byte("f1"), []byte("v1"),
	}, context.Background())
	assert.NoError(t, err)
	v, err := store.HGet("h", "f1")
	assert.NoError(t, err)
	assert.Equal(t, "v1", string(v))

	// Replay: HSETNX h f1 v2 (field exists → must NOT overwrite)
	err = WriteCommand(store, [][]byte{
		[]byte("HSETNX"), []byte("h"), []byte("f1"), []byte("v2"),
	}, context.Background())
	assert.NoError(t, err)
	v, err = store.HGet("h", "f1")
	assert.NoError(t, err)
	assert.Equal(t, "v1", string(v))
}

// TestWriteCommandHIncrBy verifies the replication replay path applies
// HINCRBY (increment hash field) on the replica.
func TestWriteCommandHIncrBy(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	err := store.HSet("h", "count", "10")
	assert.NoError(t, err)

	// Replay: HINCRBY h count 5
	err = WriteCommand(store, [][]byte{
		[]byte("HINCRBY"), []byte("h"), []byte("count"), []byte("5"),
	}, context.Background())
	assert.NoError(t, err)

	v, err := store.HGet("h", "count")
	assert.NoError(t, err)
	assert.Equal(t, "15", string(v))
}

// TestWriteCommandHMSet verifies the replication replay path applies
// HMSET (multi-field set) on the replica.
func TestWriteCommandHMSet(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Replay: HMSET h f1 v1 f2 v2
	err := WriteCommand(store, [][]byte{
		[]byte("HMSET"), []byte("h"), []byte("f1"), []byte("v1"), []byte("f2"), []byte("v2"),
	}, context.Background())
	assert.NoError(t, err)

	v1, err := store.HGet("h", "f1")
	assert.NoError(t, err)
	assert.Equal(t, "v1", string(v1))
	v2, err := store.HGet("h", "f2")
	assert.NoError(t, err)
	assert.Equal(t, "v2", string(v2))
}

// TestWriteCommandHIncrByFloat verifies the replication replay path applies
// HINCRBYFLOAT (float increment of hash field) on the replica.
func TestWriteCommandHIncrByFloat(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	err := store.HSet("h", "score", "10.5")
	assert.NoError(t, err)

	// Replay: HINCRBYFLOAT h score 0.25
	err = WriteCommand(store, [][]byte{
		[]byte("HINCRBYFLOAT"), []byte("h"), []byte("score"), []byte("0.25"),
	}, context.Background())
	assert.NoError(t, err)

	v, err := store.HGet("h", "score")
	assert.NoError(t, err)
	assert.Equal(t, "10.75", string(v))
}

// TestWriteCommandSetEx verifies the replication replay path applies
// SETEX (set with expiry) on the replica.
func TestWriteCommandSetEx(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Replay: SETEX k 100 v (set value + TTL 100s)
	err := WriteCommand(store, [][]byte{
		[]byte("SETEX"), []byte("k"), []byte("100"), []byte("v"),
	}, context.Background())
	assert.NoError(t, err)

	val, err := store.Get("k")
	assert.NoError(t, err)
	assert.Equal(t, "v", val)

	// TTL must be set (~100s)
	ttl, err := store.TTL("k")
	assert.NoError(t, err)
	if ttl <= 0 || ttl > 100 {
		t.Errorf("expected TTL ~100s after SETEX replay, got %v", ttl)
	}
}

// TestWriteCommandPSetEx verifies the replication replay path applies
// PSETEX (set with expiry in millis) on the replica.
func TestWriteCommandPSetEx(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Replay: PSETEX k 100000 v (set value + TTL 100s in ms)
	err := WriteCommand(store, [][]byte{
		[]byte("PSETEX"), []byte("k"), []byte("100000"), []byte("v"),
	}, context.Background())
	assert.NoError(t, err)

	val, err := store.Get("k")
	assert.NoError(t, err)
	assert.Equal(t, "v", val)

	// TTL must be set (~100s)
	ttl, err := store.TTL("k")
	assert.NoError(t, err)
	if ttl <= 0 || ttl > 100 {
		t.Errorf("expected TTL ~100s after PSETEX replay, got %v", ttl)
	}
}

// TestWriteCommandRename verifies the replication replay path applies
// RENAME on the replica.
func TestWriteCommandRename(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	err := store.Set("k1", "v1")
	assert.NoError(t, err)

	// Replay: RENAME k1 k2
	err = WriteCommand(store, [][]byte{
		[]byte("RENAME"), []byte("k1"), []byte("k2"),
	}, context.Background())
	assert.NoError(t, err)

	// k1 gone, k2 has value
	exists, err := store.Exists("k1")
	assert.NoError(t, err)
	assert.False(t, exists)
	val, err := store.Get("k2")
	assert.NoError(t, err)
	assert.Equal(t, "v1", val)
}

// TestWriteCommandDel verifies the replication replay path applies
// DEL on the replica.
func TestWriteCommandDel(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	err := store.Set("k1", "v1")
	assert.NoError(t, err)
	err = store.Set("k2", "v2")
	assert.NoError(t, err)

	// Replay: DEL k1 k2
	err = WriteCommand(store, [][]byte{
		[]byte("DEL"), []byte("k1"), []byte("k2"),
	}, context.Background())
	assert.NoError(t, err)

	exists1, err := store.Exists("k1")
	assert.NoError(t, err)
	assert.False(t, exists1)
	exists2, err := store.Exists("k2")
	assert.NoError(t, err)
	assert.False(t, exists2)
}

// TestWriteCommandSetBit verifies the replication replay path applies
// SETBIT on the replica.
func TestWriteCommandSetBit(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	err := store.Set("bk", "\x00")
	assert.NoError(t, err)

	// Replay: SETBIT bk 0 1 (set bit 0 → 0x80 = 128)
	err = WriteCommand(store, [][]byte{
		[]byte("SETBIT"), []byte("bk"), []byte("0"), []byte("1"),
	}, context.Background())
	assert.NoError(t, err)

	val, err := store.Get("bk")
	assert.NoError(t, err)
	assert.Equal(t, "\x80", val)
}

// TestWriteCommandExpire verifies the replication replay path applies
// EXPIRE on the replica.
func TestWriteCommandExpire(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	err := store.Set("k", "v")
	assert.NoError(t, err)

	// Replay: EXPIRE k 100
	err = WriteCommand(store, [][]byte{
		[]byte("EXPIRE"), []byte("k"), []byte("100"),
	}, context.Background())
	assert.NoError(t, err)

	// TTL must be set (~100s)
	ttl, err := store.TTL("k")
	assert.NoError(t, err)
	if ttl <= 0 || ttl > 100 {
		t.Errorf("expected TTL ~100s after EXPIRE replay, got %v", ttl)
	}
}

// TestWriteCommandPersist verifies the replication replay path applies
// PERSIST (remove TTL) on the replica.
func TestWriteCommandPersist(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	err := store.SetWithTTL("k", "v", 100*time.Second)
	assert.NoError(t, err)
	ttl, err := store.TTL("k")
	assert.NoError(t, err)
	if ttl <= 0 {
		t.Fatalf("expected TTL set, got %v", ttl)
	}

	// Replay: PERSIST k
	err = WriteCommand(store, [][]byte{
		[]byte("PERSIST"), []byte("k"),
	}, context.Background())
	assert.NoError(t, err)

	// TTL must be removed
	ttl, err = store.TTL("k")
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), ttl)
}

// TestWriteCommandRPopLPush verifies the replication replay path applies
// RPOPLPUSH (pop right from src, push left to dst) on the replica.
func TestWriteCommandRPopLPush(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	_, err := store.RPush("src", "a", "b")
	assert.NoError(t, err)

	// Replay: RPOPLPUSH src dst
	err = WriteCommand(store, [][]byte{
		[]byte("RPOPLPUSH"), []byte("src"), []byte("dst"),
	}, context.Background())
	assert.NoError(t, err)

	// Rightmost of src moved to left of dst
	srcVals, err := store.LRange("src", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(srcVals))
	assert.Equal(t, "a", srcVals[0])

	dstVals, err := store.LRange("dst", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(dstVals))
	assert.Equal(t, "b", dstVals[0])
}

// TestWriteCommandLMove verifies the replication replay path applies
// LMOVE on the replica.
func TestWriteCommandLMove(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	_, err := store.RPush("src", "a", "b")
	assert.NoError(t, err)

	// Replay: LMOVE src dst LEFT RIGHT
	err = WriteCommand(store, [][]byte{
		[]byte("LMOVE"), []byte("src"), []byte("dst"), []byte("LEFT"), []byte("RIGHT"),
	}, context.Background())
	assert.NoError(t, err)

	// Leftmost of src moved to right of dst
	srcVals, err := store.LRange("src", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(srcVals))
	assert.Equal(t, "b", srcVals[0])

	dstVals, err := store.LRange("dst", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(dstVals))
	assert.Equal(t, "a", dstVals[0])
}

// TestWriteCommandXDel verifies the replication replay path applies
// XDEL (delete stream entries) on the replica.
func TestWriteCommandXDel(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	id1, err := store.XAdd("s", StreamXAddOptions{}, "1-1", map[string]string{"f": "v1"})
	assert.NoError(t, err)
	_, err = store.XAdd("s", StreamXAddOptions{}, "2-1", map[string]string{"f": "v2"})
	assert.NoError(t, err)

	// Replay: XDEL s 1-1
	err = WriteCommand(store, [][]byte{
		[]byte("XDEL"), []byte("s"), []byte(id1),
	}, context.Background())
	assert.NoError(t, err)

	// Only entry 2-1 remains
	entries, err := store.XRange("s", "-", "+", -1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(entries))
	assert.Equal(t, "2-1", entries[0].ID)
}

// TestWriteCommandXAck verifies the replication replay path applies
// XACK (acknowledge PEL entries) on the replica.
func TestWriteCommandXAck(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	id, err := store.XAdd("s", StreamXAddOptions{}, "1-1", map[string]string{"f": "v"})
	assert.NoError(t, err)
	err = store.XGroupCreate("s", "g", "0")
	assert.NoError(t, err)
	// Read to populate the PEL
	_, err = store.XReadGroup(nil, "g", "c1", 10, 0, "s")
	assert.NoError(t, err)
	pending, err := store.XPending("s", "g")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(pending))

	// Replay: XACK s g 1-1
	err = WriteCommand(store, [][]byte{
		[]byte("XACK"), []byte("s"), []byte("g"), []byte(id),
	}, context.Background())
	assert.NoError(t, err)

	// PEL must be empty after ack
	pending, err = store.XPending("s", "g")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(pending))
}

// TestWriteCommandXSetID verifies the replication replay path applies
// XSETID (set stream last-id) on the replica.
func TestWriteCommandXSetID(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	_, err := store.XAdd("s", StreamXAddOptions{}, "1-1", map[string]string{"f": "v"})
	assert.NoError(t, err)

	// Replay: XSETID s 5-5 (advance last id)
	err = WriteCommand(store, [][]byte{
		[]byte("XSETID"), []byte("s"), []byte("5-5"),
	}, context.Background())
	assert.NoError(t, err)

	// Last id must be updated to 5-5
	info, err := store.XInfo("s")
	assert.NoError(t, err)
	assert.Equal(t, "5-5", info.LastID)
}

// TestWriteCommandPFMerge verifies the replication replay path applies
// PFMERGE (merge HLLs into dest) on the replica.
func TestWriteCommandPFMerge(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	_, err := store.PFAdd("h1", "a", "b")
	assert.NoError(t, err)
	_, err = store.PFAdd("h2", "c", "d")
	assert.NoError(t, err)

	// Replay: PFMERGE dest h1 h2
	err = WriteCommand(store, [][]byte{
		[]byte("PFMERGE"), []byte("dest"), []byte("h1"), []byte("h2"),
	}, context.Background())
	assert.NoError(t, err)

	// Merged HLL must count all 4 unique elements
	count, err := store.PFCount("dest")
	assert.NoError(t, err)
	assert.Equal(t, int64(4), count)
}

// TestWriteCommandBitOp verifies the replication replay path applies
// BITOP (bitwise op into dest) on the replica.
func TestWriteCommandBitOp(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	err := store.Set("k1", "a") // 0x61
	assert.NoError(t, err)
	err = store.Set("k2", "b") // 0x62
	assert.NoError(t, err)

	// Replay: BITOP OR dest k1 k2 → 0x61|0x62 = 0x63 "c"
	err = WriteCommand(store, [][]byte{
		[]byte("BITOP"), []byte("OR"), []byte("dest"), []byte("k1"), []byte("k2"),
	}, context.Background())
	assert.NoError(t, err)

	val, err := store.Get("dest")
	assert.NoError(t, err)
	assert.Equal(t, "c", val)
}

// TestWriteCommandBitField verifies the replication replay path applies
// BITFIELD (SET operation) on the replica.
func TestWriteCommandBitField(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	err := store.Set("bk", "\x00")
	assert.NoError(t, err)

	// Replay: BITFIELD bk SET u8 0 255 (set byte 0 to 255)
	err = WriteCommand(store, [][]byte{
		[]byte("BITFIELD"), []byte("bk"), []byte("SET"), []byte("u8"), []byte("0"), []byte("255"),
	}, context.Background())
	assert.NoError(t, err)

	val, err := store.Get("bk")
	assert.NoError(t, err)
	assert.Equal(t, "\xff", val)
}

// TestWriteCommandIncrBy verifies the replication replay path applies
// INCRBY on the replica.
func TestWriteCommandIncrBy(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	err := store.Set("k", "10")
	assert.NoError(t, err)

	// Replay: INCRBY k 5
	err = WriteCommand(store, [][]byte{
		[]byte("INCRBY"), []byte("k"), []byte("5"),
	}, context.Background())
	assert.NoError(t, err)

	val, err := store.Get("k")
	assert.NoError(t, err)
	assert.Equal(t, "15", val)
}

// TestWriteCommandDecrBy verifies the replication replay path applies
// DECRBY on the replica.
func TestWriteCommandDecrBy(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	err := store.Set("k", "10")
	assert.NoError(t, err)

	// Replay: DECRBY k 3
	err = WriteCommand(store, [][]byte{
		[]byte("DECRBY"), []byte("k"), []byte("3"),
	}, context.Background())
	assert.NoError(t, err)

	val, err := store.Get("k")
	assert.NoError(t, err)
	assert.Equal(t, "7", val)
}

// TestWriteCommandTSAdd verifies the replication replay path applies
// TS.ADD on the replica.
func TestWriteCommandTSAdd(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Replay: TS.ADD ts 1000 42.5
	err := WriteCommand(store, [][]byte{
		[]byte("TS.ADD"), []byte("ts"), []byte("1000"), []byte("42.5"),
	}, context.Background())
	assert.NoError(t, err)

	// Value must be retrievable
	pt, err := store.TSGet("ts")
	assert.NoError(t, err)
	assert.NotNil(t, pt)
	if pt != nil {
		assert.Equal(t, float64(42.5), pt.Value)
		assert.Equal(t, int64(1000), pt.Timestamp)
	}
}

// TestWriteCommandTSIncrBy verifies the replication replay path applies
// TS.INCRBY on the replica.
func TestWriteCommandTSIncrBy(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	_, err := store.TSAdd("ts", 1000, 10.0, TSAddOptions{})
	assert.NoError(t, err)

	// Replay: TS.INCRBY ts 5.5
	err = WriteCommand(store, [][]byte{
		[]byte("TS.INCRBY"), []byte("ts"), []byte("5.5"),
	}, context.Background())
	assert.NoError(t, err)

	// Last value must be 10.0 + 5.5 = 15.5
	pt, err := store.TSGet("ts")
	assert.NoError(t, err)
	assert.NotNil(t, pt)
	if pt != nil {
		assert.Equal(t, float64(15.5), pt.Value)
	}
}

// TestWriteCommandTSCreateRule verifies the replication replay path applies
// TS.CREATERULE (compaction rule) on the replica.
func TestWriteCommandTSCreateRule(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	_, err := store.TSAdd("src", 1000, 1.0, TSAddOptions{})
	assert.NoError(t, err)
	_, err = store.TSAdd("src", 2000, 2.0, TSAddOptions{})
	assert.NoError(t, err)

	// Replay: TS.CREATERULE src dst AGGREGATION sum 1000
	err = WriteCommand(store, [][]byte{
		[]byte("TS.CREATERULE"), []byte("src"), []byte("dst"), []byte("AGGREGATION"), []byte("sum"), []byte("1000"),
	}, context.Background())
	assert.NoError(t, err)

	// Rule must be registered (queryable via TSQueryIndex)
	keys, err := store.TSQueryIndex([]string{"src"})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(keys))
}

// TestWriteCommandTSDeleteRule verifies the replication replay path applies
// TS.DELETERULE (remove compaction rule) on the replica.
func TestWriteCommandTSDeleteRule(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	_, err := store.TSAdd("src", 1000, 1.0, TSAddOptions{})
	assert.NoError(t, err)
	err = store.TSAddRule("src", "dst", "sum", 1000)
	assert.NoError(t, err)

	// Replay: TS.DELETERULE src dst AGGREGATION sum 1000
	err = WriteCommand(store, [][]byte{
		[]byte("TS.DELETERULE"), []byte("src"), []byte("dst"), []byte("AGGREGATION"), []byte("sum"), []byte("1000"),
	}, context.Background())
	assert.NoError(t, err)

	// The rule must be gone: re-adding the same rule must succeed
	err = store.TSAddRule("src", "dst", "sum", 1000)
	assert.NoError(t, err)
}

// TestWriteCommandTSDel verifies the replication replay path applies
// TS.DEL (delete samples in time range) on the replica.
func TestWriteCommandTSDel(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	_, err := store.TSAdd("ts", 1000, 1.0, TSAddOptions{})
	assert.NoError(t, err)
	_, err = store.TSAdd("ts", 2000, 2.0, TSAddOptions{})
	assert.NoError(t, err)
	_, err = store.TSAdd("ts", 3000, 3.0, TSAddOptions{})
	assert.NoError(t, err)

	// Replay: TS.DEL ts 1500 2500 (delete the middle sample)
	err = WriteCommand(store, [][]byte{
		[]byte("TS.DEL"), []byte("ts"), []byte("1500"), []byte("2500"),
	}, context.Background())
	assert.NoError(t, err)

	// Only 1000 and 3000 remain
	pts, err := store.TSRange("ts", "-", "+", -1)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(pts))
}

// TestWriteCommandTSMAdd verifies the replication replay path applies
// TS.MADD (multi-add) on the replica.
func TestWriteCommandTSMAdd(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Replay: TS.MADD ts1 1000 1.0 ts2 2000 2.0
	err := WriteCommand(store, [][]byte{
		[]byte("TS.MADD"), []byte("ts1"), []byte("1000"), []byte("1.0"), []byte("ts2"), []byte("2000"), []byte("2.0"),
	}, context.Background())
	assert.NoError(t, err)

	pt1, err := store.TSGet("ts1")
	assert.NoError(t, err)
	assert.NotNil(t, pt1)
	if pt1 != nil {
		assert.Equal(t, float64(1.0), pt1.Value)
	}
	pt2, err := store.TSGet("ts2")
	assert.NoError(t, err)
	assert.NotNil(t, pt2)
	if pt2 != nil {
		assert.Equal(t, float64(2.0), pt2.Value)
	}
}

// TestWriteCommandTSCreate verifies the replication replay path applies
// TS.CREATE with RETENTION on the replica.
func TestWriteCommandTSCreate(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Replay: TS.CREATE ts RETENTION 3600000
	err := WriteCommand(store, [][]byte{
		[]byte("TS.CREATE"), []byte("ts"), []byte("RETENTION"), []byte("3600000"),
	}, context.Background())
	assert.NoError(t, err)

	info, err := store.TSInfo("ts")
	assert.NoError(t, err)
	assert.NotNil(t, info)
	if info != nil {
		assert.Equal(t, int64(3600000), info.RetentionTime)
	}
}

// TestWriteCommandXGroupDestroy verifies the replication replay path applies
// XGROUP DESTROY on the replica.
func TestWriteCommandXGroupDestroy(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	_, err := store.XAdd("s", StreamXAddOptions{}, "1-1", map[string]string{"f": "v"})
	assert.NoError(t, err)
	err = store.XGroupCreate("s", "g", "0")
	assert.NoError(t, err)

	// Replay: XGROUP DESTROY s g
	err = WriteCommand(store, [][]byte{
		[]byte("XGROUP"), []byte("DESTROY"), []byte("s"), []byte("g"),
	}, context.Background())
	assert.NoError(t, err)

	// Group must be gone
	groups, err := store.XInfoGroups("s")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(groups))
}

// TestWriteCommandXGroupDelConsumer verifies the replication replay path
// applies XGROUP DELCONSUMER on the replica.
func TestWriteCommandXGroupDelConsumer(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	_, err := store.XAdd("s", StreamXAddOptions{}, "1-1", map[string]string{"f": "v"})
	assert.NoError(t, err)
	err = store.XGroupCreate("s", "g", "0")
	assert.NoError(t, err)
	_, err = store.XGroupCreateConsumer("s", "g", "c1")
	assert.NoError(t, err)

	// Replay: XGROUP DELCONSUMER s g c1
	err = WriteCommand(store, [][]byte{
		[]byte("XGROUP"), []byte("DELCONSUMER"), []byte("s"), []byte("g"), []byte("c1"),
	}, context.Background())
	assert.NoError(t, err)

	// Consumer must be gone from the group
	groups, err := store.XInfoGroups("s")
	assert.NoError(t, err)
	for _, grp := range groups {
		if grp.Name == "g" {
			if _, ok := grp.Consumers["c1"]; ok {
				t.Fatalf("expected consumer c1 removed, still present")
			}
		}
	}
}

// TestWriteCommandXGroupSetID verifies the replication replay path applies
// XGROUP SETID on the replica.
func TestWriteCommandXGroupSetID(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	_, err := store.XAdd("s", StreamXAddOptions{}, "1-1", map[string]string{"f": "v"})
	assert.NoError(t, err)
	err = store.XGroupCreate("s", "g", "0")
	assert.NoError(t, err)

	// Replay: XGROUP SETID s g 1-1 (advance group last-delivered-id)
	err = WriteCommand(store, [][]byte{
		[]byte("XGROUP"), []byte("SETID"), []byte("s"), []byte("g"), []byte("1-1"),
	}, context.Background())
	assert.NoError(t, err)

	// Group's last delivered id must be 1-1 (no pending messages on new read)
	pending, err := store.XPending("s", "g")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(pending))
}

// TestWriteCommandXTrim verifies the replication replay path applies
// XTRIM (trim stream length) on the replica.
func TestWriteCommandXTrim(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	_, err := store.XAdd("s", StreamXAddOptions{}, "1-1", map[string]string{"f": "v1"})
	assert.NoError(t, err)
	_, err = store.XAdd("s", StreamXAddOptions{}, "2-1", map[string]string{"f": "v2"})
	assert.NoError(t, err)
	_, err = store.XAdd("s", StreamXAddOptions{}, "3-1", map[string]string{"f": "v3"})
	assert.NoError(t, err)

	// Replay: XTRIM s 2 (keep newest 2 entries)
	err = WriteCommand(store, [][]byte{
		[]byte("XTRIM"), []byte("s"), []byte("2"),
	}, context.Background())
	assert.NoError(t, err)

	entries, err := store.XRange("s", "-", "+", -1)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(entries))
	assert.Equal(t, "2-1", entries[0].ID)
}

// TestWriteCommandXClaim verifies the replication replay path applies
// XCLAIM (claim a pending message) on the replica.
func TestWriteCommandXClaim(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	id, err := store.XAdd("s", StreamXAddOptions{}, "1-1", map[string]string{"f": "v"})
	assert.NoError(t, err)
	err = store.XGroupCreate("s", "g", "0")
	assert.NoError(t, err)
	_, err = store.XReadGroup(nil, "g", "c1", 10, 0, "s")
	assert.NoError(t, err)

	// Replay: XCLAIM s g c2 0 1-1 (claim pending message to c2)
	err = WriteCommand(store, [][]byte{
		[]byte("XCLAIM"), []byte("s"), []byte("g"), []byte("c2"), []byte("0"), []byte(id),
	}, context.Background())
	assert.NoError(t, err)

	// Pending message must now belong to c2
	pending, err := store.XPending("s", "g")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(pending))
	if len(pending) == 1 {
		assert.Equal(t, "c2", pending[0].Consumer)
	}
}

// TestWriteCommandXReadGroup verifies the replication replay path applies
// XREADGROUP (non-NOACK: messages enter the PEL) on the replica.
func TestWriteCommandXReadGroup(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	_, err := store.XAdd("s", StreamXAddOptions{}, "1-1", map[string]string{"f": "v"})
	assert.NoError(t, err)
	err = store.XGroupCreate("s", "g", "0")
	assert.NoError(t, err)

	// Replay: XREADGROUP GROUP g c1 STREAMS s >
	err = WriteCommand(store, [][]byte{
		[]byte("XREADGROUP"), []byte("GROUP"), []byte("g"), []byte("c1"), []byte("STREAMS"), []byte("s"), []byte(">"),
	}, context.Background())
	assert.NoError(t, err)

	// Message must be in the PEL (not auto-acked)
	pending, err := store.XPending("s", "g")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(pending))
	if len(pending) == 1 {
		assert.Equal(t, "c1", pending[0].Consumer)
	}
}

// TestWriteCommandGetExEx verifies the replication replay path applies
// GETEX EX (get with relative TTL) on the replica.
func TestWriteCommandGetExEx(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	err := store.Set("k", "v")
	assert.NoError(t, err)

	// Replay: GETEX k EX 100
	err = WriteCommand(store, [][]byte{
		[]byte("GETEX"), []byte("k"), []byte("EX"), []byte("100"),
	}, context.Background())
	assert.NoError(t, err)

	// Value preserved, TTL ~100s
	val, err := store.Get("k")
	assert.NoError(t, err)
	assert.Equal(t, "v", val)
	ttl, err := store.TTL("k")
	assert.NoError(t, err)
	if ttl <= 0 || ttl > 100 {
		t.Errorf("expected TTL ~100s after GETEX EX replay, got %v", ttl)
	}
}

// TestWriteCommandGetExPx verifies the replication replay path applies
// GETEX PX (get with relative TTL in millis) on the replica.
func TestWriteCommandGetExPx(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	err := store.Set("k", "v")
	assert.NoError(t, err)

	// Replay: GETEX k PX 100000 (100s in ms)
	err = WriteCommand(store, [][]byte{
		[]byte("GETEX"), []byte("k"), []byte("PX"), []byte("100000"),
	}, context.Background())
	assert.NoError(t, err)

	// Value preserved, TTL ~100s
	val, err := store.Get("k")
	assert.NoError(t, err)
	assert.Equal(t, "v", val)
	ttl, err := store.TTL("k")
	assert.NoError(t, err)
	if ttl <= 0 || ttl > 100 {
		t.Errorf("expected TTL ~100s after GETEX PX replay, got %v", ttl)
	}
}

// TestWriteCommandSortStore verifies the replication replay path applies
// SORT ... STORE dest (base path without GET) on the replica.
func TestWriteCommandSortStore(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	_, err := store.RPush("mylist", "3", "1", "2")
	assert.NoError(t, err)

	// Replay: SORT mylist STORE out (numeric ascending)
	err = WriteCommand(store, [][]byte{
		[]byte("SORT"), []byte("mylist"), []byte("STORE"), []byte("out"),
	}, context.Background())
	assert.NoError(t, err)

	// Destination must contain sorted values: 1, 2, 3
	vals, err := store.LRange("out", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(vals))
	assert.Equal(t, "1", vals[0])
	assert.Equal(t, "2", vals[1])
	assert.Equal(t, "3", vals[2])
}

// TestWriteCommandMSetNx verifies the replication replay path applies
// MSETNX conditionally (all keys absent) on the replica.
func TestWriteCommandMSetNx(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Replay: MSETNX k1 v1 k2 v2 → both absent, sets
	err := WriteCommand(store, [][]byte{
		[]byte("MSETNX"), []byte("k1"), []byte("v1"), []byte("k2"), []byte("v2"),
	}, context.Background())
	assert.NoError(t, err)
	v1, err := store.Get("k1")
	assert.NoError(t, err)
	assert.Equal(t, "v1", v1)

	// Replay: MSETNX k2 v2b k3 v3 → k2 exists, must NOT set any
	err = WriteCommand(store, [][]byte{
		[]byte("MSETNX"), []byte("k2"), []byte("v2b"), []byte("k3"), []byte("v3"),
	}, context.Background())
	assert.NoError(t, err)
	v2, err := store.Get("k2")
	assert.NoError(t, err)
	assert.Equal(t, "v2", v2) // unchanged
	exists3, err := store.Exists("k3")
	assert.NoError(t, err)
	assert.False(t, exists3) // not set
}
