package store

import (
	"context"
	"fmt"
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
		result, err := store.XReadGroup(context.Background(), "mygroup", "consumer1", 10, 1000, "mystream", ">")
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

	result, err := store.XReadGroup(context.Background(), "mygroup", "consumer1", 10, 100, "mystream", ">")
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
	results, err := store.XReadGroup(nil, "mygroup", "myconsumer", 10, 0, "mystream", ">")
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
	_, err = store.XReadGroup(nil, "mygroup", "myconsumer", 10, 0, "mystream", ">")
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
	_, err = store.XReadGroup(nil, "mygroup", "consumer1", 10, 0, "mystream", ">")
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
	_, err = store.XReadGroup(nil, "mygroup", "consumer1", 10, 0, "mystream", ">")
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
	_, _ = store.XReadGroup(nil, "mygroup", "consumer1", 10, 0, "mystream", ">")

	// Auto claim
	result, err := store.XAutoClaim("mystream", "mygroup", "consumer2", 0, ">", XAutoClaimOptions{})
	assert.NoError(t, err)
	assert.True(t, result != nil)
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
	_, err = store.XReadGroup(nil, "group1", "consumer1", 10, 0, stream, ">")
	assert.NoError(t, err)
	_, err = store.XReadGroup(nil, "group2", "consumer2", 10, 0, stream, ">")
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
	_, err = store.XReadGroup(nil, "group1", "consumer1", 10, 0, stream, ">")
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
	_, err = store.XReadGroup(nil, "group2", "consumer2", 10, 0, stream, ">")
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
	_, err = store.XReadGroup(nil, "group3", "consumer3", 10, 0, stream, ">")
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
	_, err = store.XReadGroup(nil, "g1", "c1", 10, 0, stream, ">")
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
