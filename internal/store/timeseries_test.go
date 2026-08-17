package store

import (
	"strconv"
	"testing"
	"time"

	"github.com/zeebo/assert"
)

func TestTSCreate(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	var err error

	// Test basic creation
	err = s.TSCreate("ts1", TSCreateOptions{})
	assert.NoError(t, err)

	// Verify type
	keyType, err := s.Type("ts1")
	assert.NoError(t, err)
	assert.Equal(t, "ts", keyType)

	// Test creation with retention
	err = s.TSCreate("ts2", TSCreateOptions{Retention: 3600000})
	assert.NoError(t, err)

	// Test creation with encoding
	err = s.TSCreate("ts3", TSCreateOptions{Encoding: "compressed"})
	assert.NoError(t, err)

	// Test duplicate key
	err = s.TSCreate("ts1", TSCreateOptions{})
	assert.Error(t, err)
}

func TestTSAdd(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	var err error

	// Add data points
	ts1, err := s.TSAdd("ts1", time.Now().UnixNano()/int64(time.Millisecond), 25.5, TSAddOptions{})
	assert.NoError(t, err)
	assert.NotEqual(t, int64(0), ts1)

	ts2, err := s.TSAdd("ts1", ts1+1000, 30.0, TSAddOptions{})
	assert.NoError(t, err)
	assert.NotEqual(t, ts1, ts2)

	// Test auto-creation if key doesn't exist
	ts3, err := s.TSAdd("ts2", time.Now().UnixNano()/int64(time.Millisecond), 100.0, TSAddOptions{})
	assert.NoError(t, err)
	assert.NotEqual(t, int64(0), ts3)

	// Verify type
	keyType, err := s.Type("ts2")
	assert.NoError(t, err)
	assert.Equal(t, "ts", keyType)
}

func TestTSGet(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	var err error

	// Add data points
	now := time.Now().UnixNano() / int64(time.Millisecond)
	_, err = s.TSAdd("ts1", now, 25.5, TSAddOptions{})
	assert.NoError(t, err)
	_, err = s.TSAdd("ts1", now+1000, 30.0, TSAddOptions{})
	assert.NoError(t, err)
	_, err = s.TSAdd("ts1", now+2000, 35.0, TSAddOptions{})
	assert.NoError(t, err)

	// Get should return last value
	dp, err := s.TSGet("ts1")
	assert.NoError(t, err)
	assert.NotNil(t, dp)
	assert.Equal(t, now+2000, dp.Timestamp)
	assert.Equal(t, 35.0, dp.Value)

	// Get non-existent key
	dp, err = s.TSGet("nonexistent")
	assert.Error(t, err)
	assert.Nil(t, dp)
}

func TestTSRange(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	var err error

	now := time.Now().UnixNano() / int64(time.Millisecond)
	// Add data points
	s.TSAdd("ts1", now, 10.0, TSAddOptions{})
	s.TSAdd("ts1", now+1000, 20.0, TSAddOptions{})
	s.TSAdd("ts1", now+2000, 30.0, TSAddOptions{})
	s.TSAdd("ts1", now+3000, 40.0, TSAddOptions{})
	s.TSAdd("ts1", now+4000, 50.0, TSAddOptions{})

	// Get all
	results, err := s.TSRange("ts1", "-", "+", -1)
	assert.NoError(t, err)
	assert.Equal(t, 5, len(results))

	// Get range
	results, err = s.TSRange("ts1", strconv.FormatInt(now+1000, 10), strconv.FormatInt(now+3000, 10), -1)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(results))
	assert.Equal(t, 20.0, results[0].Value)
	assert.Equal(t, 30.0, results[1].Value)
	assert.Equal(t, 40.0, results[2].Value)

	// Get with count
	results, err = s.TSRange("ts1", "-", "+", 2)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(results))

	// Get non-existent key returns empty array
	results, err = s.TSRange("nonexistent", "-", "+", -1)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(results))
}

func TestTSDel(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	var err error

	now := time.Now().UnixNano() / int64(time.Millisecond)
	// Add data points
	s.TSAdd("ts1", now, 10.0, TSAddOptions{})
	s.TSAdd("ts1", now+1000, 20.0, TSAddOptions{})
	s.TSAdd("ts1", now+2000, 30.0, TSAddOptions{})
	s.TSAdd("ts1", now+3000, 40.0, TSAddOptions{})

	// Delete range
	deleted, err := s.TSDel("ts1", strconv.FormatInt(now+1000, 10), strconv.FormatInt(now+2000, 10))
	assert.NoError(t, err)
	assert.Equal(t, int64(2), deleted)

	// Verify deletion
	results, err := s.TSRange("ts1", "-", "+", -1)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(results))

	// Delete non-existent range
	deleted, err = s.TSDel("ts1", strconv.FormatInt(now+10000, 10), strconv.FormatInt(now+20000, 10))
	assert.NoError(t, err)
	assert.Equal(t, int64(0), deleted)

	// Delete non-existent key
	deleted, err = s.TSDel("nonexistent", "-", "+")
	assert.Error(t, err)
}

func TestTSInfo(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	var err error

	now := time.Now().UnixNano() / int64(time.Millisecond)
	s.TSAdd("ts1", now, 10.0, TSAddOptions{})
	s.TSAdd("ts1", now+1000, 20.0, TSAddOptions{})
	s.TSAdd("ts1", now+2000, 30.0, TSAddOptions{})

	info, err := s.TSInfo("ts1")
	assert.NoError(t, err)
	assert.NotNil(t, info)
	assert.Equal(t, int64(3), info.TotalSamples)
	assert.Equal(t, now, info.FirstTimestamp)
	assert.Equal(t, now+2000, info.LastTimestamp)
	assert.Equal(t, "compressed", info.Encoding)

	// Non-existent key
	info, err = s.TSInfo("nonexistent")
	assert.Error(t, err)
}

func TestTSLen(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	var err error

	now := time.Now().UnixNano() / int64(time.Millisecond)
	s.TSAdd("ts1", now, 10.0, TSAddOptions{})
	s.TSAdd("ts1", now+1000, 20.0, TSAddOptions{})
	s.TSAdd("ts1", now+2000, 30.0, TSAddOptions{})

	length, err := s.TSLen("ts1")
	assert.NoError(t, err)
	assert.Equal(t, int64(3), length)

	// Non-existent key
	length, err = s.TSLen("nonexistent")
	assert.Error(t, err)
}

func TestTSMGet(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	var err error

	now := time.Now().UnixNano() / int64(time.Millisecond)
	s.TSAdd("ts1", now, 10.0, TSAddOptions{})
	s.TSAdd("ts1", now+1000, 20.0, TSAddOptions{})
	s.TSAdd("ts2", now, 30.0, TSAddOptions{})

	results, err := s.TSMGet("*", "ts1", "ts2", "nonexistent")
	assert.NoError(t, err)
	assert.Equal(t, 3, len(results))

	assert.NotNil(t, results[0])
	assert.Equal(t, 20.0, results[0].Value)

	assert.NotNil(t, results[1])
	assert.Equal(t, 30.0, results[1].Value)

	assert.Nil(t, results[2])
}

func TestTSDuplicatePolicy(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	var err error

	now := time.Now().UnixNano() / int64(time.Millisecond)
	timestamp := now

	// Add first value
	_, err = s.TSAdd("ts1", timestamp, 10.0, TSAddOptions{OnDuplicate: "block"})
	assert.NoError(t, err)

	// Try to add duplicate with block policy
	_, err = s.TSAdd("ts1", timestamp, 20.0, TSAddOptions{OnDuplicate: "block"})
	assert.Error(t, err)

	// Add duplicate with skip policy
	_, err = s.TSAdd("ts1", timestamp, 30.0, TSAddOptions{OnDuplicate: "skip"})
	assert.NoError(t, err)

	// Verify value wasn't changed
	dp, err := s.TSGet("ts1")
	assert.NoError(t, err)
	assert.Equal(t, 10.0, dp.Value)

	// Add duplicate with update policy
	_, err = s.TSAdd("ts1", timestamp, 40.0, TSAddOptions{OnDuplicate: "update"})
	assert.NoError(t, err)

	// Verify value was updated
	dp, err = s.TSGet("ts1")
	assert.NoError(t, err)
	assert.Equal(t, 40.0, dp.Value)
}

func TestTSType(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	var err error

	now := time.Now().UnixNano() / int64(time.Millisecond)
	s.TSAdd("ts1", now, 10.0, TSAddOptions{})

	// Check if key is a time series
	isTS, err := s.TimeSeriesType("ts1")
	assert.NoError(t, err)
	assert.Equal(t, true, isTS)

	// Check non-existent key
	isTS, err = s.TimeSeriesType("nonexistent")
	assert.NoError(t, err)
	assert.Equal(t, false, isTS)
}

func TestTSAutoTimestamp(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	var err error

	now := time.Now().UnixNano() / int64(time.Millisecond)

	// Add with auto timestamp
	ts1, err := s.TSAdd("ts1", now, 10.0, TSAddOptions{})
	assert.NoError(t, err)
	assert.NotEqual(t, int64(0), ts1)
}

// TestTSAddWrongType tests that TSAdd returns ErrWrongType when key exists with different type
func TestTSAddWrongType(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	var err error

	// Create a string key
	err = s.Set("mystring", "value")
	assert.NoError(t, err)

	// Verify it's a string
	keyType, err := s.Type("mystring")
	assert.NoError(t, err)
	assert.Equal(t, "string", keyType)

	// Try to TSAdd to the string key - should return ErrWrongType
	_, err = s.TSAdd("mystring", time.Now().UnixNano()/int64(time.Millisecond), 10.0, TSAddOptions{})
	assert.Error(t, err)
	assert.Equal(t, ErrWrongType, err)

	// Create a list key
	_, err = s.LPush("mylist", "value1")
	assert.NoError(t, err)

	// Verify it's a list
	keyType, err = s.Type("mylist")
	assert.NoError(t, err)
	assert.Equal(t, "list", keyType)

	// Try to TSAdd to the list key - should return ErrWrongType
	_, err = s.TSAdd("mylist", time.Now().UnixNano()/int64(time.Millisecond), 10.0, TSAddOptions{})
	assert.Error(t, err)
	assert.Equal(t, ErrWrongType, err)

	// Create a hash key
	err = s.HSet("myhash", "field1", "value1")
	assert.NoError(t, err)

	// Verify it's a hash
	keyType, err = s.Type("myhash")
	assert.NoError(t, err)
	assert.Equal(t, "hash", keyType)

	// Try to TSAdd to the hash key - should return ErrWrongType
	_, err = s.TSAdd("myhash", time.Now().UnixNano()/int64(time.Millisecond), 10.0, TSAddOptions{})
	assert.Error(t, err)
	assert.Equal(t, ErrWrongType, err)

	// Create a set key
	_, err = s.SAdd("myset", "member1")
	assert.NoError(t, err)

	// Verify it's a set
	keyType, err = s.Type("myset")
	assert.NoError(t, err)
	assert.Equal(t, "set", keyType)

	// Try to TSAdd to the set key - should return ErrWrongType
	_, err = s.TSAdd("myset", time.Now().UnixNano()/int64(time.Millisecond), 10.0, TSAddOptions{})
	assert.Error(t, err)
	assert.Equal(t, ErrWrongType, err)

	// Create a sorted set key
	err = s.ZAdd("myzset", []ZSetMember{{Member: "member1", Score: 1.0}})
	assert.NoError(t, err)

	// Verify it's a zset
	keyType, err = s.Type("myzset")
	assert.NoError(t, err)
	assert.Equal(t, "zset", keyType)

	// Try to TSAdd to the zset key - should return ErrWrongType
	_, err = s.TSAdd("myzset", time.Now().UnixNano()/int64(time.Millisecond), 10.0, TSAddOptions{})
	assert.Error(t, err)
	assert.Equal(t, ErrWrongType, err)

	// TSAdd on non-existent key should work fine
	_, err = s.TSAdd("newts", time.Now().UnixNano()/int64(time.Millisecond), 10.0, TSAddOptions{})
	assert.NoError(t, err)

	// Verify it's now a time series
	keyType, err = s.Type("newts")
	assert.NoError(t, err)
	assert.Equal(t, "ts", keyType)
}

// TestTSAddRule_Persisted verifies TS.CREATERULE actually persists the rule
// (previously a stub that returned nil without storing anything), and that
// TSGetRule can read it back with the normalized aggregator.
func TestTSAddRule_Persisted(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	// Lowercase aggregator is normalized to uppercase on store
	err := s.TSAddRule("ts_src", "ts_dst", "avg", 60000)
	assert.NoError(t, err)

	agg, dur, found, err := s.TSGetRule("ts_src", "ts_dst")
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "AVG", agg)
	assert.Equal(t, int64(60000), dur)

	// Duplicate rule on the same dest must fail (Redis semantics)
	err = s.TSAddRule("ts_src", "ts_dst", "sum", 60000)
	assert.Error(t, err)
}

// TestTSAddRule_InvalidArgs verifies aggregator and bucket-duration validation.
func TestTSAddRule_InvalidArgs(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	// Unknown aggregator
	err := s.TSAddRule("src", "dst", "bogus", 1000)
	assert.Error(t, err)

	// Non-positive bucket duration
	err = s.TSAddRule("src", "dst", "SUM", 0)
	assert.Error(t, err)
	err = s.TSAddRule("src", "dst", "SUM", -5)
	assert.Error(t, err)

	// Nothing should have been persisted
	_, _, found, err := s.TSGetRule("src", "dst")
	assert.NoError(t, err)
	assert.False(t, found)
}

// TestTSDelRule_Removes verifies TS.DELETERULE removes a persisted rule,
// and deleting a non-existent rule is a silent no-op.
func TestTSDelRule_Removes(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	assert.NoError(t, s.TSAddRule("src", "dst", "SUM", 60000))
	_, _, found, err := s.TSGetRule("src", "dst")
	assert.NoError(t, err)
	assert.True(t, found)

	assert.NoError(t, s.TSDelRule("src", "dst", "SUM", 60000))
	_, _, found, err = s.TSGetRule("src", "dst")
	assert.NoError(t, err)
	assert.False(t, found)

	// Re-creating after delete succeeds
	assert.NoError(t, s.TSAddRule("src", "dst", "MIN", 30000))
	agg, dur, found, err := s.TSGetRule("src", "dst")
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "MIN", agg)
	assert.Equal(t, int64(30000), dur)

	// Deleting a non-existent rule is a no-op, not an error
	assert.NoError(t, s.TSDelRule("nope", "nope2", "SUM", 60000))
}
