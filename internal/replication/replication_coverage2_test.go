package replication

import (
	"testing"

	"github.com/zeebo/assert"
)

// parseBacklogSize coverage

func TestParseBacklogSize_Empty_Coverage(t *testing.T) {
	t.Parallel()
	size, err := parseBacklogSize("")
	assert.NoError(t, err)
	assert.Equal(t, DefaultBacklogSize, size)
}

func TestParseBacklogSize_Megabytes_Coverage(t *testing.T) {
	t.Parallel()
	size, err := parseBacklogSize("1mb")
	assert.NoError(t, err)
	assert.Equal(t, int64(1*1024*1024), size)
}

func TestParseBacklogSize_Gigabytes_Coverage(t *testing.T) {
	t.Parallel()
	// MaxBacklogSize is 512MB, so any value above that gets capped
	size, err := parseBacklogSize("2gb")
	assert.NoError(t, err)
	assert.Equal(t, MaxBacklogSize, size)
}

func TestParseBacklogSize_Kilobytes_Coverage(t *testing.T) {
	t.Parallel()
	size, err := parseBacklogSize("512kb")
	assert.NoError(t, err)
	assert.Equal(t, int64(512*1024), size)
}

func TestParseBacklogSize_NoUnit_Coverage(t *testing.T) {
	t.Parallel()
	// No unit means multiplier = MibInBytes
	size, err := parseBacklogSize("2")
	assert.NoError(t, err)
	assert.Equal(t, int64(2*1024*1024), size)
}

func TestParseBacklogSize_Invalid_Coverage(t *testing.T) {
	t.Parallel()
	_, err := parseBacklogSize("abc")
	assert.Error(t, err)
}

func TestParseBacklogSize_InvalidUnit_Coverage(t *testing.T) {
	t.Parallel()
	_, err := parseBacklogSize("10xyz")
	assert.Error(t, err)
}

func TestParseBacklogSize_Zero_Coverage(t *testing.T) {
	t.Parallel()
	size, err := parseBacklogSize("0mb")
	assert.NoError(t, err)
	assert.Equal(t, DefaultBacklogSize, size)
}

// GetSlaveReplOffset coverage: test nil slaveReconnector path

func TestReplicationManager_GetSlaveReplOffset_Nil_Coverage(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)

	rm := NewReplicationManager(testStore)
	assert.Equal(t, int64(0), rm.GetSlaveReplOffset())
}

func TestReplicationManager_GetReconnectCount_Nil_Coverage(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)

	rm := NewReplicationManager(testStore)
	assert.Equal(t, int64(0), rm.GetReconnectCount())
}
