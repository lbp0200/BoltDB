package metrics

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/zeebo/assert"
)

func TestSnapshot_JSON(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 6, 16, 10, 0, 0, 0, time.UTC)
	s := Snapshot{
		Time:             now,
		ActiveRetries:    1,
		L0Score:          2.5,
		Goroutines:       10,
		AllocBytes:       4096,
		MasterReplOffset: 100,
		Role:             "master",
		ActiveClients:    5,
	}

	b := s.JSON()
	var decoded Snapshot
	err := json.Unmarshal(b, &decoded)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), decoded.ActiveRetries)
	assert.Equal(t, 2.5, decoded.L0Score)
	assert.Equal(t, 10, decoded.Goroutines)
	assert.Equal(t, "master", decoded.Role)
	assert.Equal(t, 5, decoded.ActiveClients)
}

func TestSnapshot_String(t *testing.T) {
	t.Parallel()
	s := Snapshot{
		Time:             time.Date(2026, 6, 16, 10, 0, 0, 0, time.UTC),
		L0Score:          1.5,
		ActiveRetries:    2,
		TotalRetries:     10,
		WritesBlocked:    0,
		L0Rejected:       0,
		L0Delayed:        1,
		Goroutines:       8,
		AllocBytes:       1048576,
		HeapInuse:        2097152,
		StackInuse:       524288,
		NumGC:            42,
		Role:             "master",
		MasterReplOffset: 500,
		SlaveReplOffset:  0,
		ReplicationLag:   0,
		ReconnectCount:   3,
		SlaveCount:       2,
		BacklogSize:      65536,
		BacklogAvailable: 32768,
		ActiveClients:    4,
		BlockedClients:   1,
		MonitorClients:   0,
		PubSubClients:    2,
		PubSubSubs:       10,
		TotalOutputBytes: 8192,
	}

	out := s.String()
	assert.True(t, strings.Contains(out, "L0:"))
	assert.True(t, strings.Contains(out, "score=1.5"))
	assert.True(t, strings.Contains(out, "Go:"))
	assert.True(t, strings.Contains(out, "goroutines=8"))
	assert.True(t, strings.Contains(out, "Repl:"))
	assert.True(t, strings.Contains(out, "role=master"))
	assert.True(t, strings.Contains(out, "master_offset=500"))
	assert.True(t, strings.Contains(out, "Backlog:"))
	assert.True(t, strings.Contains(out, "Clients:"))
	assert.True(t, strings.Contains(out, "active=4"))
	assert.True(t, strings.Contains(out, "blocked=1"))
}

func TestBytesStr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input uint64
		want  string
	}{
		{0, "0B"},
		{1, "1B"},
		{1023, "1023B"},
		{1024, "1.0KB"},
		{1536, "1.5KB"},
		{1048576, "1.0MB"},
		{1073741824, "1.0GB"},
		{1099511627776, "1.0TB"},
		{1125899906842624, "1.0PB"},
	}

	for _, tt := range tests {
		got := bytesStr(tt.input)
		assert.Equal(t, tt.want, got)
	}
}

func TestFormatLastGC_Never(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "never", formatLastGC(0))
}

func TestFormatLastGC_Valid(t *testing.T) {
	t.Parallel()
	// formatLastGC uses time.Unix which returns local time.
	// Use local time to match the implementation.
	tm := time.Date(2026, 6, 16, 10, 30, 45, 123000000, time.Local)
	result := formatLastGC(uint64(tm.UnixNano()))
	assert.Equal(t, "10:30:45.123", result)
}

func TestCollectRuntime(t *testing.T) {
	t.Parallel()
	goroutines, mem := CollectRuntime()
	assert.True(t, goroutines > 0)
	assert.True(t, mem.Alloc > 0)
}

func TestSnapshot_JSON_Empty(t *testing.T) {
	t.Parallel()
	s := Snapshot{}
	b := s.JSON()
	var decoded map[string]interface{}
	err := json.Unmarshal(b, &decoded)
	assert.NoError(t, err)
	assert.Equal(t, "0001-01-01T00:00:00Z", decoded["time"])
}

func TestBytesStr_EdgeCases(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "0B", bytesStr(0))
	assert.Equal(t, "1B", bytesStr(1))
	assert.Equal(t, "1023B", bytesStr(1023))
	assert.Equal(t, "1.0KB", bytesStr(1024))
	assert.Equal(t, "1.0MB", bytesStr(1048576))
}
