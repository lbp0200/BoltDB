package replication

import (
	"testing"

	"github.com/zeebo/assert"
)

func TestBacklog_New(t *testing.T) {
	t.Parallel()
	backlog := NewReplicationBacklog(1024)
	assert.Equal(t, int64(0), backlog.GetCurrentOffset())
	assert.Equal(t, int64(1024), backlog.GetSize())
}

func TestBacklog_New_DefaultSize(t *testing.T) {
	t.Parallel()
	b := NewReplicationBacklog(0)
	assert.Equal(t, DefaultBacklogSize, b.GetSize())
}

func TestBacklog_Append(t *testing.T) {
	t.Parallel()
	backlog := NewReplicationBacklog(100)

	offset1 := backlog.Append([]byte("hello"))
	assert.Equal(t, int64(0), offset1)
	assert.Equal(t, int64(5), backlog.GetCurrentOffset())

	offset2 := backlog.Append([]byte("world"))
	assert.Equal(t, int64(5), offset2)
	assert.Equal(t, int64(10), backlog.GetCurrentOffset())
}

func TestBacklog_GetRange(t *testing.T) {
	t.Parallel()
	backlog := NewReplicationBacklog(100)
	backlog.Append([]byte("hello world"))

	result, err := backlog.GetRange(0, 10)
	assert.NoError(t, err)
	assert.Equal(t, "hello worl", string(result))

	result, err = backlog.GetRange(0, 11)
	assert.NoError(t, err)
	assert.Equal(t, "hello world", string(result))

	result, err = backlog.GetRange(0, 5)
	assert.NoError(t, err)
	assert.Equal(t, "hello", string(result))

	result, err = backlog.GetRange(5, 10)
	assert.NoError(t, err)
	assert.Equal(t, " worl", string(result))
}

func TestBacklog_CumulativeOffset(t *testing.T) {
	t.Parallel()
	backlog := NewReplicationBacklog(10)

	backlog.Append([]byte("abcde"))
	assert.Equal(t, int64(5), backlog.GetCurrentOffset())

	backlog.Append([]byte("fghij"))
	assert.Equal(t, int64(10), backlog.GetCurrentOffset())

	backlog.Append([]byte("xyz"))
	assert.Equal(t, int64(13), backlog.GetCurrentOffset())

	data, err := backlog.GetRange(10, 13)
	assert.NoError(t, err)
	assert.Equal(t, "xyz", string(data))

	data, err = backlog.GetRange(3, 13)
	assert.NoError(t, err)
	assert.Equal(t, "defghijxyz", string(data))
}

func TestBacklog_GetRange_Circular(t *testing.T) {
	t.Parallel()
	backlog := NewReplicationBacklog(10)

	backlog.Append([]byte("abcdefghij"))
	assert.Equal(t, int64(10), backlog.GetCurrentOffset())

	backlog.Append([]byte("xy"))
	assert.Equal(t, int64(12), backlog.GetCurrentOffset())

	data, err := backlog.GetRange(10, 12)
	assert.NoError(t, err)
	assert.Equal(t, "xy", string(data))

	_, err = backlog.GetRange(0, 10)
	assert.Error(t, err)
}

func TestBacklog_GetRange_Errors(t *testing.T) {
	t.Parallel()
	backlog := NewReplicationBacklog(100)
	backlog.Append([]byte("hello"))

	_, err := backlog.GetRange(-1, 5)
	assert.Error(t, err)

	_, err = backlog.GetRange(5, 3)
	assert.Error(t, err)

	_, err = backlog.GetRange(0, 5)
	assert.NoError(t, err)

	_, err = backlog.GetRange(5, 10)
	assert.Error(t, err)

	_, err = backlog.GetRange(5, 6)
	assert.Error(t, err)
}

func TestBacklog_GetRange_Empty(t *testing.T) {
	t.Parallel()
	backlog := NewReplicationBacklog(100)

	_, err := backlog.GetRange(0, 0)
	assert.Error(t, err)
}

func TestBacklog_MultipleAppends(t *testing.T) {
	t.Parallel()
	backlog := NewReplicationBacklog(50)

	offsets := []int64{}
	for i := 0; i < 10; i++ {
		offset := backlog.Append([]byte("x"))
		offsets = append(offsets, offset)
	}

	assert.Equal(t, int64(10), backlog.GetCurrentOffset())

	for i := 1; i < len(offsets); i++ {
		assert.True(t, offsets[i] > offsets[i-1])
	}
}

func TestBacklog_DataTooLarge(t *testing.T) {
	t.Parallel()
	backlog := NewReplicationBacklog(10)

	backlog.Append([]byte("abcdefghijklmnopqrst"))
	assert.Equal(t, int64(20), backlog.GetCurrentOffset())

	data, err := backlog.GetRange(10, 20)
	assert.NoError(t, err)
	assert.Equal(t, "klmnopqrst", string(data))
}

func TestBacklog_BasicWriteRead(t *testing.T) {
	t.Parallel()
	backlog := NewReplicationBacklog(100)

	backlog.Append([]byte("test"))
	assert.Equal(t, int64(4), backlog.GetCurrentOffset())

	result, err := backlog.GetRange(0, 4)
	assert.NoError(t, err)
	assert.Equal(t, "test", string(result))
}

func TestBacklog_WrapAround(t *testing.T) {
	t.Parallel()
	backlog := NewReplicationBacklog(10)

	for i := 0; i < 15; i++ {
		backlog.Append([]byte{byte('a' + i)})
	}
	assert.Equal(t, int64(15), backlog.GetCurrentOffset())

	result, err := backlog.GetRange(5, 15)
	assert.NoError(t, err)
	assert.Equal(t, "fghijklmno", string(result))
}

func TestBacklog_AvailableStartOffset(t *testing.T) {
	t.Parallel()
	backlog := NewReplicationBacklog(100)
	assert.Equal(t, int64(0), backlog.AvailableStartOffset())

	backlog.Append([]byte("hello"))
	assert.Equal(t, int64(0), backlog.AvailableStartOffset())

	for i := 0; i < 20; i++ {
		backlog.Append(make([]byte, 10))
	}
	assert.Equal(t, int64(205), backlog.GetCurrentOffset())
	assert.Equal(t, int64(105), backlog.AvailableStartOffset())
}

func TestBacklog_IsOffsetAvailable(t *testing.T) {
	t.Parallel()
	backlog := NewReplicationBacklog(100)

	assert.False(t, backlog.IsOffsetAvailable(0))
	assert.False(t, backlog.IsOffsetAvailable(5))

	backlog.Append([]byte("hello"))
	assert.True(t, backlog.IsOffsetAvailable(0))
	assert.True(t, backlog.IsOffsetAvailable(4))
	assert.False(t, backlog.IsOffsetAvailable(5))

	for i := 0; i < 20; i++ {
		backlog.Append(make([]byte, 10))
	}
	assert.False(t, backlog.IsOffsetAvailable(0))
	assert.True(t, backlog.IsOffsetAvailable(105))
	assert.False(t, backlog.IsOffsetAvailable(205))
}

func TestBacklog_GetAvailableLength(t *testing.T) {
	t.Parallel()
	backlog := NewReplicationBacklog(100)
	assert.Equal(t, int64(0), backlog.GetAvailableLength())

	backlog.Append([]byte("hello"))
	assert.Equal(t, int64(5), backlog.GetAvailableLength())

	for i := 0; i < 20; i++ {
		backlog.Append(make([]byte, 10))
	}
	assert.Equal(t, int64(100), backlog.GetAvailableLength())
}

func TestResizeBacklogPreservesWindow(t *testing.T) {
	t.Parallel()
	old := NewReplicationBacklog(64)
	payload := []byte("hello-replication-backlog-data-0123456789")
	old.Append(payload)
	off := old.GetCurrentOffset()

	grown := resizeBacklog(old, 256)
	assert.Equal(t, int64(256), grown.GetSize())
	assert.Equal(t, off, grown.GetCurrentOffset())
	got, err := grown.GetRange(0, off)
	assert.NoError(t, err)
	assert.Equal(t, payload, got)

	shrunk := resizeBacklog(grown, 48)
	assert.Equal(t, int64(48), shrunk.GetSize())
	assert.Equal(t, off, shrunk.GetCurrentOffset())
	got2, err := shrunk.GetRange(0, off)
	assert.NoError(t, err)
	assert.Equal(t, payload, got2)
}

func TestSetBacklogSizeMigrates(t *testing.T) {
	t.Parallel()
	rm := &ReplicationManager{
		role:    RoleMaster,
		slaves:  make(map[string]*SlaveConnection),
		backlog: NewReplicationBacklog(DefaultBacklogSize),
	}
	msg := []byte("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n")
	rm.backlog.Append(msg)
	off := rm.backlog.GetCurrentOffset()

	rm.SetBacklogSize(2 * 1024 * 1024)
	assert.Equal(t, int64(2*1024*1024), rm.backlog.GetSize())
	assert.Equal(t, off, rm.backlog.GetCurrentOffset())
	got, err := rm.backlog.GetRange(0, off)
	assert.NoError(t, err)
	assert.Equal(t, msg, got)
}
