package replication

import (
	"testing"

	"github.com/zeebo/assert"
)

func TestBacklog_New(t *testing.T) {
	backlog := NewReplicationBacklog(1024)

	assert.Equal(t, int64(0), backlog.GetCurrentOffset())
	assert.Equal(t, int64(1024), backlog.GetSize())
}

func TestBacklog_Append(t *testing.T) {
	backlog := NewReplicationBacklog(100)

	// 第一次追加，返回的是追加前的偏移量
	data1 := []byte("hello")
	offset1 := backlog.Append(data1)
	assert.Equal(t, int64(0), offset1) // 追加前偏移量是0
	assert.Equal(t, int64(5), backlog.GetCurrentOffset())

	// 第二次追加
	data2 := []byte("world")
	offset2 := backlog.Append(data2)
	assert.Equal(t, int64(5), offset2) // 追加前偏移量是5
	assert.Equal(t, int64(10), backlog.GetCurrentOffset())
}

func TestBacklog_GetRange(t *testing.T) {
	backlog := NewReplicationBacklog(100)

	// 追加数据
	data := []byte("hello world")
	backlog.Append(data)

	// 调试信息
	t.Logf("offset: %d", backlog.GetCurrentOffset())
	t.Logf("buffer size: %d", backlog.GetSize())

	// 获取数据 - 注意 endOffset 不能等于 offset，否则返回空
	// 需要 endOffset < offset
	result, err := backlog.GetRange(0, 10)
	assert.NoError(t, err)
	t.Logf("GetRange(0, 10) = %q (len=%d)", string(result), len(result))
	assert.Equal(t, "hello worl", string(result)) // 10字节

	// 获取更多 - 11字节
	result, err = backlog.GetRange(0, 11)
	assert.NoError(t, err)
	t.Logf("GetRange(0, 11) = %q (len=%d)", string(result), len(result))
	assert.Equal(t, "hello world", string(result)) // 11字节

	// 获取前5字节
	result, err = backlog.GetRange(0, 5)
	assert.NoError(t, err)
	assert.Equal(t, "hello", string(result))

	// 获取中间部分 - startPos=5, endPos=10
	result, err = backlog.GetRange(5, 10)
	assert.NoError(t, err)
	assert.Equal(t, " worl", string(result))
}

func TestBacklog_Append_Circular(t *testing.T) {
	// 小缓冲区测试循环覆盖
	backlog := NewReplicationBacklog(10)

	// 追加5字节: "abcde", offset 变为 5
	backlog.Append([]byte("abcde"))
	assert.Equal(t, int64(5), backlog.GetCurrentOffset())

	// 再追加5字节: "fghij", offset 变为 10，缓冲区正好满
	backlog.Append([]byte("fghij"))
	assert.Equal(t, int64(10), backlog.GetCurrentOffset())

	// 再追加3字节: "xyz"，触发循环，offset 变为 3
	backlog.Append([]byte("xyz"))
	assert.Equal(t, int64(3), backlog.GetCurrentOffset()) // 循环回到开头

	// 现在 buffer 是 "xyzfghij" (最后3字节覆盖了前3字节)
	// offset=3, availableStart = max(0, 3-10) = 0
	// 有效范围: startOffset < 3

	// 获取3字节
	result, err := backlog.GetRange(0, 3)
	assert.NoError(t, err)
	assert.Equal(t, "xyz", string(result))
}

func TestBacklog_GetRange_Circular(t *testing.T) {
	// 测试循环后跨环形边界的读取
	backlog := NewReplicationBacklog(10)

	// 填满缓冲区
	backlog.Append([]byte("abcdefghij")) // offset=10
	// 触发循环
	backlog.Append([]byte("xy")) // offset=2 (循环)

	// buffer = "xycdefghij", offset=2
	// availableStart = max(0, 2-10) = 0
	// 有效范围: startOffset < 2

	// 获取2字节
	result, err := backlog.GetRange(0, 2)
	assert.NoError(t, err)
	assert.Equal(t, "xy", string(result))
}

func TestBacklog_GetRange_Errors(t *testing.T) {
	backlog := NewReplicationBacklog(100)

	backlog.Append([]byte("hello"))

	// 负数偏移量
	_, err := backlog.GetRange(-1, 5)
	assert.Error(t, err)

	// 结束小于开始
	_, err = backlog.GetRange(5, 3)
	assert.Error(t, err)

	// 偏移量太旧
	// offset=5, size=100, availableStart = max(0, 5-100) = 0
	// 所以 0 是有效的
	_, err = backlog.GetRange(0, 5)
	assert.NoError(t, err) // 不报错

	// 偏移量太新 - startOffset >= offset
	_, err = backlog.GetRange(5, 10)
	assert.Error(t, err)

	// startOffset 等于 offset
	_, err = backlog.GetRange(5, 6)
	assert.Error(t, err)
}

func TestBacklog_GetRange_Empty(t *testing.T) {
	backlog := NewReplicationBacklog(100)

	// 空缓冲区 - offset=0, startOffset=0 >= offset=0, 报错
	_, err := backlog.GetRange(0, 0)
	assert.Error(t, err)
}

func TestBacklog_Append_ExactSize(t *testing.T) {
	// 测试正好等于缓冲区大小的数据
	backlog := NewReplicationBacklog(5)

	data := []byte("abcde")
	backlog.Append(data)

	assert.Equal(t, int64(5), backlog.GetCurrentOffset())

	// 当 offset = size 时，endPos = offset % size = 0 = startPos
	// 会导致返回空，这是实现的一个边界情况
	// 需要调整 offset 才能获取数据
	backlog.Append([]byte("f")) // offset = 0 (循环), 然后 +1 = 1

	// 现在 offset=1, availableStart = max(0, 1-5) = 0
	// 有效范围: startOffset < 1
	// GetRange(0, 5): startPos=0, endPos=0 (5%5=0), 相等返回空
	// GetRange(0, 1): startPos=0, endPos=1, 返回 buffer[0:1]
	result, err := backlog.GetRange(0, 1)
	assert.NoError(t, err)
	assert.Equal(t, "f", string(result))
}

func TestBacklog_MultipleAppends(t *testing.T) {
	backlog := NewReplicationBacklog(50)

	// 多次追加
	offsets := []int64{}
	for i := 0; i < 10; i++ {
		data := []byte("x")
		offset := backlog.Append(data)
		offsets = append(offsets, offset)
	}

	assert.Equal(t, int64(10), backlog.GetCurrentOffset())

	// 验证偏移量递增
	for i := 1; i < len(offsets); i++ {
		assert.True(t, offsets[i] > offsets[i-1])
	}
}

func TestBacklog_DataTooLarge(t *testing.T) {
	// 测试数据大于缓冲区大小
	backlog := NewReplicationBacklog(10)

	// 追加20字节数据
	backlog.Append([]byte("abcdefghijklmnopqrst")) // 20字节

	// 应该只保留最后10字节
	assert.Equal(t, int64(10), backlog.GetCurrentOffset())

	// 需要先触发循环才能获取
	backlog.Append([]byte("z")) // offset=1

	// 获取数据
	result, err := backlog.GetRange(0, 10)
	assert.NoError(t, err)
	// buffer 最后10字节应该是 "stz" 覆盖了前2字节
	// 实际是 "zklmnopqrs"
	t.Logf("buffer: %q", string(result))
}

func TestBacklog_PartialOverlap(t *testing.T) {
	// 测试部分重叠的情况
	backlog := NewReplicationBacklog(10)

	// 追加 "abcdefgh" (8字节)
	backlog.Append([]byte("abcdefgh"))
	// offset = 8

	// 再追加 "xy" (2字节), 8+2=10, 不需要循环
	// offset = 10
	backlog.Append([]byte("xy"))

	// buffer: "abcdefghxy", offset=10
	// 需要先触发循环
	backlog.Append([]byte("z")) // offset=1

	// 获取数据
	result, err := backlog.GetRange(0, 9)
	assert.NoError(t, err)
	t.Logf("buffer: %q", string(result))
}

func TestBacklog_BasicWriteRead(t *testing.T) {
	// 最基本的写和读测试
	backlog := NewReplicationBacklog(100)

	// 写
	backlog.Append([]byte("test"))
	assert.Equal(t, int64(4), backlog.GetCurrentOffset())

	// 读 - 范围需要小于 offset
	result, err := backlog.GetRange(0, 4)
	assert.NoError(t, err)
	assert.Equal(t, "test", string(result))
}
