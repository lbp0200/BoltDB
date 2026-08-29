package replication

import (
	"fmt"
	"math"
	"sync"
)

const DefaultBacklogSize int64 = 1024 * 1024
const MaxBacklogSize int64 = 512 * 1024 * 1024

type ReplicationBacklog struct {
	buffer []byte
	offset int64
	size   int64
	mu     sync.RWMutex
}

func NewReplicationBacklog(size int64) *ReplicationBacklog {
	if size <= 0 {
		size = DefaultBacklogSize
	}
	if size > MaxBacklogSize {
		size = MaxBacklogSize
	}
	return &ReplicationBacklog{
		buffer: make([]byte, size),
		offset: 0,
		size:   size,
	}
}

func (rb *ReplicationBacklog) Append(data []byte) int64 {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	dataLen := int64(len(data))
	if dataLen == 0 {
		return rb.offset
	}

	startOffset := rb.offset

	if dataLen >= rb.size {
		copy(rb.buffer, data[dataLen-rb.size:])
		rb.offset += dataLen
		return startOffset
	}

	writePos := rb.offset % rb.size
	endPos := writePos + dataLen

	if endPos <= rb.size {
		copy(rb.buffer[writePos:endPos], data)
	} else {
		firstPart := rb.size - writePos
		copy(rb.buffer[writePos:], data[:firstPart])
		copy(rb.buffer[:endPos-rb.size], data[firstPart:])
	}

	rb.offset += dataLen
	return startOffset
}

func (rb *ReplicationBacklog) GetRange(startOffset, endOffset int64) ([]byte, error) {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if startOffset < 0 || endOffset < startOffset {
		return nil, fmt.Errorf("invalid offset range: start=%d end=%d", startOffset, endOffset)
	}

	currentOffset := rb.offset
	availableStart := currentOffset - rb.size
	if availableStart < 0 {
		availableStart = 0
	}

	if startOffset < availableStart {
		return nil, fmt.Errorf("offset too old, min available: %d, requested: %d", availableStart, startOffset)
	}

	if startOffset >= currentOffset {
		return nil, fmt.Errorf("offset too new, max available: %d, requested: %d", currentOffset-1, startOffset)
	}

	if endOffset > currentOffset {
		endOffset = currentOffset
	}

	length := endOffset - startOffset
	if length <= 0 {
		return []byte{}, nil
	}

	result := make([]byte, length)
	for i := int64(0); i < length; i++ {
		result[i] = rb.buffer[(startOffset+i)%rb.size]
	}

	return result, nil
}

func (rb *ReplicationBacklog) GetCurrentOffset() int64 {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return rb.offset
}

// SetOffset 将写入水位前移到 offset（不回退）。
//
// 仅供启动恢复与测试使用：运行期的水位只由 Append 推进，这样才能保证
// 它恒等于"已完整写入环的字节数"，即永远落在命令边界上。复制偏移量
// （GetMasterReplOffset）就是这个水位，不得另设计数器。
func (rb *ReplicationBacklog) SetOffset(offset int64) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	if offset > rb.offset {
		rb.offset = offset
	}
}

func (rb *ReplicationBacklog) GetSize() int64 {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return rb.size
}

func (rb *ReplicationBacklog) GetAvailableLength() int64 {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	if rb.offset == 0 {
		return 0
	}
	availableStart := rb.offset - rb.size
	if availableStart < 0 {
		availableStart = 0
	}
	return rb.offset - availableStart
}

// StartsAtCommandBoundary 报告给定 offset 是否落在命令边界上。
//
// 每条被传播/入 backlog 的命令都由 serializeCommand 生成，以 RESP 数组头
// '*' 开头。若传入的 offset 落在一个命令的字节中间（如从节点 offset 失步、
// 错位续传），该位置的字节几乎不可能是 '*'。此检查用于在 PSYNC CONTINUE
// 前拦截错位 offset，避免主节点把错位字节流发给从节点 → ReadRESP 误把 key
// 名当命令名（K:HASH:47 类 mis-frame）→ 无限重同步。
//
// offset 超出可服务窗口（已截断或超出当前）时返回 false，调用方应据此
// 降级为 FULLRESYNC。
func (rb *ReplicationBacklog) StartsAtCommandBoundary(offset int64) bool {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if rb.offset == 0 || offset < 0 {
		return false
	}
	availableStart := rb.offset - rb.size
	if availableStart < 0 {
		availableStart = 0
	}
	// 仅当 offset 在 [availableStart, currentOffset) 内才可定位字节。
	if offset < availableStart || offset >= rb.offset {
		return false
	}
	return rb.buffer[offset%rb.size] == '*'
}

func (rb *ReplicationBacklog) AvailableStartOffset() int64 {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	start := rb.offset - rb.size
	if start < 0 {
		return 0
	}
	return start
}

func (rb *ReplicationBacklog) IsOffsetAvailable(offset int64) bool {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	availableStart := rb.offset - rb.size
	if availableStart < 0 {
		availableStart = 0
	}
	return offset >= availableStart && offset < rb.offset
}

const MibInBytes int64 = 1024 * 1024

func ParseBacklogSize(s string) (int64, error) {
	if len(s) == 0 {
		return DefaultBacklogSize, nil
	}

	var value float64
	var unit string
	n, err := fmt.Sscanf(s, "%f%s", &value, &unit)
	if err != nil && n == 0 {
		return 0, fmt.Errorf("invalid backlog size: %s", s)
	}

	var multiplier int64
	switch unit {
	case "b", "B":
		multiplier = 1
	case "kb", "KB", "Kb", "kB", "k":
		multiplier = 1024
	case "mb", "MB", "Mb", "mB", "m":
		multiplier = MibInBytes
	case "gb", "GB", "Gb", "gB", "g":
		multiplier = MibInBytes * 1024
	case "":
		multiplier = MibInBytes
	default:
		return 0, fmt.Errorf("invalid backlog size unit: %s", unit)
	}

	result := int64(math.Round(value * float64(multiplier)))
	if result <= 0 {
		result = DefaultBacklogSize
	}
	if result > MaxBacklogSize {
		result = MaxBacklogSize
	}

	return result, nil
}
