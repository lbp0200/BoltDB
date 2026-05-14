package replication

import (
	"fmt"
	"sync"
)

// ReplicationBacklog 复制积压缓冲区
type ReplicationBacklog struct {
	buffer []byte
	offset int64 // 当前偏移量
	size   int64 // 缓冲区大小
	mu     sync.RWMutex
}

// NewReplicationBacklog 创建新的复制积压缓冲区
func NewReplicationBacklog(size int64) *ReplicationBacklog {
	return &ReplicationBacklog{
		buffer: make([]byte, size),
		offset: 0,
		size:   size,
	}
}

// Append 追加数据到缓冲区，返回新的偏移量
func (rb *ReplicationBacklog) Append(data []byte) int64 {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	dataLen := int64(len(data))
	startOffset := rb.offset

	// 如果数据太大，只保留最后的部分
	if dataLen > rb.size {
		// 只保留最后size字节
		data = data[dataLen-rb.size:]
		dataLen = rb.size
		rb.offset = rb.size
	} else {
		// 检查是否需要循环覆盖
		if rb.offset+dataLen > rb.size {
			// 需要循环，从开头开始
			rb.offset = 0
		}
		rb.offset += dataLen
	}

	// 写入数据
	copy(rb.buffer[rb.offset-dataLen:rb.offset], data)

	return startOffset
}

// GetRange 获取指定偏移量范围的数据
func (rb *ReplicationBacklog) GetRange(startOffset, endOffset int64) ([]byte, error) {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if startOffset < 0 || endOffset < startOffset {
		return nil, fmt.Errorf("invalid offset range")
	}

	// 计算实际可用的偏移量范围
	availableStart := rb.offset - rb.size
	if availableStart < 0 {
		availableStart = 0
	}

	if startOffset < availableStart {
		// 请求的偏移量太旧，无法提供
		return nil, fmt.Errorf("offset too old, min available: %d", availableStart)
	}

	if startOffset >= rb.offset {
		// 请求的偏移量太新
		return nil, fmt.Errorf("offset too new, max available: %d", rb.offset-1)
	}

	// 计算在缓冲区中的位置
	startPos := startOffset % rb.size
	endPos := endOffset % rb.size

	if endPos < startPos {
		// 跨越了缓冲区边界，需要拼接
		part1 := rb.buffer[startPos:]
		part2 := rb.buffer[:endPos]
		result := make([]byte, 0, len(part1)+len(part2))
		result = append(result, part1...)
		result = append(result, part2...)
		return result, nil
	}

	// 没有跨越边界
	return rb.buffer[startPos:endPos], nil
}

// GetCurrentOffset 获取当前偏移量
func (rb *ReplicationBacklog) GetCurrentOffset() int64 {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return rb.offset
}

// GetSize 获取缓冲区大小
func (rb *ReplicationBacklog) GetSize() int64 {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return rb.size
}
