package replication

import (
	"testing"

	"github.com/zeebo/assert"
)

func TestRDBDecoder_New(t *testing.T) {
	t.Parallel()
	// 创建一个简单的 RDB 数据
	rdbData := []byte{
		'R', 'E', 'D', 'I', 'S', '0', '0', '0', '9', // header
		0xFE, 0x00, // database selector
		0xFF,                   // EOF
		0x00, 0x00, 0x00, 0x00, // checksum (placeholder)
	}

	dec := NewRDBDecoder(rdbData)
	assert.NotEqual(t, nil, dec)
}

func TestRDBDecoder_DecodeHeader(t *testing.T) {
	t.Parallel()
	// 创建一个有效的 RDB 头部
	rdbData := []byte{
		'R', 'E', 'D', 'I', 'S', '0', '0', '0', '9', // header
	}

	dec := NewRDBDecoder(rdbData)
	err := dec.DecodeHeader()
	assert.NoError(t, err)
}

func TestRDBDecoder_DecodeHeader_Invalid(t *testing.T) {
	t.Parallel()
	// 无效的 RDB 头部 - magic string 不匹配
	rdbData := []byte{
		'X', 'R', 'E', 'D', 'I', 'S', // invalid magic (only 6 bytes)
	}

	dec := NewRDBDecoder(rdbData)
	err := dec.DecodeHeader()
	// 应该报错，因为 magic string 不匹配
	assert.Error(t, err)
}
