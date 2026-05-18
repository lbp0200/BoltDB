package sentinel

import (
	"testing"
	"time"

	"github.com/zeebo/assert"
)

func TestConfigProvider_New(t *testing.T) {
	t.Parallel()
	s := NewSentinel(2, 30*time.Second)
	cp := NewConfigProvider(s)

	assert.NotEqual(t, nil, cp)
}

func TestConfigProvider_GetMasterAddrByName(t *testing.T) {
	t.Parallel()
	s := NewSentinel(2, 30*time.Second)
	cp := NewConfigProvider(s)

	// 初始没有主节点
	addr, err := cp.GetMasterAddrByName("mymaster")
	assert.Error(t, err)
	assert.Equal(t, "", addr)
}

func TestConfigProvider_GetMasters(t *testing.T) {
	t.Parallel()
	s := NewSentinel(2, 30*time.Second)
	cp := NewConfigProvider(s)

	// 初始没有主节点
	masters := cp.GetMasters()
	assert.Equal(t, 0, len(masters))
}

func TestConfigProvider_GetSlaves(t *testing.T) {
	t.Parallel()
	s := NewSentinel(2, 30*time.Second)
	cp := NewConfigProvider(s)

	// 初始没有从节点
	slaves := cp.GetSlaves("mymaster")
	assert.Equal(t, 0, len(slaves))
}
