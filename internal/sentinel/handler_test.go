package sentinel

import (
	"testing"

	"github.com/zeebo/assert"
)

// TestSentinelHandler_New tests NewSentinelHandler
func TestSentinelHandler_New(t *testing.T) {
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	handler := NewSentinelHandler(sentinel)
	assert.True(t, handler != nil)
	assert.True(t, handler.sentinel != nil)
}
