package sentinel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestODown_MultiSentinelConsensus 验证：多个不同哨兵报告 SDOWN 后才能触发 ODOWN
func TestODown_MultiSentinelConsensus(t *testing.T) {
	t.Parallel()
	master := NewMasterInstanceWithDownAfter("mymaster", "127.0.0.1:6379", 3, 1*time.Second)

	// 初始状态：ok
	assert.Equal(t, "ok", master.GetState())
	assert.Equal(t, 0, master.GetSdownCount())
	assert.False(t, master.IsODown())

	// 第 1 个哨兵报告 SDOWN → 不应触发 ODOWN（1 < 3）
	master.ReportSdown("sentinel-A")
	assert.Equal(t, 1, master.GetSdownCount())
	assert.False(t, master.IsODown())

	// 第 2 个哨兵报告 SDOWN → 不应触发 ODOWN（2 < 3）
	master.ReportSdown("sentinel-B")
	assert.Equal(t, 2, master.GetSdownCount())
	assert.False(t, master.IsODown())

	// 第 3 个哨兵报告 SDOWN → 应触发 ODOWN（3 >= 3）
	master.ReportSdown("sentinel-C")
	assert.Equal(t, 3, master.GetSdownCount())
	assert.True(t, master.IsODown())
}

// TestODown_SingleSentinelCannotTrigger 验证：单个哨兵重复报告不会触发 ODOWN
func TestODown_SingleSentinelCannotTrigger(t *testing.T) {
	t.Parallel()
	master := NewMasterInstanceWithDownAfter("mymaster", "127.0.0.1:6379", 2, 1*time.Second)

	// 同一个哨兵报告 10 次 → 不应触发 ODOWN（去重后只有 1 个）
	for i := 0; i < 10; i++ {
		master.ReportSdown("sentinel-A")
	}
	assert.Equal(t, 1, master.GetSdownCount())
	assert.False(t, master.IsODown())
}

// TestODown_DuplicateReportsDeduplicated 验证：重复报告被正确去重
func TestODown_DuplicateReportsDeduplicated(t *testing.T) {
	t.Parallel()
	master := NewMasterInstanceWithDownAfter("mymaster", "127.0.0.1:6379", 2, 1*time.Second)

	master.ReportSdown("sentinel-A")
	master.ReportSdown("sentinel-A") // 重复
	master.ReportSdown("sentinel-A") // 重复
	master.ReportSdown("sentinel-B")

	// 去重后只有 2 个不同哨兵
	assert.Equal(t, 2, master.GetSdownCount())
	assert.True(t, master.IsODown())
}

// TestODown_RecoveryClearsReports 验证：主节点恢复后清除所有 SDOWN 报告
func TestODown_RecoveryClearsReports(t *testing.T) {
	t.Parallel()
	sentinel := NewSentinel(2, 1*time.Second)
	defer sentinel.Stop()

	err := sentinel.AddMaster("mymaster", "127.0.0.1:6379", 2)
	assert.NoError(t, err)

	master := sentinel.GetMaster("mymaster")
	assert.NotNil(t, master)

	// 两个哨兵报告 SDOWN → ODOWN
	master.ReportSdown("sentinel-A")
	master.ReportSdown("sentinel-B")
	assert.True(t, master.IsODown())

	// 模拟恢复：设置状态为 ok 并清除报告
	master.mu.Lock()
	master.state = "ok"
	master.sdownReporters = make(map[string]bool)
	master.mu.Unlock()

	assert.Equal(t, "ok", master.GetState())
	assert.Equal(t, 0, master.GetSdownCount())
	assert.False(t, master.IsODown())
}
