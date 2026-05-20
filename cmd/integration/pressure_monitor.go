package integration

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/replication"
	"github.com/lbp0200/BoltDB/internal/store"
)

// PressureSample 一次压力采样的所有指标
type PressureSample struct {
	Timestamp  time.Time
	Goroutines int
	NumGC      int32
	Mem        runtime.MemStats

	// Backpressure from BotreonStore
	ActiveRetries int64
	TotalRetries  int64
	WritesBlocked int64
	L0Rejected    int64
	L0Delayed     int64
	LastL0Score   float64

	// Replication
	MasterOffset    int64
	SlaveOffset     int64
	BacklogSize     int64
	ReconnectCount  int64
	ConnectedSlaves int
}

// PressureMonitor 定时采样系统压力指标，支持退化不变性断言
type PressureMonitor struct {
	mu      sync.Mutex
	samples []PressureSample
	stopped atomic.Bool

	store *store.BotreonStore
	replM *replication.ReplicationManager

	interval time.Duration
}

// NewPressureMonitor 创建压力监控器
// store 和 repl 可以为 nil，对应的指标会填零值
func NewPressureMonitor(s *store.BotreonStore, r *replication.ReplicationManager) *PressureMonitor {
	return &PressureMonitor{
		store: s,
		replM: r,
	}
}

// Start 启动后台采样 goroutine
func (pm *PressureMonitor) Start(ctx context.Context, interval time.Duration) {
	pm.interval = interval
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		// 首次立即采样
		pm.sample()

		for {
			select {
			case <-ctx.Done():
				pm.stopped.Store(true)
				return
			case <-ticker.C:
				pm.sample()
			}
		}
	}()
}

// Samples 返回所有采样（线程安全快照）
func (pm *PressureMonitor) Samples() []PressureSample {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	c := make([]PressureSample, len(pm.samples))
	copy(c, pm.samples)
	return c
}

// Latest 返回最近一次采样
func (pm *PressureMonitor) Latest() PressureSample {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if len(pm.samples) == 0 {
		return PressureSample{}
	}
	return pm.samples[len(pm.samples)-1]
}

func (pm *PressureMonitor) sample() {
	var s PressureSample
	s.Timestamp = time.Now()
	s.Goroutines = runtime.NumGoroutine()
	runtime.ReadMemStats(&s.Mem)
	s.NumGC = int32(s.Mem.NumGC)

	if pm.store != nil {
		m := pm.store.GetRetryMetrics()
		s.ActiveRetries = m.ActiveRetries
		s.TotalRetries = m.TotalRetries
		s.WritesBlocked = m.WritesBlocked
		s.L0Rejected = m.L0Rejected
		s.L0Delayed = m.L0Delayed
		s.LastL0Score = m.LastL0Score
	}

	if pm.replM != nil {
		s.MasterOffset = pm.replM.GetMasterReplOffset()
		s.SlaveOffset = pm.replM.GetSlaveReplOffset()
		s.ReconnectCount = pm.replM.GetReconnectCount()
		s.ConnectedSlaves = pm.replM.GetSlaveCount()
		if bg := pm.replM.GetBacklog(); bg != nil {
			s.BacklogSize = bg.GetSize()
		}
	}

	pm.mu.Lock()
	pm.samples = append(pm.samples, s)
	pm.mu.Unlock()

	fmt.Println(pm.formatSnapshot(s))
}

func (pm *PressureMonitor) formatSnapshot(s PressureSample) string {
	var b strings.Builder
	fmt.Fprintf(&b, "[pm] t=%s go=%d", s.Timestamp.Format("15:04:05"), s.Goroutines)
	fmt.Fprintf(&b, " heap=%.1fM", float64(s.Mem.HeapInuse)/1024/1024)
	fmt.Fprintf(&b, " alloc=%.1fM", float64(s.Mem.HeapAlloc)/1024/1024)
	fmt.Fprintf(&b, " gc=%d", s.NumGC)
	if pm.store != nil {
		fmt.Fprintf(&b, " L0=%.1f ar=%d tr=%d wb=%d rj=%d dl=%d",
			s.LastL0Score, s.ActiveRetries, s.TotalRetries, s.WritesBlocked, s.L0Rejected, s.L0Delayed)
		extra := s.ActiveRetries + s.L0Rejected + s.L0Delayed
		fmt.Fprintf(&b, " pressure=%d", extra)
	}
	if pm.replM != nil {
		fmt.Fprintf(&b, " mo=%d so=%d backlog=%d recon=%d slaves=%d",
			s.MasterOffset, s.SlaveOffset, s.BacklogSize, s.ReconnectCount, s.ConnectedSlaves)
	}
	return b.String()
}

// LogSummary 输出所有采样的统计摘要
func (pm *PressureMonitor) LogSummary(t *testing.T) {
	samples := pm.Samples()
	if len(samples) == 0 {
		t.Log("[pm] no samples collected")
		return
	}

	// 汇总统计
	var maxGoroutines, maxL0Score int
	var maxActiveRetries, totalRetries, totalRejected, totalDelayed int64
	var maxBacklog int64
	reconnectFinal := int64(0)

	for _, s := range samples {
		if s.Goroutines > maxGoroutines {
			maxGoroutines = s.Goroutines
		}
		if int(s.LastL0Score) > maxL0Score {
			maxL0Score = int(s.LastL0Score)
		}
		if s.ActiveRetries > maxActiveRetries {
			maxActiveRetries = s.ActiveRetries
		}
		if s.BacklogSize > maxBacklog {
			maxBacklog = s.BacklogSize
		}
		reconnectFinal = s.ReconnectCount
	}
	if len(samples) > 0 {
		last := samples[len(samples)-1]
		totalRetries = last.TotalRetries
		totalRejected = last.L0Rejected
		totalDelayed = last.L0Delayed
	}

	t.Logf("[pm] === Pressure Summary ===")
	t.Logf("[pm]   samples: %d over %s", len(samples), pm.interval)
	t.Logf("[pm]   goroutines: max=%d final=%d", maxGoroutines, samples[len(samples)-1].Goroutines)
	t.Logf("[pm]   heap: final=%.1fM peak=%.1fM",
		float64(samples[len(samples)-1].Mem.HeapInuse)/1024/1024,
		float64(maxHeap(samples))/1024/1024)
	t.Logf("[pm]   L0 score: max=%d final=%.1f", maxL0Score, samples[len(samples)-1].LastL0Score)
	t.Logf("[pm]   backpressure: activeRetries(max)=%d totalRetries=%d rejected=%d delayed=%d",
		maxActiveRetries, totalRetries, totalRejected, totalDelayed)
	if pm.replM != nil {
		t.Logf("[pm]   replication: reconnectCount=%d backlog(max)=%d",
			reconnectFinal, maxBacklog)
	}
}

func maxHeap(samples []PressureSample) uint64 {
	var m uint64
	for _, s := range samples {
		if s.Mem.HeapInuse > m {
			m = s.Mem.HeapInuse
		}
	}
	return m
}

// DegradationAssertion 退化不变性断言
type DegradationAssertion struct {
	MaxGoroutineDelta   int // 允许的 goroutine 增长上限（相对于基线）
	MaxActiveRetries    int64
	MaxL0Score          float64
	L0RecoveryThreshold float64 // soak 结束时 L0 score 必须低于此值
	MaxReconnectCount   int64   // 复制 soak 专用
	AllowDegraded       bool    // true 时允许部分退化（仅记录不 fail）
}

// DefaultDegradationAssertion 默认退化不变性断言
func DefaultDegradationAssertion() DegradationAssertion {
	return DegradationAssertion{
		MaxGoroutineDelta:   50,
		MaxActiveRetries:    100,
		MaxL0Score:          25,
		L0RecoveryThreshold: 10,
		MaxReconnectCount:   50,
	}
}

// CheckDegradation 执行退化不变性断言
// baselineGoroutines: soak 开始时的 goroutine 数量
func (pm *PressureMonitor) CheckDegradation(t *testing.T, a DegradationAssertion, baselineGoroutines int) {
	latest := pm.Latest()
	samples := pm.Samples()

	t.Logf("[pm] === Degradation Invariants ===")

	// 1. Goroutine 必须有界
	goDelta := latest.Goroutines - baselineGoroutines
	t.Logf("[pm]   goroutine delta: %d (threshold: %d)", goDelta, a.MaxGoroutineDelta)
	if goDelta > a.MaxGoroutineDelta {
		t.Errorf("DEGRADATION: goroutine unbounded growth: delta=%d > %d", goDelta, a.MaxGoroutineDelta)
	}

	// 2. Active retries 必须有界
	t.Logf("[pm]   max active retries: %d (threshold: %d)", latest.ActiveRetries, a.MaxActiveRetries)
	if latest.ActiveRetries > a.MaxActiveRetries {
		t.Errorf("DEGRADATION: active retries exceeded: %d > %d", latest.ActiveRetries, a.MaxActiveRetries)
	}

	// 3. L0 score 最终必须恢复
	t.Logf("[pm]   final L0 score: %.1f (recovery threshold: %.1f)", latest.LastL0Score, a.L0RecoveryThreshold)
	if latest.LastL0Score > a.L0RecoveryThreshold {
		if a.AllowDegraded {
			t.Logf("[pm]   WARN: L0 score did not fully recover (%.1f > %.1f)",
				latest.LastL0Score, a.L0RecoveryThreshold)
		} else {
			t.Errorf("DEGRADATION: L0 score did not recover: %.1f > %.1f",
				latest.LastL0Score, a.L0RecoveryThreshold)
		}
	}

	// 4. L0 score 历史最高不得超 Max
	var peakL0 float64
	for _, s := range samples {
		if s.LastL0Score > peakL0 {
			peakL0 = s.LastL0Score
		}
	}
	t.Logf("[pm]   peak L0 score: %.1f (threshold: %.1f)", peakL0, a.MaxL0Score)
	if peakL0 > a.MaxL0Score {
		t.Errorf("DEGRADATION: L0 score peak exceeded: %.1f > %.1f", peakL0, a.MaxL0Score)
	}

	// 5. 重连次数必须有界（复制 soak）
	if pm.replM != nil {
		t.Logf("[pm]   reconnect count: %d (threshold: %d)", latest.ReconnectCount, a.MaxReconnectCount)
		if latest.ReconnectCount > a.MaxReconnectCount {
			t.Errorf("DEGRADATION: excessive reconnects: %d > %d", latest.ReconnectCount, a.MaxReconnectCount)
		}
	}

	// 6. 检查是否存在单调增长趋势
	pm.checkMonotonicGrowth(t)
}

// checkMonotonicGrowth 检查指标是否存在持续增长（不可恢复退化）
func (pm *PressureMonitor) checkMonotonicGrowth(t *testing.T) {
	samples := pm.Samples()
	if len(samples) < 3 {
		return
	}

	// 检查最后 1/3 的采样中 L0 score 是否持续增长
	tail := samples[len(samples)/2:]
	rising := 0
	for i := 1; i < len(tail); i++ {
		if tail[i].LastL0Score > tail[i-1].LastL0Score {
			rising++
		}
	}
	// 如果后半段超过 70% 的采样都在增长，视为不可恢复退化
	if len(tail) > 0 && float64(rising)/float64(len(tail)) > 0.7 {
		t.Errorf("DEGRADATION: L0 score shows monotonic rising trend (%d/%d samples rising)",
			rising, len(tail))
	}
}
