// Package monitor provides system pressure monitoring and degradation assertions
// for soak tests and regression replay.
package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lbp0200/BoltDB/internal/replication"
	"github.com/lbp0200/BoltDB/internal/store"
)

// TestingT 是 testing.T 的迷你接口，使 internal 包可在测试代码中使用
type TestingT interface {
	Log(args ...any)
	Logf(format string, args ...any)
	Errorf(format string, args ...any)
	FailNow()
}

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

type jsonlSample struct {
	Timestamp       int64   `json:"ts"`
	Goroutines      int     `json:"go"`
	HeapInuseMB     float64 `json:"heap_mb"`
	HeapAllocMB     float64 `json:"alloc_mb"`
	NumGC           int32   `json:"gc"`
	ActiveRetries   int64   `json:"ar"`
	TotalRetries    int64   `json:"tr"`
	WritesBlocked   int64   `json:"wb"`
	L0Rejected      int64   `json:"rj"`
	L0Delayed       int64   `json:"dl"`
	LastL0Score     float64 `json:"l0"`
	MasterOffset    int64   `json:"mo,omitempty"`
	SlaveOffset     int64   `json:"so,omitempty"`
	BacklogSize     int64   `json:"bl,omitempty"`
	ReconnectCount  int64   `json:"rc,omitempty"`
	ConnectedSlaves int     `json:"sl,omitempty"`
}

// PressureMonitor 定时采样系统压力指标，支持退化不变性断言
type PressureMonitor struct {
	mu      sync.Mutex
	samples []PressureSample
	stopped atomic.Bool

	store *store.BotreonStore
	replM *replication.ReplicationManager

	interval  time.Duration
	jsonlFile *os.File
}

// NewPressureMonitor 创建压力监控器
func NewPressureMonitor(s *store.BotreonStore, r *replication.ReplicationManager) *PressureMonitor {
	return &PressureMonitor{
		store: s,
		replM: r,
	}
}

// SetJSONLPath 启用 JSONL 时间线输出到指定文件（在 Start 之前调用）
func (pm *PressureMonitor) SetJSONLPath(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	pm.jsonlFile = f
	return nil
}

// Start 启动后台采样 goroutine
func (pm *PressureMonitor) Start(ctx context.Context, interval time.Duration) {
	pm.interval = interval
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		if pm.jsonlFile != nil {
			defer func() { _ = pm.jsonlFile.Close() }()
		}

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

// Stopped 返回监控器是否已停止
func (pm *PressureMonitor) Stopped() bool {
	return pm.stopped.Load()
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

	fmt.Println(FormatSnapshot(s))

	if pm.jsonlFile != nil {
		js := jsonlSample{
			Timestamp:       s.Timestamp.UnixNano(),
			Goroutines:      s.Goroutines,
			HeapInuseMB:     float64(s.Mem.HeapInuse) / 1024 / 1024,
			HeapAllocMB:     float64(s.Mem.HeapAlloc) / 1024 / 1024,
			NumGC:           s.NumGC,
			ActiveRetries:   s.ActiveRetries,
			TotalRetries:    s.TotalRetries,
			WritesBlocked:   s.WritesBlocked,
			L0Rejected:      s.L0Rejected,
			L0Delayed:       s.L0Delayed,
			LastL0Score:     s.LastL0Score,
			MasterOffset:    s.MasterOffset,
			SlaveOffset:     s.SlaveOffset,
			BacklogSize:     s.BacklogSize,
			ReconnectCount:  s.ReconnectCount,
			ConnectedSlaves: s.ConnectedSlaves,
		}
		line, err := json.Marshal(js)
		if err == nil {
			line = append(line, '\n')
			_, _ = pm.jsonlFile.Write(line)
		}
	}
}

// FormatSnapshot 格式化单次采样为一行日志
func FormatSnapshot(s PressureSample) string {
	var b strings.Builder
	fmt.Fprintf(&b, "[pm] t=%s go=%d", s.Timestamp.Format("15:04:05"), s.Goroutines)
	fmt.Fprintf(&b, " heap=%.1fM", float64(s.Mem.HeapInuse)/1024/1024)
	fmt.Fprintf(&b, " alloc=%.1fM", float64(s.Mem.HeapAlloc)/1024/1024)
	fmt.Fprintf(&b, " gc=%d", s.NumGC)
	fmt.Fprintf(&b, " L0=%.1f ar=%d tr=%d wb=%d rj=%d dl=%d",
		s.LastL0Score, s.ActiveRetries, s.TotalRetries, s.WritesBlocked, s.L0Rejected, s.L0Delayed)
	extra := s.ActiveRetries + s.L0Rejected + s.L0Delayed
	fmt.Fprintf(&b, " pressure=%d", extra)
	if s.MasterOffset > 0 || s.SlaveOffset > 0 {
		fmt.Fprintf(&b, " mo=%d so=%d backlog=%d recon=%d slaves=%d",
			s.MasterOffset, s.SlaveOffset, s.BacklogSize, s.ReconnectCount, s.ConnectedSlaves)
	}
	return b.String()
}

// LogSummary 输出所有采样的统计摘要
func (pm *PressureMonitor) LogSummary(t TestingT) {
	samples := pm.Samples()
	if len(samples) == 0 {
		t.Log("[pm] no samples collected")
		return
	}

	var maxGoroutines, maxL0Score int
	var maxActiveRetries, totalRetries, totalRejected, totalDelayed int64

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
	}
	if len(samples) > 0 {
		last := samples[len(samples)-1]
		totalRetries = last.TotalRetries
		totalRejected = last.L0Rejected
		totalDelayed = last.L0Delayed
	}

	t.Log("[pm] === Pressure Summary ===")
	t.Logf("[pm]   samples: %d over %s", len(samples), pm.interval)
	t.Logf("[pm]   goroutines: max=%d final=%d", maxGoroutines, samples[len(samples)-1].Goroutines)
	t.Logf("[pm]   heap: final=%.1fM peak=%.1fM",
		float64(samples[len(samples)-1].Mem.HeapInuse)/1024/1024,
		float64(MaxHeap(samples))/1024/1024)
	t.Logf("[pm]   L0 score: max=%d final=%.1f", maxL0Score, samples[len(samples)-1].LastL0Score)
	t.Logf("[pm]   backpressure: activeRetries(max)=%d totalRetries=%d rejected=%d delayed=%d",
		maxActiveRetries, totalRetries, totalRejected, totalDelayed)
}

// HealthScore 基于所有采样计算系统健康评分
func (pm *PressureMonitor) HealthScore(baselineGoroutines int) HealthScore {
	return ComputeHealth(pm.Samples(), baselineGoroutines)
}

// MaxHeap 返回所有采样中的峰值堆使用
func MaxHeap(samples []PressureSample) uint64 {
	var m uint64
	for _, s := range samples {
		if s.Mem.HeapInuse > m {
			m = s.Mem.HeapInuse
		}
	}
	return m
}
