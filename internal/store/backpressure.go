package store

import (
	"math"
	"sync/atomic"
	"time"

	"github.com/lbp0200/BoltDB/internal/logger"
)

const (
	defaultMaxConcurrentWrites = 50
	defaultL0SoftThreshold     = 8.0
	defaultL0HardThreshold     = 20.0
	defaultMaxPreDelay         = 1 * time.Second
	l0CacheInterval            = 100 * time.Millisecond
)

// BackpressureConfig 主动背压配置
type BackpressureConfig struct {
	Enabled             bool
	MaxConcurrentWrites int
	L0SoftThreshold     float64
	L0HardThreshold     float64
	MaxPreDelay         time.Duration
}

// DefaultBackpressureConfig 返回默认背压配置
// MaxConcurrentWrites=50 防止 retry goroutine 雪崩
// L0SoftThreshold=8: score>8 开始延迟写入
// L0HardThreshold=20: score>20 拒绝写入
func DefaultBackpressureConfig() BackpressureConfig {
	return BackpressureConfig{
		Enabled:             true,
		MaxConcurrentWrites: defaultMaxConcurrentWrites,
		L0SoftThreshold:     defaultL0SoftThreshold,
		L0HardThreshold:     defaultL0HardThreshold,
		MaxPreDelay:         defaultMaxPreDelay,
	}
}

// writeSlot 限制并发写入的简易信号量
type writeSlot struct {
	ch chan struct{}
}

func newWriteSlot(max int) *writeSlot {
	return &writeSlot{ch: make(chan struct{}, max)}
}

func (ws *writeSlot) Acquire() {
	ws.ch <- struct{}{}
}

func (ws *writeSlot) Release() {
	<-ws.ch
}

// l0Cache 缓存 BadgerDB L0 score，避免每次写入都查询 level manifest
type l0Cache struct {
	score     atomic.Value // float64
	lastCheck atomic.Int64 // unix nano
}

func (c *l0Cache) get() (float64, bool) {
	s, ok := c.score.Load().(float64)
	if !ok {
		return 0, false
	}
	return s, true
}

func (c *l0Cache) set(s float64) {
	c.score.Store(s)
	c.lastCheck.Store(time.Now().UnixNano())
}

// L0Score 返回当前 BadgerDB L0 层的 compaction score
func (s *BotreonStore) L0Score() float64 {
	levels := s.db.Levels()
	for _, l := range levels {
		if l.Level == 0 {
			return l.Score
		}
	}
	return 0
}

// l0ScoreCached 返回缓存的 L0 score，超过 l0CacheInterval 时重新查询
func (s *BotreonStore) l0ScoreCached() float64 {
	if s.l0Cache == nil {
		return s.L0Score()
	}
	now := time.Now().UnixNano()
	last := s.l0Cache.lastCheck.Load()
	if now-last < l0CacheInterval.Nanoseconds() {
		if v, ok := s.l0Cache.get(); ok {
			return v
		}
	}
	score := s.L0Score()
	s.l0Cache.set(score)
	return score
}

// preWriteCheck 在调用 db.Update 前执行主动背压检查
// 返回 (preDelay, shouldReject)
// preDelay > 0 表示应等待后再写入
// shouldReject == true 表示应拒绝本次写入
func (s *BotreonStore) preWriteCheck() (time.Duration, bool) {
	if s.backpressure == nil {
		return 0, false
	}

	cfg := s.bpConfig
	if !cfg.Enabled {
		return 0, false
	}

	score := s.l0ScoreCached()

	if score >= cfg.L0HardThreshold && cfg.L0HardThreshold > 0 {
		logger.Logger.Warn().
			Float64("l0_score", score).
			Float64("hard_threshold", cfg.L0HardThreshold).
			Msg("L0 score 超过硬阈值，拒绝写入")
		return 0, true
	}

	if score > cfg.L0SoftThreshold && cfg.L0SoftThreshold > 0 {
		ratio := (score - cfg.L0SoftThreshold) / (cfg.L0HardThreshold - cfg.L0SoftThreshold)
		ratio = math.Min(ratio, 1.0)
		delay := time.Duration(ratio * float64(cfg.MaxPreDelay))
		logger.Logger.Debug().
			Float64("l0_score", score).
			Float64("soft_threshold", cfg.L0SoftThreshold).
			Dur("pre_delay", delay).
			Msg("L0 score 超过软阈值，延迟写入")
		return delay, false
	}

	return 0, false
}

// RetryMetrics 记录重试和背压的运行时指标
type RetryMetrics struct {
	ActiveRetries int64
	TotalRetries  int64
	WritesBlocked int64
	L0Rejected    int64
	L0Delayed     int64
	LastL0Score   float64
}

func (s *BotreonStore) GetRetryMetrics() RetryMetrics {
	s.retryMu.Lock()
	defer s.retryMu.Unlock()
	return RetryMetrics{
		ActiveRetries: s.retryMetrics.activeRetries,
		TotalRetries:  s.retryMetrics.totalRetries,
		WritesBlocked: s.retryMetrics.writesBlocked,
		L0Rejected:    s.retryMetrics.l0Rejected,
		L0Delayed:     s.retryMetrics.l0Delayed,
		LastL0Score:   s.l0ScoreCached(),
	}
}
