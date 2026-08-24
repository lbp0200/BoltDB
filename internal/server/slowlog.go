package server

import (
	"sync"
	"time"
)

// slowlogEntry 是单条慢日志（Redis SLOWLOG 语义的最小可用子集）。
type slowlogEntry struct {
	id         int64
	timestamp  int64 // unix seconds
	duration   int64 // microseconds
	args       [][]byte
	clientAddr string
	clientName string
}

const (
	defaultSlowlogThreshold = 10000 // microseconds (10ms), Redis 默认
	defaultSlowlogMaxLen    = 128
	maxSlowlogArgLen        = 128
	maxSlowlogArgs          = 32
)

// slowlogState 维护内存慢日志环形缓冲与阈值配置。
type slowlogState struct {
	mu        sync.Mutex
	threshold int64 // 微秒；0 = 记录所有，<0 = 禁用（与 Redis 一致）
	maxLen    int
	nextID    int64
	entries   []slowlogEntry // 新→旧
}

func newSlowlogState() *slowlogState {
	return &slowlogState{
		threshold: defaultSlowlogThreshold,
		maxLen:    defaultSlowlogMaxLen,
	}
}

func (s *slowlogState) setThreshold(us int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.threshold = us
}

func (s *slowlogState) setMaxLen(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maxLen = n
	if s.maxLen < 0 {
		s.maxLen = 0
	}
	if len(s.entries) > s.maxLen {
		s.entries = s.entries[:s.maxLen]
	}
}

func (s *slowlogState) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = nil
}

func (s *slowlogState) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries)
}

func (s *slowlogState) get(n int) []slowlogEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n <= 0 || n > len(s.entries) {
		n = len(s.entries)
	}
	out := make([]slowlogEntry, n)
	copy(out, s.entries[:n])
	return out
}

// EnsureSlowlog 保证 Handler.slowlog 非空（兼容手写 &Handler{} 的零星测试与 main 构造）。
func (h *Handler) EnsureSlowlog() {
	if h.slowlog == nil {
		h.slowlog = newSlowlogState()
	}
}

// EnsureStartTime 保证 Handler.startTime 非零（用于 INFO uptime 真值）。
func (h *Handler) EnsureStartTime() {
	if h.startTime.IsZero() {
		h.startTime = time.Now()
	}
}

func (h *Handler) ensureSlowlog() *slowlogState {
	if h.slowlog == nil {
		h.slowlog = newSlowlogState()
	}
	return h.slowlog
}

func (h *Handler) uptimeSeconds() int64 {
	if h.startTime.IsZero() {
		return 0
	}
	return int64(time.Since(h.startTime).Seconds())
}

// add 记录一条慢日志，需在命令执行后调用。
func (s *slowlogState) add(duration time.Duration, args [][]byte, clientAddr, clientName string) {
	us := duration.Microseconds()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.threshold < 0 {
		return
	}
	if s.maxLen == 0 {
		return
	}
	if us < s.threshold {
		return
	}
	// 截断参数，避免大 bulk 撑爆内存。
	copied := make([][]byte, 0, len(args))
	for i, a := range args {
		if i >= maxSlowlogArgs {
			break
		}
		b := make([]byte, len(a))
		copy(b, a)
		if len(b) > maxSlowlogArgLen {
			b = b[:maxSlowlogArgLen]
		}
		copied = append(copied, b)
	}
	e := slowlogEntry{
		id:         s.nextID,
		timestamp:  time.Now().Unix(),
		duration:   us,
		args:       copied,
		clientAddr: clientAddr,
		clientName: clientName,
	}
	s.nextID++
	s.entries = append([]slowlogEntry{e}, s.entries...)
	if len(s.entries) > s.maxLen {
		s.entries = s.entries[:s.maxLen]
	}
}
