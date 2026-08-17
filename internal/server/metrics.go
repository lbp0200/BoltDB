package server

import "sync/atomic"

func (h *Handler) ActiveClientCount() int {
	h.connsMu.RLock()
	defer h.connsMu.RUnlock()
	return len(h.conns)
}

// GetMaxClients 返回有效的最大连接数（未配置时默认 10000）
func (h *Handler) GetMaxClients() int {
	if h.MaxClients > 0 {
		return h.MaxClients
	}
	return 10000
}

func (h *Handler) BlockedClientCount() int {
	if h.Db == nil {
		return 0
	}
	return h.Db.GetBlockedClientCount()
}

func (h *Handler) MonitorClientCount() int {
	h.monitorMu.Lock()
	defer h.monitorMu.Unlock()
	return len(h.monitorClients)
}

func (h *Handler) PubSubClientCount() int {
	h.connsMu.RLock()
	defer h.connsMu.RUnlock()
	count := 0
	for state := range h.conns {
		state.mu.Lock()
		if state.subscriber != nil {
			count++
		}
		state.mu.Unlock()
	}
	return count
}

func (h *Handler) TotalOutputBytes() int64 {
	h.connsMu.RLock()
	defer h.connsMu.RUnlock()
	var total int64
	for _, meta := range h.conns {
		total += meta.outputBytes
	}
	return total
}

// TotalInputBytes 返回所有连接累计读取的字节总数
// （CumulativeLimitReader.Total 汇总，CLUSTER CALLS NetInputBytes 数据源）。
func (h *Handler) TotalInputBytes() int64 {
	h.connsMu.RLock()
	defer h.connsMu.RUnlock()
	var total int64
	for _, meta := range h.conns {
		if meta.limitReader != nil {
			total += meta.limitReader.Total()
		}
	}
	return total
}

// TotalCommandsProcessed 返回所有命令累计执行次数
// （CLUSTER CALLS CommandsProcessed 数据源）。
func (h *Handler) TotalCommandsProcessed() int64 {
	if h.cmdCounters == nil {
		return 0
	}
	h.cmdCountersMu.Lock()
	defer h.cmdCountersMu.Unlock()
	var total int64
	for _, c := range h.cmdCounters {
		total += c.Load()
	}
	return total
}

// incrementCmdCounter 按命令名称递增调用计数器。
// 线程安全，计数器在首次访问时惰性初始化。
func (h *Handler) incrementCmdCounter(cmd string) {
	if h.cmdCounters == nil {
		return
	}
	h.cmdCountersMu.Lock()
	counter, exists := h.cmdCounters[cmd]
	if !exists {
		counter = new(atomic.Int64)
		h.cmdCounters[cmd] = counter
	}
	h.cmdCountersMu.Unlock()
	counter.Add(1)
}

// GetCmdCount 返回指定命令的调用次数。
// 如果该命令从未被调用或计数未启用，返回 0。
func (h *Handler) GetCmdCount(cmd string) int64 {
	if h.cmdCounters == nil {
		return 0
	}
	h.cmdCountersMu.Lock()
	defer h.cmdCountersMu.Unlock()
	counter, exists := h.cmdCounters[cmd]
	if !exists {
		return 0
	}
	return counter.Load()
}
