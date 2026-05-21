package server

func (h *Handler) ActiveClientCount() int {
	h.connsMu.RLock()
	defer h.connsMu.RUnlock()
	return len(h.conns)
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
