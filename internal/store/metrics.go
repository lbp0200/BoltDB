package store

func (psm *PubSubManager) GetTotalSubscriberCount() int {
	psm.mu.RLock()
	defer psm.mu.RUnlock()
	return len(psm.subscribers)
}

func (s *BotreonStore) GetBlockedClientCount() int {
	s.blockingMu.RLock()
	defer s.blockingMu.RUnlock()
	total := 0
	for _, chans := range s.blockingPopChans {
		total += len(chans)
	}
	return total
}
