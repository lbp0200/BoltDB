package store

import (
	"context"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// BLPOP 实现 Redis BLPOP 命令
func (s *BotreonStore) BLPOP(keys []string, timeout int) (string, string, error) {
	for _, key := range keys {
		value, err := s.LPop(key)
		if err == nil && value != "" {
			return key, value, nil
		}
	}
	return "", "", nil
}

// BRPOP 实现 Redis BRPOP 命令
func (s *BotreonStore) BRPOP(keys []string, timeout int) (string, string, error) {
	for _, key := range keys {
		value, err := s.RPop(key)
		if err == nil && value != "" {
			return key, value, nil
		}
	}
	return "", "", nil
}

// BRPOPLPUSH 实现 Redis BRPOPLPUSH 命令
func (s *BotreonStore) BRPOPLPUSH(source, destination string, timeout int) (string, error) {
	return s.RPopLPush(source, destination)
}

// BLMPopBlocking 实现 Redis BLMPOP 命令：阻塞式从多个 list 按序弹出元素。
// 阻塞等待被唤醒时只弹出 1 个元素（Redis 语义：阻塞时 COUNT 被忽略）。
func (s *BotreonStore) BLMPopBlocking(ctx context.Context, keys []string, fromLeft bool, count int, timeout int) (string, []string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	popOne := func(key string) (string, error) {
		if fromLeft {
			return s.LPop(key)
		}
		return s.RPop(key)
	}

	// 立即尝试：按序对每个 key 弹出最多 count 个
	for _, key := range keys {
		values := make([]string, 0, count)
		for i := 0; i < count; i++ {
			v, err := popOne(key)
			if err != nil {
				return "", nil, err
			}
			if v == "" {
				break
			}
			values = append(values, v)
		}
		if len(values) > 0 {
			return key, values, nil
		}
	}

	// 阻塞等待：唤醒时只弹出 1 个（Redis 语义）
	resultCh := make(chan BlockingResult, 1)
	var timerCh <-chan time.Time
	if timeout > 0 {
		timer := time.NewTimer(time.Duration(timeout) * time.Second)
		defer func() {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}()
		timerCh = timer.C
	}

	if key, value, ok := s.registerAndRecheck(keys, resultCh, popOne); ok {
		return key, []string{value}, nil
	}

	select {
	case result := <-resultCh:
		return result.Key, []string{result.Value}, nil
	case <-timerCh:
		s.unregisterBlockingPop(resultCh, keys)
		return "", nil, nil
	case <-ctx.Done():
		s.unregisterBlockingPop(resultCh, keys)
		return "", nil, nil
	case <-s.closeCh:
		s.unregisterBlockingPop(resultCh, keys)
		return "", nil, nil
	}
}

// LMove 实现 Redis LMOVE 命令
func (s *BotreonStore) LMove(source, destination, sourceDirection, destinationDirection string) (string, error) {
	var fromLeft, toLeft bool
	switch sourceDirection {
	case "LEFT":
		fromLeft = true
	case "RIGHT":
		fromLeft = false
	default:
		return "", fmt.Errorf("ERR wrong source direction argument")
	}
	switch destinationDirection {
	case "LEFT":
		toLeft = true
	case "RIGHT":
		toLeft = false
	default:
		return "", fmt.Errorf("ERR wrong destination direction argument")
	}

	unlock := s.lockListKeysOrdered(source, destination)
	defer unlock()

	var value string
	err := s.retryUpdate(func(txn *badger.Txn) error {
		value = ""
		if err := checkKeyType(txn, source, KeyTypeList); err != nil {
			return err
		}
		popped, err := s.listPopInTxn(txn, source, fromLeft)
		if err != nil {
			return err
		}
		if popped == "" {
			return nil
		}
		value = popped
		return s.listPushInTxn(txn, destination, value, toLeft)
	}, 30)
	if err == nil && value != "" {
		s.notifyBlockingPop(destination, value)
	}
	return value, err
}

// BLMove 实现 Redis BLMOVE 命令
func (s *BotreonStore) BLMove(source, destination, sourceDirection, destinationDirection string, timeout float64) (string, error) {
	return s.LMove(source, destination, sourceDirection, destinationDirection)
}

// notifyBlockingPop notifies one waiting channel for a key
func (s *BotreonStore) notifyBlockingPop(key, value string) {
	s.blockingMu.Lock()
	defer s.blockingMu.Unlock()

	chans, exists := s.blockingPopChans[key]
	if !exists || len(chans) == 0 {
		return
	}

	select {
	case chans[0] <- BlockingResult{Key: key, Value: value}:
		s.blockingPopChans[key] = chans[1:]
	default:
	}
}

// registerBlockingPop registers a channel to wait for a key
func (s *BotreonStore) registerBlockingPop(key string, ch chan BlockingResult) {
	s.blockingMu.Lock()
	defer s.blockingMu.Unlock()

	s.blockingPopChans[key] = append(s.blockingPopChans[key], ch)
}

// unregisterBlockingPop removes a specific channel from all keys' wait lists
func (s *BotreonStore) unregisterBlockingPop(ch chan BlockingResult, keys []string) {
	s.blockingMu.Lock()
	defer s.blockingMu.Unlock()

	for _, key := range keys {
		chans := s.blockingPopChans[key]
		for j, c := range chans {
			if c == ch {
				s.blockingPopChans[key] = append(chans[:j], chans[j+1:]...)
				break
			}
		}
		if len(s.blockingPopChans[key]) == 0 {
			delete(s.blockingPopChans, key)
		}
	}
}

// registerAndRecheck registers a channel for keys and re-checks for data after registration.
func (s *BotreonStore) registerAndRecheck(keys []string, ch chan BlockingResult, popFn func(string) (string, error)) (string, string, bool) {
	s.blockingMu.Lock()
	for _, key := range keys {
		s.blockingPopChans[key] = append(s.blockingPopChans[key], ch)
	}
	s.blockingMu.Unlock()

	for _, key := range keys {
		value, err := popFn(key)
		if err == nil && value != "" {
			s.unregisterBlockingPop(ch, keys)
			return key, value, true
		}
	}
	return "", "", false
}

// BLPOPBlocking implements blocking left pop with timeout
func (s *BotreonStore) BLPOPBlocking(ctx context.Context, keys []string, timeout int) (string, string, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	for _, key := range keys {
		value, err := s.LPop(key)
		if err == nil && value != "" {
			return key, value, nil
		}
	}

	resultCh := make(chan BlockingResult, 1)
	var timerCh <-chan time.Time
	if timeout > 0 {
		timer := time.NewTimer(time.Duration(timeout) * time.Second)
		defer func() {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}()
		timerCh = timer.C
	}

	if key, value, ok := s.registerAndRecheck(keys, resultCh, s.LPop); ok {
		return key, value, nil
	}

	select {
	case result := <-resultCh:
		return result.Key, result.Value, nil
	case <-timerCh:
		s.unregisterBlockingPop(resultCh, keys)
		return "", "", nil
	case <-ctx.Done():
		s.unregisterBlockingPop(resultCh, keys)
		return "", "", nil
	case <-s.closeCh:
		s.unregisterBlockingPop(resultCh, keys)
		return "", "", nil
	}
}

// BRPOPBlocking implements blocking right pop with timeout
func (s *BotreonStore) BRPOPBlocking(ctx context.Context, keys []string, timeout int) (string, string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for _, key := range keys {
		value, err := s.RPop(key)
		if err == nil && value != "" {
			return key, value, nil
		}
	}

	resultCh := make(chan BlockingResult, 1)
	var timerCh <-chan time.Time
	if timeout > 0 {
		timer := time.NewTimer(time.Duration(timeout) * time.Second)
		defer func() {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}()
		timerCh = timer.C
	}

	if key, value, ok := s.registerAndRecheck(keys, resultCh, s.RPop); ok {
		return key, value, nil
	}

	select {
	case result := <-resultCh:
		return result.Key, result.Value, nil
	case <-timerCh:
		s.unregisterBlockingPop(resultCh, keys)
		return "", "", nil
	case <-ctx.Done():
		s.unregisterBlockingPop(resultCh, keys)
		return "", "", nil
	case <-s.closeCh:
		s.unregisterBlockingPop(resultCh, keys)
		return "", "", nil
	}
}

// BRPOPLPUSHBlocking implements blocking rpoplpush with timeout
func (s *BotreonStore) BRPOPLPUSHBlocking(ctx context.Context, source, destination string, timeout int) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if timeout == 0 {
		value, err := s.RPopLPush(source, destination)
		if err != nil || value == "" {
			return "", nil
		}
		return value, nil
	}

	value, err := s.RPopLPush(source, destination)
	if err == nil && value != "" {
		return value, nil
	}

	resultCh := make(chan BlockingResult, 1)
	keys := []string{source}

	s.blockingMu.Lock()
	s.blockingPopChans[source] = append(s.blockingPopChans[source], resultCh)
	s.blockingMu.Unlock()

	value, err = s.RPopLPush(source, destination)
	if err == nil && value != "" {
		s.unregisterBlockingPop(resultCh, keys)
		return value, nil
	}

	timeoutDur := time.Duration(timeout) * time.Second
	timer := time.NewTimer(timeoutDur)
	defer func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}()

	for {
		select {
		case <-resultCh:
			value, err := s.RPopLPush(source, destination)
			if err == nil && value != "" {
				return value, nil
			}
			s.registerBlockingPop(source, resultCh)
		case <-timer.C:
			s.unregisterBlockingPop(resultCh, keys)
			return "", nil
		case <-ctx.Done():
			s.unregisterBlockingPop(resultCh, keys)
			return "", nil
		case <-s.closeCh:
			s.unregisterBlockingPop(resultCh, keys)
			return "", nil
		}
	}
}

// BLMoveBlocking implements blocking lmove with timeout
func (s *BotreonStore) BLMoveBlocking(ctx context.Context, source, destination, sourceDirection, destinationDirection string, timeout float64) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if timeout == 0 {
		return s.LMove(source, destination, sourceDirection, destinationDirection)
	}

	value, err := s.LMove(source, destination, sourceDirection, destinationDirection)
	if err == nil && value != "" {
		return value, nil
	}

	resultCh := make(chan BlockingResult, 1)
	keys := []string{source}

	s.blockingMu.Lock()
	s.blockingPopChans[source] = append(s.blockingPopChans[source], resultCh)
	s.blockingMu.Unlock()

	value, err = s.LMove(source, destination, sourceDirection, destinationDirection)
	if err == nil && value != "" {
		s.unregisterBlockingPop(resultCh, keys)
		return value, nil
	}

	timeoutDur := time.Duration(timeout) * time.Second
	timer := time.NewTimer(timeoutDur)
	defer func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}()

	for {
		select {
		case <-resultCh:
			value, err := s.LMove(source, destination, sourceDirection, destinationDirection)
			if err == nil && value != "" {
				return value, nil
			}
			s.registerBlockingPop(source, resultCh)
		case <-timer.C:
			s.unregisterBlockingPop(resultCh, keys)
			return "", nil
		case <-ctx.Done():
			s.unregisterBlockingPop(resultCh, keys)
			return "", nil
		}
	}
}
