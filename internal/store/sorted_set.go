package store

import (
	"context"
	"time"
)

// notifyBlockingZPop notifies one waiting channel for a sorted set key
func (s *BotreonStore) notifyBlockingZPop(key string) {
	s.blockingZPopMu.Lock()
	defer s.blockingZPopMu.Unlock()

	chans, exists := s.blockingZPopChans[key]
	if !exists || len(chans) == 0 {
		return
	}

	select {
	case chans[0] <- key:
		s.blockingZPopChans[key] = chans[1:]
	default:
	}
}

// unregisterBlockingZPop removes a specific channel from all keys' wait lists
func (s *BotreonStore) unregisterBlockingZPop(ch chan string, keys []string) {
	s.blockingZPopMu.Lock()
	defer s.blockingZPopMu.Unlock()

	for _, key := range keys {
		chans := s.blockingZPopChans[key]
		for j, c := range chans {
			if c == ch {
				s.blockingZPopChans[key] = append(chans[:j], chans[j+1:]...)
				break
			}
		}
		if len(s.blockingZPopChans[key]) == 0 {
			delete(s.blockingZPopChans, key)
		}
	}
}

// registerAndRecheckZMax registers a channel for keys and re-checks with ZPopMax
func (s *BotreonStore) registerAndRecheckZMax(keys []string, ch chan string) (string, *ZSetMember, bool) {
	s.blockingZPopMu.Lock()
	for _, key := range keys {
		s.blockingZPopChans[key] = append(s.blockingZPopChans[key], ch)
	}
	s.blockingZPopMu.Unlock()

	for _, key := range keys {
		members, err := s.ZPopMax(key, 1)
		if err == nil && len(members) > 0 {
			s.unregisterBlockingZPop(ch, keys)
			return key, &members[0], true
		}
	}
	return "", nil, false
}

// registerAndRecheckZMin registers a channel for keys and re-checks with ZPopMin
func (s *BotreonStore) registerAndRecheckZMin(keys []string, ch chan string) (string, *ZSetMember, bool) {
	s.blockingZPopMu.Lock()
	for _, key := range keys {
		s.blockingZPopChans[key] = append(s.blockingZPopChans[key], ch)
	}
	s.blockingZPopMu.Unlock()

	for _, key := range keys {
		members, err := s.ZPopMin(key, 1)
		if err == nil && len(members) > 0 {
			s.unregisterBlockingZPop(ch, keys)
			return key, &members[0], true
		}
	}
	return "", nil, false
}

// BZMPopBlocking 实现 Redis BZMPOP 命令，阻塞式从多个排序集合弹出成员
func (s *BotreonStore) BZMPopBlocking(ctx context.Context, keys []string, modifier string, count int, timeout int) (string, []ZSetMember, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	key, members, err := s.ZMPop(keys, modifier, count)
	if err != nil {
		return "", nil, err
	}
	if key != "" && len(members) > 0 {
		return key, members, nil
	}

	resultCh := make(chan string, 1)
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

	if modifier == "MAX" {
		if key, _, ok := s.registerAndRecheckZMax(keys, resultCh); ok {
			members, err := s.ZPopMax(key, count)
			if err != nil {
				return "", nil, err
			}
			return key, members, nil
		}
	} else {
		if key, _, ok := s.registerAndRecheckZMin(keys, resultCh); ok {
			members, err := s.ZPopMin(key, count)
			if err != nil {
				return "", nil, err
			}
			return key, members, nil
		}
	}

	select {
	case key := <-resultCh:
		if modifier == "MAX" {
			members, err = s.ZPopMax(key, count)
		} else {
			members, err = s.ZPopMin(key, count)
		}
		if err != nil || len(members) == 0 {
			return "", nil, nil
		}
		return key, members, nil
	case <-timerCh:
		s.unregisterBlockingZPop(resultCh, keys)
		return "", nil, nil
	case <-ctx.Done():
		s.unregisterBlockingZPop(resultCh, keys)
		return "", nil, nil
	}
}

// BZPopMaxBlocking 实现 Redis BZPOPMAX 命令，阻塞式弹出分数最高的成员
func (s *BotreonStore) BZPopMaxBlocking(ctx context.Context, keys []string, timeout int) (string, *ZSetMember, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	for _, key := range keys {
		members, err := s.ZPopMax(key, 1)
		if err != nil {
			return "", nil, err
		}
		if len(members) > 0 {
			return key, &members[0], nil
		}
	}

	resultCh := make(chan string, 1)
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

	if key, member, ok := s.registerAndRecheckZMax(keys, resultCh); ok {
		return key, member, nil
	}

	select {
	case key := <-resultCh:
		members, err := s.ZPopMax(key, 1)
		if err != nil || len(members) == 0 {
			return "", nil, nil
		}
		return key, &members[0], nil
	case <-timerCh:
		s.unregisterBlockingZPop(resultCh, keys)
		return "", nil, nil
	case <-ctx.Done():
		s.unregisterBlockingZPop(resultCh, keys)
		return "", nil, nil
	}
}

// BZPopMinBlocking 实现 Redis BZPOPMIN 命令，阻塞式弹出分数最低的成员
func (s *BotreonStore) BZPopMinBlocking(ctx context.Context, keys []string, timeout int) (string, *ZSetMember, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	for _, key := range keys {
		members, err := s.ZPopMin(key, 1)
		if err != nil {
			return "", nil, err
		}
		if len(members) > 0 {
			return key, &members[0], nil
		}
	}

	resultCh := make(chan string, 1)
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

	if key, member, ok := s.registerAndRecheckZMin(keys, resultCh); ok {
		return key, member, nil
	}

	select {
	case key := <-resultCh:
		members, err := s.ZPopMin(key, 1)
		if err != nil || len(members) == 0 {
			return "", nil, nil
		}
		return key, &members[0], nil
	case <-timerCh:
		s.unregisterBlockingZPop(resultCh, keys)
		return "", nil, nil
	case <-ctx.Done():
		s.unregisterBlockingZPop(resultCh, keys)
		return "", nil, nil
	}
}

// BZPopMax keeps backward compatibility
func (s *BotreonStore) BZPopMax(keys []string, timeout int) (string, *ZSetMember, error) {
	ctx := context.Background()
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
		defer cancel()
	}
	return s.BZPopMaxBlocking(ctx, keys, timeout)
}

// BZPopMin keeps backward compatibility
func (s *BotreonStore) BZPopMin(keys []string, timeout int) (string, *ZSetMember, error) {
	ctx := context.Background()
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
		defer cancel()
	}
	return s.BZPopMinBlocking(ctx, keys, timeout)
}
