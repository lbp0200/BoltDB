package store

import (
	"sync"
	"testing"
)

// TestConcurrentINCRAtomicity 验证并发 INCR 同 key 无丢失更新。
//
// 保护链：S1-A1 的 key 锁（INCR→INCRBY chokepoint 的 Lock）串行化读-改-写；
// 当前（Open 模式）下 badger 冲突重试为第二层保护。S1-A2 managed 切换
// （§10 附4——冲突检测退役）后本测试成为 key 锁层对 RMW 原子性的**唯一回归门**：
// 任一 INCR 族 key 锁缺失 → 并发下出现重复返回值（两次 INCR 读到同一旧值）→
// 唯一返回值集合 < 总数 → 测试失败。
//
// 判定：每次成功的 INCR 返回自增后的计数值；无丢失时 800 次 INCR 的返回值恰为
// {1..800}——唯一集合大小 == 总数且值域正确。
func TestConcurrentINCRAtomicity(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	const goroutines = 16
	const perGoroutine = 50
	total := goroutines * perGoroutine

	var wg sync.WaitGroup
	var mu sync.Mutex
	returns := make(map[int64]struct{}, total)
	errCh := make(chan error, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				v, err := s.INCR("atomic:counter")
				if err != nil {
					errCh <- err
					return
				}
				mu.Lock()
				returns[v] = struct{}{}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent INCR failed: %v", err)
	}

	if len(returns) != total {
		t.Fatalf("lost updates: observed %d unique counter values, want %d "+
			"(duplicates/gaps mean two INCRs read the same old value)", len(returns), total)
	}
	for v := range returns {
		if v < 1 || v > int64(total) {
			t.Fatalf("counter value %d out of range [1..%d]", v, total)
		}
	}
}
