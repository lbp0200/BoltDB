package replication

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
)

// TestDrainDoneBoundedNoGap 是 TODO §9 真修的回归守卫（done-前缀读）：复刻 flake 的
// 并发形状（prefill 20 + 8 writer × 40），drain 走真实生产路径 rm.FeedEntriesFrom
// （done 水位限界 + 解析 + verifyFeedTSContinuity），断言：
//
//   - 任一轮都不返回 gap 错误（读集 [since, done] 按构造稠密——in-flight 不可见）；
//   - 总帧数精确 == writers*perWriter（无丢失无重复）；
//   - 游标收敛到 maxTS+1。
//
// 修前（无界 View@MaxUint64）本构造 10 轮挂约 5 轮（签名② gap→drop）；修后须恒绿。
func TestDrainDoneBoundedNoGap(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	rm := NewReplicationManager(s)
	defer rm.Stop()

	const (
		prefill   = 20
		writers   = 8
		perWriter = 40
	)
	for i := 0; i < prefill; i++ {
		if err := s.Set(fmt.Sprintf("pre:%d", i), "1"); err != nil {
			t.Fatal(err)
		}
	}
	resumeTS, err := s.ReplLogCurrentTS()
	if err != nil {
		t.Fatal(err)
	}
	if resumeTS == 0 {
		t.Fatal("prefill produced no log ts")
	}

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				if err := s.Set(fmt.Sprintf("live:%d-%d", w, i), "1"); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}

	since := resumeTS + 1
	total := 0
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
drain:
	for {
		out, err := rm.FeedEntriesFrom(since)
		if err != nil {
			t.Fatalf("done-bounded drain returned gap (fix broken): %v", err)
		}
		if len(out) > 0 {
			last, err := strconv.ParseUint(out[len(out)-1][1], 10, 64)
			if err != nil {
				t.Fatalf("feed last ts parse: %v", err)
			}
			since = last + 1
			total += len(out)
		}
		select {
		case <-done:
			// 排空：静置后连续三轮空才认定收敛。
			quiet := 0
			for quiet < 3 {
				out, err := rm.FeedEntriesFrom(since)
				if err != nil {
					t.Fatalf("done-bounded drain (quiesce) returned gap: %v", err)
				}
				if len(out) == 0 {
					quiet++
					continue
				}
				last, err := strconv.ParseUint(out[len(out)-1][1], 10, 64)
				if err != nil {
					t.Fatalf("feed last ts parse: %v", err)
				}
				since = last + 1
				total += len(out)
				quiet = 0
			}
			break drain
		default:
		}
	}

	if want := writers * perWriter; total != want {
		t.Fatalf("done-bounded drain: got %d frames, want %d (loss or dup)", total, want)
	}
	maxTS, err := s.ReplLogCurrentTS()
	if err != nil {
		t.Fatal(err)
	}
	if since != maxTS+1 {
		t.Fatalf("cursor did not converge: since=%d maxTS=%d", since, maxTS)
	}
}
