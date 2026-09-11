package replication

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestDrainGapForensic 是 TODO §9 的机制实证探针 v3（非回归守卫）：复刻 flake 的
// drain 语义（游标 + ReplLogEntriesFrom(since) + verifyFeedTSContinuity，cursor
// 只在连续时推进——与 FeedSlave 一致，只是不发送），writers 记录逐键提交墙钟。
// 每次 gap 记录缺失 ts M；churn 结束后用静置全扫建 ts→key 映射，反查 M 的提交
// 时刻相对扫描窗口的位置：
//
//   - AFTER_END（扫描结束时仍未提交）→ snapshot-race / commit 乱序（in-flight）
//
//   - DURING（提交于扫描窗口内）→ 遍历竞速（transient，下一轮自愈）
//
//   - BEFORE_START（扫描开始前已提交）→ 迭代器漏已提交键（预期为零）
//
//     Hammond：本测试只读 store + 复用包内 verify，不碰生产代码。收敛断言（finalSince == maxTS+1）是自愈不变量。
func TestDrainGapForensic(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

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

	var mu sync.Mutex
	commitWall := make(map[string]int64, writers*perWriter)

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				k := fmt.Sprintf("live:%d-%d", w, i)
				if err := s.Set(k, "1"); err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				commitWall[k] = time.Now().UnixNano()
				mu.Unlock()
			}
		}(w)
	}

	type gapRec struct {
		missingTS uint64
		scanStart int64
		scanEnd   int64
	}
	var gaps []gapRec
	since := resumeTS + 1
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	// drain 循环：writers 活跃期高频扫 + 结束后排空。
	for {
		start := time.Now().UnixNano()
		entries, err := s.ReplLogEntriesFrom(since)
		end := time.Now().UnixNano()
		if err != nil {
			t.Fatal(err)
		}
		if err := verifyFeedTSContinuity(entries); err != nil {
			// 定位第一个断裂 ts（与 verify 同判据）。
			for i := 1; i < len(entries); i++ {
				if entries[i].TS != entries[i-1].TS+1 {
					gaps = append(gaps, gapRec{
						missingTS: entries[i-1].TS + 1,
						scanStart: start, scanEnd: end,
					})
					break
				}
			}
		} else if len(entries) > 0 {
			since = entries[len(entries)-1].TS + 1
		}
		select {
		case <-done:
			// 排空：静置后最后三轮确认游标收敛。
			quiet := 0
			for ; quiet < 3; quiet++ {
				entries, err := s.ReplLogEntriesFrom(since)
				if err != nil {
					t.Fatal(err)
				}
				if err := verifyFeedTSContinuity(entries); err != nil {
					break
				}
				if len(entries) == 0 {
					quiet++
					continue
				}
				since = entries[len(entries)-1].TS + 1
				quiet = 0
			}
			goto drained
		default:
		}
	}
drained:

	// 静置全扫建 ts→key；反查每个 gap 的提交时刻分类。
	full, err := s.ReplLogEntries()
	if err != nil {
		t.Fatal(err)
	}
	tsKey := make(map[uint64]string, len(full))
	var maxTS uint64
	for _, e := range full {
		args, aerr := parseReplLogValue(e.Value)
		if aerr != nil || len(args) < 2 || args[0] != "SET" {
			continue
		}
		tsKey[e.TS] = args[1]
		if e.TS > maxTS {
			maxTS = e.TS
		}
	}
	counts := map[string]int{"BEFORE_START": 0, "DURING": 0, "AFTER_END": 0, "NEVER_COMMITTED": 0}
	for _, g := range gaps {
		k, ok := tsKey[g.missingTS]
		if !ok {
			counts["NEVER_COMMITTED"]++
			t.Logf("forensic: gap ts=%d NEVER present at rest (tombstone hole?)", g.missingTS)
			continue
		}
		mu.Lock()
		cw, ok := commitWall[k]
		mu.Unlock()
		if !ok { // prefill 键（测试开始前提交）——不可能缺席，仅防御。
			counts["BEFORE_START"]++
			t.Logf("forensic: gap ts=%d key=%q prefill (unexpected)", g.missingTS, k)
			continue
		}
		switch {
		case cw < g.scanStart:
			counts["BEFORE_START"]++
			t.Logf("forensic: gap ts=%d key=%q committed %.1fms BEFORE scan start (iterator skip)",
				g.missingTS, k, float64(g.scanStart-cw)/1e6)
		case cw <= g.scanEnd:
			counts["DURING"]++
		default:
			counts["AFTER_END"]++
		}
	}
	t.Logf("forensic: gaps=%d class=%v finalSince=%d maxTS=%d", len(gaps), counts, since, maxTS)
	if since != maxTS+1 {
		t.Fatalf("forensic: cursor did not converge: since=%d maxTS=%d (self-heal broken)", since, maxTS)
	}
}
