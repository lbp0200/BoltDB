package replication

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/store"
)

// TestRDBSnapshot_RoundTripFidelity 度量「生成 RDB → 载入新 store」是否丢条目
// ——TODO §6 lost 候选 ⑤（RDB 生成/载入侧）。
//
// 为什么查这一环：lost 的表现是「从侧 apply 了全部帧但某条 INCR 未生效」，而实验 6
// 证明从侧确实经历了 FULLRESYNC 重建（键值重置为 1）。重建 = FlushDB + 载入 RDB。
// 生成侧的撕裂已由 rdb_torn_snapshot_test.go 排除（fenced 0/120，对照 unfenced
// 119/120——栅栏有效），**未测的是另一头**：encoder 输出的 RDB 少写条目，或
// LoadRDBWithStore apply 少落条目。二者都会表现为「帧全集已应用但值偏小」——
// 与 lost 签名一致，且完全不需要复制通道参与。
//
// 判据遵 §6 教训：比对**计数与元素序列**（非幂等、跨键），不用键集合。
// 两阶段：
//
//	A 静默点全量比对——状态固定，可精确判定；
//	B 并发写后静默重比对（多轮）——复刻 lost 的"多周期"形状里唯一不依赖复制的一环。
//
// 任一阶段失配 → t.Errorf（RDB 往返不保真——lost 机制定位到生成/载入侧）。
func TestRDBSnapshot_RoundTripFidelity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping RDB round-trip probe in short mode（60+ 次 store 开合，属定向/nightly）")
	}
	s := setupTestStore(t)
	defer s.Close()

	const (
		counters  = 8
		listElems = 20
		hashField = 12
	)

	seed := func(bump int64) {
		for i := 0; i < counters; i++ {
			if _, err := s.INCRBY(fmt.Sprintf("rt:ctr:%d", i), 1+bump); err != nil {
				t.Fatalf("INCRBY: %v", err)
			}
		}
		for i := 0; i < listElems; i++ {
			if _, err := s.RPush("rt:list", fmt.Sprintf("e%d-%d", bump, i)); err != nil {
				t.Fatalf("RPush: %v", err)
			}
		}
		for i := 0; i < hashField; i++ {
			if err := s.HSet("rt:hash", fmt.Sprintf("f%d", i), fmt.Sprintf("v%d-%d", i, bump)); err != nil {
				t.Fatalf("HSet: %v", err)
			}
		}
		if err := s.Set("rt:str", fmt.Sprintf("s%d", bump)); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	seed(0)

	// ---- 阶段 A：静默点全量比对 ----
	if mismatches := snapshotCompare(s, t, "A/quiescent"); mismatches > 0 {
		t.Errorf("阶段 A：RDB 往返失配 %d 处——生成/载入侧丢条目", mismatches)
	}

	// ---- 阶段 B：并发写 → 静默 → 多轮重比对 ----
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var writes atomic.Uint64
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				n := writes.Add(1)
				s.SnapshotMuRLock() // 与生产 processRequest 同构
				_, _ = s.INCRBY(fmt.Sprintf("rt:ctr:%d", n%counters), 1)
				_, _ = s.RPush("rt:list", strconv.FormatUint(n, 10))
				_ = s.HSet("rt:hash", fmt.Sprintf("g%d", n%64), strconv.FormatUint(n, 10))
				s.SnapshotMuRUnlock()
			}
		}(w)
	}
	// 让并发写累积一段时间（时间驱动——空转轮询攒不出负载），再静默——
	// 静默后的状态是确定的，可比对。
	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
	t.Logf("阶段 B: 并发写完成 writes=%d", writes.Load())

	seed(1) // 静默后再补一批，制造第二轮确定状态
	total := 0
	const rounds = 30
	for r := 0; r < rounds; r++ {
		total += snapshotCompare(s, t, fmt.Sprintf("B/round%d", r))
		seed(int64(r + 2))
	}
	if total > 0 {
		t.Errorf("阶段 B：%d/%d 轮 RDB 往返失配——lost 候选 ⑤（生成/载入侧）成立", total, rounds)
	} else {
		t.Logf("阶段 B：%d/%d 轮全部保真——候选 ⑤ 未获支持", rounds, rounds)
	}
}

// snapshotCompare 在写锁内生成 RDB、载入全新 store、逐键比对主/载状态，返回失配处数。
func snapshotCompare(src *store.BotreonStore, t *testing.T, tag string) int {
	t.Helper()

	src.SnapshotMuLock()
	rdb, err := GenerateRDBWithSnapshotLock(src)
	src.SnapshotMuUnlock()
	if err != nil {
		t.Fatalf("%s: GenerateRDBWithSnapshotLock: %v", tag, err)
	}

	dst, err := store.NewBotreonStore(t.TempDir())
	if err != nil {
		t.Fatalf("%s: fresh store: %v", tag, err)
	}
	defer dst.Close()
	if err := LoadRDBWithStore(rdb, dst); err != nil {
		t.Fatalf("%s: LoadRDBWithStore: %v", tag, err)
	}

	var mismatches int
	// string 计数键
	for i := 0; i < 8; i++ {
		k := fmt.Sprintf("rt:ctr:%d", i)
		sv, serr := src.Get(k)
		dv, derr := dst.Get(k)
		if serr != nil || derr != nil {
			t.Errorf("%s: %s 读取失败 src=%v dst=%v", tag, k, serr, derr)
			mismatches++
			continue
		}
		if sv != dv {
			t.Errorf("%s: TORN-CANDIDATE %s src=%s dst=%s（载入少 %d）", tag, k, sv, dv,
				subInt(t, sv, dv))
			mismatches++
		}
	}
	// list 元素序列（长度 + 逐元素）
	sl, _ := src.LRange("rt:list", 0, -1)
	dl, _ := dst.LRange("rt:list", 0, -1)
	if len(sl) != len(dl) {
		t.Errorf("%s: list 长度 src=%d dst=%d（载入丢 %d 元素）", tag, len(sl), len(dl), len(sl)-len(dl))
		mismatches++
	}
	sh, _ := src.HGetAll("rt:hash")
	dh, _ := dst.HGetAll("rt:hash")
	if len(sh) != len(dh) {
		t.Errorf("%s: hash 字段数 src=%d dst=%d", tag, len(sh), len(dh))
		mismatches++
	}
	for f, v := range sh {
		if dv, ok := dh[f]; !ok || string(v) != string(dv) {
			t.Errorf("%s: hash 字段 %s src=%q dst=%q", tag, f, v, dv)
			mismatches++
		}
	}
	ss, _ := src.Get("rt:str")
	ds, _ := dst.Get("rt:str")
	if ss != ds {
		t.Errorf("%s: str src=%q dst=%q", tag, ss, ds)
		mismatches++
	}
	return mismatches
}

// subInt 报告 src−dst 的差值（非数字时返回哨兵 -999，仅用于日志）。
func subInt(t *testing.T, a, b string) int64 {
	t.Helper()
	ai, ea := strconv.ParseInt(a, 10, 64)
	bi, eb := strconv.ParseInt(b, 10, 64)
	if ea != nil || eb != nil {
		return -999
	}
	return ai - bi
}
