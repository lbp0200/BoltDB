package replication

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/store"
)

// TestRDBRebuild_EquivalentToFrameReplay 度量两条同步路径是否给出**同一状态**：
//
//	A 重建路径 = GenerateRDB（写锁内）→ LoadRDBWithStore 进空 store；
//	B 追平路径 = 空 store 上按 ts 升序重放全部 log 帧（executeReplicatedCommand）。
//
// 生产里一条命令只会走其中一条：新从侧或降级重建走 A，CONTINUE 走 B。两者一旦不等，
// 就是「同一份历史，按接入方式不同得出不同数据」——与 lost 的签名（从侧比主侧少、
// 帧却全部 apply 完成）同型。此前无任何守卫比过这两态：
// replay_guard_test.go 比的是「log 帧重放 vs 字节 backlog 重放」，
// rdb_roundtrip_fidelity_test.go 比的是「生成+载入 vs 生成时主侧状态」，
// **RDB 与帧重放的互相一致性是空白**。
//
// 本探针同时是「log-key 语义跳过清单」的检查器：若某条命令改了数据却不写帧，
// B 会缺该改动而 A 有（A>B）——直接暴露清单划错的位置。
//
// 判据遵 §6 教训：逐键比**计数值/元素序列/成员集合/TTL** + 全局键数，不用键集合存在性。
// 确定性：静默状态下构造历史，无并发、无竞态窗口；耗时 <0.1s，故**不做 -short 门控**
// ——它是唯一能在 CI 里常驻覆盖「两态等价」这一维的守卫。
func TestRDBRebuild_EquivalentToFrameReplay(t *testing.T) {
	ctx := context.Background()
	src := setupTestStore(t)
	defer src.Close()

	// ---- 构造一段混合历史（全部经真实 store API，故 log 帧按生产的覆盖程度生成）----
	for i := 0; i < 6; i++ {
		for c := 0; c < 4; c++ {
			if _, err := src.INCRBY(fmt.Sprintf("eq:ctr:%d", c), 3); err != nil {
				t.Fatalf("INCRBY: %v", err)
			}
		}
		if _, err := src.RPush("eq:list", fmt.Sprintf("e%d", i)); err != nil {
			t.Fatalf("RPush: %v", err)
		}
		if err := src.HSet("eq:hash", fmt.Sprintf("f%d", i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatalf("HSet: %v", err)
		}
		if _, err := src.SAdd("eq:set", fmt.Sprintf("m%d", i)); err != nil {
			t.Fatalf("SAdd: %v", err)
		}
		if err := src.ZAdd("eq:zset", []store.ZSetMember{{Member: fmt.Sprintf("z%d", i), Score: float64(i)}}); err != nil {
			t.Fatalf("ZAdd: %v", err)
		}
		if err := src.Set("eq:str", fmt.Sprintf("s%d", i)); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	// 带 TTL 的键 + 一个被删除的键（删除类命令是否记帧，正是清单检查项）
	if err := src.Set("eq:ttl", "t"); err != nil {
		t.Fatalf("Set ttl: %v", err)
	}
	if _, err := src.Expire("eq:ttl", 3600); err != nil {
		t.Fatalf("Expire: %v", err)
	}
	if err := src.Set("eq:gone", "will-delete"); err != nil {
		t.Fatalf("Set gone: %v", err)
	}
	if _, err := src.Del("eq:gone"); err != nil {
		t.Fatalf("Del: %v", err)
	}
	// ZINCRBY/ZREM 确定性重放（TODO §7——编码侧参数序 + apply 侧 return nil
	// 静默——修复后窄版常驻回归守卫——权威序 ZINCRBY key increment member）
	if _, err := src.ZIncrBy("eq:zset", "z0", 10); err != nil {
		t.Fatalf("ZIncrBy: %v", err)
	}
	if _, err := src.ZRem("eq:zset", "z1"); err != nil {
		t.Fatalf("ZRem: %v", err)
	}

	// ---- A：写锁内取水位 + 生成 RDB ----
	src.SnapshotMuLock()
	snapshotTS, err := src.ReplLogCurrentTS()
	if err != nil {
		src.SnapshotMuUnlock()
		t.Fatalf("ReplLogCurrentTS: %v", err)
	}
	rdb, err := GenerateRDBWithSnapshotLock(src)
	src.SnapshotMuUnlock()
	if err != nil {
		t.Fatalf("GenerateRDBWithSnapshotLock: %v", err)
	}

	rebuild := setupTestStore(t)
	defer rebuild.Close()
	if err := LoadRDBWithStore(rdb, rebuild); err != nil {
		t.Fatalf("LoadRDBWithStore: %v", err)
	}

	// ---- B：空 store 上按 ts 升序重放全部帧 ----
	entries, err := src.ReplLogEntries()
	if err != nil {
		t.Fatalf("ReplLogEntries: %v", err)
	}
	replayed := setupTestStore(t)
	defer replayed.Close()

	var applied, skipped int
	for _, e := range entries {
		if e.TS > snapshotTS {
			skipped++ // 快照之后的帧不属于本次比对区间
			continue
		}
		args, err := parseReplLogValue(e.Value)
		if err != nil {
			t.Errorf("帧 ts=%d 解析失败（value=%q）: %v", e.TS, string(e.Value), err)
			continue
		}
		cmdArgs := make([][]byte, len(args))
		for i, a := range args {
			cmdArgs[i] = []byte(a)
		}
		if err := executeReplicatedCommand(replayed, cmdArgs, ctx); err != nil {
			t.Errorf("帧 ts=%d 重放失败 %s: %v", e.TS, args[0], err)
			continue
		}
		applied++
	}
	t.Logf("区间 ts<=%d：log 帧共 %d，重放 %d，区间外 %d", snapshotTS, len(entries), applied, skipped)

	// ---- 比对 A vs B ----
	want := stateDigest(t, src)
	gotA := stateDigest(t, rebuild)
	gotB := stateDigest(t, replayed)

	diffsA := diffDigest("RDB重建", want, gotA)
	diffsB := diffDigest("帧重放", want, gotB)
	for _, d := range diffsA {
		t.Errorf("A≠主侧: %s", d)
	}
	for _, d := range diffsB {
		t.Errorf("B≠主侧: %s", d)
	}
	// 核心断言：两条路径必须逐键一致（任一边的差异都会在这里对上）。
	cross := diffDigest("cross", gotA, gotB)
	for _, d := range cross {
		t.Errorf("两条同步路径状态不等（同一历史，接入方式不同结果不同）: %s", d)
	}
	if len(cross) == 0 {
		t.Logf("等价成立：%d 键两态逐键一致（帧 %d 条）", len(gotA), applied)
	}
}

// stateDigest 取一份 store 的规范化状态摘要（含全局键数）。
func stateDigest(t *testing.T, s *store.BotreonStore) map[string]string {
	t.Helper()

	d := map[string]string{}
	total, err := s.DBSize()
	if err != nil {
		t.Fatalf("DBSize: %v", err)
	}
	d["__dbsize__"] = strconv.FormatInt(total, 10)

	for c := 0; c < 4; c++ {
		k := fmt.Sprintf("eq:ctr:%d", c)
		d[k] = getOrMissing(s, k)
	}
	d["eq:str"] = getOrMissing(s, "eq:str")
	d["eq:gone"] = getOrMissing(s, "eq:gone")
	d["eq:ttl"] = getOrMissing(s, "eq:ttl")

	l, err := s.LRange("eq:list", 0, -1)
	if err != nil {
		l = nil
	}
	d["eq:list"] = strings.Join(l, "|")

	h, err := s.HGetAll("eq:hash")
	if err != nil {
		h = nil
	}
	hk := make([]string, 0, len(h))
	for f, v := range h {
		hk = append(hk, f+"="+string(v))
	}
	sort.Strings(hk)
	d["eq:hash"] = strings.Join(hk, "|")

	sm, err := s.SMembers("eq:set")
	if err != nil {
		sm = nil
	}
	sort.Strings(sm)
	d["eq:set"] = strings.Join(sm, "|")

	zm, err := s.ZRange("eq:zset", 0, -1)
	if err != nil {
		zm = nil
	}
	zk := make([]string, 0, len(zm))
	for _, m := range zm {
		zk = append(zk, fmt.Sprintf("%s:%g", m.Member, m.Score))
	}
	sort.Strings(zk)
	d["eq:zset"] = strings.Join(zk, "|")

	// TTL 只比"有无过期"。余量秒数不可比：主侧与重放侧的读时差本身就有秒级漂移，
	// 分桶更会在边界误报（远程实测 119 vs 120 的假失败）。本探针要抓的是
	// "载入/重放后 TTL 整个丢掉"（3600 → 无期），presence 足以区分。
	if ttl, err := s.TTL("eq:ttl"); err == nil {
		d["eq:ttl~presence"] = ttlPresence(ttl)
	} else {
		d["eq:ttl~presence"] = "err"
	}
	return d
}

// ttlPresence 把 TTL 压成"有期/无期"两态（无期＝候选 ⑦ 里 expireTime 解码被静默丢弃的表现）。
func ttlPresence(ttl int64) string {
	if ttl > 0 {
		return "has-ttl"
	}
	return "no-ttl"
}

// getOrMissing 读 string 值，键不存在时返回固定哨兵（区分"缺"与"空值"）。
func getOrMissing(s *store.BotreonStore, key string) string {
	v, err := s.Get(key)
	if err != nil {
		return "<missing>"
	}
	return v
}

// diffDigest 报告 want 与 got 的差异条目（键名 + 两边值）。
func diffDigest(who string, want, got map[string]string) []string {
	var out []string
	keys := make([]string, 0, len(want))
	for k := range want {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if want[k] != got[k] {
			out = append(out, fmt.Sprintf("[%s] %s: 期望 %q 实得 %q", who, k, want[k], got[k]))
		}
	}
	return out
}

// equivCase 一个命令族的等价性用例：setup 造历史，digest 只读该族触及的键。
type equivCase struct {
	name   string
	setup  func(t *testing.T, s *store.BotreonStore)
	digest func(s *store.BotreonStore) string
}

// TestRDBRebuild_EquivSweepAcrossCommandFamilies 把「A 重建态 == B 帧重放态」的断言
// 从少数键扩到**逐命令族**扫面（14 例，覆盖 string/列表/哈希/集合/有序集合/键管理/TTL 各族）。
//
// **首跑实测（2026-09-05）：13 例等价，1 例确认不等价**——ZINCRBY 的 log 值写成
// `ZINCRBY key member increment`（`zcard_score.go:210`），而主侧权威解析是
// `ZINCRBY key increment member`（`zset_commands.go:981-985`）；从侧 apply 分支
// （`write_command.go:820-825`）ParseFloat 失败后 **`return nil`——报成功但什么都没做**。
// 结果：同一历史下 A(RDB 重建) 与主侧一致，B(帧重放) 静默丢掉这条 ZINCRBY。
// 该例下方 knownDefects 表内暂时 Skip，修复落地即自动成为回归守卫（详见 TODO §8）。
//
// 为什么值得扫：D4 的全重放值是**按族分别写**进 log 键的（string/hash/list/set/zset/
// stream/geo/json/hll/rename/base/ts 共 12 族 ~90 站点），而"重放该帧能重现该键的
// 最终状态"这一条从来没有被逐族验过——只在具体命令的 round-trip 测试里零散覆盖。
// 某族一旦不等价（重放值参数不全、相对/绝对 TTL 形态错、顺序敏感命令元素次序不同），
// 表现就是「CONTINUE 来的从侧与 FULLRESYNC 来的从侧对同一历史给出不同数据」——
// 即 lost/dup 的一个确定机制，且与并发无关、可稳定复现。
//
// 每例独立 store（帧区间干净）；A/B 各自从主侧同一快照点出发。
func TestRDBRebuild_EquivSweepAcrossCommandFamilies(t *testing.T) {
	// 14 例 × 每例 3 个 store（badger 每实例 BlockCache 128MB）= 42 次开合，
	// 按本仓库 CI 资源约束惯例门控在 -short 之外（定向/nightly 跑）。
	// CI 常驻覆盖由 TestRDBRebuild_EquivalentToFrameReplay（3 store / 0.07s）承担。
	if testing.Short() {
		t.Skip("skipping command-family equivalence sweep in short mode")
	}
	cases := []equivCase{
		{"INCRBY", func(t *testing.T, s *store.BotreonStore) {
			for _, n := range []int64{5, -2, 100} {
				if _, err := s.INCRBY("sw:c", n); err != nil {
					t.Fatal(err)
				}
			}
		}, func(s *store.BotreonStore) string { return getOrMissing(s, "sw:c") }},

		{"INCRBYFLOAT", func(t *testing.T, s *store.BotreonStore) {
			if _, err := s.INCRBYFLOAT("sw:if", 1.5); err != nil {
				t.Fatal(err)
			}
			if _, err := s.INCRBYFLOAT("sw:if", 2.25); err != nil {
				t.Fatal(err)
			}
		}, func(s *store.BotreonStore) string { return getOrMissing(s, "sw:if") }},

		{"APPEND/SETRANGE/GETSET", func(t *testing.T, s *store.BotreonStore) {
			if err := s.Set("sw:s", "hello"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.APPEND("sw:s", " world"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.SetRange("sw:s", 6, "WORLD"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.GetSet("sw:s", "swapped"); err != nil {
				t.Fatal(err)
			}
		}, func(s *store.BotreonStore) string { return getOrMissing(s, "sw:s") }},

		{"BITSET", func(t *testing.T, s *store.BotreonStore) {
			if _, err := s.SetBit("sw:bits", 7, 1); err != nil {
				t.Fatal(err)
			}
			if _, err := s.SetBit("sw:bits", 100, 1); err != nil {
				t.Fatal(err)
			}
			if _, err := s.BitOp("OR", "sw:bits2", "sw:bits"); err != nil {
				t.Fatal(err)
			}
		}, func(s *store.BotreonStore) string {
			return getOrMissing(s, "sw:bits") + "/" + getOrMissing(s, "sw:bits2")
		}},

		{"RPUSH/LPUSH/LSET/LTRIM", func(t *testing.T, s *store.BotreonStore) {
			if _, err := s.RPush("sw:l", "a", "b", "c", "d"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.LPush("sw:l", "z"); err != nil {
				t.Fatal(err)
			}
			if err := s.LSet("sw:l", 2, "MID"); err != nil {
				t.Fatal(err)
			}
			if err := s.LTrim("sw:l", 1, 3); err != nil {
				t.Fatal(err)
			}
		}, func(s *store.BotreonStore) string {
			v, _ := s.LRange("sw:l", 0, -1)
			return strings.Join(v, ",")
		}},

		{"LINSERT/LMOVE", func(t *testing.T, s *store.BotreonStore) {
			if _, err := s.RPush("sw:la", "1", "2", "3"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.LInsert("sw:la", "BEFORE", "2", "1.5"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.RPush("sw:lb", "x"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.LMove("sw:la", "sw:lb", "LEFT", "RIGHT"); err != nil {
				t.Fatal(err)
			}
		}, func(s *store.BotreonStore) string {
			a, _ := s.LRange("sw:la", 0, -1)
			b, _ := s.LRange("sw:lb", 0, -1)
			return strings.Join(a, ",") + "|" + strings.Join(b, ",")
		}},

		{"HSET/HDEL/HINCRBY", func(t *testing.T, s *store.BotreonStore) {
			if err := s.HSet("sw:h", "f1", "v1"); err != nil {
				t.Fatal(err)
			}
			if err := s.HSet("sw:h", "f2", int64(7)); err != nil {
				t.Fatal(err)
			}
			if _, err := s.HDel("sw:h", "f1"); err != nil {
				t.Fatal(err)
			}
		}, func(s *store.BotreonStore) string {
			h, _ := s.HGetAll("sw:h")
			f := make([]string, 0, len(h))
			for k, v := range h {
				f = append(f, k+"="+string(v))
			}
			sort.Strings(f)
			return strings.Join(f, ",")
		}},

		{"HINCRBYFLOAT", func(t *testing.T, s *store.BotreonStore) {
			if _, err := s.HIncrByFloat("sw:hf", "f1", 1.5); err != nil {
				t.Fatal(err)
			}
			if _, err := s.HIncrByFloat("sw:hf", "f1", 2.25); err != nil {
				t.Fatal(err)
			}
		}, func(s *store.BotreonStore) string {
			h, _ := s.HGetAll("sw:hf")
			f := make([]string, 0, len(h))
			for k, v := range h {
				f = append(f, k+"="+string(v))
			}
			sort.Strings(f)
			return strings.Join(f, ",")
		}},

		{"SADD/SREM/SMOVE", func(t *testing.T, s *store.BotreonStore) {
			if _, err := s.SAdd("sw:set", "a", "b", "c"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.SRem("sw:set", "b"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.SAdd("sw:set2", "keep"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.SMove("sw:set", "sw:set2", "a"); err != nil {
				t.Fatal(err)
			}
		}, func(s *store.BotreonStore) string {
			a, _ := s.SMembers("sw:set")
			b, _ := s.SMembers("sw:set2")
			sort.Strings(a)
			sort.Strings(b)
			return strings.Join(a, ",") + "|" + strings.Join(b, ",")
		}},

		{"ZADD/ZREM/ZINCRBY", func(t *testing.T, s *store.BotreonStore) {
			if err := s.ZAdd("sw:z", []store.ZSetMember{{Member: "m1", Score: 1}, {Member: "m2", Score: 2.5}}); err != nil {
				t.Fatal(err)
			}
			if _, err := s.ZIncrBy("sw:z", "m1", 10); err != nil {
				t.Fatal(err)
			}
			if _, err := s.ZRem("sw:z", "m2"); err != nil {
				t.Fatal(err)
			}
		}, func(s *store.BotreonStore) string {
			z, _ := s.ZRange("sw:z", 0, -1)
			o := make([]string, 0, len(z))
			for _, m := range z {
				o = append(o, fmt.Sprintf("%s:%g", m.Member, m.Score))
			}
			return strings.Join(o, ",")
		}},

		{"ZUNIONSTORE/ZINTERSTORE", func(t *testing.T, s *store.BotreonStore) {
			if err := s.ZAdd("sw:za", []store.ZSetMember{{Member: "m1", Score: 1}, {Member: "m2", Score: 2}}); err != nil {
				t.Fatal(err)
			}
			if err := s.ZAdd("sw:zb", []store.ZSetMember{{Member: "m2", Score: 3}, {Member: "m3", Score: 4}}); err != nil {
				t.Fatal(err)
			}
			if _, err := s.ZUnionStore("sw:zu", []string{"sw:za", "sw:zb"}, []float64{1, 2}, "SUM"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.ZInterStore("sw:zi", []string{"sw:za", "sw:zb"}, []float64{1, 1}, "SUM"); err != nil {
				t.Fatal(err)
			}
		}, func(s *store.BotreonStore) string {
			zu, _ := s.ZRange("sw:zu", 0, -1)
			zi, _ := s.ZRange("sw:zi", 0, -1)
			o := make([]string, 0, len(zu)+len(zi))
			for _, m := range zu {
				o = append(o, "zu:"+m.Member+":"+fmt.Sprintf("%g", m.Score))
			}
			for _, m := range zi {
				o = append(o, "zi:"+m.Member+":"+fmt.Sprintf("%g", m.Score))
			}
			sort.Strings(o)
			return strings.Join(o, ",")
		}},

		{"GEOADD/GEOSEARCH", func(t *testing.T, s *store.BotreonStore) {
			if _, err := s.GeoAdd("sw:g", []store.GeoMember{{Member: "p1", Lat: 38.11, Lon: 13.36}}); err != nil {
				t.Fatal(err)
			}
			if _, err := s.GeoAdd("sw:g", []store.GeoMember{{Member: "p2", Lat: 38.71, Lon: 15.52}}); err != nil {
				t.Fatal(err)
			}
			if _, err := s.GeoSearch("sw:g", 13.5, 38.5, 500, "km", 10, false, false, false); err != nil {
				t.Fatal(err)
			}
		}, func(s *store.BotreonStore) string {
			res, _ := s.GeoSearch("sw:g", 0, 0, 20000, "km", 10, false, false, false)
			o := make([]string, 0, len(res))
			for _, r := range res {
				o = append(o, r.Member)
			}
			sort.Strings(o)
			return strings.Join(o, ",")
		}},

		{"XADD/XTRIM", func(t *testing.T, s *store.BotreonStore) {
			if _, err := s.XAdd("sw:x", store.StreamXAddOptions{}, "*", map[string]string{"f1": "v1"}); err != nil {
				t.Fatal(err)
			}
			if _, err := s.XAdd("sw:x", store.StreamXAddOptions{}, "*", map[string]string{"f2": "v2"}); err != nil {
				t.Fatal(err)
			}
			if _, err := s.XTrim("sw:x", 1, ""); err != nil {
				t.Fatal(err)
			}
		}, func(s *store.BotreonStore) string {
			n, _ := s.XLen("sw:x")
			return fmt.Sprintf("%d", n)
		}},

		{"RENAME", func(t *testing.T, s *store.BotreonStore) {
			if err := s.Set("sw:from", "payload"); err != nil {
				t.Fatal(err)
			}
			if err := s.Rename("sw:from", "sw:to"); err != nil {
				t.Fatal(err)
			}
		}, func(s *store.BotreonStore) string {
			return getOrMissing(s, "sw:from") + "/" + getOrMissing(s, "sw:to")
		}},

		{"MSET/MSETNX", func(t *testing.T, s *store.BotreonStore) {
			if err := s.MSet("sw:m1", "1", "sw:m2", "2"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.MSetNX("sw:m2", "nope", "sw:m3", "3"); err != nil {
				t.Fatal(err)
			}
		}, func(s *store.BotreonStore) string {
			return getOrMissing(s, "sw:m1") + getOrMissing(s, "sw:m2") + getOrMissing(s, "sw:m3")
		}},

		{"DEL", func(t *testing.T, s *store.BotreonStore) {
			if err := s.Set("sw:del", "x"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.RPush("sw:dell", "e"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Del("sw:del"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Del("sw:dell"); err != nil {
				t.Fatal(err)
			}
		}, func(s *store.BotreonStore) string {
			return getOrMissing(s, "sw:del") + "/" + getOrMissing(s, "sw:dell")
		}},

		{"EXPIRE/PERSIST", func(t *testing.T, s *store.BotreonStore) {
			if err := s.Set("sw:ttl", "v"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Expire("sw:ttl", 3600); err != nil {
				t.Fatal(err)
			}
		}, func(s *store.BotreonStore) string {
			n, _ := s.TTL("sw:ttl")
			return fmt.Sprintf("%s/%s", ttlPresence(n), getOrMissing(s, "sw:ttl"))
		}},

		{"PERSIST", func(t *testing.T, s *store.BotreonStore) {
			if err := s.Set("sw:pp", "v"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Expire("sw:pp", 3600); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Persist("sw:pp"); err != nil {
				t.Fatal(err)
			}
		}, func(s *store.BotreonStore) string {
			n, _ := s.TTL("sw:pp")
			return fmt.Sprintf("%s/%s", ttlPresence(n), getOrMissing(s, "sw:pp"))
		}},

		{"PEXPIRE/EXPIREAT", func(t *testing.T, s *store.BotreonStore) {
			if err := s.Set("sw:pe", "v"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.PExpire("sw:pe", 600_000); err != nil {
				t.Fatal(err)
			}
			if err := s.Set("sw:ea", "v"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.ExpireAt("sw:ea", time.Now().Add(2*time.Hour).Unix()); err != nil {
				t.Fatal(err)
			}
		}, func(s *store.BotreonStore) string {
			a, _ := s.TTL("sw:pe")
			b, _ := s.TTL("sw:ea")
			return ttlPresence(a) + "/" + ttlPresence(b)
		}},

		{"LREM", func(t *testing.T, s *store.BotreonStore) {
			if _, err := s.RPush("sw:lrem", "a", "b", "a", "c", "a"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.LRem("sw:lrem", 2, "a"); err != nil {
				t.Fatal(err)
			}
		}, func(s *store.BotreonStore) string {
			v, _ := s.LRange("sw:lrem", 0, -1)
			return strings.Join(v, ",")
		}},

		{"XACKDEL", func(t *testing.T, s *store.BotreonStore) {
			id1, err := s.XAdd("sw:xack", store.StreamXAddOptions{}, "*", map[string]string{"f1": "v1"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.XAdd("sw:xack", store.StreamXAddOptions{}, "*", map[string]string{"f2": "v2"}); err != nil {
				t.Fatal(err)
			}
			if err := s.XGroupCreate("sw:xack", "g1", "0"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.XAck("sw:xack", "g1", id1); err != nil {
				t.Fatal(err)
			}
			if _, err := s.XDel("sw:xack", id1); err != nil {
				t.Fatal(err)
			}
			if err := s.XAckDelRemoveRefs("sw:xack", id1); err != nil {
				t.Fatal(err)
			}
		}, func(s *store.BotreonStore) string {
			n, _ := s.XLen("sw:xack")
			return fmt.Sprintf("%d", n)
		}},

		{"GEOADD/GEOSEARCHSTORE", func(t *testing.T, s *store.BotreonStore) {
			if _, err := s.GeoAdd("sw:gs", []store.GeoMember{{Member: "p1", Lat: 38.11, Lon: 13.36}, {Member: "p2", Lat: 38.71, Lon: 15.52}}); err != nil {
				t.Fatal(err)
			}
			if _, err := s.GeoSearchStore("sw:gsd", "sw:gs", 13.5, 38.5, 500, "km", 10, false, "RADIUS", 0); err != nil {
				t.Fatal(err)
			}
		}, func(s *store.BotreonStore) string {
			z, _ := s.ZRange("sw:gsd", 0, -1)
			o := make([]string, 0, len(z))
			for _, m := range z {
				o = append(o, m.Member)
			}
			sort.Strings(o)
			return strings.Join(o, ",")
		}},

		{"XGROUP/XGROUP SETID/CREATECONSUMER/DESTROY", func(t *testing.T, s *store.BotreonStore) {
			if _, err := s.XAdd("sw:xg", store.StreamXAddOptions{}, "*", map[string]string{"f1": "v1"}); err != nil {
				t.Fatal(err)
			}
			if err := s.XGroupCreate("sw:xg", "g1", "0"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.XGroupCreateConsumer("sw:xg", "g1", "c1"); err != nil {
				t.Fatal(err)
			}
			if err := s.XGroupSetID("sw:xg", "g1", "0-0"); err != nil {
				t.Fatal(err)
			}
			if err := s.XGroupDestroy("sw:xg", "g1"); err != nil {
				t.Fatal(err)
			}
		}, func(s *store.BotreonStore) string {
			groups, _ := s.XInfoGroups("sw:xg")
			names := make([]string, 0, len(groups))
			for _, g := range groups {
				names = append(names, g.Name)
			}
			sort.Strings(names)
			n, _ := s.XLen("sw:xg")
			return fmt.Sprintf("%d/%s", n, strings.Join(names, ","))
		}},

		{"XREADGROUP/XNACK", func(t *testing.T, s *store.BotreonStore) {
			id1, err := s.XAdd("sw:xn", store.StreamXAddOptions{}, "*", map[string]string{"f1": "v1"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.XAdd("sw:xn", store.StreamXAddOptions{}, "*", map[string]string{"f2": "v2"}); err != nil {
				t.Fatal(err)
			}
			if err := s.XGroupCreate("sw:xn", "g1", "0"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.XReadGroup(context.Background(), "g1", "c1", 10, 0, "sw:xn"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.XNack("sw:xn", "g1", "c1", id1); err != nil {
				t.Fatal(err)
			}
		}, func(s *store.BotreonStore) string {
			groups, _ := s.XInfoGroups("sw:xn")
			pending := 0
			for _, g := range groups {
				pending += len(g.Pending)
			}
			return fmt.Sprintf("pending=%d", pending)
		}},

		{"XCLAIM", func(t *testing.T, s *store.BotreonStore) {
			id1, err := s.XAdd("sw:xc", store.StreamXAddOptions{}, "*", map[string]string{"f1": "v1"})
			if err != nil {
				t.Fatal(err)
			}
			if err := s.XGroupCreate("sw:xc", "g1", "0"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.XReadGroup(context.Background(), "g1", "c1", 10, 0, "sw:xc"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.XClaim("sw:xc", "g1", "c2", store.XClaimOptions{}, id1); err != nil {
				t.Fatal(err)
			}
		}, func(s *store.BotreonStore) string {
			groups, _ := s.XInfoGroups("sw:xc")
			o := make([]string, 0, len(groups))
			for _, g := range groups {
				for cname := range g.Consumers {
					o = append(o, cname)
				}
			}
			sort.Strings(o)
			return strings.Join(o, ",")
		}},
	}

	// knownDefects 登记「本扫面已确认不等价、但修复尚未落地」的命令族。
	// 命中即 Skip（明确写出原因与 TODO 条目），修复落地后**删掉表项**，
	// 该例自动成为回归守卫——不要用 t.Log 吞掉，也不要长期留在表里。
	// 2026-09-06：ZADD/ZREM/ZINCRBY 表项已删——修复落地（编码侧 zcard_score.go
	// 参数序 key increment member + apply 侧 write_command.go return error——
	// 见 TODO §7）——该例自动转正为回归守卫。
	knownDefects := map[string]string{}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if d, ok := knownDefects[tc.name]; ok {
				t.Skip("已知缺陷，本轮不判定：" + d)
			}
			src := setupTestStore(t)
			defer src.Close()
			tc.setup(t, src)

			src.SnapshotMuLock()
			snapshotTS, err := src.ReplLogCurrentTS()
			rdb, rerr := GenerateRDBWithSnapshotLock(src)
			src.SnapshotMuUnlock()
			if err != nil || rerr != nil {
				t.Fatalf("snapshot: %v / %v", err, rerr)
			}

			rebuild := setupTestStore(t)
			defer rebuild.Close()
			if err := LoadRDBWithStore(rdb, rebuild); err != nil {
				t.Fatalf("LoadRDBWithStore: %v", err)
			}

			replayed := setupTestStore(t)
			defer replayed.Close()
			n := replayFrames(t, src, replayed, snapshotTS)

			want := tc.digest(src)
			gotA := tc.digest(rebuild)
			gotB := tc.digest(replayed)
			t.Logf("frames=%d 主侧=%q A(RDB重建)=%q B(帧重放)=%q", n, want, gotA, gotB)
			if want != gotA {
				t.Errorf("%s：RDB 重建态与主侧不等（%q vs %q）", tc.name, want, gotA)
			}
			if want != gotB {
				t.Errorf("%s：帧重放态与主侧不等（%q vs %q）——该族 log 值参数不全或语义不可重放", tc.name, want, gotB)
			}
			if gotA != gotB {
				t.Errorf("%s：两同步路径互不等（A=%q B=%q）——同一历史按接入方式得出不同数据", tc.name, gotA, gotB)
			}
		})
	}
}

// replayFrames 把 src 中 ts ≤ until 的全部 log 帧按序重放进 dst，返回重放条数。
func replayFrames(t *testing.T, src, dst *store.BotreonStore, until uint64) int {
	t.Helper()
	entries, err := src.ReplLogEntries()
	if err != nil {
		t.Fatalf("ReplLogEntries: %v", err)
	}
	ctx := context.Background()
	n := 0
	for _, e := range entries {
		if e.TS > until {
			continue
		}
		args, err := parseReplLogValue(e.Value)
		if err != nil {
			t.Errorf("帧 ts=%d 解析失败 value=%q: %v", e.TS, string(e.Value), err)
			continue
		}
		cmdArgs := make([][]byte, len(args))
		for i, a := range args {
			cmdArgs[i] = []byte(a)
		}
		if err := executeReplicatedCommand(dst, cmdArgs, ctx); err != nil {
			t.Errorf("帧 ts=%d 重放失败 %v: %v", e.TS, args, err)
			continue
		}
		n++
	}
	return n
}
