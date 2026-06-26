package store

import (
	"fmt"
	"testing"

	"pgregory.net/rapid"
)

// TestProperty_SetGetRoundtrip 验证 SET(k,v); GET(k) == v
// 使用独立前缀保证每次 rapid 迭代的 key 不重叠。
func TestProperty_SetGetRoundtrip(t *testing.T) {
	s := setupTestStore(t)
	var iter int

	rapid.Check(t, func(rt *rapid.T) {
		iter++
		prefix := fmt.Sprintf("iter%d:", iter)
		key := prefix + rapid.StringN(1, 20, 100).Draw(rt, "key")
		val := rapid.StringN(0, 100, 1000).Draw(rt, "val")

		if err := s.Set(key, val); err != nil {
			rt.Fatalf("Set(%q, %q): %v", key, val, err)
		}

		got, err := s.Get(key)
		if err != nil {
			rt.Fatalf("Get(%q): %v", key, err)
		}
		if got != val {
			rt.Fatalf("Get(%q) = %q, want %q", key, got, val)
		}
	})
}

// TestProperty_DelRemovesKey 验证 Del(k); Get(k) 返回错误
func TestProperty_DelRemovesKey(t *testing.T) {
	s := setupTestStore(t)
	var iter int

	rapid.Check(t, func(rt *rapid.T) {
		iter++
		prefix := fmt.Sprintf("iter%d:", iter)
		key := prefix + rapid.StringN(1, 20, 100).Draw(rt, "key")
		val := rapid.StringN(0, 100, 1000).Draw(rt, "val")

		if err := s.Set(key, val); err != nil {
			rt.Fatalf("Set: %v", err)
		}
		if _, err := s.Del(key); err != nil {
			rt.Fatalf("Del: %v", err)
		}
		if _, err := s.Get(key); err == nil {
			rt.Fatalf("Get after Del should fail")
		}
	})
}

// TestProperty_DelIdempotent 验证 Del 已删除的 key 返回 0
func TestProperty_DelIdempotent(t *testing.T) {
	s := setupTestStore(t)
	var iter int

	rapid.Check(t, func(rt *rapid.T) {
		iter++
		prefix := fmt.Sprintf("iter%d:", iter)
		key := prefix + rapid.StringN(1, 20, 100).Draw(rt, "key")
		val := rapid.StringN(0, 100, 1000).Draw(rt, "val")

		s.Set(key, val)

		n, err := s.Del(key)
		if err != nil {
			rt.Fatalf("first Del: %v", err)
		}
		if n != 1 {
			rt.Fatalf("first Del returned %d, want 1", n)
		}

		n, err = s.Del(key)
		if err != nil {
			rt.Fatalf("second Del: %v", err)
		}
		if n != 0 {
			rt.Fatalf("second Del returned %d, want 0", n)
		}
	})
}

// TestProperty_LPushLPop 验证 LPush(k,v); LPop(k) == v
func TestProperty_LPushLPop(t *testing.T) {
	s := setupTestStore(t)
	var iter int

	rapid.Check(t, func(rt *rapid.T) {
		iter++
		prefix := fmt.Sprintf("list%d:", iter)
		key := prefix + rapid.StringN(1, 10, 50).Draw(rt, "key")
		vals := rapid.SliceOfN(rapid.StringN(1, 20, 100), 1, 20).Draw(rt, "vals")

		for _, v := range vals {
			if _, err := s.LPush(key, v); err != nil {
				rt.Fatalf("LPush: %v", err)
			}
		}

		// LPush followed by LPop is LIFO
		for i := len(vals) - 1; i >= 0; i-- {
			got, err := s.LPop(key)
			if err != nil {
				rt.Fatalf("LPop: %v", err)
			}
			if got != vals[i] {
				rt.Fatalf("LPop: got %q, want %q (index %d)", got, vals[i], i)
			}
		}

		// Now empty — LPop should return "" (no error)
		got, err := s.LPop(key)
		if err != nil {
			rt.Fatalf("LPop on empty list: unexpected error: %v", err)
		}
		if got != "" {
			rt.Fatalf("LPop on empty list: got %q, want empty", got)
		}
	})
}

// TestProperty_SAddSIsMember 验证 SAdd(k,m); SIsMember(k,m) == true
func TestProperty_SAddSIsMember(t *testing.T) {
	s := setupTestStore(t)
	var iter int

	rapid.Check(t, func(rt *rapid.T) {
		iter++
		prefix := fmt.Sprintf("set%d:", iter)
		key := prefix + rapid.StringN(1, 10, 50).Draw(rt, "key")
		members := rapid.SliceOfNDistinct(
			rapid.StringN(1, 10, 50),
			1, 10,
			func(v string) string { return v },
		).Draw(rt, "members")

		for _, m := range members {
			if _, err := s.SAdd(key, m); err != nil {
				rt.Fatalf("SAdd: %v", err)
			}
		}

		for _, m := range members {
			ok, err := s.SIsMember(key, m)
			if err != nil {
				rt.Fatalf("SIsMember: %v", err)
			}
			if !ok {
				rt.Fatalf("SIsMember(%q) = false, want true", m)
			}
		}

		card, err := s.SCard(key)
		if err != nil {
			rt.Fatalf("SCard: %v", err)
		}
		if card != uint64(len(members)) {
			rt.Fatalf("SCard = %d, want %d", card, len(members))
		}
	})
}

// TestProperty_INCR_Monotonic 验证 INCR(k) 结果严格递增
func TestProperty_INCR_Monotonic(t *testing.T) {
	s := setupTestStore(t)
	var iter int

	rapid.Check(t, func(rt *rapid.T) {
		iter++
		key := fmt.Sprintf("incr%d:", iter)
		ops := rapid.IntRange(1, 20).Draw(rt, "ops")

		var prev int64
		for i := 0; i < ops; i++ {
			val, err := s.INCR(key)
			if err != nil {
				rt.Fatalf("INCR attempt %d: %v", i, err)
			}
			if val != prev+1 {
				rt.Fatalf("INCR attempt %d: got %d, want %d", i, val, prev+1)
			}
			prev = val
		}
	})
}
