package store

import (
	"testing"

	"github.com/zeebo/assert"
)

// TestZAdd tests ZAdd operations
func TestZAdd(t *testing.T) {
	store := setupTestStore(t)

	zSetName := "myset"

	// 添加成员
	members := []ZSetMember{
		{Member: "member1", Score: 1.5},
		{Member: "member2", Score: -2.0},
		{Member: "member3", Score: 0.0},
	}
	err := store.ZAdd(zSetName, members)
	assert.NoError(t, err)

	// 验证成员已添加
	card, _ := store.ZCard(zSetName)
	assert.Equal(t, int64(3), card)

	// 更新成员分数
	updateMembers := []ZSetMember{
		{Member: "member1", Score: 2.5},
	}
	err = store.ZAdd(zSetName, updateMembers)
	assert.NoError(t, err)

	// 验证分数已更新
	score, exists, _ := store.ZScore(zSetName, "member1")
	assert.True(t, exists)
	assert.Equal(t, 2.5, score)
}

// TestZCard tests ZCard operations using table-driven approach
func TestZCard(t *testing.T) {
	store := setupTestStore(t)

	tests := []struct {
		name     string
		setup    func()
		expected int64
	}{
		{
			name:     "empty set",
			setup:    func() {},
			expected: 0,
		},
		{
			name: "with members",
			setup: func() {
				mustZAdd(t, store, "myset", []ZSetMember{
					{Member: "member1", Score: 1.0},
					{Member: "member2", Score: 2.0},
				})
			},
			expected: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup()
			card, err := store.ZCard("myset")
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, card)
		})
	}
}

// TestZScore tests ZScore operations using table-driven approach
func TestZScore(t *testing.T) {
	store := setupTestStore(t)

	// 准备数据
	mustZAdd(t, store, "myset", []ZSetMember{
		{Member: "member1", Score: 1.5},
		{Member: "member2", Score: -2.0},
	})

	tests := []struct {
		name      string
		member    string
		wantErr   bool
		exists    bool
		expectErr bool
		expected  float64
	}{
		{
			name:     "existing member",
			member:   "member1",
			exists:   true,
			expected: 1.5,
		},
		{
			name:     "negative score member",
			member:   "member2",
			exists:   true,
			expected: -2.0,
		},
		{
			name:     "nonexistent member",
			member:   "nonexistent",
			exists:   false,
			expected: 0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			score, exists, err := store.ZScore("myset", tt.member)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.exists, exists)
			assert.Equal(t, tt.expected, score)
		})
	}
}

// TestZCount tests ZCount operations
func TestZCount(t *testing.T) {
	store := setupTestStore(t)

	// 准备数据
	mustZAdd(t, store, "myset", []ZSetMember{
		{Member: "member1", Score: 1.0},
		{Member: "member2", Score: 2.0},
		{Member: "member3", Score: 3.0},
		{Member: "member4", Score: 4.0},
	})

	tests := []struct {
		name     string
		min      float64
		max      float64
		expected int64
	}{
		{
			name:     "within range",
			min:      1.0,
			max:      3.0,
			expected: 3,
		},
		{
			name:     "all members",
			min:      -100.0,
			max:      100.0,
			expected: 4,
		},
		{
			name:     "empty range",
			min:      10.0,
			max:      20.0,
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			count, err := store.ZCount("myset", tt.min, tt.max)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, count)
		})
	}
}

// TestZIncrBy tests ZIncrBy operations
func TestZIncrBy(t *testing.T) {
	store := setupTestStore(t)

	zSetName := "myset"

	// 对不存在的成员增加分数
	newScore, err := store.ZIncrBy(zSetName, "member1", 5.0)
	assert.NoError(t, err)
	assert.Equal(t, 5.0, newScore)

	// 再次增加
	newScore, err = store.ZIncrBy(zSetName, "member1", 2.5)
	assert.NoError(t, err)
	assert.Equal(t, 7.5, newScore)

	// 减少分数
	newScore, err = store.ZIncrBy(zSetName, "member1", -1.0)
	assert.NoError(t, err)
	assert.Equal(t, 6.5, newScore)

	// 验证分数
	score, exists, _ := store.ZScore(zSetName, "member1")
	assert.True(t, exists)
	assert.Equal(t, 6.5, score)
}

func TestZRank(t *testing.T) {
	store := setupTestStore(t)

	// 准备数据（按分数排序：member2(-2.0), member3(0.0), member1(1.5)）
	mustZAdd(t, store, "myset", []ZSetMember{
		{Member: "member1", Score: 1.5},
		{Member: "member2", Score: -2.0},
		{Member: "member3", Score: 0.0},
	})

	tests := []struct {
		name     string
		member   string
		expected int64
	}{
		{
			name:     "lowest score",
			member:   "member2",
			expected: 0, // 最低分数，排名0
		},
		{
			name:     "middle score",
			member:   "member3",
			expected: 1,
		},
		{
			name:     "highest score",
			member:   "member1",
			expected: 2, // 最高分数，排名2
		},
		{
			name:     "nonexistent",
			member:   "nonexistent",
			expected: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rank, err := store.ZRank("myset", tt.member)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, rank)
		})
	}
}

func TestZRevRank(t *testing.T) {
	store := setupTestStore(t)

	// 准备数据（按分数排序：member2(-2.0), member3(0.0), member1(1.5)）
	mustZAdd(t, store, "myset", []ZSetMember{
		{Member: "member1", Score: 1.5},
		{Member: "member2", Score: -2.0},
		{Member: "member3", Score: 0.0},
	})

	tests := []struct {
		name     string
		member   string
		expected int64
	}{
		{
			name:     "highest score",
			member:   "member1",
			expected: 0, // 最高分数，反向排名0
		},
		{
			name:     "middle score",
			member:   "member3",
			expected: 1,
		},
		{
			name:     "lowest score",
			member:   "member2",
			expected: 2, // 最低分数，反向排名2
		},
		{
			name:     "nonexistent",
			member:   "nonexistent",
			expected: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rank, err := store.ZRevRank("myset", tt.member)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, rank)
		})
	}
}

func TestZRange(t *testing.T) {
	store := setupTestStore(t)


	zSetName := "myset"

	// 准备数据
	store.ZAdd(zSetName, []ZSetMember{
		{Member: "member1", Score: 1.5},
		{Member: "member2", Score: -2.0},
		{Member: "member3", Score: 0.0},
	})

	// 获取所有成员
	members, err := store.ZRange(zSetName, 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(members))
	assert.Equal(t, "member2", members[0].Member) // 最低分数
	assert.Equal(t, "member3", members[1].Member)
	assert.Equal(t, "member1", members[2].Member) // 最高分数

	// 获取范围
	members, err = store.ZRange(zSetName, 0, 1)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(members))

	// 负索引
	members, err = store.ZRange(zSetName, -2, -1)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(members))
}

func TestZRevRange(t *testing.T) {
	store := setupTestStore(t)


	zSetName := "myset"

	// 准备数据
	store.ZAdd(zSetName, []ZSetMember{
		{Member: "member1", Score: 1.5},
		{Member: "member2", Score: -2.0},
		{Member: "member3", Score: 0.0},
	})

	// 获取所有成员（反向）
	members, err := store.ZRevRange(zSetName, 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(members))
	assert.Equal(t, "member1", members[0].Member) // 最高分数
	assert.Equal(t, "member3", members[1].Member)
	assert.Equal(t, "member2", members[2].Member) // 最低分数

	// 获取范围
	members, err = store.ZRevRange(zSetName, 0, 1)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(members))
	assert.Equal(t, "member1", members[0].Member)
}

func TestZRangeByScore(t *testing.T) {
	store := setupTestStore(t)


	zSetName := "myset"

	// 准备数据
	store.ZAdd(zSetName, []ZSetMember{
		{Member: "member1", Score: 1.0},
		{Member: "member2", Score: 2.0},
		{Member: "member3", Score: 3.0},
		{Member: "member4", Score: 4.0},
	})

	// 获取分数范围内的成员
	members, err := store.ZRangeByScore(zSetName, 1.0, 3.0, 0, 0, false, false)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(members))
	assert.Equal(t, "member1", members[0].Member)
	assert.Equal(t, "member2", members[1].Member)
	assert.Equal(t, "member3", members[2].Member)

	// 带offset和count
	members, err = store.ZRangeByScore(zSetName, 1.0, 4.0, 1, 2, false, false)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(members))
}

func TestZRevRangeByScore(t *testing.T) {
	store := setupTestStore(t)


	zSetName := "myset"

	// 准备数据
	store.ZAdd(zSetName, []ZSetMember{
		{Member: "member1", Score: 1.0},
		{Member: "member2", Score: 2.0},
		{Member: "member3", Score: 3.0},
	})

	// 获取分数范围内的成员（反向）
	members, err := store.ZRevRangeByScore(zSetName, 3.0, 1.0, 0, 0, false, false)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(members))
	assert.Equal(t, "member3", members[0].Member) // 最高分数
	assert.Equal(t, "member2", members[1].Member)
	assert.Equal(t, "member1", members[2].Member) // 最低分数
}

func TestZRem(t *testing.T) {
	store := setupTestStore(t)


	zSetName := "myset"

	// 准备数据
	store.ZAdd(zSetName, []ZSetMember{
		{Member: "member1", Score: 1.0},
		{Member: "member2", Score: 2.0},
	})

	// 删除成员
	err := store.ZRem(zSetName, "member1")
	assert.NoError(t, err)

	// 验证成员已删除
	_, exists, _ := store.ZScore(zSetName, "member1")
	assert.False(t, exists)

	// 验证集合大小
	card, _ := store.ZCard(zSetName)
	assert.Equal(t, int64(1), card)

	// 删除不存在的成员
	err = store.ZRem(zSetName, "nonexistent")
	assert.NoError(t, err)
}

func TestZRemRangeByRank(t *testing.T) {
	store := setupTestStore(t)


	zSetName := "myset"

	// 准备数据
	store.ZAdd(zSetName, []ZSetMember{
		{Member: "member1", Score: 1.0},
		{Member: "member2", Score: 2.0},
		{Member: "member3", Score: 3.0},
		{Member: "member4", Score: 4.0},
	})

	// 删除排名范围内的成员
	removed, err := store.ZRemRangeByRank(zSetName, 1, 2)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), removed)

	// 验证剩余成员
	card, _ := store.ZCard(zSetName)
	assert.Equal(t, int64(2), card)

	members, _ := store.ZRange(zSetName, 0, -1)
	assert.Equal(t, 2, len(members))
}

func TestZRemRangeByScore(t *testing.T) {
	store := setupTestStore(t)


	zSetName := "myset"

	// 准备数据
	store.ZAdd(zSetName, []ZSetMember{
		{Member: "member1", Score: 1.0},
		{Member: "member2", Score: 2.0},
		{Member: "member3", Score: 3.0},
		{Member: "member4", Score: 4.0},
	})

	// 删除分数范围内的成员
	removed, err := store.ZRemRangeByScore(zSetName, 2.0, 3.0, false, false)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), removed)

	// 验证剩余成员
	card, _ := store.ZCard(zSetName)
	assert.Equal(t, int64(2), card)
}

func TestZPopMax(t *testing.T) {
	store := setupTestStore(t)


	zSetName := "myset"

	// 准备数据
	store.ZAdd(zSetName, []ZSetMember{
		{Member: "member1", Score: 1.0},
		{Member: "member2", Score: 2.0},
		{Member: "member3", Score: 3.0},
	})

	// 弹出最高分数的成员
	members, err := store.ZPopMax(zSetName, 1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(members))
	assert.Equal(t, "member3", members[0].Member)
	assert.Equal(t, 3.0, members[0].Score)

	// 验证成员已删除
	_, exists, _ := store.ZScore(zSetName, "member3")
	assert.False(t, exists)

	// 弹出多个成员
	members, err = store.ZPopMax(zSetName, 2)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(members))
	assert.Equal(t, "member2", members[0].Member)
	assert.Equal(t, "member1", members[1].Member)
}

func TestZPopMin(t *testing.T) {
	store := setupTestStore(t)


	zSetName := "myset"

	// 准备数据
	store.ZAdd(zSetName, []ZSetMember{
		{Member: "member1", Score: 1.0},
		{Member: "member2", Score: 2.0},
		{Member: "member3", Score: 3.0},
	})

	// 弹出最低分数的成员
	members, err := store.ZPopMin(zSetName, 1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(members))
	assert.Equal(t, "member1", members[0].Member)
	assert.Equal(t, 1.0, members[0].Score)

	// 验证成员已删除
	_, exists, _ := store.ZScore(zSetName, "member1")
	assert.False(t, exists)

	// 弹出多个成员
	members, err = store.ZPopMin(zSetName, 2)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(members))
	assert.Equal(t, "member2", members[0].Member)
	assert.Equal(t, "member3", members[1].Member)
}

func TestZSetDel(t *testing.T) {
	store := setupTestStore(t)


	zSetName := "myset"

	// 准备数据
	store.ZAdd(zSetName, []ZSetMember{
		{Member: "member1", Score: 1.0},
		{Member: "member2", Score: 2.0},
	})

	// 删除整个集合
	err := store.ZSetDel(zSetName)
	assert.NoError(t, err)

	// 验证集合已删除
	card, _ := store.ZCard(zSetName)
	assert.Equal(t, int64(0), card)

	members, _ := store.ZRange(zSetName, 0, -1)
	assert.Equal(t, 0, len(members))
}

func TestSortedSetEdgeCases(t *testing.T) {
	store := setupTestStore(t)


	zSetName := "myset"

	// 测试空集合操作
	card, _ := store.ZCard(zSetName)
	assert.Equal(t, int64(0), card)

	rank, _ := store.ZRank(zSetName, "member1")
	assert.Equal(t, int64(-1), rank)

	// 测试相同分数的成员
	store.ZAdd(zSetName, []ZSetMember{
		{Member: "member1", Score: 1.0},
		{Member: "member2", Score: 1.0},
		{Member: "member3", Score: 1.0},
	})

	card, _ = store.ZCard(zSetName)
	assert.Equal(t, int64(3), card)

	// 测试大量成员
	largeMembers := make([]ZSetMember, 100)
	for i := 0; i < 100; i++ {
		largeMembers[i] = ZSetMember{
			Member: string(rune('a' + i)),
			Score:  float64(i),
		}
	}
	store.ZAdd("large", largeMembers)
	card, _ = store.ZCard("large")
	assert.Equal(t, int64(100), card)

	// 测试负分数
	store.ZAdd("negative", []ZSetMember{
		{Member: "member1", Score: -10.0},
		{Member: "member2", Score: -5.0},
		{Member: "member3", Score: 0.0},
	})
	members, _ := store.ZRange("negative", 0, -1)
	assert.Equal(t, 3, len(members))
	assert.Equal(t, "member1", members[0].Member) // 最低分数
}

func TestSortedSetOperations(t *testing.T) {
	store := setupTestStore(t)


	zSetName := "myset"

	// 综合测试：添加、更新、查询、删除
	store.ZAdd(zSetName, []ZSetMember{
		{Member: "member1", Score: 1.0},
		{Member: "member2", Score: 2.0},
		{Member: "member3", Score: 3.0},
	})

	// 使用ZIncrBy增加分数
	newScore, _ := store.ZIncrBy(zSetName, "member1", 1.5)
	assert.Equal(t, 2.5, newScore)

	// 获取排名
	rank, _ := store.ZRank(zSetName, "member1")
	assert.True(t, rank >= 0)

	// 获取反向排名
	revRank, _ := store.ZRevRank(zSetName, "member1")
	assert.True(t, revRank >= 0)

	// 范围查询
	members, _ := store.ZRange(zSetName, 0, -1)
	assert.Equal(t, 3, len(members))

	// 分数范围查询
	scoreMembers, _ := store.ZRangeByScore(zSetName, 1.0, 3.0, 0, 0, false, false)
	assert.True(t, len(scoreMembers) > 0)

	// 删除成员
	_ = store.ZRem(zSetName, "member2")
	card, _ := store.ZCard(zSetName)
	assert.Equal(t, int64(2), card)
}

func TestZUnionStore(t *testing.T) {
	store := setupTestStore(t)


	// 创建两个有序集合
	_ = store.ZAdd("zset1", []ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 2.0},
	})
	_ = store.ZAdd("zset2", []ZSetMember{
		{Member: "b", Score: 3.0},
		{Member: "c", Score: 4.0},
	})

	// 测试并集（默认SUM聚合）
	count, err := store.ZUnionStore("dest", []string{"zset1", "zset2"}, nil, "")
	assert.NoError(t, err)
	assert.Equal(t, int64(3), count)

	// 验证结果
	members, _ := store.ZRange("dest", 0, -1)
	assert.Equal(t, 3, len(members))
	
	// 验证b的分数是2.0+3.0=5.0
	score, exists, _ := store.ZScore("dest", "b")
	assert.True(t, exists)
	assert.Equal(t, 5.0, score)

	// 测试MIN聚合
	count, err = store.ZUnionStore("dest2", []string{"zset1", "zset2"}, nil, "MIN")
	assert.NoError(t, err)
	assert.Equal(t, int64(3), count)
	
	score, exists, _ = store.ZScore("dest2", "b")
	assert.True(t, exists)
	assert.Equal(t, 2.0, score) // MIN(2.0, 3.0) = 2.0

	// 测试权重
	count, err = store.ZUnionStore("dest3", []string{"zset1", "zset2"}, []float64{2.0, 1.0}, "")
	assert.NoError(t, err)
	assert.Equal(t, int64(3), count)
	
	score, exists, _ = store.ZScore("dest3", "a")
	assert.True(t, exists)
	assert.Equal(t, 2.0, score) // 1.0 * 2.0 = 2.0
}

func TestZInterStore(t *testing.T) {
	store := setupTestStore(t)


	// 创建两个有序集合
	_ = store.ZAdd("zset1", []ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 2.0},
	})
	_ = store.ZAdd("zset2", []ZSetMember{
		{Member: "b", Score: 3.0},
		{Member: "c", Score: 4.0},
	})

	// 测试交集（默认SUM聚合）
	count, err := store.ZInterStore("dest", []string{"zset1", "zset2"}, nil, "")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)

	// 验证结果
	members, _ := store.ZRange("dest", 0, -1)
	assert.Equal(t, 1, len(members))
	assert.Equal(t, "b", members[0].Member)
	
	// 验证b的分数是2.0+3.0=5.0
	score, exists, _ := store.ZScore("dest", "b")
	assert.True(t, exists)
	assert.Equal(t, 5.0, score)
}

func TestZDiffStore(t *testing.T) {
	store := setupTestStore(t)


	// 创建两个有序集合
	store.ZAdd("zset1", []ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 2.0},
		{Member: "c", Score: 3.0},
	})
	store.ZAdd("zset2", []ZSetMember{
		{Member: "b", Score: 2.0},
		{Member: "c", Score: 3.0},
	})

	// 测试差集
	count, err := store.ZDiffStore("dest", []string{"zset1", "zset2"})
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)

	// 验证结果
	members, _ := store.ZRange("dest", 0, -1)
	assert.Equal(t, 1, len(members))
	assert.Equal(t, "a", members[0].Member)
}

func TestZLexCount(t *testing.T) {
	store := setupTestStore(t)


	// 创建有序集合（相同分数，按字典序）
	store.ZAdd("zset", []ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 1.0},
		{Member: "c", Score: 1.0},
		{Member: "d", Score: 1.0},
	})

	// 测试范围计数
	count, err := store.ZLexCount("zset", "[a", "[c")
	assert.NoError(t, err)
	assert.Equal(t, int64(3), count) // a, b, c

	count, err = store.ZLexCount("zset", "(a", "(c")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count) // b
}

func TestZRangeByLex(t *testing.T) {
	store := setupTestStore(t)


	// 创建有序集合（相同分数，按字典序）
	store.ZAdd("zset", []ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 1.0},
		{Member: "c", Score: 1.0},
		{Member: "d", Score: 1.0},
	})

	// 测试范围查询
	members, err := store.ZRangeByLex("zset", "[a", "[c", 0, 0)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(members))
	assert.Equal(t, "a", members[0])
	assert.Equal(t, "b", members[1])
	assert.Equal(t, "c", members[2])

	// 测试offset和count
	members, err = store.ZRangeByLex("zset", "[a", "[d", 1, 2)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(members))
	assert.Equal(t, "b", members[0])
	assert.Equal(t, "c", members[1])
}

func TestZRevRangeByLex(t *testing.T) {
	store := setupTestStore(t)


	// 创建有序集合
	store.ZAdd("zset", []ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 1.0},
		{Member: "c", Score: 1.0},
	})

	// 测试反向范围查询
	members, err := store.ZRevRangeByLex("zset", "[c", "[a", 0, 0)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(members))
	assert.Equal(t, "c", members[0])
	assert.Equal(t, "b", members[1])
	assert.Equal(t, "a", members[2])
}

func TestZRemRangeByLex(t *testing.T) {
	store := setupTestStore(t)


	// 创建有序集合
	store.ZAdd("zset", []ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 1.0},
		{Member: "c", Score: 1.0},
		{Member: "d", Score: 1.0},
	})

	// 删除范围内的成员
	removed, err := store.ZRemRangeByLex("zset", "[b", "[c")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), removed) // b, c

	// 验证结果
	members, _ := store.ZRange("zset", 0, -1)
	assert.Equal(t, 2, len(members))
	assert.Equal(t, "a", members[0].Member)
	assert.Equal(t, "d", members[1].Member)
}

func TestZMScore(t *testing.T) {
	store := setupTestStore(t)


	// 创建有序集合
	store.ZAdd("zset", []ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 2.0},
		{Member: "c", Score: 3.0},
	})

	// 测试批量获取分数
	scores, err := store.ZMScore("zset", "a", "b", "nonexistent", "c")
	assert.NoError(t, err)
	assert.Equal(t, 4, len(scores))
	assert.Equal(t, 1.0, scores[0])
	assert.Equal(t, 2.0, scores[1])
	assert.Equal(t, 0.0, scores[2]) // 不存在的成员
	assert.Equal(t, 3.0, scores[3])
}

// TestBZPopMax 测试 BZPOPMAX 命令
func TestBZPopMax(t *testing.T) {
	store := setupTestStore(t)


	// Add elements to sorted set
	store.ZAdd("zset", []ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 2.0},
		{Member: "c", Score: 3.0},
	})

	// Test BZPopMax
	key, member, err := store.BZPopMax([]string{"zset"}, 1)
	assert.NoError(t, err)
	assert.Equal(t, "zset", key)
	assert.Equal(t, "c", member.Member)
	assert.Equal(t, 3.0, member.Score)

	// Verify element was removed
	count, _ := store.ZCard("zset")
	assert.Equal(t, uint64(2), count)
}

// TestBZPopMin 测试 BZPOPMIN 命令
func TestBZPopMin(t *testing.T) {
	store := setupTestStore(t)


	// Add elements to sorted set
	store.ZAdd("zset", []ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 2.0},
		{Member: "c", Score: 3.0},
	})

	// Test BZPopMin
	key, member, err := store.BZPopMin([]string{"zset"}, 1)
	assert.NoError(t, err)
	assert.Equal(t, "zset", key)
	assert.Equal(t, "a", member.Member)
	assert.Equal(t, 1.0, member.Score)

	// Verify element was removed
	count, _ := store.ZCard("zset")
	assert.Equal(t, uint64(2), count)
}

// TestZScan 测试 ZSCAN 命令
func TestZScan(t *testing.T) {
	store := setupTestStore(t)


	// Add elements to sorted set
	for i := 0; i < 50; i++ {
		store.ZAdd("zset", []ZSetMember{
			{Member: "member" + string(rune('a'+i)), Score: float64(i)},
		})
	}

	// Test ZScan
	result, err := store.ZScan("zset", 0, "*member1*", 10)
	assert.NoError(t, err)
	_ = result

	// Test with non-existent key
	result, err = store.ZScan("nonexistent", 0, "*", 10)
	assert.NoError(t, err)
	_ = result
}
