package store

import (
	"math/rand/v2"
)

// randomIntn 返回 [0, n) 范围内的随机整数
// 使用 math/rand/v2 (PCG 算法，快于 crypto/rand ~100-500×)
// 蓄水池采样等非密码学场景无需 crypto/rand
func randomIntn(n int) int {
	if n <= 0 {
		return 0
	}
	return rand.IntN(n)
}

// randomShuffle 使用 Fisher-Yates 打乱切片
func randomShuffle(n int, swap func(i, j int)) {
	if n <= 1 {
		return
	}
	for i := n - 1; i > 0; i-- {
		j := randomIntn(i + 1)
		swap(i, j)
	}
}
