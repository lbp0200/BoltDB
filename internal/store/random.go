package store

import (
	"crypto/rand"
	"math/big"
)

// randomIntn 返回 [0, n) 范围内的随机整数
// 当 crypto/rand 失败时返回 0
func randomIntn(n int) int {
	if n <= 0 {
		return 0
	}
	val, err := rand.Int(rand.Reader, big.NewInt(int64(n)))
	if err != nil {
		return 0
	}
	return int(val.Int64())
}

// randomShuffle 使用 crypto/rand 打乱切片
// 当 crypto/rand 失败时不做任何操作
func randomShuffle(n int, swap func(i, j int)) {
	if n <= 1 {
		return
	}
	// Fisher-Yates shuffle with crypto/rand
	for i := n - 1; i > 0; i-- {
		j := randomIntn(i + 1)
		swap(i, j)
	}
}
