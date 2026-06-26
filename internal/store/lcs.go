package store

import (
	"sort"
)

// LCIMatch represents a single common subsequence match with positions
type LCIMatch struct {
	Value    string
	StartA   int
	EndA     int
	StartB   int
	EndB     int
	MatchLen int
}

// computeLCS computes the Longest Common Subsequence of two strings.
func computeLCS(a, b string) (string, int) {
	m, n := len(a), len(b)
	dp := make([][]int, m+1)
	for i := range dp {
		dp[i] = make([]int, n+1)
	}
	for i := 1; i <= m; i++ {
		for j := 1; j <= n; j++ {
			if a[i-1] == b[j-1] {
				dp[i][j] = dp[i-1][j-1] + 1
			} else if dp[i-1][j] > dp[i][j-1] {
				dp[i][j] = dp[i-1][j]
			} else {
				dp[i][j] = dp[i][j-1]
			}
		}
	}
	lcsLen := dp[m][n]
	if lcsLen == 0 {
		return "", 0
	}
	lcs := make([]byte, lcsLen)
	i, j := m, n
	idx := lcsLen - 1
	for i > 0 && j > 0 {
		if a[i-1] == b[j-1] {
			lcs[idx] = a[i-1]
			i--
			j--
			idx--
		} else if dp[i-1][j] > dp[i][j-1] {
			i--
		} else {
			j--
		}
	}
	return string(lcs), lcsLen
}

// computeLCSLength computes only the LCS length (space-optimized).
func computeLCSLength(a, b string) int {
	m, n := len(a), len(b)
	prev := make([]int, n+1)
	curr := make([]int, n+1)
	for i := 1; i <= m; i++ {
		for j := 1; j <= n; j++ {
			if a[i-1] == b[j-1] {
				curr[j] = prev[j-1] + 1
			} else if prev[j] > curr[j-1] {
				curr[j] = prev[j]
			} else {
				curr[j] = curr[j-1]
			}
		}
		prev, curr = curr, prev
	}
	return prev[n]
}

// ComputeLCSMatches finds the contiguous segments that form the LCS.
// It builds the DP table, backtracks to collect matching position pairs,
// then groups consecutive pairs into contiguous segments.
// minMatchLen filters out segments shorter than this threshold.
func ComputeLCSMatches(a, b string, minMatchLen int) []LCIMatch {
	m, n := len(a), len(b)
	if m == 0 || n == 0 {
		return nil
	}

	// Build DP table
	dp := make([][]int, m+1)
	for i := range dp {
		dp[i] = make([]int, n+1)
	}
	for i := 1; i <= m; i++ {
		for j := 1; j <= n; j++ {
			if a[i-1] == b[j-1] {
				dp[i][j] = dp[i-1][j-1] + 1
			} else if dp[i-1][j] >= dp[i][j-1] {
				dp[i][j] = dp[i-1][j]
			} else {
				dp[i][j] = dp[i][j-1]
			}
		}
	}

	lcsLen := dp[m][n]
	if lcsLen == 0 {
		return nil
	}

	// Backtrack to collect (posA, posB) for each character in the LCS.
	// Use a recursive approach that explores all optimal paths:
	// when dp[i-1][j] == dp[i][j-1], we branch left (decrement j only)
	// to avoid finding duplicate segments.
	type pair struct{ aIdx, bIdx int }
	var pairs []pair

	var backtrack func(i, j int)
	backtrack = func(i, j int) {
		if i == 0 || j == 0 {
			return
		}
		if a[i-1] == b[j-1] {
			pairs = append(pairs, pair{aIdx: i - 1, bIdx: j - 1})
			backtrack(i-1, j-1)
		} else if dp[i-1][j] > dp[i][j-1] {
			backtrack(i-1, j)
		} else {
			backtrack(i, j-1)
		}
	}
	backtrack(m, n)

	// pairs is in reverse order (last character first), reverse it
	for l, r := 0, len(pairs)-1; l < r; l, r = l+1, r-1 {
		pairs[l], pairs[r] = pairs[r], pairs[l]
	}

	// Group consecutive position pairs into contiguous segments.
	// Consecutive meaning: pair[i+1].aIdx == pair[i].aIdx+1 && pair[i+1].bIdx == pair[i].bIdx+1
	var segments []struct {
		startA, startB int
		length         int
	}

	i := 0
	for i < len(pairs) {
		segStartA := pairs[i].aIdx
		segStartB := pairs[i].bIdx
		segLen := 1
		for i+1 < len(pairs) && pairs[i+1].aIdx == pairs[i].aIdx+1 && pairs[i+1].bIdx == pairs[i].bIdx+1 {
			segLen++
			i++
		}
		segments = append(segments, struct {
			startA, startB int
			length         int
		}{startA: segStartA, startB: segStartB, length: segLen})
		i++
	}

	// Convert to LCIMatch, filter by minMatchLen
	matches := make([]LCIMatch, 0)
	for _, seg := range segments {
		if seg.length >= minMatchLen {
			matches = append(matches, LCIMatch{
				Value:    a[seg.startA : seg.startA+seg.length],
				StartA:   seg.startA,
				EndA:     seg.startA + seg.length - 1,
				StartB:   seg.startB,
				EndB:     seg.startB + seg.length - 1,
				MatchLen: seg.length,
			})
		}
	}

	// Sort by position in A
	sort.Slice(matches, func(i, j int) bool {
		return matches[i].StartA < matches[j].StartA
	})

	return matches
}

// GetLCS reads two string keys and computes the LCS.
func (s *BotreonStore) GetLCS(key1, key2 string) (string, error) {
	val1, err := s.Get(key1)
	if err != nil {
		return "", err
	}
	val2, err := s.Get(key2)
	if err != nil {
		return "", err
	}
	lcs, _ := computeLCS(val1, val2)
	return lcs, nil
}

// GetLCSLength reads two string keys and returns the LCS length.
func (s *BotreonStore) GetLCSLength(key1, key2 string) (int, error) {
	val1, err := s.Get(key1)
	if err != nil {
		return 0, err
	}
	val2, err := s.Get(key2)
	if err != nil {
		return 0, err
	}
	return computeLCSLength(val1, val2), nil
}

// GetLCSWithValues reads two string keys and returns both values.
func (s *BotreonStore) GetLCSWithValues(key1, key2 string) (string, string, error) {
	val1, err := s.Get(key1)
	if err != nil {
		return "", "", err
	}
	val2, err := s.Get(key2)
	if err != nil {
		return "", "", err
	}
	return val1, val2, nil
}
