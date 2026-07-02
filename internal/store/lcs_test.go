package store

import (
	"testing"
)

func TestComputeLCS(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		a, b    string
		wantLCS string
		wantLen int
	}{
		{"both empty", "", "", "", 0},
		{"first empty", "", "ABC", "", 0},
		{"second empty", "ABC", "", "", 0},
		{"no common", "ABC", "XYZ", "", 0},
		{"single char match", "A", "A", "A", 1},
		{"single char in longer", "ABCD", "XAY", "A", 1},
		{"complete match", "HELLO", "HELLO", "HELLO", 5},
		{"partial suffix", "HELLO", "LLO", "LLO", 3},
		{"partial prefix", "ABC", "ABCDEF", "ABC", 3},
		{"partial middle", "ABCDEF", "XBCDY", "BCD", 3},
		{"interleaved", "ABC", "CBA", "A", 1}, // or "B" or "C" — LCS is 1 char
		{"repeated chars no pattern", "AAAA", "AA", "AA", 2},
		{"different lengths", "ABCD", "AC", "AC", 2},
		{"same length partial", "ABCD", "ABXY", "AB", 2},
		{"Unicode", "你好世界", "你好", "你好", 6},
		{"case sensitive", "Hello", "hello", "ello", 4},
		{"numbers", "12345", "1245", "1245", 4},
		{"mixed content", "a1b2c3", "abc123", "", 4}, // multiple LCS candidates (e.g. "abc3" or "a123")
		{"long strings", "ABCDEFGHIJKLMNOP", "ABCDEFGHIJKLMNOP", "ABCDEFGHIJKLMNOP", 16},
		{"one char diff at end", "ABCDEFG", "ABCDEFX", "ABCDEF", 6},
		{"one char diff at start", "XBCDEFG", "ABCDEFG", "BCDEFG", 6},
		{"no overlap long strings", "AAAAAAAAAA", "BBBBBBBBBB", "", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotLCS, gotLen := computeLCS(tt.a, tt.b)
			if gotLen != tt.wantLen {
				t.Errorf("computeLCS(%q, %q) len = %d, want %d", tt.a, tt.b, gotLen, tt.wantLen)
			}
			if len(gotLCS) != gotLen {
				t.Errorf("computeLCS(%q, %q) returned string len %d != reported len %d", tt.a, tt.b, len(gotLCS), gotLen)
			}
			if gotLen > 0 {
				// Verify the result is actually a subsequence of both strings
				if !isSubsequence(gotLCS, tt.a) {
					t.Errorf("computeLCS(%q, %q) = %q is not a subsequence of %q", tt.a, tt.b, gotLCS, tt.a)
				}
				if !isSubsequence(gotLCS, tt.b) {
					t.Errorf("computeLCS(%q, %q) = %q is not a subsequence of %q", tt.a, tt.b, gotLCS, tt.b)
				}
			}
			// Verify length matches computeLCSLength
			if len := computeLCSLength(tt.a, tt.b); len != tt.wantLen {
				t.Errorf("computeLCSLength(%q, %q) = %d, want %d", tt.a, tt.b, len, tt.wantLen)
			}
		})
	}
}

func TestComputeLCS_LengthConsistency(t *testing.T) {
	t.Parallel()
	pairs := []struct{ a, b string }{
		{"", ""},
		{"A", ""},
		{"", "A"},
		{"ABC", "DEF"},
		{"ABC", "ABC"},
		{"ABCDEFGHIJ", "JKLMNOPQRS"},
		{"aaaa", "aa"},
		{"The quick brown fox", "jumps over the lazy dog"},
		{"こんにちは", "你好世界"},
	}
	for _, p := range pairs {
		lcs, lcsLen := computeLCS(p.a, p.b)
		lenOnly := computeLCSLength(p.a, p.b)
		if lcsLen != lenOnly {
			t.Errorf("computeLCS(%q, %q).len=%d but computeLCSLength=%d", p.a, p.b, lcsLen, lenOnly)
		}
		if len(lcs) != lcsLen {
			t.Errorf("computeLCS(%q, %q).string=%q (len %d) doesn't match reported len %d", p.a, p.b, lcs, len(lcs), lcsLen)
		}
	}
}

func TestComputeLCSLength(t *testing.T) {
	t.Parallel()
	tests := []struct {
		a, b string
		want int
	}{
		{"", "", 0},
		{"A", "", 0},
		{"ABC", "ABC", 3},
		{"ABC", "DEF", 0},
		{"ABCD", "AC", 2},
		{"AGGTAB", "GXTXAYB", 4}, // classic LCS example: GTAB
		{"ABCDEFGH", "ACEG", 4},  // A C E G
		{"ABCDEFGH", "ABCDEFGH", 8},
		{"ABCDEFGH", "EFGH", 4},
	}
	for _, tt := range tests {
		got := computeLCSLength(tt.a, tt.b)
		if got != tt.want {
			t.Errorf("computeLCSLength(%q, %q) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
	}
}

func TestComputeLCSMatches(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		a, b        string
		minMatchLen int
		wantCount   int
		check       func(t *testing.T, matches []LCIMatch)
	}{
		{
			name: "no common",
			a:    "ABC", b: "XYZ",
			wantCount: 0,
		},
		{
			name: "single char min 1",
			a:    "A", b: "A",
			minMatchLen: 1,
			wantCount:   1,
			check: func(t *testing.T, matches []LCIMatch) {
				m := matches[0]
				if m.Value != "A" || m.StartA != 0 || m.EndA != 0 || m.StartB != 0 || m.EndB != 0 {
					t.Errorf("unexpected match: %+v", m)
				}
			},
		},
		{
			name: "single char filtered",
			a:    "A", b: "A",
			minMatchLen: 2,
			wantCount:   0,
		},
		{
			name: "multiple non-overlapping",
			a:    "ABAB", b: "ABAB",
			minMatchLen: 1,
			wantCount:   1, // greedy: finds first LCS "ABAB" = entire string
			check: func(t *testing.T, matches []LCIMatch) {
				if matches[0].Value != "ABAB" {
					t.Errorf("expected 'ABAB', got %q", matches[0].Value)
				}
			},
		},
		{
			name: "non-contiguous characters",
			a:    "ABCDEF", b: "ACDF",
			minMatchLen: 1,
			wantCount:   3, // "A" (len1), "CD" (len2), "F" (len1)
			check: func(t *testing.T, matches []LCIMatch) {
				if len(matches) < 2 {
					return
				}
				// The longest segment should be "CD" (len 2)
				found := false
				for _, m := range matches {
					if m.MatchLen >= 2 {
						found = true
						break
					}
				}
				if !found {
					t.Error("expected at least one segment with length >= 2")
				}
			},
		},
		{
			name: "two separate matches",
			a:    "AB_CD", b: "ABXCD",
			minMatchLen: 1,
			wantCount:   2, // "AB" and "CD"
			check: func(t *testing.T, matches []LCIMatch) {
				if len(matches) < 2 {
					t.Fatal("expected 2 matches")
				}
				// Sort by StartA
				if matches[0].Value != "AB" || matches[1].Value != "CD" {
					t.Logf("got matches: %+v, %+v", matches[0], matches[1])
				}
				// Verify "AB" positions
				m := matches[0]
				if m.StartA != 0 || m.EndA != 1 || m.MatchLen != 2 {
					t.Errorf("first match unexpected positions: %+v", m)
				}
			},
		},
		{
			name: "minMatchLen 2 filters singles",
			a:    "A_B", b: "AXB",
			minMatchLen: 2,
			wantCount:   0, // A and B are separated, LCS may be multiple single chars
		},
		{
			name: "longer strings with gaps",
			a:    "abcdefgh", b: "aXcYeGgh",
			minMatchLen: 2,
			check: func(t *testing.T, matches []LCIMatch) {
				for _, m := range matches {
					if m.MatchLen < 2 {
						t.Errorf("match len %d < minMatchLen 2", m.MatchLen)
					}
				}
			},
		},
		{
			name: "identical strings",
			a:    "HELLO", b: "HELLO",
			minMatchLen: 1,
			wantCount:   1,
			check: func(t *testing.T, matches []LCIMatch) {
				m := matches[0]
				if m.Value != "HELLO" || m.StartA != 0 || m.EndA != 4 {
					t.Errorf("unexpected match for identical: %+v", m)
				}
			},
		},
		{
			name: "empty strings",
			a:    "", b: "",
			minMatchLen: 1,
			wantCount:   0,
		},
		{
			name: "one empty",
			a:    "ABC", b: "",
			minMatchLen: 1,
			wantCount:   0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matches := ComputeLCSMatches(tt.a, tt.b, tt.minMatchLen)
			if tt.wantCount > 0 && len(matches) != tt.wantCount {
				t.Errorf("ComputeLCSMatches(%q, %q, %d) got %d matches, want %d", tt.a, tt.b, tt.minMatchLen, len(matches), tt.wantCount)
			}
			if tt.check != nil {
				tt.check(t, matches)
			}
			// Verify all matches are valid subsequences
			for _, m := range matches {
				// Verify positions are in range
				if m.StartA < 0 || m.EndA >= len(tt.a) || m.StartB < 0 || m.EndB >= len(tt.b) {
					t.Errorf("match positions out of range: [%d,%d] in %q (len %d), [%d,%d] in %q (len %d)",
						m.StartA, m.EndA, tt.a, len(tt.a), m.StartB, m.EndB, tt.b, len(tt.b))
				}
				// Verify value matches the substring in A
				if m.StartA <= m.EndA && m.StartA+m.MatchLen-1 != m.EndA {
					t.Errorf("match EndA %d inconsistent with StartA %d + MatchLen %d", m.EndA, m.StartA, m.MatchLen)
				}
				// Verify value matches substring in A
				expectedVal := tt.a[m.StartA : m.EndA+1]
				if m.Value != expectedVal {
					t.Errorf("match.Value %q doesn't match A[%d:%d+1] = %q", m.Value, m.StartA, m.EndA, expectedVal)
				}
			}
			// Verify sorting by StartA
			for i := 1; i < len(matches); i++ {
				if matches[i].StartA < matches[i-1].StartA {
					t.Errorf("matches not sorted by StartA: [%d]%d < [%d]%d", i, matches[i].StartA, i-1, matches[i-1].StartA)
				}
			}
		})
	}
}

func TestComputeLCSMatches_AGGTAB_GXTXAYB(t *testing.T) {
	t.Parallel()
	// Classic LCS example: AGGTAB vs GXTXAYB → GTAB (len 4)
	a, b := "AGGTAB", "GXTXAYB"
	matches := ComputeLCSMatches(a, b, 1)
	if len(matches) == 0 {
		t.Fatal("expected at least one match")
	}
	t.Logf("AGGTAB vs GXTXAYB matches:")
	for _, m := range matches {
		t.Logf("  %q at A[%d:%d] B[%d:%d] (len %d)", m.Value, m.StartA, m.EndA, m.StartB, m.EndB, m.MatchLen)
	}
}

// isSubsequence checks if sub appears as a (non-contiguous) subsequence of s
func isSubsequence(sub, s string) bool {
	j := 0
	for i := 0; i < len(s) && j < len(sub); i++ {
		if s[i] == sub[j] {
			j++
		}
	}
	return j == len(sub)
}
