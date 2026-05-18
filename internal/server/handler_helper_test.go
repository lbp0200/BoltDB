package server

import (
	"math"
	"testing"

	"github.com/zeebo/assert"
)

func TestParseScore(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input    string
		expected float64
		hasError bool
	}{
		// Regular numbers
		{"0", 0, false},
		{"1", 1, false},
		{"-1", -1, false},
		{"3.14", 3.14, false},
		{"-2.5", -2.5, false},
		{"100", 100, false},

		// Special values
		{"-inf", math.Inf(-1), false},
		{"+inf", math.Inf(1), false},
		{"inf", math.Inf(1), false},

		// Exclusive special values
		{"-inf(", math.Inf(-1), false},
		{"-inf[", math.Inf(-1), false},
		{"+inf(", math.Inf(1), false},
		{"+inf[", math.Inf(1), false},

		// Invalid
		{"abc", 0, true},
		{"", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result, err := parseScore(tt.input)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				// Handle NaN comparison
				if math.IsNaN(result) {
					assert.True(t, math.IsNaN(tt.expected))
				} else if math.IsInf(result, 1) {
					assert.True(t, math.IsInf(tt.expected, 1))
				} else if math.IsInf(result, -1) {
					assert.True(t, math.IsInf(tt.expected, -1))
				} else {
					assert.Equal(t, tt.expected, result)
				}
			}
		})
	}
}

func TestParseScoreExclusive(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input       string
		expectedVal float64
		expectedExc bool
		hasError    bool
	}{
		// Regular inclusive
		{"0", 0, false, false},
		{"1", 1, false, false},
		{"3.14", 3.14, false, false},

		// Exclusive with parenthesis
		{"(0", 0, true, false},
		{"(1", 1, true, false},
		{"(3.14", 3.14, true, false},

		// Inclusive with bracket
		{"[0", 0, false, false},
		{"[1", 1, false, false},

		// Special values
		{"-inf", math.Inf(-1), false, false},
		{"+inf", math.Inf(1), false, false},
		{"(-inf", math.Inf(-1), true, false},
		{"(+inf", math.Inf(1), true, false},

		// Invalid
		{"abc", 0, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result, exclusive, err := parseScoreExclusive(tt.input)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedExc, exclusive)
				// Handle NaN/Infinity comparison
				if math.IsInf(result, 1) {
					assert.True(t, math.IsInf(tt.expectedVal, 1))
				} else if math.IsInf(result, -1) {
					assert.True(t, math.IsInf(tt.expectedVal, -1))
				} else {
					assert.Equal(t, tt.expectedVal, result)
				}
			}
		})
	}
}
