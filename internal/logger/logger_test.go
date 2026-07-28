package logger

import (
	"testing"

	"github.com/rs/zerolog"
	"github.com/zeebo/assert"
)

func TestParseLevel(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input    string
		expected zerolog.Level
	}{
		// Debug
		{"debug", zerolog.DebugLevel},
		{"DEBUG", zerolog.DebugLevel},
		{"dbg", zerolog.DebugLevel},
		{"DBG", zerolog.DebugLevel},

		// Info
		{"info", zerolog.InfoLevel},
		{"INFO", zerolog.InfoLevel},
		{"inf", zerolog.InfoLevel},
		{"INF", zerolog.InfoLevel},

		// Warning
		{"warning", zerolog.WarnLevel},
		{"WARNING", zerolog.WarnLevel},
		{"warn", zerolog.WarnLevel},
		{"WARN", zerolog.WarnLevel},

		// Error
		{"error", zerolog.ErrorLevel},
		{"ERROR", zerolog.ErrorLevel},
		{"err", zerolog.ErrorLevel},
		{"ERR", zerolog.ErrorLevel},

		// Fatal
		{"fatal", zerolog.FatalLevel},
		{"FATAL", zerolog.FatalLevel},

		// Panic
		{"panic", zerolog.PanicLevel},
		{"PANIC", zerolog.PanicLevel},

		// Trace
		{"trace", zerolog.TraceLevel},
		{"TRACE", zerolog.TraceLevel},

		// Unknown - defaults to warn
		{"", zerolog.WarnLevel},
		{"unknown", zerolog.WarnLevel},
		{"INVALID", zerolog.WarnLevel},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := parseLevel(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseLevel_WithSpaces(t *testing.T) {
	t.Parallel()
	// Test with leading/trailing spaces
	assert.Equal(t, zerolog.InfoLevel, parseLevel(" info "))
	assert.Equal(t, zerolog.DebugLevel, parseLevel("  debug  "))
	assert.Equal(t, zerolog.ErrorLevel, parseLevel("   error   "))
}

func TestSetLevel(t *testing.T) {
	// NOTE: no t.Parallel() — modifies global Logger, would race with
	// other tests reading Logger via Debug/Info etc.
	// Save original level
	originalLevel := GetLevel()
	defer SetLevel(originalLevel)

	// Set to debug
	SetLevel(zerolog.DebugLevel)
	assert.Equal(t, zerolog.DebugLevel, GetLevel())

	// Set to error
	SetLevel(zerolog.ErrorLevel)
	assert.Equal(t, zerolog.ErrorLevel, GetLevel())
}

func TestSetLevelFromString(t *testing.T) {
	// NOTE: no t.Parallel() — modifies global Logger
	// Save original level
	originalLevel := GetLevel()
	defer SetLevel(originalLevel)

	// Set from string
	SetLevelFromString("debug")
	assert.Equal(t, zerolog.DebugLevel, GetLevel())

	SetLevelFromString("error")
	assert.Equal(t, zerolog.ErrorLevel, GetLevel())

	// Invalid - should default to warn
	SetLevelFromString("invalid_level")
	assert.Equal(t, zerolog.WarnLevel, GetLevel())
}

func TestGetLevelString(t *testing.T) {
	// NOTE: no t.Parallel() — modifies global Logger
	// Save original level
	originalLevel := GetLevel()
	defer SetLevel(originalLevel)

	// Test different levels
	SetLevel(zerolog.DebugLevel)
	assert.Equal(t, "debug", GetLevelString())

	SetLevel(zerolog.InfoLevel)
	assert.Equal(t, "info", GetLevelString())

	SetLevel(zerolog.WarnLevel)
	assert.Equal(t, "warn", GetLevelString())

	SetLevel(zerolog.ErrorLevel)
	assert.Equal(t, "error", GetLevelString())
}
