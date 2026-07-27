package logger

import (
	"io"
	"testing"

	"github.com/rs/zerolog"
	"github.com/zeebo/assert"
)

func withSilentLogger(t *testing.T, fn func()) {
	t.Helper()
	orig := Logger
	Logger = zerolog.New(io.Discard)
	t.Cleanup(func() { Logger = orig })
	fn()
}

func TestDebug_Info_Warning_Error_Coverage(t *testing.T) {
	t.Parallel()
	withSilentLogger(t, func() {
		SetLevel(zerolog.DebugLevel)
		Debug("test %s", "debug")
		Info("test %s", "info")
		Warning("test %s", "warn")
		Error("test %s", "err")
	})
}

func TestDebugWith_InfoWith_WarningWith_ErrorWith_Coverage(t *testing.T) {
	t.Parallel()
	withSilentLogger(t, func() {
		SetLevel(zerolog.DebugLevel)
		e := DebugWith("key", "val")
		assert.NotNil(t, e)
		e.Send()

		e = InfoWith("key", "val")
		assert.NotNil(t, e)
		e.Send()

		e = WarningWith("key", "val")
		assert.NotNil(t, e)
		e.Send()

		e = ErrorWith("key", "val")
		assert.NotNil(t, e)
		e.Send()
	})
}
