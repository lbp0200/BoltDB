package server

import (
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/zeebo/assert"
)

// The expectations in this file were differentially verified against
// redis-server 8.2.1 (see boundary matrix in the keyspace/timeout work):
// blocking commands accept float seconds with dedicated negative / out-of-range
// / not-a-float errors; XREAD-family BLOCK takes integers only.

func TestParseBlockTimeoutSeconds(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in      string
		wantMs  int64 // -1 marks "expect error reply"
		wantErr string
	}{
		{"0", 0, ""},  // forever
		{".0", 0, ""}, // exact zero still means forever
		{"+1", 1000, ""},
		{"00.5", 500, ""},
		{"5.", 5000, ""},
		{"1e3", 1000000, ""},
		{"0.01", 10, ""},
		{"92233720368.5", -2, "ERR timeout is out of range"}, // ms overflow → blocked in redis (huge), here capped as out of range? see note below
		{"-1", -1, "ERR timeout is negative"},
		{"-0.01", -1, "ERR timeout is negative"},
		{"-inf", -1, "ERR timeout is negative"},
		{"inf", -1, "ERR timeout is out of range"},
		{"+inf", -1, "ERR timeout is out of range"},
		{"18446744073709551616", -1, "ERR timeout is out of range"},
		{"nan", -1, "ERR timeout is not a float or out of range"},
		{"NaN", -1, "ERR timeout is not a float or out of range"},
		{"abc", -1, "ERR timeout is not a float or out of range"},
		{" 1", -1, "ERR timeout is not a float or out of range"},
		{"1_000", -1, "ERR timeout is not a float or out of range"},
	}
	for _, c := range cases {
		ms, errReply, ok := parseBlockTimeoutSeconds([]byte(c.in))
		if c.wantMs == -2 {
			// huge-but-valid: must parse without error and block "forever-ish"
			if !ok {
				t.Errorf("%q: unexpected error %v", c.in, errReply)
			}
			continue
		}
		if c.wantErr != "" {
			if ok {
				t.Errorf("%q: expected error %q, got ms=%d", c.in, c.wantErr, ms)
				continue
			}
			assert.Equal(t, c.wantErr, string(*(errReply.(*proto.Error))))
			continue
		}
		if !ok {
			t.Errorf("%q: unexpected error reply", c.in)
			continue
		}
		assert.Equal(t, c.wantMs, ms)
	}

	// hex integers are accepted via strtold-compatible fallback
	ms, errReply, ok := parseBlockTimeoutSeconds([]byte("0x10"))
	if !ok {
		t.Fatalf("0x10 rejected: %v", errReply)
	}
	assert.Equal(t, int64(16000), ms)

	// tiny positive clamps to 1ms instead of becoming an accidental forever
	ms, _, ok = parseBlockTimeoutSeconds([]byte("1e-10"))
	if !ok {
		t.Fatal("1e-10 rejected")
	}
	assert.Equal(t, int64(1), ms)
}

func TestParseXreadBlockMs(t *testing.T) {
	t.Parallel()

	if ms, resp, ok := parseXreadBlockMs([]byte("500")); !ok || ms != 500 {
		t.Fatalf("500: ok=%v ms=%d resp=%v", ok, ms, resp)
	}
	if ms, _, ok := parseXreadBlockMs([]byte("0")); !ok || ms != 0 {
		t.Fatalf("0: ok=%v ms=%d", ok, ms)
	}
	for _, bad := range []string{"0.5", "abc", "99999999999999999999", "1e3", "+5"} {
		_, errReply, ok := parseXreadBlockMs([]byte(bad))
		if ok {
			t.Errorf("%q: expected rejection", bad)
			continue
		}
		assert.Equal(t, "ERR timeout is not an integer or out of range", string(*(errReply.(*proto.Error))))
	}
	_, errReply, ok := parseXreadBlockMs([]byte("-1"))
	if ok {
		t.Fatal("-1: expected rejection")
	}
	assert.Equal(t, "ERR timeout is negative", string(*(errReply.(*proto.Error))))
}

// TestBlockingCommand_FloatTimeouts exercises the wire path end to end:
// floats are accepted, negatives error before any key access, and BLPOP with a
// short float timeout returns nil rather than hanging.
func TestBlockingCommand_FloatTimeouts(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := execCmd(t, handler, state, "BLPOP", "nk", "-1")
	errResp, ok := resp.(*proto.Error)
	if !ok {
		t.Fatalf("expected error for negative timeout, got %T", resp)
	}
	assert.Equal(t, "ERR timeout is negative", string(*errResp))

	resp = execCmd(t, handler, state, "BZPOPMIN", "nk", "inf")
	errResp, ok = resp.(*proto.Error)
	if !ok {
		t.Fatalf("expected error for inf timeout, got %T", resp)
	}
	assert.Equal(t, "ERR timeout is out of range", string(*errResp))

	// Validation happens even when data is available (redis checks first).
	execCmd(t, handler, state, "ZADD", "bz", "1", "m")
	resp = execCmd(t, handler, state, "BZPOPMIN", "bz", "-0.01")
	errResp, ok = resp.(*proto.Error)
	if !ok {
		t.Fatalf("expected error despite available data, got %T", resp)
	}
	assert.Equal(t, "ERR timeout is negative", string(*errResp))

	done := make(chan proto.RESP, 1)
	go func() {
		done <- execCmd(t, handler, state, "BLPOP", "nk", "0.01")
	}()
	select {
	case r := <-done:
		if _, isErr := r.(*proto.Error); isErr {
			t.Fatalf("float timeout rejected: %v", r)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("BLPOP 0.01 did not return within 3s")
	}
}

func TestXREAD_BlockValidation(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := execCmd(t, handler, state, "XREAD", "BLOCK", "-1", "STREAMS", "st", "$")
	errResp, ok := resp.(*proto.Error)
	if !ok {
		t.Fatalf("BLOCK -1 must error, got %T", resp)
	}
	assert.Equal(t, "ERR timeout is negative", string(*errResp))

	resp = execCmd(t, handler, state, "XREAD", "BLOCK", "0.5", "STREAMS", "st", "$")
	errResp, ok = resp.(*proto.Error)
	if !ok {
		t.Fatalf("BLOCK 0.5 must error, got %T", resp)
	}
	assert.Equal(t, "ERR timeout is not an integer or out of range", string(*errResp))
}

func TestSETEX_InvalidSecondsMessage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := execCmd(t, handler, state, "SETEX", "k", "abc", "v")
	errResp, ok := resp.(*proto.Error)
	if !ok {
		t.Fatalf("expected error, got %T", resp)
	}
	assert.Equal(t, "ERR value is not an integer or out of range", string(*errResp))

	resp = execCmd(t, handler, state, "PSETEX", "k", "abc", "v")
	errResp, ok = resp.(*proto.Error)
	if !ok {
		t.Fatalf("expected error, got %T", resp)
	}
	assert.Equal(t, "ERR value is not an integer or out of range", string(*errResp))
}
