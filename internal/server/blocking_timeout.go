package server

import (
	"errors"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/lbp0200/BoltDB/internal/proto"
)

// maxTimerMs is the largest millisecond value that still fits into
// time.Duration without nanosecond overflow (~292 years).
const maxTimerMs = math.MaxInt64 / int64(time.Millisecond)

// parseBlockTimeoutSeconds parses the timeout argument of the blocking
// commands (BLPOP/BRPOP/BRPOPLPUSH/BLMOVE/BZPOPMIN/BZPOPMAX/BLMPOP/BZMPOP).
// Semantics differentially verified against redis-server 8.2.1:
//   - float seconds are accepted ("0.01", "5.", ".0", "00.5", "1e3", "+1",
//     and hex integers like "0x10" which strtold also accepts)
//   - exact zero ("0"/".0") means block forever → returns 0ms
//   - negative values (incl. -Inf) → "ERR timeout is negative"
//   - +Inf / millisecond overflow → "ERR timeout is out of range"
//   - NaN / unparseable / embedded whitespace or underscores
//     → "ERR timeout is not a float or out of range"
//   - tiny positive values below 1ms clamp to 1ms (redis returns quickly,
//     it never blocks forever on them)
//
// ok=false means errReply holds the client-facing response.
func parseBlockTimeoutSeconds(arg []byte) (ms int64, errReply proto.RESP, ok bool) {
	s := string(arg)
	notFloat := proto.NewError("ERR timeout is not a float or out of range")

	if strings.ContainsAny(s, "_ \t\n\v\f\r") {
		return 0, notFloat, false
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		if errors.Is(err, strconv.ErrRange) {
			// Overflow/underflow: ParseFloat still yields ±Inf or 0, which
			// the classification below maps to redis's responses.
			err = nil
		} else if v, ierr := strconv.ParseInt(s, 0, 64); ierr == nil {
			// glibc strtold accepts hex integers without a 'p' exponent
			// ("0x10" == 16s); Go's ParseFloat does not, so fall back.
			f = float64(v)
			err = nil
		}
	}
	if err != nil || math.IsNaN(f) {
		return 0, notFloat, false
	}

	const maxMs = float64(math.MaxInt64)
	switch {
	case f < 0:
		// Catches -Inf before the range check, matching redis.
		return 0, proto.NewError("ERR timeout is negative"), false
	case math.IsInf(f, 1) || f*1000 >= maxMs:
		return 0, proto.NewError("ERR timeout is out of range"), false
	case f == 0:
		// Exact zero blocks forever; ".0" behaves like "0".
		return 0, nil, true
	case f*1000 < 1:
		return 1, nil, true
	default:
		ms := int64(f * 1000)
		// Saturate below time.Duration's nanosecond overflow (~292y):
		// redis keeps such timeouts effectively-forever too.
		if ms > maxTimerMs {
			ms = maxTimerMs
		}
		return ms, nil, true
	}
}

// parseXreadBlockMs parses XREAD/XREADGROUP's BLOCK argument. Unlike the
// blocking-pop timeouts, redis requires an integer here: floats, scientific
// notation and a leading '+' are all rejected with
// "ERR timeout is not an integer or out of range"; negatives report
// "ERR timeout is negative".
func parseXreadBlockMs(arg []byte) (int64, proto.RESP, bool) {
	s := string(arg)
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || strings.HasPrefix(s, "+") {
		return 0, proto.NewError("ERR timeout is not an integer or out of range"), false
	}
	if v < 0 {
		return 0, proto.NewError("ERR timeout is negative"), false
	}
	return v, nil, true
}
