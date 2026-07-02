package server

import (
	"errors"
	"io"
	"sync/atomic"
)

// ErrInputLimitExceeded is returned when a client connection exceeds its
// cumulative input byte limit, as a safeguard against OOM from large bulk
// requests (see F2a).
var ErrInputLimitExceeded = errors.New("input limit exceeded")

// CumulativeLimitReader wraps an io.Reader and enforces a total byte limit
// across all reads. Once the cumulative count reaches the limit, every
// subsequent Read returns ErrInputLimitExceeded.
// It is safe for concurrent use (atomics on the counter).
type CumulativeLimitReader struct {
	r     io.Reader
	limit int64
	total atomic.Int64
}

// NewCumulativeLimitReader creates a reader that allows at most limit bytes
// to be read cumulatively. limit <= 0 means unlimited.
func NewCumulativeLimitReader(r io.Reader, limit int64) *CumulativeLimitReader {
	return &CumulativeLimitReader{r: r, limit: limit}
}

// Read reads from the underlying reader up to len(p) bytes, subject to the
// cumulative limit. Returns ErrInputLimitExceeded if the limit would be
// exceeded. Single reads are capped to not overshoot the limit.
func (c *CumulativeLimitReader) Read(p []byte) (int, error) {
	if c.limit > 0 {
		prev := c.total.Load()
		if prev >= c.limit {
			return 0, ErrInputLimitExceeded
		}
		remaining := c.limit - prev
		if int64(len(p)) > remaining {
			p = p[:remaining]
		}
	}
	n, err := c.r.Read(p)
	if n > 0 {
		c.total.Add(int64(n))
	}
	return n, err
}

// Total returns the cumulative bytes read so far.
func (c *CumulativeLimitReader) Total() int64 {
	return c.total.Load()
}

// ResetLimit resets the total counter and optionally updates the limit.
// Pass 0 for newLimit to keep the existing limit.
func (c *CumulativeLimitReader) ResetLimit(newLimit int64) {
	c.total.Store(0)
	if newLimit > 0 {
		c.limit = newLimit
	}
}
