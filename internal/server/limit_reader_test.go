package server

import (
	"io"
	"strings"
	"testing"
)

func TestCumulativeLimitReader_Unlimited(t *testing.T) {
	t.Parallel()
	src := strings.NewReader("hello world")
	r := NewCumulativeLimitReader(src, 0) // 0 = unlimited
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "hello world" {
		t.Fatalf("expected %q, got %q", "hello world", string(data))
	}
	if r.Total() != 11 {
		t.Fatalf("expected total 11, got %d", r.Total())
	}
}

func TestCumulativeLimitReader_BelowLimit(t *testing.T) {
	t.Parallel()
	src := strings.NewReader("hello")
	r := NewCumulativeLimitReader(src, 100)
	data := make([]byte, 10)
	n, err := r.Read(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 5 {
		t.Fatalf("expected 5 bytes, got %d", n)
	}
	if r.Total() != 5 {
		t.Fatalf("expected total 5, got %d", r.Total())
	}
}

func TestCumulativeLimitReader_ExceedsLimit(t *testing.T) {
	t.Parallel()
	src := strings.NewReader("hello world this is a test")
	r := NewCumulativeLimitReader(src, 10)

	// First read: 10 bytes should succeed (at limit)
	buf := make([]byte, 20)
	n, err := r.Read(buf)
	if err != nil {
		t.Fatalf("first read unexpected error: %v", err)
	}
	if n != 10 {
		t.Fatalf("expected 10 bytes, got %d", n)
	}
	if r.Total() != 10 {
		t.Fatalf("expected total 10, got %d", r.Total())
	}

	// Second read: should fail with ErrInputLimitExceeded
	n, err = r.Read(buf)
	if err != ErrInputLimitExceeded {
		t.Fatalf("expected ErrInputLimitExceeded, got %v (n=%d)", err, n)
	}
	if r.Total() != 10 {
		t.Fatalf("total should still be 10, got %d", r.Total())
	}
}

func TestCumulativeLimitReader_ExactLimit(t *testing.T) {
	t.Parallel()
	src := strings.NewReader("1234567890")
	r := NewCumulativeLimitReader(src, 10)

	// io.ReadAll reads all data; when the limit is reached on the next read
	// it returns the data read so far + ErrInputLimitExceeded.
	data, err := io.ReadAll(r)
	// We got data "1234567890" + the limit error (expected).
	if string(data) != "1234567890" {
		t.Fatalf("expected %q, got %q", "1234567890", string(data))
	}
	if err != ErrInputLimitExceeded {
		t.Fatalf("expected ErrInputLimitExceeded, got %v", err)
	}
	if r.Total() != 10 {
		t.Fatalf("expected total 10, got %d", r.Total())
	}
}

func TestCumulativeLimitReader_Reset(t *testing.T) {
	t.Parallel()
	src := strings.NewReader("abcdefghijklmnopqrstuvwxyz")
	r := NewCumulativeLimitReader(src, 10)

	// Read 10 bytes, then reset
	buf := make([]byte, 20)
	n, _ := r.Read(buf)
	if n != 10 {
		t.Fatalf("expected 10 before reset, got %d", n)
	}
	r.ResetLimit(0)

	// After reset, should be able to read more
	n, err := r.Read(buf)
	if err != nil {
		t.Fatalf("after reset unexpected error: %v", err)
	}
	if n == 0 {
		t.Fatal("expected data after reset")
	}
}

func TestCumulativeLimitReader_MultipleSmallReads(t *testing.T) {
	t.Parallel()
	src := strings.NewReader("abcdefghij")
	r := NewCumulativeLimitReader(src, 8)

	// Read 3, then 3, then 3 — third should hit limit
	buf := make([]byte, 3)
	for i := 0; i < 2; i++ {
		n, err := r.Read(buf)
		if err != nil {
			t.Fatalf("read %d unexpected error: %v", i, err)
		}
		if n != 3 {
			t.Fatalf("read %d expected 3 bytes, got %d", i, n)
		}
	}
	// Third read: 2 bytes available but limit is 8, we read 6 already
	n, err := r.Read(buf)
	if err != nil {
		t.Fatalf("third read unexpected error: %v", err)
	}
	if n != 2 {
		t.Fatalf("third read expected 2 bytes, got %d", n)
	}
	// Fourth read: should hit limit
	_, err = r.Read(buf)
	if err != ErrInputLimitExceeded {
		t.Fatalf("expected ErrInputLimitExceeded, got %v", err)
	}
}
