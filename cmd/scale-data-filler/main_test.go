package main

import (
	"testing"

	"github.com/lbp0200/BoltDB/internal/cluster"
)

// TestKeySlotMatchesCluster verifies the filler's built-in CRC16 slot
// calculation agrees with the server-side internal/cluster.Slot used for
// routing. If they diverge, keys would be pipelined to the wrong node and
// Exec would fail with MOVED (the exact problem this rewrite fixes).
func TestKeySlotMatchesCluster(t *testing.T) {
	keys := []string{
		"scale:k:000000000000",
		"scale:k:000000000001",
		"scale:k:000000001234",
		"scale:k:000000999999",
		"scale:k:000012345678",
		"plain-key-without-braces",
		"{user1000}.following",
		"foo{bar}zap",
		"key{user123}",
	}
	for _, key := range keys {
		got := keySlot(key)
		want := int(cluster.Slot(key))
		if got != want {
			t.Errorf("keySlot(%q) = %d, cluster.Slot = %d", key, got, want)
		}
	}
}

func TestCRC16KnownValue(t *testing.T) {
	// CRC-16/XModem of "123456789" is 0x31C3 per the standard check value.
	if got := crc16([]byte("123456789")); got != 0x31C3 {
		t.Errorf("crc16(\"123456789\") = %#04x, want 0x31c3", got)
	}
}

func TestOwnerAddr(t *testing.T) {
	owners := []slotOwner{
		{start: 0, end: 5460, addr: "10.0.0.1:6379"},
		{start: 5461, end: 10922, addr: "10.0.0.2:6379"},
		{start: 10923, end: 16383, addr: "10.0.0.3:6379"},
	}
	cases := []struct {
		slot int
		want string
	}{
		{0, "10.0.0.1:6379"},
		{5460, "10.0.0.1:6379"},
		{5461, "10.0.0.2:6379"},
		{10922, "10.0.0.2:6379"},
		{10923, "10.0.0.3:6379"},
		{16383, "10.0.0.3:6379"},
	}
	for _, c := range cases {
		if got := ownerAddr(owners, c.slot); got != c.want {
			t.Errorf("ownerAddr(%d) = %q, want %q", c.slot, got, c.want)
		}
	}
	// Unassigned slot → empty
	if got := ownerAddr(owners, 20000); got != "" {
		t.Errorf("ownerAddr(20000) = %q, want empty", got)
	}
	if got := ownerAddr(nil, 5); got != "" {
		t.Errorf("ownerAddr on empty owners = %q, want empty", got)
	}
}

func TestCountOwnerAddrs(t *testing.T) {
	owners := []slotOwner{
		{start: 0, end: 100, addr: "a:1"},
		{start: 101, end: 200, addr: "a:1"},
		{start: 201, end: 300, addr: "b:2"},
	}
	if got := countOwnerAddrs(owners); got != 2 {
		t.Errorf("countOwnerAddrs = %d, want 2", got)
	}
}
