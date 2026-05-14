package store

import (
	"testing"
)

// TestHyperLogLogEncodeDecode tests encodeRegister and decodeRegister
func TestHyperLogLogEncodeDecode(t *testing.T) {
	tests := []struct {
		name     string
		input    uint8
		expected byte
	}{
		{"zero", 0, 0},
		{"max value", 63, 63},
		{"random value", 42, 42},
		{"value with high bits", 0xFF, 0x3F},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := encodeRegister(tt.input)
			if encoded != tt.expected {
				t.Errorf("encodeRegister(%d) = %d, want %d", tt.input, encoded, tt.expected)
			}
			decoded := decodeRegister(encoded)
			if decoded != tt.expected {
				t.Errorf("decodeRegister(%d) = %d, want %d", encoded, decoded, tt.expected)
			}
		})
	}
}

// TestNewHyperLogLog tests newHyperLogLog
func TestNewHyperLogLog(t *testing.T) {
	hll := newHyperLogLog()
	if hll == nil {
		t.Fatal("newHyperLogLog returned nil")
	}
	if hll.encoding != 1 { // sparse encoding
		t.Errorf("newHyperLogLog encoding = %d, want 1", hll.encoding)
	}
	if hll.registers != nil {
		t.Errorf("newHyperLogLog registers = %v, want nil", hll.registers)
	}
}

// TestHashData tests hashData
func TestHashData(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"empty", ""},
		{"simple", "hello"},
		{"long", "this is a longer string for testing hash function"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hash1 := hashData([]byte(tt.input))
			hash2 := hashData([]byte(tt.input))
			if hash1 != hash2 {
				t.Errorf("hashData not deterministic: %d != %d", hash1, hash2)
			}
			// Hash should be non-zero for non-empty input
			if tt.input != "" && hash1 == 0 {
				t.Errorf("hashData(%q) = 0, want non-zero", tt.input)
			}
		})
	}
}

// TestCountTrailingZeros tests countTrailingZeros
func TestCountTrailingZeros(t *testing.T) {
	tests := []struct {
		name     string
		input    uint64
		expected int
	}{
		{"zero", 0, 64},
		{"one", 1, 0},
		{"two", 2, 1},
		{"four", 4, 2},
		{"eight", 8, 3},
		{"power of 2", 16, 4},
		{"max uint64", ^uint64(0), 0},
		{"all zeros except LSB", ^uint64(0) ^ 1, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := countTrailingZeros(tt.input)
			if result != tt.expected {
				t.Errorf("countTrailingZeros(%d) = %d, want %d", tt.input, result, tt.expected)
			}
		})
	}
}

// TestHyperLogLogEstimate tests Estimate with various states
func TestHyperLogLogEstimate(t *testing.T) {
	// Test uninitialized HLL
	t.Run("uninitialized", func(t *testing.T) {
		hll := &HyperLogLog{encoding: 0}
		est := hll.Estimate()
		if est != 0 {
			t.Errorf("Estimate() = %f, want 0 for uninitialized", est)
		}
	})

	// Test sparse encoding
	t.Run("sparse encoding", func(t *testing.T) {
		hll := newHyperLogLog()
		est := hll.Estimate()
		// Should not panic
		_ = est
	})

	// Test dense encoding
	t.Run("dense encoding with data", func(t *testing.T) {
		hll := &HyperLogLog{
			encoding:  2,
			registers: make([]byte, 16384),
		}
		// Add some data
		hll.add([]byte("item1"))
		hll.add([]byte("item2"))
		hll.add([]byte("item3"))
		est := hll.Estimate()
		if est <= 0 {
			t.Errorf("Estimate() = %f, want > 0", est)
		}
	})
}

// TestHyperLogLogAdd tests add method
func TestHyperLogLogAdd(t *testing.T) {
	hll := newHyperLogLog()

	// First add should return true (changed)
	changed1 := hll.add([]byte("test1"))
	if !changed1 {
		t.Error("add() = false, want true for first addition")
	}

	// Adding same item should return false
	changed2 := hll.add([]byte("test1"))
	if changed2 {
		t.Error("add() = true, want false for duplicate")
	}

	// Adding different item should return true
	changed3 := hll.add([]byte("test2"))
	if !changed3 {
		t.Error("add() = false, want true for new item")
	}

	// Verify encoding changed to dense
	if hll.encoding != 2 {
		t.Errorf("encoding = %d, want 2 after add", hll.encoding)
	}
}

// TestHyperLogLogCount tests count method
func TestHyperLogLogCount(t *testing.T) {
	hll := &HyperLogLog{
		encoding:  2,
		registers: make([]byte, 16384),
	}

	// Empty HLL
	c := hll.count()
	if c != 0 {
		t.Errorf("count() = %d, want 0", c)
	}

	// Add items
	hll.add([]byte("a"))
	hll.add([]byte("b"))
	hll.add([]byte("c"))

	c = hll.count()
	if c == 0 {
		t.Error("count() = 0, want > 0 after adding items")
	}
}

// TestHyperLogLogCountZeros tests countZeros
func TestHyperLogLogCountZeros(t *testing.T) {
	hll := &HyperLogLog{
		encoding:  2,
		registers: make([]byte, 16384),
	}

	// All zeros
	z := hll.countZeros()
	if z != 16384 {
		t.Errorf("countZeros() = %d, want 16384 for all zeros", z)
	}

	// Add some data
	hll.add([]byte("item1"))
	z = hll.countZeros()
	if z == 16384 {
		t.Error("countZeros() = 16384, want < 16384 after adding items")
	}
}

// TestHyperLogLogMerge tests merge method
func TestHyperLogLogMerge(t *testing.T) {
	// Test merge with uninitialized other
	t.Run("merge with uninitialized", func(t *testing.T) {
		hll := newHyperLogLog()
		other := &HyperLogLog{encoding: 0}
		result := hll.merge(other)
		if result {
			t.Error("merge with uninitialized should return false")
		}
	})

	// Test merge sparse to dense
	t.Run("merge sparse to dense", func(t *testing.T) {
		hll := newHyperLogLog()
		hll.add([]byte("item1"))

		other := newHyperLogLog()
		other.add([]byte("item2"))

		changed := hll.merge(other)
		if !changed {
			t.Error("merge should return true when adding new items")
		}
	})

	// Test merge dense to dense
	t.Run("merge dense to dense", func(t *testing.T) {
		hll := &HyperLogLog{
			encoding:  2,
			registers: make([]byte, 16384),
		}
		hll.add([]byte("a"))

		other := &HyperLogLog{
			encoding:  2,
			registers: make([]byte, 16384),
		}
		other.add([]byte("b"))

		changed := hll.merge(other)
		if !changed {
			t.Error("merge should return true when adding new items")
		}
	})
}

// TestPFAdd tests the store-level PFAdd function
func TestPFAdd(t *testing.T) {
	store := setupTestStore(t)

	// Test basic PFAdd
	count, err := store.PFAdd("hll1", "a", "b", "c")
	if err != nil {
		t.Errorf("PFAdd error: %v", err)
	}
	if count != 1 {
		t.Errorf("PFAdd count = %d, want 1", count)
	}

	// Test PFAdd to existing key
	count, err = store.PFAdd("hll1", "d", "e")
	if err != nil {
		t.Errorf("PFAdd error: %v", err)
	}
	if count != 1 {
		t.Errorf("PFAdd count = %d, want 1", count)
	}

	// Test PFAdd with no elements (should return 0)
	count, err = store.PFAdd("hll2")
	if err != nil {
		t.Errorf("PFAdd error: %v", err)
	}
	if count != 0 {
		t.Errorf("PFAdd count = %d, want 0", count)
	}
}

// TestPFCount tests the store-level PFCount function
func TestPFCount(t *testing.T) {
	store := setupTestStore(t)

	// Add elements to first HLL
	store.PFAdd("hll1", "a", "b", "c")

	// Test single key count
	count, err := store.PFCount("hll1")
	if err != nil {
		t.Errorf("PFCount error: %v", err)
	}
	if count <= 0 {
		t.Errorf("PFCount = %d, want > 0", count)
	}

	// Add elements to second HLL
	store.PFAdd("hll2", "c", "d", "e")

	// Test multiple keys count
	count, err = store.PFCount("hll1", "hll2")
	if err != nil {
		t.Errorf("PFCount error: %v", err)
	}
	if count <= 0 {
		t.Errorf("PFCount = %d, want > 0", count)
	}

	// Test non-existent key
	count, err = store.PFCount("nonexistent")
	if err != nil {
		t.Errorf("PFCount error: %v", err)
	}
	if count != 0 {
		t.Errorf("PFCount = %d, want 0 for nonexistent key", count)
	}
}

// TestPFMerge tests the store-level PFMerge function
func TestPFMerge(t *testing.T) {
	store := setupTestStore(t)

	// Create two HLLs
	store.PFAdd("hll1", "a", "b", "c")
	store.PFAdd("hll2", "d", "e", "f")

	// Merge hll2 into hll1
	err := store.PFMerge("hll1", "hll2")
	if err != nil {
		t.Errorf("PFMerge error: %v", err)
	}

	// Verify merged count
	count, err := store.PFCount("hll1")
	if err != nil {
		t.Errorf("PFCount error: %v", err)
	}
	if count < 3 {
		t.Errorf("PFCount = %d, want >= 3", count)
	}
}

// TestPFInfo tests the store-level PFInfo function
func TestPFInfo(t *testing.T) {
	store := setupTestStore(t)

	// Test PFInfo on non-existent key
	_, err := store.PFInfo("nonexistent")
	if err == nil {
		t.Error("PFInfo should error on nonexistent key")
	}

	// Create a HLL
	store.PFAdd("hll1", "a", "b", "c")

	// Test PFInfo on existing key
	info, err := store.PFInfo("hll1")
	if err != nil {
		t.Errorf("PFInfo error: %v", err)
	}
	if info == nil {
		t.Fatal("PFInfo returned nil")
	}
	// The map should have at least some keys
	if len(info) == 0 {
		t.Error("PFInfo returned empty map")
	}
}
