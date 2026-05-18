package helper

import (
	"math"
	"testing"

	"github.com/zeebo/assert"
)

func TestUint64ToBytes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input    uint64
		expected []byte
	}{
		{0, []byte{0, 0, 0, 0, 0, 0, 0, 0}},
		{1, []byte{0, 0, 0, 0, 0, 0, 0, 1}},
		{256, []byte{0, 0, 0, 0, 0, 0, 1, 0}},
		{math.MaxUint64, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			result := Uint64ToBytes(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBytesToUint64(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input    []byte
		expected uint64
	}{
		{[]byte{0, 0, 0, 0, 0, 0, 0, 0}, 0},
		{[]byte{0, 0, 0, 0, 0, 0, 0, 1}, 1},
		{[]byte{0, 0, 0, 0, 0, 0, 1, 0}, 256},
		{[]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, math.MaxUint64},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			result := BytesToUint64(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBytesToUint64_InvalidLength(t *testing.T) {
	t.Parallel()
	// Test with invalid length
	result := BytesToUint64([]byte{1, 2, 3})
	assert.Equal(t, uint64(0), result)

	// Test with empty slice
	result = BytesToUint64([]byte{})
	assert.Equal(t, uint64(0), result)
}

func TestFloat64ToBytes(t *testing.T) {
	t.Parallel()
	tests := []float64{
		0,
		1,
		-1,
		3.14,
		math.MaxFloat64,
		math.SmallestNonzeroFloat64,
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			result := Float64ToBytes(tt)
			assert.Equal(t, 8, len(result))
		})
	}
}

func TestBytesToFloat64(t *testing.T) {
	t.Parallel()
	tests := []float64{
		0,
		1,
		-1,
		3.14,
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			bytes := Float64ToBytes(tt)
			result, err := BytesToFloat64(bytes)
			assert.NoError(t, err)
			assert.Equal(t, tt, result)
		})
	}
}

func TestBytesToFloat64_InvalidLength(t *testing.T) {
	t.Parallel()
	// Test with invalid length
	_, err := BytesToFloat64([]byte{1, 2, 3})
	assert.Error(t, err)

	// Test with empty slice
	_, err = BytesToFloat64([]byte{})
	assert.Error(t, err)
}

func TestInterfaceToBytes(t *testing.T) {
	t.Parallel()
	// Test with string
	data := "hello world"
	bytes, err := InterfaceToBytes(data)
	assert.NoError(t, err)
	assert.True(t, len(bytes) > 0)

	// Test with int
	data2 := 42
	bytes2, err := InterfaceToBytes(data2)
	assert.NoError(t, err)
	assert.True(t, len(bytes2) > 0)

	// Test with map
	data3 := map[string]int{"a": 1, "b": 2}
	bytes3, err := InterfaceToBytes(data3)
	assert.NoError(t, err)
	assert.True(t, len(bytes3) > 0)
}
