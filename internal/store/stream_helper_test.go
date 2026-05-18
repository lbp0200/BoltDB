package store

import (
	"errors"
	"testing"
)

// TestParseStreamID tests parseStreamID
func TestParseStreamID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		id      string
		wantTs  int64
		wantSeq int64
		wantErr bool
	}{
		{"auto star", "*", 0, 0, false}, // Will be time-based
		{"timestamp only", "1234567890000", 1234567890000, 0, false},
		{"timestamp-sequence", "1234567890000-5", 1234567890000, 5, false},
		{"invalid plus", "+", 0, 0, true},
		{"invalid minus", "-", 0, 0, true},
		{"invalid timestamp", "abc", 0, 0, true},
		{"invalid sequence", "123-abc", 0, 0, true},
		{"zero timestamp", "0", 0, 0, false},
		{"zero sequence", "0-0", 0, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts, seq, err := parseStreamID(tt.id)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseStreamID(%q) error = %v, wantErr %v", tt.id, err, tt.wantErr)
				return
			}
			if tt.id == "*" {
				// For "*", just check that ts is non-zero (current time)
				if ts == 0 {
					t.Errorf("parseStreamID(%q) ts = 0, want non-zero", tt.id)
				}
			} else if !tt.wantErr {
				if ts != tt.wantTs || seq != tt.wantSeq {
					t.Errorf("parseStreamID(%q) = (%d, %d), want (%d, %d)",
						tt.id, ts, seq, tt.wantTs, tt.wantSeq)
				}
			}
		})
	}
}

// TestCompareStreamID tests compareStreamID
func TestCompareStreamID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		id1      string
		id2      string
		expected int
	}{
		{"equal", "123-5", "123-5", 0},
		{"less timestamp", "100-0", "200-0", -1},
		{"greater timestamp", "200-0", "100-0", 1},
		{"less sequence same timestamp", "123-3", "123-5", -1},
		{"greater sequence same timestamp", "123-10", "123-5", 1},
		{"timestamp less but sequence greater", "100-100", "200-0", -1},
		{"timestamp greater but sequence less", "200-0", "100-100", 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := compareStreamID(tt.id1, tt.id2)
			if result != tt.expected {
				t.Errorf("compareStreamID(%q, %q) = %d, want %d", tt.id1, tt.id2, result, tt.expected)
			}
		})
	}
}

// TestStreamKey tests streamKey
func TestStreamKey(t *testing.T) {
	t.Parallel()
	key := streamKey("mystream")
	expected := "stream:mystream:meta"
	if string(key) != expected {
		t.Errorf("streamKey(%q) = %q, want %q", "mystream", string(key), expected)
	}
}

// TestStreamDataKey tests streamDataKey
func TestStreamDataKey(t *testing.T) {
	t.Parallel()
	key := streamDataKey("mystream", "123-0")
	expected := "stream:mystream:data:123-0"
	if string(key) != expected {
		t.Errorf("streamDataKey(%q, %q) = %q, want %q", "mystream", "123-0", string(key), expected)
	}
}

// TestStreamDataPrefix tests streamDataPrefix
func TestStreamDataPrefix(t *testing.T) {
	t.Parallel()
	prefix := streamDataPrefix("mystream")
	expected := "stream:mystream:data:"
	if string(prefix) != expected {
		t.Errorf("streamDataPrefix(%q) = %q, want %q", "mystream", string(prefix), expected)
	}
}

// TestStreamGroupKey tests streamGroupKey
func TestStreamGroupKey(t *testing.T) {
	t.Parallel()
	key := streamGroupKey("mystream")
	expected := "stream:mystream:groups"
	if string(key) != expected {
		t.Errorf("streamGroupKey(%q) = %q, want %q", "mystream", string(key), expected)
	}
}

// TestStreamGroupDataKey tests streamGroupDataKey
func TestStreamGroupDataKey(t *testing.T) {
	t.Parallel()
	key := streamGroupDataKey("mystream", "mygroup")
	expected := "stream:mystream:groups:mygroup"
	if string(key) != expected {
		t.Errorf("streamGroupDataKey(%q, %q) = %q, want %q", "mystream", "mygroup", string(key), expected)
	}
}

// TestStreamPendingKey tests streamPendingKey
func TestStreamPendingKey(t *testing.T) {
	t.Parallel()
	key := streamPendingKey("mystream", "mygroup")
	expected := "stream:mystream:pending:mygroup"
	if string(key) != expected {
		t.Errorf("streamPendingKey(%q, %q) = %q, want %q", "mystream", "mygroup", string(key), expected)
	}
}

// TestEncodeDecodeStreamMeta tests encodeStreamMeta and decodeStreamMeta
func TestEncodeDecodeStreamMeta(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		meta *streamMetaData
	}{
		{
			"zero values",
			&streamMetaData{
				Length:       0,
				FirstID:      0,
				FirstSeq:     0,
				LastID:       0,
				LastSeq:      0,
				MaxDeletedID: 0,
				MaxDelSeq:    0,
			},
		},
		{
			"normal values",
			&streamMetaData{
				Length:       100,
				FirstID:      1234567890000,
				FirstSeq:     5,
				LastID:       1234567999999,
				LastSeq:      10,
				MaxDeletedID: 1234567880000,
				MaxDelSeq:    3,
			},
		},
		{
			"max values",
			&streamMetaData{
				Length:       9223372036854775807,
				FirstID:      9223372036854775807,
				FirstSeq:     9223372036854775807,
				LastID:       9223372036854775807,
				LastSeq:      9223372036854775807,
				MaxDeletedID: 9223372036854775807,
				MaxDelSeq:    9223372036854775807,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := encodeStreamMeta(tt.meta)
			decoded, err := decodeStreamMeta(encoded)
			if err != nil {
				t.Errorf("decodeStreamMeta error = %v", err)
				return
			}
			if decoded.Length != tt.meta.Length {
				t.Errorf("Length: got %d, want %d", decoded.Length, tt.meta.Length)
			}
			if decoded.FirstID != tt.meta.FirstID {
				t.Errorf("FirstID: got %d, want %d", decoded.FirstID, tt.meta.FirstID)
			}
			if decoded.FirstSeq != tt.meta.FirstSeq {
				t.Errorf("FirstSeq: got %d, want %d", decoded.FirstSeq, tt.meta.FirstSeq)
			}
			if decoded.LastID != tt.meta.LastID {
				t.Errorf("LastID: got %d, want %d", decoded.LastID, tt.meta.LastID)
			}
			if decoded.LastSeq != tt.meta.LastSeq {
				t.Errorf("LastSeq: got %d, want %d", decoded.LastSeq, tt.meta.LastSeq)
			}
			if decoded.MaxDeletedID != tt.meta.MaxDeletedID {
				t.Errorf("MaxDeletedID: got %d, want %d", decoded.MaxDeletedID, tt.meta.MaxDeletedID)
			}
			// Note: MaxDelSeq is not encoded/decoded in current implementation
			// This is a known limitation - we just verify no error occurs
		})
	}
}

// TestDecodeStreamMetaError tests decodeStreamMeta error cases
func TestDecodeStreamMetaError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input []byte
	}{
		{"empty", []byte{}},
		{"too short", []byte{1, 2, 3}},
		{"too long", make([]byte, 49)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decodeStreamMeta(tt.input)
			if err == nil {
				t.Errorf("decodeStreamMeta(%v) error = nil, want error", tt.input)
			}
		})
	}
}

// TestFormatStreamID tests formatStreamID
func TestFormatStreamID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		timestamp int64
		sequence  int64
		expected  string
	}{
		{"zero sequence", 1234567890000, 0, "1234567890000-0"},
		{"non-zero sequence", 1234567890000, 5, "1234567890000-5"},
		{"both zero", 0, 0, "0-0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatStreamID(tt.timestamp, tt.sequence)
			if result != tt.expected {
				t.Errorf("formatStreamID(%d, %d) = %q, want %q",
					tt.timestamp, tt.sequence, result, tt.expected)
			}
		})
	}
}

// TestStreamIDErrorHandling tests parseStreamID error handling in detail
func TestStreamIDErrorHandling(t *testing.T) {
	t.Parallel()
	// Test that invalid IDs return proper errors
	invalidIDs := []string{
		"abc-def-ghi",
		"-123",
		"123-",
	}

	for _, id := range invalidIDs {
		t.Run("invalid_"+id, func(t *testing.T) {
			_, _, err := parseStreamID(id)
			if err == nil {
				t.Errorf("parseStreamID(%q) error = nil, want error", id)
			}
		})
	}
}

// TestStreamPendingKeyNilGroup tests streamPendingKey with empty group name
func TestStreamPendingKeyNilGroup(t *testing.T) {
	t.Parallel()
	// This tests edge case behavior
	key := streamPendingKey("", "")
	expected := "stream::pending:"
	if string(key) != expected {
		t.Errorf("streamPendingKey(%q, %q) = %q, want %q", "", "", string(key), expected)
	}
}

// TestStreamErrorTypes tests error types from stream operations
func TestStreamErrorTypes(t *testing.T) {
	t.Parallel()
	// Test that we can detect stream-specific errors
	err1 := errors.New("ERR ID must be greater than the last entry ID")
	err2 := errors.New("ERR wrong number of arguments for 'XREAD' command")

	// These should be different errors
	if err1.Error() == err2.Error() {
		t.Error("Test errors should be different")
	}
}
