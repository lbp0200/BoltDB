package proto

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/zeebo/assert"
)

func TestArrayString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		args     [][]byte
		expected string
	}{
		{
			name:     "empty array",
			args:     [][]byte{},
			expected: "*0\r\n",
		},
		{
			name:     "single element",
			args:     [][]byte{[]byte("GET")},
			expected: "*1\r\n$3\r\nGET\r\n",
		},
		{
			name:     "two elements",
			args:     [][]byte{[]byte("SET"), []byte("key"), []byte("value")},
			expected: "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n",
		},
		{
			name:     "nil element",
			args:     [][]byte{[]byte("CMD"), nil},
			expected: "*2\r\n$3\r\nCMD\r\n$-1\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Array{Args: tt.args}
			result := a.String()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBulkString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		data     []byte
		expected string
	}{
		{
			name:     "normal string",
			data:     []byte("hello"),
			expected: "$5\r\nhello\r\n",
		},
		{
			name:     "empty string",
			data:     []byte(""),
			expected: "$0\r\n\r\n",
		},
		{
			name:     "nil",
			data:     nil,
			expected: "$-1\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bs := BulkString(tt.data)
			result := bs.String()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSimpleString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		s        SimpleString
		expected string
	}{
		{name: "OK", s: SimpleString("OK"), expected: "+OK\r\n"},
		{name: "PONG", s: SimpleString("PONG"), expected: "+PONG\r\n"},
		{name: "empty", s: SimpleString(""), expected: "+\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.s.String()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		e        Error
		expected string
	}{
		{name: "ERR", e: Error("ERR"), expected: "-ERR\r\n"},
		{name: "not found", e: Error("not found"), expected: "-not found\r\n"},
		{name: "custom", e: Error("WRONGTYPE Operation"), expected: "-WRONGTYPE Operation\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.e.String()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestInteger(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		i        Integer
		expected string
	}{
		{name: "zero", i: Integer(0), expected: ":0\r\n"},
		{name: "positive", i: Integer(100), expected: ":100\r\n"},
		{name: "negative", i: Integer(-50), expected: ":-50\r\n"},
		{name: "large", i: Integer(999999), expected: ":999999\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.i.String()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestReadRESPArray(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		input    string
		expected *Array
		wantErr  bool
	}{
		{
			name:     "simple command",
			input:    "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n",
			expected: &Array{Args: [][]byte{[]byte("SET"), []byte("key"), []byte("value")}},
			wantErr:  false,
		},
		{
			name:     "empty array",
			input:    "*0\r\n",
			expected: &Array{Args: [][]byte{}},
			wantErr:  false,
		},
		{
			name:     "command with nil",
			input:    "*2\r\n$3\r\nCMD\r\n$-1\r\n",
			expected: &Array{Args: [][]byte{[]byte("CMD"), nil}},
			wantErr:  false,
		},
		{
			name:     "inline PING",
			input:    "PING\r\n",
			expected: &Array{Args: [][]byte{[]byte("PING")}},
			wantErr:  false,
		},
		{
			name:     "inline GET key",
			input:    "GET key\r\n",
			expected: &Array{Args: [][]byte{[]byte("GET"), []byte("key")}},
			wantErr:  false,
		},
		{
			name:     "inline SET key value",
			input:    "SET key value\r\n",
			expected: &Array{Args: [][]byte{[]byte("SET"), []byte("key"), []byte("value")}},
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bufio.NewReader(bytes.NewBufferString(tt.input))
			result, err := ReadRESP(r)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, result)
			assert.Equal(t, len(tt.expected.Args), len(result.Args))

			for i, arg := range tt.expected.Args {
				if arg == nil {
					assert.Nil(t, result.Args[i])
				} else {
					assert.Equal(t, string(arg), string(result.Args[i]))
				}
			}
		})
	}
}

func TestWriteRESP(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		resp     RESP
		expected string
	}{
		{
			name:     "simple string",
			resp:     NewSimpleString("OK"),
			expected: "+OK\r\n",
		},
		{
			name:     "integer",
			resp:     NewInteger(100),
			expected: ":100\r\n",
		},
		{
			name:     "bulk string",
			resp:     NewBulkString([]byte("hello")),
			expected: "$5\r\nhello\r\n",
		},
		{
			name:     "error",
			resp:     NewError("ERR"),
			expected: "-ERR\r\n",
		},
		{
			name:     "array",
			resp:     &Array{Args: [][]byte{[]byte("OK")}},
			expected: "*1\r\n$2\r\nOK\r\n",
		},
		{
			name:     "nil bulk string",
			resp:     NewBulkString(nil),
			expected: "$-1\r\n",
		},
		{
			name:     "nested array",
			resp: &NestedArray{Elems: []RESP{
				NewSimpleString("a"),
				NewInteger(42),
			}},
			expected: "*2\r\n+a\r\n:42\r\n",
		},
		{
			name:     "nil array",
			resp:     NilArray{},
			expected: "*-1\r\n",
		},
		{
			name:     "raw string",
			resp:     RawString("+CUSTOM\r\n"),
			expected: "+CUSTOM\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := WriteRESP(&buf, tt.resp)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, buf.String())
		})
	}
}

func TestParseInlineCommand(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		line     []byte
		expected *Array
	}{
		{
			name:     "PING",
			line:     []byte("PING"),
			expected: &Array{Args: [][]byte{[]byte("PING")}},
		},
		{
			name:     "GET key",
			line:     []byte("GET key"),
			expected: &Array{Args: [][]byte{[]byte("GET"), []byte("key")}},
		},
		{
			name:     "SET key value EX 100",
			line:     []byte("SET key value EX 100"),
			expected: &Array{Args: [][]byte{[]byte("SET"), []byte("key"), []byte("value"), []byte("EX"), []byte("100")}},
		},
		{
			name:     "multiple spaces",
			line:     []byte("GET   key"),
			expected: &Array{Args: [][]byte{[]byte("GET"), []byte("key")}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseInlineCommand(tt.line)
			assert.NoError(t, err)
			assert.NotNil(t, result)
			assert.Equal(t, len(tt.expected.Args), len(result.Args))
		})
	}
}

func TestJoinBulkStrings(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		args     [][]byte
		expected string
	}{
		{
			name:     "empty",
			args:     [][]byte{},
			expected: "",
		},
		{
			name:     "single",
			args:     [][]byte{[]byte("hello")},
			expected: "$5\r\nhello\r\n",
		},
		{
			name:     "multiple",
			args:     [][]byte{[]byte("a"), []byte("bb"), []byte("ccc")},
			expected: "$1\r\na\r\n$2\r\nbb\r\n$3\r\nccc\r\n",
		},
		{
			name:     "with nil",
			args:     [][]byte{[]byte("a"), nil, []byte("b")},
			expected: "$1\r\na\r\n$-1\r\n$1\r\nb\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := joinBulkStrings(tt.args)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestNewFactoryFunctions(t *testing.T) {
	t.Parallel()
	// Test factory functions
	ss := NewSimpleString("test")
	assert.Equal(t, "+test\r\n", ss.String())

	bs := NewBulkString([]byte("bulk"))
	assert.Equal(t, "$4\r\nbulk\r\n", bs.String())

	errResp := NewError("test error")
	assert.Equal(t, "-test error\r\n", errResp.String())

	i := NewInteger(42)
	assert.Equal(t, ":42\r\n", i.String())

	// Test nil bulk string
	nilBS := NewBulkString(nil)
	assert.Equal(t, "$-1\r\n", nilBS.String())

	// Test OK constant
	assert.Equal(t, "+OK\r\n", OK.String())
}

// FuzzParseRESP tests RESP protocol parsing with fuzzed input
func FuzzParseRESP(f *testing.F) {
	// Seed corpus with valid and invalid inputs
	f.Add("PING\r\n")
	f.Add("SET key value\r\n")
	f.Add("GET key\r\n")
	f.Add("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n")
	f.Add("*0\r\n")
	f.Add("*1\r\n$4\r\nPING\r\n")
	f.Add("")

	f.Fuzz(func(t *testing.T, data string) {
		// Test that parsing doesn't panic
		r := bufio.NewReader(bytes.NewReader([]byte(data)))
		_, err := ReadRESP(r)

		// We don't assert anything - just ensure no panics
		// Invalid inputs are expected to return errors
		_ = err
	})
}

// FuzzParseInlineCommand tests inline command parsing with fuzzed input
func FuzzParseInlineCommand(f *testing.F) {
	// Seed corpus
	f.Add("PING")
	f.Add("GET key")
	f.Add("SET key value")
	f.Add("")
	f.Add("GET key with spaces")
	f.Add("GET\tkey\ttab")

	f.Fuzz(func(t *testing.T, data string) {
		// Test that parsing doesn't panic
		_, err := parseInlineCommand([]byte(data))

		// We don't assert anything - just ensure no panics
		_ = err
	})
}

// FuzzBulkString tests BulkString serialization with fuzzed input
func FuzzBulkString(f *testing.F) {
	// Seed corpus
	f.Add("hello")
	f.Add("")
	f.Add("test value with spaces")
	f.Add("unicode: 你好世界")
	f.Add("newlines\n\r\n")

	f.Fuzz(func(t *testing.T, data string) {
		// Test that serialization doesn't panic
		bs := BulkString([]byte(data))
		_ = bs.String()
	})
}

// TestNestedArrayString tests NestedArray String method
func TestNestedArrayString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		elems    []RESP
		expected string
	}{
		{
			name:     "empty array",
			elems:    []RESP{},
			expected: "*0\r\n",
		},
		{
			name:     "single element",
			elems:    []RESP{NewSimpleString("OK")},
			expected: "*1\r\n+OK\r\n",
		},
		{
			name: "nested array",
			elems: []RESP{
				NewSimpleString("OK"),
				&NestedArray{
					Elems: []RESP{
						NewSimpleString("inner"),
					},
				},
			},
			expected: "*2\r\n+OK\r\n*1\r\n+inner\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &NestedArray{Elems: tt.elems}
			result := n.String()
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestRawString tests RawString String method
func TestRawString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		input    RawString
		expected string
	}{
		{
			name:     "simple string",
			input:    RawString("+OK\r\n"),
			expected: "+OK\r\n",
		},
		{
			name:     "error string",
			input:    RawString("-ERR test\r\n"),
			expected: "-ERR test\r\n",
		},
		{
			name:     "bulk string",
			input:    RawString("$5\r\nhello\r\n"),
			expected: "$5\r\nhello\r\n",
		},
		{
			name:     "empty string",
			input:    RawString(""),
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.input.String()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestReadRESP_PartialPackets(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		input    string
		wantErr  bool
		errMatch error
	}{
		{
			name:     "empty input (io.EOF)",
			input:    "",
			wantErr:  true,
			errMatch: io.EOF,
		},
		{
			name:    "just CRLF",
			input:   "\r\n",
			wantErr: true,
		},
		{
			name:    "just newline",
			input:   "\n",
			wantErr: true,
		},
		{
			name:     "truncated array header - no CRLF",
			input:    "*3",
			wantErr:  true,
			errMatch: io.EOF,
		},
		{
			name:     "array header only, no bulk strings",
			input:    "*2\r\n",
			wantErr:  true,
			errMatch: io.EOF,
		},
		{
			name:     "truncated bulk string header",
			input:    "*1\r\n$",
			wantErr:  true,
			errMatch: io.EOF,
		},
		{
			name:     "partial bulk string data (io.ErrUnexpectedEOF)",
			input:    "*1\r\n$5\r\nhel",
			wantErr:  true,
			errMatch: io.ErrUnexpectedEOF,
		},
		{
			name:     "missing trailing CRLF after bulk data",
			input:    "*1\r\n$5\r\nhello",
			wantErr:  true,
			errMatch: io.ErrUnexpectedEOF,
		},
		{
			name:    "negative array length",
			input:   "*-1\r\n",
			wantErr: true,
		},
		{
			name:    "invalid array length text",
			input:   "*abc\r\n",
			wantErr: true,
		},
		{
			name:    "invalid bulk string length",
			input:   "*1\r\n$abc\r\n",
			wantErr: true,
		},
		{
			name:     "truncated simple string - just +",
			input:    "+",
			wantErr:  true,
			errMatch: io.EOF,
		},
		{
			name:     "partial multi-bulk - first element ok, second truncated",
			input:    "*2\r\n$3\r\nGET\r\n$",
			wantErr:  true,
			errMatch: io.EOF,
		},
		{
			name:    "bulk string expects trailing CRLF but gets nothing",
			input:   "*1\r\n$0\r\n",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bufio.NewReader(bytes.NewBufferString(tt.input))
			_, err := ReadRESP(r)

			if !tt.wantErr {
				assert.NoError(t, err)
				return
			}
			assert.Error(t, err)
			if tt.errMatch != nil && !errors.Is(err, tt.errMatch) {
				t.Errorf("expected error %v, got %v", tt.errMatch, err)
			}
		})
	}
}

func TestReadRESP_TruncatedInlineCommand(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{
			name:    "empty inline command with CRLF",
			input:   "\r\n",
			wantErr: true,
		},
		{
			name:    "just spaces with CRLF",
			input:   "   \r\n",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bufio.NewReader(bytes.NewBufferString(tt.input))
			_, err := ReadRESP(r)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestNewScanResponse tests NewScanResponse function
func TestNilArrayString(t *testing.T) {
	t.Parallel()
	n := NilArray{}
	assert.Equal(t, "*-1\r\n", n.String())
}

func TestNewScanResponse(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		cursor   uint64
		keys     []string
		expected string
	}{
		{
			name:     "empty keys",
			cursor:   0,
			keys:     []string{},
			expected: "*2\r\n$1\r\n0\r\n*0\r\n",
		},
		{
			name:     "single key",
			cursor:   0,
			keys:     []string{"key1"},
			expected: "*2\r\n$1\r\n0\r\n*1\r\n$4\r\nkey1\r\n",
		},
		{
			name:     "multiple keys",
			cursor:   100,
			keys:     []string{"key1", "key2", "key3"},
			expected: "*2\r\n$3\r\n100\r\n*3\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n",
		},
		{
			name:     "non-zero cursor",
			cursor:   12345,
			keys:     []string{"mykey"},
			expected: "*2\r\n$5\r\n12345\r\n*1\r\n$5\r\nmykey\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := NewScanResponse(tt.cursor, tt.keys)
			assert.Equal(t, tt.expected, result.String())
		})
	}
}
