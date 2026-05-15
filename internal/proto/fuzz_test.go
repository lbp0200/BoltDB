package proto

import (
	"bufio"
	"bytes"
	"strconv"
	"testing"
)

const maxFuzzAlloc = 10 << 20

func FuzzReadRESP(f *testing.F) {
	seeds := []string{
		"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n",
		"*1\r\n$4\r\nPING\r\n",
		"*0\r\n",
		"+PONG\r\n",
		"-ERR\r\n",
		":100\r\n",
		"PING\r\n",
		"GET key\r\n",
		"SET key value\r\n",
		"*1\r\n$-1\r\n",
		"*1\r\n$0\r\n\r\n",
		"*1\r\n$1\r\n\x00\r\n",
		"*-1\r\n",
		"*abc\r\n",
		"*1\r\n$abc\r\n",
		"*1\r\n$-2\r\n",
		"",
		"\r\n",
		"\n",
		"*3",
		"*2\r\n",
		"*1\r\n$",
		"*1\r\n$5\r\nhel",
		"*2\r\n$3\r\nGET\r\n$",
		"+",
		"$",
		"$5\r\nhel",
		":",
		":abc\r\n",
		"*",
		"$",
		"+",
		"-",
		":",
		"   \r\n",
		"PING\r\nPING\r\n",
		"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n",
		"PING\r\n*1\r\n$4\r\nPING\r\n",
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		if exceedsLimit(data) {
			return
		}

		defer func() {
			if r := recover(); r != nil {
				t.Errorf("ReadRESP panicked on %q: %v", data, r)
			}
		}()

		r := bufio.NewReader(bytes.NewReader(data))
		result, err := ReadRESP(r)

		if result == nil && err == nil {
			t.Errorf("ReadRESP returned nil result with nil error on %q", data)
		}
	})
}

func FuzzReadRESPPipeline(f *testing.F) {
	seeds := []string{
		"",
		"PING\r\n",
		"PING\r\nPING\r\n",
		"PING\r\nPING\r\nPING\r\n",
		"*1\r\n$4\r\nPING\r\n",
		"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n",
		"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n",
		"PING\r\n*1\r\n$4\r\nPING\r\n",
		"*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$",
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		if exceedsLimit(data) {
			return
		}

		defer func() {
			if r := recover(); r != nil {
				t.Errorf("pipeline panicked on %q: %v", data, r)
			}
		}()

		r := bufio.NewReader(bytes.NewReader(data))
		for {
			result, err := ReadRESP(r)
			if err != nil {
				break
			}
			if result == nil {
				t.Errorf("ReadRESP returned nil with nil error in pipeline on %q", data)
				break
			}
		}
	})
}

func exceedsLimit(data []byte) bool {
	for i := 0; i < len(data); i++ {
		if data[i] == '*' || data[i] == '$' {
			j := i + 1
			for j < len(data) && data[j] >= '0' && data[j] <= '9' {
				j++
			}
			if j > i+1 {
				n, err := strconv.Atoi(string(data[i+1 : j]))
				if err == nil && n > maxFuzzAlloc {
					return true
				}
			}
		}
	}
	return false
}

func BenchmarkReadRESP(b *testing.B) {
	inputs := []struct {
		name string
		data []byte
	}{
		{"SET", []byte("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")},
		{"GET", []byte("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n")},
		{"PING", []byte("*1\r\n$4\r\nPING\r\n")},
		{"inline_PING", []byte("PING\r\n")},
		{"large_bulk", []byte("*1\r\n$8192\r\n" + string(make([]byte, 8192)) + "\r\n")},
	}
	for _, in := range inputs {
		b.Run(in.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r := bufio.NewReader(bytes.NewReader(in.data))
				ReadRESP(r)
			}
		})
	}
}
