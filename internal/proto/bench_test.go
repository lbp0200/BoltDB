package proto

import (
	"bufio"
	"bytes"
	"testing"
)

func BenchmarkWriteRESP_SimpleString(b *testing.B) {
	resp := NewSimpleString("OK")
	var buf bytes.Buffer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		WriteRESP(&buf, resp)
	}
}

func BenchmarkWriteRESP_BulkString_Small(b *testing.B) {
	resp := NewBulkString([]byte("hello"))
	var buf bytes.Buffer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		WriteRESP(&buf, resp)
	}
}

func BenchmarkWriteRESP_BulkString_Large(b *testing.B) {
	data := make([]byte, 4096)
	for i := range data {
		data[i] = 'x'
	}
	resp := NewBulkString(data)
	var buf bytes.Buffer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		WriteRESP(&buf, resp)
	}
}

func BenchmarkWriteRESP_Integer(b *testing.B) {
	resp := NewInteger(12345)
	var buf bytes.Buffer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		WriteRESP(&buf, resp)
	}
}

func BenchmarkWriteRESP_Error(b *testing.B) {
	resp := NewError("ERR unknown command 'foobar'")
	var buf bytes.Buffer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		WriteRESP(&buf, resp)
	}
}

func BenchmarkWriteRESP_Array_Small(b *testing.B) {
	resp := &Array{Args: [][]byte{[]byte("GET"), []byte("key"), []byte("value")}}
	var buf bytes.Buffer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		WriteRESP(&buf, resp)
	}
}

func BenchmarkWriteRESP_Array_100(b *testing.B) {
	n := 100
	args := make([][]byte, n)
	for i := range args {
		args[i] = []byte("value")
	}
	resp := &Array{Args: args}
	var buf bytes.Buffer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		WriteRESP(&buf, resp)
	}
}

func BenchmarkWriteRESP_NestedArray(b *testing.B) {
	inner := &Array{Args: [][]byte{[]byte("key"), []byte("value")}}
	resp := &NestedArray{Elems: []RESP{
		NewSimpleString("OK"),
		inner,
	}}
	var buf bytes.Buffer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		WriteRESP(&buf, resp)
	}
}

func BenchmarkWriteRESP_NilArray(b *testing.B) {
	var buf bytes.Buffer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		WriteRESP(&buf, NilArray{})
	}
}

func BenchmarkWriteRESP_LargeArray_1000(b *testing.B) {
	n := 1000
	args := make([][]byte, n)
	for i := range args {
		args[i] = []byte("val")
	}
	resp := &Array{Args: args}
	var buf bytes.Buffer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		WriteRESP(&buf, resp)
	}
}

func BenchmarkWriteRESP_LargeArray_10000(b *testing.B) {
	n := 10000
	args := make([][]byte, n)
	for i := range args {
		args[i] = []byte("val")
	}
	resp := &Array{Args: args}
	var buf bytes.Buffer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		WriteRESP(&buf, resp)
	}
}

func BenchmarkReadRESP_Array_Small(b *testing.B) {
	data := []byte("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := bytes.NewReader(data)
		br := bufio.NewReader(r)
		ReadRESP(br)
	}
}

func BenchmarkReadRESP_Array_100(b *testing.B) {
	var buf bytes.Buffer
	buf.WriteString("*100\r\n")
	for i := 0; i < 100; i++ {
		buf.WriteString("$5\r\nvalue\r\n")
	}
	data := buf.Bytes()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := bytes.NewReader(data)
		br := bufio.NewReader(r)
		ReadRESP(br)
	}
}

func BenchmarkWriteRESP_RawString(b *testing.B) {
	resp := RawString("+OK\r\n")
	var buf bytes.Buffer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		WriteRESP(&buf, resp)
	}
}
