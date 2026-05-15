package server

import (
	"fmt"
	"testing"

	"github.com/lbp0200/BoltDB/internal/store"
)

func BenchmarkExecuteCommand_MGET_5(b *testing.B) {
	handler := setupBenchmarkHandler(b)
	defer handler.Db.Close()

	for i := 0; i < 5; i++ {
		handler.executeCommand(nil, "SET", [][]byte{[]byte(fmt.Sprintf("key%d", i)), []byte("value")}, "127.0.0.1:12345")
	}

	args := [][]byte{[]byte("key0"), []byte("key1"), []byte("key2"), []byte("key3"), []byte("key4")}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler.executeCommand(nil, "MGET", args, "127.0.0.1:12345")
	}
}

func BenchmarkExecuteCommand_MGET_10(b *testing.B) {
	handler := setupBenchmarkHandler(b)
	defer handler.Db.Close()

	for i := 0; i < 10; i++ {
		handler.executeCommand(nil, "SET", [][]byte{[]byte(fmt.Sprintf("key%d", i)), []byte("value")}, "127.0.0.1:12345")
	}

	args := make([][]byte, 10)
	for i := range args {
		args[i] = []byte(fmt.Sprintf("key%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler.executeCommand(nil, "MGET", args, "127.0.0.1:12345")
	}
}

func BenchmarkExecuteCommand_Pipeline_SET(b *testing.B) {
	handler := setupBenchmarkHandler(b)
	defer handler.Db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler.executeCommand(nil, "SET", [][]byte{[]byte("key"), []byte("value")}, "127.0.0.1:12345")
	}
}

func BenchmarkExecuteCommand_Pipeline_Mixed(b *testing.B) {
	handler := setupBenchmarkHandler(b)
	defer handler.Db.Close()

	handler.executeCommand(nil, "SET", [][]byte{[]byte("counter"), []byte("0")}, "127.0.0.1:12345")

	commands := []struct {
		cmd  string
		args [][]byte
	}{
		{"SET", [][]byte{[]byte("k1"), []byte("v1")}},
		{"SET", [][]byte{[]byte("k2"), []byte("v2")}},
		{"GET", [][]byte{[]byte("k1")}},
		{"GET", [][]byte{[]byte("k2")}},
		{"INCR", [][]byte{[]byte("counter")}},
		{"PING", nil},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, c := range commands {
			handler.executeCommand(nil, c.cmd, c.args, "127.0.0.1:12345")
		}
	}
}

func BenchmarkPubSub_Publish_1(b *testing.B) {
	handler := setupBenchmarkHandler(b)
	defer handler.Db.Close()

	sub := store.NewSubscriber("bench-sub")
	handler.PubSub.Subscribe(sub, "ch")

	payload := []byte("hello")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler.PubSub.Publish("ch", payload)
	}
}

func BenchmarkPubSub_Publish_10(b *testing.B) {
	handler := setupBenchmarkHandler(b)
	defer handler.Db.Close()

	for i := 0; i < 10; i++ {
		sub := store.NewSubscriber(fmt.Sprintf("bench-sub-%d", i))
		handler.PubSub.Subscribe(sub, "ch")
	}

	payload := []byte("hello")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler.PubSub.Publish("ch", payload)
	}
}

func BenchmarkPubSub_Publish_100(b *testing.B) {
	handler := setupBenchmarkHandler(b)
	defer handler.Db.Close()

	for i := 0; i < 100; i++ {
		sub := store.NewSubscriber(fmt.Sprintf("bench-sub-%d", i))
		handler.PubSub.Subscribe(sub, "ch")
	}

	payload := []byte("hello")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler.PubSub.Publish("ch", payload)
	}
}


