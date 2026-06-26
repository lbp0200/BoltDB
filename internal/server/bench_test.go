package server

import (
	"testing"

	"github.com/lbp0200/BoltDB/internal/backup"
	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/replication"
	"github.com/lbp0200/BoltDB/internal/store"
)

// setupBenchmarkHandler creates a handler for benchmark testing
func setupBenchmarkHandler(b *testing.B) (*Handler, *connState) {
	dbPath := b.TempDir()
	testDB, err := store.NewBadgerStore(dbPath)
	if err != nil {
		b.Fatalf("failed to create store: %v", err)
	}

	pubsubMgr := store.NewPubSubManager()
	backupMgr := backup.NewBackupManager(testDB, dbPath)
	replMgr := replication.NewReplicationManager(testDB)

	return &Handler{
		Db:          testDB,
		PubSub:      pubsubMgr,
		Backup:      backupMgr,
		Replication: replMgr,
		Port:        6337,
		conns:       make(map[*connState]*connMeta),
	}, &connState{}
}

// BenchmarkExecuteCommand_PING benchmarks PING command
func BenchmarkExecuteCommand_PING(b *testing.B) {
	handler, state := setupBenchmarkHandler(b)
	defer handler.Db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler.executeCommand(state, "PING", nil, "127.0.0.1:12345")
	}
}

// BenchmarkExecuteCommand_SET benchmarks SET command
func BenchmarkExecuteCommand_SET(b *testing.B) {
	handler, state := setupBenchmarkHandler(b)
	defer handler.Db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler.executeCommand(state, "SET", [][]byte{[]byte("key"), []byte("value")}, "127.0.0.1:12345")
	}
}

// BenchmarkExecuteCommand_GET benchmarks GET command
func BenchmarkExecuteCommand_GET(b *testing.B) {
	handler, state := setupBenchmarkHandler(b)
	defer handler.Db.Close()

	// Pre-populate
	handler.executeCommand(state, "SET", [][]byte{[]byte("key"), []byte("value")}, "127.0.0.1:12345")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler.executeCommand(state, "GET", [][]byte{[]byte("key")}, "127.0.0.1:12345")
	}
}

// BenchmarkExecuteCommand_INCR benchmarks INCR command
func BenchmarkExecuteCommand_INCR(b *testing.B) {
	handler, state := setupBenchmarkHandler(b)
	defer handler.Db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler.executeCommand(state, "INCR", [][]byte{[]byte("counter")}, "127.0.0.1:12345")
	}
}

// BenchmarkExecuteCommand_DEL benchmarks DEL command
func BenchmarkExecuteCommand_DEL(b *testing.B) {
	handler, state := setupBenchmarkHandler(b)
	defer handler.Db.Close()

	// Pre-populate
	for i := 0; i < 100; i++ {
		handler.executeCommand(state, "SET", [][]byte{[]byte("key"), []byte("value")}, "127.0.0.1:12345")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler.executeCommand(state, "DEL", [][]byte{[]byte("key")}, "127.0.0.1:12345")
	}
}

// BenchmarkParseScore benchmarks parseScore function
func BenchmarkParseScore(b *testing.B) {
	scores := []string{"0", "1", "3.14", "-2.5", "+inf", "-inf", "100"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, s := range scores {
			parseScore(s)
		}
	}
}

// BenchmarkResponseTypes benchmarks response type creation
func BenchmarkResponseTypes(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		proto.NewSimpleString("OK")
		proto.NewError("ERR test")
		proto.NewInteger(123)
		proto.NewBulkString([]byte("test"))
	}
}
