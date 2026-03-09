package replication

import (
	"testing"

	"github.com/lbp0200/BoltDB/internal/store"
)

// BenchmarkSerializeCommand tests serializeCommand performance
func BenchmarkSerializeCommand(b *testing.B) {
	cmd := [][]byte{[]byte("SET"), []byte("key"), []byte("value")}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		serializeCommand(cmd)
	}
}

// BenchmarkGenerateRDB_Empty tests GenerateRDB with empty store
func BenchmarkGenerateRDB_Empty(b *testing.B) {
	testStore, _ := store.NewBadgerStore("/tmp/bench_empty")
	defer testStore.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		GenerateRDB(testStore)
	}
}

// BenchmarkGenerateRDB_WithData tests GenerateRDB with data
func BenchmarkGenerateRDB_WithData(b *testing.B) {
	testStore, _ := store.NewBadgerStore("/tmp/bench_data")
	defer testStore.Close()

	// Add some data
	for i := 0; i < 100; i++ {
		testStore.Set(string(rune('a'+i%26)), "value")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		GenerateRDB(testStore)
	}
}

// BenchmarkReplicationBacklog_Append tests ReplicationBacklog Append
func BenchmarkReplicationBacklog_Append(b *testing.B) {
	backlog := NewReplicationBacklog(1024 * 1024)
	data := []byte("test data")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		backlog.Append(data)
	}
}

// BenchmarkReplicationBacklog_GetRange tests ReplicationBacklog GetRange
func BenchmarkReplicationBacklog_GetRange(b *testing.B) {
	backlog := NewReplicationBacklog(1024 * 1024)

	// Add some data
	for i := 0; i < 1000; i++ {
		backlog.Append([]byte("test data"))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		backlog.GetRange(0, 100)
	}
}

// BenchmarkReplicationManager_GetRole tests ReplicationManager GetRole
func BenchmarkReplicationManager_GetRole(b *testing.B) {
	testStore, _ := store.NewBadgerStore("/tmp/bench_role")
	defer testStore.Close()
	rm := NewReplicationManager(testStore)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rm.GetRole()
	}
}

// BenchmarkReplicationManager_GetSlaves tests ReplicationManager GetSlaves
func BenchmarkReplicationManager_GetSlaves(b *testing.B) {
	testStore, _ := store.NewBadgerStore("/tmp/bench_slaves")
	defer testStore.Close()
	rm := NewReplicationManager(testStore)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rm.GetSlaves()
	}
}
