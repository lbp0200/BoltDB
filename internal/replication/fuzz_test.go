package replication

import (
	"testing"

	"github.com/lbp0200/BoltDB/internal/store"
)

// FuzzExecuteReplicatedCommand tests executeReplicatedCommand with random input
func FuzzExecuteReplicatedCommand(f *testing.F) {
	// Seed corpus
	f.Add("SETkeyvalue")
	f.Add("GETkey")
	f.Add("DELkey")

	f.Fuzz(func(t *testing.T, data string) {
		testStore, err := store.NewBadgerStore(t.TempDir())
		if err != nil {
			t.Fatalf("failed to create store: %v", err)
		}
		defer testStore.Close()

		_ = executeReplicatedCommand(testStore, [][]byte{[]byte(data)})
	})
}

// FuzzHandlePSync tests HandlePSync with random input
func FuzzHandlePSync(f *testing.F) {
	// Seed corpus
	f.Add("?", int64(-1))
	f.Add("abc", int64(0))
	f.Add("replid123", int64(100))

	f.Fuzz(func(t *testing.T, replId string, offset int64) {
		testStore, err := store.NewBadgerStore(t.TempDir())
		if err != nil {
			t.Fatalf("failed to create store: %v", err)
		}
		defer testStore.Close()

		rm := NewReplicationManager(testStore)
		rm.SetRole(RoleMaster)

		// Just ensure it doesn't panic
		HandlePSync(rm, replId, offset)
	})
}

// FuzzLoadRDBWithStore tests LoadRDBWithStore with random input
func FuzzLoadRDBWithStore(f *testing.F) {
	// Seed corpus
	f.Add([]byte{})
	f.Add([]byte("REDIS0009"))
	f.Add([]byte("INVALID"))

	f.Fuzz(func(t *testing.T, data []byte) {
		testStore, err := store.NewBadgerStore(t.TempDir())
		if err != nil {
			t.Fatalf("failed to create store: %v", err)
		}
		defer testStore.Close()

		// Just ensure it doesn't panic
		LoadRDBWithStore(data, testStore)
	})
}
