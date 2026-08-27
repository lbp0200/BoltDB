package store

import (
	"context"
	"errors"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/zeebo/assert"
)

// =============================================================================
// Mutation Kill Tests: base.go Del Stream/HLL/Geo + cleanup functions
// Targets NOT COVERED mutations introduced by the Stream/HLL/Geo Del cleanup code
// =============================================================================

// ---------- Del: Stream key cleanup ----------

func TestDelStreamKey(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	// Create a stream via XAdd
	_, err := s.XAdd("del_stream_key", StreamXAddOptions{}, "*", map[string]string{"f1": "v1"})
	assert.NoError(t, err)

	// Verify stream exists
	meta, err := s.XLen("del_stream_key")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), meta)

	// Delete via Del
	deleted, err := s.Del("del_stream_key")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), deleted)

	// Verify stream is gone
	meta, err = s.XLen("del_stream_key")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), meta)
}

func TestDelStreamKeyNonExistent(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	deleted, err := s.Del("del_stream_nonexist")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), deleted)
}

func TestDelStreamWithConsumerGroup(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	// Create a stream
	_, err := s.XAdd("del_stream_cg", StreamXAddOptions{}, "*", map[string]string{"f1": "v1"})
	assert.NoError(t, err)

	// Create a consumer group
	err = s.XGroupCreate("del_stream_cg", "testgroup", "0")
	assert.NoError(t, err)

	// Read from group
	_, err = s.XReadGroup(context.Background(), "testgroup", "consumer1", 1, 0, "del_stream_cg")
	assert.NoError(t, err)

	// Delete stream with consumer group data
	deleted, err := s.Del("del_stream_cg")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), deleted)

	// Verify gone
	meta, err := s.XLen("del_stream_cg")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), meta)
}

// ---------- Del: HLL key cleanup ----------

func TestDelHyperLogLogKey(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	// Add some values to HLL
	_, err := s.PFAdd("del_hll_key", "a", "b", "c")
	assert.NoError(t, err)

	// Verify HLL exists
	count, err := s.PFCount("del_hll_key")
	assert.NoError(t, err)
	assert.True(t, count > 0)

	// Delete via Del
	deleted, err := s.Del("del_hll_key")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), deleted)

	// Verify HLL is gone
	count, err = s.PFCount("del_hll_key")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

func TestDelHyperLogLogKeyNonExistent(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	deleted, err := s.Del("del_hll_nonexist")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), deleted)
}

// ---------- Del: Geo key cleanup ----------

func TestDelGeoKey(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	// Add geo data
	_, err := s.GeoAdd("del_geo_key", []GeoMember{
		{Member: "Beijing", Lon: 116.4, Lat: 39.9},
		{Member: "Shanghai", Lon: 121.5, Lat: 31.2},
	})
	assert.NoError(t, err)

	// Verify geo exists
	positions, err := s.GeoPos("del_geo_key", "Beijing")
	assert.NoError(t, err)
	assert.True(t, len(positions) > 0)

	// Delete via Del
	deleted, err := s.Del("del_geo_key")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), deleted)

	// Verify geo is gone — check that key no longer exists
	exists, err := s.Exists("del_geo_key")
	assert.NoError(t, err)
	assert.False(t, exists)
}

func TestDelGeoKeyNonExistent(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	deleted, err := s.Del("del_geo_nonexist")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), deleted)
}

// ---------- NextStartup: orphan cleanup ----------

func TestNextStartupCleanupOrphanedStreamData(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	// Create a stream
	_, err := s.XAdd("startup_stream", StreamXAddOptions{}, "*", map[string]string{"f1": "v1"})
	assert.NoError(t, err)

	// Create a consumer group
	err = s.XGroupCreate("startup_stream", "grp1", "0")
	assert.NoError(t, err)

	// Verify stream exists
	meta, err := s.XLen("startup_stream")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), meta)

	// Run NextStartup — should not crash and should keep valid data
	err = s.NextStartup()
	assert.NoError(t, err)

	// Verify stream still exists
	meta, err = s.XLen("startup_stream")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), meta)
}

func TestNextStartupCleanupOrphanedHLLData(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	// Create HLL
	_, err := s.PFAdd("startup_hll", "a", "b", "c")
	assert.NoError(t, err)

	// Run NextStartup
	err = s.NextStartup()
	assert.NoError(t, err)

	// Verify HLL still exists
	count, err := s.PFCount("startup_hll")
	assert.NoError(t, err)
	assert.True(t, count > 0)
}

func TestNextStartupCleanupOrphanedGeoData(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	// Create geo data
	_, err := s.GeoAdd("startup_geo", []GeoMember{
		{Member: "Paris", Lon: 2.35, Lat: 48.86},
	})
	assert.NoError(t, err)

	// Run NextStartup
	err = s.NextStartup()
	assert.NoError(t, err)

	// Verify geo data still exists
	positions, err := s.GeoPos("startup_geo", "Paris")
	assert.NoError(t, err)
	assert.True(t, len(positions) > 0)
}

func TestNextStartupEmptyStore(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	err := s.NextStartup()
	assert.NoError(t, err)
}

func TestNextStartupCleanupOrphanedStringData(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	// 制造孤儿 STRING 数据：直接写入 STRING:orphan_str 无 TYPE_
	err := s.GetDB().Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("STRING:orphan_str"), []byte("orphan"))
	})
	assert.NoError(t, err)

	err = s.NextStartup()
	assert.NoError(t, err)

	// 孤儿应被清理
	err = s.GetDB().View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte("STRING:orphan_str"))
		return err
	})
	assert.True(t, errors.Is(err, badger.ErrKeyNotFound))

	// 正常 STRING 不应被误删
	mustSet(t, s, "startup_keep_str", "keep")
	assert.NoError(t, s.NextStartup())
	val, err := s.Get("startup_keep_str")
	assert.NoError(t, err)
	assert.Equal(t, "keep", val)
}

// ---------- Del: multiple key types in sequence ----------

func TestDelMultipleKeyTypes(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	// Create various key types
	mustSet(t, s, "del_str", "val")
	mustLPush(t, s, "del_list", "a", "b")
	s.HSet("del_hash", "f1", "v1")
	s.SAdd("del_set", "m1", "m2")
	mustZAdd(t, s, "del_zset", []ZSetMember{{Member: "a", Score: 1}})
	_, _ = s.XAdd("del_stream", StreamXAddOptions{}, "*", map[string]string{"f1": "v1"})
	_, _ = s.PFAdd("del_hll", "a", "b")
	_, _ = s.GeoAdd("del_geo", []GeoMember{{Member: "NYC", Lon: -74, Lat: 40.7}})

	// Delete all
	keys := []string{"del_str", "del_list", "del_hash", "del_set", "del_zset", "del_stream", "del_hll", "del_geo"}
	for _, k := range keys {
		deleted, err := s.Del(k)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), deleted)
	}

	// Verify all gone
	for _, k := range keys {
		exists, err := s.Exists(k)
		assert.NoError(t, err)
		assert.False(t, exists)
	}
}

// ---------- Del: stream with entries and groups ----------

func TestDelStreamWithMultipleGroupsAndEntries(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	// Add multiple entries
	for i := 0; i < 10; i++ {
		_, err := s.XAdd("del_stream_multi", StreamXAddOptions{}, "*", map[string]string{"idx": string(rune('0' + i))})
		assert.NoError(t, err)
	}

	// Create two consumer groups
	err := s.XGroupCreate("del_stream_multi", "grp1", "0")
	assert.NoError(t, err)
	err = s.XGroupCreate("del_stream_multi", "grp2", "0")
	assert.NoError(t, err)

	// Read some entries in grp1
	_, err = s.XReadGroup(context.Background(), "grp1", "consumer1", 5, 0, "del_stream_multi")
	assert.NoError(t, err)

	// Delete stream — should clean up all groups and entries
	deleted, err := s.Del("del_stream_multi")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), deleted)

	// Verify completely gone
	length, err := s.XLen("del_stream_multi")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), length)
}

// ---------- FlushDB: full cleanup including stream/hll/geo ----------

func TestFlushDBWithAllKeyTypes(t *testing.T) {
	t.Parallel()

	s := setupTestStore(t)

	// Create various types
	mustSet(t, s, "flush_str", "val")
	mustLPush(t, s, "flush_list", "a")
	s.HSet("flush_hash", "f1", "v1")
	s.SAdd("flush_set", "m1")
	mustZAdd(t, s, "flush_zset", []ZSetMember{{Member: "a", Score: 1}})
	_, _ = s.XAdd("flush_stream", StreamXAddOptions{}, "*", map[string]string{"f1": "v1"})
	_, _ = s.PFAdd("flush_hll", "a")
	_, _ = s.GeoAdd("flush_geo", []GeoMember{{Member: "Berlin", Lon: 13.4, Lat: 52.5}})

	// Verify some keys exist
	exists, _ := s.Exists("flush_str")
	assert.True(t, exists)

	// FlushDB
	err := s.FlushDB()
	assert.NoError(t, err)

	// Verify all gone
	for _, k := range []string{"flush_str", "flush_list", "flush_hash", "flush_set", "flush_zset", "flush_stream", "flush_hll", "flush_geo"} {
		exists, _ = s.Exists(k)
		assert.False(t, exists)
	}
}
