package store

import (
	"testing"
	"time"

	"github.com/zeebo/assert"
)

func TestExpire_TTL_Persist_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	mustSet(t, s, "k1", "v1")

	ok, err := s.Expire("k1", 100)
	assert.NoError(t, err)
	assert.True(t, ok)

	ttl, err := s.TTL("k1")
	assert.NoError(t, err)
	assert.True(t, ttl > 0)

	ok, err = s.Persist("k1")
	assert.NoError(t, err)
	assert.True(t, ok)

	ttl, err = s.TTL("k1")
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), ttl)

	ttl, err = s.PTTL("k1")
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), ttl)
}

func TestExpire_PExpire_ExpireAt_PExpireAt_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	mustSet(t, s, "k2", "v2")

	ok, err := s.ExpireAt("k2", time.Now().Unix()+50)
	assert.NoError(t, err)
	assert.True(t, ok)

	mustSet(t, s, "k3", "v3")
	ok, err = s.PExpire("k3", 50000)
	assert.NoError(t, err)
	assert.True(t, ok)

	mustSet(t, s, "k4", "v4")
	ok, err = s.PExpireAt("k4", time.Now().UnixMilli()+50000)
	assert.NoError(t, err)
	assert.True(t, ok)
}

func TestExpire_TTL_NonexistentKey_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	ok, err := s.Expire("nonexistent", 100)
	assert.NoError(t, err)
	assert.False(t, ok)

	ttl, err := s.TTL("nonexistent")
	assert.NoError(t, err)
	assert.Equal(t, int64(-2), ttl)

	pttl, err := s.PTTL("nonexistent")
	assert.NoError(t, err)
	assert.Equal(t, int64(-2), pttl)
}

func TestPersist_NoExpiry_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	mustSet(t, s, "k5", "v5")

	ok, err := s.Persist("k5")
	assert.NoError(t, err)
	assert.False(t, ok)
}

func TestRename_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	mustSet(t, s, "old", "value1")

	err := s.Rename("old", "new")
	assert.NoError(t, err)

	val, err := s.Get("new")
	assert.NoError(t, err)
	assert.Equal(t, "value1", val)

	exists, err := s.Exists("old")
	assert.NoError(t, err)
	assert.False(t, exists)
}

func TestRenameNX_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	mustSet(t, s, "a", "va")
	mustSet(t, s, "b", "vb")

	ok, err := s.RenameNX("a", "b")
	assert.NoError(t, err)
	assert.False(t, ok)

	ok, err = s.RenameNX("a", "c")
	assert.NoError(t, err)
	assert.True(t, ok)
}

func TestDelString_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	mustSet(t, s, "ds1", "v1")

	err := s.DelString("ds1")
	assert.NoError(t, err)

	exists, err := s.Exists("ds1")
	assert.NoError(t, err)
	assert.False(t, exists)
}

func TestKeys_Scan_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	mustSet(t, s, "ks:a", "1")
	mustSet(t, s, "ks:b", "2")
	mustSet(t, s, "other", "3")

	keys, err := s.Keys("ks:*")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(keys))

	res, err := s.Scan(0, "ks:*", 10)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), res.Cursor)
	assert.Equal(t, 2, len(res.Keys))
}

func TestRandomKey_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	mustSet(t, s, "rk1", "v1")

	key, err := s.RandomKey()
	assert.NoError(t, err)
	assert.Equal(t, "rk1", key)
}

func TestRandomKey_Empty_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	key, err := s.RandomKey()
	if err == nil {
		assert.Equal(t, "", key)
	}
}

func TestTime_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	sec, nsec, err := s.Time()
	assert.NoError(t, err)
	assert.True(t, sec > 0)
	assert.True(t, nsec >= 0)
}

func TestMemoryUsage_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	mustSet(t, s, "mu1", "hello world")

	usage, err := s.MemoryUsage("mu1")
	assert.NoError(t, err)
	assert.True(t, usage > 0)
}

func TestMemoryUsage_Nonexistent_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	_, err := s.MemoryUsage("nonexistent")
	assert.Error(t, err)
}

func TestObjectRefCount_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	mustSet(t, s, "orc1", "v1")

	_, err := s.ObjectRefCount("orc1")
	assert.NoError(t, err)
}

func TestObjectEncoding_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	mustSet(t, s, "oe1", "v1")

	enc, err := s.ObjectEncoding("oe1")
	assert.NoError(t, err)
	assert.True(t, enc != "")
}

func TestObjectIdleTime_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	mustSet(t, s, "oit1", "v1")

	idle, err := s.ObjectIdleTime("oit1")
	assert.NoError(t, err)
	assert.True(t, idle >= 0)
}

func TestCloseWithTimeout_Coverage(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewBadgerStore(dir)
	assert.NoError(t, err)
	err = s.CloseWithTimeout(5 * time.Second)
	assert.NoError(t, err)
}

func TestFlushDB_ClearCaches_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	mustSet(t, s, "fdb1", "v1")

	err := s.FlushDB()
	assert.NoError(t, err)

	exists, err := s.Exists("fdb1")
	assert.NoError(t, err)
	assert.False(t, exists)

	mustSet(t, s, "fdb2", "v2")
	s.ClearCaches()

	val, err := s.Get("fdb2")
	assert.NoError(t, err)
	assert.Equal(t, "v2", val)
}

func TestClearAllData_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	mustSet(t, s, "cad1", "v1")

	err := s.ClearAllData()
	assert.NoError(t, err)

	mustSet(t, s, "cad2", "v2")
	val, err := s.Get("cad2")
	assert.NoError(t, err)
	assert.Equal(t, "v2", val)
}

func TestDump_Restore_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	mustSet(t, s, "dump1", "hello dump")

	data, err := s.Dump("dump1")
	assert.NoError(t, err)
	assert.True(t, len(data) > 0)

	_, err = s.Del("dump1")
	assert.NoError(t, err)

	err = s.Restore("dump1", data, 0, false)
	assert.NoError(t, err)

	val, err := s.Get("dump1")
	assert.NoError(t, err)
	assert.Equal(t, "hello dump", val)
}

func TestRestore_Replace_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	mustSet(t, s, "r1", "original")
	data, err := s.Dump("r1")
	assert.NoError(t, err)

	err = s.Restore("r1", data, 0, true)
	assert.NoError(t, err)
}

func TestRunNextStartup_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	err := s.NextStartup()
	assert.NoError(t, err)
}

func TestBackpressureConfig_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	cfg := DefaultBackpressureConfig()
	cfg.L0SoftThreshold = 5.0
	cfg.L0HardThreshold = 15.0
	s.SetBackpressureConfig(cfg)

	got := s.GetBackpressureConfig()
	assert.Equal(t, 5.0, got.L0SoftThreshold)
	assert.Equal(t, 15.0, got.L0HardThreshold)
}

func TestGetRetryMetrics_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	metrics := s.GetRetryMetrics()
	assert.NotNil(t, metrics)
}

func TestGetDB_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	db := s.GetDB()
	assert.NotNil(t, db)
}

func TestIterateRawKeys_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	mustSet(t, s, "irk1", "v1")

	var count int
	err := s.IterateRawKeys(func(rawKey string) bool {
		count++
		return true
	})
	assert.NoError(t, err)
	assert.True(t, count > 0)
}


