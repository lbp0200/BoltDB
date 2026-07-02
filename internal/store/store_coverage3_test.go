package store

import (
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/zeebo/assert"
)

func TestGeoSearch_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	_, err := s.GeoAdd("geo1", []GeoMember{
		{Member: "p1", Lat: 39.9, Lon: 116.4},
		{Member: "p2", Lat: 31.2, Lon: 121.5},
	})
	assert.NoError(t, err)

	results, err := s.GeoSearch("geo1", 116.4, 39.9, 1000, "km", 10, false, false, false)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(results)) // only p1 within 1000km (p2 ~1071km away)
}

func TestGeoSearch_WithDistHashCoord_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	_, err := s.GeoAdd("geo2", []GeoMember{
		{Member: "p1", Lat: 39.9, Lon: 116.4},
	})
	assert.NoError(t, err)

	results, err := s.GeoSearch("geo2", 116.4, 39.9, 100, "km", 10, true, true, true)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(results))
}

func TestGeoSearchStore_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	_, err := s.GeoAdd("gssrc", []GeoMember{
		{Member: "p1", Lat: 39.9, Lon: 116.4},
	})
	assert.NoError(t, err)

	n, err := s.GeoSearchStore("gsdst", "gssrc", 116.4, 39.9, 500, "km", 10, false)
	assert.NoError(t, err)
	assert.True(t, n > 0)
}

func TestGeoDel_GeoRemove_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	_, err := s.GeoAdd("geodel", []GeoMember{
		{Member: "pa", Lat: 39.9, Lon: 116.4},
		{Member: "pb", Lat: 31.2, Lon: 121.5},
	})
	assert.NoError(t, err)

	err = s.GeoDel("geodel", "pa")
	assert.NoError(t, err)

	n, err := s.GeoRemove("geodel", "pb")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), n)
}

func TestGeoGetHash_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	_, err := s.GeoAdd("geohash", []GeoMember{
		{Member: "p1", Lat: 39.9, Lon: 116.4},
	})
	assert.NoError(t, err)

	h, err := s.GeoGetHash("geohash", "p1")
	assert.NoError(t, err)
	assert.True(t, h != "")
}

func TestGeoGetAllHashes_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	_, err := s.GeoAdd("geohashes", []GeoMember{
		{Member: "p1", Lat: 39.9, Lon: 116.4},
	})
	assert.NoError(t, err)

	hashes, err := s.GeoGetAllHashes("geohashes")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(hashes))
}

func TestGeoGetAllPositions_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	_, err := s.GeoAdd("geopos", []GeoMember{
		{Member: "p1", Lat: 39.9, Lon: 116.4},
	})
	assert.NoError(t, err)

	pos, err := s.GeoGetAllPositions("geopos")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(pos))
}

func TestGeoGetAllDistances_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	_, err := s.GeoAdd("geodist", []GeoMember{
		{Member: "p1", Lat: 39.9, Lon: 116.4},
		{Member: "p2", Lat: 31.2, Lon: 121.5},
	})
	assert.NoError(t, err)

	dists, err := s.GeoGetAllDistances("geodist", "p1", "km")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(dists))
}

func TestHScan_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	mustHSet(t, s, "hscan1", "f1", "v1")
	mustHSet(t, s, "hscan1", "f2", "v2")

	res, err := s.HScan("hscan1", 0, "*", 10)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), res.Cursor)
	assert.Equal(t, 2, len(res.Fields))
}

func TestZMPop_Max_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	mustZAdd(t, s, "zmpop1", []ZSetMember{
		{Score: 1, Member: "a"},
		{Score: 2, Member: "b"},
	})

	key, members, err := s.ZMPop([]string{"zmpop1"}, "MAX", 1)
	assert.NoError(t, err)
	assert.Equal(t, "zmpop1", key)
	assert.Equal(t, 1, len(members))
}

func TestZMPop_Min_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	mustZAdd(t, s, "zmpop2", []ZSetMember{
		{Score: 1, Member: "a"},
		{Score: 2, Member: "b"},
	})

	key, members, err := s.ZMPop([]string{"zmpop2"}, "MIN", 1)
	assert.NoError(t, err)
	assert.Equal(t, "zmpop2", key)
	assert.Equal(t, 1, len(members))
}

func TestZMPop_Count_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	mustZAdd(t, s, "zmpop3", []ZSetMember{
		{Score: 1, Member: "a"},
		{Score: 2, Member: "b"},
		{Score: 3, Member: "c"},
	})

	key, members, err := s.ZMPop([]string{"zmpop3"}, "MAX", 2)
	assert.NoError(t, err)
	assert.Equal(t, "zmpop3", key)
	assert.Equal(t, 2, len(members))
}

func TestZMPop_EmptyKey_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	key, members, err := s.ZMPop([]string{"nokey"}, "MAX", 1)
	assert.NoError(t, err)
	assert.Equal(t, "", key)
	assert.Equal(t, 0, len(members))
}

func TestZDiff_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	mustZAdd(t, s, "zd1", []ZSetMember{
		{Score: 1, Member: "a"},
		{Score: 2, Member: "b"},
		{Score: 3, Member: "c"},
	})
	mustZAdd(t, s, "zd2", []ZSetMember{
		{Score: 1, Member: "a"},
	})

	members, err := s.ZDiff([]string{"zd1", "zd2"})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(members))
}

func TestZRandMember_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	mustZAdd(t, s, "zrm1", []ZSetMember{
		{Score: 1, Member: "a"},
		{Score: 2, Member: "b"},
		{Score: 3, Member: "c"},
	})

	members, err := s.ZRandMember("zrm1", 2)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(members))
}

func TestZRandMember_CountZero_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	mustZAdd(t, s, "zrm2", []ZSetMember{
		{Score: 1, Member: "a"},
	})

	members, err := s.ZRandMember("zrm2", 0)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(members))
}

func TestZRandMember_Negative_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	mustZAdd(t, s, "zrm3", []ZSetMember{
		{Score: 1, Member: "a"},
		{Score: 2, Member: "b"},
	})

	members, err := s.ZRandMember("zrm3", -5)
	assert.NoError(t, err)
	assert.Equal(t, 5, len(members))
}

func TestLMPop_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	mustLPush(t, s, "lmpop1", "a", "b", "c")

	key, vals, err := s.LMPop([]string{"lmpop1"}, "LEFT", 2)
	assert.NoError(t, err)
	assert.Equal(t, "lmpop1", key)
	assert.Equal(t, 2, len(vals))
}

func TestLMPop_Right_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	mustRPush(t, s, "lmpop2", "x", "y", "z")

	key, vals, err := s.LMPop([]string{"lmpop2"}, "RIGHT", 2)
	assert.NoError(t, err)
	assert.Equal(t, "lmpop2", key)
	assert.Equal(t, 2, len(vals))
}

func TestLMPop_MultiKey_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	mustLPush(t, s, "lmk1", "a", "b")
	mustLPush(t, s, "lmk2", "x", "y")

	key, vals, err := s.LMPop([]string{"lmk1", "lmk2"}, "LEFT", 1)
	assert.NoError(t, err)
	assert.True(t, key != "")
	assert.Equal(t, 1, len(vals))
}

func TestCreateEmptyStream_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	err := s.CreateEmptyStream("empty_stream")
	assert.NoError(t, err)

	length, err := s.XLen("empty_stream")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), length)
}

func TestSetStringBatch_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	err := s.SetStringBatch([]StringEntry{
		{Key: "batch1", Value: "v1"},
		{Key: "batch2", Value: "v2", TTL: time.Hour},
	})
	assert.NoError(t, err)

	val, err := s.Get("batch1")
	assert.NoError(t, err)
	assert.Equal(t, "v1", val)
}

// 读缓存已移除，TestLRUCache_Size_Coverage 测试一并删除

func TestPubSubManager_Clear_Coverage(t *testing.T) {
	t.Parallel()
	psm := NewPubSubManager()
	sub := NewSubscriber("test-sub")
	psm.mu.Lock()
	psm.subscribers[sub] = true
	psm.mu.Unlock()
	assert.Equal(t, 1, psm.GetTotalSubscriberCount())

	psm.Clear()
	assert.Equal(t, 0, psm.GetTotalSubscriberCount())
}

func TestRestoreHLL_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	_, err := s.PFAdd("hll1", "a", "b", "c")
	assert.NoError(t, err)

	card, err := s.PFCount("hll1")
	assert.NoError(t, err)
	assert.True(t, card > 0)
}

func TestIsUUIDFormat_Coverage(t *testing.T) {
	assert.True(t, isUUIDFormat("550e8400-e29b-41d4-a716-446655440000"))
	assert.False(t, isUUIDFormat(""))
	assert.False(t, isUUIDFormat("not-a-uuid"))
}

func TestDecompressZSTD_Coverage(t *testing.T) {
	t.Parallel()
	original := []byte(strings.Repeat("compressible data for ZSTD test! ", 200))

	compressed, err := compressZSTD(original)
	assert.NoError(t, err)

	compressed = compressed[len(compressionMagicZSTD):]

	decompressed, err := decompressZSTD(compressed)
	assert.NoError(t, err)
	assert.Equal(t, string(original), string(decompressed)) // exact round-trip
}

func TestReadValueInTxn_Coverage(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	mustSet(t, s, "rvt_key", "test_value_for_readtxn")

	err := s.db.View(func(txn *badger.Txn) error {
		val, err := s.ReadValueInTxn(txn, []byte(s.stringKey("rvt_key")))
		assert.NoError(t, err)
		assert.Equal(t, "test_value_for_readtxn", string(val))
		return nil
	})
	assert.NoError(t, err)
}

func TestDecompressData_Coverage(t *testing.T) {
	t.Parallel()
	original := []byte(strings.Repeat("decompress data test ", 50))

	compressed, err := compressZSTD(original)
	assert.NoError(t, err)

	decompressed, err := DecompressData(compressed)
	assert.NoError(t, err)
	assert.Equal(t, string(original), string(decompressed))
}
