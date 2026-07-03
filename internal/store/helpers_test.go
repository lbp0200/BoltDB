package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/zeebo/assert"
)

func TestReadRDBExpireTime_EmptyBuffer(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	expireAt, ok := readRDBExpireTime(&buf)
	assert.False(t, ok)
	assert.Equal(t, int64(0), expireAt)
}

func TestReadRDBExpireTime_FC(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	buf.WriteByte(0xFC)
	var ms int64 = 1234567890123
	assert.NoError(t, binary.Write(&buf, binary.LittleEndian, ms))

	expireAt, ok := readRDBExpireTime(&buf)
	assert.True(t, ok)
	assert.Equal(t, ms, expireAt)
}

func TestReadRDBExpireTime_FD(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	buf.WriteByte(0xFD)
	var sec int32 = 1234567890
	assert.NoError(t, binary.Write(&buf, binary.LittleEndian, sec))

	expireAt, ok := readRDBExpireTime(&buf)
	assert.True(t, ok)
	assert.Equal(t, int64(sec)*1000, expireAt)
}

func TestReadRDBExpireTime_UnknownType(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	buf.WriteByte(0xFE) // not 0xFC or 0xFD

	expireAt, ok := readRDBExpireTime(&buf)
	assert.False(t, ok)
	assert.Equal(t, int64(0), expireAt)
	assert.Equal(t, 0, buf.Len()) // byte consumed by readRDBExpireTime
}

func TestReadRDBExpireTime_FC_Truncated(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	buf.WriteByte(0xFC)
	buf.Write([]byte{1, 2, 3, 4}) // only 4 bytes, need 8

	expireAt, ok := readRDBExpireTime(&buf)
	assert.False(t, ok)
	assert.Equal(t, int64(0), expireAt)
}

func TestReadRDBExpireTime_FD_Truncated(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	buf.WriteByte(0xFD)
	buf.Write([]byte{1, 2}) // only 2 bytes, need 4

	expireAt, ok := readRDBExpireTime(&buf)
	assert.False(t, ok)
	assert.Equal(t, int64(0), expireAt)
}

func TestDeleteByPrefix(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	err := s.retryUpdate(func(txn *badger.Txn) error {
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("prefix:%d", i)
			if err := txn.Set([]byte(key), []byte("value")); err != nil {
				return err
			}
		}
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("other:%d", i)
			if err := txn.Set([]byte(key), []byte("value")); err != nil {
				return err
			}
		}
		return nil
	}, 30)
	assert.NoError(t, err)

	err = s.retryUpdate(func(txn *badger.Txn) error {
		return deleteByPrefix(txn, []byte("prefix:"))
	}, 30)
	assert.NoError(t, err)

	err = s.db.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		count := 0
		for iter.Seek([]byte("prefix:")); iter.ValidForPrefix([]byte("prefix:")); iter.Next() {
			count++
		}
		assert.Equal(t, 0, count)

		otherCount := 0
		for iter.Seek([]byte("other:")); iter.ValidForPrefix([]byte("other:")); iter.Next() {
			otherCount++
		}
		assert.Equal(t, 5, otherCount)
		return nil
	})
	assert.NoError(t, err)
}

func TestDeleteByPrefix_EmptyPrefix(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	err := s.retryUpdate(func(txn *badger.Txn) error {
		for i := 0; i < 3; i++ {
			key := fmt.Sprintf("k%d", i)
			if err := txn.Set([]byte(key), []byte("v")); err != nil {
				return err
			}
		}
		return nil
	}, 30)
	assert.NoError(t, err)

	err = s.retryUpdate(func(txn *badger.Txn) error {
		return deleteByPrefix(txn, []byte("nonexistent:"))
	}, 30)
	assert.NoError(t, err)

	err = s.db.View(func(txn *badger.Txn) error {
		for i := 0; i < 3; i++ {
			key := fmt.Sprintf("k%d", i)
			_, err := txn.Get([]byte(key))
			assert.NoError(t, err)
		}
		return nil
	})
	assert.NoError(t, err)
}

func TestCheckDataExists_String(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	assert.NoError(t, s.Set("strkey", "strval"))

	err := s.db.View(func(txn *badger.Txn) error {
		exists, err := s.checkDataExists(txn, "strkey", KeyTypeString)
		assert.NoError(t, err)
		assert.True(t, exists)
		return nil
	})
	assert.NoError(t, err)
}

func TestCheckDataExists_StringNotFound(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	err := s.db.View(func(txn *badger.Txn) error {
		exists, err := s.checkDataExists(txn, "nonexistent", KeyTypeString)
		assert.NoError(t, err)
		assert.False(t, exists)
		return nil
	})
	assert.NoError(t, err)
}

func TestCheckDataExists_List(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	mustLPush(t, s, "listkey", "a", "b", "c")

	err := s.db.View(func(txn *badger.Txn) error {
		exists, err := s.checkDataExists(txn, "listkey", KeyTypeList)
		assert.NoError(t, err)
		assert.True(t, exists)
		return nil
	})
	assert.NoError(t, err)
}

func TestCheckDataExists_Hash(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	mustHSet(t, s, "hashkey", "field1", "val1")

	err := s.db.View(func(txn *badger.Txn) error {
		exists, err := s.checkDataExists(txn, "hashkey", KeyTypeHash)
		assert.NoError(t, err)
		assert.True(t, exists)
		return nil
	})
	assert.NoError(t, err)
}

func TestCheckDataExists_Set(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	mustSAdd(t, s, "setkey", "m1", "m2")

	err := s.db.View(func(txn *badger.Txn) error {
		exists, err := s.checkDataExists(txn, "setkey", KeyTypeSet)
		assert.NoError(t, err)
		assert.True(t, exists)
		return nil
	})
	assert.NoError(t, err)
}

func TestCheckDataExists_SortedSet(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	mustZAdd(t, s, "zsetkey", []ZSetMember{{Score: 1.0, Member: "a"}})

	err := s.db.View(func(txn *badger.Txn) error {
		exists, err := s.checkDataExists(txn, "zsetkey", "zset")
		assert.NoError(t, err)
		assert.True(t, exists)
		return nil
	})
	assert.NoError(t, err)
}

func TestCheckDataExists_Default(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	err := s.db.View(func(txn *badger.Txn) error {
		exists, err := s.checkDataExists(txn, "anything", "unknown_type")
		assert.NoError(t, err)
		assert.True(t, exists)
		return nil
	})
	assert.NoError(t, err)
}

func TestGetListData(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	mustRPush(t, s, "testlist", "a", "b", "c")

	data, err := s.getListData("testlist")
	assert.NoError(t, err)
	assert.Equal(t, 3, len(data))
	assert.Equal(t, "a", data[0])
	assert.Equal(t, "b", data[1])
	assert.Equal(t, "c", data[2])
}

func TestGetListData_Empty(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	data, err := s.getListData("nonexistent")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(data))
}

func TestGetListData_Single(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	mustRPush(t, s, "singlelist", "only")

	data, err := s.getListData("singlelist")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(data))
	assert.Equal(t, "only", data[0])
}

func TestRestoreLegacy_String(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	err := s.restoreLegacy("legacykey", []byte("string:hello"), 0, false)
	assert.NoError(t, err)

	val, err := s.Get("legacykey")
	assert.NoError(t, err)
	assert.Equal(t, "hello", val)
}

func TestRestoreLegacy_StringWithTTL(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	err := s.restoreLegacy("legacyttl", []byte("string:world"), time.Second, false)
	assert.NoError(t, err)

	val, err := s.Get("legacyttl")
	assert.NoError(t, err)
	assert.Equal(t, "world", val)
}

func TestRestoreLegacy_List(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	err := s.restoreLegacy("legacylist", []byte("list:a,b,c"), 0, false)
	assert.NoError(t, err)

	val, err := s.LIndex("legacylist", 0)
	assert.NoError(t, err)
	assert.Equal(t, "c", val)
}

func TestRestoreLegacy_Hash(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	err := s.restoreLegacy("legacyhash", []byte("hash:f1=v1,f2=v2"), 0, false)
	assert.NoError(t, err)

	val, err := s.HGet("legacyhash", "f1")
	assert.NoError(t, err)
	assert.Equal(t, []byte("v1"), val)
}

func TestRestoreLegacy_Set(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	err := s.restoreLegacy("legacyset", []byte("set:m1,m2"), 0, false)
	assert.NoError(t, err)

	exists, err := s.SIsMember("legacyset", "m1")
	assert.NoError(t, err)
	assert.True(t, exists)
}

func TestRestoreLegacy_InvalidFormat(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	err := s.restoreLegacy("badkey", []byte("nocolon"), 0, false)
	assert.Error(t, err)
}

func TestRestoreLegacy_UnsupportedType(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	err := s.restoreLegacy("badtype", []byte("unknown:data"), 0, false)
	assert.Error(t, err)
}

func TestRestoreLegacy_EmptyList(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	err := s.restoreLegacy("emptylist", []byte("list:"), 0, false)
	assert.NoError(t, err)

	llen, err := s.LLen("emptylist")
	assert.NoError(t, err)
	assert.Equal(t, 0, llen)
}

func TestCopyKeysByPrefix_List(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	mustRPush(t, s, "oldlist", "a", "b", "c")

	err := s.retryUpdate(func(txn *badger.Txn) error {
		return copyKeysByPrefix(txn, []byte("LIST:oldlist:"), "oldlist", "newlist", KeyTypeList)
	}, 30)
	assert.NoError(t, err)

	val, err := s.LIndex("newlist", 2)
	assert.NoError(t, err)
	assert.Equal(t, "c", val)

	val, err = s.LIndex("oldlist", 2)
	assert.NoError(t, err)
	assert.Equal(t, "c", val)
}

func TestRestoreHLL(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	// Minimal HLL data (registration array)
	hllData := make([]byte, 16384)
	for i := range hllData {
		hllData[i] = byte(i % 256)
	}

	err := s.RestoreHLL("hllkey", hllData)
	assert.NoError(t, err)

	// Verify type is stored correctly (Type returns "none" for HLL, but key exists)
	exists, err := s.Exists("hllkey")
	assert.NoError(t, err)
	assert.True(t, exists)
}

func TestRestoreLegacy_EmptyHash(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	err := s.restoreLegacy("emptyhash", []byte("hash:"), 0, false)
	assert.NoError(t, err)

	exists, err := s.Exists("emptyhash")
	assert.NoError(t, err)
	assert.False(t, exists)
}

func TestRestoreLegacy_EmptySet(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	err := s.restoreLegacy("emptyset", []byte("set:"), 0, false)
	assert.NoError(t, err)

	exists, err := s.Exists("emptyset")
	assert.NoError(t, err)
	assert.False(t, exists)
}
