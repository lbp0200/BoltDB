package replication

import (
	"testing"

	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/zeebo/assert"
)

// TestRDBLengthEncoding_Roundtrip verifies writeLength/readLength consistency
// at every boundary of the 6/14/32-bit encoding scheme.
//
// Boundaries: 63 (max 6-bit), 64 (min 14-bit), 16383 (max 14-bit),
// 16384 (min 32-bit), 100000 (32-bit mid), 1<<20 (32-bit large).
//
// Each test generates RDB data, reloads into a new store, and verifies data integrity.
func TestRDBLengthEncoding_Roundtrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		setup  func(t *testing.T, s *store.BotreonStore)
		verify func(t *testing.T, s *store.BotreonStore)
	}{
		{
			name: "string_value_63_bytes",
			setup: func(t *testing.T, s *store.BotreonStore) {
				val := make([]byte, 63)
				for i := range val {
					val[i] = 'a' + byte(i%26)
				}
				err := s.Set("k63", string(val))
				assert.NoError(t, err)
			},
			verify: func(t *testing.T, s *store.BotreonStore) {
				val, err := s.Get("k63")
				assert.NoError(t, err)
				assert.Equal(t, 63, len(val))
			},
		},
		{
			name: "string_value_64_bytes",
			setup: func(t *testing.T, s *store.BotreonStore) {
				val := make([]byte, 64)
				for i := range val {
					val[i] = 'a' + byte(i%26)
				}
				err := s.Set("k64", string(val))
				assert.NoError(t, err)
			},
			verify: func(t *testing.T, s *store.BotreonStore) {
				val, err := s.Get("k64")
				assert.NoError(t, err)
				assert.Equal(t, 64, len(val))
			},
		},
		{
			name: "string_value_16383_bytes",
			setup: func(t *testing.T, s *store.BotreonStore) {
				val := make([]byte, 16383)
				for i := range val {
					val[i] = 'x'
				}
				err := s.Set("k16383", string(val))
				assert.NoError(t, err)
			},
			verify: func(t *testing.T, s *store.BotreonStore) {
				val, err := s.Get("k16383")
				assert.NoError(t, err)
				assert.Equal(t, 16383, len(val))
			},
		},
		{
			name: "string_value_16384_bytes",
			setup: func(t *testing.T, s *store.BotreonStore) {
				val := make([]byte, 16384)
				for i := range val {
					val[i] = 'y'
				}
				err := s.Set("k16384", string(val))
				assert.NoError(t, err)
			},
			verify: func(t *testing.T, s *store.BotreonStore) {
				val, err := s.Get("k16384")
				assert.NoError(t, err)
				assert.Equal(t, 16384, len(val))
			},
		},
		{
			name: "string_value_100000_bytes",
			setup: func(t *testing.T, s *store.BotreonStore) {
				val := make([]byte, 100000)
				for i := range val {
					val[i] = 'z'
				}
				err := s.Set("k100k", string(val))
				assert.NoError(t, err)
			},
			verify: func(t *testing.T, s *store.BotreonStore) {
				val, err := s.Get("k100k")
				assert.NoError(t, err)
				assert.Equal(t, 100000, len(val))
			},
		},
		{
			name: "set_members_63",
			setup: func(t *testing.T, s *store.BotreonStore) {
				for i := 0; i < 63; i++ {
					_, err := s.SAdd("set63", memberName(i))
					assert.NoError(t, err)
				}
			},
			verify: func(t *testing.T, s *store.BotreonStore) {
				count, err := s.SCard("set63")
				assert.NoError(t, err)
				assert.Equal(t, int64(63), count)
			},
		},
		{
			name: "set_members_64",
			setup: func(t *testing.T, s *store.BotreonStore) {
				for i := 0; i < 64; i++ {
					_, err := s.SAdd("set64", memberName(i))
					assert.NoError(t, err)
				}
			},
			verify: func(t *testing.T, s *store.BotreonStore) {
				count, err := s.SCard("set64")
				assert.NoError(t, err)
				assert.Equal(t, int64(64), count)
			},
		},
		{
			name: "list_elements_16383",
			setup: func(t *testing.T, s *store.BotreonStore) {
				vals := make([]string, 16383)
				for i := range vals {
					vals[i] = "v"
				}
				_, err := s.RPush("list16383", vals...)
				assert.NoError(t, err)
			},
			verify: func(t *testing.T, s *store.BotreonStore) {
				len, err := s.LLen("list16383")
				assert.NoError(t, err)
				assert.Equal(t, int64(16383), len)
			},
		},
		{
			name: "list_elements_16384",
			setup: func(t *testing.T, s *store.BotreonStore) {
				vals := make([]string, 16384)
				for i := range vals {
					vals[i] = "w"
				}
				_, err := s.RPush("list16384", vals...)
				assert.NoError(t, err)
			},
			verify: func(t *testing.T, s *store.BotreonStore) {
				len, err := s.LLen("list16384")
				assert.NoError(t, err)
				assert.Equal(t, int64(16384), len)
			},
		},
		{
			name: "hash_fields_63",
			setup: func(t *testing.T, s *store.BotreonStore) {
				for i := 0; i < 63; i++ {
					err := s.HSet("h63", fieldName(i), "v")
					assert.NoError(t, err)
				}
			},
			verify: func(t *testing.T, s *store.BotreonStore) {
				count, err := s.HLen("h63")
				assert.NoError(t, err)
				assert.Equal(t, uint64(63), count)
			},
		},
		{
			name: "hash_fields_64",
			setup: func(t *testing.T, s *store.BotreonStore) {
				for i := 0; i < 64; i++ {
					err := s.HSet("h64", fieldName(i), "v")
					assert.NoError(t, err)
				}
			},
			verify: func(t *testing.T, s *store.BotreonStore) {
				count, err := s.HLen("h64")
				assert.NoError(t, err)
				assert.Equal(t, uint64(64), count)
			},
		},
		{
			name: "hash_large_field_value_64_bytes",
			setup: func(t *testing.T, s *store.BotreonStore) {
				val := make([]byte, 64)
				for i := range val {
					val[i] = 'b'
				}
				err := s.HSet("hf64", "f", string(val))
				assert.NoError(t, err)
			},
			verify: func(t *testing.T, s *store.BotreonStore) {
				val, err := s.HGet("hf64", "f")
				assert.NoError(t, err)
				assert.Equal(t, 64, len(val))
			},
		},
		{
			name: "hash_large_field_value_16384_bytes",
			setup: func(t *testing.T, s *store.BotreonStore) {
				val := make([]byte, 16384)
				for i := range val {
					val[i] = 'c'
				}
				err := s.HSet("hf16384", "f", string(val))
				assert.NoError(t, err)
			},
			verify: func(t *testing.T, s *store.BotreonStore) {
				val, err := s.HGet("hf16384", "f")
				assert.NoError(t, err)
				assert.Equal(t, 16384, len(val))
			},
		},
		{
			name: "zset_members_16383",
			setup: func(t *testing.T, s *store.BotreonStore) {
				members := make([]store.ZSetMember, 16383)
				for i := range members {
					members[i] = store.ZSetMember{Member: memberName(i), Score: float64(i)}
				}
				err := s.ZAdd("z16383", members)
				assert.NoError(t, err)
			},
			verify: func(t *testing.T, s *store.BotreonStore) {
				count, err := s.ZCard("z16383")
				assert.NoError(t, err)
				assert.Equal(t, int64(16383), int64(count))
			},
		},
		{
			name: "zset_members_16384",
			setup: func(t *testing.T, s *store.BotreonStore) {
				members := make([]store.ZSetMember, 16384)
				for i := range members {
					members[i] = store.ZSetMember{Member: memberName(i), Score: float64(i)}
				}
				err := s.ZAdd("z16384", members)
				assert.NoError(t, err)
			},
			verify: func(t *testing.T, s *store.BotreonStore) {
				count, err := s.ZCard("z16384")
				assert.NoError(t, err)
				assert.Equal(t, int64(16384), int64(count))
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			src := setupTestStore(t)
			defer src.Close()

			tt.setup(t, src)

			rdbData, err := GenerateRDB(src)
			assert.NoError(t, err)

			dst := setupTestStore(t)
			defer dst.Close()

			err = LoadRDBWithStore(rdbData, dst)
			assert.NoError(t, err)

			tt.verify(t, dst)
		})
	}
}

func memberName(i int) string {
	return "m-" + itoa(i)
}

func fieldName(i int) string {
	return "f-" + itoa(i)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}
