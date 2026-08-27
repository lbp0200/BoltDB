package store

import (
	"fmt"
	"testing"
	"time"

	"github.com/zeebo/assert"
)

func TestRenameAllTypes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		setup  func(*BotreonStore) error
		verify func(*BotreonStore, string) error
	}{
		{
			name: "JSON",
			setup: func(s *BotreonStore) error {
				_, _, err := s.JSONSet("rn_json_src", "$", `{"a":1}`, false, false)
				return err
			},
			verify: func(s *BotreonStore, dst string) error {
				vals, err := s.JSONGet(dst, "$")
				if err != nil || len(vals) == 0 {
					return fmt.Errorf("json get: %v vals=%v", err, vals)
				}
				if vals[0] != `{"a":1}` {
					return fmt.Errorf("json val %q", vals[0])
				}
				return nil
			},
		},
		{
			name: "GEO",
			setup: func(s *BotreonStore) error {
				_, err := s.GeoAdd("rn_geo_src", []GeoMember{{Member: "m1", Lat: 31.23, Lon: 121.47}})
				return err
			},
			verify: func(s *BotreonStore, dst string) error {
				members, err := s.GeoMembers(dst)
				if err != nil {
					return err
				}
				if len(members) != 1 || members[0] != "m1" {
					return fmt.Errorf("geo members %v", members)
				}
				return nil
			},
		},
		{
			name: "Stream",
			setup: func(s *BotreonStore) error {
				_, err := s.XAdd("rn_stream_src", StreamXAddOptions{}, "*", map[string]string{"f": "v"})
				return err
			},
			verify: func(s *BotreonStore, dst string) error {
				entries, err := s.XRange(dst, "-", "+", 0)
				if err != nil {
					return err
				}
				if len(entries) != 1 {
					return fmt.Errorf("stream len %d", len(entries))
				}
				return nil
			},
		},
		{
			name: "TimeSeries",
			setup: func(s *BotreonStore) error {
				_, err := s.TSAdd("rn_ts_src", 1000, 1.23, TSAddOptions{})
				return err
			},
			verify: func(s *BotreonStore, dst string) error {
				dps, err := s.TSRange(dst, "-", "+", 0)
				if err != nil {
					return err
				}
				if len(dps) != 1 {
					return fmt.Errorf("ts len %d", len(dps))
				}
				return nil
			},
		},
		{
			name: "HLL",
			setup: func(s *BotreonStore) error {
				_, err := s.PFAdd("rn_hll_src", "a", "b", "c")
				return err
			},
			verify: func(s *BotreonStore, dst string) error {
				c, err := s.PFCount(dst)
				if err != nil {
					return err
				}
				if c != 3 {
					return fmt.Errorf("hll count %d", c)
				}
				return nil
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := setupTestStore(t)
			src := "rn_" + tc.name + "_src"
			if tc.name == "JSON" {
				src = "rn_json_src"
			} else if tc.name == "GEO" {
				src = "rn_geo_src"
			} else if tc.name == "Stream" {
				src = "rn_stream_src"
			} else if tc.name == "TimeSeries" {
				src = "rn_ts_src"
			} else if tc.name == "HLL" {
				src = "rn_hll_src"
			}
			dst := src[:len(src)-3] + "dst"
			if err := tc.setup(s); err != nil {
				t.Fatalf("setup %s: %v", tc.name, err)
			}
			if err := s.Rename(src, dst); err != nil {
				t.Fatalf("rename %s: %v", tc.name, err)
			}
			if ok, _ := s.Exists(src); ok {
				t.Fatalf("%s old still exists", tc.name)
			}
			if ok, _ := s.Exists(dst); !ok {
				t.Fatalf("%s new missing", tc.name)
			}
			if err := tc.verify(s, dst); err != nil {
				t.Fatalf("%s verify: %v", tc.name, err)
			}
		})
	}
}

func TestRenameTTLPreservedAllTypes(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	s.GeoAdd("rn_geo_ttl_src2", []GeoMember{{Member: "m1", Lat: 10, Lon: 10}})
	s.Expire("rn_geo_ttl_src2", 60)
	assert.NoError(t, s.Rename("rn_geo_ttl_src2", "rn_geo_ttl_dst2"))
	ttl, err := s.TTL("rn_geo_ttl_dst2")
	assert.NoError(t, err)
	assert.True(t, ttl > 0 && ttl <= 60)

	s2 := setupTestStore(t)
	_, _, _ = s2.JSONSet("rn_json_ttl_src2", "$", `{"x":1}`, false, false)
	s2.Expire("rn_json_ttl_src2", 60)
	assert.NoError(t, s2.Rename("rn_json_ttl_src2", "rn_json_ttl_dst2"))
	ttl2, err := s2.TTL("rn_json_ttl_dst2")
	assert.NoError(t, err)
	assert.True(t, ttl2 > 0 && ttl2 <= 60)
	_ = time.Now()
}

func TestRenameOverwriteDifferentTypes(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	mustSet(t, s, "rn_over_src", "hello")
	_, _ = s.GeoAdd("rn_over_dst", []GeoMember{{Member: "x", Lat: 1, Lon: 1}})
	assert.NoError(t, s.Rename("rn_over_src", "rn_over_dst"))
	typ, _ := s.Type("rn_over_dst")
	// Type() maps STRING -> "string"
	assert.Equal(t, "string", typ)
	val, _ := s.Get("rn_over_dst")
	assert.Equal(t, "hello", val)
}
