package store

import (
	"testing"

	"github.com/zeebo/assert"
)

// TestGeoAdd tests GeoAdd function
func TestGeoAdd(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Add geo points
	added, err := store.GeoAdd("mygeo", []GeoMember{
		{Member: "Paris", Lat: 48.8566, Lon: 2.3521},
		{Member: "London", Lat: 51.5074, Lon: -0.1276},
	})
	assert.NoError(t, err)
	assert.Equal(t, int64(2), added)
}

// TestGeoAddWrongType tests that GeoAdd returns ErrWrongType when key exists with different type
func TestGeoAddWrongType(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// First set a string key
	err := store.Set("mykey", "value")
	assert.NoError(t, err)

	// Try to GeoAdd to the string key - should return ErrWrongType
	_, err = store.GeoAdd("mykey", []GeoMember{
		{Member: "Paris", Lat: 48.8566, Lon: 2.3521},
	})
	assert.Equal(t, ErrWrongType, err)

	// Set a hash key
	err = store.HSet("hashkey", "field1", "value1")
	assert.NoError(t, err)

	// Try to GeoAdd to the hash key - should return ErrWrongType
	_, err = store.GeoAdd("hashkey", []GeoMember{
		{Member: "Paris", Lat: 48.8566, Lon: 2.3521},
	})
	assert.Equal(t, ErrWrongType, err)

	// Set a list key
	_, err = store.LPush("listkey", "value1")
	assert.NoError(t, err)

	// Try to GeoAdd to the list key - should return ErrWrongType
	_, err = store.GeoAdd("listkey", []GeoMember{
		{Member: "Paris", Lat: 48.8566, Lon: 2.3521},
	})
	assert.Equal(t, ErrWrongType, err)

	// Set a set key
	_, err = store.SAdd("setkey", "member1")
	assert.NoError(t, err)

	// Try to GeoAdd to the set key - should return ErrWrongType
	_, err = store.GeoAdd("setkey", []GeoMember{
		{Member: "Paris", Lat: 48.8566, Lon: 2.3521},
	})
	assert.Equal(t, ErrWrongType, err)

	// Set a zset key
	err = store.ZAdd("zsetkey", []ZSetMember{{Member: "member1", Score: 1.0}})
	assert.NoError(t, err)

	// Try to GeoAdd to the zset key - should return ErrWrongType
	_, err = store.GeoAdd("zsetkey", []GeoMember{
		{Member: "Paris", Lat: 48.8566, Lon: 2.3521},
	})
	assert.Equal(t, ErrWrongType, err)

	// Verify that GeoAdd works on a geo key (same type)
	_, err = store.GeoAdd("mygeo", []GeoMember{
		{Member: "Paris", Lat: 48.8566, Lon: 2.3521},
	})
	assert.NoError(t, err)

	// Add more to the same geo key - should work
	_, err = store.GeoAdd("mygeo", []GeoMember{
		{Member: "London", Lat: 51.5074, Lon: -0.1276},
	})
	assert.NoError(t, err)
}

// TestGeoPos tests GeoPos function
func TestGeoPos(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Add geo points
	_, _ = store.GeoAdd("mygeo", []GeoMember{
		{Member: "Paris", Lat: 48.8566, Lon: 2.3521},
	})

	// Get position
	positions, err := store.GeoPos("mygeo", "Paris")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(positions))
}

// TestGeoHash tests GeoHash function
func TestGeoHash(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Add geo points
	_, _ = store.GeoAdd("mygeo", []GeoMember{
		{Member: "Paris", Lat: 48.8566, Lon: 2.3521},
	})

	// Get hash
	hashes, err := store.GeoHash("mygeo", "Paris")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(hashes))
	assert.True(t, len(hashes[0]) > 0)
}

// TestGeoDist tests GeoDist function
func TestGeoDist(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Add geo points
	_, _ = store.GeoAdd("mygeo", []GeoMember{
		{Member: "Paris", Lat: 48.8566, Lon: 2.3521},
		{Member: "London", Lat: 51.5074, Lon: -0.1276},
	})

	// Get distance
	dist, err := store.GeoDist("mygeo", "Paris", "London", "km")
	assert.NoError(t, err)
	assert.True(t, dist > 0)
}

// TestGeoRadius tests GeoRadius function
func TestGeoRadius(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Add geo points
	_, _ = store.GeoAdd("mygeo", []GeoMember{
		{Member: "Paris", Lat: 48.8566, Lon: 2.3521},
		{Member: "London", Lat: 51.5074, Lon: -0.1276},
	})

	// Search by radius
	results, err := store.GeoRadius("mygeo", 2.3521, 48.8566, 500, "km", 10, false, false, false)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(results)) // Paris + London within 500km
}

// TestGeoMembers tests GeoMembers function
func TestGeoMembers(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Add geo points
	_, _ = store.GeoAdd("mygeo", []GeoMember{
		{Member: "Paris", Lat: 48.8566, Lon: 2.3521},
		{Member: "London", Lat: 51.5074, Lon: -0.1276},
	})

	// Get members
	members, err := store.GeoMembers("mygeo")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(members))
}

// TestGeoCard tests GeoCard function
func TestGeoCard(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Test empty key
	count, err := store.GeoCard("emptygeo")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)

	_, err = store.GeoAdd("mygeo", []GeoMember{
		{Member: "Paris", Lat: 48.8566, Lon: 2.3521},
		{Member: "London", Lat: 51.5074, Lon: -0.1276},
	})
	assert.NoError(t, err)

	count, err = store.GeoCard("mygeo")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)
}

// TestGeoRadiusByMember tests GeoRadiusByMember function
func TestGeoRadiusByMember(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Add geo points
	_, _ = store.GeoAdd("mygeo", []GeoMember{
		{Member: "Paris", Lat: 48.8566, Lon: 2.3521},
		{Member: "London", Lat: 51.5074, Lon: -0.1276},
	})

	// Search by member radius
	results, err := store.GeoRadiusByMember("mygeo", "Paris", 500, "km", 10, false, false, false)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(results)) // Paris + London within 500km
}

// addGeoSFSet adds the standard SF test set used by BYBOX tests (center = SF):
//   - SF      (37.7749, -122.4194)   center
//   - LA      (34.0522, -118.2437)   556km from center
//   - North   (47.7749, -122.4194)   1113km north (dlat 10°)
//   - NW      (56.2749, -124.4194)   2065km away
//   - East    (37.7749, -100.0)      1968km east
//   - FarEast (37.7749, -90.0)       2838km east
//
// Half-extents at the center latitude (37.7749°):
//
//	width 1000km → halfLon  5.68°;   height 1000km → halfLat  4.49°
//	width 2000km → halfLon 11.37°;   height 5000km → halfLat 22.46°
//	width 5000km → halfLon 28.41°;   height 5000km → halfLat 22.46°
//
// Every point sits well clear of any box edge (margin ≫ the geohash 26-bit
// decode error ≈ 0.5m), and each box below has at least one point that
// discriminates a true rectangle from the old width/2-circle fallback.
func addGeoSFSet(t *testing.T, s *BotreonStore, key string) {
	t.Helper()
	_, err := s.GeoAdd(key, []GeoMember{
		{Member: "SF", Lat: 37.7749, Lon: -122.4194},
		{Member: "LA", Lat: 34.0522, Lon: -118.2437},
		{Member: "North", Lat: 47.7749, Lon: -122.4194},
		{Member: "NW", Lat: 56.2749, Lon: -124.4194},
		{Member: "East", Lat: 37.7749, Lon: -100.0},
		{Member: "FarEast", Lat: 37.7749, Lon: -90.0},
	})
	assert.NoError(t, err)
}

func geoMemberNames(results []GeoSearchResult) map[string]bool {
	m := make(map[string]bool, len(results))
	for _, r := range results {
		m[r.Member] = true
	}
	return m
}

// TestGeoSearchBox_RectangleNotCircle verifies BYBOX rectangle semantics in
// both orientations. The old degenerate implementation treated BYBOX as a
// circle of radius width/2; these assertions fail under that behavior.
//
// Width 5000km × height 1000km: the box extends 2500km east/west but only
// 500km north/south. North (~1112km north) is INSIDE a 2500km circle but
// OUTSIDE the rectangle → must NOT be returned.
func TestGeoSearchBox_RectangleNotCircle(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	addGeoSFSet(t, s, "box1")

	results, err := s.GeoSearchBox("box1", -122.4194, 37.7749, 5000, 1000, "km", 0, false, false, false)
	assert.NoError(t, err)
	names := geoMemberNames(results)
	assert.True(t, names["SF"])   // center
	assert.True(t, names["LA"])   // ~556km, within 2500x1000km box
	assert.True(t, names["East"]) // ~1970km east, within box
	// North (~1112km north) exceeds the 500km half-height — a width/2 circle
	// implementation would wrongly include it.
	assert.False(t, names["North"])
	assert.False(t, names["NW"])
	assert.False(t, names["FarEast"]) // ~2850km east exceeds the 2500km half-width
}

// TestGeoSearchBox_TallRectangle verifies the tall-orientation box:
// width 2000km × height 5000km. The box extends only ~1120km east/west but
// ~2500km north/south. North (1113km north) and NW (2065km away) are OUTSIDE
// a width/2 (1000km) circle but INSIDE the rectangle → they MUST be returned.
// East (1968km east) exceeds the ~1120km half-width → it must NOT appear.
func TestGeoSearchBox_TallRectangle(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	addGeoSFSet(t, s, "box2")

	results, err := s.GeoSearchBox("box2", -122.4194, 37.7749, 2000, 5000, "km", 0, false, false, false)
	assert.NoError(t, err)
	names := geoMemberNames(results)
	assert.True(t, names["SF"])
	assert.True(t, names["LA"])    // 556km, well inside the 2000x5000km box
	assert.True(t, names["North"]) // 1113km north: inside the box, outside a width/2 circle
	assert.True(t, names["NW"])    // inside the box, outside a width/2 circle
	assert.False(t, names["East"]) // 1968km east exceeds the ~1120km half-width
	assert.False(t, names["FarEast"])
}

// TestGeoSearchBox_Options verifies WITHDIST (distance from box center),
// WITHHASH, COUNT and the wrong-type error path.
func TestGeoSearchBox_Options(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	addGeoSFSet(t, s, "box3")

	// WITHDIST + WITHHASH: every result carries distance and geohash.
	results, err := s.GeoSearchBox("box3", -122.4194, 37.7749, 5000, 5000, "km", 0, true, true, false)
	assert.NoError(t, err)
	// Inside the 5000x5000km box: SF, LA, North, NW, East.
	assert.Equal(t, 5, len(results))
	for _, r := range results {
		assert.True(t, r.Dist >= 0)
		assert.True(t, len(r.Hash) == 11)
	}

	// COUNT limits the result set.
	results, err = s.GeoSearchBox("box3", -122.4194, 37.7749, 5000, 5000, "km", 2, false, false, false)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(results))

	// Wrong type must surface ErrWrongType.
	err = s.Set("strkey", "value")
	assert.NoError(t, err)
	_, err = s.GeoSearchBox("strkey", 0, 0, 10, 10, "km", 0, false, false, false)
	assert.Equal(t, ErrWrongType, err)
}

// TestGeoSearchStore_Box verifies the BYBOX path of GeoSearchStore, including
// STOREDIST (score = distance from box center in the requested unit).
// Box 1000km × 5000km → members SF, LA, North, NW (East is 1968km east,
// beyond the 5.68° half-width).
func TestGeoSearchStore_Box(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	addGeoSFSet(t, s, "boxsrc")

	stored, err := s.GeoSearchStore("boxdst", "boxsrc", -122.4194, 37.7749, 1000, "km", 0, true, "BOX", 5000)
	assert.NoError(t, err)
	assert.Equal(t, int64(4), stored) // SF, LA, North, NW

	members, err := s.ZRange("boxdst", 0, -1)
	assert.NoError(t, err)
	names := make(map[string]float64, len(members))
	for _, m := range members {
		names[m.Member] = m.Score
	}
	assert.Equal(t, 4, len(names))
	assert.True(t, names["SF"] < 1.0)                                  // STOREDIST: SF distance from center ≈ 0 (km)
	assert.True(t, names["North"] > 1000.0 && names["North"] < 1300.0) // ≈ 1113km
	assert.True(t, names["NW"] > 1900.0 && names["NW"] < 2300.0)       // ≈ 2065km
	assert.True(t, names["LA"] > 400.0 && names["LA"] < 700.0)         // ≈ 556km

	// Without STOREDIST the score is the member geohash (not a distance).
	stored, err = s.GeoSearchStore("boxdst2", "boxsrc", -122.4194, 37.7749, 1000, "km", 0, false, "BOX", 5000)
	assert.NoError(t, err)
	assert.Equal(t, int64(4), stored)
	members, err = s.ZRange("boxdst2", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 4, len(members))
	assert.True(t, members[0].Score > 1e10) // geohashes, not distances
}
