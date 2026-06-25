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
	assert.True(t, len(results) > 0)
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
	assert.True(t, len(results) > 0)
}
