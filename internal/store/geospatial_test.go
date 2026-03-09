package store

import (
	"testing"

	"github.com/zeebo/assert"
)

// TestGeoAdd tests GeoAdd function
func TestGeoAdd(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	// Add geo points
	added, err := store.GeoAdd("mygeo", []GeoMember{
		{Member: "Paris", Lat: 48.8566, Lon: 2.3521},
		{Member: "London", Lat: 51.5074, Lon: -0.1276},
	})
	assert.NoError(t, err)
	assert.Equal(t, int64(2), added)
}

// TestGeoPos tests GeoPos function
func TestGeoPos(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

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
	store := setupTestStore(t)
	defer store.Close()

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
	store := setupTestStore(t)
	defer store.Close()

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
	store := setupTestStore(t)
	defer store.Close()

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
	store := setupTestStore(t)
	defer store.Close()

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
	store := setupTestStore(t)
	defer store.Close()

	// Test empty key
	count, err := store.GeoCard("emptygeo")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

// TestGeoRadiusByMember tests GeoRadiusByMember function
func TestGeoRadiusByMember(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

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
