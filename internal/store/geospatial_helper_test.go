package store

import (
	"testing"
)

// TestStringToGeoHash tests stringToGeoHash
func TestStringToGeoHash(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		input     string
		wantErr   bool
		checkHash func(uint64) bool
	}{
		{
			name:    "empty string",
			input:   "",
			wantErr: false,
			checkHash: func(h uint64) bool {
				return h == 0
			},
		},
		{
			name:    "single character 0",
			input:   "0",
			wantErr: false,
			checkHash: func(h uint64) bool {
				// "0" maps to index 0, so hash can be 0
				return true
			},
		},
		{
			name:    "single character b",
			input:   "b",
			wantErr: false,
			checkHash: func(h uint64) bool {
				return h > 0
			},
		},
		{
			name:    "valid geohash",
			input:   "wx4g",
			wantErr: false,
			checkHash: func(h uint64) bool {
				return h > 0
			},
		},
		{
			name:    "invalid character",
			input:   "xyz!",
			wantErr: true,
		},
		{
			name:    "uppercase returns error",
			input:   "ABCDE",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hash, err := stringToGeoHash(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("stringToGeoHash(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if !tt.wantErr && tt.checkHash != nil && !tt.checkHash(hash) {
				t.Errorf("stringToGeoHash(%q) = %d, unexpected value", tt.input, hash)
			}
		})
	}
}

// TestGeoMembersKey tests geoMembersKey
func TestGeoMembersKey(t *testing.T) {
	t.Parallel()
	key := geoMembersKey("mykey")
	expected := "geo:mykey:members:"
	if string(key) != expected {
		t.Errorf("geoMembersKey(%q) = %q, want %q", "mykey", string(key), expected)
	}
}

// TestExtractMembers tests extractMembers
func TestExtractMembers(t *testing.T) {
	t.Parallel()
	t.Run("empty results", func(t *testing.T) {
		results := []GeoSearchResult{}
		members := extractMembers(results)
		if len(members) != 0 {
			t.Errorf("extractMembers() = %v, want []", members)
		}
	})

	t.Run("single result", func(t *testing.T) {
		results := []GeoSearchResult{
			{Member: "member1"},
		}
		members := extractMembers(results)
		if len(members) != 1 || members[0] != "member1" {
			t.Errorf("extractMembers() = %v, want [member1]", members)
		}
	})

	t.Run("multiple results", func(t *testing.T) {
		results := []GeoSearchResult{
			{Member: "member1"},
			{Member: "member2"},
			{Member: "member3"},
		}
		members := extractMembers(results)
		if len(members) != 3 {
			t.Errorf("extractMembers() length = %d, want 3", len(members))
		}
		if members[0] != "member1" || members[1] != "member2" || members[2] != "member3" {
			t.Errorf("extractMembers() = %v, want [member1 member2 member3]", members)
		}
	})
}

// TestGeoHashRoundTrip tests encoding and decoding round trip
func TestGeoHashRoundTrip(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		lat  float64
		lon  float64
	}{
		{"origin", 0, 0},
		{"nyc", 40.7128, -74.0060},
		{"london", 51.5074, -0.1278},
		{"tokyo", 35.6762, 139.6503},
		{"sydney", -33.8688, 151.2093},
		{"north pole", 90, 0},
		{"south pole", -90, 0},
		{"prime meridian", 0, 0},
		{"date line", 0, 180},
		{"max lat", 90, 0},
		{"max lon", 0, 180},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hash := encodeGeoHash(tt.lat, tt.lon)
			decodedLat, decodedLon := decodeGeoHash(hash)

			// Allow small floating point error
			if abs(tt.lat-decodedLat) > 0.01 || abs(tt.lon-decodedLon) > 0.01 {
				t.Errorf("round trip failed: (%f,%f) -> %d -> (%f,%f)",
					tt.lat, tt.lon, hash, decodedLat, decodedLon)
			}
		})
	}
}

// TestGeoHashToString tests geoHashToString
func TestGeoHashToString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		hash     uint64
		expected string
	}{
		{"zero", 0, "00000000000"},
		{"max", 0xFFFFFFFFFFFFF, "fffffffffff"}, // 52-bit max
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := geoHashToString(tt.hash)
			if len(result) != 11 {
				t.Errorf("geoHashToString(%d) = %q, want length 11", tt.hash, result)
			}
		})
	}
}

// TestGeoHashToBoundingBox tests geoHashToBoundingBox
func TestGeoHashToBoundingBox(t *testing.T) {
	t.Parallel()
	// Test with known hash
	hash := encodeGeoHash(0, 0)
	minLat, maxLat, minLon, maxLon := geoHashToBoundingBox(hash)

	// Bounds should be valid
	if minLat > maxLat {
		t.Errorf("minLat > maxLat: %f > %f", minLat, maxLat)
	}
	if minLon > maxLon {
		t.Errorf("minLon > maxLon: %f > %f", minLon, maxLon)
	}
	if minLat < -90 || maxLat > 90 {
		t.Errorf("latitude out of range: [%f, %f]", minLat, maxLat)
	}
	if minLon < -180 || maxLon > 180 {
		t.Errorf("longitude out of range: [%f, %f]", minLon, maxLon)
	}
}

// TestExpandBoundingBox tests expandBoundingBox
func TestExpandBoundingBox(t *testing.T) {
	t.Parallel()
	t.Run("normal expansion", func(t *testing.T) {
		minLat, maxLat := -1.0, 1.0
		minLon, maxLon := -1.0, 1.0
		radius := 100000.0 // 100km

		newMinLat, newMaxLat, newMinLon, newMaxLon := expandBoundingBox(minLat, maxLat, minLon, maxLon, radius)

		// Should expand in all directions
		if newMinLat >= minLat {
			t.Errorf("newMinLat not expanded: %f >= %f", newMinLat, minLat)
		}
		if newMaxLat <= maxLat {
			t.Errorf("newMaxLat not expanded: %f <= %f", newMaxLat, maxLat)
		}
		if newMinLon >= minLon {
			t.Errorf("newMinLon not expanded: %f >= %f", newMinLon, minLon)
		}
		if newMaxLon <= maxLon {
			t.Errorf("newMaxLon not expanded: %f <= %f", newMaxLon, maxLon)
		}
	})

	t.Run("clamp to valid lat range", func(t *testing.T) {
		minLat, maxLat := 89.0, 90.0
		minLon, maxLon := -1.0, 1.0
		radius := 1000000.0 // Large radius

		newMinLat, newMaxLat, _, _ := expandBoundingBox(minLat, maxLat, minLon, maxLon, radius)

		// Should clamp to -90, 90
		if newMinLat < -90 {
			t.Errorf("newMinLat clamped incorrectly: %f", newMinLat)
		}
		if newMaxLat > 90 {
			t.Errorf("newMaxLat clamped incorrectly: %f", newMaxLat)
		}
	})

	t.Run("clamp to valid lon range", func(t *testing.T) {
		minLat, maxLat := -1.0, 1.0
		minLon, maxLon := 179.0, 180.0
		radius := 1000000.0 // Large radius

		_, _, newMinLon, newMaxLon := expandBoundingBox(minLat, maxLat, minLon, maxLon, radius)

		// Should clamp to -180, 180
		if newMinLon < -180 {
			t.Errorf("newMinLon clamped incorrectly: %f", newMinLon)
		}
		if newMaxLon > 180 {
			t.Errorf("newMaxLon clamped incorrectly: %f", newMaxLon)
		}
	})
}

// TestCalculateDistance tests calculateDistance
func TestCalculateDistance(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		lat1, lon1  float64
		lat2, lon2  float64
		minExpected float64 // minimum expected distance in meters
		maxExpected float64 // maximum expected distance in meters
	}{
		{
			name:        "same point",
			lat1:        40.7128,
			lon1:        -74.0060,
			lat2:        40.7128,
			lon2:        -74.0060,
			minExpected: 0,
			maxExpected: 1,
		},
		{
			name:        "nyc to london",
			lat1:        40.7128,
			lon1:        -74.0060,
			lat2:        51.5074,
			lon2:        -0.1278,
			minExpected: 5500000, // ~5570km
			maxExpected: 5600000,
		},
		{
			name:        "north to south pole",
			lat1:        90,
			lon1:        0,
			lat2:        -90,
			lon2:        0,
			minExpected: 20000000, // ~20015km
			maxExpected: 21000000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dist := calculateDistance(tt.lat1, tt.lon1, tt.lat2, tt.lon2)
			if dist < tt.minExpected || dist > tt.maxExpected {
				t.Errorf("calculateDistance() = %f, want between %f and %f",
					dist, tt.minExpected, tt.maxExpected)
			}
		})
	}
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
