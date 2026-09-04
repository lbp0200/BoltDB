package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/dgraph-io/badger/v4"
	"github.com/lbp0200/BoltDB/internal/logger"
)

const (
	KeyTypeGeo        = "GEOHASH"
	prefixKeyGeoBytes = "geo:"
	geoIndex          = ":index"
	geoMeta           = ":meta"
	geoMembers        = ":members"
	earthRadiusMeters = 6378137.0 // Earth semi-major axis in meters
)

// GeoMember represents a geo member with coordinates
type GeoMember struct {
	Member string
	Lat    float64
	Lon    float64
}

// GeoSearchResult represents a search result with distance
type GeoSearchResult struct {
	Member string
	Lat    float64
	Lon    float64
	Dist   float64 // Distance in meters
	Hash   string  // Geohash string
}

// encodeGeoHash encodes latitude and longitude into a 52-bit integer geohash
func encodeGeoHash(lat, lon float64) uint64 {
	// Redis uses 26-bit geohash for lat/lon each, combined to 52-bit
	latBits := encodeLatLonBits(lat, -90, 90, 26)
	lonBits := encodeLatLonBits(lon, -180, 180, 26)
	return (latBits << 26) | lonBits
}

// encodeLatLonBits encodes a coordinate value to bits
func encodeLatLonBits(value, min, max float64, bits uint) uint64 {
	var result uint64
	low, high := min, max
	for i := uint(0); i < bits; i++ {
		mid := (low + high) / 2
		if value >= mid {
			result = (result << 1) | 1
			low = mid
		} else {
			result <<= 1
			high = mid
		}
	}
	return result
}

// DecodeGeoHash decodes a 52-bit geohash to latitude and longitude
func DecodeGeoHash(hash uint64) (lat, lon float64) {
	latBits := hash >> 26
	lonBits := hash & ((1 << 26) - 1)
	lat = DecodeLatLonBits(latBits, -90, 90, 26)
	lon = DecodeLatLonBits(lonBits, -180, 180, 26)
	return
}

// DecodeLatLonBits decodes bits back to coordinate value
func DecodeLatLonBits(bits uint64, min, max float64, totalBits uint) float64 {
	low, high := min, max
	for i := uint(0); i < totalBits; i++ {
		mid := (low + high) / 2
		bitPos := totalBits - i - 1
		if (bits>>bitPos)&1 == 1 {
			low = mid
		} else {
			high = mid
		}
	}
	return (low + high) / 2
}

// geoHashToString converts a 52-bit geohash to Base32 string
func geoHashToString(hash uint64) string {
	const base32Chars = "0123456789bcdefghjkmnpqrstuvwxyz"
	var result strings.Builder
	for i := 0; i < 11; i++ { // 52 bits / 5 bits per char = 11 chars
		result.WriteByte(base32Chars[hash&0x1F])
		hash >>= 5
	}
	return result.String()
}

// calculateDistance calculates distance between two points using Haversine formula
func calculateDistance(lat1, lon1, lat2, lon2 float64) float64 {
	lat1Rad := lat1 * math.Pi / 180
	lat2Rad := lat2 * math.Pi / 180
	deltaLat := (lat2 - lat1) * math.Pi / 180
	deltaLon := (lon2 - lon1) * math.Pi / 180

	a := math.Sin(deltaLat/2)*math.Sin(deltaLat/2) +
		math.Cos(lat1Rad)*math.Cos(lat2Rad)*
			math.Sin(deltaLon/2)*math.Sin(deltaLon/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return earthRadiusMeters * c
}

// geoKey returns the key for storing geo metadata
func geoKey(key string) []byte {
	return []byte(prefixKeyGeoBytes + key + geoMeta)
}

// geoIndexKey returns the key for storing a member's geohash
func geoIndexKey(key, member string) []byte {
	return []byte(prefixKeyGeoBytes + key + geoIndex + ":" + member)
}

// geoHashToCoordKey returns the key for storing hash -> member mapping
func geoHashToCoordKey(key string, hash uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, hash)
	return []byte(prefixKeyGeoBytes + key + ":hash:" + string(b))
}

// GeoAdd adds geographic locations to a sorted set
// GeoAddOptions 携带 GEOADD 的可选参数（Redis 语义）。
// NX: 仅添加不更新；XX: 仅更新不添加；CH: 返回添加+更新总数（默认仅返回新增数）。
type GeoAddOptions struct {
	NX bool
	XX bool
	CH bool
}

func (s *BotreonStore) GeoAdd(key string, members []GeoMember) (int64, error) {
	return s.GeoAddWithOptions(key, GeoAddOptions{}, members)
}

// GeoAddWithOptions 实现带选项的 GEOADD（NX/XX/CH）。
func (s *BotreonStore) GeoAddWithOptions(key string, opts GeoAddOptions, members []GeoMember) (int64, error) {
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)
	var added int64
	err := s.retryUpdate(func(txn *badger.Txn) error {
		added = 0 // reset each attempt; stale value must not survive conflict retry
		// Set type key
		typeKey := TypeOfKeyGet(key)

		// Check if key already exists with a different type
		item, err := txn.Get(typeKey)
		if err == nil {
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			keyType := string(val)
			if keyType != "" && keyType != KeyTypeGeo {
				return ErrWrongType
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}

		if err := txn.Set(typeKey, []byte(KeyTypeGeo)); err != nil {
			logger.Logger.Error().Err(err).Str("key", key).Msg("GeoAdd: Failed to set type")
			return err
		}

		// Get current count
		metaKey := geoKey(key)
		var count int64 = 0
		item, err = txn.Get(metaKey)
		if err == nil {
			err = item.Value(func(val []byte) error {
				count = int64(binary.BigEndian.Uint64(val))
				return nil
			})
			if err != nil {
				return err
			}
		}

		ops := make([]struct {
			indexKey    []byte
			coordKey    []byte
			hashKey     []byte
			score       []byte
			oldScoreKey []byte
			oldHash     uint64
			exists      bool
			skip        bool // NX/XX 过滤：该成员不写入
		}, len(members))

		for i, m := range members {
			hash := encodeGeoHash(m.Lat, m.Lon)

			// Check if member exists
			oldHashKey := geoIndexKey(key, m.Member)
			var oldHash uint64
			oldItem, err := txn.Get(oldHashKey)
			if err == nil {
				err = oldItem.Value(func(val []byte) error {
					oldHash = binary.BigEndian.Uint64(val)
					return nil
				})
				if err != nil {
					return err
				}
				ops[i].exists = true
				ops[i].oldHash = oldHash
				ops[i].oldScoreKey = sortedSetKeyMember(key, m.Member)
			} else if !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}

			// NX: 仅添加不更新；XX: 仅更新不添加
			if opts.NX && ops[i].exists {
				ops[i].skip = true
				continue
			}
			if opts.XX && !ops[i].exists {
				ops[i].skip = true
				continue
			}

			score := encodeScore(float64(hash))
			ops[i].indexKey = sortedSetKeyIndex(key, float64(hash), m.Member, 0)
			ops[i].coordKey = geoIndexKey(key, m.Member)
			ops[i].hashKey = geoHashToCoordKey(key, hash)
			ops[i].score = score
		}

		// Calculate new count
		newCount := count
		for i := range ops {
			if !ops[i].exists && !ops[i].skip {
				newCount++
			}
		}

		// Update metadata
		metaBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(metaBytes, uint64(newCount))
		if err := txn.Set(metaKey, metaBytes); err != nil {
			return err
		}

		// Execute operations
		for i, op := range ops {
			if op.skip {
				continue
			}
			// Delete old index entry if exists
			if op.exists {
				oldIndexKey := sortedSetKeyIndex(key, float64(op.oldHash), members[i].Member, 0)
				if err := txn.Delete(oldIndexKey); err != nil {
					return err
				}
				if err := txn.Delete(op.oldScoreKey); err != nil {
					return err
				}
			}

			// Store geohash as score in sorted set
			if err := txn.Set(sortedSetKeyMember(key, members[i].Member), op.score); err != nil {
				return err
			}
			if err := txn.Set(op.indexKey, nil); err != nil {
				return err
			}
			// Store member -> hash mapping
			hashBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(hashBytes, encodeGeoHash(members[i].Lat, members[i].Lon))
			if err := txn.Set(op.coordKey, hashBytes); err != nil {
				return err
			}
			// Store hash -> member mapping (for GEOSEARCH)
			hashKey := geoHashToCoordKey(key, encodeGeoHash(members[i].Lat, members[i].Lon))
			if err := txn.Set(hashKey, []byte(members[i].Member)); err != nil {
				return err
			}

			// added 计数：CH 时含更新，否则仅计新增
			if opts.CH || !op.exists {
				added++
			}
		}

		return nil
	}, 20, func() []byte {
		// D4 全重放：GEOADD key [NX|XX] [CH] <lon> <lat> <member>...
		args := make([][]byte, 0, 1+3*len(members)+3)
		args = append(args, []byte("GEOADD"), []byte(key))
		if opts.NX {
			args = append(args, []byte("NX"))
		}
		if opts.XX {
			args = append(args, []byte("XX"))
		}
		if opts.CH {
			args = append(args, []byte("CH"))
		}
		for _, m := range members {
			args = append(args, []byte(strconv.FormatFloat(m.Lon, 'f', -1, 64)),
				[]byte(strconv.FormatFloat(m.Lat, 'f', -1, 64)), []byte(m.Member))
		}
		return encodePropagateCommand(args...)
	}())

	return added, err
}

// GeoPos returns the positions of all members
func (s *BotreonStore) GeoPos(key string, members ...string) ([][2]float64, error) {
	var results [][2]float64
	err := s.db.View(func(txn *badger.Txn) error {
		if err := checkKeyType(txn, key, KeyTypeGeo); err != nil {
			return err
		}
		for _, member := range members {
			hashKey := geoIndexKey(key, member)
			item, err := txn.Get(hashKey)
			if errors.Is(err, badger.ErrKeyNotFound) {
				results = append(results, [2]float64{})
				continue
			}
			if err != nil {
				return err
			}
			var hash uint64
			err = item.Value(func(val []byte) error {
				hash = binary.BigEndian.Uint64(val)
				return nil
			})
			if err != nil {
				return err
			}
			lat, lon := DecodeGeoHash(hash)
			results = append(results, [2]float64{lat, lon})
		}
		return nil
	})
	return results, err
}

// GeoHash returns the geohash strings for members
func (s *BotreonStore) GeoHash(key string, members ...string) ([]string, error) {
	var results []string
	err := s.db.View(func(txn *badger.Txn) error {
		if err := checkKeyType(txn, key, KeyTypeGeo); err != nil {
			return err
		}
		for _, member := range members {
			hashKey := geoIndexKey(key, member)
			item, err := txn.Get(hashKey)
			if errors.Is(err, badger.ErrKeyNotFound) {
				results = append(results, "")
				continue
			}
			if err != nil {
				return err
			}
			var hash uint64
			err = item.Value(func(val []byte) error {
				hash = binary.BigEndian.Uint64(val)
				return nil
			})
			if err != nil {
				return err
			}
			results = append(results, geoHashToString(hash))
		}
		return nil
	})
	return results, err
}

// GeoDist calculates the distance between two members
func (s *BotreonStore) GeoDist(key, member1, member2, unit string) (float64, error) {
	var dist float64

	err := s.db.View(func(txn *badger.Txn) error {
		if err := checkKeyType(txn, key, KeyTypeGeo); err != nil {
			return err
		}
		// Get first member position
		hashKey1 := geoIndexKey(key, member1)
		item1, err := txn.Get(hashKey1)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return fmt.Errorf("member not found: %s", member1)
		}
		if err != nil {
			return err
		}
		var hash1 uint64
		if err := item1.Value(func(val []byte) error {
			hash1 = binary.BigEndian.Uint64(val)
			return nil
		}); err != nil {
			return err
		}
		lat1, lon1 := DecodeGeoHash(hash1)

		// Get second member position
		hashKey2 := geoIndexKey(key, member2)
		item2, err := txn.Get(hashKey2)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return fmt.Errorf("member not found: %s", member2)
		}
		if err != nil {
			return err
		}
		var hash2 uint64
		if err := item2.Value(func(val []byte) error {
			hash2 = binary.BigEndian.Uint64(val)
			return nil
		}); err != nil {
			return err
		}
		lat2, lon2 := DecodeGeoHash(hash2)

		dist = calculateDistance(lat1, lon1, lat2, lon2)

		// Convert unit
		switch strings.ToUpper(unit) {
		case "M", "":
			// meters, default
		case "KM":
			dist /= 1000
		case "MI":
			dist /= 1609.344
		case "FT":
			dist *= 3.28084
		default:
			return fmt.Errorf("unsupported unit: %s", unit)
		}

		return nil
	})

	return dist, err
}

// geoHashToBoundingBox converts a geohash to bounding box coordinates
func geoHashToBoundingBox(hash uint64) (minLat, maxLat, minLon, maxLon float64) {
	latBits := hash >> 26
	lonBits := hash & ((1 << 26) - 1)

	minLat = DecodeLatLonBits(latBits, -90, 90, 26)
	maxLat = DecodeLatLonBits(latBits|((1<<26)-1), -90, 90, 26)
	minLon = DecodeLatLonBits(lonBits, -180, 180, 26)
	maxLon = DecodeLatLonBits(lonBits|((1<<26)-1), -180, 180, 26)

	return
}

// expandBoundingBox expands a bounding box to include adjacent areas
func expandBoundingBox(minLat, maxLat, minLon, maxLon float64, radiusMeters float64) (newMinLat, newMaxLat, newMinLon, newMaxLon float64) {
	// Convert radius to degrees (approximate)
	latDelta := radiusMeters / earthRadiusMeters * 180 / math.Pi
	lonDelta := radiusMeters / (earthRadiusMeters * math.Cos(minLat*math.Pi/180)) * 180 / math.Pi

	newMinLat = minLat - latDelta
	newMaxLat = maxLat + latDelta
	newMinLon = minLon - lonDelta
	newMaxLon = maxLon + lonDelta

	// Clamp to valid ranges
	if newMinLat < -90 {
		newMinLat = -90
	}
	if newMaxLat > 90 {
		newMaxLat = 90
	}
	if newMinLon < -180 {
		newMinLon = -180
	}
	if newMaxLon > 180 {
		newMaxLon = 180
	}

	return
}

func convertGeoRadiusToMeters(radius float64, unit string) float64 {
	radiusM := radius
	switch strings.ToUpper(unit) {
	case "M", "":
		// meters
	case "KM":
		radiusM *= 1000
	case "MI":
		radiusM *= 1609.344
	case "FT":
		radiusM *= 0.3048
	}
	return radiusM
}

func formatGeoDistance(distM float64, unit string) float64 {
	switch strings.ToUpper(unit) {
	case "M", "":
		return distM
	case "KM":
		return distM / 1000
	case "MI":
		return distM / 1609.344
	case "FT":
		return distM * 3.28084
	default:
		return distM
	}
}

// geoRadiusInTxn searches geo members within radius inside an open transaction.
func geoRadiusInTxn(s *BotreonStore, txn *badger.Txn, key string, lon, lat, radiusM float64, unit string, count int, withDist, withHash, withCoord bool) ([]GeoSearchResult, error) {
	if err := checkKeyType(txn, key, KeyTypeGeo); err != nil {
		return nil, err
	}

	centerHash := encodeGeoHash(lat, lon)
	minLat, maxLat, minLon, maxLon := geoHashToBoundingBox(centerHash)
	minLat, maxLat, minLon, maxLon = expandBoundingBox(minLat, maxLat, minLon, maxLon, radiusM)
	minScore := float64(encodeGeoHash(minLat, minLon))
	maxScore := float64(encodeGeoHash(maxLat, maxLon))

	opts := badger.DefaultIteratorOptions
	prefix := keyBadgerGet(prefixKeySortedSetBytes, []byte(key+sortedSetIndex))
	opts.Prefix = prefix
	opts.PrefetchValues = false

	it := txn.NewIterator(opts)
	defer it.Close()

	var results []GeoSearchResult
	var scanCount int64
	startKey := append(prefix, encodeScore(minScore)...)
	for it.Seek(startKey); it.ValidForPrefix(prefix); it.Next() {
		scanCount++
		if err := s.checkScanBudget(scanCount); err != nil {
			return nil, err
		}
		score, member, _, ok := parseZSetIndexKey(it.Item().Key(), prefix)
		if !ok {
			continue
		}
		// 上界裁剪：超过扩展后的 geohash 范围则停止扫描
		if score > maxScore {
			break
		}
		memberHash := score
		memberLat, memberLon := DecodeGeoHash(uint64(memberHash))

		dist := calculateDistance(lat, lon, memberLat, memberLon)
		if dist > radiusM {
			continue
		}

		result := GeoSearchResult{
			Member: member,
			Lat:    memberLat,
			Lon:    memberLon,
		}

		if withDist {
			result.Dist = formatGeoDistance(dist, unit)
		}
		if withHash {
			result.Hash = geoHashToString(uint64(memberHash))
		}
		if withCoord {
			result.Lat = memberLat
			result.Lon = memberLon
		}

		results = append(results, result)
		if count > 0 && len(results) >= count {
			break
		}
	}
	return results, nil
}

// GeoRadius searches for members within a radius
func (s *BotreonStore) GeoRadius(key string, lon, lat, radius float64, unit string, count int, withDist, withHash, withCoord bool) ([]GeoSearchResult, error) {
	radiusM := convertGeoRadiusToMeters(radius, unit)
	var results []GeoSearchResult
	err := s.db.View(func(txn *badger.Txn) error {
		var err error
		results, err = geoRadiusInTxn(s, txn, key, lon, lat, radiusM, unit, count, withDist, withHash, withCoord)
		return err
	})
	return results, err
}

// geoBoxInTxn searches geo members inside an axis-aligned bounding box
// (Redis GEOSEARCH BYBOX semantics). The box is centered at (lon, lat) and
// spans widthM × heightM meters; its edges stay axis-aligned in lon/lat space
// (unlike a circle). A member is returned when its decoded coordinates fall
// within [lat±halfLat] and its longitude is within halfLon of the center.
//
// The scan walks every zset index entry of the key and filters in memory:
// the geohash score is not space-contiguous, so a score-range seek cannot
// bound a rectangular box the way it can bound a circle.
func geoBoxInTxn(s *BotreonStore, txn *badger.Txn, key string, lon, lat, widthM, heightM float64, unit string, count int, withDist, withHash, withCoord bool) ([]GeoSearchResult, error) {
	if err := checkKeyType(txn, key, KeyTypeGeo); err != nil {
		return nil, err
	}

	// Box edges in degrees. halfLon converts the width to degrees at the
	// center latitude's meridian length (Redis normalizes to center lat).
	halfLatDeg := (heightM / 2) / earthRadiusMeters * 180 / math.Pi
	halfLonDeg := (widthM / 2) / (earthRadiusMeters * math.Cos(lat*math.Pi/180)) * 180 / math.Pi
	minLat, maxLat := lat-halfLatDeg, lat+halfLatDeg

	var results []GeoSearchResult
	var scanCount int64
	prefix := keyBadgerGet(prefixKeySortedSetBytes, []byte(key+sortedSetIndex))
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	defer it.Close()
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		scanCount++
		if err := s.checkScanBudget(scanCount); err != nil {
			return nil, err
		}
		score, member, _, ok := parseZSetIndexKey(it.Item().Key(), prefix)
		if !ok {
			continue
		}
		memberLat, memberLon := DecodeGeoHash(uint64(score))
		if memberLat < minLat || memberLat > maxLat {
			continue
		}
		// Longitude via wrap-around distance: equivalent to the plain
		// [lon±halfLon] interval when the box does not straddle the
		// antimeridian, and correct when it does.
		wrapDist := math.Abs(memberLon - lon)
		if wrapDist > 180 {
			wrapDist = 360 - wrapDist
		}
		if wrapDist > halfLonDeg {
			continue
		}
		// WITHDIST: Redis reports distance from the box center (haversine).
		dist := calculateDistance(lat, lon, memberLat, memberLon)
		result := GeoSearchResult{
			Member: member,
			Lat:    memberLat,
			Lon:    memberLon,
		}
		if withDist {
			result.Dist = formatGeoDistance(dist, unit)
		}
		if withHash {
			result.Hash = geoHashToString(uint64(score))
		}
		results = append(results, result)
		if count > 0 && len(results) >= count {
			break
		}
	}
	return results, nil
}

// GeoSearch searches for members by various criteria.
func (s *BotreonStore) GeoSearch(key string, centerLon, centerLat float64, radius float64, unit string, count int, withDist, withHash, withCoord bool) ([]GeoSearchResult, error) {
	return s.GeoRadius(key, centerLon, centerLat, radius, unit, count, withDist, withHash, withCoord)
}

// GeoSearchBox searches for members inside a BYBOX rectangle centered at
// (centerLon, centerLat) with the given width/height in the given unit.
func (s *BotreonStore) GeoSearchBox(key string, centerLon, centerLat, width, height float64, unit string, count int, withDist, withHash, withCoord bool) ([]GeoSearchResult, error) {
	widthM := convertGeoRadiusToMeters(width, unit)
	heightM := convertGeoRadiusToMeters(height, unit)
	var results []GeoSearchResult
	err := s.db.View(func(txn *badger.Txn) error {
		var err error
		results, err = geoBoxInTxn(s, txn, key, centerLon, centerLat, widthM, heightM, unit, count, withDist, withHash, withCoord)
		return err
	})
	return results, err
}

// GeoSearchStore searches and stores results to a destination key.
// shape selects the search geometry: "RADIUS" (circle) or "BOX" (BYBOX, where
// radius carries the box width and boxHeight carries the box height).
func (s *BotreonStore) GeoSearchStore(dstKey, srcKey string, centerLon, centerLat float64, radius float64, unit string, count int, storeDist bool, shape string, boxHeight float64) (int64, error) {
	unlock := s.keyLockMgr.LockMulti([]string{dstKey, srcKey})
	defer unlock()
	radiusM := convertGeoRadiusToMeters(radius, unit)
	var added int64
	var addedNewMember bool
	err := s.retryUpdate(func(txn *badger.Txn) error {
		added = 0
		addedNewMember = false
		var results []GeoSearchResult
		var err error
		if shape == "BOX" {
			heightM := convertGeoRadiusToMeters(boxHeight, unit)
			results, err = geoBoxInTxn(s, txn, srcKey, centerLon, centerLat, radiusM, heightM, unit, count, storeDist, false, false)
		} else {
			results, err = geoRadiusInTxn(s, txn, srcKey, centerLon, centerLat, radiusM, unit, count, storeDist, false, false)
		}
		if err != nil {
			return err
		}
		if len(results) == 0 {
			return nil
		}

		members := make([]ZSetMember, 0, len(results))
		for _, result := range results {
			if storeDist {
				members = append(members, ZSetMember{Member: result.Member, Score: result.Dist})
				continue
			}
			hash := encodeGeoHash(result.Lat, result.Lon)
			members = append(members, ZSetMember{Member: result.Member, Score: float64(hash)})
		}

		addedNewMember, err = zAddMembersInTxn(txn, dstKey, members)
		if err != nil {
			return err
		}
		added = int64(len(members))
		return nil
	}, 20, func() []byte {
		// D4 全重放：GEOSEARCHSTORE dst src FROMLONLAT <lon> <lat>
		// BYRADIUS <r> <unit> 或 BYBOX <w> <h> <unit>——[COUNT <n>] [STOREDIST]
		args := make([][]byte, 0, 10)
		args = append(args, []byte("GEOSEARCHSTORE"), []byte(dstKey), []byte(srcKey),
			[]byte("FROMLONLAT"), []byte(strconv.FormatFloat(centerLon, 'f', -1, 64)),
			[]byte(strconv.FormatFloat(centerLat, 'f', -1, 64)))
		if shape == "BOX" {
			args = append(args, []byte("BYBOX"), []byte(strconv.FormatFloat(radius, 'f', -1, 64)),
				[]byte(strconv.FormatFloat(boxHeight, 'f', -1, 64)), []byte(unit))
		} else {
			args = append(args, []byte("BYRADIUS"), []byte(strconv.FormatFloat(radius, 'f', -1, 64)), []byte(unit))
		}
		if count > 0 {
			args = append(args, []byte("COUNT"), []byte(strconv.Itoa(count)))
		}
		if storeDist {
			args = append(args, []byte("STOREDIST"))
		}
		return encodePropagateCommand(args...)
	}())
	if err == nil && addedNewMember {
		s.notifyBlockingZPop(dstKey)
	}
	return added, err
}

// extractMembers extracts member names from results.
//
//nolint:unused // used by geospatial_helper_test.go; linter skips _test.go
func extractMembers(results []GeoSearchResult) []string {
	members := make([]string, len(results))
	for i, r := range results {
		members[i] = r.Member
	}
	return members
}

// geoDelMemberInTxn removes one geo member inside an open update transaction.
// Returns true when the member existed and was deleted.
func geoDelMemberInTxn(txn *badger.Txn, key, member string) (bool, error) {
	hashKey := geoIndexKey(key, member)
	item, err := txn.Get(hashKey)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var hash uint64
	if err := item.Value(func(val []byte) error {
		hash = binary.BigEndian.Uint64(val)
		return nil
	}); err != nil {
		return false, err
	}

	if err := txn.Delete(sortedSetKeyMember(key, member)); err != nil {
		return false, err
	}
	if err := txn.Delete(sortedSetKeyIndex(key, float64(hash), member, 0)); err != nil {
		return false, err
	}
	if err := txn.Delete(hashKey); err != nil {
		return false, err
	}
	if err := txn.Delete(geoHashToCoordKey(key, hash)); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return false, err
	}

	metaKey := geoKey(key)
	var count int64
	metaItem, err := txn.Get(metaKey)
	if err == nil {
		err = metaItem.Value(func(val []byte) error {
			count = int64(binary.BigEndian.Uint64(val))
			return nil
		})
		if err != nil {
			return false, err
		}
	}
	count--
	if count <= 0 {
		if err := txn.Delete(metaKey); err != nil {
			return false, err
		}
		if err := txn.Delete(TypeOfKeyGet(key)); err != nil {
			return false, err
		}
	} else {
		metaBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(metaBytes, uint64(count))
		if err := txn.Set(metaKey, metaBytes); err != nil {
			return false, err
		}
	}
	return true, nil
}

// GeoDel removes members from a geo set
func (s *BotreonStore) GeoDel(key, member string) error {
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)
	return s.retryUpdate(func(txn *badger.Txn) error {
		_, err := geoDelMemberInTxn(txn, key, member)
		return err
	}, 20, encodePropagateCommand([]byte("ZREM"), []byte(key), []byte(member)))
}

// GeoRadiusByMember searches for members near a member
func (s *BotreonStore) GeoRadiusByMember(key, member string, radius float64, unit string, count int, withDist, withHash, withCoord bool) ([]GeoSearchResult, error) {
	radiusM := convertGeoRadiusToMeters(radius, unit)
	var results []GeoSearchResult
	err := s.db.View(func(txn *badger.Txn) error {
		if err := checkKeyType(txn, key, KeyTypeGeo); err != nil {
			return err
		}
		hashKey := geoIndexKey(key, member)
		item, err := txn.Get(hashKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return fmt.Errorf("member not found: %s", member)
		}
		if err != nil {
			return err
		}
		var hash uint64
		if err := item.Value(func(val []byte) error {
			hash = binary.BigEndian.Uint64(val)
			return nil
		}); err != nil {
			return err
		}
		lat, lon := DecodeGeoHash(hash)
		results, err = geoRadiusInTxn(s, txn, key, lon, lat, radiusM, unit, count, withDist, withHash, withCoord)
		return err
	})
	return results, err
}

// geoAllPositionsInTxn returns lat/lon for every member in one transaction.
func geoAllPositionsInTxn(txn *badger.Txn, key string) (map[string][2]float64, error) {
	if err := checkKeyType(txn, key, KeyTypeGeo); err != nil {
		return nil, err
	}

	opts := badger.DefaultIteratorOptions
	prefix := keyBadgerGet(prefixKeySortedSetBytes, []byte(key+sortedSetIndex))
	opts.Prefix = prefix
	opts.PrefetchValues = false

	it := txn.NewIterator(opts)
	defer it.Close()

	result := make(map[string][2]float64)
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		score, member, _, ok := parseZSetIndexKey(it.Item().Key(), prefix)
		if !ok {
			continue
		}
		lat, lon := DecodeGeoHash(uint64(score))
		result[member] = [2]float64{lat, lon}
	}
	return result, nil
}

// GeoMembers returns all members in a geo set
func (s *BotreonStore) GeoMembers(key string) ([]string, error) {
	var members []string
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		prefix := keyBadgerGet(prefixKeySortedSetBytes, []byte(key+sortedSetIndex))
		opts.Prefix = prefix
		opts.PrefetchValues = false

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			_, member, _, ok := parseZSetIndexKey(it.Item().Key(), prefix)
			if ok {
				members = append(members, member)
			}
		}
		return nil
	})
	return members, err
}

// GeoCard returns the number of members in a geo set
func (s *BotreonStore) GeoCard(key string) (int64, error) {
	var count int64
	err := s.db.View(func(txn *badger.Txn) error {
		if err := checkKeyType(txn, key, KeyTypeGeo); err != nil {
			return err
		}
		item, err := txn.Get(geoKey(key))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			count = int64(binary.BigEndian.Uint64(val))
			return nil
		})
	})
	return count, err
}

// GeoRemove removes multiple members from a geo set in one transaction.
func (s *BotreonStore) GeoRemove(key string, members ...string) (int64, error) {
	if len(members) == 0 {
		return 0, nil
	}
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)
	var removed int64
	err := s.retryUpdate(func(txn *badger.Txn) error {
		removed = 0 // reset each attempt; stale value must not survive conflict retry
		for _, member := range members {
			ok, err := geoDelMemberInTxn(txn, key, member)
			if err != nil {
				return err
			}
			if ok {
				removed++
			}
		}
		return nil
	}, 20, encodePropagateStringArgs([]byte("ZREM"), append([]string{key}, members...)))
	return removed, err
}

// GetHash returns the geohash for a member
func (s *BotreonStore) GeoGetHash(key, member string) (string, error) {
	hashes, err := s.GeoHash(key, member)
	if err != nil {
		return "", err
	}
	if len(hashes) == 0 {
		return "", nil
	}
	return hashes[0], nil
}

// GetAllGeoHashes returns geohashes for all members
func (s *BotreonStore) GeoGetAllHashes(key string) (map[string]string, error) {
	result := make(map[string]string)
	err := s.db.View(func(txn *badger.Txn) error {
		if err := checkKeyType(txn, key, KeyTypeGeo); err != nil {
			return err
		}
		opts := badger.DefaultIteratorOptions
		prefix := keyBadgerGet(prefixKeySortedSetBytes, []byte(key+sortedSetIndex))
		opts.Prefix = prefix
		opts.PrefetchValues = false

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			score, member, _, ok := parseZSetIndexKey(it.Item().Key(), prefix)
			if !ok {
				continue
			}
			result[member] = geoHashToString(uint64(score))
		}
		return nil
	})
	return result, err
}

// GetAllGeoPositions returns positions for all members
func (s *BotreonStore) GeoGetAllPositions(key string) (map[string][2]float64, error) {
	var result map[string][2]float64
	err := s.db.View(func(txn *badger.Txn) error {
		var err error
		result, err = geoAllPositionsInTxn(txn, key)
		return err
	})
	return result, err
}

// GetAllGeoDistances calculates distances between all pairs
func (s *BotreonStore) GeoGetAllDistances(key string, fromMember string, unit string) (map[string]float64, error) {
	result := make(map[string]float64)
	err := s.db.View(func(txn *badger.Txn) error {
		positions, err := geoAllPositionsInTxn(txn, key)
		if err != nil {
			return err
		}
		fromPos, ok := positions[fromMember]
		if !ok {
			return fmt.Errorf("member not found: %s", fromMember)
		}
		for member, pos := range positions {
			if member == fromMember {
				continue
			}
			dist := calculateDistance(fromPos[0], fromPos[1], pos[0], pos[1])
			result[member] = formatGeoDistance(dist, unit)
		}
		return nil
	})
	return result, err
}
