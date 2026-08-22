package server

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/store"
)

// handleGEOADD 实现 GEOADD 命令
func (h *Handler) handleGEOADD(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 4 {
		return proto.NewError("ERR wrong number of arguments for 'GEOADD' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	var opts store.GeoAddOptions
	members := make([]store.GeoMember, 0)

	// Parse options: NX XX CH (before the lon/lat/member triples)
	i := 1
parseOpts:
	for i < len(args)-2 {
		switch strings.ToUpper(string(args[i])) {
		case "NX":
			opts.NX = true
			i++
		case "XX":
			opts.XX = true
			i++
		case "CH":
			opts.CH = true
			i++
		default:
			break parseOpts
		}
	}
	if opts.NX && opts.XX {
		return proto.NewError("ERR XX and NX options at the same time are not compatible")
	}

	for ; i+2 < len(args); i += 3 {
		lon, err1 := strconv.ParseFloat(string(args[i]), 64)
		lat, err2 := strconv.ParseFloat(string(args[i+1]), 64)
		if err1 != nil || err2 != nil {
			return proto.NewError("ERR value is not a valid float")
		}
		members = append(members, store.GeoMember{
			Lat:    lat,
			Lon:    lon,
			Member: string(args[i+2]),
		})
	}
	h.markDirtyKeys(state, key)
	added, err := h.Db.GeoAddWithOptions(key, opts, members)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return wrapLogError(err)
	}
	return proto.NewInteger(added)
}

// handleGEOPOS 实现 GEOPOS 命令
func (h *Handler) handleGEOPOS(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'GEOPOS' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	members := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		members[i-1] = string(args[i])
	}
	positions, err := h.Db.GeoPos(key, members...)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return wrapLogError(err)
	}
	results := make([]proto.RESP, len(positions))
	for i, pos := range positions {
		if pos[0] == 0 && pos[1] == 0 {
			if state.respVersion == 3 {
				results[i] = &proto.Null{}
			} else {
				results[i] = proto.NewBulkString(nil)
			}
		} else {
			results[i] = &proto.NestedArray{
				Elems: []proto.RESP{
					proto.NewBulkString([]byte(fmt.Sprintf("%.6f", pos[1]))),
					proto.NewBulkString([]byte(fmt.Sprintf("%.6f", pos[0]))),
				},
			}
		}
	}
	return &proto.NestedArray{Elems: results}
}

// handleGEOHASH 实现 GEOHASH 命令
func (h *Handler) handleGEOHASH(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'GEOHASH' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	members := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		members[i-1] = string(args[i])
	}
	hashes, err := h.Db.GeoHash(key, members...)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return wrapLogError(err)
	}
	hashResults := make([][]byte, len(hashes))
	for i, h := range hashes {
		hashResults[i] = []byte(h)
	}
	return &proto.Array{Args: hashResults}
}

// handleGEODIST 实现 GEODIST 命令
func (h *Handler) handleGEODIST(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'GEODIST' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	member1 := string(args[1])
	member2 := string(args[2])
	unit := "m"
	if len(args) >= 4 {
		unit = string(args[3])
	}
	dist, err := h.Db.GeoDist(key, member1, member2, unit)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return wrapLogError(err)
	}
	return proto.NewBulkString([]byte(fmt.Sprintf("%.4f", dist)))
}

// handleGEORADIUS 实现 GEORADIUS 命令
func (h *Handler) handleGEORADIUS(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 5 {
		return proto.NewError("ERR wrong number of arguments for 'GEORADIUS' command")
	}
	gKey := string(args[0])
	if resp := h.checkAndHandleRedirect(state, gKey); resp != nil {
		return resp
	}
	gLon, err := strconv.ParseFloat(string(args[1]), 64)
	if err != nil {
		return proto.NewError("ERR value is not a valid float")
	}
	gLat, err := strconv.ParseFloat(string(args[2]), 64)
	if err != nil {
		return proto.NewError("ERR value is not a valid float")
	}
	gRadius, err := strconv.ParseFloat(string(args[3]), 64)
	if err != nil {
		return proto.NewError("ERR value is not a valid float")
	}
	gUnit := strings.ToLower(string(args[4]))

	var gCount int
	var gWithDist, gWithHash, gWithCoord bool

	gI := 5
	for gI < len(args) {
		opt := strings.ToUpper(string(args[gI]))
		switch opt {
		case "WITHCOORD":
			gWithCoord = true
			gI++
		case "WITHDIST":
			gWithDist = true
			gI++
		case "WITHHASH":
			gWithHash = true
			gI++
		case "COUNT":
			if gI+1 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			c, err := strconv.Atoi(string(args[gI+1]))
			if err != nil {
				return proto.NewError("ERR value is not an integer")
			}
			gCount = c
			gI += 2
		case "ASC", "DESC":
			gI++
		default:
			return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option '%s'", opt))
		}
	}

	gResults, err := h.Db.GeoRadius(gKey, gLon, gLat, gRadius, gUnit, gCount, gWithDist, gWithHash, gWithCoord)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return wrapLogError(err)
	}

	if !gWithCoord && !gWithDist && !gWithHash {
		gResp := make([][]byte, len(gResults))
		for i, r := range gResults {
			gResp[i] = []byte(r.Member)
		}
		return &proto.Array{Args: gResp}
	}

	gResp := make([]proto.RESP, len(gResults))
	for i, r := range gResults {
		elems := []proto.RESP{proto.NewBulkString([]byte(r.Member))}
		if gWithDist {
			elems = append(elems, proto.NewBulkString([]byte(fmt.Sprintf("%.4f", r.Dist))))
		}
		if gWithHash {
			elems = append(elems, proto.NewBulkString([]byte(r.Hash)))
		}
		if gWithCoord {
			elems = append(elems, &proto.NestedArray{
				Elems: []proto.RESP{
					proto.NewBulkString([]byte(fmt.Sprintf("%.6f", r.Lon))),
					proto.NewBulkString([]byte(fmt.Sprintf("%.6f", r.Lat))),
				},
			})
		}
		gResp[i] = &proto.NestedArray{Elems: elems}
	}
	return &proto.NestedArray{Elems: gResp}
}

// handleGEOSEARCH 实现 GEOSEARCH 命令
func (h *Handler) handleGEOSEARCH(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 4 {
		return proto.NewError("ERR wrong number of arguments for 'GEOSEARCH' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	var centerLon, centerLat float64
	var radius, boxHeight float64
	var unit string
	var count int
	var withDist, withHash, withCoord bool
	searchByBox := false

	i := 1
	if strings.ToUpper(string(args[i])) == "FROMMEMBER" {
		if i+1 >= len(args) {
			return proto.NewError("ERR syntax error")
		}
		member := string(args[i+1])
		positions, err := h.Db.GeoPos(key, member)
		if err != nil || len(positions) == 0 || (positions[0][0] == 0 && positions[0][1] == 0) {
			return proto.NewError("ERR could not decode query zset member")
		}
		centerLon = positions[0][1]
		centerLat = positions[0][0]
		i += 2
	} else if strings.ToUpper(string(args[i])) == "FROMLONLAT" {
		if i+2 >= len(args) {
			return proto.NewError("ERR syntax error")
		}
		var err1, err2 error
		centerLon, err1 = strconv.ParseFloat(string(args[i+1]), 64)
		centerLat, err2 = strconv.ParseFloat(string(args[i+2]), 64)
		if err1 != nil || err2 != nil {
			return proto.NewError("ERR value is not a valid float")
		}
		i += 3
	} else {
		return proto.NewError("ERR syntax error")
	}

	if i >= len(args) {
		return proto.NewError("ERR syntax error")
	}
	if strings.ToUpper(string(args[i])) == "BYRADIUS" {
		if i+2 >= len(args) {
			return proto.NewError("ERR syntax error")
		}
		var err error
		radius, err = strconv.ParseFloat(string(args[i+1]), 64)
		if err != nil {
			return proto.NewError("ERR value is not a valid float")
		}
		unit = string(args[i+2])
		i += 3
	} else if strings.ToUpper(string(args[i])) == "BYBOX" {
		if i+3 >= len(args) {
			return proto.NewError("ERR syntax error")
		}
		width, err := strconv.ParseFloat(string(args[i+1]), 64)
		if err != nil {
			return proto.NewError("ERR value is not a valid float")
		}
		height, err := strconv.ParseFloat(string(args[i+2]), 64)
		if err != nil {
			return proto.NewError("ERR value is not a valid float")
		}
		unit = string(args[i+3])
		// Redis accepts 0 or non-positive width/height for BYBOX (empty result).
		if width <= 0 || height <= 0 {
			return &proto.Array{Args: [][]byte{}}
		}
		radius = width
		boxHeight = height
		searchByBox = true
		i += 4
	} else {
		return proto.NewError("ERR syntax error")
	}

	for i < len(args) {
		opt := strings.ToUpper(string(args[i]))
		switch opt {
		case "ASC", "DESC":
			i++
		case "COUNT":
			if i+1 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			c, err := strconv.Atoi(string(args[i+1]))
			if err != nil {
				return proto.NewError("ERR value is not an integer")
			}
			count = c
			i += 2
		case "WITHCOORD":
			withCoord = true
			i++
		case "WITHDIST":
			withDist = true
			i++
		case "WITHHASH":
			withHash = true
			i++
		default:
			return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option '%s'", opt))
		}
	}

	var results []store.GeoSearchResult
	var err error
	if searchByBox {
		results, err = h.Db.GeoSearchBox(key, centerLon, centerLat, radius, boxHeight, unit, count, withDist, withHash, withCoord)
	} else {
		results, err = h.Db.GeoSearch(key, centerLon, centerLat, radius, unit, count, withDist, withHash, withCoord)
	}
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return wrapStoreError(err)
	}

	if !withCoord && !withDist && !withHash {
		resp := make([][]byte, len(results))
		for i, r := range results {
			resp[i] = []byte(r.Member)
		}
		return &proto.Array{Args: resp}
	}

	resp := make([]proto.RESP, len(results))
	for i, r := range results {
		elems := []proto.RESP{proto.NewBulkString([]byte(r.Member))}
		if withDist {
			elems = append(elems, proto.NewBulkString([]byte(fmt.Sprintf("%.4f", r.Dist))))
		}
		if withHash {
			elems = append(elems, proto.NewBulkString([]byte(r.Hash)))
		}
		if withCoord {
			elems = append(elems, &proto.NestedArray{
				Elems: []proto.RESP{
					proto.NewBulkString([]byte(fmt.Sprintf("%.6f", r.Lon))),
					proto.NewBulkString([]byte(fmt.Sprintf("%.6f", r.Lat))),
				},
			})
		}
		resp[i] = &proto.NestedArray{Elems: elems}
	}
	return &proto.NestedArray{Elems: resp}
}

// handleGEOSEARCHSTORE 实现 GEOSEARCHSTORE 命令
func (h *Handler) handleGEOSEARCHSTORE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 4 {
		return proto.NewError("ERR wrong number of arguments for 'GEOSEARCHSTORE' command")
	}
	dstKey := string(args[0])
	srcKey := string(args[1])
	if resp := h.checkAndHandleMultiKeyRedirect([]string{dstKey, srcKey}); resp != nil {
		return resp
	}

	var centerLon, centerLat float64
	var radius, boxHeight float64
	var unit string
	var count int
	var storeDist bool
	searchByBox := false

	i := 2
	if i < len(args) && strings.ToUpper(string(args[i])) == "FROMMEMBER" {
		if i+1 >= len(args) {
			return proto.NewError("ERR syntax error")
		}
		member := string(args[i+1])
		positions, err := h.Db.GeoPos(srcKey, member)
		if err != nil || len(positions) == 0 || (positions[0][0] == 0 && positions[0][1] == 0) {
			return proto.NewError("ERR could not decode query zset member")
		}
		centerLon = positions[0][1]
		centerLat = positions[0][0]
		i += 2
	} else if i < len(args) && strings.ToUpper(string(args[i])) == "FROMLONLAT" {
		if i+2 >= len(args) {
			return proto.NewError("ERR syntax error")
		}
		var err1, err2 error
		centerLon, err1 = strconv.ParseFloat(string(args[i+1]), 64)
		centerLat, err2 = strconv.ParseFloat(string(args[i+2]), 64)
		if err1 != nil || err2 != nil {
			return proto.NewError("ERR value is not a valid float")
		}
		i += 3
	}

	if i >= len(args) {
		return proto.NewError("ERR syntax error")
	}
	if strings.ToUpper(string(args[i])) == "BYRADIUS" {
		if i+2 >= len(args) {
			return proto.NewError("ERR syntax error")
		}
		var err error
		radius, err = strconv.ParseFloat(string(args[i+1]), 64)
		if err != nil {
			return proto.NewError("ERR value is not a valid float")
		}
		unit = string(args[i+2])
		i += 3
	} else if strings.ToUpper(string(args[i])) == "BYBOX" {
		if i+3 >= len(args) {
			return proto.NewError("ERR syntax error")
		}
		width, err := strconv.ParseFloat(string(args[i+1]), 64)
		if err != nil {
			return proto.NewError("ERR value is not a valid float")
		}
		height, err := strconv.ParseFloat(string(args[i+2]), 64)
		if err != nil {
			return proto.NewError("ERR value is not a valid float")
		}
		unit = string(args[i+3])
		// Redis accepts 0 or non-positive width/height for BYBOX (empty result).
		if width <= 0 || height <= 0 {
			return proto.NewInteger(0)
		}
		radius = width
		boxHeight = height
		searchByBox = true
		i += 4
	} else {
		return proto.NewError("ERR syntax error")
	}

	for i < len(args) {
		opt := strings.ToUpper(string(args[i]))
		switch opt {
		case "ASC", "DESC":
			i++
		case "COUNT":
			if i+1 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			c, err := strconv.Atoi(string(args[i+1]))
			if err != nil {
				return proto.NewError("ERR value is not an integer")
			}
			count = c
			i += 2
		case "STOREDIST":
			storeDist = true
			i++
		default:
			return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option '%s'", opt))
		}
	}

	h.markDirtyKeys(state, dstKey)
	shape := "RADIUS"
	if searchByBox {
		shape = "BOX"
	}
	stored, err := h.Db.GeoSearchStore(dstKey, srcKey, centerLon, centerLat, radius, unit, count, storeDist, shape, boxHeight)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return wrapStoreError(err)
	}
	return proto.NewInteger(stored)
}
