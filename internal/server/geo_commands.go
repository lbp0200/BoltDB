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
	for _, pos := range positions {
		if pos[0] == 0 && pos[1] == 0 {
			h.recordKeyspaceMiss()
		} else {
			h.recordKeyspaceHit()
		}
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
	for _, hv := range hashes {
		if hv == "" {
			h.recordKeyspaceMiss()
		} else {
			h.recordKeyspaceHit()
		}
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
		if errors.Is(err, store.ErrKeyNotFound) {
			h.recordKeyspaceMiss()
			if state.respVersion == 3 {
				return &proto.Null{}
			}
			return proto.NewBulkString(nil)
		}
		return wrapLogError(err)
	}
	h.recordKeyspaceHit()
	return proto.NewBulkString([]byte(fmt.Sprintf("%.4f", dist)))
}

// handleGEORADIUS 实现 GEORADIUS 命令
func (h *Handler) handleGEORADIUS(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 5 {
		return proto.NewError("ERR wrong number of arguments for 'GEORADIUS' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	lon, err := strconv.ParseFloat(string(args[1]), 64)
	if err != nil {
		return proto.NewError("ERR value is not a valid float")
	}
	lat, err := strconv.ParseFloat(string(args[2]), 64)
	if err != nil {
		return proto.NewError("ERR value is not a valid float")
	}
	radius, err := strconv.ParseFloat(string(args[3]), 64)
	if err != nil {
		return proto.NewError("ERR value is not a valid float")
	}
	unit := strings.ToLower(string(args[4]))
	return h.geoRadiusCommon(state, key, lon, lat, radius, unit, args[5:])
}

// handleGEORADIUSBYMEMBER 实现 GEORADIUSBYMEMBER 命令：以成员坐标为圆心搜索。
func (h *Handler) handleGEORADIUSBYMEMBER(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 4 {
		return proto.NewError("ERR wrong number of arguments for 'GEORADIUSBYMEMBER' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	positions, err := h.Db.GeoPos(key, string(args[1]))
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return wrapLogError(err)
	}
	if len(positions) == 0 || (positions[0][0] == 0 && positions[0][1] == 0) {
		return proto.NewError("ERR could not decode query zset member")
	}
	radius, err := strconv.ParseFloat(string(args[2]), 64)
	if err != nil {
		return proto.NewError("ERR value is not a valid float")
	}
	unit := strings.ToLower(string(args[3]))
	return h.geoRadiusCommon(state, key, positions[0][1], positions[0][0], radius, unit, args[4:])
}

// hasGeoRadiusStoreOpt 报告选项里是否含 STORE/STOREDIST（_RO 变体禁止）。
func hasGeoRadiusStoreOpt(opts [][]byte) bool {
	for _, o := range opts {
		switch strings.ToUpper(string(o)) {
		case "STORE", "STOREDIST":
			return true
		}
	}
	return false
}

// handleGEORADIUS_RO 实现 GEORADIUS_RO 命令（只读变体，禁止 STORE）。
func (h *Handler) handleGEORADIUS_RO(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) >= 6 && hasGeoRadiusStoreOpt(args[5:]) {
		return proto.NewError("ERR syntax error")
	}
	return h.handleGEORADIUS(state, args, remoteAddr)
}

// handleGEORADIUSBYMEMBER_RO 实现 GEORADIUSBYMEMBER_RO 命令（只读变体，禁止 STORE）。
func (h *Handler) handleGEORADIUSBYMEMBER_RO(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) > 4 && hasGeoRadiusStoreOpt(args[4:]) {
		return proto.NewError("ERR syntax error")
	}
	return h.handleGEORADIUSBYMEMBER(state, args, remoteAddr)
}

// geoRadiusCommon 执行 GEORADIUS 系列的公共逻辑（选项解析 + 搜索 + 响应组装）。
// 支持 STORE key / STOREDIST key（Redis 语义：结果写入 zset，返回写入数量）。
func (h *Handler) geoRadiusCommon(state *connState, key string, lon, lat, radius float64, unit string, opts [][]byte) proto.RESP {
	var count int
	var withDist, withHash, withCoord bool
	var storeKey string
	storeDist := false
	i := 0
	for i < len(opts) {
		opt := strings.ToUpper(string(opts[i]))
		switch opt {
		case "WITHCOORD":
			withCoord = true
			i++
		case "WITHDIST":
			withDist = true
			i++
		case "WITHHASH":
			withHash = true
			i++
		case "COUNT":
			if i+1 >= len(opts) {
				return proto.NewError("ERR syntax error")
			}
			c, err := strconv.Atoi(string(opts[i+1]))
			if err != nil {
				return proto.NewError("ERR value is not an integer")
			}
			count = c
			i += 2
		case "ASC", "DESC":
			i++
		case "STORE":
			if i+1 >= len(opts) {
				return proto.NewError("ERR syntax error")
			}
			storeKey = string(opts[i+1])
			storeDist = false
			i += 2
		case "STOREDIST":
			if i+1 >= len(opts) {
				return proto.NewError("ERR syntax error")
			}
			storeKey = string(opts[i+1])
			storeDist = true
			i += 2
		default:
			return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option '%s'", opt))
		}
	}

	// STORE/STOREDIST 路径：结果写入 zset，返回写入数量（Redis 语义）。
	if storeKey != "" {
		h.markDirtyKeys(state, storeKey)
		added, err := h.Db.GeoSearchStore(storeKey, key, lon, lat, radius, unit, count, storeDist, "RADIUS", 0)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			return wrapLogError(err)
		}
		// GEORADIUS 是只读标志命令，通用传播不触发；这里按规范化命令
		// （GEORADIUS key lon lat radius unit [COUNT n] STORE|STOREDIST dst）
		// 显式传播，保证 replica 与 master 的 zset 结果一致。
		if h.Replication != nil && h.Replication.IsMaster() {
			propArgs := [][]byte{[]byte("GEORADIUS"), []byte(key),
				[]byte(strconv.FormatFloat(lon, 'f', -1, 64)),
				[]byte(strconv.FormatFloat(lat, 'f', -1, 64)),
				[]byte(strconv.FormatFloat(radius, 'f', -1, 64)),
				[]byte(unit)}
			if count > 0 {
				propArgs = append(propArgs, []byte("COUNT"), []byte(strconv.Itoa(count)))
			}
			if storeDist {
				propArgs = append(propArgs, []byte("STOREDIST"), []byte(storeKey))
			} else {
				propArgs = append(propArgs, []byte("STORE"), []byte(storeKey))
			}
			h.Replication.PropagateCommand(propArgs)
		}
		return proto.NewInteger(added)
	}

	results, err := h.Db.GeoRadius(key, lon, lat, radius, unit, count, withDist, withHash, withCoord)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return wrapLogError(err)
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
