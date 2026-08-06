package cluster

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/lbp0200/BoltDB/internal/logger"
	"github.com/lbp0200/BoltDB/internal/proto"
)

const (
	BusPortOffset  = 10000
	busDialTimeout = 5 * time.Second
	busReadTimeout = 30 * time.Second
	gossipSectionN = 3 // nodes to gossip per message

	// gossipPayloadFormatJSON indicates JSON-encoded gossip payload.
	gossipPayloadFormatJSON byte = 'J'
	// gossipPayloadFormatBinary indicates gob-encoded gossip payload.
	gossipPayloadFormatBinary byte = 'G'
)

// GossipNodeInfo is a snapshot of a node's state carried in gossip messages.
type GossipNodeInfo struct {
	ID       string   `json:"id"`
	Addr     string   `json:"addr"`
	Flags    []string `json:"flags"`
	Epoch    int64    `json:"epoch"`
	PingSent int64    `json:"ping_sent"`
	PongRecv int64    `json:"pong_recv"`
}

// SlotOwnerRange represents a slot range owned by a node.
type SlotOwnerRange struct {
	Start uint32 `json:"start"`
	End   uint32 `json:"end"`
}

// SlotOwnerEntry associates a node with its owned slot ranges.
type SlotOwnerEntry struct {
	NodeID string           `json:"node_id"`
	Epoch  int64            `json:"epoch"`
	Ranges []SlotOwnerRange `json:"ranges"`
}

// GossipPayload is the payload carried in PING/PONG bus messages.
type GossipPayload struct {
	Epoch      int64            `json:"epoch"`
	Slots      string           `json:"slots"` // "0-16383" or ""
	SlotOwners []SlotOwnerEntry `json:"slot_owners,omitempty"`
	Nodes      []GossipNodeInfo `json:"nodes"` // gossip section
	PFail      []string         `json:"pfail"` // node IDs we consider PFAIL
}

type busPeer struct {
	conn   net.Conn
	id     string
	reader *bufio.Reader
}

type ClusterBus struct {
	cluster  *Cluster
	listener net.Listener
	peers    map[string]*busPeer
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	mu       sync.RWMutex
	addr     string
	tlsCfg   *tls.Config

	lastSave time.Time
}

func NewClusterBus(cluster *Cluster, ctx context.Context) *ClusterBus {
	busCtx, cancel := context.WithCancel(ctx)
	return &ClusterBus{
		cluster: cluster,
		peers:   make(map[string]*busPeer),
		ctx:     busCtx,
		cancel:  cancel,
	}
}

func (b *ClusterBus) Addr() string { return b.addr }

// SetContext replaces the bus's context with the server lifecycle context.
// This makes the bus respond to server shutdown without requiring explicit Stop().
func (b *ClusterBus) SetContext(ctx context.Context) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.cancel()
	b.ctx, b.cancel = context.WithCancel(ctx)
}

// SetTLSConfig sets the TLS configuration for the cluster bus.
// When set, all bus connections (listener and outbound) use TLS.
func (b *ClusterBus) SetTLSConfig(tlsCfg *tls.Config) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.tlsCfg = tlsCfg
}

func (b *ClusterBus) Start(host string, dataPort int) error {
	busPort := dataPort + BusPortOffset
	addr := fmt.Sprintf("%s:%d", host, busPort)

	var ln net.Listener
	var err error
	if dataPort == 0 {
		ln, err = net.Listen("tcp", host+":0")
	} else {
		ln, err = net.Listen("tcp", addr)
		if err != nil {
			// Port conflict (e.g. TIME_WAIT from previous test) — fall back to random port.
			// In production this should never happen because the bus port is always unique;
			// in tests, rapid start/stop cycles can leave the port in TIME_WAIT.
			logger.Logger.Warn().Err(err).Str("addr", addr).Msg("cluster bus: preferred port in use, falling back to random port")
			ln, err = net.Listen("tcp", host+":0")
		}
	}
	if err != nil {
		return fmt.Errorf("cluster bus: failed to listen on %s: %w", addr, err)
	}
	// Wrap with TLS if configured
	b.mu.RLock()
	tlsCfg := b.tlsCfg
	b.mu.RUnlock()
	if tlsCfg != nil {
		ln = tls.NewListener(ln, tlsCfg)
	}
	b.listener = ln
	b.addr = ln.Addr().String()

	logger.Logger.Info().Str("addr", b.addr).Msg("cluster bus: listening")
	b.wg.Add(1)
	go b.acceptLoop()
	return nil
}

func (b *ClusterBus) Stop() {
	b.cancel()
	if b.listener != nil {
		_ = b.listener.Close()
	}
	b.mu.RLock()
	for _, peer := range b.peers {
		_ = peer.conn.Close()
	}
	b.mu.RUnlock()
	b.wg.Wait()
	logger.Logger.Info().Msg("cluster bus: stopped")
}

func (b *ClusterBus) acceptLoop() {
	defer b.wg.Done()
	for {
		conn, err := b.listener.Accept()
		if err != nil {
			select {
			case <-b.ctx.Done():
				return
			default:
			}
			logger.Logger.Warn().Err(err).Msg("cluster bus: accept error, retrying")
			time.Sleep(100 * time.Millisecond)
			continue
		}
		b.wg.Add(1)
		go b.handleBusConn(conn, "")
	}
}

func (b *ClusterBus) Connect(peerAddr, peerID string) error {
	b.mu.RLock()
	tlsCfg := b.tlsCfg
	b.mu.RUnlock()

	var conn net.Conn
	var err error
	if tlsCfg != nil {
		conn, err = tls.DialWithDialer(&net.Dialer{Timeout: busDialTimeout}, "tcp", peerAddr, tlsCfg)
	} else {
		conn, err = net.DialTimeout("tcp", peerAddr, busDialTimeout)
	}
	if err != nil {
		return fmt.Errorf("cluster bus: connect to %s: %w", peerAddr, err)
	}

	logger.Logger.Info().Str("peer", peerID).Str("addr", peerAddr).Msg("cluster bus: connected")

	b.wg.Add(1)
	go b.handleBusConn(conn, peerID)

	payload := b.BuildGossipPayload()
	if err := writeBusMsg(conn, "PING", b.cluster.Myself.ID, payload); err != nil {
		logger.Logger.Warn().Err(err).Str("peer", peerID).Msg("cluster bus: initial PING failed")
	}
	return nil
}

func (b *ClusterBus) handleBusConn(conn net.Conn, knownPeerID string) {
	defer b.wg.Done()
	defer func() { _ = conn.Close() }()

	reader := bufio.NewReader(conn)
	peerID := knownPeerID
	registered := false

	if peerID != "" {
		b.registerPeer(peerID, conn, reader)
		registered = true
	}

	for {
		select {
		case <-b.ctx.Done():
			return
		default:
		}

		_ = conn.SetDeadline(time.Now().Add(busReadTimeout))
		resp, err := proto.ReadRESP(reader)
		if err != nil {
			if peerID != "" {
				logger.Logger.Warn().Err(err).Str("peer", peerID).Msg("cluster bus: read error, removing peer")
				b.removePeerIfMatch(peerID, conn)
			}
			return
		}

		args := resp.Args
		if len(args) < 2 {
			continue
		}

		senderID := string(args[1])
		if !registered {
			peerID = senderID
			b.registerPeer(senderID, conn, reader)
			registered = true
		}

		cmd := string(args[0])

		var payload *GossipPayload
		if len(args) >= 3 {
			payload, _ = unmarshalGossipPayload(args[2])
		}
		if payload == nil {
			payload = &GossipPayload{}
		}

		switch cmd {
		case "PING":
			aliveDirty := b.handlePING(senderID)
			dirty := b.ApplyGossipPayloadFrom(senderID, payload)
			// Respond with PONG + our payload
			respPayload := b.BuildGossipPayload()
			_ = writeBusMsg(conn, "PONG", b.cluster.Myself.ID, respPayload)
			b.saveIfDirty(aliveDirty || dirty)
		case "PONG":
			aliveDirty := b.handlePONG(senderID)
			dirty := b.ApplyGossipPayloadFrom(senderID, payload)
			b.saveIfDirty(aliveDirty || dirty)
		}
	}
}

func (b *ClusterBus) registerPeer(peerID string, conn net.Conn, reader *bufio.Reader) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if existing, ok := b.peers[peerID]; ok {
		_ = existing.conn.Close()
	}
	b.peers[peerID] = &busPeer{conn: conn, id: peerID, reader: reader}
}

func (b *ClusterBus) handlePING(senderID string) bool {
	b.cluster.mu.Lock()
	defer b.cluster.mu.Unlock()
	node, ok := b.cluster.Nodes[senderID]
	if !ok {
		return false
	}
	node.UpdatePong()
	// 收到存活节点的 PING：若其此前被标记 FAIL/PFAIL，则清除标记
	// 并归还 FAIL 晋升时接管的槽位（P3）。
	return b.cluster.recoverFailedNode(senderID)
}

func (b *ClusterBus) handlePONG(senderID string) bool {
	b.cluster.mu.Lock()
	defer b.cluster.mu.Unlock()
	node, ok := b.cluster.Nodes[senderID]
	if !ok {
		return false
	}
	node.UpdatePong()
	// 收到存活节点的 PONG：若其此前被标记 FAIL/PFAIL，则清除标记
	// 并归还 FAIL 晋升时接管的槽位（P3）。
	return b.cluster.recoverFailedNode(senderID)
}

func (b *ClusterBus) SendPING() {
	payload := b.BuildGossipPayload()

	b.cluster.mu.RLock()
	peers := make([]string, 0, len(b.cluster.Nodes))
	for _, n := range b.cluster.Nodes {
		if n.ID != b.cluster.Myself.ID {
			peers = append(peers, n.ID)
		}
	}
	b.cluster.mu.RUnlock()

	myID := b.cluster.Myself.ID

	b.mu.RLock()
	for _, peerID := range peers {
		peer, ok := b.peers[peerID]
		if !ok {
			continue
		}
		if err := writeBusMsg(peer.conn, "PING", myID, payload); err != nil {
			logger.Logger.Warn().Err(err).Str("peer", peerID).Msg("cluster bus: PING send failed")
			_ = peer.conn.Close()
		} else {
			b.cluster.mu.RLock()
			if n, ok := b.cluster.Nodes[peerID]; ok {
				n.UpdatePing()
			}
			b.cluster.mu.RUnlock()
		}
	}
	b.mu.RUnlock()
}

// BuildGossipPayload builds the gossip payload from the current cluster state.
func (b *ClusterBus) BuildGossipPayload() *GossipPayload {
	b.cluster.mu.RLock()
	defer b.cluster.mu.RUnlock()

	payload := &GossipPayload{
		Epoch: b.cluster.Epoch,
	}

	// Slot range summary (self)
	if b.cluster.Myself != nil {
		mySlots := b.cluster.Myself.GetSlotRanges()
		if len(mySlots) > 0 {
			payload.Slots = formatSlotsBrief(mySlots)
		}
	}

	// Slot owners: collect every known node's slot ranges
	for _, n := range b.cluster.Nodes {
		n.mu.RLock()
		if len(n.Slots) > 0 {
			ranges := make([]SlotOwnerRange, len(n.Slots))
			for i, sr := range n.Slots {
				ranges[i] = SlotOwnerRange(sr)
			}
			nodeEpoch := n.Epoch
			payload.SlotOwners = append(payload.SlotOwners, SlotOwnerEntry{
				NodeID: n.ID,
				Epoch:  nodeEpoch,
				Ranges: ranges,
			})
		}
		n.mu.RUnlock()
	}

	// PFail nodes + gossip section candidates
	others := make([]*Node, 0, len(b.cluster.Nodes))
	for _, n := range b.cluster.Nodes {
		if n.ID == b.cluster.Myself.ID {
			continue
		}
		n.mu.RLock()
		if n.hasFailFlag() {
			payload.PFail = append(payload.PFail, n.ID)
		}
		n.mu.RUnlock()
		others = append(others, n)
	}
	if len(others) > 0 {
		rand.Shuffle(len(others), func(i, j int) {
			others[i], others[j] = others[j], others[i]
		})
		n := gossipSectionN
		if n > len(others) {
			n = len(others)
		}
		payload.Nodes = make([]GossipNodeInfo, n)
		for i, node := range others[:n] {
			node.mu.RLock()
			payload.Nodes[i] = GossipNodeInfo{
				ID:       node.ID,
				Addr:     node.Addr,
				Flags:    make([]string, len(node.Flags)),
				Epoch:    node.Epoch,
				PingSent: node.PingSent,
				PongRecv: node.PongRecv,
			}
			copy(payload.Nodes[i].Flags, node.Flags)
			node.mu.RUnlock()
		}
	}

	return payload
}

// ApplyGossipPayloadFrom processes gossip payload with a known reporter ID.
// reporterID is the node that sent this gossip (for PFAIL tracking).
func (b *ClusterBus) ApplyGossipPayloadFrom(reporterID string, payload *GossipPayload) (dirty bool) {
	if payload == nil {
		return false
	}

	b.cluster.mu.Lock()
	defer b.cluster.mu.Unlock()

	// Update epoch
	if payload.Epoch > b.cluster.Epoch {
		b.cluster.Epoch = payload.Epoch
	}

	// Gossip section first: learn about other nodes before processing slot owners
	now := time.Now().UnixMilli()
	for _, gi := range payload.Nodes {
		if gi.ID == b.cluster.Myself.ID {
			continue
		}
		if existing, ok := b.cluster.Nodes[gi.ID]; ok {
			existing.MergeGossipState(gi.Epoch, gi.PongRecv)
			// 间接恢复：其他节点 gossip 报告该节点最近有响应（新鲜 PongRecv），
			// 说明它已恢复 → 清除 FAIL 标记并归还 FAIL 晋升时接管的槽位（P3）。
			if existing.HasFailFlag() && gi.PongRecv > 0 && now-gi.PongRecv < failTimeout.Milliseconds() {
				if b.cluster.recoverFailedNode(gi.ID) {
					dirty = true
				}
			}
			// P3 重启边界：usurpedSlots 从持久化恢复，但节点从未被标 FAIL
			// （重启后内存态干净，无 FAIL 清除事件）→ 只要 PONG 新鲜即归还，
			// 否则 node 重启后槽位被旧 usurp 清单永久占用。
			// 注意：此处已持有 b.cluster.mu 写锁，直接读 map（勿用 RLock 方法）。
			_, usurped := b.cluster.usurpedSlots[gi.ID]
			if !existing.HasFailFlag() && usurped &&
				gi.PongRecv > 0 && now-gi.PongRecv < failTimeout.Milliseconds() {
				if b.cluster.recoverFailedNode(gi.ID) {
					dirty = true
				}
			}
		} else {
			// 防幽灵节点：gossip 可能带来 MEET 过程中残留的 placeholder
			//（不同 NodeID 但同 Addr），跳过已存在相同地址的条目
			if nodeByAddr := findNodeByAddr(b.cluster.Nodes, gi.Addr); nodeByAddr != nil {
				continue
			}
			node := NewNodeFromGossip(gi.ID, gi.Addr, gi.Flags, gi.Epoch, gi.PingSent, gi.PongRecv)
			b.cluster.Nodes[gi.ID] = node
			logger.Logger.Info().Str("peer", gi.ID).Str("addr", gi.Addr).Msg("cluster gossip: learned new node")
		}
	}

	// Slot owner reconciliation: resolve conflicts by epoch
	// 注意：不跳过 "自己" 的条目——FAIL 接管期间节点被 kill，恢复后本地视图
	// 可能已被接管方的广播污染（epoch 更高），必须允许其他节点广播的
	// "自己拥有槽位"（归还时提升过 epoch）参与仲裁以自我纠正（P3）。
	for _, entry := range payload.SlotOwners {
		for _, r := range entry.Ranges {
			for slot := r.Start; slot <= r.End; slot++ {
				currentOwner := b.cluster.Slots[slot]
				ownerEpoch := int64(0)
				if currentOwner != nil {
					ownerEpoch = currentOwner.GetEpoch()
				}
				if currentOwner == nil || ownerEpoch < entry.Epoch {
					var peerNode *Node
					if entry.NodeID == b.cluster.Myself.ID {
						peerNode = b.cluster.Myself
					} else {
						peerNode = b.cluster.Nodes[entry.NodeID]
					}
					if peerNode != nil && b.cluster.Slots[slot] != peerNode {
						b.cluster.Slots[slot] = peerNode
						// 同步 node.Slots 字段：CLUSTER NODES 显示与
						// BuildGossipPayload 广播都基于它。不同步会导致
						// 恢复节点自己广播时丢失"自己拥有"的声明（P3）。
						peerNode.AddSlotRange(slot, slot)
						dirty = true
						logger.Logger.Debug().
							Str("slotOwner", entry.NodeID).
							Uint32("slot", slot).
							Msg("cluster gossip: slot owner updated via gossip")

					}
				}
			}
		}
	}

	// Clear migration state for slots that are now owned by the current node
	// via gossip. This handles IMPORTING on the target node and stale MIGRATING on the source.
	for _, entry := range payload.SlotOwners {
		if entry.NodeID == b.cluster.Myself.ID {
			for _, r := range entry.Ranges {
				for slot := r.Start; slot <= r.End; slot++ {
					if b.cluster.Myself.IsImportingSlot(slot) {
						b.cluster.Myself.ClearSlotMigration(slot)
						logger.Logger.Debug().
							Uint32("slot", slot).
							Msg("cluster gossip: cleared IMPORTING on slot now owned by self")
					}
				}
			}
		} else {
			for _, r := range entry.Ranges {
				for slot := r.Start; slot <= r.End; slot++ {
					if b.cluster.Myself.IsMigratingSlot(slot) {
						b.cluster.Myself.ClearSlotMigration(slot)
						logger.Logger.Debug().
							Uint32("slot", slot).
							Msg("cluster gossip: cleared MIGRATING on slot no longer owned by self")
					}
				}
			}
		}
	}
	// Mark PFAIL nodes with reporter tracking
	if reporterID != "" {
		for _, pfailID := range payload.PFail {
			if pfailID == b.cluster.Myself.ID || pfailID == reporterID {
				continue
			}
			if _, ok := b.cluster.Nodes[pfailID]; !ok {
				continue
			}
			// Record this reporter's claim
			if b.cluster.pfailReports[pfailID] == nil {
				b.cluster.pfailReports[pfailID] = make(map[string]struct{})
			}
			b.cluster.pfailReports[pfailID][reporterID] = struct{}{}

			// Count unique reporters (excluding self)
			reportCount := len(b.cluster.pfailReports[pfailID])

			// Count total non-self nodes known
			totalNodes := 0
			for id := range b.cluster.Nodes {
				if id != b.cluster.Myself.ID {
					totalNodes++
				}
			}

			// Threshold: number of OTHER nodes' reports required for FAIL,
			// counting our own local PFAIL detection as one vote.
			// Majority of N nodes = N/2+1; reports needed = N/2 = (totalNodes+1)/2.
			// 2-node: 1 report; 3-node: 1 report + local; 5-node: 2 reports + local.
			// (The old `totalNodes/2+1` with the `totalNodes <= 2` special case
			// ignored the local vote and let a single gossip report promote a
			// healthy node to FAIL under load — P5.)
			threshold := (totalNodes + 1) / 2

			if reportCount >= threshold {
				node := b.cluster.Nodes[pfailID]
				// Local confirmation is required: the node must have been
				// detected as PFAIL by OUR OWN failure check, otherwise a
				// single peer report could promote a healthy node.
				if node != nil && node.HasFailFlag() && node.PromotePFailToFail() {
					logger.Logger.Warn().Str("node", pfailID).
						Int("reports", reportCount).
						Int("threshold", threshold).
						Msg("cluster gossip: FAIL promoted via multi-node agreement")
					dirty = true

					// Reassign failed node's slots to self, recording the takeover
					// so slots can be returned when the node recovers (P3).
					b.cluster.usurpFailedNodeSlots(pfailID)
					// Clean up PFAIL reports for this node
					delete(b.cluster.pfailReports, pfailID)
				}
			}
		}
	}
	return dirty
}

func (b *ClusterBus) removePeerIfMatch(peerID string, conn net.Conn) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if p, ok := b.peers[peerID]; ok && p.conn == conn {
		delete(b.peers, peerID)
	}
}

func (b *ClusterBus) saveIfDirty(dirty bool) {
	if !dirty {
		return
	}
	// Throttle: at most once per 30s
	if time.Since(b.lastSave) < 30*time.Second {
		return
	}
	b.lastSave = time.Now()
	if err := b.cluster.SaveConfig(); err != nil {
		logger.Logger.Warn().Err(err).Msg("cluster gossip: failed to persist learned state")
	}
}

func (b *ClusterBus) PeerCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.peers)
}

func (b *ClusterBus) HasPeer(peerID string) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	_, ok := b.peers[peerID]
	return ok
}

// Disconnect removes a peer from the bus and closes its connection.
func (b *ClusterBus) Disconnect(peerID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if p, ok := b.peers[peerID]; ok {
		delete(b.peers, peerID)
		_ = p.conn.Close()
	}
}

func init() {
	// Register types for gob encoding
	gob.Register(GossipPayload{})
	gob.Register(GossipNodeInfo{})
	gob.Register(SlotOwnerEntry{})
	gob.Register(SlotOwnerRange{})
}

// marshalGossipPayloadBinary encodes a GossipPayload using gob binary encoding.
// Returns the format version byte + gob data.
func marshalGossipPayloadBinary(payload *GossipPayload) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte(gossipPayloadFormatBinary)
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(payload); err != nil {
		return nil, fmt.Errorf("gob encode gossip payload: %w", err)
	}
	return buf.Bytes(), nil
}

// unmarshalGossipPayload decodes a gossip payload from either JSON or gob format.
// The first byte indicates the format: 'J' = JSON, 'G' = gob binary.
func unmarshalGossipPayload(data []byte) (*GossipPayload, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty gossip payload")
	}

	var payload GossipPayload

	switch data[0] {
	case gossipPayloadFormatJSON:
		if err := json.Unmarshal(data[1:], &payload); err != nil {
			return nil, fmt.Errorf("json unmarshal gossip payload: %w", err)
		}
	case gossipPayloadFormatBinary:
		buf := bytes.NewReader(data[1:])
		dec := gob.NewDecoder(buf)
		if err := dec.Decode(&payload); err != nil {
			return nil, fmt.Errorf("gob decode gossip payload: %w", err)
		}
	default:
		// 兼容旧版本：没有格式标识的纯 JSON
		if err := json.Unmarshal(data, &payload); err != nil {
			return nil, fmt.Errorf("unmarshal gossip payload: %w", err)
		}
	}

	return &payload, nil
}

func writeBusMsg(conn net.Conn, cmd, nodeID string, payload *GossipPayload) error {
	if payload == nil {
		msg := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(cmd), cmd, len(nodeID), nodeID)
		_, err := conn.Write([]byte(msg))
		return err
	}

	data, err := marshalGossipPayloadBinary(payload)
	if err != nil {
		// Fall back to JSON on gob error
		data, err = json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("marshal gossip payload: %w", err)
		}
		// Add JSON format marker
		jsonData := make([]byte, 1+len(data))
		jsonData[0] = gossipPayloadFormatJSON
		copy(jsonData[1:], data)
		data = jsonData
	}

	msg := fmt.Sprintf("*3\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
		len(cmd), cmd, len(nodeID), nodeID, len(data), string(data))
	_, err = conn.Write([]byte(msg))
	return err
}

func busAddrForPeer(peerAddr string) string {
	host, portStr, err := net.SplitHostPort(peerAddr)
	if err != nil {
		return peerAddr
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return peerAddr
	}
	return fmt.Sprintf("%s:%d", host, port+BusPortOffset)
}

func formatSlotsBrief(ranges []SlotRange) string {
	if len(ranges) == 0 {
		return ""
	}
	if len(ranges) == 1 {
		return fmt.Sprintf("%d-%d", ranges[0].Start, ranges[0].End)
	}
	// For multiple ranges, just show count
	return fmt.Sprintf("%d ranges", len(ranges))
}

// findNodeByAddr 在 Nodes 表中查找指定地址的节点。
// 用于 gossip 处理时跳过幽灵节点（不同 NodeID 但同 IP:Port）。
func findNodeByAddr(nodes map[string]*Node, addr string) *Node {
	for _, n := range nodes {
		if n.Addr == addr {
			return n
		}
	}
	return nil
}
