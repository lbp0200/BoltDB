package cluster

import (
	"bufio"
	"context"
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

	lastSave time.Time
}

func NewClusterBus(cluster *Cluster) *ClusterBus {
	ctx, cancel := context.WithCancel(context.Background())
	return &ClusterBus{
		cluster: cluster,
		peers:   make(map[string]*busPeer),
		ctx:     ctx,
		cancel:  cancel,
	}
}

func (b *ClusterBus) Addr() string { return b.addr }

func (b *ClusterBus) Start(host string, dataPort int) error {
	busPort := dataPort + BusPortOffset
	addr := fmt.Sprintf("%s:%d", host, busPort)

	var ln net.Listener
	var err error
	if dataPort == 0 {
		ln, err = net.Listen("tcp", host+":0")
	} else {
		ln, err = net.Listen("tcp", addr)
	}
	if err != nil {
		return fmt.Errorf("cluster bus: failed to listen on %s: %w", addr, err)
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
	conn, err := net.DialTimeout("tcp", peerAddr, busDialTimeout)
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

		var payload GossipPayload
		if len(args) >= 3 {
			_ = json.Unmarshal(args[2], &payload)
		}

		switch cmd {
		case "PING":
			b.handlePING(senderID)
			dirty := b.ApplyGossipPayloadFrom(senderID, &payload)
			// Respond with PONG + our payload
			respPayload := b.BuildGossipPayload()
			_ = writeBusMsg(conn, "PONG", b.cluster.Myself.ID, respPayload)
			b.saveIfDirty(dirty)
		case "PONG":
			b.handlePONG(senderID)
			dirty := b.ApplyGossipPayloadFrom(senderID, &payload)
			b.saveIfDirty(dirty)
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

func (b *ClusterBus) handlePING(senderID string) {
	b.cluster.mu.RLock()
	node, ok := b.cluster.Nodes[senderID]
	b.cluster.mu.RUnlock()
	if ok {
		node.UpdatePong()
	}
}

func (b *ClusterBus) handlePONG(senderID string) {
	b.cluster.mu.RLock()
	node, ok := b.cluster.Nodes[senderID]
	b.cluster.mu.RUnlock()
	if ok {
		node.UpdatePong()
	}
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
	if b.cluster.Myself != nil && len(b.cluster.Myself.Slots) > 0 {
		payload.Slots = formatSlotsBrief(b.cluster.Myself.Slots)
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
	for _, gi := range payload.Nodes {
		if gi.ID == b.cluster.Myself.ID {
			continue
		}
		if existing, ok := b.cluster.Nodes[gi.ID]; ok {
			existing.MergeGossipState(gi.Epoch, gi.PongRecv)
		} else {
			node := NewNode(gi.ID, gi.Addr)
			node.Flags = gi.Flags
			node.Epoch = gi.Epoch
			node.PingSent = gi.PingSent
			node.PongRecv = gi.PongRecv
			b.cluster.Nodes[gi.ID] = node
			logger.Logger.Info().Str("peer", gi.ID).Str("addr", gi.Addr).Msg("cluster gossip: learned new node")
		}
	}

	// Slot owner reconciliation: resolve conflicts by epoch
	for _, entry := range payload.SlotOwners {
		if entry.NodeID == b.cluster.Myself.ID {
			continue
		}
		for _, r := range entry.Ranges {
			for slot := r.Start; slot <= r.End; slot++ {
				currentOwner := b.cluster.Slots[slot]
				if currentOwner == nil || currentOwner.Epoch < entry.Epoch {
					if peerNode, ok := b.cluster.Nodes[entry.NodeID]; ok {
						if b.cluster.Slots[slot] != peerNode {
							b.cluster.Slots[slot] = peerNode
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

			// Threshold: majority (at least 2 for 3+ node cluster, at least 1 for 2-node)
			threshold := totalNodes/2 + 1
			if totalNodes <= 2 {
				threshold = 1 // single reporter is enough for 2-node cluster
			}

			if reportCount >= threshold {
				node := b.cluster.Nodes[pfailID]
				if node != nil && node.PromotePFailToFail() {
					logger.Logger.Warn().Str("node", pfailID).
						Int("reports", reportCount).
						Int("threshold", threshold).
						Msg("cluster gossip: FAIL promoted via multi-node agreement")
					dirty = true

					// Reassign failed node's slots to self
					for i := uint32(0); i < SlotCount; i++ {
						if b.cluster.Slots[i] != nil && b.cluster.Slots[i].ID == pfailID {
							b.cluster.Slots[i] = b.cluster.Myself
							b.cluster.Myself.AddSlotRange(i, i)
						}
					}
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

func writeBusMsg(conn net.Conn, cmd, nodeID string, payload *GossipPayload) error {
	if payload == nil {
		msg := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(cmd), cmd, len(nodeID), nodeID)
		_, err := conn.Write([]byte(msg))
		return err
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal gossip payload: %w", err)
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
