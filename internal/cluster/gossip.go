package cluster

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/lbp0200/BoltDB/internal/logger"
)

const (
	pingPeriod       = 1 * time.Second
	gossipFanout     = 3 // nodes pinged per cycle
	failTimeout      = 5 * time.Second
	cleanupInterval  = 10 * time.Second
	staleNodeTimeout = 60 * time.Second
)

// Gossiper manages periodic PING/PONG exchange between cluster nodes.
type Gossiper struct {
	cluster *Cluster
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	started bool
}

// NewGossiper creates a new Gossiper for the given cluster.
// The ctx controls the gossip loop lifecycle; it should be derived from the
// server's root context so gossip stops when the server shuts down.
func NewGossiper(ctx context.Context, c *Cluster) *Gossiper {
	ctx, cancel := context.WithCancel(ctx)
	return &Gossiper{
		cluster: c,
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Start begins the gossip loop. Must be called after cluster is configured.
func (g *Gossiper) Start() {
	if g.started {
		return
	}
	g.started = true
	g.wg.Add(2)
	go g.gossipLoop()
	go g.cleanupLoop()
	logger.Logger.Info().Msg("cluster gossip started")
}

// Stop terminates the gossip loop.
func (g *Gossiper) Stop() {
	if !g.started {
		return
	}
	g.cancel()
	g.wg.Wait()
	g.started = false
	logger.Logger.Info().Msg("cluster gossip stopped")
}

// gossipLoop periodically sends PINGs to random peers.
func (g *Gossiper) gossipLoop() {
	defer g.wg.Done()
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-g.ctx.Done():
			return
		case <-ticker.C:
			g.pingRandomPeers()
		}
	}
}

// cleanupLoop periodically removes stale nodes and checks failure detection.
func (g *Gossiper) cleanupLoop() {
	defer g.wg.Done()
	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-g.ctx.Done():
			return
		case <-ticker.C:
			g.checkFailures()
		}
	}
}

// pingRandomPeers sends PINGs to a random subset of peer nodes.
func (g *Gossiper) pingRandomPeers() {
	g.cluster.mu.RLock()
	peers := make([]*Node, 0, len(g.cluster.Nodes))
	for _, node := range g.cluster.Nodes {
		if node.ID != g.cluster.Myself.ID {
			peers = append(peers, node)
		}
	}
	g.cluster.mu.RUnlock()

	if len(peers) == 0 {
		return
	}

	// Ensure bus connections for known peers missing one
	if g.cluster.Bus != nil {
		for _, peer := range peers {
			if !g.cluster.Bus.HasPeer(peer.ID) {
				busAddr := busAddrForPeer(peer.Addr)
				if err := g.cluster.Bus.Connect(busAddr, peer.ID); err != nil {
					logger.Logger.Debug().
						Err(err).Str("peer", peer.ID).Str("bus", busAddr).
						Msg("cluster gossip: bus connect retry")
				}
			}
		}
	}

	// Shuffle and pick first gossipFanout
	rand.Shuffle(len(peers), func(i, j int) {
		peers[i], peers[j] = peers[j], peers[i]
	})

	count := gossipFanout
	if count > len(peers) {
		count = len(peers)
	}

	// If cluster bus is available and has peers, send real PING over TCP
	if g.cluster.Bus != nil && g.cluster.Bus.PeerCount() > 0 {
		g.cluster.Bus.SendPING()
		for _, peer := range peers[:count] {
			logger.Logger.Debug().
				Str("peer", peer.ID).
				Str("addr", peer.Addr).
				Msg("cluster gossip: PING via bus")
		}
		return
	}

	// Fallback: update local timestamps (unit tests, or bus not yet connected)
	for _, peer := range peers[:count] {
		peer.UpdatePing()
		logger.Logger.Debug().
			Str("peer", peer.ID).
			Str("addr", peer.Addr).
			Msg("cluster gossip: PING (local fallback)")
	}
}

// checkFailures marks nodes as failed if they haven't responded in time,
// and removes nodes that have been unreachable beyond the stale timeout.
func (g *Gossiper) checkFailures() {
	now := time.Now().UnixMilli()

	g.cluster.mu.Lock()
	defer g.cluster.mu.Unlock()

	for _, node := range g.cluster.Nodes {
		if node.ID == g.cluster.Myself.ID || node.HasFailFlag() {
			continue
		}

		pongRecv := node.GetPongRecv()
		var elapsed int64
		if pongRecv > 0 {
			elapsed = now - pongRecv
		} else if node.DiscoveredAt > 0 {
			elapsed = now - node.DiscoveredAt
		} else {
			continue
		}

		if elapsed > failTimeout.Milliseconds() {
			node.MarkPFail()
			logger.Logger.Warn().
				Str("node", node.ID).
				Str("addr", node.Addr).
				Dur("elapsed", time.Duration(elapsed)*time.Millisecond).
				Msg("cluster gossip: node marked PFAIL")
		}
	}

	// Remove nodes that have been gone too long
	for id, node := range g.cluster.Nodes {
		if node.ID == g.cluster.Myself.ID {
			continue
		}
		age := now - node.DiscoveredAt
		if age < staleNodeTimeout.Milliseconds() {
			continue
		}
		pongRecv := node.GetPongRecv()
		if pongRecv > 0 && now-pongRecv <= staleNodeTimeout.Milliseconds() {
			continue
		}
		logger.Logger.Warn().
			Str("node", id).
			Str("addr", node.Addr).
			Dur("age", time.Duration(age)*time.Millisecond).
			Msg("cluster gossip: removing stale node")
		delete(g.cluster.Nodes, id)
		for i := uint32(0); i < SlotCount; i++ {
			if g.cluster.Slots[i] != nil && g.cluster.Slots[i].ID == id {
				g.cluster.Slots[i] = g.cluster.Myself
				g.cluster.Myself.AddSlotRange(i, i)
			}
		}
	}
}
