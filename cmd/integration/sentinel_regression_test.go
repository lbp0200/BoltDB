package integration

import (
	"context"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/backup"
	"github.com/lbp0200/BoltDB/internal/replication"
	"github.com/lbp0200/BoltDB/internal/sentinel"
	"github.com/lbp0200/BoltDB/internal/server"
	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/redis/go-redis/v9"
	"github.com/zeebo/assert"
)

// testBoltNode holds a single boltDB server + replication manager for test use.
type testBoltNode struct {
	DB       *store.BotreonStore
	Handler  *server.Handler
	Client   *redis.Client
	Addr     string
	listener net.Listener
	replMgr  *replication.ReplicationManager
	stopCh   chan struct{}
}

func startBoltNode(t *testing.T) *testBoltNode {
	t.Helper()

	dbPath := t.TempDir()
	db, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)
	assert.NoError(t, db.NextStartup())

	replMgr := replication.NewReplicationManager(db)
	backupMgr := backup.NewBackupManager(db, dbPath+"/backup")
	pubsubMgr := store.NewPubSubManager()

	handler := &server.Handler{
		Db:          db,
		Replication: replMgr,
		Backup:      backupMgr,
		PubSub:      pubsubMgr,
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)

	stopCh := make(chan struct{})
	go func() {
		_ = handler.ServeTCP(listener)
		close(stopCh)
	}()

	addr := listener.Addr().String()
	client := redis.NewClient(&redis.Options{Addr: addr, DialTimeout: 5 * time.Second})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	assert.NoError(t, client.Ping(ctx).Err())

	return &testBoltNode{
		DB:       db,
		Handler:  handler,
		Client:   client,
		Addr:     addr,
		listener: listener,
		replMgr:  replMgr,
		stopCh:   stopCh,
	}
}

func (n *testBoltNode) Close() {
	n.Handler.Shutdown()
	n.listener.Close()
	n.Client.Close()
	if n.replMgr != nil {
		n.replMgr.Stop()
	}
	_ = n.DB.CloseWithTimeout(store.CloseTimeout)
}

func (n *testBoltNode) Kill() {
	// stop replication first so slave replication goroutines exit before Shutdown
	if n.replMgr != nil {
		n.replMgr.Stop()
	}
	n.listener.Close()
	n.Handler.Shutdown()
	n.Client.Close()
}

func (n *testBoltNode) MakeSlave(masterAddr string) error {
	return replication.StartSlaveReplication(n.replMgr, n.DB, masterAddr)
}

// partitionProxy is a TCP forwarder that can simulate network partitions.
type partitionProxy struct {
	listener net.Listener
	target   string
	active   atomic.Bool
	conns    sync.Map
	stopCh   chan struct{}
	stopped  atomic.Bool
}

func newPartitionProxy(target string) (*partitionProxy, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &partitionProxy{
		listener: listener,
		target:   target,
		stopCh:   make(chan struct{}),
	}
	p.active.Store(true)
	go p.acceptLoop()
	return p, nil
}

func (p *partitionProxy) Addr() string {
	return p.listener.Addr().String()
}

func (p *partitionProxy) Stop() {
	p.stopped.Store(true)
	p.active.Store(false)
	p.conns.Range(func(key, value any) bool {
		if c, ok := value.(net.Conn); ok {
			_ = c.Close()
		}
		return true
	})
	_ = p.listener.Close()
}

func (p *partitionProxy) Isolate() {
	p.active.Store(false)
	p.conns.Range(func(key, value any) bool {
		if c, ok := value.(net.Conn); ok {
			_ = c.Close()
		}
		return true
	})
	if p.listener != nil {
		_ = p.listener.Close()
	}
}

func (p *partitionProxy) Heal() {
	p.active.Store(true)
}

func (p *partitionProxy) acceptLoop() {
	for {
		conn, err := p.listener.Accept()
		if err != nil {
			return
		}
		go p.handleConn(conn)
	}
}

func (p *partitionProxy) handleConn(client net.Conn) {
	if p.stopped.Load() || !p.active.Load() {
		_ = client.Close()
		return
	}
	target, err := net.DialTimeout("tcp", p.target, 3*time.Second)
	if err != nil {
		_ = client.Close()
		return
	}
	p.conns.Store(client.RemoteAddr().String(), client)
	p.conns.Store(target.RemoteAddr().String(), target)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = io.Copy(target, client)
	}()
	go func() {
		defer wg.Done()
		_, _ = io.Copy(client, target)
	}()
	wg.Wait()
	_ = client.Close()
	_ = target.Close()
}

// ---------------------------------------------------------------------------
// Failover Convergence Metrics
// ---------------------------------------------------------------------------

func TestSentinelFailoverConvergence(t *testing.T) {
	skipHeavyIntegrationInShort(t)
	master := startBoltNode(t)
	defer master.Close()

	promotee := startBoltNode(t)
	defer promotee.Close()

	downAfter := 2 * time.Second
	quorum := 1
	s := sentinel.NewSentinel(quorum, downAfter)
	err := s.AddMaster("mymaster", master.Addr, quorum)
	assert.NoError(t, err)

	slaveInst := sentinel.NewSlaveInstance("slave-1", promotee.Addr)
	slaveInst.State = "online"
	slaveInst.Offset = 1000
	s.GetMaster("mymaster").AddSlave(slaveInst)

	s.Start()
	defer s.Stop()

	// wait for at least 2 successful pings before killing
	time.Sleep(3 * time.Second)

	killTime := time.Now()
	master.Kill()

	pollStart := time.Now()
	for time.Since(pollStart) < 15*time.Second {
		m := s.GetMaster("mymaster")
		if m.GetAddr() != master.Addr {
			t.Logf("Address changed to %s after %v", m.GetAddr(), time.Since(killTime))
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	totalDuration := time.Since(killTime)
	detectLat := s.Metrics.DetectionLatency("mymaster")
	electionDur := s.Metrics.ElectionDuration("mymaster")
	recoverDur := s.Metrics.RecoveryDuration("mymaster")

	t.Logf("=== Failover Convergence Metrics ===")
	t.Logf("Total failover time:     %v", totalDuration)
	t.Logf("Detection latency:       %v  (master death -> ODown)", detectLat)
	t.Logf("Election duration:       %v  (ODown -> failover start)", electionDur)
	t.Logf("Recovery duration:       %v  (failover start -> new master)", recoverDur)
	t.Logf("Leader changes recorded: %d", s.Metrics.GetLeaderChanges())
	t.Logf("Detection count:         %d", s.Metrics.GetDetectionCount())
	t.Logf("SDown broadcasts:        %d", s.Metrics.GetSDownBroadcasts())
	t.Logf("Failover attempts:       %d", s.Metrics.GetFailoverStarted())
	t.Logf("Successful failovers:    %d", s.Metrics.GetSuccessfulFailovers())

	mAfter := s.GetMaster("mymaster")
	if mAfter.GetAddr() != promotee.Addr {
		t.Fatalf("failover did not update addr: got %q want %q (master was %q)",
			mAfter.GetAddr(), promotee.Addr, master.Addr)
	}
	if s.Metrics.GetSuccessfulFailovers() == 0 {
		t.Fatal("failover not recorded in metrics")
	}
	if detectLat <= 0 {
		t.Fatal("detection latency should be positive")
	}
	if detectLat >= 15*time.Second {
		t.Fatalf("detection too slow: %v", detectLat)
	}
	if s.Metrics.GetLeaderChanges() == 0 {
		t.Fatal("no leader change recorded")
	}

	ctx := context.Background()
	assert.NoError(t, promotee.Client.Set(ctx, "convergence:key", "ok", 0).Err())
	val, err := promotee.Client.Get(ctx, "convergence:key").Result()
	assert.NoError(t, err)
	assert.Equal(t, "ok", val)
}

func waitForSlaveSync(ctx context.Context, t *testing.T, slave, master *testBoltNode) {
	t.Helper()
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("timeout waiting for slave sync (masterOff=%d slaveOff=%d)",
				master.replMgr.GetMasterReplOffset(),
				slave.replMgr.GetSlaveReplOffset())
			return
		default:
		}
		masterOff := master.replMgr.GetMasterReplOffset()
		slaveOff := slave.replMgr.GetSlaveReplOffset()
		if slaveOff >= masterOff && masterOff > 0 {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// Gossip Stability Metrics
// ---------------------------------------------------------------------------

func TestSentinelGossipStability(t *testing.T) {
	skipHeavyIntegrationInShort(t)

	downAfter := 30 * time.Second
	quorum := 2

	s1 := sentinel.NewSentinel(quorum, downAfter)
	s2 := sentinel.NewSentinel(quorum, downAfter)
	s3 := sentinel.NewSentinel(quorum, downAfter)

	err := s1.AddMaster("mymaster", "127.0.0.1:19999", quorum)
	assert.NoError(t, err)
	err = s2.AddMaster("mymaster", "127.0.0.1:19999", quorum)
	assert.NoError(t, err)
	err = s3.AddMaster("mymaster", "127.0.0.1:19999", quorum)
	assert.NoError(t, err)

	cfg := sentinel.DefaultGossipConfig()
	cfg.HelloInterval = 500 * time.Millisecond
	cfg.PeerTimeout = 60 * time.Second

	gp1 := sentinel.NewGossipProtocol(s1, cfg)
	gp2 := sentinel.NewGossipProtocol(s2, cfg)
	gp3 := sentinel.NewGossipProtocol(s3, cfg)
	s1.Gossip = gp1
	s2.Gossip = gp2
	s3.Gossip = gp3

	err = gp1.Start()
	assert.NoError(t, err)
	defer gp1.Stop()
	err = gp2.Start()
	assert.NoError(t, err)
	defer gp2.Stop()
	err = gp3.Start()
	assert.NoError(t, err)
	defer gp3.Stop()

	time.Sleep(500 * time.Millisecond)

	p1 := "127.0.0.1:" + strconv.Itoa(gp1.GetPort())
	p2 := "127.0.0.1:" + strconv.Itoa(gp2.GetPort())
	p3 := "127.0.0.1:" + strconv.Itoa(gp3.GetPort())

	// full mesh: all three connected to each other
	assert.NoError(t, gp1.AddPeer(p2, "test-runid-1"))
	assert.NoError(t, gp1.AddPeer(p3, "test-runid-1"))
	assert.NoError(t, gp2.AddPeer(p1, "test-runid-2"))
	assert.NoError(t, gp2.AddPeer(p3, "test-runid-2"))
	assert.NoError(t, gp3.AddPeer(p1, "test-runid-3"))
	assert.NoError(t, gp3.AddPeer(p2, "test-runid-3"))

	time.Sleep(2 * time.Second)

	t.Logf("Gossip peers: s1=%d s2=%d s3=%d",
		gp1.GetPeersCount(), gp2.GetPeersCount(), gp3.GetPeersCount())
	assert.True(t, gp1.GetPeersCount() >= 1)
	assert.True(t, gp2.GetPeersCount() >= 2)
	assert.True(t, gp3.GetPeersCount() >= 1)

	// measure gossip propagation: s1 broadcasts SDOWN, measure arrival at s2 and s3
	s2Before := s2.GetMaster("mymaster").GetSdownCount()
	s3Before := s3.GetMaster("mymaster").GetSdownCount()

	broadcastTime := time.Now()
	gp1.BroadcastSdown("mymaster", 1)

	s2Arrival := waitForSdownIncrement(t, s2, "mymaster", s2Before, 8*time.Second)
	s3Arrival := waitForSdownIncrement(t, s3, "mymaster", s3Before, 10*time.Second)

	s2Propagation := s2Arrival.Sub(broadcastTime)
	s3Propagation := s3Arrival.Sub(broadcastTime)

	t.Logf("=== Gossip Stability Metrics ===")
	t.Logf("S1->S2 propagation:  %v  (direct)", s2Propagation)
	t.Logf("S1->S3 propagation:  %v  (direct)", s3Propagation)

	if s2Propagation >= 8*time.Second {
		t.Fatalf("S1->S2 gossip propagation too slow: %v", s2Propagation)
	}
	if s3Propagation >= 8*time.Second {
		t.Fatalf("S1->S3 gossip propagation too slow: %v", s3Propagation)
	}

	// node view divergence: after gossip, all sentinels should have sdownCount>0
	s1Count := s1.GetMaster("mymaster").GetSdownCount()
	s2Count := s2.GetMaster("mymaster").GetSdownCount()
	s3Count := s3.GetMaster("mymaster").GetSdownCount()

	t.Logf("Node view divergence:")
	t.Logf("  S1 sdownCount: %d", s1Count)
	t.Logf("  S2 sdownCount: %d", s2Count)
	t.Logf("  S3 sdownCount: %d", s3Count)

	assert.True(t, s2Count > 0)
	assert.True(t, s3Count > 0)
}

func waitForSdownIncrement(t *testing.T, s *sentinel.Sentinel, masterName string, beforeCount int, timeout time.Duration) time.Time {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		count := s.GetMaster(masterName).GetSdownCount()
		if count > beforeCount {
			return time.Now()
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for sdown increment on %s (before=%d)", masterName, beforeCount)
	return time.Time{}
}

// ---------------------------------------------------------------------------
// Split-Brain Regression
// ---------------------------------------------------------------------------

func TestSentinelSplitBrainRegression(t *testing.T) {
	skipHeavyIntegrationInShort(t)
	master := startBoltNode(t)
	defer master.Close()

	// Use proxy that rejects connections when isolated (close before Accept completes)
	proxy, err := newPartitionProxy(master.Addr)
	assert.NoError(t, err)
	defer proxy.Stop()

	downAfter := 3 * time.Second

	// sentinel A monitors via proxy, sentinel B monitors directly
	sA := sentinel.NewSentinel(1, downAfter)
	err = sA.AddMaster("mymaster", proxy.Addr(), 1)
	assert.NoError(t, err)

	sB := sentinel.NewSentinel(1, downAfter)
	err = sB.AddMaster("mymaster", master.Addr, 1)
	assert.NoError(t, err)

	// gossip for sentinel communication
	cfg := sentinel.DefaultGossipConfig()
	cfg.HelloInterval = 500 * time.Millisecond
	gpA := sentinel.NewGossipProtocol(sA, cfg)
	gpB := sentinel.NewGossipProtocol(sB, cfg)
	sA.Gossip = gpA
	sB.Gossip = gpB

	err = gpA.Start()
	assert.NoError(t, err)
	defer gpA.Stop()
	err = gpB.Start()
	assert.NoError(t, err)
	defer gpB.Stop()

	time.Sleep(500 * time.Millisecond)
	pA := "127.0.0.1:" + strconv.Itoa(gpA.GetPort())
	pB := "127.0.0.1:" + strconv.Itoa(gpB.GetPort())
	assert.NoError(t, gpA.AddPeer(pB, "sentinel-A"))
	assert.NoError(t, gpB.AddPeer(pA, "sentinel-B"))
	time.Sleep(2 * time.Second)

	sA.Start()
	defer sA.Stop()
	sB.Start()
	defer sB.Stop()

	time.Sleep(3 * time.Second)

	assert.Equal(t, "ok", sA.GetMaster("mymaster").GetState())
	assert.Equal(t, "ok", sB.GetMaster("mymaster").GetState())

	// Partition: stop the proxy listener so sentinel A gets connection refused
	t.Log("Partition: stopping proxy listener")
	proxy.Stop()
	time.Sleep(downAfter + 2*time.Second)

	stateA := sA.GetMaster("mymaster").GetState()
	stateBState := sB.GetMaster("mymaster").GetState()
	t.Logf("During partition - sentinel A state: %s", stateA)
	t.Logf("During partition - sentinel B state: %s", stateBState)

	assert.Equal(t, "ok", stateBState)
	if stateA == "ok" {
		t.Fatalf("sentinel A should have detected partition")
	}

	t.Logf("Partition detection: sentinel A saw %s, sentinel B saw %s", stateA, stateBState)

	t.Logf("=== Split-Brain Metrics ===")
	t.Logf("Metrics - S1 detection count:  %d", sA.Metrics.GetDetectionCount())
	t.Logf("Metrics - S1 ODown reached:    %d", sA.Metrics.GetODownReached())
	t.Logf("Metrics - S1 failover started: %d", sA.Metrics.GetFailoverStarted())
	t.Logf("Metrics - S1 failover success: %d", sA.Metrics.GetSuccessfulFailovers())
	t.Logf("Metrics - S2 detection count:  %d", sB.Metrics.GetDetectionCount())
	t.Logf("Metrics - S2 ODown reached:    %d", sB.Metrics.GetODownReached())
	t.Logf("Metrics - S2 failover started: %d", sB.Metrics.GetFailoverStarted())

	// Isolated sentinel should have detected the partition
	assert.True(t, sA.Metrics.GetDetectionCount() > 0)
	// Healthy sentinel should not have detected anything
	assert.Equal(t, int64(0), sB.Metrics.GetDetectionCount())

	t.Log("PASS: Sentinel A detected partition, B stayed healthy")
}

// ---------------------------------------------------------------------------
// False Positive Fail Detection Test
// ---------------------------------------------------------------------------

func TestSentinelFalsePositiveDetection(t *testing.T) {
	skipHeavyIntegrationInShort(t)
	master := startBoltNode(t)
	defer master.Close()

	downAfter := 3 * time.Second
	s := sentinel.NewSentinel(1, downAfter)
	err := s.AddMaster("mymaster", master.Addr, 1)
	assert.NoError(t, err)

	s.Start()
	defer s.Stop()

	time.Sleep(4 * time.Second)

	state := s.GetMaster("mymaster").GetState()
	if state != "ok" {
		t.Fatalf("master should stay 'ok' when healthy (state=%s)", state)
	}
	if s.Metrics.GetDetectionCount() != 0 {
		t.Fatalf("detected false positive sdown: %d detections for healthy master",
			s.Metrics.GetDetectionCount())
	}
}
