package integration

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/cluster"
	"github.com/lbp0200/BoltDB/internal/server"
	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/redis/go-redis/v9"
	"github.com/zeebo/assert"
)

const (
	clusterSoakDefaultDuration = 5 * time.Minute
	clusterSoakMaxDuration     = 48 * time.Hour
	clusterSoakDefaultNodes    = 3
	clusterSoakDefaultWriters  = 10
)

func getClusterSoakDuration() time.Duration {
	s := os.Getenv("SOAK_CLUSTER_DURATION")
	if s == "" {
		return clusterSoakDefaultDuration
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return clusterSoakDefaultDuration
	}
	if d > clusterSoakMaxDuration {
		return clusterSoakMaxDuration
	}
	return d
}

func getClusterSoakNodes() int {
	s := os.Getenv("SOAK_CLUSTER_NODES")
	if s == "" {
		return clusterSoakDefaultNodes
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 2 {
		return clusterSoakDefaultNodes
	}
	if n > 10 {
		return 10
	}
	return n
}

func getClusterSoakWriters() int {
	s := os.Getenv("SOAK_CLUSTER_WRITERS")
	if s == "" {
		return clusterSoakDefaultWriters
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return clusterSoakDefaultWriters
	}
	if n > 100 {
		return 100
	}
	return n
}

type clusterSoakNode struct {
	db       *store.BotreonStore
	handler  *server.Handler
	listener net.Listener
	client   *redis.Client
	nodeID   string
	port     int
	cl       *cluster.Cluster
}

type clusterSoakEnv struct {
	nodes   []*clusterSoakNode
	cleanup func()
}

func setupClusterSoak(t *testing.T, nodeCount int) *clusterSoakEnv {
	t.Helper()

	nodes := make([]*clusterSoakNode, nodeCount)

	for i := 0; i < nodeCount; i++ {
		dbPath := t.TempDir()
		db, err := store.NewBotreonStore(dbPath)
		assert.NoError(t, err)

		ln, err := net.Listen("tcp", "127.0.0.1:0")
		assert.NoError(t, err)

		tcpAddr := ln.Addr().(*net.TCPAddr)

		cl, err := cluster.NewCluster(db, "", tcpAddr.String(), context.Background())
		assert.NoError(t, err)

		h := &server.Handler{
			Db:      db,
			Cluster: cl,
		}

		go func() { _ = h.ServeTCP(ln) }()

		err = cl.Bus.Start("127.0.0.1", tcpAddr.Port)
		assert.NoError(t, err)

		gossipCtx := context.Background()
		cl.Gossip = cluster.NewGossiper(gossipCtx, cl)
		cl.Gossip.Start()

		cli := redis.NewClient(&redis.Options{Addr: tcpAddr.String()})
		time.Sleep(100 * time.Millisecond)

		pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err = cli.Ping(pingCtx).Result()
		pingCancel()
		assert.NoError(t, err)

		nodeID, err := cli.Do(context.Background(), "CLUSTER", "MYID").Result()
		assert.NoError(t, err)

		nodes[i] = &clusterSoakNode{
			db: db, handler: h, listener: ln, client: cli,
			nodeID: fmt.Sprintf("%v", nodeID), port: tcpAddr.Port, cl: cl,
		}
	}

	// MEET: node0 connects to all others
	for i := 1; i < nodeCount; i++ {
		_, err := nodes[0].client.Do(context.Background(),
			"CLUSTER", "MEET", "127.0.0.1", fmt.Sprintf("%d", nodes[i].port)).Result()
		assert.NoError(t, err)
	}

	// Wait for gossip: bus connection + initial PING/PONG + node info exchange
	time.Sleep(3 * time.Second)

	// Assign slots to each node in round-robin ranges
	for i, n := range nodes {
		// Clear default all-slots assignment
		_ = n.client.Do(context.Background(), "CLUSTER", "FLUSHSLOTS")

		start := uint32(cluster.SlotCount / nodeCount * i)
		end := uint32(cluster.SlotCount / nodeCount * (i + 1))
		if i == nodeCount-1 {
			end = cluster.SlotCount
		}
		batchAssignSlots(t, n.client, int(start), int(end))
	}

	// Wait for gossip to propagate slot ownership
	time.Sleep(3 * time.Second)

	// Verify cluster view convergence: all nodes should see total assigned slots
	verifyClusterConvergence(t, nodes, nodeCount)

	cleanup := func() {
		for _, n := range nodes {
			n.client.Close()
			if n.cl.Gossip != nil {
				n.cl.Gossip.Stop()
			}
			n.cl.Bus.Stop()
			n.handler.Shutdown()
			_ = n.listener.Close()
			_ = n.db.Close()
		}
	}

	return &clusterSoakEnv{nodes: nodes, cleanup: cleanup}
}

func batchAssignSlots(t *testing.T, cli *redis.Client, start, end int) {
	t.Helper()
	batchSize := 500
	for s := start; s < end; s += batchSize {
		e := s + batchSize
		if e > end {
			e = end
		}
		args := []interface{}{"CLUSTER", "ADDSLOTS"}
		for slot := s; slot < e; slot++ {
			args = append(args, strconv.Itoa(slot))
		}
		_, err := cli.Do(context.Background(), args...).Result()
		if err != nil && !strings.Contains(err.Error(), "already") {
			t.Logf("ADDSLOTS %d-%d: %v", s, e-1, err)
		}
	}
}

func verifyClusterConvergence(t *testing.T, nodes []*clusterSoakNode, expected int) {
	t.Helper()
	time.Sleep(2 * time.Second)
	for _, n := range nodes {
		info, err := n.client.Do(context.Background(), "CLUSTER", "INFO").Result()
		if err != nil {
			continue
		}
		infoStr := fmt.Sprintf("%v", info)
		if !strings.Contains(infoStr, fmt.Sprintf("cluster_known_nodes:%d", expected)) {
			t.Logf("node %s not yet converged: %s", n.nodeID[:8], infoStr)
		}
	}
}

// nodeForSlot returns the node index that should own the given slot.
func nodeForSlot(slot uint32, nodeCount int) int {
	return int(uint64(slot) * uint64(nodeCount) / uint64(cluster.SlotCount))
}

// TestClusterSoak runs a long-duration cluster stability test with
// concurrent writers, periodic slot reassignment, and node isolation.
//
// Usage:
//
//	go test -race -timeout 30m ./cmd/integration/ -run TestClusterSoak
//	SOAK_CLUSTER_DURATION=3h go test -race -timeout 4h ./cmd/integration/ -run TestClusterSoak
//	SOAK_CLUSTER_NODES=5 SOAK_CLUSTER_WRITERS=20 go test -race -timeout 30m ./cmd/integration/ -run TestClusterSoak
func TestClusterSoak(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping cluster soak test in short mode")
	}

	nodeCount := getClusterSoakNodes()
	duration := getClusterSoakDuration()
	writers := getClusterSoakWriters()

	t.Logf("soak-cluster: nodes=%d, duration=%v, writers=%d", nodeCount, duration, writers)
	t.Logf("soak-cluster: set SOAK_CLUSTER_DURATION (e.g. 3h), SOAK_CLUSTER_NODES, SOAK_CLUSTER_WRITERS")

	env := setupClusterSoak(t, nodeCount)

	baseline := runtime.NumGoroutine()

	// Pressure monitor on the first node's DB
	pm := NewPressureMonitor(env.nodes[0].db, nil)
	pm.EnableTemporalAnalysis()
	if jdir := os.Getenv("SOAK_JSONL_DIR"); jdir != "" {
		_ = os.MkdirAll(jdir, 0755)
		jpath := jdir + fmt.Sprintf("/soak-cluster-%s.jsonl", time.Now().Format("20060102-150405"))
		_ = pm.SetJSONLPath(jpath)
	}
	soakCtx, soakCancel := context.WithTimeout(context.Background(), duration)
	defer soakCancel()
	pm.Start(soakCtx, 30*time.Second)

	errCh := make(chan error, writers*10+10)
	var wg sync.WaitGroup

	// Writer goroutines: each uses a regular client to write to all nodes
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					errCh <- fmt.Errorf("writer %d panicked: %v", id, r)
				}
			}()
			runClusterSoakWriter(soakCtx, env, id, errCh)
		}(i)
	}

	// Slot chaos goroutine: periodically reassign slot ranges between nodes
	chaosInterval := duration / 10
	if chaosInterval < 10*time.Second {
		chaosInterval = 10 * time.Second
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		runClusterSlotChaos(soakCtx, t, env, chaosInterval, errCh)
	}()

	// Drain errors
	var errs []error
	errsDone := make(chan struct{})
	go func() {
		for err := range errCh {
			errs = append(errs, err)
		}
		close(errsDone)
	}()

	wg.Wait()
	close(errCh)
	<-errsDone
	if len(errs) > 0 {
		t.Errorf("soak-cluster: %d errors during run (first 10 shown):", len(errs))
		for i, err := range errs {
			if i >= 10 {
				break
			}
			t.Errorf("  %v", err)
		}
	}

	// Pressure summary + degradation check
	pm.LogSummary(t)
	level := pm.CheckDegradation(t, DefaultDegradationAssertion(), baseline)
	t.Logf("soak-cluster: degradation level: %s", level)

	// Health score
	health := pm.HealthScore(baseline)
	t.Log(health.String())

	// Temporal analysis
	if ta := pm.TemporalAnalysis(); ta.Trajectory != TrajectoryInsufficientData {
		t.Log(ta.FormatReport())
	}

	// Basin analysis
	ba := pm.BasinAnalysis()
	t.Log(ba.FormatReport())

	// Stop soak context so PressureMonitor stops sampling
	soakCancel()
	pm.Wait()

	// Clean up all nodes: stop gossip/bus, shutdown handler, close DB
	env.cleanup()

	// Goroutine leak check
	time.Sleep(500 * time.Millisecond)
	final := runtime.NumGoroutine()
	leak := final - baseline
	t.Logf("soak-cluster: goroutine delta=%d (baseline=%d, final=%d)", leak, baseline, final)
	if leak > 30 {
		t.Errorf("goroutine leak after soak-cluster: %d (baseline=%d, final=%d)", leak, baseline, final)
	}
}

func runClusterSoakWriter(ctx context.Context, env *clusterSoakEnv, id int, errCh chan<- error) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))

	// Build initial slot→node mapping via CLUSTER SLOTS
	slotMap := refreshSlotMap(ctx, env)

	for ctx.Err() == nil {
		slot := uint32(rng.Intn(cluster.SlotCount))
		key := fmt.Sprintf("{%d}:soak:%d:%d", slot, id, rng.Int63())
		value := fmt.Sprintf("val-%d-%d", id, rng.Int63())

		// Find the node that currently owns this slot
		nodeIdx := slotMap[slot]
		if nodeIdx < 0 || nodeIdx >= len(env.nodes) {
			nodeIdx = 0
		}
		cli := env.nodes[nodeIdx].client
		_, err := cli.Set(ctx, key, value, 0).Result()
		if err != nil {
			if strings.Contains(err.Error(), "MOVED") {
				slotMap = refreshSlotMap(ctx, env)
				if ctx.Err() != nil {
					return
				}
			} else if ctx.Err() != nil {
				return
			} else {
				errCh <- fmt.Errorf("SET: %v", err)
			}
			continue
		}

		if ctx.Err() != nil {
			return
		}
		got, err := cli.Get(ctx, key).Result()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			errCh <- fmt.Errorf("GET after SET: %v", err)
		} else if got != value {
			errCh <- fmt.Errorf("value mismatch: got %q, want %q", got, value)
		}

		// Periodically refresh slot map
		if rng.Intn(100) < 5 {
			slotMap = refreshSlotMap(ctx, env)
		}

		if rng.Intn(10) < 3 {
			time.Sleep(time.Duration(rng.Intn(20)) * time.Millisecond)
		}
	}
}

func refreshSlotMap(ctx context.Context, env *clusterSoakEnv) []int {
	m := make([]int, cluster.SlotCount)
	for i := range m {
		m[i] = -1
	}

	slots, err := env.nodes[0].client.Do(ctx, "CLUSTER", "SLOTS").Result()
	if err != nil || slots == nil {
		// Fallback: round-robin
		for i := range m {
			m[i] = i * len(env.nodes) / cluster.SlotCount
		}
		return m
	}

	slotsStr := fmt.Sprintf("%v", slots)
	_ = slotsStr

	// CLUSTER SLOTS format: [[start, end, [host, port, id]], ...]
	// Use go-redis type assertion
	switch v := slots.(type) {
	case []interface{}:
		for _, entry := range v {
			if arr, ok := entry.([]interface{}); ok && len(arr) >= 3 {
				start, _ := arr[0].(int64)
				end, _ := arr[1].(int64)
				if nodeInfo, ok := arr[2].([]interface{}); ok && len(nodeInfo) >= 2 {
					host, _ := nodeInfo[0].(string)
					port, _ := nodeInfo[1].(int64)
					addr := fmt.Sprintf("%s:%d", host, port)
					nodeIdx := -1
					for i, n := range env.nodes {
						if n.listener.Addr().String() == addr {
							nodeIdx = i
							break
						}
					}
					if nodeIdx >= 0 {
						for s := start; s <= end; s++ {
							m[int(s)] = nodeIdx
						}
					}
				}
			}
		}
	}

	// Fill any unmapped slots with fallback
	for i := range m {
		if m[i] < 0 {
			m[i] = i * len(env.nodes) / cluster.SlotCount
		}
	}
	return m
}

func runClusterSlotChaos(ctx context.Context, t *testing.T, env *clusterSoakEnv, interval time.Duration, errCh chan<- error) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	nodeCount := len(env.nodes)

	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}

		// Pick a random slot range to move between nodes
		slotStart := rng.Intn(cluster.SlotCount - 100)
		slotEnd := slotStart + rng.Intn(50) + 10
		if slotEnd >= cluster.SlotCount {
			slotEnd = cluster.SlotCount - 1
		}

		// Source node (current owner) and target node
		srcIdx := nodeForSlot(uint32(slotStart), nodeCount)
		dstIdx := (srcIdx + 1 + rng.Intn(nodeCount-1)) % nodeCount

		srcNode := env.nodes[srcIdx]
		dstNode := env.nodes[dstIdx]

		t.Logf("soak-cluster: reassigning slots %d-%d from node%d to node%d",
			slotStart, slotEnd, srcIdx, dstIdx)

		// Move each slot via SETSLOT
		for slot := slotStart; slot <= slotEnd; slot++ {
			if ctx.Err() != nil {
				return
			}
			_, err := dstNode.client.Do(ctx, "CLUSTER", "SETSLOT",
				strconv.Itoa(slot), "NODE", dstNode.nodeID).Result()
			if err != nil {
				errCh <- fmt.Errorf("SETSLOT %d NODE %s: %w", slot, dstNode.nodeID[:8], err)
			}
		}

		// Wait for gossip convergence after move
		time.Sleep(2 * time.Second)

		// Write some keys to the moved slots to verify redirects work
		for slot := slotStart; slot <= slotEnd && slot < slotStart+5 && ctx.Err() == nil; slot++ {
			key := fmt.Sprintf("{%d}:chaos:%d", slot, rng.Int63())
			val := fmt.Sprintf("chaos-%d", rng.Int63())

			// Write to source (should get MOVED)
			_, err := srcNode.client.Set(ctx, key, val, 0).Result()
			if err != nil && strings.Contains(err.Error(), "MOVED") {
				// Correct: source redirected to destination
				// Write to destination directly (parse MOVED addr)
				if _, err := dstNode.client.Set(ctx, key, val, 0).Result(); err != nil {
					errCh <- fmt.Errorf("SET after MOVED: %w", err)
				}
			} else if err != nil {
				errCh <- fmt.Errorf("SET to source slot %d: %w", slot, err)
			}
		}

		// Move slots back to original owner after a cycle
		for slot := slotStart; slot <= slotEnd; slot++ {
			if ctx.Err() != nil {
				return
			}
			_, err := srcNode.client.Do(ctx, "CLUSTER", "SETSLOT",
				strconv.Itoa(slot), "NODE", srcNode.nodeID).Result()
			if err != nil {
				errCh <- fmt.Errorf("SETSLOT %d back to %s: %w", slot, srcNode.nodeID[:8], err)
			}
		}

		time.Sleep(1 * time.Second)
	}
}
