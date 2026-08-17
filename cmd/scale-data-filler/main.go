// scale-data-filler — 快速填充 BoltDB Cluster 数据用于规模化验证
//
// Usage:
//
//	go run cmd/scale-data-filler/main.go \
//	   --nodes 10.1.2.16:6337,10.1.2.16:6338,10.1.2.16:6339 \
//	   --size 10GB --value-size 1024 --concurrency 20
//
// 集群模式说明：启动时先拉取 CLUSTER SLOTS 得到槽位 → 节点映射，然后按槽
// 位归属把每个 batch 的 key 分组，每组用普通 client（非 ClusterClient）
// pipeline 发往对应节点。原因：go-redis ClusterClient 的 Pipeline() 跨槽
// key 时 Exec 会整批失败（任一节点错误毒化整批），回退逐 key Set 只有
// ~10 keys/s；按槽分组后每组 pipeline 的 key 都属于同一节点，无跨槽问题。
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// slotOwner 描述 CLUSTER SLOTS 中一个槽位范围的归属节点。
type slotOwner struct {
	start, end int
	addr       string
}

func main() {
	nodesFlag := flag.String("nodes", "10.1.2.16:6337,10.1.2.16:6338,10.1.2.16:6339", "comma-separated node addrs")
	sizeFlag := flag.String("size", "1GB", "target data size (e.g. 1GB, 10GB, 100GB)")
	valueSize := flag.Int("value-size", 1024, "value size in bytes")
	concurrency := flag.Int("concurrency", 20, "number of concurrent workers per node")
	dryRun := flag.Bool("dry-run", false, "print estimated key count and exit")
	flag.Parse()

	nodes := strings.Split(*nodesFlag, ",")
	if len(nodes) == 0 {
		log.Fatal("at least one node required")
	}

	// Parse size
	sizeBytes := parseSize(*sizeFlag)
	if *dryRun {
		keyOverhead := 30 // key prefix + separator + random digits
		totalKeys := sizeBytes / int64(*valueSize+keyOverhead)
		fmt.Printf("Size: %s (%d bytes)\n", *sizeFlag, sizeBytes)
		fmt.Printf("Value size: %d bytes\n", *valueSize)
		fmt.Printf("Key overhead: ~30 bytes\n")
		fmt.Printf("Concurrency: %d per node (%d total)\n", *concurrency, *concurrency*len(nodes))
		fmt.Printf("Estimated total keys: %d\n", totalKeys)
		fmt.Printf("Keys per node: ~%d\n", totalKeys/int64(len(nodes)))
		return
	}

	ctx := context.Background()

	// Create cluster client (used for Ping and measureGET)
	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:        nodes,
		PoolSize:     *concurrency,
		MinIdleConns: *concurrency / 2,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	})
	defer func() { _ = rdb.Close() }()

	// Verify cluster health
	err := rdb.Ping(ctx).Err()
	if err != nil {
		log.Fatalf("cluster ping failed: %v", err)
	}
	log.Printf("Cluster healthy (%d nodes)", len(nodes))

	// Fetch CLUSTER SLOTS: slot ranges → node addr. This must be done before
	// writing so each key can be routed to the node owning its slot (see file
	// header for why ClusterClient pipeline is not used).
	slotOwners, err := fetchSlotOwners(ctx, nodes[0])
	if err != nil {
		log.Fatalf("failed to fetch CLUSTER SLOTS: %v", err)
	}
	if len(slotOwners) == 0 {
		log.Fatal("CLUSTER SLOTS returned no ranges — are slots assigned?")
	}
	log.Printf("Cluster slots: %d ranges across %d nodes", len(slotOwners), countOwnerAddrs(slotOwners))

	// One plain client per owner node: pipelines built from it only contain
	// keys owned by that node, so Exec never fails across slots.
	clients := make(map[string]*redis.Client, len(slotOwners))
	for _, o := range slotOwners {
		if _, ok := clients[o.addr]; ok {
			continue
		}
		clients[o.addr] = redis.NewClient(&redis.Options{
			Addr:         o.addr,
			PoolSize:     *concurrency,
			MinIdleConns: *concurrency / 2,
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 30 * time.Second,
		})
		defer func() { _ = clients[o.addr].Close() }()
	}

	// Calculate work
	keyOverhead := 30
	totalKeys := int(sizeBytes / int64(*valueSize+keyOverhead))

	// batchSize: cap each pipeline batch at ~16MB request body so large
	// values (e.g. 1MB) do not produce 1GB single requests that exceed the
	// client WriteTimeout / server processing capacity (observed as i/o
	// timeouts with DBSIZE unchanged and a misleading "Keys written").
	const maxBatchBytes = 16 << 20 // 16MB
	batchSize := maxBatchBytes / (*valueSize + keyOverhead)
	if batchSize < 1 {
		batchSize = 1
	}

	log.Printf("Target: %s = %d keys, %d bytes each", *sizeFlag, totalKeys, *valueSize)
	log.Printf("Starting fill with %d concurrent workers...", *concurrency)

	value := makeValue(*valueSize)

	var completed atomic.Int64
	var errors atomic.Int64
	start := time.Now()
	ticker := time.NewTicker(5 * time.Second)
	done := make(chan struct{})

	// Progress reporter
	go func() {
		for {
			select {
			case <-ticker.C:
				done := completed.Load()
				elapsed := time.Since(start).Seconds()
				rate := float64(done) / elapsed
				pct := float64(done) * 100 / float64(totalKeys)
				log.Printf("  %d/%d keys (%.1f%%) — %.0f keys/s, %d errors",
					done, totalKeys, pct, rate, errors.Load())
			case <-done:
				ticker.Stop()
				return
			}
		}
	}()

	// Worker pool: each batch is split by slot ownership and sent as one
	// pipeline per owner node (all keys in a group belong to that node).
	var wg sync.WaitGroup
	sem := make(chan struct{}, *concurrency)
	for i := 0; i < totalKeys; i += batchSize {
		sem <- struct{}{}
		wg.Add(1)
		go func(keyStart int) {
			defer wg.Done()
			defer func() { <-sem }()

			end := keyStart + batchSize
			if end > totalKeys {
				end = totalKeys
			}

			// Group keys by owning node.
			groups := make(map[string][]string)
			for j := keyStart; j < end; j++ {
				key := fmt.Sprintf("scale:k:%012d", j)
				addr := ownerAddr(slotOwners, keySlot(key))
				if addr == "" {
					errors.Add(1)
					continue
				}
				groups[addr] = append(groups[addr], key)
			}

			for addr, keys := range groups {
				client := clients[addr]
				pipe := client.Pipeline()
				for _, key := range keys {
					pipe.Set(ctx, key, value, 0)
				}
				if _, err := pipe.Exec(ctx); err != nil {
					// Pipeline failed (e.g. i/o timeout): retry each key
					// individually so the completed counter reflects reality
					// instead of unconditionally counting the whole batch.
					for _, key := range keys {
						if e := client.Set(ctx, key, value, 0).Err(); e != nil {
							errors.Add(1)
							if errors.Load() <= 10 {
								log.Printf("  Write error: %v", e)
							}
						} else {
							completed.Add(1)
						}
					}
				} else {
					completed.Add(int64(len(keys)))
				}
			}
		}(i)
	}

	wg.Wait()
	close(done)

	elapsed := time.Since(start)
	log.Printf("=== Fill complete ===")
	log.Printf("  Keys written: %d", completed.Load())
	log.Printf("  Errors: %d", errors.Load())
	log.Printf("  Elapsed: %s (%.0f keys/s)", elapsed, float64(completed.Load())/elapsed.Seconds())

	// Final DBSIZE
	var foundKeys int64
	for _, node := range nodes {
		c := redis.NewClient(&redis.Options{Addr: node, PoolSize: 1})
		n, err := c.DBSize(ctx).Result()
		if err == nil {
			foundKeys += n
		}
		_ = c.Close()
	}
	log.Printf("  Total DBSIZE (sum): %d", foundKeys)

	// Performance measurement
	log.Printf("=== Performance after fill ===")
	measureGET(rdb, foundKeys, *concurrency)
}

// fetchSlotOwners 通过任一节点拉取 CLUSTER SLOTS，返回每个槽位范围及其
// 主节点地址（Nodes[0] 为主节点）。
func fetchSlotOwners(ctx context.Context, addr string) ([]slotOwner, error) {
	client := redis.NewClient(&redis.Options{Addr: addr, PoolSize: 1})
	defer func() { _ = client.Close() }()

	slots, err := client.ClusterSlots(ctx).Result()
	if err != nil {
		return nil, err
	}
	owners := make([]slotOwner, 0, len(slots))
	for _, s := range slots {
		if len(s.Nodes) == 0 {
			continue
		}
		owners = append(owners, slotOwner{start: s.Start, end: s.End, addr: s.Nodes[0].Addr})
	}
	return owners, nil
}

// ownerAddr 返回拥有指定槽位的节点地址；未分配时返回空串。
func ownerAddr(owners []slotOwner, slot int) string {
	for _, o := range owners {
		if slot >= o.start && slot <= o.end {
			return o.addr
		}
	}
	return ""
}

// countOwnerAddrs 统计槽位范围涉及的独立节点数。
func countOwnerAddrs(owners []slotOwner) int {
	seen := make(map[string]struct{}, len(owners))
	for _, o := range owners {
		seen[o.addr] = struct{}{}
	}
	return len(seen)
}

// keySlot 计算 key 的槽位（CRC-16/XModem + {} hashtag 规则），
// 与 internal/cluster.Slot 的算法保持一致。
func keySlot(key string) int {
	start := strings.IndexByte(key, '{')
	if start == -1 {
		return int(crc16([]byte(key))) % 16384
	}
	end := strings.IndexByte(key[start+1:], '}')
	if end == -1 {
		return int(crc16([]byte(key))) % 16384
	}
	return int(crc16([]byte(key[start+1:start+1+end]))) % 16384
}

// crc16 implements CRC-16/XModem (polynomial 0x1021, initial value 0x0000).
// This matches Redis's CRC16 implementation for slot calculation.
func crc16(data []byte) uint16 {
	crc := uint16(0)
	for _, b := range data {
		crc ^= uint16(b) << 8
		for i := 0; i < 8; i++ {
			if crc&0x8000 != 0 {
				crc = (crc << 1) ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}

func measureGET(rdb *redis.ClusterClient, keyCount int64, concurrency int) {
	const samples = 10000
	start := time.Now()
	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency)

	for i := 0; i < samples; i++ {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			key := fmt.Sprintf("scale:k:%012d", rand.Int63n(keyCount))
			rdb.Get(context.Background(), key)
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	avgLatency := float64(elapsed.Microseconds()) / float64(samples)
	log.Printf("  GET %d samples: %s total, %.2f µs avg",
		samples, elapsed, avgLatency)
}

func makeValue(size int) string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, size)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return string(b)
}

func parseSize(s string) int64 {
	s = strings.ToUpper(strings.TrimSpace(s))
	multiplier := int64(1)
	if strings.HasSuffix(s, "KB") {
		multiplier = 1024
		s = strings.TrimSuffix(s, "KB")
	} else if strings.HasSuffix(s, "MB") {
		multiplier = 1024 * 1024
		s = strings.TrimSuffix(s, "MB")
	} else if strings.HasSuffix(s, "GB") {
		multiplier = 1024 * 1024 * 1024
		s = strings.TrimSuffix(s, "GB")
	} else if strings.HasSuffix(s, "TB") {
		multiplier = 1024 * 1024 * 1024 * 1024
		s = strings.TrimSuffix(s, "TB")
	} else if strings.HasSuffix(s, "B") {
		s = strings.TrimSuffix(s, "B")
	}
	v, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		log.Fatalf("invalid size: %s", s)
	}
	return v * multiplier
}
