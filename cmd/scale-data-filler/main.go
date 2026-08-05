// scale-data-filler — 快速填充 BoltDB Cluster 数据用于规模化验证
//
// Usage:
//
//	go run cmd/scale-data-filler/main.go \
//	   --nodes 10.1.2.16:6337,10.1.2.16:6338,10.1.2.16:6339 \
//	   --size 10GB --value-size 1024 --concurrency 20
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

	// Create cluster client
	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:        nodes,
		PoolSize:     *concurrency,
		MinIdleConns: *concurrency / 2,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	})

	// Verify cluster health
	err := rdb.Ping(context.Background()).Err()
	if err != nil {
		log.Fatalf("cluster ping failed: %v", err)
	}
	log.Printf("Cluster healthy (%d nodes)", len(nodes))

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

	// Worker pool
	var wg sync.WaitGroup
	sem := make(chan struct{}, *concurrency)
	for i := 0; i < totalKeys; i += batchSize {
		sem <- struct{}{}
		wg.Add(1)
		go func(keyStart int) {
			defer wg.Done()
			defer func() { <-sem }()

			pipe := rdb.Pipeline()
			end := keyStart + batchSize
			if end > totalKeys {
				end = totalKeys
			}
			for j := keyStart; j < end; j++ {
				key := fmt.Sprintf("scale:k:%012d", j)
				pipe.Set(context.Background(), key, value, 0)
			}
			_, err := pipe.Exec(context.Background())
			if err != nil && !strings.Contains(err.Error(), "MOVED") &&
				!strings.Contains(err.Error(), "ASK") {
				// Pipeline failed (e.g. i/o timeout): retry each key
				// individually so the completed counter reflects reality
				// instead of unconditionally counting the whole batch.
				for j := keyStart; j < end; j++ {
					key := fmt.Sprintf("scale:k:%012d", j)
					if e := rdb.Set(context.Background(), key, value, 0).Err(); e != nil {
						errors.Add(1)
						if errors.Load() <= 10 {
							log.Printf("  Write error: %v", e)
						}
					} else {
						completed.Add(1)
					}
				}
			} else {
				completed.Add(int64(end - keyStart))
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
		n, err := c.DBSize(context.Background()).Result()
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
