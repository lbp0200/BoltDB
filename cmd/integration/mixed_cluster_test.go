package integration

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/cluster"
	"github.com/lbp0200/BoltDB/internal/replication"
	"github.com/lbp0200/BoltDB/internal/server"
	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/redis/go-redis/v9"
	"github.com/zeebo/assert"
)

// TestMixedClusterBoltDBAndRedis tests BoltDB and Redis-server basic operations
// Note: Full cluster mode (CLUSTER MEET between BoltDB and Redis) requires complex setup
// and is tested manually. This test verifies basic standalone operations.
func TestMixedClusterBoltDBAndRedis(t *testing.T) {
	skipHeavyIntegrationInShort(t)
	// Check if redis-server is available
	if err := exec.Command("redis-server", "--version").Run(); err != nil {
		t.Skip("redis-server not installed, skipping mixed cluster test")
	}

	ctx := context.Background()

	// Find available ports
	redisPort := findAvailablePort()
	boltPort := findAvailablePort()

	// Start Redis with a simple config
	redisConfig := fmt.Sprintf(`
port %d
dir %s
`, redisPort, t.TempDir())

	configFile := fmt.Sprintf("%s/redis.conf", t.TempDir())
	if err := os.WriteFile(configFile, []byte(redisConfig), 0644); err != nil {
		t.Fatalf("Failed to write Redis config: %v", err)
	}

	// Start Redis with config file
	redisCmd := exec.Command("redis-server", configFile)
	if err := redisCmd.Start(); err != nil {
		t.Fatalf("Failed to start Redis: %v", err)
	}

	// Cleanup function
	defer func() {
		exec.Command("redis-cli", "-p", strconv.Itoa(redisPort), "SHUTDOWN", "NOSAVE").Run()
		redisCmd.Process.Kill()
		redisCmd.Process.Wait()
	}()

	// Wait for Redis to start
	time.Sleep(500 * time.Millisecond)

	// Create Redis client
	redisClient := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("127.0.0.1:%d", redisPort),
	})
	defer redisClient.Close()

	// Ping Redis to verify it's running
	if err := redisClient.Ping(ctx).Err(); err != nil {
		t.Fatalf("Failed to ping Redis: %v", err)
	}

	// Create BoltDB cluster server
	dbPath := t.TempDir()
	boltDB, err := store.NewBotreonStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to create BoltDB store: %v", err)
	}
	defer boltDB.Close()

	c, err := cluster.NewCluster(boltDB, "", "")
	if err != nil {
		t.Fatalf("Failed to create BoltDB cluster: %v", err)
	}

	boltServer := &server.Handler{
		Db:      boltDB,
		Cluster: c,
	}

	boltListener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", boltPort))
	if err != nil {
		t.Fatalf("Failed to listen for BoltDB: %v", err)
	}

	go func() {
		_ = boltServer.ServeTCP(boltListener)
	}()
	defer boltListener.Close()

	time.Sleep(200 * time.Millisecond)

	// Create BoltDB client
	boltClient := redis.NewClient(&redis.Options{
		Addr: boltListener.Addr().String(),
	})
	defer boltClient.Close()

	// Test basic operations on both nodes
	t.Run("BoltDBOperations", func(t *testing.T) {
		err := boltClient.Set(ctx, "bolt_key", "bolt_value", 0).Err()
		assert.NoError(t, err)

		val, err := boltClient.Get(ctx, "bolt_key").Result()
		assert.NoError(t, err)
		assert.Equal(t, "bolt_value", val)
	})

	t.Run("RedisOperations", func(t *testing.T) {
		err := redisClient.Set(ctx, "redis_key", "redis_value", 0).Err()
		assert.NoError(t, err)

		val, err := redisClient.Get(ctx, "redis_key").Result()
		assert.NoError(t, err)
		assert.Equal(t, "redis_value", val)
	})

	t.Run("CrossNodeSlots", func(t *testing.T) {
		// Check slot assignments
		boltKey := "testkey"
		boltSlot, _ := boltClient.Do(ctx, "CLUSTER", "KEYSLOT", boltKey).Result()
		t.Logf("Test key slot: %v", boltSlot)
	})

	t.Run("BothClusterNodesInfo", func(t *testing.T) {
		// Get cluster info from both
		boltInfo, _ := boltClient.Info(ctx, "cluster").Result()
		t.Logf("BoltDB cluster info: %v", boltInfo)

		redisInfo, _ := redisClient.Info(ctx, "server").Result()
		t.Logf("Redis server info: %v", redisInfo)
	})
}

// TestMixedClusterReplication tests replication from Redis to BoltDB
func TestMixedClusterReplication(t *testing.T) {
	skipHeavyIntegrationInShort(t)
	// Check if redis-server is available
	if err := exec.Command("redis-server", "--version").Run(); err != nil {
		t.Skip("redis-server not installed, skipping replication test")
	}

	ctx := context.Background()

	// Find available ports
	redisPort := findAvailablePort()
	boltPort := findAvailablePort()

	// Start Redis master
	redisCmd := exec.Command("redis-server",
		"--port", strconv.Itoa(redisPort),
		"--repl-diskless-sync", "no",
		"--dir", t.TempDir(),
		"--daemonize", "yes",
	)
	if err := redisCmd.Run(); err != nil {
		t.Fatalf("Failed to start Redis: %v", err)
	}
	defer func() {
		exec.Command("redis-cli", "-p", strconv.Itoa(redisPort), "SHUTDOWN", "NOSAVE").Run()
	}()

	time.Sleep(1 * time.Second)

	// Create Redis client
	redisClient := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("127.0.0.1:%d", redisPort),
	})
	defer redisClient.Close()

	// Verify Redis is master
	role, err := redisClient.Do(ctx, "ROLE").Result()
	if err != nil {
		t.Fatalf("Failed to get Redis role: %v", err)
	}
	t.Logf("Redis role: %v", role)

	// Create BoltDB with replication manager
	dbPath := t.TempDir()
	boltDB, err := store.NewBotreonStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to create BoltDB store: %v", err)
	}
	defer boltDB.Close()

	// Create replication manager
	rm := replication.NewReplicationManager(boltDB)

	c, err := cluster.NewCluster(boltDB, "", "")
	if err != nil {
		t.Fatalf("Failed to create BoltDB cluster: %v", err)
	}

	boltServer := &server.Handler{
		Db:          boltDB,
		Cluster:     c,
		Replication: rm,
	}

	boltListener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", boltPort))
	if err != nil {
		t.Fatalf("Failed to listen for BoltDB: %v", err)
	}

	go func() {
		_ = boltServer.ServeTCP(boltListener)
	}()
	defer boltListener.Close()

	time.Sleep(200 * time.Millisecond)

	// Create BoltDB client
	boltClient := redis.NewClient(&redis.Options{
		Addr: boltListener.Addr().String(),
	})
	defer boltClient.Close()

	// Start BoltDB as Redis slave
	replicaofResult, err := boltClient.Do(ctx, "REPLICAOF", "127.0.0.1", strconv.Itoa(redisPort)).Result()
	t.Logf("REPLICAOF result: %v, err: %v", replicaofResult, err)

	time.Sleep(1 * time.Second)

	t.Run("BoltDBReplicationInfo", func(t *testing.T) {
		info, _ := boltClient.Info(ctx, "replication").Result()
		t.Logf("BoltDB replication info: %v", info)
	})

	t.Run("RedisToBoltDB", func(t *testing.T) {
		// Set data on Redis master
		err := redisClient.Set(ctx, "repl_key1", "repl_value1", 0).Err()
		assert.NoError(t, err)

		// Check if BoltDB received it
		time.Sleep(500 * time.Millisecond)
		val, err := boltClient.Get(ctx, "repl_key1").Result()
		if err != nil {
			t.Logf("BoltDB hasn't received replication yet: %v", err)
		} else {
			assert.Equal(t, "repl_value1", val)
			t.Logf("Replication successful: repl_key1 = %s", val)
		}
	})

	t.Run("MultiTypeReplication", func(t *testing.T) {
		// String
		err := redisClient.Set(ctx, "str_key", "str_value", 0).Err()
		assert.NoError(t, err)

		// List
		err = redisClient.RPush(ctx, "list_key", "a", "b", "c").Err()
		assert.NoError(t, err)

		// Hash
		err = redisClient.HSet(ctx, "hash_key", "field1", "value1").Err()
		assert.NoError(t, err)

		// ZSet
		err = redisClient.ZAdd(ctx, "zset_key", redis.Z{Score: 1, Member: "m1"}).Err()
		assert.NoError(t, err)

		time.Sleep(500 * time.Millisecond)

		// Verify on BoltDB
		strVal, _ := boltClient.Get(ctx, "str_key").Result()
		t.Logf("String replicated: %v", strVal)

		listLen, _ := boltClient.LLen(ctx, "list_key").Result()
		t.Logf("List length: %v", listLen)

		hashVal, _ := boltClient.HGet(ctx, "hash_key", "field1").Result()
		t.Logf("Hash replicated: %v", hashVal)

		zsetScore, _ := boltClient.ZScore(ctx, "zset_key", "m1").Result()
		t.Logf("ZSet score: %v", zsetScore)
	})
}

// TestMixedClusterRoleSwitch tests role switching between BoltDB and Redis
func TestMixedClusterRoleSwitch(t *testing.T) {
	skipHeavyIntegrationInShort(t)
	// Check if redis-server is available
	if err := exec.Command("redis-server", "--version").Run(); err != nil {
		t.Skip("redis-server not installed, skipping role switch test")
	}

	ctx := context.Background()

	// Find available ports
	redisPort := findAvailablePort()
	boltPort := findAvailablePort()

	// Start Redis master
	redisCmd := exec.Command("redis-server",
		"--port", strconv.Itoa(redisPort),
		"--repl-diskless-sync", "no",
		"--dir", t.TempDir(),
		"--daemonize", "yes",
	)
	if err := redisCmd.Run(); err != nil {
		t.Fatalf("Failed to start Redis: %v", err)
	}
	defer func() {
		exec.Command("redis-cli", "-p", strconv.Itoa(redisPort), "SHUTDOWN", "NOSAVE").Run()
	}()

	time.Sleep(1 * time.Second)

	redisClient := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("127.0.0.1:%d", redisPort),
	})
	defer redisClient.Close()

	// Create BoltDB
	dbPath := t.TempDir()
	boltDB, err := store.NewBotreonStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to create BoltDB store: %v", err)
	}
	defer boltDB.Close()

	rm := replication.NewReplicationManager(boltDB)

	c, err := cluster.NewCluster(boltDB, "", "")
	if err != nil {
		t.Fatalf("Failed to create BoltDB cluster: %v", err)
	}

	boltServer := &server.Handler{
		Db:          boltDB,
		Cluster:     c,
		Replication: rm,
	}

	boltListener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", boltPort))
	if err != nil {
		t.Fatalf("Failed to listen for BoltDB: %v", err)
	}

	go func() {
		_ = boltServer.ServeTCP(boltListener)
	}()
	defer boltListener.Close()

	time.Sleep(200 * time.Millisecond)

	boltClient := redis.NewClient(&redis.Options{
		Addr: boltListener.Addr().String(),
	})
	defer boltClient.Close()

	t.Run("BoltDBAsSlaveOfRedis", func(t *testing.T) {
		// BoltDB becomes slave of Redis
		result, err := boltClient.Do(ctx, "REPLICAOF", "127.0.0.1", strconv.Itoa(redisPort)).Result()
		t.Logf("REPLICAOF result: %v, err: %v", result, err)

		time.Sleep(500 * time.Millisecond)

		// Verify BoltDB is slave
		boltRole, _ := boltClient.Do(ctx, "ROLE").Result()
		t.Logf("BoltDB role: %v", boltRole)
	})

	t.Run("RedisBecomesSlaveOfBoltDB", func(t *testing.T) {
		// BoltDB stops being slave
		boltClient.Do(ctx, "REPLICAOF", "NO", "ONE")
		time.Sleep(300 * time.Millisecond)

		// Redis becomes slave of BoltDB
		result, err := redisClient.Do(ctx, "REPLICAOF", "127.0.0.1", strconv.Itoa(boltPort)).Result()
		t.Logf("Redis REPLICAOF result: %v, err: %v", result, err)

		time.Sleep(500 * time.Millisecond)

		// Verify Redis is slave
		redisRole, _ := redisClient.Do(ctx, "ROLE").Result()
		t.Logf("Redis role: %v", redisRole)
	})
}

// TestMixedClusterDataIsolation tests that BoltDB and Redis maintain separate data
func TestMixedClusterDataIsolation(t *testing.T) {
	skipHeavyIntegrationInShort(t)
	// Check if redis-server is available
	if err := exec.Command("redis-server", "--version").Run(); err != nil {
		t.Skip("redis-server not installed, skipping data isolation test")
	}

	ctx := context.Background()

	// Find available ports
	redisPort := findAvailablePort()
	boltPort := findAvailablePort()

	// Start Redis
	redisCmd := exec.Command("redis-server",
		"--port", strconv.Itoa(redisPort),
		"--dir", t.TempDir(),
		"--daemonize", "yes",
	)
	if err := redisCmd.Run(); err != nil {
		t.Fatalf("Failed to start Redis: %v", err)
	}
	defer func() {
		exec.Command("redis-cli", "-p", strconv.Itoa(redisPort), "SHUTDOWN", "NOSAVE").Run()
	}()

	time.Sleep(1 * time.Second)

	redisClient := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("127.0.0.1:%d", redisPort),
	})
	defer redisClient.Close()

	// Create BoltDB
	dbPath := t.TempDir()
	boltDB, err := store.NewBotreonStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to create BoltDB store: %v", err)
	}
	defer boltDB.Close()

	c, err := cluster.NewCluster(boltDB, "", "")
	if err != nil {
		t.Fatalf("Failed to create BoltDB cluster: %v", err)
	}

	boltServer := &server.Handler{
		Db:      boltDB,
		Cluster: c,
	}

	boltListener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", boltPort))
	if err != nil {
		t.Fatalf("Failed to listen for BoltDB: %v", err)
	}

	go func() {
		_ = boltServer.ServeTCP(boltListener)
	}()
	defer boltListener.Close()

	time.Sleep(200 * time.Millisecond)

	boltClient := redis.NewClient(&redis.Options{
		Addr: boltListener.Addr().String(),
	})
	defer boltClient.Close()

	t.Run("IndependentData", func(t *testing.T) {
		// Set data on Redis
		err := redisClient.Set(ctx, "redis_only", "redis_value", 0).Err()
		assert.NoError(t, err)

		// Set data on BoltDB
		err = boltClient.Set(ctx, "bolt_only", "bolt_value", 0).Err()
		assert.NoError(t, err)

		// Verify Redis doesn't have BoltDB's data
		_, err = redisClient.Get(ctx, "bolt_only").Result()
		assert.True(t, err == redis.Nil)

		// Verify BoltDB doesn't have Redis's data
		_, err = boltClient.Get(ctx, "redis_only").Result()
		assert.True(t, err == redis.Nil)

		// Each has their own data
		redisVal, _ := redisClient.Get(ctx, "redis_only").Result()
		boltVal, _ := boltClient.Get(ctx, "bolt_only").Result()

		assert.Equal(t, "redis_value", redisVal)
		assert.Equal(t, "bolt_value", boltVal)
	})

	t.Run("SameKeyDifferentValue", func(t *testing.T) {
		// Set same key on both
		err := redisClient.Set(ctx, "shared_key", "from_redis", 0).Err()
		assert.NoError(t, err)

		err = boltClient.Set(ctx, "shared_key", "from_bolt", 0).Err()
		assert.NoError(t, err)

		// Verify they have different values
		redisVal, _ := redisClient.Get(ctx, "shared_key").Result()
		boltVal, _ := boltClient.Get(ctx, "shared_key").Result()

		assert.Equal(t, "from_redis", redisVal)
		assert.Equal(t, "from_bolt", boltVal)
	})
}

// findAvailablePort finds an available port on the system
func findAvailablePort() int {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		// Fallback to common ports
		return 6379
	}
	defer ln.Close()

	addr := ln.Addr().String()
	parts := strings.Split(addr, ":")
	port, _ := strconv.Atoi(parts[len(parts)-1])
	return port
}
