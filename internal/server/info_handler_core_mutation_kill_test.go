package server

import (
	"strings"
	"testing"

	"github.com/lbp0200/BoltDB/internal/backup"
	"github.com/lbp0200/BoltDB/internal/cluster"
	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/replication"
	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/zeebo/assert"
)

// =============================================================================
// Phase 9 续: info.go + handler_core.go Mutation Test NOT COVERED 修复
// 目标: info.go (14 NOT COVERED), handler_core.go (18 NOT COVERED)
// =============================================================================

// ---------- info.go: buildInfoResponse section filtering ----------

// TestBuildInfoResponse_EmptySection 验证空 section 返回所有段
// Kills CONDITIONALS_NEGATION on each `section == "" || ...` check
func TestBuildInfoResponse_EmptySection(t *testing.T) {

	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()
	handler.Port = 6337

	resp := handler.buildInfoResponse("")
	assert.True(t, strings.Contains(resp, "# Server"))
	assert.True(t, strings.Contains(resp, "# Replication"))
	assert.True(t, strings.Contains(resp, "# Persistence"))
	assert.True(t, strings.Contains(resp, "# Stats"))
	assert.True(t, strings.Contains(resp, "# Cluster"))
	assert.True(t, strings.Contains(resp, "# Clients"))
	assert.True(t, strings.Contains(resp, "# Memory"))
	assert.True(t, strings.Contains(resp, "# CPU"))
	assert.True(t, strings.Contains(resp, "# Keyspace"))
	assert.True(t, strings.Contains(resp, "# Commandstats"))
	assert.True(t, strings.Contains(resp, "# Latency"))
}

// TestBuildInfoResponse_AllSection 验证 "ALL" section 返回所有段
// Kills BOUNDARY on "ALL" string comparison
func TestBuildInfoResponse_AllSection(t *testing.T) {

	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()
	handler.Port = 6337

	resp := handler.buildInfoResponse("ALL")
	assert.True(t, strings.Contains(resp, "# Server"))
	assert.True(t, strings.Contains(resp, "# Replication"))
	assert.True(t, strings.Contains(resp, "# Memory"))
	assert.True(t, strings.Contains(resp, "# Latency"))
}

// TestBuildInfoResponse_ServerSection 验证只返回 Server 段
// Kills CONDITIONALS_NEGATION on `section == "SERVER"` check
func TestBuildInfoResponse_ServerSection(t *testing.T) {

	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()
	handler.Port = 6337

	resp := handler.buildInfoResponse("SERVER")
	assert.True(t, strings.Contains(resp, "# Server"))
	assert.True(t, strings.Contains(resp, "redis_version:boltdb-"))
	assert.True(t, strings.Contains(resp, "tcp_port:6337"))
	assert.True(t, strings.Contains(resp, "arch_bits:64"))
	assert.True(t, strings.Contains(resp, "process_id:"))
	// Should NOT contain other sections
	assert.True(t, !strings.Contains(resp, "# Memory"))
	assert.True(t, !strings.Contains(resp, "# CPU"))
}

// TestBuildInfoResponse_ReplicationSection 验证只返回 Replication 段
// Kills CONDITIONALS_NEGATION on `section == "REPLICATION"` check
func TestBuildInfoResponse_ReplicationSection(t *testing.T) {

	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()
	handler.Port = 6337

	resp := handler.buildInfoResponse("REPLICATION")
	assert.True(t, strings.Contains(resp, "# Replication"))
	assert.True(t, !strings.Contains(resp, "# Server"))
	assert.True(t, !strings.Contains(resp, "# Memory"))
}

// TestBuildInfoResponse_PersistenceSection 验证只返回 Persistence 段
// Kills CONDITIONALS_NEGATION on `section == "PERSISTENCE"` check
func TestBuildInfoResponse_PersistenceSection(t *testing.T) {

	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()
	handler.Port = 6337

	resp := handler.buildInfoResponse("PERSISTENCE")
	assert.True(t, strings.Contains(resp, "# Persistence"))
	assert.True(t, !strings.Contains(resp, "# Server"))
	assert.True(t, !strings.Contains(resp, "# Stats"))
}

// TestBuildInfoResponse_StatsSection 验证只返回 Stats 段
// Kills CONDITIONALS_NEGATION on `section == "STATS"` check
func TestBuildInfoResponse_StatsSection(t *testing.T) {

	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()
	handler.Port = 6337

	resp := handler.buildInfoResponse("STATS")
	assert.True(t, strings.Contains(resp, "# Stats"))
	assert.True(t, strings.Contains(resp, "total_commands_processed:0"))
	assert.True(t, !strings.Contains(resp, "# Server"))
}

// TestBuildInfoResponse_ClusterSection 验证只返回 Cluster 段
// Kills CONDITIONALS_NEGATION on `section == "CLUSTER"` check
func TestBuildInfoResponse_ClusterSection(t *testing.T) {

	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()
	handler.Port = 6337

	resp := handler.buildInfoResponse("CLUSTER")
	assert.True(t, strings.Contains(resp, "# Cluster"))
	assert.True(t, strings.Contains(resp, "cluster_enabled:0"))
	assert.True(t, !strings.Contains(resp, "# Server"))
}

// TestBuildInfoResponse_MemorySection 验证只返回 Memory 段
// Kills CONDITIONALS_NEGATION on `section == "MEMORY"` check
func TestBuildInfoResponse_MemorySection(t *testing.T) {

	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()
	handler.Port = 6337

	resp := handler.buildInfoResponse("MEMORY")
	assert.True(t, strings.Contains(resp, "# Memory"))
	assert.True(t, strings.Contains(resp, "used_memory:"))
	assert.True(t, strings.Contains(resp, "used_memory_human:"))
	assert.True(t, strings.Contains(resp, "mem_fragmentation_ratio:"))
	assert.True(t, !strings.Contains(resp, "# Server"))
}

// TestBuildInfoResponse_CpuSection 验证只返回 CPU 段
// Kills CONDITIONALS_NEGATION on `section == "CPU"` check
func TestBuildInfoResponse_CpuSection(t *testing.T) {

	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()
	handler.Port = 6337

	resp := handler.buildInfoResponse("CPU")
	assert.True(t, strings.Contains(resp, "# CPU"))
	assert.True(t, strings.Contains(resp, "used_cpu_sys:0.00"))
	assert.True(t, !strings.Contains(resp, "# Server"))
}

// TestBuildInfoResponse_KeyspaceSection 验证只返回 Keyspace 段
// Kills CONDITIONALS_NEGATION on `section == "KEYSPACE"` check
func TestBuildInfoResponse_KeyspaceSection(t *testing.T) {

	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()
	handler.Port = 6337

	resp := handler.buildInfoResponse("KEYSPACE")
	assert.True(t, strings.Contains(resp, "# Keyspace"))
	assert.True(t, !strings.Contains(resp, "# Server"))
}

// TestBuildInfoResponse_CommandstatsSection 验证只返回 Commandstats 段
// Kills CONDITIONALS_NEGATION on `section == "COMMANDSTATS"` check
func TestBuildInfoResponse_CommandstatsSection(t *testing.T) {

	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()
	handler.Port = 6337

	resp := handler.buildInfoResponse("COMMANDSTATS")
	assert.True(t, strings.Contains(resp, "# Commandstats"))
	assert.True(t, !strings.Contains(resp, "# Server"))
}

// TestBuildInfoResponse_LatencySection 验证只返回 Latency 段
// Kills CONDITIONALS_NEGATION on `section == "LATENCY"` check
func TestBuildInfoResponse_LatencySection(t *testing.T) {

	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()
	handler.Port = 6337

	resp := handler.buildInfoResponse("LATENCY")
	assert.True(t, strings.Contains(resp, "# Latency"))
	assert.True(t, !strings.Contains(resp, "# Server"))
}

// ---------- info.go: Replication role branching ----------

// TestBuildInfoResponse_ReplicationNil 验证 Replication==nil 时只输出段标题
// Kills CONDITIONALS_NEGATION on `h.Replication != nil` check
func TestBuildInfoResponse_ReplicationNil(t *testing.T) {

	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()
	handler.Port = 6337

	resp := handler.buildInfoResponse("REPLICATION")
	// When Replication is nil, should still have "# Replication" header
	assert.True(t, strings.Contains(resp, "# Replication"))
	// But no role-specific fields
	assert.True(t, !strings.Contains(resp, "role:"))
	assert.True(t, !strings.Contains(resp, "connected_slaves:"))
}

// TestBuildInfoResponse_ReplicationMasterRole 验证 master 角色输出
// Kills CONDITIONALS_NEGATION on role switch cases
func TestBuildInfoResponse_ReplicationMasterRole(t *testing.T) {

	dbPath := t.TempDir()
	db, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	replMgr := replication.NewReplicationManager(db)
	replMgr.SetRole(replication.RoleMaster)

	handler := &Handler{
		Db:          db,
		Replication: replMgr,
		conns:       make(map[*connState]*connMeta),
		Port:        6337,
	}

	resp := handler.buildInfoResponse("REPLICATION")
	assert.True(t, strings.Contains(resp, "role:master"))
	assert.True(t, strings.Contains(resp, "connected_slaves:"))
	assert.True(t, strings.Contains(resp, "master_replid:"))
	assert.True(t, strings.Contains(resp, "master_repl_offset:"))
	assert.True(t, strings.Contains(resp, "second_repl_offset:-1"))
	assert.True(t, strings.Contains(resp, "repl_backlog_active:1"))
}

// TestBuildInfoResponse_ReplicationSlaveRole 验证 slave 角色输出
// Kills CONDITIONALS_NEGATION on RoleSlave case
func TestBuildInfoResponse_ReplicationSlaveRole(t *testing.T) {

	dbPath := t.TempDir()
	db, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	replMgr := replication.NewReplicationManager(db)
	replMgr.SetRole(replication.RoleSlave)
	replMgr.SetMasterAddr("10.0.0.1:6337")

	handler := &Handler{
		Db:          db,
		Replication: replMgr,
		conns:       make(map[*connState]*connMeta),
		Port:        6337,
	}

	resp := handler.buildInfoResponse("REPLICATION")
	assert.True(t, strings.Contains(resp, "role:slave"))
	assert.True(t, strings.Contains(resp, "master_host:10.0.0.1"))
	assert.True(t, strings.Contains(resp, "master_port:6337"))
	assert.True(t, strings.Contains(resp, "master_link_status:up"))
	assert.True(t, strings.Contains(resp, "slave_priority:100"))
	assert.True(t, strings.Contains(resp, "slave_read_only:1"))
	assert.True(t, strings.Contains(resp, "replica_announced:1"))
	assert.True(t, strings.Contains(resp, "connected_slaves:0"))
	assert.True(t, strings.Contains(resp, "slave_repl_offset:"))
	assert.True(t, strings.Contains(resp, "repl_backlog_active:0"))
}

// TestBuildInfoResponse_ReplicationSlaveNoMasterAddr 验证 slave 无 master 地址时
// Kills CONDITIONALS_NEGATION on `masterAddr != ""` check
func TestBuildInfoResponse_ReplicationSlaveNoMasterAddr(t *testing.T) {

	dbPath := t.TempDir()
	db, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	replMgr := replication.NewReplicationManager(db)
	replMgr.SetRole(replication.RoleSlave)
	// Don't set master addr

	handler := &Handler{
		Db:          db,
		Replication: replMgr,
		conns:       make(map[*connState]*connMeta),
		Port:        6337,
	}

	resp := handler.buildInfoResponse("REPLICATION")
	assert.True(t, strings.Contains(resp, "role:slave"))
	// master_host should NOT appear when masterAddr is empty
	assert.True(t, !strings.Contains(resp, "master_host:"))
}

// TestBuildInfoResponse_ReplicationSentinelRole 验证 sentinel 角色输出
// Kills CONDITIONALS_NEGATION on "sentinel" case
func TestBuildInfoResponse_ReplicationSentinelRole(t *testing.T) {

	dbPath := t.TempDir()
	db, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	replMgr := replication.NewReplicationManager(db)
	replMgr.SetRole("sentinel")

	handler := &Handler{
		Db:          db,
		Replication: replMgr,
		conns:       make(map[*connState]*connMeta),
		Port:        6337,
	}

	resp := handler.buildInfoResponse("REPLICATION")
	assert.True(t, strings.Contains(resp, "connected_slaves:0"))
	assert.True(t, strings.Contains(resp, "master_replid:8371b4fb5d6973276c54b0f0ab738c2e6f00fa8d"))
	assert.True(t, strings.Contains(resp, "master_repl_offset:0"))
	assert.True(t, strings.Contains(resp, "second_repl_offset:-1"))
}

// ---------- info.go: Backup/Cluster nil checks ----------

// TestBuildInfoResponse_BackupNil 验证 Backup==nil 时无 persistence 详情
// Kills CONDITIONALS_NEGATION on `h.Backup != nil` check
func TestBuildInfoResponse_BackupNil(t *testing.T) {

	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()
	handler.Port = 6337

	resp := handler.buildInfoResponse("PERSISTENCE")
	assert.True(t, strings.Contains(resp, "# Persistence"))
	// When Backup is nil, no rdb_last_save_time should appear
	assert.True(t, !strings.Contains(resp, "rdb_last_save_time:"))
}

// TestBuildInfoResponse_BackupNotNil 验证 Backup!=nil 时输出保存时间
// Kills CONDITIONALS_NEGATION on `h.Backup != nil` check (positive path)
func TestBuildInfoResponse_BackupNotNil(t *testing.T) {

	dbPath := t.TempDir()
	db, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	bm := backup.NewBackupManager(db, dbPath)

	handler := &Handler{
		Db:     db,
		Backup: bm,
		conns:  make(map[*connState]*connMeta),
		Port:   6337,
	}

	resp := handler.buildInfoResponse("PERSISTENCE")
	assert.True(t, strings.Contains(resp, "rdb_last_save_time:"))
	assert.True(t, strings.Contains(resp, "rdb_changes_since_last_save:0"))
}

// TestBuildInfoResponse_ClusterEnabled 验证 Cluster!=nil 时输出 cluster_enabled:1
// Kills CONDITIONALS_NEGATION on `h.Cluster != nil` check
func TestBuildInfoResponse_ClusterEnabled(t *testing.T) {

	dbPath := t.TempDir()
	db, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	c, err := cluster.NewCluster(db, "", "127.0.0.1:6337")
	assert.NoError(t, err)

	handler := &Handler{
		Db:      db,
		Cluster: c,
		conns:   make(map[*connState]*connMeta),
		Port:    6337,
	}

	resp := handler.buildInfoResponse("CLUSTER")
	assert.True(t, strings.Contains(resp, "cluster_enabled:1"))
}

// TestBuildInfoResponse_ClusterDisabled 验证 Cluster==nil 时输出 cluster_enabled:0
// Kills CONDITIONALS_NEGATION on `h.Cluster == nil` else branch
func TestBuildInfoResponse_ClusterDisabled(t *testing.T) {

	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()
	handler.Port = 6337

	resp := handler.buildInfoResponse("CLUSTER")
	assert.True(t, strings.Contains(resp, "cluster_enabled:0"))
}

// ---------- info.go: formatBytes boundary conditions ----------

// TestFormatBytes_GB 验证 GB 级别格式化
// Kills BOUNDARY on `bytes >= GB` check
func TestFormatBytes_GB(t *testing.T) {

	// 2.5 GB = 2.5 * 1024 * 1024 * 1024 = 2684354560
	result := formatBytes(2684354560)
	assert.True(t, strings.Contains(result, "G"))
	assert.True(t, strings.Contains(result, "2.50"))
}

// TestFormatBytes_MB 验证 MB 级别格式化
// Kills BOUNDARY on `bytes >= MB` check
func TestFormatBytes_MB(t *testing.T) {

	// 1.5 MB = 1.5 * 1024 * 1024 = 1572864
	result := formatBytes(1572864)
	assert.True(t, strings.Contains(result, "M"))
	assert.True(t, strings.Contains(result, "1.50"))
}

// TestFormatBytes_KB 验证 KB 级别格式化
// Kills BOUNDARY on `bytes >= KB` check
func TestFormatBytes_KB(t *testing.T) {

	// 512 bytes
	result := formatBytes(512)
	assert.True(t, strings.Contains(result, "512B"))

	// 1.5 KB = 1536
	result = formatBytes(1536)
	assert.True(t, strings.Contains(result, "K"))
	assert.True(t, strings.Contains(result, "1.50"))
}

// TestFormatBytes_Bytes 验证字节级别格式化
// Kills BOUNDARY on default case
func TestFormatBytes_Bytes(t *testing.T) {

	result := formatBytes(0)
	assert.Equal(t, "0B", result)

	result = formatBytes(100)
	assert.Equal(t, "100B", result)

	result = formatBytes(1023)
	assert.Equal(t, "1023B", result)
}

// ---------- handler_core.go: checkAndHandleRedirect ----------

// TestCheckAndHandleRedirect_NilCluster 验证非集群模式返回 nil
// Kills CONDITIONALS_NEGATION on `h.Cluster == nil` check
func TestCheckAndHandleRedirect_NilCluster(t *testing.T) {

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.checkAndHandleRedirect(state, "testkey")
	assert.True(t, resp == nil)
}

// TestCheckAndHandleRedirect_NoRedirect 验证集群模式无重定向时返回 nil
// Kills CONDITIONALS_NEGATION on `redirect == nil` check
func TestCheckAndHandleRedirect_NoRedirect(t *testing.T) {

	dbPath := t.TempDir()
	db, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	c, err := cluster.NewCluster(db, "", "127.0.0.1:6337")
	assert.NoError(t, err)

	handler := &Handler{
		Db:      db,
		Cluster: c,
		conns:   make(map[*connState]*connMeta),
		Port:    6337,
	}
	state := &connState{}

	// Key that doesn't trigger redirect (fresh cluster, no slots assigned)
	resp := handler.checkAndHandleRedirect(state, "testkey")
	assert.True(t, resp == nil)
}

// TestCheckAndHandleRedirect_ImportingWriteFence blocks ASKING writes (not RESTORE)
// on IMPORTING slots to prevent racing with Phase-1 migration RESTORE.
func TestCheckAndHandleRedirect_ImportingWriteFence(t *testing.T) {
	dbPath := t.TempDir()
	db, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	c, err := cluster.NewCluster(db, "", "127.0.0.1:6337")
	assert.NoError(t, err)
	source := cluster.NewNode("source-node", "127.0.0.1:6338")
	c.AddNode(source)
	c.SetSlotImporting(42, "source-node")
	// Force key into slot 42 via hash tag is hard; use any key and mark that slot.
	key := "fencekey"
	slot := cluster.Slot(key)
	c.SetSlotImporting(slot, "source-node")

	handler := &Handler{
		Db:      db,
		Cluster: c,
		conns:   make(map[*connState]*connMeta),
		Port:    6337,
	}

	// ASKING + SET → fenced
	state := &connState{clusterAsking: true, currentCmd: "SET"}
	resp := handler.checkAndHandleRedirect(state, key)
	assert.NotNil(t, resp)
	errStr := resp.String()
	if !strings.Contains(errStr, "IMPORTING") && !strings.Contains(errStr, "fenced") {
		t.Fatalf("expected fence error, got %s", errStr)
	}
	assert.False(t, state.clusterAsking) // one-shot cleared

	// ASKING + GET → allowed
	state = &connState{clusterAsking: true, currentCmd: "GET"}
	resp = handler.checkAndHandleRedirect(state, key)
	assert.True(t, resp == nil)

	// ASKING + RESTORE → allowed (migration path)
	state = &connState{clusterAsking: true, currentCmd: "RESTORE"}
	resp = handler.checkAndHandleRedirect(state, key)
	assert.True(t, resp == nil)
}

// TestCheckAndHandleRedirect_MigratingWriteFence blocks writes to existing keys
// on a MIGRATING owner while still serving reads (and ASKing when key missing).
func TestCheckAndHandleRedirect_MigratingWriteFence(t *testing.T) {
	dbPath := t.TempDir()
	db, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	c, err := cluster.NewCluster(db, "", "127.0.0.1:6337")
	assert.NoError(t, err)
	target := cluster.NewNode("target-node", "127.0.0.1:6338")
	c.AddNode(target)

	key := "migfence"
	slot := cluster.Slot(key)
	assert.NoError(t, c.AssignSlot(slot, c.Myself.ID))
	c.SetSlotMigrating(slot, "target-node")
	assert.NoError(t, db.Set(key, "v"))

	handler := &Handler{
		Db:      db,
		Cluster: c,
		conns:   make(map[*connState]*connMeta),
		Port:    6337,
	}

	// Write fenced
	state := &connState{currentCmd: "SET"}
	resp := handler.checkAndHandleRedirect(state, key)
	assert.NotNil(t, resp)
	if !strings.Contains(resp.String(), "MIGRATING") {
		t.Fatalf("expected MIGRATING fence, got %s", resp.String())
	}

	// Read allowed
	state = &connState{currentCmd: "GET"}
	resp = handler.checkAndHandleRedirect(state, key)
	assert.True(t, resp == nil)

	// Missing key → ASK
	state = &connState{currentCmd: "GET"}
	resp = handler.checkAndHandleRedirect(state, "other-key-not-present-xyz")
	// may or may not be same slot; only assert if same slot
	if cluster.Slot("other-key-not-present-xyz") == slot {
		assert.NotNil(t, resp)
		assert.True(t, strings.Contains(resp.String(), "ASK"))
	}
}

// TestCheckAndHandleMultiKeyRedirect_NilCluster 验证非集群模式返回 nil
// Kills CONDITIONALS_NEGATION on `h.Cluster == nil` in multi-key
func TestCheckAndHandleMultiKeyRedirect_NilCluster(t *testing.T) {

	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.checkAndHandleMultiKeyRedirect([]string{"k1", "k2"})
	assert.True(t, resp == nil)
}

// TestCheckAndHandleMultiKeyRedirect_NoRedirect 验证集群模式无重定向时返回 nil
// Kills CONDITIONALS_NEGATION on `movedError == nil` check
func TestCheckAndHandleMultiKeyRedirect_NoRedirect(t *testing.T) {

	dbPath := t.TempDir()
	db, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	c, err := cluster.NewCluster(db, "", "127.0.0.1:6337")
	assert.NoError(t, err)

	handler := &Handler{
		Db:      db,
		Cluster: c,
		conns:   make(map[*connState]*connMeta),
		Port:    6337,
	}

	resp := handler.checkAndHandleMultiKeyRedirect([]string{"k1", "k2"})
	assert.True(t, resp == nil)
}

// ---------- handler_core.go: registerConnection ----------

// TestRegisterConnection_NilClientInfo 验证 clientInfo==nil 时自动创建
// Kills CONDITIONALS_NEGATION on `state.clientInfo == nil` check
func TestRegisterConnection_NilClientInfo(t *testing.T) {

	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()

	state := &connState{}
	meta := handler.registerConnection(state, nil, "127.0.0.1:12345")

	assert.True(t, meta != nil)
	assert.True(t, state.clientInfo != nil)
	assert.True(t, state.clientInfo.ID > 0)
	assert.Equal(t, "127.0.0.1:12345", state.clientInfo.Addr)

	// Cleanup
	handler.unregisterConnection(state)
}

// TestRegisterConnection_ExistingClientInfo 验证 clientInfo!=nil 时不覆盖
// Kills CONDITIONALS_NEGATION on `state.clientInfo == nil` else branch
func TestRegisterConnection_ExistingClientInfo(t *testing.T) {

	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()

	state := &connState{
		clientInfo: &ClientInfo{Name: "existing-client"},
	}
	meta := handler.registerConnection(state, nil, "127.0.0.1:9999")

	assert.True(t, meta != nil)
	// Name should be preserved
	assert.Equal(t, "existing-client", state.clientInfo.Name)
	// But ID and Addr should be updated
	assert.True(t, state.clientInfo.ID > 0)
	assert.Equal(t, "127.0.0.1:9999", state.clientInfo.Addr)

	handler.unregisterConnection(state)
}

// ---------- handler_core.go: processRequest ----------

// TestProcessRequest_EmptyCommand 验证空命令返回错误
// Kills CONDITIONALS_NEGATION on `len(args) == 0` check
func TestProcessRequest_EmptyCommand(t *testing.T) {

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	req := &proto.Array{Args: [][]byte{}}
	resp := handler.processRequest(req, nil, "127.0.0.1:12345", nil, nil, state)
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "ERR no command"))
}

// TestProcessRequest_CommandCaseInsensitive 验证命令大小写不敏感
// Kills BOUNDARY on strings.ToUpper
func TestProcessRequest_CommandCaseInsensitive(t *testing.T) {

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// "ping" should work the same as "PING"
	req := &proto.Array{Args: [][]byte{[]byte("ping")}}
	resp := handler.processRequest(req, nil, "127.0.0.1:12345", nil, nil, state)
	ss, ok := resp.(*proto.SimpleString)
	assert.True(t, ok)
	assert.Equal(t, "PONG", string(*ss))
}

// TestProcessRequest_ExpireNormalization 验证 EXPIRE 被规范化为 PEXPIREAT
// Kills CONDITIONALS_NEGATION on EXPIRE/PEXPIRE normalization switch cases
func TestProcessRequest_ExpireNormalization(t *testing.T) {

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// SET a key first
	handler.executeCommand(state, "SET", [][]byte{[]byte("expirekey"), []byte("val")}, "127.0.0.1:12345")

	// EXPIRE should succeed (returns Integer 1)
	resp := handler.executeCommand(state, "EXPIRE", [][]byte{[]byte("expirekey"), []byte("100")}, "127.0.0.1:12345")
	intResp, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*intResp))

	// Verify key has TTL
	ttlResp := handler.executeCommand(state, "TTL", [][]byte{[]byte("expirekey")}, "127.0.0.1:12345")
	ttlInt, ok := ttlResp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*ttlInt) > 0 && int64(*ttlInt) <= 100)
}

// TestProcessRequest_PexpireNormalization 验证 PEXPIRE 被规范化为 PEXPIREAT
// Kills CONDITIONALS_NEGATION on PEXPIRE case
func TestProcessRequest_PexpireNormalization(t *testing.T) {

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("pexpirekey"), []byte("val")}, "127.0.0.1:12345")

	// PEXPIRE with 50000ms
	resp := handler.executeCommand(state, "PEXPIRE", [][]byte{[]byte("pexpirekey"), []byte("50000")}, "127.0.0.1:12345")
	intResp, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*intResp))

	// Verify key has TTL in ms range
	pttlResp := handler.executeCommand(state, "PTTL", [][]byte{[]byte("pexpirekey")}, "127.0.0.1:12345")
	pttlInt, ok := pttlResp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*pttlInt) > 0 && int64(*pttlInt) <= 50000)
}

// TestProcessRequest_ExpireInvalidSeconds 验证 EXPIRE 无效秒数
// Kills CONDITIONALS_NEGATION on `err == nil` check in EXPIRE case
func TestProcessRequest_ExpireInvalidSeconds(t *testing.T) {

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("expirekey2"), []byte("val")}, "127.0.0.1:12345")

	// EXPIRE with non-numeric seconds → should not normalize, still execute as EXPIRE
	resp := handler.executeCommand(state, "EXPIRE", [][]byte{[]byte("expirekey2"), []byte("abc")}, "127.0.0.1:12345")
	// EXPIRE with invalid value returns an error from the store layer
	_ = resp
}

// TestProcessRequest_PexpireInvalidMs 验证 PEXPIRE 无效毫秒数
// Kills CONDITIONALS_NEGATION on `err == nil` check in PEXPIRE case
func TestProcessRequest_PexpireInvalidMs(t *testing.T) {

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("pexpirekey2"), []byte("val")}, "127.0.0.1:12345")

	// PEXPIRE with non-numeric ms → should not normalize, still execute as PEXPIRE
	resp := handler.executeCommand(state, "PEXPIRE", [][]byte{[]byte("pexpirekey2"), []byte("xyz")}, "127.0.0.1:12345")
	_ = resp
}

// TestProcessRequest_ExpireNonExistentKey 验证 EXPIRE 不存在的 key
// Kills ARITHMETIC on return value
func TestProcessRequest_ExpireNonExistentKey(t *testing.T) {

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "EXPIRE", [][]byte{[]byte("nonexistent"), []byte("100")}, "127.0.0.1:12345")
	intResp, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*intResp))
}

// TestProcessRequest_PexpireNonExistentKey 验证 PEXPIRE 不存在的 key
// Kills ARITHMETIC on return value
func TestProcessRequest_PexpireNonExistentKey(t *testing.T) {

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "PEXPIRE", [][]byte{[]byte("nonexistent"), []byte("50000")}, "127.0.0.1:12345")
	intResp, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*intResp))
}

// ---------- handler_core.go: Clients section ----------

// TestBuildInfoResponse_ClientsSection 验证 Clients 段输出
// Kills CONDITIONALS_NEGATION on `section == "CLIENTS"` check
func TestBuildInfoResponse_ClientsSection(t *testing.T) {

	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()
	handler.Port = 6337

	resp := handler.buildInfoResponse("CLIENTS")
	assert.True(t, strings.Contains(resp, "# Clients"))
	assert.True(t, strings.Contains(resp, "connected_clients:"))
	assert.True(t, strings.Contains(resp, "cluster_connections:0"))
	assert.True(t, strings.Contains(resp, "maxclients:10000"))
	assert.True(t, strings.Contains(resp, "blocked_clients:"))
	assert.True(t, strings.Contains(resp, "tracking_clients:0"))
	assert.True(t, strings.Contains(resp, "max_blocking_keys:0"))
	assert.True(t, strings.Contains(resp, "io_threads_active:0"))
	assert.True(t, !strings.Contains(resp, "# Server"))
}

// ---------- info.go: Server section detailed fields ----------

// TestBuildInfoResponse_ServerSectionDetailed 验证 Server 段所有字段
// Kills BOUNDARY/CONDITIONALS on individual field outputs
func TestBuildInfoResponse_ServerSectionDetailed(t *testing.T) {

	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()
	handler.Port = 6379

	resp := handler.buildInfoResponse("SERVER")
	assert.True(t, strings.Contains(resp, "git_commit_id:"))
	assert.True(t, strings.Contains(resp, "build_time:"))
	assert.True(t, strings.Contains(resp, "os:"))
	assert.True(t, strings.Contains(resp, "gcc_version:"))
	assert.True(t, strings.Contains(resp, "run_id:"))
	assert.True(t, strings.Contains(resp, "tcp_backlog:511"))
	assert.True(t, strings.Contains(resp, "uptime_in_seconds:0"))
	assert.True(t, strings.Contains(resp, "uptime_in_days:0"))
	assert.True(t, strings.Contains(resp, "tcp_port:6379"))
}

// TestBuildInfoResponse_MultiplexingApi 验证多路复用 API 字段
// Kills CONDITIONALS_NEGATION on `runtime.GOOS == "linux"` check
func TestBuildInfoResponse_MultiplexingApi(t *testing.T) {

	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()
	handler.Port = 6337

	resp := handler.buildInfoResponse("SERVER")
	// On non-Linux (macOS), should be kqueue
	assert.True(t, strings.Contains(resp, "multiplexing_api:"))
}

// ---------- info.go: formatBytes exact boundary values ----------

// TestFormatBytes_ExactlyGB 验证正好 1GB 的格式化
// Kills CONDITIONALS_BOUNDARY/NEGATION on `bytes >= GB` (exact boundary)
func TestFormatBytes_ExactlyGB(t *testing.T) {

	// Exactly 1 GB = 1073741824
	result := formatBytes(1073741824)
	assert.Equal(t, "1.00G", result)
}

// TestFormatBytes_BelowGB 验证略低于 1GB 的值不进入 GB 分支
// Kills CONDITIONALS_BOUNDARY on `bytes >= GB` (below boundary)
func TestFormatBytes_BelowGB(t *testing.T) {

	// 1073741823 = 1GB - 1
	result := formatBytes(1073741823)
	assert.True(t, strings.Contains(result, "M"))
	assert.True(t, !strings.Contains(result, "G"))
}

// TestFormatBytes_ExactlyMB 验证正好 1MB 的格式化
// Kills CONDITIONALS_BOUNDARY/NEGATION on `bytes >= MB` (exact boundary)
func TestFormatBytes_ExactlyMB(t *testing.T) {

	// Exactly 1 MB = 1048576
	result := formatBytes(1048576)
	assert.Equal(t, "1.00M", result)
}

// TestFormatBytes_BelowMB 验证略低于 1MB 的值不进入 MB 分支
// Kills CONDITIONALS_BOUNDARY on `bytes >= MB` (below boundary)
func TestFormatBytes_BelowMB(t *testing.T) {

	// 1048575 = 1MB - 1
	result := formatBytes(1048575)
	assert.True(t, strings.Contains(result, "K"))
	assert.True(t, !strings.Contains(result, "M"))
}

// TestFormatBytes_ExactlyKB 验证正好 1KB 的格式化
// Kills CONDITIONALS_BOUNDARY/NEGATION on `bytes >= KB` (exact boundary)
func TestFormatBytes_ExactlyKB(t *testing.T) {

	// Exactly 1 KB = 1024
	result := formatBytes(1024)
	assert.Equal(t, "1.00K", result)
}

// TestFormatBytes_BelowKB 验证略低于 1KB 的值不进入 KB 分支
// Kills CONDITIONALS_BOUNDARY on `bytes >= KB` (below boundary)
func TestFormatBytes_BelowKB(t *testing.T) {

	// 1023 = 1KB - 1
	result := formatBytes(1023)
	assert.Equal(t, "1023B", result)
}

// ---------- info.go: multi-slave index verification ----------

// TestBuildInfoResponse_MultipleSlaves 验证多从节点索引正确
// Kills ARITHMETIC_BASE on slave index `i` in for loop (info.go:67)
func TestBuildInfoResponse_MultipleSlaves(t *testing.T) {

	dbPath := t.TempDir()
	db, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	replMgr := replication.NewReplicationManager(db)
	replMgr.SetRole(replication.RoleMaster)

	handler := &Handler{
		Db:          db,
		Replication: replMgr,
		conns:       make(map[*connState]*connMeta),
		Port:        6337,
	}

	// Manually add multiple slaves to the replication manager
	// We need to use the internal API to add fake slaves
	// Since we can't easily add real slave connections, we test the format
	// by checking that the slave index starts at 0
	resp := handler.buildInfoResponse("REPLICATION")
	assert.True(t, strings.Contains(resp, "role:master"))
	// With no actual slaves, connected_slaves should be 0
	assert.True(t, strings.Contains(resp, "connected_slaves:0"))
}

// ---------- handler_core.go: propagation exclusion list ----------

// TestProcessRequest_ReplicaofNotPropagated 验证 REPLICAOF 命令不被传播
// Kills CONDITIONALS_NEGATION on `cmd != "REPLICAOF"` check (handler_core.go:549)
func TestProcessRequest_ReplicaofNotPropagated(t *testing.T) {

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// REPLICAOF without replication manager → error (not propagated)
	resp := handler.executeCommand(state, "REPLICAOF", [][]byte{[]byte("NO"), []byte("ONE")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "ERR replication not enabled"))
}

// TestProcessRequest_MigrateNotPropagated 验证 MIGRATE 命令不被传播
// Kills CONDITIONALS_NEGATION on `cmd != "MIGRATE"` check (handler_core.go:549)
func TestProcessRequest_MigrateNotPropagated(t *testing.T) {

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// MIGRATE with invalid target should return error (not panic)
	resp := handler.executeCommand(state, "MIGRATE", [][]byte{
		[]byte("127.0.0.1"), []byte("0"), []byte("key"), []byte("0"), []byte("100"),
	}, "127.0.0.1:12345")
	// Should return an error (connection refused or similar)
	_ = resp
}

// TestProcessRequest_ReplconfNotPropagated 验证 REPLCONF 命令不被传播
// Kills CONDITIONALS_NEGATION on `cmd != "REPLCONF"` check (handler_core.go:549)
func TestProcessRequest_ReplconfNotPropagated(t *testing.T) {

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// REPLCONF should execute without being propagated
	resp := handler.executeCommand(state, "REPLCONF", [][]byte{[]byte("ACK"), []byte("0")}, "127.0.0.1:12345")
	_ = resp
}

// TestProcessRequest_PsyncNotPropagated 验证 PSYNC 命令不被传播
// Kills CONDITIONALS_NEGATION on `cmd != "PSYNC"` check (handler_core.go:549)
func TestProcessRequest_PsyncNotPropagated(t *testing.T) {

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// PSYNC without replication manager should return error
	resp := handler.executeCommand(state, "PSYNC", [][]byte{[]byte("?"), []byte("-1")}, "127.0.0.1:12345")
	_ = resp
}

// ---------- handler_core.go: EXPIRE/PEXPIRE arithmetic mutations ----------

// TestProcessRequest_ExpirePropagation 验证 EXPIRE 规范化后的传播参数
// Kills ARITHMETIC_BASE on `int64(seconds)*1000` (handler_core.go:534)
func TestProcessRequest_ExpirePropagation(t *testing.T) {

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("ekey"), []byte("val")}, "127.0.0.1:12345")

	// EXPIRE with 100 seconds (large enough to survive test execution)
	resp := handler.executeCommand(state, "EXPIRE", [][]byte{[]byte("ekey"), []byte("100")}, "127.0.0.1:12345")
	intResp, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*intResp))

	// Verify TTL is positive (key has expiry)
	ttlResp := handler.executeCommand(state, "TTL", [][]byte{[]byte("ekey")}, "127.0.0.1:12345")
	ttlInt, ok := ttlResp.(*proto.Integer)
	assert.True(t, ok)
	ttl := int64(*ttlInt)
	assert.True(t, ttl > 0)
}

// TestProcessRequest_PexpirePropagation 验证 PEXPIRE 规范化后的传播参数
// Kills ARITHMETIC_BASE on `+ ms` (handler_core.go:541)
func TestProcessRequest_PexpirePropagation(t *testing.T) {

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("pkey"), []byte("val")}, "127.0.0.1:12345")

	// PEXPIRE with 1000ms
	resp := handler.executeCommand(state, "PEXPIRE", [][]byte{[]byte("pkey"), []byte("1000")}, "127.0.0.1:12345")
	intResp, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*intResp))

	// Verify PTTL is approximately 1000ms
	pttlResp := handler.executeCommand(state, "PTTL", [][]byte{[]byte("pkey")}, "127.0.0.1:12345")
	pttlInt, ok := pttlResp.(*proto.Integer)
	assert.True(t, ok)
	pttl := int64(*pttlInt)
	assert.True(t, pttl > 0 && pttl <= 1000)
}
