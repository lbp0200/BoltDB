# 生产事故回归测试计划

## 已覆盖的生产事故

所有规划的回归测试已全部实现：

| 生产事故 | 回归测试 | 状态 |
|---------|---------|------|
| Backlog Exhaustion | `TestRegressionBacklogExhaustion` | ✅ |
| L0 Collapse | `TestRegressionL0Collapse` | ✅ |
| Replication Thrash | `TestRegressionReplicationThrash` + `Fullresync` | ✅ |
| Retry Storm | `TestRegressionRetryStorm` | ✅ |
| Shutdown Race | `TestRegressionShutdownRace` | ✅ |
| Snapshot Inconsistency | `TestRegressionSnapshotConsistency` + `ConcurrentWrites` + `FullresyncOffset` | ✅ |
| Deterministic Replay | `TestRegressionCanonical*` (SPOP/XADD/EXPIRE) | ✅ |
| Duplicate Window | `TestRegressionDuplicateWindowMeasurement` | ✅ |
| PSYNC Reconnect | `TestRegressionPsyncReconnectNoLoss` | ✅ |
| Failover Oscillation | `TestRegressionFailoverOscillation` | ✅ |
| Replication Write Deadline | `TestRegressionWriteDeadlineStorm` | ✅ |
| Split-Brain Convergence | `TestRegressionSplitBrainConvergence` | ✅ |
| Concurrent FULLRESYNC | `TestRegressionConcurrentFullresyncWriteStorm` | ✅ |
| Disk Pressure | `TestRegressionDiskPressureDegradation` | ✅ |
| Client Buffer Overflow | `TestRegressionClientBufferOverflow` | ✅ |
| RDB Config Change | `TestRegressionRdbConcurrentConfigChange` | ✅ |
| PubSub Fan-Out | `TestRegressionPubSubFanOutStorm` | ✅ |
| FULLRESYNC + Geo | `TestRegressionFullResyncGeo` | ✅ |
| Slave Ownership | `TestRegressionSlaveConnectionOwnership` | ✅ |
