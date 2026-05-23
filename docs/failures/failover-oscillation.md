# Failover Oscillation

## Symptom

After a sentinel failover, the system enters a cycle where:

- Sentinel views oscillate between different master addresses
- Agreement reaches 100%, then drops, then recovers
- `FailoverStarted` >> `SuccessfulFailovers` (many failed attempts)
- Failed failover attempts continue indefinitely via gossip re-triggering
- `ODownReached` keeps incrementing without resolution
- Leader change count doesn't stabilize

The system is stuck in a **limit cycle** of failover → fail → retrigger → fail.

## Root Cause

Three distinct mechanisms cause oscillation:

### 1. Post-Heal View Flipping

After a partition heals, the old master becomes reachable again. Sentinels that were monitoring the promoted slave may briefly see the old master via stale gossip, causing their views to oscillate between "old master" and "new master" until gossip converges.

This is a **temporal convergence problem**: the agreement trajectory should be monotonic (once convergence starts, agreement should only increase). Non-monotonic convergence = oscillation.

### 2. Chain Failover with Dead Slave Selection

When a promoted slave (new master) also dies shortly after promotion:

1. `selectNewMaster` iterates all registered slaves
2. It does NOT verify the slave is actually reachable — only checks `State == "online"`
3. If the highest-offset slave is dead, it's still selected
4. `SendSlaveOfNoOne` fails → failover fails
5. `checkMaster` guards prevent immediate retry (state check), but:
6. Gossip SDOWN from other sentinels re-triggers `AutoFailover`
7. Same dead slave is selected again → same failure → repeat

This creates a **gossip-driven failover loop** where every incoming SDOWN broadcast triggers another failed attempt against the same dead slave.

### 3. Cascading Failure Instability

When multiple nodes fail in sequence, the sentinel's slave state tracking becomes stale. There is no mechanism to:
- Mark a slave as "offline" when connectivity is lost
- Verify slave liveness during `selectNewMaster`
- Back off failover attempts after repeated failures

The result: the system can enter a non-convergent oscillation bounded only by gossip interval and quorum timing.

## Invariant Violated

- **Convergence trajectory must be monotonic**: Once agreement starts increasing toward full consensus, it must NEVER drop. A drop after peak = oscillation.
- **Failover must be idempotent**: Each distinct master death should cause exactly one failover. Extra failover attempts = oscillation.
- **Leader changes must converge**: After the last failure event, no new leader changes should occur.

## Fix

### Short-term (test detects the gap):

1. Add liveness verification in `selectNewMaster` — probe the candidate slave before returning it
2. Add failover cooldown — after a failed failover, prevent re-triggering for `N * downAfter` seconds
3. Mark slaves as "offline" when the master they were replicating from is detected as down

### Medium-term:

4. `convergenceHistory` tracking with monotonicity assertion should be added to all sentinel regression tests
5. Add temporal oscillation detection: if agreement drops > 1 level after reaching >= 50% consensus, flag as oscillation

## Prevention

- In soak tests, track `convergenceHistory.HasOscillation()` — any true return is a fail
- Monitor `FailoverStarted / SuccessfulFailovers` ratio: sustained ratio >> 1.0 means oscillation
- Monitor `ODownReached` growth rate after heal — continued growth means the system hasn't stabilized
- Dashboard: agreement trajectory plot should be monotonically increasing during convergence
