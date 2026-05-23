# Split-Brain Convergence

## Symptom

After a network partition heals and sentinels converge, the system may exhibit:

- **Non-monotonic agreement**: Sentinel views reach full consensus, then drop, then recover
- **Overshoot oscillation**: Agreement overshoots the target and oscillates around full consensus before settling
- **Extended convergence window**: Convergence takes much longer than gossip propagation would suggest
- **Post-heal leader instability**: After the heal, an unnecessary leader change occurs (e.g., failing back to the original master)
- **False split-brain recurrence**: The system briefly re-enters a split-brain state after healing

The recovery trajectory is not a smooth exponential decay — it shows **underdamped oscillation** around the target.

## Root Cause

### 1. Stale Gossip after Heal

When a partition heals, the old master becomes reachable again. However:

- Sentinels that detected SDOWN during the partition may still have stale state
- Other sentinels that were promoted may be broadcasting their view of the new master
- The first sentinel to detect the old master's return may broadcast confusing signals
- This creates a **competition window** where different sentinels have different "truth"

### 2. No Monotonicity Guarantee

The current sentinel implementation has:

- No ordering guarantee on view transitions
- No stabilization delay before accepting a new master view
- No monotonicity mechanism (once convergence starts, enforce forward progress)

This means after heal, agreement can:
1. Increase (sentinels converge on new master)
2. Drop (stale gossip about old master reaches a sentinel)
3. Increase again (corrective gossip arrives)

This is **temporal oscillation** in the convergence process.

### 3. No Post-Heal Leader Stability Window

After a heal, the system should wait for a **leader stability window** before considering convergence achieved. The current code has no such concept:

- As soon as `sdownCount == 0`, the state goes back to "ok"
- There's no verification that the state persists for multiple monitoring cycles
- This makes the system vulnerable to transient view changes

## Invariant Violated

- **Recovery trajectory must be monotonic**: After heal, agreement must never decrease. A single drop = oscillation.
- **Leader stability window**: After convergence first reaches full agreement, it must stay at full agreement for ≥ 3 consecutive samples.
- **Post-heal leader changes must be zero**: After heal, no new failover should be triggered. Any leader change after heal = oscillation.

## Fix

### Short-term:

1. Add post-heal stabilization delay: after detecting a heal, wait for 2× monitoring cycles before accepting state transitions
2. Add monotonic convergence enforcement: in the convergence tracker, flag any agreement drop after reaching ≥ 50% consensus

### Medium-term:

3. Add a **convergence damping** mechanism: once a sentinel reaches "ok" on a particular master addr, prevent transitions away from that addr for a stabilization window of `N` monitoring cycles
4. Track `lastAgreedTime` in the convergence tracker — if agreement drops after being stable for > 2 samples, emit a temporal oscillation signal

## Prevention

- In soak tests, assert `convergenceHistory.IsConvergenceMonotonic()` after heal
- Track `timeToFirstFullAgreement` and `timeToStableAgreement` separately — a large gap between them indicates oscillation
- Dashboard: plot "agreed sentinels" over time during convergence — should be a monotonic step function
- Monitor leader changes in the 5-second window after heal — any change is a regression
