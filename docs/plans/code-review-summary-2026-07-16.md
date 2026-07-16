# Review Summary

- **Mode**: comprehensive codebase audit (working tree clean; not a local-diff review)
- **Target**: BoltDB `main` @ `8cbfe19` (XACKDEL DELREF fix)
- **Scope**: replication, shutdown, cluster migration, stream/blocking/txn, store backpressure, recent fixes, TODO.md
- **Issue counts**: **8** bugs, **7** suggestions, **1** nit (16 total)

## Top issues

1. **[bug]** SPOP double-propagates (SREM + SPOP) → replica drops extra members
2. **[bug]** XACKDEL/XNACK/BZMPOP/TS.* propagated but unexecutable on replica → FULLRESYNC thrash
3. **[bug]** L0 "write rejected" / max retries treated as skippable → permanent replica data loss
4. **[bug]** post-AddSlave gap-fill residual race → duplicate command delivery
5. **[bug]** stream RDB omits consumer groups/PEL → FULLRESYNC loses XGROUP state

## Files

- Full review: `docs/plans/code-review-comprehensive-2026-07-16.md`
- Scratch copy: `/var/folders/_2/qm8jtp894t71ywjy3x5qvmd00000gn/T//grok-501/grok-review-c68fe2e5.md`
