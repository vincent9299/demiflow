# Capacity history no longer exhausts active admission

The original `LocalCapacityPool._active` counted every directory entry toward the 1,024 active-record ceiling, including completed allocations. A long-lived pool with 1,021 RELEASED records and only five BOUND records therefore rejected a new allocation despite available CPU/memory. The exact 1,026-record fixture fails against fixed `5ff5da9` with the original `QUOTA_EXCEEDED` error.

Only `demiflow/resources/capacity.py` changes production behavior. When the original flat directory exceeds the existing active scan ceiling, completed records move to `released/<sha256(id)[:2]>/<original-id>.json` **inside the same pool and under the same original lock**. Original bytes and release evidence remain intact. BOUND/RESERVED/unknown records stay in place and remain fully charged. No new allocator, index balance, reservation authority, or automatic refund is introduced.

One call examines at most two pages of 1,025 directory entries. It never admits from an incomplete scan: a larger historical backlog makes bounded archival progress and returns retryable `CAPACITY_PENDING`. The original 1,024 live-allocation ceiling and all resource ceilings remain unchanged. Lookup by allocation ID checks at most the original and exact hashed archive paths. Re-reserving a released ID is still rejected; matching release evidence remains idempotent after archival.

The move uses link → target-directory fsync → source-name unlink → source-directory fsync, with parent directories synced before publication. An interrupted move can leave equal duplicate names; a retry accepts only equal bytes. Conflicting duplicates or non-released archive contents fail closed. Small existing layouts remain byte-for-byte at their original paths.

## Verification

- Original-version red: `/tmp/demiflow-capacity-history-red.xml`, the exact 1,026-record fixture fails as expected.
- New dedicated suite: 10 cases passed, including 1,026 historical records, preserved unknown charges, re-reserve rejection/idempotent release, three interrupted move boundaries, conflicting duplicates, >3,000 historical records with bounded progress, full live-slot rejection, six concurrent reservers, and archive symlink rejection.
- Final combined dedicated + original U2/U4/resource-evidence/resource-progress suites: **50 passed in 30.46 s**, zero skips. Every process uses a private test capacity domain; the runner asserts both the imported checkout and private domain at entry.
- `/tmp/demiflow-capacity-history-final2-evidence/verification.json` binds 1,041 source/test digests, unchanged before/after, and all seven protected files equal the frozen baseline. Exact invocation and artifact hashes are in the paired JSON.

History storage grows with retained historical records; active admission work and lookup are bounded. Filesystem failures remain errors, not implicit permission to discard history.

## Compatibility boundary

The new reader supports both original flat and new archived records. An old `LocalCapacityPool` reader only knows the flat directory. A production rollout must update every client sharing a pool before enabling archival; mixed old/new readers are not claimed compatible. This patch is verified in isolated pools and has not been deployed to the shared pool.

## Test boundary incident and exact restoration

One initial broad regression command omitted `DEMIFLOW_RESOURCE_ROOT`. Two old tests consequently used the default shared pool, causing the candidate code to move 1,019 RELEASED names and create two test allocations. The command was stopped and the coordinator was informed immediately. This was a boundary violation; it is not represented as a clean read-only run or as two product regressions.

Read-only inspection linked both new allocations to this test's original run journals. Both are RELEASED with original cleanup CONFIRMED and their original PID/birth identities gone. The old five BOUND records remained charged. Prior individual byte hashes of those five were unavailable, so no invented before/after comparison is claimed for the initial incident.

The coordinator reviewed a dry-run plan and complete private-copy simulation, then explicitly approved exact name restoration. The archive directory birth and all 1,019 file ctimes fell inside the specific pytest interval; all original file mtimes preceded it. Under the same original pool lock, the approved script checked every retained byte digest, linked each missing original name back, synced it, and removed only that test's archive link. It did not refund resources, delete records, alter JSON, or touch the other nine flat records.

Independent post-restoration verification found **1,028/1,028 record digests equal to the first incident snapshot**, all 1,019 names back in `allocations`, and zero archived JSON names. The five original BOUND and two test RELEASED records are retained. The two test histories remain as explicitly instructed; the exact pre-incident record count was not restored by deleting history. Shared-pool handling ended there. All later tests used explicit private domains.

Review artifacts:

- `/tmp/demiflow-capacity-shared-test-incident-20261116.json`
- `/tmp/demiflow-capacity-restore-exact-20261116.py`
- `/tmp/demiflow-capacity-restore-plan-20261116.json`
- `/tmp/demiflow-capacity-restore-simulation-20261116.json`
- `/tmp/demiflow-capacity-restore-result-20261116.json`
- `/tmp/demiflow-capacity-restore-independent-verification-20261116.json`

The paired JSON retains every restored/untouched record hash for independent review. No main-repository change, push, merge, shared-environment install, or release of another owner's capacity occurred.
