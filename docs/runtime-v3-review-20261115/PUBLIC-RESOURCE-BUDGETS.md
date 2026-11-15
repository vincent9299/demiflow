# Public worker and memory budgets

`LocalDatasetExecutor(workers=...)` and `local_execution(workers=...,
memory_bytes=...)` now constrain real unified execution. There is one optional
`RuntimePolicy.max_active_workers` and one existing `Bounds.max_resident_bytes`;
the native ResourceManager/RunResourceState remains the allocator and ledger.

## Implemented contract

- `max_active_workers` bounds all simultaneously live hosted workers across
  stages, independently of each stage's `max_replicas`. Compiler preflight,
  driver placement, and the original resource scope use this same ceiling.
- A new plan whose memory-connected component exceeds its worker budget can
  select the existing recoverable durable parking protocol. The exchange's
  sealed `reason` identifies the worker-budget trigger. Cleanup must still be
  confirmed by the provider before those slots become available.
- An indivisible stage must fit first. A missing GPU or a single oversized
  stage cannot be fixed by durable transport; that resource error is preserved.
- Actual admission uses `max(StageSpec.resident_bytes, profile.memory_bytes)`
  per worker, not merely the input/output/state estimate. Driver phase checks
  and scheduler charges use the same maximum. The estimate and grant remain
  separate fields so proven safe pure-kernel fusion retains its existing rules.
- Source split identities remain unchanged. `_refine_source_partitions` assigns
  multiple splits to bounded partitions rather than spawning one worker for
  every split. A true run verifies eight source splits with two source workers.
- A local memory setting intersects the existing run limit downward. Default
  row, batch, and state windows are recomputed for the smaller envelope, with
  room for input/output/state/control. Explicit finite limits remain explicit;
  an impossible declared working set fails preflight before any worker starts.
- `None` is the legacy worker-limit value: its wire field is omitted and it is
  excluded from the physical identity projection. Old valid policy and physical
  frames round-trip byte-for-byte; new positive values affect the new plan ID.
- Recovery loads the original sealed physical plan. A current caller may further
  tighten the live worker envelope, but does not rewrite that plan or enlarge its
  original limit. The real recovery test goes from two workers to one while
  preserving the original two-worker policy and plan ID in the sealed artifact.

These are logical resource admission bounds, not claims of hard process RSS or
OS CPU enforcement. No second allocator, hidden additional worker, all-rows driver
fallback, Ray Data execution, or replay of unknown model effects is introduced.

## Verification

All tests use this isolated tree and a private `/tmp/demiflow-*` resource domain,
with `PYTHONPATH="$PWD:$PWD/tests"`, `/yzp/zhaozy/yangzepeng/0905/env/bin/python -B -m pytest`.
No Python edits/commits occurred during test execution.

Final command:

```sh
DEMIFLOW_RESOURCE_ROOT=/tmp/demiflow-public-budget-capacity7 PYTHONPATH="$PWD:$PWD/tests" \
 /yzp/zhaozy/yangzepeng/0905/env/bin/python -B -m pytest -q \
 tests/test_runtime_v3_p.py tests/test_runtime_v3_public_budgets.py \
 --basetemp=/tmp/demiflow-public-budget7 --junitxml=/tmp/demiflow-public-budget7.xml
```

17 passed in 53.19s. Covers real 1/2-worker three-stage Dataset runs under 128 KiB,
original allocator charges, unchanged source order, eight-split coverage, exact
legacy wire/digest behavior, explicit limits, recovery tightening, and no upstream
launch when a concurrent operator's actual profile exceeds the entire envelope.

Additional evidence:

- `/tmp/demiflow-public-budget5.xml`: 105 passed (complete C/P contracts, public
  budgets, fixed-memory refusal). Precedes the final actual-profile footprint
  check; codec production code did not change afterward.
- `/tmp/demiflow-public-budget6.xml`: 18 passed, one new test initially failed on
  the reported error reason. The runtime refused before launch; final preflight
  now reports the indivisible stage's actual memory failure and that same
  unweakened assertion passes in the final 17.
- `/tmp/demiflow-public-budget2.xml`: 110 passed, two skipped Ray cases; two test
  harness failures (non-sha split fixture and missing new helper parameter),
  both fixed and rerun. Includes original finite/durable crash, actual ACK-loss,
  source-vector, limit, and Lance recovery cases. Not claimed all-green.
- Lifecycle suite passed all 14 cases after this public worker ceiling was wired.
- P fixtures are the exact already-integrated `f0a34cc` version, synchronized in
  local test-only `d8f67fe`. The parent already has this fixture; only pick the
  resource implementation commit. An earlier `/tmp` placement of these fixtures
  correctly failed their strict worktree-binding guard; no guard was weakened.

The seven frozen files remain unchanged from
`362d8528c27ae60a0d6ae0ccc986a0ff93847de8`. Main checkout, production services,
shared environments, and remote branches are untouched.

## Explicit remaining public configuration work

This package completes **workers + memory**, not every Local setting:

- `local_execution.partitions` historically means shuffle partitions. It is not
  silently reinterpreted as source parallelism; operation-specific shuffle and
  `repartition.num_blocks` consumption still needs its own implementation.
- `worker_mode='thread'` still lacks a true unified thread host. This package
  does not turn a process-host run into evidence that thread mode is complete.
- `native_resources` query memory/RSS/scratch/row/batch/thread/partition/time/
  admission/headroom fields still need explicit unified native-kernel consumers.
  No unused constants or configuration documents are counted as that work.
- `temp_directory` and auxiliary legacy `sort_workers` need separately defined
  unified consumers. The known actor synchronous-start follow-up and historical
  handle-release defect are owned by the parent/other lane.

For an attempt-only source or unfreezable state, an impossible simultaneous
worker envelope is honestly rejected. Automatically inventing checkpointability
or an extra worker is not an acceptable way to preserve that combination.
