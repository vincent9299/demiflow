# Shared actor ownership correction — 2026-11-17

Isolated branch `engine/gpt-v3-shared-actor-owner-20261117`, based on `a445d12`, in `gpt_v3_perf_mainpath_20261116`. Main checkout, frozen seven files and shared environments were not changed. No push or main merge.

## Defect and resulting behavior

The same actor reused in two real Dataset map occurrences was serialized, instantiated and snapshotted independently. Two count=12 owners replaced the intended single count=24 owner. This was a semantic defect behind the old native-barrier test's retired `node-0` observation, not merely an obsolete assertion.

New captured actor occurrences explicitly declare `actor_binding_version=1`; actual same-instance groups additionally record owner/members/state_owner. Object identity discovers the relation only during capture; persisted identity uses stable occurrence IDs. Functions and class factories do not become shared actors. Same-valued distinct instances and two byte-identical shared program bundles remain separate owners.

Programs in a shared group are serialized together within the original byte bound, mounted from the original pinned business sources and loaded once per worker/group. A cache keyed by original group occurrence, never pickle digest, retains aliases. Kernel call adapters retain their own parameters, contract validation, invocation cursor, scope and deadlines. One actual actor has one event loop and lifecycle; its state is snapshotted only at the last topological owner. Other original kernel images contain authenticated aliases. Resume requires every member image and the authority image before opening sources.

Mandatory physical placement includes every path between shared members. Overlapping closures and cycles induced by crossed contractions are merged into an acyclic quotient. It keeps original per-occurrence CPU/memory/output/state requests conservatively; requests and explicit limits are never silently discarded. Optional stateless replicas may use their already-allowed minimum of one. An impossible co-location fails before business start. Default public calls retain the existing automatic window-fitting behavior. Explicit oversized Local windows are rejected, not expanded.

Final lifecycle observations are collected after all kernels close, so the first owner's observation includes the single actual close performed by its last borrower.

## History admission integration required by root

`execution/dataflow_shared.py::validate_shared_actor_binding(request, graph, *, bounds)` is a pure capture-and-compare check. It rejects both sharing→independent and independent→sharing changes for new versioned plans. New independent actor occurrences also carry the version marker, preventing the reverse direction from escaping validation. An old graph without the marker is returned unchanged and does not inspect a new recipe. Historical plan_identity, stored dag_id, journal and commit protocols are untouched.

Root owns `dataflow.py`. Apply `/tmp/demiflow-shared-binding-driver.patch` immediately after historical `load_dataflow` obtains graph/physical, before `validate_literal_schema_replay` and any worker/source start:

```python
from .dataflow_shared import validate_shared_actor_binding
validate_shared_actor_binding(self._request, graph, bounds=physical.bounds)
```

The pure validator and both-direction negative tests are delivered here. The driver call is intentionally not included in this candidate and must be integrated before claiming admission closure.

## Verification

All invocations used the original `execute_batch`/inventory runner, exact node IDs, in-pytest source-root and three private-domain assertions, `/tmp/demiflow-*` scratch, full three-phase reports and before/after input hashes. Every accepted batch reports unchanged source. JSON alongside this document lists receipt SHA256 and final production/test bytes.

- Red: unchanged pre-fix tracked production from `a445d12`, new actual-actor test: **1 failed**, two independent hooks states and no shared alias. `/tmp/demiflow-shared-actor-red/batch/receipt.json`. Candidate bytes were backed up and restored only after that runner exited; this is not evidence of a test running while production changed.
- Core candidate plus affected existing lifecycle tests: **36 passed**. This preceded only the later version-marker/admission addition; do not describe it as 36 tests on the final exact source.
- Final versioned admission and Local checks: **9 passed**, including positive/negative relation comparisons, old unversioned preservation, real shared execution/default public entry, process recovery and distinct equal-valued groups.
- Final versioned real Ray Core recovery: **1 passed**, owned private 8-CPU cluster, stopped only by the fixture that created it. Actual first ACK interrupted; resume restores the single original state once and ends at count24 with exactly 12 output rows. Each actual attempt has one start/close.
- Before final version marker, actual lifecycle observation plus existing success/start/call+close/close/cancel/empty/stale-boot cases: **14 passed**.

Covered semantics include Local process/thread, actual commit/cut resume, per-cut state equal to original confirmed occurrence output cursors, independent same-value instances, identical-byte groups, class factories, two calls of one function, partial/bound parameters, synchronous map task boundaries, malformed/missing alias rejection, default-window public use, explicit-budget pre-start refusal, path and crossed-branch placement, real final lifecycle observations and existing cancellation cleanup.

## Limits and follow-on work

Explicit device/affinity/limit profiles that cannot be safely represented by one shared worker are rejected before execution. This change does not claim support for shared exclusive GPU services. Profile/state reservations remain conservative even though only one state value is retained; no quota is enlarged. A parked worker may legitimately reconstruct one owner from its original checkpoint; lifecycle assertions refer to actual worker ownership, not an invented immortal Python object across process loss.

`recovery_rows` and the later-discovered single-worker class/union placement failures are separate authorized follow-ons and are not claimed fixed by this commit. Whole engine/product acceptance remains with root integration.

## Passive admission follow-up

The initial versioned validator used full logical capture. Review found that unnecessary for ownership comparison and liable to rescan declared literal rows for schema. It now traverses only the original declaration preorder with the existing relational/extra-input expansion helpers. Leaf sources occupy one position without examining rows, schema, connection factories or reader contents. It neither serializes programs nor constructs actors. Original occurrence IDs and alias sets are compared without reconstructing execution state.

Fifteen final follow-up checks passed in 1.09 seconds (`/tmp/demiflow-shared-passive-admission-final/batch/receipt.json`, SHA256 `737a7e172d97299eb35330f512426bbe97b56761f0ff5df28ecc0abcd56f859f`): items/iterable/SQL × valid/changed relation, explicit forbidden full-compile/source-schema/factory-constructor probes, union and prepend occurrence ordering in both positive and negative cases, both alias-change directions, and unversioned history preservation. The source snapshot remained unchanged. This is additional evidence for the two changed follow-up files, not a rerun claim about all prior cases. Driver call/signature stays the same.
