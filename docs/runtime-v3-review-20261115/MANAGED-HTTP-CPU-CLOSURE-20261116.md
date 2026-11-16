# Native Embedding ManagedHTTPService: CPU closure

This delivery restores the original passive CPU `ManagedHTTPService` on the
unified Dataset worker route. It does not claim GPU placement or Ray service
lifecycle has been validated. Main checkout, shared environments and production
services were not changed. The explicit default legacy-lock boundary deviation
below is not omitted from this statement.

## Implementation and authority

- `dataflow_managed_http.freeze_managed_http/restore_managed_http` serialize only
  the exact native service's original constructor fields, at most 64 KiB. Paths
  become strings; original constructor validation runs in the worker. Unknown
  fields, subclasses, live state, instance/class `bind` replacement and custom
  log callbacks cannot silently become a different native declaration.
- `embeddings.flow_contract.freeze_declaration` seals a separate service-only
  digest. `service=None` adds no attribute or new fact; the original embedding
  declaration, vector-space facts and request identity formulas remain unchanged.
  Changing/removing a service after declaration is rejected before worker launch.
- The Embedding branch of `native_model_declaration` sends the passive service
  through the existing code-pinned `CallableSpec` program. `EmbeddingFactory`
  recreates it in the admitted worker. Started actors and attached live resources
  remain rejected. Prompt/Routed factories are untouched.
- The original `_HTTPOwner` still owns lazy readiness, exclusive port admission,
  process groups and bounded termination/reaping; its algorithm was not changed.
  CPU service children see `CUDA_VISIBLE_DEVICES=''`. Existing ResourceManager
  worker admission remains authoritative; no new allocator or resource balance
  was introduced. This is logical resource admission, not new OS memory/cpu caps.
- Private delivery cache access uses the existing driver-owned proxy from
  `a61b826` (locally cherry-picked as `f1566e6`). No worker opens the private
  cache as another authority. All-cache-hit execution does not start the service.

## Actual validation

13 cases passed on the final helper in 41.66 s, JUnit `/tmp/demiflow-managed-http-final.xml` (an earlier run also passed in 41.37 s):

- Both original `test_embeddings.py` nodeids are retained. The former parent
  in-memory `Owner`/`Popen` mocks were replaced by an actual HTTP executable that
  records PID, birth, startup, request and termination. Original cleanup and
  no-new-start-on-replay assertions remain, now crossing the real worker seam.
- Cache replay succeeds after deleting the startup script; startup and HTTP
  counts remain one. Every recorded PID/birth is gone after action cleanup.
  Emergency fixture teardown runs only after assertions and only for a matching
  recorded PID/birth; it never supplies the asserted runtime exit evidence.
- Real HTTP 503, readiness failure and user cancellation reap the service.
- An occupied port is not attached or its owner terminated.
- Declaration roundtrip, no-service identity preservation, changed service
  method/fields/removal, live actor/service rejection and ungranted GPU rejection.

22 further cases passed in 74.49 s, JUnit
`/tmp/demiflow-managed-http-regression2.xml`: six original HTTP owner tests plus
16 driver-private embedding cache tests. One unrelated original Prompt cache
test is explicitly deselected after its failure was observed: the Dataset API
rejects Prompt `sqlite_journal` before these Embedding changes run. Root assigned
that separate capability to the regression lane. It is not counted as passed.

`MANAGED-HTTP-CPU-VERIFICATION-20261116.json` binds all production Python bytes,
the five involved test/helper files, the JUnit hashes, actual service process
records and private resource-pool release state. The recorded base is `f1566e6`
with explicit dirty sources; these exact bytes are what will be committed.
No full repository/Ray/GPU claim follows from these 35 selected passing cases.

Every test command uses this worktree as cwd, `PYTHONPATH=$PWD:$PWD/tests`,
`PYTHONDONTWRITEBYTECODE=1`, and an asserted private `DEMIFLOW_RESOURCE_ROOT`.
The final 13-case launcher asserts ROOT, HOME and FORGE_DOMAIN under `/tmp/demiflow-*`; its helper uses a private service lock HOME too.
The corrected 22-case command explicitly sets `DEMIFLOW_RESOURCE_HOME` too.
Future launchers must set and assert **all three** `DEMIFLOW_RESOURCE_ROOT`,
`DEMIFLOW_RESOURCE_HOME`, and `DEMIFLOW_FORGE_DOMAIN` under `/tmp/demiflow-*`
before importing/running pytest; no helper may replace them with default paths.

## Boundary deviation disclosed

The first original-owner regression command omitted `DEMIFLOW_RESOURCE_HOME`
while correctly isolating `DEMIFLOW_RESOURCE_ROOT`. Six owner tests briefly used
the default legacy flock domain. Detection caused interruption; no default files
were deleted, migrated, overwritten or reacquired. This is separate from the
DevicePool/capacity ledgers, which stayed private.

`MANAGED-HTTP-BOUNDARY-DEVIATION-20261116.json` records the exact default directory,
five recent zero-byte port names, two preexisting GPU lock names, timestamps,
SHA-256/inodes, zero matching `/proc/locks` entries after exit, known supervisor
and worker PIDs absent, and all six private allocations released. The original
test process PID and four ephemeral service child PIDs were not persisted by
the old tests; this evidence gap is explicit. Original six `assert_reaped`
checks passed. There was no preincident snapshot, so byte equality with the
preincident default domain is **not claimed**. The corrected rerun used private
ROOT and HOME and passed the relevant owner/cache tests.

## Remaining GPU design boundary

Nonempty `service.gpus` is deliberately rejected today: a legacy index/flock
cannot stand in for a V3 device grant. A follow-up must resolve physical local
indices to NVML UUIDs against the explicitly configured deployment inventory,
seal those UUIDs, request the same devices from the original DevicePool, and
inherit only the granted UUIDs into the service. Ray's legacy numeric indices
lack a host-location contract and remain rejected pending a separate definition.
CPU success does not close these GPU requirements.
