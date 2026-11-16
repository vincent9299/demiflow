# Explicit private embedding journal compatibility

This repair preserves the unified V3 executor and the two original intentions:
usable public Dataset operations, and a correct bounded engine with honest
recovery guarantees. It does not introduce another scheduler or commit owner.

## Contract decision

The pre-V3 revision `267b9144c6bf7447c6822aa716f6d5c0eda16990` distinguished
delivery from native recovery. `native_plan.execute_native_plan(local_stream)`
called `stream_delivery.iter_stream_rows`, whose `_run_stream` had no progress
adapter. Private SQLite embedding request caches worked there. Materialization
called `StreamMaterialization.bind_stages`, then
`EmbeddingActor.native_stream_recovery`, which already rejected a private
SQLite journal. `upgrade-contracts-v2-20261113/tasks/M004.md` explicitly retained
independent non-native operation and prohibited simultaneous native/private
request-budget authorities. V3 M021 accidentally made native binding universal.

The coordinator's November 16 decision restores the explicit private mode for
`take`, `take_all`, `take_batch`, `iter_rows`, `iter_batches` and ordinary
nonqueue/nonproducer `run_stream` in the same V3
engine. Native checkpoint/materialize and managed execution continue to reject
it. The document-embedding test now uses the historically legal `take_all`
terminal; its actual text, hash, invalid-span and cache-reuse assertions remain.
A separate positive test locks the materialize rejection.
The old `Dataset.run_stream` explicitly required a sealed `delivery_only`
placement at lines 1043–1044, so it belongs to this compatibility surface.
Acquisition/queue-backed execution and streaming Lance writes still reject the
private mode.

## Ownership and sequence

* Only explicit private nodes add `private_journal` and `max_requests` to their
  sealed kernel declaration. Native default configuration bytes do not change.
* Driver prepare consumes this declaration, rejects incompatible terminals and
  managed execution, and writes attempt-only to the original RunJournal before
  workers can send HTTP. The physical plan also reports nonrecoverable state.
* The driver owns the original `SQLitePromptJournal`, its path-level persistent
  quota, request intent, response/error records, leases and explicit requeue
  history. Worker actor startup receives a bounded typed proxy and opens no DB.
* A confirmed private cache hit uses the exact old request key and consumes no
  new run/node request admission. Pending/unknown intent is never a cache miss.
* For a genuinely new request the driver first uses original RunJournal
  `reserve_budgets` for finite run/node admission, with a stable action/node/key
  reservation ID; it then calls the original private `reserve`. Only a positive
  reply allows HTTP. The existing bounded transport receipt handles duplicate
  RPC delivery; eviction falls back to the private ledger and fails closed.
* This is explicitly **not a transaction across two databases**. An exception or
  crash between them retains run admission conservatively. This unfinished run
  cannot resume. A new run can reuse confirmed private responses; unknown private
  requests still require the existing explicit reconciliation procedure.
* Responses and errors are written only to the private journal. The RunJournal
  owns run admission, restriction and ordinary Dataset commits, not a second
  copy of HTTP effects. Native models retain their existing atomic native
  intent-plus-budget savepoint and native recovery guard.
* Shutdown waits for accepted driver service calls before closing the private
  journal. Bounded close returns retain a visible owned closing thread; physical
  resource exit remains false until it finishes. Closure failure is not counted
  as released ownership.

## Evidence boundary

The new tests exercise real public Dataset, Local worker processes, original
SQLite journals and loopback HTTP. They inspect process file descriptors to
verify the private DB is open in the driver and absent from workers, assert
original run admission and absence of native HTTP records, and inject failures
on both sides of the private reservation. Separate fixed-vocabulary failure
tests preserve protocol/unknown exceptions and carry only public HTTP
method/status/URL; credentials, query, fragments, raw exception text and
tracebacks are not carried for HTTPStatusError.

Historical private cache reads require a bound before SQLite returns large JSON
fields to Python; response/error/reference byte lengths must be evaluated in the
same SQL statement that conditionally projects the payload. Default historical
read behavior and stored bytes remain unchanged.

Final source-bound commands, outcomes and limitations accompany the delivery
receipt. Exploratory failures are retained in `/tmp/demiflow-private-embedding-*`
and are not counted as a green acceptance run. No main checkout, shared
environment, online service or frozen seven files are modified.

## Remaining work at this candidate

The exploratory full group ran 89 tests: 79 passed, 10 failed. After restoring
ordinary `run_stream` admission and adding bounded-history tests, the focused
group ran 34 tests: 32 passed, 2 failed. These are **not full acceptance claims**.

Two related cross-layer gaps remain for the next commit: a cooperative stop can
cancel already-admitted HTTP tasks without the original `ServiceStopped` cause
(prefetch 2 produces two HTTP requests but zero confirmed private responses),
and V3 StreamStats lacks the original embedding `models` / journal-audit fields.
The coordinator assigned both to this lane after fixing the cache candidate.

Seven other failures from `tests/test_embeddings.py` were handed to the
coordinator, with the unchanged assertions retained: adaptive live gate;
mocked managed-service lifecycle; real managed service; native quota exception
type; retired internal `_arun` cancellation; prefetch test sharing parent-thread
events with process workers; and image packing instrumentation sharing a
parent-only mutable list. Those failures require separate compatibility or
test-harness adjudication and are not claimed fixed here. Full traces remain at
`/tmp/demiflow-private-embedding-r3.log`; the two related traces are at
`/tmp/demiflow-private-embedding-r4.log`.
