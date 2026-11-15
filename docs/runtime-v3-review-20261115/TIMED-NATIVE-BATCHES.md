# Timed native batch execution and recovery

The public `flush_interval` now advances through the original worker kernel poll loop. A batch flushes on the declared size, timer, schema boundary or input completion; transport blocks and checkpoint cuts do not choose business batch sizes. The checkpoint carries bounded **remaining milliseconds**, rearmed against the new worker clock. A pending historical timed batch with no relative timer evidence is rejected.

SaveLance and Embedding bind the invocation's ordered record IDs and encoded payload hashes before running effects. These versioned input facts live in the original `RunJournal.runtime_sources`, with atomic original-journal reservations for batch count, member count and metadata bytes. Only sealed native batch nodes and their physical partitions may read/bind them. During replay the kernel restores the original group boundary, so changed input speed cannot regroup an already-confirmed request. A changed member/payload fails before another publication/request. Effect receipts, HTTP known/unknown decisions, budgets and commits remain owned by their existing native authorities; these input facts do not prove effects or authorize retries.

The writer uses the original child commit scope and CommitCoordinator. Recovery walks the original scope table via an expression index and `LIMIT 1` pages in node/ordinal order, instead of loading all historical requests. Schema metadata uses `record_schema.encode_schema`; retained Python rows and Arrow buffers share one batch ceiling before publication. Earlier confirmed microbatches remain visible after a later failure; this is not whole-action atomicity.

Actual evidence in the isolated artifact recovery branch:

- Removing only the new timer poll behavior makes the downstream-ready BatchMap counterexample fail: `/tmp/demiflow-batch-ready-red.xml` (one expected red). This excludes cold worker startup from the timing claim.
- `/tmp/demiflow-timed-native-recovery.xml`: 6 passed. Local BatchMap, SaveLance and Embedding deliver the first batch before a slow source's EOI. Save and Embedding then survive real `os._exit` after the first native confirmation and before a parent cut; resume supplies faster input, preserves groups `[1, 1]`, and produces writer versions `[1, 2]` / exactly two HTTP requests with original request budget consumed = 2. The sixth test proves copy coexistence rejection preserves the old target.
- `/tmp/demiflow-timing-restore-extra.xml`: 3 passed. Changed payloads after that crash are rejected with no extra HTTP/write; a real parent cut stores a bounded relative timer and resumes its one buffered row without duplication.
- `/tmp/demiflow-timed-ray.xml`: 3 passed on real Ray Core workers. The same three public timed pipelines deliver before EOI.

The tests use local owned HTTP transport and `/tmp/demiflow-*` state. They do not establish two-host, GPU, arbitrary UDF effect exactly-once, or single-worker source/batch fusion support. A blocking source and a timed batch cannot share one business thread; the coordinating branch owns that fusion admission rule. Full API, legacy and integrated workbench checks remain separate acceptance obligations.

### Integrated lifecycle verification

The readiness probe delegates to real `DataflowExecution.start` and only then
announces that physical endpoints are active. User actors still start lazily on
first input. The recovery pipeline has five independent stages (source, Slow,
batch/native operator, Observe, terminal); the test explicitly supplies eight
worker slots. The production default is four (`DataAPI.__init__` and
`LocalDatasetExecutor.__init__`), not one. Insufficient-worker rejection/fusion
is tested separately by `test_runtime_v3_single_worker_fusion.py`.

On root-integrated `c28f5c8`: the three real first-output cases and all five
crash/restore/drift/timer cases pass with the corrected physical readiness
probe. The original before-EOI, confirmed-native-before-parent-cut, changed
payload, exact two requests/charges and relative timer assertions remain.
Evidence: `/tmp/demiflow-timer-integrated.xml` (first three plus fusion pass;
old implicit-resource recovery cases fail honestly) and
`/tmp/demiflow-timer-integrated-r2.xml` (all five recovery cases pass).
