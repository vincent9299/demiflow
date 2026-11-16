# Ready peer data reads: bounded candidate, functional evidence

Production commits `d9ad440` + `dce26e3`, isolated residency branch. Parent owns RayHostEndpoint asynchronous mailbox and terminal batch selection integration. This candidate does **not** claim performance or full real-host recovery completion. Actual Local diagnostics are recorded in the follow-up below; integrated Ray/recovery and formal paired performance remain pending.

## Original execution and authority

`WorkerGraphRunner._ready_peer_commands` selects only already queued consecutive memory data commands from the same edge, producer partition, route epoch, and peer endpoint. It never dequeues or fetches future payloads. The window is the minimum of 32, live producer capability, native credit rows, max_pending_events, credit bytes and max_resident_bytes. Every locator row length is checked before constructing the read generator. Controls, finite input, EOI, changed authority and invalid size stop selection.

`_step_ready` shares one original `read_peer_many` generator for that bounded metadata selection while continuing **one original step(max_records=1, deadline_ms=50) per record**. Original kernel poll/push/source progression, delivery validation, monotonic memory frontier, per-record typed decode and original ACK remain. Each record's allocation and sequence are validated before fetching or executing it. A priority close, cancel, changed queue head, elapsed 50 ms, or exception closes the generator in finally. Unconsumed commands remain queued. No batch fills by waiting and no list of raw payloads is collected.

`peer_batch_limit(locator)` interprets the optional **non-persistent** max_batch_records capability: strict int 1..32; missing/invalid values safely return 1. It cannot enlarge the consumer's own limits. Historical locators remain readable. No sealed schema, semantic identity, cut/commit or durable journal formula changes.

## Finite transport resources and release

The existing PeerBuffers TCP server accepts at most four connections, using a fixed I/O-only worker pool. The original listener/base connection keep their provider lifecycle. Additional three descriptors are granted via the original store.file_slots authority, only when the initial available window also leaves store.PUT_FILES headroom. If no grant is available the original single connection is retained and capability is 1. A semaphore is taken before accept, including queued sockets; there is no unbounded request thread creation or second resource allocator.

Senders transmit the size header and a memoryview of the original immutable credit-owned payload, without header+whole-payload copying. TCP_NODELAY avoids a delayed-ACK wait after splitting the header. A borrowed sender reference remains counted; PeerBuffers.release first retires the key against new sends and waits for existing send references to disappear. Original channel credit and delivery ownership are released only afterward. Timeout leaves those owners charged. read_peer_many drops its prior yielded raw reference before constructing the next frame; only one frame is yielded, not a materialized batch. The existing decoder/read-copy workspace is unchanged, while source buffers remain charged to their original credits.

close stops accepting, signals all owned active sockets, drains the bounded accepted-request queue, and joins only owned I/O threads within the original timeout. Only confirmed worker exit permits clearing buffers and releasing the FD grant. The acceptance-stopped event prevents workers leaving before the last accepted request is queued.

## Verified and pending

Fixed d9ad440: **39 passed in 40.40 s**, original B20 plus new ready-peer14 and existing peer5. Companion source-bound receipt covers 1808 production/test inputs, same HEAD and bytes before/after, clean checkout, interpreter/path assertion and exact selectors/JUnit digest. Fixed dce26e3 helper extraction: **14 passed in 16.88 s**, likewise source-bound.

New tests use real sockets, original RunArtifactStore file slots, and the original native compiler/store/channel/kernel/worker authorities (only their existing in-process network-boundary fixture). They establish four records over one connection with four original step calls and four ordered ACKs; second-frame digest/sequence/business failure preserves only the first success; cut boundary and a slow first callback stop without dequeueing the suffix; low FD and historical/invalid capability use single reads; one blocked 4 MiB sender does not block another reader; ACK cannot free its borrowed bytes; full connection-window shutdown releases the original grant and terminates all I/O threads.

These are substantive component tests, not actual two-host or GPU claims. At this component-proof revision, real integrated Local/Ray, nonfinal cut/recovery and actual performance were pending; the subsequent Local diagnostics are recorded below. The preceding control-only two-host fast trial failed its original 120 s budget and remains red; this candidate must be judged using unchanged workloads, worker/memory limits and timeouts, not microbenchmarks. No main checkout or shared environment was changed.


## Follow-up: exact raw lifetime and actual Local diagnostics

`cf974fe` removes the yielded raw frame from the suspended generator entirely (`yield _read_peer_frame(...)`), retaining original per-frame length/SHA validation. Thus the previous raw cannot survive across the next native kernel poll. `fcde31d` validates the actual received object's unique consumer ownership (`sys.getrefcount == 2` including the counting call). First probe r6 was **18 passed / 1 failed** because enumerate caches its `(index, raw)` tuple and adds a consumer-side reference. The test now uses direct next and explicitly checks final StopIteration/socket closure; the ownership assertion is unchanged. Fixed r7: **19 passed in 17.58 s**, source-bound and clean. The failed evidence is retained. Earlier actual Local process early-close/second-read-error plus original terminal-close tests: **6 passed in 15.69 s**, source-bound a538eb3.

Fixed combined candidate **266aa548db029d8a2af6b1017b4a38872a6b5545** adds parent's asynchronous Ray mailbox (`6e1020c`), terminal capability/authority checks (`dc3572c`), canonical document/ASCII improvements (`1e9fb87`, `218c3b5`), and native/Prompt lazy imports (`fdffa2e`, `92d9474`) to this peer candidate. Parent/regression/performance lanes released their heavy-test windows before these diagnostic runs; the shared machine and unrelated workloads were not controlled.

| Original Local workload, four workers, 256 MiB, 120 s | Elapsed | First row | Output / original final ACK |
|---|---:|---:|---|
| fast_cpu, 20,000 inputs | 26.982835 s | 4.076723 s | 10,000 correct rows, 50 total ACKs including final |
| finite_sealed p1, 40,000 regular 8 KiB rows each side plus original null/duplicate rows | 113.739444 s | 63.669495 s | 40,002 correct rows, 152 total ACKs including final |

Both trials passed unchanged independent value/multiplicity digests, native resource inventory and physical cleanup checks: original pool `active=[]`, `charged={}`, observed descendants empty before/after driver exit, return code 0, no observation errors, source before/after equal and clean. Full exact configuration, original journals, snapshots and sampler output remain under `/tmp/demiflow-perf-ready-peer-local-r1`; trial hashes are in the companion JSON. The sampler observes the entire owned process tree, not a hard RSS guarantee. Measured peak RSS was 297,717,760 bytes (fast) and 264,966,144 bytes (wide); worker row/state budgets do not claim an equal whole-process-tree RSS cap.

These are **one diagnostic per workload**, not the five paired formal repetitions. Narrow-chain time remains about 14.4 times the earlier historical 1.872 s reference. Wide completion has only about six seconds of timeout margin. No same-version Ray/two-host performance claim follows from these Local passes. The performance intent is still open. This lane released the timing window immediately afterward; parent owns integrated actual Ray/recovery acceptance before another performance decision.
