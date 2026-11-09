# Native stream barrier checkpoints

This is the `aligned-barrier-v2` extension of the native execution recovery
protocol. It uses the existing local stream executor, `RunContext`,
`CommitCoordinator` and `run.sqlite`. The historical phase descriptions in
`execution-foundation-contract.json` describe their original scope; this document
describes this extension. Unsupported graph shapes and remote backends remain
subject to the existing capability gate.

## Consistency and ownership

| Layer | Permitted | Required |
| --- | --- | --- |
| Pure intermediate computation | Recompute unconfirmed work; existing unordered output | Preserve values, duplicate multiplicity and explicit fixed batch boundaries |
| Native HTTP model calls | Reuse a durable response; send a previously unreserved occurrence | Persist intent before dispatch; never automatically regenerate an unknown external result |
| Mutable actor state | Local alignment at a safe point | One snapshot per actual shared semantic owner; consistent restore |
| Output segments | Stage immutable generations before global confirmation | Durable prepared descriptors and original sink publication contract |
| Queue processing | Replay an unsettled delivery under the existing delivery protocol | Results persist before ACK; checkpoint progress alone does not complete a task |
| Metrics | Eventual observations and bounded summaries | No metric used as a substitute for a durable commit fact |

There is no global weak-consistency switch. An arbitrary Python callback can
repeat on recovery and does not acquire an exactly-once side-effect guarantee.
Multiple external sinks retain their existing partial-visibility/reconciliation
contract; a barrier does not create a distributed atomic transaction.

Source occurrences propagate through expansion as parent identity plus child
ordinal. Fixed batches have a stable batch ordinal. These identities distinguish
duplicate input records with identical contents. Model request identities retain
the provider, endpoint, template and actual payload, and add the native occurrence.
Changing the plan, code, limits or source identity is not a compatible resume.

## Flow and lifecycle

`CheckpointControl` owns a single pending checkpoint. Row and time triggers while
it is pending coalesce. They do not force the source to wait for publication.
Source input waits on queue capacity, explicit unconfirmed row/byte admission,
failure, or final completion. The controller has an independent timer and inserts
the marker under the same gate as source input, preserving channel order.

Each barrier moves through `created`, `aligning`, `sealed`, then `published`,
`failed` or `cancelled`. Its independent deadline starts at creation, including
time spent inserting a marker into a full queue. A failure releases local gates
and completion waiters with the same cause. A suppressed trigger is not a failure.

Ordinary or opaque actors finish their current local calls before snapshotting.
After the local snapshot and marker forwarding, that stage can continue while
downstream stages and publication proceed. Shared mutable actors extend the local
alignment group to the last owner. Sharing an HTTP pool or immutable object store
does not create such a group.

Native row-local HTTP prompt stages expose a capability from the model adapter.
The generic executor records bounded input blocks and occurrence identifiers for
in-flight calls, never futures, sockets or thread handles. One worker can snapshot
and forward the marker while other calls are outstanding; free workers then
continue. An emission gate assigns each completed output to exactly one generation.
Final markers wait for all native calls. Stateful filter owners retain local
alignment rather than replaying already-applied filter state. For a native row-local
call with pre-filters or a predicate, its pending input copy precedes any mutation,
so replay applies those mutations once.

The model adapter uses the existing SQLite prompt journal's group writer in the
run database. Text responses and image references use the same mechanism. There
is no per-image `CommitCoordinator`, result block or marker file. Request/result
JSON is bounded; image pixels live in the owned CAS. Known responses protect their
object registrations before the response becomes visible. A response may be
recoverable even if its output checkpoint was never prepared.

At recovery startup, an index of unresolved calls rejects unknown results before
new provider work starts. A durable transport fact confirming cancellation before
dispatch permits another reservation, archived in the existing `recovery_events`.
The original request charge is retained. Absence of a response alone is never
proof that a request was not sent. When live responses temporarily occupy the
control budget, new admission waits asynchronously so those responses can save;
a permanently insufficient budget fails before dispatch. Replayed ObjectRefs and
call metadata preserve the live output structure, including nested Arrow schemas.

Cooperative cancellation stops request admission while allowing already received
image bytes to finish local validation and durable storage under the same fence
and resource/time budgets. The Forge supervisor signals the worker first for a
user cancellation; it retains the original grace deadline and then kills the
entire owned tree. Resource violations still stop the tree immediately. Result
storage during drain does not publish a row or authorize another provider call.

The final barrier includes fixed-batch tails and `recovery_rows` output, including
an empty or exhausted source. Source EOF alone does not imply sink or ACK completion.

## Resource admission and recovery

| Resource | Bound and behavior |
| --- | --- |
| Pending checkpoint | One; later triggers coalesce |
| Unconfirmed source | Defaults: 4096 rows / 64 MiB; actual encoded-size admission and queue backpressure |
| Checkpoint triggers | Defaults: 512 input rows / 1000 ms; independent 60 s checkpoint deadline |
| Actor and buffered state | Aggregate `max_state_bytes`, including fixed-batch and native in-flight inputs; estimate before encoding/copy |
| State copy and encoding | Off event loop while semantic ownership is held; fixed reservation includes up to three state-budget representations |
| Model journal writer | 64 operations per batch, 5 ms flush, 32 MiB queue, individual records at most 16 MiB and the run control budget |
| Control data | `max_control_bytes`; request intent reserves response capacity before HTTP dispatch; spent request budgets are not refunded |
| Artifact inventory | Indexed counters and ownership in the same journal; defaults retain 65536 live registrations and 16384 commits |
| Object/block manifests | Pages bounded by bytes and entry count, authenticated ordered index; result handles page commit references |
| Validation proofs | At most 256 file proofs; inode, size, timestamps and hash binding; very recent files are rehashed because timestamp precision may coalesce writes |
| Directory durability proofs | At most 512 per store; new shard directories and their durable ancestor are synced |

The existing terminal writer coalesces callers already waiting to append, with
at most 64 admitted row references. Each caller retains ownership until its row
has been serialized. A blocking submission holds at most 64 rows and the larger
of the declared batch/row byte bounds; it adds no source lookahead or batching
timer. The caller's worker budget already accounts for the referenced row payload.
Cancellation joins a started submission before the writer can close.

Platform payload estimates are not hard process RSS limits. User callbacks that
construct a list have already allocated that list before returning it; the stream
checks its byte budget at the earliest return boundary. Iterative expansion
consumes one child under downstream backpressure. Arrow, decoder and other native
allocations require their existing process guard and conservative reserves.
JSON escape expansion is counted in bounded chunks before allocating the full
encoded state token. Decoder input and result metadata reserve space before the
isolated process starts.

The journal keeps trigger-maintained item/byte counters and protected-reference
indexes. Temporary deletion releases only the producer's bounded list of absent,
unprotected paths under the current fence. It does not scan all previous commits
or the directory tree for each object. Preparation traverses at most one manifest
page and 64 path names at a time, outside write transactions. Provisional reference
ownership is separate from allocation ownership; the final indexed protection
update and prepared fact share one transaction. Failed traversal leaves provisional
files unprotected. Quiescent orphan cleanup pages unprotected paths;
it never deletes prepared or known-effect objects.

Recovery locates the latest commit by index, reconciles a prepared commit before
new graph work, and restores its cumulative progress and state. A pending intent
retains its exact input cut. The result handle pages historical commits only when
their results are consumed. Full result iteration necessarily reads all requested
results; each consumed generation verifies its native row and object publication
receipt before exposing blocks. This is not part of per-row admission or
latest-state lookup.

Native publication uses a bounded OS lease shared with owner replacement. File
I/O does not hold the SQLite write transaction. Every control mutation verifies
the attempt/fence. Cancellation stops admission and drains native calls/writers
under their deadlines. If a terminal producer outlives the drain budget, its run
lease and resources remain quarantined until it exits; a replacement owner cannot
publish concurrently. This includes bounded `take` and iterator early-close.
The isolated Forge worker caps glibc allocator arenas at two, preserving its
existing memory ceiling when native persistence adds threads. Python threads cannot be forcibly preempted safely. User
callbacks must bound their blocking I/O; no checkpoint timeout claims otherwise.

## Terminal behavior

| Existing supported terminal | Behavior |
| --- | --- |
| `materialize()` | Same executor; durable output segments; lazy repeatable result handle after success |
| Direct Lance/datasink | Bounded internal stream-to-sink handoff; one graph execution; original final publication and overwrite/append contract |
| `take`, iteration and `run_stream` delivery | Existing delivery limits and early-close semantics; handed-off object references retain their original ownership contract |
| Queue producer | Existing prepared publication and producer recovery |
| Queue consumer / results / postprocessing | Existing delivery, result commit, ACK and postprocessing identities and ordering |

Direct terminal recovery inspects the parent prepared fact first. It resumes the
internal producer only when more durable segments are needed. It does not invoke
known paid calls merely because a final writer failed. The final receipt adopts
the child stream’s object manifests under the same immutable CAS owner, so
prepared-first inspection verifies image bytes as well as row segments. Explicit public checkpoint
orchestration APIs remain retired by the shared capability gate; applications use
native run recovery.

## Comparison with Flink and validation

The design was compared with Flink revision
`2e89b879f1e99612c92be845ec53c90efeb77b0d`, including
`AsyncWaitOperator`, `SingleCheckpointBarrierHandler`,
`SubtaskCheckpointCoordinatorImpl` and mailbox lifecycle code. Flink's bounded
async input snapshot is a useful ownership pattern. Its replay of pending inputs
cannot be applied directly to a paid HTTP effect: native response facts or an
explicit unresolved-call failure are necessary here. This implementation does not
copy Flink's cluster scheduler or introduce network-channel checkpoints.

The branch's validation artifacts bind the exact source and raw logs. They cover
local barrier overlap, slow native calls, fixed batches, shared actors, aggregate
state limits, corrupt manifests/blocks, publication fencing, deadlines, EOF,
quarantined writers, indexed historical-load cost, actual loopback text/image
effects and terminal recovery. Managed load probes count requests, control
transactions, group commits, artifact fsync calls, files and registrations. Python
`os.fsync` instrumentation does not count SQLite's internal WAL sync system calls;
those are reported separately by durable batch transactions. PostgreSQL, real
providers and distributed deployments require their own matching evidence.
