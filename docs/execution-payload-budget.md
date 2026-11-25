# P0 payload resource contract

The eight-MiB `ARROW_BATCH_BYTES` value remains a writer batching target. It is
not a universal single-row ceiling. The target can shrink under an explicit hard
budget. An indivisible row may exceed the target only while fitting all effective
row, batch and memory ceilings; it is emitted alone and never truncated.

`execution.payload_limits` shares the existing native calculation: a row reserves
one sixteenth of the effective native pool and one sixty-fourth of native RSS,
subject to the existing 128-MiB native maximum and existing eight-MiB minimum
default allowance when the physical pool/RSS can support it. Actual smaller hard
ceilings still win over this minimum; it is never a reason to ignore a user cap. The run reserves half its RSS for
the native worker, matching the existing Forge launch policy. The native pool is
bounded by that allowance and `memory_bytes`. Explicit row/batch ceilings only
tighten the result. Default row/batch ceilings are calculated from these admitted
resources; changing a constant is not the compatibility mechanism. Existing explicit
native row allowances remain explicit ceilings and are not replaced by the default
pool/16 estimate. They still obey the finite native maximum, admitted batch/memory
ceilings and process RSS protection. Cgroup headroom is preserved when tightening
the native pool or worker RSS.

For streams the existing queue/worker/materialization residency calculation also
includes source prefetch. If the declared ceilings cannot all be simultaneously
resident, the platform resolves smaller effective row/batch allocations before
source reads or actor/model startup. It finds the largest fitting allocation
under those ceilings; it never increases memory, concurrency or a user ceiling.
The calculation is bounded, deterministic from the frozen graph and run limits,
and scoped to the existing action. Checked value validation, native queues,
source capture, terminal blocks and native queries see the same effective
payload limits. Action-local resolution covers peak payload budgets only.
Cumulative output/request/time ceilings remain owned by RunContext/RunJournal;
a checked controller may project a remaining allowance without replacing that
durable ceiling or subtracting consumption twice on resume. Leaving the action
restores the run payload ceilings. No scheduler, execution
path or recovery mode is added.

`batch_rows` retains the native writer's existing row target when omitted.
Explicit row-count ceilings apply to native packing/queries as well. The set of
explicit budget declarations is part of the run configuration, so resume cannot
silently reinterpret an omitted default as an explicit ceiling even when their
numbers happen to match. Resume retains the same limits, graph and implementation
revision; rejected work cannot obtain a larger budget by starting another attempt.

Arrow logical bytes govern row/batch admission. Borrowed slices retain their
legal offsets; retained parent buffers are charged against the memory allowance,
not mistaken for logical batch bytes. Python-to-Arrow sink conversion batches by
both rows and bytes before allocating a table. Original explicit callback group
sizes are unaffected. Native opaque packing and hydration honor the effective
byte ceiling before writer admission. Output and scratch limits remain enforced
before public sink commit. Models may run to discover an unknown output, but
oversized captured input is rejected before model calls; output failures do not
publish or advance confirmed progress.

These are payload/native-pool admission guarantees. Python/library internals and
process-wide RSS still require the existing process guard and RSS headroom;
Arrow logical bytes are not a process RSS measurement. P3 grouping, stage sinks,
lookup and document adapters remain outside this change.

## Output byte budgets

`max_output_bytes` limits what an action publishes: Lance writes and merges, file
and object sinks, key-value sinks, and queue results and raw objects. The result
blocks of `materialize()` and `take_all()` stay in the run's own artifact store;
their bytes count toward `max_scratch_bytes` (as `artifact_staging_bytes`) and their
rows toward `max_output_rows`, not toward `max_output_bytes`.
