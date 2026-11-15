# Streaming terminals use the same public execution envelope

`run_stream` previously resolved only run defaults, silently ignoring the worker
and memory limits chosen by `local_execution`. `resolve_action_runtime` now
provides the single adapter for collecting and streaming terminals. It preserves
explicit row/batch limits and the original allocator; it adds no budget ledger.

Real 1-worker and 2-worker runs under the unchanged 128 KiB envelope verify
original allocation charges, six committed output records, and ACKed commits.
An indivisible concurrent stage fails before an upstream worker can launch.

Source co-location also excludes operators with a positive `flush_interval`.
A blocking source pull and a timer in the same business thread cannot guarantee
incremental flushing. Timer-bearing operators keep their own scheduling boundary;
an infeasible resource declaration cannot silently change the timer into EOI
flushing. This guard is a placement check, not evidence for the separate native
timer/crash-recovery implementation.

Validation: `/tmp/demiflow-root-stream-budget-r2.xml`, **4 passed / 38.67s**,
using the isolated tree and pinned interpreter/PYTHONPATH. The first diagnostic
incorrectly used ordinary `map(...).run_stream()` and hit the existing public
preflight guard; the corrected test uses actual `map_async(concurrency=1)` and
retains every original worker/memory/output assertion. That failed diagnostic
remains `/tmp/demiflow-root-combo-c048f43.log`.

The same integration diagnostic found a separate real failure combining logical
task provenance with block copying. Its 17 failures are tracked for a common
strict Record-copy fix, not waived by this four-test result. Explicit native
query limits and the new thread host remain separate integration work.
