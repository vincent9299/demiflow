# Bounded joins and materialized handle lifetime

The public Dataset join previously registered an Arrow RecordBatchReader as an
in-memory Dataset. That consumed the complete input and carried full payloads
through the native hash join. It also rejected empty partitions, and treated a
resident output-row allowance as a cumulative lifetime output-byte limit.

`kernels/join_spool.py::join_pairs` now writes each original Record once to one
random-access file owned by the original RunJournal artifact authority. Hash
indexes contain only keys and offsets. An oversized index recursively partitions
by relinking offsets; payload bytes are not rewritten or globally sorted. A hot
key uses a bounded scan, preserving duplicate cartesian multiplicity, null-no-match,
missing-field errors, exact typed keys and all Record provenance. Its output order
is deliberately unspecified, like the public ordinary join contract.

Each write reserves its next extent before physical growth. The original staging
lease and journal recovery remove scratch only after the owning process exited;
there is no additional allocator, commit log or orphan-directory age heuristic.
The hash-index and row-codec windows derive from the assigned stage residency,
intersected with the run ceiling. These bound managed buffers and encoded rows,
not every Python allocator overhead or process RSS. A single row that cannot fit
its simultaneous encoded/decoded window is rejected rather than exempted.

Finite relational output uses a resident per-row allowance, while its cumulative
row ceiling and downstream native output budgets remain enforced. User flat-map
and batch invocations retain their cumulative expansion allowance.

Materialized handle release is a driver-only consumption of the exact original
owner pin. It may run after the execution context closes; ordinary worker unpin,
publication and progress still require the current fence. Release never deletes
immutable objects, refunds their storage or changes another owner/historical pin.
An unknown release result remains retryable rather than marking the handle closed.

## Evidence

Pinned isolated tree `gpt_runtime_v3_review_20261115`; no main-checkout edits/tests.
`/tmp/demiflow-join-release-r3.xml`: **45 passed in 126.86 s**. The command used
tree-local `PYTHONPATH`, private resource/Forge roots and `/tmp/demiflow-*` output:

```
tests/test_runtime_v3_join_spool.py
tests/test_runtime_v3_materialized_release.py
tests/test_runtime_v3_k.py
tests/test_async_materialize.py
tests/test_runtime_v3_file_recovery.py
tests/test_dataset_native.py::test_typed_writer_avoids_row_decoding_and_preserves_nested_nulls
```

The new cases prove write-before-admission rejection, true killed-process cleanup,
bounded recursive partitioning without payload copies, a real wide Dataset join
under a 16 MiB run grant, handle release after scope exit, preservation of another
owner and native commits, rejection of a wrong pin and retry after unknown reply.
Earlier 146-test relational composition gate is `/tmp/demiflow-relational-r3.xml`.

This delivery does not assert full API, performance, multi-host Ray, or complete
product acceptance. Worker terminal/finite temporary spools still need to consume
the same physical staging budget; that identified follow-up is not disguised as
already solved by the join scratch implementation.
