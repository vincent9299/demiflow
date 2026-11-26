# Lance partial-column merge

Business code states the key, the ownership of columns and the behaviour for missing keys with `Dataset.write_lance(mode='merge', on=..., update_columns=...)`. Demiflow manages the physical write strategy; the Dataset API has no `rewrite_rows`, `rewrite_columns` or engine session parameters.

## Automatic selection

The Linux local executor takes the exact row count, including a scan limit, from a direct Lance scan of a pinned version without filter or expression projection. It reads row-count metadata only, does not run Python callbacks early and does not scan the filtered data an extra time. This internal hint does not enter `LanceWriteSpec` and does not change the request identity, the business configuration or the commit receipt.

The platform selects the column rewrite of Lance 12 when all of the following hold:

- The missing-key policy is the default `error`; only existing rows are updated.
- The source contains at least one non-key column and omits at least one target column.
- The known source row count is greater than 0 and not greater than the target row count.
- Neither the fields carried by the source nor their target fields have a Blob marker or an Arrow extension type, nested fields included.

The column rewrite reuses the data files of the omitted columns and writes replacement column files only for the fragments it touches. A replacement file covers a whole fragment, so a small update can still write a lot. The earlier 50% row-count threshold was removed on 2026-10-03: a row ratio says nothing about the byte cost of the omitted wide columns, and a document-relation patch of about 39% once fell back to the whole-row path and triggered the 32 GiB process-tree guard. The current choice prefers not reading payloads that are not updated and accepts fragment-level write amplification for sparse updates; it does not claim optimal I/O in every case.

Sources of unknown size, streams that went through callbacks or relational computation, filtered scans, inserts, whole-row updates, special fields and other operating systems keep Lance's native `auto`. Native `auto` can itself choose a column update through an index, so it must not be read as "always rewrite whole rows". An ordinary partial-column update without an index usually rewrites whole rows. The platform logs the actual choice and the source and target row counts; the choice of strategy is not handed to business configuration.

## Correctness and resource bounds

Both strategies share the existing checks for key uniqueness, non-null keys, column sets, types, missing keys and the committed version. The automatically selected column rewrite prepares an uncommitted transaction in a separate worker and returns only the transaction metadata; the original writer verifies the prepared version and performs the single commit. A failure publishes no partial result, and choosing a strategy never replays callbacks or retries a commit automatically. Old Lance versions are kept; a failed native preparation may leave unreferenced data files, so the existence of a file does not show that a commit happened.

This step is a Lance storage write, and Lance has its own internal sort pool. The automatic column rewrite shares the platform's admission slots, CPU and RSS budgets and cgroup headroom checks with Dataset relational tasks. The current large-container configuration is a sort pool of at most 12 GiB, an RSS budget of 24 GiB per admission slot and a 128 GiB guard on the temporary directory, scaled down with the container's capacity. For a patch known to have at least 2,000,000 rows the merge takes up to two existing slots exclusively; the corresponding local RSS guard is 48 GiB, the total reserved capacity does not grow, and other native tasks sharing the pool wait. A container with one slot still uses one. Lance sorting and fragment writing use single-way CPU concurrency to reduce the buffers of simultaneous merging and fragment writing. The budget is set only in the child process environment; the main process environment is not changed and spilling is not turned off. The pool budget is neither an amount reserved in advance, nor a minimum requirement, nor a cap on the whole process; the RSS sampling guard is not a hard kernel limit either.

When admission, a timeout, RSS, disk space or the cgroup guard fails, no transaction is committed and there is no automatic switch back to the whole-row path, which could take more memory. The worker is terminated when its parent exits, and the temporary directory is cleaned afterwards; unreferenced staging files written to the target directory do not count against the temporary-directory limit. Paths this does not apply to still run as before; it cannot be claimed that every Lance merge is isolated or resource-bounded. Detection of duplicate source keys keeps at most 100,000 keys in memory and then turns to a temporary SQLite unique index using the full typed key rather than a hash, with an 8 MiB page cache and mmap disabled; one conversion handles at most 8,192 keys. It keeps detecting duplicates across batches and across the spill, and is removed at the end; it is neither a business result nor a new public dependency. This bound limits the number of resident keys; it is not a hard limit on a single huge value or on the RSS of the whole process.

## Verification

`tests/test_lance_merge_strategy.py` uses real temporary Lance tables to verify the strategy boundaries for pinned scans, limits and filters; original fields and old versions; file reuse; composite keys and nested nulls; that missing, duplicate and null keys fail without a commit; and that a target Blob marker is not hidden by the source schema. It also covers late duplicate keys across the memory/disk boundary, composite binary keys, cleanup of the temporary index, a commit race after preparation, termination by the RSS guard and mutual exclusion over several slots. Reviewed together with the existing regressions for partial-column updates, Arrow copying, write modes, memory guards and native relational execution, 82 tests pass.

A comparison on real data used one pinned wide table and patch: 1,166,309 images with a SHA below `10` and 768,884 patch rows, each run in its own child process on its own shallow-cloned table. The old whole-row strategy was forced only inside the test; normal business always uses the automatic selection. The data and test scripts are not everyday business dependencies.

| Comparison | Whole-row write | Final automatic strategy |
| --- | ---: | ---: |
| 1,166,309-row base table, 768,884-row patch | 24.184 s; process peak 13.367 GiB | 21.554 s; worker peak 4.934 GiB, parent peak 0.192 GiB |
| New data file bytes, sample | 1,352,762,414 | 684,007,833 |
| New data file bytes, full | 20,894,149,716 | 10,814,413,761 |

All 22 fields of the sample agree. The durations include process start-up and I/O on a shared host, and the final sample overlapped with a read-only acceptance run, so they are not a promise of undisturbed performance; an early probe of 8.119 s did not include the final single-way writing, key spilling and resource configuration and does not represent the current default strategy.

Final full run: an 18,437,838-row base table and a 12,163,256-row patch took 290.503 s; worker peak RSS 45.321 GiB, temporary directory peak 30.696 GiB, 18 original data files kept. Compared with all 22 fields of the production image table @2, fully aligned, the difference is 0. The old whole-row merge was not timed separately, and the old 94.9 GiB figure is one process observation rather than a full peak, so no exact full-scale improvement ratio for time or peak is reported. All tests wrote only to separate tables; the production public version stayed @2.

Earlier failures — default sort pool exhausted, 8 GiB pool exhausted, the 24 GiB RSS guard triggered — all kept the original version without a partial commit; the system oom/oom_kill counters stayed 0. A controlled test also verified that while both slots are taken no other native task may enter and that it can continue after release.

The full receipts are kept in the project's internal acceptance records.

The implementation rests on the `MergeInsertBuilder.write_mode` documentation of the local pylance 12.0.0 and on [Lance v12's native column update](https://github.com/lance-format/lance/blob/v12.0.0/rust/lance/src/dataset/write/merge_insert.rs); strategy selection and resource management are done by Demiflow's adapter layer without changing Lance or DataFusion sources.

2026-10-04, a real run restoring document associations: on a 22,301,207-row target, an 8,738,644-row QID patch and a 20,668-row downloaded-concept patch were published in turn in 67.12 s in total; the sampled peak RSS of the process tree was 7,632,039,936 bytes (7.11 GiB), the outer 32 GiB guard held, and the public version went @19 → @20 → @21. No body objects were re-read and nothing was fetched over HTTP. The whole-row path had triggered the guard before finishing, so no full speed-up factor can be computed; the lower peak this time speaks only for this narrow-patch case and does not cover updates of arbitrary wide fields.

## The low-memory goal not yet reached

The full run succeeded inside the 48 GiB process guard, which does not mean it can finish within 8 GiB of total memory. There is real external sorting within the pool budget, but some sort-merge and fragment-write buffers cannot spill further, and Arrow, Python and native allocations can also sit outside the pool. Stopping the merge when the guard triggers is a failure guard; it does not turn all the excess memory into disk and then finish.

Next to verify: match only keys and fixed row numbers, leave large fields in the original Lance files and take them back in bounded batches; prepare updates fragment by fragment and commit one transaction at the end. Lance 12 offers the combination of `fragment.update_columns` and an uncommitted Update transaction, but its in-fragment hash join may still need significant memory, so the existence of the interface is no ground to promise low-memory completion. A reasonable minimum budget must also hold the single largest record and the necessary buffers; with a smaller budget the answer should first be more chunks, more spilling and less concurrency, and a clear refusal when measurement shows the budget cannot be met — running in any amount of memory cannot be promised.
