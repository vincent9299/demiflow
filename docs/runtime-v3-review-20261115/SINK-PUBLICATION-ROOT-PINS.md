# Publication root pins: narrower writes, unchanged historical authentication

Candidate: `e8bc413c92d8852ccb1864e10a573fce58bec82d`, branch `engine/gpt-v3-residency-20261115`, worktree `/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_v3_residency_20261115`. The only production change is `_DataflowArtifactSink.prepare` in `execution/dataflow_sinks.py`. This follows coordinator approval of the root-pin proposal. The main checkout, shared environments, GC protocol, original journal, schemas, identity functions and frozen seven files are unchanged.

Each cut formerly traversed the whole terminal prefix and retained every descendant under that cut's owner. The measured 50-cut workload produced 2,514 owner rows. The new implementation still traverses every index and inspects every referenced object for existence, size and SHA; it retains the publication object as the cut's original owner root. The same workload now produces 50 owner rows. No historical digest check is skipped, no cache of business payload is introduced, and no separate ownership ledger is added.

## Why the original GC protects the descendants

- `dataflow_gc._Trace.roots` reads every native commit intent/prepared descriptor, receipt, action and state carrier. It also obtains typed references for `artifact_owner` roots from the original staging publication facts.
- `_STRUCTURED` already includes sink publications and terminal segment indexes. `_Trace.walk` follows their typed child references and authenticates stored bytes, including nested record payload/schema references. An unknown carrier, missing/corrupt object or exhausted scan budget blocks collection.
- `_exclusive` requires the original physical object lease and confirmed cleanup of every original allocation. Active writers/readers prevent collection. This preserves the gap before the publication owner is written. After `prepare` returns but before its descriptor is recorded, the publication owner alone protects the whole tree.
- `abort` is unchanged. Its full-tree unpin remains idempotent when new descendants have no direct pin and still releases old-version descendant pins. A committed publication remains protected. There is no schema migration or deletion of historical pins.

This removes repeated owner writes, not the full-prefix read cost. Authentication remains proportional to the prefix size on each cut; avoiding that would require a separate justified integrity design.

## Verification

**77 passed in 183.45 seconds**, with no skips/failures. The source-bound runner verifies the loaded checkout and records 1,798 input digests. Every digest matches the candidate commit; before/after source changes are zero. All seven frozen files byte-match `362d8528c27ae60a0d6ae0ccc986a0ff93847de8`.

The selected tests are:

- 9 new tests using the real artifact store, native journal and commit coordinator: owner-only reachability before prepared recording, prepared/ACKed publication roots, two historical cuts after GC, same-size leaf/index tampering, missing leaf/index rejection, and old-pin abort compatibility.
- The existing published-GC suite, including unknown carriers, open readers, physical lease identity, budget limits and interrupted reclamation.
- Actual scalar/Arrow value preservation and public Dataset materialization/readback.
- All 6 actual process-kill commit windows, with original ACK/recovery artifacts preserved.

The new pre-record and prepared tests inject loss at the original coordinator fault point and then perform real quiescent GC; they are not represented as process-kill tests. The separate six-window suite supplies actual process-kill evidence.

JUnit and verification: `/tmp/demiflow-sink-root-pins-bound-r2.xml` and `/tmp/demiflow-sink-root-pins-bound-r2-evidence/verification.json`. The companion JSON binds these files, all source digests and 144 original recovery JSON/SQLite artifacts.

## Actual fast CPU preflight

The unchanged harness from `602f7ea` ran 20,000 narrow rows → arithmetic map → odd filter → projection → 10,000 public results, 4 workers and 256 MiB declared memory. Every arm has the identical input digest, complete independently checked output digest, 50 native ACKs including one final ACK, no Ray Data import, no remaining observed child process and no active original resource reservation.

| Fixed version | Total | First public result | Original owner rows |
|---|---:|---:|---:|
| Intermediate reference `5ff5da9` | 36.178 s | 6.255 s | 2,514 |
| Strict codec `0d06e8a`, measured tree `8f85cf2` | 33.539 s | 5.506 s | 2,514 |
| Root pins `e8bc413`, uncontended by this lane | 32.662 s | 5.826 s | 50 |

These are single diagnostic trials, not the formal five alternating pairs. The root-pin result is about 2.6% faster than the codec trial and 9.7% faster than the intermediate reference; the first-result measurement did not improve. This does not prove statistical significance or satisfy the overall performance target. Historical `267b914` was still much faster in the prior harness report.

The first root-pin trial was correct but took 34.237 s while this lane ran per-file Git digest verification. That overlap contaminated the comparison. Its receipt is preserved and excluded from the speed comparison; the second run had no concurrent task from this lane. Other coordinator lanes remained autonomous, so this still is not a controlled whole-machine benchmark.

Final receipt: `/tmp/demiflow-perf-sink-root-pins-e8-r2/fast_cpu/trial.json`. The sampled owned process-tree RSS peak was 448,581,632 bytes, FD peak 38 and 3 processes; these are actual sampled observations, not a claim of a 256 MiB hard RSS bound. The unchanged harness uses the original 4 GiB RSS ceiling and 256 MiB declared engine memory.

The old driver profile also contains about 50,000 per-row control sends. That larger cost remains for the coordinator to assess. No control batching, checkpoint weakening, larger limits or changed workload is part of this patch.
