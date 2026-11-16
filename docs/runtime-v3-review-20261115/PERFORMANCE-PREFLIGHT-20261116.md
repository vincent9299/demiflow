# Performance preflight: actual regression risk, not a final benchmark

The harness now runs actual public Dataset actions in version-pinned child processes. B0 is the historical `267b914`; B1 is the intermediate new-engine reference `5ff5da9`, **not** an upgrade-before baseline and not the eventual candidate. No production changes or shared-environment installation were made by this lane.

The unchanged workload is 20,000 narrow rows → arithmetic map → odd filter → projection → 10,000 public `iter_rows` results, 4 workers and 256 MiB declared memory. An independent digest validates all values and multiplicity. Both profiled arms are correct, the new-engine result has the original final ACK, original resource charges are zero, and observed owned processes exited. Profile totals are diagnostic and must not be mixed with unprofiled final measurements.

| Fixed version | Run | Total | Public first result |
|---|---|---:|---:|
| B0 `267b914` | driver cProfile | 2.535 s | see original receipt |
| B1 `5ff5da9` | driver cProfile | 58.230 s | 8.620 s |
| B1 `5ff5da9` | unprofiled, external sampler | 36.178 s | 6.255 s |

Receipts, commands, all production input hashes, harness/plan hashes, original ACK IDs, per-process birth identities and allocation snapshots are under `/tmp/demiflow-perf-fastcpu-profile-r1/{b0,b1}` and `/tmp/demiflow-perf-preflight-b1-r2/fast_cpu`. The paired `.json` indexes their exact receipt hashes. `driver.prof` and `profile-top.txt` retain the raw diagnostic profile.

The fixed B1 driver profile spends 42.87 s in `pump`: 53,026 `decode_wire` calls total 17.98 s; `send_business` runs 50,293 times and totals 6.20 s; 49 original commits total 5.84 s, with 2,415 `RunArtifactStore.retain` calls totaling 3.74 s. Initial prepare/start costs 7.88 s. These are cumulative overlapping profile costs, not additive independent stages. The coordinator is examining the actual control path; no test budget or business size was reduced.

An earlier draft sampled `/proc` and directories from a thread inside the measured driver. That could contend for the GIL, so the sampler now runs in the benchmark parent, outside the measured Python process. The unprofiled fixed B1 result remains approximately 36 s. Earlier receipts are retained as diagnostic history; one also saw the coordinator tree change HEAD during the trial and correctly failed the immutable-source gate. They are not final evidence.

No formal five-pair measurement has started. It must wait until the actual regression and final candidate are fixed, with one separate warmup per arm, five alternating repetitions, cancellation, all nine Local classes, and the approved representative Ray/two-host cells. The harness must report missing/unsupported cells rather than claim whole-platform performance from these preliminary measurements.

## Executable workload/guard delivery

`tests/runtime_v3_performance_scenarios.py` implements the nine exact businesses, original-route/ACK observations, version-pinned subprocess execution, external bounded resource sampling, real HTTP/CUDA exercises, separate cancellation, five-pair orchestration, nearest-rank p50/p95 and a fail-closed report reader. `tests/test_runtime_v3_performance_harness.py` has 27 guard/oracle tests plus one real child Dataset/ACK/cleanup test: **28 passed**. This count is not a claim that 28 performance comparisons passed. The JSON binds the exact harness bytes and JUnit file.

New real preflight facts:

| Workload | Historical B0 | New-engine B1 `5ff5da9` |
|---|---|---|
| fast CPU, external sampler/no profile | correct, 1.872 s | correct, 36.178 s |
| slow API / uneven model | actual first-before-EOI handshake failed after 30 s; no historical throughput invented | each has 64 real native requests, public first result before EOI and final ACK; 11.827 / 11.296 s |
| 67-node shared-source large graph, 4 workers / 256 MiB | all 1024 rows correct, 12.834 s | rejected at parked durable credit window, 5.434 s |
| hotspot reducer, 16,384 × 4096 B / 64 MiB | all 128 groups correct, 9.416 s | rejected at finite producer reserved storage before reducer, 5.563 s |
| CPU → actual GPU → CPU, 32 rows | not yet run | correct, 15.430 s, original GPU allocation/32 CUDA calls/final ACK/physical cleanup |
| original wide join, partitions 1 / 2 | not yet run | both hit original 120 s run budget; first result 73.027 / 70.544 s, 150 ACKs each |

The original wide `take_all` regression was not changed. These are `iter_rows` companion probes with the same input values, native 256 MiB / RSS 2 GiB / threads 2 / partitions 1 and 2. The approved performance plan also explicitly binds the total Run timeout to 120 s. No timeout was widened to obtain a passing result.

Real cancellation probes pass for small-data (source entered), native API (HTTP request entered), and CUDA (one real computation completed): cancellation latencies 0.091 / 0.498 / 0.327 s. Original reservations are released and owned processes exit. GPU0 is free afterward; all four hardware memory counters are zero. These are cleanup probes, not measured completion rounds.

**Still incomplete:** formal five rounds await the coordinator's repaired, fixed candidate. Ray's same-budget public `workers`/`native_resources` contract and Ray/remote actor process sampling are not accepted yet. The harness checks for the public constructor parameters; it never writes `_local_kernel` to simulate support. Ray/two-host results remain `not_run` until actor RSS/FD and physical cleanup are actually observed. The full report consumer requires all approved cells (10 Local including both wide partitions, plus four Ray and four two-host); a smaller chosen subset cannot certify the full requirement.

Programmatic entry: `run_comparison(plan, baseline_roots, candidate_root, output_root, frozen_candidate=True, cells=...)`. `baseline_roots` maps a cell (or `historical`) to `{root, kind, python}`; `kind='reference'` is explicitly distinguished from `historical`. `measure_trial(...)` supports isolated preflight/diagnostic trials. `performance(fx)` consumes only a complete same-source/same-harness report named by `DEMIFLOW_PERFORMANCE_REPORT`; it otherwise raises the original `NotRunError`. The coordinator owns wiring this into `runtime_v3_support._s_performance`.
