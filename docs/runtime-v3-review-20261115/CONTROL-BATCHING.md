# Ready control-frame batching: verified scope and remaining failure

Source tested: `d8d241e`, isolated `engine/gpt-v3-residency-20261115`. Production changes are `f796580`, `81842e3`; test corrections `3cd2ffd`, `936fd9d`, `d8d241e`. Parent integrated these at `cc2f4c4`. Harness-only cherry-picks `a2d2593`, `e5c28d7` correspond to performance lane `c879676`, `2f01747`. This report does not claim performance closure.

## Contract preserved

Local batches concatenate the unchanged 4-byte length and canonical document frames. At most 64 already-ready frames and 1 MiB aggregate are sent, with the legacy solitary maximum raw frame retaining its four-byte header. No timer waits for a batch to fill. One bounded shared reader owns each Local socket across poll and K3, including partial/coalesced frames and EOF. Raw validation, identity, per-record credit and original business dispatch remain authoritative.

Ray `receive_many` validates its bounded command collection then calls original `receive` in order. Partial/unknown failure propagates and is not retried as success. Transport scopes flush at existing pump and terminal admitted-batch boundaries, including exceptions and before cut/K3/reply waits. Terminal read admission, decoding, queue removal, ACK, and frame cleanup retain their original order. Thread execution remains direct.

## Functional evidence

Source-bound original tests plus new counterexamples: **27 passed in 118.10 s**. Exact selectors: `tests/test_runtime_v3_control_batching.py`, `tests/test_runtime_v3_ray_terminal_seams.py`, and `tests/test_runtime_v3_recovery_scenarios.py::test_real_dataset_commit_six_kill_windows`. Includes actual Ray terminal/final ACK replay, actual Local process early close and second-read failure, and six actual SIGKILL commit windows. Evidence is `/tmp/demiflow-control-batching-bound-r5-evidence/verification.json`, with production/test source hashes before/after and JUnit digest. One pre-existing Lance fork warning is recorded.

Additional original H group: **79 passed**; its controlled lifecycle doubles are not represented as actual Ray. A previous combined r4 run had **25 passed / 2 failed**, both new Local fixtures collided with the active Ray context before production startup. d8d241e fixes fixtures by explicitly constructing the Local DataAPI, retaining all process/error/cleanup assertions. The failed log remains available.

A separate source-bound actual Ray probe at the same HEAD recorded **512 distinct terminal ACKs in 28 successful RPCs**, batch sizes 1–30; all 512 original delivery identities are retained in `/tmp/demiflow-control-batching-ray-count-r1-evidence/actual-terminal-ack-rpcs.json`. The observation occurs after successful original flush and counts only the actual `read_admitted` terminal scope. This is not a simulated RPC test. **1 passed in 13.34 s**.

## Actual performance; unresolved two-host result

Unchanged fast CPU workload: 20,000 inputs, map/filter/map, 10,000 public iter_rows results, four workers, 256 MiB, 120 s timeout. Same independent input/output digests and original ACK/resource evidence.

| Fixed candidate | Provider | Elapsed | First row | Verdict |
|---|---|---:|---:|---|
| f796580 | Local process | 33.764 s | 6.134 s | Correct; no material Local improvement |
| 936fd9d | same-host Ray | 40.764 s | 8.859 s | Correct; allocation observations and physical cleanup complete |
| 936fd9d | two physical Ray hosts, 2 CPU each | 124.009 s | 17.654 s | **Failed original 120 s budget**, 18 nonfinal ACKs; incomplete output |

The prior same-host Ray 60.79 s observation is a different fixed version and a single run; no paired statistical speedup is claimed. The two-host trial remains red. Both private nodes were cleaned, children empty, Ray shut down, remote exit 0. No GPUs were used. All three source-bound trial paths and hashes are in the companion JSON.

Driver diagnostic profile at f796580: `/tmp/demiflow-perf-control-batching-profile-r1/driver.prof` (48.669 s under profiling, not benchmark timing). Control batch flush itself was 0.178 s; strict decode/document and ready waits still dominate. Data-plane per-row connection and Ray empty-pull overhead remain separate work, with parent owning RayHostEndpoint reads after this handoff. Source hashes must remain fixed for the next benchmark. No main checkout changes, shared-env installs, global service shutdown, push, or merge-to-main occurred.
