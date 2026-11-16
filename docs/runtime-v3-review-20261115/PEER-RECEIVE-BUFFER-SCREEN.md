# Peer receive buffering: correctness passed, gain not established

This independent candidate starts from original production `8184c10` and contains
neither the locator PRF nor the fixed-vocabulary codec experiment. Baseline
`6ba3f57` adds only analysis documentation and the independently verified native
peer test fixture's finite source credit. Candidate `dcb483e` changes only
`exchange/dataflow_transport.py`, `_OutputSubscription.read_admitted`, and a new
test file. The original driver event handling, kernels, placement, terminal
scratch, resource limits and frozen DTOs are untouched.

## Bounded mechanism

`read_peer_many(..., buffer_bytes=0)` keeps the original behavior for existing
consumers. The terminal consumer opts into `B=min(65536, largest_frame_bytes)`
only after the existing receive workspace admission succeeds. The same original
workspace is `3*L + min(65536,L) + 256`: the optional fixed buffer replaces the
previous receive-fragment allocation allowance; old/new raw frames, decode and
Arrow conversion accounting remain unchanged. This is the existing managed-byte
envelope, not a hard process-RSS promise.

The standard-library buffered socket reader has one fixed receive buffer, reads
at most one declared frame per call, and never calls read-all or allocates a
batch-sized buffer. It borrows the existing socket descriptor. The size and SHA
of every frame are checked before delivery; individual credits and ACKs retain
their original owner. The buffer is closed in the generator's finally block
before socket closure. Windows below eight bytes use the old path. An invalid or
oversized requested buffer is rejected before connecting.

## Verification

**43 passed in 42.97 s**, all 129 setup/call/teardown reports passed, no skipped or
deselected cases. All 2,941 input digests stayed unchanged at `dcb483e`.

Coverage includes real buffered sockets, retained payload reference counts,
maximum receive request size, early-close release, cross-frame corruption and
original ACK prefix, pre-allocation budget rejection, original low-FD behavior,
slow sender/consumer ownership, existing peer batching, terminal grant checks,
96 KiB live execution and historical replay, and original Arrow conversion
workspace counterexamples. Original test assertions were retained.

Receipt: `/tmp/demiflow-peer-buffer-protocol-r1/receipt.json`.

## Original public workload screen

One unprofiled before/after diagnostic reused the original full `fast_cpu` workload
and independent output oracle: 20,000 → 10,000 rows; four-worker maximum; 256 MiB;
64 KiB row; 256 KiB batch; 2 MiB state; 2 GiB scratch; 120 seconds. Same interpreter,
checkout path, physical filesystems and private resource/location domains. Neither
arm contains the preceding two rejected optimizations.

| Arm | Full elapsed | Public first row | Original ACKed cuts |
| --- | ---: | ---: | ---: |
| Baseline `6ba3f57` | 13.7581 s | 1.9379 s | 36, including one final |
| Candidate `dcb483e` | 13.3126 s | 1.8602 s | 35, including one final |

Both produced all 10,000 exact outputs, released the original pool and left no
observed child processes. Source snapshots were unchanged. The original row and
one-second checkpoint triggers were unchanged; actual cut count depends on timing
and is recorded as 36/35, not replaced by the earlier profile run's 34 cuts.

The candidate is 3.24% lower in this one comparison. This is within the baseline
variation seen in preceding measurements and does **not** establish a repeatable
gain. Five formal pairs were not started, and V034/I08 is not marked complete.

Receipts: `/tmp/demiflow-peer-buffer-baseline-r1/trial.json` and
`/tmp/demiflow-peer-buffer-candidate-r1/trial.json`.

## Mechanism diagnostic and disposition

A separate real-socket diagnostic transferred 512 records of 512 bytes per arm,
using the same original credit and per-record release. Three alternating rounds
observed 1,024 `recv` calls on the original path versus 871 / 873 / 903 `recv_into`
calls with buffering; every receive request remained at most 512 bytes. Thus the
buffer reduces these calls by 12%–15%. Diagnostic median elapsed was approximately
0.01602 s versus 0.01626 s, so fewer calls alone did not establish a speedup.
This small mechanism diagnostic is not substituted for the original business
workload. Raw observations: `/tmp/demiflow-peer-buffer-syscalls/result.json`.

**Keep this as an isolated candidate; do not adopt or claim a performance win on
these results.** The original profiling wait time in `recv` is not entirely
recoverable syscall CPU cost. A larger execution improvement needs to target the
remaining per-record control/terminal work rather than infer an additive saving
from nested or waiting profile frames. No larger redesign was implemented here.
