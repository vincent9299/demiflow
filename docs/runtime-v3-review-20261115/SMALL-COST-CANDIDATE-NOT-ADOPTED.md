# Locator PRF and fixed codec vocabulary: not adopted

The scoped candidate is retained on `engine/gpt-fastcpu-profile-20261117`.
Production changes are `8a4c0e6`; final test-input commit is `8935989`.
Baseline `1aba5b5` has production bytes identical to `8184c10`.

The two intended changes were implemented without changing wire bytes, DTOs,
identity formulas, resource budgets, ACKs, or recovery authority:

- `PeerBuffers` uses an independent private secret and keyed BLAKE2 over a bounded
  counter to retain unguessable 32-hex locator capabilities without per-row entropy.
  Exhaustion and active collision reject before changing owned bytes.
- `_measure_document` uses an immutable, finite schema vocabulary cost map. All
  canonical byte, legacy estimate, node, depth, malformed-input and DTO checks
  remain; arbitrary strings do not enter a cache.

**The original pipeline did not demonstrate a performance benefit. Do not merge
this candidate as an I08 improvement or count performance acceptance complete.**

## Correctness evidence

240 distinct tests have passing latest setup/call/teardown outcomes on the final
production bytes: all original wire types, original codec counterexamples, live
socket/credit/hash/slow-reader tests, terminal windows, and 13 new capability and
codec boundary cases.

The first batch had 230 passed / 10 failed. Those 10 were an existing fixture
omission: native peer tests started a source worker without the now-required
bounded resume credit. The same first failure was reproduced on unmodified
`1aba5b5`. A first attempted migration reused the B helper but its one-max-row
byte window correctly paused after one small row; that red receipt is retained.
The final fixture assigns the original run's existing 64 MiB allowance to its
only source owner, with a four-row ceiling. It does not raise the run budget.
All original four-row, connection count, per-record ACK, corruption, failure-prefix,
deadline and cancellation assertions remain. The final peer suite is 14 passed.

- `/tmp/demiflow-small-cost-protocol-r1/receipt.json`: 230 pass, 10 pre-existing fixture failures.
- `/tmp/demiflow-small-cost-old-peer-baseline/receipt.json`: original baseline failure.
- `/tmp/demiflow-small-cost-peer-r2/receipt.json`: first fixture attempt, 4 pass / 10 fail.
- `/tmp/demiflow-small-cost-peer-r3/receipt.json`: final fixture, 14 pass.

These receipts are not flattened into an all-green first attempt. Final evidence
uses latest outcome per nodeid; the production bytes are identical across r1–r3.

## Same-workload measurements

Both arms use the original `fast_cpu`: 20,000 → 10,000 exact outputs, four-worker
maximum, 256 MiB, 120 seconds, same interpreter, same checkout path and same
filesystems. Four resource/location domains and TMPDIR are private. Each arm has
one excluded warmup followed by two measurements, without cProfile or py-spy.
Every trial passed original semantic checks, source snapshots, original ACK and
pool/physical-child cleanup checks.

| Arm | Warmup, excluded | Measured 1 | Measured 2 | Median |
| --- | ---: | ---: | ---: | ---: |
| Baseline | 13.2439 s | 12.7074 s | 12.8867 s | 12.7970 s |
| Candidate | 13.1615 s | 13.5755 s | 12.8279 s | 13.2017 s |

Candidate/baseline is approximately **1.0316**: +3.16%, not a gain. Two samples
and before/after ordering cannot establish a stable regression; they also cannot
justify claiming improvement. No five-round formal result is asserted.

Raw trials and summaries:
`/tmp/demiflow-fastcpu-small-cost-baseline-r1/` and
`/tmp/demiflow-fastcpu-small-cost-candidate-r1/`.

A small alternating CPU diagnostic used the actual baseline methods extracted
from committed source and the candidate methods, seven rounds of 10,000 calls
each. Codec measurement improved only about 1.9%; `put` plus original release
became about 4.2% slower. It measures local call cost, not full execution. In
particular, the earlier active-stack weight under `randbytes` was not a direct
measure of recoverable entropy CPU time; the syscall may release the GIL and
interact with other worker threads. This is why profile attribution alone was
insufficient to approve the change.

Microdiagnostic: `/tmp/demiflow-small-cost-microdiagnostic/result.json`.
No terminal scratch, driver, kernels, placement, main checkout or shared
environment was modified. The next independent experiment must start from the
baseline production without these two changes.
