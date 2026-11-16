# Original 20k → 10k CPU pipeline: current-source attribution

## Scope and result

One diagnostic run of the original `fast_cpu` workload on fixed production
`8184c10156d65f4205eba2ffeea668a96cfbfa6f`. The original benchmark builds
`from_iter → map arithmetic → filter odd input IDs → map projection → iter_rows`.
Its spec exactly equals the prior benchmark spec: 20,000 input rows with a 32-byte
text field, 10,000 output rows, four-worker maximum, 256 MiB memory, 64 KiB maximum
row, 256 KiB batch, 2 MiB state, 2 GiB scratch, 120-second timeout. No production,
test assertion, benchmark workload, budget, or shared environment was changed.

The actual sealed plan co-locates this pure linear graph in one worker, preserving
all five logical occurrences and their individual 20k/20k/10k/10k frontiers. The
four-worker value is a maximum, not a requirement to allocate four idle workers.

Result: **passed**, all 10,000 output values matched the independent original
oracle (`sum256=4e99a2879fb5861c2810a125b1c4d4c6134685f6902befd87e4778caa6adb467`).
There were 34 original ACKed cuts including one final cut, no original pool charge
left, and no surviving observed child processes. Source/harness/plan snapshots
were unchanged. Four resource/location domains and TMPDIR were private.

Diagnostic full time was 18.4949 s; public first result was 2.1106 s. These numbers
include cProfile and sampling overhead and are **not** a throughput comparison or
formal five-repeat V034 result. No earlier measurement is used as a speedup ratio.

The checkout and parent isolated checkout were both on device `0:196`, `dpc`,
mount `/yzp`; this trial and the previous trial parent were both on device
`259:14`, `xfs`, mount `/tmp`. Mount source/options/fsid also match. No storage
relocation is being credited to code performance.

## Three priority slices

Times below are nested or run on different threads. They must not be added into
an alleged recoverable wall-time saving. Driver numbers are exact instrumented
call timings; worker numbers are 100 Hz active-stack sample weights, not precise
CPU timers. They identify candidates requiring a controlled follow-up trial.

### 1. Live terminal record transport and locator generation

- Driver read 10,000 peer frames: `_read_peer_frame` 4.3787 s, including 20,000
  exact header/body reads and 4.2807 s inside their socket `recv` calls.
- Existing `read_peer_many` batching already works: only **456 connections**,
  averaging 21.93 records per connection. `socket.create_connection` totals
  0.0509 s. Do not describe this as one TCP connection per row or propose merely
  adding the batching that already exists.
- Worker business-thread samples place 1.08 sample-seconds in
  `PeerBuffers.put → secrets.token_hex → token_bytes → randbytes`: 19.8% of its
  5.46 active sample-seconds. Each immutable live buffer currently obtains a fresh
  system-random 128-bit locator key.

**Small independent first patch:** `execution/exchange/dataflow_transport.py`,
`PeerBuffers.__init__` and `put`. Generate one private random key-generation
secret per PeerBuffers owner, distinct from the transmitted authentication token;
derive each existing 32-hex-character key with keyed BLAKE2/HMAC over an advancing
bounded counter. Keep the original per-owner token, per-record SHA, row/byte
credits, retirement, active-send ownership and ACK release unchanged. Plain
counter keys are unsuitable: one producer token can be seen by multiple consumers,
so records must retain an unguessable capability. The key secret stays private,
and counter exhaustion/collision is an explicit bounded failure. Neither key nor
counter changes durable record identity or becomes a recovery authority.

**Separate next experiment:** retain the existing authenticated wire frames and
per-frame size/SHA checks, but introduce a bounded buffered reader for the current
batch so consecutive headers/bodies can share socket receives. Small windows and
the old singleton path must retain their current behavior. This requires an
explicit receive-workspace value in `read_peer_many` and one narrow consumer change
in `dataflow.py::_OutputSubscription.read_admitted`; charge the retained buffer, old/new
raw frame and decode workspace before allocation. Never pre-read the whole batch
without a grant or assume the existing largest-row workspace covers a larger
buffer. Root must allocate the driver hunk before this experiment.

Do not resurrect the prior unadopted peer-iovec screen as a proven improvement:
its two full-chain candidate samples did not establish a gain. Buffered receiving
is a different hypothesis; it still requires the original workload comparison.

### 2. Repeated control-document measurement and reconstruction

11,099 WorkerEvents go through `_event_from_dict` / `decode_wire_document`:
2.6902 / 2.6816 s on the driver. Within that, `_measure_document` takes 1.5628 s;
its recursive walk is called 337,467 times and its string measurement helper
512,171 times. Payload construction contributes 0.6035 s across all observed
document types. Worker bootstrap separately spends 0.90 active sample-seconds in
its existing message sender, including 0.49 in `encode_wire`.

**Small patch:** `execution/contracts/dataflow_codec.py::_measure_document` only,
plus dedicated codec tests. Precompute canonical byte and legacy-estimate costs
for a closed, finite set of known plain ASCII schema/field strings. The map is
derived from fixed schema vocabulary; no runtime-generated string or unbounded
cache is allowed. Keep the existing remaining-byte check before looking up a
string, and keep every existing depth, node, conservative estimate, malformed
type, non-finite-number and DTO invariant check. Unknown/user strings use the
original path. Do not bypass `_validate_payload` or frozen constructors, discard
worker fencing fields, or skip document measurement.

The hoped-for improvement is reducing repeated work inside the same validator,
not replacing the wire contract. Run exact-byte/depth boundary and Unicode/escape
counterexamples, representative WorkerEvent equivalence cases, then the same
pipeline. A second bulk-event schema is outside this minimal patch.

The original benchmark's `driver_control_decode` spy observes only `decode_wire`,
so its 34 calls do not describe the 11,099 already-decoded WorkerEvents. Its copy
observation is explicitly partial; use the profile for this attribution instead
of reporting that control traffic is only 34 messages.

### 3. One scratch write per terminal output row

The worker business-thread sample puts 1.01 sample-seconds in
`TerminalBlocks.append`, 0.99 in `ScratchStream.write`, and 0.97 at
`KernelScratch.write_at` (the `os.pwrite` path). This is 17.8% of worker business
active samples. Together with locator generation, these two row-level service
costs account for 37.5% of the business thread's active samples; the arithmetic
and predicate themselves are not the dominant work.

**Minimal file scope:** `execution/worker/terminal_blocks.py` and a narrow
append-only helper in `execution/dataflow_scratch.py`. Coalesce small terminal
documents within the terminal's original sealed payload window; retain row/byte
counters per business row. Use one bounded buffer, not a per-row object list or
an additional unaccounted queue. Reserve the original staging extent before
accepting bytes into the buffer, flush before a block switch, snapshot/cut,
manifest creation, seek/read, and close, and preserve original inode ownership,
journal staging charge, crash reconciliation and immutable-artifact publication.
An unchanged row that does not fit the optional buffer goes directly through the
old write path; a tighter window cannot introduce a new capability rejection.

Admission must include raw input plus the buffer and flush copies. Choose a
buffer only from the already sealed terminal payload grant, and keep pending
buffer state represented in any snapshot boundary until flushed. Do not add a
new spool manager, move writes to the driver, or acknowledge before the original
cut. This patch needs terminal restore/partial-failure/tiny-window and cancellation
coverage before performance comparison.

## Priority and ownership

Start with the transport locator PRF, which touches one uncontended implementation
file and avoids broad runtime changes. The fixed-schema codec optimization is a
separate small slice. Terminal scratch buffering is more invasive because its
reservation and snapshot boundaries are correctness-bearing; take it after those
two or assign a separate owner. Buffered peer receiving needs the driver hunk
assigned explicitly and should be measured independently from locator changes.

`dataflow.py`, `kernels/dataflow.py`, `kernels/operator.py` and placement have not
been edited. No independent source/execution/commit authority is introduced by
these proposals. Startup (1.48 s) and published-orphan GC (0.826 s) are visible but
are lower priority than the per-row slices in this workload. General operator
vectorization, larger budgets, forced worker counts and deleted historical
statistics assertions are not part of these proposals.

## Evidence

All raw evidence is under `/tmp/demiflow-historical-fastcpu-profile-20261117/`:

- `fast_cpu/config.json`: unchanged original workload and budget.
- `fast_cpu/trial.json`: public outputs, physical observations, ACKs, resources,
  actual command, source snapshots and storage conditions.
- `fast_cpu/driver.prof`, `driver-cumtime.txt`, `driver-tottime.txt`: exact driver
  call profile and syscall callers.
- `process-tree.speedscope.json`: 1,950 active Python stack samples, zero sampling
  errors; process/thread identities distinguish driver, worker, publication and
  observer threads.
- `verification.json`: pinned HEAD/import, source/harness/plan snapshots, original
  spec equality, storage comparison, no surviving descendants.
- `attribution.json`: extracted call timings and sampling aggregates.

Runner: `/tmp/demiflow-historical-fastcpu-profile-20261117.py`, invoked by the
existing `env/bin/py-spy` over the existing `env/bin/python`; no package installation.
This deliverable is analysis only. No optimization gain is claimed until an
implemented candidate passes the original semantic/resource gates and a valid
same-workload comparison.
