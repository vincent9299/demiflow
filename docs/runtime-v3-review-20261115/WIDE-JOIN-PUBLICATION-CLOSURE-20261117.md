# Wide join and finite native publication closure — 2026-11-17

The two original 64/128 MiB `Dataset.join → write_lance` cases and the original
80 KiB `sort → sort → write_lance` recovery case now pass without changing their
data, budgets, timeout or assertions. Final fixed-source validation is **18 Local
tests plus one actual Ray Core test**, all three pytest phases passed. This is a
bounded completion of those functional gaps; it is not the full V035, product
or I08 performance gate.

## Delivery and evidence identity

- Baseline: `6e9d31c24dc977385a81ba0dcf5d25415226202b`.
- Tested implementation: `2bc1c7fd741c1d7286be536af73f36434b4450cc`.
- Single integration commit: `22b6ed8df36debb31b9a8fd3ce55ff2d3ca1b715`, parent is the baseline.
- Both commits have **the same complete Git tree**: `b8dcb9bf65b257239ceb0a57b7e30b67ff2c0303`.
- Worktree: `/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_wide_join_20261117`.
- Delivery branch: `engine/gpt-wide-join-delivery-20261117`.
- Original `engine/gpt-wide-join-20261117` branch and all intermediate red/green commits are retained.
- Production scope: 14 files and one new test file. No original test was edited.

The paired [machine report](WIDE-JOIN-PUBLICATION-CLOSURE-20261117.json) contains
receipt hashes, exact commands, exact selectors, all setup/call/teardown outcomes,
2979 input hashes per final run, the seven frozen-file hashes, and the byte
differences between each historical receipt and the delivered implementation.
Both final receipts match every recorded input in the delivered tree; their
before/after inputs and HEAD are unchanged, and their live-child lists are empty.

## Original failures and causal repair

The unchanged wide-input cases failed on the baseline before execution:
`B + 6R` gave the join a 128 MiB default estimate, while the terminal and its
conversion workspace were also reserved against every earlier finite phase.
The default-window fitter tried to compensate by shrinking state to 3 KiB.
This could not make a 64 MiB or 128 MiB plan feasible.

An independent clean checkout of the same baseline also reproduced the original
80 KiB finite sorting/native-write failure. After genuine phase scheduling was
enabled, that case reached conversion and exposed a second fixed estimate:
64 KiB of writer metadata alone could not fit its approximately 30 KiB driver
workspace. The actual small Arrow schema and writer framing require much less.
Neither failure was repaired by changing the total budget or dropping a guard.

The changes keep the user's platform intent concrete: the public Dataset path
can finish, commit and recover the full original values through the same engine
on Local and Ray Core. Original journal, receipts and immutable artifacts remain
the execution facts available to the product. No parallel result ledger or
workbench reconstruction was added. This package does not itself revalidate the
complete workbench user journey.

## Frozen new-plan contracts

### One reservation interpreted by every layer

New pure producers with nonempty, fully finite-complete inputs can seal
`publication_workspace.schema = demiflow.native_publication_workspace.v2` and a
finite-complete terminal input. This is independent of the compact join carrier:
the sort case uses ordinary Records with the same finite publication scope.

`PublicationReservation` is a pure interpreter of the existing sealed graph and
physical residency, with no allocator, journal, mutable capacity or second
owner. Its reservation is original control plus the original native workspace
when a terminal is physically resident or publication is requested; preceding
finite phases use the control reservation alone. Compiler feasibility, phase
selection, scheduler admission and the native `prepare` boundary all consume
this interpretation. Multiple native sinks still share the maximum workspace
because the original coordinator prepares them serially.

The scheduler counts its actual live allocations. Original ACK, drain and
confirmed physical-exit requirements remain in force before an upstream grant
can disappear from that set. A still-live upstream worker plus native workspace
is rejected; merely finishing logical input does not release its memory.

Missing declarations and v1 declarations preserve their old reservation and
codec behavior. No old seal is rewritten or recompiled, and no identity formula
changes. The physical plan continues to declare the maximum control reservation.

### Compact carrier only where the producer implements it

The new `stored_typed_row.v1` carrier is limited to a direct, single pure join
feeding `write_lance`, with literal/range/materialized inputs and the constrained
budget condition recorded in the compiler. Multi-join/reducer regions keep
their existing representation. The marker is never added just because a
consumer could understand it.

The join writes the original canonical typed payload into the existing
`RunArtifactStore`, then emits an ordinary Record containing its original
position, ID, schema, encoded size and artifact reference. The terminal owns a
small bounded metadata window and streams authentication of the full original
payload before accepting it. Size, media type, store identity and SHA still
apply. Native conversion hydrates the payload under the one driver workspace.

Join scratch, keys, decoded left/right rows, merged row, codec copies and
yield-suspended peer rows remain accounted under the actual worker grant. The
finite reader drops its validated raw frame before yielding the Record instead
of retaining another wide copy across downstream execution. Existing broadcast
validation retains the bytes it genuinely needs.

The join memory cap only replaces an internal default estimate:
`planning/dataflow_compile.py::_Capture._capture_relational` constructs
`_default_profile(None)` and sets `B + 6R`; neither `KeyedSource`,
`local_kernel.keyed_dataset` nor public `Dataset.join` has a business
`ResourceProfile`/worker-memory argument. Original explicit total memory, R/B,
executor/native limits, and other operators' resource requests are not lowered.
Runtime allocation admission remains mandatory and a 32 MiB counterexample
with the same wide input is rejected before native publication.

### Native conversion still accounts for simultaneous copies

Only v2 uses schema-dependent writer metadata admission: the original Python
buffered-writer window, IPC framing, bounded schema/field/type metadata and
variadic Arrow buffer descriptors. The bound is computed before construction
and carried through the same conversion check. Tests compare it with real Arrow
IPC sizes, including nested binary-view arrays and large schema metadata.

The input Python tree, Arrow result, two writer payload copies, reader/index
state and retained previous batch still share the original publication
workspace. The original equal-width cast input-plus-target counterexample is
unchanged and passes. This is managed byte admission; it does not claim native
library/process RSS equals the managed pool.

## Function-level integration scope

| File | Changed seam |
| --- | --- |
| `execution/dataflow_publication_budget.py` | v2 parser, pure `PublicationReservation` |
| `planning/dataflow_compile.py` | `_Capture.capture_terminal`, `fit_codec_windows` |
| `planning/dataflow_policy.py` | finite v2 branch of `fit_default_stream_windows` |
| `planning/dataflow_place.py` | compact terminal estimate; `solve_residency` and its three callers |
| `execution/dataflow_scheduler.py` | optional reservation, `_feasible`, `admit_publication` |
| `execution/dataflow.py` | `prepare` wiring and three `_configure_phases` fit checks |
| `execution/kernels/join_spool.py` | bounded stored-row mode; retained row/key admission |
| `execution/kernels/relational.py` | `join_partitions` stored-payload output and actual peak checks |
| `execution/kernels/operator.py` | join call passes existing retained-peer callback only |
| `execution/dataflow_finite_input.py` | release validated raw before yielding Record |
| `execution/worker/dataflow_graph_runner.py` | `_push` terminal stored-reference branch only |
| `execution/worker/terminal_blocks.py` | strict sealed compact carrier and reference validation |
| `execution/dataflow_sinks.py` | native publication callback; shared pre-admitted writer workspace |
| `execution/batch_conversion.py` | bounded native writer metadata admission |

For integration beside the separate runner close/cancel work, the exact narrow
patches are `/tmp/demiflow-wide-join-delivery-20261117/runner-push-only.patch` and
`driver-phase-only.patch`. No lifecycle, shutdown or cancellation method is
changed by this package. The source/allocator/frozen seven remain untouched.

## Validation and evidence boundaries

| Receipt | Source | Result and purpose |
| --- | --- | --- |
| `/tmp/demiflow-wide-join-r1-evidence/verification.json` | baseline `6e9d31c` | Original two wide cases fail planning; red baseline retained |
| `/tmp/demiflow-wide-join-base-r1-evidence/verification.json` | clean baseline `6e9d31c` | Original 80 KiB sort/native case fails planning |
| `/tmp/demiflow-wide-join-r6-evidence/verification.json` | intermediate `5c42494` | 47 passed / 8 failed; seven literal eligibility failures subsequently repaired, eighth is the original 80 KiB case; original 19 join-chain and 20 projection tests passed here |
| `/tmp/demiflow-wide-join-r8-evidence/verification.json` | intermediate `6bd393d` | Original 80 KiB case reaches conversion but rejects fixed writer metadata; causal red retained |
| `/tmp/demiflow-wide-join-r9-evidence/verification.json` | intermediate `ed75f78` | 35 passed, including repaired original cases and projection set |
| `/tmp/demiflow-wide-join-r10-evidence/verification.json` | final `2bc1c7f` | **18 passed**, 70.35 s; real Ray node intentionally deselected |
| `/tmp/demiflow-wide-join-r11-evidence/verification.json` | same final `2bc1c7f` | **1 actual Ray Core passed**, 22.17 s; same deselected node really executed |

The intermediate successes are historical evidence, not added together and
advertised as one fully green final-HEAD run. The final 19 unique nodes cover:

- Both unchanged wide native allowance cases, the unchanged 80 KiB finite
  sorting/write recovery and original explicit native `batch_rows=2` behavior.
- Real native publication followed by lost return: original final `prepared`
  commit is inspected and ACKed on resume, target remains version 1, and no
  completed source/join/terminal is launched again (64 and 128 MiB).
- Interrupt immediately after the real join phase ACK: completed phases do not
  restart; the same original finite seal, payload references and plan ID resume.
- Original close-time GC actually removes an owned orphan while all referenced
  payloads survive; damaged and missing payloads reject before target creation.
- Full values, duplicate multiplicity, nulls, bytes, tuple representation before
  native Arrow conversion, and negative-zero sign remain intact.
- Lower-budget rejection; unchanged old/v1 reservation; live-owner reservation
  rejection; original cast double-copy admission and publication-workspace tests.
- Private real Ray Core execution of a 6.3 MB payload under 64 MiB, full value
  equality, original final ACK replay without worker launch, and no Ray Data use.

The Ray fixture initializes its own `address='local'` cluster (4 CPU, 0 GPU,
100 MiB object store, unique private temporary directory) and closes the executor
and Ray runtime in `finally`. The harness provider variable is deliberately
`local` so it does not initialize a second cluster; the test asserts the actual
sealed provider is `ray_core`. No optional import skip is counted as a pass.

All test commands use private resource root/home/Forge/location domains,
private TMPDIR and basetemp, explicit interpreter/PYTHONPATH, and in-process
source-root assertions. Complete phases, fixed source hashes, frozen seven,
post-test clean status and no surviving subprocesses were checked. FD inventories
are recorded; native-library process-lifetime FD caches are not claimed to return
to zero. The main checkout and live business data were not used for edits or tests.

## Remaining scope

There is no unresolved failure in this assigned closure. GPU, two-host Ray,
the full history inventory, workbench browser journeys and the formal I08
performance comparison were not rerun by this package. No throughput percentage,
global completion percentage or whole-product acceptance is inferred from these
19 tests. Parent integration must preserve its concurrent changes and apply its
appropriate final combined checks.
