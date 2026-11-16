# Native adapter closure, 2026-11-16

This lane binds legacy native delivery to the same Dataset v3 execution, action,
allocator and journal. Main checkout and shared environments were not modified.
Production is frozen at source head `7208b9a`; the document commit is separate.

## Behavior closed

- `dataset_native.execute` honors the caller placement's CPU/memory/scratch/row/
  payload ceilings by intersecting original planning bounds and original allocation
  scope. Optional fusion and durable fallback must obey the same ceilings.
- Arrow delivery returns `RecordBatch`, respects the smaller run/caller batch limit,
  and applies `sink_schema` in a real worker `MapBatchesOp`. The conversion identity,
  schema, execution ceilings and native take limit enter the original graph seal.
- Cast admission includes simultaneously retained input and new output. The
  equal-width negative uses an Arrow proxy pool to prove zero new Arrow payload
  allocation before rejection. Safe cast overflow/column mismatch publishes no rows.
- A borrowed action must match original source and complete operations, and belong
  to the current run. It creates no second action and does not complete the owner.
  Same-parameter recovery reuses the original v2 carrier and v3 graph. Changed
  schema/ceilings are rejected before worker launch or committed-result delivery.
- `limit=0` starts no source/worker. Row and Arrow limits use the original take
  terminal/ActionHandle, without collecting all output or slicing in the driver.
  Early close and failure retain the original lifecycle and ACK authority.
- Real Ray testing exposed missing allocation context in business threads. The fix
  reads the original RUNNING allocation's effective resources/spec hash, passes
  this read-only envelope through the existing actor handle, checks allocation ID,
  spec hash and actual Ray assigned CPU/memory, and uses the original context
  manager plus copy_context. Missing/mismatched grants fail before business work;
  command/close cleanup leaves no context behind. K3 schema and allocation authority
  are unchanged.
- Remaining `Engine.python_rows` compatibility forwards the original execution
  placement to this same adapter and retains original stats/finally behavior.

## Commits and dependencies

Own ordered commits: `f05676c`, `d5f7b1a`, `fc993cd`, `7370b1b`, `42a86e3`,
`b07c416`, `dc5c9c4`. These include production and associated tests.

Sibling history dependencies: `ad96846` (explicit original v2 carrier selection),
`46da6c4` (thin old stream/pull adapters), `912dad7` (old Arrow caller preserves take
limit). This tree contains equivalent cherry-picks `aa77e1a`, `19a8557`, `7208b9a`;
do not apply both versions. `dataset.py` and the frozen seven files were untouched.

## Fixed-source validation

Every run records the interpreter, full selectors, isolated environment, worktree
import, source HEAD and SHA256 input manifest. The source remains clean/unchanged
for the run, frozen-seven bytes match `49b666f`, and owned descendant processes
are empty at completion. The JSON index independently matches every recorded input
hash against the actual Git commit (2,194 or 2,195 files per report).

| Source HEAD | Result | Evidence and meaning |
| --- | --- | --- |
| `fc993cd` | 73 passed, 257.50 s | Local adapter/old L010/L009, original active action, dual seal recovery/drift rejection, limits, residency, Arrow/empty schema |
| `7370b1b` | 8 passed, 75.02 s | 3 actual Ray business cases (adapter and two native join partition settings), plus 5 declaration/constructor cases |
| `dc5c9c4` | 12 passed, 3.40 s | H021 original RUNNING grant wiring, F019 budget passthrough, zero-allocation cast rejection, 9 grant/cleanup negative tests |

A previous fixed Ray run at `fc993cd` failed because the worker lacked its original
allocation context. It is retained, and superseded by the actual Ray green run at
`7370b1b`. Another fixed Local host suite at `7370b1b` had 107 passed / 1 failed:
H021's fake state lacked the original read API. `42a86e3` updates the fixture and
strengthens rejection assertions; the final 12-test run re-verifies this exact case.
Neither red record is removed or counted as passing.

The old native_limits parking test explicitly selects existing `fusion=False` so
its nonfinal worker-restoration boundary still exists. It now also asserts original
nonfinal ACK state and reconstructed workers. The separate completed-action replay
case intentionally proves zero new worker launch; it is not advertised as worker
reconstruction.

## Limits of this receipt

These results bind to the listed source commits, not a later root integration.
The native `arrow_batch`/`lance_scan` legacy branches are outside this change.
Actual payload admission and original allocator budgets are tested; this is not an
OS RSS/VRAM isolation claim. Formal performance, broader product acceptance and
cross-lane graph integration remain root gates. Ray model recovery and the sibling
L045/old Arrow caller combination are recorded below once complete.

## Additional actual Ray model recovery

Fixed `7208b9a`: **3 passed / 216.61 s**, nine actual model/window combinations.
Prompt, Embedding and Agent each run before-send, after-send-unconfirmed and
confirmed-before-reply recovery on real Ray Core workers. Only the external model
HTTP endpoint is a loopback fixture. Every case consumes exactly one request;
unconfirmed sends remain `UncertainPromptCall` with zero commits, while confirmed
responses are reused and ACKed. No Ray Data import occurs. Source/HEAD remain
unchanged and owned descendants are empty. The JSON index includes every scenario
artifact path/hash and verifies all 2,195 input hashes against this source commit.

Sibling history evidence was read independently: `85834ff` L045's three selected
items each passed setup/call/teardown (its full batch had unrelated failures and is
not called green); `29b69c0` old Arrow stream caller has four passing cases. That
last matrix checks take/take_batch with two workers rejects before reading or
committing, while four admitted workers produce exactly three int32 rows in
RecordBatches of at most two from 1,000 lazy inputs, read fewer than 1,000, and
leave the borrowed owner uncompleted. The four shared production boundary files
are byte-identical to this delivery. Test revisions come from history_closure;
they are not reimplemented in this lane.

Machine-readable commands, SHA256 bindings, retained red reports and selected
cross-lane proofs are in `NATIVE-ADAPTER-CLOSURE-20261116.json`. Primary full receipts
are the absolute `/tmp/demiflow-*` paths listed there. The document does not assert
all root integration gates have passed.
