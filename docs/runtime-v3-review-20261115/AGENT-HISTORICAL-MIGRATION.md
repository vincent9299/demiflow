# Historical Agent tests and unified execution

Worktree: `gpt_v3_regression_20261115`; branch: `engine/gpt-v3-regression-20261115`.
This is platform test migration, not business-pipeline execution. Every pytest
process pins this checkout in `PYTHONPATH` and asserts `demiflow.__file__` belongs
to it before collection. All projects, journals, capacities and artifacts are
owned temporary data under `/tmp/demiflow-*`. No main checkout or production
service is modified.

## Why the old tests failed

The three-file baseline contains 161 nodeids: 82 passed, 79 failed. The local
capture is `/tmp/demiflow-regression-agents-baseline.{log,xml}`. The failures are
not reported as 79 independent engine bugs:

- Agent configuration: 3 tests still supplied a private SQLite journal to
  `Dataset.agentmap_async`.
- Agent HTTP: 6 private journal options; 3 offline cases whose native journal
  capability conflict is a production defect owned and fixed by the root lane;
  the remaining 13 failures inject Python globals in the driver while the real
  operator now runs in a separate worker. Four other tests with such injection
  happened to pass but did not prove the injection reached the operator.
- Codex: all 54 failures initially stopped at the fixture's default private
  `sqlite_journal` option, before the behavior named by the test could execute.

Native recovery is owned by the run. Re-enabling private Dataset journals would
violate the one-journal contract and hide the actual migration.

## Preserved behavior and new proof boundary

`tests/_agent_runtime_fixtures.py::collect_before_commit` observes records
returned by the real `_OutputSubscription.read` and injects a driver interruption
before the first real `DataflowCheckpointCoordinator.commit`. It returns the
actual worker rows and original `RunRef`; it also verifies no cut was committed.
`resume_collect` resumes that same reference and same declaration using actual
workers. There is no fake worker, fabricated output, or copied journal.

Known replies must be reused without new HTTP requests or Codex subprocesses.
Original trace, material, tool argument, artifact byte/hash, and budget assertions
remain. The immutable run declaration is reused; decreasing `max_requests` on
resume would change history identity and no longer represents native recovery.
Request counters are read from the original `RunJournal`, and token metrics are
computed from actual returned worker trace, not `Dataset._stages`, which is an
unexecuted driver declaration.

Unknown replies cause `UncertainPromptCall` before new dispatch. This is stronger
than returning a row-local unknown error while continuing other work. In the
mixed known/unknown Codex test the first attempt must still produce the good row,
the known response must remain in the journal, and resume must not start another
session. No completed response is discarded to manufacture an all-known run.

Changed artifact configuration/image feature flags must fail historical identity
validation; they cannot be treated as the original run with a new zero budget.
The lower Codex client's request-identity normalization for explicit false image
flags remains separately checked without changing the run's sealed declaration.

## Explicit component tests

The tests that inject native `read_documents` errors run the actual `PromptActor`
in-process through `collect_actor_component`. This makes the fault injection real
and preserves invalid-argument repair, native-argument forwarding, scope refusal,
error classification, budget/cancellation and cleanup assertions. It is explicitly
a component boundary, not proof of worker serialization. Genuine Dataset HTTP
and Codex tool execution/recovery tests coexist with it.

Codex cancellation and local artifact-store failure injection use the real
`CodexAgentClient` and real temporary stdio subprocess. Their private SQLite
journal is a client-component fixture, never a Dataset execution journal. The
existing export-cancellation component likewise stays explicit. Full native run
recovery is tested independently with the original run journal.

The local app-server transport is a deterministic executable fixture. It checks
real stdio messages, callback content, process state and exported bytes; it does
not call an external Codex service or a paid model. HTTP tests use a local server.
These results do not imply production service or multi-host verification.

## Evidence

`AGENT-MIGRATION-PROGRESS.json` records the first Agent HTTP/configuration batch:
80 unique nodeids passed in staged runs (63 in the first run plus 17 after the
component fixture was corrected to read `InputMaterializedHandle`). Three offline
nodeids were explicitly pending root production fix; no skip was added to tests.
Codex and the final combined result are recorded in the follow-up evidence file.

Codex first native run: `/tmp/demiflow-regression-codex-first.{log,xml}`,
**77 passed / 1 real regression**, 290.52 seconds, source base `3116146` plus the
Codex test diff. No skips. The remaining red test is
`test_node_and_persistent_session_budget`: first attempt returns A success and B
`PromptBudgetExceededError`; after interruption/resume A is reused, but B's
original run quota raises `RecoveryContractError` through RPC and aborts the
Dataset. The expected error row was not relaxed. Root lane owns the narrow
native quota exception mapping fix; the budget transaction and unknown/fatal
recovery exceptions must remain authoritative.
