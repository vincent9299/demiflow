# Local worker connection loss — 2026-11-17

The three original actor-method closure code-pin tests now pass their real
SIGKILL, unchanged-code recovery and changed-code rejection arms. The repair
maps actual connection reset/abort to the existing `UNKNOWN_EFFECT` boundary.
It does not retry a command, acknowledge work or classify an unknown effect as a
definite failure/success.

- Baseline: `8928e5bef612f00d751b0ec02729cfeccfbb97cf`.
- Isolated tree: `/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_worker_transport_20261117`.
- Branch: `engine/gpt-worker-transport-20261117`.
- New test commit: `88aea235fedad39c9d245d0bf4c247ae5edc44c4`.
- Production commit: `37544e843c186f0a2263894851ca1f257c93bed5`.
- Only production file: `execution/dataflow_hosts.py`, 12 added / 2 removed lines.

`ControlFrameReader.read_ready()` legitimately receives `ECONNRESET` when an
owned worker is killed. Previously both `LocalHostEndpoint.poll` and its
`_send_k3` wait loop let that OS exception escape, while ordinary EOF already
raised `DataflowError(UNKNOWN_EFFECT)`. A private `_read_control` now serves
both callers and translates only `ConnectionResetError` and
`ConnectionAbortedError`. It retains the original exception as the cause.
Truncated/oversized frames, invalid JSON, timeout, fencing and other OS errors
are not caught by this translation. Bootstrap, sends, ACKs, journal,
allocation cleanup and resume decisions are unchanged.

The new tests use actual loopback TCP and abortive close to reproduce reset in
both the poll and lifecycle-command paths. They check one original command,
no manufactured receipt/cleanup confirmation, and the original cause. Separate
real-socket tests retain truncated-frame, oversized-header and invalid-JSON
errors. Existing full-stack tests still perform real worker SIGKILL and inspect
actual process/resource release; no original assertion or fixture was edited.

## Fixed-source evidence

| Run | Source | Result |
| --- | --- | --- |
| `/tmp/demiflow-final-8928-controls-r1-evidence/verification.json` | parent `8928e5b` | The three original actor-method tests fail with leaked `ConnectionResetError` |
| `/tmp/demiflow-worker-transport-r1-evidence/verification.json` | test-only `88aea23` | 2 new real-reset failures / 5 EOF and protocol checks passed |
| `/tmp/demiflow-worker-transport-r2-evidence/verification.json` | fixed `37544e8` | **28 passed**, 40.79 s, all **84 setup/call/teardown phases** passed |

The final selection includes the seven new socket tests, all three original
`test_method_closure_code_edit_rejected_before_call` variants, original Local
control batching and partial-frame/timeout/fencing/no-retry tests, original
process early-close/read-error cleanup, and control-mailbox wakeup tests. The
unaffected real-Ray batching node was explicitly deselected, not counted green.

Each original closure variant runs two actual SIGKILL cases: the unchanged
declaration resumes to all eight expected rows while preserving earlier ACK
receipts; the edited dependency refuses before any new business-call trace.
Original allocator/process termination assertions remain in both arms.

The final receipt records 2986 input hashes, unchanged before/after source and
HEAD, clean tree, private four domains/TMPDIR/D8 pin, actual import root, and no
surviving children. The seven frozen files remain byte-identical to their
baseline. Receipt hashes, exact commands, selectors and all phases are in the
[machine report](WORKER-CONNECTION-LOSS-CLOSURE-20261117.json).

No main/root source file was edited or tested outside the authorized frozen
integration run. This narrow package does not claim new Ray, GPU, multi-host or
performance coverage. The separately completed `8928e5b` real-Ray API matrix
(98 entries / 168 cases) and two dedicated Ray recovery cases remain available
at `/tmp/demiflow-root-final-ray-8928-summary/summary.json`.
