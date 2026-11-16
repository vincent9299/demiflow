# V033 artifact security closure

The formal `tests/test_runtime_v3_acceptance.py::test_artifact_security` now
executes its attacks and a hosted public Dataset. It no longer returns NotRun.
No production change was required by these cases. This closes this gate, not
the remaining runtime/business gates or a claim of universal artifact security.

Worktree: `/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_v3_regression_20261115`.
Branch: `engine/gpt-v3-regression-20261115`.
Integrated root `91c8658` in `c2836b4`; retained root's later service/metrics
changes in the historical cherry-pick conflicts. The implementation commits
to integrate are `1e276a8` and `f33fcbd`; do not duplicate the merge's root code.

Fixed tested HEAD: `f33fcbdc4e02a0803f59310d9b601470ff644836`.

## Executed evidence

Both providers run two public Dataset rows, each containing 98,304 payload
bytes, through a real map worker and the original final ACK. They retain the
observed `execute_action` invocation, run/action/attempt/dag/plan IDs and commit
ID. All loaded platform modules are checked against the isolated worktree;
the original Ray Data import blocker remains active.

Fourteen negative cases execute against the original store/loader/bootstrap:

| Case | Actual rejection |
|---|---|
| Program expected code pin mismatch | `INVALID_CONTRACT`, before loading the payload |
| Worker sealed code-pin membership mismatch | `CODE_MISMATCH` in the original worker program loader |
| Reference in another store domain | `INVALID_CONTRACT` in the original store |
| Worker assignment from another store domain | `INVALID_CONTRACT` in `make_runtime_services` |
| ZIP `../` traversal | `INVALID_CONTRACT`; no published or leaked extraction |
| ZIP symlink | `INVALID_CONTRACT`; no published or leaked extraction |
| Same-size altered program payload | Original loader rejects checksum mismatch before unpickling |
| Dishonest byte count | Original store rejects size mismatch |
| Expired copy of allocator-issued worker lease | Original worker journal gate returns `FENCED` without issuing an RPC |
| Missing original logical plan | `UNKNOWN_EFFECT`, no compilation fallback |
| Missing original program pickle | `UNKNOWN_EFFECT`, although the current callable is available |
| Missing archived business source | `UNKNOWN_EFFECT`, despite the current module being imported |
| Oversized event | Original closed wire encoder returns `BUDGET_EXCEEDED` |
| Oversized 65,537-byte launch | Real bootstrap child fails before opening the control descriptor |

Every faulted original object is restored byte-for-byte before normal execution
continues; the evidence records the original and restored SHA-256. No original
journal state, receipt or resource decision is fabricated. Loader negatives
run synchronously at the prepared/assignment boundary; their transport adapter
calls the original driver dispatch. They are not claimed as malicious remote
network intrusion tests. The expired-lease counterexample injects an expired
lease DTO; it does not change the production clock or wait out a live lease.

Historical checks observe both the compiler export and its execution-module
alias, and file opens under the main checkout. Recompile count and main-file
reads are both zero. Missing facts are never filled from current code.

Actual endpoint poll/reply observers delegate to the real methods and inspect
their original encoded DTOs. Maximum observed control frames are **1,297 bytes
Local / 1,300 bytes Ray**, with zero occurrences of the long business payload.
Assignment carriers are original typed artifact references. Actual launch
sizes are **2,383 bytes Local / 2,438 bytes Ray**, below the unchanged 65,536-byte
bootstrap bound. Negative control encoding uses the actual plan's finite
encoder bound, separately from the stricter bootstrap bound.

## Fixed verification

| Environment | Result | Evidence |
|---|---|---|
| Local process | **31 passed, 0 skipped**, 24.96 s | `/tmp/demiflow-v033-final-local-evidence/verification.json` |
| Real same-host Ray Core | **1 passed, 0 skipped**, 7.56 s | `/tmp/demiflow-v033-final-ray2-evidence/verification.json` |

Local includes the formal node, 20 evidence guard tests, the existing seven
launch-reference tests, the original B002 worker loader test, the original
sealed worker/no-SQLite test, and the original missing-evidence delegation
guard. The evidence rejects omitted cases, fake success, missing ACKs, changed
historical bytes, payload-bearing/oversized frames and recompilation fallback.
The `evidence_integrity` placeholder was removed only for artifact_security;
its original missing-evidence refusal test remains wired.

Each receipt contains the full command, interpreter, PYTHONPATH, private
resource root, exact node outcomes, module binding, fixed HEAD, and 2,177
source/test input SHA-256 values. All inputs match before/after, Git remains
clean during verification, and all seven frozen files match `49b666f`.
Scenario JSON was copied before fixture cleanup; its SHA is in the receipt.
The attached JSON indexes all evidence and runner hashes.

Run commands are in the verification JSON. The Ray fixed runner initializes
its owned private two-CPU/no-GPU cluster **before pytest creates fixtures**, then
closes that cluster after pytest. The fixture's original FD guard is unchanged:
Local is 53→53; Ray is 38→40, within its original bound. This is functional
verification, not a performance comparison. Both dedicated runners exited.
The post-exit cleanup receipt also checks the actual driver PID and all three
worker PIDs extracted from this private cluster's hashed worker logs: none
exists in `/proc`. No live command refers to the owned cluster directory.
See `/tmp/demiflow-v033-final-ray2-evidence/post-exit-cleanup.json`.

## Retained failed attempts

The first fixed Ray call passed every business assertion but failed fixture
cleanup (12→23 FDs). Its exit remains failed in
`/tmp/demiflow-v033-fixed-ray-evidence/verification.json`. A minimal probe with
no Dataset independently shows Ray `init/shutdown` retaining exactly eleven
process-global descriptors: three SDK logs plus poll/timer/event/socket FDs;
garbage collection does not reclaim them. The original guard was not relaxed.
Initializing the owned runtime before measuring Dataset resources separates
environment ownership correctly. Dedicated interpreter exit reclaims the
SDK's process-global handles. Probe code/output are retained and hashed.

An initial environment-runner directory name exceeded Linux's Unix socket
path limit and failed before pytest. `/tmp/demiflow-v033-final-ray.log` is
retained; the owned temporary prefix was shortened, no engine limit changed.
No passing result is claimed for either failure. The final Local runner source
is preserved as `/tmp/demiflow-v033-local-fixed-runner.py`; the final Ray source
is `/tmp/demiflow-artifact-security-fixed-runner.py`, each bound by its receipt.

Main checkout, shared environments/services and the frozen seven remain
untouched. No push or main merge was performed.
