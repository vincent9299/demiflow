# Real Ray public API audit and boundary repairs

## Decision and delivery

The first complete Ray Core matrix was **not green**: 92/98 API entries verified, 161/168 actual cases passed, 7 cases failed, and fixture teardown detected excess file descriptors. This report preserves that result. The seven failing cases now have actual passing Ray runs. The integrated final 98-entry gate has **not** been rerun; do not convert cross-version evidence into a same-HEAD full-matrix pass.

- Isolated branch: `engine/gpt-v3-residency-20261115`.
- Worktree: `/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_v3_residency_20261115`.
- Source base includes root `e304640`: finite-input borrowing and default suffix placement.
- Production repair: `0fd249f`; acceptance-process repair: `0a12460`.
- Main checkout, shared environments, production services untouched. No installation, download, push, main merge, external cluster, paid model, or real GPU inference.
- Seven frozen files remain byte-identical to `362d8528c27ae60a0d6ae0ccc986a0ff93847de8`; exact hashes are in the companion JSON.

## What actually failed and changed

| Original case | Cause | Closure |
|---|---|---|
| `sql-invalid-query` | Ray concatenated the entire structured failure and traceback into a 4096-limited `DataflowError`, masking `invalid_column` with a constructor error. | Ray uses the same fixed-vocabulary `restore_failure` as Local. SQLite `OperationalError` is explicitly allowlisted; its type/message survive. No message limit was raised. |
| `model-prompt`, `model-async`, `model-agent` | Ray actors lacked required model variables set in the driver after cluster initialization. | For an authenticated assignment, only owned native prompt/agent/embedding programs are inspected. Exact platform passive factory/model types supply bounded declared names. Values are ephemeral per-allocation Ray `runtime_env`; absent required values fail before launch, unrelated driver variables are not copied, no actor or user hook is run for discovery. |
| `wiki-native`, `wiki-character-bound` | `env-ray` lacked `mwparserfromhell`. | Same-ABI existing package used in an isolated task environment; both original semantic/error assertions passed. |
| `native-search-http` | `env-ray` lacked Babel and native search bootstrap dependencies. Its isolated child deliberately resets PYTHONPATH, so a parent-only overlay was insufficient. | Private no-pip environment references existing packages with Ray/Arrow/NumPy versions preserved. The real private search subprocess and HTTP request succeed. Production search isolation was not changed. |

The environment projection does not change sealed plan IDs, program manifests, journal receipts, grants, or recovery bookkeeping. Secrets are not added to those durable documents or this report. Connector temporary values are dropped on adoption, abort, or close. This is a native-model declaration projection, not arbitrary UDF environment replication.

## FD attribution and the real owner boundary

Two independent observations distinguish an actual per-action leak from Ray process-global retention:

1. Pure Ray 2.60.0, without importing DemiFlow or executing a Dataset: FD count `3 → 20` after `ray.init` and `ray.shutdown`; garbage collection did not release the remaining 17 descriptors. They include Ray log files, poll/timer descriptors and pipes.
2. Five real Dataset actions stayed at **44 FDs per action** while Ray was active. Shutdown retained the same Ray +17 after Arrow/Lance initialization; no per-action growth was observed.

The formal Ray API gate now delegates the original matrix to a private child process that owns the cluster, calls `ray.shutdown`, and exits normally. The parent waits for that real exit and retains the fixture's original **baseline +8** rule. It never forcibly closes third-party descriptors. The child records per-case FD identities, all original case results, input hashes and shutdown status.

The focused process proof ran the eight original `models` cases: all passed, each ended at **44 FDs**, four successful `execute_action` observations were `ray_core`, parent FD count was **34 → 34**, and child code hashes were unchanged. This eight-case group is explicitly not a 98-API completion claim. The inner case counts and outer pytest test are distinct levels and must not be summed.

## Real additional proofs

- Variables present when the Ray cluster was initialized are stale fixtures. Late per-action prompt variables override those stale values and each native model case makes exactly one local HTTP request.
- Removing required driver variables fails despite the cluster's stale environment. An unrelated variable added after cluster start is absent in an actual Dataset worker.
- The actual worker reports the task interpreter/prefix, confirming the private environment is used. Optional-package imports and source paths were also probed before search bootstrap; package source links are fully listed in JSON.
- Real Ray typed sort consumes borrowed finite input without a second payload copy; original scalar types and negative zero remain intact.
- A 1000-row real Ray join is interrupted after an original nonfinal ACK with emitted finite output; resume produces every `(i,j)` pair exactly once and retains the original committed cut. This incorporates the new finite-input borrowing implementation.
- Platform environment discovery does not call arbitrary instance properties/functions. SQL error round-trip retains its type with an 8000-character traceback note while the transported message remains bounded.

## Verification chronology, including unsuccessful checks

| Evidence | Result | Meaning |
|---|---:|---|
| `/tmp/demiflow-ray-api-full-r1.xml` + copied `api-matrix.json` | 161/168 cases passed; outer failure + teardown error | First complete, clean-head real Ray run. 1747 code inputs unchanged; all 106 recorded successful action calls used `ray_core`; no Ray Data imports. |
| `/tmp/demiflow-ray-api-short-r1.xml` | 10 passed /1 failed | SQL and model repairs worked; search still lacked dependencies in its own subprocess. |
| `/tmp/demiflow-ray-boundary-contracts-r2.xml` | 87 passed /1 failed | Recovery/error/host units; H021's isolated K3 double lacked the new environment-selection seam. |
| `/tmp/demiflow-ray-h021.xml` | 1 passed | H021 fixture supplied that new seam; original K3 ordering assertions retained. Run separately from real Ray. |
| `/tmp/demiflow-ray-api-short-r2.xml` | interrupted, 3 recorded failures | **Invalid real-Ray evidence**: collecting H021 in the same process installed its fake carrier. Real provider received `_RemoteRef`, producing admission timeouts. Only this owned pytest process was interrupted, with normal fixture cleanup. No product conclusion is taken from this run. |
| `/tmp/demiflow-ray-api-short-r3.xml` | 12 passed /2 failed | All original seven red cases, finite borrowing and child FD proof passed. Two newly added tests had fixture issues described below. |
| `/tmp/demiflow-ray-api-short-r4.xml` | 2 setup errors | New real-carrier guard incorrectly assumed `ray.ObjectRef.__module__`; Ray exposes it as `ray`, with identity equal to `ray._raylet.ObjectRef`. Guard corrected to actual class identity. |
| `/tmp/demiflow-ray-api-short-r5.xml` | 1 passed /1 failed | Real 1000-row join recovery passed. Environment probe still pulled the entire pytest module into the old by-reference code-pin dependency scan under the symlinked environment. |
| `/tmp/demiflow-ray-api-short-r6.xml` | 1 passed | Environment probe isolated in a normal stdlib-only business fixture, retaining the original worker isolation assertions and strict code-pin limit. |

The two r3 fixture defects were: an invented `CutRequest.plan_id` read (correct authority is `coordinator._physical.plan_id`), and the environment probe's by-reference test module pulling pytest's dependency graph across the symlinked environment's site-package boundary. The production code-pin budget was not weakened, and no third-party import was hidden from it. The final probe has only stdlib dependencies and records the real worker interpreter/prefix.

All production Python bytes in the final delivery match r3's recorded input hashes. Only the identified test fixtures were corrected afterward. The 14 focused tests each have a final passing observation across r3/r5/r6; this is deliberately recorded as selective reruns, not one all-green invocation at the final commit.

## Commands and environment

All commands ran from the isolated worktree with `PYTHONPATH="$PWD:$PWD/tests"` and a distinct `/tmp/demiflow-*` `DEMIFLOW_RESOURCE_ROOT`. First full run used original `/yzp/zhaozy/yangzepeng/0905/env-ray/bin/python -B` and `DEMIFLOW_V3_PROVIDER=ray_core`; the observer copied fixture evidence before cleanup.

Final focused Ray invocations use `/tmp/demiflow-ray-validation-env/bin/python -B -m pytest`, private `--basetemp=/tmp/demiflow-ray-api-short-rN`, and XML/log names in the table. r3 selected `tests/test_runtime_v3_ray_api_boundary.py` plus `tests/test_runtime_v3_ray_matrix_process.py`; r5 selected environment isolation and finite-join recovery; r6 selected only environment isolation. H units use the original normal environment in a separate process. **Do not collect host-double unit modules together with real Ray tests.**

Private environment: Python 3.11 / `cpython-311-x86_64-linux-gnu`; existing Ray 2.60.0, Arrow 25.0.1 and NumPy 2.3.5 remain primary. `venv --without-pip` and bounded links to already-installed packages only. Native search dependency discovery was bounded to 20 probes, used local fixture declarations, and performed no installation or network download. Package versions/source paths and native ABI checks are recorded. Do not treat this task environment as a deployed production environment.

## Remaining boundaries

The result strengthens single-host Local/Ray API parity and real original-journal recovery. It does not establish a same-HEAD complete 98-API green gate, multi-host deployment, physical GPU execution, external PostgreSQL integration, general published-artifact GC, or the performance objective. Existing 161 passing first-round cases remain evidence for their original code snapshot; the final integrated matrix belongs to the coordinator's acceptance decision.
