# Final candidate: complete real Ray API matrix

**Passed at the fixed integrated candidate:** all **98 API entries**, all **168 original positive/negative cases**, zero failed or not-run entries. The outer pytest gate passed in **576.03 seconds**, without skips or teardown errors. This supersedes the *full-matrix not yet rerun* boundary in `RAY-API-BOUNDARY-CLOSURE.md` for this exact candidate only; the original red run remains preserved there.

## Exact code and isolation

- Root candidate: `8e85e7a47c421714eafe16e6b044d23ad6b69f1a`.
- Actual tested worktree HEAD: `265843a524d8a8d5cbe13fbec83842933a6578a8`, branch `engine/gpt-v3-residency-20261115`.
- Worktree: `/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_v3_residency_20261115`.
- `demiflow/` and `tests/` are byte-identical to the root candidate. The local merge also retains this lane's earlier documentation.
- Clean start, unchanged HEAD and **1762 unchanged inputs**: 1761 Python source/test files plus the original 98-entry plan JSON. The child independently recorded the 1761 Python hashes, also unchanged.
- All seven protected files remain byte-identical to baseline `362d8528c27ae60a0d6ae0ccc986a0ff93847de8`.
- No source edit or commit during the run. Main checkout, shared environments, production services untouched; no push, main merge, shared install, package download or paid external model requests.

The candidate includes Ray error/environment repairs, original finite-input borrowing, default suffix placement, route-document reuse, per-occurrence checkpoint progress, bounded finite-reference metadata and unique observation IDs. This is their combined API proof, not a sum of earlier module test counts.

## Results and real execution

| Measurement | Observed result |
|---|---:|
| API entries tested | 98/98 |
| Original cases passed | 168/168 |
| Failed API / missing API / failed case | 0 / 0 / 0 |
| Unique successful `execute_action` observations | 107 |
| Observed provider | `ray_core` for all 107 |
| Ray Data import attempts | 0 |
| Outer pytest gate | 1 passed, 0 skipped/errors |

The 107 observations have 107 unique `observation_id` values; duplicate outer/case views are not summed. They count successful returns observed by the original recorder, not a claim that invalid API arguments or intentionally failed worker actions never entered the execution path. Every original semantic and error assertion remains active. Full per-case API mappings, expected failures and run/commit facts are retained in the copied `api_matrix.json` and `api-matrix.json`.

This exercises actual Dataset sources, scalar/typed transformations, relational and finite operations, result reads, writes, model calls against local transports, object/document operations, native search, queue submission/settlement/history, and retired API rejections. Ray Core hosts the same workers and original journal; the case harness blocks Ray Data.

## Resource ownership verification

The private child starts its own local Ray cluster and shuts it down in the original provider context. It then exits normally. No external cluster or global `ray stop` is used.

- Original fixture FD baseline **12**, teardown **12**. The existing `+8` threshold is unchanged and passed.
- Parent subprocess boundary **11 → 11** FDs; child exit code **0**.
- Across **all 168 cases**, the child has exactly **44** FDs after each case: no per-action increase.
- No post-case open file points into `api-runs/`, `run.sqlite`, or a native `demiflow-search-worker-*` scope.
- `ray.is_initialized()` is false after shutdown. Ray/Arrow process-global FDs remain until child exit, as independently demonstrated by the earlier pure-Ray control. They are released by actual owner-process termination, not by force-closing foreign descriptors or raising the fixture limit.

Full FD identities per case and the real process identity are preserved, not just counts.

## Reproduction and dependencies

From the isolated worktree:

```bash
DEMIFLOW_V3_PROVIDER=ray_core \
DEMIFLOW_RESOURCE_ROOT=/tmp/demiflow-ray-api-integrated-capacity \
PYTHONPATH="$PWD:$PWD/tests" \
/tmp/demiflow-ray-validation-env/bin/python -B \
/tmp/demiflow-ray-api-integrated.py
```

The observer invokes only:

```text
-vv --tb=short -ra tests/test_runtime_v3_acceptance.py::test_api_matrix
--basetemp=/tmp/demiflow-ray-api-integrated
--junitxml=/tmp/demiflow-ray-api-integrated.xml
```

Do not collect H carrier-double units in this process. Before running, the observer verifies actual Ray module/host-provider identities and real `ObjectRef`/`ActorHandle` classes. The private child records every case and the source snapshot; the parent copies evidence before the original fixture removes its scope.

The task-local no-pip environment preserves Ray 2.60.0, Arrow 25.0.1, NumPy 2.3.5 and Python 3.11 from the existing Ray environment. Only missing dependencies are referenced from already-installed, same-ABI packages; the production native-search subprocess isolation is unchanged. Exact package versions, import/source paths and all environment links are in the JSON. This environment is a reproducible local verification setup, not a claim about production dependency deployment.

## Evidence

- `/tmp/demiflow-ray-api-integrated.xml` and `.log`: outer original pytest gate.
- `/tmp/demiflow-ray-api-integrated-evidence/api_matrix.json`: final gate report and unique action observations.
- `api-matrix.json` in the same directory: original complete 168-case outcomes and API classification.
- `ray-api-child.json`: every case's FD identities and independent source hashes.
- `ray-api-process.json`: actual child PID, normal exit and parent FD comparison.
- `verification.json`: exact HEAD, command, input digests, dependencies and zero changed inputs.
- Companion `RAY-API-FINAL-CANDIDATE.json`: portable audit index, precise case list, hashes for all evidence artifacts and all 1762 inputs.

## What this proves and does not prove

This closes the previously missing **single-host real Ray complete public API matrix** for the integrated candidate and supports the original engine intent: one Dataset engine and semantics, Local/Ray as hosts, no Ray Data execution, and actual original-authority publication across the tested business routes. The earlier targeted evidence separately proves real Ray finite-output checkpoint recovery, declared credential isolation and typed finite borrowing.

It does not establish multi-host deployment, physical GPU inference, external PostgreSQL, the separate performance target, complete product-browser acceptance, general immutable published-artifact GC, or every possible combination of API arguments. Those need their own evidence and must not be inferred from 98/98.
