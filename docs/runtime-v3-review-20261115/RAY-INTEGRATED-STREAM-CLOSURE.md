# Real Ray integration verification

Production base: `d87bddadb0db4127a9c575c651267de1beebcb29`.
Tested commit: `f16a7d34778f06dd001deab314a7d7487e3f379f`.
The only change from that production base is the new test-domain Ray wrapper
`tests/test_runtime_v3_stream_business_contract_ray.py`. No production code,
original test assertions, dependency environment, or main checkout changed.

## Result

**6 passed in 86.80 seconds** in one fixed-source run. All six setup, call, and
teardown reports passed. No skip, xfail, rerun, deselection, or collection error.
All 2,941 recorded input digests were unchanged throughout execution.

| Real execution | Assertions preserved |
| --- | --- |
| Shared actor cut and resume | Same state owner; original committed cut restored once; all 12 rows; one start/close per attempt; final shared count 24 |
| Publication overlap | Original commit authority; one pending cut; workers continue during publication and stop exactly at the six-row unconfirmed credit; final 15 rows and original ACKs |
| Local exception class in row callback | Original qualified class identity survives serialization; only declared business exception is caught |
| Local exception class in batch callback | Same original catch contract for a one-row batch |
| Same name, different exception identity | The undeclared class still fails with the original INTERNAL_PROTOCOL error; no name-only whitelist |
| Stream label and progress | Original label `actual`; intermediate frontiers exactly 5, 10, 15, 20, 25; final emitted and committed output both 25 |

The last four tests directly call the original test functions. Their test module's
`DataAPI` constructor is bound to a real `RayDatasetExecutor`; it is not enough to
install Ray while executing Local. Every worker activation additionally checks the
live and sealed physical provider is `ray_core` and records plan/stage/allocation
identity. The wrapper blocks Ray Data imports. Shared actor and overlap use their
existing real Ray fixtures without modification.

The fixtures own separate sequential local Ray clusters (existing shared fixture:
8 CPU, 0 GPU; overlap and stream fixtures: 2 CPU, 0 GPU). There is no outer
`ray.init`, no resource or timing threshold increase, and each fixture shuts down
its own cluster. This proves same-host Ray Core integration; it does not add a new
multi-host or GPU claim.

## Reproduction and evidence

- Interpreter: `/tmp/demiflow-ray-api-optional-env/bin/python`.
- Worktree: `/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_v3_artifact_recovery_20261115`.
- Branch: `engine/gpt-ray-integrated-validation-20261117`.
- Runner: `/tmp/demiflow-ray-integrated-validation-runner.py`.
- Exact invocation, interpreter/import binding, and four private domain paths:
  `/tmp/demiflow-ray-integrated-validation-r1/invocation.json`.
- Original setup/call/teardown and before/after source-hash receipt:
  `/tmp/demiflow-ray-integrated-validation-r1/receipt.json`.
- Receipt SHA-256:
  `c1acc19c577bbe6cb3fb1b9179dfff2df95714e65d050f961425025e3ff65064`.
- Output: `/tmp/demiflow-ray-integrated-validation-r1/stdout.txt`.
- Per-case actual Ray activations: under that run's `tmp/test_*/real-ray-activations.json`.

The runner explicitly pins `PYTHONPATH`, verifies the in-process import belongs to
this worktree and verifies the fixed Git HEAD. `DEMIFLOW_RESOURCE_ROOT`,
`DEMIFLOW_RESOURCE_HOME`, `DEMIFLOW_FORGE_DOMAIN`, and `DEMIFLOW_LOCATION_CACHE`
all point into this run's private `/tmp/demiflow-*` directory.

This is the requested narrow six-case integration acceptance, not a replacement
for the earlier Local, historical, multi-host, or resource-bound receipts.
