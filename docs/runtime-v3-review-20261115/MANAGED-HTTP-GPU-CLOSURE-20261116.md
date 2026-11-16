# Managed Embedding service: original GPU grant and unknown cleanup

Worktree: `/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_v3_artifact_recovery_20261115`.
Branch: `engine/gpt-v3-artifact-recovery-20261115`.
Base before this change: `6b877fe` (the parent already owns its original cleanup-retry production change).
The accompanying `MANAGED-HTTP-GPU-VERIFICATION-20261116.json` binds all 1,041 production Python files, the tested fixtures, raw JUnit files, five actual process/GPU proofs, and 18 released private allocations. The dirty-base notation is intentional: these source hashes bind the delivered code rather than claiming an earlier clean commit was tested.

## Result and user-visible behavior

The existing `map_embeddings(..., service=ManagedHTTPService(..., gpus=[physical_index]))` declaration now runs through the unified Local process runtime and its existing DevicePool. The driver resolves physical indices under explicit `CUDA_DEVICE_ORDER=PCI_BUS_ID`, verifies them against the explicit deployment inventory, and seals the UUIDs, host, inventory digest and RSS ceiling into the original execution constants. No model or CUDA context starts on the driver.

The original stage profile reserves the caller's explicit `native_resources.max_rss_bytes`. The original `WorkerRequest.placement.required.device_ids` requests the sealed UUIDs from the existing ResourceManager/DevicePool. The worker checks its actual CUDA UUID environment, memory envelope and current journal grant before starting the service. The service receives those UUIDs; its legacy GPU flock path is removed only for this already admitted worker path. Ordinary standalone ManagedHTTPService owners keep their original behavior.

The caller must provide enough original run memory for the worker and its service tree; the implementation does not invent a larger default. The guardian samples the worker, guardian and service descendants against that same RSS ceiling. GPU memory retains the existing whole-card exclusive model. This is not a new fractional VRAM allocator or an OS hard RSS/VMS limit.

All-cache execution starts no HTTP/CUDA service. It still participates in the original worker/device admission; no claim is made that a cache hit avoids that grant. Default Embedding declarations without a service keep their old facts and request/cache identities.

## Unknown cleanup now retains ownership

The original guardian protected worker SIGKILL and detached children, but two cleanup paths needed additional protection:

1. Original `/proc` observation helpers can themselves raise during `signal_owned` or `drain_owned`. The guardian now keeps its original unit FD and retries at a bounded frequency until real observation proves the tree gone. An error is never converted to success.
2. The ordinary service owner's final SIGKILL would kill the guardian after a bounded close timeout. The worker-specific owner instead sends TERM to its guardian, waits only its existing timeout, and returns an unconfirmed-cleanup error while preserving the guardian, original unit and port ownership. The ordinary owner is unchanged.

Port custody uses the original successfully acquired flock file descriptions. Only these original FDs are inherited by the guardian alongside the original unit FD. There is no new lock or second allocation. The guardian never calls `LOCK_UN`; after actual tree exit its copies close naturally. Failed acquisition still uses the original ExitStack cleanup.

The real outer-unit counterexample proves this is not cancelled out by `ManagedProcessUnit.stop`: the worker exits, the guardian remains alive while observation is unknown, both original port locks remain unavailable, and the original capacity record remains charged. When observation recovers, the guardian reaps the real service and its separate-session, default-close-fds child. Only then are unit cleanup and resource release confirmed and both port locks available.

Evidence: `/tmp/demiflow-managed-gpu-final-tests/test_worker_owner_and_outer_un0/outer-unit-proof.json`. It records the same worker PID/birth/unit identity with `confirmed=false` before observation recovery and `confirmed=true` afterward, as well as both original port paths and lock outcomes.

## Actual validation

Final production bytes were used in two invocations, **16 passed total**, not a fictitious single run:

| Invocation | Result | What it proves |
| --- | --- | --- |
| `/tmp/demiflow-managed-gpu-final.xml` | 9 passed, 41.53 s | Three actual single-GPU cases; four missing/ambiguous/insufficient/Ray admission cases; two real unknown-cleanup/process-unit counterexamples |
| `/tmp/demiflow-managed-owner-final.xml` | 7 passed, 1 deselected, 2.03 s | Six ordinary HTTP owner regressions and default Embedding facts/identity preservation |

The real GPU was the explicitly authorized local physical device 0, UUID `GPU-b1e12243-75a0-c769-ce2c-3c7ac9d8ef71`. The service process imported Torch, allocated a real CUDA tensor and computed `[3, 4, 0]`; its Embedding result normalized to `[0.6, 0.8, 0]`. The actual service PID appeared under that UUID in `nvidia-smi`, matched the original active DevicePool grant, and had the original 2 GiB CPU-memory reservation. Successful replay used the native private cache after the service script was removed, with one total service startup. Cancellation also reaped the real service and released its original device grant.

A separate real GPU request grew host RSS beyond its explicitly reserved 1 GiB tree ceiling. The original guardian reported the exceeded grant, execution failed, and the original CPU/device reservations were released only after physical exit. All 18 allocation records in the final private fixtures are RELEASED. The final read-only hardware check showed four GPUs at 0 MiB/0% and no compute processes; the one-card test window was returned to the parent.

Earlier evidence is retained separately, with its revision scope:

- Initial GPU run: three fixture/implementation failures and one pass. Dataset construction occurred outside the requested Local executor scope, and the canonical process policy's `worker_mode=None` was incorrectly rejected. Both were corrected; budgets and business assertions were not weakened.
- Later GPU runs: 4 passed, then 5 passed, before the final owner-close/port changes.
- Original C/D/E and service contracts: 142 passed in 76.24 s, before those final narrow cleanup changes. This is supporting prior evidence, not a claim of 142 tests rerun against the final bytes.
- CPU service lifecycle and worker kill/unknown cases: 18 passed in 65.776 s, after finite owner close but before original-port FD inheritance. The two unknown cases were rerun with port inheritance in the final nine-case invocation.

The deselected original Prompt private-journal test belongs to the parent's separately assigned Prompt repair. It is disclosed and does not count as a pass. No existing failure was silently removed from a claimed all-green suite.

## Scope and remaining boundaries

- This restores Local process managed GPU services. Legacy numeric GPU slots for Ray are explicitly rejected until host-specific placement is defined; this is not a claim of managed GPU service parity on Ray.
- Explicit deployment inventory and PCI ordering are required. MIG deployment and arbitrary worker-relative numeric interpretation are not accepted by this path.
- RSS is sampled and may overshoot between observations. The SDK adds no OS virtual-memory isolation here; existing external process limits still apply. Whole-card exclusivity is retained, without a fractional VRAM cap.
- Passive exact native declarations are supported. Live service/actor state, replaced native methods and arbitrary logging callbacks are not silently converted into passive declarations.
- This remains the existing cooperating process unit, not an OS sandbox. Single worker SIGKILL and catchable guardian/observation errors are tested, including a default-close-fds detached child. Simultaneous guardian SIGKILL plus a detached descendant discarding the inherited unit FD remains outside the guarantee. No synthetic CONFIRMED proof is claimed for that case.
- The parent owns the additional ResourceManager OSError retry change `b10997b`; it is not part of this lane's tested source. This lane contains the already integrated `resource_state.py` original cleanup retry from `90f24a4` as `6b877fe`.

All new launchers assert that RESOURCE_ROOT, RESOURCE_HOME and FORGE_DOMAIN are private `/tmp/demiflow-*` directories and that the imported platform comes from this worktree. The earlier legacy RESOURCE_HOME omission remains fully disclosed in `MANAGED-HTTP-BOUNDARY-DEVIATION-20261116.md`; nothing in this delivery deletes or rewrites that incident's default-domain files. The seven frozen files match `362d8528` byte for byte. Main checkout, shared environments, external services and other owners were not modified or tested; no push or main merge occurred.
