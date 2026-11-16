# Restore original small-budget terminal residency

The original 40 KiB tests `test_two_stage_dataset_progresses_when_only_one_stage_fits` and `test_shared_source_branches_keep_multiplicity` both rejected before workers started. Terminal metadata admission had added a fixed 12 KiB preference to the complete 24 KiB payload, producing a 36 KiB worker profile. The original solver reserves 5 KiB for control, leaving only 35 KiB. An optional extra KiB had therefore disabled valid temporal execution.

`capture_terminal` now caps only the new terminal metadata window by the space left after the exact original control reservation and whole payload. The original solver, physical profile, resource provider and `TerminalBlocks` checks still enforce admission. The minimum remains 3 KiB for one block and one forest root; insufficient room rejects before launching or committing. Larger budgets retain their previous metadata windows. Historical sealed grants are read unchanged.

For the original 40 KiB case, real execution verifies 24 KiB payload + 11 KiB metadata + 5 KiB control = 40 KiB. The measured original allocation peak is 35 KiB, with the control reservation kept separate. Results include every original row and branch multiplicity, and every native cut is ACKed. A real interruption after this action's completed result allows a new run attempt; it reuses the exact sealed graph/physical plan and original commits and launches zero additional workers. A 30 KiB counterexample leaves only 2,304 bytes for metadata and rejects before execution. These are managed-object/profile guarantees, not process RSS claims.

## Acceptance and source binding

| Receipt | Fixed source | Result |
| --- | --- | --- |
| `/tmp/demiflow-terminal-budget-red-evidence/verification.json` | `f679a0f` | Four failures: both original cases and the new budget counterexamples. |
| `/tmp/demiflow-terminal-budget-r1-evidence/verification.json` | `245ea8c` | 16 original tests passed in the 172.37-second run; two new fixture lifecycle assumptions failed. |
| `/tmp/demiflow-terminal-budget-r2-evidence/verification.json` | `ff500e1` | Both corrected new tests passed in 19.74 seconds. |
| `/tmp/demiflow-terminal-budget-ray-r1-evidence/verification.json` | `ff500e1` | Original real Ray Core durable parking test passed in 24.22 seconds, original 40 KiB / 60 seconds / two CPUs. |

The original 16 cover the two reported failures, all three existing durable interruption boundaries, actual lost consumer ACK reconciliation, no invented proof before the first commit, default source quantum, shared-source multiplicity, early limit/recovery, and real Lance interrupted-create recovery. They also retain the existing terminal metadata admission and historical-window checks. Ray uses the original test which forbids Ray Data execution.

The two new fixture errors were corrected without changing production: a fully succeeded RunContext cannot resume, so the positive now interrupts the open run after the completed action; an expected compilation error must propagate through RunContext cleanup instead of being swallowed inside it. The failed receipts remain available. Production bytes in the 16-pass group, the corrected two-test group and the real Ray group are identical; only this new test file changed between `245ea8c` and `ff500e1`. The already green groups were not rerun merely for a single headline count.

Each of the four runs contains 2,198 input hashes verified byte-for-byte against its recorded git archive. HEAD and inputs remained unchanged, the tree stayed clean, the original seven frozen files match, and no child process remained. Each uses this isolated import path and three private resource domains. `TERMINAL-RESIDENCY-BUDGET-VERIFICATION.json` retains input maps, receipt hashes, exact commands/environments, counts and original failures. Functional elapsed times are not a comparative performance result.

## Integration

Apply `f679a0f` (red regression cases), `245ea8c` (only production change: `planning/dataflow_compile.py::capture_terminal`), `ff500e1` (correct new fixture lifecycle), then this documentation commit. No original test, solver, worker terminal implementation, scheduler close path, checkpoint, ACK protocol or historical seal was weakened. Root owns scheduler close and native owns the independent terminal flush repair; neither file was modified here. No main checkout, shared environment, running user service, push or main merge was involved.
