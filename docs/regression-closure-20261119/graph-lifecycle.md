# Graph stability and startup loss — 2026-11-19

Task branch: `gpt/engine/graph-lifecycle-20261119`, based on main `e3b645b`.
Only the task worktree was edited/tested. Formal main, online preview and business history were untouched.

## Slow branch acceptance

The previous test inferred independent progress from a sleep and a 100 ms difference between observed completion times. Host scheduling could consume that margin even when the unrelated branches finished first.

The replacement uses real workers and two-way filesystem rendezvous within the private test directory. The slow branch cannot emit its first row until all five unrelated branches process the complete source. Each fast branch cannot publish its completion marker until the slow branch has been scheduled. A serial execution therefore cannot pass by finishing the fast branches before starting the slow one. The existing original native graph/ACK/managed-worker assertions and complete output checks remain. A 30 second watchdog detects deadlocks; no performance threshold is relaxed or claimed as a performance result.

## Registration failure

The historical failed `durable_incremental` run is `/tmp/demiflow-i19h2/f/128/receipt.json`.
Its final allocation `828a178a6dfc469da3784d52666aaa11` was spawned and confirmed cleaned up 139 ms later; its worker log is empty. The old error merged EOF and an invalid initial frame into `INTERNAL_PROTOCOL`, and retained no exit code. The available record cannot establish why that process exited. Passing a rerun does not establish a historical root cause.

`LocalHostEndpoint` now distinguishes:

- EOF/reset/aborted connection before registration: `UNKNOWN_EFFECT`, allocation/boot identity and the actual process exit code when already observable. No automatic retry.
- Socket deadline: `DEADLINE_EXCEEDED`, unchanged timeout policy.
- Invalid first message: `INTERNAL_PROTOCOL`.
- Corrupt/truncated/oversized framing: original decoding/budget error retained.

A real managed-process SIGKILL test checks both existing recovery admission branches. With no ACK, resume remains rejected as `INCOMPATIBLE_HISTORY` and provisions no replacement. After an actual phase ACK, loss of the next worker before registration releases its resources; attempt 2 resumes and preserves the original ACKs byte-for-byte while returning the expected rows. This improves loss handling and future diagnostic evidence; it does not claim to prevent arbitrary external process death or prove the historical signal's cause.

## Final usage projection

The graph entry point now closes/drains its handle before recording execution metadata, including metadata when close raises. This retains final prompt/model observations delivered during cleanup. The sibling prompt-accounting task owns the matching worker observation changes and Dataset/stream entry points.

## Evidence

All tests explicitly use the task worktree on `PYTHONPATH`, assert the imported module path, and use private `/tmp/demiflow-graph-lifecycle-20261119/` resource roots. Shared environments were not modified.

- `admission/`: `test_runtime_v3_worker_connection_loss.py`, `test_runtime_v3_control_drain.py`, `test_runtime_v3_control_batching.py`: **38 passed, 1 skipped**, 18.06 s. Before/after SHA-256 maps for all tracked Python inputs are identical. The skip is the Ray batching test in the non-Ray interpreter.
- `targeted/`: full `test_u3_graph_local.py`: **3 passed**, including the new causal independence assertion. Unmodified `test_graph_restore_across_exchange_modes[durable_incremental]`: **passed**, 166.63 s, with original ACK/output/resource assertions. This combined run also contained 12 successful connection tests and one failed new test fixture; the fixture failures are retained, not overwritten.
- The new fixture initially nested a RunContext via the outer execution context, then incorrectly expected recovery without an ACK. Both test assumptions were corrected to the existing public usage and recovery contract. No production admission checks were weakened. The final two-case real-process test above covers the corrected contract.
- `original-startup-evidence.json`: minimal extraction of the original failed allocation's spawn, cleanup and empty log evidence.

The earlier reports remain available under `slow-proof/`, `slow-proof2/`, `registration/`, `targeted/`, `owned-loss/`, and `final-registration/`. The first observer-monkeypatch approach triggered the real code-pin guard and was discarded; the delivered test does not monkeypatch platform observation logic.

No whole-repository green claim, no main update, no push, no preview restart. Integration and cross-lane usage verification belong to the parent coordinator.
