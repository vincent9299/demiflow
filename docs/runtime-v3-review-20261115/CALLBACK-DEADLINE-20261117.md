# Callback boundary candidate — 2026-11-17

Base 8184c10 + recovery_rows ab4c121 + root timeout precedence 41725d0. Production scope is only kernels/async_instance.py and kernels/operator.py. Parent owns callback classification and observation-wire decoding.

`CallbackDeadlineExceeded` certifies an explicit hard_timeout and complete cancellation of this invocation's coroutine, spawned tasks and actual to_thread futures. A ContextVar tracks the owned subset, so cancelling one shared-actor occurrence does not cancel another. Run/control deadlines and undrained work stay ordinary fatal DataflowError. Cancellation translated by the callback into a value or its own error retains that actual outcome. Lifecycle uses its original execution deadline, not the callback timeout. Thread execution rejects a returned awaitable; inline async remains supported.

Both batch paths and pending-batch restore now pass the existing observe/dead_batch hooks. No new diagnostic ledger exists.

Original private inventory runner: 9 focused checks passed in 3.17 s. Real native action regression: 17 passed, 4 failed in 67.88 s; all four failures are batch caught/deadline diagnostic transport hitting the parent's strict dead_batches wire validator, reported for independent fix. This is not an all-green claim. Four new real-stream observation selectors are added but not yet executed; they will validate that follow-up without removing any old assertion. See JSON for byte-bound receipts.

## Final cancellation and wire integration

Root control JSON thaw (8347451, local cherry-pick f86d36c) restores the correct diagnostic list shape. Four actual Dataset run_stream row/batch × ordinary/catch TimeoutError cases now pass with exact miss counts, cancelled-callback file evidence and original dead rows. Cancellation ownership additionally keeps the inner call in the original cleanup set: on fixed service stop it cancels/drains that root first, allowing its shielded paid exchange to settle, then drains abandoned descendants. Cancelling all descendants at once would bypass that shield and is expressly prohibited. Ordinary cancel and arbitrary cooperative-prefixed text still abort.

Final 18 checks passed in 16.89 s: the original 9 focused controls, four real stream diagnostic/catch combinations, four explicit-hard-timeout cooperative/ordinary cancellation cases, and unsafe single-worker source admission. All private assertions and unchanged-source checks passed. This closes the prior four diagnostic transport failures at their shared cause; native terminal regression selection was already run and is not falsely counted as rerun here.
