# Callback boundary candidate — 2026-11-17

Base 8184c10 + recovery_rows ab4c121 + root timeout precedence 41725d0. Production scope is only kernels/async_instance.py and kernels/operator.py. Parent owns callback classification and observation-wire decoding.

`CallbackDeadlineExceeded` certifies an explicit hard_timeout and complete cancellation of this invocation's coroutine, spawned tasks and actual to_thread futures. A ContextVar tracks the owned subset, so cancelling one shared-actor occurrence does not cancel another. Run/control deadlines and undrained work stay ordinary fatal DataflowError. Cancellation translated by the callback into a value or its own error retains that actual outcome. Lifecycle uses its original execution deadline, not the callback timeout. Thread execution rejects a returned awaitable; inline async remains supported.

Both batch paths and pending-batch restore now pass the existing observe/dead_batch hooks. No new diagnostic ledger exists.

Original private inventory runner: 9 focused checks passed in 3.17 s. Real native action regression: 17 passed, 4 failed in 67.88 s; all four failures are batch caught/deadline diagnostic transport hitting the parent's strict dead_batches wire validator, reported for independent fix. This is not an all-green claim. Four new real-stream observation selectors are added but not yet executed; they will validate that follow-up without removing any old assertion. See JSON for byte-bound receipts.
