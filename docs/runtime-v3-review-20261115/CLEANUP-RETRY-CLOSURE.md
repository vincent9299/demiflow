# Original resource cleanup retry

The managed-service worker-kill probe exposed two different problems. The new
guardian keeps the original process-unit lock while its service descendants
exit. The original resource state then kept a completed UNCONFIRMED cleanup
operation claimed for 30 seconds, equal to the scheduler's total close deadline.
The child could be gone while its original allocation remained charged.

`RunResourceState.record_observations` now shortens only a **returned cleanup
UNCONFIRMED** operation's next eligible retry to `min(100ms, claim_lease_ms)`.
It retains the same intent, native authority/fence, cleanup obligation and full
resource charge. A provider operation that has not returned retains its original
crash lease. No new resource owner, ledger, cleanup receipt or refund is added.
The original Local provider must still obtain physical exit evidence before
releasing capacity. ResourceError cleanup failures follow the same conservative
retry path; stale observations do not become exit proof.

Validation is bound in [the receipt](CLEANUP-RETRY-PRIMARY-VERIFICATION.json):

- At `90f24a4`: 19 passed, one detached-worker SIGKILL fixture failed because the
  severed socket reported `Broken pipe`, rather than its already listed read-side
  reset. The original failure and source digest receipt remain available.
- At `2dd3a44`: both real worker SIGKILL cases passed in 11.04 seconds, including
  a detached SIGTERM-immune descendant with default close_fds. Assertions require
  no release while a child lives, disappearance of the real PID/birth, **all** run
  allocations CONFIRMED and all corresponding original pool records RELEASED.
- The combined unique coverage is 20 cases, not 21 and not one 20-case invocation.
  It includes original resource conformance, real Local pool/process retries,
  the unreturned-I/O crash lease and Prompt's real worker-owned blocking pools.

The Prompt test now binds the actual thread provider at Dataset construction and
observes `RemotePromptJournal.reserve`, the port used by its actual journal pool.
It retains the two HTTP calls, all four pool-thread assertions, exact client close
count, closed executors and forbidden unrequested historical audit. Observing an
unused private SQLite adapter in the parent process could not verify this path.

The guardian's simultaneous hard-kill plus descendants discarding the inherited
FD remains the explicitly documented cooperating-process-unit boundary; this
change does not claim OS sandbox containment. GPU service admission and guardian
cleanup-helper exception retention are separate in-flight follow-ups. The
earlier private test allocation left BOUND has not been manually refunded or
relabeled as confirmed.
