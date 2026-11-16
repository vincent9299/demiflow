# Worker death and managed HTTP service ownership

CPU compatibility exposed a real P0: killing the actual Dataset worker left its
HTTP process alive under PID 1, while the worker's original allocation became
`RELEASED`. The fixed base `5327f7e` reproduction is preserved at
`/tmp/demiflow-managed-http-worker-kill-probe-r2/result.json`: worker 60603,
service 60834, original allocation `01cbe4dbec264d5da291d0b6faad1764`.
The test subsequently cleaned only its recorded service PID/birth. No surviving
test service is left, and no shared service or allocation was touched.

The new `dataflow_managed_http_child` is an owned guardian inside the existing
worker process unit. It does not schedule or allocate resources. Its inherited
original unit-lock FD covers the startup window; it checks the parent PID/birth
before and after spawning, registers Linux parent-death notification, and is a
subreaper. The model command inherits that same unit FD. Bounded original
`signal_owned`/`drain_owned` observation reaps the entire owned tree, including
another session whose child used ordinary `close_fds=True`. The guardian holds
the original lock until the tree is gone; unknown/stuck ownership is not a
successful cleanup proof.

The original service owner gains only `_spawn_process` as an overridable seam.
Ordinary callers still use the original argv/Popen/port/readiness/process-group
algorithm. Only the unified worker factory wraps its passive declaration with
the guarded spawn. The guardian runs its pinned platform path with Python `-I`,
so a model's declared PYTHONPATH cannot substitute guardian code. There is no
change to ResourceManager, DevicePool, ManagedProcessUnit or their ledgers.

Actual green evidence:

- The same real worker SIGKILL now leaves no live service after runtime close;
  original resource release occurs after the owned service exits. The original
  UNKNOWN_EFFECT remains an error, not an automatic external-call replay.
- A separate-session, default-close-fds descendant ignores SIGTERM. The real
  guardian adopts/kills/reaps it; an observer sees the original allocation still
  charged while the service tree is alive and records zero premature releases.
- A fault injected into only the guardian's descendant observer throws after
  that separate-session child exists. Real original ManagedProcessUnit startup,
  inherited FD, subreaping and cleanup still run; both child PIDs disappear
  before its original unit cleanup confirms exit.
- Original success/cache/no-start, HTTP failure, startup readiness failure,
  cancellation, port exclusion and six original owner regressions remain green.

There are **22 distinct passing cases across the two recorded invocations**,
not a fictional single 22-case run. The first run passed 13/15; the two failures
were the new fixture's 0.3-second readiness deadline expiring before its real
HTTP child could start through the guardian, and the kill fixture expecting only
UNKNOWN_EFFECT while the underlying socket reported ConnectionResetError. The
fixture now uses its original five-second service startup bound to actually
exercise started-but-unready cleanup, and explicitly accepts that kill-induced
socket failure. PID/birth disappearance and no-premature-release assertions are
unchanged. Both corrected cases, the observer-exception case, a repeated worker
kill and all six original owner tests passed: 10 passed, one unrelated Prompt
private-cache test deselected, in 22.33 seconds.

`MANAGED-HTTP-GUARDIAN-VERIFICATION-20261116.json` retains both invocations,
including the initial failures, their resolved per-node statuses, red/green
probe records, production source hashes and private allocation release states.
All launchers assert private RESOURCE_ROOT, RESOURCE_HOME and FORGE_DOMAIN,
pin this worktree, and write only owned `/tmp/demiflow-*` evidence.

## Explicit guarantee boundary

This remains the platform's **cooperating process unit, not an OS sandbox**.
The normal guardian, its catchable errors and single Dataset-worker SIGKILL are
covered, including a descendant that closes inherited FDs. A simultaneous
guardian SIGKILL followed by direct-service exit, while a detached descendant
has itself discarded the unit FD, is **not guaranteed** by this userspace scheme.
No cgroup isolation or universal hard-fault process-tree guarantee is claimed;
no counterexample is relabelled CONFIRMED. Root explicitly accepted this narrow
boundary for this delivery. Ray service lifecycle, GPU device authorization and
model-tree RSS admission remain separate follow-up work.
