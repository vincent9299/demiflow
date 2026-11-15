# Finite boundary residency: implementation and evidence

Implementation is isolated in `engine/gpt-v3-residency-20261115`, based on
`49b666f` plus the in-flight checkpoint fix `2c5b391` (local cherry-pick `68efeb9`).
Main checkout, production services and the seven frozen files are unchanged.

## Behavior and intent

A real Dataset with two global sorts now executes in at least three residency
phases when its total working-set reservation exceeds the admitted memory budget.
Each phase streams/spills through the existing finite exchange store. Driver
messages contain references, not a collected copy of the dataset. A phase must
finish and commit before it can retire. `complete()` no longer returns memory:
only the original ResourceManager's `cleanup=CONFIRMED` proof releases the charge.
The next phase starts only after every previous worker actually exits.

The default still launches all stages when they fit; no stage/plan identities or
user Dataset semantics change. Per-group CPU/memory/GPU admission replaces the
planner's unconditional sum over all phases. Runtime preflights every group
before activating business code. Streaming memory and durable-incremental edges
keep their participants co-resident; unsupported staging fails before effects.

## Single-journal phase cut protocol

The existing native CommitCoordinator and RunJournal remain the only durable
prepare/publish/receipt/ACK authority. The existing `dataflow-cut-v3` state carrier
has an optional `residency` object:

- `schema = demiflow.finite_residency.v1`
- `active_stages`: the actually resident stage IDs for this cut.
- `completed_stages`: retired stages whose earlier completion cuts were ACKed.
- `phase_complete`: every active worker actually emitted its done frontier.
- `finite_seals`: channel ID to the existing finite scope seal ArtifactRef.
- Optional `satisfied_source_splits`: declared source splits whose downstream
  logical limit was satisfied, authenticated before the cut. Recovery restores
  this fact without changing the source cursor into a false physical EOI. Old
  residency cuts without this optional field remain compatible.

`cut.participants` contains only this phase's actual workers. `contributions`
contains their actual current snapshots plus earlier retired snapshots, preserving
those earlier snapshots' original cut ID, boot ID, sources and state references.
No unstarted worker gets a contribution, completion event or observation.
Source vectors merge the actual current and retained contributions.
Only fresh contributions feed sink preparation, preventing old output replay.
Intermediate cuts are never final and never return a terminal result to the user.

Recovery verifies that every carried contribution equals its original earlier
ACKed native commit, and that every retired stage has an ACKed completion cut.
It rejects changed boot IDs, missing origin facts, undeclared stages/channels and
premature final cuts. A completed phase is not restarted; an unfinished phase
restores its own input/kernel/frontier and finite spool prefix. Finite consumer
frontiers prevent replay of already consumed members. Old v3 cuts without this
optional object retain the original full-participant validation.

GC must retain `residency.finite_seals` and their `producers[].manifest_ref`, in
addition to the existing contribution reference graph. No second ledger exists.

## Evidence

All commands used `PYTHONPATH=$PWD:$PWD/tests`, private
`DEMIFLOW_RESOURCE_ROOT=/tmp/demiflow-residency-*`, `python -B -m pytest`, and
`--basetemp=/tmp/demiflow-residency-*` from this isolated worktree.

- `/tmp/demiflow-residency-real3.xml`: **27 passed**, 2 initial real phase proofs
  plus 25 public Dataset/native commit/recovery tests.
- `/tmp/demiflow-residency-final2.xml`: **26 passed, 1 skipped**, 4 phase proofs,
  17 original checkpoint tests, 5 resource progress tests. The phase proofs cover
  confirmed exit before re-admission, unchanged historical contribution/source
  vector, forged origin rejection, crash after phase commit, crash inside the
  first phase with a partial finite spool, and infeasible memory-connected groups.
- `/tmp/demiflow-residency-ray.xml`: the skipped Ray test was actually run using
  `env-ray/bin/python`: **1 passed**. It starts its own `address='local'` cluster,
  verifies the same three-phase native cut protocol, forbids Ray Data entry
  points, and shuts down only that owned cluster in `finally`.
- Legacy lane-unit probes P/E are **not counted as passed**: P has 7 failures and
  E has 19 failures from stale fake DTO/API fixtures; the primary coordinator was
  notified. They are owned by the separate regression repair lane.

Additional business edge evidence after the initial phase/device commits:
`/tmp/demiflow-residency-phase-edges2.xml` has **2 passed** for logical
`limit(2) -> sort -> sort` phase recovery and real phase `write_lance(create)`
recovery. The limit case proves final=true while physical source EOI remains
false. The write case proves upstream phase cuts do not create an empty table;
resumption creates exactly Lance version 1 with the four expected rows.
`/tmp/demiflow-residency-edges-final.xml` has **23 passed, 1 skipped** combining
all six Local phase proofs and 17 existing checkpoint tests. Ray's earlier
same-host execution proof is recorded above; no new physical-host claim is made.

## Explicit limits

This proves bounded *admitted working sets* and confirmed process lifetimes,
not hard RSS isolation or cgroup memory enforcement. It proves same-host Local
and Ray Core, not multiple physical hosts. Arbitrary durable-incremental parking
and resumption is still a gap: live ACK/credit ownership would need a separately
specified parked-channel cut. No durable edge is silently converted to finite,
and timely streaming semantics are not replaced with full materialization.
GPU placement checks alone do not prove device assignment: provider/device-pool
assembly and actual child device visibility are a separate follow-up commit.


2026-11-15 后续：上面的 durable/GPU 限制是该有限分期提交当时的边界；后续实现与实际证据分别见 [DURABLE-RESIDENCY.md](DURABLE-RESIDENCY.md) 和 [DEVICE-GRANTS.md](DEVICE-GRANTS.md)，不可把新切片等同于任意算子可抢占或跨物理宿主验证。
