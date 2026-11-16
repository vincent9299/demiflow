# V027 real actor-owner loss

## Coordinator integration correction (2026-11-16)

The original c761875 pass below is a historical result. Independent integration
at `5ee2d49` found a fixture race: `HostedDataflowWorker.close` cancels its HTTP
request and can finish before the asserted fault boundary (2 failed / 22 passed).
The engine was not changed to accommodate that assumption.

Test-only fix `9272433` first stops the exact owned actor process (PID and birth
identity checked), proves it stopped, then submits the original `identity` RPC.
It kills that process with SIGKILL while the genuine RPC is pending. Ray reports
ActorDied and zero restarts. No lifecycle cancellation precedes the fault; the
original accepted Prompt/Embedding/Agent intent stays unknown. Output, original
ACK prefix, request keys, single charge, no retransmission and no fabricated
commit assertions all remain. Two refusal checks now additionally reject an
unstopped process and mismatched dead-owner evidence.

At the fixed root revision, formal V027 + V033 + the guards passed **23 tests**
with **4 standalone component nodes deselected** (their four scenarios run
inside V027). All 2,184 source digests match that commit and stay unchanged;
the original FD allowance is unchanged, the private Ray cluster shuts down,
and the four killed workers are absent after exit. Exact commands, per-node
results, retained red and proof digests are in
[ACTOR-OWNER-LOSS-ROOT-VERIFICATION.json](ACTOR-OWNER-LOSS-ROOT-VERIFICATION.json).

The sections below describe the earlier candidate. In particular, their
`close`-as-pending-RPC assumption is superseded by the correction above.

The formal `tests/test_runtime_v3_acceptance.py::test_node_loss` now executes
actual Ray worker loss and recovery. Fixed candidate
`c76187520131ff8fd36863d3999b906b30ba0534` passed **21 tests in 48.18 s**, with
zero failures/skips. Four standalone component nodes were deselected because
the formal node executes all four components itself. No production change was
needed by these cases.

Worktree: `/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_v3_regression_20261115`.
Branch: `engine/gpt-v3-regression-20261115`.
Implementation commits: `11b2892` and `c761875`, on top of V033's delivered
commits. Changes are confined to the new scenario/test modules, node_loss's
support delegate/environment/self-managed entry, and its evidence-integrity
guard. Merge the independent CPU/GPU gate's support changes by symbol.

## Original contract and scope

V027 explicitly permits killing a producer node **or actor owner**. This test
kills actual `HostedDataflowWorker` actors on an owned same-host Ray cluster.
It does not claim physical host shutdown, a network partition or two-host
placement. The independent `two_hosts` gate retains its mandatory two-host
environment check. Local processes cannot stand in for Ray actors; the
scenario records NotRun if selected with a Local provider.

The original engine uses durable ArtifactRefs. `resolve_data_location` does
not manufacture Ray ObjectRefs. The test's public map business function
deliberately creates a transient `ray.put` cache and serializes a locator so
the real driver can borrow it before killing its actual worker owner. This
cache is **never** included in a checkpoint or used for recovery.

## Executed producer recovery

Public `Dataset.range(12).map(...).take_all()` runs on the original allocator,
workers, journal and commit protocol. After an actual nonfinal ACK for two
rows, the observer locates the original transform actor by its sealed stage
assignment and Ray state, then kills it with `no_restart=True`. It observes
DEAD and zero restarts. The unmodified runtime, rather than a test-thrown
exception, reports `UNKNOWN_EFFECT: Ray worker became unreachable`.

The owned dead worker's actual code cache (four entries) is deleted. Original
store/journal roots remain intact. Resume increments the original attempt and
fence, retains exactly the same sealed plan ID and the complete original
commit prefix, and returns all twelve rows with a final native ACK. The first
two rows retain the old actor's identity and have only one producer invocation;
their business probe files show they were not recomputed. The original killed
actor remains DEAD with zero restarts after resume.

Completed Ray cache copies need not disappear immediately with their owner.
In the final run the borrowed 262,144-byte cache remained readable. The test
records that fact and its payload hash; it grants no recovery authority. The
serialized RecoveryImage is also checked not to contain that Ray locator.

## Executed pending RPC and unknown paid effects

Each of Prompt, Embedding and Agent runs its real public Dataset operator and
original native journal. Only the external model endpoint is a bounded
loopback fixture. Once that endpoint has durably recorded its accepted HTTP
request, the original journal already contains one unresolved intent and one
request charge.

The test calls the same original worker's existing `close` method while the
HTTP response remains outstanding. `ray.wait` proves this genuine actor-task
ObjectRef has not completed. It then kills that actor. All three original
pending RPCs fail with actual **ActorDiedError**; no synthetic worker or new
actor method is introduced. This supplies the unconfirmed-Ray-result loss
counterexample, separately from completed `ray.put` cache behavior.

Each initial Dataset fails through the original `UNKNOWN_EFFECT` boundary.
Resume creates the next attempt with a new fence, and the original
`UncertainPromptCall` guard forbids automatic regeneration. For **all three**:

- One HTTP acceptance before and after resume; no second request.
- One request charge, no refund or second debit.
- The exact original request key persists.
- One unresolved call, zero confirmed responses and zero fabricated commits.
- The killed actor remains DEAD with zero Ray restarts after resume.

This is original native effect handling, not a second effects ledger or a
mocked reserve/receipt. Five successful `execute_action` handle constructions
are directly observed: producer initial/resume and the three initial model
attempts. The three unknown-model resumes are rejected before a handle can be
returned; their attempts/fences and preserved facts come from the real journal.
No observation is fabricated for those early rejections. Ray Data import
attempts remain zero.

## Evidence and retained red results

Primary receipt: `/tmp/demiflow-v027-final-evidence/verification.json`.
Actual scenario: `/tmp/demiflow-v027-final-evidence/scenario-node_loss.json`.
Runner: `/tmp/demiflow-node-loss-fixed-runner.py`.
The adjacent closure JSON hashes these files, JUnit, logs and post-exit proof.

The receipt binds exact HEAD, full command, interpreter/PYTHONPATH, worktree
module paths, 2,179 input digests, per-node outcomes and the seven frozen files.
Source digests are unchanged before/after; Git is clean during verification.
The original FD guard remains unchanged: 38 before the fixture, 41 before
close, within its original +8 allowance. The owned Ray cluster was shut down.
Post-exit proof checks all logged worker/driver PIDs absent in `/proc` and no
remaining process referencing this private cluster directory.

The first implementation mistakenly required every completed `ray.put` copy
to become unreadable immediately after owner death. Both the initial small
cache test and fixed `11b2892` exposed that incorrect test assumption. The
fixed failed receipt `/tmp/demiflow-v027-fixed-evidence/verification.json`
retains **1 failed / 18 passed** and is not counted as passing. A separate
component run did observe OwnerDied for a larger cache, but that single
observation was not treated as a universal SDK guarantee. The coordinator's
contract ruling distinguishes completed cached data from an actually pending
actor task; final assertions lock the latter and preserve all original
ArtifactRef/ACK, identity, output, accounting and unknown-effect requirements.
No Ray object was freed or evicted to manufacture a failure, and no engine
budget, retry policy, API, test timeout or resource guard was relaxed.

Main checkout, production data/services, shared environments and the frozen
seven remain untouched. No push or main merge was performed.
