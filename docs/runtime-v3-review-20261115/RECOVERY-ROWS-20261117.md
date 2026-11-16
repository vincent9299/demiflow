# Original actor recovery inputs — 2026-11-17

Isolated branch `engine/gpt-v3-shared-actor-owner-20261117`, base `92c99a2`, in `gpt_v3_perf_mainpath_20261116`. Main checkout and the seven frozen files are unchanged. No push or main merge.

The unified kernels previously ignored `recovery_rows`; an exhausted or empty source silently lost the documented 101/102 recovery inputs. The hook now feeds the original actor, including a shared actor's original first occurrence. It does not send the hook result directly to the terminal.

Recovery inputs and an invocation's pending outputs use the original bounded spool/ArtifactStore. The original kernel image contains their references, input/output cursors and completion flags together with actor state. A hook that eagerly clears its pending list is consumed into bounded storage before calls start. Each call's normalized output is staged before a prefix can be published, so a cut midway through expansion resumes the suffix without repeating the call sealed in that immutable image. Unconfirmed pure UDF work after the cut may replay after process loss. Row, count, artifact and spill admission remain the original authorities; checkpoint interval/frequency is unchanged. Original cut/journal/ACK/GC keep ownership and restore authority.

The shared actor hook waits for live calls within its convex ownership closure and external inputs to drain. It does not wait on unrelated downstream batch/native activity. Existing paused/finish-replayable protocol controls the generator; source EOF is not final completion. The original worker runner change is confined to `_advance_input_completions`; root owns concurrent source and observation changes.

A shared recovery hook spanning a finite-complete intermediate stage is rejected before actor startup. Such a stage cannot accept new input after its completion, and silently skipping/repositioning the hook would be incorrect. Generic multi-slot class-factory recovery hooks and task-scoped synchronous hooks are not claimed by this evidence; the accepted cases use the original asynchronous actor contract. Small-worker placement remains a separately assigned follow-on.

## Accepted evidence

All batches use the original regression inventory runner, exact nodeids, in-process import-root and three private-domain checks, private scratch and three-phase receipts. Source hashes stayed unchanged throughout each batch.

- Final Local batch: **11 passed in 68.00 s**. Original final ACK interruption/resume preserves action facts; 10 recovery inputs expanding to 30 outputs survive a nonfinal ACK interruption without reinvoking any input already covered by its immutable kernel image; shared actor linear/timed-batch and union-branch tails complete; oversized row/infinite hook hit original bounds before actor invocation; finite-stage shared recovery is rejected; original process ownership/recovery remains green.
- Real private Ray Core batch: **1 passed in 16.25 s**. Empty source, same actor at two occurrences, two recovery inputs produce exactly two rows with two visits, one hook invocation and four real actor calls. The fixture shuts down only its own cluster.

The accompanying JSON records all receipt paths/digests and candidate input hashes. This is a component delivery; root must combine it with publication overlap and global admission changes before whole-engine acceptance.

## Publication overlap follow-up

The root combination legitimately ran an unconfirmed suffix input during publication. The initial test incorrectly asserted physical exactly-once for that suffix. It now reads the original ACK's kernel image and requires a cut partway through one callback's staged output. The actor calls count must equal input_cursor; every confirmed input including that partially emitted call runs exactly once across attempts; restore reinstates that exact counter; post-restore calls are exactly the remaining input IDs. Final 30 rows and one hook invocation remain strict. This is stronger image-bound verification, not a blanket duplicate allowance. The new assertion passed on the overlap baseline in 12.49 s.
