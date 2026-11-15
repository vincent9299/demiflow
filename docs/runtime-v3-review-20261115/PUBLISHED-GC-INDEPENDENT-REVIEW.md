# Independent review of published-orphan GC

Candidate: `0d729696fb4c7fe5b6fb1d81fae746cac075275d`. Independently reviewed and exercised in isolated residency tree at `ec2ca5f`, which additionally includes both real late-final completion fixes and root's idle-notification fix. No main checkout, shared environment, production service or candidate-owner dirty tree was modified.

**No additional history, physical-lease or generation-refund blocker was found in the inspected and exercised scope. Overall resource acceptance is still blocked by the separately confirmed file-descriptor budgeting gap.** The module owner measured five actual store/scratch/read descriptors under `max_open_files=2` and is implementing a separate correction. This report does not treat that pending fix as delivered.

## Source-level adjudication

- `RunJournal` remains the persistent authority: original object registration and reservation charge own each physical generation. GC intent/quarantine/refund rows live in that same journal. The inode/header is physical exclusion evidence, not a second history or budget authority.
- Root traversal reads all original historical actions, source declarations, commit intents/prepared records, receipts, effects, child scopes and native calls. It does not restrict protection to the latest or successful cut. Owner pins recover their original typed publication, including media aliases.
- StateArtifacts, nested record strings, typed state, channel base64 records, finite borrowed seals, segment-tree descendants and payload/schema references are traced. Unsupported, missing, over-budget or corrupt carriers stop selection of new deletion candidates. Already prepared GC intents are reconciled using their original exclusion/publication protocol.
- Publication and root mutations use original shared exclusion; collection holds exclusive physical ownership and original publication ownership. A live read generator retains its own shared lease across yields even after its store closes. Real resource cleanup and native journal thread exit remain separate requirements.
- Refund requires original inode/hash/size, physical quarantine/removal and exact original `charge_id`. Original settlement receipts are immutable. A reused content hash receives a new charge generation; repeating an old completed refund cannot debit that new generation.
- This remains quiescent collection. It does not trim historical roots or promise bounded disk for infinitely retained history. Deadline/scan bounds and conservative refusal are explicit; OS-level I/O is not claimed to be interruptible by a Python timer.

## Independent execution

**17 passed, 40.01 seconds**, `/tmp/demiflow-gc-independent-tests.xml` and `.log`.

The selection includes the real old ACK plus later pending intent and nested borrowed/segment/payload/schema roots; owner/unknown-media/missing-root/scan-budget cases; surviving read FD and native batch-journal thread; actual process crashes after rename and unlink, twice each; real Dataset materialization and historical reads; and all five ordinary/durable final-completion and incomplete-prefix cases. It intentionally excludes H's Ray doubles and does not repeat the complete Ray API matrix.

Two independently written counterexamples add a gap beyond the candidate's original cases:

1. Interrupt the original collector after rename or after unlink, leaving its authentic pending journal intent and old charge.
2. Attempt to create a new writer using the pre-interruption authority: it must fail closed, preserve the pending charge, and release its failed initialization's shared lease.
3. A fresh original authority first reconciles the pending GC, then publishes the same bytes under a new charge generation.
4. Repeating the old `finish_dataflow_gc` returns zero; the new object and full new charge remain, while the old settlement rows are unchanged.

Both passed. Reproduction source is `/tmp/demiflow-gc-independent/test_gc_generation.py`; the paired JSON embeds its exact contents/hash, command/environment, every testcase and the integrated source hashes. The module owner received these probes for its FD follow-up regression.

The protected seven files remain byte-identical to the original baseline. No multi-host/NFS, physical GPU, production database, new full browser/API matrix, active-action GC or history-retention deletion claim follows from this focused review.
