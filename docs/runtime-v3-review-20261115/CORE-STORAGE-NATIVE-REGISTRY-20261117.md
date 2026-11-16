# Core/storage historical acceptance: native registration repair

Production candidate: `ec1ee1f512ab895e5beaea7c05d5a9ce2798db3f`, based on root `01413fc`.
Owned tree: `/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_v3_core_storage_validation_20261116`.
Branch: `engine/gpt-v3-core-storage-validation-20261116`.

## Real defect and repair

The original remaining-history first batch selected 67 tests: **45 passed, 22 failed**.
Ten registry failures share an actual public-entry omission: `write_registered_table`
still invoked retired `checkpoint_lance`. Merely restoring that old executor would
violate the single-engine intent. Its old driver closure row counter would also
remain zero when the actual iterable runs in the managed worker.

`write_registered_table` now calls the original public `Dataset.write_lance`
through its original checked action, observation, compiler, workers, native sink,
RunJournal, commit and source ACK. A narrow internal `lance_action` extraction
allows the original RunAction to be located before source activation. Public
Dataset parameters, native-option parsing, default None result, optional receipt,
checked receipt and observer completion remain unchanged.

A bounded, atomic `registered-native.json` contains **only** schema version,
RunRef location/run_id and action_id. It does not contain copied results, progress,
row counts, versions, fingerprints or replacement receipts. The fingerprint/schema
request is bound in the original RunJournal. Fixed version and row count come only
from the original final ACK/native Lance receipt and are checked against the table.

Without an active run, registration owns a bounded ordinary RunContext under its
control directory. With an active run, it reuses it and never closes it. An already
confirmed result is read without executing or validating a replacement factory.
An unfinished write resumes the original run and passes its original code/source
admission; a different factory is rejected. An unfinished externally owned run
must be resumed explicitly by its owner. Owned locator paths are relative so a
root move preserves confirmed fixed-version reads. A missing locator beside an
already-created original run fails closed rather than inventing recovery authority.

Historical checkpoint sidecars remain read-only compatible. No new checkpoint
sidecar or retired executor is created. `LanceRun` remains an unreferenced legacy
helper in `lance/run.py`; no public facade/export/current tests use it. Its retired
execution is not resurrected by this change.

## Test observation migrations

* The old scan spy did not expose real dataset version/fragments and lived only
  in the driver. The replacement transparently delegates the real dataset and
  records the actual managed worker scanner call in a test-owned file. The three
  original bounds and all seven rows are still asserted, plus a different PID.
* The two overwrite source-failure tests preserve exact `RuntimeError` and exact
  original message. The source now fails before native prepare, so fabricating
  an indeterminate Lance publication receipt would be false. They additionally
  assert unchanged old table/version and no final/native publication in journal.
* All eight old RayData-private-adapter combinations now run **real Ray Core**
  public Dataset writes. overwrite/merge × empty/nonempty × expected-version
  combinations, receipt versions, old versions/values and exactly one external
  publication remain asserted. Legal nonfinal ACK cuts are allowed only with
  no native preparation or native receipt. No RayData execution is restored.

## Evidence

All formal runs use the unchanged `execute_batch` and
`runtime_v3_regression_inventory.py`, which assert the actual import tree and all
three exact private domain variables **inside the pytest process**. Inputs are
hashed before and after; all listed formal receipts report unchanged source and
all setup/call/teardown outcomes are retained.

| Evidence | Result | Receipt SHA256 |
|---|---|---|
| Original first 67, `/tmp/demiflow-perf-owned-core-history-r1/core_storage_passive-000/receipt.json` | 45 pass / 22 fail; source `01413fc` | `1f572de4002f2394ec8b003c63709bcb5dfcc5bce46db1a83e0f4c3f12c37eb7` |
| `/tmp/demiflow-native-register-r3/batch/receipt.json` | 8 pass / 3 fail; includes all four new registration recovery cases passing | `0e150d8e410836ddf920e32738c7b5535ba3f1ffc758411bade2a8f8b1620858` |
| `/tmp/demiflow-native-register-r5/batch/receipt.json` | **54 pass**, 132.97s, `ec1ee1f`, 162 phases passed | `3c3442d5c1bcc69c91e7390e01a74a59d2b0ab0c5e7fdff745716e2b6aa26e42` |
| `/tmp/demiflow-native-register-ray-r2/batch/receipt.json` | **8 real Ray pass**, 46.20s, 24 phases passed | `8e0f2a890077bda552f8345cf8d39a23b6332c57294172d7adb058bf3b8abe81` |

The 54 comprise original registry 18, scanner 13, local write modes 13,
new registration/recovery 4, and original checked sink rejection 6. The stale
expected-version case is assigned to the Arrow lane and was explicitly not
included here. The Ray receipt predates only a registry fingerprint-whitespace
validation and the separate pending append-schema test edit; Dataset helper and
all eight Ray test bodies exactly match `ec1ee1f`. The 54 receipt includes the
uncommitted append-schema test in its input inventory, but does not execute it.
A later registry header-docstring correction has no executable delta.

Earlier diagnostic red receipts are preserved: r2 rejected non-exact selectors
before pytest, r3 exposed real BlobType admission plus my over-strict message
regex, r4 exposed my incorrect nesting that swallowed the original failure before
RunContext exit, Ray r1 exposed my incorrect single-journal-cut assertion. These
are not called successful acceptance, and no production protocol was changed to
accommodate those test mistakes.

## Explicit remaining work

* The **572 original selectors after batch 000** are not covered by this report.
* Original stale expected-version preflight is owned by Arrow lane.
* `test_lance_artifacts.py::test_streamed_blob_checkpoint_replays_without_reopening_source`
  exposed real `batch_conversion` rejection of `lance.blob.v2`; Arrow lane is
  implementing bounded native BlobType handling. The other three artifact tests
  passed through this repaired registry.
* The append schema-mismatch historical selector needs the Arrow lane's
  pre-prepare validation. Its narrow test migration preserves the selector,
  expects exact deterministic InvalidLanceRequest, verifies unchanged old version
  and rows, and excludes prepared native/final publication. A pending intent with
  `prepared=None` and empty receipts is legitimate before prepare succeeds.
* No claim that all 639 tests, the whole product, or all engine intent is complete.

## Private-domain command incident, excluded from acceptance

An initial manual command incorrectly set `DEMIFLOW_RUNTIME_DIR`,
`DEMIFLOW_CACHE_DIR`, `DEMIFLOW_STORAGE_DIR` instead of the three effective domain
keys. Its 18 passed result is **not formal evidence**. Full command, effective
keys, time window, process records, allocation records and read-only investigation
are in [CORE-STORAGE-DOMAIN-INCIDENT-20261117.json](CORE-STORAGE-DOMAIN-INCIDENT-20261117.json).

The process exited normally before the stop request; the historical driver PID
was not captured and is not reconstructed by guesswork. Because TMPDIR was
private, actual default capacity was still under
`/tmp/demiflow-native-register-r1/tmp/demiflow-capacity-783128e571016bbe9b8e4569`:
20 allocation records all RELEASED, 22 recorded worker birth identities gone.
Default shared HOME/FORGE trees had no surviving mtime/ctime changes in the test
window. There is no before-run snapshot, so this does not prove absence of every
possible transient shared write. No shared files, records, or other allocations
were deleted or released. Parent reviewed this boundary and authorized resuming
only the original asserted runner.

No main-repository edits/tests, pushes, environment installs, shared-cluster stops
or protected-file edits were performed. The seven frozen files match `01413fc`.
