# Workbench project source resolution and text selection

Workbench resolves a project's Python sources from its selected directory and declared package. Managed development, plan preview and managed run submission share the source resolution rules. A platform operator can grant a workspace access to explicitly named external Python source modules. Preview and managed submission record the same source digests. An unresolved import produces a diagnostic that identifies the missing module without searching other projects. The Workbench sidebar and graph allow normal browser text selection.

On the fixed integration commit `cfe5ec723ccb286dd424275740a0a7aadf051627`, the seven actual RefGen entries each produced an engine plan using their declared dependencies. The reference delivery plan had 16 nodes. The test used a private Workbench service and registry and did not run these business pipelines. Six focused native-run and source-binding tests passed. All 268 pre-merge guard tests passed.

The [known-state inventory](engine-known-state/cfe5ec7.json) preserves the four original acceptance lanes.

| Lane | Collected | Executed | Passed | Failed | Errors | Skipped | Timeouts | Not run |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Standard | 4856 | 4720 | 4529 | 94 | 33 | 64 | 0 | 136 |
| Historical | 1808 | 1790 | 1580 | 102 | 0 | 108 | 0 | 18 |
| Ray | 451 | 451 | 440 | 10 | 1 | 0 | 0 | 0 |
| Historical Ray | 13 | 13 | 13 | 0 | 0 | 0 | 0 | 0 |

The hash-route recovery test's deliberate duplicate was rejected in both versions. The test then compares two snapshots of the entire journal while an independently scheduled checkpoint may be acknowledged. Controlling only that scheduling reproduces the original failed assertion three times on the prior release and three times on this commit. Checkpoint 3 goes from `checkpoint_committed` to `acked` without adding a duplicate. Separate unmodified whole-file runs also reproduced an alternate fixture failure, “no active worker at the nonfinal cut,” on both versions. The first failure remains in the standard lane.

In the unprivileged acceptance account, a SQL Notebook fixture and a browser test's hardcoded temporary JS file encountered permission errors. Both versions reproduce the SQL permission error three times and pass under the original release account. On that original account both versions pass W018 and W020, and reproduce the same existing W019 and W021 assertions. The Ray worker-loss test reproduces its previously observed teardown FD error. The prior performance deferral is still recorded as not run.

Local release evidence is under `_demiflow/shared-review/workbench-shared-sources-release-20261120/`. It contains complete receipts, source bindings, fixed-commit comparisons, and all repeated reports. The Workbench service preview has a separate deployment record.
