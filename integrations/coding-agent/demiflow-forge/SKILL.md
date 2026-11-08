---
name: demiflow-forge
description: Develop or resume a local demiflow Forge Pipeline under its frozen contract, behavior tests, review, and revision-bound lifecycle.
---

# demiflow Forge

Use the standalone `demiflow forge` CLI. Do not read DemiForge, Demiurge, or a business repository as a substitute for the project's `contract.lock.json`.

1. For a new Pipeline, inspect `demiflow forge schema request` and `rules --json`, create a closed Request, then run `new`. For an existing project, run `status --json` and inspect the current diff.
2. Before editing, read `context --json` and the current Definition, operator, tests, and README. Only edit `pipeline.py`, `operators/`, `tests/`, RunConfig, and README. Do not edit the Request, project binding, locked context, generated Manifest, evidence, or contract to make the task pass.
3. Implement only the declared `read_lance → map → write_lance` flow. Do not add model/network clients, schedulers, operator registries, hidden subflows, unregistered modules, or external writes.
4. Regenerate Manifest after source changes. Run `check`, `dry-run`, and `verify`; inspect actual logs and fix the implementation rather than weakening tests. Tests must use local fixtures and must not modify project sources.
5. Review against the same context and current revision. Record concrete file/line findings; self-review is not independent review. Do not Freeze with failed, missing, or stale gates.
6. Run only after current Freeze and only with project-local `datasets/` Lance paths. Report verification scope and unverified business quality separately. Production execution, model nodes, and evaluation-driven evolution are outside contract v1.
