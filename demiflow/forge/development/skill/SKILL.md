---
name: demiforge
description: Develop or resume business pipelines under the versioned DemiForge standard, project context, scoped changes and snapshot-bound verification. Use for pipeline creation, edits, review and handoff; not platform development or production execution.
---

# DemiForge

Use the target business project's `.demiforge/project.json`, not the current
directory by assumption. From a parent workspace identify the target repository
and read its AGENTS.md first. No project registration means bootstrap is needed;
do not claim enforcement is already active. The tool is at
`scripts/forge.py --project <absolute-project-root>` relative to this skill's real
location, the project's `python3 .demiforge/forge` launcher, or the installed
`demiflow forge dev --project <absolute-project-root>` public CLI. Use `--help`
for command syntax. Source-checkout setup is in `docs/demiforge-quickstart.md`;
wheel installations contain this Skill, its launcher and all nine bound standards.

## Before Editing / Resuming

- Run `list` and `history --id <task>` to inspect saved tasks read-only. Saved
  accepted results describe their original snapshot; migration makes old bindings
  stale and does not authorize continuation. Use `context --id <task>` only for a
  task with the current tool/config binding. Do not create a new
  baseline to make existing violations disappear. Inspect the current diff too;
  the baseline already includes user changes and does not assign authorship.
- For a new task, bind the actual request with `start --id ... --pipeline ...
  --goal ... --write <precise-project-relative-path> --non-goal ...` before edits.
  Multiple `--write` / `--non-goal` flags are supported. Register a new pipeline
  and trusted fixture commands before starting its development task.
- `context` lists authoritative sources, selected excerpts, pages and evidence.
  Read every required page with `context --read <source-id> --page N --id ...`;
  do not suppress output merely to mark a source read. Receipts mean text was
  delivered, not that it was understood. Follow any relevant references and read
  implementation/tests beyond the index when needed. If selection omits a binding
  rule, correct the project context deliberately before starting the task.
- Use the bound common standard plus project business definitions and public
  platform contracts. The project may specialize business semantics, not disable
  common rules. Surface conflicts or missing authorization instead of guessing.

## During Development

Stay in the declared writable scope. Platforms (demiflow in the pilot) remain
read-only. Record platform gaps separately; never fix them through business-side
clients, private APIs, schedulers or platform patches. Development permission is
not production/model/network permission. `--allow-tests` only acknowledges running
the registered trusted tests; it is neither user authorization nor a sandbox.

Use `note --kind decision|progress|question|unverified|next --text ... --id ...`.
Distinguish user-confirmed decisions from proposals; lasting business facts belong
in the pipeline README. Questions block acceptance until `resolve --question N
--reason ...`. Changed decisions invalidate earlier evidence. After editing a
required source, use `refresh --reason ...`, then read changed pages; refresh never
resets the original change boundary. Tool/standard/project-config changes require
an explicit new task, not silent rebasing.

For projects adopting candidate.4 or later, follow the README maintenance requirement in
[DF-02](../standards/pipeline-v1.0.0-candidate.4.md#readme-maintenance-for-every-change-2026-10-12)
on every pipeline change. Include the affected README in the writable scope and
update its relevant sections during the same task. Check the final diff against
the README in DF-02 review and cite both; a mechanical check alone does not prove
the documentation is current. Keep older task bindings intact when upgrading.

For candidate.9 and later, use project-v2 registrations with one owned directory
per pipeline. Register the actual repository-relative `entry: {path, symbol}`,
operator files/directories and optional notebooks inside that owner. File and
directory names do not determine roles; keep wiring, configuration, transforms,
documentation and tests with the project instead of splitting/copying them into
repository-wide trees. Shared operators are imported from their declared owner.
The authoritative layout and dependency rules are DF-02 of the bound standard.
Candidate.7/.8 bindings retain their historical layout only until explicitly
migrated; do not apply those layout requirements to new candidate.9 projects or
claim old evidence under the new standard. Never reduce watched coverage to fit a budget. Inspect capacity,
set explicit bounded `snapshot_limits`, and retain full notebook/output hashes.
Platform contract verification remains a registered fixture command; this skill
and the structural checker do not implement or authorize platform execution.

The current candidate.9 tool selects coverage by the task's actual standard,
including in mixed projects. `runs`/`datasets` are legal author path names, with
roles, ownership, watch, readonly and writes checked separately. Normal watch
coverage and full notebook/output hashes remain intact. Under broad watch roots,
those names are pruning candidates: declared files and supported static local
imports penetrate only the necessary branches; explicit operator/watch subtrees
cover their declared contents. A precise source exception does not expand sibling
history/table trees. Technical control, environment and cache paths stay protected.

Inspect `source_coverage` in context/check/handoff. Unknown pruned ranges are
partial coverage and fail check; the tool cannot classify them as artifacts by
name. Start rejects writes intersecting unknown coverage and necessary unwatched
local sources. Watched missing implementations and syntax errors may be drafted,
but cannot pass. Clarify actual sources using the existing registration fields
under the request's authority; never silently shrink watch, change business
registration or reset an original baseline to remove a finding.

Local import checks are static: module existence, supported top-level symbols,
package initializers, re-exports and dependency direction share the same bounded
observation and snapshot. Candidate code and installed packages are never imported
to discover sources. External imports, dynamic imports/reflection, arbitrary file
reads and runtime effects are outside this proof. Necessary facts must fit the
declared budgets and the tool's inspection limits; truncation is not completion.
File/package/missing-state races reject the observation. Verification re-observes
after commands; added dependencies change current evidence without rebasing the
original tree or readonly baseline. Older standards and saved task bytes retain
their original rules and bindings; use history for stale tasks.

For candidate.8 and later, apply the bound standard's DF-03 performance review:
identify full-data actions and repeated execution, and validate startup cost,
throughput and temporary I/O for performance changes. Do not invent a second
policy here or claim automatic performance certification.

## Verification / Handoff

1. `check --id ...`: inspect changed paths, unread context and structural findings.
   Fix failures; never disable mandatory checks to get a green result.
2. `verify --id ... --allow-tests`: only when those fixture commands and effects are
   within the actual request. Inspect the saved logs. No real models or production
   runs are implied. Tests must not modify source; any relevant change invalidates
   previous verification.
3. Review DF-01 through DF-07 against the actual changed files and tests. Supply a
   JSON object keyed by those seven IDs; each value has `status: "pass"`, a specific
   `reason`, and `evidence: ["project/relative/file"]` from the watched sources.
   Only DF-05 permits justified `"not_applicable"` when no prompts/models apply.
   Use `review --file <json> --reviewer <identity-and-self/independent> --id ...`.
   Do not auto-fill passing claims or call self-review independent verification.
4. `handoff --id ... --accept` rejects missing/stale/failed evidence and unresolved
   questions. Without `--accept`, it can save an incomplete handoff. Report exact
   changed scope, tests, remaining gaps and next step. Acceptance is local to this
   development task, not whole-pipeline compliance or business-quality approval.

Records live in `.demiforge/tasks/<id>/`; they are local and ignored by Git. Use
`handoff.md` / `handoff.json` to continue on this machine; explicitly export selected
non-secret evidence for cross-machine handoff. Check current evidence on every
resume: an old accepted handoff is only an as-of snapshot. `history` pages saved
artifacts without writing receipts or substituting current source text. It does
not implement cross-tool continuation; retain the original baseline and unresolved
facts for separately reviewed continuation when needed. The skill cannot prevent
shell commands outside the tool or recover deleted local state.
