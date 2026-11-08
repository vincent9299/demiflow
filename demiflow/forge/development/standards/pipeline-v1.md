# DemiForge Pipeline Standard

Version: `1.0.0-candidate.1` (2026-10-06). Status: candidate, not a universal
certification. Changes to this document require a new version before distribution.

## Authority and Scope

This is the common engineering standard for business pipelines. A project binds
this exact version, public platform contracts, business definitions and verification
commands. It cannot disable these rules. A platform adapter supplies API mappings,
not alternative engineering rules. Conflicting requirements stop acceptance until
resolved explicitly. Existing project specifications remain authoritative during
migration; this candidate does not silently replace them.

The standard is derived from demiwtg/PIPELINE_SPEC.md S01-S12. QID semantics,
Lance schemas, image ownership, thresholds, credentials and deployment paths are
NOT part of the common standard. The initial reference is the demiwtg/subset
fixture-backed development task, not a claim that every legacy pipeline complies.

## DF-01 Context and Change Boundary

Before editing, bind the task goal, non-goals, exact writable paths, required
sources, platform boundary and allowed verification. Read current instructions,
business contracts, target README, implementation and relevant tests. Context
selection must be explicit: excerpts are not full-document reads. Record durable
decisions in their owner document, and task progress/questions/evidence in the task.
Resume from current files and versioned evidence, never chat memory alone.

No business task authorizes platform patches, dependency installation, real model
calls, collection, production execution, data migration or publication. A platform
gap becomes a separate issue with a reproducer and affected public API. Do not
build a private client, scheduler or alternate execution engine to bypass it.

Example: changing `orders/operators/validate.py` does not authorize changing the
execution platform or rerunning the production orders table.

Acceptance: source receipts, baseline-to-current scope comparison, explicit
non-goals and reviewer confirmation. File checks detect changes only in the
declared watch/readonly roots; they are not a filesystem or network sandbox.

## DF-02 Layout and One Entrypoint

Each pipeline owns `<prefix>_pipeline.py`, `<prefix>_debug.ipynb`, `operators/`,
`prompts/` when using prompts, `tests/` and `README.md`. Optional `configs/` holds
named business parameters. Runtime datasets, runs and caches are not source code.
The Python entry exposes `config(...)` and `run_pipeline(config)`; CLI and notebook
call this same entry. README links the applicable standard/project specification
and explains input/output, stages, failure, completion, resumption and validation.

Operators contain business transforms, not hidden orchestration. No pipeline
imports/calls another pipeline's execution entry. Shared schemas and pure domain
helpers can have an explicit shared owner. Notebooks contain parameters, calls and
inspection, not function/class definitions or a second implementation; preserve
existing outputs and metadata unless explicitly asked to change them.

Acceptance: AST, layout, notebook and import checks plus review of actual routing.
Static checks cannot prove dynamic imports or all runtime call relationships.

## DF-03 Explicit Flow and Model Boundary

The main flow visibly expresses fixed input -> selection -> transforms -> model
nodes when needed -> validation -> writer. Use the platform's public data APIs;
do not collect a large dataset to a driver list and rebuild hidden subflows.
Ordinary inference uses standard platform model nodes with explicit input/output,
concurrency, failure policy and budget. A local agent is justified only by genuinely
dynamic decisions, with bounded tools and stopping conditions, not as a substitute
for a known DAG. Cross-pipeline work uses persistent, identifiable handoff records,
not nested pipeline execution or hidden in-memory coordination.

Acceptance: semantic review of the entry and operators; fixture tests of stage
relationships. Core checks intentionally do not guess framework semantics.

## DF-04 Identity, Scope and Completion

Pin actual source identity/version and define row granularity, keys, field ownership,
joins, deduplication and output schema. Apply the authorized scope before expensive
operations. Preserve source fields and relationships where the business contract
requires them. Do not reconstruct authoritative references from incidental paths.
Empty input, excluded rows, business rejection, technical failure, inconclusive
results and incomplete stages must not masquerade as successful full completion.
Keep the declared denominator; failures do not silently disappear from statistics.

Example: an image without a downloadable object remains explicit pending metadata;
it is not reported as downloaded or silently removed from the selected cohort.

Acceptance: schema/identity/failure fixtures and business contract review. Real-data
coverage and quality require separate authorized evidence.

## DF-05 Prompts and Review Contracts

Version prompts with model settings and output schema. Make the task, available
input, judgment criteria, output and useful examples explicit; structure headings
by actual content, not obligatory boilerplate. Keep reusable instructions stable
and per-row material late. Inspect the rendered request, not just a template.
No secret dependencies on omitted context, unavailable images or previous chats.
Distinguish author checks from independent review; state which evidence supports
each judgment. Specific rubrics and prompt text belong to the business project.

Acceptance: rendered-request/schema tests and semantic review. No model calls means
an explicit not-applicable finding with evidence, not an omitted review category.

## DF-06 Failure, Reuse and Resources

Declare limits, concurrency, retries, timeout, cleanup, idempotency and publication
boundaries. Bind reuse to actual input, prompt, model and semantic configuration;
changed identity invalidates reuse. Preserve uncertainty after interruption rather
than blindly retrying paid or externally mutating actions. Stage completion is not
whole-pipeline completion; multi-table publication is not automatically atomic.
Use platform lifecycle and storage contracts, not business-owned infrastructure.

Acceptance: targeted failure/resume tests and resource/publication review. A local
timeout is not a memory limit, network isolation or permission to run production.

## DF-07 Verification and Handoff

Separate mechanical checks, behavior tests, reviewer attestations, authorized real
runs and business quality. Bind evidence to the exact watched code, tests, prompts,
config, standard, selected context and platform snapshot. A changed fingerprint
invalidates earlier acceptance. Tests cannot update sources and still certify the
pre-test snapshot. Successful processes are evidence only for what their registered
commands actually check. Do not add full-table scans without incremental value.

Handoff states goal, changed files, decisions, unanswered questions, verification,
unverified scope and next steps. Local acceptance is scoped to this development
task, never blanket certification of a pipeline, dataset or platform.

## Ownership and Check Coverage

| Rule | Source in initial project | Mechanical coverage | Required review/test |
| --- | --- | --- | --- |
| DF-01 | S01, S11 | selected context hashes, watched scope, readonly roots | authorization and context sufficiency |
| DF-02 | S02, S10 | layout, Python AST, notebook definitions, direct imports | one actual execution path |
| DF-03 | S03, S04 | direct cross-pipeline imports only | flow/platform/model boundaries |
| DF-04 | S04, S05, S11 | no generic semantic proof | identity, schema, scope, failure fixtures |
| DF-05 | S09 | no generic semantic proof | rendered prompts or justified N/A |
| DF-06 | S06, S07, S08 | verification subprocess timeout/output bound | reuse, failure, resource behavior |
| DF-07 | S12 | snapshot-bound local records and stale-evidence rejection | test relevance, limits, next steps |

First candidate limitations: Python pipelines, POSIX local tooling, trusted project
test commands, declared source roots only. No automatic semantic reviewer, OS
sandbox, production authorization system, cross-machine evidence signatures,
automatic context understanding or historical rule migration. New adapters and
stronger checks must preserve this common baseline and document coverage.
