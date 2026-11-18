# DemiForge Pipeline Standard

Version: `1.0.0-candidate.10` (2026-11-18). Status: candidate, not a universal
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

## DF-02 Project, Flows and Reusable Definitions

A business project is the ownership boundary. It can contain several named flows,
shared code operators and shared Prompt definitions. A flow is a wiring recipe,
not a container that owns a private copy of every operator it invokes. Do not force
unrelated flows into one `run(stage=...)` dispatcher or parameter variants merely
to satisfy a single-entry rule. Extract common code only when it represents a
stable, useful capability; similar-looking wiring is not sufficient justification.

New projects normally use this layout (only create directories that have contents):

```text
project/
  flows/select.py       # select flow: one explicit callable entry
  flows/evaluate.py     # evaluate flow: another explicit callable entry
  operators/quality.py  # shared plain functions / non-model Actors
  prompts/review.yaml   # named, versioned prompt pack definitions
  tests/
  README.md
```

Use `demiforge-project-v3`: `projects.<project-id>` declares `path`, `flows`,
`operators`, `prompts`, `notebooks` and trusted `verify` commands. `flows` maps each
flow ID to its actual repository-relative `{path, symbol}`. Each flow has its own
entry module and one public function; a project has NO default execution entry.
An explicit flow is selected for execution. The development task selects a project
and exact writable paths; checks cover every declared flow and shared definition.
There may be 1..64 flows per project and 1..128 projects per registration. Empty
operator/Prompt/notebook lists are valid. A library-only project is not covered by
this schema; a global operator registry is not required for ordinary code reuse.

All owned roles are watched, normalized, free of symlinks, inside their project
and separate from readonly dependencies. Definitions have one owner, not one owner
per consumer. No overlapping operator/Prompt/entry roles or duplicate ownership.
Registered nonstandard filenames remain valid; do not move/copy existing source
just to satisfy a display label. The library discovers declared definitions, not
executed node instances or every arbitrary helper found in the repository.

Flows may import common operators from their declared owner. Operators cannot
import flow entries, notebooks or tests, including through helpers/re-exports.
Flows do not import/call another flow's entry, even in the same project; shared
pure transformations belong in operators. Cross-flow execution uses explicit,
persistent input/output handoffs. Native model/I/O/commit nodes stay visible in
the Dataset wiring; do not hide a sub-pipeline or model client in a reusable UDF.
Notebooks/CLI are callers of explicitly selected flows, not a second wiring truth.

### One business-parameter contract per flow

Each flow has ONE parameter definition and ONE current set of editable values.
Expose only business inputs needed for that wiring: Dataset references, selection
scope, business thresholds, output targets and other explicitly justified inputs.
Use named entry parameters with documented defaults/types/structured schemas.
Parameters already fixed by the flow do not require an extra configuration choice.
No mandatory parameter file, configuration-profile selector, or generic `config`
object carrying every operator/engine option. Multiple flows are not parameter
presets. A parameter-free flow needs no empty form or placeholder configuration.

Keep four distinct authorities:

| Authority | Owns |
| --- | --- |
| Flow | Wiring and its necessary business parameters/current values |
| Code operator | Callable contract, implementation and its own defaults |
| Prompt definition | Template/messages, version, declared model settings and output contract |
| Engine / deployment | Scheduling, resource limits, credentials and transport infrastructure |

A flow may bind an operator argument explicitly; expose it to the user only when
it is genuinely a business input of that flow. The effective value must be visible
at the call node, with its origin. Runtime/model profile binding already permitted
by the native API remains explicit; it does not create another editable Prompt.
Parameter files are optional import/export or storage carriers for the SAME values,
not separate mandatory domain objects. Each run seals its effective parameter and
source/model/Prompt identity snapshot. Editing current values never rewrites a
historical run or silently changes resume inputs.

### Code and Prompt libraries share source ownership

A code-operator entry points to a real function/Actor definition. A Prompt-operator
entry points to a named definition within a versioned Prompt pack, which may hold
several definitions. Prompt text need not move into Python `op` modules. Keep fixed
text in its declared Prompt source, including simple image-generation templates;
per-row material is bound by the flow at the visible native prompt/agent node.
The library and graph read the SAME definitions, not copied code, a parallel
execution registry, or a second model implementation. A library definition and a
call node have different identities: one definition may serve many calls/flows.

The library shows input variables/declared contracts, defaults, actual Prompt/code,
output contract and proven references. Unknown/inferred schemas or dynamic usages
must be labeled; no invented fields, fabricated reference counts or implicit
certification from a directory name. Editing a shared definition affects every
referencing flow; inspect affected consumers and bind verification to changed
bytes. Global sharing uses declared, versioned code/Prompt dependencies with an
owner; do not copy assets across projects or create a global registry preemptively.

README describes each flow's purpose, entry, inputs/outputs, shared definitions,
failure/completion/recovery boundary and tests. Preserve saved Notebook outputs.

### Migration and enforcement boundary

Candidate.1–9 documents, task bindings and saved evidence retain their original
rules. Candidate.10 requires explicit project-v3 adoption and a new task, with
prior source/snapshot/history preserved. A standards upgrade does not authorize
production runs, mass registration, source relocation or historical replay changes.
Update conflicting project rules in the same adoption change; do not claim old
single-entry declarations or certifications have automatically become multi-flow.

The tool mechanically checks project/role ownership, each registered entry, bounded
source coverage, local static import direction and snapshot identity. Prompt schema
semantics, useful operator reuse, parameter relevance, accurate UI display and model
behavior require native validation, targeted fixtures and review. This standard
does not claim a new operator-library UI or flow-parameter editor already exists.

### README maintenance for every change (2026-10-12)

Every pipeline change MUST include a timely update to its owning project's README in
the same task/change, before review and handoff. This includes code, prompts,
configuration/defaults, schemas, dependencies/paths, notebooks, tests and operating
or recovery instructions. Small fixes and refactors are not exempt. For shared
changes, update the affected flows' sections where their documented contracts,
usage or verification are affected; unrelated flows do not require edits. A single
project README may cover all its flows; separate per-flow READMEs are not required.

Update the relevant current-state sections so that responsibilities, inputs and
outputs, processing rules, configuration, failure/recovery and verification remain
consistent with the final implementation. If business behavior is unchanged,
explain the actual maintenance change and its validation in the relevant section.
A timestamp-only edit, appended changelog, chat message or task note alone does
not satisfy this requirement. Remove or revise superseded instructions; distinguish
implemented behavior, verified results and unimplemented plans without claiming
that historical production results have been rerun.

Example: changing a retry default updates the configuration and recovery sections;
adding only a regression test updates the verification section with the covered
case and actual result, even when pipeline behavior is unchanged.

Acceptance: DF-02 semantic review MUST compare the final diff and README, cite
the updated README and relevant implementation/test evidence, and reject missing,
stale or cosmetic-only updates. The current checker verifies README presence and
the specification reference; it does not automatically prove this synchronization.

### Optional inspection surfaces

A registered notebook may explicitly call its project's named flows and inspect fixed results.
It does not define execution functions/classes or implement a second flow. This
version imposes no three-cell count or role tags. Inspection must identify the
selected fixed table version/run, distinguish execution status from business
completion, and state missing/partial evidence. A console or other client can
provide the same inspection without any notebook dependency.

Full notebook bytes, including outputs, remain part of the watched snapshot.
Code/tags can be projected with bounded streaming parsing for context/checks;
this does not exclude outputs from integrity protection. Explicit per-project
snapshot budgets are bound to task evidence and have tool-enforced ceilings.
Budget exhaustion fails visibly; it cannot silently omit source roots or files.

Acceptance: registered entry/layout checks, optional notebook AST checks,
full-file integrity/limit tests, platform contract checks and supported-view tests.
Core checks resolve static Python imports, including aliases, relative imports,
package initializers and local re-export/helper chains. Operators cannot depend
on any registered entry module; an entry cannot depend on another pipeline entry
module (including legacy entries). Local shared dependencies require a declared
operator owner and watched coverage. These checks do not import candidate code,
prove function-level purity, resolve dynamic imports/reflection or certify an
arbitrary Python execution path. The registered platform verification command
must still check the actual execution source closure and effects.

Static import inspection has bounded source entries, bytes and elapsed time;
limits can only be tightened by the declared snapshot budget. Oversized, missing,
unsafe or unwatched sources cannot produce a passing result. This is an
inspection guard, not a process memory sandbox or permission to execute code.

## DF-03 Explicit Flow and Model Boundary

The main flow visibly expresses fixed input -> selection -> transforms -> model
nodes when needed -> validation -> writer. Use the platform's public data APIs;
do not collect a large dataset to a driver list and rebuild hidden subflows.
All business model calls use the platform's public, unified prompt or agent mapping
nodes. Fixed input/output inference, including text analysis, image generation and
scoring, uses the ordinary prompt mapping node. Use its asynchronous form for
concurrent evaluation stages. A local agent is justified only by dynamic decisions,
with bounded tools and stopping conditions, not as a substitute for a known DAG.
Cross-pipeline work uses persistent, identifiable handoff records, not nested
pipeline execution or hidden in-memory coordination.

A project binds these roles to its platform's public APIs. The common standard
does not select a model, transport, scheduler or storage backend. Unsupported
capabilities are explicit contract gaps; a business callback is not an adapter.

Business operators prepare row fields, bind versioned templates/model configuration
and validate outputs. They must not call model HTTP endpoints or SDKs, instantiate
model runtimes, define model Actors, redefine the mapping nodes, or wrap a direct
model call in ordinary `map`/`map_async` callbacks. Do not add a business client,
executor, retry loop or per-model invocation helper that replaces a platform node.
Pure row transformations and ordinary non-model I/O are not model invocations.
Model-native ViT/VAE encoding, decoding and input layout stay inside the existing
platform/model adapter, not in business operators.

An unsupported task or historical dedicated model entrypoint (for example query
embedding) is an explicit platform migration gap, not permission to introduce a
fourth business model gateway or to disguise it with a renamed wrapper. Record its
actual API, affected stage, required platform capability and verification boundary.
Do not modify the read-only platform or silently claim whole-pipeline compliance;
an existing gap does not exempt newly changed model calls from this rule.

Acceptance: inspect every changed model call and its reachable row callbacks;
verify rendered inputs and the public-node boundary with isolated fixtures. Record
legacy gaps separately. Core structural checks do not prove this semantic boundary
and do not automatically detect every SDK/HTTP bypass. This revision adds the
contract and review requirement, not runtime isolation or a complete call scanner.

### Efficient and minimal orchestration

Every scan, join, aggregation, sort and materialization must serve a stated business
output, necessary validation or actual reuse. Prefer one forward pass: project
needed columns and apply scope early, then transform, infer and write in bounded
batches. Do not add a full-data action merely to print progress, check non-emptiness
or collect a count before doing the real work. Use fixed-source metadata, execution
metrics or writer receipts when those already answer the question.

Validate static configuration without scanning business data. Check row-local
conditions during the existing pass. A consumer may rely on a fixed producer's
published key/schema contract; it must not repeat full-table uniqueness checks at
every start without a concrete unmet requirement. Required validation remains
explicit, but should share an existing pass or belong to source publication.
Do not silently truncate inputs or derive an expected denominator from filtered
outputs to make a run appear complete.

Read only information used by the current decision or delivery. Do not join extra
tables for speculative completeness. Reusing a lazy Dataset variable does not
necessarily reuse computed rows: inspect repeated actions and shared branches.
Materialize only at a necessary reuse, publication or recovery boundary, with a
bounded lifetime. Avoid tiny batches, per-row files and synchronous per-row state;
batch sizes must respect both row and byte budgets and downstream backpressure.

Acceptance: DF-03 review identifies full scans, joins, materializations and repeated
upstream execution, and explains their necessity. For a performance change, use a
representative bounded fixture to measure time to first useful output, throughput
and temporary I/O; distinguish measurements from full-scale estimates. Reject
redundant full-data actions, not operator names: a count that is itself a required
business result is valid. Platform execution defects belong to a separate platform
task, not business-side replacement engines. This requirement is semantic review
and targeted verification; the checker does not prove performance automatically.

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

Prompt-writing principles:

- **State the primary goal first.** Explain what the model must decide or produce
  before giving supporting instructions. Organize those instructions around the
  stated goal.
- **Give each section one purpose.** Arrange sections in a clear task order, with
  headings that describe their content. Keep each rule and its explanation in one
  place rather than repeating them across sections.
- **Be concise without losing meaning.** Remove repetition and irrelevant content,
  while preserving necessary conditions, distinctions and explanations. Use clear,
  complete wording; do not compress sentences into ambiguous shorthand.

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
| DF-02 | S01.8, S02, S10 | layout, Python AST, notebook definitions, direct imports, README presence/reference | explicit entry per flow; shared definitions and business-only parameters; substantive README synchronization |
| DF-03 | S03, S04 | direct cross-pipeline imports only | flow/platform/model boundaries; scan, reuse and startup cost review |
| DF-04 | S04, S05, S11 | no generic semantic proof | identity, schema, scope, failure fixtures |
| DF-05 | S09 | no generic semantic proof | rendered prompts or justified N/A |
| DF-06 | S06, S07, S08 | verification subprocess timeout/output bound | reuse, failure, resource behavior |
| DF-07 | S12 | snapshot-bound local records and stale-evidence rejection | test relevance, limits, next steps |

Current limitations: Python pipelines, POSIX local tooling, trusted project
test commands, declared source roots only. No automatic semantic reviewer, OS
sandbox, production authorization system, cross-machine evidence signatures,
automatic context understanding or historical rule migration. New adapters and
stronger checks must preserve this common baseline and document coverage.
