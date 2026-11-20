# Forge HTTP API v1

HTTP is an optional entrance to the same `workspace` / `runs` Python functions used by the CLI. The Web application is one client. HTTP v1 does not replace the independently versioned workspace, Flow, native operator, or run contracts.

## Connection and identity

Start the existing server with:

```sh
demiflow workbench console --registry /absolute/console.json serve --port 59865 --enable-runs
```

Omit `--enable-runs` to disable run mutations, or select `--control-only` to allow stopping/reconciliation without new submissions or result aliases. Inspection is still allowed and **saves a report**. Use `--api-only` to omit Web assets; the API is identical. The server binds to loopback, and an explicitly occupied port fails instead of selecting another port.

Source observation is distinct from compute admission. If the strict source
resolver rejects an import, default or decorator, the graph can still show
calls in the same declared, bounded, SHA-bound entry AST. These nodes and edges
remain unchecked syntax; imported/global implementations are not inferred, and
implementation requests fall back explicitly to the bound call site. Read,
parse, resource and ambiguous-entry failures remain unavailable. An empty graph
keeps its diagnostics and ends the source-loading state. Check and execution
permissions and admission rules are unchanged.

Requests require `X-Forge-Token`. The startup URL supplies a temporary private token in its fragment; the browser removes that fragment and retains the credential in session storage. Project/object links omit credentials. An optional `--token-file PATH` reuses a private credential across restarts. It must be a regular, nonsymlink file owned by the server user, mode 0600, at most 256 bytes, containing 43–128 URL-safe characters and optionally a newline. Generate it with a cryptographically secure random generator. File mode does not establish random entropy. In this mode startup prints only the address, not the token.

`Host` must be a literal loopback authority. If `Origin` is present it must exactly match `http://Host`. An SSH forwarded local port can differ from the listening port. Arbitrary forwarded headers, cross-origin browser fetches, and iframe embedding are not authorized.

Clients identify projects by `(connection, workspace_id)`, obtained from `GET /api/projects`. Use `/api/workspaces/{workspace_id}/{operation}` for project operations. Only explicitly registered roots can be selected. Single-workspace `workbench view` additionally accepts `/api/{operation}`; registry servers require the ID-qualified route.

## Discovery and budgets

Authenticated `GET /api/capabilities` returns:

- `contract: "demiflow_forge_http_v1"` and `web_enabled`;
- `operations.GET` / `operations.POST`, describing the transport surface;
- server permissions for `read`, `inspect`, `audit_check`, `runs`, `project_check`, and `project_manage` (registry servers only);
- optional `runtime_contract: "demiflow_forge_runtime_v1"` and permissions `run_submit`, `run_control`, `attach`;
- optional `audit_contract: "demiflow_forge_audit_status_v1"` when workspace auditing is supported;
- optional `workbench_contract: "demiflow_workbench_graph_v1"` and a separate `workbench_permissions` projection;
- body / response / request-path and concurrency budgets.

Project and server permissions must both permit each operation. Legacy `allow_runs` maps to submit/control/attach without a GET migration; explicit `console permissions` upgrades the registry using CAS. Context exposes `run_permissions` and the compatibility `runs_enabled` submit flag. Local OS users with filesystem access are not constrained by HTTP credentials; this is not multi-tenant authorization.

The HTTP v1 `permissions` key set remains unchanged. New `config_write`,
`analysis_execute`, `analysis_write`, and `case_annotate` permissions appear under
`workbench_permissions` at the service and `capabilities` in project/context
projections, calculated from the same grants and service ceilings. P1 fixes the
three analysis capabilities to false. Configuration writing requires explicit
console v3 migration, a project grant, and `--enable-config-write`; run grants do
not confer file writes. Single-workspace mode additionally needs the independent
`--workspace-config-write` project grant. Reads do not migrate a registry.

Limits: request path 8192 characters, at most 16 query fields, POST body 64 KiB, encoded response 9 MiB, list pages at most 100 entries, 10 admitted request threads, 1 ordinary operation and 1 independent short control operation, socket backlog 8. Ordinary slots wait up to 5 seconds. Cancel/snapshot/events use the short lane with at most 2 waiters and a 1-second wait, then return busy. I/O workers retain their existing deadlines and allocation limits. Encoded byte limits are not a process RSS guarantee.

Native operator availability comes from the platform catalog and inspection, not from a second list inside HTTP capabilities.

## Read operations

All fields are closed: unknown, missing required, repeated, or empty query fields are rejected. Unless indicated, query values are strings. `offset` and `row_id` are nonnegative integers; `limit` is 1–100; `attempt` is 1–32; `report` is exactly `0` or `1`.

| Operation | Required query | Optional query |
| --- | --- | --- |
| context, changes, audit, project-status | — | — |
| runs, reports | — | offset, limit, cursor |
| run | run_id | report, attempt, compact (0/1) |
| run-snapshot | run_id | attempt, revision |
| run-events | run_id | attempt, cursor, limit |
| run-metrics | run_id | attempt, points (1–600), resolution (raw/minute) |
| run-request | request_id | — |
| run-compare | left, right | — |
| run-logs | run_id | offset, attempt |
| run-source | run_id, path | — |
| graph | basis_kind (source/report/run) | entry, params, report_digest, run_id, attempt, node_id, node_offset |
| graph-source | graph_binding (JSON object), node_id, kind | resource |
| parameter-state | path | — |
| run-observations | run_id, attempt | node_id, action_id, cursor, limit |
| run-evidence | run_id, reference (JSON object) | — |
| parameters | path | — |
| source | path | expected_sha256 |
| table | alias | expected_binding |
| sample | alias | columns (JSON list), offset, limit, row_id, match (JSON object), expected_binding |
| media | alias, row_id, exactly one of field / field_path (JSON list) | expected_binding |
| table-evidence | alias, row_id, field_path (JSON list) | expected_binding |
| report | digest | — |
| diff | left, right | — |

Source paths are project-relative author Python files; parameters are project-relative author JSON files. Hidden components, traversal and symlinks are rejected. Directory names do not assign Flow/operator roles. Context includes a bounded `source_files` list and `source_diagnostics` for incomplete listing. Images and evidence must satisfy the existing declared-resource and fixed-row contracts. These are not arbitrary filesystem endpoints.

Table description and sample return a binding digest. Carry `expected_binding` into subsequent Case, media and evidence reads. Media returns bytes, a MIME type and `X-Forge-Binding`; other operations return JSON. A mismatched binding is a conflict, not permission to switch to the new alias target.

`changes` is a bounded observation hint. It is not a transactional workspace revision or a content-integrity proof. Listing cursors can become invalid after an index rebuild; incomplete indexes are reported explicitly. Reading does not automatically repair an index or run an inspection.

`graph` accepts exactly one basis: source uses entry and optional params, report
uses its digest, and run uses an exact run/attempt. Its seven-field binding is
`workspace_id,basis_kind,basis_digest,entry,params,run_id,attempt`. It separates
semantic completeness from display clipping. The default is 128 nodes/256 edges;
an explicit node neighborhood is capped at 256/512; the complete projection is
at most 1 MiB. The bounded node index pages 64 records with `node_offset` (0–65536).
Source graphs describe static references, not a checked execution plan. AST
parsing uses the existing read worker's parent/deadline/allocation protections.

`graph-source` accepts implementation/callsite/configuration/prompt projections,
verifies node and source identities, and returns at most 64 KiB/300 source lines.
Report-bound source is evidence of that revision, not a new freshness check.
Current source bytes must match their SHA; run source comes only from its sealed
closure. Missing sealed source never falls back to a current file. Prompt previews
contain approved public template/model fields, not runtime credentials or actual
model requests. Configuration projections are read-only and exclude opaque resource
contents.

`run?compact=1` preserves the run identity/attempt history while omitting full
action and observation documents; the default v1 response is unchanged. Selected
node evidence uses `run-observations`: at most 64 records and 96 KiB including the
cursor/envelope, bound to project, run, attempt, seal, filters and observation
revision. Added/updated/removed observations, an earlier restored snapshot or a
different binding invalidate the cursor with HTTP 409, `reset_required:true` and a
reason. Discard the old page and reread from the beginning; do not append mixed
versions. Historical unsupported journals remain explicitly unsupported. This is
correlated evidence, not a complete node log; raw logs remain run/attempt scoped.

## Mutations

All are POST with one Content-Length and a JSON object. Duplicate JSON properties, nonfinite numbers, unsupported transfer encodings and unknown fields are rejected. String fields must be nonempty and at most 4096 UTF-8 bytes. Domain contracts impose additional restrictions.

| Operation | Required body | Optional body |
| --- | --- | --- |
| inspect | entry, params | — |
| project-check | — (empty object) | — |
| audit-check | kind, expected_policy | digest (required only for kind=report) |
| run-prepare | entry, params | inputs, outputs, limits (objects); new_outputs, telemetry (booleans); request_id |
| run-admission | entry, params | limits |
| run-request-reconcile | request_id | — |
| run-start | run_id, expected, expected_attempt | — |
| run-resume | run_id, expected, expected_attempt | — |
| run-cancel | run_id, expected_attempt | compact (boolean; return small snapshot) |
| run-reconcile | run_id, expected_attempt | — |
| run-attach | run_id, attempt, output, alias, expected_result | — |
| parameter-preview | path, expected_sha256, graph_binding, replace (list) | — |
| parameters-update | path, expected_sha256, proposed_sha256, graph_binding, replace (list) | — |

`expected` is the prepared manifest digest. `expected_attempt` is the attempt the client observed. Explicit null is accepted only for genuinely historical journals without attempt support; booleans are never integers. Resume requires a modern stopped attempt and remaining original budgets.

`expected_result` is the output's `result_digest`, derived from the complete fixed DatasetRef. Result attachment checks the specified attempt and native commit evidence and creates a new alias. An existing alias is not replaced. The final catalog-resolved ref must exactly match the verified ref. Historical results can be named explicitly without substituting the current attempt.

Inspection and preparation save facts. Preserve the exact request ID and payload before prepare. With `request_id`, a retry returns the same durable run identity; conflicting payloads are rejected. Pending requests require explicit `run-request-reconcile`; expired IDs cannot create a new run. Without an ID, do not blindly replay prepare. Start/resume/cancel still require the observed attempt; read facts after lost responses. See [runtime management](forge-runtime-management.md) for replay windows, permissions, quotas, freshness and version boundaries. `run-admission` returns prerequisites and `requires_prepare`, never an inspection pass.

Parameter preview/update require effective `config_write`. They accept at most
32 scalar replacements in an explicitly registered JSON file of at most 64 KiB.
Only proven public paths are writable; computed values and code literals stay
read-only. Preview returns exact candidate bytes' SHA under
`parameters-json-v1`; update rechecks the binding, raw-file SHA, membership/grant
and candidate SHA under the existing control lock before same-directory atomic
replacement and fsync. Success invalidates the current check. This is CAS for
cooperating writers; it does not promise an atomic filesystem CAS against an
editor that ignores the lock.

There is no parameter request ID or receipt ledger. After response loss or an
I/O failure, `parameter-state` reports current content only: a matching SHA does
not prove which request wrote it or resolve an earlier durability error. Retain
the draft, adopt the observed content explicitly or retry the original conditions.

### Project creation, in-place registration, removal and uniform checks

Registry servers expose these authenticated, console-level operations (not workspace-qualified):

| Endpoint | Required body | Optional body |
| --- | --- | --- |
| POST /api/project-create | root, name | — |
| POST /api/project-register | root | name |
| POST /api/project-remove | workspace_id, expected | — |
| POST /api/project-permissions | workspace_id, expected, run_submit, run_control, attach | — |
| POST /api/project-capabilities | workspace_id, expected, capabilities (object with four strict booleans) | — |

`root` is a normalized absolute directory on the server. Create requires an absent/empty directory; register preserves existing business files and compatible controls. Both use the same public functions as `workbench console create/add`. They initialize only platform management metadata when absent, never business layouts, data locations, execution permissions or business code. Unknown controls/conflicting identities are rejected. Publication failure can leave an initialized project; inspect it and retry registration, not destructive rollback. Same-directory registration is idempotent with matching identity and permissions.

`project-permissions` is an explicit console configuration operation using the same
`console.set_permissions` CAS as the CLI. `expected` is the registry digest from
`GET /api/projects`; all three permissions must be JSON booleans. A read-only
deployment rejects this operation. A control-only deployment cannot grant submit
or attach. The Web permission form is writable only on a full execution deployment.
This changes no workspace files and starts no job. Ordinary project registration
and listing never enable execution. The bearer credential already controls this
local console's project inventory; it is not a multi-tenant administration role.

On console v3, legacy permission updates merge the original three grants without
dropping the new capabilities or downgrading the registry. `project-capabilities`
uses the same registry lock/CAS and cannot raise a deployment ceiling. Its P1
analysis grants must all be false. Explicit migration preserves the old registry
in a backup and grants no new capability; rollback must retain both the backup
and v3 registry and must not overwrite parameters already saved by the user.

`POST /api/workspaces/{id}/flow-register` accepts only `entry` and `params` strings,
requires effective project `run_submit`, and calls `workspace.register_flow`.
Entry and JSON paths must be author files within the project. Registration parses
at most 1 MiB of Python without importing/executing it, reads at most 64 KiB of
parameters, and retains the existing 256-entry / 64-configurations-per-entry bounds.
Registering the same pair is idempotent; concurrent workspace updates use the
existing control lock and CAS. Registration does not assert native compatibility
or certification. Preparation still checks effective inputs, outputs, budgets,
source and platform seals; starting still uses the existing admission and journal.

The pipeline workbench submits prepare/start through one Run action. Lost
prepare responses retain the request ID across
reloads. The receipt can be read, a pending request explicitly reconciled, a ready
run opened, or a confirmed failed request ended before a new submission. A lost
start response opens the known run identity for inspection rather than creating
another run. Authored source, configuration and input/location declarations remain
project responsibilities; this is not arbitrary-script onboarding.

Runtime registration and permission dialogs bind their target workspace and view
generation when opened. Navigation, including browser history, invalidates and
closes them. Submissions use that captured identity; late responses cannot change
another view's context, draft or navigation. Closing a dialog does not roll back
an already delivered mutation: its facts remain on the original target project.

Remove uses `console.remove`, also used by `workbench console remove ID --expected DIGEST`. `expected` is the exact digest returned by `GET /api/projects`; concurrent registry changes return 409 and require renewed selection/confirmation. Only membership in this registry is removed: project code, management files, historical results and existing work are preserved. Broken/unavailable projects can also be removed without opening their directories. Subsequent workspace-qualified reads and mutations return 404; already admitted work is not cancelled. A watcher using this registry drops the project on its next inventory refresh, after any in-flight check. Other registries and explicit local CLI access are independent. Re-registering a valid original directory restores access with its existing identity/history. No file-deletion option is accepted.

`POST /api/workspaces/{id}/project-check` accepts only `{}` and saves bounded static inspection facts. It has no rule overrides and is independent of run permission. `GET .../project-status` reads the record and verifies freshness. The single `status` field describes static inspection of the registered Flow and parameter variants. Diagnostics identify the affected variant and source location. Run admission uses its own controls. No endpoint automatically repairs business code. `changes.project_check` refreshes clients when the saved record changes.

## Original project recognition

These authenticated GET operations are workspace-qualified and share the CLI/Python implementation. `context.project` contains the same discovery response; `changes.discovery` is its digest for client refresh. Discovery is bounded, read-only evidence interpretation, never execution authorization. No business name, directory prefix or client identity selects an adapter. See [supported declarations and coverage](forge-project-discovery.md).

| Operation | Required query | Optional query |
| --- | --- | --- |
| project-discovery | — | — |
| project-document | path, expected (document SHA256) | — |
| project-table | id (discovered data identity), expected (discovery digest) | version |
| project-sample | id, expected, version, binding (from project-table) | columns (JSON array), offset, limit, row_id |

Discovery format `demiflow_forge_project_discovery_v1` returns `entries`, `operators`, `prompts`, `configurations`, `tables`, `runs`, `diagnostics`, `coverage`, content `evidence` and `digest`. Entries come from the selected workspace registration. Configurations are references unless an existing native parameter binding explicitly selects them. Table references may be `available`, `missing` or `outside_project`; an outside reference grants no access. A missing version means a documented location, not a mutable execution input.

`project-table` pins metadata at the declared version, or observes one current version when none was declared. It returns `version`, `row_count`, `schema_hash` and `binding`. `project-sample` requires those exact observed conditions and the same discovery digest; a later table head cannot move the page. These previews do not persist table registrations or create executable inputs. Changed declarations/metadata bindings return 409. Arbitrary paths, private controls and external resources cannot be read through these endpoints.

The current project-check record uses `demiflow_workbench_project_static_check_v2` and includes the inspected variant count, per-variant report digests, actual source digests, and checker platform provenance. Status checks the registered configuration and bound evidence. A prior report format requires an explicit new check before the UI has a current result. No record is migrated during a status read.

## Workspace observations

`GET audit` returns bounded derived observations; it never scans author files or grants admission. The response includes `workspace_id`, `policy_digest`, `revision`, server `observed_at` (UTC epoch seconds), `coverage` (`tracked`, `registered`), `watcher`, and `observations`.

Each observation identifies `kind` (`scope` or `report`), `identity`, a `result`, `freshness`, `verified_at`, and `due_at`. Scope results are `within_scope`, `violated`, `unconfigured`, `unknown`, or `unverifiable`. Report results preserve checked/stale/invalid or indicate unverifiable. Freshness is independently current/pending/overdue/unknown/unconfigured: a recent failed verification is not a pass. File changes include path, change type and allowed flag; counts and truncation remain explicit.

`POST audit-check` verifies one current target and writes only derived observation state. Use `kind: "scope"` without digest, or `kind: "report"` with a pinned report digest. `expected_policy` is the latest `GET audit.policy_digest`. Audit-check is available independently of run permission and has its own advertised capability. Unknown fields, temporary writes/scope overrides and untracked reports are rejected. Control changes produce conflict; an unavailable object returns an explicit incomplete result. It does not create an inspection pass or advance a baseline.

`changes.audit` is a cheap cache revision hint. A client must also expire observations after `due_at` even if this hint stops changing. Use `observed_at` plus a monotonic elapsed timer to avoid assuming the client and server clocks match; heartbeat freshness is independent of completed verification. The Web displays unavailable watcher state after 180 seconds without a recent heartbeat. A running watcher is not evidence that every object was just checked.

Scope/baseline changes are local CLI operations, not HTTP privileges. Prepare/start/resume read the active workspace constraint and perform fresh checks, even when every client is closed, the observer is stopped, or the cache is missing/corrupt. This does not govern arbitrary Python execution outside Forge.

## Errors and client migration

Success payloads remain the public-function result, without an additional RPC envelope. Failures have `error` (human-readable) and `code` (machine-readable):

Capacity rejection before dispatch additionally has `operation_started: false`.
This field is emitted only by the HTTP request/worker admission boundaries;
`code: busy` from inside an invoked operation does not imply it. The check UI
uses the explicit flag to say that the check has not started, preserves the
previous result and its basis, and requires a deliberate retry. Other write
failures, including a busy response without this flag, retain their existing
result-verification behavior. There is no automatic POST retry.

Portable v2 views also support a bounded `table_request` read intent with
`workspace`, `alias`, and optional `expected_binding`, `columns`, and `offset`.
It is not a fixed table or Case identity. Existing v1/v2 `table` and `case`
references still require their real `binding_digest`; Case still requires its
fixed row identity. Only a successful authorized table-description read promotes
the request to a fixed table reference. An existing expected binding is always
passed to that read; mismatch fails explicitly without adopting a newer ref.
The same page epoch, 4 KiB reference and 16 KiB encoded-link limits apply.
The request is written to the URL before loading, so failure, explicit refresh,
retry, reload and browser history keep its target. Ordinary background sync does
not retry a failed table/Case read or substitute a default table.

| Code | HTTP | Meaning |
| --- | --- | --- |
| invalid_request | 400 | Malformed or unsupported request |
| unauthorized / forbidden | 403 | Invalid session or transport access denied |
| not_found | 404 | Missing resource / endpoint |
| conflict | 409 | Observed digest, attempt, or fixed reference no longer matches |
| resource_limit | 413 | A known size / allocation / lifetime budget was exceeded |
| operation_rejected | 422 | Domain admission refused the operation |
| busy | 503 | Bounded request / worker / control capacity is occupied |
| operation_failed | 500 | Failure not safely classified as client input |

Read clients may retry transient failures with bounded polling. Conflicts require re-reading and a deliberate new decision. Session failures require a valid authorization entry. An empty or non-JSON proxy error must be handled as a transport error rather than a JSON parser failure. No error means a POST is universally safe to replay.

Existing unversioned paths are retained for the first formal contract. Clients must add observed attempt fields and exact result identity to mutations; missing conditions are rejected rather than silently filled with latest. Existing project deep links and stored run/table formats are unchanged. Future incompatible transport versions require an explicit new contract.
