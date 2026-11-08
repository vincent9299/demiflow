---
name: demiflow-workspace
description: Inspect and operate an initialized Demi workspace through its public CLI, including registered Flows, fixed data versions, runs and result references. Use for workspace operations; does not replace project-specific pipeline development or certification standards.
---

# Demi workspace operations

Identify the actual repository and workspace before acting. Read its AGENTS and pipeline specification; use its selected interpreter or project wrapper. A DemiForge-registered business project retains its own development checks and authorization scope. This skill supplies platform usage, not certification or permission to execute production workloads.

## Read before acting

- Read `workbench context ROOT --json` to discover registered entries, parameter paths, data locations and limits. Do not assume a folder is a Flow or invent an entry.
- Use `workspace source` / `parameters`, `table show` / `sample`, and `workspace run show` / `logs` to read bounded facts. For repeated Case / image / evidence reads, carry the returned binding digest.
- Use `workspace changes` only as a refresh hint. It is not a source-integrity proof or a transactional revision.

These are subcommands of the project's `demiflow forge` entry. They work without a Web server, browser, conversation bridge or host SDK.

## Check, run and reuse

- `flow inspect ROOT ENTRY --params PATH` performs a fresh contract inspection and saves a report. Source changes can invalidate a previous report.
- Within the user's authorized scope, `workspace run prepare` seals the inputs and source. Use its actual run_id, digest and attempt in `run start --expected DIGEST --expected-attempt N`.
- Read recorded status, native commit evidence and output references. A completed process, a committed table and business-quality approval are separate facts.
- Cancel / reconcile with the observed `--expected-attempt`; resume also requires the manifest digest and original remaining budgets. On conflict, read current facts before deciding again.
- To retain an exact result, use `run attach ROOT RUN_ID OUTPUT ALIAS --attempt N --expected-result DIGEST`. The digest comes from the selected output. This creates an alias for that fixed reference; it does not overwrite an existing alias.

On a timeout or missing response, query first. Do not automatically replay prepare, inspect, resume or attachment; a missing response does not prove nothing happened. Never edit journals, manifests or verification records to make an operation pass.

## Multiple clients

CLI, Web and other HTTP clients observe the same physical workspace. Preserve fixed versions, historical attempts and other clients' unsubmitted edits. Different clones / worktrees are not automatically synchronized. Conversation claims are not execution evidence.

Do not start or reconfigure an HTTP service just to use the CLI. Preserve an existing deployment's port, credential and workspace registration unless changing them is in scope. The public HTTP contract and client guide are maintained in the demiflow repository at `docs/forge-http-api.md` and `docs/forge-client-integration.md`; consult them when building an external client.
