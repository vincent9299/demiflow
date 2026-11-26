# Forge and the Workbench

- **Forge** is the command-line tool: inside a project directory it creates standard files, answers questions about the current SDK, runs static checks and registers runnable entries. People and coding agents use the same commands.
- **The Workbench** is the browser interface: it opens the same project directory, shows the graph of a Flow, tables and run records, and runs Flows when authorized.

Both read the same project declaration, `workspace.json`; there is no second copy of the state.

## Quick start: from an empty directory to a registered Flow

```bash
demiflow forge init --root ./experiment
demiflow forge add flow counts --root ./experiment
demiflow forge add operator is_even --root ./experiment
```

`init` writes the project declaration and a snapshot of the SDK; `add` creates standard files with placeholder content. Then fill in the business logic:

```python
# operators/is_even.py
def is_even(row):
    return row["value"] % 2 == 0


__demiflow_operators__ = ["is_even"]
```

```python
# flows/counts.py
import pyarrow as pa
from demiflow import data

from operators.is_even import is_even


def run(*, output: str, limit: int = 10):
    """Keep even values and write them to a Lance table."""
    rows = data.from_items([{"value": value} for value in range(limit)])
    rows.filter(is_even).write_lance(output, mode="create",
                                     schema=pa.schema([("value", pa.int64())]))
```

The parameters of a Flow live in a configuration file:

```json
// configs/counts.json
{"output": "/abs/path/experiment/data/evens.lance", "limit": 10}
```

Check, and register once it passes:

```bash
demiflow forge check --root ./experiment --entry flows/counts.py:run --params configs/counts.json
demiflow forge add flow --root ./experiment --register --entry flows/counts.py:run --params configs/counts.json
```

After a successful registration the entry appears in the pipeline list of the Workbench.

## Project directory

| Path | Content |
|---|---|
| `workspace.json` | Project declaration: identity, registered entries and configurations, data locations |
| `flows/` | Flows: entry functions that wire operators together with the Dataset API |
| `operators/` | Operators: plain functions or classes |
| `prompts/` | Prompt definitions |
| `configs/` | Flow parameters and business configuration |
| `data_tools/`, `notebooks/` | Data tools and notebooks |
| `data/` | Tables and run outputs |
| `logs/` | Run logs |

Directories are created when needed. Initialize a directory that already has code with `demiflow forge init --root DIR --existing`; existing files are kept.

## Forge commands

| Command | What it does |
|---|---|
| `forge init --root DIR` | Creates the project declaration and the SDK snapshot |
| `forge context --root DIR` | Shows coding rules, SDK signatures and facts about this project |
| `forge context --root DIR --api demiflow.data:Dataset.filter` | Looks up the signature and origin of one interface |
| `forge context --root DIR --topic prompt` | Shows the rules of one topic |
| `forge add flow NAME --root DIR` | Creates a Flow file |
| `forge add operator NAME --root DIR` | Creates an operator; `--actor` creates a stateful one |
| `forge add prompt NAME --root DIR --from FILE` | Creates a prompt definition; `-` as `FILE` reads standard input |
| `forge check --root DIR [--entry F:FN] [--params configs/X.json]` | Static check; does not run business code |
| `forge add flow --root DIR --register --entry F:FN [--params ...]` | Registers an entry that passed the check |
| `forge review --root DIR --entry F:FN --goal "..."` | Starts an independent, read-only review |

`context`, `check` and `review` support `--json` and then print one JSON document.

Exit codes:

| Code | `check`, `add` and others | `review` |
|---|---|---|
| 0 | Done, or the check is explicitly not applicable | Review completed without problems that affect delivery |
| 1 | A known violation or content conflict | Problems that affect delivery were found |
| 2 | Invocation error | Invalid invocation |
| 3 | Insufficient evidence, uncovered scope or exhausted budget | The review did not complete |

### What a check tells you

`forge check` reads sources and declarations and verifies them statically: the entry exists, the parameters are complete, the SDK interfaces used are supported, the directory layout follows the convention. It neither imports nor runs your business code.

A passed check therefore says the wiring is compliant, not that the business result is right. Business behaviour is verified by the project's own tests; record the two kinds of results separately.

- To check only what a change affects: `--path RELATIVE_PATH`, repeatable.
- A Flow with parameters needs `--params`; check and registration must use the same configuration.

### Independent review

`forge review` has another model session review the current change read-only: whether business choices sit in the right place and whether the delivery evidence is sufficient.

```bash
demiflow forge review --root ./experiment --entry flows/counts.py:run --goal "Count the records that pass the filter"
```

- Before running it, point the environment variable `DEMIFLOW_FORGE_REVIEW_CONFIG` at the configuration file (JSON) of the review model.
- By default it reviews the project's actual changes relative to HEAD; `--base REF` names a baseline to compare with.
- `--evidence RELATIVE_PATH` attaches test or small-sample results, repeatable.
- The default time limit is 180 seconds; adjust it with `--timeout`.

## Opening a project in the Workbench

The Workbench manages several projects through one registry file:

```bash
# Create a project, or register an existing one (absolute paths on the server)
demiflow workbench console --registry /abs/console.json create /abs/new-project --name my-project
demiflow workbench console --registry /abs/console.json add /abs/experiment
demiflow workbench console --registry /abs/console.json list

# Start the interface
demiflow workbench console --registry /abs/console.json serve --port 59865
```

- The service listens on the loopback address only. When you reach the server through SSH, forward the port: `ssh -N -L 59865:127.0.0.1:59865 user@server`, then open the address printed by the command in your local browser.
- Registration records only the directory and the permission ceiling; it neither copies nor changes your code and data. `remove` only cancels the registration; the directory and its history stay.
- **Running is off by default.** To run from the interface, register with `--allow-runs` and start the service with `--enable-runs`; both are required. Every run is still checked on its own.
- One registry file holds at most 128 projects.

In the interface every project has views for an overview, tables, Flows and checks, and run records. The Flow view shows the wiring graph, the fields and parameters of every node, diagnostic locations and the source.

## Tables

Attach a pinned version of a Lance table to a project and then browse and sample it by name:

```bash
demiflow workbench location ./experiment corpus /abs/read-only-root
demiflow workbench table attach ./experiment candidates --location-id corpus \
    --uri candidates.lance --version 3 \
    --dataset-id questions --schema-name questions --schema-version 1
demiflow workbench table list ./experiment --json
demiflow workbench table sample ./experiment candidates --column case_id --column question --limit 20 --json
```

- A table name is only an alias; the real identity is the location, the table address and the pinned version. Later reads do not follow new versions automatically.
- External locations are read-only by default; add `--writable` when declaring a location that has to be written.

## Running from the command line

A registered entry can also run without the interface, in two steps: prepare (which pins source, configuration and data versions), then start.

```bash
demiflow workbench run prepare ./experiment flows/counts.py:run --params configs/counts.json --json
demiflow workbench run start ./experiment RUN_ID --expected DIGEST --wait
demiflow workbench run show ./experiment RUN_ID
```

`RUN_ID` and `DIGEST` come from the output of `prepare`. Other subcommands: `list`, `logs`, `cancel`, `resume`.

Managed runs put extra requirements on a Flow; when they are not met, `prepare` refuses and says why. For example, writing a Lance table must use `mode="create"` with an explicit `schema` (as the example above does), and the project directory has to lie inside the run partition the deployment allows.

## Limits to be aware of

- Forge's check and registration are not a sandbox: code executed directly with Python, bypassing Forge, is not constrained by them.
- A check report is not permission to run; before a run the platform verifies again that source, configuration and data versions still match what was checked.
- The Workbench is a single-machine service for local access or access through SSH forwarding, not a multi-tenant public service.
