# Agent operators: multi-turn tool use within one row

`agentmap_async` still turns one input row into one result. The difference is that before answering, the model may call the tools you open to it: read documents, search vectors, call your own functions. How often it calls and with which arguments is the model's decision; the platform validates the arguments, runs the tool, hands the result back to the model and enforces the budgets.

A flow with fixed steps does not need it: chain ordinary `map` / `map_prompt` nodes. Use an agent operator only when "what to do next depends on what the previous step showed".

## Quick start

A tool is a plain function in the project's `operators/` package:

```python
# operators/scores.py
def compute_score(checks):
    return {"score": sum(1 for c in checks if c) / max(len(checks), 1)}
```

Model, tools, budgets, task and output format are written in one configuration:

```yaml
# agent.yaml
schema_version: demiflow_agent_v2
runtime: demiflow
model:
  name: gpt-4o-mini
  transport: openai_compatible
  base_url_env: MODEL_BASE_URL
  api_key_env: MODEL_API_KEY
operators:
  compute_score:
    fn: operators.scores:compute_score
    description: Compute the share of checks that passed.
    version: "1"
    arguments:
      type: object
      properties:
        checks:
          type: array
          maxItems: 64
          items: {type: boolean}
      required: [checks]
      additionalProperties: false
budgets:
  max_requests: 200
  max_turns: 4
  max_calls_per_turn: 2
options:
  timeout_s: 60
tasks:
  grade:
    version: v1
    template: "Grade this submission: {{ payload | json }}"
    response_schema:
      type: object
      required: [result]
      additionalProperties: false
      properties:
        result: {type: string}
```

```python
from demiflow import data

rows = (
    data.from_items([{"submission": {"id": 1}}])
    .agentmap_async("grade", config="agent.yaml",
                    inputs={"payload": "submission"}, output="grade",
                    call_output="agent_call", error_output="agent_error",
                    concurrency=2)
    .materialize()
    .take_all()
)
# [{'submission': {'id': 1}, 'grade': '...', 'agent_call': {...}, 'agent_error': None}]
```

## The configuration file

One `demiflow_agent_v2` configuration holds everything and refers to no other prompt file:

| Item | Meaning |
|---|---|
| `runtime` | `demiflow`: calls an OpenAI-compatible model over HTTP; `codex`: runs through Codex |
| `model` | Under `demiflow` the same as `model` in a prompt pack; under `codex` only `name` |
| `operators` | The tools opened to the model |
| `budgets` | The budgets |
| `options` | Request settings such as `timeout_s` and `stream` |
| `tasks` | Tasks: `version`, `template`, `response_schema`, written as in a prompt pack |

The node parameters (`inputs`, `output` / `outputs`, `call_output`, `error_output`, `concurrency`, `queue_depth`) are the same as for `map_prompt_async`. A node's `max_requests` can only lower the ceiling of the configuration.

## Opening tools

### Your own functions

```yaml
operators:
  compute_score:
    fn: operators.scores:compute_score     # module:function
    description: The tool description the model sees
    version: "1"                           # change it when the implementation or meaning changes
    arguments: {...}                       # arguments the model may fill in, as JSON Schema
    execution: thread                      # sync functions default to thread, async ones to async
    replay: verify
```

- `arguments` is a closed object schema. Valid arguments from the model are passed to the function unchanged; for invalid ones the platform tells the model what is wrong, and the model can correct them in its remaining turns.
- The model can only call the tools listed here by name; it cannot choose a Python implementation path.
- A custom function must actually be defined inside the project's `operators/` package directory.
- To fix some arguments so that the model cannot change them, use `fixed_arguments` and `fixed_schema`.
- The same function can still be used directly in `Dataset.map`.

This is a constraint on where code lives, not a sandbox: what the function does internally is still yours to review.

### Stateful tools

Replace `fn` with `actor: module:ClassName` and provide constructor arguments in `init`. The class implements `__call__` and needs no base class.

- At most one instance per row and tool; calls within one row reuse it, and rows are isolated from each other.
- It is created on the first valid call. Optional `astart`, `astop` and `aclose` are invoked; cleanup also happens on failure, timeout and cancellation.
- Only configure in the constructor; put I/O initialization into `astart`.

### Built-in tools

Refer to a platform function directly; its description and argument structure come with it:

```yaml
operators:
  read_documents:
    fn: demiflow.collect.reading:read_documents
resources: document_resources
```

`resources` names a column of the row listing the documents this row may read. The model can read only the objects declared there in advance. Built-in tools are `read_documents`, `map_embeddings` and `search_vectors`.

### Tools that return images

When a tool result carries images, declare the fields that hold them; the platform verifies them and gives the real images to the model:

```yaml
operators:
  search_vectors:
    fn: demiflow.lance.search_api:search_vectors
    result_images:
      items_field: candidates
      uri_field: image_uri
      sha256_field: sha256
```

## Budgets

Budgets are written under `budgets` in the configuration and are all hard limits.

`runtime: demiflow`:

| Budget | Default | Meaning |
|---|---|---|
| `max_requests` | required | The most model requests this node may make; every turn counts |
| `max_turns` | 4 | Most turns per row, including the turn of the final answer |
| `max_calls_per_turn` | 2 | Most tool calls per turn |
| `max_context_chars` | 60,000 | Characters of the task text plus accumulated tool results |
| `max_observation_chars` | 24,000 | Characters of one tool call and its result; exceeding it fails, it is not truncated |
| `max_response_chars` | 24,000 | Characters of the final answer |
| `max_tool_images` | 8 | Tool images attached for the model per row |

`runtime: codex` does not accept `max_turns` and `max_calls_per_turn`; it uses:

| Budget | Default | Meaning |
|---|---|---|
| `max_requests` | required | The most Codex sessions started (not internal model requests) |
| `max_operator_calls` | 6 | Most calls per row to the tools you opened |
| `options.timeout_s` | 600 | Time limit of the whole session, including waiting for tools |

There is no hard limit in advance on Codex's internal model turns, tokens or cost; they can only be read from the records afterwards.

## Two runtimes

**`runtime: demiflow`** calls the model over HTTP. Connection and timeout options are the same as in [Calling LLMs](llm.md), but written under `options` of the configuration file:

```yaml
options:
  stream: true
  timeout_s: 1800
  read_timeout_s: 180
  request_options:
    reasoning_effort: high
```

**`runtime: codex`** starts a separate Codex session per row and uses the authentication and model provider already configured for Codex:

```yaml
runtime: codex
model: {name: your-codex-model}
operators: []
budgets:
  max_requests: 300
options:
  timeout_s: 600
  codex_agent:
    web_search: live      # disabled (default), cached or live
    shell_tool: true      # allow reading files in a read-only sandbox; default false
```

If the model name actually returned differs from the configuration, the call fails; the model is never switched quietly.

## Failures and repeated execution

- Invalid arguments from the model, a call to a tool that was not opened, and a final answer that does not satisfy `response_schema` are first reported back to the model so that it can correct them within the remaining budget.
- When the budget is used up or the result is still invalid, the row fails. With `error_output` only that row is affected and the others continue.
- `call_output` keeps the call record of the row, including the model requests and tool results used.
- Running the same request again reuses the existing result: no new session, no budget consumed.
- A session interrupted by an exception or cancellation is recorded as "outcome uncertain"; recovery does not rerun a call that may already have been charged.
- If a custom tool has side effects, the platform does not guarantee it runs only once.

## Limits to be aware of

- At most 64 tools per configuration.
- The budgets limit characters, call counts and time, not process memory; limits on memory and I/O inside a tool function are yours to implement.
- A tool running in a thread cannot be aborted; I/O inside the function must have its own timeout.
- Only local execution is supported at present.
