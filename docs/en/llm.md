# Calling LLMs

`map_prompt` and `map_prompt_async` turn a model call into a Dataset node: each row renders a prompt, calls the model, has the reply validated against a JSON Schema, and gets the result written back. The model, the prompt and the reply format live together in one versioned YAML file (a prompt pack); secrets are supplied only through environment variables.

## Quick start

```yaml
# prompts.yaml
schema_version: demiflow_prompt_pack_v2
prompts:
  summarize:
    version: summarize-v1
    model:
      name: gpt-4o-mini
      transport: openai_compatible
      base_url_env: MODEL_BASE_URL
      api_key_env: MODEL_API_KEY
    schema_retries: 1
    response_schema:
      type: object
      additionalProperties: false
      required: [summary]
      properties:
        summary: {type: string, maxLength: 280}
    template: |
      Summarize this paper title in one sentence for a newsletter:
      {{ title }}
```

```python
from demiflow import data

table = data.from_items([{"title": "Attention is all you need"}])
rows = table.map_prompt(
    "summarize", config="prompts.yaml",
    inputs={"title": "title"}, output="summary",
    max_requests=1_000,
).take_all()
# [{'title': 'Attention is all you need', 'summary': '...'}]
```

## Two entry points

| | `map_prompt` | `map_prompt_async` |
|---|---|---|
| Execution | Synchronous, row by row | Streaming, several rows at once |
| Concurrency | — | `concurrency`, `queue_depth` |
| A failing row | Ends the action | Can be kept as a row with `error_output` |
| Backends | Local and Ray | Local only at present |

Both share the same prompt pack, the same input and output bindings and the same validation. Use `map_prompt_async` for large volumes:

```python
rows = (
    table.map_prompt_async(
        "summarize", config="prompts.yaml",
        inputs={"title": "title"}, output="summary",
        concurrency=8, call_output="call", error_output="error",
        max_requests=10_000,
    )
    .materialize()
)
```

`materialize()` before joins, grouping and other relational operations.

## Writing a prompt pack

Each prompt consists of:

| Item | Meaning |
|---|---|
| `version` | Version label of this prompt; change it when the prompt changes |
| `model` | `name`, `transport` (`openai_compatible` or `azure_openai`), `api_key_env`, and exactly one of `base_url` and `base_url_env`. Azure also needs `api_version` |
| `response_schema` | JSON Schema of the reply; the root must be an object |
| `schema_retries` | Retries when the reply is invalid: 0 or 1 |
| `template` | The prompt template, one string |

The configuration stores names of environment variables, never the secrets themselves.

### Template placeholders

| Form | Use |
|---|---|
| `{{ name }}` or `{{ name \| text }}` | Text or scalars |
| `{{ name \| json }}` | Embeds the value as JSON |
| `{{ name \| image }}` | Images: image bytes, data URLs, HTTP(S) URLs, or a list of them |
| `{{ name \| numbered_image }}` | The same, with `Image 1:`, `Image 2:` before each image |

### Reply format

Keywords supported in `response_schema`: `type`, `properties`, `required`, `additionalProperties`, `items`, `minItems`, `maxItems`, `minLength`, `maxLength`, `minimum`, `maximum`, `exclusiveMinimum`, `exclusiveMaximum`, `enum`, `const`, `format` (`date`, `date-time`).

The platform adds a system instruction that asks for JSON satisfying the schema and nothing else. A reply that is not valid JSON or does not satisfy the schema is retried as `schema_retries` allows; if it is still invalid, the row fails.

## Binding inputs and outputs

- `inputs` maps placeholder names of the template to field names of the row and must cover all placeholders exactly.
- `output="column"`: for a schema with one required top-level property. It writes **the value of that property**, not the whole reply object.
- `outputs={"property": "column", ...}`: for several properties, mapped one by one.

Use exactly one of `output` and `outputs`.

## Controlling cost

- `max_requests`: the most new model requests this node may make; retries count, results that already exist do not. The node stops when the budget is exceeded.
- Every node has its own budget; two nodes declared for the same prompt have two budgets.
- `token_budget`: checks the token count of the complete rendered messages before a request is sent.
- `temperature`: sent only when given explicitly; the platform injects no default.

## Handling failures (`map_prompt_async`)

```python
rows = table.map_prompt_async(
    "summarize", config="prompts.yaml", inputs={"title": "title"}, output="summary",
    concurrency=8, call_output="call", error_output="error", max_requests=10_000,
).materialize().take_all()

failed = [row for row in rows if row.get("error")]
```

- `error_output="column"`: when the call or the validation of a row fails, the original row is kept with `{category, type, detail, call}` in this column and no result column. For example, `category` is `invalid_response` when both attempts return invalid JSON.
- `call_output="column"`: attaches information about the call to every row, including duration, HTTP status, model name and request identifier.
- `when=function`: rows for which it returns `False` pass through unchanged without a model call.
- `catch=(ExceptionType, ...)`: matching rows are dropped and counted.
- Transport errors are never retried silently. A managed model service that fails to start ends the whole node, even with `error_output`.

## Interruption and recovery

When another stage fails, the engine closes admission for new model requests. Calls already sent may finish within their request deadline and the run’s cleanup grace period. Their responses are recorded in the original journal for replay. The action reports the original failure.

Inside a `RunContext`, progress is committed in batches at the downstream `materialize()`: on resume, confirmed inputs are skipped and their committed results returned; a batch that was not confirmed may call the model again. See "Resume where you stopped" in the README.

A plain `run_stream()` outside a `RunContext` has no durable progress, and running it again repeats the calls. Several actions on the same Dataset that was not materialized also repeat calls; `materialize()` first when results are to be reused.

To reuse replies across runs, give the node a private cache:

```python
options={"sqlite_journal": {"path": "/abs/path/review.sqlite", "max_requests": 100}}
```

The cache identifies requests by what is actually sent: a change of model, prompt, input or request parameters is a new request.

## Request and connection options

Passed through `options`:

| Option | Default | Meaning |
|---|---|---|
| `stream` | `False` | `True` receives the reply as a stream (SSE); each row still yields one complete, validated result |
| `timeout_s` | 120 | Total time limit of one request |
| `connect_timeout_s` | min(10, total) | Time limit for establishing the connection |
| `read_timeout_s` | total | Idle limit while waiting for data; every chunk restarts it |
| `write_timeout_s` / `pool_timeout_s` | total / min(30, total) | Idle limit of the upload / wait for a pool slot |
| `max_connections` | same as `concurrency` | Connection pool limit |
| `request_options` | — | Extra parameters passed through to the model API |
| `gateway` | none | `litellm` explicitly turns off the gateway's retries and fallbacks |

`request_policy={"initial_concurrency": 2, "max_concurrency": 4}` lets the node adapt the number of requests in flight to latency and errors, never above `concurrency`.

## Several models in one node: `routes`

Send different rows to different models, decided by an upstream field:

```python
rows.map_prompt_async(
    "review", config=pack_a, inputs={"payload": "request"}, output="result",
    routes={"a": {"config": pack_a, "concurrency": 4},
            "b": {"config": pack_b, "concurrency": 8}},
    route="reviewer", concurrency=12, error_output="error",
)
```

- Each row enters the route named by its `reviewer` field. The platform does not choose a model for you and does not send a failed row to another model.
- At most 32 routes; each can have its own `options`, `max_requests` and model service.
- `isolate_route_failures=True` together with `error_output`: when the service of one route is unavailable, only the rows assigned to it are marked `route_unavailable` and the other routes continue.

## Supplying your own messages

For full control over the conversation (multi-turn history, a custom system prompt), use `input_mode: messages` in the prompt instead of `template` and bind `inputs={"messages": "column"}`. The column holds `[{role, content}, ...]`, where `content` is a string or a list of text and image parts.

In this mode the platform sends the messages as they are: no system instruction is added, nothing is trimmed, and `schema_retries` is not supported; put any output requirements into your own messages. The reply is still validated against `response_schema`.

## Using a local model server

`service=VLLMService({...})` lets the node manage a local model server: it starts when the first request that really needs a call arrives and closes when the action finishes, fails or is cancelled. When two models have to use the same GPUs one after the other, `materialize()` between them.

## Limits to be aware of

- `map_prompt_async` currently supports local execution only.
- Concurrency and queues limit rows. Estimate the memory of large images and long contexts yourself as "concurrency × size of one request".
- The number of requests in flight is also bounded by a memory window: each request reserves the actual size of its input row plus one full output row. Under the default budget "read → model call → output" runs at most 7 requests at once; further requests queue and nothing fails. The concurrency actually reached is `call_concurrency` in the run statistics; see [streaming map](streaming-map.md#estimating-memory).
- `max_requests` applies within one process; it is not a global quota across processes.
