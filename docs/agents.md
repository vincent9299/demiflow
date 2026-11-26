# Agent 算子：让模型在一行里多轮调用工具

`agentmap_async` 处理的仍然是“一行输入、一个结果”，区别在于模型可以在得出结果之前调用你开放给它的工具：读文档、查向量、调用你自己的函数。调用几次、传什么参数由模型决定，平台负责校验参数、执行工具、把结果交还模型，并执行各项预算。

固定步骤的流程不需要它，用普通的 `map` / `map_prompt` 串起来即可。只有“下一步做什么取决于上一步看到了什么”时才用 Agent 算子。

## 快速开始

工具是普通函数，放在项目的 `operators/` 包里：

```python
# operators/scores.py
def compute_score(checks):
    return {"score": sum(1 for c in checks if c) / max(len(checks), 1)}
```

模型、工具、预算、任务和输出格式都写在一份配置里：

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

## 配置文件

一份 `demiflow_agent_v2` 配置包含全部内容，不引用别的提示词文件：

| 项 | 含义 |
|---|---|
| `runtime` | `demiflow`：通过 HTTP 调用 OpenAI 兼容的模型；`codex`：由 Codex 运行 |
| `model` | `demiflow` 下与 prompt pack 的 `model` 相同；`codex` 下只写 `name` |
| `operators` | 开放给模型的工具 |
| `budgets` | 各项预算 |
| `options` | 请求设置，如 `timeout_s`、`stream` |
| `tasks` | 任务：`version`、`template`、`response_schema`，写法同 prompt pack |

节点上的参数（`inputs`、`output` / `outputs`、`call_output`、`error_output`、`concurrency`、`queue_depth`）与 `map_prompt_async` 相同。节点的 `max_requests` 只能把配置里的上限调低。

## 开放工具

### 自己的函数

```yaml
operators:
  compute_score:
    fn: operators.scores:compute_score     # 模块:函数
    description: 模型看到的工具说明
    version: "1"                           # 实现或含义变了就改
    arguments: {...}                       # 模型可以填写的参数，JSON Schema
    execution: thread                      # 同步函数默认 thread，异步函数默认 async
    replay: verify
```

- `arguments` 是一个封闭的对象 schema。模型提交的参数合法时原样传给函数；不合法时平台把错误告诉模型，模型可以在剩余轮次里改正。
- 模型只能按名字调用这里列出的工具，不能选择 Python 实现路径。
- 自定义函数必须实际定义在项目的 `operators/` 包目录下。
- 需要固定某些参数、不让模型改动时，用 `fixed_arguments` 和 `fixed_schema`。
- 同一个函数仍然可以直接用在 `Dataset.map` 里。

这是对代码归属的约束，不是沙箱：函数内部做什么仍然需要你自己审查。

### 有状态的工具

把 `fn` 换成 `actor: 模块:类名`，用 `init` 提供构造参数。类实现 `__call__`，不需要继承基类。

- 每行每个工具最多一个实例，同一行的多次调用复用它，不同行之间隔离。
- 第一次合法调用时才创建。可选的 `astart`、`astop`、`aclose` 会被调用；失败、超时和取消时也会清理。
- 构造函数里只做配置，I/O 初始化放进 `astart`。

### 平台自带的工具

直接引用平台函数即可，说明和参数结构自动带上：

```yaml
operators:
  read_documents:
    fn: demiflow.collect.reading:read_documents
resources: document_resources
```

`resources` 指定行里的一列，列出这一行允许读取的文档。模型只能读取这里预先声明的对象。自带工具有 `read_documents`、`map_embeddings`、`search_vectors`。

### 工具返回图片

工具结果里带图片时，声明图片所在的字段，平台会校验后把真实图片交给模型：

```yaml
operators:
  search_vectors:
    fn: demiflow.lance.search_api:search_vectors
    result_images:
      items_field: candidates
      uri_field: image_uri
      sha256_field: sha256
```

## 预算

预算写在配置的 `budgets` 里，全部是硬上限。

`runtime: demiflow`：

| 预算 | 默认 | 含义 |
|---|---|---|
| `max_requests` | 必填 | 这个节点最多发起多少次模型请求，每一轮都计入 |
| `max_turns` | 4 | 每行最多几轮，含最后回答的一轮 |
| `max_calls_per_turn` | 2 | 每轮最多调用几次工具 |
| `max_context_chars` | 60,000 | 任务文字加累计工具结果的字符数 |
| `max_observation_chars` | 24,000 | 单次工具调用及结果的字符数，超出时失败而不是截断 |
| `max_response_chars` | 24,000 | 最终回答的字符数 |
| `max_tool_images` | 8 | 每行附给模型的工具图片张数 |

`runtime: codex` 不接受 `max_turns` 和 `max_calls_per_turn`，改用：

| 预算 | 默认 | 含义 |
|---|---|---|
| `max_requests` | 必填 | 最多开启多少个 Codex 会话（不是内部的模型请求数） |
| `max_operator_calls` | 6 | 每行最多调用几次你开放的工具 |
| `options.timeout_s` | 600 | 整个会话的时限，含等待工具的时间 |

Codex 内部的模型轮数、token 和费用没有事前硬上限，只能事后从记录里看到。

## 两种运行方式

**`runtime: demiflow`** 通过 HTTP 调用模型，连接和超时选项与 [调用大模型](llm.md) 相同，只是写在配置文件的 `options` 里：

```yaml
options:
  stream: true
  timeout_s: 1800
  read_timeout_s: 180
  request_options:
    reasoning_effort: high
```

**`runtime: codex`** 每行启动一个独立的 Codex 会话，使用 Codex 自己已配置好的认证和模型提供方：

```yaml
runtime: codex
model: {name: your-codex-model}
operators: []
budgets:
  max_requests: 300
options:
  timeout_s: 600
  codex_agent:
    web_search: live      # disabled（默认）、cached 或 live
    shell_tool: true      # 允许在只读沙箱里读文件，默认 false
```

返回的实际模型名与配置不一致时失败，不会悄悄换模型。

## 失败与重复执行

- 模型给出的参数不合法、调用了未开放的工具、最终回答不符合 `response_schema`，都会先反馈给模型，让它在剩余预算内改正。
- 预算用尽或仍然不合格时，这一行失败。设置了 `error_output` 时只影响这一行，其他行继续。
- `call_output` 里保存这一行的调用记录，包括用到的模型请求和工具结果。
- 相同的请求再次执行时复用已有结果，不新开会话、不占预算。
- 异常或取消中断的会话记为“结果不确定”，恢复时不会自动重跑可能已经计费的调用。
- 自定义工具如果有副作用，平台不保证它只执行一次。

## 需要知道的限制

- 每份配置最多 64 个工具。
- 各项预算限制的是字符数、调用次数和时间，不是进程内存；工具函数内部的内存和 I/O 上限需要自己实现。
- 线程里执行的工具不能被强制中止，函数里的 I/O 必须自带超时。
- 目前只支持本地执行。
