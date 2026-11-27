# 调用大模型

`map_prompt` 和 `map_prompt_async` 把一次模型调用变成 Dataset 的一个节点：每行渲染一段提示词，调用模型，按 JSON Schema 校验返回，再把结果写回这一行。模型、提示词和返回格式集中写在一个带版本的 YAML 文件（prompt pack）里，密钥只通过环境变量提供。

## 快速开始

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

## 两个入口

| | `map_prompt` | `map_prompt_async` |
|---|---|---|
| 执行方式 | 同步，逐行 | 流式，多行并发 |
| 并发 | — | `concurrency`、`queue_depth` |
| 单行失败 | 终止动作 | 可用 `error_output` 保留为一行 |
| 后端 | 本地和 Ray | 目前只支持本地 |

两者共用同一份 prompt pack、同样的输入输出绑定和校验规则。大批量调用用 `map_prompt_async`：

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

后面要接关联、分组等运算时先 `materialize()`。

## 编写 prompt pack

每个 prompt 由这几项组成：

| 项 | 含义 |
|---|---|
| `version` | 这个 prompt 的版本标识，改了提示词就换一个 |
| `model` | `name`、`transport`（`openai_compatible` 或 `azure_openai`）、`api_key_env`，以及 `base_url` 或 `base_url_env` 二选一。Azure 还需要 `api_version` |
| `response_schema` | 返回的 JSON Schema，根必须是 object |
| `schema_retries` | 返回不合格时重试的次数，0 或 1 |
| `template` | 提示词模板，一个字符串 |

配置里只写环境变量的名字，不写密钥本身。

### 模板占位符

| 写法 | 用途 |
|---|---|
| `{{ name }}` 或 `{{ name \| text }}` | 文本或标量 |
| `{{ name \| json }}` | 把值按 JSON 嵌入 |
| `{{ name \| image }}` | 图片：图片字节、data URL、HTTP(S) 地址，或它们的列表 |
| `{{ name \| numbered_image }}` | 同上，并在每张图前加 `Image 1:`、`Image 2:` |

### 返回格式

`response_schema` 支持的关键字：`type`、`properties`、`required`、`additionalProperties`、`items`、`minItems`、`maxItems`、`minLength`、`maxLength`、`minimum`、`maximum`、`exclusiveMinimum`、`exclusiveMaximum`、`enum`、`const`、`format`（`date`、`date-time`）。

平台会自动加上一条要求“只输出符合该 Schema 的 JSON”的系统指令。返回不是合法 JSON 或不符合 Schema 时，按 `schema_retries` 重试；仍不合格则这一行失败。

## 绑定输入和输出

- `inputs` 把模板里的占位符名映射到行里的字段名，必须正好覆盖所有占位符。
- `output="列名"`：Schema 只有一个必填的顶层属性时使用，写入的是**这个属性的值**，不是整个返回对象。
- `outputs={"属性名": "列名", ...}`：有多个属性时使用，逐个映射。

`output` 和 `outputs` 只能用一个。

## 控制花费

- `max_requests`：这个节点最多发起多少次新的模型请求，重试也计入。命中已有结果不计数。超出后节点停止。
- 每个节点的额度独立；同一个 prompt 声明两个节点就是两份额度。
- `token_budget`：发请求前按渲染后的完整消息检查 token 数。
- `temperature`：只在显式给出时发送，平台不注入默认值。

## 处理失败（`map_prompt_async`）

```python
rows = table.map_prompt_async(
    "summarize", config="prompts.yaml", inputs={"title": "title"}, output="summary",
    concurrency=8, call_output="call", error_output="error", max_requests=10_000,
).materialize().take_all()

failed = [row for row in rows if row.get("error")]
```

- `error_output="列名"`：这一行调用或校验失败时，保留原行并写入 `{category, type, detail, call}`，不写结果列。例如返回两次都不是合法 JSON 时，`category` 为 `invalid_response`。
- `call_output="列名"`：每行附带这次调用的信息，包括耗时、HTTP 状态、模型名和请求标识。
- `when=函数`：返回 `False` 的行不调用模型，原样通过。 单路 `map_prompt_async` 的 `when` 判断器可用成对的 `snapshot_state()` 和 `restore_state(value)` 保存跨行状态。引擎在检查点前收完该节点的在途调用，把判断器状态和输入进度一起恢复。旧检查点缺少已使用的判断器状态时，继续计算需要新开一次运行。
- `catch=(异常类型, ...)`：命中的行被丢弃并计数。
- 传输错误不会被悄悄重试。受管模型服务启动失败会终止整个节点，即使设置了 `error_output`。

## 中断与恢复

上游某一行失败时，引擎停止准入新的模型请求。已经发出的请求可以在原请求期限和运行清理期限内收完并记入原账本，供恢复时复用。该次操作仍会报告最初的失败。

在 `RunContext` 里运行时，进度按批提交在下游的 `materialize()` 处：恢复时跳过已确认的输入，直接返回已提交的结果；尚未确认的一批可能重新调用模型。用法见 README 的“从中断处继续”。

不在 `RunContext` 里直接 `run_stream()` 时没有持久进度，重复执行会重复调用。同一个未物化的 Dataset 上执行多个动作也会重复调用，需要复用结果时先 `materialize()`。

需要跨运行复用返回结果时，可以给节点一个私有缓存：

```python
options={"sqlite_journal": {"path": "/abs/path/review.sqlite", "max_requests": 100}}
```

缓存按实际发出的请求内容识别：模型、提示词、输入或请求参数任何一项变化都是新的请求。

## 请求与连接选项

通过 `options` 传入：

| 选项 | 默认 | 含义 |
|---|---|---|
| `stream` | `False` | `True` 时以流式（SSE）接收，仍然每行只产出一个完整、校验过的结果 |
| `timeout_s` | 120 | 单个请求的总时限 |
| `connect_timeout_s` | min(10, 总时限) | 建立连接的时限 |
| `read_timeout_s` | 总时限 | 等待数据的空闲时限，每收到一块数据重新计时 |
| `write_timeout_s` / `pool_timeout_s` | 总时限 / min(30, 总时限) | 上传空闲时限 / 等待连接池的时限 |
| `max_connections` | 等于 `concurrency` | 连接池上限 |
| `request_options` | — | 透传给模型接口的额外参数 |
| `gateway` | 无 | 设为 `litellm` 时明确关闭网关的重试和回退 |
| `max_response_bytes` | 8MiB | 单个回复的字节上限，超出即失败。调小可以减少每个调用的内存预留，提高同时进行的调用数，见流式 map 文档“调用窗口” |

`request_policy={"initial_concurrency": 2, "max_concurrency": 4}` 让节点按延迟和错误自动调节同时进行的请求数，上限不超过 `concurrency`。

## 一个节点里用多个模型：`routes`

把不同的行分给不同的模型，由上游的一个字段决定去向：

```python
rows.map_prompt_async(
    "review", config=pack_a, inputs={"payload": "request"}, output="result",
    routes={"a": {"config": pack_a, "concurrency": 4},
            "b": {"config": pack_b, "concurrency": 8}},
    route="reviewer", concurrency=12, error_output="error",
)
```

- 每行按 `reviewer` 字段的值进入对应路由。平台不替你选模型，也不会把失败的行转给另一个模型。
- 最多 32 个路由；每个路由可以有自己的 `options`、`max_requests` 和模型服务。
- `isolate_route_failures=True` 配合 `error_output`：某个路由的服务不可用时，只把分给它的行标记为 `route_unavailable`，其他路由继续。

## 自己拼消息

需要完全控制对话内容（多轮历史、自定义系统提示）时，在 prompt 里用 `input_mode: messages` 代替 `template`，并绑定 `inputs={"messages": "列名"}`。该列是 `[{role, content}, ...]`，`content` 是字符串或文本/图片片段的列表。

这种方式下平台原样发送消息，不加系统指令、不裁剪，也不支持 `schema_retries`；需要的输出要求请写进你自己的消息里。返回仍按 `response_schema` 校验。

## 使用本地模型服务

`service=VLLMService({...})` 让节点自己管理一个本地模型服务：第一个真正需要调用的请求到来时启动，动作结束、失败或取消时关闭。两个模型要先后使用同一批 GPU 时，在它们之间 `materialize()`。

## 需要知道的限制

- `map_prompt_async` 目前只支持本地执行。
- 并发和队列限制的是行数。大图片、长上下文的内存占用要按“并发数 × 单个请求大小”自行估算。
- 同时进行的请求数还受内存窗口限制：每个请求预留它这一行输入的实际大小加一整行输出。默认额度下，“读取 → 模型调用 → 输出”最多同时进行 7 个请求，多出的请求排队等待，不报错。实际并发见运行统计的 `call_concurrency`，说明见 [流式 map](streaming-map.md#内存估算)。
- `max_requests` 在单个进程内生效，不是跨进程的全局配额。
