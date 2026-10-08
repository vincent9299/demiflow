# agentmap_async：行内算子环境

`Dataset.agentmap_async` 对一行执行入口任务，由模型决定是否调用已开放的原生算子、提供哪些参数以及何时完成。外层仍是 Dataset／流及其上的一个 `AgentMapOp`；内部不创建子 Dataset、不运行嵌套数据流。业务不实现工具循环。`map_prompt_async` 保持原来的单次完成契约。

**agentmap 不做业务规则转换。** 模型的 `arguments` 与原生单行算子 API 一致，合法参数原样传给原函数。平台只做契约／资源范围／预算校验，绑定运行上下文与固定执行配置，然后回传结果或错误。不转换 `document_ids/query`，不生成 questions，不重组阅读请求，也不猜测缺失参数。

## 唯一完整配置

`agentmap_async` 的 `config` 是一份完整的 `demiflow_agent_v2` YAML（或其已解析的 `AgentConfig`）。模型、后端、工具、预算、任务正文和输出 schema 全部内联在同一文件中，不引用另一份 prompt/任务文件。普通 `map_prompt` / `map_prompt_async` 继续使用完整的 `demiflow_prompt_pack_v2`，原契约保持。两类配置不可混用。

```yaml
schema_version: demiflow_agent_v2
runtime: codex
model: {name: gpt-6-astra}
operators: []
budgets:
  max_requests: 300
  max_context_chars: 60000
options:
  timeout_s: 600
  codex_agent:
    bin: codex
    shell_tool: true
    web_search: live
tasks:
  verify_claim:
    version: v1
    template: "核实以下内容：{{ payload | json }}"
    response_schema:
      type: object
      required: [result]
      additionalProperties: false
      properties:
        result: {type: string}
```

```python
prepared.agentmap_async(
    'verify_claim', config='agent_codex.yaml',
    inputs={'payload': 'prompt_payload'}, output='verification',
    call_output='agent_call', error_output='agent_error',
    concurrency=2, queue_depth=1,
    options={'sqlite_journal': {'path': 'agent_calls.sqlite'}},
)
```

节点只传行映射、调度及日志/回放位置；模型、工具和执行设置都由唯一 config 解析。节点 max_requests 可收紧配置中的额度，不能扩额；0 用于只回放。原 `PromptPack + environment` 分开传入的方式明确拒绝。OperatorEnvironment 仍是平台实现及程序化 AgentConfig 的内部组成部分；旧 v1 环境策略不是完整 agent 配置。

### HTTP 流式配置

`runtime: demiflow` 复用 `map_prompt_async` 的 HTTP 客户端和 SSE 接收器。流式开关写在同一 agent YAML 的顶层 `options` 下，首轮、工具后续轮和输出修正轮全部沿用：

```yaml
options:
  stream: true
  stream_include_usage: true
  timeout_s: 1800          # 整次 HTTP 请求总时限
  read_timeout_s: 180      # 收到响应数据之间的最长空闲时间
  request_options:
    reasoning_effort: xhigh
```

未配置 `stream` 或显式设为 `false` 时仍为非流式 JSON。`async` 表示异步执行，本身不启用 SSE。普通 `map_prompt_async` 将这些字段放在节点 `options` 中；`agentmap_async` 将执行配置集中在 agent YAML，节点 `options` 只接日志/回放位置。不要把 `stream` 放在 `request_options` 内，否则校验拒绝。平台在实际 API 请求中设置 `stream: true`，完整接收并校验后才提交该轮；不会将半截 JSON 当作结果或自动降级为非流式。工具调用仍用 agent JSON envelope，不依赖 provider 的原生 function-call 增量。

其余连接、响应字节/事件上限、日志与恢复选项同 [HTTP prompt 传输](prompt_http.md)。改变流式开关会改变实际请求身份，既有非流式响应不会被冒充为流式回放。`runtime: codex` 由 Codex 管理模型传输，不适用这些 HTTP 字段。隔离回归 `test_yaml_stream_survives_tool_turns_repairs_and_replay` 覆盖原生文档读取、输出修正、逐轮 SSE、精确回放和截断流不交付。

两套后端是平台能力，由同一文件的 runtime 选择。HTTP 使用 `runtime: demiflow`，model 为原 HTTP 模型契约（name/transport/base_url 或 base_url_env/api_key_env），options 为 HTTP 请求设置；回调可声明 `operators: [read_documents]`、`resources: document_resources`，budgets 指定 max_turns/max_calls_per_turn 等原行内限制。HTTP/offline 由平台维护 loop，节点 offline_store 可登记同任务的离线请求。Codex 使用自身 loop，model 只接受 name，options.codex_agent 配置原生工具；operators=[] 表示不注入回调。

`budgets.timeout_s` 是算子时限，`options.timeout_s` 是 HTTP 请求或 Codex 整会话时限。模型调用原生算子时，参数只校验、原样执行；不补业务输入或改写规则。文件上限 1 MiB、最多 64 个内联任务，拒绝外部任务文件引用、未知字段、错误模型/后端组合和无效预算。解析配置不启动任何服务。

Codex YAML 的 budgets 不接受 `max_turns/max_calls_per_turn`，因为 app-server 的一次 turn 可以包含多次内部模型完成，不能拿 HTTP 轮次限制冒充 Codex 模型调用上限。Codex agent 配置必须提供有限的 `budgets.max_requests`（允许 0 用于回放）。不能混用 `codex_exec`、offline 或 HTTP transport 配置；普通 prompt 的旧 `codex_exec` 路径仍保留。

## operators 直接配置 fn/actor

Agent 的唯一 YAML 配置直接选择普通函数或 actor，不需要 `OperatorAPI` 类、Python 注册调用或可变全局注册表。Dataset.map/map_async 继续接收现有 fn/actor；没有给 Dataset 增加新抽象。模型只提交已开放的方法名和动态 arguments，不能选择 Python 实现路径。两种 runtime 共用同一个参数校验与调用流程，不改写原参数和返回值。

标准能力直接引用平台原函数，自动沿用其原生说明、参数结构、绑定、校验和重放契约。例如：

```yaml
operators:
  read_documents:
    fn: demiflow.collect.reading:read_documents
resources: document_resources
```

旧 `operators: [read_documents]` 和 `operator_settings` 保持兼容；内置标准能力仍为 read_documents、map_embeddings、search_vectors。其原生模块维护普通契约字典，导入时不注册。这些标准简写不开放其他函数，也不表示所有 Dataset 建图方法都变成了工具。标准配置解析后的原请求身份保持一致，历史调用记录不重写。

业务自定义工具在同一 `operators` 配置内声明。例如下面引用业务已有的同步算分函数，函数仍可直接用于 Dataset.map：

```yaml
operators:
  compute_score:
    fn: my_pipeline.operators.scores:compute_score
    description: 根据检查结果计算分数。
    version: '1'
    arguments:
      type: object
      properties:
        checks:
          type: array
          maxItems: 64
          items: {type: boolean}
      required: [checks]
      additionalProperties: false
    execution: thread
    replay: verify
```

这段是消费者声明示例，不随平台安装虚构业务实现。`arguments` 是模型可填的闭合对象 schema。
若需要固定参数，在该条目增加 `fixed_arguments` 和 `fixed_schema`；`bindings` 仍只允许
`context`、`resources` 或 `limits.<字段>`，三类参数名称互斥。可选的 `validate_arguments`、
`validate_resources` 引用相同目录中的普通函数，签名分别是 `(arguments, resources, limits)`、
`(resources, limits)`。已有标准契约自动提供这些信息，无需在业务 YAML 复制文档请求结构。
实现、参数或权限含义变化时更新 version。

自定义 `fn`、`actor` 和校验函数只允许实际定义在 `operators/` 包目录下，
同时核对导入模块和实现文件；把 `os.system` 等转导入算子目录仍拒绝。消费者目录统一为 `operators/`，不保留旧拼写或平行目录。标准平台实现保持原归属，不强制搬迁或增加业务包装。
这是代码归属和可信配置约束，不是 Python 沙箱；任意自定义实现是否隐藏模型、子 Dataset、
数据库或服务旁路仍需按 pipeline 规范 review。固定步骤在外层 Dataset 编排。

### actor 生命周期

同一条目用 `actor: my_pipeline.operators.module:ClassName` 替换 fn，可通过 `init` 提供
静态构造参数。类实现普通 `__call__`，不需要继承新基类；`arguments` 对应调用参数而非
构造参数。配置解析只核对声明和签名，不构造实例、不启动服务；构造函数只配置状态，
I/O 初始化放入 `astart`。模型参数不能修改 init。

首次参数合法的调用才创建 actor，每行每工具至多一个实例，同一行多次调用复用，跨行隔离。
平台复用现有资源生命周期协议执行 `astart`，退出时执行 `astop` 和 `aclose`，并关闭该实例
声明的 resources；回调结束不会提前执行外层 Dataset action 的全局清理。启动失败、调用失败、
超时和取消也会清理已构造实例。无调用或 recorded 缓存重放不创建 actor。

异步 `__call__`／函数默认 execution=async，同步默认 thread，显式配置须与实现匹配。
阻塞线程沿用有界 I/O 池，取消后等待已开始的工作结束再关闭资源；不能强制中止底层线程，
实现必须给 I/O 有限超时。actor 的并发与生命周期由调用所在的 Agent 行管理，不把它的
Dataset concurrency 策略当成创建嵌套 worker 的指令。

每个配置最多64个工具，完整 operator 声明最多256KiB、actor init最多64KiB；运行仍受行资源、
回调次数、上下文、观察、附图和时间预算约束。配置的字节／字符预算不覆盖函数内部任意分配，
算子仍须实现自身输入、I/O和输出上限。这些限制不等同于整个进程的RSS硬上限。

### 原生图片返回

有图片的函数／actor 可在同一 operators 条目声明结果字段：

```yaml
operators:
  search_vectors:
    fn: demiflow.lance.search_api:search_vectors
    # fixed_arguments/fixed_schema 按实际固定向量表配置。
    result_images:
      items_field: candidates
      uri_field: image_uri
      sha256_field: sha256
```

items_field 指定返回对象中的列表字段；空字符串表示返回值本身是列表。这里仅声明字段，不自动补写或改名原生结果。统一渲染层读取 URI、核验 SHA、检查文件头/格式和图片预算；Codex 工具响应包含 inputText 与实际 inputImage，HTTP 下一轮使用真实图片输入。图片不再仅以 URI 文本交回模型。返回值仍原样放在 result；额外 images 回执包含稳定内容身份 image_id=sha256:<SHA>、object_ref、原列表 position、attachment_index、实际附图状态和尺寸；本地对象另提供已解析的 local_path。attached 才代表成功送入上下文；unavailable / not_attached_budget 保留候选及原因，不能假装已经看过。持久化回执不存 base64。工具附图的 attachment_index 与初始业务图片编号相互独立，消费者须显式按 image_id 绑定，不能直接把它当作 Edit 的 seed_image。

工具图片预算由同一配置 budgets 声明：max_tool_images 默认每行 8 次实际附图（重复附图也计数），max_tool_image_bytes 每张 8 MiB，max_tool_image_total_bytes 每行总原始字节 32 MiB，max_tool_image_pixels 每张 2000 万像素，max_tool_image_candidates 每次最多 64 个候选。按张读取，未取得预算不读字节；HTTP 保留已附图片到当前行完成，Codex 当前回调发送后释放平台图片内容。Base64/JSON 会增加内存和传输量，Codex 发出消息仍受 max_input_bytes 约束。文件头检查不等于完整像素解码验收，业务最终交付仍需自己的图像验证；这些限制不覆盖初始业务图或 Codex 原生工具内部图片。

回放策略由 operators 的原生契约声明：verify 只用于可安全重执行、结果可比较的读取 API；recorded 在 Codex 缓存回放时直接使用当时结果，不再执行可能付费或有副作用的 API，同时重新核验已附图片对象。API 本身在会话失败后是否可安全重做仍取决于原生 journal/恢复机制，不宣称任意外部 API exactly-once。HTTP/offline 当前只接受 verify API，避免恢复模型请求时重复执行付费/有副作用的方法。未知回放模式拒绝，不自动猜测。

隔离测试覆盖配置解析、不同参数及返回类型、参数错误修复、固定参数保护、两种 runtime 的实际附图、字节/像素/附图数量预算、缓存图片损坏及取消排空。标准名称支持 read_documents、map_embeddings、search_vectors；后两者在各自原生模块维护原生函数与契约，分别复用 EmbeddingActor/VectorSearch，不需修改 agentmap 调度器。没有把 Dataset 建图方法当作行内 API 调用。


## 原生文字编码与向量检索 API

`embeddings.api.map_embeddings(text, *, model, object_directory, timeout_s)` 使用同一个原生 EmbeddingActor；单次最多 4096 字符、65536 维、一次 HTTP 请求，外部服务须预先就绪，不自动部署。每调用创建并排空有限的 actor 连接/IO 资源；并发由外层 agent 限制。原始 HTTP 请求最多 256 KiB、响应 4 MiB。返回 `embedding_ref`（URI+SHA）、encoder_id、dimensions 和调用元数据，不把高维数组交给模型抄写。向量对象保存格式 `demiflow.embedding.v1`，每对象最多 2 MiB；持久化空间最多按会话数 × 每会话回调次数 × 2 MiB 增长，SHA 相同复用。不是整个进程内存或存储余量的硬保证。

`lance.search_api.search_vectors(query_ref, top_k=4, *, uri, version, vector_column, columns, encoder_id, object_directory, contract_metadata_key, metric, max_top_k, options)` 接受上述原生对象引用。只读取已配置对象目录的内容寻址对象，核验 SHA、格式、完整 encoder_id、维度和固定 Lance schema metadata 中的契约摘要。query_ref 不是 agentmap 的 `$ref` 替换协议。固定表必填正版本，原生 VectorSearch 控制索引缓存、候选、结果字节和 Python 转换预算；阻塞调用取消时先排空再释放资源。结果为原生 projected candidates（带 _distance）及固定 source/encoder_id；图片字段由消费者明确配置。

两者均为 recorded replay：已完成 Codex 会话复用时不重发编码 HTTP 或重跑向量查询，并重新校验已交付图片；中断会话仍遵守 uncertain 保留，不声称跨中断的 exactly-once。原始模型完整 HTTP body 没有独立 journal，返回对象及调用证据保存在外层会话中；需要单独的批量编码审计仍用 Dataset.map_embeddings 的原生 journal。HTTP agent 暂不接 recorded API。

## 相同的原生 API

共享实现为：

```python
async def read_documents(request, *, context, row=None,
                         document_concurrency=2, timeout_s=30,
                         max_bytes=8 * 1024 * 1024): ...
```

`Dataset.read_documents` 把 request 列的值交给该函数；agent 调用的 `arguments.request` 是完全相同的值。`context`、执行并发、超时和文档字节上限由平台绑定，不允许模型覆盖。`row` 使用原生默认值；提示词上下文由当前行绑定的 `PromptContext` 提供。

例如以下模型参数直接传入函数，不经过 ID 解析、字段改名、默认字段补齐或业务映射：

```json
{
  "request": {
    "documents": [{
      "document_ref": {"uri": "file:///objects/document.json", "sha256": "实际对象SHA256"},
      "url": "https://example.org/source",
      "bindings": ["scope"],
      "eligible": true
    }],
    "questions": [{"id": "scope", "text": "这条结论的适用条件"}],
    "requests": [{
      "request_id": "more", "document_ref": {"uri": "file:///objects/document.json", "sha256": "实际对象SHA256"},
      "block_ids": ["b000002"], "bindings": ["scope"]
    }],
    "new_chars": 6000,
    "total_chars": 6000
  }
}
```

文档引用须使用本行 `resources` 提供的完整实际值，示例路径仅用于说明格式。`retained`、`requests` 是原生可选字段；可以省略，平台不会补写。`requests` 支持具体块及 `section_id`；`questions` 用于原生相关块选择，不要求强行变成一个 query。`new_chars/total_chars` 是模型请求的材料数量，平台验证不超过声明上限，不改写它们。原生阅读函数内部仍会在保留完整块的前提下做已有的提示词容量拟合，这是算子自身行为。

当前 agent 绑定 `CharacterBudget`，因此暴露原生 `new_chars/total_chars`；原生 `TokenBudget` 调用继续使用 `new_tokens/total_tokens`。这两个契约由同一 `read_documents_arguments(unit)` 声明。原生结果中的 `status/reason/selected/materials/readings/receipts` 原样保留。

`read_documents` API v2 增加可选 `request.selection`，Dataset 和 agent 共用：`heading_weight`（1–8，默认 1）提高标题命中权重，`include_neighbors`（默认 true）控制自动带入相邻块，`excluded_kinds`（最多 16 类，默认空）排除自动候选类型，`min_matches`（0–32，默认 0）限制最低词项重合数；某问题在该文档无命中时可用 `fallback_blocks`（0–4，默认 0）读取前几个合格块。省略此配置保持原排序。仅影响自动选段，显式 requests 和 retained 不受排除规则限制；完整原文及未读范围保持，不剪切块、不将排序分当作事实可信度。额外缓存与排序仍随当前有界文档块数、问题数增长，沿用文档字节/超时和总输入预算；不引入模型或额外文档下载。

对 read_documents，本行 `resources` 列是编号到原生文档记录的映射，默认最多 64 份（上限 256）。编号只用于展示；平台不接受或解析 `$ref` 别名。一次调用最多 4 份不重复文档、8 个 questions、8 个定位 requests、256 个 retained；这是调用资源限制，不改变参数内容。只允许读取本行预先声明的固定对象，引用、字节数及 SHA 仍由原生读取校验；不重新下载 URL。

旧 `document_ids/query` 精简接口及 `$ref` 解析已移除。旧格式会作为参数错误交回模型，不暗中转成另一套 API。HTTP 环境协议为 `demiflow.operator_environment.v4`，提示词版本和请求身份随之变化；历史日志不改写。

## 两种循环与错误反馈

HTTP／offline 的中间响应仍是 demiflow envelope：

```json
{"api_calls":[{"method":"read_documents","arguments":{"request":{}}}],"response":null}
```

上例故意遗漏原生必需字段，平台返回 `invalid_arguments` 观察值；模型在剩余轮次内修复参数。合法调用才执行原生函数。最终返回 `{"api_calls":[],"response":<完整业务 JSON>}`，再按原 schema 校验及 output/outputs 映射。格式错误、未知方法、额外字段、外部资源、预算参数越界以及无效最终业务结果都可反馈；真实传输／日志／平台故障不伪装成模型参数错误。HTTP 每次修正占用一轮和同一节点的请求预算。

Codex 使用 [app-server 动态工具回调](https://learn.chatgpt.com/docs/app-server)：初始化开启 `experimentalApi`，在 `thread/start.dynamicTools` 注入算子 schema，收到 `item/tool/call` 时校验并执行原函数，然后把结果作为工具响应返回 Codex。参数错误使用 `success=false` 和稳定错误码，Codex 可继续调用并修复；只有一个外层 `turn/start`，demiflow 不再调用模型 loop。最终直接返回原业务 JSON，不使用 HTTP envelope。app-server 接收 outputSchema，demiflow 仍独立验证最终结果；不合规则作为技术失败保留，不另外启动付费会话修复。

Codex 工具结果的外层包含原样 `result` 与平台生成的 `environment.used_operator_calls/remaining_operator_calls`。每次尝试都计数，包含参数错误；达到上限后仍可最终回答，再发一次调用则终止且不执行超额工具。平台和文档异常保留各自错误路径。原生阅读结果可能整体 ok 但有部分文档失败，模型须检查 `readings`；未读、失败或容量不足不证明事实不存在。

## Codex 设置、预算及边界

每行独立启动 stdio app-server 和 ephemeral thread，不恢复其他行会话。Codex 读取已有认证／provider 配置，模型名取 agent 配置；返回的实际模型名不一致会失败，不静默换模型。Codex 模型配置只接受 name，不接受 HTTP 地址或密钥字段；需要事先配置好 Codex 自己的 provider。使用 SQLite 回放时，provider 或运行时配置变更应显式更新 `codex_agent.model_revision`，避免把不同部署当作相同请求。

`codex_agent.web_search` 可为 `disabled`（默认）、`cached` 或 `live`。原生检索由 Codex 执行，不经过 demiflow 回调，记录原始事件和可见检索次数；能否使用取决于 Codex provider。`codex_agent.shell_tool` 为布尔值，默认 false；true 时允许 Codex 在 read-only sandbox 中自行读取文件。文件权限由 Codex sandbox 控制，不受回调的行资源白名单限制。原生读取及搜索的工具历史由 Codex 管理，不转换为 demiflow 阅读回执。

每行保留 Codex 默认基础指令，不设置 thread/start 的 baseInstructions/developerInstructions；这是复用原生运行上下文机制，不是继承当前桌面会话。启动参数关闭 subagents、apps、hooks、外部 MCP，默认关闭 view_image、image generation，使用 never approval 及临时 cwd；未识别的服务端交互请求会拒绝并终止。本实现不把未来 CLI 行为或第三方 provider 当作已验证的独占工具沙箱。当前协议核对基于本机 `codex-cli 0.155.0-alpha.16.3` 导出的实验 schema，不支持该接口的版本会明确失败。

图像任务可显式配置 `view_image: true`、`image_generation: true` 和 `shell_tool: true`。生图必须同时配置 `artifact_store: {directory: <绝对路径>}`。启用产物交付后 sandbox 为临时 cwd 的 workspace-write（排除额外 /tmp 和 TMPDIR 写范围、禁用 shell 网络）；否则保持 read-only。原生图像生成／查看的图片结果由 Codex 自己送入后续模型上下文，平台不模拟第二个图片或工具循环，也不宣称强制控制 Codex 内部图片历史。

启用产物交付时，平台按输入图片顺序把实际字节落入临时文件，追加 `CODEX_EXEC_FILE_CONTEXT`，内含 `input_images[{image_number,path,mime}]`、`artifact_directory` 和文件限额，兼容已有 Codex 任务 prompt。初始输入仍包含实际图片。临时路径不进入请求身份；请求身份包含图片内容、全部设置和 `codex-files/2` 协议。该标记沿用历史名称，不表示运行了 `codex exec`。

会话完成且进程组停止后，平台在清理临时目录之前保存 artifact_directory 内的独立文件，返回 `artifacts[{name,byte_size,object_ref:{uri,sha256}}]`。只接受单层普通文件，拒绝符号链接、硬链接、目录和特殊文件。`max_artifact_files` 默认 8，`max_artifact_bytes` 默认 64 MiB；在写对象前校验总量，最多暂存该字节预算。业务仍需校验图片解码和题目绑定。保存失败记为 `artifact_failed`，保留已完成响应，不重开付费会话。中断／超时会话不保证导出中间文件；诊断可用性以日志里实际持久化的引用为准。

共享文件传输实现位于 `operator_llm/codex_files.py`，旧 `codex_exec` 也复用同一文件校验与持久化实现。回放先按已存字节数有界读取并核验 SHA；缺失／损坏／超限属于交付失败，绝不以重新生图掩盖。

| 预算 | 默认值和实际覆盖 |
| --- | --- |
| `max_requests` | 必填；节点及 SQLite journal 的新 Codex **会话数**，不是内部模型请求数 |
| `max_operator_calls` | 每行 6 次 demiflow 回调尝试，最大 256；原生工具不计入 |
| `options.timeout_s` | 整个 session 600 秒，包含工具等待；取消时清理所属进程组 |
| `max_context_chars` | demiflow 提供的任务文字和累计算子观察 60,000 字符；不含 Codex 隐藏指令、内部历史或原生工具内容 |
| `max_material_chars` | 每次阅读算子材料 12,000 字符；模型请求只能更小，不约束原生文件读取 |
| `max_observation_chars` | 单次调用与结果 24,000 字符，超限不截断 |
| `max_response_chars` | 最终回答 24,000 字符；独立于 RPC 事件字节上限 |
| `max_document_bytes/timeout_s` | 每文档 8 MiB、读取阶段 30 秒，复用原生隔离 worker；最多 2 个并发 worker |
| `codex_agent.max_input_bytes` | 初始请求及单个发出消息 16 MiB，含图片；图片编码前预检，编码时再校验 |
| `codex_agent.max_message_bytes` | 默认单条接收 RPC 消息 4 MiB；显式设为 `null` 可取消独立单条上限，此时流缓冲仍受 `max_output_bytes` 会话总量约束，JSON 解码前检查累计字节 |
| `codex_agent.max_output_bytes/max_events` | stdout 累计 16 MiB、最多 4096 个事件，超限终止 |
| `codex_agent.max_stderr_bytes` | stderr 64 KiB，持续排空，超限终止 |
| `codex_agent.max_rss_bytes` | app-server 及其子进程合计 RSS 采样保护 2 GiB，每 0.1 秒检查；不是内核硬上限 |
| `codex_agent.max_scratch_bytes` | 临时目录 64 MiB 采样保护、最多 1024 个目录项；单文件另有 RLIMIT_FSIZE |

`codex_agent.reasoning_effort` 可选；`output_schema` 可显式提供供应商接受的 JSON schema，最终业务校验仍使用原 prompt schema，平台不改写业务结果。请求身份包括工具定义、完整原生资源、runtime、全部预算、任务版本、模型与这些设置。

进程启动器设置每进程 FD 上限 256、CPU 秒上限为会话秒数的 2 倍；线程环境默认各 2 个线程。CPU、FD、单文件限制不等于全树总量硬保证。进程数采样上限 32；RSS、磁盘与进程数均可能在采样间隔内超过阈值。临时 cwd、log_dir、sqlite_home、TMPDIR 随会话回收；认证目录及系统级 Codex 配置不属于临时目录配额。Linux 父进程退出会终止 app-server；正常完成、失败和取消清理其进程组。不能保证脱离进程组的第三方进程行为。

以上都不提供 Codex 内部模型轮次、内部 token、原生搜索次数或费用的事前硬上限；usage 和搜索计数是观测值。输入 schema／模板／图片先由原生 prompt 机制解析，已在调用方分配的行对象不在 transport 字节预算覆盖内。JSON 解码、Python 对象、原生文档索引和并发驻留另有开销；字符和序列化字节预算不证明父进程 RSS 上限。C 个并发行最多同时持有 C 份会话缓冲、工具结果和进程预算，流队列另计。历史日志总量随显式 max_requests 增长。

HTTP 默认 4 轮（含最终回答）、每轮 2 次算子调用。状态含 current_turn、remaining_turns（含当前轮）、used_operator_calls 和 limits。各轮与 HTTP 重试共用节点请求额度、准入、日志及服务生命周期；每轮前检查完整提供文字，旧观察不删除。HTTP 响应字节上限、流式事件与连接限制继续由原 transport 控制。offline 缺响应保持 pending，补交后继续原 loop。

## 日志、回放与验证

`call_output.environment` 保存 runtime、会话／模型轮次引用及原始算子观察。Codex SQLite 记录有界 RPC 事件、stderr、usage 和结果；图片 data URL 在元数据中换成摘要。相同完整请求回放不新开会话、不占新预算；存在阅读回调时先重新执行纯阅读以核验固定对象和观察结果，文档变化或损坏则拒绝缓存交付，不偷偷付费重跑。原生工具使用当时保存的会话证据，回放不重新执行命令或搜索。原生文件访问没有 demiflow 阅读回执或自动逐文件缓存核验；需要固定证据时，由业务提供不可变版本和内容身份，并在输入构造阶段验证。

异常／取消后的会话保留为 uncertain，恢复不自动重复可能已计费的执行。最终 schema 不合法的已完成响应也保留供 review，不自动新开会话。日志／IO 故障继续走技术失败路径。内置读取与自定义 fn/actor 各自声明回放策略，不宣称任意副作用工具具备 exactly-once 语义。

隔离测试见 `tests/test_codex_agent.py` 和 `tests/test_agentmap_async.py`：实际 stdio 子进程和本地 HTTP，覆盖工具参数错误后修复、原生参数不改写、原生阅读一致、提前到达的回调、跨行隔离、未授权算子、图片和原 schema、回放核验、节点／会话／字符／字节／事件／RSS 上限、超时和取消清理。未启动真实模型或真实检索；线上 provider 可用性与出题质量仍待用户允许运行后验证。

### Codex pipe disconnects

A broken pipe or reset connection during app-server transport send/read is a
`PromptResponseContractError`. With `error_output` configured, only that row
fails; the uncertain journal and partial callback observations are preserved,
and unrelated rows continue. Rerunning the same request still requires explicit
recovery and never silently starts another model session. This conversion is
limited to the transport: ordinary filesystem and journal `OSError`s retain
their fatal storage boundary. A subprocess regression covers a disconnect while
replying to a callback alongside a successful concurrent row, then verifies
replay does not launch either session again.
