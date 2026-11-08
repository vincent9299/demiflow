# 模型服务：配置、并发、质量验收

本功能把模型服务作为 demiflow 的内置部署能力；vLLM/PyTorch 等重依赖放在
独立环境中。使用同一份完整 prompt/agent 配置，业务行函数不负责安装、启动
或调用另一套执行器。已有外部 URL 和 `service=VLLMService(...)` 等入口保留。

GPU 复验发现 pplx 原生合批、JEV 原生合批及内部 continuous batching 会改变
部分输出，生产合批继续关闭。JEV 每副本默认并发接收、串行执行；
多个独立副本可真正并行，不绕过质量门槛。
结果与适用范围见 [GPU review](model-services-gpu-review-20261031.md)。
后续原生对照与独立副本并行见 [根因与并行验证](model-services-rootcause-20261031.md)。
单卡 CUDA stream/Graph 实验与 JEV 流水优化见 [单卡性能复核](model-services-single-gpu-20261031.md)。
已有解释器已用于实测；全新环境下载安装配方仍未端到端验收。

## 最小使用方式

安装部署辅助依赖：`pip install 'demiflow[services]'`。该 extra 提供 HTTP 客户端
与 uv，不把 vLLM/Torch 装进业务 Python 环境。

在原有完整 YAML 的 `model` 中声明部署，例如：

```yaml
schema_version: demiflow_prompt_pack_v2
prompts:
  score:
    version: v1
    model:
      name: pplx-decider-v1.2
      deployment:
        profile: pplx
        weights: /models/pplx-decider-v1.2
        revision: weights-v1
        gpus: [0]
        lifetime: action
    schema_retries: 0
    systemone:
      type: noul
      instructions: 判断文本是否符合给定条件。
    response_schema:
      type: object
      additionalProperties: false
      required: [result]
      properties:
        result:
          type: object
          additionalProperties: false
          required: [type, noul]
          properties:
            type: {type: string, enum: [noul]}
            noul: {type: number, minimum: 0, maximum: 1}
    template: '{{ text }}'
```

权重使用本地绝对路径；更换权重必须更新 `revision`。GPU 是显式配置，不自动
选择空闲卡。`python: /absolute/model-env/bin/python` 可接入已有环境：平台只读
检查它，不安装或修改其中的包。d3 首版要求此项。

```sh
demiflow services prepare --config prompt.yaml
demiflow services doctor --config prompt.yaml
```

`prepare` 显式创建/复用隔离环境并登记端口；不加载模型。`doctor` 检查环境
元数据，不能替代 GPU 兼容性和质量验收。解析 YAML、构造 Dataset 或命中
journal 都不会安装或启动模型。未 prepare 的缓存未命中请求给出准备命令。

原算子用法保持不变：

```python
from demiflow import data
from demiflow.operator_llm import load_prompt_pack

rows = data.from_items([{'text': '待判断的文本'}])
result = rows.map_prompt_async(
    'score', config=load_prompt_pack('prompt.yaml'),
    inputs={'text': 'text'}, output='answer', concurrency=8,
)
```

`concurrency=8` 保留八行并发执行能力，不要求开启显式合批。`action` 部署在
同一事件循环内按部署身份共享，最后一份 lease 释放后关闭。多进程/多次 action
需要共享模型时使用 `persistent`；平台不会让各进程绕过 GPU 资源锁重复加载。
托管声明用于 `map_prompt_async` 和异步 agent；同步逐行 `map_prompt` 明确
拒绝托管部署，避免每行重新创建事件循环、加载和卸载模型。

需要两个真正同时执行的模型副本时，在同一份配置中声明：

```yaml
deployment:
  profile: pplx  # 或 autotrust-jev / amos / d3 / vllm
  weights: /models/pplx-decider-v1.2
  revision: weights-v1
  gpus: [0, 1]
  replicas: 2
  lifetime: action
```

保持算子 `concurrency >= 2`，不要开启 `systemone_batch`。每张卡加载完整模型，
每个副本一次处理一个独立 HTTP 请求；算子通过 service lease 选择空闲端点，
没有额外 HTTP 代理。两个 GPU 可以同时 forward，显存也按两个副本计算。
`replicas` 为 1–16，GPU 列表按连续分组平均分配；pplx/amos/d3 每副本一张卡。
vLLM/JEV 可显式设置每副本 TP，原生 DP 与这里的副本机制不能叠加。
省略 `replicas` 保持原先语义，不会把已有多 GPU 的 TP 部署自动改成多个副本。

`request_concurrency` 至少等于副本数；复制部署内部每个实例只准入一次调用，
其余等待受算子并发约束。端点只在本次请求租约里返回，不改全局 base_url，
不改变行请求键；副本数与设备位置不是业务语义。HTTP 错误不触发隐式跨副本重试。
副本与端口一同 prepare，`start/status/logs/stop` 管理所有组件，支持 persistent。
跨进程共享使用原有文件请求锁；空闲端点选择在调用进程内完成，不是集群负载均衡器。
副本配置当前只支持独立单条；批量证据绑定具体设备，不能将一份证据扩张到多个副本。

```sh
# 仅 lifetime: persistent；会加载模型，维护窗口内明确执行
demiflow services start --config prompt.yaml
demiflow services status --config prompt.yaml
demiflow services logs --config prompt.yaml --bytes 65536
demiflow services stop --config prompt.yaml
```

有 Dataset 使用者时 stop 拒绝停止。组合部署先检查所有组件，再按相反依赖
顺序停止。`status` 对 action 服务明确说明由活跃 runtime 持有，不冒充跨进程
运行状态。外部 URL 不归平台所有，平台不会停止该服务。

## 四个本地模型的统一性能入口

四个 profile 均支持托管部署和 `map_prompt_async`。新部署可统一加入
`deployment.performance: optimized`，选择各后端经过验证的执行配方：

| 本地模型 | profile | optimized 的执行路径 | 单卡独立 forward |
|---|---|---|---|
| pplx-decider-v1.2 | `pplx` | 原生读出 + 有界 CUDA Graph | 默认 2 条 stream |
| amos-1-large | `amos` | 原生 decoder 图 + 原生 FP32 读出/校准 | 默认 2 条 stream |
| d3-turbo | `d3` | 原生 state-once 分阶段图 + 原生读出 | 默认 2 条 stream |
| AutoTrust-JEV-27B-v1.1（文本） | `autotrust-jev` | 进程内原生 vLLM + 独立状态的有界 CUDA Graph | 默认 2 条 stream；冷路径原生串行 |

```yaml
deployment:
  profile: amos                 # 可换为上表中的 profile
  weights: /models/amos-1-large
  python: /models/.decision-env/bin/python
  revision: immutable-weights-v1
  gpus: [0]
  performance: optimized
  request_concurrency: 4
```

仍通过同一套 prepare/doctor 和 action/persistent 生命周期启动，算子配置无需
写模型专属优化。四种模型均以 `options.cuda_graphs` 细调下述公共预算。
JEV 使用一个托管 worker，在进程内持有原生 vLLM 和一份权重，复用同一
BoundedExecution/GraphCache；模型内部状态与版本限定在专用后端。
优化不跨请求合批，batch 继续关闭；不更改权重、精度或校准。显式指定矛盾参数
会拒绝配置。省略 performance（或设 `native`）维持已有行为，已有运行服务不会
自动切换；native 预设不覆盖另行显式指定的 `options.cuda_graphs`。

四种图模式当前只支持单副本；多 GPU 独立副本仍可使用原生配置。
同一模型不同输入长度不必人工登记：实际形状按热度和预算自动捕获。冷启动、
长尾长度和预算外形状仍可能使用原生执行，不能保证任意流量都变快或 GPU 始终
100%。具体实现、质量覆盖和实测限制见
[统一性能设计](four-model-performance-design.md) 与
[四模型验收记录](four-model-performance-review-20261031.md)；JEV 正式并行接入及
后续公共图池修复见 [JEV 后端验收](jev-runtime-review-20261101.md)。

## 部署时自动调优

四个上述 profile 可以将 `deployment.performance` 设为 `auto`。业务继续使用
同一份完整 YAML 和 `map_prompt_async`；部署阶段执行：

```bash
demiflow services prepare --config prompt.yaml
demiflow services tune --config prompt.yaml --cases real-requests.jsonl \
  --objective throughput --max-p95-ms 2000 --timeout-seconds 1800
demiflow services doctor --config prompt.yaml
```

当前自动调优只支持单副本部署；`replicas > 1` 明确拒绝，多副本的设备分配与
扩缩容不参与本轮参数搜索。

`real-requests.jsonl` 每行是完整 SystemOne 请求，包含原始 `state`、`questions`
及可选 `model`，至少 400 个不同请求。应覆盖真实输入长度、问题类型和选项数量；
平台不会替换业务内容，也不会靠重复几个形状证明质量。文件最多 2,000 条、16 MiB，
每行最多 64 KiB；超限明确拒绝。

调优只在空闲且显式指定的 GPU 上启动本次拥有的临时服务，结束或取消时清理。
它先建立禁用图和候选内核的原生参考，再在后端允许的宽度内搜索 streams、在途数、CPU 线程、
图缓存容量和捕获热度。默认最多 13 个候选、1,800 秒，每个候选预热两轮并测量
两轮；未完成的搜索不会被冒充成全局最优。质量要求完整 `model/answers/usage`
精确一致，不调低精度、不截断输入、不减少校验频率、不打开跨请求 batch。
最终配置必须在另一进程上通过独立留出集，才原子发布。
当前 pplx / D3 / JEV 允许 1–4 streams；AMOS 只允许 1–2，因为当前运行时的
3/4-stream 已复现非法显存访问。已知不安全配置不能靠一次测试通过重新放行，
其档案即使填写通过指标也会被拒绝；恢复更宽并行需要先修复并重新验证后端。

目标是满足 p95 上限的稳定 HTTP 吞吐；每个候选取多轮最低吞吐，距离最佳 2%
以内优先较短尾延迟，再比较在途数和 streams。`--objective latency` 优先尾延迟。
HTTP 指标包含编码、网络及完整响应，不包含任意业务 Dataset 前后处理。
上游算子仍需提供足够请求；调优不扩大业务行数、重试预算或并发。

结果保存在部署目录的 `tuning.json`，包含原生参考、候选、留出验收及资源证据。
绑定权重、适配器源码、运行时、声明、GPU/driver、CPU 可用核、相关执行环境和样本摘要；
同样的调优命令可直接复用有效结果，`--force` 显式重测。失败保留之前已发布档案。
缺失、损坏或过期时 auto 部署拒绝启动，并提示重新调优；不会在业务首行偷偷跑基准。
不会热切换活动服务，已经完成的 journal 仍可离线重放。

执行档案独立于逻辑模型身份，因此对应的 optimized/auto 声明和不同调优结果可
复用原有逐行 journal。原 `native`、`optimized` 配置不自动应用档案；从无图 native
切换到 auto 仍可能改变原有模型声明身份，不能承诺任意旧配置都直接复用。

资源上限沿用下述图后端约束；调优的 Torch 峰值也必须在预算内。它不是驱动和所有
第三方显存分配的硬上限。响应每条最多 256 KiB、每阶段累计最多 32 MiB；只保存
摘要，单份报告最多 1 MiB。日志每服务最多 8 MiB，历史最多 16 次调优；历史超过
256 MiB 或磁盘不足 512 MiB 时拒绝新实验，不自动删除旧证据。

新模型可以复用调优器、准入、生命周期和缓存；仍需接入支持独立请求的 backend，
提供原生参考并验证状态隔离及数值一致性。当前普通 `vllm` profile 尚无统一任务
质量配方，明确拒绝 auto。不同流量分布需要重新提供代表性样本并调优；有限样本
不等于所有未来输入均已验证。细节见 [自动调优设计](model-autotune-design-20261103.md)。

多 stream worker 会在进程内保护 Triton 自动调参器已知的共享参数提交状态，
不修改外部安装包。锁按内核实例分配，异步提交返回即释放；独立 GPU 请求和图重放
继续并行。CPU 回归及 GPU 质量/性能验收见 [本轮复核](model-autotune-review-20261103.md)。

## 运行时与质量边界

| profile | 实际推理 | 显式跨请求合批 |
|---|---|---|
| `vllm` | 原生 OpenAI-compatible 服务与内部调度 | 不增加客户端 SystemOne 合批 |
| `autotrust-jev` | 原生格式器、校准读出、vLLM 双向 attention 插件 | 候选 prompt 列表，必须通过质量验收 |
| `pplx` | 原生 `decide`、执行锁和原有精度 | 候选 `decide(list)`，必须通过质量验收 |
| `amos` | 原生 `load_engine`、校准与 state cache | 关闭；当前 DecoderEngine 没有原生跨请求列表接口 |
| `d3` | 原生 D3/Decider，由统一有界 HTTP worker 托管 | 关闭；其内部 question batch 不代表跨请求 batch |

JEV 不拼接不同请求的上下文。按兼容的选项数分组，独立 prompt 交给 vLLM，
校验响应索引再回填。单条路径保留字符串 prompt；只在 batch 路径发送列表。
禁止 prefix caching/chunked prefill，保留原生插件、bf16/fp16 和校准参数。
不为追求速度默认启用量化、fast path 或改变问题内容。
原生 HTTP 路径未提供通过的质量证据时，JEV wrapper 默认在整个单条请求上持有执行锁；
显式验收模式或匹配的通过证据才允许引擎跨请求合批。并发接口与有限排队仍保留，
但不能把它描述为多个同时执行的 forward。`replicas: 2` 才会建立两个独立执行
实例，单副本执行锁不会挡住另一个副本。

原生 HTTP 路径的单卡 JEV 可在原有部署配置里显式设置：

```yaml
request_concurrency: 2
options:
  request_pipelining: true
```

此时 wrapper 保证至多一个后端请求在途，同时可以重叠其他请求的 CPU 准备
和结果读出，减少 GPU 等待下一次请求的空隙。能力声明为 `parallel: 1`、
`request_pipelining: true`、`continuous_batching: false`、`batch.supported: false`。
它属于 CPU/HTTP 与 GPU 的流水重叠，不是多个 forward 并行，也不需要 batch 证据。
该模式不能与 batch 验收或 batch 证据同时配置；后端仍由统一 profile 托管，
不能仅给任意外部 URL 声明“单序列”便绕过保护。算子通常配 `concurrency=2`，
继续增大并发可能只增加排队与 p95 延迟。本轮只验证现有 JEV 文本模型。
设置改变 model options，需新的部署 prepare；旧有 journal 不会被改写。
显式 `max_num_seqs: 1` 的旧配置仍支持流水线，但不是统一预设。JEV 曾出现
跨启动原生基线差异；容量 64/1 的后续串行配对完全一致，不能归因于容量参数。
不能仅凭某个配置内的串行/并发一致就宣称所有启动与引擎配置均无损。

JEV 的 `performance: optimized` 则支持单卡独立图重放，不使用上述 HTTP
流水线开关。只开放已验证的 SM90、bf16、单 GPU 和固定版本组合：vLLM 0.31.0、
Torch 2.13.0 的已审计构建、StartLux 1.1.0、FlashInfer 0.7.0.post1、CUTLASS DSL 4.7.1。
未知构建拒绝启动，可显式选择 native。进程内修复经过源码哈希确认的 FlashInfer
SM90 共享 scratch 缺陷，不修改安装目录；各 lane 独占 KV/GDN state 和图工作区。
原生调用与捕获独占全局 ForwardContext，通过精确对照的图才在固定 stream 上
并行重放。短输入保留原生 padding 布局；不同请求不拼接、不合批。
质量失败返回本次原生结果，禁用图并将实际 `parallel` 降为 1。
设计、GPU 验收及历史数值漂移的证据范围见 [JEV 后端设计](jev-runtime-design-20261101.md)。

pplx、AMOS、d3 单卡也可直接通过原有 deployment 显式配置独立 CUDA stream 和精确形状图缓存：

```yaml
deployment:
  profile: pplx
  weights: /absolute/path/pplx-decider-v1.2
  revision: your-immutable-revision
  gpus: [1]
  request_concurrency: 4
  options:
    cuda_graphs: {}
```

`cuda_graphs: {}` 使用下列有限默认值。原有配置不自动开启；仍使用同一套
`services prepare`、生命周期和 `map_prompt_async`，算子通常从 `concurrency=2`
开始。现有解释器可用 `deployment.python` 只读接入。此模式只允许一个副本，
禁止同时配置 batch 证据、batch 验收或 `systemone_batch`。

| 选项 | 默认值 | 含义 |
|---|---:|---|
| `streams` | 2 | 单份权重、独立可变状态与 CUDA stream；独立单条执行 |
| `max_entries` | 32 | 全部 stream 合计的图缓存条目上限 |
| `cache_memory_mib` | 4096 | 图私有池 reserved 字节与静态输入的合计上限 |
| `memory_limit_mib` | 65536；JEV 75776 | 整个 worker 的 Torch allocator 上限，含权重和临时工作区 |
| `max_input_tokens` | 8192 | 原生请求 token 预算；pplx 按 chunk 合计，AMOS 按编码长度，d3 按 state-once 的 state 加各 question block，JEV 按各问题完整 prompt 合计；超限拒绝 |
| `max_graph_tokens` | 4096 | 单次捕获阶段的 token 上限，超过则使用原生路径 |
| `min_observations` | 8 | 同一 lane 的同一图 key 重复出现后才捕获；不使用固定长度白名单 |
| `max_captures` | 128 | 进程生命周期内捕获次数上限，包括重建 |
| `capture_budget_s` | 60 | 累计捕获耗时准入预算；当前一次捕获可越过剩余时间 |
| `verify_every` | 64 | 每张图首次精确对照，此后按重放次数周期精确抽查 |

图不补齐长度、不合并请求，不改精度和原生校准。图片、原生多 chunk 批次和
较长输入保持原生计算；图片以及 pplx 多 chunk、超图长度输入使用独占 GPU
保护，其余文本在工作区预算允许时可并行。每张图保留全部输入和
独立工作区；捕获、淘汰期间会排空其他 GPU 工作。pplx/AMOS/d3 的捕获前原生
参考与周期检查使用各自 lane，可以和其他 lane 重叠；JEV 的原生全局状态仍受
独占保护。质量不一致时返回本次
原生结果、清空缓存并停用图加速；pplx/AMOS/d3 的独立原生 lane 继续并行，
JEV 回到原生串行执行并更新能力声明。
`verify_every: 1` 可逐次对照，性能成本也更高。
CUDA 异常使实例报告失败，不能将未知错误当作可安全重试的图未命中。

缓存按实际 `(lane, shape)` 图副本统计需求并保留高频条目，相同热度按 LRU
淘汰；新条目需比待淘汰条目更常见。一个 lane 的请求不会提高另一个 lane
副本的热度；同一形状仍可在多个 lane 上各自捕获、独立并行。
缓存不覆盖无限种长度。冷启动捕获会增加延迟；预算耗尽后不再新建图，已有图
继续服务，未命中走原生。热度表最多 `4 × max_entries` 条。编码前还限制 JSON
大小为 `max_input_tokens × 128` 字节、按问题数估算的展开量为 16 MiB；这只是
编码输入预算，不能当作 Python RSS 硬上限。原生图片解码仍受既有像素上限保护。
状态中的 `observations` 是近期图 key 的数量，不是不同输入长度的数量。

`memory_limit_mib` 必须为设备保留至少 2048 MiB 余量；它约束 Torch 分配器，
不包含驱动和所有第三方分配，不能称为整个进程的显存硬上限。缓存使用独立
预留及实际计费；按形状估算工作区后背压，过大的单请求明确拒绝。
`GET /v1/models` 的 `capabilities.execution` 提供命中、捕获、淘汰、校验、
缓存字节和 allocator 当前/峰值；`parallel`、`batch=false` 如实声明。
GPU lane 按到达准入点的顺序分配，避免新请求反复抢占旧请求；大请求等待工作区时
也可能挡住后续小请求。等待者仍受服务的 `max_inflight` 约束，不增加后台请求队列。

缓存策略的 JEV 端到端对照、预热成本及适用范围见
[吞吐 review](jev-throughput-review-20261101.md)。公共机制可供后续 backend 复用，
模型执行、状态隔离和质量验证仍需由对应 backend 实现；不保证所有模型得到相同收益。
`queue_wait_seconds` 和 `queue_wait_max_seconds` 分别记录成功准入请求的进程累计与
最长 lane 等待时间，不包含前面的编码、HTTP 排队或获得 lane 后的捕获等待。
静态输入只额外预热一次；捕获前原生对照、首次/周期精确校验保持不变。
本轮尾延迟、JEV 数值实现定位和未通过的并行实验见
[运行时复核](model-runtime-latency-review-20261101.md)。
方案、生命周期修复及实测限制见 [CUDA Graph 设计](pplx-cuda-graphs-design.md)
和 [验收记录](pplx-cuda-graphs-review-20261101.md)。AMOS/d3 复用同一资源控制、
固定线程和图缓存；AMOS 保留原生 decoder/FP32 读出，d3 保留 state-once 与
原生问题组批。d3 仅把图内不允许的 CPU→GPU 索引创建提前，首轮/周期检查
仍对照未改动的原生 questions 方法。缓存图输出在返回上层前复制到普通分配器，
图淘汰不会悬空原生后处理所用的张量。d3 的跨层 recurrent state 和卷积切片
保留的完整存储也计入预算；工作区估算可能在 token 上限之前拒绝大请求。

pplx 保留其原生上下文上限，不偷偷缩短；batch 前逐条使用原生 encoder 验证，
防止一个编码错误污染其他请求。这会增加编码开销，是否满足目标加速比必须实测。
amos 新 profile 使用原生 strict 输入模式，超限明确失败，不截断文本。

`parallel` 是真实独立执行能力，`max_inflight` 是 HTTP 准入，Dataset
`concurrency` 是行并发。三者不是同一概念。vLLM 的 continuous batching
单独标记，不能写成无限 parallel。`parallel × batch_size` 不是吞吐公式。

## 验收并启用合批

默认 batch 关闭。先准备至少 200 条真实业务请求的 JSONL，每行是
`{"state": ..., "questions": {...}, "images": [...]}`，images 可省略。
样本需由 reviewer 检查覆盖：长短输入、不同选项数、临界概率、真实分布，
支持图片的模型还需图片。工具记录样本摘要，不能替 reviewer 判断代表性。

维护窗口内，对 `lifetime: action`、无旧 `batch_evidence` 的配置执行：

```sh
demiflow services validate --config prompt.yaml \
  --cases cases-200.jsonl --report quality.json \
  --batch-size 8 --parallel 8 --probability-atol 0.000001
```

工具显式启动该部署，完成预热、串行单条、并发单条，并对从 1 到最大值的每个
批大小各跑三轮随机伙伴/位置对比，然后关闭。至少 200 条不同输入，完整核对
请求/响应问题和选项键，避免一致漏答也通过。报告给出 labels、最大概率误差、吞吐、每行完成延迟
p50/p95 和本部署进程树的 GPU 显存采样峰值。显存不可测时为 null，不记零。
采样有 0.5 秒间隔，不能声称捕获所有瞬时峰值；缓存驻留会影响不同阶段读数。
服务层基准不含 Dataset/journal 时间，仍需另外测端到端管线。

标签必须一致，概率容差在运行前给定且不超过 1e-3。报告绑定模型 revision、
profile 参数、模型文件签名、关键运行时版本、适配器源码摘要、推理环境变量和 GPU/驱动身份。fake CPU
报告不能认证真实模型。通过说明这些样本和运行条件通过，不是所有输入数学上
完全相等的证明。GPU、依赖、适配器或模型变化后需要重新验收。

review 通过后在部署声明中增加：

```yaml
batch_evidence: /absolute/quality.json
batch_max_size: 8
```

重新 prepare 此部署，然后给原算子指定有界策略：

```python
options = {
    'systemone_batch': {
        'max_size': 8, 'wait_ms': 5, 'parallel': 1,
        'max_queue_rows': 64,
        'max_queue_bytes': 16 * 1024 * 1024,
        'max_batch_bytes': 4 * 1024 * 1024,
    },
}
```

这里 `parallel` 是**批量 HTTP 调用的并发上限**，不声明模型 forward 并行度。
`max_size` 不得超过服务已验证上限。外部 SystemOne 服务还需在执行 options
声明 `model_revision`，并提供同等能力与证据协议；只有 API 支持列表不够。
服务启动重新检查证据，客户端在首批前核对能力和 revision，每批核对证据摘要。
不满足条件就报错，不静默发送未验证 batch。
部署的 `request_concurrency` 也不能超过报告中测试过的 `parallel`；增加并发
需要重测，不能只验证小并发再按大并发使用。
v2 证据还记录输入模态、问题类型、选项数量与问题组合；未覆盖的组合明确拒绝，
不能用“测过文本 choice 和图片 noul”推导“图片 choice 已通过”。旧 v1 证据不接受。
模型大权重采用路径/大小/时间签名，小型元数据另加内容哈希；这是常规变更检测，
不是对恶意保留时间戳改写权重的密码学保证。权重 revision 仍要求不可变。

## journal、失败与资源

每行先复用原有预算/持久预留，再进入传输队列；批次 ID 只放在响应元数据中，
不参与行请求身份。单条与 batch 可复用同一行 journal。新部署使用逻辑端点，
物理端口改变不改变模型身份；旧 URL 配置的请求键不变。choice 的选项顺序
参与身份，noul 和原有图片 state 保持兼容。

完整逐行 HTTP 错误由原有有限策略重试，重试只发该行的单条请求。数量不符、
证据变更、断连、截断或超时不自动重发。取消队列内未派发行会留下明确元数据；
已预留的请求额度仍保守计数，不能假称只统计已完成推理。已派发请求取消后，
其他同行结果继续回填；原有 uncertain 恢复规则保留。

资源上限包括：队列行数/字节、批大小/字节、有限 HTTP 并发、连接线程数、
16 MiB 服务请求/响应、最多 128 questions/请求、255 choices/question，
每张图片 1600 万像素/每请求总计 3200 万像素。pplx/JEV 显式批量编码总量
限制为 262144 tokens，JEV 每次最多 512 questions；超限失败，不截断。
编码字节预算不是 RSS/GPU 显存硬上限，仍需实际模型预算与压测。

服务日志默认两个文件，每个最多 8 MiB；安装输出最多 1 MiB。准备环境监测
16 GiB/20 万文件的预算，下载临时文件归入该目录；这是周期监测而非文件系统
硬配额。准备记录最多 256 个，锁等待/子进程/HTTP 都有有限超时。

GPU 协作锁在同宿主机、同用户各项目间共享，并保留声明目录中的旧锁。
已运行的旧版本进程不会被自动迁移到新锁域，外部手工进程也不受协作锁管理。
当前维护窗口约束仍必须遵守，不能凭锁可获取就认定 GPU 可用。

设计和实现自审记录见 [model-services-design.md](model-services-design.md)。

## 单条与并发性能基准

四个 SystemOne profile 可用同一命令；配置可以是原生，也可以开启 optimized：

```sh
demiflow services prepare --config prompt.yaml
demiflow services benchmark --config prompt.yaml --cases cases-200.jsonl \
  --parallel 2 --report performance.json
```

至少 200 条不同业务输入，报告单条/并发单条的吞吐、p50/p95、显存采样峰值、
概率和 usage 差异。使用 action 单副本部署，测试后关闭本次创建的服务；不接管
既有外部服务。该报告是 `demiflow_single_performance_v1`，不能开启 batch。
它比较同一部署配置下的执行；原生与 optimized 的跨配置对照仍需另跑并比较。
图捕获和缓存热度会影响测量，短基准不能当作稳定态承诺，混合/长尾/高频输入
应分别报告。GPU 利用率不能代替实际吞吐。

原生 Torch 阶段的额外回归可在模型环境执行
`benchmarks/native_cuda_graphs.py --profile amos --gpu 0 --model /weights \
--cases reviewed.jsonl --output /tmp/graphs.json`。`--streams 1 --cold-shapes`
逐条清空缓存检查新形状的捕获和回退，报告每条实际捕获数量；该脚本也支持
pplx 和 d3。它检查 GPU 空闲及资源锁，输入与输出规模有界，不修改权重或环境。
# Configurable native compilation and service batching

Service administration can use a standalone `demiflow_model_service_v1` document
with exactly `schema_version` and `model` (the existing model/deployment schema).
No business prompt or agent task is required. Existing prompt-pack and agent
declarations remain supported by the same CLI.

The `pplx` profile accepts `options.pplx_fast` for the installed model package's
native `FastOptions`. It preserves native text encoding, question boundaries,
BF16 weights, calibration and response attribution. `compile` (`layer` or null),
`cuda_graphs`, ascending `buckets`, `token_budget`, `max_input_tokens`,
`memory_limit_mib`, `prune_last` and `prefetch` are deployment options. FP8 is not
enabled by this recipe. It cannot be combined with another performance recipe.

`options.batching` independently configures service-side aggregation of ordinary
`/v1/systemone` calls: `max_size`, `wait_ms`, `max_pending_rows`,
`max_pending_bytes`, `max_batch_bytes` and `max_request_bytes`. Nothing about
application categories, prompts, thresholds, recovery or datasets belongs here.
Clients keep their single-request protocol; their declared concurrency must be
large enough to fill the desired service batch. There is no automatic increase
of client concurrency. Initially the verified native batching backend is pplx;
other adapters must qualify before the same queue can be enabled for them.

The queue holds at most the configured pending rows and encoded bytes, including
the executing group. HTTP admission bounds decoded callers outside that queue;
encoded bytes are not a Python RSS measurement. Full capacity returns 503;
oversized single bodies return 413, and invalid individual inputs return 422
without discarding valid partners. Queue state is entirely in memory. Close
drains admitted work, then closes the backend. Forced termination can lose the
bounded in-flight work; pipeline checkpoint recovery remains independent.

The pplx backend serializes encoding and GPU execution onto one persistent
thread because native packed execution has mutable process-global state.
At most 64 requests enter an explicit batch, packs obey `token_budget`, and each
request obeys `max_input_tokens`. Longer-than-pack-budget requests run alone via
the native non-static path; inputs are never truncated. Only text is supported
by this recipe. Torch's allocator cap leaves device headroom but does not bound
non-Torch allocations or host RSS. Native compile cache limits and at most eight
declared static buckets bound specialization policy; first use of a new shape
may compile. Warmup/readiness is not a claim that every possible shape is warm.

Production batching requires identity-bound `batch_evidence` and
`batch_max_size`, matching the configured queue size. Without evidence it is
available only in explicit validation mode. `services validate` still checks
at least 200 distinct cases, concurrent calls, every permitted batch size and
three partner orders. Legacy v2 evidence retains its original strict tolerance.
For numerical variants, `--quality-policy FILE` explicitly declares a JSON
`{"probability_atol": ..., "label_margin": ...}` and produces v3 evidence.
All probabilities must meet the declared tolerance. Changed labels are allowed
only when the competing choices are within `label_margin` in both distributions;
raw label changes remain in the report. This policy is model-service evidence,
not application accuracy or approval to change a consumer's decision threshold.

Execution identities include these options, adapter code and the installed
`pplx_decider/fast.py`. Reprepare after changes. `/v1/models` exposes effective
limits, pending rows/bytes, batch-size counts and backend execution statistics.
