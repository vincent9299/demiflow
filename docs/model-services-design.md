# 统一模型服务：设计、Review 与实施记录

日期：2026-10-31。状态：完成第二轮实现 review，GPU 实测发现候选合批不通过；
生产合批保持关闭。当前结果见 [GPU review](model-services-gpu-review-20261031.md)。
后续继续完成 [原生根因对照与副本并行](model-services-rootcause-20261031.md)，
独立副本并行不依赖跨请求合批。
单卡后续结果见 [单卡性能复核](model-services-single-gpu-20261031.md)：JEV 可显式
将引擎限制为单序列并重叠请求准备；pplx CUDA stream/Graph 仍为隔离实验原型。

具体配置与命令见 [使用说明](model-services.md)。本文保留设计决策和实施 review，
不将 CPU 契约通过写成真实模型质量或性能通过。

本方案依据本轮用户确认，覆盖原 `systemone_service_adapters_handoff.md` 中与
无限并发、默认合批、超时自动重发、全局保序及直接修改共享环境相冲突的建议。
目标是统一架构、简化使用、保持真实并发，并在质量约束内提高端到端吞吐。
首轮仅执行无 GPU 验证；后续用户明确授权 GPU 0/1，第二轮使用测试自有实例。
GPU 2/3、其他任务服务、生产数据和历史 journal 不属于本次运行范围。
不 commit/push，不修改已安装模型包。

## 1. 不可破坏的契约

1. YAML 解析、对象构造和 `bind` 无安装、下载、网络、进程及 GPU 副作用。
2. 命中 journal、空输入、跳过行、预算耗尽不启动模型。
3. 每行保留预算、请求身份、输出与恢复证据。批次只属于传输元数据。
4. 完整错误响应可按显式有限策略重试；超时、断连、截断仍是 uncertain。
5. `noul`、图片 state、旧请求键、现有 Dataset 顺序契约保持兼容。
6. 并发和显式 batch 独立。排队容量不等于 forward 并行度或吞吐。
7. 真实模型批量默认关闭；模型原生接口存在不等于质量验收通过。
8. 管理器只停止自己拥有的进程；关闭一个使用者不能停止其他使用者。
9. 有界约束覆盖准备、排队、请求、响应、模型输入、持久化和慢消费者。
10. 缺少模型/GPU 实测时明确记录 pending，不伪造能力或性能认证。

## 2. 架构与模块边界

| 部分 | 输入/输出 | 责任 |
|---|---|---|
| 配置解析 | 完整 prompt/agent 配置 → 不可变模型/部署声明 | 静态校验、唯一配置来源 |
| 环境准备 | profile + 固定依赖 → 环境 manifest | 显式安装、缓存、兼容检查、失败清理 |
| 部署 profile | 声明 + 已准备环境 → 启动计划 | 引擎参数、模型族协议、组件依赖 |
| 服务核心 | 启动计划 → 服务 lease | 资源、进程、健康、排空和关闭 |
| 算子 runtime | 行请求 → 行结果 | 预算、journal、并发、可选有界合批 |
| 服务适配器 | SystemOne 请求 → 模型输入/输出 | 原生推理、逐条输入验证、隔离与回填 |

`ManagedHTTPService` 持有唯一 HTTP 进程 owner 实现。`VLLMService` 保留公开
构造器与参数校验，将启动计划交给该核心，不再维护第二套进程生命周期。
`SharedHTTPService` 与 supervisor 复用同一个核心。prompt/embeddings 使用
结构化 service/lease 协议，不硬编码具体 Python 类名单。

服务管理不转发推理载荷。算子直接连接已解析的端点，保持共享 HTTP 连接池。
引擎内部的 token 调度、KV 管理和计算内核由原生运行时负责。
复制部署仅增加有界空闲副本选择；`request_slot()` 可返回本次调用的物理 URL，
逐层传给现有客户端。ReplicaService 复用 group lease 的启动/回滚/释放逻辑，
不维护另一套生命周期，不新增 HTTP 转发服务，也不承接行调度或 journal。
一副本一在途调用，GPU 实例之间独立执行；关闭唤醒等待者，失败不隐式换副本重发。

## 3. 用户配置与兼容

保持 `demiflow_prompt_pack_v2` 完整内联配置；新增可选 `model.deployment`。
旧 `model.transport/base_url/base_url_env`、Python `service=` 继续有效。
声明部署与显式 `service=` 同时出现时拒绝冲突，不能悄悄覆盖。

模型片段：

```yaml
model:
  name: AutoTrust-JEV-27B-v1.1
  deployment:
    profile: autotrust-jev
    weights: /models/AutoTrust-JEV-27B-v1.1
    revision: local-weights-v1
    gpus: [0, 1]
    lifetime: action
```

profile 推导协议和必要参数。`revision` 是用户对本地权重版本的显式声明；更换
权重必须更新。已有远程模型继续声明 URL，平台不拥有其进程。
本地模型文件路径在准备/启动时验证，不在解析阶段扫描权重。

新托管配置使用逻辑模型身份，物理端口在运行期解析。逻辑身份包括模型名、
权重 revision、profile/适配版本及影响结果的配置；GPU、端口、排队大小等
部署属性不作为业务请求身份。旧配置保留旧哈希，不迁移或重写旧 journal。
环境锁记录具体依赖版本；更新环境需重新验证与更新模型执行版本。

## 4. 环境与部署

`services prepare --config ...` 显式准备；`doctor` 只检查并输出诊断；
`status/logs/stop` 统一管理已登记服务。首次缺环境报出准确的 prepare 命令，
不在普通行调用期间安装。解释器可显式指向已有环境，只读验证而不修改。

安装环境按 profile/依赖/Python 平台的指纹隔离。并发 prepare 共享文件锁；
完整构建和元数据/关键源码检查成功后发布 manifest；不在 prepare 导入模型库。
真实加载留给显式启动及 GPU 验收。失败的构建不能作为可复用环境。
下载、子进程时间、输出日志和磁盘增长需要明确限制；不自动安装 GPU 驱动。
权重下载和任意模型远程代码不隐式启用。首版使用本地权重和已声明依赖来源。

服务侧轻量 runner 不导入 demiflow 的 Dataset/Arrow 依赖；模型库只在 runner
进程中加载。现有 `models/decision-deploy` 的 live wrapper 本轮不切换、不重启。

生命周期：

* `action`：按执行范围共享相同部署，最后一份 lease 释放后排空并关闭。
* `persistent`：supervisor 跨执行保存，显式 stop；有使用者时拒绝停止。
* `external`：只连端点，使用者关闭只释放连接。

多组件部署只支持显式、有界、无环依赖；JEV 后端先就绪再启动 wrapper。
失败仅回收本次获得的组件/lease。GPU 只由真正加载模型的组件持有。
新核心增加同用户/宿主机跨 root 的资源协调域，同时保留声明目录的锁。
已运行的旧版本或外部手工进程不会自动加入该域；不能声称新锁能识别所有旧
GPU 占用。占用检查不接管未知进程，仍须遵守维护窗口。

## 5. 能力、准入与并发

模型能力与节点策略分开。能力描述 single/batch、输入类型、问题类型、
有限执行容量及上限；节点可以收紧，不能扩张。`parallel` 仅用于有限独立
执行槽位，continuous batching 单独标记。`max_inflight` 单位是 HTTP 调用，
同时用行数/字节/token 限制防止一批载荷绕过容量。

缺能力的旧模型保持其显式 concurrency；缺省不启用新增 batch。
服务全局准入保护多个调用方，客户端准入限制节点自身。服务 lease 和
实际 HTTP 请求槽位分离，组好批后再取得一次请求槽位。

仅对兼容的模型、端点、认证、采样参数合批。队列按行数和字节有界；达到
大小、字节或第一条请求的截止时间就发送。不会因新行进入延后截止时间。
队列关闭/取消要唤醒所有 waiter；只取消一行不能丢弃其他已发送行的结果。

## 6. SystemOne 协议与质量门槛

扩展 choice 的请求 criteria/instructions 和响应 choice/probabilities 校验。
选项顺序参与 choice 请求身份，不改动既有 noul 键。保留 images 及模型已
支持的输入字段，不能在批量封装中丢失。

批量请求 `requests` 与 `responses` 一一对应，显式校验数量/索引，响应
包含逐条 answers 或 error、可获得的逐条 usage。顶层总 usage 只计一次；
不把未知 token 用量写成已知零值。输入错误可逐条隔离，共享设备故障如实
标记影响范围。超限请求在模型推理前拒绝，不静默截断。

生产 batch 必须绑定质量证据：模型 revision、引擎/插件/适配器版本、配置、
硬件、最大批量、样本数量、容差和结果。fake 测试证据不能认证真实模型。
至少 200 条，包含长短混合、不同选项数、随机批次伙伴/位置、临界概率，
支持图片的模型包含图片。标签须一致，概率容差预先固定。vLLM 并发单条
同样可能内部合批，必须测试并发对结果的影响。

| profile | 实现与约束 |
|---|---|
| vllm | 原生 OpenAI-compatible 服务，独立运行环境 |
| autotrust-jev | vLLM + 原生双向 attention 插件 + 轻量 SystemOne wrapper；保持禁止 prefix caching/chunked prefill 等限制；只组合兼容采样参数 |
| pplx | 保留执行锁，使用原生 decide(list)；fast/compile 等另行验证，不默认改变精度 |
| amos | 单条与能力声明先落地；两条请求 GPU PoC 后再决定 batch |
| d3 | 统一有界 HTTP worker + 原生 D3/Decider，跨请求 batch=false |

## 7. journal、预算与背压

按行准备与查缓存 → 逐行有限额度预留并持久化 → 未命中请求入队 → HTTP →
逐行结果持久化 → 交付。相同 request key 保持已有复用行为，不能将重复行
误计为必须独立的 journal 条目。合批不增加业务批次记录。

复用现有 SQLite 写队列的事务合并；不增加第二套数据库。整批完整 HTTP
错误按每行保存，按显式有限策略重试；超时/断连/截断不自动重发。
取消排空遵循既有 shield/持久化边界；队列内取消行标记未派发，已预留额度仍
保守计数，未修改旧 uncertain 恢复策略。这样不新增第二套预留/退还预算机制。

内存预算涵盖原始行、准备结果、编码副本、在途体、响应和已完成未消费
结果；编码字节限制不声称是 RSS 硬上限。模型内部 token/图片分配受各自
运行时上限约束。超出单行限制直接报错。

## 8. 分阶段实施与 Review 门槛

| 阶段 | 内容 | 无 GPU 验收 |
|---|---|---|
| D | 设计 review、工作树基线 | 职责/生命周期/身份/质量/资源清单 |
| A | 统一 HTTP owner、service 协议、SystemOne、组合依赖 | 旧服务回归、并发启动、失败/取消、租约、跨类型接入 |
| B | 部署声明、profile、显式 prepare/doctor/管理 CLI | 解析无副作用、安装模拟、环境复用与失败、逻辑身份 |
| C | choice、能力/质量证据、有界批量传输与模型 adapters | 图片/顺序/错误隔离、journal/预算/重试/取消/背压 |
| R | 实现 review、文档与 CPU 回归 | 基线改动保留、针对性与常规测试、验证脚本 |
| G | GPU 0/1 验收 | 候选批量的质量/性能未通过；默认安全路径及 1k smoke 见第二轮报告 |

每阶段 review 检查：是否重复了生命周期/调度/配置；是否改变旧身份；是否
让 GPU 模型被 import/fixture 启动；是否无界；是否跨请求污染；取消是否丢
持久化；API 和文档是否与实测一致。发现契约变更或范围取舍需要用户决定时，
先记录具体影响并提问，不以实现方便为由绕过约束。

## 9. Review 与实施日志

### D：设计自审（2026-10-31）

* 已核对两套 owner 的重复逻辑；选择合并到 HTTP 核心，保留旧 vLLM 参数入口。
* 已核对 embeddings 的具体类限制及缺失的共享 request_slot；统一 service 协议。
* 已核对旧 journal endpoint 参与哈希；新托管逻辑身份单独引入，旧记录不改。
* 已核对 JEV 插件限制；禁止把通用 vLLM 优化开关直接套用。
* 安装与 GPU 质量验证分开；本轮不执行真实安装、模型推理或生产服务起停。
* 工作树基线：`/tmp/demiflow-services-baseline-20261031T130053`，51 个文件。
* 审查结论：可以进入 A；后续阶段必须继续检查实现与本文的一致性。

### A/B/C/R：实现自审

本轮为实现者自审与测试复核，不冒充独立 reviewer 已签字。

| 检查点 | 实现与审查结论 |
|---|---|
| 生命周期重复 | 删除独立 vLLM owner，vLLM 生成启动参数并复用 HTTP 核心；supervisor 同源 |
| 原生协议差异 | discovery 同时接受 OpenAI `data/id` 和 SystemOne `models/name`；amos 使用原生 engine 的轻量 wrapper，d3 保留原生服务 |
| 无副作用解析 | 深度冻结部署声明；显式 prepare，首次未命中才获取 lease；缓存重放不读环境/启动模型 |
| 错误的逐行加载 | 同步 `map_prompt` 拒绝新增托管声明；异步 runtime 才持有并复用模型 |
| 组合部署失败 | 依赖先启动、反序释放；新建且闲置的持久组件回收，已有持久组件保留 |
| 并发 | 行并发、服务全局准入、有限批量 HTTP 并发分开；不把队列容量宣称为 forward parallel |
| JEV 质量 | 原生双向插件和读出参数保留；单条字符串 prompt，batch 按选项数分组；不启用通用量化/缓存捷径 |
| pplx 质量 | 恢复原生默认上下文上限；原生 encode 验证及 `decide(list)`，不默认 fast/量化 |
| 合批门槛 | 原生列表接口只是候选；报告匹配模型、配置、运行时/源码、环境摘要、硬件/驱动；批大小与并发不得超过已测上限 |
| 行语义 | 同一行单条/batch 请求键相同；逐行 journal、usage、有限重试；批次信息仅为响应元数据 |
| 失败与取消 | 批量项 429 只重试该行的单条请求；数量不符/截断不重发；取消一个 waiter 不取消同行 |
| 有界资源 | 行数/编码字节/批 token/连接/响应/日志/准备时间与目录监测；明确不冒充 RSS 或磁盘硬配额 |
| 原有脏改动 | 51 文件基线中 48 个逐字节不变；cli、systemone、pyproject 仅增量；原图片 request_payload AST 完全相同 |

环境检查仅启动短时 Python 元数据读取子进程，禁用 CUDA 可见性，不 import
torch/vLLM、不加载权重。确认本机已有 pplx 0.3.0、amos-serve 1.0.0、vLLM
0.31.0、transformers 5.17.0 和 startlux-decide 1.1.0；未修改这些环境。

CPU 验证：第一轮相关回归 376 passed；增加恢复/边界覆盖后广泛回归
391 passed。后续审查修正又通过最终针对性回归 **105 passed**，包含共享服务、
SQLite journal、发现协议、准备失败恢复及批量字节边界。两组覆盖有重叠，
不相加冒充独立测试数量。最终测试代码摘要在
`/tmp/demiflow-services-tests/final-code-sha256.json`；JUnit 结果为
`cpu-regression.xml` 和 `final-targeted.xml`（同目录）。最后核对代码摘要未改变。
没有写入生产 checkpoint。

### G：首轮结束时的待验收清单（后续结果见第二轮报告）

* 新环境安装配方的真实下载/安装，以及四个真实服务的启停；本轮仅模拟准备流程。
* reviewer 检查 200 条真实样本覆盖，再运行串行/并发/批量质量验证。
* pplx ≥3×、JEV ≥2× 目标尚无实测结论；必须同时比较最佳并发单条和批量，
  不能只挑慢的串行基线。还需测端到端 journal/算子瓶颈。
* GPU 显存采样、真实 1k 身份 smoke、amos 跨请求 PoC；amos/d3 batch 继续关闭。
* 业务 triage 切换与现有 pplx/amos 服务迁移由维护窗口和业务 reviewer 决定。

本轮没有重启 pplx/amos，没有占用 GPU 2/3，没有运行模型推理，没有 commit/push。

### 第二轮 review：真实 GPU 反馈后的修正

* 独立 worker 启动时排除脚本目录对 stdlib `http` 的遮蔽，补真实子进程启动测试。
* 整批异常复制为独立行异常，避免行 journal 引用串写。
* 聚合 token 预算改为拆分子批，不让合法请求因同批伙伴而失败。
* 最后一份 lease 关闭期间保持 registry 可见，下一 action 等待清理；调用方取消
  不取消后台资源回收。
* 证据 v2 必须覆盖所有允许批大小、三轮伙伴、200 个不同输入、完整响应键，
  校验逐模式结果与输入类型组合，并绑定模型文件签名。旧证据不能宽松放行。
* JEV 并发单条也会触发内部合批，因此默认串行执行，验证模式与通过证据才放开。
  这是实际执行能力的保守声明；HTTP/算子并发仍支持，不把排队冒充 parallel forward。
* d3 保留原生解析/推理/读出，使用与其他模型相同的连接、请求、线程及响应预算。
* 原生 batch 候选在真实样本上改变标签和概率；vLLM batch-invariant 又不支持
  当前 GDN_ATTN。没有为满足加速目标而放宽质量标准或改写底层模型内核。

## 10. 引用与优化约束

vLLM 的安装、优化与可复现性信息以原生文档为依据；本项目不会复制其 token
调度/KV 管理，也不假设默认不同批次必然数值一致：

* [vLLM GPU installation](https://docs.vllm.ai/en/latest/getting_started/installation/gpu/)
* [vLLM optimization](https://docs.vllm.ai/en/latest/configuration/optimization/)
* [vLLM reproducibility](https://docs.vllm.ai/en/latest/usage/reproducibility/)
* [vLLM batch invariance](https://docs.vllm.ai/en/latest/features/batch_invariance/)

JEV 还受本地模型 `README.md`、`serve_vllm.sh` 和固定 StartLux 插件版本的
双向 attention 契约约束；不能把普通生成模型的所有优化开关套用到该模型。
