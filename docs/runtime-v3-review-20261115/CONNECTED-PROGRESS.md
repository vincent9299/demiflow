# Dataset 原生输入、调用、提交与恢复接通记录

这是隔离分支上的实施检查点，**整体尚未完成**。产品 root 仍是能看懂、配置、运行真实 pipeline 并分析、比较、评测数据；引擎 root 仍是正确、稳定、高性能、通用且架构清晰的统一计算平台。I01–I14 以 `docs/unified-runtime-v3-20261114/INTENT.md` 为准。

## 本批代码修复

1. 并发输出恢复公开 unordered 语义，慢的先行调用不堵住已完成的后行结果；调用身份与独立状态所有权仍随 checkpoint 保存。
2. Artifact 写入从“预留整个声明上限”改为有界初始预留、扩写前准入。`grow_staging` 使用原 RunJournal 的同一 fenced transaction，重复与较小旧请求不重复收费，已结算的 reservation 不可重开。原有唯一对象结算、删除临时文件后退款、未知发布保留计费不变。
3. SQL 捕获写成逐行有界 spool；恢复读取原封存 ref，不重新查库。游标和来源定位中的内层只读映射在持久化/重建边界显式解冻，未放松 DTO 的 JSON 约束。二进制单行与图片解码有前置上限。
4. 批次输出支持惰性生成器，并保留所有输入 batch_format 下的行列表返回约定。四个格式的组合反例发现并修正了本批一次误分类。
5. cache 声明在 worker 重建，状态、生命周期委托给真实 fn（含 partial/bound method）；缓存命中保留当前输入行，包裹 Prompt 时绑定同一个 native journal。同步与异步缓存的状态 actor 均实际跨 cut 恢复。
6. Agent 使用单独的受验证 `agent_facts`，不修改原 `prompt_facts` 守卫；Agent 最终回答走原生请求 journal、预算与提交。多轮工具及 Codex 的完整验收仍需继续。
7. RawFetch 通过 driver 原生服务执行。QueueDatasource 封存声明，不发送锁/连接；worker 的 poll/acquire/finalize 绑定真实图 occurrence 与 allocation/boot。delivery ref 通过 Record 来源 sidecar 传递，不注入业务行。结果先经原 QueueResultSink/CommitCoordinator 完成任务，再进入 Dataset 结果提交，不重复结算任务。
8. pending 轮询回复不缓存成终态。没有 Dataset 首个 cut 的队列恢复仅在匹配的原生 queue journal 事实下准入，由原生源协调已提交任务；普通源和 raw fetch 没有此例外。自动结算和显式 ACK 的两处故障恢复均不重发 HTTP。
9. 固定异常词汇经数据协议透传，不远程导入异常类型。取消先唤醒待回执 RPC，未确认效果仍是 unknown，端口保留给生命周期清理；避免清理等待线程、线程等待回执的循环。ActionHandle 保留 pump 直接抛出的原始业务失败。

## 本批验收证据

所有命令显式将 PYTHONPATH 指向隔离树，测试用 `env/bin/python -B -m pytest`；Ray 使用 `env-ray/bin/python`。历史 pytest 模块的顶层业务函数通过本树 `tests/` 的显式导入路径供真实 worker 校验，不修改主仓安装映射。数据、HTTP 测试服务、Ray 私有集群均位于本任务 `/tmp/demiflow-*` 域；未使用生产队列或付费模型。

| 证据 | 结果与边界 |
| --- | --- |
| `/tmp/demiflow-original33-current-2.xml` | 原指定 33 个 nodeid + M 的 27 项，60 passed。原负例未跳过或删减。 |
| `/tmp/demiflow-connected-v3-1.xml` | 公开 Dataset、真实接缝、原生模型、缓存、并发、磁盘、SQL 共 83 项：78 passed，5 failed。四项为批次返回误分类；一项为测试期待旧的泛化错误，已改为更具体的预算拒绝，HTTP/计费/无提交断言不变。 |
| `/tmp/demiflow-connected-v3-2.xml` | 上述 5 项 + 取消唤醒反例 + 惰性批次反例，7 passed。与前一组组合看，不冒充一次完整全量重跑。 |
| `/tmp/demiflow-queue-v3-8.log` | 正常队列、启动前 catch 拒绝、自动/显式四种提交恢复先通过 7 项；随后在负例清理等待处人工中断诊断，不把该次整组当通过。 |
| `/tmp/demiflow-queue-v3-10.log` | 修复清理等待后，丢行、扩行、篡改回执不 ACK，以及合法业务拒绝仍保留 raw 结果，4 passed。 |
| `/tmp/demiflow-ray-native-v3-4.xml` | 真实 Ray Core actor：Prompt→sort→take_all、请求预算及 ACK，1 passed；Ray Data 常见入口被禁止调用。只证明本机私有 Ray 集群，不代表多机/GPU已验。 |
| `/tmp/demiflow-workbench-v3-2.xml` | 真实工作台 prepare/start/show→worker→Lance 结果、身份桥与节点观测，1 passed。不替代 Notebook/Case/媒体/开发对话及浏览器完整验收。 |
| `/tmp/demiflow-lanes-current-3.xml` | 全 19 个 lane 文件：376 passed / 183 failed。未通过项继续处理，未计作成功。 |

全 lane 剩余失败初步集中于自定义 fake DTO 与正式 DTO 混用、缺失真实 action/journal、非 sha256 身份、旧 worker 启动与控制接缝，以及部分实际行为差异；逐项修复时仍必须保留原本负例目标，不能用放松正式 DTO 或替换整个被测实现取得通过。

## 下一步必须处理

- 队列生产端的 source unit/零输出/多输出/EOS 与统一执行衔接，及搜索、文档、managed 模型的原生能力组合。
- Local 共享物理准入、pending 重新调度、跨长任务的 grant 生命周期；Ray 实际主机/数据域证明、GPU 与多机放置，不能以 driver 的声明代替远端事实。
- 孤儿 staging 的恢复清理、SIGKILL 提交窗口、宽行/倾斜/跨算子驻留资源以及公共 API 完整回归。
- 183 项 lane 失败及相关历史套件，随后对照两大原始意图验收产品与引擎完整体验。

保护核验：主仓仍 `e40a6dd3dd756aba423dca4c7d995ada0a4bff65`，原 11 处文档变更保留。raw_fetch/queue_execution/queue_sinks/queue_source/planning.dag/execution.contracts.exchange/resources.contracts 七件相对 `362d8528c27ae60a0d6ae0ccc986a0ff93847de8` 无 diff。未 push、未合并主仓、未发送会话消息。
