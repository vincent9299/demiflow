# 七组关键接口的修订规范（待任务卡装配）

2026-11-14。此文件确定本轮 Review 后的设计决定及必须补齐的接口，**不是可直接执行的新版全量工单**。原 interfaces.pyi、144 张卡未全部与之同步，所以整包 `dispatch_ready=false`。不允许 agent 自选新旧版本。本文中的新增函数是拟定修订点，须完成签名、引用和类型样例校验后进入 plan.json。

## 1. action 生命周期与真实 terminal 执行

生产者是公开 Dataset action 边界，既有 `checked.action` / `RunContext.action` 是权威；下层 executor、delivery、native backend 借用同一个 RunAction。没有当前 action 时不能用假 action_id 封存。

必须保持：

- 一个公开终端操作只产生一个语义 action。`count` 已有 action，下层不能再建一次；materialize/write 已有的 action 也不能重包。
- `iter_rows`/`iter_batches` 返回惰性迭代器时，action 的进入与退出覆盖实际消费期；提前 close、异常、取消都触发同一收尾。不能在“返回迭代器对象”时先退出 action。
- 一个内部 helper 借用 action 时不得替调用者 finish/abort；对不存在外层 action 的直接内部入口，是否允许自建必须显式定义，不能悄悄猜测。公开与内部入口分别测试。
- schema 的纯声明读取不应虚构一次计算；`fetch_if_missing` 真读数据时才进入相应执行路径。size/aggregate/write 等实际执行必须有绑定。
- 准入发生在读源、构造业务 actor、发请求之前。真实 executor 产生 backend capabilities，不能由消费者默认 `()` 或由用户任填字符串启动后端。

| 必补代码单元 | 输入/输出和职责 | 消费者 |
|---|---|---|
| `Dataset.take` / `take_all` / `iter_rows` / `iter_batches` 各一任务 | 保持公开签名；返回值不变；补 action 生命周期。需先查同级 helper，防重复建 action | LocalDatasetExecutor 对应 terminal |
| `stream_delivery.iter_stream_rows` | source/plan/limit + 显式借用当前 action；返回有界行迭代器；禁止 nested action | take/iter/count 的 stream 路线 |
| `LocalDatasetExecutor._execution_capabilities`（新） | 基于本 executor 可用后端/源类型/原生选项产生只读能力描述；不读取业务数据、不执行用户代码 | A001、各 terminal |
| `LocalDatasetExecutor._apply_plan` | 已封存 execution 作为显式可选输入；绑定调用不得为空；实际 backend 与其一致；不能重新挑一个“能跑”的默认后端 | 所有需要执行的 terminal |
| `LocalDatasetExecutor.iter_batches/count/take_batch/aggregate/schema/size_bytes/materialize/write_datasink/write_file` | 每个现有函数独立一任务或 verify_preserve；每个调用点明确传递 execution，不能以 _apply_plan 一处修改代表全部调用点完成 | 原 Dataset 公共方法 |

公开层现有 write_parquet/json/csv 等可以保留薄包装，但要在清单中证明其实际汇合到被覆盖的 write_file；不能重复造 action。现有 native/Arrow/Lance 快路分别保留并显式绑定，静默退回 legacy 不得代替不支持错误。

统一 `RecoveryMode = Literal['native_checkpoint','final_commit','delivery_only','attempt_only']`，由 ActionDecision → execution_bindings → ExecutionPlacement 原样携带。四值指能力和边界，不意味着四种都可 resume。F1 载体仍是 `sealed_plan_facts.v1`；不要把 execution_bindings v2 当成 F1 v2。

验收：公开 take/iter/count/write/aggregate 的正常结果；只建一次 action；迭代早停及取消不留资源；改封存 backend/profile/program 后必须在业务读取前被拒；启动时消费的 execution ID/program ref/profile 与封存文档逐一对应。记录 `unified_chain_eligible` 调用次数不作为通用统一执行判据。

## 2. Embedding 与原子请求预算

现有唯一预算账本是 RunJournal，效果记录走 RunPromptJournal。同一 fresh 请求的预算与 reserve intent 必须在其已有事务中生效。EmbeddingActor 不具有 Prompt coordinator，不能通过假字段或假 coordinator 让 `bind_client` 勉强可调用。

增加的窄接口拟为：

```python
@contextmanager
def native_request_budget(run, *, node_id: str, max_requests: int | None):
    # yield reservation_id；只构造上下文，不在事务外扣账
    ...
```

位置：`operator_llm/stream_recovery.py`，由 M 统一装配。`max_requests=None` 表示采用 run 上限；显式 0 不能因 `or` 运算被当成缺省。使用既有全局 requests 及 node 额度键，不能发明第二个 fresh counter。reservation ID 唯一；其稳定/去重语义遵从既有 journal 请求身份，不用“只要 request_hash 一样就重新免费执行”的捷径。

完整交接：

1. `_exchange` 先查询已保存响应/unknown，命中已完成响应时不 fresh reserve；unknown 保持既有恢复规则。
2. 真正 fresh `_fresh_exchange` 的 reserve 边界进入预算上下文，设置既有 CALL_RESERVATION `(reservation_id, charges)`。
3. RunPromptJournal._enqueue 在调用线程读出该上下文，附加保护载荷；_apply_operation 在同一数据库事务内 reserve_budgets + intent。**不允许先外部 reserve 再内部 reserve 计两次。**
4. 如跨线程 `_io`，在提交工作时复制当前 context；进入/异常/取消都 reset token，不能污染下一请求。
5. reserve 失败禁止发 transport；reserve 成功但 transport 状态未知不能退款后无条件重发。响应复用、额度、unknown 与原生恢复仍是同一账本。

必须新增函数级任务：上述 contextmanager、`NativeEmbeddingRecovery.bind_actor` 预算/原 journal 绑定消费、`EmbeddingActor._fresh_exchange` 上下文入口、跨线程 context 传播核验；原 M007/M008 的身份构造与请求作用域不能冒充预算预留。

验收：总额度=1 的两个 fresh 调用只有一个真正 transport；响应重放不再 fresh；两个不同 node 共同受 run 总额约束；额度=0 在发请求前拒绝；事务失败不出现仅扣额度无 intent；取消/异常后 ContextVar 不泄漏；worker 崩溃/恢复不自动重发 unknown。保留 prompt_facts 与 execution-owned journal 守卫。

## 3. buffer 持有者、聚合与原生单次快照

新 adapter 只拥有本地缓冲快照，不是调度器、资源分配器或 CommitCoordinator。stage 有 `buffer_adapter` 字段，其 slots 与构造器同一提交装配；普通业务状态继续由 OperatorStates 管，效果继续由原 journal 管。

必须明确的状态对象：

| 数据 | 唯一持有者 | 更新点 | 快照/恢复职责 |
|---|---|---|---|
| worker 当前 batch、分组集合、定时剩余量 | adapter 内按固定 worker slot 编号登记的状态 | grouped/batch worker 的入队、flush、消费 | adapter 验证所有参与 slot 后输出描述符 |
| prepend 位置 | 对应 stage adapter | 实际成功交付前缀行的边界 | 恢复先还原位置，不能按当前计数猜测 |
| batch ordinal | 保留原 stage.batch_ordinal 的语义，若迁入 adapter 必须有单一委托 | 原批次构造/完成点 | 旧 fixed-batch 快照继续可读 |
| native row-local in-flight | 现有 `stage.native_pending` 全阶段注册表 | 原 native input/emit 锁保护的登记/删除 | real align 读取整个注册表，不读取某个 worker 的 batch 代替 |
| cut arrivals | `_CheckpointBarrier`，按 stage + worker slot + cut 去重 | 所有 worker 的 barrier 分支 | 收齐应到成员后才调用一次本地快照 |

对普通 buffered stage：每个 worker 必须有稳定、有限的 slot ID；arrive 必须能够识别 slot，重复到达/错误 cut 被拒。每个 slot 的 payload 一次登记，不能简单整数计数后只取最后一个 batch。grouped worker 必须识别 barrier，不能把它当业务行交 grouping key。所有参与者退出、EOF、取消的处理要列明；不能等待已结束且永不再到达的成员。

对 native row-local stage：保留“单个 token 触发阶段级完整注册表快照”的可能性。单 token 不是只拍单 worker；必须证明输入登记与结果发布在快照 cut 两侧受到现有锁/顺序保护。**不得仅根据 Qwen 的假 batch 探针将其改为所有 worker 排空后才 snapshot**，这可能改变原协议并引入死锁。

旧 StageBufferSnapshot 草案的 groups/row_ordinals/pending_batches 不能直接冻结：先规定 rows 描述符内的序号空间，再明确每个索引指向哪一段；source ordinal、batch ordinal、output ordinal、channel sequence 均不得混作一列。未确认副作用引用只使用原 effect journal 的身份与状态，不复制 response/unknown 状态机。

新增/修改任务单元必须包含：`_CheckpointBarrier.__init__ / tokens / arrive / fail`；`_make_worker` 中 grouped、batch、native 分支（需按现有嵌套函数装配，不允许两个 agent 各改整个外层函数）；`StageBufferAdapter.__init__/align/restore/close`；`StreamStageProgress.bind_stages/align/restore_stages`。所有 snapshot participant/aggregate bytes 在 prepare 前共同受原 commit 限额约束。

验收：c=2 两 worker 各有 pending；同 cut 重复 arrive、错 slot、EOF worker、取消；定时窗口/压力 flush/空上游 prepend；快照前后崩溃；有 effect 的响应已存和 unknown；恢复后输出多重性/顺序按原语义一致。固定批满批守卫与原 batch_ordinal 恢复不被回退。

## 4. 工作台图、occurrence、源码与状态

冻结逻辑 DAG 的节点/边/ID。当前 graph 边只有 logical_node_id/port，缺少按 occurrence 展开所需的边身份；不能仅换 DOM key 就伪造多份执行图。

采用可落地的展示方式：一个 logical vertex，详情面板提供其 occurrence 列表。选择值为：

```python
Selection = {"logical_node_id": str, "occurrence_id": str | None}
ObservationKey = (run_id, action_id, attempt, logical_node_id, occurrence_id)
```

无 occurrence 的历史记录可以显示“未绑定到具体出现位置”，不能随便取第一个。封存身份桥负责从实际执行位置绑定报告/source；工作台只读 join，不能重算另一份 DAG 授予“契约通过”。

职责分开：graph `update` 收逻辑图和只读节点观测投影；`select` 使用 Selection；`onSelect` 回传同值；physical summary 继续按 logical ID 索引；源码请求携带 action/attempt/report_digest 和 Selection。`loadSource`/`chooseNode`/`chooseLogicalNode` 与后端 `bound_source`/bridge resolver 需要各自任务，不能只派 render/update/select。

来源可信等级与运行状态是两个字段：sealed 是事实来源，不是 succeeded。节点没有可信映射时显示 unknown/unavailable，不通过整 action 的成功状态捏造节点成功。历史读取只校验有界载体、摘要和绑定；不运行当前环境 verify_seal，不导入当前业务模块，不因今日代码改动抹掉旧图。

验收：同函数两次出现、选错 occurrence、跨 attempt 回包、过期异步源码响应、重绘后选择、没有桥的旧运行、物理资源摘要，以及 D1.9 demo 的可达性/响应式。此轮不自动替代 Notebook/Case/开发历史等其他产品验收。

## 5. 程序交付方式、源侧代码及秘密参数

两种既有载体各司其职：

| 执行方式 | 生产者 | 加载/验证 | 禁止 |
|---|---|---|---|
| 独立 worker 的 in-run program | stage_plan_artifact | 校验载体摘要，加载后 verify_loaded_plan；worker 自身重算代码依据 | 仅验证 pickle hash 就认为代码固定 |
| 当前进程 native driver/write/model | seal_write_plan_ref | 使用原 code-pin-only 记录与当前代码来源核验；不反序列化为 worker program | 无条件 cloudpickle 已物化 PromptActor 或用 pickle 字节定 durable 身份 |

ProgramRef 现有五键不变，不凭同样键形状猜载体类型。execution/backend 已封存的交付方式必须决定验证器。多 input_path 的 native program 需要稳定、碰撞安全的 per-action program key；原只含 action 的 write pin key 不够表达多 slice。新 key 与旧记录兼容规则必须在 `seal_write_plan_ref` 对应任务中定义；恢复加载旧 record，不重 pickle。

source-side callable 的收集新增窄入口，返回真实 CallableSpec/函数/声明，不执行用户代码。复用现有 dependency collector 的有界 BFS、代码源摘要和归档规则；plan_code_pin 与 snapshot_code_sources 用同一个依赖集合。`describe(source)` 用于声明语义，但不能替代 globals/导入依赖的 code pin。不要借一个假的 MapOp 包住 source 以绕过模型。

秘密字段边界：operation_facts 的模型/推理语义字段按白名单投影；transport 凭据、Authorization、API key 和仅运行环境路径不能原样写进永久事实。业务确实引用的数据路径按原源身份契约处理，不能笼统删掉所有路径导致输入碰撞。凭据轮换不改业务 identity；模型/revision/参数变化应改变身份。验证只用合成凭据，不打印真实配置。

待补任务：source callable collector；plan_code_pin/source collection 消费；snapshot_code_sources 同集合；seal_write_plan_ref 稳定 program key；stage_native_programs 双分派；verify_native_programs 分派验证；F002 白名单与对应候选三方比较。

验收：真实模型 actor 带锁仍能 driver 封存；worker 真 import 验证；只改 source reducer 导入 helper 恢复拒绝；凭据轮换不改身份、模型语义改变会改身份；源存在两个 input_path 时分别绑定执行位置；旧可读历史不被新格式重写。

## 6. 原 exchange、资源 ceiling/floor 与实际分配

既有 InputGate/ChannelSequence/route_epoch 管物理通道的 barrier/replay；driver CommitCoordinator 是唯一结算权威。局部 buffer adapter 输出自己的状态描述符，通过原 progress 并入同一次 cut；它不重新定义 channel 序号、ACK 或另一份 journal。

必须写成实际可校验的对应关系：一个 sealed execution/group/stage 的输入 channel 集、各 channel 的 route_epoch/next sequence、source frontier 和各 stage state ref 属于同一个 commit cut。不同坐标无需数值相等，但不能由恢复时计数从 0 重猜。filter/flatmap/partition 反例必须在绑定验证中出现。

资源约束：

- 真实 ResourceProfile 只有 requests/limits/requirements 下已有资源；本处 CPU 为 cpu_millis，内存为 memory_bytes，临时空间为 scratch_bytes。不要向 profile 读不存在的 threads/rows。
- 线程数由真实 CPU/native options 推导并受限；批行数、单行 bytes、队列、预读来自运行/阶段预算；固定模型/actor 驻留与动态 reader/queue/buffer/sink 同时存活量不能重复许诺。
- `derive_prefetch` 原协议 `max_pending_floor` 与 channel credit 一起校验。若 `required_floor > granted_ceiling`，拒绝该布局，或重新编译一份能同时满足的 credit 和布局；不能保留旧 credit 后压低 pending，也不能超过 grant 强行 max。
- Arrow native Tasks/临时 spill 的真实分配点要借用同一 allocator/grant；向 execute 传一个 execution 对象而调度器仍用无限默认值不算落实。包括成功、失败、取消时的释放。
- commit 参与者数、快照聚合 bytes、journal reservation 条目量是实际预算的一部分；每个 stage 的上限不能各自复制全局额度。资源不足须在可能时于读取/发请求前暴露，动态超限不得提交残缺成功。

待补任务归属：R 负责 cut/state 绑定；L 负责 execution→runtime 消费；Q 负责原 allocator/grant 与真实窗口。新增 `derive_prefetch` 交接测试、Arrow scheduler/temp spill 分配和释放任务；不是新增第二个调度系统。

验收：credit floor=2、grant ceiling=1 明确拒绝且无读取；合法小预算仍能让 barrier 前进；慢消费者/大行/多驻留 stage/磁盘不足/取消；同预算与基准的吞吐、首条延迟、峰值及 I/O 证据。Python 对象估计和第三方分配的覆盖边界须如实声明。

## 7. F3 保护、能力声明与放行条件

R006 不能一次删掉现行拒绝。能力表按 kind 分开：固定 batch、并发 batch、定时 batch、grouped、prepend、native row-local。每个 kind 列 native partial checkpoint 的状态、实际 adapter/切 cut 路径、故障测试。缺一个就仍拒绝该 durable 组合；这不等于禁止普通非 durable run_stream 的既有能力。

原生固定批恢复的 `batch_ordinal`、恢复 batch 小于原 max_batch 的检查保留。源重放条件保留原 source frontier 语义及 `ordinal >= replay_cut OR full` 中容量压力可推进的条件；不能用 next_output_ordinal 替换 source ordinal。不能通过强制 concurrency=1、放大队列/额度、取消 unknown、取消 Budget reservation changed 守卫通过测试。

S 的 declaration/instance 两阶段也纳入能力表：class factory 在声明期可声明平台 deferred adapter；`adapter_present=True` 表示平台机制可用，不能表示实例验证已发生。实际构造后的 validate/register 必须在第一行业务之前执行；含锁/句柄不能自动 JSON snapshot。不应把 durable 的限制前移成普通执行的无条件限制。

最小放行证据：每种 kind 正常运行、cut 前/后崩溃、prepare 后/commit 前崩溃、恢复再次崩溃、取消、两 worker pending、预算压力。结果数量、重复多重性、状态计数、源/输出位置、模型调用次数和提交权威都核对。关掉新 adapter 后对应测试必须能红，才能证明没有走 legacy 绕过验证。

## 8. 仍未覆盖的全局目标

本轮改接口不等于完成独立 worker、Local/Ray 等价、CPU/GPU 调度、九段业务 M01—M09 × 三种来源 × 两 backend 的整个历史架构矩阵。当前 capture_source 的全源等待、graph 逐行段/同步 I/O、长任务 reservation 条目增长也仍要纳入性能交付，不因本轮关注恢复而从意图中删除。

编号统一写 task:M001（本包叶任务）、business:M01（历史九段业务）、architecture:A（历史五模块）；U0—U6 是原验收检查表。保留独立架构/完整记忆原文，本包不能改名吞掉其中未交付项。

重新发布条件：上述接口进入类型文件及独立任务卡；每个新增字段有真实写入者和读取者；每条路径有正向样例和拒绝/故障样例；候选实现按函数三方吸收；验收命令/门依赖及所有原意图未完成项可对账。**这只是重新允许独立编码的条件，真实合流通过之后才能判断功能完成度。**
