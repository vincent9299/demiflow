# 冻结接口契约 C01–C16

签名与字段见 `interfaces.pyi`、`function-signatures.pyi`；身份精确公式见 `IDENTITY.md`，状态所有权见 `OBJECT-OWNERS.md`。这两个文件是设计声明，业务包不可导入。所有新接口由唯一生产 owner 实现，消费者不能写 fallback/stub。任务卡中的 `calls` 是代码调用义务，不是等待对方提交才可编码。

## C01 数据表示、版本与错误

所有 DTO frozen 且内层深冻结，脱离调用者可变对象。wire 只接受显式字段，不透传未知字段；schema 必填，v3 不自动读取为 v1/v2。Json 有限深度、有限 UTF-8 字节、禁止 NaN/Infinity；业务 Record 编码可保留 IEEE 浮点特殊值、负零、tuple/bytes、Arrow/numpy 值，这些不能直接作为 Json 身份输入。`inline` 与 `payload_ref` 恰有一个；空行是合法 payload，不等于 EOI。

ID 使用 lowercase sha256 hex；人类 source/port 名称不是 hash。ArtifactRef.key 是 store 内相对不可变 key，不含绝对路径、`..`、URL query 或凭证。原有 DataRef/Artifact 格式由桥接函数调用原校验器，不能把 ArtifactRef 直接交给 K4 的 v1 validator。代码 pickle 和业务数据编码有不同 media_type，数据读取不得当程序执行。

错误类型统一 `DataflowError(code, message, *, retryable=False, context={})`，code 枚举：INVALID_CONTRACT、UNSUPPORTED_CAPABILITY、RESOURCE_INFEASIBLE、BUDGET_EXCEEDED、SOURCE_CHANGED、CODE_MISMATCH、FENCED、CANCELLED、DEADLINE_EXCEEDED、CAPACITY_PENDING、CORRUPT_ARTIFACT、UNKNOWN_EFFECT、INCOMPATIBLE_HISTORY、INTERNAL_PROTOCOL。未知 effect 不可标 retryable；CAPACITY_PENDING 可在原有限 deadline 内等 credit，不重新执行业务调用。异常 context 仅含有界身份/位置，不泄露请求内容和密钥。与现有错误类转换只能保留语义，不吞异常。

CAPACITY_PENDING 与相邻码的边界（AMEND-001 固定）：它表示 credit/容量**暂时**不足，可在**原有**有限 deadline 内等待重试，不延长 deadline、不重新执行业务调用、不自动提高预算；`BUDGET_EXCEEDED` 是终态硬拒绝（如真实超大行、超硬上界），不可等待；`DEADLINE_EXCEEDED` 是原 deadline 已耗尽。三者不得互相替代。`UNKNOWN_EFFECT` 永不可标 retryable。码集仍闭合，未知 code 仍拒绝，消费者仍不得自行扩展。

## C02 编译输入与共享实例

ActionRequest.kind 精确为 `take,take_all,take_batch,iter_rows,iter_batches,count,aggregate,schema,columns,size_bytes,materialize,write_datasink,write_lance,write_file,run_stream,collect_summary`。参数各自来自现有公开签名，sink/programs保存进程内纯声明，并由stage_dataflow_programs封存。schema/columns在元数据足够时不读行；fetch_if_missing=False保持未知结果，True且需推断时经同一action执行有界探测，不在compiler里执行UDF。

编译只遍历真实 source/plan/PlanInput，不执行源、UDF、actor 构造、网络或模型。共享前缀依据构造引用拓扑；两次独立 map(fn) 不合并。join 有 `left/right` 端口，union 的输入端口为稳定输入序号；端口顺序影响语义和身份。循环、深度、节点/边上限在遍历前沿逐次检查，不先递归到底再拒绝。初始默认上限：4096 节点、16384 边、深度 64、4096 partitions；可显式收紧，不根据输入无限扩额。

## C03 物理计划与资源

KernelSpec 是执行声明：配置属于业务身份；profile、融合、并发/批次中非语义部分归物理计划。业务 batch_size 影响 actor 语义时必须进入逻辑身份，网络合并大小不进入。默认未知 UDF 不可拆 partial reduce，不自动跨 effect 融合。

PhysicalDataflow 全局验证：所有 occurrence 唯一归属一个 stage；每条边恰有 ExchangeSpec；每 partition 一组有效角色；所有 ports 有唯一连接或显式多输入；每 terminal 可从 source 到达；无悬空 stage、重复 sink_id、隐式 driver 业务执行。资源由既有 ResourceManager 申请，resident reservation、累计计费、持久存储容量分别计量。计划要同时满足工作、控制和消费者最小可运行集合，GPU 数量与显存声明分开；provider 不支持 required 隔离时准入拒绝。

Bounds 不允许 0 表无限或负数；配置必须显式解析旧 run.limits 的字段，缺字段使用有版本的 RuntimePolicy 默认，不由消费者各自猜值。运行时只能收紧资源窗口，不能改变已声明业务 batch。memory credit 和 output reservation 在分配前占用，转移 ownership 后释放，不能先释放再下游入队。

## C04 来源与 cursor

SourceSnapshot 固定版本/成员，不将所有数据提前装入 driver。split_id 稳定绑定 source identity+partition；SourceCursor.offset 对不同 kind 的结构分别固定：range `{next:int}`，items `{index:int}`，file `{file_index:int,record_index:int}`，Lance `{version:int,fragment_id:int,row_offset:int}`，spool `{segment:int,record:int}`，queue `{claim_ref:str,record_index:int}`；SQL snapshot `{snapshot_id:str,partition:str,last_key:Json}`。CSV/text/json 非可 seek 时按已确认记录重读并验证固定文件，不能使用文本 tell cookie 假装跨进程通用 offset。

SourcePoll 必须显式区分 record/pending/eoi：record 时 record 非空且 retry_after_ms=0；pending 时 record=None 且 retry_after_ms>0；eoi 时 record=None 且 retry_after_ms=0。ScheduleChoice.work=None 只能表示有界等待，不能当执行成功或 EOI。

已知有限输入 EOI 只在读取成功且全部输出确认后发；异常、队列暂空、超时不是 EOI。队列 continue/idle/closed 分开。来源读取、spool、解码、展开都受条数/字节/时间限制。from_iter 的工厂在 worker 构造；捕获 driver executor 的闭包在预检给出具体错误，不能默认拷贝活 executor。普通不可重读iterator默认attempt_only。若要求checkpoint，有限来源必须先在有界磁盘内完整capture并成功seal，之后才运行下游效果；能声明可重开稳定cursor的来源才可逐段spool增量恢复。单有已持久化前缀不能恢复尚未生成的尾部，必须返回incomplete而非伪EOI。

SQL 没有固定 snapshot 及稳定 keyset 时选择 durable capture，并公开它的首条/恢复保证；不得默默重连后读另一个快照。read_lance head 在 action 准入时只解析一次。自定义 Datasource 的旧 ReadTask 可装配，snapshot_identity=None 对应 spool/attempt_only，不冒充可 seek。

## C05 Exchange、顺序、credit 与 GC

mode=memory_incremental：记录 ready 即可发送；durable_incremental：segment checksum+publish 完成才可消费；finite_sealed：所有 scope membership 成功且 manifest 完整才可消费。空成功 scope 可以 seal；取消/失败/暂空不能。所有模式共用一个 channel sequence space，data、barrier、EOI、seal 有序。恢复重建 sequence、route ordinal、reader cursor，不能从 0 路由后缀。

hash 路由使用版本化稳定键编码，禁止 Python hash；rebalance 计数器纳入 cut；target 按业务 target 声明，不换模型；broadcast 的 delivery_id=(record_id,edge,reader) 保持消费去重而不丢业务多重性。慢 reader 自有 credit，同时计入全图总字节；取消分支必须写撤销订阅事实，不能静默删 reader。

持久段 id 绑定 channel/route_epoch/producer/sequence interval/content digest。重复发布相同 id 相同摘要幂等，不同摘要 CORRUPT_ARTIFACT。GC 水位取所有有效 reader 已提交 cursor 与最后恢复点的最小安全前沿；prepared/unknown effect/sink 引用保留。crash 后 orphan reconciliation 只删本 run 未发布、无可恢复引用对象。存储满则背压或明确失败，不自动提高预算。

## C06 Kernel 语义

kernel 输入、输出采用 Record，不裸搬 Python 任意活对象。调用线程由 Worker executor 拥有，控制 loop 独立。同步接口实现可以内部运行既有异步模型 runtime，但必须在专用业务执行上下文内，不嵌套用户 event loop。

join 保持 inner/left/semi/anti，null 不匹配，重复键多重性；默认不承诺全局顺序。reduce_by_key 保持组内序列，不擅自 combine；group_batches 的 processing-time flush 与完整分组分开，快照包含未齐 batch 与 timer 剩余逻辑状态。sort 的 null/descending/multi-key 与已有公开约定一致；相等键用稳定 record provenance 作为确定 tie break，不把该 tie break 宣称业务次序。shuffle(seed) 绑定随机键与 occurrence/source position，恢复和后端切换不改变同一 record 的随机键；全局置换需要 finite scope。

AggregateFnV2 只有明确 associative merge 的接口可局部分解；State 上限包含 merge 前后共存副本。map_batches 零拷贝承诺不可虚报，写时拷贝需先准入。group/join 倾斜、flat_map 扩增、单行超限必须触发真实受限算法或有界失败，不能截断输出。

## C07 Worker、宿主和控制

WorkerAssignment 绑定 plan/grant/code/payload schema，launch 时校验全部字段、当前 boot 和 provider capabilities。artifact verify→restore→ready→activate，activate 前不读业务输入、不调用模型。worker 只执行分配 stage/partition，不能因遇到未支持算子调用 LocalDatasetExecutor 或 Ray Data 兜底。

HostEndpoint.poll 有 max_events/deadline，不能持有无限事件 backlog；progress 可聚合，cut/effect/failure 不可丢。每条请求 operation_id 可重试且必须相同内容；冲突内容 INVALID_CONTRACT。断连期间不能再获得外部效果准入；已获准效果的结果可在有限本地 spool 中保留，重连后提交同 effect_id。grant 到期停止新工作，退出确认前资源不能计为释放。

## C08 checkpoint、buffer 和提交

CutRequest.participants 是完整 stage-partition 实例集合，含空输入与已结束分区；每 CutContribution 由当前 boot 产生，重复相同摘要幂等，冲突拒绝。buffer 状态包括输入 port、sequence、待输出、batch、group、预取引用及占用预算；引用的行必须先 durable，不能把 Ray ObjectRef 或 Python object id 放入永久状态。

final 是 driver 根据声明FiniteScope的完成条件（全源EOI或terminal_limit满足）与sink事实推导，worker 可报告本分区 finished，但不能单方面将全局 cut.final 写成 true。barrier 对齐不能让 post-cut 数据提前进入不可回滚效果。aligned snapshot 保持 source vector/operator/buffer/channel/effect/sink 一致；checkpoint 超时失败不把部分 cut 当可恢复。

唯一 CommitCoordinator 的原顺序不变，SourcePosition 桥详见架构 §6。CutContribution.sinks 是已封存 prepared 输入，不是 worker 自行发布的成功回执。driver 确认 cut 的发布/进度后返回 receipt，再释放 post-cut。队列ACK由C16原子finalize完成，cut只引用并核实原生fact，不能在receipt后重复ACK。driver 恢复先 reconcile prepared/unknown sink，再恢复业务，不先重新运行 source/UDF。

## C09 效果与远程 journal

WorkerJournalPort 不暴露 begin_attempt、bind_execution_action、complete_execution_action、commit、SQL、publication_guard。effect 命令使用原 Prompt/Embedding/Agent/Fetch 身份与原账本；不创建额外数据库。各 adapter 保留原 request_policy、预算权威、raw CAS/queue 协议。

effect 状态：absent→reserved→sent→confirmed；reserved→unsent 仅凭确定未发送证据；sent→unknown 不能重置 absent。相同 effect_id 的 request_digest 不同必拒。`mark_sent` 的持久确认先于网络发送；confirm 引用结果 artifact 必须能恢复读取。结果已确认但响应丢失，lookup 重用；未知结果不能通过更换 actor/worker/attempt 重计再调用。

DriverJournalService 只负责校验和调用原 journal/effect 实现；不能提供泛化方法代理。控制事件中的 payload_ref 只引用有界命令，大小超过 max_control_bytes 拒绝。远程接口不承诺 transport exactly-once，通过 operation_id 和原生 fenced transaction 幂等。

## C10 工件与跨节点读域

不可变工件包括平台 revision、依赖/运行环境摘要、业务模块/按值程序、source声明、operator config。worker 必须自己校验导入代码，不信 driver 发送的旧 digest。不可从绝对路径自动 import 当前主仓。传输分块有总量/单块/并发限额，下载临时文件完成验证后原子发布，失败清理只删自己拥有的 staging。

shared store 认证必须跨不同 host_id 完成写入挑战/读取/digest/namespace 验证，要求故障后仍可用；Ray object 只负责短期传输。checkpoint 所有数据必须有 durable artifact 引用。owner node 丢失后不能凭 ObjectRef 重新计算未知付费效果。driver journal 在其受保护持久目录，独立于 worker cache；worker 没有该数据库文件的读写依赖。

## C11 API 和 terminal

有效 API 的参数、返回值和错误类型兼容矩阵逐一冻结。take(0) 在不读源、不构造 actor 情况返回；take(n) 收够 n 后关闭本 action 的必要输入，不把未执行 suffix 当完整成功，已发生效果仍落账。iter_rows/iter_batches 在 generator.close、异常和 GC 后走明确释放；GC 不是唯一清理机制。take_all/collect_summary 必须先检查 driver 返回总量预算；超限不截断。

materialize 返回现有 MaterializedDataset 语义，存结果引用、类型、schema 和总量，不隐式恢复不同程序；release 只释放当前持有引用，不删除其他 run 依赖。write_file 要声明仅交付或原子发布保证；datasink 四方法才有 final commit。run_stream 的显式 checkpoint 参数保持退役；不能恢复旧 checkpoint API 凑覆盖。native Ray tuning 参数的迁移在 API-MATRIX 中逐项判定，不忽略参数。

## C12 历史兼容

历史 v1/v2 原文、digest、identity 不变。只读历史不执行用户代码。恢复先核平台/程序/源/sink/state/恢复保证；支持明确列出的原生 v1/v2 adapter，未知格式/缺原程序/运行资源不可恢复报 INCOMPATIBLE_HISTORY。不能以“新 graph_id”顶替历史 action；新运行用新域。原环境才能恢复的历史运行保留原引用，不由当前引擎静默运行旧 Ray Data。

## C13 产品契约

workbench 的 graph、source、parameter、observation、result 全绑定同一 action/attempt/dag_id/plan_id。物理组、融合、CPU/GPU、通道模式、恢复能力可解释；未观察节点明确无独立观测，不推算成功。配置修改产生新检查/新运行，不改历史封存。停止/恢复沿原 manager 控制，不能编辑状态文件冒充停止。

## C14 文件所有权与测试替身

同文件一个 lane owner；每函数一个任务 ID，可在该 lane 内分给独立 agent。跨 lane 对 `.pyi`/样例写消费者；缺模块时仅测试域允许 spec-bound fake，禁止生产 fallback。所有新生产符号必须有已授权修改的 caller 或唯一 registry 装配入口；仅有 verify_preserve caller 不合格。registry 表由对应工厂任务拥有，禁止 agent 各自添加第二张表。

## C15 验收与证据

结构 checker 验证所有叶、调用边、API覆盖、文件权限、旧路径清理和 schema；不能代替真实运行。每项实现证据包含 commit、完整命令、进程内模块路径、源码摘要、测试 nodeid、结果。缺环境/依赖/凭证记 not_run，不算 pass。新全链路在禁止 Ray Data 的进程中运行；两 host/GPU 性能有独立环境标签，替身和单机不能填该栏。

## C16 范围与契约变更

本包不授权生产运行/部署/主仓合并。契约冻结后任何跨边界字段、签名、状态迁移改动都必须带具体反例和受影响 producer/consumer 清单，由集成 owner 一次更新契约/样例/任务；无需暂停无关 lane。不得用逐函数实现过程中反复 Review 代替最后整体验收。Qwen 分发、GPT/Qwen 最终 Review、GPT 统一修复的用户工作方式保留。

纯关系/排序 kernel 的恢复保存 durable 输入范围及确认输出 cursor，允许从固定输入重建纯计算；不 pickle DataFusion 活 session。模型 cache key 与原生 effect_id 分开：批装箱不变性仅修改新 cache 域，不能改历史请求/付费效果身份。

## 第三轮补充：远程与快照闭合

- SourceCursor.offset 是下一次源读取位置。已经读取但未进入有序输出的预取内容必须在 pending_refs；channel snapshot 保存已发前沿。恢复先重放pending_refs再从offset读取，不能同时从旧offset重读这些记录。reader实现不能把“下一个未确认位置”与“下一个未读取位置”混称。
- WorkerEvent.payload/payload_ref恰有一个；小控制命令inline且不超过max_control_bytes，避免申请artifact配额前先要写artifact的循环。大快照只传refs。ControlReply同样有界，request_id严格匹配。
- ArtifactCommand只允许pin/unpin/inspect/reserve_staging，driver校验owner属于当前assignment；只有driver调用RunJournal.bind/release_artifact_owner。worker通过WorkerJournalPort.artifact请求，不能直开SQLite。
- K3 assignment保留原字段集合：assignment_id/operation_id/scope_id/allocation_id/boot_id/worker_id/grant_ref/controller_epoch/plan_revision/expected_plan_revision/route_epoch/topology_revision/artifact_manifest_ref/stages/partitions/channels/restore_point/ready_deadline_ms/schema_version。v3 assignment存K1引用的工件，不把新字段塞到旧validator。
- load_execution_version直接读取journal指向的原始封存文档，不回调load_sealed_plan/load_dataflow；避免版本分派递归。build_plan_view纯读入参graph/physical/observations，不进行I/O或重新编译。

## 第四轮补充：有限范围与wire边界

- take(n)成功表示本action的terminal_limit范围满足，不声称整个来源EOI。TerminalResult.completion区分source_exhausted/terminal_satisfied/cancelled/incomplete；begin(final)校验该FiniteScope的完成证据。未消费queue claim不ACK，部分claim按旧队列协议保留/释放。finite_sealed的seal依然只对其声明范围membership成功成立，不能把下游limit取消冒充全源seal。
- take(0)编译成显式literal_empty source + terminal的零读计划；不能在prepare中解析Lance head/SQL snapshot或构造actor。没有该显式证明的无source图仍非法。无限来源接需要finite_complete的节点，必须提供有限scope，否则准入拒绝，不能永远等EOI。
- NativeOperation仅含kind/config/bindings/CallableSpec factory，无活client/model/journal；声明归data模块，G维护类型，F构造，M在worker装配。所有运行时类型导入用TYPE_CHECKING/延迟注解，contracts不可反向导入run_context/runtime造成循环。
- encode_wire/decode_wire只能处理封闭的持久DTO白名单。ActionRequest、RuntimeServices、DriverServices、CallableInstance、各Protocol及本地句柄是进程内类型，禁止序列化；程序工件经已有code-pin路径。顶层wire为{schema:demiflow.runtime_wire.v3,type:固定类型名,payload:闭合字段}，不得动态import type字符串。

## 第六轮补充：纯编译到程序封存

`capture_graph/lower_action`返回CompiledDataflow（graph+ProgramDeclaration），不是假装已存在程序工件的Graph。ProgramDeclaration为进程内类型，含纯CallableSpec/SourcePlan，不进wire。`stage_dataflow_programs`是编译之后、物理编译之前唯一写入程序的步骤；graph中的optional ref此时必须补齐。程序身份来自code/config语义，store路径和attempt不影响逻辑身份；seal前验证所有可执行节点有真实工件。禁止全局pending-program缓存来弥补返回值缺口。

v3程序封装复用已有业务模块收集、按值程序归档及code pin计算；普通CallableSpec/原生factory按真实入口收集依赖。旧stage_plan_artifact/verify_loaded_plan的历史算法不改，v3 loader校验其自己的声明及实际加载代码。程序与数据有不同媒体类型，SourcePlan中的iterator工厂也必须进入代码依赖集。

seed=None生成一次运行常量，存SealedExecution.constants_ref，同时进入PhysicalDataflow.constants_ref；worker RuntimeServices.constants只读。显式业务seed仍在逻辑身份。随机重分发与全局随机排序分别使用对应稳定键，不能恢复时重新随机。

### C16 补充冻结：原生队列/RawFetch 服务（第七轮源码复审）

这里不是普通 checkpoint 之后再发 ACK。`queue_source.QueueDatasource.acknowledge` 明确拒绝普通 ACK；`queue_execution.finalize_task` 在原 task scope 原子提交结果并 ACK。新 `ack_source_claims` 仅验证该原生事实，不承担第二结算。

- driver 上唯一 `NativeAcquisitionService` 持有原 QueueDatasource、RawFetch、lease/heartbeat/原生 namespace；普通 fetch 同样走该服务。worker 只处理有界的 row/ref，不能实例化这些会直连 journal/CommitCoordinator 的原生对象。driver 的获取 IO 线程池与驻留受同一资源账本约束，独立于控制轮询；这是一处受管的外部效果/结算端点，不是旧 executor 计算回退。
- `configure` 自动 sink 模式严格复用 `validate_topology`；显式 ACK 严格复用 `configure_explicit`。该协议当前只允许一次匹配 RawFetch、纯一对一转换/保留必要字段和匹配 QueueResultSink。不会因为统一执行而悄悄允许 filter/drop/模型任意插入原子队列结算。需要模型的数据读取已确认历史 receipt 再进入普通统一图；这是业务结算边界，不是为调度添加 materialize。
- 所有 QueueCommand.operation_id 由 action/occurrence/record/delivery epoch/op 确定；poll 重试返回同一领取结果，不能再次 claim。delivery_ref 原内容含 generation/task/fingerprint/epoch/lease owner，只由 driver 解引用为原对象；worker 不得修改。acquire/finalize 必须有 row_ref；poll/status 不得携业务payload。
- 原 RawFetch 的 start/call/close 在原 `current_action`/`current_row_scope` context 调用。获取对象直接存原 RawNamespace，跨 host 仅经已验证 read_domain 的共享持久 store 读取；未配置可达域在启动前拒绝。
- 原生 acquisition/final scopes 仍为唯一事实；数据流 cut 引用它们，不再对同一任务创建 sink commit。finalize 回执丢失查原事实；未知请求保持 unknown，不重下载或提前释放租约。action 完成必须 `assert_settled`，空开放池是 pending；没有 task 的已 drain 池可合法 EOI。
- worker 只有 `queue` 这一个有限 RPC 类别，driver dispatch 校验 run/attempt/fence/scope/allocation/boot/controller_epoch/plan_revision/授权 binding。不得 generic method forwarding，不新建 queue DB。

## 第八轮补充：真正可接线的控制应答与物理计划

- `PhysicalDataflow.residency/scopes` 是 `solve_residency/plan_exchanges` 的封存输出，不可计算后丢弃。scheduler只消费该布局，不能自行猜消费者保留容量；scope membership完整且有限。
- Ray driver的RunAction/RunJournal不pickle到actor。`RayControlTransport`用当前worker内的有界Mailbox；`HostedDataflowWorker.poll`让driver拉取事件，`DataflowExecution.pump`提交到有限DriverJournalService队列，后续poll_replies返回CompletedControl，再由对应HostEndpoint.deliver_reply送到同一boot的worker。Local用同一请求/回执契约和socket。所有业务等待在业务线程，actor控制方法保持可响应。
- dispatch只在服务队列的有界执行上下文运行；pump不能同步等待queue acquisition/SQL/模型结果。submit返回False不代表操作失败或未执行，只表示本次尚未入队；相同operation严格查既有inflight/原生幂等事实。close有期限且unknown保持。
- MaterializedDataset.release维持现有契约：同一handle的派生视图此后不能执行；另一独立handle/run自己的pin不受影响，不能误承诺所有派生图release后仍可运行。

### 原生模型适配逐方法冻结

`EmbeddingActor`没有Prompt的coordinator。它在`native_request_budget`→`journal.reserve`路径将预算与intent交原RunPromptJournal同一savepoint处理；这是必须保留的事实。M的RemotePromptJournal实现原client实际消费的九个窄方法；H只提供NativeJournalCommand列举的八种操作，不能把任意journal方法暴露出来。

Prompt与Embedding分别适配：RemotePromptRecovery复用原bind_client的record/execute/decode包装；RemoteEmbeddingRecovery复用原scope_request/bind_actor/drain，重写membership到driver。构造不调用原会开SQLite的构造器；driver唯一持有原RunPromptJournal。CALL_RESERVATION包含reservation及requests/node额度，跨线程和RPC显式携带；不得把它拆成先reserve_budgets后写intent两个独立事务，也不得因泛化EffectCommand再扣一遍。

NativeActionView/NativeRunView为进程内只读最小视图，不伪造完整RunAction。当前`NativePromptRecovery.bind_client`只读action；原构造中的native_image_effects赋值在构造view时确定。image使用原对象参与者的端口，不调用依赖driver.current_action的原bind_execution分支。agent/Codex不强行冒充row-local可恢复；infer_recovery按真实效果与会话能力返回限制，普通完成路径仍须Local/Ray等价。

NativeJournalCommand.reserve携reservation，response/failed携value_ref，lookup/references不得携结果；media_references的register只允许bool；bind_membership复用原embedding-request-membership键。request_key从原RequestRecord复算核对。reply.value与value_ref最多一个非空，None是合法lookup miss；unknown以DataflowError返回而非None。remote wrapper保留原异常类型的映射，不能吞进普通cache miss。

`verify_dataflow_program`输入必须是可传输ref+expected_pin；不能让worker接收禁止wire的ProgramDeclaration。输出LoadedProgram是进程内对象，在RuntimeServices.programs按occurrence_id只读登记。source source_id对应其图occurrence，SourceReader从该表获取已验证SourcePlan/工厂，不能仅凭snapshot猜构造来源。make_runtime_services先验证bundle/程序再产生完整services；load对同一assignment验证绑定，不执行构造器。LoadedProgram绝不进wire。

## 第九轮补充：终端程序不是 Json 参数

ActionRequest是进程内声明，sink保存真实Datasink声明对象，programs为有名称的ActionProgram（aggregate/sink），不提前伪造sink_ref。aggregate的函数对象不能放arguments.Json中；arguments只放名称/次序/有限值，程序在programs中保存。ProgramDeclaration.callable_spec/source_spec/action_program三者按kind互斥，lower_action将终端程序登记，stage_dataflow_programs统一封存；只能序列化纯配置/工厂，活连接/锁/FD须预检拒绝并指明迁移点。sink已存在的prepare/publish/reconcile/abort能力不由JSON猜测。

原HTTP client在构造期检查http_error_retry必须有可写有限journal；不能事后bind再期待它启动。M拥有client构造器/工厂与runtime.call_with_trace三张接线卡：内部新增keyword-only native_journal=None，旧调用签名保持可用；先绑定经固定能力检查的原生remote journal，再执行原retry校验。不能删除该守卫或将任意duck对象视为可信journal。显式历史private SQLite写恢复不得悄悄与原生恢复双写：当前迁移要求在预检指明冲突并提供原历史只读引用；普通无私有journal的Prompt/Embedding与原生恢复是必交范围。此兼容限制须在配置检查中可见，不计成“所有旧options无差异”。

## 最终装配顺序

make_driver_services按RunAction→DriverArtifactPort（不依赖store）→RunArtifactStore→DriverJournalService→DriverServices构造，不用未初始化全局变量绕环。DriverServices.journal_service提供明确submit/poll/close。prepare创建并configure原生获取服务后调用bind_queue，随后start才启动它。driver与worker的artifact事务共用apply_artifact_command；worker入口先验grant，本地driver端口只允许当前action，二者仍写同一原journal。

schema/columns必须遵守既有fetch_if_missing参数：只读已有元数据的检查不执行UDF；用户明确允许取缺失schema时，可经统一有界action计算，错误与效果同样记录，不能在编译器内部偷偷调用。一次性不可重读源的完整性不能靠前缀spool证明，详见C04。

## prepend不是隐藏子执行

expand_operation_inputs将StreamPrependOp.source_dataset捕获为同一action的prefix端口，主输入为main；prefix先完整校验max_rows/max_row_bytes并按有限范围完成，再按声明顺序接main。prefix可以与其他分支共享上游，不能在worker运行中创建新Dataset/RunAction。原stream._make_worker隐式执行prefix分支与source_snapshot.prefix_rows退役，旧prefix snapshot仅按历史读取合同保留。

## 总控契约修订记录

### AMEND-001 · C01 错误码闭合集补入 CAPACITY_PENDING（2026-11-15，总控裁决）

**触发**：lane X 提交最小裁决请求。C01 同一段先闭合列出 13 个 code，紧接着又规定 `CAPACITY_PENDING` 的语义（"可在原有限 deadline 内等 credit，不重新执行业务调用"），但该词不在枚举内，且在整个设计包中仅出现这一次。C006 要求"code 闭合集、未知 code 拒绝"，C075 要求"枚举必须按 CONTRACTS，不得由消费者自行扩展"，C05:43 又要求"存储满则背压或明确失败，不自动提高预算"。

**反例（可复现）**：lane C 忠实实现 13 码闭合 frozenset（`contracts/dataflow_errors.py:16-30`），构造器对未知码 `raise ValueError`（:143）。lane X 忠实按 C01 散文实现 credit 背压。⇒ 集成树用真实 C 跑 X005 背压路径必然 `ValueError`。**两个 lane 都无缺陷，是冻结合同自相矛盾。**

**总控独立查证的既有权威（决定性依据）**：
- `demiflow/resources/contracts.py:18-28` 的 `ERROR_CODES` frozenset **已含 `CAPACITY_PENDING`**；该文件是七份**字节冻结**文件之一，`git diff 70e357c..HEAD` 为空，任何 lane 不得修改 ⇒ 它是不可争辩的基线权威。
- v2 `demiflow/execution/exchange/channel.py:49`：`raise ProtocolError("CAPACITY_PENDING", "channel credit exhausted", retriable=True)` —— **正是 X 所需的 credit 耗尽可等待背压语义**。
- `demiflow/resources/local.py:156/174`、`ray.py:173` 捕获该码后返回 `PENDING/FRESH` + `conditions=["capacity_pending"]`，即**等待而非重执行业务、而非抬预算**。
- `resources/device_pool.py:276`、`capacity.py:170/180` 同样以该码报告设备/scratch 容量暂时不足。

**裁决**：枚举遗漏属**设计转录缺陷**，不是有意排除。补入 `CAPACITY_PENDING` 使枚举与同段既有散文、以及冻结基线权威和 v2 既有行为一致。**不是新增能力、不是放宽任何 guard、不是消费者自行扩展。** 闭合集由 13 码变为 **14 码**，其余全部不变。

**语义固定**：见 C01 新增的边界段（暂时不足可等待 / 原 deadline 不延长 / 不重执行业务 / 不抬预算；与 BUDGET_EXCEEDED 终态硬拒绝、DEADLINE_EXCEEDED 原 deadline 已耗尽三者不得互替；UNKNOWN_EFFECT 永不可 retryable；未知 code 仍拒绝）。

**执行分工**：
- 总控：修订本文件 C01 枚举与边界段（本 commit，仅文档）。
- **lane C**（`dataflow_errors.py` 唯一 owner）：在 `DATAFLOW_ERROR_CODES` 增 `"CAPACITY_PENDING"` 一行；强化 C006/C075 反例（CAPACITY_PENDING 可 retryable；UNKNOWN_EFFECT+retryable 仍拒绝；未知 code 仍拒绝；闭合集 14 码）；追加新 commit，重跑 G-C。不改任何字段/身份/wire 白名单。
- **lane X**：无需改代码（其替身已按 C01 散文含该码）；集成树用真实 C 重跑 G-X 时该路径自然成立。
- 消费 credit/容量背压的 lane（B/D/K/M/E/H/P）：按上述固定语义使用，不得自行扩码。

**不改的东西**：七份字节冻结文件、任何 DTO 字段、任何身份公式、任何资源额度、任何既有 guard。
