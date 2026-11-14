"""Human-authored function contracts. Generation only formats these records."""
GROUPS=[]
def group(lane,file,contract,intents,rows):
    for line in rows.strip().splitlines():
        symbol,rest=line.split(' | ',1)
        signature,behavior,counterexample=rest.rsplit(' | ',2)
        GROUPS.append(dict(lane=lane,file='demiflow/'+file,contract=contract,intents=intents.split(','),symbol=symbol,signature=signature,behavior=behavior,counterexample=counterexample))

group('G','planning/dataflow_compile.py','C02','I01,I05,I14',r'''
capture_graph | (request: ActionRequest, *, bounds: Bounds) -> CompiledDataflow | 遍历真实 source/PlanInput/LogicalPlan，使用共享前缀 token 建完整多端口 DAG；只登记不执行 | diamond 共享前缀计一次，两个独立 map 同函数仍计两次；cycle 在读源前拒绝
bind_occurrences | (nodes: tuple[GraphNode, ...], edges: tuple[GraphEdge, ...], *, action_id: str) -> tuple[GraphNode, ...] | 以稳定遍历位置和共享引用建立 v3 occurrence，host/attempt 不参与逻辑身份 | 序列化重建、不同 PYTHONHASHSEED 得同身份；左右 join 交换必须改变身份
compile_kernel_spec | (operation: LogicalOp, *, program_ref: ArtifactRef | None, bounds: Bounds) -> KernelSpec | 按唯一算子注册表翻译真实操作参数/state/effect/traits，不实例化 UDF | 带副作用构造器计数保持零；未知算子明确 UNSUPPORTED
expand_relational_inputs | (source: SourcePlan, *, path: tuple[str, ...], bounds: Bounds) -> tuple[PlanInputBinding, ...] | 展开 KeyedSource 的 left/right、UnionSource 的序号端口，保留真实输入关系 | 两侧同 schema 不可混端口；三个 union 输入保持 multiplicity
validate_graph | (graph: DataflowGraph, *, bounds: Bounds) -> None | 校验 DAG 无环、端口/节点唯一、源终端可达、所有边引用有主、身份可重算 | 悬空端口、重复 occurrence、只有终端没有源、超深链都在业务前失败
lower_action | (request: ActionRequest, *, bounds: Bounds) -> CompiledDataflow | 将终端的收集/统计/写出语义编入图，take0 不增加 source read | schema fetch_if_missing=False不执行；True且未知时经统一action有界探测；take0 不创建模型；write sink 未注册不能启动
''')
group('G','planning/dataflow_registry.py','C02','I01,I09',r'''
operator_registry | () -> Mapping[str, OperatorDefinition] | 单一不可变登记全部现有 LogicalOp 和原生专用 kind，绑定 compiler 与 kernel factory 名称 | 公共操作存在而 registry 缺项时 gate 失败；不允许 runtime 动态注册旁路
source_registry | () -> Mapping[str, SourceDefinition] | 单一登记 SourcePlan 与队列/文档回执适配，不持有源实例 | 每个公开 read/from 入口有对应 kind 或明确 metadata adapter
terminal_registry | () -> Mapping[str, TerminalDefinition] | 精确登记 C02 所有终端及返回值/恢复要求，使用同一调度入口 | 写出不能误路由为普通 take；未知字符串不 fallback
''')
group('P','planning/dataflow_place.py','C03','I03,I04,I08',r'''
compile_physical_dataflow | (graph: DataflowGraph, snapshots: tuple[SourceSnapshot, ...], splits: tuple[SourceSplit, ...], *, policy: RuntimePolicy, hosts: tuple[HostDescriptor, ...], bounds: Bounds, constants_ref: ArtifactRef) -> PhysicalDataflow | 统一组合 stage 分组、分区、Exchange、资源可行性与恢复保证，输出唯一布局 | 同一图 Local/Ray 结果语义相同，GPU 无可用资源不可退成 CPU 成功
partition_stages | (graph: DataflowGraph, *, policy: RuntimePolicy, bounds: Bounds) -> tuple[StageSpec, ...] | 按 kernel role/profile、状态、effect、多输入和边界拆 stage，有限副本 | CPU→GPU→IO 不能固定 chain-group；stateful map 不自动扩成多个共享实例
select_fusion_groups | (graph: DataflowGraph, stages: tuple[StageSpec, ...], *, policy: RuntimePolicy) -> tuple[StageSpec, ...] | 只融合纯同资源线性节点，保留 occurrence 和累计同时驻留预算 | 两 stage 各占满 run 预算时不可融合后两倍放行；模型初始化不复制
plan_exchanges | (graph: DataflowGraph, stages: tuple[StageSpec, ...], *, policy: RuntimePolicy, bounds: Bounds) -> tuple[ExchangeSpec, ...] | 输入 requirements 决定 sealed，durability 决定 durable/default memory，固定理由与读取域 | incremental map 不插 finite seal；global sort 不能 memory 增量冒充完成
solve_residency | (stages: tuple[StageSpec, ...], exchanges: tuple[ExchangeSpec, ...], *, hosts: tuple[HostDescriptor, ...], bounds: Bounds) -> ResidencyPlan | 计入队列/输入/输出/转换共存/状态及控制余量，证明最小可运行集合 | 生产者占完 slots、消费者不能启动必须准入失败或合法 durable 分阶段
validate_physical_dataflow | (graph: DataflowGraph, physical: PhysicalDataflow) -> None | 校验每 occurrence/edge 精确覆盖、预算闭合、分区/路由/持久域与恢复保证一致 | 未使用 stage、两 owner、未知 read_domain、source snapshot 漂移拒绝
infer_recovery | (graph: DataflowGraph, snapshots: tuple[SourceSnapshot, ...], *, policy: RuntimePolicy) -> CapabilityReport | 由源/state/effect/sink 能力计算最弱可证明保证与原因，不暴露可伪造成功开关 | 非可恢复资源 actor 配 required checkpoint 拒绝；未知付费结果不可判可重试
''')
group('P','planning/dataflow_policy.py','C03','I04,I08,I10',r'''
resolve_runtime_policy | (options: Mapping[str, Json], limits: Mapping[str, Json], *, provider: Provider) -> tuple[RuntimePolicy, Bounds] | 唯一旧 limits→有版本 policy/bounds 映射，输入收紧与未知字段拒绝 | bool 不当 int；0 不当无限；各消费者不得注入不同默认值
normalize_native_options | (options: Mapping[str, Json] | None, *, operation_kind: str) -> Mapping[str, Json] | 逐项翻译有语义参数到统一 profile/partition，Ray Data 内部调优显式报告不支持 | num_cpus/GPU 等有效参数生效；未知 native_options 不能静默丢弃
''')
group('S','execution/dataflow_sources.py','C04','I01,I06,I09',r'''
snapshot_source | (source: SourcePlan, *, action: RunAction, store: ArtifactStore, bounds: Bounds) -> SourceSnapshot | 冻结版本与成员，元数据有界；不可重读选 spool/attempt_only，不全源 list | 大 Lance 首条前不读取全部 fragment；mutable head 只绑定一次
plan_source_splits | (snapshot: SourceSnapshot, source: SourcePlan, *, target_partitions: int, bounds: Bounds) -> tuple[SourceSplit, ...] | 构建有界 splits 与起始 cursor；普通源读取放 worker，lease_queue按C16驱动端领取描述；自定义 ReadTask 复用旧契约 | 同 partition 恢复稳定；数百万文件清单用 manifest paging 不全塞控制包
open_source_reader | (snapshot: SourceSnapshot, split: SourceSplit, *, cursor: SourceCursor | None, services: RuntimeServices) -> SourceReader | 唯一 source registry 装配已冻结 reader，按域读取并校验 | source_changed 在业务效果前失败；不从当前最新版本重读
verify_source_snapshot | (snapshot: SourceSnapshot, *, store: ArtifactStore, bounds: Bounds) -> None | 恢复前校验固定版本/清单/配置摘要和可读域，拒绝未知 snapshot | 同路径内容替换不接受；空文件集仍可合法结束
spool_record | (record: Record, *, split_id: str, sequence: int, store: ArtifactStore, bounds: Bounds) -> ArtifactRef | 一次性来源逐条/段先持久再发布，稳定 key 幂等，返还占用按确认前沿 | crash 在写完未发布可回收；重复发布不能重复扣配额
''')
for cls,detail,bad in [
 ('ItemsReader','已物化列表或 Arrow/numpy/pandas block，按引用与索引读取，保持类型','tuple/bytes/-0.0 和空数组不漂移'),
 ('RangeReader','range 起止与 split 确定分片，无全量展开','take0 不生成元素；恢复不能重复边界'),
 ('FileReader','Parquet/CSV/JSON/JSONL/text/binary/images/records 共用固定清单和格式解码器','压缩不可 seek 时固定重读；损坏行保持 read_records 的 error/raw'),
 ('LanceReader','固定版本 fragment/row offset，投影过滤检索保持原语义','删除当前 head 不影响固定版本；global top_k 不变为每片 top_k'),
 ('SqlReader','固定 snapshot+稳定 keyset，不能保证时由 snapshot 标明 spool','断线重连不能拼接新事务快照'),
 ('TaskReader','现有 Datasource/ReadTask 的有界执行和声明 cursor','snapshot_identity None 不伪装可恢复'),
 ('IteratorReader','在 worker 调用零参 iterator factory；可重开且有明确cursor才增量恢复，不可重读源须完整capture后sealed或attempt_only','关闭生成器一次；捕获 executor 的 closure 预检定位'),
 ('QueueReader','只调用C16 driver queue服务领取描述，worker无原QueueDatasource；空开放池pending，native final原子ACK','idle 不发 EOI；lease 失效不能确认旧任务'),
 ('HistoryReader','queue records/legacy/document receipts 固定历史快照只读','不得领取任务、修复历史或调用 fetch')]:
 group('S','execution/source_readers.py','C04','I06,I09,I13',f'''
{cls}.__init__ | (self, snapshot: SourceSnapshot, *, services: RuntimeServices) -> None | {detail}；持有 snapshot/services/当前 cursor，构造不读源 | {bad}
{cls}.open | (self, split: SourceSplit, cursor: SourceCursor | None) -> None | 验证 split 归属和 cursor，建立有限 reader；初始化状态不能产生业务效果 | 错 split/版本拒绝，不从头隐式重跑
{cls}.next_record | (self) -> SourcePoll | 每次最多推进一个有界记录，携带稳定 record_id/sequence/offset；pending 与结束按 C04 表示 | {bad}
{cls}.snapshot | (self) -> SourceCursor | 返回下一次源读取位置与已读未发pending_refs；输出前沿另在channel，禁止把两者混用 | 已消费未确认和已确认不能混为一项；空源 cursor 有定义
{cls}.close | (self) -> None | 幂等关闭 reader/解码器/自有句柄，未确认 queue claim 留原恢复机制 | 取消后不泄漏 FD，不 ACK 未提交数据
''')
group('K','execution/kernels/dataflow.py','C06','I01,I05,I09',r'''
make_kernel | (spec: KernelSpec, *, services: RuntimeServices, partition_id: str) -> Kernel | 唯一工厂按 registry 装配普通/map/batch/关系/顺序/聚合/原生模型 kernel | 未知 kind 在 start 前失败，不能调用旧 executor 兜底
invoke_unary | (spec: KernelSpec, record: Record, *, instance: CallableInstance, services: RuntimeServices) -> Iterator[Record] | 保留 map/filter/flat_map/列变换/sample/limit 的逐条语义与子记录 provenance | flat_map 零输出不等 EOI；10000倍展开受输出预算而非无限 list
invoke_batch | (spec: KernelSpec, records: tuple[Record, ...], *, instance: CallableInstance, services: RuntimeServices) -> Iterator[Record] | batch_map 与 map_batches 分离，保留 batch size/flush/format/zero-copy约定 | Arrow cast 前后共存预算；不可改 batch_size 使测试变绿
finish_kernel | (kernel: Kernel, *, port: str, services: RuntimeServices) -> Iterator[Record] | 输入 EOI 触发剩余 batch/组输出，全部输入齐后才关闭 kernel | join 一侧结束不能结束另一侧；timer flush 不重复末批
''')
group('K','execution/kernels/relational.py','C06','I01,I05,I08',r'''
join_partitions | (left: Iterator[Record], right: Iterator[Record], *, spec: KernelSpec, services: RuntimeServices) -> Iterator[Record] | 同 key 分区的两侧交给受限 DataFusion join，保留 inner/left/semi/anti/null/重复语义 | 倾斜键笛卡尔扩增按输出预算失败，不偷偷 dedup；不额外全局排序
reduce_partition | (records: Iterator[Record], *, spec: KernelSpec, services: RuntimeServices) -> Iterator[Record] | 外部分组按 provenance 组内排序后调用用户 reducer；不猜结合律 | 非结合减法 reducer 与单流结果一致；大组磁盘有界
union_inputs | (inputs: tuple[Iterator[Record], ...], *, spec: KernelSpec, services: RuntimeServices) -> Iterator[Record] | 明确多输入调度和记录多重性，引用共享不等于业务输出去重 | 同一个上游被 union 两次，源可执行一次但输出必须两份
emit_groups | (records: Iterator[Record], *, spec: KernelSpec, services: RuntimeServices) -> Iterator[Record] | 完整组与定时在线组分别消费；组状态/timer 纳入 kernel snapshot | batch 未齐 crash 恢复不丢行；负零/bytes key 不折叠
''')
group('K','execution/kernels/ordering.py','C06','I01,I03,I08',r'''
build_sort_runs | (records: Iterator[Record], *, spec: KernelSpec, services: RuntimeServices) -> tuple[ArtifactRef, ...] | 有界内存创建稳定排序 runs，磁盘预留后写入，key/null/multikey兼容 | 单行超限先拒绝；run 数有界分页索引，不无限 FD
merge_sort_runs | (runs: tuple[ArtifactRef, ...], *, spec: KernelSpec, services: RuntimeServices) -> Iterator[Record] | 有限 fan-in 多轮 merge，排序比较/tie break 稳定，引用归属可回收 | run 比 FD 多仍可完成；中途失败只删未发布文件
shuffle_key | (record_id: str, *, seed: int, occurrence_id: str) -> bytes | 固定版本 keyed hash 生成全局随机排序键，独立于 host/attempt/到达顺序 | 两 provider/重试/分区数变化同 record 得同键
repartition_records | (records: Iterator[Record], *, exchange: ExchangeSpec, services: RuntimeServices) -> Iterator[RoutedRecord] | hash/rebalance/target 经统一 Router；shuffle=True 的完整重排按语义调 ordering | Python hashseed 变化不漂移；resume ordinal 不能清零
''')
group('K','execution/kernels/aggregate.py','C06','I01,I08',r'''
accumulate_partial | (records: Iterator[Record], *, spec: KernelSpec, services: RuntimeServices) -> AggregateState | 调原 AggregateFnV2 或 count，累加前后按 state预算保护 | 空输入 count0；超大用户 state 不能先复制再拒绝
merge_partials | (partials: Iterator[AggregateState], *, spec: KernelSpec, services: RuntimeServices) -> AggregateState | 仅 mergeable 实例分解；确定 partition 顺序合并并保留数学约定 | Std/Mean 空值和合并误差按明确容差；自定义非结合不强拆
finalize_aggregate | (state: AggregateState, *, spec: KernelSpec) -> TerminalResult | 恢复原 aggregate 名称唯一性及 sum/min/max/mean/std 返回语义 | 重名先拒绝；all-null/空输入不伪造零
''')
group('X','execution/exchange/dataflow_channels.py','C05','I03,I05,I06,I08',r'''
open_channels | (specs: tuple[ExchangeSpec, ...], *, services: RuntimeServices) -> ChannelSet | 装配既有 channel/router/gate，v3端口与冻结K4显式转换，一个连接一份credit | 缺 reader/重复 channel 拒绝；不为每节点复制完整run预算
encode_record | (record: Record, *, bounds: Bounds) -> EncodedRecord | 稳定类型化数据编码，区分负零、tuple/list、bytes及数字类型；payload 上界前置 | {'n':[-0.0],'p':[0.0]} 保持符号与独立容器；恶意长度拒绝
resolve_record | (encoded: EncodedRecord, *, store: ArtifactStore, bounds: Bounds) -> Record | 校验 checksum/域/媒体类型和解码膨胀上界后解码，不执行代码pickle | 小编码巨大解码声明、错误媒体类型、跨域引用拒绝
route_record | (record: Record, *, spec: ExchangeSpec, state: RoutingState) -> tuple[RoutedRecord, ...] | 稳定键哈希、目标路由、广播和可恢复轮转 ordinal | 同模型副本均衡但不换模型；慢分支不消耗全部credit
reserve_credit | (channel_id: str, reader_id: str, *, rows: int, size_bytes: int, operation_id: str, state: ChannelSet) -> CreditLease | 原子扣单channel和全局窗口，operation_id重复幂等 | 默认64条但只有1slot应降窗口而非超配；真实超大行拒绝
publish_segment | (records: tuple[EncodedRecord, ...], *, spec: ExchangeSpec, store: ArtifactStore, operation_id: str) -> ArtifactRef | 持久写→校验→发布，segment identity含序列区间与摘要；非checkpoint也可增量发布 | crash发布前不可见；发布后响应丢失重试不重复计费
seal_scope | (scope_id: str, producers: tuple[ProducerCompletion, ...], *, store: ArtifactStore) -> ArtifactRef | 精确 membership全部成功才 seal；保存 generation/manifest | 少一个producer、取消、未知、失败都不可seal；零条成功合法
snapshot_channels | (channels: ChannelSet, *, cut: CutRequest, store: ArtifactStore) -> Mapping[str, ArtifactRef] | 序列、路由计数、读者位置、credit归属、缓冲引用随cut封存 | 反压时cut不越过数据，恢复不能重复grant
restore_channels | (specs: tuple[ExchangeSpec, ...], refs: Mapping[str, ArtifactRef], *, services: RuntimeServices) -> ChannelSet | 仅恢复同plan/route版本，重建未确认数据和占用，不从零开始路由 | 同后缀的physicalchannel必须与首跑一致
reclaim_segments | (channels: ChannelSet, *, committed: RecoveryImage, store: ArtifactStore) -> tuple[ArtifactRef, ...] | 取所有reader和checkpoint/effect/sink保留引用的安全前沿，回收幂等 | reader取消未确认不可提前删；多次crash孤儿不永久耗尽预算
close_channels | (channels: ChannelSet, *, reason: str, deadline_ms: int) -> None | 关闭数据准入并排空控制，取消有tombstone，释放确认后的临时资源 | late data不可复活；close超时保留unknown归属而非假释放
''')
group('B','execution/worker/dataflow_execution.py','C07','I01,I04,I07',r'''
DataflowWorkerExecutor.__init__ | (self, *, services: RuntimeServices, mailbox: WorkerControlMailbox) -> None | 持有assignment=None、kernel/reader/channel表、有限控制与业务队列，构造不执行用户代码 | actor构造计数零；不创建RunJournal写连接或CommitCoordinator
DataflowWorkerExecutor.load | (self, assignment: WorkerAssignment) -> None | 校验sealed refs、code pin、分配ports/stages/预算，准备工厂不调用start | stale boot/plan/program/source摘要错误在业务前失败
DataflowWorkerExecutor.restore | (self, image: RecoveryImage | None) -> None | 恢复source cursor、kernel state、buffer、channel和effects，读取refs有界 | 缺bufferref拒绝，不能只恢复actor state便算成功
DataflowWorkerExecutor.start | (self) -> None | grant生效后建立本地actor/client/model，启动控制与独立业务执行上下文 | 无Activate不读源；CPU慢函数不能阻塞cancel/heartbeat
DataflowWorkerExecutor.step | (self, *, max_records: int, deadline_ms: int) -> WorkerStep | 公平从ready ports取有限记录，持有输入/输出grant到下游确认 | 阻塞GPU时其他组/控制可推进；无credit不提前执行付费调用
DataflowWorkerExecutor.request_cut | (self, request: CutRequest) -> None | 保存cut参与身份并通过所有输出有序发barrier，空输入/已EOI也贡献 | 两worker不可共用一个token；cut_id不由worker随意新造
DataflowWorkerExecutor.snapshot | (self, request: CutRequest) -> CutContribution | 全部输入对齐后封存业务state/缓冲/游标/效果/准备输出；不提交 | batch未满/flatmap部分输出/多输入空侧都被记录
DataflowWorkerExecutor.accept_receipt | (self, commit_id: str, *, cut_id: str) -> None | 验证driver原生commit回执对应cut后才放post-cut与返还ownership | 错cut/重复receipt不会二次释放或ACK
DataflowWorkerExecutor.cancel | (self, reason: str, *, deadline_ms: int) -> None | 停新准入，保留已发效果记录，有界排空并执行全体生命周期清理 | astart创建Task或持锁actor清理；超时不得mark_success
DataflowWorkerExecutor.close | (self) -> None | 幂等关闭所有reader/kernel/channel与模型，清理异常合并保原异常 | 一个owner.close失败仍关闭其余owner，上游aclose必达
''')
group('E','execution/dataflow.py','C03','I01,I02,I04,I14',r'''
execute_action | (request: ActionRequest, *, policy: RuntimePolicy, bounds: Bounds) -> ActionHandle | 新action唯一入口；编译、snapshot、准入、封存、资源、runtime、terminal、cleanup串成闭环 | 禁止RayData后每terminal可执行；失败不转旧executor
DataflowExecution.__init__ | (self, request: ActionRequest, *, services: DriverServices) -> None | 保存唯一action、policy、sealed、worker/cut/ready表；无第二账本 | 实例初始化不能隐式启动独立run或source
DataflowExecution.prepare | (self) -> SealedExecution | 完整预检后冻结snapshot与graph/layout/programs/bridge，产生唯一plan_id | 计划不可行时无worker/付费请求/文件publish
DataflowExecution.start | (self) -> None | 优先消费者/控制资源，使用ResourceManager申请，装配HostEndpoint并activate | partialallocation能推进；全部分给producer的死锁反例拒绝
DataflowExecution.pump | (self, *, deadline_ms: int) -> ExecutionStatus | 有界轮询events，校验fence/boot，调ready work、cut与终端，不搬payload | 一worker不回receipt不能永久阻塞；跨attempt迟到事件拒绝
DataflowExecution.finish | (self) -> TerminalResult | 所有必需source/worker/sink证据齐全后finalcut→receipt→complete | process退出不等成功；final未finish恢复不生成空commit
DataflowExecution.cancel | (self, reason: str) -> None | 关闭准入、fence迟到、撤订阅、收集退出证明；未确认不释放物理owner | cancel时unknown模型请求不重放、不把已提交结果删除
DataflowExecution.close | (self) -> None | 一处回收作用域，等待ResourceManager退出证明且保留失败证据 | apply失败/部分启动/用户迭代器早关都不泄漏worker
''')
group('E','execution/dataflow_scheduler.py','C03','I04,I06,I08',r'''
DataflowScheduler.__init__ | (self, physical: PhysicalDataflow, *, resources: ResourceManager) -> None | 有限ready队列、DRR deficit、lease表、控制消费者保留容量；不新造allocator | 单一ResourceManager实例计量；重复reserve不 double charge
DataflowScheduler.enqueue | (self, work: ReadyWork) -> None | 验证stage/partition与大小，按operation_id去重，满则背压 | 巨大payload不得进队列；same id different内容拒绝
DataflowScheduler.choose | (self, *, now_ms: int) -> ScheduleChoice | 从已可行组按DRR选择，返回等待状态不忙轮询；同模型内平衡 | 慢分支/热点key不饿死无关组；无资源不能假调度
DataflowScheduler.place | (self, work: ReadyWork) -> WorkerLease | 复用资源层进行真实放置，lease绑定分区、boot、profile、host | GPUprofile不能被CPUhost承载；资源拒绝不能吞掉
DataflowScheduler.complete | (self, operation_id: str, *, receipt_ref: ArtifactRef) -> None | 确认work完成并归还瞬时resident占用，累计成本不退回 | 重复完成幂等；退出未确认不能复用GPUslot
DataflowScheduler.drain | (self, stage_id: str, *, deadline_ms: int) -> None | sealed阶段资源重用前等待所有work/cut引用安全释放 | 状态不可迁移时不能抢占模型；deadline明确失败
''')
group('D','execution/dataflow_checkpoint.py','C08','I07,I13',r'''
DataflowCheckpointCoordinator.__init__ | (self, action: RunAction, physical: PhysicalDataflow, *, store: ArtifactStore) -> None | 持有现有CommitCoordinator、固定参与表和有限pendingcuts，无新数据库 | worker不能构造此类；同action只一个driver owner
DataflowCheckpointCoordinator.begin | (self, *, final: bool) -> CutRequest | 确定checkpoint sequence/id/participants/route revision，绑定原生cut事实 | 并发begin不能同序列不同成员；final必须来自EOI证据
DataflowCheckpointCoordinator.contribute | (self, contribution: CutContribution) -> None | 验boot/participant/cut/digest/refs，去重并拒绝冲突，完整性有限检查 | 第二worker缺失不能因第一个ready判全cut完成
DataflowCheckpointCoordinator.commit | (self, cut_id: str, *, sinks: Mapping[str, Datasink]) -> RecoveryImage | 将sourcevector/各state/buffer refs交唯一CommitCoordinator，prepare/publish/receipt/ACK沿原协议 | afterprepare-before-record crash不重复计费；ACK不能早于所有sink receipt
DataflowCheckpointCoordinator.recover | (self, *, sinks: Mapping[str, Datasink]) -> RecoveryImage | 先reconcile已prepared/unknown，选最后完整cut；返回完整恢复图 | 有文件无journal证明不可恢复；部分cut/未知sink不能跳过
DataflowCheckpointCoordinator.abort | (self, cut_id: str, *, reason: str) -> None | 只释放本cut未发布暂态，保留已发效果/已提交refs，通知worker不当成功 | abort不能删除已发布输出；过期cut不能影响新attempt
source_vector_position | (image: RecoveryImage, *, action_id: str) -> SourcePosition | 使用固定v3虚拟source+cut_sequence，真实cursor向量放states并交叉校验 | 不把checkpoint序号当所有来源行号跳读
''')
group('D','execution/dataflow_sinks.py','C08','I07,I09',r'''
make_sink_adapter | (spec: KernelSpec, *, services: DriverServices) -> Datasink | 装配Lance/文件/自定义/queue sink，保留各自recover/commit能力；纯声明不写出 | legacy write callback不冒充四方法可恢复sink
prepare_sink_partition | (records: Iterator[Record], *, spec: KernelSpec, services: RuntimeServices) -> ArtifactRef | worker有界写owned staging，返prepared descriptor；没有全局publish权 | crash staging留可对账引用；行数/字节/原schema不漂移
combine_prepared_sinks | (refs: tuple[ArtifactRef, ...], *, sink_id: str, services: DriverServices) -> Mapping[str, Json] | 组合有界manifest引用而非把全数据拉driver；校验分区完整且唯一 | 两worker重复提交同partition拒绝；空分区仍记完成
ack_source_claims | (image: RecoveryImage, *, action: RunAction) -> None | 只核对image引用的原生finalize_task fact已经acked并推进已确认前沿；不发ACK、不调用acknowledge，实际原子结算属于C16服务 | response丢失重试幂等；未知sink不ACK；不改frozen queue协议
''')
group('T','execution/dataflow_terminals.py','C11','I01,I02,I09',r'''
open_terminal | (request: ActionRequest, *, execution: DataflowExecution) -> ActionHandle | 唯一terminal registry把同一graph输出变现有API返回值，不启动第二action | take/iter/count/write全咨询同一执行入口
collect_rows | (handle: ActionHandle, *, limit: int | None, bounds: Bounds) -> list[Row] | 有界driver收集，limit0零读；达到limit后关闭其订阅并保留effect事实 | take_all超总字节明确失败，不返回截断成功
iterate_rows | (handle: ActionHandle) -> Iterator[Row] | 持有action上下文到迭代完成/close；从DataRef按需读取 | next一次后close立即取消必要生产者并清理，不靠GC
iterate_batches | (handle: ActionHandle, *, batch_size: int | None, batch_format: str, drop_last: bool) -> Iterator[object] | 原batch格式/尾批约定，转换峰值受准入，不改业务batch | numpy/pandas/Arrow类型保真；零拷贝声明无法满足明确拒绝
materialized_handle | (result: TerminalResult, *, execution: DataflowExecution) -> MaterializedHandle | 保存固定结果引用、schema、rows、reader domain，复用现有MaterializedDataset表面 | 两次take不重新跑模型；release不删其他引用
write_result | (result: TerminalResult, *, kind: str) -> WriteResult | 校验已确认sink结果转换旧返回结构，unknown不能构造成成功 | commit失败不得仅凭written_rows非零返回成功
inspect_result | (handle: ActionHandle, *, kind: str) -> object | schema/columns/size/stats/summary仅消费相应真实元数据或有界执行结果 | 未执行stats明确；schema未知遵循fetch_if_missing，True经统一有界执行；不得猜空表等成功
''')
group('M','execution/dataflow_models.py','C09','I04,I07,I09',r'''
build_model_factory | (operation: LogicalOp, *, program_ref: ArtifactRef | None, bounds: Bounds) -> KernelSpec | 只冻结原Prompt/Embedding/Agent/Fetch/Document完整配置与构造声明，不捕获锁/client | cloudpickle只含代码配置；driver构造模型计数0
bind_model_services | (instance: CallableInstance, spec: KernelSpec, *, services: RuntimeServices) -> CallableInstance | worker构造后绑定原生effect/journal预算、资源、生命周期，保持模型配置唯一 | Embedding必须有原子预算与自身NativeEmbeddingRecovery；它没有Prompt coordinator，不借用bind_client
make_effect_transport | (port: WorkerJournalPort, *, actor_id: str, bounds: Bounds) -> NativeEffectTransport | 适配原生prompt journal事务请求到类型化driver RPC，非另一份账本 | 相同effect response丢失返回原结果；unknown拒重放
restore_actor_state | (instance: CallableInstance, *, ref: ArtifactRef | None, services: RuntimeServices) -> None | 业务state用原Adapter/auto_json，运行期资源本地重建；半套hook拒绝 | 持锁类/astart Task不抹业务累加值；partial空vars不当有状态
snapshot_actor_state | (instance: CallableInstance, *, services: RuntimeServices) -> ArtifactRef | 封存真实实例业务状态，构造后检查而非仅class豁免；大小前置有界 | class内部累加状态恢复正确；opaque状态不能诚实False却升级ownership
bind_cached_map | (spec: KernelSpec, *, services: RuntimeServices) -> Kernel | 复用原cache namespace与结果共享，不把Path/lock当业务state；实例lifecycle独立 | None/list/bytes结果保真；shared wait取消、asyncmaterialize七个反例
embedding_cache_key | (record_id: str, *, model_digest: str, payload_digest: str, batch_semantics: Mapping[str, Json]) -> str | 区分单请求幂等与结果缓存；纯批装箱变化不改变单记录模型结果缓存身份 | concurrency改变不全部失效；不同模型/归一化/任务配置绝不能复用
''')
group('H','execution/dataflow_authority.py','C09','I07,I10,I11',r'''
DriverJournalService.__init__ | (self, action: RunAction, *, store: ArtifactStore, bounds: Bounds) -> None | 绑定唯一driver journal及原生effect实现和幂等operation表，禁止任意SQL代理 | worker不可通过此接口start/complete action或commit
DriverJournalService.dispatch | (self, event: WorkerEvent) -> ControlReply | 验run/attempt/fence/boot/grant/allowlist/字节上限再调固定handler | 陈旧boot/任意method/payload超限都拒绝，不能缓存无限events
DriverJournalService.effect | (self, command: EffectCommand, *, lease: WorkerLease) -> EffectReply | 按原状态转移调用原journal，预算预留、mark_sent、confirm持久顺序固定 | 先网络后mark_sent的实现被故障测试拒绝；unknown不能变absent
DriverJournalService.reserve | (self, operation_id: str, amounts: Mapping[str, int], *, lease: WorkerLease) -> None | 原生fenced预算事务以稳定operation去重，瞬时/累计分开 | 重复reserve同量幂等不同量冲突；crash resume不永久耗尽预算
WorkerJournalClient.__init__ | (self, *, transport: ControlTransport, lease: WorkerLease, bounds: Bounds) -> None | 保存有限待请求表和deadline，不打开RunJournal/SQLite文件 | 禁止worker文件系统访问driver journal的测试仍可启动
WorkerJournalClient.effect | (self, command: EffectCommand) -> EffectReply | 发类型化请求并等待有界回执；同operation_id重查，不生成第二付费请求 | 连接中断后unknown保持，取消不吞结果归档
WorkerJournalClient.reserve | (self, operation_id: str, amounts: Mapping[str, int]) -> None | 调driver唯一预算准入，预调用前占用，返回只代表准入非业务成功 | 不足预算阻止真实外部请求，不能本地先扣再远端二次扣
WorkerJournalClient.read_action | (self, action_id: str) -> Mapping[str, Json] | 只读本assignment获授权action声明与状态，有界快照 | 跨run/action访问拒绝，无任意SQL
WorkerJournalClient.read_configuration | (self) -> Mapping[str, Json] | 读取封存limits/版本/当前grant信息，不接受worker篡改配置 | worker重启不能读取新的默认预算替换封存预算
WorkerJournalClient.check_grant | (self) -> None | 本地期限与driver当前owner检查，有界RPC；无结果按不可继续准入处理 | 网络分区不得继续发新效果；late grant不能复活
''')
group('H','execution/dataflow_artifacts.py','C10','I07,I11,I13',r'''
stage_runtime_bundle | (sealed: SealedExecution, *, store: ArtifactStore, bounds: Bounds) -> ArtifactRef | manifest绑定平台/依赖/业务模块与按值程序、schema；无凭证；有界打包 | 相同模块名不同代码摘要拒绝；不得包含整个工作区/环境秘密
fetch_runtime_bundle | (ref: ArtifactRef, *, cache: Path, store: ArtifactStore, bounds: Bounds) -> Path | 分块下载到owned临时目录，校验摘要/路径后原子发布，worker本机导入 | zip traversal/symlink/超包大小拒绝；失败只删owned临时
verify_host_domain | (host: HostDescriptor, *, store: ArtifactStore, challenge: ArtifactRef, bounds: Bounds) -> DomainProof | 对指定共享store challenge做真实读校验，证明namespace和对象一致 | 两host同路径不同字节拒绝；仅metadata声明不通过
resolve_data_location | (ref: ArtifactRef, *, host: HostDescriptor, store: ArtifactStore) -> DataLocation | RayObject临时定位与durable恢复定位分开，优先本地缓存但必验digest | producer节点死后从durable读取，不靠lineage重放付费调用
''')
group('H','execution/dataflow_hosts.py','C07','I01,I11',r'''
make_host_endpoint | (lease: WorkerLease, *, services: DriverServices) -> HostEndpoint | 按provider装配Local socket或Ray actor控制，承载同一worker runtime | 不允许ray.data.ActorPoolStrategy，provider差异不改业务图
LocalHostEndpoint.activate | (self, assignment: WorkerAssignment) -> None | 经既有Local provider启动/绑定worker，传有界manifest和port，不传全payload | process启动失败有退出证明，不能遗留scope
LocalHostEndpoint.poll | (self, *, max_events: int, deadline_ms: int) -> tuple[WorkerEvent, ...] | socket有限读取控制事件，校验帧长度及身份 | partial frame/peer沉默不阻塞全driver
LocalHostEndpoint.request_cut | (self, request: CutRequest) -> None | 发稳定cut身份，不在host重算checkpoint | 重复相同cut幂等，内容冲突拒绝
LocalHostEndpoint.cancel | (self, reason: str, *, deadline_ms: int) -> None | 发送取消、等待有界退出，必要时由provider终止并保留unknown状态 | kill不当成sink成功或恢复安全证明
LocalHostEndpoint.closed | (self) -> bool | 查询既有provider退出证明，不用本地布尔代替真实退出 | socket断开但进程仍活不能报资源已释放
RayHostEndpoint.activate | (self, assignment: WorkerAssignment) -> None | Ray Core named actor运行同一WorkerRuntime，禁自动重放，worker本地装配工件 | 两host不同cache路径仍跑通；max_task_retries不得默认重业务
RayHostEndpoint.poll | (self, *, max_events: int, deadline_ms: int) -> tuple[WorkerEvent, ...] | actor有界outbox pull，等待超时不阻塞其他host | slow actor不饿死driver cancel；事件积压有限
RayHostEndpoint.request_cut | (self, request: CutRequest) -> None | Ray控制方法转发同一CutRequest，不复制journal权威 | stale actor消息被fence拒绝
RayHostEndpoint.cancel | (self, reason: str, *, deadline_ms: int) -> None | 排空/取消→provider确认actor退出，未知效果保留 | actor死亡不自动重跑unknown模型请求
RayHostEndpoint.closed | (self) -> bool | 读取Ray provider allocation observation，确认对应boot终止 | actor同名新boot不能替旧boot完成证明
''')
group('W','execution/dataflow_history.py','C12','I07,I12,I13',r'''
load_execution_version | (directory: Path, action_id: str, *, max_bytes: int) -> HistoricalBinding | 读取v1/v2/v3封存事实，按原版本校验，不重算旧identity | 1025枚举prompt旧digest保持；未来schema拒绝
validate_resume_compatibility | (binding: HistoricalBinding, *, current_runtime: str, store: ArtifactStore) -> CapabilityReport | 逐项检查原代码/源/state/sink/cursor/历史恢复保证，列具体不兼容原因 | 缺代码或attempt_only未完成不得编新图继续
translate_native_checkpoint | (binding: HistoricalBinding, *, store: ArtifactStore, bounds: Bounds) -> RecoveryImage | 仅已列原生v1/v2格式转换内存视图，保留原身份和effect/sink引用不覆写历史 | unknown字段/未封存buffer拒绝；不把旧scalar位置猜成多源vector
build_plan_view | (graph: DataflowGraph, sealed: SealedExecution, physical: PhysicalDataflow, *, observations: tuple[NodeObservation, ...]) -> Mapping[str, Json] | 输出真实逻辑/物理图、资源、exchange原因、恢复限制，保持桥字段 | 同函数不同occurrence不合并；无观测不推算节点成功
''')
group('X','execution/exchange/dataflow_bridge.py','C05','I05,I07',r'''
to_channel_plan_v1 | (spec: ExchangeSpec) -> Mapping[str, Json] | 将v3已确定exchange无损投影冻结K4需要的channel plan字段，验证后使用 | scope membership/reader set不能丢；不能给v1 validator额外塞字段
artifact_to_data_ref | (ref: ArtifactRef, *, reader_domain: str, owner: str) -> Mapping[str, Json] | 显式构造并验证现有DataRef，永久store与临时Ray location分开 | ArtifactRef不能直接冒充v1 DataRef；跨域和大小冲突拒绝
record_to_envelope | (record: Record, *, channel_id: str, route_epoch: int, sequence: int) -> Mapping[str, Json] | 使用冻结ChannelSequence/InputGate约定构造data frame，尺寸来自实际编码 | data/control共用序列；尺寸小报不能超配额
cut_to_barrier | (request: CutRequest, *, channel_id: str, sequence: int) -> Mapping[str, Json] | 将cut_id/epoch/revision与producer bound映射现有barrier，不改变旧格式 | postcut不能超越barrier，EOI空输入也可对齐
''')
group('H','execution/dataflow_services.py','C09','I07,I11',r'''
make_driver_services | (action: RunAction, *, policy: RuntimePolicy, bounds: Bounds) -> DriverServices | 装配唯一resources/store/journal/checkpoint/host端口与关闭栈；明确owner | 同进程两个action不共享可变cut状态；不建第二数据库
make_runtime_services | (assignment: WorkerAssignment, *, transport: ControlTransport, cache: Path) -> RuntimeServices | 只装配read-only layout、artifactstore、journalclient、reservation、cancel/clock，不打开driver数据库 | worker目录无run.sqlite仍可读取配置并完成native模型流程
LocalHostEndpoint.__init__ | (self, lease: WorkerLease, *, services: DriverServices) -> None | 保存lease/控制连接/有限pending表，构造不启动业务；由make_host_endpoint唯一调用 | 资源未准入前不得fork；错误清理不能关闭其他scope
RayHostEndpoint.__init__ | (self, lease: WorkerLease, *, services: DriverServices) -> None | 保存lease/actor handle/有限pending表，不触发RayData或隐式重启 | actor失联时不生成同名新actor冒充旧boot
''')
# Host constructors belong to the same class file, not an accidental second class.
for r in GROUPS:
    if r['symbol'].startswith(('LocalHostEndpoint.__init__','RayHostEndpoint.__init__')):
        r['file']='demiflow/execution/dataflow_hosts.py'
group('E','execution/dataflow_api.py','C11','I01,I02,I14',r'''
make_action_request | (executor: DatasetExecutor, source: SourcePlan, plan: LogicalPlan, *, kind: str, arguments: Mapping[str, Json], sink: Datasink | None = None, programs: tuple[ActionProgram, ...] = ()) -> ActionRequest | 复用当前RunAction/checked绑定，构造只含声明的统一request，必要时创建一次原生action | take/count不绕行准入；不得嵌套启动新run或序列化活executor
run_dataset_action | (request: ActionRequest) -> object | 唯一公共facade取冻结policy调用execute_action并保持旧返回类型 | Local/Ray差异仅provider；任何新action无RayData调用
make_native_operation | (kind: str, configuration: Mapping[str, Json], *, bindings: Mapping[str, Json]) -> LogicalOp | 生成惰性原生模型/获取/缓存声明，保存代码配置身份，driver无client实例 | map_embeddings链构造不能创建journal/network；配置错误在运行前报告
explain_dataset | (source: SourcePlan, plan: LogicalPlan, *, provider: Provider, options: Mapping[str, Json]) -> CapabilityReport | 公共检查复用同编译/能力判定，不产生worker/claim/model/effect | explain不能在无snapshot情况下谎称完整读域已验证；明确待运行解析
''')
group('D','execution/dataflow_seal.py','C12','I01,I07,I12,I13',r'''
seal_dataflow | (action: RunAction, graph: DataflowGraph, physical: PhysicalDataflow, *, store: ArtifactStore) -> SealedExecution | 原生action下原子封存v3logical/physical/program/code/source/bridge，写唯一plan事实 | 封存失败不启动worker；同action重复不同plan拒绝
load_dataflow | (directory: Path, action_id: str, *, store: ArtifactStore, bounds: Bounds) -> tuple[DataflowGraph, PhysicalDataflow, SealedExecution] | 由journal指向读取并完整校验原工件，禁止扫描有文件就认成功 | 部分文件/坏摘要/未来版本/错误action都拒绝
build_dataflow_identity_bridge | (graph: DataflowGraph, *, report_digest: str, bindings: Mapping[str, Json]) -> Mapping[str, Json] | 完整source/transform/relational/terminal occurrence对应工作台report实例，真实绑定无猜图 | union/嵌套materialize/同函数两处每条report_node_id均在展示图
''')
group('H','execution/dataflow_store.py','C10','I07,I08,I11',r'''
RunArtifactStore.__init__ | (self, *, root: Path, store_id: str, journal: ArtifactAuthorityPort, bounds: Bounds) -> None | 装配已认证共享store根与唯一journal端口，绝对路径不进持久key | 根含symlink或域未认证拒绝；不创建平行权威数据库
RunArtifactStore.put | (self, chunks: Iterator[bytes], *, max_bytes: int, media_type: str) -> ArtifactRef | 分块限额预留→owned staging→hash/fsync→不可变发布，内容寻址且校验碰撞 | 超预算不先全读；相同内容重复发布只计一份物理字节
RunArtifactStore.read | (self, ref: ArtifactRef, *, max_bytes: int) -> Iterator[bytes] | 受限打开不可变对象，逐块读验digest/大小/域，close异常清理FD | 大于maxbytes在读前拒绝；相同路径被替换不能接受
RunArtifactStore.inspect | (self, ref: ArtifactRef) -> bool | 只读证明存在且字节一致；确定不存在返回False，权限/超时抛未知 | 无权限不能当不存在然后覆写
RunArtifactStore.retain | (self, ref: ArtifactRef, *, owner: str) -> None | 经原journal追加owner pin，owner为action/cut/effect/sink声明身份 | 重复pin幂等；未知owner不允许延长无限保留
RunArtifactStore.release | (self, ref: ArtifactRef, *, owner: str) -> None | 幂等删除本owner pin，实际删除须经过committed前沿与其他owner引用检查 | 一个结果release不能删其他run/cut依赖
''')
group('X','execution/exchange/dataflow_state.py','C05','I05,I06,I08',r'''
ChannelSetState.__init__ | (self, specs: tuple[ExchangeSpec, ...], *, bounds: Bounds) -> None | 保存每reader credit、序列、buffer refs、EOI/seal/取消、resident总量，有界表 | 节点/reader数量超限先拒绝，禁每次扫描全历史
ChannelSetState.close | (self) -> None | 幂等关闭全部carrier，归还已确认瞬时lease，保留恢复pins | 一个close失败仍遍历其他carrier
RoutingCounters.__init__ | (self, *, values: Mapping[str, int], route_epoch: int) -> None | 恢复确定channel ordinal与route版本，输入不可共享可变map | 负数/错误routeepoch拒绝
RoutingCounters.next_ordinal | (self, channel_id: str) -> int | 在既有worker串行路由上下文原子取递增ordinal，纳入checkpoint | 并发发送不能重复sequence，resume后不重置
''')
group('T','execution/dataflow_handles.py','C11','I01,I07,I09',r'''
RuntimeActionHandle.__init__ | (self, execution: DataflowExecution, *, bounds: Bounds) -> None | 绑定同一execution、有限输出订阅和完成事件，拥有明确close责任 | 不另起run；返回handle不代表action成功
RuntimeActionHandle.rows | (self) -> Iterator[Record] | 驱动原execution pump并从受credit限制的输出订阅拉取 | 慢消费者施加背压；一条后关闭不留生产worker
RuntimeActionHandle.result | (self) -> TerminalResult | 等待原final receipt或返回有界错误，不能先consume所有rows到list | writer部分成功不能完整成功；timeout可取消
RuntimeActionHandle.close | (self) -> None | 正常结束只释放订阅，未结束关闭需要的action并保留已发生效果 | close两次幂等，异常不能吞业务failure
StoredMaterializedHandle.__init__ | (self, result: TerminalResult, *, store: ArtifactStore, owner: str) -> None | 保留结果manifest pin、schema与rows，非payload全量缓存 | 恢复读原类型，不把tuple转换list
StoredMaterializedHandle.rows | (self) -> Iterator[Row] | 只读固定manifest逐段输出，不再调用source/model | 连续两次读取源调用计数不变
StoredMaterializedHandle.release | (self) -> None | 释放本handle pin，保持其他owner和历史结果引用 | result被workbench引用时不删除底层段
''')
group('K','execution/kernels/operator.py','C06','I01,I05,I07,I09',r'''
OperatorKernel.__init__ | (self, spec: KernelSpec, *, services: RuntimeServices, partition_id: str) -> None | 唯一Kernel具体状态持有者：instance、input-port前沿、有限batch、spool refs、output cursor和lifecycle，构造不执行 | 所有kind必须有具体kernel实例；声明类不能冒充可调用实现
OperatorKernel.start | (self) -> None | 校验profile后构造普通fn/actor或model并执行astart，一次性拥有生命周期 | 同partition恢复不重复实例状态初始化；失败清理已建资源
OperatorKernel.push | (self, port: str, record: Record) -> Iterator[Record] | incremental逐条调用；finite输入只受限落spool，模型batch有限组装；调用对应唯一计算函数 | right未complete不能错误join；大组不能放Python全量list
OperatorKernel.finish | (self, port: str) -> Iterator[Record] | 标记该输入EOI，required ports齐后调用纯关系/排序/聚合kernel或flush末批 | 一侧EOI不能结束所有ports；空输入正确产生count0
OperatorKernel.snapshot | (self) -> ArtifactRef | 保存state/batch/timer/source-port前沿/输入spool/output确认cursor；纯外存kernel重建输入再跳已确认输出 | DataFusion活session不pickle；crash后不能丢未满batch
OperatorKernel.restore | (self, ref: ArtifactRef | None) -> None | 核验kernel/config/partition身份，恢复业务状态和外存重建依据 | 不能用空ref把有状态actor当新建；路由改变拒绝
OperatorKernel.close | (self) -> None | 全部stop/close与spool ownership清理，异常不阻断其他owner | 业务异常保留，上游aclose也执行，close幂等
''')
group('F','execution/dataflow_inputs.py','C11','I01,I02,I09',r'''
build_input_handle | (value: object, *, format: str, bounds: Bounds | None = None) -> MaterializedHandle | 只包装已经物化的受限driver数据，保留Arrow/numpy/pandas/row类型，不创建run、不执行输入迭代器 | from_items必须list；SourcePlan不当handle；原已分配输入不再无界复制
InputMaterializedHandle.__init__ | (self, value: object, *, format: str, max_bytes: int) -> None | 持有已有数据只读引用和schema/块数，使用可验证大小与有界结构检查；可写对象按原约定安全复制前准入 | 生成器拒绝；大Arrow不to_pylist估计大小；调用后外部突变不悄悄改封存内容
InputMaterializedHandle.rows | (self) -> Iterator[Row] | 本地逐块按需解码；进入action时先bind/snapshot为可跨worker引用 | 构造时不读全量，bytes/负零/tuple保真
InputMaterializedHandle.num_blocks | (self) -> int | 返回真实物化块数，不启动计算或连接Ray | 空输入块数按兼容约定，不能因provider变化漂移
InputMaterializedHandle.release | (self) -> None | 释放本handle所拥有引用/ownedcopy，不销毁用户其他引用或历史artifact | 重复release幂等，读取已release句柄明确错误
''')
group('H','execution/dataflow_authority.py','C09','I07,I08,I11',r'''
DriverJournalService.artifact | (self, command: ArtifactCommand, *, lease: WorkerLease) -> ArtifactReply | 原journal内pin/unpin/inspect/reserve-staging允许列表；owner必须属于当前assignment | worker不能解除别人的pin；不存在与权限未知分开
WorkerJournalClient.artifact | (self, command: ArtifactCommand) -> ArtifactReply | 用有界inline命令发artifact metadata请求，不为发送reserve先写artifact形成循环 | 初始无artifact配额仍能请求reserve；请求幂等不重复扣费
''')
group('H','execution/worker/dataflow_host.py','C07','I01,I07,I11',r'''
HostedDataflowWorker.__init__ | (self, launch_spec: Mapping[str, Json], spec_hash: str, boot_id: str) -> None | Local进程/Ray actor同一宿主壳，装配同一WorkerRuntime/executor和有限outbox | constructor不打开driver数据库，不执行用户actor构造
HostedDataflowWorker.activate | (self, assignment: WorkerAssignment) -> None | v3assignment工件校验后转K3固定生命周期与assignment，ready后activate | 错代码/域/boot前置失败；不能直接跳到业务start
HostedDataflowWorker.poll | (self, *, max_events: int, deadline_ms: int) -> tuple[WorkerEvent, ...] | 有界outbox消费，progress可合并，cut/effect/failure不可丢 | 慢driver时不无限内存；控制线程不被UDF占用
HostedDataflowWorker.request_cut | (self, request: CutRequest) -> None | 控制线程投递切点到executor，快返回不执行业务计算 | cut消息重复幂等；未activate拒绝
HostedDataflowWorker.cancel | (self, reason: str, *, deadline_ms: int) -> None | 控制面关闭准入，业务面排空，join有界并发出真实closed | astart死锁不让driver永久等；unknown效果保留
HostedDataflowWorker.closed | (self) -> bool | 汇报本worker executor清理结束，物理退出仍由provider确认 | closed事件不能替代ResourceManager退出证明
make_worker_control_assignment | (assignment: WorkerAssignment, *, grant: Mapping[str, Json], manifest_ref: Mapping[str, Json], ready_deadline_ms: int) -> Mapping[str, Json] | 完整映射冻结K3 assignment字段，v3本体放K1工件并由host先验证；不改K3字节 | v3额外字段不能直接塞v1validator；stage/partition/channel名单精确一致
''')
group('H','execution/dataflow_control.py','C07','I07,I11',r'''
LocalControlTransport.__init__ | (self, connection: SocketConnection, *, lease: WorkerLease, bounds: Bounds) -> None | 保存专属有限帧连接及pending表，不共享业务socket造成控制饥饿 | 巨帧声明先拒绝，半包不可无期限等待
LocalControlTransport.request | (self, event: WorkerEvent, *, deadline_ms: int) -> ControlReply | 原Local控制通道有界请求应答，按operation_id关联并验证身份 | 乱序/重复/取消/断连不会将其他request回执错配
RayControlTransport.__init__ | (self, mailbox: WorkerControlMailbox, *, bounds: Bounds) -> None | 保存当前worker有界mailbox引用，不创建driver actor，不导入ray.data | 不持有driver RunJournal或sqlite连接
RayControlTransport.request | (self, event: WorkerEvent, *, deadline_ms: int) -> ControlReply | 通过本worker mailbox/outbox由driver轮询处理并deliver_reply；不存在driver journal actor，超时幂等重查 | effect确认响应丢失不重复付费，driver取消仍可处理
''')
group('C','execution/contracts/dataflow_codec.py','C01','I01,I07,I10,I13',r'''
encode_wire | (value: object, *, type_name: str, bounds: Bounds) -> bytes | 唯一固定wire类型表编码，闭合schema字段，深度/字节/元素数检查；禁止runtime服务对象 | 任意class/__reduce__/SQL/CallableSpec不能伪装wire；NaN和bool当int拒绝
 decode_wire | (raw: bytes, *, type_name: str, bounds: Bounds) -> object | 仅解固定schema并构造深冻结DTO，跨字段和引用校验；未知版本拒绝 | 缺字段/额外字段/inline+ref双有/错误enum不默许
validate_dataflow_record | (value: object, *, type_name: str, bounds: Bounds) -> None | 校验数值、ID、枚举、oneof、scope、cursor、预算；运行时对象显式禁止序列化 | zero rows合法但zero Bounds不合法；負cursor和超控制帧拒绝
compute_graph_digest | (graph: DataflowGraph) -> str | 对v3域的规范语义投影哈希，排除自身dag_id与宿主/attempt；包含端口顺序和别名拓扑 | 不依赖Python对象id/hashseed；独立调用不能被共享前缀吞掉
compute_physical_digest | (physical: PhysicalDataflow) -> str | 规范投影含实际stage/group/profile/exchange/recovery/policy/revision，排除自身plan_id | 不同资源布局不能同plan_id；变attempt但布局相同保持
''')
# Trim leading whitespace typo robustly, not silently alter a public symbol name.
for r in GROUPS:r['symbol']=r['symbol'].strip()
group('E','execution/dataflow_api.py','C02','I01,I07,I13',r'''
declare_action_v3 | (source: SourcePlan, plan: LogicalPlan, *, kind: str, arguments: Mapping[str, Json], sink_declaration: Mapping[str, Json] | None) -> Mapping[str, Json] | action绑定前纯计算v3声明，结构别名/配置/代码身份有界，无action_id循环；RunContext.action仍唯一绑定 | 新NativeOperation不可落旧describe兜底；历史plan_identity输出保持
''')
group('D','execution/dataflow_seal.py','C12','I01,I07,I13',r'''
stage_dataflow_programs | (compiled: CompiledDataflow, *, action: RunAction, store: ArtifactStore, bounds: Bounds) -> DataflowGraph | 消费编译返回的ProgramDeclaration，复用代码依赖归档/plan_code_pin封存纯程序，填实refs后才物理编译；不使用隐式全局pending表 | compile没有store不能假造已存在ArtifactRef；worker核实际发送程序；图semantic_id不因store路径变化
verify_dataflow_program | (ref: ArtifactRef, *, expected_pin: Mapping[str, Json], store: ArtifactStore, bounds: Bounds) -> LoadedProgram | worker自身load/import后复算代码摘要，核对封存依赖和v3语义映射，复用旧pin算法但不改旧公式 | import当前主仓同名模块、缺按值依赖、pickle摘要变化冒充同代码都拒绝
resolve_execution_constants | (graph: DataflowGraph, *, action: RunAction, store: ArtifactStore) -> ArtifactRef | seed=None等运行期常量仅一次生成并在原action封存；恢复读原constants，不进逻辑业务配置 | 未指定seed的shuffle/sample跨crash仍一致；显式seed不被覆盖
''')
group('C','execution/contracts/dataflow_errors.py','C01','I07,I10',r'''
DataflowError.__init__ | (self, code: str, message: str, *, retryable: bool = False, context: Mapping[str, Json] | None = None) -> None | 有界错误对象，code闭合集、context脱敏深冻结；UNKNOWN_EFFECT不允许retryable | 错误消息不能包含api key/模型正文；未知code拒绝
''')
group('H','execution/worker/dataflow_host.py','C07','I01,I10,I11',r'''
HostedDataflowWorker.identity | (self) -> Mapping[str, Json] | 返回实际spec_hash和boot_id，符合RayResourceProvider现有ensure_present的核验调用 | 同名actor不同spec/boot不接受；不得虚报身份
HostedDataflowWorker.handle | (self, message: Mapping[str, Json]) -> Mapping[str, Json] | 兼容现有RayControlConnector.handle，将K3固定命令交WorkerRuntime，不绕过状态机 | 缺grant/跳过verify/旧controller拒绝
HostedDataflowWorker.pull_events | (self, limit: int) -> tuple[WorkerEvent, ...] | 兼容现有connector.pull，委托有界outbox；保留control/event身份 | 超limit不能返回全历史，duplicate可校验
HostedDataflowWorker.deliver_receipt | (self, receipt: Mapping[str, Json]) -> None | 兼容现有connector回执入口，校验commit/cut后交executor接受 | 旧boot/错误cut receipt不能释放post-cut
HostedDataflowWorker.close | (self) -> None | 兼容provider ensure_absent调用，结束所有业务/控制线程，幂等 | 不能仅设closed布尔；异步清理失败仍报告并释放已确认资源
''')
group('H','execution/dataflow_hosts.py','C07','I01,I10,I11',r'''
DataflowRayConnector.host_class | (self) -> object | 继承既有RayControlConnector，仅返回ray.remote(HostedDataflowWorker)，max_restarts/max_task_retries=0 | 与provider实际.remote(launch_spec,spec_hash,boot_id)三参数完全一致
DataflowLocalConnector.spawn_payload | (self, allocation_id: str) -> Mapping[str, Json] | 继承既有LocalControlConnector，复用有限socket/FD与启动协议，v3entrypoint交既有bootstrap版本分派 | 不再造进程launcher；错误spawn必须执行既有abort清理
''')

group('Q','execution/dataflow_queue.py','C16','I01,I07,I09,I10',r'''
NativeAcquisitionService.__init__ | (self, action: RunAction, *, services: DriverServices) -> None | 唯一driver绑定原生QueueDatasource/RawFetch，持有有限claim表和任务队列；构造不claim | worker侧创建本类必须拒绝；不建立第二SQLite账本
NativeAcquisitionService.configure | (self, source: SourcePlan, plan: LogicalPlan, sink: object | None) -> QueueBinding | 自动结算调用冻结validate_topology，显式ACK调用configure_explicit；普通fetch登记原RawFetch声明；绑定原action/namespace | 任意filter/模型混入自动ACK拓扑仍按原协议拒绝，不放宽为通用queue写入
NativeAcquisitionService.start | (self) -> None | 在原action context启动source/RawFetch生命周期，IO与claim容量经driver资源预算计量，控制线程与阻塞rows分离 | astart失败关闭已开owner；RawFetch在worker不存在
NativeAcquisitionService.poll | (self, command: QueueCommand, *, lease: WorkerLease) -> QueueReply | claim/poll只返回有限delivery描述或ref，暂空pending与真实drained分离，稳定operation重试不再领取 | lease过期、owner变更、不匹配pool拒绝；未发布claim恢复归原协议
NativeAcquisitionService.acquire | (self, command: QueueCommand, *, lease: WorkerLease) -> QueueReply | 收到worker前缀输出ref后恢复原RowScope及delivery，驱动端调用原RawFetch；返回原receipt与rowref；原字节对象留namespace | acquisition确认后RPC丢失查原commit，不重新下载；请求预算/对象大小保持
NativeAcquisitionService.finalize | (self, command: QueueCommand, *, lease: WorkerLease) -> QueueReply | 严格校验task/generation/epoch/receipt及final row，调用冻结finalize_task原子结果提交+ACK，返回原fact | 不调用QueueDatasource.acknowledge，不在全局cut后另做ACK；未知final不声明成功
NativeAcquisitionService.finish | (self) -> Mapping[str, Json] | source.assert_settled后返回snapshot_metrics，全部task原生fact齐全才complete | 原始未确认/耗尽任务使整个action incomplete，不能用worker退出替代
NativeAcquisitionService.close | (self) -> None | 停止claim，排空有界inflight，关闭RawFetch/source；未确认交回原lease/domain恢复 | 不提前释放未知delivery/删原acquisition对象；清理异常不漏其他owner
queue_service_request | (command: QueueCommand, *, services: RuntimeServices) -> QueueReply | QueueReader/fetch/ack kernel统一调用WorkerJournalPort.queue；保留record sidecar且控制帧只载ref | 不pickle QueueDatasource/原生DB/锁给worker，不另写队列状态
''')
group('H','execution/dataflow_authority.py','C16','I07,I10,I11',r'''
DriverJournalService.queue | (self, command: QueueCommand, *, lease: WorkerLease) -> QueueReply | 固定op白名单映射同一NativeAcquisitionService，逐请求校验assignment授权queue/task及fence | 任意source方法/SQL调用拒绝，worker不得调用complete或通用commit
WorkerJournalClient.queue | (self, command: QueueCommand) -> QueueReply | 有界类型化RPC，超时不推定未执行；按原operation_id查询结果 | finalize回执丢失不第二ACK；claim重复请求不领第二task
''')

group('H','execution/dataflow_hosts.py','C07','I01,I07,I11',r'''
LocalHostEndpoint.deliver_reply | (self, reply: ControlReply) -> None | 在当前allocation/boot的有界socket发回同operation_id回执，超时保留unknown | 不能给重启后的worker发旧回执，丢响应后幂等查询
RayHostEndpoint.deliver_reply | (self, reply: ControlReply) -> None | 调当前HostedDataflowWorker.deliver_control_reply；driver journal留本进程 | 不把RunJournal pickle成Ray actor，不创建第二authority
HostedDataflowWorker.deliver_control_reply | (self, reply: ControlReply) -> None | 控制线程将回执交本地Mailbox，业务等待独立唤醒 | 正在长UDF时仍可收回复；错operation拒绝
ControlMailbox.__init__ | (self, *, bounds: Bounds, check_stop: Callable[[], None]) -> None | 只建有限outbox/pending/condition表，控制数据不持有大payload | 达上限先背压，不无限pending futures
ControlMailbox.request | (self, event: WorkerEvent, *, deadline_ms: int) -> ControlReply | 注册同operation摘要，向outbox发布，业务线程等条件并尊重deadline/取消 | 不在actor控制event loop阻塞；超时不能推导远端未执行
ControlMailbox.drain | (self, *, max_events: int) -> tuple[WorkerEvent, ...] | 有界取发送事件；未收到回复仍保留有限inflight身份 | 单次poll不丢重要cut/effect，重复请求同摘要幂等
ControlMailbox.deliver | (self, reply: ControlReply) -> None | 按operation关联唤醒，重复相同reply无额外释放，拒绝冲突 | wrongreply不唤醒其他请求，unknown不变success
ControlMailbox.close | (self) -> None | 唤醒全部等待者并保留unknown效果证据，清空确定结束的控制对象 | cancel/driver丢失不永久挂future.result
''')
group('H','execution/dataflow_authority.py','C09','I07,I10,I11',r'''
DriverJournalService.submit | (self, event: WorkerEvent) -> bool | 前置校验身份/大小后进入有限服务队列，重复operation不重复排队；返回False仅代表尚未接收 | 线程池未空不能无限queue；拒绝不得产生业务效果
DriverJournalService.poll_replies | (self, *, max_replies: int) -> tuple[CompletedControl, ...] | 非阻塞收集已完成固定handler结果含目标allocation/boot，错误也有类型化reply | 慢queue获取不堵heartbeat/cancel；future未完成不能调用result阻塞
DriverJournalService.close | (self, *, deadline_ms: int) -> None | 停准入，排空或记录unknown，关闭服务线程池与原生service，幂等 | 已发请求不得被标unsent；关闭失败仍收其他owner
''')

group('M','execution/dataflow_model_journal.py','C09','I07,I09,I10',r'''
RemotePromptJournal.__init__ | (self, *, port: WorkerJournalPort, action: NativeActionView, bounds: Bounds) -> None | 实现现有PromptClient/Embedding所用的窄journal表面；只持端口与封存max_record_bytes/timeout_s/read_only/limit，无SQLite路径 | 未声明属性或方法不可通过__getattr__任意转发；关actor不能关driver journal
RemotePromptJournal.lookup | (self, request: Mapping[str, Json]) -> Mapping[str, Json] | None | 原RequestRecord.request_key原样传driver lookup，返回原journal值或None | 已确认结果复用，pending/unknown不能被当cache miss后发送
RemotePromptJournal.reserve | (self, request: Mapping[str, Json]) -> bool | 捕获CALL_RESERVATION的reservation/charges，与请求intent经一次RPC同一原生savepoint提交，回执后才允许发送 | Embedding缺charges在driver拒绝；0请求预算不折叠None；不再另调用泛化effect.reserve
RemotePromptJournal.reserve_http_retry | (self, request: Mapping[str, Json], **kwargs: Json) -> bool | 只映射原journal许可的确定HTTP失败重试字段，保留retry budget；unknown不可重试 | 未知kwargs前置拒绝，不开放requeue_uncertain
RemotePromptJournal.response | (self, request: Mapping[str, Json], record: Mapping[str, Json]) -> Mapping[str, Json] | 有界结果先存artifact再调用原response，返回原references；响应丢失查同key | 保存后断线不再次网络请求；image/media引用必须保持可恢复
RemotePromptJournal.failed | (self, request: Mapping[str, Json], error: BaseException, elapsed: float) -> None | 映射原错误分类与是否确定未发送；异常只传现有错误合同而非任意pickle | timeout/连接中断不自动unsent，elapsed必须有限非负
RemotePromptJournal.references | (self, request: Mapping[str, Json]) -> Mapping[str, Json] | 读原请求/响应引用，受授权与字节界限 | 不返回driver本地绝对路径冒充远端可读工件
RemotePromptJournal.media_references | (self, record: Mapping[str, Json], *, register: bool = False) -> Mapping[str, Json] | 确认媒体引用存在、归属与保留；register通过driver原生媒体实现 | 缓存命中图片丢失不可返回成功；旧ObjectRef格式不重写
RemotePromptJournal.close | (self) -> None | 只释放本worker等待者，driver仍拥有唯一原生journal | 一个actor结束不影响另一个节点正在确认的请求
RemotePromptRecovery.__init__ | (self, actor: CallableInstance, action: NativeActionView, node_id: str, *, services: RuntimeServices) -> None | 继承NativePromptRecovery的bind_client，重写构造为RemotePromptJournal，不打开RunPromptJournal；保留semantic_owners/row_local/copy_inputs | 真PromptActor首次调用复用原record/decode/prepare语义与request_policy
RemoteEmbeddingRecovery.__init__ | (self, actor: CallableInstance, action: NativeActionView, node_id: str, *, services: RuntimeServices) -> None | 继承NativeEmbeddingRecovery的scope_request/bind_actor/drain，构造只绑定RemotePromptJournal及有限inflight，保留响应上限 | Embedding没有Prompt coordinator，不得借用Prompt bind_client或伪造_coordinator
RemoteEmbeddingRecovery.verify_request_membership | (self, request: Mapping[str, Json]) -> None | 经native_request.bind_membership复用原journal.bind_source与原key，确认后才能lookup/send | 相同native_occurrence不同batch membership拒绝，不改历史embedding身份
bind_prompt_adapter | (actor: CallableInstance, *, services: RuntimeServices, node_id: str) -> None | 装配RemotePromptRecovery与原InProcessCoordinator预算上下文，_native_group_journal标识先于第一次reserve；client start使用原生命周期 | CALL_RESERVATION跨BlockingIOPool保持；request_gate从声明式policy重建
bind_embedding_adapter | (actor: CallableInstance, *, services: RuntimeServices, node_id: str) -> None | 装配RemoteEmbeddingRecovery与NativeActionView，原astart绑定actor journal，保留scope_request及逐批预算 | 真EmbeddingActor无coordinator也能正常发送/恢复；run预算与node预算同一次扣账
bind_agent_adapter | (actor: CallableInstance, *, services: RuntimeServices, node_id: str) -> None | agent配置仍唯一，子调用使用原动态工具执行器与同run额度；Codex环境能力保持真实恢复等级 | 不新造每行Dataset或业务toolloop；未知Codex请求不自动恢复成功
bind_media_adapter | (actor: CallableInstance, *, services: RuntimeServices, node_id: str) -> None | image输出使用driver原生对象参与者与工件端口，worker只持可达store/预算句柄，原ObjectRef和效果身份不变 | 缺对象参与者/读域不启动；生成后确认前故障不重复生成
make_native_action_view | (services: RuntimeServices, *, node_id: str) -> NativeActionView | 返回只读action_id/limits/fence/check_stop和有限预算端口，不冒充完整RunContext/RunAction | worker无run.directory/journal.path/publication_guard，调用未知能力立即拒绝
''')
group('H','execution/dataflow_authority.py','C09','I07,I10,I11',r'''
DriverJournalService.native_request | (self, command: NativeJournalCommand, *, lease: WorkerLease) -> NativeJournalReply | 固定白名单映射原RunPromptJournal/RunJournal.bind_source，恢复CALL_RESERVATION并在原事务原子扣账+intent，旧request_key不变 | 同operation不同payload冲突；driver不存在worker自己造的coordinator；不得generic getattr执行任意方法
WorkerJournalClient.native_request | (self, command: NativeJournalCommand) -> NativeJournalReply | 有界RPC搬请求及结果ref，保持reservation context与原错误，重试同operation | 日志不得打印api key或完整业务输入；unknown不转换false
''')

group('T','execution/dataflow_handles.py','C11','I08,I09',r'''
StoredMaterializedHandle.num_blocks | (self) -> int | 从已固定结果manifest返回block数，released句柄报原错误，不读取payload或重跑 | 空结果为0；多次调用无模型/源副作用
''')

group('V','../tests/runtime_v3_support.py','C15','I01,I07,I09,I11,I12,I14',r'''
RuntimeV3Fixture.__init__ | (self, root: Path, *, provider: str, bounds: Mapping[str, int]) -> None | 只用/tmp/demiflow-*创建本测试scope与实际source/sink，本进程断言模块来自工作树；外部transport可fake | 主仓editable导入立即失败，测试不得触达业务目录/在线服务
RuntimeV3Fixture.run | (self, scenario: str, *, fault: str | None = None) -> Mapping[str, Json] | 从真实Dataset/Pipeline/workbench公开入口运行指定场景，保存plan/worker/commit/source/输出证据 | 不允许fake compiler/allocator/barrier/journal/sink，未知scenario拒绝
RuntimeV3Fixture.crash | (self, boundary: str) -> None | 在真实事务/消息边界杀本测试owner进程，持久化边界命中证据 | 只throw一个普通异常不冒充process crash；不得杀外部进程
RuntimeV3Fixture.assert_semantics | (self, expected: object, actual: object, *, ordering: str) -> None | 类型/空值/负零/tuple/bytes/重复多重性比较，明确无序关系与有序结果 | 不用set去重比较join输出，不用JSON破坏类型
RuntimeV3Fixture.close | (self) -> None | 关闭本测试资源并检查租约/子进程/FD/owned临时目录，保留失败证据 | 清理失败不吞原失败，不rm非本scope目录
''')
# Test support paths are repository-root paths, not production package files.
for r in GROUPS:
 if r['file'].startswith('demiflow/../tests/'):r['file']=r['file'].replace('demiflow/../','',1)

group('H','execution/dataflow_authority.py','C10','I07,I08,I10',r'''
DriverArtifactPort.__init__ | (self, action: RunAction, *, bounds: Bounds) -> None | driver内部窄artifact端口，直接绑定原action journal，构造不依赖store/service，解除服务与store初始化循环 | 不可传给worker，无queue/effect/complete方法
DriverArtifactPort.reserve | (self, operation_id: str, amounts: Mapping[str, int]) -> None | 只准入driver自身artifact staging预算，复用原fenced预算事务和稳定operation | 不创建第二allocator，不借driver身份替worker绕过grant
DriverArtifactPort.artifact | (self, command: ArtifactCommand) -> ArtifactReply | 调同一apply_artifact_command，校验owner是当前action的合法对象，返回原pin事实 | 主进程并非无限权限，跨run owner拒绝
apply_artifact_command | (action: RunAction, command: ArtifactCommand, *, bounds: Bounds) -> ArtifactReply | 唯一artifact pin/unpin/inspect/reserve事务实现，driver本地端口与已验权worker service共用 | 重复operation幂等；未知command拒绝；旧已提交引用保护
DriverJournalService.bind_queue | (self, service: object) -> None | prepare在configure成功后绑定同action唯一NativeAcquisitionService；类型/action核对且只允许一次同对象 | 不能通过WorkerEvent绑定，不能运行中替换source/原生namespace
''')

group('G','planning/dataflow_compile.py','C02','I01,I05,I09',r'''
expand_operation_inputs | (operation: LogicalOp, *, path: tuple[str, ...], bounds: Bounds) -> tuple[PlanInputBinding, ...] | 展开StreamPrependOp等持有Dataset的真实附加输入，prefix与main成为同一action显式端口，max_rows/max_row_bytes保留 | prefix不可在worker偷偷启动嵌套Dataset/action；prefix shared前缀按实例复用，超限不截断
''')
