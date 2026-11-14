# v2 共享接口与数据契约

> **2026-11-14：修订中，不可按旧任务卡直接派发。** Qwen Review 发现真实接口与验收缺口；当前 `dispatch_ready=false`。先读 [逐条裁决](review-20261114/RESPONSE.md) 和 [七组关键修订](review-20261114/CRITICAL-REVISION.md)。下文旧目标/草案保留追溯，不代表已经闭合。

以下为待实现目标。保留接口按基线签名；新增/修改签名以任务卡为准。`plan.json` 是派工索引，本文是语义权威；两者矛盾必须修设计，不能让 agent 各选一种。

## C01 通用值、引用与权限

- `JSONValue = None | bool | int | finite float | str | list[JSONValue] | dict[str, JSONValue]`。控制文档禁止 NaN/Infinity；数据行中的浮点值是另一份契约，不能因此修改数据值。
- `ArtifactRef = {path: str, sha256: str[64 hex], bytes: int >= 0}`；相对 run 根路径，无绝对路径、`..`、symlink 越界；先查大小，再有界读，再验摘要，再解析。
- `ProgramRef = ArtifactRef & {code_pin_digest: str[64 hex], code_sources: list[CodeSourceRef]}`；`CodeSourceRef = ArtifactRef & {source:str}`，source 为原模块来源，不是可执行命令。
- `ManifestRef = {manifest_id: str, sha256: str[64 hex]}`。
- Run/Action/Attempt 不混用：`run_id:str`、`action_id:str`、`attempt:int >= 1`；静态 DAG 和 program identity 不含 attempt、PID、对象地址或时钟。
- API 接收引用不等于授予读取/执行权限。读取者沿用当前项目授权；恢复还必须匹配原 fence、代码、输入、输出及 attempt。
- 未改动协议的键集和摘要保持原样。禁止为了方便消费者给所有 v1 文档补新键。

## C02 算子事实：F 生产，A/Q/L/S 消费

保留 `operation_capability(operation) -> dict` 与 `operation_facts(operation) -> dict`。前者决定 kind；后者不能再维护另一份精确类型准入集合。

基础返回键：`kind:str, driver:'pull'|'stream', native_options:bool, stream_options:bool, bound:bool, arguments:dict`。现有 native 算子可含 `native:dict, reserved_bytes:int>=0`；已有受支持类型的全部输出必须逐字节兼容。

| kind | arguments 的固定内容 |
|---|---|
| map | inputs/output/outputs；存在 MapStreamOptions 时包含 concurrency、queue_depth、execution、catch、label、callable_scope |
| async_map | concurrency、queue_depth、catch、label、hard_timeout、execution |
| batch_map | concurrency、queue_depth、catch、label、hard_timeout、max_batch、flush_interval |
| limit | limit |
| project/drop | cols:list[str] |
| rename | names:dict[str,str] |
| filter/flat_map | 保持既有空 arguments；callable 身份由既有代码固定链绑定 |
| map_batches | batch_size、batch_format、zero_copy_batch |
| add_column | column、batch_format |
| sample | fraction、seed |
| model_map | prompt/config 的有界语义身份、inputs/output/outputs、options 的非秘密语义、max_requests；不含随机 node_id |

native domain 必须来自声明，不遍历运行中客户端/锁/连接池：VectorSearch 的规范化 search_spec/query/output；grouping 的 on/output/chunk_bytes/max_groups/buffer_bytes；deduplicate 的 on/key/output 与既有持久边界；embedding 的模型契约和输入输出；prepend 的已固定 source identity 与 max_rows/max_row_bytes。其余新接纳类型逐个完成 facts→plan_identity→实际入口检查，不能只解开第一道守卫。

错误：未知/未表示语义抛既有 RuntimeContractError；不偷偷丢 native_options。F 不构造 Actor、不读源、不发模型请求、不提交结果。A 在入口选择正确能力后再让 payload_scope 读取事实，避免错误阶段倒置。

## C03 动作准入与路由：A 唯一负责

新增只读 `ActionDecision`（execution/capabilities.py）：

```python
@dataclass(frozen=True)
class ActionDecision:
    action_kind: str
    backend: str
    route: str
    recovery: str

admit_action_plan(source, plan, *, action_kind: str,
                  backend: str = 'local', sink_spec=None,
                  available_backends: tuple[str,...] = ()) -> ActionDecision
```

`route` 固定枚举：pull、stream_delivery、stream_materialize、native_relational、arrow_batch、lance_scan、queue_stream、static_graph。`recovery` 固定枚举：native_checkpoint、final_commit、delivery_only、attempt_only。不是由用户选择的另一套恢复模式，必须由动作、源和计划确定。

sink_spec 为 LanceWriteSpec 或 None；仅 write_lance 可传非空。available_backends 是 executor 已拥有能力的有限声明（local_pull/local_stream/datafusion/arrow_batch/lance_scan），不是 probe 或授权；Arrow 需要 spec.schema 及真实可用 local kernel。未提供时只选择普通 local 路线，不凭字符串启用后端。

纯检查阶段不读源/构造 Actor/创建 writer/claim queue。它复用 C02、现有 check_action/check_call，不另设 capability 表。具体 backend 的内部算法由封存 binding 固定，运行时不能再次自由选择。

| 输入形态与动作 | 结果/路线 | 强制行为 |
|---|---|---|
| 普通纯 map/filter + take/iter | pull 或既有等价原生路线 | 保持惰性、有限读取 |
| 合法 thread/inline streaming map + take/take_all/iter | stream_delivery | 必须继续支持；早停仍 drain/隔离迟到 worker，释放前保留 Run 所有权；不承诺跨 attempt 重放用户收取 |
| 流式计划 + materialize | stream_materialize | 原生 checkpoint、状态/效果适配器与预算先验；返回可重复读的 MaterializedDataset |
| 流式计划 + write_lance | stream_materialize + 唯一 LanceSink | 使用原生内部 checkpoint，再按原有目标写事务提交 |
| 流式计划 + count | 复用内部执行/有界计数 | 不能调用另一个公开 Dataset action 触发嵌套；不改 RunContext 的普遍嵌套保护 |
| take(0) | 先做静态合法性检查，再返回 [] | 不读源、不建 Actor、不发请求 |
| 懒 join/union 的流式输入 | 使用已完成显式边界或明确拒绝 | 不暗中跑子 Dataset、不开隐藏无限物化 |
| 本地收到 Ray-only native_options | 拒绝 | 读取前 UnsupportedExecutionOptionError |
| 退休 checkpoint 参数、私有 prompt journal | 拒绝 | 保留单一恢复协议；旧测试迁移意图而非拆守卫 |

Q 的预算不足与 S 的状态不支持不是“自动换路线”的依据。任何消费者不得捕获失败后偷偷走 legacy。

## C04 状态语义与身份：S 统一拥有

状态预检、运行时注册、identity、OperatorSpec 和 traits 使用同一个策略函数，避免 E/N3 分别猜测。

新增 `execution/operator_policy.py`：

```python
@dataclass(frozen=True)
class StatePolicy:
    mode: Literal['function', 'factory', 'hooks', 'auto_json', 'unsupported']
    ownership: Literal['none', 'partitionable', 'shared_colocated']
    adapter_present: bool
    reason: str | None

classify_operator_state(target, *, phase: Literal['declaration','instance'],
                        ownership: str = 'none') -> StatePolicy
snapshot_auto_state(owner, *, max_bytes: int) -> dict
restore_auto_state(owner, state: dict, *, max_bytes: int) -> None
```

判定顺序：普通 fn/builtin 保留既有无跨行业务状态约定；class 在 declaration 阶段是 factory；实例有完整 hooks 使用 hooks；只有一个 hook 必须拒绝；没有 hook 的普通 Python 实例，如果全部实例字段可按有界严格 JSON 无损保存/恢复，则用 auto_json；含锁/句柄/自定义描述器/不支持 slots 或不可表示状态的实例拒绝并要求原生适配器。**不把无 hook 当作无状态。** auto_json 是平台对普通数据状态的适配，不是新 journal；数据仍由 OperatorStates→StateArtifacts→同一个 commit 保存。

默认状态所有权明确固定：普通function为none；factory或auto_json如果调用者尚未声明状态所有权，采取shared_colocated，不把未知当作无状态。已有原生hooks按其明确声明保留partitionable/shared_colocated。auto_json初版要求同owner调用并发为1；声明更高并发时准入明确拒绝，不偷偷降并发；分区局部语义需显式partitionable适配器。编译和实例检查的ownership必须一致，实例不得悄悄升级封存拓扑。旧有效纯函数/hooks身份不变；过去错误放行的类不能通过修改旧记录来兼容，历史平台版本保护继续生效。

auto_json 初版只接纳普通 `__dict__` 数据实例，不能执行 property/custom __getattribute__；不支持形态必须明确拒绝，不先调用用户代码探测。JSON copy 无共享可变别名；保持字段集合，拒绝原型/dunder 注入。state = `{schema:'demiflow.auto_state.v1', fields:dict[str,JSONValue]}`。restore 检查 schema/预算/字段合法性后原子替换实例字段；验证完成前不改 owner。实例数据初始值仍参与 semantic identity。

`OperatorStates.validate(owner)` 保持签名；`register(owner,key)`、`snapshot(...)`、restore 仍由原对象持有 aliases/owners/identities。factory 在构造前可以通过；实际 `_MapWorker` 构造实例后必须再次 validate/register，且早于首行业务调用。Counter 必须能跨 checkpoint 恢复计数。资源型 native Actor 用显式 snapshot_state/restore_state/recovery_identity，禁止序列化连接池。

`precheck_operator_state(callable_spec, traits) -> bool` 保留返回“可用适配器”的既有语义；class 尚未构造时允许 deferred，实际实例是必验点。`compile_operator_spec` 的 state.adapter_present 表示适配器可用，不限定必须是对象两个方法；不会在编译时构造 class。

身份规则：
1. 普通 callable 的配置/constructor args/initial_state 不得因缺少 hooks 而消失。
2. thresholds 0.5/0.9、不同 ContextVar 业务值、不同 model/prompt、不同 source 必须区别。
3. 只有明确证明无业务语义的同步资源可以使用稳定资源声明；不能把所有锁/ContextVar/Thread 统一按类型吞掉。
4. 对未证明可重建的运行时依赖，使用现有显式 `recovery_identity()` 及原生适配器，或明确拒绝 durable 路径；不得声称 plain run_stream 的成功等于可恢复。
5. 已接纳历史身份的规范化字节不变；增加过去明确拒绝类型须有碰撞反例。platform_revision 变化导致旧运行拒绝执行仍保留；历史阅读不重算、不改写载体。

S 可提出 recovery_identity.py 的必要最小修复，但只接受上述语义，不继承“两个对象当前被某一路拒绝，所以允许抹掉身份”的推理。本修订的字节冻结路径和摘要明确列在 review-20261114/frozen-files.json，不再使用未枚举的“其余六个”。签名保留，不增加隐式全局模式开关。

## C05 buffer/barrier 与恢复：R 负责

Actor 语义状态归 S；pending rows、批次边界、grouping、prepend 位置归 R。模型响应与不确定调用归 M。三者通过同一 cut 提交，不互相新建账本。

新增 `execution/stage_buffers.py` 的有状态 `StageBufferAdapter`，由 `_Stage` 持有，可实现多种 native kind：

```python
StageBufferAdapter(stage, *, action, stage_key: str)
async StageBufferAdapter.align(barrier, pending) -> dict
StageBufferAdapter.restore(snapshot: dict) -> None
StageBufferAdapter.close() -> None
make_stage_buffer_adapter(stage, *, action, stage_key, restored=None)
    -> StageBufferAdapter | None
```

**旧 snapshot 全键集冻结已撤回。** `groups/row_ordinals/pending_batches/next_output_ordinal` 的关系尚未完成 cut/slot/恢复证明，不是可编码的持久协议。`StageBufferSnapshot` 在类型草案仅保留未定义标记；R 的相关任务继续 blocked，不能自行猜 payload。

能力分类 `CheckpointStageKind` 有六种：fixed_batch/concurrent_batch/timed_batch/grouped/prepend/native_row_local。前五种才是本地 buffer 类型；native_row_local 继续原 native_pending 注册表协议，不塞进新 buffer 快照。旧 `{batch_ordinal,data}` 不重命名；新增格式若获准必须显式版本与旧读者迁移，当前不创建虚假的 next_batch_ordinal 兼容。

旧 fixed-batch `{batch_ordinal,data}` 继续可读；不改写原快照。新 schema 仅用于新 native buffer 形态。

cut 顺序：停止跨 cut 的新输入 → 排空或按已有 native effect 协议固定已发调用 → 固定未消费行、批次编号与分组/前缀位置 → 与 Actor 状态共同进入原 commit → ACK 后释放旧快照。不得让有副作用的未确认调用因 buffer 重放再次发送。无法判定的效果保持 unknown，不能以超时当作未发生。

恢复顺序：核验身份/载体 → 恢复 Actor → 恢复 buffer 与 ordinal → 重放已提交输出 → 从原输入 frontier 继续。禁止把单个 stage 的 max_batch/concurrency 强行改成 1 来冒充对原计划的正确支持。

R 拥有 stream.py、stream_materialize.py 的接入；S/M 只提供固定适配接口，不并发改这些文件。已有 native row-local adapter 的 fields/methods 保留，R 不能让新 buffer 层覆盖它。

write_table create 的 absent-target 准入保留已实现 exact intent/operation/request_hash 绑定。已发布目标、unknown receipt、历史代码不符、append/merge/overwrite 不得套用该特例。恢复使用原封存 program/manifest/bindings，不重新 pickle 决定身份。

## C06 资源、队列与编码：Q 负责

`stream_limits(facts,limits,source_batch_size=1,*,retained_rows=0)->dict` 只返回更紧或相等的预算，不修改输入；事实无效、固定驻留不可能准入应在读取前失败。固定 Actor 保留量与行/队列动态保留量不能重复计算，也不能把真实固定保留量伪装成可缩减行预算。

`StreamResources.make_queue(depth,*,max_bytes=None,max_item_bytes=None)` 保留签名，生产队列只有既有 ObservedQueue；不能把任意 asyncio.Queue 当成已证明字节有界。旧 seam 测试迁移到实际有界队列观测/注入点，不新增第二个 transport。

`_canonical_row(value)->value` 和 `BlockSink.append(row)`：保留值类型、字典插入序、浮点符号和 NaN 位模式；容器去重只按不会改变编码语义的签名。float 签名使用明确的 IEEE-754 字节，而不是 Python `==`。不合并 bool/int/float；不改变 tuple/list/bytes；不调用不透明对象用户代码。控制 JSON 拒绝 NaN 不意味着数据行编码可以改掉 NaN。

set/frozenset 不能保证跨进程重放字节时，明确边界并保留类型；若要宣称全合法值重放，必须补稳定编码与跨 PYTHONHASHSEED 反例。不得通过取消 `Budget reservation changed` 守卫解决漂移。输入 check_size、编码后上限、累计 output_rows/row_bytes、磁盘准入全部保留。

资源窗口必须由封存布局传到底层 reader/queue/stage/sink。只改文档里的预算而执行仍读当前默认值不合格。性能在同一工作负载和预算下比较；吞吐下降超过 10% 或峰值增加超过 10% 必须定位解释，不能增加预算掩盖。

## C07 模型与效果：M 负责

Prompt 保留 RunPromptJournal / NativePromptRecovery / request_policy；不恢复用户私有 journal 参数，不混用 retired gate 与新声明式 request_policy。

Embedding 使用同一 run-owned effect authority，但其真实 HTTP 请求由 `EmbeddingActor._exchange/_fresh_exchange` 发送，并不存在 PromptClient.record/execute/decode 接口。因此不复制 NativePromptRecovery.bind_client。

新增 `NativeEmbeddingRecovery(actor,action,node_id)`，字段：actor/action/node_id/journal、`semantic_owners:tuple=()`、`row_local:bool=True`、`copy_inputs:bool=False`。inflight:dict[str,Task] 记录真实在途请求；journal 借用 run 的 RunPromptJournal；首次取得时执行 check_recovery。`bind_actor()->None` 把 `_BorrowedJournal(journal)` 安装到 actor._journal，关闭借用包装不得关闭 run journal；`scope_request(request)->RequestRecord` 在原请求 JSON 加 native_response_bytes 和 native_occurrence，绑定 action/node/current_row_scope.occurrence，重新使用 RequestRecord 规范 request_key。scope 不存在即拒绝。Actor 现有 snapshot_state/restore_state 空状态钩子保持：连接是资源，效果进度归 journal，输入余量归 C05。不另造无人消费的适配器快照接口。

`EmbeddingActor.native_stream_recovery(action,node_id)` 缓存该 actor 的适配器；astart 建本 actor 资源后调用 bind_actor；_exchange 在第一次 lookup 前 scope_request；reserve真实发生在_exchange；该处进入native_request_budget设置CALL_RESERVATION并覆盖reserve到send/持久化结束。_fresh_exchange仅保留send/failed/response-before-return，不承载reserve入口。aclose先await NativeEmbeddingRecovery.drain，再关闭本actor资源及借用包装。BlockingIOPool.run已有copy_context，不另建传播机制。cancel_drain_timeout_s 在 native journal 存在时也给出有限 drain 时长，不能因 sqlite_journal=None 返回零。此修改由 M 的独立叶任务分别承接，R 只按既有 native capability 属性消费。

请求状态至少区别未发送、发送中/unknown、响应已持久化、输出已提交。已持久化响应重用不再占 fresh-call 配额；unknown 默认拒绝自动重发；取消排空已发响应并存证，不能发布不完整成功。请求键绑定固定输入、模型/提示/配置及 occurrence/batch lineage，不能使用 attempt 或本机地址。

PromptActor 与 EmbeddingActor 的资源生命周期仍由各 actor 开关 pools/client；恢复适配器不夺取所有权。native buffer 通过 C05 固定 batch lineage，M 不能在恢复时重新分批生成不同请求键。

预算 helper：`native_request_budget(run,*,node_id,max_requests)->ContextManager[str]`，通过 `@contextlib.contextmanager` 设置/清理既有 CALL_RESERVATION。_enqueue 在调用线程读取并附payload，writer线程仅从payload取值；原RunPromptJournal的预算与intent同savepoint原子性保持。相同charges重复预留已幂等；禁止组件集合不一致的双重预留。native的_calls仅观测、私有SQLite计数不作第二权威；非native路径原行为保持。构造器max_requests=0原本就拒绝，新helper单独验证0上限，不报告为现网漏洞。

## C08 一份封存布局实际驱动所有已支持路线：L 负责

现有 `CheckpointPolicy/WorkerPlacement/ChannelBinding/ExecutionLayout` 与 v1 读取保留。现有 v1 支持集合：chain_v1、queue_transform_v1、static_fanout_v1。不能把 driver 原生执行冒充一个实际不存在的 worker。

为补真实 driver backend binding，新增 **execution_bindings.v2**（不是修改 v1 键集）。顶层键与 v1 同名身份/资源/源/通道/checkpoint 键保持；用 `executions` 代替 `programs`，保留 driver_stage_ids。executions 每项严格为：

```text
{execution_id:str, placement:'driver'|'worker', group_id:str|null,
 backend:'local_stream'|'local_pull'|'datafusion'|'arrow_batch'|'lance_scan',
 stage_ids:list[str], profile_ref:str, programs:list[ProgramBinding],
 recovery:'native_checkpoint'|'final_commit'|'delivery_only'|'attempt_only'}
ProgramBinding = {program_id:str,input_path:list[str|int],plan_ref:ProgramRef,
 source_identity_ref:str,
 sealed_operators:list[{logical_node_id:str,occurrence_id:str}],
 operator_bindings:list[{program_index:int,stage_id:str}]}
```

v2 顶层精确键集为 `{schema,run_id,action_id,dag_id,plan_id,artifact_manifest_ref,supported_slice,profiles,executions,driver_stage_ids,source_bindings,channel_options,checkpoint_rows}`；schema 固定 `demiflow.execution_bindings.v2`。同名 v1 字段完全继承约束。执行单元编号 execution_id=identity([action_id,"driver",decision.route])；backend 取 decision.backend，recovery 由动作声明确定，不能依 budget 失败重选。

ProgramBinding.sealed_operators 是该子程序**线性局部编译**的原始顺序，供原 verify_loaded_plan 使用；operator_bindings 单独映射到完整 DAG 的 occurrence/stage。绝不能把重编号的全图 logical_node_id 直接喂给要求局部索引的 verify_loaded_plan。

input_path 是从根计划定位 PlanInput 的稳定结构路径，根为 `[]`；子项使用真实字段及索引，例如 `["source","left"]`、`["source","inputs",0]`，递归嵌套依此拼接。不得用 id(object) 生成 program_id；program_id=identity([action_id,input_path])。相同对象在两个位置不自动折叠成共享执行，只有明确共享声明才能复用。循环引用拒绝。

source_identity_ref=identity(describe(source))，覆盖该子计划真实源声明和 callable 源的代码语义；持久读数据另按既有 source_bindings 固定版本/源块。需执行的 source 若不能稳定 describe/fix，durable 路径先拒绝；不伪称通用任意 Python source 都可恢复。源代码在原 action code archive 中必须可定位；没有历史代码载体的外部源仅展示不可用，不冒充当前代码就是历史代码。

新增只读 `ProgramSlice(program_id,input_path,source,plan,sealed_operators,operator_bindings)`，其中 source/plan 是借用的只读实际声明；编译返回后不得变更。新增只读 `ExecutionPlacement` 对应 execution 条目（profile_ref 解释为递归只读 profile，programs 为 ProgramBinding 的只读 tuple；额外 stage_limits:Mapping[stage_id,Mapping[str,int]] 从 physical.stages[].budgets 唯一派生，不写入第二份 wire 权威）；`ExecutionLayout.executions:tuple[ExecutionPlacement,...]` 为末尾默认空 tuple 的加性内存字段。v1 解释时从原 worker program 派生一个 ProgramBinding；v1 identity和载体不变。

v2 的 source/terminal stages 归 driver_stage_ids；所有 driver 执行 operation stages 也列在 driver_stage_ids；worker operation stages 与之互斥。所有 operation stages 恰好属于一个 execution；source/terminal 不需要伪造算子程序。空 plan 允许 execution 的 stage_ids/programs 为空，但源与 terminal 必须被物理计划覆盖。v2 首批 native_driver_v1 只接 driver；未来 v2 worker 没有实现前明确拒绝，不能只因为 schema 允许就放行。v1 workers 行为不变。

`seal_execution_bindings(..., supported_slice, executions=None)`：None 保留 v1；v2 supported_slice='native_driver_v1'，旧 programs 参数传空序列。新 consumer 必须显式按 schema 分派；未知 schema 拒绝；历史 v1 不自动升级。

新增 `execution/native_plan.py`：

```python
stage_native_programs(action, program_slices) -> tuple[ProgramBinding,...]
verify_native_programs(source, plan, *, programs, action_id) -> None
compile_native_topology(action, source, plan, *, decision) -> tuple
load_native_execution(run, action_id, *, execution_id) -> tuple
execute_native_plan(executor, source, plan, *, layout, execution_id,
                    arrow=False, sink_schema=None) -> Iterator
```

compile 调用 F 编译器后 stage_native_programs：本函数仅服务native_driver_v1，逐slice调用seal_write_plan_ref(action,slice.plan,source=slice.source,program_id=slice.program_id)，保存code-pin-only ProgramRef；独立worker仍由既有stage_plan_artifact发送in-run pickle。封存后从同一编译结果产生binding；恢复读取原引用不重新pickle。verify按封存placement/backend选载体校验方式，native重算包含source callable依赖的plan_code_pin；worker沿原verify_loaded_plan。pin和归档现状已共用_business_dependency_modules，本轮只扩source根；缺段、旧记录未覆盖source代码、错input_path或错stage映射均拒绝，不重写旧档案。

load 返回 `(ExecutionLayout,ExecutionPlacement)`。execute 只能执行 placement.backend/profile/原 program，先 verify_native_programs 后下发；不能再自由 eligible() 重选路线。DataFusion 内部优化由 DataFusion 所有，平台只表达 backend stage 的真实粒度，不伪装逐个底层算子独立调度。`dataset_native.execute(...,execution=None)`、`arrow_batch_sink.execute(...,execution=None)` 为加性参数；绑定调用必须消费 profile 和 stage 预算。

`planning/dataset_dag.py:compile_dataset_logical_dag(*,action_id,source,plan,terminal_kind,sink_ids)` 返回 `(LogicalDag,tuple[ProgramSlice,...])`。递归真实 PlanInput，线性片段用 dag.py 公开 API 编译，保留局部身份，另确定全图 occurrence；使用既有 validate_logical_dag/physical validator。无任意业务 import、无源读取。

local take/iter_rows 的 native_relational 路线同样要 compile/load→execute_native_plan；不能只有 write_lance 走新布局。write seal 的 decision 参数加性，恢复时原绑定为权威，新 decision 仅用于验证兼容，不能替换历史 backend。纯 delivery 不宣称跨 attempt 可恢复。

chain/graph/queue worker 保持原 launch_spec.execution_plan `{action_id,group_id,bindings_ref}`；worker 先读同一封存输入、验证 assignment/code-pin，再运行。控制 receipt 只有 driver 提交；缺失/损坏 binding 不回退。等待有 deadline/cancel；不能在 event loop 无限 future.result()。

## C09 工作台事实、控制与产品：W/V 消费

IdentityBridge v1 保持 `{schema,action_id,dag_id,report_digest,entries}`；entry 固定 `{report_node_id,report_occurrence,logical_node_id,occurrence_id,mode}`。不写 attempt/状态计数；已有 observation 以 action/attempt/occurrence 精确 join。

read_sealed_plan 返回 `available=True` 的原 F1/图/物理计划，或原有 unavailable reason；read_identity_bridge 成功 `{available:True,ref,bridge}`，失败 `{available:False,reason,detail}`。分页中缺席是 not_in_page，不等于从未执行。

展示增加 `binding_level:'declared'|'sealed'`：declared=静态报告；sealed=引擎合法封存计划。**来源等级与执行状态是两个不同字段。** 不增加 executed 伪证据，也不因为存在 F1/dag_id 就说运行成功。运行状态/进度/结果直接来自原 attempt 的 observation/receipt，分页不完整如实显示。历史字段 basis='executed' 兼容读取时仅表示运行封存来源，前端文案仍用“运行封存计划”；不新增 journal 事件或工作台账本。真实是否消费布局由 L 的执行路径与验收反例证明，而非 UI 标签证明。

Run 选择固定 `(run_id,attempt)`；历史源码/参数读封存载体，编辑视图另有 workspace revision。恢复/停止调用现有 manager，核 expected/expected_attempt 和真实进程所有权；前端不改状态文件模拟操作。

Notebook/Python/SQL、Case、媒体、开发会话继续独立生命周期，不能强制变成 Pipeline run。它们的现有协议由源码快照与 V 的功能验收锁定；函数任务不授权重写已完成产品。只有发现具体不满足目标的行为才修其责任函数，保留 existing state/CAS/generation/权限。

## C10 错误、副作用与连接规则

PlanContractError 的 reason 保留现有固定集合；S 使用既有 ProtocolError(STATE_ADAPTER_MISSING/INVALID_SPEC)；A 使用既有 UnsupportedExecutionOptionError/退休契约错误；预算使用原 RecoveryContractError。不得用空 dict/None 表示验证通过但没有值；未预期异常不得伪装 missing。

所有读取/编译/helper 标注 pure、read_only、runtime_state、durable_write 之一。durable_write 必须写明原 RunJournal/commit/StateArtifacts 中的 owner。前置拒绝应满足源读取=0、Actor 构造=0、请求数=0、目标副作用=0；运行中失败则排空/收尾并如实保留 unknown。

每条任务卡列 caller/callee；新公开接口不能只有生产者没有消费者。一个消费者不得更改对方 wire shape。跨包集成在同一 revision 使用真实模块；测试替身只替换外部 transport 或符合固定 schema 的边界值。

## C11 数据类与生命周期装配

字段和准确类型见 [interfaces.pyi](interfaces.pyi)。这是设计声明，不是可导入运行时代码。Python 的 frozen dataclass 只冻外壳；profile/program/document 内层必须递归冻结并脱离调用者可变对象。恢复状态仍由已有 journal/StateArtifacts 持有，数据类不新增数据库。

新字段：OperatorStates.policies 为按 owner identity 关联的 StatePolicy；保留现有 owners/aliases/identities。_Stage.buffer_adapter 默认 None，与 slots 同步。EmbeddingActor._native_recovery 默认 None。_WriteSeal 增加 native_layout/native_execution_id/source，默认 None；首次编译与恢复加载都先验证再设置。字段声明和构造器是一个文件装配事务。

能力不足（无法证明资源可恢复、外部 source 不可固定、环境没有 Ray）必须明确显示受支持边界，不能算相应验收通过。100% 不能由本包索引计数推出；必须原意图完整集合都有实际证据，不指任意输入/任意第三方对象都能自动恢复。

## C12 原生预算接入的确切位置

`dataset_native.execute(...,execution=None)` 传给 `Engine(...,execution=None)`；Engine.session 调 `native_resources.dataset_session(...,execution=None)`。同一placement借用到底，不修改共享executor/options。session的pool/RSS/scratch取原run/envelope/native配置与封存资源上限的有效交集；threads由CPU/native配置推导，rows由阶段/reader预算决定；ResourceProfile没有threads/rows字段；原allocate/admission仍是同一权威。Engine.batch_rows对真实payload宽度核字节上限，不能在单行超限时靠max(1)继续。Arrow execute在TaskScheduler构造前按同一规则得到局部options，不改executor全局值。

stage_limits不是“建议”：inflight_rows限制当前阶段同时在途行数，inflight_bytes限制当前阶段输入/排队字节，output_bytes限制输出暂存。backend合并执行多个平台stage时按同时存活阶段计量，不能每个stage各拿完整run预算。物理I/O可以缩批；明确声明的业务batch_size/max_batch不能为资源测试偷偷改变语义，整批不能准入就明确失败。性能门要求验证真实效果，不接受仅参数透传。

C09 UI映射补充：图顶点、边、DOM/select/onSelect都保持logical_node_id；物理摘要仍按logical ID查询。occurrence只在详情Selection={logical_node_id,occurrence_id}中选择，复用已有identity_bridge/picker与_resolve_bridge_node。_bridge_node_map保留原logical键和多义省略规则，不新增occurrence图映射。state.node仍是报告节点ID，新增state.logical_selection；chooseNode更新二者并递增detail，transition清空；loadSource携带二元键并校验ticket及响应身份。GET_FIELDS['graph-source']仅扩两可选键；source_view.get已支持转发，按verify_preserve核验，不重复新建。

C08 ACK回调固定为async：WorkerChainExecutor._request_commit与GraphWorkerExecutor._request_contribution返回Awaitable[CommitReceipt]。原progress.seal返回publication callable，stream_barrier已支持inspect.isawaitable后await，因此无需再造线程提交器。等待上限取既有 checkpoint_timeout_s 与有效运行/控制 deadline 的最小剩余值（不另造120秒默认），使用asyncio.wrap_future/有限await，取消按原信号传播；超时抛TimeoutError，保留unknown和未确认提交，不自动重发、不提前释放所有权。测试必须同时证明event-loop heartbeat未冻结及迟到ACK不能推动新cut；只测“最终返回”不足。
