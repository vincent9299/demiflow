# 函数级任务总表

一个叶任务=一个函数/方法。数据类声明、imports/slots 的装配按 FILE-OWNERSHIP.md 处理；不能让函数 agent 自由改变公共字段。

`verify_preserve` 表示有实现，先验证并保留，只有发现该合同的真实缺陷才修改该函数；不能重复实现。

| ID | 包 | 文件/函数 | 动作 | 输出 | 验收 |
|---|---|---|---|---|---|
| [F001](tasks/F001.md) | F | `demiflow/execution/plan_facts.py` · `operation_capability` | implement | CapabilityFact，C02 基础键，不新增 admitted 字段 | FACTS, HISTORY |
| [F002](tasks/F002.md) | F | `demiflow/execution/plan_facts.py` · `operation_facts` | implement | OperationFact：C02 arguments/native/reserved_bytes | FACTS, VECTOR, EMBED, PREPEND, HISTORY |
| [F003](tasks/F003.md) | F | `demiflow/execution/plan_facts.py` · `_catch_facts` | verify_preserve | None 或 sorted list[str]；原空值 null/[] 不变 | FACTS, HISTORY |
| [F004](tasks/F004.md) | F | `demiflow/planning/dataset_dag.py` · `compile_dataset_logical_dag` | implement | (LogicalDag v1, tuple[ProgramSlice,...])；路径完整且局部程序→全图stage映射唯一 | LAYOUT, RELATIONAL, HISTORY |
| [F005](tasks/F005.md) | F | `demiflow/planning/graph_dag.py` · `compile_graph_logical_dag` | verify_preserve | (LogicalDag v1, list[program mapping])；保留现有group_id/role/reader_id/operators键 | GRAPH, HISTORY |
| [A001](tasks/A001.md) | A | `demiflow/execution/capabilities.py` · `admit_action_plan` | implement | ActionDecision(action_kind,backend,route,recovery) | ADMISSION, DELIVERY |
| [A002](tasks/A002.md) | A | `demiflow/execution/capabilities.py` · `validate_stream_plan` | implement | None；无效抛原能力错误 | ADMISSION, FACTS |
| [A003](tasks/A003.md) | A | `demiflow/data/dataset.py` · `Dataset.run_stream` | implement | StreamStats；原回调/取消/资源语义 | ADMISSION, STREAM, QUEUE |
| [A004](tasks/A004.md) | A | `demiflow/data/dataset.py` · `Dataset.count` | implement | int >= 0 | ADMISSION, STREAM |
| [A005](tasks/A005.md) | A | `demiflow/data/plan.py` · `reject_streaming_map_options` | implement | None 或 UnsupportedExecutionOptionError | ADMISSION, DELIVERY |
| [A006](tasks/A006.md) | A | `demiflow/execution/executors/local.py` · `LocalDatasetExecutor.take` | implement | list[RuntimeRow]，最多 limit 行 | DELIVERY, ADMISSION |
| [A007](tasks/A007.md) | A | `demiflow/execution/executors/local.py` · `LocalDatasetExecutor.iter_rows` | implement | Iterator[RuntimeRow]；关闭时原所有权收尾 | DELIVERY, STREAM |
| [A008](tasks/A008.md) | A | `demiflow/execution/executors/local.py` · `LocalDatasetExecutor.write_lance` | implement | LanceWriteReceipt；uri/version/rows/operation_id等字段保持现有模型 | WRITE, RELATIONAL, LAYOUT, HISTORY |
| [S001](tasks/S001.md) | S | `demiflow/execution/operator_policy.py` · `classify_operator_state` | implement | StatePolicy；C04 完整枚举与字段 | STATE, IDENTITY |
| [S002](tasks/S002.md) | S | `demiflow/execution/operator_policy.py` · `snapshot_auto_state` | implement | {schema:'demiflow.auto_state.v1',fields:dict} | STATE, BOUNDS |
| [S003](tasks/S003.md) | S | `demiflow/execution/operator_policy.py` · `restore_auto_state` | implement | None；失败时 owner 不变 | STATE, BOUNDS |
| [S004](tasks/S004.md) | S | `demiflow/execution/operator_precheck.py` · `precheck_operator_state` | implement | bool：是否有可用 state adapter；factory 为延后实例检查 | STATE, VECTOR |
| [S005](tasks/S005.md) | S | `demiflow/execution/operator_precheck.py` · `precheck_plan_state` | implement | None；失败使用原 STATE_ADAPTER_MISSING | STATE, ADMISSION |
| [S006](tasks/S006.md) | S | `demiflow/execution/operator_state.py` · `OperatorStates.validate` | implement | None；unsupported/半套 hooks 抛 RecoveryContractError | STATE |
| [S007](tasks/S007.md) | S | `demiflow/execution/operator_state.py` · `OperatorStates.register` | implement | None（async）；更新 owners/aliases/identities，必要时恢复 | STATE, RECOVERY |
| [S008](tasks/S008.md) | S | `demiflow/execution/operator_state.py` · `OperatorStates.snapshot` | implement | dict[stage_key,JSONState]（async）；聚合受预算限制 | STATE, RECOVERY, BOUNDS |
| [S009](tasks/S009.md) | S | `demiflow/execution/contracts/operator.py` · `_adapter_present` | implement | bool | STATE, LAYOUT |
| [S010](tasks/S010.md) | S | `demiflow/execution/contracts/operator.py` · `compile_operator_spec` | implement | validated OperatorSpec v1；已支持旧输入输出兼容 | STATE, LAYOUT, HISTORY |
| [S011](tasks/S011.md) | S | `demiflow/planning/traits.py` · `unified_operation_traits` | implement | 原 OperationTraits dataclass 值 | STATE, VECTOR |
| [S012](tasks/S012.md) | S | `demiflow/execution/stream_map.py` · `_MapWorker.prepare` | implement | None（async）；原 callable 生命周期 | STATE, DELIVERY |
| [S013](tasks/S013.md) | S | `demiflow/execution/recovery_identity.py` · `describe` | implement | JSON 语义描述；已有成功类型字节不变 | IDENTITY, HISTORY, STATE |
| [S014](tasks/S014.md) | S | `demiflow/lance/search.py` · `VectorSearch.snapshot_state` | implement | 有界 JSON：固定表版本/语义进度，不含锁/client | VECTOR, STATE, RECOVERY |
| [S015](tasks/S015.md) | S | `demiflow/lance/search.py` · `VectorSearch.restore_state` | implement | None | VECTOR, STATE, RECOVERY |
| [R001](tasks/R001.md) | R | `demiflow/execution/stage_buffers.py` · `StageBufferAdapter.__init__` | implement | StageBufferAdapter：stage/action/stage_key、kind、受限 pending、ordinal、closed | BUFFER |
| [R002](tasks/R002.md) | R | `demiflow/execution/stage_buffers.py` · `StageBufferAdapter.align` | implement | C05 StageBufferSnapshot（async） | BUFFER, RECOVERY, UNKNOWN |
| [R003](tasks/R003.md) | R | `demiflow/execution/stage_buffers.py` · `StageBufferAdapter.restore` | implement | None | BUFFER, RECOVERY |
| [R004](tasks/R004.md) | R | `demiflow/execution/stage_buffers.py` · `StageBufferAdapter.close` | implement | None，幂等 | BUFFER, BOUNDS |
| [R005](tasks/R005.md) | R | `demiflow/execution/stage_buffers.py` · `make_stage_buffer_adapter` | implement | StageBufferAdapter | None | BUFFER |
| [R006](tasks/R006.md) | R | `demiflow/execution/stream_materialize.py` · `StreamStageProgress.bind_stages` | implement | None；设置 stages/group_ends/buffer adapter/direct_owners | BUFFER, EMBED, VECTOR |
| [R007](tasks/R007.md) | R | `demiflow/execution/stream_materialize.py` · `StreamStageProgress.align` | implement | None（async）；写本 cut 的 barrier.states | BUFFER, RECOVERY, UNKNOWN |
| [R008](tasks/R008.md) | R | `demiflow/execution/stream_materialize.py` · `StreamStageProgress.restore_stages` | implement | None（async） | STATE, BUFFER, RECOVERY |
| [R009](tasks/R009.md) | R | `demiflow/execution/stream_materialize.py` · `prepare_stream_blocks` | implement | Iterator[Arrow/Python batch]；原最终写入输入 | WRITE, BUFFER, RECOVERY |
| [R010](tasks/R010.md) | R | `demiflow/execution/stream.py` · `_Stage.__init__` | implement | 完整 _Stage 实例；新增 buffer_adapter=None，既有 slots 保留 | BUFFER |
| [R011](tasks/R011.md) | R | `demiflow/execution/stream.py` · `_materialize` | implement | list[_Stage]，operation_index/budget/adapter 位置固定 | BUFFER, LAYOUT |
| [R012](tasks/R012.md) | R | `demiflow/execution/stream.py` · `_arun` | implement | StreamStats；传入stats实例，rows_in/rows_out/errors/耗时按原结构更新 | BUFFER, RECOVERY, UNKNOWN, BOUNDS |
| [R013](tasks/R013.md) | R | `demiflow/execution/run_context.py` · `RunContext.action` | verify_preserve | contextmanager[RunAction] | WRITE, HISTORY, RECOVERY |
| [R014](tasks/R014.md) | R | `demiflow/execution/run_context.py` · `create_target_absent_binding` | verify_preserve | dict{kind,action_id,operation_id,request_hash,uri,mode}或None；None不是准入 | WRITE, HISTORY |
| [Q001](tasks/Q001.md) | Q | `demiflow/execution/block_sink.py` · `_canonical_row` | implement | 值/类型/float bits/字典序保真的 RuntimeValue | CODEC, BOUNDS |
| [Q002](tasks/Q002.md) | Q | `demiflow/execution/block_sink.py` · `BlockSink.append` | implement | None；向本 sink 有界 buffer 追加 | CODEC, BOUNDS |
| [Q003](tasks/Q003.md) | Q | `demiflow/execution/block_sink.py` · `BlockSink.prepare` | verify_preserve | BlockDescriptor；原 commit-keyed 幂等预留 | CODEC, RECOVERY, BOUNDS |
| [Q004](tasks/Q004.md) | Q | `demiflow/execution/payload_limits.py` · `stream_limits` | implement | dict 有效 limits；每项<=输入预算 | BOUNDS, STREAM |
| [Q005](tasks/Q005.md) | Q | `demiflow/execution/payload_limits.py` · `payload_scope` | implement | contextmanager[dict limits] | BOUNDS, ADMISSION |
| [Q006](tasks/Q006.md) | Q | `demiflow/execution/stream_resources.py` · `StreamResources.make_queue` | implement | ObservedQueue 实例；depth/bytes/item 三重限制 | BOUNDS, STREAM |
| [M001](tasks/M001.md) | M | `demiflow/operator_llm/stream_recovery.py` · `NativePromptRecovery.bind_client` | verify_preserve | None；将既有 client 绑定 RunPromptJournal | PROMPT, UNKNOWN |
| [M002](tasks/M002.md) | M | `demiflow/operator_llm/stream_recovery.py` · `RunPromptJournal.check_recovery` | verify_preserve | None 或 UncertainPromptCall | PROMPT, UNKNOWN |
| [M003](tasks/M003.md) | M | `demiflow/embeddings/stream_recovery.py` · `NativeEmbeddingRecovery.__init__` | implement | 适配器 actor/action/node_id；借用同一 run effect authority | EMBED, UNKNOWN |
| [M004](tasks/M004.md) | M | `demiflow/embeddings/stream_recovery.py` · `NativeEmbeddingRecovery.bind_actor` | implement | None | EMBED, UNKNOWN |
| [M005](tasks/M005.md) | M | `demiflow/embeddings/runtime.py` · `EmbeddingActor.native_stream_recovery` | implement | NativeEmbeddingRecovery | EMBED, BUFFER |
| [M006](tasks/M006.md) | M | `demiflow/embeddings/runtime.py` · `EmbeddingActor.astart` | implement | None（async）；原有客户端/pools 启动 | EMBED, BOUNDS |
| [M007](tasks/M007.md) | M | `demiflow/embeddings/runtime.py` · `EmbeddingActor._exchange` | implement | 原 embedding call/result 结构（async），结果字段不变 | EMBED, UNKNOWN |
| [L001](tasks/L001.md) | L | `demiflow/execution/plan_interpreter.py` · `_validate_bindings` | implement | 解释上下文字典，仅内部消费 | LAYOUT, HISTORY |
| [L002](tasks/L002.md) | L | `demiflow/execution/plan_interpreter.py` · `interpret_physical_plan` | implement | ExecutionLayout，增加末尾 executions 默认 tuple | LAYOUT, HISTORY |
| [L003](tasks/L003.md) | L | `demiflow/execution/plan_interpreter.py` · `validate_worker_assignment` | implement | WorkerPlacement；非法抛 invalid_assignment | LAYOUT, GRAPH, QUEUE |
| [L004](tasks/L004.md) | L | `demiflow/execution/sealed_plans.py` · `seal_execution_bindings` | implement | ArtifactRef，journal 中唯一 execution-bindings 记录 | LAYOUT, HISTORY |
| [L005](tasks/L005.md) | L | `demiflow/execution/sealed_plans.py` · `load_execution_bindings` | implement | {ref:ArtifactRef,document:ExecutionBindingsV1|V2} | LAYOUT, HISTORY |
| [L006](tasks/L006.md) | L | `demiflow/execution/sealed_plans.py` · `load_sealed_plan` | verify_preserve | {facts,logical_dag,physical_plan} | LAYOUT, HISTORY |
| [L007](tasks/L007.md) | L | `demiflow/execution/native_plan.py` · `compile_native_topology` | implement | (logical_dag,physical_plan,bindings_ref) | LAYOUT, RELATIONAL |
| [L008](tasks/L008.md) | L | `demiflow/execution/native_plan.py` · `load_native_execution` | implement | (ExecutionLayout,ExecutionPlacement) | LAYOUT, HISTORY |
| [L009](tasks/L009.md) | L | `demiflow/execution/native_plan.py` · `execute_native_plan` | implement | Iterator[RuntimeRow|ArrowRecordBatch] | LAYOUT, RELATIONAL, WRITE |
| [L010](tasks/L010.md) | L | `demiflow/execution/dataset_native.py` · `execute` | implement | 原 Iterator[rows|ArrowRecordBatch]；增加 execution=None kw | RELATIONAL, BOUNDS |
| [L011](tasks/L011.md) | L | `demiflow/execution/arrow_batch_sink.py` · `execute` | implement | Iterator[ArrowRecordBatch]；增加 execution=None kw | RELATIONAL, WRITE, BOUNDS |
| [L012](tasks/L012.md) | L | `demiflow/execution/chain.py` · `seal_write_topology` | implement | _WriteSeal；首次封存，恢复读取原绑定 | WRITE, LAYOUT, HISTORY |
| [L013](tasks/L013.md) | L | `demiflow/execution/chain.py` · `_WriteSeal.layout` | implement | 原 (checkpoint_policy,stage_limits)；由 placement 派生 | LAYOUT, WRITE |
| [L014](tasks/L014.md) | L | `demiflow/execution/chain.py` · `verify_loaded_plan` | verify_preserve | None 或原拒绝错误 | HISTORY, LAYOUT |
| [L015](tasks/L015.md) | L | `demiflow/execution/chain.py` · `run_local_chain` | verify_preserve | _LocalMaterializedHandle；含_ChainBlocks、terminal.rows、recovery_reference(run_id/action_id/semantic_id) | LAYOUT, RECOVERY |
| [L016](tasks/L016.md) | L | `demiflow/execution/chain.py` · `run_ray_chain` | verify_preserve | _LocalMaterializedHandle；含_ChainBlocks、terminal.rows、recovery_reference(run_id/action_id/semantic_id) | RAY, LAYOUT |
| [L017](tasks/L017.md) | L | `demiflow/execution/graph.py` · `run_local_graph` | verify_preserve | dict{target_rows,contributions,egress_rows,workers,ancestor_contributions,final_arrival,checkpoints}；基线键集不变 | GRAPH, RECOVERY |
| [L018](tasks/L018.md) | L | `demiflow/execution/graph.py` · `run_ray_graph` | verify_preserve | dict{target_rows,contributions,egress_rows,workers,ancestor_contributions,final_arrival,checkpoints}；基线键集不变 | RAY, GRAPH |
| [L019](tasks/L019.md) | L | `demiflow/execution/queue_chain.py` · `run_local_queue_chain` | verify_preserve | WriteResult(written_rows,failed_rows,blocks_written,target,metadata)；原claim/finalize/ACK权威不变 | QUEUE, RECOVERY |
| [L020](tasks/L020.md) | L | `demiflow/execution/worker/layout.py` · `load_worker_layout` | implement | (WorkerPlacement,CheckpointPolicy,stage_limits) | LAYOUT, GRAPH, QUEUE |
| [L021](tasks/L021.md) | L | `demiflow/execution/worker/execution.py` · `WorkerChainExecutor._load_plan` | verify_preserve | LogicalPlan；反序列化及code-pin/布局检查均通过后返回 | LAYOUT, HISTORY, RECOVERY |
| [L022](tasks/L022.md) | L | `demiflow/execution/worker/execution.py` · `WorkerChainExecutor.start` | verify_preserve | None；启动本实例后台执行线程，重复调用按原状态机拒绝 | LAYOUT, HISTORY, RECOVERY |
| [L023](tasks/L023.md) | L | `demiflow/execution/worker/execution.py` · `WorkerChainExecutor._run` | verify_preserve | None；记录统计/错误并设置done，贡献只发送driver | LAYOUT, HISTORY, RECOVERY |
| [L024](tasks/L024.md) | L | `demiflow/execution/worker/graph_execution.py` · `GraphWorkerExecutor._load_plan` | verify_preserve | LogicalPlan；反序列化及code-pin/布局检查均通过后返回 | GRAPH, HISTORY, RECOVERY |
| [L025](tasks/L025.md) | L | `demiflow/execution/worker/graph_execution.py` · `GraphWorkerExecutor.start` | verify_preserve | None；启动本实例后台执行线程，重复调用按原状态机拒绝 | GRAPH, HISTORY, RECOVERY |
| [L026](tasks/L026.md) | L | `demiflow/execution/worker/graph_execution.py` · `GraphWorkerExecutor._run` | verify_preserve | None；记录统计/错误并设置done，贡献只发送driver | GRAPH, HISTORY, RECOVERY |
| [L027](tasks/L027.md) | L | `demiflow/execution/worker/queue_transform.py` · `QueueTransformExecutor._load_plan` | verify_preserve | LogicalPlan；反序列化及code-pin/布局检查均通过后返回 | QUEUE, HISTORY, RECOVERY |
| [L028](tasks/L028.md) | L | `demiflow/execution/worker/queue_transform.py` · `QueueTransformExecutor.start` | verify_preserve | None；启动本实例后台执行线程，重复调用按原状态机拒绝 | QUEUE, HISTORY, RECOVERY |
| [L029](tasks/L029.md) | L | `demiflow/execution/worker/queue_transform.py` · `QueueTransformExecutor._run` | verify_preserve | None；记录统计/错误并设置done，贡献只发送driver | QUEUE, HISTORY, RECOVERY |
| [W001](tasks/W001.md) | W | `demiflow/workbench/runs/facts.py` · `read_sealed_plan` | implement | 原 available 分支文档 | WORKBENCH, HISTORY |
| [W002](tasks/W002.md) | W | `demiflow/workbench/runs/facts.py` · `read_identity_bridge` | implement | {available:True,ref,bridge} 或 {available:False,reason,detail} | WORKBENCH, HISTORY |
| [W003](tasks/W003.md) | W | `demiflow/workbench/runs/facts.py` · `join_node_observations` | implement | tuple[entry + observation + availability] | WORKBENCH, HISTORY |
| [W004](tasks/W004.md) | W | `demiflow/workbench/runs/facts.py` · `overlay_sealed` | implement | 原 logical view；增加派生 binding_level | WORKBENCH, HISTORY |
| [W005](tasks/W005.md) | W | `demiflow/workbench/graph/model.py` · `get` | implement | 原 graph 响应 | WORKBENCH, HISTORY |
| [W006](tasks/W006.md) | W | `demiflow/workbench/graph/model.py` · `bound_source` | implement | 原源码/参数响应 | WORKBENCH, HISTORY |
| [W007](tasks/W007.md) | W | `demiflow/workbench/runs/worker.py` · `execute` | verify_preserve | 原 worker退出/执行结果；events归原run | WORKBENCH, WRITE, HISTORY |
| [W008](tasks/W008.md) | W | `demiflow/workbench/runs/manager.py` · `prepare` | verify_preserve | RunView；固定准备摘要及未启动状态 | WORKBENCH, WRITE, HISTORY |
| [W009](tasks/W009.md) | W | `demiflow/workbench/runs/manager.py` · `start` | verify_preserve | RunView；已启动attempt和真实进程状态 | WORKBENCH, WRITE, HISTORY |
| [W010](tasks/W010.md) | W | `demiflow/workbench/runs/manager.py` · `show` | verify_preserve | RunView；只读指定attempt | WORKBENCH, WRITE, HISTORY |
| [W011](tasks/W011.md) | W | `demiflow/workbench/runs/manager.py` · `cancel` | verify_preserve | RunView；停止请求/实际状态，不把尚未退出称已停止 | WORKBENCH, WRITE, HISTORY |
| [W012](tasks/W012.md) | W | `demiflow/workbench/runs/manager.py` · `resume` | verify_preserve | RunView；新增attempt和保留历史输出 | WORKBENCH, WRITE, HISTORY |
| [W013](tasks/W013.md) | W | `demiflow/workbench/runs/manager.py` · `register_results` | verify_preserve | None；按原receipt原子更新结果登记 | WORKBENCH, WRITE, HISTORY |
| [V001](tasks/V001.md) | V | `tests/test_upgrade_v2_acceptance.py` · `test_v2_supported_stream_terminals` | implement | None；断言通过才有验收证据 | ADMISSION |
| [V002](tasks/V002.md) | V | `tests/test_upgrade_v2_acceptance.py` · `test_v2_actor_counter_crash_resume` | implement | None；断言通过才有验收证据 | STATE |
| [V003](tasks/V003.md) | V | `tests/test_upgrade_v2_acceptance.py` · `test_v2_identity_distinguishes_business_configuration` | implement | None；断言通过才有验收证据 | IDENTITY |
| [V004](tasks/V004.md) | V | `tests/test_upgrade_v2_acceptance.py` · `test_v2_signed_zero_roundtrip` | implement | None；断言通过才有验收证据 | CODEC |
| [V005](tasks/V005.md) | V | `tests/test_upgrade_v2_acceptance.py` · `test_v2_reprepare_budget_same_bytes` | implement | None；断言通过才有验收证据 | CODEC |
| [V006](tasks/V006.md) | V | `tests/test_upgrade_v2_acceptance.py` · `test_v2_vector_search_real_state` | implement | None；断言通过才有验收证据 | VECTOR |
| [V007](tasks/V007.md) | V | `tests/test_upgrade_v2_acceptance.py` · `test_v2_embedding_batch_recovery` | implement | None；断言通过才有验收证据 | EMBED |
| [V008](tasks/V008.md) | V | `tests/test_upgrade_v2_acceptance.py` · `test_v2_grouped_and_prepend_buffers` | implement | None；断言通过才有验收证据 | BUFFER |
| [V009](tasks/V009.md) | V | `tests/test_upgrade_v2_acceptance.py` · `test_v2_prompt_policy_and_unknown` | implement | None；断言通过才有验收证据 | PROMPT |
| [V010](tasks/V010.md) | V | `tests/test_upgrade_v2_acceptance.py` · `test_v2_write_routes_consume_layout` | implement | None；断言通过才有验收证据 | LAYOUT |
| [V011](tasks/V011.md) | V | `tests/test_upgrade_v2_acceptance.py` · `test_v2_write_create_recovery` | implement | None；断言通过才有验收证据 | WRITE |
| [V012](tasks/V012.md) | V | `tests/test_upgrade_v2_acceptance.py` · `test_v2_graph_queue_commit_authority` | implement | None；断言通过才有验收证据 | GRAPH |
| [V013](tasks/V013.md) | V | `tests/test_upgrade_v2_acceptance.py` · `test_v2_workbench_history_end_to_end` | implement | None；断言通过才有验收证据 | WORKBENCH |
| [V014](tasks/V014.md) | V | `tests/test_upgrade_v2_acceptance.py` · `test_v2_notebook_sql_lifecycle` | implement | None；断言通过才有验收证据 | NOTEBOOK |
| [V015](tasks/V015.md) | V | `tests/test_upgrade_v2_acceptance.py` · `test_v2_case_media_review_roundtrip` | implement | None；断言通过才有验收证据 | CASE |
| [V016](tasks/V016.md) | V | `tests/test_upgrade_v2_acceptance.py` · `test_v2_demo_navigation_and_layout` | implement | None；断言通过才有验收证据 | DEMO |
| [V017](tasks/V017.md) | V | `tests/test_upgrade_v2_acceptance.py` · `test_v2_development_context_isolation` | implement | None；断言通过才有验收证据 | DEVELOPMENT |
| [V018](tasks/V018.md) | V | `tests/test_upgrade_v2_acceptance.py` · `test_v2_resource_and_performance_envelope` | implement | None；断言通过才有验收证据 | BOUNDS |
| [V019](tasks/V019.md) | V | `tests/test_upgrade_v2_acceptance.py` · `test_v2_ray_backend_actual` | implement | None；断言通过才有验收证据 | RAY |
| [V020](tasks/V020.md) | V | `tests/test_upgrade_v2_acceptance.py` · `test_v2_historical_read_and_code_drift` | implement | None；断言通过才有验收证据 | HISTORY |
| [R015](tasks/R015.md) | R | `demiflow/execution/stream.py` · `_make_worker` | implement | Callable[[],Awaitable[None]]；真实stage worker协程工厂 | BUFFER, EMBED, RECOVERY |
| [R016](tasks/R016.md) | R | `demiflow/execution/stream.py` · `run_stream` | verify_preserve | StreamStats；成功/失败回调与关闭顺序保持 | LAYOUT, BUFFER, BOUNDS |
| [M008](tasks/M008.md) | M | `demiflow/embeddings/stream_recovery.py` · `NativeEmbeddingRecovery.scope_request` | implement | RequestRecord；绑定native_occurrence和响应上限 | EMBED, UNKNOWN |
| [M009](tasks/M009.md) | M | `demiflow/embeddings/runtime.py` · `EmbeddingActor.__init__` | implement | EmbeddingActor完整实例；新增_native_recovery=None | EMBED |
| [M010](tasks/M010.md) | M | `demiflow/embeddings/runtime.py` · `EmbeddingActor.cancel_drain_timeout_s` | implement | float>=0秒；native journal有有限正drain时间 | EMBED, UNKNOWN |
| [M011](tasks/M011.md) | M | `demiflow/embeddings/runtime.py` · `EmbeddingActor.aclose` | verify_preserve | None（async）；原资源全部收尾 | EMBED, BOUNDS |
| [M012](tasks/M012.md) | M | `demiflow/embeddings/runtime.py` · `EmbeddingActor._fresh_exchange` | verify_preserve | (response_record:dict,references:dict|None)（async） | EMBED, UNKNOWN |
| [L030](tasks/L030.md) | L | `demiflow/execution/native_plan.py` · `stage_native_programs` | implement | tuple[ProgramBinding,...] | LAYOUT, HISTORY, RELATIONAL |
| [L031](tasks/L031.md) | L | `demiflow/execution/native_plan.py` · `verify_native_programs` | implement | None或RecoveryContractError | LAYOUT, HISTORY, RELATIONAL |
| [L032](tasks/L032.md) | L | `demiflow/execution/chain.py` · `_WriteSeal.__init__` | implement | _WriteSeal；原字段与native_layout/native_execution_id/source | LAYOUT, WRITE |
| [L033](tasks/L033.md) | L | `demiflow/execution/chain.py` · `_WriteSeal.verify` | implement | None或原拒绝 | HISTORY, WRITE |
| [W014](tasks/W014.md) | W | `demiflow/workbench/runs/facts.py` · `_sealed_entry` | implement | logical action entry；增加binding_level=sealed；原运行状态单独显示 | WORKBENCH, HISTORY |
| [V021](tasks/V021.md) | V | `tests/test_upgrade_v2_acceptance.py` · `test_v2_nested_program_code_pin` | implement | None；断言通过才有证据 | LAYOUT, HISTORY, RELATIONAL |
| [L034](tasks/L034.md) | L | `demiflow/execution/worker/execution.py` · `WorkerChainExecutor._request_commit` | implement | CommitReceipt（async）；错误/取消抛出原异常 | RECOVERY, GRAPH, BOUNDS |
| [L035](tasks/L035.md) | L | `demiflow/execution/worker/graph_execution.py` · `GraphWorkerExecutor._request_contribution` | implement | CommitReceipt（async）；错误/取消抛出原异常 | RECOVERY, GRAPH, BOUNDS |
| [S016](tasks/S016.md) | S | `demiflow/execution/operator_state.py` · `OperatorStates.__init__` | implement | 完整OperatorStates实例；增加policies映射 | STATE, RECOVERY |
| [L036](tasks/L036.md) | L | `demiflow/execution/dataset_native.py` · `Engine.__init__` | implement | Engine实例；新增execution引用 | RELATIONAL, LAYOUT, BOUNDS |
| [L037](tasks/L037.md) | L | `demiflow/execution/dataset_native.py` · `Engine.session` | implement | DataFusionSession；property惰性创建一次 | RELATIONAL, LAYOUT, BOUNDS |
| [L038](tasks/L038.md) | L | `demiflow/execution/dataset_native.py` · `Engine.batch_rows` | implement | int>=1；真实reader/hydration行窗口 | RELATIONAL, BOUNDS |
| [L039](tasks/L039.md) | L | `demiflow/execution/native_resources.py` · `dataset_session` | implement | DataFusionSession；原contextmanager资源对象 | RELATIONAL, BOUNDS, LAYOUT |
| [W015](tasks/W015.md) | W | `demiflow/workbench/runs/facts.py` · `_bridge_node_map` | implement | dict[str,str]；选定key_kind到唯一report_node_id | WORKBENCH, HISTORY |
| [W016](tasks/W016.md) | W | `demiflow/workbench/web/workbench.js` · `logicalName` | implement | string | WORKBENCH, DEMO, HISTORY |
| [W017](tasks/W017.md) | W | `demiflow/workbench/web/workbench.js` · `logicalEntry` | implement | LogicalActionEntry|null | WORKBENCH, DEMO, HISTORY |
| [W018](tasks/W018.md) | W | `demiflow/workbench/web/workbench.js` · `renderLogical` | implement | undefined；更新当前视图DOM/选择状态 | WORKBENCH, DEMO, HISTORY |
| [W019](tasks/W019.md) | W | `demiflow/workbench/web/workbench.js` · `chooseLogicalNode` | implement | undefined；更新当前视图DOM/选择状态 | WORKBENCH, DEMO, HISTORY |
| [W020](tasks/W020.md) | W | `demiflow/workbench/web/workbench-graph.js` · `mountLogical.update` | implement | undefined；更新当前视图DOM/选择状态 | WORKBENCH, DEMO, HISTORY |
| [W021](tasks/W021.md) | W | `demiflow/workbench/web/workbench-graph.js` · `mountLogical.select` | implement | undefined；更新当前视图DOM/选择状态 | WORKBENCH, DEMO, HISTORY |
