# D2 · P8 批次 4/5 的限定设计 v1

绑定 `main@122edf4bd0c827338846abd0114f0b36cf8c1b6d`。本文件是后续范围和决策清单，**不授权、不实施任何批次 4/5 删除或搬移**。批次 0–2 已发布，批次 3 详设另见 DESIGN。

## 1. 批次 4：先辨别符号，不整删 chain 或 host

| 对象组 | 当前责任/实际消费者 | 后续限定切片 |
|---|---|---|
| `worker/execution.py::WorkerChainExecutor`、`worker/layout.py`、`worker/progress.py` | 11-09 载体；Local `bootstrap.run_worker` 的 `execution` 分支，Ray `worker/ray_host.py` 顶层导入；旧测试直接使用 | 核实现在没有产品产生旧 launch spec 和活外部消费者后，才可同批去旧分支/导入/实现。不能只删实现留下启动分支 |
| `worker/bootstrap.py` | 活 Local connector、v3 worker 启动和控制协议 owner | 保留文件、v3 分支、ControlFrameReader、连接身份/取消/清理；只设计逐符号退出旧 execution 载体。旧 shell WorkerRuntime 若仍被测试/公共资源用也必须留 |
| `worker/ray_host.py` | `DataflowRayConnector` 继承其中 `RayControlConnector`，这是现用能力；旧 `_RayWorkerHost` 同文件创建 WorkerChainExecutor | **不整删文件**。先区分现用 connector 方法与旧 host_class/receipt 专属部分，现用方法不搬；若需要改模块身份或原生历史路径转 C 的对应设计 |
| `chain.py` 的 20 个 live 代码固定工具 | dataflow_seal、dataflow_code、kernels.task_scope 等产品调用 | 留原模块/限定名/方法体，不能为了“去掉旧 chain 导入”搬文件；删相邻函数仍会改变整文件 source_hash，须声明 |
| 两个 chain compatibility entry | `run_local_chain/run_ray_chain` 一次 v3 materialize、保留签名和前置校验 | 一律保留，无调用不是删除理由 |
| chain 的旧封存/提交/driver 符号与 `native_plan.py` | graph/native_plan/worker.execution 的显式引用；历史封存、代码固定与恢复测试也在调用 | 按引用闭包和十一条逐项审，不以 old 标签直接删除；历史读取仍用的对象保留。`unified_chain_eligible` 已由 TASK-D 提到批次 3，不重复实施 |

chain 在当前 retention 内标为 old 的其余对象包括：`ChainResultSink`、`DriverStreamTerminal`、`_ChainBlocks`、`_WriteSeal`、`_build_sealed_manifest`、`_seal_chain_bindings`、`_seal_chain_identity_bridge`、`_seal_plan_code`、`_worker_failure`、`build_commit_cut`、`build_commit_receipt`、`chain_checkpoint_manifest`、`compile_chain_topology`、`seal_plan_facts`、`seal_write_plan_ref`、`seal_write_topology`、`snapshot_code_sources`、`stage_plan_artifact`、`validate_commit_cut`、`validate_commit_receipt`、`verify_loaded_plan`。这是一份**调查候选**，不是删除名单；例如 stage/verify 在 `test_u3_graph_code_pin.py` 等旧/历史用例中有真实职责。

必须先做的测试处置：逐例判断 test_u2_chain_*、test_u3_graph_code_pin/restore、test_stage2_unified_plan、test_d1a_sealed_plan_facts 对旧载体还是持久/身份合同的断言；保留封存 bytes、篡改拒绝、预算不提交、固定版本、ACK 前缀一次恢复和资源释放。v3 替代映射走 chain compatibility、graph adapter、history protection 真实入口，不能只检查函数源码出现一个字符串。

`docs/engine-known-state/3640116.json` 已有三条 d1a 失败（standard/Ray 分别记录）的历史裁决：local/queue 仍断言写 v1 sealed binding，Ray 仍断言 `compile_chain_topology(`。本批不删除、不修这些旧失败；后续随批次 4 按裁决对应到 v3 sealed plan 和 v1 历史只读。此旧失败事实不是“当前全绿”，也不能通过把源历史函数全删来消除。

后续顺序：固定消费者/launch 证据与每例处置→旧 worker 单闭包方案审→保留现用 connector/兼容入口的最小变更→Local/Ray 真失败恢复验证→封存/身份/历史逐对象裁定。任何同文件历史符号未决，不拖住已证明独立的闭包，也不砍断它。

## 2. 批次 5：类型、历史和冻结依赖先保留

| 对象 | 现状和保持的边界 | 默认决定 |
|---|---|---|
| `execution/stream.py::StreamStats` | `dataflow_stream.py:12` 活使用；`data/execution_contract.py:21,151,173` 以 TYPE_CHECKING/返回类型使用 | 原路径原类型保留；不能整删 stream |
| `stream.py` 的 run_stream/_arun/_materialize 等旧驱动 | Dataset._run_stream、旧 WorkerChainExecutor、若干直接合同测试使用；公开 Dataset.run_stream 已走 v3 | 调查候选，先逐测试对应，不能把使用私有旧入口的错误/取消测试全删 |
| `Dataset._run_stream` | 两个生产源码调用只剩冻结 `write_queue_datasink` 和 `prepare_stream_blocks`；另有两个 native_barrier 手工脚本；内部对 `_apply_plan` 的悬空调用 | 不为悬空调用重建旧 executor；先处理冻结函数/脚本的真正迁移依据。未裁定前保留，不将其可调用性描述为已修复 |
| `stream_materialize.stream_view` | 冻结 `queue_execution.py::write_queue_datasink` 的函数内导入；该冻结函数自身当前无仓内使用方 | 无调用不等于可切依赖；保持现状，交下节策略裁定 |
| `stream_materialize.has_stream` | `execution/capabilities.py` 的现用依赖 | 保留原函数与语义，不随旧 helper 整文件删除 |
| `StreamStageProgress`、StreamMaterialization、prepare_stream_blocks 及旧 map/checkpoint/materialize helpers | 旧 worker 依赖、直接测试和历史读取需逐一核对；不能按文件年代判断 | 在固定 ledger/真实消费者/历史 type 路径表完成前保留 |
| `stream_checkpoint.read_checkpoint`、plan_interpreter、PlanContractError、v1 dag | P1 明确保留的公开 checkpoint/历史读取；planning、sealed_plans、dataflow_history、工作台 facts 有实际依赖 | 不纳入本批删除，保持原路径，不制造 shim 或重新序列化 |

特别保留 `tests/test_stream_timeout_propagation.py` 的合同：回调原 TimeoutError、声明可认缺的 hard_timeout、执行 deadline/control failure 分开；取消排空后 actor/线程释放，失败不能空表成功。这些用例中直接调用旧 `_arun/_materialize/run_stream` 的部分必须逐断言映射到生产 v3，同时保留原错因、commit 和资源结果；不能只按文件名整删。

## 3. 冻结无调用函数的决定清单

七件冻结保持：raw_fetch.py、queue_execution.py、queue_sinks.py、queue_source.py、contracts/exchange.py、planning/dag.py、resources/contracts.py。TASK-D 不授权修改其中任何字节，也不授权只更新 FROZEN 摘要。

具体待真人裁定只有 `queue_execution.py:156::write_queue_datasink` 的后续策略：

| 方案 | 含义与成本 | 本设计建议 |
|---|---|---|
| A 保持冻结函数和依赖 | 函数无仓内调用，但 API/外部和历史未知；保留 stream_view/Dataset._run_stream 及其必要闭包，暂不完成全批次 5 | **当前默认，等待裁定；不以用户休息当选择** |
| B 明确批准退役冻结死函数 | 先完成消费者/外部承诺、历史 fixture 与逐测试映射；单独精确解冻该符号，同批删依赖、修守卫且保留错误/预算/ACK 反例 | 如果确认无需继续兼容，推荐后续采用；必须另有范围裁决/审阅，不在 D1 实施 |
| C 保留公开语义并改译到一次 v3 动作 | 只有真实消费者要求该函数语义时才值得做；需说明 action 归属、队列 settlement、error/unknown、预算/输出与历史边界 | 不为“保持函数存在”自动造适配层；需要时另审具体合同 |

另外两类不需真人选择：StreamStats/PlanContractError/历史读取有明确消费者，直接保留；批次 4/5 按普通门槛可证独立的旧实现由协调在授权范围内组织设计审阅，不把所有工程细节都转成新许可。

## 4. 后续验收与停止边界

每个后续闭包仍逐项过十一条，精确更新 retention、依赖棘轮与测试处置。保留五个兼容入口、活 types/常量、原文件历史路径、七冻结件；公开/历史待定项不得通过删测试或只更摘要消除。

批次 4 必须真实 Local/Ray 正常、worker death、unknown receipt、ACK 前缀恢复、资源退出；批次 5 必须真实公开 run_stream 的错误/timeout/miss/取消与清理、旧历史只读及冻结函数所决定的语义。旧结果/封存数据逐字节保护，跨平台版本仍拒绝恢复。

涉及对象移动、序列化表示或跨版本迁移/恢复承诺先转 C 对应设计；不为一般源码变化阻塞其他已批准任务。未完成的功能、夹具、Ray、外部范围逐项列缺口；“保留”可以是正确交付，不伪称该对象已经退役。
