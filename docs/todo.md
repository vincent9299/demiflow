# DemiFlow 待办

平台层已确认、尚未安排的事项。每条写清现象、依据和下一步；做完的移到文末"已完成"并注明提交。更早的历史条目在工作区的 `DEMIFLOW_PLATFORM_TODO.md`（DF 编号），不再新增。

新增条目请写：是什么问题、在哪段代码或哪次实测里确认的、建议怎么做。

## 08 资源管理

### T-001 执行单元的退出证明改由内核给出（可用时）

- **现象。** 本机容量账本靠"unit 锁没有持有者"证明一个执行单元已退出。锁只覆盖继承了它的进程：平台自己的启动原语（worker、SQL 子进程、受管进程组、`run_isolated`、隔离进程池）继承，算子自己启动的子进程不继承，线程单元根本没有锁。所以 worker 被强杀后，这类子进程还在跑时额度就可能归还。启动点及归类见 `tests/test_unit_child_processes.py`。
- **依据。** 2026-11-24 的账本改造评审（`_demiflow/shared-review/CLAUDE-DESIGN-capacity-ledger-20261124-v3.md` 及 v4-B 修订、GPT 的复审）。真实进程实测：外层进程退出、原锁可取得时，`collect/pdf_text.py:50` 启动的解析器仍在运行；thread 模式下宿主退出后隔离子进程仍完成工作。
- **建议。** cgroup v2 可写（已委托）的部署里，给每个执行单元建一个 cgroup，用 `cgroup.events` 的 `populated=0` 判断整组进程是否清空，替代锁；不可写时保持现在的锁机制。要点：
  - 先确认目标部署形态。2026-11-24 查过 devbox 的 Pod：cgroup v2，但 `/sys/fs/cgroup` 是只读挂载，这条路在那里用不了。Kubernetes 默认如此。
  - 线程单元可以先单独加锁（宿主持有、子进程继承），不依赖 cgroup。
- **不做的理由要记住。** 不引入常驻守护进程：资源管理由引擎已有的循环驱动（`resources/manager.py` 开头），守护进程是架构方向的变更，需要单独决定。

### T-002 多用户、多作业共用一台机器时的额度

- **现象。** 默认账本目录按"主机名 + 用户 ID + cgroup"区分（`execution/hosts/capacity.py:69`）。不同用户各有一本账，每一方都把整机额度当成自己的，会互相超发。账本是逻辑准入，不约束非 DemiFlow 进程（同文件第 5 行）。
- **现在的边界。** 单容器或单用户下不会超发。多用户共用时，要么由运维指定一个共用的 `DEMIFLOW_RESOURCE_ROOT`，要么用 Ray 后端（额度由 Ray 记账）。
- **建议。** 在用户文档里把这条边界写明，并把"多作业共享"的推荐路径定为 Ray；本机账本只服务单用户的嵌入式运行。是否需要硬限制（cgroup 配额）随 T-001 一起评估。

### T-003 GPU 额度已归还、运行账本未补记时，原 run 的恢复卡住

- **现象。** 设备账本对已释放的记录返回空（`resources/device_pool.py` 的 `reservation_view`），原 run 的恢复入口据此判定"无法核验"并一直返回 `CLEANUP_UNCONFIRMED`（`execution/hosts/execution_recovery.py:136`）。同一文件里 CPU 账本没有这个问题。
- **依据。** GPT 在私有逻辑 GPU 账本和真实 Local 子进程上复现：原设备计费可读，恢复连续两次未确认。
- **建议。** 恢复入口对设备记录也接受"已释放"终态并核对释放证据，与 CPU 账本的处理一致。另外 `resources/local.py` 里先退 CPU、再退 GPU 的两步之间可以崩溃，一并处理。

### T-004 本机容量账本仍然收不回的额度

2026-11-24 起，driver 被强杀后留下的进程单元额度，会在下一次额度不足时按物理事实归还（契约见 `docs/sdk-capacity-cleanup-20261119.md` 的 2026-11-24 修订）。以下几类仍然只能靠原 run 恢复或停机维护：

- 机器重启后仍保留的账本记录（分不清"原主机重启"和"另一台机器看到同一账本"）；
- 跨容器或跨主机共用账本目录的记录；
- 锁文件已丢失或被替换的记录；
- 线程单元、带加速器的分配、未绑定的预留；
- 改造之前写入、没有来源信息的旧记录。devbox 默认账本在 2026-11-24 有 59 条这样的记录，占 63.2 核，进程均已不存在；清理需要逐条核实并经负责人批准。

建议：线程单元随 T-001 的加锁解决；其余按实际出现频率再决定。

### T-005 正常准入时账本锁没有期限

- **现象。** `LocalCapacityPool._lock` 和 `DevicePool._lock` 在正常准入路径上是无期限的阻塞锁；持锁进程被挂起时，整台机器的准入都会等待，准入超时不生效。只有恢复路径带期限。
- **建议。** 把准入截止时间传到取锁处，超时按"额度等待"返回。

## 发版与验收

### T-006 四条线的逐文件驱动脚本进仓库

- **现象。** 发版规范第 5 节要求的四线运行，靠一份逐文件驱动脚本（每个测试文件一个私有资源域、一个期限、一份回执）。这份脚本目前只在仓库外的共享评审目录里，各次发版各自拷贝修改（`_demiflow/shared-review/` 下的 `run_lanes.py` 与 `harness/e1run.py`）。仓库里只有它调用的回执工具 `tests/runtime_v3_regression_inventory.py` 和比对工具 `tests/engine_known_state.py`。
- **建议。** 把驱动脚本收进仓库并固定用法：输入是提交和合入的人定下的范围（哪些线、哪些测试文件），输出是各线回执和已知状态清单。每日全量和按范围验收用同一个入口。

## 引擎对架构规约的符合性

来源：2026-11-24 的方案评审，完整内容和每项的代码位置见 `_demiflow/shared-review/CLAUDE-engine-design-FINAL-711f2573-20261124/FINAL.md`。判定以三条要求为准：模块遵循架构规约；模块以下保持整洁、无错误耦合、无垫片与脏代码、不多增实体；功能与性能不回退。环的规模只作诊断。以下各项不单独为它们发版，在因别的原因改到相关文件时顺带收敛。清单不是对全仓的系统排查。

### T-007 `pipeline/run_context.py` 混了两层职责

- **现象。** 同一个文件里有被 24 个文件使用、不向上依赖的环境访问（`current_run`、`current_action`、`active_limits` 等），也有向上依赖 06、12、14 的 `RunContext` 生命周期（388 行）。底层文件为读一个 ContextVar 导入它，它又导入这些文件的上层。`storage/location_index.py:243` 和 `native_queue/stream_lance.py` 的写入器为同样的原因分别导入 `executors/plan_capture.py` 和 `pipeline/checked.py`。
- **建议。** 环境访问归到 `pipeline/scope.py`，`RunAction` 留在原位（它读写动作账本）。`RunContext` 内 8 处直接读写 ContextVar（388、406、458、475、493、497、547、551 行）改为调用 `scope.py` 的函数。测试、Forge、工作台里约 45 处导入要一并改；守卫测试 `tests/test_dataflow_compile_inputs.py` 直接读写 `checked._CURRENT`，要改它的准备代码。

### T-008 运行模块为纯函数导入主编译器，声明层导入规划器

- **现象。** `seal/plan.py:305`、`seal/program_identity.py:64` 导入规划包的私有 `_plain`、`_hash`；`native_queue/dataflow_fetch.py:24`、`operator_bindings/dataflow_search.py:27`、`action_driver/services.py:83` 为一个纯声明遍历导入 `planning/dataflow_compile.py`；`seal/plan.py:906` 的 `validate_literal_schema_replay` 在 06 里调用 04；`data/native_options.py:35` 导入 `planning/dataflow_policy`；`data/flow_ordering.py:6` 导入 `data/dataset.py` 的私有函数。
- **建议。** 只挪代码：两个纯函数下沉到 `contracts/dataflow_codec.py` 并改公开名；声明遍历归到 `planning/dataflow_declaration.py`；`validate_literal_schema_replay` 搬到唯一调用者 `action_driver/dataflow.py`；`resource_only_options` 搬到 `planning/dataflow_policy.py`；两个列工具下沉到 `data/flow_contract.py`。守卫测试 `tests/test_dataflow_configuration.py:15` 的导入要跟着改。

### T-009 账本认领时直接调用主机的资源恢复

- **现象。** `journal/run_journal.py:765–776` 的 `claim_execution` 认领后调用 08 的 `recover_execution_resources`，再对账已发布产物。
- **建议。** 账本拆成认领与对账两个接口，由 `RunContext` 按原次序调用三步。两个入口都要改：正常启动（`pipeline/run_context.py:381`）和停机维护（`_maintenance_owner`）。后者的 `claimed` 标志必须仍在三步都成功后才置位；`publication_guard` 里的发布前对账不动。

### T-010 请求仍携带整个 executor，使用方读它的私有属性

- **现象。** 2026-11-24 起每个 `ActionRequest` 携带自己的 executor，不再挂在共享的 `RunAction` 上。但 `action_driver/services.py`、`compiler_support/dataflow_compile_inputs.py` 仍读 executor 的 `_local_kernel`、`_workers`、`_block_size`，`journal/run_telemetry.py` 仍拿整个 executor 更新用量。
- **建议。** 请求只携带各使用方需要的值；15 生成观测结果，由持有 executor 的 02 更新统计。

### T-011 退役 Lance checkpoint 的旧实现仍在

- **现象。** `lance/checkpoint.py:72` 的 `checkpoint_lance` 第一步即拒绝，其后约 370 行写入路径不可达；`data/dataset.py:324–329` 同理。文件说明写明这段实现留待 P6 删除。
- **建议。** 随 P6 删除，保留 `read_checkpoint_record`。

### T-012 违反规约但修复有风险的位置

每处修起来都要改行为、改封存身份的字节，或要动冻结文件，需单独评估。

- 06 封存和 05 数据源直接识别 17 的具体类型（`seal/plan_facts.py:46–47,243,272–274`、`seal/program_identity.py:362–363`、`sources/snapshots.py:170–172`）；规约写的是算子提供事实、06 负责封存。
- 13 与 17 互相调用，13 按具体队列类选择确认方式；队列完成证明的核验放在 05（`checkpoint/commit.py:102,121,128,133,207–208`、`native_queue/raw_objects.py:462,511`、`sources/queue_history.py:27`）。
- Dataset 层构造具体 actor 并写它的私有运行配置；队列声明入口为返回声明先构造运行对象（`data/dataset.py:1571–1580`、`executors/queue_flow.py:44–51`）。
- 流式写出服务长期持有整个 03 的执行对象（`native_queue/stream_lance.py:451–453`）。
- 13 既协调提交，又为训练输入选写者、定物化目录（`checkpoint/fit_inputs.py:17–25`）。
- 14 根据“是否在捕获计划”决定行为（`storage/location_index.py:243–245`）。
- 采集的声明文件导入 17；根目录对象存储 SDK 调用 17；08 的结果对象返回 02 的 Dataset（`collect/fetch_flow.py:48,80`、`objects.py:246`、`hosts/datafusion.py:132–134`）。
- 04 编译器导入 06 和 09（`planning/dataflow_compile.py:51,131,132,271,776`）；规约没有明确裁定方向。
- `hosts/environment.py:9` 经公开门面 `demiflow.operator_llm` 导入 `load_referenced_prompt_packs`。
- `operator_llm/parser.py:210` 经公开门面 `demiflow.pipeline` 导入 `discover_pipeline_definition`。`tests/test_map_prompt_async.py:270` 在门面上替换这个函数，改成直接导入定义文件会让替换失效，要连同测试的替换目标一起改。

## 测试

2026-11-24 对全量日跑（提交 `9887311d`）里未通过的 333 条失败记录（332 个测试）做了一次核对，并在低并发下把失败的文件重跑了一遍：重跑的 287 个里 285 个再次失败、2 个通过，不是并发负载造成的。原始清单、两次运行的对比和逐条摘要在 `_demiflow/shared-review/CLAUDE-test-audit-20261124/`。去向：

| 数量 | 去向 |
|---|---|
| 26 | 确认过时，已删除（见 T-015） |
| 45 | 需要 Ray 的测试被排进了没有 Ray 的标准线，在 Ray 线里另有结果；不是产品失败（见 T-016） |
| 1 | prompt 节点预算的口径，已定为按每次运行计（T-013，已完成） |
| 13 | `request_gate` 参数被引擎拒绝，已退役（T-014，已完成） |
| 140 | 引擎测试，未判定（T-017） |
| 108 | 界面测试，未判定（T-018） |

### T-015 已删除的过时测试里，仍然有效但现在没有测试的保证

26 个测试已删除：调用引擎明确拒绝的已退役用法（显式 checkpoint、节点上的 journal 选项、搜索 checkpoint）18 个；在驱动进程里替换类或函数、而执行发生在 worker 进程 5 个；替换已不存在的内部函数 3 个（另 1 个把执行器换成空对象）。以下保证仍然有效，需要按 v3 的写法补测试：

- prompt 节点：带编号的多张图片按顺序进入请求；`when` 跳过的行和出错的行保留原始数据；响应不符合 schema 时不发布结果。（原 `tests/test_map_prompt_async.py`）
- prompt HTTP 重试：声明为非致命的内容错误保留在行内且不中断其他行；429/500/503 不重试、重放时不重复计数；未完成的流不重试。（原 `tests/test_prompt_http_retry.py`）
- Dataset：已物化数据的 `count()` 只读元数据；`schema(fetch_if_missing=False)` 不触发读取；`take_all` 成功和超限时观察者的完成/失败各记一次。（原 `tests/test_upgrade_v2_unit_A004.py`、`A013.py`、`tests/test_dataset_api_contract.py`）

### T-016 标准线里排进了需要 Ray 的测试

- **现象。** 标准线（没有 Ray）每天有 45 个测试因 `No module named 'ray'` 或 `NOT_RUN` 记为失败，涉及 11 个 `test_runtime_v3_*` 文件；同样的文件在 Ray 线里有真实结果。
- **建议。** 随 T-006 的驱动脚本进仓库时，把这些文件从标准线的执行清单里去掉，让标准线的失败数只反映真实失败。
- **补充（2026-11-25）。** `test_runtime_v3_provider_recovery.py` 用 Ray 环境跑全部通过。`test_runtime_v3_node_loss.py` 没有真实 Ray 集群时按设计报 `NOT_RUN`；`test_runtime_v3_finite_reader_recovery.py` 依赖合入闸设置的 `DEMIFLOW_RESOURCE_HOME`。这三个文件都只是排线问题。

### T-017 引擎测试里 140 个未判定的失败

逐个看过报错，没有一个能仅凭报错判成"过时"，都需要对着当前设计逐条判断是测试的场景失效还是引擎行为变了。按报错形态分组：

- **内存额度与放置。** `RESOURCE_INFEASIBLE`（`test_local_kernel.py` 10 个、`test_local_union_plan.py` 3 个、`test_prompt_stream_recovery.py` 3 个等）；断言里写死的额度数字对不上（`test_runtime_v3_stream_suffix.py` 5 个、`test_upgrade_business_acceptance.py` 2 个）。
- **崩溃与恢复。** 已处理（2026-11-25，分支 `claude/tests/effectiveness-20261124`）。主因是测试按旧的落盘环节名 `output` 找杀进程的位置，现在叫 `materialize` / `take_all`，子进程因此一直没被杀。改对之后恢复行为正常。同时按"测的是否是用户可依赖的保证、保证坏了是否会失败、是否重复"复查了整个文件，不只是失败的用例：删掉了永远走不到的分支、在主进程里替换但代码在子进程跑的替身、读不存在目录因而恒为空的资源检查、和专门文件重复的用例。剩下要裁定的引擎行为见 T-019 到 T-023。
- **向量检索。** `tests/test_vector_search_stream.py` 19 个：查询向量校验的错误以 `INTERNAL_PROTOCOL` 报出，以及结果为空。
- **测试的替身落后一步，合同还在。** `tests/test_dataset_native.py` 2 个（`_group_order_fixtures` 的替身少一个 `window` 参数）。改测试即可。
- **其余**见 `_demiflow/shared-review/CLAUDE-test-audit-20261124/engine-lines.txt`，每个测试一行。

### T-018 界面测试里 108 个未判定的失败

- **现象。** 涉及 40 个 `test_forge_*` / `test_workbench_*` 文件，低并发重跑后同样失败。大多数是等一个按钮或节点等到超时：`检查问题`、`业务参数`、`整图配置`、导航里的 `算子` 和 `运行`、`.wb-node`。这些文字和样式名在产品源码里大多还在，说明是入口位置或流程变了，而不是整块功能被删。
- **建议。** 由熟悉当前界面的人逐个文件判断：界面有意改了就按新流程改测试或删掉；界面不该变就是缺陷。原因摘要在 `_demiflow/shared-review/CLAUDE-test-audit-20261124/ui-waits.txt`。

### T-019 经过队列 `fetch` 的行不再保留元组和字节类型

- **现象。** `read_queue(...).map(...).fetch(...)` 里，上游 map 产生的元组到下游变成列表，字节类型直接报 `Object of type bytes is not JSON serializable`。原因是 worker 把整行用 JSON 编码后交给驱动侧的队列服务（`execution/native_queue/dataflow_acquisition_call.py:58`）。
- **依据。** `tests/test_mvp2_queue_unified.py` 的 `..._preserves_tuple_intermediate` / `..._preserves_bytes_intermediate`，保留为失败。11-12 的 `9192ebd5` 专门做过"保留类型的队列载体"，11-15 的 `397c1f96` 改成 JSON 编码。普通（非队列）图的类型保真由 `tests/test_u3_graph_type_fidelity.py` 覆盖且通过。
- **建议。** 定口径：若队列行只承诺 JSON 值，就在文档写明并删掉这两个测试；否则按退步修。

### T-020 `graph_spec` 适配器没有产品调用方

- **现象。** `LocalDatasetExecutor.materialize_graph` / `planning/dataflow_graph_adapter.py`（文件头自称 internal）在 `demiflow/` 里没有调用方，只有约 19 个测试文件驱动它。
- **建议。** 确认它是否还是产品路径。不是的话连同这些测试一起删除；是的话说明入口。

### T-021 `docs/prompt_http.md` 对 `sqlite_journal` 自相矛盾

- **现象。** 第 5 行说 `map_prompt_async` 不再接受 `sqlite_journal`；同一文档约第 178 行的示例在用它，代码也接受（2026-11-25 实测，声明阶段不报错）。
- **建议。** 按实际行为改第 5 行。

### T-022 子进程在第一次存盘之前崩溃，运行不能续跑

- **现象。** 第一次存盘之前丢失 worker，续跑报 `INCOMPATIBLE_HISTORY`（`coordinator.py:449`），只能重新开一次运行。
- **依据。** 2026-11-24 改 `tests/test_runtime_builtin_recovery.py` 时实测，原 `during_batch` 用例因此删除。
- **建议。** 确认是有意设计（没有任何已确认进度时不续跑）就写进恢复文档；否则作为缺陷。

### T-023 其他待确认

- `max_output_bytes` 不约束 `materialize()`，物化只受磁盘额度和行数上限约束。若这是有意的，文档里写明适用范围。
- `tests/test_gpt_integration_repairs.py::test_embedding_actual_process_loss[after_prepared]` 在机器负载高时失败过 1 次，之后串行 6 次、并行 5 次都通过，原因未抓到。

## 已完成

### T-013 prompt 节点的 `max_requests` 按每次运行重新计

定为有意的设计：预算按每次运行计，同一个节点再跑一次时重新计数。写进 `docs/prompt_http.md`，删掉了要求累计的测试。提交 `ba72d8e2`。

### T-014 `map_prompt_async(request_gate=...)` 有文档但不可用

已退役：参数从签名去掉，传入时报 `TypeError`；文档改为只介绍 `request_policy`。只测这个参数的测试删除，测重用连接池和并发上限的改用 `request_policy`。提交 `ba72d8e2`。
