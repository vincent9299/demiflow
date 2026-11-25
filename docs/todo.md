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

### T-016 标准线里排进了需要 Ray 的测试

- **现象。** 标准线（没有 Ray）每天有 45 个测试因 `No module named 'ray'` 或 `NOT_RUN` 记为失败，涉及 11 个 `test_runtime_v3_*` 文件；同样的文件在 Ray 线里有真实结果。
- **建议。** 随 T-006 的驱动脚本进仓库时，把这些文件从标准线的执行清单里去掉，让标准线的失败数只反映真实失败。
- **补充（2026-11-25）。** `test_runtime_v3_provider_recovery.py` 用 Ray 环境跑全部通过。`test_runtime_v3_node_loss.py` 没有真实 Ray 集群时按设计报 `NOT_RUN`；`test_runtime_v3_finite_reader_recovery.py` 依赖合入闸设置的 `DEMIFLOW_RESOURCE_HOME`。这三个文件都只是排线问题。

### T-017 引擎测试里 140 个未判定的失败

逐个看过报错，没有一个能仅凭报错判成"过时"，都需要对着当前设计逐条判断是测试的场景失效还是引擎行为变了。按报错形态分组：

- **内存额度与放置。** 已核对（2026-11-25）。`test_local_kernel.py` 10 个、`test_local_union_plan.py` 3 个、`test_stream_map.py` 1 个是引擎行为退步，测试保持原样，见 T-025。`test_runtime_v3_stream_suffix.py` 5 个是测试按旧公式重算融合阶段的内存；同一次引擎提交有意改成按选定布局分配，测试改为核对实际分配等于封存计划、同时驻留的阶段合计不超过额度、融合后的 CPU 规则。`test_upgrade_business_acceptance.py` 的 2 个已在第 2 批处理。
- **崩溃与恢复。** 已处理（2026-11-25，分支 `claude/tests/effectiveness-20261124`）。主因是测试按旧的落盘环节名 `output` 找杀进程的位置，现在叫 `materialize` / `take_all`，子进程因此一直没被杀。改对之后恢复行为正常。同时按"测的是否是用户可依赖的保证、保证坏了是否会失败、是否重复"复查了整个文件，不只是失败的用例：删掉了永远走不到的分支、在主进程里替换但代码在子进程跑的替身、读不存在目录因而恒为空的资源检查、和专门文件重复的用例。剩下要裁定的引擎行为见 T-019 到 T-023。
- **prompt。** 已核对（2026-11-25）18 个。测试写法过时、已改 12 个：在驱动进程里替换 HTTP 客户端或 actor 而 prompt 跑在 worker 进程里（改用线程 worker）；读驱动端请求闸门副本的计数（改读服务端）；用旧的落盘环节名 `output` 找故障点；`map` 写成了一行变多行的 `flat_map`；列目录时没算每次运行的 `-driver-resources`；总超时短到请求可能送不到服务端；替换服务对象后改由更早的类型检查拒绝；取消测试按旧位置打开调用记录。删 1 个：`test_exact_source_threshold_uses_one_final_cut` 检查的“恰好 1 次提交”是 v3 之前设计的实现细节，v3 多出的最终提交引用同一数据块。其余 6 个是引擎问题，测试保持失败：T-025 1 个、T-027 1 个、T-028 3 个、T-029 1 个。提交 `9d5563d2`。
- **其余引擎测试（第 6 批）。** 已核对（2026-11-25）。普查 35 个文件：66 失败、13 错误。已改 15 个：去掉已退役的 `.checkpoint()`（`test_prompt_messages` 4）；替身跟上真实函数签名、引用改名后的常量（`test_dataset_native` 2）；查询失败可先确认一个不发布输出的中间 cut，改为检查"没有发布输出"（`test_dataset_native` 1）；算子调用记录在模块属性里，改用线程 worker（`test_operator_api` 3）；自定义服务对象不再能跨 worker，改为声明被动的 `ManagedHTTPService`（`test_readonly_prompt_replay` 1）；清单改由 `Path.open` 读取、读不了的上级清单按不存在处理（`7c4b06dd`，`test_artifact_relocation` 2）；有状态写入器的方法交给 `batch_map` 后驱动端看不到其批次，改为直接驱动（`test_evidence_primitives` 1）；重复写入目标的报错类型（`test_native_web_dataset` 1）。约 21 个来自 T-033；`test_stream` 1 个、`test_prompt_routes` 2 个来自 T-025；`test_operator_api` 3 个见 T-034；大负载 5 个见 T-035；需要 Ray 的 3 个属于 Ray 线；工作区格式相关的 forge、托管图片（7 个）与 `runtime_v3_w`（6 个，含 2 个 JS 场景）留给界面批次。提交 `153d35af`、`60a5d1ed`、`5fbc33cc`。
- **向量检索。** 已核对（2026-11-25）19 个。结果为空的 9 个是测试在行回调里往闭包列表追加结果：回调从封存的程序重建，线程 worker 下也是副本，调用方的列表永远为空；改为经文件取回行（共用的 `tests/_row_capture.py`），背压测试的消费计数同样改用文件。同样的写法还在 `tests/test_document_library.py`（`capture()`，2 个测试）和 `tests/test_google_backends.py`（1 个测试）里，一并改了；这 3 个在 `9f3d4e53` 之前就因结果为空失败，之后又被 T-025 拒绝，调大默认额度验证新写法 3 个全过。另 1 个回调捕获了 pyarrow `Schema`，见 T-032，测试改为在回调里读表的 schema。几处检查驱动端 actor 对象 `_dataset`/`_version` 的断言永远成立，改为替换 `open_lance_dataset` 和 `VectorSearch.aclose` 记录真实的打开与关闭。当时仍失败 10 个：T-030 8 个（已修）、T-031 2 个（这两个测试的其他断言都已通过，资源统计放在最后检查）。提交 `979363a1`。
- **测试的替身落后一步，合同还在。** `tests/test_dataset_native.py` 2 个（`_group_order_fixtures` 的替身少一个 `window` 参数）。改测试即可。
- **其余**见 `_demiflow/shared-review/CLAUDE-test-audit-20261124/engine-lines.txt`，每个测试一行。

### T-018 界面测试里 108 个未判定的失败

- **现象。** 涉及 40 个 `test_forge_*` / `test_workbench_*` 文件，低并发重跑后同样失败。大多数是等一个按钮或节点等到超时：`检查问题`、`业务参数`、`整图配置`、导航里的 `算子` 和 `运行`、`.wb-node`。这些文字和样式名在产品源码里大多还在，说明是入口位置或流程变了，而不是整块功能被删。
- **建议。** 由熟悉当前界面的人逐个文件判断：界面有意改了就按新流程改测试或删掉；界面不该变就是缺陷。原因摘要在 `_demiflow/shared-review/CLAUDE-test-audit-20261124/ui-waits.txt`。

### T-024 强杀后续跑偶尔打不开运行记录库

- **现象。** 进程被 SIGKILL 后 `RunContext.resume` 读取恢复身份时，只读连接执行 `PRAGMA cache_size` 报 `sqlite3.OperationalError: attempt to write a readonly database`（`execution/journal/run_journal.py:161-166`），续跑失败。推测是强杀留下的 WAL/共享内存需要恢复，而只读连接不能做这一步；未逐行证实。
- **依据。** 2026-11-25 `tests/test_queue_v2_crash.py::test_sigkill_and_native_recovery[sqlite-True-claimed]` 在 main `aa997f1e` 上单独复跑 5 次失败 2 次，原因都是这条；同一时期带 T-019 修复的分支 5 次全过（该场景在下载前就被杀，不经过 T-019 改动的代码）。
- **建议。** 续跑时第一次打开运行记录库改用可写连接完成 WAL 恢复，或在只读连接失败时按这一错误重试一次可写打开；修复后用该测试加压复跑验证。

### T-025 reduce、join 和单 worker 计划不再能在小额度下运行

- **现象。** 两类计划被 `RESOURCE_INFEASIBLE` 拒绝，之前都能运行：
  - 含 `reduce_by_key` 或 `join` 的计划，在不大的 `memory_bytes` 下装不下。例：120 行的 map→filter→reduce（3 个 worker、7 个分区、线程模式），以前 192KB 就能跑；现在 64MB、128MB 都被拒绝，要到 192MB 才行。reduce 阶段的最低需求是两份默认状态上限（各 16MB）加十几个批次窗口，窗口不随额度缩小。把 `max_state_bytes` 降到 16KB，192KB 下仍差 3 倍以上。join 在 128KB 下要 224KB。
  - 默认额度（256MB）下也有计划被拒：`tests/test_stream_map.py::test_thread_map_allows_async_consumer_to_release_other_workers`（线程 map 并发 3、后接异步消费）需要 240MB，可用 224MB。
  - 默认额度下并发 8 的 prompt 节点：`tests/test_sqlite_journal.py::test_concurrent_first_requests_verify_model_once` 需要 336MB，可用 224MB；`ab3b4d45` 上通过，`9f3d4e53` 上失败。
  - 流式交付被慢行阻塞：`tests/test_stream.py::test_unordered_emission_no_head_of_line_blocking`（`map_async` 并发 6、后接 `map`）中，慢的第 0 行挡住已完成的第 1 行交给下游，5 秒超时；`ab3b4d45` 上通过，`9f3d4e53` 上失败。
  - `workers=1` 时，union 后接 reduce（普通函数或类回调都一样），以及类回调后接 semi join，报“memory group cannot fit and its source/state cannot safely park (max_active_workers required=2 或 4, available=1)”。内存从 256MB 加到 4GB 结果不变；普通函数后接 semi join 能跑。
- **依据。** 二分定位到 `9f3d4e53`（11-22，“Derive physical stage grants from the selected layout…”）。它的前一提交 `ab3b4d45` 上 `test_local_kernel.py`、`test_local_union_plan.py`、`test_runtime_v3_stream_suffix.py` 共 42 个全部通过；到 `9f3d4e53` 有 18 个失败，至今未变。这次提交删除了 `planning/dataflow_policy.py::fit_default_stream_windows`。原函数按实际同时驻留的工作量缩小行、批次和状态窗口，并对单 worker 计划整体核算（见其说明）。替代它的 `planning/dataflow_budget.py::minimum_workspace_graph` 只调整声明了 `codec_min_bytes` 的算子（数据源、map 等），reduce、join 仍按默认窗口计。单 worker 的放置失败未逐行证实，可能与同一提交给类回调加的完整物化规则有关。
- **影响。** 用户给本机执行设置几十 MB 的额度，或用 1 个 worker 跑带 union/join 的计划，会在启动前被拒绝；默认额度下并发较高的计划也可能被拒。`tests/test_local_kernel.py` 10 个、`tests/test_local_union_plan.py` 3 个、`tests/test_stream_map.py` 1 个、`tests/test_sqlite_journal.py` 1 个因此失败，测试未改；它们在 `ab3b4d45` 上都通过。默认额度下被拒的还有 `tests/test_document_library.py` 2 个、`tests/test_google_backends.py::test_dataset_http_backend_receipt_schema`（第 5 批修好结果收集后，调大默认额度可通过）、`tests/test_prompt_routes.py` 2 个（`ab3b4d45` 上 3 个全过，`9f3d4e53` 上 2 个失败）。`tests/test_prompt_stream_recovery.py` 守的是 prompt 崩溃续跑，第 2 批（`7a74d464`）为了让它测到续跑行为，把运行条件从 1 个 worker、128MB 改成默认 worker 数、512MB，绕开的就是这里的问题。这个 prompt 测试在 `ab3b4d45` 上能被接受，到 `9f3d4e53` 被拒绝。
- **建议。** 由引擎负责人确认是否有意。如果不是：给没有 `codec_min_bytes` 的算子恢复按额度缩小窗口和状态，并恢复单 worker 计划的放置；修好后以上 42 个测试和 `test_stream_map`、`test_sqlite_journal` 各 1 个应全部通过。如果是有意：在 `local_execution` 的文档里写明 reduce、join 的最低额度和单 worker 的限制，再按新规则改这 15 个测试。

### T-026 `local_execution(temp_directory=...)` 被接受但不起作用

- **现象。** `LocalKernelOptions` 保存这个参数，但 v3 执行代码不读它，运行中的文件都在运行目录的存储里。`local_execution` 的说明没有提这个参数。工作台的运行进程（`workbench/runs/worker.py`）仍然传 `temp_directory=str(work)`，可能以为溢出文件会落在那里。
- **依据。** 2026-11-25 全仓检索，`demiflow/` 下除 DataFusion 主机自己的同名参数外没有读取方。最后几处读取在 11-23 的退役提交里删除（`96a55963`、`2cb25ec5`、`bf8bb2bf`）。
- **影响。** 几个测试原本靠“运行后这个目录为空”证明清理，这样的断言永远成立。第 3 批已改为检查本次运行的 worker 退出、分配归还（`tests/test_local_kernel.py`），或删掉重复的检查（`tests/test_plan_capture.py`、`tests/test_stream_map.py`）。
- **建议。** 引擎负责人决定：去掉这个参数并改掉工作台的调用；或者保留但在说明里写明不再使用。

### T-027 prompt 节点的有状态 `when` 判断器不存盘也不恢复

- **现象。** 判断器提供 `snapshot_state` / `restore_state`，运行在第一次存盘后中断再续跑：整个过程中这两个方法一次都没被调用，续跑时判断器从初始状态重新计数，重复判断已判过的行。用“只对前 3 行为真”的判断器，续跑后有回答的是第 0 到 4 行，多发了请求。
- **依据。** 2026-11-25 `tests/test_native_prompt_owners.py::test_stateful_model_predicate_resumes_from_its_snapshot`，在判断器里记录每次调用、存盘和恢复，日志里只有调用。引擎仍把非普通函数的判断器标记为语义所有者（`execution/operator_bindings/dataflow_model_journal.py` 的 `semantic_owners`），但全仓没有任何代码读取这个标记去保存或恢复它。带状态的 `when` 判断器是少见用法，影响面小。原测试按 v3 之前的逐节点状态键检查判断器快照，至迟 11-22 起就因找不到旧键名失败，这个问题一直没被发现；已改写为按行为检查。
- **建议。** 引擎负责人确认 `when` 判断器是否应作为语义状态存盘。应该：在 cut 中保存并在续跑时恢复。不支持：对带 `snapshot_state` 的判断器在声明时拒绝，不要静默从头开始。

### T-028 上游出错时，prompt 在途的付费请求没有按期限收完

- **现象。** prompt 请求已发出、响应正在流式返回时，上游另一行报错使 action 失败。此时在途请求不会被收完：即使请求期限远未到、响应很快就能收完，连接也被直接关闭，响应丢失，账本只记一条错误。文档写明取消时在原生 actor 的期限内收完在途调用再关闭客户端（`docs/prompt_http.md` "Cancellation drains active calls…"，`docs/native-barrier-checkpoints.md` "drains native calls/writers under their deadlines"）。
- **依据。** 2026-11-25 用真实的本地 HTTP 流式服务、默认进程 worker、不替换任何代码的独立脚本复现：请求期限 5 秒、响应 0.4 秒即可收完，另一行在上游报错后，在途请求被切断（服务端看到客户端提前断开），运行账本里该调用没有响应，只记了一条错误。`tests/test_prompt_http_stream.py::test_service_stop_drains_paid_response_but_does_not_publish_partial_success` 已改为同样的写法（真实 HTTP、默认 worker、文件信号、按运行记录核对"没有发布部分结果"）：期限 5 秒、响应 0.4 秒的两个参数固定失败（账本里 `responses` 为 0），期限 1 秒、响应 2 秒的两个参数固定通过；连跑 8 轮结果一致。原测试在驱动进程里替换 HTTP 客户端、靠闭包对象和 `asyncio.Event` 跨阶段传信号，在 v3 里一直测不到这个场景；期间一版用 0.1 秒期限的写法时过时不过，原因是请求自己的超时与上游报错几乎同时发生、谁先成为 action 的失败原因不确定，已把两者在时间上拉开。另：prompt 后面若接其他算子，有限输入的 action 会等上游全部完成后才开始 prompt，因此测试里 prompt 后面不接算子。
- **建议。** 对齐实现与文档：action 失败或取消时，在途的 prompt 调用在其期限内收完并记录响应，超过期限的记为传输错误。

### T-029 工作台读不到托管运行的模型调用证据

- **现象。** `runs.evidence`（工作台 API 与 `runs evidence` 命令）只读取运行清单 `evidence` 中声明的账本。v3 把调用记录写进运行自己的 `run.sqlite`，行里的 `request_ref.journal_path` 指向它，而清单的 `evidence` 为空，所以每次读取都报 "Evidence is not a declared bounded resource of this run"。
- **依据。** 2026-11-25 `tests/test_operator_foundation_runs.py::test_managed_model_results_evidence_and_read_only_access`，打印了引用和清单；至迟 11-22 起失败。同文件的取消测试原先打开 `evidence/calls.sqlite`，已改为读 `run.sqlite`。
- **建议。** 工作台把运行自己的调用记录作为有界证据读取（按引用核对属于本次运行），或由运行在清单中声明它；修好后该测试应通过。

### T-033 封存指纹取决于进程里已加载的模块：函数体内延迟导入同级模块时，worker 误判"代码已改"

- **现象。** 自 `31c779c6`（main-20261125-20，合入 `eafb549a`）起，封存业务依赖时会读取源码里的绝对 `import`，并且只在该模块已被本进程加载（`sys.modules`）时才计入依赖（`execution/seal/business_sources.py::_ast_import_dependencies`）。驱动进程与 worker 进程加载过的模块不同，两边算出的依赖集合就不同，worker 加载程序时报 `INVALID_CONTRACT: Loaded program code differs from the sealed pin (edited code or wrong module)`，代码并没有改。
- **依据（不经过 pytest 的用户写法，2026-11-25）。** 业务函数体内 `import biz_extra`，驱动进程此前已导入过 `biz_extra`：进程 worker 下，`40c0034a`（改动前）正常，最新 main `c00afcb3` 失败；驱动进程没有提前导入时，前后都正常。
- **同一改动修好了更大的真实问题，不要回退。** 使用相对导入的业务包（顶层 `from ._consts import X`、函数体内 `from . import _lazy`）在 `40c0034a` 上用进程 worker 运行 map、map+reduce、运行内 materialize 全部以 `CORRUPT_ARTIFACT` 加载失败，改动后全部通过。
- **对测试的影响。** `tests/test_runtime_v3_graph_adapter.py` 与 `tests/test_runtime_v3_route_recovery.py` 合计 25 个，改动前 23 过 2 败，改动后 23 败 2 过；`tests/test_runtime_v3_reducer_values.py::test_reducer_value_limits_reject_without_committed_output` 2 个由通过变为失败；`tests/test_runtime_v3_thread_host.py` 整文件运行时有 4 个同样报错（只挑这 4 个用例单独运行则通过）；多个文件同会话运行时还有更多测试出现同一报错。都是同一机制：场景模块在函数体内导入了其他测试模块，pytest 进程早已加载它们，worker 没有。
- **建议。** 绝对导入的依赖不要按"本进程是否已加载"来确定：改为与相对导入一样按文件系统解析，或只在封存时确定依赖并写入清单，worker 按清单核对而不重新发现。修好后上述测试和用户写法的复现都应通过。
- **更正。** 本条 11-25 首次发布（main-20261125-37）时写的是"修复或回退"，并把影响描述为路由图与恢复程序被拒绝加载；当时只有测试证据。经独立脚本核实后改为以上内容。

### T-031 向量检索的资源统计缺失

- **现象。** `docs/vector_search.md` 写明 `stats.metrics['resources']['VectorSearch:0']` 记录实际版本、查询数和命中数；v3 的 `run_stream()` 统计里没有 `resources` 这一项。
- **依据。** 2026-11-25 实测 `StreamStats.metrics` 的键为 `committed_output`、`queues`、`stage_processing_latency`、`models` 等。`test_lazy_native_node_preserves_queries_and_duplicates`、`test_head_pinned_for_action_and_explicit_version` 只在这项检查上失败（固定版本、打开与关闭次数等其余断言都已通过）。
- **建议。** 恢复这项统计，或改文档并删除对应断言。

### T-032 行回调捕获 pyarrow 对象后，短的流式链在默认 4 个 worker 下被拒

- **现象。** 检索后接两个 map 再 `run_stream()` 可以运行；其中一个 map 的闭包若捕获了 `pa.Schema`，同样的链报 `RESOURCE_INFEASIBLE: memory group cannot fit and its source/state cannot safely park (max_active_workers required=5, available=4)`。把 schema 改为在回调内部构造即可运行。
- **依据。** 2026-11-25 对照实测（同一流水线、同一数据，只换回调）。`ab3b4d45` 上已是如此，不是 T-025 的那次提交引起的。推测捕获不透明对象的回调被当作需要单独加载的程序，既不能与相邻阶段合并，也不能挂起让出 worker；未逐行证实。
- **建议。** 引擎负责人确认：这类回调是否必须独占 worker；若是，在报错里指出是哪个回调、并在文档中说明，或允许其挂起。

### T-034 Codex 的记录型重放仍按"私有缓存"描述，而引擎已禁止这种组合

- **现象。** `docs/operator_environment.md`（"recorded 在 Codex 缓存回放时直接使用当时结果"及原生编码、检索 API 一节）描述的回放方式，是给 Codex 节点挂私有 `sqlite_journal` 后再跑一遍。现在引擎拒绝这种组合：Codex 报 "Private Prompt cache requires a single explicit HTTP journal"；带算子环境的 HTTP agent 报 "Native remote journal cannot coexist with a private prompt journal"。`docs/prompt_http.md` 也写明私有缓存不能与 CLI/Codex 传输同用。
- **依据。** 2026-11-25 `tests/test_operator_api.py` 中 `test_recorded_api_is_not_reexecuted_and_cached_pixels_are_verified`、`test_yaml_actor_runs_and_closes_through_real_runtime[codex]` / `[demiflow]` 因上述报错失败（至迟 11-22 起）。同文件另 3 个失败是算子调用记录放在模块属性里、而算子在 worker 进程执行，已改为线程 worker。
- **建议。** 引擎负责人确认记录型重放在 v3 中的入口（应是运行中断后续跑复用已完成的 Codex 会话），据此修改文档，并把这 3 个测试改为"中断后续跑"的写法；在此之前它们保持失败。

### T-035 大负载 join 测试：探测点已不在执行路径上，另有 3 个更早的失败

- **现象。** `tests/test_dataset_large_payload.py` 中 `test_opaque_join_uses_bounded_key_index_and_preserves_values`、`test_wide_payload_after_earlier_query_rebounds_native_batches` 的结果断言通过，但 worker 内的 `JoinSpool` 探测报告 0 行输入：自 `031359ba`（11-23，"Seal and run bounded relational strategies inside the unified Dataset flow"）起 join 改走 `operators/relational_region.py`，"键索引有界、整行只取回需要的次数"等证据不再被收集。同文件另有 3 个至迟 11-22 起失败：`test_indivisible_payload_over_eight_mib_survives_join_and_sink` 期望超预算时报 `BUDGET_EXCEEDED` 却没有报；`test_wide_join_hydrates_after_key_only_join_under_small_pool[1]`、`[2]` 超过 120 秒执行期限。
- **依据。** 2026-11-25 二分：`9f3d4e53` 上前两个通过，`031359ba` 起失败（其父提交 `803fb9fc` 通过）。后三个在 `ab3b4d45`、`40c0034a` 和当前 main 上都失败，本批未逐个分析。
- **建议。** 引擎负责人指出新路径里对应的有界性观察点（或确认这些界限已由别的测试覆盖），据此改写探测；后三个需逐一判断是行为变化还是性能退步。在此之前这 5 个测试保持失败。

## 已完成

### T-030 向量检索的输入错误跨 worker 后变成 `INTERNAL_PROTOCOL`

已修（2026-11-25，用户同意改平台）。`execution/contracts/dataflow_failures.py` 的失败类型名单加入 `demiflow.errors.InvalidLanceRequest` 和内置的 `MemoryError`，与 11-24 `e908557c` 对 prompt 错误的做法相同；调用方拿到的是节点抛出的类型。`tests/test_vector_search_stream.py` 中原先因此失败的 8 个测试通过，新增 `tests/test_runtime_v3_lance_failure_types.py` 做类型往返检查。驱动端已有的两处 `MemoryError` 处理是本进程内的解码与序列化，不受影响。提交 `49f4d760`。

### T-015 已删除的过时测试里，仍然有效但现在没有测试的保证

全部按现在的写法补回（2026-11-25）。prompt 节点：带编号的多张图片按顺序进入请求；图片与多个输出映射回原行；`when` 跳过的行和出错的行保留原始数据；不符合 schema 的响应不发布结果。prompt HTTP：声明为非致命的内容错误留在行内、不重试、不影响下一行；429/500/503 不重试，再跑一次既不重发也不重复计数；声明的 401 重试保留每次尝试、重放不再发请求；不完整的流和未列出的 403 不重试。重放都用私有 `sqlite_journal` 缓存。Dataset：已物化数据的 `count()` 不读任何数据块、不加载程序（与同一句柄的 `take_all` 对照）；`schema(fetch_if_missing=False)` 不启动 action、不打开数据源；`take_all` 成功只记一次完成、超限只记一次失败（按日志事件）。提交 `06ab6aeb`、`2323bc8d`。

### T-019 经过队列 `fetch` 的行不再保留元组和字节类型

按退步修复。v3 重写（`397c1f96`，11-15）让 worker 和驱动侧队列服务之间用 JSON 传整行，丢掉了 11-12 `9192ebd5` 在旧路径上做的类型保真；v3 协议只要求控制消息是 JSON，业务行本应走 `record_codec` 的行编码。现在下载与检索的业务行、以及返回给 worker 的回执都用 `RECORD_MEDIA_TYPE` 存放；读取按存档的媒体类型区分，旧运行里按 JSON 存的回执仍可读、可恢复。结算行仍是 JSON（写入队列数据库，冻结的 `queue_sinks.py` 不变）。冻结文件均未改动。新增"下载后丢失 worker、续跑后类型仍保留且不重新下载"和"JSON 旧存档可读"两个测试；去掉修复后前者以 `bytes is not JSON serializable` 失败。提交 `f8dfc3b5`。

### T-020 `graph_spec` 适配器没有产品调用方

查证后保留，不删除。`docs/unified-runtime-v3-20261114/MIGRATION.md` 把 Local/Ray 的 `materialize_graph` 列为执行带路由的图的 v3 入口；`docs/p1-sdk-engine-boundary-20261119.md` 删除 16 个重复执行器方法时明确保留了它；引擎负责人至 11-24 仍在维护。规划代码里的 7 个钩子只在出现 `RoutedGraphSource` / `graph_route` 时生效，这两者只由该适配器产生。仓库与工作区（不含测试、文档）没有其他调用方，它的测试是目前唯一的调用者，应保留。

### T-021 `docs/prompt_http.md` 对 `sqlite_journal` 自相矛盾

2026-11-25 实测（声明并执行）：两种 prompt 节点都接受 `sqlite_journal` 的 `path`、`max_requests`、`read_only`、`timeout_s`，会建私有缓存；`throughput` 和 `journal_dir` 都被拒绝（11-16 `b150e748` 恢复了受限的私有缓存）。文档首段按此改写，`throughput` 一节注明 prompt 节点不接受。提交 `7b039a08`。

### T-022 子进程在第一次存盘之前崩溃，运行不能续跑

有意的设计：`docs/regression-closure-20261119/graph-lifecycle.md` 第 24 行写明没有确认进度时续跑按 `INCOMPATIBLE_HISTORY` 拒绝，并有真实杀进程测试守着。实测范围：物化被拒；写当时不存在的新 Lance 表可以续跑；代码另允许"只登记了第一个 cut 的意图"和"绑定原生模型账本"两种情况。写进 `docs/streaming-map.md` 的"顺序和恢复"。提交 `7b039a08`。

### T-023 其他待确认

- 输出字节限额：实测 `max_output_bytes` 只管对外发布的输出（Lance、文件、对象、键值、队列结果）；`materialize()` / `take_all()` 的结果计入 `max_scratch_bytes` 和 `max_output_rows`。写进 `docs/execution-payload-budget.md`。提交 `7b039a08`。
- `test_embedding_actual_process_loss...[after_prepared]` 的偶发失败：复跑 50 次（单独 10 次、四路并行 40 次）抓到 1 次失败，原因是测试本身的时序竞争——存盘时模型请求照常继续，崩溃那一刻偶尔有请求已发出未回复，续跑按设计拒绝重发（`UncertainPromptCall`）。测试改为在 8 个请求都已回复后再丢进程，续跑必须复用全部回复。不是产品缺陷，不登记为偶发。提交 `61822648`。

### T-013 prompt 节点的 `max_requests` 按每次运行重新计

定为有意的设计：预算按每次运行计，同一个节点再跑一次时重新计数。写进 `docs/prompt_http.md`，删掉了要求累计的测试。提交 `ba72d8e2`。

### T-014 `map_prompt_async(request_gate=...)` 有文档但不可用

已退役：参数从签名去掉，传入时报 `TypeError`；文档改为只介绍 `request_policy`。只测这个参数的测试删除，测重用连接池和并发上限的改用 `request_policy`。提交 `ba72d8e2`。
