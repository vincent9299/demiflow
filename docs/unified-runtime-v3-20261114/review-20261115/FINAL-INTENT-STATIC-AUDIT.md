# 固定候选的独立架构与原始意图审计

审计对象：`55acec616b2c6c7e2297b070d5e6b6001217fa81`。审计在隔离分支完成；只读候选源码，没有修改主仓或总控树，也没有重跑全量业务矩阵。本文件是**静态审计**，不替代同一候选的 Local、Ray、工作台、提交恢复运行证据。

## 结论与本轮发现

候选的 A–E 分工、单一原生执行/提交权威、真实封存计划的产品接线成立；没有发现重新引入 Ray Data 或 worker 自建运行账本。但不能据此声称全部意图 100% 完成。

**本轮发现并交总控确认的核心缺陷：返回端累计预算未闭环（I08/C11）。** `Dataset.take_all(limit=None)` 虽然进入统一引擎，却用 `iter_rows` 在 Driver 自行无界 append；`collect_rows` 检查在 decode/append 后，`iterate_batches` 只在整批构造后检查。另 `limit+1` 使恰好 `max_output_rows` 的合法结果也被拒。已获授权在本隔离分支最小修复，真实反例与最终修复证据见 `TERMINAL-COLLECTION-BOUNDS.md/json`。这项修复不属于原 `55acec6`，不能把其测试成绩追记给旧候选。

除以上实际缺陷，在下面限定的核心交接范围内，未发现另一个可证实的 P0/P1。没有以文件数量、保留旧私有 helper 或尚未运行某个门本身作为缺陷。

## 两大 root 与源码闭环

产品 root：统一、真正可用的工作台，让用户看懂、配置、运行 pipeline，分析、比较、评测数据。平台 root：正确、稳定、高性能、通用、架构干净的统一数据计算平台，完整支撑实际业务场景。具体 I01–I14 仍以本包 `INTENT.md` 及原会话证据为准。

| 边界 | 本次逐源码核对的权威与调用链 | 判断及实际边界 |
|---|---|---|
| A 资源 | `dataflow_services._driver_resources` → `ResourceManager` → Local/Thread/Ray provider；`RunResourceState(run.journal)` 保存原分配/清理事实 | scheduler 中的准入、活跃租约和 resident window 不构成第二个 allocator；仅原物理退出证据可释放。实际 CPU/GPU/多机性能仍需对应运行环境验证。 |
| B worker | 同一个 `DataflowWorkerExecutor` / `WorkerGraphRunner` 消费已封存 graph、physical plan、program refs；启动核 assignment、lease、code pin | Local/Ray 是承载与传输差异。对生产 worker 模块做 AST 构造调用检查，未见 `RunJournal`、`CommitCoordinator` 或 SQLite connect。原生效果走 driver RPC。 |
| C exchange | 原 `ChannelSetState`、路由、credit、peer block 与 durable/finite carriers；前沿随原 contribution/cut 保存 | 传输队列/路由计数是有界运行状态；持久化仍使用原 ArtifactStore/RunJournal。borrowed finite refs 沿原 seal/segment/payload/schema 校验，不以文件存在冒充成功。 |
| D 提交恢复 | `DataflowCheckpointCoordinator` → 原 `CommitCoordinator`/`RunJournal`；`DataflowExecution._advance_cut` 在 commit 返回后发送 receipt并更新前沿 | 源、算子、sink、效果的确定事实使用原提交协议。late-final 需要真实 final contribution，不把已提交非 final cut 重标为 final。SaveLance 子作用域仍绑定原 parent/action/node/membership，经 driver 原生提交。 |
| E 编译编排 | 新运行 `lower_action` → physical compile → seal；恢复读取原封存 bundle，核版本和 pin | 不从当前业务声明重算历史执行图。物理融合不会替换逻辑 occurrence、状态或效果身份。保留旧 helper 的存在不代表公开路径回退。 |

### 公开 Dataset 与后端一致性

逐公开终端/执行适配器核对：Dataset 的 materialize、take、take_all、take_batch、count、aggregate、迭代、写出、实际取 schema 等经过 `make_action_request` / `run_dataset_action` → `execute_action`；`run_stream_action` 也调用同一个 `execute_action`，使用同一个运行 envelope。构造/变换 API 负责声明 source/plan，元数据只读与 materialized release 不应伪造执行。

对固定候选全部 **1,034 个生产 Python 文件**的 AST 扫描：`ray.data` 导入为 **0**，文本核查未见实际调用。Ray executor 保留兼容类名，其终端适配落入相同 v3 执行；没有把 DAG 交给 Ray Data。没有发现公开 Dataset 入口调用已退休 `_apply_plan` 作为 fallback。`Dataset._run_stream`、冻结 queue helper 等仍保留历史私有代码；本审计核对其未接回公开 v3 action，不宣称删除了仓库所有历史实现。

“统一入口”并不自动证明有界：本次 take_all 的缺陷正是同一执行入口之后返回层遗漏预算，故另行实修，未用入口数量掩盖行为缺口。

### 工作台真实图、观测与历史

- `workbench/runs/facts.read_sealed_plan` 识别原 v3 journal marker，读取原封存 graph/physical/code refs，经 `dataflow_history.build_plan_view` 只读投影；不以 Forge 的 declared DAG 替代执行图。
- `workbench/graph/logical` 的 `compile_declared_dag` 仍用于**声明图投影**，其 basis/status 是 declared/projected。页面明确区分“运行封存计划”和“声明图投影 / 未执行”，没有把未执行声明伪称实际执行。
- identity bridge 按 action/dag/report 绑定；`_attach_observations` 按 selected attempt 加载原观测，缺页显示 `not_in_page`，没有把未观察节点推断为成功。
- `bound_source` 对执行 occurrence 经原 bridge 定位封存 report 节点；历史源码和配置读取当时 evidence，不退回当前项目文件。prepare-only 的声明图仍可读当时源码，不能因没有执行桥而伪造一个桥。
- 配置修改与 start/cancel/resume 仍沿原工作台管理入口，最终结果使用原 committed facts。此静态审计不代替总控/产品路的真实浏览器操作证据，也不覆盖所有 Notebook/Case/开发对话领域。

### 新 GC、FD 与 journal 通知

- published orphan GC 只在真实 worker/resource cleanup 和 journal/native 线程退出之后、同 inode 独占对象租约下执行。原 RunJournal 的 protocol/charge/gc intent 是唯一许可与额度事实；物理 header 仅提示锁协议，不是第二个对象或恢复账本。
- 历史 cuts、prepared/unknown、原生效果、pins、borrowed/segment/payload/schema/嵌入 state record 都参与追根。未知 carrier、旧协议、缺对象、扫描/时间预算不足时保守不删。GC 不在活跃 cut 中删对象，不回收任何仍被历史根引用的对象；因此不能宣称它实现了任意历史保留策略。
- prepare → quarantine/unlink 的物理确认 → 原 journal finish/refund 绑定 charge generation；同 hash 重新 put 是新 charge，旧 settlement 不被重写。读者、put、pin、root publication 共享原对象租约，GC 排他；不凭旧 fence、mtime 或文件消失退款。
- store FD 计数是原 store 范围的有界内存准入视图，共用物理 root 的 views 取最紧有效限额；没有新增持久 DB/资源 allocator。sort 同时为仍存活的读者和立即写出的消费者留窗口。它不等价于控制整个 Python 进程及第三方库全部 FD。
- `DriverJournalService` 的固定 socketpair 仅唤醒原 driver。结果仍来自原有、按 `max_pending_events` 限制的 queue；operation/payload/boot/fence 校验不变。空→非空通知、部分消费保持可读、清空后 drain、完成后 rearm 在同锁下；关闭/初始化失败关闭原两个 FD。通知字节不携带数据、不承认 ACK、不增加提交权威。慢回包跨 epoch 的效果仍由原 journal known/unknown 判定。

## 历史、冻结与证据边界

七个承重文件在固定候选和本分支修复工作区均与 `362d8528` **字节相同**。完整 SHA-256 及扫描结果见 `FINAL-INTENT-STATIC-AUDIT.json`。这只证明冻结边界，不把未修改等同于旧实现无缺陷。

当前平台版本摘要覆盖全部生产 Python 源码。终端修复会改变新运行 terminal shape 和平台 revision；外层 checked action 仍使用原 take_all 声明。旧运行仍由 `RunContext.resume` 的原 platform revision / sealed identity 规则拒绝不兼容恢复，不重写旧 seal/dag/pin。

本审计未运行跨真实 host 的网络分区、GPU 隔离、生产数据库、付费模型，也未重跑全 API/浏览器矩阵。多机与性能结论必须绑定总控最终运行证据；同机 Ray 不能升级表述成多机已验，静态审计不能换算成产品或平台 100%。
