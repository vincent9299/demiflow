# SDK 崩溃恢复后的资源清理

根因调查基线：9188be2；实施已同步候选26b6541。方案在实施前经root与独立Review通过（见 shared-review/sdk-capacity-cleanup-20261119/design-review.json）。实现已完成，最终验证与限制见下节。

## 已证实根因

1. `os._exit` / 进程丢失跳过 `DataflowExecution.close` 是不可避免的；正常 close 已有 scheduler → ResourceManager → provider.ensure_absent → 原 unit 退出证明 → capacity.release。
2. 漏洞是后续恢复：RunContext 取得原 runtime 锁、替换 owner 后只做 artifact clean_orphans；新 DataflowExecution 使用新 attempt scope。旧 scope 的 ensure_present 已 done，因此 reconcile 不会自动产生 ensure_absent。
3. 原结果已提交时，恢复可直接返回最终结果，也遗漏旧 scope。业务恢复 succeeded 不能证明资源归还。
4. 默认池 58 条旧 BOUND 计 63000 cpu_millis，域上限 64000。53 条能关联原 unit/Journal（58 核），50 条所属 run 已恢复 succeeded。另 5 条缺原 unit 证据，保持未知。
5. 独立空池运行原 projection 与 single-worker crash/resume 两测试均通过，但 crash/resume 完成仍留 1 个 BOUND（2000 cpu_millis）、旧 scope OPEN / cleanup NONE。证据 `/tmp/demiflow-capacity-root-20261119-IdhZEn/`；历史关联见 shared-review/sdk-capacity-cleanup-20261119。

## 修复契约

- 归属：仍用原 Journal、resource_scopes/allocations/intents、ResourceManager、Local/Thread provider 和 capacity ledger；不建全局回收器、第二账本，不按 PID 消失或 TTL 直接退款。
- 入口：RunContext 已持原 runtime 租约的 owner claim，以及 P4 clean_stopped_execution 的同等原租约入口。原 admit_absent_create 不打开资源恢复模式。恢复必须在任何 action（含缓存结果）之前先清旧资源，再原 GC，然后开放业务。停机维护可收尾已成功的历史 run，不创建 attempt、不改结果/commit/ACK/累计预算。
- claim/GC 顺序：原 claim_execution 增加只由上述持锁入口选择的资源恢复步骤，放在 owner 已登记、原 published GC 之前。资源清理专用 Journal guard 只校验同 attempt/fence 与原 SQLite 预算，不先触发依赖物理退出的 GC；普通 execution_guard 的 GC 门禁原样保留。资源清理状态实例禁止 open/apply/ensure_present，只能原 scope ABANDON、领取 ensure_absent、登记 cleanup 观察。
- 旧 scope：有界读取同 run 的未 CLOSED scopes；OPEN 先用当前栅栏和 revision 转 CLOSING，supersede 所有旧 ensure_present；CLOSING 复用原 absent 意图。已 CONFIRMED 不重复释放。逐 scope 派发，不能让某个 provider 领到别的 scope 操作。旧 epoch 的 claimed absent 可立即重领，当前 epoch 保持 claim TTL。确认全部原 allocation cleanup 后沿原 _check_closed 关闭 scope。
- 准入：旧资源清理未证实/超时则当前恢复失败，保留资源占用、owner 与清理事实供再维护；不启动业务、不把 unknown 当成功。复用 cancel_grace_s 的有限维护时间，上层维护 deadline 更紧时取更紧值。每次只领取一条，provider 终止时长裁到当前剩余时间。恢复专用 capacity pool 的 flock 必须非阻塞轮询原总 deadline；默认执行池仍保留原行为。所有分页固定不超过64，单文档沿原控制字节上界。磁盘 fsync/read 等不可抢占 IO 仅提供软期限，不承诺硬墙钟上限。
- 维护 owner：`_maintenance_owner` 不得在未确认资源清理后无条件 release；资源恢复模式记录 claim 完整成功（含资源清理和 GC）。claim失败且原资源事实仍未结清或读取未知时保留同一 fence；不能覆盖原异常。无资源遗留的原 partial-claim 和普通 body 异常仍沿用 P4 的释放语义；不为了一个清理异常把原控制状态改成成功。
- 原 provider：Local process/thread 必须回到原 run 的 sibling driver-resources 根；使用原 process/unit.lock 或 thread 证据。先核 scope/allocation/spec_hash/effective、capacity reservation 与物理身份，再调用原 ensure_absent。任何冲突、双路径、证据缺失或原域无法确认都拒绝。不能绕开父子/单位锁退出证明，也不信任 provider 的缺池线程快捷成功。旧孤儿 process 活着不是单独的拒绝条件；允许原 provider 安全终止并生成退出证明，未证实才拒绝。
- 新 scope 在原 provider_binding 记录 worker_mode 与原 capacity_root；这是运行时清理位置，不改变封存编译产物/计划ID。必须与本次明确使用的原容量域一致；资源域改变时拒绝，不跨域猜测。旧 scope 不补写旧事实；仅当当前明确域中有精确匹配的 reservation 和单一原 unit 证据时兼容恢复。无证据的旧记录不猜测 never-spawned。新记录只有显式原域/模式、Journal仍为REQUESTED/PENDING、无任何原unit目录，且无reservation或原reservation为RESERVED/RELEASED且identity为空时，才可按尚未spawn处理。已有prepared目录但缺process身份时无法证明原锁inode，必须拒绝；当前路径上的空闲锁不构成原worker退出证明。RELEASED是原池已归还终态，重试只补Journal观察，原provider不再次退款；无需新增release_evidence接口。
- GPU/Ray：不得把 GPU 当 CPU 清理或用 Local provider 处理 Ray。原 provider/设备绑定可完整恢复才执行；没有可验证绑定时明确拒绝并保持占用。本批必须验 Local CPU process/thread，其他路径未经真实验证不宣称支持新增恢复能力。
- 历史/部署：除下文“2026-11-24 修订”的账本物理观察回收外，不扫描并清全机池，不删除历史/改默认容量或超时。已有默认池本轮只读；53 条有证据记录可后续按原 run 逐一维护，5 条须先找回证据。预览不重启。

## 落点及并行边界

原 run_context / run_journal 负责入口和栅栏；resource_state 负责范围、状态转移与领取；resources.manager 负责原 provider 派发；dataflow_services 的窄装配/恢复 helper 负责原 provider 实例；dataflow.start 只附运行时绑定。新增的恢复模块只编排这些原所有者，不解释业务、不开新数据库。

不改 P1 的 Dataset/api/local_kernel/kinds/执行初始化、Claude 编译契约和平台 UI。所有源码在 gpt_p4_v31_20261119 / fix/sdk-capacity-cleanup-20261119 修改；已同步P1、整页对话、Lance依赖修复的最新候选26b6541，无文件交叉。合入前复核最新候选及并行分支；共享公告不冒充负责人确认。

## 验证和 Review 门槛

1. 修前失败、修后通过：真实进程 crash → 原 run resume，旧 scope CLOSED/所有资源 CONFIRMED/原池零占用，同时原固定输出/commit/ACK 不变；覆盖缓存结果快速返回、继续执行、process/thread。
2. 有限小池：原 worker 占满额度，恢复仍能先退役再启动；连续故障恢复不累积占用。
3. 负例：原 driver 仍持 runtime 租约、provider 仍无法证实 worker/线程退出、unit 锁仍忙、身份/池/根路径不匹配、证据缺失、期限耗尽均保留占用并拒业务；另一个 run 的占用不变。
4. 原 CLOSING/旧 epoch claimed、物理 release 已完成但 Journal 观察未落盘可重试且不重复退款；cleanup-only 不能发起业务/ensure_present；GC 顺序和 P4维护状态保持验证。
5. 适当回归：资源进度/退出证据/原历史池压缩、RunContext恢复、P4维护、原7项SDK失败用例在独立池验证；不把隔离池通过当默认池已修复。

审阅记录另写 shared-review/sdk-capacity-cleanup-20261119/design-review.json，批准之后再实施。实现后的独立 Review 必须核对最终差异、真实遗留归零及保护项。

## 最终实施与验证

- 方案先由root与独立Review批准，再实施；实现Review发现的三项（原锁证据不足、executescript切断事务、读取先限字节）全部修正。最后补齐RELEASED终态重试，不新增账本或provider协议。
- 同步候选26b6541后运行143项组合：142通过；唯一失败是新增RUNNING负例构造只写了phase列，未同步原观察document。修正该测试事实，保留拒绝/占用/owner断言，并新增释放后观察丢失用例，最终3项定向全部通过。共144个独立用例有通过记录，不宣称最终快照全套一次性重跑。
- 原先7个SDK容量等待用例均通过；真实process/thread崩溃恢复、固定Lance版本/commit、旧scope关闭、小池满额后恢复再执行、原生维护、P1边界和资源历史均有组合证据。组合公共私有池结束为0条活动占用；故意保留占用的负例使用各自私有池。
- 最后生产差异仅放行严格未启动条件下的原池RELEASED终态；定向注入原provider已释放而Journal尚未确认，重试后scope CLOSED、容量记录字节不变且禁止二次release。
- 默认池原58条活动记录hash全部保持；未清理当前机器旧占用，未重启服务、修改业务数据或更改容量上限。Local CPU process/thread验证通过；缺原证据、GPU/Ray绑定不足的恢复明确拒绝。
- 完整证据：工作区 `_demiflow/shared-review/sdk-capacity-cleanup-20261119/` 中的 `validation.json`、`combined-result.xml`、`final-retry-tests.xml`、`combined-pool-protection.json` 与独立Review记录。

## 2026-11-24 修订：账本物理观察回收

真人决定：执行单元确实已经退出的额度应当归还，不能只等原 run 恢复。原因是额度由整台机器的所有 run 共用，而崩溃的 run 经常不再恢复；实测 driver 被 `kill -9` 后 0.2 秒内 worker 全部退出、unit 锁全部空闲，账本记录却一直占用。本节在“原 run 持租约恢复”和“停机维护”之外增加第三个释放入口，上文的证据标准不变。

- **入口。** 同一容量域内的进程在本地额度不足（或活动记录数达到上限）时，对账本中的活动记录做一次核实，每个进程每秒至多一次；判定在账本锁外进行，释放时由账本在锁内重新核对记录。没有常驻进程。实现在 `resources/local.py` 的 `_unit_state` 与 `_release_gone`。
- **条件，全部成立才释放。** 记录带有创建来源（`origin`：创建它的 driver 的进程身份、启动周期、进程命名空间、单元目录、是否含加速器）；记录与观察者处于同一启动周期和同一进程命名空间；创建它的 driver 已不存在（进程号与启动时间同时相符才算仍在）；记录已绑定进程单元的物理身份，且该进程已不存在；在原 `unit.lock` 的同一个描述符上核对设备号和 inode 与记录一致，并取得排他锁。
- **保持占用。** 任何一项无法确定即不释放：线程单元、含加速器的分配、未绑定的预留、没有来源的旧记录、driver 仍在的记录、锁被持有、被替换或已不存在、启动周期或进程命名空间不同。等待方在准入超时的错误里得到占用方的分类统计。
- **只写容量账本。** 不写原 run 的证据文件和运行账本，不向任何进程发信号。释放引用为 `df-capacity-observed-exit:v1:` 加上对账本根目录、分配标识、规格、物理身份和来源的摘要，可由账本记录重算核验。
- **账本的释放校验不变。** 已释放的记录只接受与原释放相同的证据引用，其他引用一律拒绝，不再次退款，记录字节不变。原 run 与观察者并发时，先在账本锁内落盘的一方为准：观察者后到则不做任何事；原 run 后到则转为核验并采用账本的释放（见下一条）。
- **原 run 之后的恢复与维护。** 在自己的租约下先核验账本的释放：引用须与账本记录重算一致，记录的物理身份须与原 `process.json` 一致；相符则把清理记为已确认并补记运行账本，不再打开原锁；不符则拒绝并保持未确认。
- **锁覆盖的范围。** 平台自己的子进程启动原语（`run_isolated`、隔离进程池、SQL 子进程、受管进程组）让子进程继承 unit 锁，子进程未退出时单元不算退出。算子自行启动的有界子进程不持有该锁；`tests/test_unit_child_processes.py` 列出平台内每一个启动点及其归类，新增启动点必须归类。
- **未覆盖。** 机器重启后仍保留的账本记录、跨容器或跨主机共用账本目录的记录、锁文件已丢失的记录，以及上面列出的保持占用的各类，仍然只能由原 run 的恢复或停机维护处理。

验证见 `tests/test_capacity_observed_release.py`：真实进程下的回收与原 run 恢复（含释放后锁被删除、停机维护、回收者与原 run 恢复并发、原 run 在两次账本调用之间被抢先）、各类保持占用的反例、并发回收只释放一次、不同证据的二次释放被拒绝、准入超时错误内容。
