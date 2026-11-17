# 架构与功能对齐复核（2026-11-17）

状态（经 Claude 报告独立复核后纠正）：本轮引擎核心修复及受影响组合验收完成；D1.9/2 新前端与最新引擎尚未合流，完整产品组合、全库验收和发布未完成。测试候选 `9e614af`，最终生产代码 `08335c8`（后续为测试修正）；新前端在独立候选 `3fe56f1`。此前“核心架构与功能收口”遗漏了这项产品阻塞，不能解释成两大原始意图已经交付。生产修复与证据按各自提交记录，不能把不同版本或不同前端的通过数相加成最终版本全绿。见[复核裁决](CLAUDE-REVIEW-RECONCILIATION-20261117.md)。

本轮取舍：性能保持现状，专项优化后延。资源上限、数据正确性、原提交与历史保护仍是功能要求。20 机已由用户取消本轮验收，不再访问或列作当前阻塞。所有实现、测试在隔离树；不修改或运行主仓、不部署。

工作树：`/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_runtime_v3_review_20261115`；分支：`engine/gpt-runtime-v3-review-20261115`。

## 原始意图与架构结论

产品 root：统一、真正可用的工作台，让用户看懂、配置、运行实际 pipeline，分析、比较、评测数据。D1.9、开发、Notebook、Case、媒体和历史不因引擎改造而消失。

平台 root：正确、稳定、通用、资源有界、架构清晰的统一数据计算平台，完整支撑实际业务。高性能目标仍保留，但本轮延期验收。直接原话、来源会话和 I01–I14 见 [INTENT.md](../unified-runtime-v3-20261114/INTENT.md)。

源码主干已经统一：Dataset → 原生 action/编译/封存 → DataflowExecution → Local 或 Ray Core worker → 原 Exchange → 原 RunJournal / CommitCoordinator → 结果与恢复。Ray Core 承载同一运行时；新公开运行不把执行图交给 Ray Data。工作台读取该运行的封存计划和身份桥，不独立编造另一份执行账本。

这支持继续收尾已有架构。发现的缺陷需要修复，但没有证据要求重新搭建执行、资源或恢复体系。

| 原始意图 | 已核对的能力与架构 | 当前保留项 |
| --- | --- | --- |
| I01/I02：同一 Dataset、Local/Ray 承载 | 两适配器下沉同一 execute_action；原公开 API 矩阵与禁止 Ray Data 导入有真实证据 | reducer 和显式 seed shuffle 已修、已组合验收；原矩阵未覆盖的缓存状态缺口另有真实故障与修复 |
| I03/I05/I06：三种交接、多输入/共享/路由、及时背压 | memory_incremental、durable_incremental、finite_sealed；共享调用执行一次，独立调用不误合并；有字节/行 credit | 原三种交接验收观察器已补 explicit_limits 原样透传；性能不在本轮加验 |
| I04：CPU/GPU 独立调度 | stage/资源兼容融合、GPU 隔离和原 ResourceManager 准入；本轮 7 个真实 GPU 场景通过，含四张 H100 同时运行的正常/失败/取消及释放 | 不宣称任意有状态模型自动副本化、MIG、硬 VRAM 限额或在线 rescale |
| I07/I10/I13：状态、效果、提交恢复、单一权威与历史 | worker 不持第二 coordinator；恢复基于原 ACK、代码与来源核验；未知付费效果不自动重发 | 本轮补拒绝未完成任务静默变更 provider，已完成结果仍可跨 provider 只读 |
| I08：有限资源、稳定、高性能 | 有限预算及超限/取消/清理继续属于验收要求 | 性能延期，不把原宽行时限失败和未完成五轮矩阵改成通过 |
| I09：完整实际业务 | 模型/缓存/队列/获取/结果写出接入同一运行时和原 journal | Local/Ray 原完整队列、模型/图像/恢复已有证据；函数缓存恢复另有专门组合证据；本地 transport 不冒充外部付费实调 |
| I11：多机 | 保留已有真实双宿主、远端 actor 丢失和原 ACK 恢复证据 | 不冒充当前最终版本双机复验；20 机本轮豁免；整机失效/网络分区仍未证明 |
| I12：真实产品 | 旧前端的封存计划、身份桥、源码、运行控制与结果相连；18 项场景有通过证据 | D1.9/2 新前端 `3fe56f1` 尚未与最新引擎组合验收，是当前产品交付阻塞；受管提交仅 Local；隔离分支尚未部署 |
| I14：完整交付 | 公开入口、原失败逐项映射；剩余项按真实能力而非卡数判断 | 历史兼容代码和低影响死代码尚未字面全部清理；本轮不以外围清理挤掉核心功能 |

## 本轮发现与处理

### AR-01：未完成任务不得静默更换承载后端——已修

原恢复会把封存 physical policy 与当前 hosts 混用。修复在原 coordinator 验证恢复结果之后、启动 worker 之前验证 provider；不同后端继续执行明确拒绝。同后端续算、final ACK 历史结果只读保持。关闭 Ray 后经 Local 读取原 final 结果不会重开 worker、模型或提交；损坏的 final carrier 仍由原摘要校验拒绝。

root 提交：`8ce7adc`、`8ba214e`、`e58da95`。叶 30 项完整通过，另保留修复前真实红例；总控逐 Git 核验每组 2,997 项输入摘要、测试三阶段及子进程退出。

证据：`/tmp/demiflow-architecture-audit-bd8a642-20261117/PROVIDER-RECOVERY-FIX.md`；`/tmp/demiflow-provider-recovery-independent-audit-20261117.json`。

### FC-01：恢复 reducer 返回 list/scalar 的原能力——已修、已组合验收

旧 `267b914` 六个实际场景全部成功；新实现四个 list/scalar 场景被顶层 Mapping 限制拒绝、两个 dict 场景成功。修复沿现有封闭类型 codec 传递非空业务值，不增加 pickle、不修改旧 Mapping 编码。`None` 仍表示不输出；`False`、`0` 和空容器不能被误丢。

独立复核又发现 `columns=None` 不能证明实际存在非 Mapping 值，不能用来清除表格漂移证据。已补明确的 `non_mapping_values` 事实及原 manifest 封存/恢复：仅实际非 Mapping 写 true，历史缺省 false，非法类型与矛盾字段拒绝；真实 payload_ref 未知列不能掩盖纯表格漂移。审查者在最终 `e740652` 确认该问题闭环。

最终叶 `e740652` 的 metadata 10、Local 20、Ray 8、原相关 99 项具有完整阶段回执，合计 135 通过、2 项下述既有失败；同版本 3,000 个输入摘要已逐 Git 比对，零不符、零残留子进程。12 个叶提交已合入 root 至 `8e2da6d`；`0ac0056` 对齐公开返回值注解和契约说明，与叶生产代码的差异仅为注解/文档，已用 AST 对比确认。root `799d04a` 的 Local/Ray reducer → shuffle → materialize → map → final ACK 恢复 2 项完整通过；保留叶与 root 各自版本，不冒称全库同提交全绿。

完整精简证据及各原收据摘要见 [REDUCER-VALUE-CLOSURE-20261117.json](REDUCER-VALUE-CLOSURE-20261117.json)。此前两次退出 143 的长批没有最终回执，仍不计通过；最终短组均正常收尾。

兼容对照：`/tmp/demiflow-functional-compat-independent-audit-20261117.json`。当前叶证据逐 Git 复核：`/tmp/demiflow-reducer-independent-audit-20261117.json`。

原 D009/D010 在本次候选和 `c3393e4` 同样因测试赋予 1,024 字节、实际 codec 需 2,777 字节而拒绝；两例尚未到列信息断言，不能记为通过，也不是本次 reducer 修复引入。保留原预算及失败证据，另用合法有界赋额验证其列一致/漂移语义。

### FC-02：显式 seed 应保持随机结果顺序——已修、已组合验收

旧 Ray 在相同 30 行、seed=7 下重复 action 同序；当前 Local/Ray 均不同序。原因是随机排序键混入 action 绑定的运行身份。新计划将业务随机次序与运行身份分开；旧封存计划继续使用旧公式，不能重写其恢复顺序。

最终候选处理了重复记录的恢复身份、实际 OperatorKernel 接线、借用输入元数据准入和合法小行 spool 窗口，也修正归并尾帧、堆和输入同时驻留的核算。root `799d04a` 已合流；叶 56 个不同用例完整 168 阶段通过，含 Local/Ray、30 行同 seed 重复、120 行 union 重复值、非 final ACK 和 1 MiB 宽行预算。算法继续复用原 store/grant/GC；新计划用 typed-multiset-v1，旧封存计划仍读旧公式。详见 [SHUFFLE-SEED-COMPAT.md](../unified-runtime-v3-20261114/review-20261117/SHUFFLE-SEED-COMPAT.md)。

### 验收夹具与说明修正——已完成

- `c3393e4`：三种 Exchange 的观察器原样转发 `explicit_limits`，原数据、模式和断言不变；原 selector 完整通过。前一次测试缩进错误及启动包装器错误均保留日志，不算生产缺陷。
- `29ca535`：Case 浏览器的无内核测试 Helper 明确 `execution_profile() -> None`。此前它覆盖构造函数却继承依赖未初始化字段的方法；生产 WebService 无此问题。原 Case 交互断言未改，实际场景完整通过。
- `e4eb6bc`：修正 Dataset / write_json 的旧 Ray Data 文案，与统一运行时、原生文件提交的源码行为一致。

## 产品 root：旧前端的 18 个实际场景，不代表 D1.9/2 组合通过

范围纠正：这些回执来自最新引擎修复线所带的旧前端。该树没有新前端的 presentation/charts 模块；D1.9/2 的四 tab、完整呈现与用户旅程在另一棵 `3fe56f1` 候选上实现和验收。因此下表只保留实际经过的旧前端功能，不能用它认定新 demo 已保留并接上最新运行时。新前端与引擎合流后的两条完整旅程及受影响门仍未运行。

| 产品意图 | 实际覆盖 |
| --- | --- |
| R.1 既有前端操作可达；D1.9/2 组合待验 | 旧前端的真实浏览器图、节点选择、来源/配置，1440/1024/390 宽度；开发对话入口保留；不据此认证新 demo |
| R.2 看懂、配置、运行真实 pipeline | union 与嵌套 materialize 的真实封存 action/桥，节点映射落在展示图内；原代码移除后的历史查看与停止；A/B 运行回执及固定结果 |
| R.3 数据理解、Notebook、媒体 | 实际私有 Jupyter → SQL 编译/Broker → DataFusion/Lance；Notebook 保存与 generation/membership；媒体真实展示路径 |
| R.4 Case 比较与评测 | 两个方案、三种评测范围、保存/历史及不同窗口布局；实际操作回执和截图 |
| R.5 边界、清理、历史 | 关闭成功/失败、断连、排空与采用已有对象；来源与上下文隔离、历史保持 |

17 项在 `c3393e4` 通过；最后 Case 项在仅改其测试 Helper 的 `29ca535` 通过。两提交生产代码逐字一致，其余 17 项测试也未变化。每项都有完整 setup/call/teardown，源码前后不变且无残留子进程。这是 18 个不同场景的完整证据，不是一次调用的全仓全绿。

总控独立摘要核验：`/tmp/demiflow-product-c339-independent-audit-20261117.json`、`/tmp/demiflow-product-29ca-independent-closure-20261117.json`。最终 Case 真实操作/截图：`/tmp/demiflow-pf-case-29ca-r1`。产品只读审查：`/tmp/demiflow-product-audit-bd8a642-20261117/REVIEW.md`。

## FC-03：缓存结果不能代替原 ACK 中的 actor 状态

最终业务组合发现：async CachedMap 包装有状态 partial actor，checkpoint_rows=2、6行，首个非final ACK中断后恢复，后缀 count 返回3/4而预期5/6。同步版本通过。原因是未ACK回调已独立发布到结果缓存，恢复从原状态快照继续时命中缓存而漏掉状态推进。独立叶已复现真实红例。

修复已合流，保留原缓存键、每键锁、即时跨 run 共享及原提交权威。缓存仅附有界的创建者/代次/结果摘要；恢复状态和接受代次仍由原 operator image/ACK 决定。恢复遇到同一所有者在 ACK 后产生的缓存结果时，补做状态推进并逐字核验结果，不能覆盖已经共享的值。

所有者绑定 run、action、真正的 state owner、逻辑分区、worker slot 及同步任务序号，不绑定短暂的进程或 attempt。其他所有者不能覆盖带有状态恢复来源的结果；相同值的普通 KV/原生 sink 写入保留来源信息，不同值要求使用新 version/namespace。旧的无来源普通 KV 保留覆盖语义。最多 512 字节/键的来源信息计入原 max_cache_bytes，不另加预算。

若 cache_when 已覆盖掉恢复所需的旧判断输入，续算明确拒绝，不能假装恢复正确。旧有状态快照缺少 frontier 也拒绝；更旧、尚无首个快照的运行由原 platform_revision 校验阻止跨运行时续算。没有建立第二份恢复账本。

独立叶确认 53 个不同用例通过（16 原批次 + 1 真进程崩溃 + 35 原兼容 + 1 多进程共享补验，保留各版本/原失败）。原故障确实发生在原非 final ACK 之后，已有至少 4 条缓存结果；进程直接退出后恢复输出精确为 1..6。合流后 Local/Ray 的缓存→恢复→物化→固定 seed shuffle 2 例及旧恢复/清理 5 例共 7 例通过。

另保留一条原 KV 用例失败：`test_dataset_process_crash_recovers_original_sqlite_commit[after_prepare_before_record]` 在原 `e1e90a0` 和本修复上都期望纯计算只执行 2 次，实际为 4 次；结果、单份提交、历史值和 ACK 断言全部通过。原 CommitCoordinator.recover 明文规定 pending（尚未持久化 prepared）的计算可以重做。本轮未削弱原断言，也未修改提交协议迁就该次数期待；它仍记为未通过，不算新修复回归，更不等于外部付费效果可任意重发。

## FC-04：普通函数的可变状态——已修、Local/Ray 组合验收完成

缓存包装器有 snapshot/restore 方法，因此编译器按包装器判定为可恢复；此前内层普通函数却一律快照为 None，漏掉闭包、默认参数、partial 参数及判断函数的可变状态。这不是 code-pin 可以解决的问题：代码固定只能保证初始程序一致，不能代替运行中的状态。

实现直接复用已有 function_slots_v1 快照/恢复与 clone_resident_callable，不创建新的状态序列化体系。常见闭包、默认值、partial 数据接入原缓存代次和原 ACK。不能由现有适配器完整表达的全局、嵌套共享对象在业务调用前写原 journal 的 attempt_only 限制，普通执行继续允许，恢复不能静默丢状态。六种实际进程退出场景已验证：actor、闭包、默认参数、关键字默认参数、partial 位置和关键字参数。都在原非 final ACK 之后且至少 4 条结果已入缓存时终止进程，恢复精确输出 1..6；不是只调用 snapshot/restore 的单元演示。最终 Local/Ray 的实际组合结果见下表。


本轮另外保护多个回调共享的 list/dict 与 nonlocal 标量 cell：不能各自克隆后把原先共享的业务状态拆开。该范围保留正常执行关系，沿原 attempt_only 契约限制恢复；编译后的 kernel 同时标为 nonrecoverable，不能被调度器当作可暂停重建的实例。

改动入口：`result_cache._state_policy`、`CacheCall.validate_runtime_state/snapshot_state/restore_state`、`CacheFactory.__new__`、`OperatorKernel._create_instance/_snapshot_state`、`compile_kernel_spec`。使用原 function_slots_v1 与原 journal，不新增状态账本、提交协议或通用 pickle 状态。叶提交 `d763746 → 0c3b699 → 50d02e7` 已精确合入 root `57eacc7 → 21b8b0f → 08335c8`；五个改动文件逐字一致。三批 38/27/11 项均有完整阶段、正常退出、零源码漂移和零遗留子进程，不能相加为不同用例总数。

## 验收结果

| 本轮已完成组 | 结果与版本 | 原始证据 |
| --- | --- | --- |
| reducer → shuffle → materialize → 后续 map → final ACK 恢复 | Local/Ray 2 例通过，`799d04a` | `/tmp/demiflow-final-composed-r1-evidence/verification.json` |
| 关系/排序聚合/三种 Exchange/共享分支 | Local 5、Ray 5 例通过，`799d04a`；Local 另包含及时首行 | `/tmp/demiflow-final-core-{local,ray}-r{1,2}-evidence/verification.json` |
| 公开入口矩阵 | Local/Ray 各 98 入口、168 场景通过；其中 4 个退役 checkpoint 入口只验证明确拒绝，不算可用能力；Ray Data 导入尝试 0 | `/tmp/demiflow-final-api-{local,ray}-r1-evidence/verification.json` |
| 原生业务 B01 | 三模型恢复、完整队列、document 原载荷 3 项通过；原 cache gate 的 7 个历史子场景在测试导入路径修正后通过，生产字节相同 | B01 原回执及 `/tmp/demiflow-final-cache-closure-r2-evidence/verification.json` |
| 业务边界 B02 | 原 `e1e90a0` 的 12 通过/FC03 1 失败保留；原失败的 partial actor 同步/异步两模式已在 `08335c8` 完整通过，不改写旧回执 | `/tmp/demiflow-final-b02-r1-evidence/verification.json` |
| 工作台真实受管提交与 A/B 结果 | 2 例、完整 6 阶段通过，`e1e90a0` | `/tmp/demiflow-fp-managed-e1e-r1/receipt.json` |
| 图像完整链路和提交恢复 | 8 例、完整 24 阶段通过，`e1e90a0`；transport 为本机替身 | `/tmp/demiflow-fp-image-e1e-r1/receipt.json` |
| 本机真实 GPU | 7 例/21 阶段通过，`e1e90a0`；四张 H100 正常/失败/取消并发及资源释放 | `/tmp/demiflow-fp-gpu-e1e-r1-evidence/verification.json` |
| 历史身份与代码/来源保护 H01 | 6 例通过，`e1e90a0` | `/tmp/demiflow-final-h01-r1-evidence/verification.json` |
| 缓存完整组合与 H02 | Local/Ray 新组合 2 + 原恢复清理 5 通过，`2309da3` | `/tmp/demiflow-final-cache-composed-r1-evidence/verification.json` |
| Ray 原模型/未知付费效果 | 2 例通过，`e1e90a0`；同批 queue 的依赖环境错误另补 | `/tmp/demiflow-final-ray-r1-evidence/verification.json` |
| 函数状态与真实业务最终组合 | 8 个 Local/Ray 状态组合 + 原缓存业务 6 项均有完整通过证据；`08335c8/00859ea/9e614af` 生产树一致 | [逐项组合证据](FUNCTIONAL-CLOSURE-EVIDENCE-20261117.json)；原 r1–r4 回执保留 |
| Ray 完整队列业务 | 正式原 selector 完整三阶段通过、退出 0，`2309da3`；5 场景、19 次统一执行、112 原分配全部 terminal/CONFIRMED；无残留子进程 | `/tmp/demiflow-final-ray-queue-r4-evidence/verification.json`；`/tmp/demiflow-final-ray-queue-r4-audit-20261117.json` |

根独立逐 Git 核验完整源码摘要、三阶段、退出码和子进程：最终接受的完整根节点组及函数状态三批均零摘要不符、正常退出、无跳过计通过和残留子进程；含失败的历史批次按 selector 保留原结果，不冒称整组通过。原 cache 外层失败是 nested pytest 丢失 tests 辅助模块导入路径；`4c04d86` 的第一次修正误命中相邻 helper，失败保留，`e1e90a0` 只修实际 run_nodeids 并还原相邻 helper。该夹具问题与 FC03 的真实缓存状态缺陷分别记录。

既有原 33 项失败已逐 XML 核实关闭；U2/U3 核心 24 项已逐三阶段回执核实关闭，均保留各自版本。不把跨版本通过数相加冒充当前全库全绿。V035 完整 collection 尚未取得当前候选全覆盖证据，继续标为未完整验收。

Ray 队列补验保留原 r1 缺 babel、r2 原生搜索子进程缺依赖、r3 外层被信号结束的证据。最终 r4 使用既有私有解释器引用已有依赖，不修改共享环境；原业务、预算、断言不变，真实入口和分配 journal 均独立核验。

## 完成条件与保留边界

本轮已证实的核心修复均已完成受影响的组合验收。最终 8 个 Local/Ray × actor/closure/default/partial 场景均在真实“已确认调用 2、缓存结果 3”的窗口中断，随后恢复精确输出 1..6，物化结果、固定 seed 后续计算、原封存 provider 和 ACK 均成立。原缓存业务 6 项也完整通过。生产树逐 Git 字节一致；测试注入修正及原失败分别保留，不能误称一次 14 项全绿。没有新增性能工作。

本报告不宣称产品或平台 100%。D1.9/2 新前端与最新引擎未合流是必须关闭的产品缺口，不能与后延的性能或缺少环境的外部验证混为一谈。未部署、工作台 Local 提交范围、性能延期、当前版本未做双机复验、没有 DSN 的 PostgreSQL、外部真实付费服务和未完成全仓回归，必须分别保留；不能把这些项目写成已验能力。

原主仓 `e40a6dd`、11 项状态/18 个 dirty 文件摘要及冻结七件保持。历史误覆盖一张浏览器截图的已披露事故仍见 [CURRENT-STATUS.md](CURRENT-STATUS.md)，不撤回、不伪造原件。

## 用户实际可用范围与剩余项分类

| 类别 | 结论 | 本轮处理 |
| --- | --- | --- |
| 已验核心架构 | Dataset 使用同一编译、执行和提交恢复；Local/Ray Core 承载；新路径无 Ray Data | 已修、已验证相关组合 |
| 已验核心功能 | 输入、变换、模型/缓存、关系运算、物化/写出、原 ACK 恢复；工作台封存事实与固定结果相连 | 保留逐版本证据，不用计数代替功能 |
| 未完成的产品组合 | D1.9/2 新前端与最新引擎分别存在；旧前端 18 项通过不能证明新产品贯通 | 隔离合流、保留两侧语义、同一候选验证两条旅程和受影响门 |
| 现有能力限制 | 工作台提交入口仅 Local；函数外部/共享状态须显式适配才能可靠续算；部分 cache_when 替换丢失历史输入时拒绝恢复 | 如实说明，不隐藏 fallback |
| 已知旧断言差异 | D009/D010 的 1,024 字节不足以容纳实际 2,777 字节 codec；KV pending 窗口要求纯计算不重做与原提交协议相反 | 三条仍记失败，基线对照证明非本轮新增；未改预算/断言迎合测试 |
| 环境/全覆盖未验 | PostgreSQL DSN、Stage2 固定真实源、离线 wheel、少数 Ray fixture 变体与完整 V035 | 不计通过，不据此推断都有生产缺陷 |
| 真人后延/取消 | 性能优化；20 机当前验收 | 本轮不作为修复阻塞，旧失败/旧双宿主证据保留 |
| 发布状态 | 只在隔离树，未部署、未 push、未合并主仓 | 用户正在使用的主仓不会自动获得本修复 |

因此本报告支持引擎核心架构与已验功能的阶段结论，不支持完整产品已交付、“全部原始意图 100%”或“当前全库测试全绿”的声明。

## 最终组合故障注入的纠正记录

root 首批 `08335c8` 的原缓存业务 6 项通过，新增组合 8 项在中断前失败。新测试要求缓存至少写出 4 条，这个数量没有契约依据；Local 在该窗口实际写到第 3 条，Ray 还涉及下述驱动 RPC 停顿。故障注入应验证缓存领先于已接受状态，不应要求运行时超过其有限预取窗口。失败不发生在恢复结果断言，也未据此修改生产调度或放大预算。

`00859ea` 只修改这份新增测试：从原 ACK 指向的原生 kernel image 读真实 call_ordinal，验证 cache_frontier，要求缓存条数严格大于已确认调用数后才注入中断。与原纯快照测试相比，该检查验证实际缓存确实领先于已接受的状态；没有固定要求运行时多预取。原 8 条失败回执全部保留，改动前后生产代码逐字一致。

最终每个 Local/Ray × actor/closure/default/partial 场景记录实际 commit_id、accepted_calls、cached_results，随后验恢复输出、物化读回、固定 seed 后续计算、封存 provider 和全部原 ACK。记录进入 pytest 原回执属性，不仅是终端文字。

`9e614af` 补齐故障注入的控制服务：等待缓存领先时，继续驱动原 worker RPC 与 journal 回复，只暂停下一个 cut 的发布。否则 Ray worker 会因合法等待驱动回复而无法进入目标故障窗口。三个原失败场景补验全部通过；没有修改生产逻辑、预算、业务数据或恢复输出断言。

完整逐项证据见 [FUNCTIONAL-CLOSURE-EVIDENCE-20261117.json](FUNCTIONAL-CLOSURE-EVIDENCE-20261117.json)；原 33 项、U2/U3 与未验余额见 [回归核对](REGRESSION-RECONCILIATION-20261117.md)。
