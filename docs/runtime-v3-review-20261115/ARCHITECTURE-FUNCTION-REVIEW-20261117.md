# 架构与功能对齐复核（2026-11-17）

状态：继续实施、尚未最终验收。生产修复与证据按各自提交记录，不能把不同版本的通过数相加成最终版本全绿。

本轮取舍：性能保持现状，专项优化后延。资源上限、数据正确性、原提交与历史保护仍是功能要求。20 机已由用户取消本轮验收，不再访问或列作当前阻塞。所有实现、测试在隔离树；不修改或运行主仓、不部署。

工作树：`/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_runtime_v3_review_20261115`；分支：`engine/gpt-runtime-v3-review-20261115`。

## 原始意图与架构结论

产品 root：统一、真正可用的工作台，让用户看懂、配置、运行实际 pipeline，分析、比较、评测数据。D1.9、开发、Notebook、Case、媒体和历史不因引擎改造而消失。

平台 root：正确、稳定、通用、资源有界、架构清晰的统一数据计算平台，完整支撑实际业务。高性能目标仍保留，但本轮延期验收。直接原话、来源会话和 I01–I14 见 [INTENT.md](../unified-runtime-v3-20261114/INTENT.md)。

源码主干已经统一：Dataset → 原生 action/编译/封存 → DataflowExecution → Local 或 Ray Core worker → 原 Exchange → 原 RunJournal / CommitCoordinator → 结果与恢复。Ray Core 承载同一运行时；新公开运行不把执行图交给 Ray Data。工作台读取该运行的封存计划和身份桥，不独立编造另一份执行账本。

这支持继续收尾已有架构。发现的缺陷需要修复，但没有证据要求重新搭建执行、资源或恢复体系。

| 原始意图 | 已核对的能力与架构 | 当前保留项 |
| --- | --- | --- |
| I01/I02：同一 Dataset、Local/Ray 承载 | 两适配器下沉同一 execute_action；原公开 API 矩阵与禁止 Ray Data 导入有真实证据 | reducer 返回值兼容已修并合流；显式种子 shuffle 尚在修，矩阵通过本身未覆盖这两处退步 |
| I03/I05/I06：三种交接、多输入/共享/路由、及时背压 | memory_incremental、durable_incremental、finite_sealed；共享调用执行一次，独立调用不误合并；有字节/行 credit | 原三种交接验收观察器已补 explicit_limits 原样透传；性能不在本轮加验 |
| I04：CPU/GPU 独立调度 | stage/资源兼容融合、GPU 隔离和原 ResourceManager 准入；已有真实 H100 与四卡证据 | 不宣称任意有状态模型自动副本化、MIG、硬 VRAM 限额或在线 rescale |
| I07/I10/I13：状态、效果、提交恢复、单一权威与历史 | worker 不持第二 coordinator；恢复基于原 ACK、代码与来源核验；未知付费效果不自动重发 | 本轮补拒绝未完成任务静默变更 provider，已完成结果仍可跨 provider 只读 |
| I08：有限资源、稳定、高性能 | 有限预算及超限/取消/清理继续属于验收要求 | 性能延期，不把原宽行时限失败和未完成五轮矩阵改成通过 |
| I09：完整实际业务 | 模型/缓存/队列/获取/结果写出接入同一运行时和原 journal | 最后合流后按受影响范围组合验收；外部付费服务用本地 transport 替身的证据不冒充实调 |
| I11：多机 | 保留已有真实双宿主、远端 actor 丢失和原 ACK 恢复证据 | 不冒充当前最终版本双机复验；20 机本轮豁免；整机失效/网络分区仍未证明 |
| I12：真实产品 | 封存计划、身份桥、源码、运行控制与结果相连；18 项本轮产品场景取得完整通过证据 | 工作台受管提交仍固定 Local，没有 UI 选择 Ray 集群入口；隔离分支尚未部署 |
| I14：完整交付 | 公开入口、原失败逐项映射；剩余项按真实能力而非卡数判断 | 历史兼容代码和低影响死代码尚未字面全部清理；本轮不以外围清理挤掉核心功能 |

## 本轮发现与处理

### AR-01：未完成任务不得静默更换承载后端——已修

原恢复会把封存 physical policy 与当前 hosts 混用。修复在原 coordinator 验证恢复结果之后、启动 worker 之前验证 provider；不同后端继续执行明确拒绝。同后端续算、final ACK 历史结果只读保持。关闭 Ray 后经 Local 读取原 final 结果不会重开 worker、模型或提交；损坏的 final carrier 仍由原摘要校验拒绝。

root 提交：`8ce7adc`、`8ba214e`、`e58da95`。叶 30 项完整通过，另保留修复前真实红例；总控逐 Git 核验每组 2,997 项输入摘要、测试三阶段及子进程退出。

证据：`/tmp/demiflow-architecture-audit-bd8a642-20261117/PROVIDER-RECOVERY-FIX.md`；`/tmp/demiflow-provider-recovery-independent-audit-20261117.json`。

### FC-01：恢复 reducer 返回 list/scalar 的原能力——已修、已合流，待最终组合验收

旧 `267b914` 六个实际场景全部成功；新实现四个 list/scalar 场景被顶层 Mapping 限制拒绝、两个 dict 场景成功。修复沿现有封闭类型 codec 传递非空业务值，不增加 pickle、不修改旧 Mapping 编码。`None` 仍表示不输出；`False`、`0` 和空容器不能被误丢。

独立复核又发现 `columns=None` 不能证明实际存在非 Mapping 值，不能用来清除表格漂移证据。已补明确的 `non_mapping_values` 事实及原 manifest 封存/恢复：仅实际非 Mapping 写 true，历史缺省 false，非法类型与矛盾字段拒绝；真实 payload_ref 未知列不能掩盖纯表格漂移。审查者在最终 `e740652` 确认该问题闭环。

最终叶 `e740652` 的 metadata 10、Local 20、Ray 8、原相关 99 项具有完整阶段回执，合计 135 通过、2 项下述既有失败；同版本 3,000 个输入摘要已逐 Git 比对，零不符、零残留子进程。12 个叶提交已合入 root 至 `8e2da6d`；`0ac0056` 对齐公开返回值注解和契约说明，与叶生产代码的差异仅为注解/文档，已用 AST 对比确认。最终合流组合测试仍待 shuffle 完成，不冒称 root 同提交全绿。

完整精简证据及各原收据摘要见 [REDUCER-VALUE-CLOSURE-20261117.json](REDUCER-VALUE-CLOSURE-20261117.json)。此前两次退出 143 的长批没有最终回执，仍不计通过；最终短组均正常收尾。

兼容对照：`/tmp/demiflow-functional-compat-independent-audit-20261117.json`。当前叶证据逐 Git 复核：`/tmp/demiflow-reducer-independent-audit-20261117.json`。

原 D009/D010 在本次候选和 `c3393e4` 同样因测试赋予 1,024 字节、实际 codec 需 2,777 字节而拒绝；两例尚未到列信息断言，不能记为通过，也不是本次 reducer 修复引入。保留原预算及失败证据，另用合法有界赋额验证其列一致/漂移语义。

### FC-02：显式 seed 应保持随机结果顺序——修复验收中

旧 Ray 在相同 30 行、seed=7 下重复 action 同序；当前 Local/Ray 均不同序。原因是随机排序键混入 action 绑定的运行身份。新计划将业务随机次序与运行身份分开；旧封存计划继续使用旧公式，不能重写其恢复顺序。

候选需同时保证：重复值的不同记录在同一 action 恢复重算中身份顺序稳定、实际 OperatorKernel 接线可用、借用输入元数据计入窗口、原合法小行 spool 不因新的整段读取上限而被拒绝。独立复核发现的这四项正在修订；尚未接受或合入前一候选。

### 验收夹具与说明修正——已完成

- `c3393e4`：三种 Exchange 的观察器原样转发 `explicit_limits`，原数据、模式和断言不变；原 selector 完整通过。前一次测试缩进错误及启动包装器错误均保留日志，不算生产缺陷。
- `29ca535`：Case 浏览器的无内核测试 Helper 明确 `execution_profile() -> None`。此前它覆盖构造函数却继承依赖未初始化字段的方法；生产 WebService 无此问题。原 Case 交互断言未改，实际场景完整通过。
- `e4eb6bc`：修正 Dataset / write_json 的旧 Ray Data 文案，与统一运行时、原生文件提交的源码行为一致。

## 产品 root：本轮 18 个实际场景

| 产品意图 | 实际覆盖 |
| --- | --- |
| R.1 统一体验、D1.9 与操作可达 | 真实浏览器图、节点选择、来源/配置，1440/1024/390 宽度；开发对话入口保留 |
| R.2 看懂、配置、运行真实 pipeline | union 与嵌套 materialize 的真实封存 action/桥，节点映射落在展示图内；原代码移除后的历史查看与停止；A/B 运行回执及固定结果 |
| R.3 数据理解、Notebook、媒体 | 实际私有 Jupyter → SQL 编译/Broker → DataFusion/Lance；Notebook 保存与 generation/membership；媒体真实展示路径 |
| R.4 Case 比较与评测 | 两个方案、三种评测范围、保存/历史及不同窗口布局；实际操作回执和截图 |
| R.5 边界、清理、历史 | 关闭成功/失败、断连、排空与采用已有对象；来源与上下文隔离、历史保持 |

17 项在 `c3393e4` 通过；最后 Case 项在仅改其测试 Helper 的 `29ca535` 通过。两提交生产代码逐字一致，其余 17 项测试也未变化。每项都有完整 setup/call/teardown，源码前后不变且无残留子进程。这是 18 个不同场景的完整证据，不是一次调用的全仓全绿。

总控独立摘要核验：`/tmp/demiflow-product-c339-independent-audit-20261117.json`、`/tmp/demiflow-product-29ca-independent-closure-20261117.json`。最终 Case 真实操作/截图：`/tmp/demiflow-pf-case-29ca-r1`。产品只读审查：`/tmp/demiflow-product-audit-bd8a642-20261117/REVIEW.md`。

## 完成条件与保留边界

下一步是接收 shuffle 兼容修复，与已经合入的 reducer 修复组合，完成受影响 Local/Ray、原生业务、历史与产品接线的验收。有证据的原通过场景仅在受影响时重跑，不为测试计数重复执行。

本报告不宣称产品或平台 100%。未部署、工作台 Local 提交范围、性能延期、当前版本未做双机复验、没有 DSN 的 PostgreSQL、外部真实付费服务和未完成全仓回归，必须分别保留；不能把这些项目写成已验能力。

原主仓 `e40a6dd`、11 项状态/18 个 dirty 文件摘要及冻结七件保持。历史误覆盖一张浏览器截图的已披露事故仍见 [CURRENT-STATUS.md](CURRENT-STATUS.md)，不撤回、不伪造原件。
