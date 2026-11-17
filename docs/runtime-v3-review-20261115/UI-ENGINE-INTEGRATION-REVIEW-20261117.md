# D1.9/2 工作台与统一引擎合流验收（2026-11-17）

结论：D1.9/2 工作台与当前统一引擎已在同一个隔离候选完成合流和受影响验收，关闭 Claude 复核指出的 M1′/M6 组合缺口。实际被测生产候选固定为 `b4396f8792a8beb58570d2a262e146391e0c4883`。这证明本次组合成立，不等于全部原始意图 100%、全仓全绿或已部署。

## 交付位置与改动性质

- 工作树：`/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_runtime_v3_review_20261115`。
- 分支：`engine/gpt-runtime-v3-review-20261115`。
- 合流提交：`a7d700427e2f352424b18709f0b158a4732bee36`，父版本为当前引擎线 `711ec63` 与新工作台 `3fe56f12be0cbd97c71c20c8322dd432845159cf`。
- 最终兼容修复：`b4396f8792a8beb58570d2a262e146391e0c4883`，补保留资源凭据的符号链接路径拒绝。

这次工作以代码合流为主，附带维持两侧既有能力所需的兼容修复。15 个代码/测试文件在两侧都曾改动，实际发生 9 个文本冲突；没有重新设计 Dataset、提交协议或资源管理。主仓、另一 UI 工作树和运行服务不切换。

## 必须保留的接口语义

| 接合处 | 合流处理与实际依据 |
| --- | --- |
| 封存计划与显示元数据 | 新前端的公共封存校验保留，同时接受引擎已有的可选 `sealed_at` 与 `observation_labels`；旧缺失值不回填、不重算身份。 |
| 真实节点观测 | producer 在真正 seal/bridge 形成后把完整 plan/dag/revision/occurrence scope 记录进原 journal；工作台只读这些事实，旧无 scope 行不升级为已验证节点。 |
| 资源凭据和退出 | `resources/local.py` 与 `process_unit.py` 保持当前引擎版本；继续原 JSON 引用和字节摘要，不恢复旧路径字符串协议。凭据的 record/allocation/scope 三层符号链接均拒绝。 |
| 原前端修复 | 声明图源码请求不带执行 occurrence；嵌套 GPU 资源画像按实际对象展示。版本、迟到响应、Case/Notebook 与同源资产注册保留。 |
| 验收接口迁移 | 资源测试改为核对当前 JSON 凭据，继续检查真实创建/退出、预算、错误域、字节变化、三层符号链接和原 CPU/device 账本；没有降低生产约束。 |

原 symlink 缺口在合流提交 `a7d7004` 真实复现，修复前证据 `/tmp/demiflow-ui-engine-symlink-red-20261117-ylv1ltqs/result.json` 保留。修复只收紧原证据读取边界，没有引入第二套存储或资源账本。

## 对照两大原始意图

产品 root：统一工作台中看懂、配置、运行真实 pipeline，分析、比较、评测数据。

| 产品分支 | 本次合流上的真实检查 |
| --- | --- |
| R.1 统一体验与 demo | 开发、管道、数据、Readme 四 tab；1440/1024/390 宽度；固定上下文及迟到响应隔离。 |
| R.2 看懂与真实运行 | 浏览器把阈值从 0.8 改为 0.9，实际提交结果从 2 行变为 1 行；原固定结果仍为 2 行。两个 v3 run 各自有真实计划、7 个节点、7 条桥与 7 条完整 scope 观测。 |
| R.3 数据与 Notebook | 真实 Jupyter Python 与 SQL 在同一 session/generation 执行；结果显式固定后进入图表和 Case。数据版本切换保持实际字段、行数与媒体来源。 |
| R.4 比较与评测 | Case 多材料、三种评测范围、冲突与历史、保存重开；Notebook 来源与标注来源保留。原 Case 交互回归另列结果，不冒充每个旧夹具都启动真实模型。 |
| R.5 工程与历史保护 | 原报告摘要、固定版本、无观测行不造状态；真实 Notebook 的 6 个租约退出并完成清理；多 action 11 条原生观测与对应身份桥匹配。 |

平台 root：同一 Dataset/运行时，Local/Ray Core 承载，资源有界，原提交恢复与唯一权威。

本轮保留当前引擎的 compiler/runtime、原 RunJournal/CommitCoordinator/ResourceManager；工作台读取引擎封存事实。native 组覆盖真实执行接合、封存与资源；真实 Local/Ray 恢复结果在下表单列。先前的公开 API、缓存状态、GPU 和队列业务仍沿各自明确版本保留证据，本次不把它们伪装成新 HEAD 全量重跑。性能按用户要求后延。

## 验收结果与证据边界

| 检查组 | 本次结果 | 原始回执/审计 |
| --- | --- | --- |
| 原生接合、封存、资源 | 101 passed，0 skipped | `/tmp/demiflow-ui-engine-20261117-native-r1-00-jpq0n57g/evidence/verification.json` |
| D1.9 九门与额外接合/导航 | 九门 13 例及额外 5 例全部通过；两条旅程包含其中，不重复计数 | `/tmp/demiflow-ui-engine-20261117-independent-nine-gates-audit.json`；`/tmp/demiflow-ui-engine-20261117-independent-native-journeys-audit.json` |
| Case、版本、投影与观测补充 | 39 passed，0 skipped | `/tmp/demiflow-ui-engine-20261117-extra-r1-00-wea8bd9y/evidence/verification.json` |
| 原 14 文件回归 | 原 37 条完整阶段记录，加尾批 45 passed；合计 82 条取得个体通过证据、4 条明确未验，共 86 条精确覆盖 | `/tmp/demiflow-ui-engine-20261117-interrupted-oldreg-independent-audit.json`；`/tmp/demiflow-ui-engine-20261117-oldtail-r1-00-u_dtxj9f/evidence/verification.json` |
| 实际 Local/Ray 恢复 | 4 passed，0 skipped；测试独占私有集群，退出后未初始化、无残留子进程 | `/tmp/demiflow-ui-engine-20261117-ray-provider-r1-00-9sasksfy/evidence/verification.json` |

旧 14 文件批次曾以 143 提前结束，没有原 finalizer/JUnit。其 37 个节点有完整原始 setup/call/teardown 通过记录，1 个节点明确 skipped，剩余 48 个节点另批完成（45 passed/3 skipped）。原批保持 `original_group_complete=false`；独立事后源码/残留核对不能冒充原运行末尾回执，也不宣称一次 86 项全绿。两个批次源码逐字一致；中断时仅完成 setup 的那条用例已在尾批完整通过。

4 条未验项分别为：两条需要真实 pre-codec 旧客户端 JS 与 SHA256 的兼容反例、一条需要历史基线和提案配对的旧 UI 对比，以及一条需要 P2 工作树和大于有符号 64 位 inode 的真实文件系统反例。保留原跳过原因，不用当前客户端或伪造 inode 代替。

每个完整批次均固定 import 归属和 Git HEAD，核对 3,022 个输入摘要、三阶段报告、JUnit 与清理。独立源码审查未发现新增意图级阻塞。最终精确覆盖、未验项和原始文件摘要见配套 JSON。

恢复四例实际检查：Local 和 Ray 的非 final ACK 都拒绝切换 provider，拒绝发生在 worker 启动之前，原封存不变；使用原 provider 恢复得到完整有序结果及 final ACK。已完成 Ray 结果在未启动 Ray 的 Local 读取路径可原样读回，不新增 worker/提交；原 final 证明内容被改写时，即使仍声称 final 且文件长度相同，也因摘要不符被拒绝。原 Ray Data 导入阻断器保持启用。

汇总索引：[UI-ENGINE-INTEGRATION-EVIDENCE-20261117.json](UI-ENGINE-INTEGRATION-EVIDENCE-20261117.json)。源码复审及各批独立核验文件在该索引按路径和 SHA256 引用，原回执、原中断与修复前失败均不覆盖。

## 保留边界

本次不宣称全部原始意图 100%、全库全绿或已经发布。工作台受管提交仍只开放 Local；引擎 Ray Core API 与恢复另有实际验收。性能延期、20 机当前复验取消；PostgreSQL、指定 Stage2 真实源、完整离线 wheel/完整 V035、外部真实付费服务等原未验项保持。原三条基线断言差异不改为通过。原历史客户端等缺少真实输入的用例明确列为未验。

主仓 `e40a6dd`、11 项状态/18 文件摘要及冻结七件保持；不 push、不合主仓、不部署。文档提交与实际被测生产提交区分，不把文档更新当新一轮运行证据。
