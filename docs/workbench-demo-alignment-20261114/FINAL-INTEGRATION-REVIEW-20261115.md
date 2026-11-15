# UI-D1.9/2 独立复核与集中修复

日期：2026-11-15。当前状态：本批 UI-D1.9/2 的 Local 集成修复与验收完成。九门 13 个参数化用例通过；14 份旧回归文件汇总 82 passed、4 not_run、0 failed。机器可读记录见 [验收清单与原生证据](acceptance-results-20261115.json)。这不等于整个平台、全部业务或未验环境已经完成。

## 意图与交付边界

产品 root 是一个真正可用的统一工作台：看懂、配置、运行 pipeline，分析、比较、评测数据。平台 root 是正确、稳定、高性能、通用、架构清晰的统一计算平台。本批按这两个目标审源码、原生记录和实际用户操作，不按 Qwen 的已完成声明或任务卡数验收。

工作基础是 `ui_d19_integration_20261115@dc29bc9`。修复树为 `/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_ui_review_20261115`，分支 `ui/gpt-d19-final-fixes-20261115`。原候选的两份 dirty Case 测试保留；所有修复和测试迁移都发生在本树。未 push、未合主仓、未部署，未操作在线服务或训练。

实际接入的平台代码是 `gpt_runtime_v3_review_20261115@49b666fbff1257770b3ee3a3a61af4efc13146dc` 的已提交版本，没有复制其在途 dirty。它在自己的进展文档中仍有未完成项；UI 验收通过也不能推出整个平台或两大 root 达到 100%。

## 源码复核发现及修复

| 项目 | 实际问题与意图影响 | 修复与证据 |
|---|---|---|
| F01 资产装配 | 新 presentation JS 和 graph CSS 都不在服务白名单，主工作台不能 mount；离线导出也漏装配 | API 共用静态资产清单，HTTP 与离线导出一致。真实浏览器与离线文件验证；并非只补 Qwen 提到的一个 JS |
| F02 旧计划投影 | `dag.sources[].source_id` 被当成 logical node ID。原生 7 节点/6 边成功运行被前端拒为 invalid_topology，显示空图 | 严格按各自身份字段验证，不重新计算 DAG 身份；由真实旧版本运行生成历史，新工作台读取并恢复图 |
| F03 统一 Runtime | 原候选只有 W 的消费接线，没有可运行的 Runtime v3 原生实现；旧执行器成功不能证明统一执行 | 合入平台已提交代码，保留 UI occurrence 端点；真实 workbench prepare/start 产生 `demiflow.dataflow_graph.v3` action |
| F04 观测来源 | 读端把当前封存计划身份加到无 scope 的旧 journal 行；前端又只按报告节点/occurrence 选状态，可能把另一计划的记录显示为成功 | Dataflow 在真实封存后绑定 scope，CheckedExecution 与 RunJournal 持久化原行；读端不补写。页和行逐一核对 plan/DAG/revision/occurrence，缺失或错配不提升 |
| F05 报告身份 | 前端比较 bridge.report_digest 与 run binding.basis_digest；后者是 manifest digest。真实多 action 的桥全被误拒 | graph 顶层显式提供原 report_digest，GraphBinding 原协议保持；前端严格比较原报告身份。错误 digest、缺失及 manifest 冒充均拒绝 |
| F06 封存时间 | 新主图丢失 sealed_at 可读展示；进一步真实验收发现 v3 producer 本身没有写时间 | 主 action 栏显示语义化 `<time>`，v3 首次封存写入时间；重封存保留原值，旧 marker 缺时间则继续缺失。18 项原生回归与实际 F10 浏览器通过，原值 `1794750246173` 精确呈现为 `2026-11-15T13:44:06.173Z` |
| F07 历史版本入口 | 固定上下文尚未完成时版本入口可访问空 aliasBinding；失败提前锁存 versionsLoaded，无法重试 | 入口等待真实固定上下文，成功后才锁存，复用同一 pending 请求；失败可重试 |
| F08 长路径资源 | Local 已启动 worker，但绝对 evidence_ref 超 256 字符，被误判 FAILED；cleanup 同样失败 | 长路径改稳定有界引用，可限定 scope/allocation 解析原文件。短路径保持原样。内部 CPU/device 释放账本仍用原绝对证据，旧记录可幂等重试，不重写历史 |
| F09 固定结果进 Case | 原生固定表读取完成后，ReturnCase 没有 records，真实 deferred Task 构造时崩溃，分析 helper 退出 | ReturnCase 绑定 controller 的同一个 Records；真实 Task/SQLite 锁检查及完整真实内核旅程验证 |
| F10 验收失真 | 原 V05 没有 SQL、图表、Case，也把 Python 侧调用当浏览器请求；一些门要求不存在的 API 字段或已替换文案 | 改为真实用户操作和原生输入/输出身份断言。旧 UI 选择器迁移保留业务/身份/异常断言；旧客户端拒绝必须提供真实历史 JS，否则独立标 skip |
| F11 Case 快速编辑 | 展开条件后立即选字段，重绘会先于原生 toggle 事件，导致面板意外收起；旧 DOM 的事件还能覆盖新状态 | 重绘前捕获实际 open，忽略已脱离 DOM 的旧事件，明确应用时才收起。实际资产快速交互回归与原 Case demo 失败用例均通过 |
| F12 页签定位丢失 | 数据与管道往返把精确 run/action/occurrence 或固定版本丢掉，回到默认入口 | 原 projectTabs 每项目保存至多四个已校验定位，离开允许后保存，仍经原 openView 的取消/epoch/dispose；草稿继续由原 Store 持有。两项真实浏览器测试通过：精确运行、v1 第二行 Case、跨项目、迟到响应、参数草稿和真实 Store 容量拒绝离开 |
| F13 运行产物版本请求 | `table-versions` 对运行产物显式请求其他版本时仍返回 200 空列表，没有明确拒绝不支持的选择；没有发生历史数据改写 | 原 `run:` 分支增加明确 `invalid_request`，无版本参数的只读列表原样保持。V02 同时保留不存在版本、半个绑定、错误绑定、别名漂移、输出固定版本和旧定位兼容反例 |

关于 Qwen D3 的收窄：实测历史 v2→v1 确实发出了 table/sample 请求，并切到 v1 的不同字段和内容。原断言只等待约 4 秒，不能据此断言 viewEpoch 导致整个版本切换失效。F07 是独立复现的具体竞态，修复没有删除 stale/epoch 防护。

## 已获得的真实组合证据

- 管道：浏览器修改阈值后真正重新检查并运行，输出从 2 行变 1 行；旧 run 的 2 行固定结果保持。
- 统一执行与观测：真实 run 的 7 个节点在原 SQLite document 中带 scope；人为删除其中一行的 scope 后，读端不因当前存在 v3 seal 而提升它。测试只修改自己的 `/tmp` 反例数据。
- 多 action：`r8cad5413d818432eb3a4b6d0` / attempt 1，materialize action `2e24459beaf6f275`（8 节点/7 边）、write action `9c2e930584fc983a`（3 节点/2 边），结果 6 行。四个同 semantic 算子保留不同 occurrence，两份 plan/code pin 各自核对；A→B→A 切换不沿用错误选择，当前业务源码修改后仍读当时源码。
- Notebook：真实 trusted_local controller 与原生内核，固定 scores → Python publish/bind → 同 generation SQL → 显式 pin → 原生 source/page 3 行及柱图 → Python Case 描述 → 用户明确采用 → 保存 view/评测 → 从 UI 重开读回。4 次 execution succeeded，同一 session/generation；最终 session stopped，6 个 lease 全部 cleanup_complete。
- 评测：Case/方案/字段三个 scope 分别保存并重开；真实 409 保留草稿；丢回执核对不重复发送写请求。

Notebook 的临时 handle 被原协议拒绝作为固定 Case 来源，这不是生产缺陷；测试必须显式 pin。Case 接入要求用户选择“采用 Notebook 输出视图”，不能测试里直接改状态或让产品自动覆盖当前配置。

## 验证位置与未验范围

原验收中的失败逐项区分为实际生产缺陷、UI 合法迁移、测试自身调用错误，没有通过删除业务反例使其转绿。例如 V02 的别名漂移必须用原控制文档摘要执行 CAS；旧式 locator 验证应投影其绑定的 v2 字段，同时保留 v1 已删除字段必须拒绝的反例。

| 验收门 | 最终结果与实际覆盖 |
|---|---|
| V01 统一壳与响应式 | 1 passed；四 tab 与不同容器宽度 |
| V02 固定版本全链 | 1 passed；真实 v1/v2 字段、行、图片不同，样例/分布/媒体/分析导出/Case 同一版本；非法版本、半绑定、漂移及运行产物版本选择拒绝；旧式读响应保持 |
| V03 配置、检查、运行 | legacy/v3 2 passed；无效配置拒绝，合法配置真实运行改变输出，旧结果保持 |
| V04 封存图、身份与源码 | legacy/v3 2 passed；真实 action/occurrence、原桥与观测对应，封存时间精确、历史参数只读 |
| V05 Notebook→Case | 1 passed；真实 Python/SQL 同内核、固定结果与图表、保存和重开、内核及资源清理 |
| V06 多材料 Case | 1 passed；原生来源与材料、显示/布局语义 |
| V07 持久评测与异常 | 1 passed；Case/方案/字段 scope、真实 409、未知回执核对不重复提交 |
| V08 迟到响应与释放 | legacy/v3 2 passed；旧响应不覆盖新图，20 次数据/管道往返保留原运行，受测浏览器 URL/interval 不增长 |
| V09 单一呈现权威 | legacy/v3 2 passed；实际装配资产、同一图适配调用链，无第二份执行计划 |

验证采用分组运行及按失败点定向复跑，没有在同一进程把所有集合重新运行。后续改变的生产文件与复跑范围有记录；早期已通过且未受影响的用例不冒充在最终 commit 上重新运行。跨模块真实链以最新完整源码 manifest 为准。

核心证据：

- `/tmp/demiflow-ui-final-acceptance/final-verification.json`：最终真实 Notebook 数据库、1309 个生产文件摘要、原生 cleanup 记录。对应 source manifest SHA256 为 `8e35d0d4e1e6aabc0fb6d45b712609a3dac6d33b2fadbd6b3fa3a3c860e79b1f`；末次真实 V05 1 passed / 113.59 秒。
- `/tmp/demiflow-ui-final-acceptance/nav/result.xml` 与 `/tmp/demiflow-ui-final-acceptance/v07/result.xml`：实际资产导航 2 passed；真实评测 scope/409/未知回执 1 passed。最终 worker 两行拒绝分支后，这两项的相关生产字节未变，沿用原结果并明确记录。
- `/tmp/demiflow-v3-sealnav/results.xml`：真实 v3 V04/V08 两项通过，20 次切页后原运行仍在、受测浏览器 object URL 与 interval 计数未增长。
- `/tmp/demiflow-projection-freeze-final/results.xml`：Case 快速编辑、原失败 Case demo、真实 E2/F10 三项通过，无候选资产替换。
- `/tmp/demiflow-ui-v02-delivery/results.xml`：V02 最终全链 1 passed / 142.81 秒；此前生产缺陷和夹具错误保留在失败日志，未混计为通过。
- `/tmp/demiflow-ui-delivery/acceptance-index.json`：全部九门和14份旧回归的最终逐 nodeid 汇总、原 XML 路径和 SHA256；内容已纳入本目录的机器可读验收清单。
- `/tmp/demiflow-ma-final/test_real_v3_multiaction_histo0/multiaction-history-evidence.json`：多 action、报告身份、选择切换与历史源码。
- `/tmp/demiflow-ui-native-scope/test_real_v3_workbench_persist0/scope-proof.json`：原生 scope 落库及旧行不提升。
- `/tmp/demiflow-evidence-legacy-green`：真实 Local 资源生命周期与旧释放账本兼容，共 26 项通过。
- `/tmp/demiflow-final-core-review`：取消/超时、固定结果恢复、Runtime 原生接线；阶段程序加载旧夹具另行迁移，禁止 worker 打开 driver journal 的断言保留。

Ray Core、多机、物理 GPU 和性能本轮未验；不能借 Local 或 DOM 单测算通过。旧回归四个 skip 分别是两项缺少真实 pre-codec JS 的旧客户端拒绝、缺少冻结 baseline/proposal 的历史 UI 配对比较、需要兄弟 P2 工作树及 inode > 2**63-1 文件系统的场景；均为 not_run。当前客户端兼容旧 Case/字段草稿仍实际验证。metadata helper 替身只用于 Case/媒体固定读取的局部测试，不冒充真实内核 V05。

14 份旧回归文件按 nodeid 合并最后有效结果：82 passed、4 not_run、0 failed；其中两条缺旧客户端的测试是独立新增的显式未验场景，不把旧客户端未知状态算通过。新增针对性回归、原生 W/封存/资源与核心接线测试另列证据，没有与九门或旧回归重复加总成一个虚高总数。

交付代码可从本分支 Git 记录核对。最终提交与真实内核使用的 1309 个生产文件逐一比对，结果写入 `/tmp/demiflow-ui-delivery/commit-verification.json`；commit 自身不能在其文件内容中引用自己的 hash，因此机器清单保留实测 manifest 和当时的 HEAD/dirty 事实，不将旧 HEAD 改写成已在新提交上运行。

## 隔离边界与操作记录

七份冻结文件保持基线字节。测试用明确隔离树 PYTHONPATH、指定解释器和私有端口；进程内核对导入来源。原候选 dirty、主仓、在线服务和生产数据不因修复而修改。

一次旧回归命令遗漏 `FORGE_WORKBENCH_EVIDENCE`，旧 harness 使用默认历史验收目录，覆盖了 `P1/browser-failure-responses.json` 和 `P1/screenshots/failure-current-browser.png` 两个验收文件。发现后停止该轮，后续命令显式设置所有 evidence 目录为 `/tmp`。未掌握覆盖前字节，未用猜测内容“恢复”。具体路径/摘要/时间见 `/tmp/demiflow-ui-reg12/evidence-path-incident.json`。这两个是旧验收产物；不把该轮计入最终通过，也不宣称本轮所有写入都严格留在 `/tmp`。
