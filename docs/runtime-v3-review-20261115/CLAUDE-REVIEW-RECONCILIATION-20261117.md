# Claude 意图审查：独立复核与范围纠正（2026-11-17）

结论：接受报告 §10 的核心发现——D1.9/2 新前端与最新引擎尚未组成同一个交付候选。这是产品原始意图的实质缺口。本会话此前以旧前端 18 个实际场景支持“核心架构与功能收口”，遗漏了新 demo 与引擎的最终组合，现明确纠正。引擎已有的真实 Local/Ray/GPU、提交恢复和业务证据仍按各自版本成立，不因产品组合缺失全部作废。

产品 root 仍是看懂、配置、运行实际 pipeline，分析、比较、评测数据，保留 D1.9/2、开发、Notebook、Case、媒体与历史。平台 root 仍是统一 Dataset 与运行时、Local/Ray Core 承载、有限资源、原提交恢复和唯一权威。性能按真人要求后延；本轮不重新引入该验收前置。

## 报告来源与核验方式

- 原报告：`sgvps:/home/claude/work/reports/demiflow-intent-alignment-review-20261117.md`，35,442 字节、265 行。
- SHA-256：`69a8c781359a707e252f9be1ec09d2169b988311063b8da4cce67fcd31c072ee`。
- 经用户指定的 VPS，使用现有代理和新配置的专用 SSH 密钥读取全文；没有用 scp/ssh-copy 复制报告或代码。本文是本地独立裁决，不改写 Claude 原文。
- 报告 §10 修订了前文多项判断，按最终修订结论核查；不能把作者已经撤回的问题继续算开放缺陷。
- 本次做 Git 字节、分支包含、模块存在与源码读取核验；未运行新的业务测试、浏览器、服务或合并操作。旧测试证据与本次只读核验分别列明。

## 已核实的候选分裂

| 项目 | 最新引擎候选 | 新前端候选 |
| --- | --- | --- |
| 工作树 | `/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_runtime_v3_review_20261115` | `/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_ui_review_20261115` |
| 分支 | `engine/gpt-runtime-v3-review-20261115` | `ui/gpt-d19-final-fixes-20261115` |
| 本次核验 HEAD | `4dc50e7205aefe97593c87b8b009ea81ff764bf4` | `3fe56f12be0cbd97c71c20c8322dd432845159cf` |
| 相对共同祖先独有提交 | 753 | 28 |
| `workbench-presentation.js` | 不存在 | 存在，857 行 |
| `analysis-charts.js` | 不存在 | 存在，204 行 |
| `app.js` | 1,113 行 | 1,444 行 |

共同祖先：`49b666fbff1257770b3ee3a3a61af4efc13146dc`。行数仅用于定位不同版本，不能当功能质量评分。源码实际差异及缺少模块证明两侧尚未组合；不同 Git 分支本身不等于存在第二套运行时账本。

本轮引擎最终生产提交为 `08335c8ea3bf4210763c07f8aee1266f1b41e72c`，最终测试候选为 `9e614afa6f25885e7d3a090f94213fc6cc26b2ef`；`4dc50e7` 仅追加文档。后续本文的文档提交不改变生产基线。

两侧同时改动的代码/测试文件共有 15 个：

```
demiflow/execution/checked.py
demiflow/execution/dataflow.py
demiflow/execution/dataflow_history.py
demiflow/execution/dataflow_seal.py
demiflow/execution/run_journal.py
demiflow/resources/local.py
demiflow/resources/process_unit.py
demiflow/workbench/api.py
demiflow/workbench/web/workbench.js
tests/test_forge_analysis_case_groups_ui_browser.py
tests/test_forge_analysis_case_ui_browser.py
tests/test_forge_workbench_browser.py
tests/test_runtime_v3_real_seams.py
tests/test_runtime_v3_w.py
tests/test_workbench_execution_binding_browser.py
```

这只是共同改动范围，不是文本冲突数，也不是全部合流文件清单。尚未试合并，不假称已解决冲突。机械覆盖这些文件可能丢掉最新恢复/资源修复，或丢掉 UI 的真实来源 scope、封存时间和历史导航修复。

## 对 Claude 结论的处置

| 结论 | 独立裁决与当前状态 |
| --- | --- |
| M1′：新前端 × 最新引擎未合流 | 接受，产品交付阻塞仍开放。旧前端 18 项不能替代 D1.9/2 验收。 |
| M2/M3/M5、M4 的真正竞态 | 按 §10 不重复派修。新前端源码、资产注册和其最终复核文档已有相应修复；本次未复跑其旧测试，不把“存在修复”写成“最新组合通过”。 |
| M6：证据版本不代表最新组合 | 接受，纳入 M1′。两侧各自的证据不能拼成同一版本验收。 |
| M7：shuffle 仍在修、引擎没有固定候选 | 该描述已过时。seed shuffle 和函数缓存最终修复已完成并有独立证据；生产固定在 `08335c8`。合流时仍须绑定同一候选，但不重开已修 shuffle。 |
| M8：协调视图过期 | 接受。本会话承担最终合流与验收，已纠正 CURRENT-STATUS、架构复核及证据账本的当前声明。外部 ENTRY 和 Claude 原报告保持原字节；此处明确当前两侧候选。 |
| 未合入叶分支、同名/异名提交残差 | 尚不能据提交主题推断丢失修复；若影响最终合流按实际代码语义核对，不先清理或删除历史树。 |
| 性能、外围清理、未运行环境 | 继续按真人取舍保留。缺少 UI Ray 选择入口已明示；不为本次合流增加第二执行入口或额外服务。 |

## 必须如何闭环

1. 以已冻结引擎为基础，在隔离候选精确合入 `3fe56f1` 的产品改动。逐项保留真实 producer scope、历史封存时间、资产清单、版本切换、Case/Notebook 交互，以及最新原生资源、缓存状态和恢复保护。不得从旧 UI 树整文件覆盖最新引擎，也不新增运行时账本。
2. 在同一冻结候选从浏览器完成 pipeline 配置/检查/真实运行/结果固定，再改阈值重跑并验证历史结果保持；记录实际 v3 action、封存图、身份桥和结果来源。
3. 在同一候选完成真实 Python/Jupyter → SQL → Case 比较/评测 → 保存重开旅程，验证四 tab 上下文、固定表版本、开发/媒体能力、窄宽窗口以及关闭清理。
4. 根据实际合流改动复跑原 UI 九门及受影响旧回归，检查真实生产路径和源码归属；引擎交叉文件触及恢复/资源时补对应原有 Local/Ray 组合。保留失败、not_run 和旧回执，不放宽断言。测试证据绑定最终源码；文档更新不冒充运行证据。
5. 组合闭环前，产品状态保持未完成。主仓、业务项目、原历史和服务保持；不 push、不合主仓、不部署。性能后延和 20 机取消保持，不借本次复核扩大任务。

## 当前已做与未做

已做：代理 SSH 与免密登录验证、报告全文读取、关键分支/源码差异复核、纠正当前完成度口径。未做：两分支合流、合流后的任何运行验收或发布。当前依据不支持重新给出“完整产品已交付”或“原始意图 100%”。
