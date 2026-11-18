# 项目算子库、多流程与单份业务参数：本轮交付

2026-11-18。实现提交 `2550579`，分支 `forge/project-flows-20261118`，基线 `794ef53`。

工作树：`/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_forge_project_flows_20261118`。

## 意图及完成范围

产品仍围绕“看懂、配置和运行 pipeline，分析、比较、评测数据”；平台仍要求统一、正确、稳定、通用且边界清楚。本次直接落实看懂项目结构和职责分离，不重开已验的数据、Notebook、Case、引擎或性能工作。

真人最新决定：沿 `operators + prompts + flows` 组织项目；多条流程独立接线；复用实际定义，不把调用节点当成库算子。不强迫业务抽成一个分派入口和多套参数。

- 每条流程只有一份业务参数契约和当前值，限接线所必需的输入。
- 算子默认值归代码算子；正文、模型声明及输出契约归 Prompt；引擎/部署参数不自动进入业务表单。
- 当前参数与历史运行快照分开，定义身份与每次调用身份分开。

已实现：新版 Forge 规范、开发登记与检查、历史兼容、产品设计。未实现：工作台算子库/参数编辑页面和业务项目迁移。现有 59866 服务未切换。

## 交付位置

- [规范 candidate.10](../demiflow/forge/development/standards/pipeline-v1.0.0-candidate.10.md)：项目拥有多个显式入口、共享代码/Prompt、四类参数归属、历史与迁移边界。
- [产品设计](forge-project-flows-product-20261118.md)：页面职责、单份参数、保存/解析/运行/恢复状态，以及后续产品验收条件。
- [快速开始](demiforge-quickstart.md)和[新登记示例](../examples/development/project-multiflow.json)。
- [验证记录](forge-project-flows-evidence-20261118.json)：提交与源码摘要、原始测试报告路径/摘要、保护核对及未验范围。

## 工具具体变化

`demiforge-project-v3` 的 `projects.<id>.flows` 登记多个具名 `{path, symbol}`；共同拥有 `operators`、`prompts`。项目无默认执行入口。开发任务用 `start --project-name <id>`，历史 `--pipeline` 别名仍有效。

检查覆盖所有具名入口、共享静态依赖、显式 Prompt 源和 Notebook；不执行候选代码来发现结构。跨流程入口调用、算子反向依赖入口仍拒绝；Notebook 可明确调用所属项目的多条流程。Prompt 改动使旧验收证据过期。

所有权、watch、readonly、符号链接、预算和版本绑定继续生效。旧九版规范原字节保留；旧 v1/v2 登记不自动升级。历史测试显式绑定原 candidate.9，未把旧夹具改造成新契约来掩盖兼容问题。

开发工具保持标准库实现。没有添加运行包装器、第二套调度、Prompt 执行器或全局算子注册中心。

## 验收

全部在本隔离树，`PYTHONPATH` 指向本树，Python 使用现有 `0905/env/bin/python`，测试私有目录为 `/tmp/demiflow-forge-v10`。

| 集合 | 结果 | 原始证据 |
|---|---|---|
| 九个开发/运行相关测试文件首轮 | 160 passed、78 subtests passed；1 条浏览器测试因未提供浏览器路径失败 | `core.log / core.xml` |
| 浏览器复跑 | 提供路径后发现旧“运行记录”导航不适用；改为现有顶层“运行监控”，保留全部运行/取消/会话/窄屏断言 | `browser.log / browser.xml` |
| 修订后首次复跑 | 编辑规范恰好与平台检查重叠，检查正确拒绝 `source_changed`；不计通过 | `browser-v2.log / browser-v2.xml` |
| 源码稳定后的浏览器复跑 | 1 passed，83.13s；开发会话→运行记录→真实停止→返回原开发会话，1440/390/320 宽度 | `browser-v3.log / browser-v3.xml` |
| 最终规范文本与新契约 | 32 passed、12 subtests passed；多流程与历史迁移两文件 | `final-contract.log / final-contract.xml` |
| 最终离线 wheel | 1 passed，6.95s；独立解包、v3 工具生命周期、旧九版摘要、53 项历史回归及静态覆盖回归 | `wheel-final-test.log / wheel-final-test.xml` |
| 交互设计稿 | 8 项交互/适配检查；4 种宽度；0 JS 异常，人工查看图、库、窄屏参数截图 | `mockup-check.json` 与 `mockup-*.png` |

首轮九文件：`test_forge_development.py`、`workspace`、`project_local`、`source_coverage`、`snapshot_links`、`migration`、`runtime`、`public`、`multiflow`（后八项均为 `test_forge_development_*.py`）。没有把重跑、子测试和 wheel 内部回归相加成一个夸大的独立用例总数。

只修改浏览器测试的导航动作，未删掉、跳过或减弱原断言。无全平台全量回归声明。无真实模型调用、生产作业、业务数据迁移或新产品页面上线声明。

## 交互设计稿

会话内示意保存在：

`/yzp/zhaozy/yangzepeng/0905/.codex-home/visualizations/2026/11/14/01a1da21-f648-7522-9324-760ac1c7c9af/demiflow-operators-flows.html`

可切换算子库/流程、查看定义与使用位置、改一条流程的当前参数、核对共享算子默认值不变。参数保存后旧示例图失效；解析只更新示意，不执行实际任务。浏览器测试及截图是这个示意的证据，不是新工作台功能的验收。

## 后续产品实施需要一起完成

1. `workbench/projects/discovery.py` 目前只识别 v1/v2；扩展 v3 只读发现与项目定义目录。
2. 真实定义与调用节点分别建视图，沿现有材料读取建立引用，不复制代码/Prompt。
3. 每流程一份参数契约/当前值，复用已有持久化、权限、revision 冲突与历史封存。
4. 管道/算子库/运行监控完整接线，再按产品设计第 7 节验收；仅移除参数文件下拉框不算完成。
5. 业务项目逐个显式采用新规范并同步其 `AGENTS`/`PIPELINE_SPEC`；不静默覆盖历史。标杆案例两处内联图像 Prompt 的提取属于后续业务迁移，本轮未修改。

## 保护核对

主仓 HEAD、原 dirty 清单和保护文档摘要均与已有保护快照一致；冻结七件和旧九版规范字节一致。原候选树 `794ef53` 干净，59866 继续监听。本轮没有 push、合入主仓、重启服务或修改业务项目。
