# 工作台对齐 D1.9：源码复核与可独立编码设计

设计版本：UI-D1.9/2，2026-11-15（原对比与源码证据日期2026-11-14）。实现基线：`267b9144c6bf7447c6822aa716f6d5c0eda16990`，分支 `engine/gpt-integration-review-20261114`，工作树 `/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_integration_review_20261114`。

本文保留设计阶段的基线与任务契约。后续实现、独立复核、集中修复和真实组合验收见 [2026-11-15 集成复核记录](FINAL-INTEGRATION-REVIEW-20261115.md)。不要把设计检查或原样稿截图计为产品验收；在线旧服务也不等于该记录中的隔离候选。

## 1. 判断

用户说“前端与意图 demo 差很多”有实据。四 tab、算子图、参数修改、运行管理、Notebook、Case 的骨架已经存在；最大的差距在**信息层级、默认呈现、操作连续性**，另有固定历史版本继续分析、Notebook 表结果便捷图表等能力缺口。不能只换 CSS，也不需要重建计算、运行记录或分析平台。

完成后的典型体验：选项目和流程，首先看得懂节点的输入、处理、参数、输出；检查问题能回到对应节点处理；点击运行后在同一图上看该次运行及结果。进入数据页，选定版本，查看样例与字段分布，直接写 Python/SQL；固定输出可进入 Case。Case 默认突出“选哪条、看什么、评什么”，多方案、多材料和标注范围不混淆，保存失败不丢答案、不跳下一条。

## 2. 原始意图及范围

产品 root：统一、真正可用的工作台，让用户看懂、配置、运行 pipeline，分析、比较、评测数据。平台 root：正确、稳定、高性能、通用、架构干净，支撑真实业务；本批依赖引擎事实，不能通过前端制造计划、观测或恢复保证。

| 意图 | 本批交付与边界 |
|---|---|
| R1.1 四 tab；R1.2 D1.9；R1.3 紧凑上下文 | 保留开发/管道/数据/Readme；统一壳与响应式；常用动作和对象在同一上下文 |
| R2.1 图/输入输出/配置/源码 | 可读节点卡、结构化 inspector、绑定源码，局部/未知明确显示 |
| R2.2 配置→检查→修复→提交 | 保存冲突保留草稿；运行依据过期时引导检查；不把源码扫描当执行计划 |
| R2.3 run/attempt 图/监控/停止/恢复/结果 | 一张主图、精确身份；历史只读；支持范围来自服务端；日志仍属 run/attempt |
| R3.1 目录/版本/字段/分布 | 紧凑目录；可读已登记历史版本；抽样统计诚实标覆盖 |
| R3.2 Python/SQL Notebook | 同一真实内核；合并合法的保存/启动/提交步骤；取消、恢复、历史及 CAS 保持 |
| R3.3 通用输出 | 复用现有格式和媒体权威；表/JSON/小型图表；材料主导，来源详情按需展开 |
| R4.1 选样分组筛选 | 快速条件＋高级编辑；分页及标注状态仍由服务端计算 |
| R4.2 多方案多材料 | 复用 v2 output_groups；不把一个字段假装成一个模型方案 |
| R4.3 配置与评测持久化 | Case/方案/字段范围明确，保存下一条、草稿、历史、冲突与回执保持 |
| R5.1 清晰边界；R5.2 有界真实验收；R5.3 历史保护 | 不引入新状态仓/引擎/SDK；每文件唯一写入方；异常、迟到响应与资源释放纳入验收 |

不在本批扩大：通用拖拽编排器、浏览器 Python/SQL 引擎、独立监控中心、实时全表统计、自动 AI 生成业务语义、全新前端框架、生产部署和主仓合并。开发对话/Readme 保留，做受壳影响的回归，不重写其领域。

## 3. 阅读顺序

1. [实际差距与截图](01-comparison.md)：先看用户会遇到什么问题，以及证据的限制。
2. [页面行为与接口契约](02-contracts.md)、[类型定义](contracts.d.ts)：唯一冻结输入输出；函数任务不能自行扩字段。
3. [任务索引](03-tasks.md)及 `tasks/`：每项一函数/方法，少数显式装配或 CSS 任务；明确调用方和修改授权。
4. [验收与集成](04-acceptance.md)：端到端用户路径、反例、命令及未完成项判定。
5. [Runtime v3 接线修订](05-runtime-v3-wiring.md)：新旧计划、实例端口、观测身份和与平台W的共享文件交接。机器台账保留设计期要求，后续真实提供方与消费者的组合证据见集成复核记录。
6. [机器清单](tasks.json)、[设计检查器](check_design.py)：检查文件归属、调用方有权修改、任务与验收闭合；不代替运行验收。

## 4. 七路工作：六路实现，一路验收

共95项任务、30个主写文件（另列旧回归测试的必要兼容范围）。包括函数/方法/类型、6项样式、1项入口装配、9个精确新验收函数；不是把样式任务伪称函数卡。

| 路 | 交付 | 唯一主写范围 | 冻结依赖 |
|---|---|---|---|
| A | 公共壳、目录、版本选择、字段分布、跨模块装配 | app.js、style.css、index.html、view.js | B/D/E 保持 mount 入口；F 固定版本读取；D 图表 |
| B | 图中心的配置/检查/运行/结果体验 | workbench.js、workbench.css、workbench-state.js | C 的只读呈现模型与图 API；既有 commands/observer |
| C | 可读图和事实呈现 | workbench-graph.js、新 workbench-presentation.js、新 workbench-graph.css | 旧 graph JSON＋v3精确实例投影，C09唯一纯适配；不读文件、不编译 DAG |
| D | Notebook、通用输出和轻量图表 | analysis.js/css、analysis-output.js/css、analysis-render.js、新 analysis-charts.js | A 数据来源；原生分析 API、E 接收固定输出 |
| E | 紧凑 Case、配置及评测 | analysis-case.js/css | D field 渲染器；既有 Store/transport/records；F 版本来源 |
| F | 已登记历史版本的只读贯通 | API/worker/工作区与分析 source 的窄改动，新 catalog_versions.py | 复用 Catalog、preview_binding、native reader，不动引擎 |
| V | 用户旅程与边界验收 | 新增本批专属 tests 和截图夹具 | 所有已冻结接口；实现未集成时可先写测试，但不能用假端点宣布通过 |

本UI包内每个文件只属于一条路；跨平台包有两处明确重叠，按C8先接收W交付再由B/C迁移，不声称全局零冲突。同一路的函数任务可细分给 agent，但必须各自分支提交，由该路一次合入。同一共享文件不让多个 agent 同时覆写。所有路从同一基线编码；不等别人实现才下笔。生产函数签名、状态和样本见契约；测试替身只验证消费者，最终必须换成真实提供者。

**三个交付里程碑，不是串行编码阶段**：M1 看懂并跑通真实管道（A/B/C）；M2 固定版本→Notebook→Case→评测（A/D/E/F）；M3 两条完整用户旅程、历史与异常验收（V＋总控）。六路同时开发，集成顺序仅为方便定位：公共脚本和类型→各模块→装配→真实验收。过程中只处理必要契约冲突，完成整批后统一 review/修改。

## 5. 执行边界

- 主仓 `/yzp/zhaozy/yangzepeng/0905/demiflow`、在线服务、运行训练、现有 dirty/staging 不动；不 push、不合主仓。
- 承重七件不改：`execution/raw_fetch.py`、`queue_execution.py`、`queue_sinks.py`、`queue_source.py`、`execution/contracts/exchange.py`、`planning/dag.py`、`resources/contracts.py`。
- 私有夹具/服务/输出在 `/tmp/demiflow-*`；端口由系统分配，销毁自身服务。显式 PYTHONPATH，并在进程内断言 import 来自验收树。
- `ForgeWorkbenchState`、`ForgeWorkbenchCommands`、`ForgeWorkbenchObserver`、`ForgeAnalysisState.Store`、现有 Records/RunJournal 保持各自权威；展示模型是临时派生值，不持久化第二份账。
- 原 API、历史 URL、Case v1/v2、Notebook revision/generation、原 source digest 保持兼容。新字段只加在本设计声明的窄接口。
- 不从单测数、截图数量、已完成任务卡推导“产品/引擎 100%”。全部验收通过可声明“本设计范围完成”，仍须披露真实服务、权限及未验环境。
