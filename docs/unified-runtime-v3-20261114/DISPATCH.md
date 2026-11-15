# 发给 Qwen 的完整执行消息

请基于这份新任务包分发实施。这次目标是完整兑现原始统一数据流引擎意图，同时保证工作台看到、配置和控制的就是实际执行的计划。不是继续补一条旁路，也不是做出新模块后保留默认Ray Data执行。

**位置与基线**

- 设计树：`/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_runtime_contracts_v3_20261114`
- 设计分支：`docs/runtime-contracts-v3-20261114`。最终设计提交以该分支交付HEAD为准；先记录 `git rev-parse HEAD` 与 `evidence/final-verification.json` 的内容摘要。
- 代码基线：`267b9144c6bf7447c6822aa716f6d5c0eda16990`。
- 任务包：`docs/unified-runtime-v3-20261114/`。
- 主仓 `/yzp/zhaozy/yangzepeng/0905/demiflow` 禁止写入/测试/合并；在线任务和服务继续原样。

**必须保持的两大意图**

产品：统一真正可用的工作台，让用户看懂、配置、运行pipeline，分析、比较、评测数据。引擎：正确、稳定、高性能、通用、架构干净的统一数据计算平台，完整支撑实际业务。展开为I01–I14见INTENT.md及原文会话链接；任何删能力/放宽guard/削弱反例的“完成”不接受。

具体交付：同一Dataset组合在Local/Ray Core执行；CPU/GPU/API/IO按语义独立调度；增量来源、共享前缀、多输入多输出、三种exchange、有限背压、关系/排序/聚合、原生模型/缓存/队列、统一cut/效果/提交恢复、多机工件/读域、默认公开入口迁移、Ray Data退出、真实工作台接线、同预算性能证据。用户不需要手工materialize补调度缺口。既有queue原子结算与历史安全约束不能放宽。

**派发方式**

按LANES.md开19个实现会话：C/G/P/S/K/X/B/E/D/T/M/H/Q/U/L/Y/F/W/V。每lane一个独立worktree/branch，从设计交付commit起步，文件零交叉。每文件一个装配owner，下面有逐函数/方法/数据声明卡；不要开数百个会话。每张卡带签名、字段合同、行为、反例、调用/消费边、测试nodeid与基线证据。

所有lane现在可以对冻结契约同时编码，不等其他lane的实现才能下笔。生产代码不得加ImportError fallback或临时stub。消费者测试允许严格按契约的测试替身；最终完整组合只能跑真实实现。类型/字段/错误/序列以CONTRACTS.md、interfaces.pyi、function-signatures.pyi、IDENTITY.md为准，状态owner见OBJECT-OWNERS.md。新helper也要登记符号和调用边，不得暗中跨文件兜底。

过程中不做逐步Review、不频繁发零碎纠偏；各lane自主完成整个范围。仅在反例证明合同本身不成立时提交一个最小裁决请求，包含影响producer/consumer；你集中更新契约、任务和样例，无关lane继续。回报明确改道当前活的Qwen总控，不发到已停止旧会话。不要把共享重测试资源的排队当编码依赖。

**最终Review与修复**

全部lane交commit/证据后，在新integration树集成，GPT与Qwen基于同一源码快照各自完整Review，GPT统一修复并完整验收。合并次序有依赖，开工没有。不要中途合主仓，不push、不重启服务、不付费模型、不写真实业务表/队列。测试只写/tmp/demiflow-*。

验收按ACCEPTANCE.md与gates.json：完整公开API、30个真实组合场景、577个基线测试文件及集成版新增用例；路径断言要在进程内，必须PYTHONPATH=<本树> python -m pytest。裸pytest可能测主仓，不接受缩写命令。原7个cache/materialize真缺陷必须修；原37个旧契约预期逐条映射新断言，不能批量deselect。

证据guard拒绝skip/xfail当pass、重复/重试遮盖失败、未收集required、错误导入树、测试前后源码漂移。GPU、两host、PostgreSQL和浏览器缺环境记not_run，不能冒充通过。单机两actor不算多机。图必须绑定真实封存计划，不能只测试新模块存在。

每lane回执包含：session_id、worktree、branch、HEAD/commits、卡片状态、修改文件、完整命令/退出码/逐nodeid结果、进程内导入路径、源码前后摘要、已验证/未验证、契约差异、清理证明和当前接收总控。整体仍按M1全入口统一、M2完整数据流、M3模型与恢复、M4多机性能交付；任务计数不是意图完成百分比。

GPT已经做了10轮设计复审并保存逐轮输入摘要和修改记录。这是一个设计交付，不是589项实现已经通过，也不是“最终100%已证明”。最后必须用真实源码、真实组合、真实环境证据关闭原始意图。
