# 原生业务、提交恢复与工作台接线复验

本批继续在 `engine/gpt-runtime-v3-review-20261115` 隔离分支修复。产品 root 是读懂、配置、运行真实 pipeline 并分析/比较/评测数据；平台 root 是统一、正确、稳定、有界、高性能且可覆盖实际业务的数据计算平台。本记录不代表整个升级已完成。

## 已落地的链路

- HTTP Agent → 原生文档工具 → 多轮模型响应 → Dataset 结果 → 原 RunJournal ACK。Codex 原生工具会话复用同一个请求 journal，节点/全局预算与请求意图原子记账，跨线程保持输入 occurrence。
- 已知 Codex 响应在首次 Dataset cut 前中断后复用；未知会话拒绝重发；节点预算不会被更大的全局预算绕过。测试使用本地协议进程，不调用外部模型。
- 图片模型 → driver 原生 ObjectSink → 原 ObjectRowsSink → v3 行结果 → Lance。worker 只发送有界编码字节和 ArtifactRef，不获得 RunContext 或提交权。生成、编辑、读取对象、早停、迭代关闭和后续写表共享原生对象/行提交协议。
- 原生文档算子显式区分资源与语义状态，避免资源自引用破坏封存；字符/文本 token 计数器有稳定的声明身份。
- worker 取得所分配 stage 的封存程序，以及有界、按内容摘要固定的业务模块和父包源文件。命名空间包不会误导入另一项目的同名 package；缓存按 allocation 隔离并由 worker 清理。
- 工作台 attach 在真实重试 attempt 上携带恢复状态；v3 按 action 的既有历史决定是否恢复。一个恢复 run 中后续首次执行的新 action 正常启动，不被误当成历史 action。
- write/create 在业务启动前保留真实目标缺席事实；准入不授予覆盖权限，原生 sink 在实际发布前仍核验冲突。恢复复用原封存计划、源绑定及代码身份。
- 工作台能只读检查 v3 NativeTerminalSink 的真实 Lance 回执，包括对象/行组合 sink。检查不产生 journal 事件、commit 或假恢复状态。

## 验收证据

所有测试显式绑定本工作树 Python 导入路径，数据和容量域位于 `/tmp/demiflow-*`。Ray 只启动本任务私有 Ray Core 集群并关闭；测试禁止调用 Ray Data。

| 证据 | 结论 |
| --- | --- |
| `/tmp/demiflow-recovery-r11.xml` | 40 passed、3 failed、1 error、1 deselected。3 个失败定位到恢复 run 中新 action 被误当成历史 action；error 为取消夹具早于真实封存的竞态。均未忽略。 |
| `/tmp/demiflow-native-r12.xml` | 38 passed。覆盖上述 3 失败和取消竞态修复、图片提交全部 5 个进程崩溃窗口、Codex 已知/未知响应和节点预算、M 组协议。 |
| `/tmp/demiflow-original33-r13.xml` | 原 33 项 + 公开 Dataset + D + artifact accounting：80 passed、1 failed。失败为 T003 的 FakeExecution 缺少 satisfy_terminal，已按真实协议迁移，原真实早停/对象保留断言保留。 |
| `/tmp/demiflow-original33-r15.xml` | 47 passed。按原 nodeid 执行全部 33 项，并包含完整 T 组；有重合项只统计一次。早停测试要求提交前缀后关闭，预算失败仍要求取消且不得提交。 |
| `/tmp/demiflow-ray-r13.xml` | 2 passed。同机真实 Ray Core Prompt→sort→结果，以及 enqueue→queue fetch→原生结果提交/ACK→历史。 |

写表测试保留目标冲突、模式拒绝、六个准入身份字段、源代码变化、封存损坏、禁止重新封存、发布后崩溃不重复发布的断言。旧 fault point 已迁到新引擎封存后/原生准入前的相同语义边界。工作台取消测试等待真实 seal 和缺席事实再取消；已保留的缺席事实是历史证据，检查后续外来目标时不能删除它或把它解释为覆盖许可。

## 尚在处理

原生模型在途输入的 checkpoint、分期驻留、崩溃 orphan staging 的安全回收、其余 lane 旧 fixture 与实际契约的接线、全产品回归仍继续。物理多机/GPU、完整性能不能由同机 Ray 或测试数量替代。历史 `known_inflight` 用例尚未通过，不计作已验收。
