# 继续实施记录：真实 Dataset 主链路

后续原生 Prompt/Embedding、真实 Ray 模型加排序组合与效果事务进展见 [NATIVE-PROGRESS.md](NATIVE-PROGRESS.md)。下文保留本阶段证据与当时待办；两者都不是最终全部验收。

本记录替代旧 REVIEW.md 中“公开 Dataset 仍完全不能运行”的当前状态判断；不撤回它记录的基线缺陷，也不宣布所有问题关闭。工作仍在 `engine/gpt-runtime-v3-review-20261115` 隔离分支进行，主仓保持 `e40a6dd3dd756aba423dca4c7d995ada0a4bff65` 与原 11 处文档改动，未 push、未合入、未调整生产服务。

两大 root 不变：产品应让用户看懂、配置、运行真实 pipeline，并分析、比较、评测数据；平台应提供正确、稳定、高性能、通用且架构清楚的统一数据流引擎。局部测试数不折算为完成百分比。

## 本轮已实现并实跑

- Local 与 Ray Core 使用同一 worker 图执行循环；严格按封存 occurrence、端口、物理通道和分片执行，driver 不运行 UDF。大数据由 worker 数据通道传输，控制消息只携带有界定位信息。
- 输入、共享前缀、分支、map/filter/bound-map、四种 map_batches 格式、异步生命周期、全局排序、不同键 join、union、结果订阅、物化、聚合/元数据及 Lance 原生提交贯通。
- JSON/JSONL 返回业务行，显式 records 扫描保留其来源包装；文件分片不再从自己起点一直重复读到最后一个文件；Lance 非零 fragment 不再丢首行。
- checkpoint 按封存行数/时间策略触发；先停止新输入，再排空交接、收齐全部 worker 状态和结果前缀，由唯一 driver 提交。中间 checkpoint 只发布封存前缀，最终 checkpoint 才发布原生 Lance 目标，避免重复追加或重复 create。
- 部分恢复复用原计划与代码依据，恢复输入 cursor、路由状态、缓冲、已结束节点/输入/分片、算子私有 JSON 状态和结果前缀。恢复后的 take_all 返回完整结果；不会只返回后缀。
- 运行资源型 actor 可在本 attempt 使用；没有可恢复状态适配的镜像明确标记 nonrecoverable。恢复时在启动任何参与者之前拒绝该镜像，避免静默丢状态或重做业务调用。最终已提交结果可直接读取。
- async actor 的 astart、调用、aclose 共用持久事件循环；宿主清理先等待生命周期结束，再确认物理资源退出。取消/关闭幂等，物理退出仍由原资源权威确认。
- `run_stream` 公开入口转入同一 Dataset execution；业务运行在 worker，进度和收尾钩子留在 driver，使用真实节点计数和最终提交行数。
- logical limit 能关闭不再需要的上游输入；共享源只有在所有消费路径都不再需要时才停止。停止事实进入 worker frontier，保留原始 source cursor 的真实 EOF 标记，不伪造读完输入。
- 工作台读取真实封存计划与 report identity bridge，并使用实际 worker 节点行数。相同 semantic 节点的不同 occurrence 使用不同显示键。

## 必须记录的契约迭代

1. **数据多重性与传输去重**：原 v3 `delivery_id=(record_id,edge,reader)` 在共享前缀经两个 union 输入再次汇合时，把第二条合法业务记录误判成重复传输。保留 `Record.record_id` 和来源不变；通道路由 delivery identity 加入其可恢复 ordinal，worker 传输再绑定 producer partition。route epoch/ordinal 仍由唯一 RoutingState 快照恢复。原 K007 的业务身份和多重性断言保留，增加真实共享分支链路证明。旧 task package 中省略 ordinal 的表述不能再作为此实现的验收公式；承重七件未改。
2. **最终 scope 与原始源 EOF**：limit 关闭的输入不是原始源 EOF。driver 先核对 consumer partition/edge，并沿封存图证明源的全部输出路径都已满足，checkpoint 才接受该输入的逻辑完成。最终原生发布可以完成；原始 cursor 仍为未读尽，结果 completion 诚实为 terminal_satisfied。
3. **运行状态比工厂声明更具体**：类工厂不在 driver 实例化。worker 对真实实例使用现有 operator_policy 分类，private 字段也纳入 auto_json；非 JSON 资源不会被静默忽略。nonrecoverable 状态镜像拒绝部分恢复，不能把 factory 豁免等同于可恢复能力。
4. **测试边界**：U 门原本是公开 API 委派单元测试。集成后原“若模块不存在才安装 fake”不再生效，却仍断言 fake 的结果与调用计数。本轮改为每个测试内用 monkeypatch 限定三个调用边界，保留真实 DTO/模块；不在 sys.modules 全局替换模块。它不证明实际执行，真实执行由独立公开 Dataset 测试覆盖。M 的 WorkerAssignment fixture 使用真实完整契约；JSON 回复在 RPC 边界解冻，业务 list 不泄漏为内部 tuple。

## 中间验收证据

这些是当前实现推进证据，**不是最终全部验收**。精确计数、nodeid 与 JUnit 摘要见 CORE-PROGRESS-EVIDENCE.json；最终交付仍需对最终提交运行前后摘要核验。

- 原 33 项新增失败：33 passed，`/tmp/demiflow-33-current.log`、`.xml`。未删除、跳过或放宽原负例。
- 公开 Dataset + 真实接缝 + M/U 单元组合：137 passed，`/tmp/demiflow-v3-core-current.log`、`.xml`。其中 25 项公开 Dataset、30 项真实接缝、27 项 M、55 项 U。随后仅删除了无调用者的旧 worker 数据消费/cut 私有路径；最终验收需绑定最终内容。
- Ray 2.60.0：独立六 CPU 单机 Core 集群真实 map→take→acked native commit，`/tmp/demiflow-ray-public-v3-current.log`；只证明单机 Core，不能冒充跨宿主或 GPU 验收。该测试集群已自行 shutdown。
- 更早的真实 workbench prepare/start/show 链路已通过并锁定两行结果、桥完整覆盖、每个报告节点可见、实际 worker 计数；本轮当前版本再次通过（1 passed / 67.24s），证据 `/tmp/demiflow-v3-workbench-current.log`、`.xml`。

## 仍须继续，不得借这些通过项宣布完成

- 原生 Prompt/Embedding/Agent、cache、采集与 queue 的完整公开链路及未知效果恢复；尤其构造、native journal 绑定、请求预算与状态必须组成同一实际链路。已改声明名称预检不等于本项关闭。
- `_EffectLedger` 的独立文件事实、预算原子性/幂等与原生 journal 合并；artifact staging 的累计预留、实际字节结算和失败/重复写回收。
- 真实 CPU/GPU/远程 host 放置、宿主本地读取域证明、资源 pending 重试/续租、跨 run 有界容量；单机 Ray 通过不能替代。
- 更完整的输入格式、batch 生成器、长期增量输入、Arrow/转换峰值及磁盘预算；大批次与源固定版本反例。
- 真实进程崩溃/unknown 效果矩阵；目前受控故障测试不能替代所有 SIGKILL 场景。
- 尚未迁移清楚的其他 lane 单元契约、历史测试和新失败；最近一次全 559 项诊断为 257 passed / 302 failed（当时代码），不能用后来的局部通过推断全门已绿。
- 工作台浏览器、Notebook Python/SQL、媒体/Case/标注、开发对话、历史保护的最终组合验收。

主仓与承重七件保持不动；继续逐项实现和验收，不停在 Review。
