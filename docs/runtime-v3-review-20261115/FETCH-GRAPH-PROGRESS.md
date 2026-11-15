# 多输入原生采集与恢复

对应 I01/I05/I07/I09：普通 fetch 依据实际封存图的 occurrence 装配，覆盖嵌套 union、共享上游和连续两个 fetch；原 RawFetch、RunJournal、CommitCoordinator 仍为唯一采集/提交权威。每个独立 occurrence 拷贝未启动声明，不改调用者 actor；声明匹配包含并发/队列参数，不能因业务身份相同而借用另一声明的资源配置。

一个现有 NativeAcquisitionService 承载所有普通 fetch，共用有界 IO 准入。worker 的 acquire 命令可携带 node_id，仅 raw_fetch 模式允许；DTO、wire codec 与 driver 的实际 worker occurrence 所有权检查同时接通。queue/producer 的原交付与 ACK 协议保持。原单个顶层 fetch 保留 action-node-index 原生作用域命名，多节点使用实际 occurrence。

封存前置绑定记录在原 journal。第一份 Dataset cut 尚未产生而 native raw 已 ACK 的窗口，恢复核对原绑定后重新走原 RawFetch 恢复，复用原对象和计费，不发重复 HTTP。不从文件存在推断提交成功；已有 Dataset cut 仍走原 coordinator。

验收：`/tmp/demiflow-fetch-graph-r2.xml` 30 passed / 2 failed，发现历史原生 raw 的 ACK→Dataset cut 间恢复未准入；已修。`/tmp/demiflow-fetch-graph-r3.xml` 9 passed / 2 skipped，包含全部 5 个新增图/原生回执反例、2 个原始 raw 结算故障窗口与 SQLite producer 零/多输出恢复；2 个 PostgreSQL 参数因未配置测试库跳过。前批其余 raw/search 用例已通过，最终集成继续统一复验。

未把同机或本地 HTTP 证明写成物理多机/付费服务验收；也未扩宽原 producer 允许的业务变换范围。整体交付仍在进行。
