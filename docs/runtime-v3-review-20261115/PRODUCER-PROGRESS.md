# 原生队列生产、搜索与提交恢复接入

本记录是继续修复中的实施检查点，不是全量完成声明。两大 root 及 I01–I14 不变：产品展示、配置和运行真实计划；引擎使用同一 Dataset 数据流、有限资源和原生提交恢复权威。本批直接补 I07/I09/I13。

## 实际实现

- `NativeProducerService` 复用原 `QueueProducer` 固定源枚举与 `QueuePublishSink`/`CommitCoordinator`。纯转换在真实 worker graph 中执行，没有回到旧 `run_stream`。
- 一次只放行一个原输入单元，各阶段排空后确认原生 unit-end，再提交 Dataset cut。零输出单元仍推进；多输出每阶段保持原 1024 行/16 MiB 上限。
- 新 run 从原生 JSONL 字节位置/Lance 固定版本偏移接续。同一 run 恢复从已提交 Dataset 游标重放未确认单元，复用原生发布回执；调用身份包含原输入单元及展开路径，避免新 run 的局部计数器从零开始而串用旧回执。
- EOS 仅在所有输入单元已排空后进入原生提交。发布与搜索回复引用实际原生 commit/receipt，不生成另一份确认账本。
- 生产链中的 SearchWeb 经固定 RPC 回到原生搜索权威；已固定响应可复用，未知 HTTP 结果拒绝重发。搜索 actor 由同一个异步任务负责启动和关闭，符合 WebSession 的共享所有权，失败路径也关闭连接维护任务。
- HistoryReadLimit 跨 worker 保留有限 JSON `next_cursor`；原生停止时仍抛 QueueIncomplete，不 ACK。Dataset 的 execution_metadata 重新投影真实观测、已确认行数和队列摘要，沿用现有 API。
- 两个历史 fault injection 在 worker 程序封存后才安装到 driver 的 QueueResultSink，保留原有停止/不 ACK/租约续期/不堵住另一 claim 的全部断言。这样测试的是 driver 故障窗口，而不是故意修改 worker 的代码固定身份。

## 已执行证据

所有命令显式 `PYTHONPATH=$PWD:$PWD/tests`，使用本树 `env/bin/python -B -m pytest`，数据与临时服务均位于 `/tmp/demiflow-*`；没有连接生产 PostgreSQL，相关参数由 `-k 'not postgresql'` 明确排除。

| 证据 | 结果 |
| --- | --- |
| `/tmp/demiflow-producer-r2.xml` | 真实零输出/65 条展开发布，1 passed。 |
| `/tmp/demiflow-producer-r4.xml` | JSONL/Lance 恢复、独立队列预算及本地持久副作用保护，5 passed。 |
| `/tmp/demiflow-producer-r8.xml` | 89 passed，1 failed；通过项含固定搜索响应接续及 88 项 DTO/codec。失败定位为 UnresolvedCommit 跨进程类型丢失。 |
| `/tmp/demiflow-producer-r10.xml` | 修复异常传输与生命周期后，搜索固定/未知响应、同一 run 的 native unit/Dataset cut 两窗口，4 passed；无遗留连接维护任务告警。 |
| `/tmp/demiflow-queue-combined1.xml` | 五文件 SQLite 组合：43 passed、3 failed、46 deselected。 |
| `/tmp/demiflow-queue-combined2.xml` | 修复历史游标和 execution_metadata 后，前述 3 项中 2 passed；剩余项定位为 QueueIncomplete 类型被泛化。 |
| `/tmp/demiflow-queue-stop3.xml` | 保留 QueueIncomplete 后，停止窗口复验 1 passed；任务仍 leased、completion 为空。 |

后续仍需普通 SearchWeb、其他未闭环原生能力、剩余 lane 的真实契约集成，以及最终 Local/Ray/工作台组合验收。分组与定向复验的结果不冒充一次全量全绿。
