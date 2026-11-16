# SQLiteKV 原生提交与 Datasink 公共回调修复

工作树：`/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_v3_kv_sink_20261116`。分支：`engine/gpt-v3-kv-sink-20261116`。基线：`df6125fdfd52eb62c0bf688f60d9288e77195389`。

## 原缺口与修改范围

基线原 `tests/test_kv.py` 为 **2 failed / 1 passed**：两条真正的 `Dataset.write_datasink(SQLiteKV.datasink())` 在 `Datasink.recovery_identity` 抛 `NotImplementedError`。旧 KV sink 只有 `write` 与 completion callback，既不能加入统一原生提交协议，也拿不到 v3 已丢失的公共回调。因此仅加 identity 会把原本不可安全恢复的副作用错误升级为可恢复，本次补完整四阶段协议。

生产文件仅三个：

| 文件 | 交付内容 |
|---|---|
| `demiflow/_kv_sink.py` | `DurableKVSink`：原上下文校验、有限 staging、原 run scratch 预留、目标身份、prepare / commit / inspect / abort |
| `demiflow/kv.py` | `KVDatasink` 复用 durable mixin；原 key/value 编码、namespace 配额、`put_many` 和 legacy `write` 保留 |
| `demiflow/execution/dataflow_api.py` | `run_dataset_action` 为所有 `write_datasink` 恢复通用 start / complete / failed；原执行体 `_run_dataset_action` 仍执行同一 v3 链路 |

未改 RunJournal、CommitCoordinator、Dataset、worker、共享环境或主仓。承重七件与 `49b666f` 逐字节一致，完整摘要由验证器记录。无 push、merge、生产运行或共享服务变更。

## 端到端链路与唯一权威

`Dataset.write_datasink` → 通用 `on_write_start` → 原 dataflow 编译/封存/worker 数据处理 → 原 `NativeTerminalSink` → 原 `CommitCoordinator` → KV `prepare/inspect/commit` → 原运行回执/checkpoint/ACK → 原聚合 `WriteResult` → 通用 `on_write_complete`。

worker 不发布 SQLite，恢复入口不绕过历史保护，KV 不创建 RunJournal / CommitCoordinator。原 `KVDatasink.receipt` 由 completion callback 接收引擎实际聚合结果，原生 commit 不伪造聚合 `WriteResult`。

新增目标 SQLite 表 `kv_store_identity` 与 `kv_sink_commits` 是 sink 自己的目标身份和原子副作用收据。收据包含原 run/action/commit identity、完整 prepared 摘要、目标及 namespace。它与所有 KV upsert、namespace usage 在**同一个 SQLite transaction** 内提交。表中没有 execution progress、checkpoint、attempt 管理或 ACK；运行状态和恢复决策仍只由原平台账本决定。

### prepare

- 单行解码、严格复用原 JSON/key 编码；保留 `batch_rows` / `batch_bytes` / `max_value_bytes` 拒绝语义。
- 每行在写 header/key/value 之前一次性向原 run 预留完整字节，分别流式写出这些块，不拼接完整 Dataset 或额外复制完整 payload。
- 累计行/字节受原 run 输出上限及原总 scratch 上限约束；持久化 staging 的 size、SHA-256、rows、batches 以及原 commit identity。
- 全部输入、单行、批次限制校验结束并 fsync staging 后，才允许创建目标身份元数据。**此阶段不写任何业务 KV 值**。原 `batch_bytes` 负例仍保证目标 SQLite 文件根本未创建。
- 创建身份使用事务内 `INSERT OR IGNORE`，已有 database_id 不重写；prepared 绑定 database_id、device 和 inode。

### commit / inspect / abort

- 原 `BEGIN IMMEDIATE` 串行化同一 SQLite 目标的发布；每次事务先查原 commit marker。已提交时直接返回同一收据，不重新 upsert，后续合法业务更新不会被历史重放覆盖。
- 输入逐行读取并在 transaction 提交前验证累计行数与完整 SHA-256。容量拒绝、校验失败或途中异常使此前 upsert 和 usage 一起回滚。
- `inspect` 只在原目标可读、device/inode/database_id 一致且原 marker 确认缺失时返回 `None`。丢失、替换、锁定、权限/数据库错误、摘要/收据不匹配均报未知；不得据此创建目标或再次执行输入。
- `abort` 只删除本 commit 确定尚未发布的 staging。对已发布的 KV 数据和原确认收据不做回滚或清理。恢复需要的原证据仍受原 journal 管理。

## 有限资源的实际保证

增长变量包括输入行数、键/值编码字节、目标现有 SQLite/WAL/journal 大小、索引/回滚增长、收据个数。staging 是逐行有界序列；SQLite page cache 是连接级有限配置。每 namespace 的原生收据最多 65,536 个，达到上限明确拒绝新发布，绝不清除历史确认收据换取准入。

SQLite 目标初始化和事务写入前，依据既有文件大小、实际 staged payload、行数和索引/收据余量，对 SQLite rollback / growth 进行保守预留。预留进入**原 run 总 scratch 账本**，与 v3 artifact staging 及其他 producer 累加；本任务预留在 finally 中释放。预留属于准入记录，不是新 allocator 或执行账本。已有数据库和其他任务的文件不会被当成本任务临时文件清理。

这些边界不等价于 SQLite allocator 或进程 RSS 的精确硬上限，也不控制其他外部写者的物理磁盘增长。SQLite cache 上限、保守临时空间预留、原执行截止时间与平台已有资源保护承担各自的边界；不将小样例成功宣称为 SQLite RSS 硬隔离。

## 通用 Datasink 回调语义

- `on_write_start(schema=None)` 每次 API 调用一次，在本次执行可能消费输入前调用，不为获取 schema 提前读取源。
- 成功时 `on_write_complete` 只在原 ACK 和真实聚合 `WriteResult` 已确认后执行。
- 执行失败时 `on_write_failed` 一次；失败回调再次抛错只附加 exception note，保留原异常。
- completion callback 自身失败不会转调 failed；抛出的异常明确附注“已确认发布，原 commit 仍有效”。原运行回执不改成未写。显式原 RunRef 恢复会重新通知 completion，但不重新 prepare/commit 或消费源。
- 验收使用独立通用 `FormalSink` 子类，未借 KV 特判证明公共契约。

## 验收与证据

运行器 `/tmp/demiflow-kv-sink-runner.py` 在进程内断言实际 import 位于本 worktree，要求三个独立 `/tmp/demiflow-*` 资源域、干净 HEAD；记录 setup/call/teardown、完整测试命令、生产/测试/配置输入 SHA-256、冻结七件摘要、结束 HEAD/dirty 状态与存活子进程。所有测试产物和证据均在 `/tmp/demiflow-*`。

| 批次 | 固定 HEAD | 结果 | 原始证据 |
|---|---|---|---|
| 原失败复现 | `df6125f` | 原 KV 2 failed / 1 passed | `/tmp/demiflow-kv-red-evidence/verification.json` |
| 原测试转绿 | `3926918` | 原 KV 3 passed | `/tmp/demiflow-kv-core-r1-evidence/verification.json` |
| 第一次新增验收 | `95ea0bf` | 6 passed / 8 failed，未宣称整批通过 | `/tmp/demiflow-kv-r2-evidence/verification.json` |
| 注入修正与原入口回归 | `df129b4` | 新增剩余 8 + 原入口 7 = 15 passed；先前通过的 6 项显式 deselected | `/tmp/demiflow-kv-r3-evidence/verification.json` |

第一次新增验收的六项通过覆盖真实进程在 prepare 未记录、prepared 已记录、目标发布但原运行收据未记录三个位置退出，以及目标 missing/replaced/receipt corruption 三种恢复拒绝。八项失败来自测试注入/断言本身：只在父进程 monkeypatch 生产方法被 worker code pin 正确拒绝；pytest assert rewrite 使 fixture 类在父/子进程代码不一致；把无副作用中间切片 ACK 误认为 KV 最终发布。修正采用同进程 thread 模式注入、普通可导入 fixture 模块及准确的 native-final ACK 判断；未修改 code-pin 守卫或生产协议。

**最终同一 HEAD 汇总批次：待执行。** 性能 lane 的无竞争窗口结束后，执行原 KV 3 + 新增 KV 13 + 通用回调 3 + 原 entrypoint 7，共 26 项。新增空输入、Unicode、负零、重复键及跨 namespace 历史值场景目前尚未执行，不计入通过。

本次只声明 Local 线程和子进程路线及原生 driver sink 协议的证据。尚未单独做本 sink 的 Ray、多机或大库吞吐验收；不把父任务已有的其他 Ray 测试替代为本 sink 的证据。

## 对两大原始意图的贡献

- **引擎**：恢复原真实 Dataset 写出能力；提交/恢复/资源仍接入同一统一引擎和原运行权威，既有历史值与配置身份不被补丁绕过。
- **产品**：业务成功能看到实际聚合写入结果，完成后回调失败有明确已提交说明，恢复继续使用原 run/action/commit 事实。工作台本身没有新展示实现，本补丁不扩大产品完成度声明。
