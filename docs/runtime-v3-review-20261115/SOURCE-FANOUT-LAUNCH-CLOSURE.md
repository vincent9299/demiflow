# 扇出背压与有界启动引用：交付与证据

本专项最终在 `78ef0ba656503c891288e512efb389c34c966b19` 验收：**50 passed、0 failed、0 skipped，146.37 秒**。真实 Local process、Local thread 和私有 Ray Core（1 CPU、0 GPU）的原提交恢复均覆盖。测试前后 2120 项源码/测试摘要一致；冻结七件逐字等于 `49b666f`。完整输入摘要、逐项结果、命令、导入路径和原始证据摘要见同目录 `SOURCE-FANOUT-LAUNCH-CLOSURE.json`。

## 修复的行为

1. **共享源的扇出占用同一个有限 delivery 窗口。** `WorkerGraphRunner` 在已有 channel/图上计算每条源记录实际能到达的外发数量，用原 64 槽总窗口、原各通道行/字节窗口和 checkpoint_rows 共同收紧源读取步长。内部 fork→union 的多条路径分别计数，broadcast 按真实收件分区计数；其他 source split 的分支不混入同一条记录，finite spool 不计成 live delivery。没有增加任何窗口，不执行 UDF 来猜扩张，不改 sequence、routing、ACK 或恢复协议。
2. **大 assignment / RecoveryImage 不再塞进 64 KiB 启动信封。** `_EndpointBase._build_launch` 经现有 codec 与 `max_control_bytes` 编码，再由原 ArtifactStore 写成带 media type 的不可变引用。worker 仅从既有 authenticated store 读取，校验引用形状、类型、store、大小和原摘要后解码。禁止任意路径，禁止同时传 wire/ref，显式 null 恢复引用必须报错。旧 inline wire 保持读取兼容。bootstrap 原 65536 字节上限不变。
3. **仍由原存储与提交权威管理生命周期。** 新载体加入既有 GC 结构遍历；原 writer lease 保护在途启动，原历史根/提交根保护子引用。没有第二套账本或分配器，也没有让 worker 提交历史。

源读取收紧修复的是可暂停记录之间的背压。单次调用自身扩张超出原窗口仍拒绝，不能靠扩大资源把失败隐藏。

## 验收覆盖

| 边界 | 实际证据 |
|---|---|
| 逻辑图与结果 | 1 个共享源、16 个分支、19 个逻辑节点；5 条输入得到完整 80 条结果，重复多重性逐条一致 |
| 跨 attempt 恢复 | checkpoint_rows=2 强制真实 nonfinal cut；原提交 ACK 后中断，resume 保留原提交前缀；源总共只读取 0–4 各一次 |
| 优化不遮住背压 | 关闭可选 fusion，真实 4-worker / 256 MiB 执行在原 ACK 处暂停；只读前 4 条、源 next_sequence=4，16 路共占原 64 槽，第 5 条未读 |
| 超窗口反例 | 关闭 fusion，单条源记录发往 65 个收件人被原 RESOURCE_INFEASIBLE 拒绝，未提交；另独立原生 probe 验证单行 flat_map 动态输出 65 行同样被拒绝 |
| 启动边界 | 真实 process 首跑与恢复均仅携带引用且信封小于 65536 字节；大于 64 KiB 的有效 RecoveryImage 可经原有有界 store 读回 |
| 历史准入 | 引用被替换、越界、错误 store/media、路径穿越、wire/ref 并存、显式 null 都拒绝；旧 inline assignment 与 RecoveryImage 可读 |
| GC | 在途 writer 阻止回收；原 pin 保留载体及其原子引用；撤 pin 后按原字节计费回收；原 published GC 崩溃恢复矩阵通过 |
| 真 Ray Core | 原 1 CPU 有状态函数 nonfinal ACK 恢复用例通过，保留状态递增、旧提交、原调用日志前缀；Ray Data 导入 blocker 无尝试，真实 Ray 私有实例完成后关闭 |

50 项包含 launch refs 7 项、fanout 5 项、published GC / idle notification 37 项及真实 Ray 恢复 1 项。独立 flat_map probe 不计入这 50 项。

## 提交与集成关系

- `d434c6f`：扇出步长、thin launch 引用及原测试，4 个生产文件 + 2 个新增测试文件。
- `1e93587`：合入总控 `1120dcd`，包括总控的纯 CPU suffix 布局、sealed physical 根引用等修复。没有在本专项重做规划。
- `ca340e5`：验收默认融合后的用户语义，同时新增关闭融合的真实 64 槽 ACK 反例；恢复用小 checkpoint 固定 nonfinal cut，不再依赖物理阶段数。
- `78ef0ba`：显式 null 恢复引用 fail-closed 及反例。

总控已集成 d434 主体，可单独取 ca340e5 与 78ef0ba。各项生产修复在本次验收时固定不变；本说明后续文档提交不改变被测源码。

## 保留的红证据与范围

`/tmp/demiflow-regression-fanout-launch-final-evidence/verification.json` 是旧 d434 的固定字节证据：45 passed / 2 failed，2111 个源码/测试输入无变化。两条 19 节点完整/恢复路径均触发原 120 秒预算。旧 67 节点首次扇出窗口失败、随后 64 KiB launch 失败与后续 120 秒红日志均未覆盖：`/tmp/demiflow-fanout-red-b161161.log`、`/tmp/demiflow-fanout-window-first.log`、`/tmp/demiflow-fanout-window-ref.log`、`/tmp/demiflow-fanout-targeted.log`。最后一个探索日志的一次 code-pin 失败是测试文件被本 agent 在执行期间编辑导致，不作为生产缺陷证据；此后正式取证均固定字节。

最终完整证据：`/tmp/demiflow-regression-fanout-launch-final2-evidence/verification.json`。旧 B/H 99 项在 d434 通过，但包含 legacy doubles 与 FakeRay 退出告警，不冒充真实 Ray 证据。此前借用 thread 状态修复的 40 项独立验收见 `BORROWED-THREAD-STATE-CLOSURE.md`，不混入本次 50 项。

本专项没有宣称本 HEAD 全历史套件、全部 API、真实多机或 GPU 已验收。原 67 节点 / 1024 结果 / 4-worker / process / 120 秒最终性能验收由总控负责，不能以本页 19 节点用例代替。主仓、共享环境和既有训练服务没有作为本专项写入或测试目标；无 push、无合主仓。
