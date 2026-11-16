# 原生数据流历史用例专项交付（2026-11-17）

本专项已完成原清单 424 个 selector 的首次实际执行、归因和本 lane 授权范围内的修复。首次为 **296 passed / 128 failed**；修复和定向复验后，**350 个有通过证据，74 个等待其他 owner 的核心修复合并后复验**。另有新增边界测试 5 个通过。这里的 350 是不同明确版本上各 selector 的最后有效结果，**不是一个最终组合 HEAD 的 350 项验收，更不代表平台验收完成**。

工作树：`/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_native_prompt_20261117`。分支：`engine/gpt-native-prompt-20261117`。最后生产／测试候选：`ee3029cca73d402b0c3742528a042ae3b3baf091`。本文件与配套 JSON 是随后追加的证据文档，不改变该候选代码。

机器可读逐项记录见 [CORE-HISTORICAL-LANE-CLOSURE-20261117.json](CORE-HISTORICAL-LANE-CLOSURE-20261117.json)。它保留全部首次红、后续复验、每次 HEAD／回执摘要以及待验 selector，不把旧失败删除或重新解释成通过。

## 已修复的真实生产缺口

### 输入读取没有取得已准入的有限资源边界

`1a717bc` 修改 `dataflow_source_context.source_run_view`：只读 SourceRunView 从现有 `services.bounds` 投影 `memory_bytes`、`max_row_bytes`、`max_batch_bytes`，再取可选 native ceiling 与它们的较小值；不允许额外配置放大准入边界。返回只读映射，不附带 journal、action 或 reservation 权限。

这修复了原只读 SQLite archive source 在真实统一执行中取不到 `memory_bytes` 的错误。原 3 个历史测试与新增 3 个边界测试同批 **6 passed**，预算不能扩大、映射不可改、无写入权威都被实际断言。

### 原生流式 Lance 恢复在访问目标前未校验封存归属

`6402437` 修改 `dataflow_stream_lance.py`，保留原 RunRef、scope、CommitCoordinator 和提交身份：

- `_BatchSink.inspect` 在访问任何表前核对 prepared reference 与原 scope 声明，核对 native prepared 与原声明的一致性。
- `_coordinator` 依据原封存 writer 校验 schema identity、reference 的精确字段／目标／版本，以及 native sink spec 的 URI 和 schema；通过后才创建 sink、访问表。

原反例先在 `93b036a` 实际失败：把原 journal 中 prepared reference 改成外来 URI，恢复会先调用 `open_lance_dataset(foreign)`。修复后原 9 个 checkpoint 历史契约通过；`ee3029c` 进一步补上 prepared 与 scope 同时被替换、native spec URI 被替换两个反例。三条目标替换反例在固定 `ee3029c` 上 **3 passed / 9 phases**，均明确断言外来 URI 没有被打开。

## 历史观察点如何迁移

统一执行的 callback 已处于受管 worker 内。旧用例依赖调用方内存 list、退役 queue/checkpoint 目录或旧子 action 名称时，迁移的是观察位置，不将真实失败改成预期成功。

| 提交 | 范围和保留的语义 |
| --- | --- |
| `39cc25a` | 首批 callback／actor／请求身份观察迁到真实 worker 效果；共享实例生命周期实际双启停仍保留为失败交 owner。 |
| `fdd2e3f` | 流、准入、取消、去重、Lance reference、线程 6 组观察迁移；保留真实输出、重复多重性、关闭一次、有限并发、取消不继续发布。 |
| `93b036a`、`a8b0fad` | 原 checkpoint 契约改查原生 scope／ACK／固定表版本；真实 commit 后回执前中断仍恢复原 RunRef；显式迁移创建新身份，旧身份及历史不改。 |
| `c2d7e5a` | timeout／cache／u2／u3 查实际 worker 效果、原累计 manifest 和只读提交回执；512 条前缀及 image cursor 断言保留，不能迁成 258。新增辅助函数只读取原有 ACK publication，不创造成功状态或第二份 journal。 |
| `7df1986` | checkpoint resume 改为原生提交链；保留因果 prepend、跳过写入但继续流动、scope 漂移拒绝、外来表 head 不得被接纳或覆盖。 |
| `ee3029c` | 增加上述 2 个封存目标替换反例。 |

退役 `queue_factory` 注入明确要求在输入或 factory 被执行前拒绝；另用当前有限交接容量验证正常输出。未知远端业务异常遵守已有 data-only 错误词汇，不为旧测试动态导入 worker 给出的任意异常类型；原始 fatal 信息与 trace、无成功 ACK、清理一次仍需保留。`test_stream.py` 的 Hard 类型名应从原 worker traceback notes 读取，root 已接手该断言的最终修正，本 lane 不再并改。

unordered 用例改为实际因果门：慢 id0 只有在消费方看到后续 id1 后才能释放，并核对全部 6 行及先后关系。它证明真实越过慢行，**没有证明原来将封存／启动成本也算在内的 `<0.9s` 性能指标**；旧实测为 3.49s，启动成本仍交性能 owner，不能用迁移后的功能通过隐去这一缺口。

## 完整执行与证据

所有批次在固定干净源码上运行，进程内断言本树 import 路径，回执绑定输入文件的前后摘要并经 `complete_phases` 验证原 setup／call／teardown。12 份回执全部 `source_unchanged=true`；没有 skip、deselect 或 incomplete 被当作通过。

| 回执目录（均位于 `/tmp/demiflow-kv-core-history-`） | 固定 HEAD | 结果 |
| --- | --- | --- |
| `r1/core_remaining424-000` | `813af2c` | 66 通过／13 失败 |
| `r2/migrated_observers-000` | `bafdcfb` | 11 通过／2 失败 |
| `r3/core_remaining345-000` | `bafdcfb` | 39 通过／37 失败 |
| `r4/core_remaining269-000` | `bafdcfb` | 41 通过／16 失败 |
| `r5/source_bounds-000` | `fdd2e3f` | 6 通过 |
| `r5/core_remaining212-000` | `fdd2e3f` | 29 通过／50 失败 |
| `r6/core_remaining133-000` | `fdd2e3f` | 66 通过／12 失败 |
| `r7/redirect_red-000` | `93b036a` | 1 失败，真实目标归属缺口红基线 |
| `r8/native_history_and_observations-000` | `6402437` | 30 通过／1 失败，迁移 fixture 的 Path→str 已由 `a8b0fad` 修复并在 r10 复验 |
| `r9/core_remaining55-000` | `fdd2e3f` | 55 通过 |
| `r10/final_observation_migrations-000` | `7df1986` | 16 通过／48 phases |
| `r11/sealed_target_negative-000` | `ee3029c` | 3 通过／9 phases |

每个目录内的 `receipt.json` 包含真实 argv、解释器、环境、节点、阶段、错误和输入摘要。完整 SHA-256 和对应记录在配套 JSON 的 `receipt_index` 中。最后两批回执摘要分别为：

- r10：`937e780d8ca86c9d4e1dec7acdfde1eefc9b0af025e498bc6d0696cda618195b`。
- r11：`98f0c1ab4e3b571263fcb3999a4f1f3c4f6c2773f07ead1754728aed0e918b1a`。

r6／r9、r8、r10 分别在私有固定源码验证树 `/tmp/demiflow-kv-core-verify-fdd2e3f`、`/tmp/demiflow-kv-native-verify-6402437`、`/tmp/demiflow-kv-native-verify-7df1986` 执行，以避免实现修改污染正在运行的证据。不得把它们描述为当前工作树最终 HEAD 的验收。

## 尚需组合复验的 74 项

精确 selector 已写入配套 JSON 的 `pending_combined_nodeids`；可以直接调度的临时清单为 `/tmp/demiflow-kv-core-final-pending-20261117.json`，group `core_final_pending`。以下是该 lane 最后一次实测的分类，root 后续真实复验才可以关闭：

| 共享根因／待验事项 | 项数 |
| --- | ---: |
| 共享 actor 实例生命周期 | 2 |
| 封存 stage 的实际计数、时长、progress | 7 |
| 原局部异常白名单身份 | 2 |
| callback deadline 起点、取消确认 | 13 |
| batch catch／整批 miss／dead_batches | 14 |
| 原生 writer 的持久驻留与暂停 | 2 |
| 原生有状态 actor placement | 2 |
| 显式 thread callback policy | 1 |
| 实际 stage 时长和失败观察 | 12 |
| 已迁移、待组合复验的 worker failure 观察 | 12 |
| 原 checkpoint 行前沿（保留 512） | 1 |
| graph physical admission | 6 |

root 已修 stage 计数、异常边界和每条物理边的 checkpoint 行前沿，并在其独立集合得到新通过证据；本文件不冒认这些结果为本 lane 已复验，也不覆盖其原始回执。共享 actor／专用 deadline／placement 等由原 owner 收口。只有所有相关修改进入同一最终版本并实际运行，才能给出组合完成结论。

## 对两大意图的作用与边界

引擎侧：真实 Dataset 的输入 reader 获得原准入预算；恢复只消费原 RunRef、原封存目标和原 ACK，篡改目标在读取前拒绝；固定版本、值／类型、重复多重性、提交后崩溃与历史身份保护都有原生链路反例。

产品侧：上述历史事实仍来自实际执行和原提交权威，工作台不需要另造运行账本来解释结果。本专项没有单独重跑工作台 UI 或全量 Local／Ray 组合，不能据此声称产品 root 或多后端整体已 100% 完成。

本专项不修改主仓、不在主仓运行测试、不 push／合主仓。测试数据与资源域均为私有 `/tmp/demiflow-*`；r4 以后显式继承 `DEMIFLOW_LOCATION_CACHE=/tmp/demiflow-kv-core-history-locations`，旧回执环境白名单未记录此变量，此处按实际命令披露而不回写回执。r1／r3 没有新增 alias 副作用，相关旧 fixture 保持各自私有路径。七件冻结文件逐字节与 `21f52b0` 相同，摘要在 JSON 中。
