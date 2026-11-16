# U3 原生图迁移与私有标签合并交付（2026-11-17）

当前结果是部分闭环：适配层冗余参与者已收敛，固定提交 9 项通过；原 6 项中的 3 项在该提交通过，另外 3 项仍保留红断言，交总控修核心切分计量。本文不表示 U3 全绿，也不表示 Local/Ray 全面验收完成。

- 隔离树：`/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_u3_native_history_20261117`
- 分支：`engine/gpt-u3-native-history-20261117`
- 起点：`83474517a73c2ca71f467057fdbc41cd5f66e9a3`
- 当前已测代码：`6e3734442857f3b8f9522242f2199f190297cd4f`
- 机器证据：[U3-NATIVE-GRAPH-CLOSURE-20261117.json](U3-NATIVE-GRAPH-CLOSURE-20261117.json)，含每轮 selector、结果、回执摘要、布局摘要及冻结件摘要。
- 主仓未修改、未跑测试；没有 push、合主仓、更新共享环境或修改其他 lane 的生产文件。

## 1. 原意图与历史资源依据

目标是保持真实单一数据流执行、唯一 driver 提交、有限资源、共享祖先只算一次、分支能独立推进、原提交可恢复。不是恢复旧执行器，也不是为了旧 PID 数量复制旧实现。

`bbd93ff` 的源码事实：

| 位置 | 事实 |
|---|---|
| `execution/executors/local.py:883-896` | `materialize_graph` 调 `run_local_graph(graph_spec)`，没有传 `self`；`workers=1` 在该路径被忽略。 |
| `execution/graph.py:46-47,65-67` | 每 target 一个 1 CPU/1 GiB worker，祖先另一个；六 target 实际请求七个 worker。 |
| `execution/graph.py:720` | 真实 LocalCapacityPool 上限为 16 CPU、16 GiB。 |
| `execution/graph.py:730-732` | scope 宽上限为 32 CPU/32 GiB/max_workers16，但实际受前述更小物理池限制。 |
| `execution/graph.py:808-817` | cut_rows 是源 ordinal 边界；没传时默认 source_rows。普通 RunContext checkpoint_rows 不控制该旧 graph 调度。 |

因此经总控裁决，本次测试显式给原有全局上限 16 worker、16 GiB，CPU 实际参与者不超过 16 核。没有把“旧 workers=1”虚构成旧全任务只有一个 worker，也没有为 21 个新参与者增额到 21。

Local 两项旧 graph 没传 `cut_rows`，60 行实际是源末尾切一次。本次显式 `checkpoint_rows=60` 保持其历史输入节奏；commit 四项原来明确传的 8/12 行切分仍保留原值。Slow-target 的 `>0.1s` 独立完成差异没有放宽。

## 2. 范围与实现

唯一生产文件：`demiflow/planning/dataflow_graph_adapter.py`。

新适配图原本有 source1 + ancestor1 + router6 + target6 + tag6 + terminal1，共 21 个参与者。它超过历史 16 个并发 worker 上限，memory 图被迫分期持久化，实际产生大量重启与切分。

现在只把每个 target 的平台私有 `_tag` 放到同一个物理 stage 中，保留两枚逻辑 occurrence/kernel。六 reader/router 分区全部保留；各 target 仍在独立 stage，慢分支没有与快分支合并。六 reader 图现为 10 个 stage、15 个参与者。

具体约束：

1. 标记由 `translate_graph/decorate_graph_capture` 在创建真实平台 `_tag` 时产生，不凭用户字段名猜测。
2. `_tag` 是普通同步 MapOp，无 streaming actor、pending queue 或后台生命周期。
3. 仅接受单输入、单输出、单分区的相邻 plain 同步 MapOp；不能跨 router、分支或显式图 exchange。
4. coroutine/async generator/generator、类 actor、带函数属性、构造参数、并发大于 1、非 inline、非 stage scope 均不采用此优化。
5. 任一 resource limits/requirements 或额外 request 使优化退出，原受限配置原样保留。
6. resident memory、output window、state window 均取两步总和；CPU 的 max 只用于同一 worker 串行执行的上述两步，不推广到 actor/async 并发。
7. 全部逻辑 occurrence、program、kernel refs 保留；新图的物理布局变化只在编译时发生，没有重新编译或改写历史封存计划。
8. `policy.fusion=False` 不启用合并。

没有修改 worker runner、driver、placement、operator、AsyncInstance、提交协议或原 journal。

测试范围：两个原 U3 文件、只读 `_native_graph_fixtures.py`、已有 `_native_receipt_fixtures.py` 的空 segment 读取上限修正。空 segment 用 `max(1, ref.size_bytes)` 满足原 store 的正上限要求，未改 store 行为。

## 3. 保留并增强的验收语义

只从原封存计划、原 action 的提交、原 StateArtifacts 与原 materialize receipt 读取事实：

- 真 dag_id/plan_id、六 reader 分区、实际 participant 集合与 stage 对应。
- 唯一 action/唯一提交权威；所有提交 ACK、连续 sequence、唯一 commit_id、原 owner attempt/fence。
- 每个当前贡献必须对应本 cut；历史继承贡献必须逐字等于前面真实 ACK 的贡献，不能用伪造 participant 补齐。
- 最后 carrier 覆盖全部实际参与者与 channel；source vector 单调到正确 EOF。
- 原 worker-frontier 中 ancestor 的实际 output 行数等于输入行数；每 reader 行数正确。
- 原 receipt 里每分支的所有行逐值准确，最终结果不能只用计数充数。
- 真实 managed worker PID、启动记录、物理池退出后零 charge。
- 空输入有真实 final seal，无业务结果。
- finite 图完成原 final ACK 后，在 enclosing RunContext 完成前注入中断；同一原 RunRef 恢复，封存 DAG/plan、receipt 不变，零新 worker/零新 process 记录。没有手工改 journal 把已成功 run 伪装成失败。
- GC 测试保留原归属 channel 删除、外部未归属字节不动、reservation 收尾、提交事实不变、GC 后结果仍可读。该用例仍因切分断言待核心修复；当前 HEAD 上重新排列后的 GC 断言尚未单独获得全绿回执，不冒认通过。

原 7-PID/K6/graph-result 子 action 是退役实现观察点，已迁为上述实际原生权威事实。原 6 个 pytest 方法名全部保留，历史 selector 映射不变。

## 4. 实测布局与性能

每组都使用历史 16 worker、16 GiB 总上限。完整 stage→occurrence/partition/profile、exchange mode/credit、commit source vector 与 process 记录在以下只读导出中：

- `/tmp/demiflow-kv-u3-history-r2/native-layouts.json`
- `/tmp/demiflow-kv-u3-history-r4/native-layouts.json`
- `/tmp/demiflow-kv-u3-history-r6/native-layouts.json`

| 场景 | 原适配 r2 | 合并后 r4/r6 |
|---|---|---|
| 六 reader、24 行、durable、cut8 | 21 participants、525 process records、75 ACK cuts | r4 为 15 participants、15 process records、13 cuts |
| 六 reader 空输入 | 21 participants、21 process records、3 cuts | r6 为 15 participants、15 process records、1 cut |
| 两 reader finite、12 行 | 9 participants、9 process records、2 cuts | r6 为 7 participants、7 process records、2 cuts；真实原 RunRef 恢复成功 |
| 六 reader memory、60 行 | 21 participants；被中断时仅处理 18 行，394 process records、36 cuts，未 final | r6 为 15 participants、15 process records、5 cuts，60 行 final，所有边为 memory_incremental |

这是实际取证的进程/切分改善，仍不是理想性能：durable 13 cuts 与原期望 3 cuts 不同。不能把 r2 不完整批次 786.84s 和 r4 完整批次 96.47s 的差全部归因于融合：Local 测试还显式恢复了旧 default-source_rows 的配置语义。

## 5. 验收回执与失败披露

所有测试用固定 clean HEAD；命令是当前隔离树中 `env/bin/python -P -B tests/runtime_v3_regression_inventory.py <request.json>`，原 inventory 记录全输入摘要、完整 argv、真实 pytest setup/call/teardown。结果判定复用原 `complete_phases`，无 skip/xfail/deselection 伪绿。

各批独立私有 `DEMIFLOW_RESOURCE_ROOT/HOME/FORGE_DOMAIN/TMPDIR`；PYTHONPATH 明确 pin 本树与 tests。继承的 `DEMIFLOW_LOCATION_CACHE=/tmp/demiflow-kv-core-history-locations` 没被旧回执字段白名单收录，此处如实补充；这些 graph fixture 不使用 alias source。

| 轮次 / 已测 HEAD | 结果 | 说明 |
|---|---|---|
| r1 / efe2342 | 1 failed | 旧 graph-result 子 action 观察点失效；真实图已有原 action 提交。 |
| r2 / 492cd54 | 1 passed、3 failed、2 incomplete | 原性能问题导致批次长跑；获总控允许后只 SIGINT 核验 argv/创建时间的自有 inventory PID。清理回执证明输入不变、owned workers 退出、原私有 pool charge 为 0。两个 incomplete 永不计绿。 |
| r3 / 2213fc1 | 5 failed | 新编译测试 fixture 忘 complete action，以及用了非法 resource key，随后原义修正；没有把 fixture 错当生产失败。 |
| r4 / 197308d | 新边界 5 passed；原六项 2 passed、4 failed | 三个核心切分/慢分支红项，另一个 finite 恢复 fixture 错把 completed run 当可恢复，后改为真实 ACK 后 enclosing-run 中断。 |
| r5 / ee1e73b | 2 passed | 真 finite 恢复、新 async 不融合反例。 |
| r6 / 6e37344 | **9 passed**、43.49s | 六种融合边界 + empty + finite 中断恢复 + memory 共享祖先一次；27 个完整主阶段，2945 个输入前后摘要一致。 |

最终固定回执：

`/tmp/demiflow-kv-u3-history-r6/u3_sync_tag-000/receipt.json`

SHA256：`512647fb94d8de9b6ab635394b309505888fb3d24e297aa08a085f32b9b5b9d1`。

它精确对应代码 `6e3734442857f3b8f9522242f2199f190297cd4f`。本次追加文档不修改该已测代码；机器证据逐个复核 2945 个回执输入仍与当前字节一致。

## 6. 尚未闭环的三项（总控已接手）

1. `tests/test_u3_graph_commit.py::test_multi_cut_single_commit_full_k6`
2. `tests/test_u3_graph_commit.py::test_durable_graph_reclaims_channel_files_after_run_close`
3. `tests/test_u3_graph_local.py::test_local_graph_fast_branch_not_blocked_by_slow_target`

前两项仍要求原 cut source frontier `[8,16,24]`。r4 实际为 `[1,3,5,...,23,24]`，断言没有改成任意增量即可通过。

第三项仍要求 slow 最终完成至少晚于全部 fast 分支 `0.1s`。r4 实际差约 `0.002s`。r6 共享祖先用例的 source frontiers 为 `[13,25,37,49,60]`，本应 60 行默认切分的图仍提前多次 barrier。

代码证据分两部分：

- `WorkerGraphRunner._source_durable_windows` 用 `credit_bytes // max_row_bytes` 推 source quantum；本测试 8 MiB / 32 MiB 被压为 1。资源强制切分是否应改变历史 cut8 精确边界，待总控按原契约裁决，不擅自放宽测试。
- driver `_checkpoint_progress` 包含同 occurrence 多 partition/同边广播 ACK 计数。六分支内部放大可让输入尚不到 60 时触发 barrier，导致慢分支持续把快分支拖回对齐。总控已明确接手 graph-adapter 路径按原 source ordinal 的切分修正；本 lane 不修改该生产文件。

注意：上述源 ordinal 兼容结论限该有明确旧输入切分契约的 graph adapter，不能据此把所有普通 Dataset 的 output/finite-phase 进度删除。最终集成要在总控修复后的同一版本重跑原六项；9 项绿不替代这一步。

## 7. 提交顺序与冻结检查

按顺序消费（都基于 root 8347451）：

1. `efe2342` — 历史真实全局资源显式化。
2. `492cd54` — 原提交/参与者/完整结果的原生观察迁移。
3. `2213fc1` — 私有 tag 定向合并，保留全部逻辑节点/资源窗口。
4. `197308d` — 编译 fixture 正确完成 action、合法 constrained profile。
5. `ee1e73b` — 真 ACK 后中断恢复，async 不融合，GC 证据断言先于仍红的精确切分断言。
6. `6e37344` — 平台 tag 改普通同步 kernel，防止将 streaming tag 计为串行无后台阶段。

七件 `raw_fetch.py / queue_execution.py / queue_sinks.py / queue_source.py / planning/dag.py / contracts/exchange.py / resources/contracts.py` 与原 `21f52b0` 字节完全一致，摘要在配套 JSON。历史前一轮交付树 `gpt_native_prompt_20261117` 的 `939d383` 保留不动。
