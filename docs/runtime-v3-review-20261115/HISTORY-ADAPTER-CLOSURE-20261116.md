# 原生适配与历史失败收口（2026-11-16）

本轮隔离树：`/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_v3_artifact_recovery_20261115`，分支 `engine/gpt-v3-artifact-recovery-20261115`。最后业务测试源码为 `8c00da5`；本文件与配套 JSON 是其后新增的回执整理。

结论只限下列实际证据：本 lane 将原 90 个失败中的 75 个完成同名复验或有明确新契约的迁移。并入 native lane 的 L010 和 root 的 L015 后，证据并集覆盖 83 个，剩余 L017 原 7 个由 root 接续。它不是一个最终集成版本的全仓通过声明。配套 JSON 逐条保留旧 nodeid、原失败阶段、实际新 nodeid、源码版本、回执与验证阶段；L015 只有无 failure/error/skip 的 JUnit，明确没有冒造三阶段回执。

## 生产改动与意图

1. **`compile_kernel_spec` 的选项准入**：同步 `map/map_batches` 原先忽略退休的 Ray Data compute/pool/worker-range 选项；统一编译入口复用原 `validate_stream_native_options` 明确拒绝。合法 backend 和 worker_resources 保留，并验证实际原 allocator 授予声明的 CPU。判断先查注册 kind，再读选项；未注册用户对象不会被 `isinstance` 的 `__class__` 读取或其他 getattr 激活。负例在任何 UDF、worker、HTTP、commit 前拒绝，不以资源不足替代非法选项拒绝。
2. **双封存载体读选择**：`load_sealed_plan(..., version=None|'v2'|'v3')`。默认行为不变，仍优先 v3；指定版本缺失直接 `missing_plan`，损坏仍按原摘要/schema 检验拒绝，不改走另一格式。`v2` 指原 F1 v1/v2 家族。旧 native binding 的两个读取点明确选原 v2；只读旧历史，没有重写旧 seal，也没有第二执行/提交权威。
3. **旧 stream/pull 入口薄适配**：`iter_stream_rows` 去除自己的线程、ObjectRowsSink 和 CommitCoordinator，复用唯一 `dataset_native.execute`；原 action/source/完整 operations/run 身份校验保留，补同长度不同 plan 反例。借来的 action 不由适配层 complete。limit=0 不创建 action、不读 source。local_pull 同接原 v3入口；local_stream Arrow 输出也把 take/take_batch 的 limit 传入原 native terminal，不能变为全量成功。
4. **公开 `take_batch(0)` 的真零读**：原来只返回空结果，真实惰性 source 仍被启动，旧 `_iter_source` spy 没有看见。现在 `_lower_request` 与 take(0) 一样生成原生空 source/plan，不另造执行路径；真实 source 文件标记证明零读。
5. **薄聚合入口 program ID**：`Sum.name` 是 `sum(id)`，不能当执行 program 的标识。仅改两行，使用公开 Dataset 已有的 `aggregate-N` 标识，用户结果名称仍是 `sum(id)`。
6. **Engine 剩余借用 placement**：native lane 的 `b07c416` 使 `Engine.python_rows` 调唯一 execute 并传原 execution；本 lane 用真实输入、原 allocator、同一 action、故意 UDF ValueError 与 finally 锁定实际行为，没有恢复 `_apply_plan`。

`arrow_batch`/`lance_scan` 原分支本轮未盲改。新的统一公开 Dataset 不依赖这些历史低层入口；本轮承诺范围是已明确接续的 native pull/stream/Engine 薄适配。

## 历史断言如何迁移

- A006/A007/A019 等准入测试换成真实惰性 source 的可观察读取标记。旧 `_iter_source` 不再是统一引擎调用点，用其永远为零的 spy 不能证明准入时没读数据。
- A015 不再断言 `local_pull/datafusion` 旧分流表，改为 Local/Ray 对同一声明有同一 capability，explain 不读 Lance、不构造 actor、不创建 run/action，并保留真实资源声明。explain 的 supported 没有冒称实际已获 grant。
- A016 不恢复 `_apply_plan`。直接测保留的 native 薄入口，非法 sealed placement 在真实 source 之前拒绝，合法路径执行真实输出。
- 旧 `streaming schema/count/aggregate/size/write` 的“必须拒绝”断言，按统一引擎已支持的契约改为实际正确结果；合法资源声明不再按旧 Ray 专用路径一概拒绝。退休 compute/range 与非法 CPU 的拒绝另有真实负例。
- A020 原 `ex.aggregate(..., ()) is None` 是低层空聚合 tuple 的行为，公开 `Dataset.aggregate()` 本来要求至少一个 aggregation。现在分别锁：公开零 aggregation 在 source 前拒绝、`aggregate(Sum('id'))` 空输入为 `{'sum(id)': None}`、公开标量 `sum('id')` 空输入仍为 `None`、非空 streaming Sum 为精确 3。没有把一个合法空数据集行为改成拒绝。
- R006/R012 不再调用退休的旧 stream 恢复执行器。原 `checkpoint_rows=2`、`checkpoint_interval_ms=60000` 不变；经真实 Dataset materialize 的原 `after_acked` 故障点中断，要求原 cut 的实际 batch/group/timer/prepend remainder，原 prepared/receipt 前缀逐字不变，连续序列、完整结果和无重复均保留。没有伪造 checkpoint 文件或加预算使测试变绿。

## 固定源码实测

| 回执目录（均在 `/tmp`） | 源码 HEAD | 实际结果 | 本表采用范围 |
|---|---|---|---|
| `demiflow-history-upgrade-contract-r1` | `d900f72` | 864 项：774 passed、87 failed、3 setup errors | 原始 90 失败清单 |
| `demiflow-history-public-migration-r2` | `8af1883` | 混合回执，保留全部失败/teardown error | 仅 setup/call/teardown 全绿的完整文件 |
| `demiflow-history-adapter-migration-r3` | `0f170dc` | 69 passed | 全部 7 文件 |
| `demiflow-history-adapter-combined-r5` | `de592f5` | 94 项：91 passed、3 failed，234.21s | 18 个全绿文件；A020 的 3 个失败不算通过 |
| `demiflow-history-adapter-final-r6` | `85834ff` | 8 项：5 passed、3 failed，16.44s | L045 全文件三阶段通过；混合文件不算通过 |
| `demiflow-history-adapter-final-r7` | `b28a534` | 6 项：4 passed、2 failed，11.45s | A020 4 项全绿；Arrow fixture 尚未通过 |
| `demiflow-history-stream-arrow-r9` | `29b69c0` | 4 passed，10.17s | Arrow 新旧 caller 真实限额与小预算拒绝 |
| `demiflow-history-aggregate-final-r10` | `8c00da5` | 4 passed，15.51s | A020 追加公开空 Sum 标量断言后完整复验 |

每个采用的回执保留精确命令、Python、PYTHONPATH、三个 `/tmp/demiflow-*` 独立资源域、源码前后摘要、全量 nodeid、setup/call/teardown。新批次没有 skip、deselected 或 collection error。逐输入文件与回执 HEAD 的 `git archive` 比较均无摘要不符；JSON 保存每份回执与 XML 的 SHA256。未把不同 HEAD 的通过数相加伪装成同版本全集通过。

r4 的错误、r6/r7/r8 的 fixture 错误及红基线均留在原 `/tmp` 目录。r6/r7/r8 中新 Arrow 测试误低估驻留数，随后只读编译探针核实 source/map/schema/terminal 是四个不可 park 的实际 stage；最终测试保留 2-worker 拒绝（零 source、零 commit），并为正例显式声明 4-worker/4000mCPU/192MiB。正例仍为 1000 行惰性输入、take/take_batch=3、batch_rows=2，要求实际读取小于 1000、精确输出 3 行、RecordBatch 每批≤2、int32 schema、原 ACK、借 action 未 complete。不存在用该预算调整掩盖旧历史小预算回归的情况。

双 seal 选择专项是实际 prepare/封存/损坏读取测试，其人为 `test-preparation-only` complete 只用于测试作用域收尾，不算真实业务通过。真实旧 datafusion caller 的首跑+恢复由 native lane `fc993cd` 的 73 项组合中 `test_real_legacy_caller_reuses_one_action_and_both_seals` 验证：旧新 seal 字节不变、原 ACK、不增 action、漂移拒绝、同参数零 worker 复用。

## 交给 root 的提交顺序

前轮 `2f06af1/9192d9b/0f170dc/8af1883` 已由 root 集成，不重复取。

本 lane 独有生产：

1. `ad96846` seal 版本选择；`ac03e09` + `24aca7a` 合在一起取，保证未知类零属性读取。
2. `46da6c4` stream/pull 薄适配；`f22882a` take_batch(0)；`912dad7` Arrow limit 透传。
3. `5162d8c` aggregate program ID。

本 lane 独有测试（按历史顺序取，最终断言以末版为准）：

`5c103bf 8a07b60 3fc245e 0d3993f 69cf877 a82e406 85834ff b28a534 411e142 29b69c0 8c00da5`

native lane 的共享依赖由它交付，别重复取：本树 `ccb9d5f = f05676c`、`de592f5 = fc993cd`、`63bb22d = b07c416`。没有它的 execute(schema/limits/limit) 及 Engine 修复，不能把本测试报告移植为旧代码已通过的证据。

## 尚未完成的范围

配套 JSON 明列原 L017 的 7 个 nodeid，root 接续；native L010 与 root L015 的原证据仅作明确标注的跨 lane 并集。最终集成版本还需按实际差异做组合复验。

原 full inventory 的 8119 nodeids / 764 文件（577 原有 + 187 新增）只是 collection。正式 V035 `all_regressions` 目前仍有真实执行/分类接线缺口，不能因这次 83 个历史问题有证据就改成通过常量。下一项工作会逐 nodeid 明确环境、实际执行/具体替代断言/未验证，未审旧 fixture 不盲跑，PG 等环境缺口单列。
