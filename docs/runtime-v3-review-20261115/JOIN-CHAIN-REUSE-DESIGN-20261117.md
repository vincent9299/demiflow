# 连续同键 join/reduce 复用：源码裁决与最小实现方案

历史状态：**本文件记录当时待裁决的未实现设计**。随后已获总控批准并完成实施；最终符号、边界和49项验收见 [实施交接](JOIN-CHAIN-REUSE-CLOSURE-20261117.md)。下文取证与建议保留为当时的设计历史，不把提案接口冒充已交付接口。

工作树：`/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_native_join_reuse_20261117`。
分支：`engine/gpt-native-join-reuse-20261117`。
取证基线：`1c2292e8d409bff20105fd34ae7c7c59255d8a33`。
还完整阅读了 performance lane 的 `87cc0c301504715b451e68c11339c80c3c788ff5`，实现须在总控合入它之后继续。

## 1. 一次裁决建议

建议实现一个**原 v3 worker 内的同键关系计算区间**：同键相邻 join 与末端 reduce 仍是原逻辑节点，物理上共用一个受准入的同步 worker；叶输入的键和原载荷只准备一次，中间关系只传受预算约束的行引用，不再次编码宽载荷、不按相同键重新分桶。换键、任意 map、reducer 输出、共享分支、资源隔离边界使区间终止，继续原通用执行。

它是有限关系 kernel 的物理优化，不创建 Dataset、独立调度器、运行日志、提交或 DataFusion 会话。原 stage lease、RunArtifactStore、OperatorKernel image、driver ACK 保持权威。没有必要为这 19 项恢复整个旧 dataset_native 执行器。

仅改 stage 合并或统计字段不够：即使同进程，当前内部 finite 输入也由 `_SpoolPort.push` 重新物化，下一次 `join_pairs` 再编码和分桶。因此真正闭环需要 planner、worker 内节点推进、关系 kernel 三处小范围协作；不能诚实地把它描述成单函数改动。

## 2. 当前源码与真实运行证据

历史测试 `tests/test_local_join_chain.py` 共 19 项：16 个 join how 组合（left/inner/semi/anti 的两层组合）接非交换 reducer、2 个 thread/process 的左右两侧 join 再 join、1 个 reducer 重写可见键后再 join。

旧实现 `execution/dataset_native.py:399-402,416-431,533-540` 的有效能力是：直接相连、同键、无额外 Python 操作的 join relation 递归合成，保留已编码键及行来源序号，末端 reduce 用该查询的连续分组；map 和 reducer 输出重新 packed。`datafusion` 查询数只是当时的实现观察点。

当前源码：

- `planning/dataflow_compile.py:_capture_relational` 为每个关系生成独立 logical occurrence；输入为 `finite_complete`。
- `planning/dataflow_place.py:_routing/choose_exchanges` 每个 join/reduce 输入都选择 hash。`_replica_bounds` 又把 multi-input 固定为一个实例，所以这里不存在可直接沿用的多分区关系布局证明。
- `kernels/operator.py:_finish_complete` 每个关系独立读取 `_SpoolPort`。
- `kernels/join_spool.py:join_pairs` 每次写入左右输入到本次 scratch，建立自己的 16 路 `_Chain`。
- `kernels/relational.py:_ordered_group_rows` 每次 reduce 从头建立排序 runs。
- `planning/dataflow_finite_fusion.py` 最多合并最后一个 finite kernel 与同步后缀，遇第一个 finite kernel 即停止，不覆盖本任务。

旧真实回执：`/tmp/demiflow-perf-owned-core-history-r2/core_storage_remaining572-000/receipt.json`，HEAD `e686bd8`，19 项调用都在 `datafusion` 次数 0 的断言失败；其中 16 个 how 组合、改键 reducer 的首次值比较已通过，而两个 thread/process 用例只跑到 change=False，后续 change=True 被前面的统计失败短路，尚未验证。这说明业务值当时正确，不证明同键复用已发生；其 expected 也是当前 DataAPI，最终验收必须补独立语义 oracle。

本轮私有真实取证（非完整测试验收）：

- `/tmp/demiflow-join-region-design-r1/probe.py`
- `/tmp/demiflow-join-region-design-r1/probe.json`
- `/tmp/demiflow-join-region-design-r1/source.json`

固定 `1c2292e` 公开 `data.local_execution(workers=2, worker_mode='thread', partitions=3, batch_rows=2)`，真实 left join→left join→reduce，含重复键与 null：输出完整，k=1 的 8 个组合顺序准确，其余 k=2/k=3/null 行保留；唯一原 action 全部 ACK。实际物理图为 7 个独立 stage（3 source、2 join、reduce、terminal）；两个关系内部边都未共 stage，仍是 finite/hash。

原 256 MiB grant 中，row/batch 各 16 MiB；每 join profile 112 MiB，reduce 32 MiB。简单将两 join+reduce 的 profile 求和已占 256 MiB，还未留控制面：**不能先 sum-fusion 再扩大预算，也不能用 max 假装内存自动共享。** 新区间需要实际共用载荷/索引窗口与对应的活对象证明。

## 3. 87cc0c3 的复用边界

保留并直接使用 `_SpoolRecords.for_join(fields)`、`borrowed_join_records` 及 `_ValidatedFiniteEntry`。后者是绑定真实 store/assignment/bounds/原 seal 的当前 attempt 能力；restore 后必须重新验原有限输入，不能把 proof 序列化成永久准入。

87cc0c3 减少有限输入第二遍完整 payload decode/encode，仍然让每次 join 独立建 scratch。新区间只补跨关系的键/载荷复用，不复制该投影、校验、typed codec 或 store 所有权机制。

## 4. 冻结的区间资格与计划对象

新增内部只读数据类，不改公共 Dataset 或 frozen transfer/commit 格式：

- `RelationalRegionInput(edge_id, consumer_occurrence, input_port, fields)`：一条真实外部输入边、其原输入位置、有效键字段。
- `RelationalRegionPlan(region_id, occurrences, exit_occurrence, inputs, internal_edges, key_bindings, ordered_groups, scratch_schema)`：所有 ID 指向原封存 graph，tuple/不可变映射；数量受原 max_nodes/max_edges/max_depth 限制。
- `RelationalRegionWindow(resident_bytes, index_bytes, reference_bytes, row_bytes, state_bytes)`：原 stage grant 内的活对象分配，不是额外 run allocator。所有 scratch 仍向原 store 提前准入。

`find_relational_regions(graph, stages, *, policy, bounds, hosts) -> tuple[RelationalRegionPlan, ...]` 的资格：

1. 至少两个直接相连关系节点；join 支持现有四种 how，reduce 只允许为区间唯一出口。
2. producer join 传给 consumer 的字段必须是 producer **左侧保留键**，且与 consumer 此端 `on/right_on` 完整同序同型。允许左右字段名不同，但证明分别绑定每条输入，不按名字相同猜测。
3. 不穿过 map/filter/rename/projection/opaque UDF；初版即使某投影看似保键也保守终止。任何 reducer 的输出都失效，不能从其输入键推断输出键。
4. 每内部节点只有一个 consumer；共享上游仍按现有 fan-out/有限 seal 处理，不能被两个区间各执行一次。
5. 不穿过显式 repartition/sort/exchange、源、terminal、actor/device/非 CPU、显式 isolation/额外 resource 要求。
6. 不跨历史已封存计划重新优化。`fusion=False` 维持通用路径，作为优化因果对照。
7. 同区间两个分支都可包含同键 join 树；不把不同键的两个子树硬拼到一个全局链。

`fuse_relational_regions(...) -> tuple[StageSpec, ...]` 保留全部 occurrence/kernel refs。建议 stage ID 由版本化 region 描述摘要派生；运行端根据原 sealed graph +该 stage 解释并严格复核同一描述，不能自行发现并优化旧 stage。无需额外 journal/manifest 账本，也不修改逻辑 DAG 节点数量。

## 5. 原 worker 内计算与载荷生命周期

建议新内部 `RelationalRegionRuntime`：只执行已确定的有限 kernel 区间，不申请资源、不启动进程、不提交、不产生二次 Dataset action。

接口：

- `__init__(plan, *, kernels, services, window)`：绑定已有 OperatorKernel、原 services 与已准入窗口；不消费输入。
- `accepts_input(occurrence_id, port) -> bool` / `input_finished(...)`：只读原端口是否完成；所有外部 finite seal 准入完整后才可产生结果。
- `advance(*, max_records, deadline_ms) -> Iterator[RegionOutput]`：受原 worker cooperative poll 限制。`RegionOutput(occurrence_id, record, external)` 对真实生成的内部/出口结果计量；内部数据不再走 `_SpoolPort.push`。
- `checkpoint_ready` / `snapshot_member(occurrence_id)` / `restore_member(...)`：原 kernel image 中的有限引用和游标；不引入独立 checkpoint。
- `close()`：只关闭本区间 scratch 和未确认临时所有权；不 release 被借用的原 finite artifacts。

内部共用 **键目录 + 行引用区**：

1. 叶端口用 performance 已完成的 `for_join` 输入。每叶键只编码一次；原文档一次写入区间受管 scratch，或借用已有可验证不可变片段。为原 typed key 保存独立 null-valid 标志（null 可分组，但从不 join 匹配）。
2. 分区只按叶的 canonical key 执行一次。后续同键 join 继承该键/分区证明，不对中间 row 再求键和分桶。
3. 中间 join 产物是**内部受限 row-reference**（叶引用、缺省右侧、合并字段/suffix、来源次序），不越过公共 Record/transport/recovery 数据格式。它保留重复多重性，不做业务 dedup。
4. 中间可能有受磁盘预算约束的引用索引/溢写；明确不是“零中间字节”。要求消除的是全量宽 payload 重编码/物化和同键重复分区，不能为口号把必要的有界索引藏掉。
5. 输出或 reducer 真需要字段时才 hydrate；按原 `_merge_row` 的字段碰撞/缺省规则处理。不能延迟必应失败的校验到已提交之后，也不能因下游过滤跳过原输入 key 的合法性检查。
6. 普通 join 不增加最终全局排序。末端 reduce 需要连续分组及原组内来源顺序时，使用现有 bounded ordering，仅对键/引用/原 provenance 建排序索引；payload 仍留在原载荷区。不同关联算法偶然的输出排序不是业务契约。
7. 任意 reducer 仍按原 sequential fold、一组 deepcopy initial、原 runtime contract、state bound 调用；不假定可交换/结合、不并行部分聚合、不复用其输出键。

`join_spool` 建议抽出的受控接口：

- `prepare_keyed_input(records, *, fields, services, arena) -> KeyedRelation`。
- `join_keyed_relations(left, right, *, left_keys, right_keys, how, suffix, occurrence_id, arena) -> KeyedRelation`。
- `KeyedRelation.iter_groups(*, ordered, provenance_order)` / `iter_records()`。

`KeyedRelation` 只能由当前区间创建，绑定 plan/attempt/store 与不可变原输入，不接受业务 dict 冒充证明。初版仅本 worker 使用；不扩跨 worker 的 opaque carrier。

## 6. 准入、恢复与失败边界

不能把“一个 worker”当成内存证明。实现先列叶 payload frame、当前 decode、合并前后 row、引用游标、index、reducer initial/current/new state 的同时存活集合；按该集合核 profile，实际增长前在同一 stage 窗口检查。引用 metadata 与索引缓存亦计入。没有证据的最大值复用禁止。

可通过只保留引用、逐字段/逐行 hydration 与索引溢写复用一份有限窗口；原 256 MiB 测试必须不增额。容量实在不适合优化的图，编译保留原通用执行，而非改变业务支持集。不得在输出已出现后改路由或重建物理计划来“fallback”。高阶宽/倾斜实例若需退出优化，必须在输入输出前由可验证条件决定；否则按同一受管外部算法完成或明确原有预算错误。

恢复沿用原 ACK image：

- 无用户 reducer 的确定性 join 区间可以从原输入 seal 重建临时索引并跳过已确认出口前缀，保持原 record identity；已确认原载荷不得删。
- 任意 reducer 继续原 `finish_replayable=False` 边界；不能因融合把 active Python fold 宣称可重放。它的 active iterator 未收尾时 checkpoint 仍不 ready；不得用“重复调用 reducer 再丢输出”恢复。
- 每原逻辑节点仍有真实状态/metrics/完成时刻；不在仅仅绑定 lazy input 时提前报告 join finished。只有实际计算的行才记 rows，不能把估算的笛卡尔乘积装成观测。
- 本次若不能在该边界保持原失败/恢复行为，就缩小优化范围并保留未闭环项，不能为 19 项 query 数绿化改恢复契约。

## 7. 精确生产文件接缝（待授权）

| 文件 | 修改内容 |
|---|---|
| 新 `planning/dataflow_relational_regions.py` | 数据类、键 lineage 资格、区间解释、实际活对象窗口与同资源 fusion。 |
| `planning/dataflow_place.py` | 在既有有限后缀融合之前调用一次上述 planner；原 placement/host/control grant 继续校验。 |
| 新 `execution/kernels/relational_region.py` | 同键引用区、区间 cooperative 执行、真实 per-occurrence 计量和原 image 的附属 state。 |
| `execution/kernels/join_spool.py` | 复用/抽出 keyed input、reference-only join；单 join 保持旧调用适配，不能双份实现 typed key 与null逻辑。 |
| `execution/kernels/relational.py` | 抽出已分组 sequential fold/merge 输出公共内部帮助函数；普通非区间入口不变。 |
| `execution/kernels/operator.py` | 端口提供已有 `for_join` 迭代器；区间成员的 finite finish/image 绑定，保留原 validator/lifecycle/预算。 |
| `execution/worker/dataflow_graph_runner.py` | 实际 sealed stage 区间识别；内部边不落第二份 payload；外部 EOI 门、bounded advance、metrics/finish/close。 |
| `execution/worker/dataflow_execution.py` | 仅在 kernel 构造/restore 后把真实成员绑定同一区间；不改资源和恢复权威。 |

`dataflow_finite_input.py` **预计无需新增机制**，直接消费 87cc0c3 的 proof/projection；若抽取 helper 需其 1 个 import 接缝，实施前明确，不复制校验。

预计测试：迁移原 `tests/test_local_join_chain.py`；新 `tests/test_runtime_v3_relational_regions.py` 负责边界、恢复与效率因果。需要独立只读 receipt helper 时复用已有原 v3 helper。

## 8. 验收替换：值、顺序、多重性、效率同验

19 个原 selector 全保留，不用 datafusion 假统计、不删除测试、不仅检查 stage 名。

1. 16 种 how 组合：小型独立 Python oracle 明确 null-no-match、重复笛卡尔积、semi/anti 左多重性、左右碰撞/suffix、左未匹配字段缺省；非交换 reducer 的每组完整 sequence 与原来源次序逐行相等。继续跨默认和 local 配置对比，但该对比不能当唯一 oracle。
2. 同键双侧 join 树（thread/process）：原 logical 节点全部在 seal 中，符合资格者同一 region；实际叶键准备次数/rows、payload 编码字节与引用中间字节由真正执行点记录。每输入只准备一次，中间 full-payload writes 为 0；不是只数“一个 region”。
3. map 改键：该真实 map 在 region 外正常执行，后续输入被重新计键/分桶；输出按改后键匹配，不能沿用旧证明。不可要求沿用旧“恰两个 DataFusion 查询”的数值，但必须证实区间边界与真实失效/重算发生。
4. reducer 输出 k=2：后续 join 必须使用可见输出 k=2，原 k=1 的分组证明失效；输出仍为 `{'k':2,'v':'matched'}`，真实重算计量大于零。
5. 因果对照：同一真实数据/原 grant、优化启用与 `fusion=False`；完整值/多重性/组内顺序相同，差异必须是实际 payload 编码/重分区次数下降，而非 wall-time 阈值或伪造 stats。普通 join 没有新增最终全局排序。
6. 小预算宽行、单键倾斜/重复扩增、总 spill 耗尽、低 FD、取消、外部输入末尾损坏：超限前拒绝/背压，失败无成功 ACK；关闭后 owned scratch/leases 清理，原 confirmed refs 保留。
7. 真 ACK 后中断恢复：原 RunRef、原 seal、不变 dag/plan/输入；零重复已确认结果。含非交换有调用痕迹 reducer，证明已确认 fold 不被无声重放；当前 attempt proof 不能跨恢复直接复用。
8. 最小 Local 与私有 Ray Core 各跑同一链，保证优化没有偷偷绑定 driver 内存或原 Local/DataFusion 执行器。

上述计量优先用原 kernel image/per-node progress 承载真实计数与原 journal 对账；如新增 metrics 字段，必须沿现有字段校验接入，不能单开运行数据库。采样 RSS 只能作为辅助证据，不当硬预算证明。

## 9. 实施前需总控的一次裁决

建议批准上述 8 文件范围，在合入 87cc0c3 后做实现；不要只授权改 stats 或测试。先完成受控引用区/窗口与组内顺序，再接原 worker，再迁移 19 项，最后真实恢复与 Local/Ray；每部分共用既有 typed codec/finite proof/原 store，不拆出重复 authority。

这比“19 项旧名字过时”大，核心剩余风险是实际窗口与非 replayable reducer 的组合。这里已给出保守边界；在实现和实测之前，不声称它已满足性能或恢复要求。
