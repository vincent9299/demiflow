# 同键 join/reduce 原生复用：实施与验收交接

本任务范围已完成：**48 项 Local、1 项真实私有 Ray Core，49 项全部通过**。保留原 `test_local_join_chain.py` 19 个 selector，改用独立业务 oracle、真实封存图和原 ACK 中的实际工作量；没有填造 `datafusion` 统计、删除失败用例或恢复第二执行器。本文不代表整个平台完成度或全局性能达标。

- 工作树：`/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_native_join_reuse_20261117`
- 分支：`engine/gpt-native-join-reuse-20261117`
- 原基线：`1c2292e8d409bff20105fd34ae7c7c59255d8a33`
- 消费 performance 的 `87cc0c301504715b451e68c11339c80c3c788ff5`，本树对应 cherry-pick `626be09`。
- 最终生产/测试验收 HEAD：`1f6f0f7096f57d4b0dfdfbb0b79e6049c8a73a25`。
- 本文和配套 JSON 在测试结束后补充，不改变受测生产/测试字节。
- 配套机器证据：[JOIN-CHAIN-REUSE-CLOSURE-20261117.json](JOIN-CHAIN-REUSE-CLOSURE-20261117.json)。其中保存全部 selector、147 条原 setup/call/teardown、四份回执摘要、12 个变更生产/测试文件摘要、承重七件摘要和历史红回执。

## 1. 对意图的实际交付

公开 API 继续使用原 Dataset：`left.join(right, on=...).join(last, on=...).reduce_by_key(...)`。符合条件的同键有限关系区间会在**原物理计划中**成为一个显式 `relational-v1-<digest>` stage，原逻辑 occurrence、输入来源、输出值、多重性、reducer 调用次序保持。

叶输入继续经过原 finite seal 和 attempt-bound proof。它的 typed key 只准备一次，原文档一次写入本区间的受管 scratch。中间 join 用引用表示原左右行、原 occurrence 和输出序号，继承已有 key hash/分区；业务 payload 不再通过内部 `_SpoolPort` 重复落盘。末端 reduce 仍使用原排序器和顺序 reducer，只排序 key/offset/provenance，不复制宽业务 payload 到排序 runs。

产品侧仍能看到原 logical occurrences、真实输入/输出计数和原封存 physical stage；没有第二份图或单独运行日志。平台侧仍是原 worker、RunArtifactStore、allocator、OperatorKernel image 和 driver 提交权威。Local 和 Ray Core 解释相同的显式 stage，Ray 验收主动阻止 `ray.data` 导入。

换 key、经过任意 map/reducer 输出、共享分支或隔离/资源要求时，复用区间终止，继续原通用计算。两个 thread/process 测试的 `change=False` 与 `change=True` 这次都真正执行；改键路径断言原 hash 边跨 stage，且值由独立 oracle 核对。

## 2. 最终调用链与文件权属

`compile_physical_dataflow → fuse_relational_regions → 原 physical seal/assignment → interpret_region → RelationalRegionRuntime → 原 GraphRunner/OperatorKernel.finish → 原输出/提交/ACK`。

| 文件 | 最终改动 |
|---|---|
| `planning/dataflow_relational_regions.py`（新） | 冻结 `RelationalRegionInput/RelationalRegionPlan`；资格、深度和同键证明；显式 stage ID；实际活对象窗口；历史 stage 严格解释 |
| `planning/dataflow_place.py` | 在原 generic fusion 后、finite suffix fusion 前增加一次 region 编译调用；不改 graph adapter 的接缝 |
| `execution/kernels/relational_region.py`（新） | 原 scratch 上的有界行引用、四种 join、原 reducer、原排序窗口、真实工作量；semi/anti 保留原左 schema |
| `execution/kernels/relational.py` | 原 sequential fold 抽为 `reduce_ordered_groups`，原通用入口继续调用它；不改 reducer 结合性/重放语义 |
| `execution/kernels/ordering.py` | 私有 frozen `SortWindow` 可选参数；原调用默认 `None`；实际 run/reader/merge/codec 窗口；仅临时 key/offset carrier 使用 `key_only` |
| `execution/kernels/operator.py` | 原 finish 委托显式 region；根 image 存实际 counters；原游标/skip/finish_replayable 契约保留 |
| `execution/worker/dataflow_execution.py` | 在原 kernel start/restore 后绑定已封存 region；不在旧 sealed stage 上自行推断优化 |
| `execution/worker/dataflow_graph_runner.py` | 原 finite EOI 后推进 region；真正算完内部关系才报告其计数和 EOI；复用原 active finish、暂停、关闭和 checkpoint |

没有修改 `dataflow_finite_input.py`、`join_spool.py` 的 prerequisite 实现；直接复用 `for_join`、原 typed codec、`_Spool/_pairs` 与原 staging 准入。没有新 journal、allocator、提交器或恢复协议。

本轮新增的唯一原审批清单外接缝为 `ordering.py`，已获总控明确同意。最终共 8 个生产文件变更。测试变更为原 19 项文件、一个只读原 seal/ACK helper、7 项 region 验收文件和 1 项 Ray 文件。

## 3. 实际资源窗口，按同时存活阶段解释

记 `R=max_row_bytes`、`B=max_batch_bytes`（原契约 `B≥R`）、`S=max_state_bytes`、`d≤2`，`M=min(4MiB,max(4KiB,min(max_control_bytes,max_resident_bytes)/16))`。

join-only 计划请求 `B+(d+4)R+M`。带 reduce 的计划请求 `B+(d+9)R+S+M`；该值还必须与原 control reserve 一起装入原 run grant，并装入一个真实 host，才允许优化。不能通过增加 run budget 或把多份同时存活对象当作 `max(profile)` 来通过。

| 阶段 | 真实同时存活对象与上限 |
|---|---|
| 输入准备 | 一个原 finite iterator/当前 typed record/文档/当前 key；叶分区 `_Chain` 只保 offset。原 `_Spool` 的 row window 检查先于 scratch append；原 store 在增长前准入 |
| join | 一个 `_pairs` 的有界索引（`R/4`）和引用；hydration 遍历深度至多 2。二叉合并最坏同时保留左兄弟、当前 pair 和 merged result，至多 `d+2` 个 row-sized 值；外层另留 codec、输出和原 batch 窗口 |
| reduce 的排序构建 | 专属 `8R`：`2R` retained run + `6R` 当前 key/record/codec 临时窗口。retained run 实际累计 encoded frame、decoded comparator key/provenance 的闭合 builtin 树及容器，使用 `sys.getsizeof`，不再仅算 `len(encoded)+256`。外层 hydration 父行在 key carrier yield 前释放 |
| reduce 的排序合并 | 同一个 `8R`：`2×(3F+C)+2F≤8R`，`F=(8R−2C)//8`，`C=min(64KiB,max(1,R//64))`。每 reader 的 frame、decoded key、buffer 与实际 chunk 同时计入；另两份 frame 覆盖 heap/output/codec 临时量。实际 fan-in 是该内存上限与原 FD 剩余量的交集 |
| reduce fold/output | 排序 reader 仍活着，因此它的 `8R` 不与 hydration/state 重叠复用；另留 `(d+1)R+B`（至少 `(d+2)R`）、`S` 与 `M`。保持原顺序 callback、initial deepcopy、state 序列化大小检查和输出预算 |

`SortWindow` 全部参数验证为正整数、不得超过原 Bounds。原 store 对象没有被代理或另建。显式窗口同时传入 build、merge 和读取；`_frame_bytes` 对 key-only 按已准入 frame window 编码，不能再从变小的空 payload 推导 key 上限。超过窗口时明确报预算错误，不截断、不临时扩大或切换已封存执行计划。

这些是原资源契约内的行/编码窗口与实际保留容器计量，**不是 Python RSS 的数学硬上界**。任意用户 callback 的 Python 分配、对象实现差异仍受原 runtime/RSS 保护；本文不将 serialized row size 等同于进程实际 RSS。

## 4. 原 ACK 读取到的实际工作量

下表来自真实运行的最终 ACK image，经原只读 StateArtifacts/RunArtifactStore 解码，原始提取为 `/tmp/demiflow-join-region-final-measurements.json`，完整值已写入配套 JSON。

| 场景 | logical→physical | region 实际内存请求 | 叶 key 数/分区建立数 | 中间 payload bytes | 引用 bytes | 临时校验编码 bytes | sort runs |
|---|---|---:|---:|---:|---:|---:|---:|
| 默认小链 | 7→5 | 222,298,112（212MiB） | 8 / 3 | 0 | 1,340 | 1,040 | 1 |
| R=4KiB、400-byte key | 7→5 | 151,552 | 36 / 3 | 0 | 53,360 | 49,600 | 23 |
| R=16KiB、6KiB key | 7→5 | 507,904 | 36 / 3 | 0 | 604,784 | 601,024 | 33 |

全部 `intermediate_key_rebuilds=0`。普通 join 没有新增全局排序；只有原 reduce 的连续分组需要排序。

**保留的开销**：中间引用仍复制 key bytes，所以宽 key 的引用体积可以大于叶 payload；中间输出仍作瞬时编码，以保留原行大小/字段碰撞/输出预算检查。没有将其宣传为零拷贝、零编码或已测得某个吞吐倍数。总控明确本轮先收口，key offset 引用留作后续优化。

## 5. 最终固定验收

统一固定 HEAD `1f6f0f7`，四份回执各自 **2,954 个输入摘要逐字节复核一致**，运行前后快照一致。每一 selector 恰好通过原 setup/call/teardown 守卫；无 skip/xfail/rerun/遗漏，147 个主阶段通过。

| 组 | 数量 | 关键事实 | receipt SHA-256 |
|---|---:|---|---|
| region_budget | 7 | 原值/非交换完整顺序；无标记历史不优化；关闭/隔离/预算 fallback；原 final ACK 恢复不重调 reducer；真实 nonfinal 输出 ACK→恢复无重无漏；semi/anti Arrow schema；小预算及6KiB宽key多run；实际2 reader/64-byte chunk、iterator关闭与超限反例 | `46b438e77c40e1960bae04b0ce9e637a6c9d5a0f3c2821966d26ac33c5e8fa78` |
| compatibility | 22 | 原 K009/K010 默认排序2项；prerequisite typed projection/attempt proof20项，含晚到坏字节、五种路由、restore重新验证、schema/provenance和typed值 | `10bcd1081f8322e7fbdcaf474087d00acd6bf1a9aec13c2c427719520d26d1da` |
| historical19 | 19 | 16 how组合；thread/process的两种改key分支；reducer改key真实重分区；独立oracle和真实计量 | `8a21d4b4eb83552b3997bd28ef0ecb5bf2b20852e1fffd4dcf19a4a297de9580` |
| ray | 1 | 新建自有 Ray Core，原同一region和值；真实ACK；同RunRef恢复无新worker；阻止Ray Data导入；最后关闭自有runtime | `f34c95980aafa7b28ed175cbb2fabe75cb34b92178683cc83fadc58055db3733` |

路径均为 `/tmp/demiflow-join-region-history-r6/<组>-000/receipt.json`。四组总 runner 时间约 274 秒；这只是验收耗时，不是性能对比基准。

### 实际启动命令与四域证据边界

下面是原 `functions.exec → exec_command` 工具调用中实际启动命令的**事后抄录**，不是伪造的进程内 environment 回执：

```sh
env PYTHONPATH="$PWD:$PWD/tests" PYTHONDONTWRITEBYTECODE=1 \
 TMPDIR=/tmp/demiflow-join-region-design-r1/scratch \
 DEMIFLOW_RESOURCE_ROOT=/tmp/demiflow-join-region-design-r1/resources \
 DEMIFLOW_RESOURCE_HOME=/tmp/demiflow-join-region-design-r1/home \
 DEMIFLOW_FORGE_DOMAIN=/tmp/demiflow-join-region-design-r1/forge \
 DEMIFLOW_LOCATION_CACHE=/tmp/demiflow-join-region-design-r1/locations \
 DEMIFLOW_REGRESSION_ENVIRONMENTS=local,ray \
 /yzp/zhaozy/yangzepeng/0905/env/bin/python -P -B \
 /tmp/demiflow-join-region-runner-r6.py /tmp/demiflow-join-region-history-r6 \
 region_budget compatibility historical19 ray \
 > /tmp/demiflow-join-region-history-r6.log 2>&1
```

cwd 就是本文工作树。原 `execute_batch` 使用 `env=dict(os.environ)`，随后将三个资源域和 TMPDIR 改为各 batch 的私有子目录；LOCATION_CACHE 没有删除或覆盖，Local/Ray 子进程继承上面的私有路径。Ray batch 显式使用 `env-ray/bin/python`。

**旧采集器只记录三个资源域，没有记录 LOCATION_CACHE**。不能拿回执本身证明第4个字段，也没有事后补写回执。第4域依据是原工具调用命令和该固定源码的环境继承路径；root 已有 `f2869f1` 修正采集器，最终组合验证使用新四域采集器。无须将本次旧回执重解释成含有它实际未记录的字段。

## 6. 保留的红证据与不混淆的结论

1. 旧19红并不证明19条语义都已过。两个 thread/process 原先在 `change=False` 后的旧统计断言短路，`change=True` 没跑。本次两个分支均有独立结果验证；原设计记录已纠正这一点。
2. r4 `913d71d`：3红/4绿。新窗口导致默认完整region退出reduce；新小预算fixture把SQLite控制库限到64KiB；低层sort fixture用了错误kind。保持回执，逐项修正；没有把失败改为预期拒绝。
3. r5 `8c2642f`：6绿/1红。新压力例的列表式reducer结果合法payload小于4KiB，但原 terminal 把包含 provenance/escaped document 的封装也限到4KiB而拒绝。精确 nodeid 为 `test_region_small_budget_multirun_wide_key_nonassociative_fold`，回执 `/tmp/demiflow-join-region-history-r5/region_budget-000/receipt.json`，SHA `07e9f11f88625a84c477dc3908412cae89ffc5f5a9bbc98eb17ab73e5f9b53b4`。原输入仍在该commit及tmp run。
4. root 已将 terminal frame/payload 真缺口交 historical_finish 独立修复，本lane不跨写 `dataflow_sinks.py`。排序压力验证用相同输入、多run和宽key改成固定大小非交换fold，独立闭式oracle验证顺序；已有完整tuple顺序例保留，绝不将terminal缺口宣称已修。

## 7. 范围与合流说明

- 初版优化限定单分区、CPU、pure、有限输入、深度≤2、同键、原stage可合并的区间。已被其他通用 fusion 合并的情况、预算/host不够、隔离等情况保留通用能力，不计作复用优化完成。没有宣称所有深度、所有分布式大join都已优化。
- 任意 reducer 不因region变成可重放；仍沿原 `finish_replayable` 规则。纯join的nonfinal恢复重建原有限引用并跳过已ACK输出，未重跑业务UDF；final ACK恢复直接读原提交，不再执行reducer。
- 没有做整个平台、真实多机或GPU的性能验收，本任务不声称这些已完成。
- 承重七件逐字节等于 `1c2292e`，摘要见JSON。未写/测试主仓，未push，未停共享服务，未改共享环境。
- 原设计文档保留为历史提案，最终符号和行为以本次代码、本文和验收为准；没有为了凑设计中的抽象类再新增无用层。

在已经消费 `87cc0c3` 的 root 上，生产/测试提交顺序为：

```text
47bbed7 1cbaaa0 26bb1d3 0d7ad84 72dc93f
913d71d 4c78b40 e781e1f 8c2642f 1f6f0f7
```

`dataflow_graph_runner.py` 仅合本lane的 region 绑定/推进/计数符号，不覆盖root的 source EOF、quantum、terminal/finite封装修复。`dataflow_place.py` 仅新增 region 调用，不覆盖 performance 的 graph adapter `hosts=hosts`。`operator.py` 只合 region finish/image 接缝；其他 owner 的共享actor/回调改动保留。原设计commit `3a9309e` 与本交付文档commit独立，可按文档需要带入。
