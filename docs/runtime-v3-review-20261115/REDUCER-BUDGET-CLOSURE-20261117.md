# Reducer 驻留预算修正与验收交接

本次修正覆盖**普通 reduce 与同键 join/reduce region 的已知平台副本**。固定 `4cf4069` 的 59 项组合全部通过；随后 `c214b83` 仅补未知 ndarray 子类的窄拒绝，2 项定向通过。合计 60 个唯一 selector、61 次执行、183 个完整 setup/call/teardown。没有把不同提交的结果冒报为最终 HEAD 的一次全量测试。

原 [49 项功能交接](JOIN-CHAIN-REUSE-CLOSURE-20261117.md) 的业务值、恢复、复用证据仍有效，但其单 `S` 预算公式确实少计，**不能作为最终预算证明**。本文件替代那部分结论。配套 [机器证据](REDUCER-BUDGET-CLOSURE-20261117.json) 保存每项 selector、五份回执摘要、各 2,956 个输入摘要与受测提交逐字节核对结果、四域进程内环境、最终十个生产/测试文件摘要和承重七件摘要。

- 工作树：`/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_native_join_reuse_20261117`
- 分支：`engine/gpt-native-join-reuse-20261117`
- 本次比较基线：`21fde4b`，原 49 项交付文档提交。
- 组合受测 HEAD：`4cf4069192cc403d69bf5978143fd3014c074ecc`。
- 最终生产/测试 HEAD：`c214b833aa6379d97761ca8c7c92a9a50dea8d8a`。
- 本文补充在测试完成之后，不改变受测生产/测试字节。

**合流接缝仍由 root 完成**：本树提供 `OperatorKernel.retained_resident_bytes`，没有修改 frame owner 正在维护的 GraphRunner/终端编码消费侧。消费侧必须把该值传入原 frame 编码的 `retained` 参数，不能借用同一份 grant 两次；这条跨文件组合验收由 root 负责。本文件不把它宣传为已经在本树完整验证。

## 1. 原缺陷与最小修正

真实旧反例位于 `/tmp/demiflow-region-state-audit-20261117`，`result.json` 记录源码前后不变。原公开 Dataset 的两个 join 加 reduce，在 1 MiB run、`R=4 KiB/S=256 KiB` 下获得 376,832 字节 stage grant；worker 的初值列表、deepcopy 列表、callback 新列表实际同时保留 832,360 字节，仍然成功 ACK。另一个原 fold 反例同时保留初值/状态/pickle 编码副本。它们是平台已知持有对象，不属于可以忽略的用户临时分配。

修正采用原 planner、原 stage grant、原 sorter、原 reducer 顺序、原提交/ACK。没有第二个 allocator、journal 或 executor，没有增加默认 256 MiB，也没有削弱原 19 条 join 的业务值、顺序、多重性或改键断言。

新 public reduce kernel 封存 `reducer_memory_v1`：实测初值窗口 `I`、初值复制窗口 `C`、复制辅助空间 `W`、状态实际驻留窗口 `A`、有界遍历空间 `M`/节点数。`S=max_state_bytes` 仍表示原 pickle 序列化字节上限，**没有把 S 悄悄改为堆大小**。实际状态堆图另受原 resident grant 内的 `A` 约束；序列化满足 S 不代表有权超过 resident grant。

`None`/bool 是进程常量，不为 accumulator 额外分配初值树；默认 `None` 的 `I=C=W=0`。存在大初值时，在 worker 启动前以实际闭合对象树和保守复制余量参与原物理准入；worker 加载后再核对真实初值，不相信 driver 对 pickle 重建容器容量的假设。

## 2. 函数与调用链

| 文件/符号 | 责任 |
|---|---|
| `planning/dataflow_compile._Capture._capture_relational` | 新 reduce 生成并封存 memory-v1，计算原 profile；原 semantic identity 输入保持 |
| `planning/dataflow_place.partition_stages` | 新 reduce stage 使用同一 resident 公式及原 state 上限 |
| `planning/dataflow_relational_regions._describe/region_resident_bytes/interpret_region` | region 身份包含冻结声明；实际生命周期窗口；旧无声明 region 明确拒绝，不运行时猜新优化 |
| `kernels/reducer_memory.declare/measure_tree` | 有界只读对象图测量，引用去重；初值与已知原生 buffer 所有权计量 |
| `resident_bytes/sort_window/check_grant/check_initial/check_state` | planner/runtime 共用窗口；按原 grant 核验；deepcopy/pickle 前检查；保留原序列化上限 |
| `kernels/relational.admit_reducer/reduce_partition/reduce_ordered_groups` | 普通与 region 共用原顺序 fold；前一行在下一行 hydrate 前释放；旧组释放后再复制下一初值；计数式 pickle 避免保留完整编码副本 |
| `kernels/ordering.SortWindow` | build 8R 与 merge 7R 在同一原 grant 内按阶段使用；原 `window=None` 调用保持 |
| `kernels/relational_region._reduce` | 原 key/offset carrier、原 sorter、同一准入与 fold；复用 root kernel 的观测字段 |
| `kernels/operator.OperatorKernel.retained_resident_bytes` | 只读观察 yield 时仍持有的 peer 字节；不持久化、不预留、不修改资源权威 |

## 3. 按同时存活阶段核算

记 `R=max_row_bytes`、`B=max_batch_bytes`、`S=max_state_bytes`。region 的深度 `d≤2`，`H=B+(d+1)R`；普通 reduce 使用 `H=B+2R`。`I/C/W/A/M` 来自冻结声明。请求值为：

```text
I + M + max(
    8R + H,
    7R + max(
        H + R + A,
        3R + 2A,
        3R + A + max(A,S),
        3R + C + W,
        B + 4R + A
    )
)
```

这是阶段峰值的 max；排序 reader 与 fold 同时存活，所以不把两者错误地取 max。以下对象持有关系同时体现在生成器释放边界、实际 sorter 参数与 runtime 检查中。

| 阶段 | 实际同时存活对象 |
|---|---|
| 排序 build | 初值 I、遍历/元数据 M、保留 run 2R、当前 key/record/codec 6R、上游 hydration H；此时还没有 fold accumulator |
| merge + hydrate | 初值 I、M、merge 7R、现有状态 A、当前 hydration H、原 group key R。上游父行在返回当前行后释放；fold 不使用 groupby 额外保留上一行 |
| callback 替换状态 | 初值 I、merge 7R、当前行/两组 key 3R、旧状态 A 与新状态 A；不能只数新状态 |
| 平台 state 编码 | 初值 I、merge 7R、行/key 3R、状态 A、已预留的编码/复制 `max(A,S)`、M。先检查真实状态树，再调用同协议 Pickler；写入计数 sink，不积攒完整 BytesIO。UTF-8/bytearray 大标量的编码副本在构造前检查 |
| 新组 deepcopy | 前组 state 已释放；仍有初值 I、merge 7R、下一行/key 3R、实测复制 C 与复制辅助 W。实际 initial 在 deepcopy 前核验 |
| 输出与挂起 yield | 还持有 initial/current、未耗尽 merge、下一组行/key；另留当前输出与编码窗口 `B+4R`。动态 peer 测量排除当前输出 Record 的可达对象，initial/state/next-row 之间同引用只计一次。正常、异常、close 归零 |

排序实际 merge 约束为 `2×(3F+Cchunk)+2F≤7R`，`F=(7R−2Cchunk)//8`、`Cchunk=min(64KiB,max(1,R//64))`；原 FD 上限继续取交集。build 的 retained run 真正累计 decoded comparator key/provenance 与 encoded frame，不能只算 encoded bytes。原 store 未被替换或代理。

对象遍历有节点、深度、字节界，不调用用户 `__sizeof__`/属性来猜对象大小。闭合 builtin 树以及 exact 非 object ndarray 的已知 buffer 是本次已验证域。NumPy view 会沿真实 `.base`/`memoryview.obj` 找到仍被持有的原数组或 bytes/bytearray，base 和 view 同树只数一次；脱离 owner 的 copy 不再计原 buffer。未知 ndarray 子类在 Pickler 前明确要求 bounded state adapter，不能落入小 header 计量分支。

**保证边界**：这不是任意 Python RSS 的数学上界。用户 callback、自定义 deepcopy/reduce hook 的未知临时分配与本次平台已知副本分开。其他历史 opaque C 扩展状态没有在此补丁中统一改成拒绝；其未显式适配的 native buffer 尚未被这套测量证明有界，不能宣称已获支持保证。原 runtime/RSS 保护继续保留。

## 4. 历史计划处理

缺 memory-v1 的旧 unfinished generic reduce，如果存在真实 assignment stages，会从原加载 initial 和原 bounds 进行**纯瞬时测量**，检查原 sealed grant。够用时使用原 ID/profile 执行，sort/fold 的子窗口也只来自该原 grant；不够则在输入消费、deepcopy、UDF、pickle 之前拒绝。没有向旧 seal 写新字段、编新 ID 或借新空间。

旧 region 缺新声明不能按新语义猜执行，明确报 `INCOMPATIBLE_HISTORY`。已最终 ACK 的 action 仍先读原提交回执，不重新进入 reducer。原非可重放 reducer 的恢复限制不变，不能因为 region 复用而假定 reducer 可重放。

真实原 ArtifactStore 的正反例锁定：旧 64 KiB grant 在 0 输入/0 copy/0 UDF/0 pickle 时拒绝；旧 512 KiB grant 输出原 `n=2`，一次根初值复制、两次原状态序列化，原配置/ID/profile 全不变。原 final ACK 恢复测试仍证明 reducer 不被重复调用。

## 5. 真实封存计量

来自 `4cf4069` 实际 sealed physical plan、原 ACK image 和真实 worker 写出的 callback 测量；完整值在机器证据中。

| 场景 | 原 run 上限 | 新 stage resident | 初值 I / 状态 A | 真实已知并存对象 |
|---|---:|---:|---:|---:|
| 默认 None 两 join + reduce | 256 MiB | 222,298,112（212 MiB） | 0 / 16 MiB | 原完整 tuple 顺序结果保持 |
| R=4 KiB、400-byte key、S=32 KiB | 1 MiB | 172,032 | 0 / 32 KiB | 原多 run 与非交换独立 oracle；旧值 151,552 的缺账已修 |
| R=16 KiB、6 KiB key、S=32 KiB | 4 MiB | 507,904 | 0 / 32 KiB | 宽 key 正例、真实多 run |
| 近 S 初值：普通/region | 4 MiB | 1,922,220 | 324,604 / 647,256 | 初值/deepcopy/新列表合计 796,520，三者 id 不同；同 fixture 在 1 MiB 早拒绝 |
| None 初值：普通/region | 1 MiB | 630,784 | 0 / 262,144 | 旧/新 bytearray 合计 516,210；原 376,832 grant 不足 |

没有改业务结果来降低内存。默认 None 不一律多收一个 S；大初值真实需要的空间不会通过缩小计量常数掩盖。

## 6. 验收与证据

| 受测提交/组 | 通过 | 主要覆盖 | 回执 SHA-256 |
|---|---:|---|---|
| 4cf4069 / budget_v2 | 19 | 新 8 项内存/旧历史边界；原 public reducer 1；region 7；原 sorter 2；原低层 K006 1 | `55b9be088dff62110651491b4b70284836339bd6865c89efd38ba71b6106169e` |
| 4cf4069 / historical19 | 19 | 原 16 how 组合；thread/process 两种改 key 分支；reducer 改 key，独立 oracle 与实际计量 | `c9885c485b26d9cda7c55e6c69204d0aa07734a52ecf16a9b8bf026db618c878` |
| 4cf4069 / projection20 | 20 | 原 typed projection、attempt proof、晚到坏字节、schema/provenance、路由/恢复重新验证 | `66ee222f3b3e5a8b6eb0d28d614bfe4a22ec45989a6a829b8a7fe049a7bf6d0c` |
| 4cf4069 / ray | 1 | 自建私有 Ray Core；原 region、值、ACK 和恢复 0 新 worker；禁 Ray Data 导入；finally shutdown | `ce95306668addb3ed19217c916ea17ecdbf5b05509891a995907ace0171243c2` |
| c214b83 / array_guard | 2 | 新 ndarray 子类拒绝；原 exact ndarray/view/base 同引用与 frombuffer 正反例再验证 | `c7d967a75c48aff25bf913040a7b1d9f7e4f31aa0106ebd214dca6d692e295ce` |

前四组位于 `/tmp/demiflow-join-region-history-r10/<组>-000/receipt.json`，最后一组在 `history-r11/array_guard-000/receipt.json`。五份回执各 2,956 个输入摘要均与其**实际受测提交**逐字节相符。无 skip/xfail/rerun/遗漏；183 个主阶段通过。最终 guard 的全部 diff 只有 helper 4 行与新增测试 19 行，完整 diff 已写入 JSON。按总控裁定不重复全包，没有冒称在最终 guard 提交跑过 59 项。

测试使用本树 `PYTHONPATH`、`env/bin/python -P -B`，Ray 组使用 `env-ray/bin/python`。新 harness 在进程内记录并向子进程传递四个私有域以及 TMPDIR；每个 batch 都位于其 `/tmp/demiflow-.../<batch>/` 内。未向主仓、共享环境、共享集群或业务数据写入。

保留失败与中间回执：r7 为 8 绿/8 红（新配置浅拷贝的 frozen tuple 错误及新 fixture 缺 store，已修）；r8 16 绿；r9 17 预算 + 19 历史绿，批间停止补历史准入后才启动 r10。没有重写旧红回执为通过。

## 7. 合流顺序与剩余范围

root 已集成原 49 项交付后，依次取：

```text
2f57449 b8b9a48 83e474b dbdd00f
 e5acc66 597967b 4cf4069 c214b83
```

本树 `5ec9b44` 是 root 原 `f2869f1` 四域守卫的 cherry-pick，不重复取。随后取本次文档提交。生产范围为八文件，新增唯一 `kernels/reducer_memory.py`；`dataflow_compile` 只改 `_capture_relational` 的 reduce，root 的 source/terminal codec profile 接缝不在本树修改范围。没有改 GraphRunner、dataflow_sinks 或 frame helper。

承重七件仍逐字节等于 `1c2292e`；原 `tests/test_local_join_chain.py` 的 19 个 selector 与内容在本轮未改。十个本轮生产/测试文件摘要已保存。剩余合流验收只针对 root 的 frame 消费接线和实际共驻峰值，不需要再扩展本 lane 的性能功能。本次不宣称整个引擎、所有 native C 状态或 I08 性能目标已经完成。
