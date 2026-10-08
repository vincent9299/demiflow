# 单机分区执行内核

入口为 `demiflow.data.local_execution`。业务仍使用原有 Dataset reader、map、join、
reduce 和 writer；执行逻辑在平台 `execution/local_kernel.py` 与 `local_tasks.py`，
不在 subset 或其他业务目录维护第二套数据处理过程。默认执行器不变，新内核显式启用。

## 执行过程

```text
固定版本 Lance 分片 / 有界输入批次
       ↓ 并行读取、map/filter/flat_map 融合
       ↓ stable_hash(key) % partitions，本地缓冲与溢写
       ├─ 分区 0：join → 同 key reduce/group
       ├─ 分区 1：join → 同 key reduce/group
       └─ 分区 N：join → 同 key reduce/group
       ↓ 协调端按既有规范键序归并
       原有 action / Lance writer
```

1. 多分片 Lance 的任务只传固定 URI/version、分片和投影列等扫描描述；worker
   自己打开来源，并检查 schema、存储配置和分片清单。任务按来源分片顺序收集，
   不按 worker 完成先后重排数据。单分片、有限量或向量检索仍由协调端读取有界
   Arrow batch，交给 worker 解码，不把全表转成 Python list。
2. map 等连续窄变换与分区交换融合。每个 key 的全部记录进入同一散列分区，
   每个分区可独立执行；一个 action 共用同一线程池或 spawn 进程池。在途未完成
   计算任务最多 `2 * workers`，输入生产者受背压约束。
3. join 先读取右侧并建立有界 Bloom 过滤器，inner/semi 可先排除左侧不可能命中
   的记录。右侧单列键为同类型字符串、整数或布尔值且键集满足工作预算时，还建立
   共享 Arrow 键表；左批次键类型相同且没有前置不透明回调时，用 Arrow membership
   过滤后才解码宽行。其他情况回退到 Bloom；假阳性仍须经过精确 join，不能直接
   当作命中。过滤不会移到可能改写 key 的用户函数前面，left/anti 不做该预过滤。
4. 分区内复用已有关系内核：小右侧哈希索引，大侧有界稳定排序，超预算热键溢写。
   相邻同 key join→reduce/group 在分区内融合；任意用户 map/filter 隔开后重新
   分区，不能假设用户没有修改键。reducer 是有序左折叠，不假设它满足结合律。
5. 最后归并分区输出的内部键，保持旧执行语义。reducer 即使删掉或修改键字段，
   也不会损坏本次输出的内部排序；后续新分组重新解释实际输出键。

## 配置和生命周期

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `workers` | 4 | 池内 worker 数，受平台 `max_parallelism` 上限约束 |
| `worker_mode` | `process` | `process` 适合 Python CPU 计算；`thread` 适合 I/O/释放 GIL 的函数 |
| `partitions` | `workers * 4` | key 交换的分区数，最多 256，不等于同时运行的 worker 数 |
| `batch_rows` | 8192 | 单批输入行数；Lance 分片任务可流式处理多个批次 |
| `memory_bytes` | 256 MiB | 所有 worker 的分区缓冲/排序工作预算，按 worker 分配 |
| `temp_directory` | 系统临时目录 | action 临时溢写根目录，建议有充足空间的本地 SSD |

关系算子的 `chunk_bytes` 仍有效，分区内取它与每 worker 工作预算的较小值。
预算不是全进程 RSS 上限：Python 对象、进程运行时、Arrow 批次、归并缓冲、待返回
结果、用户 accumulator 另占内存，单条大记录也可超过预算。分区数增加不能拆分
同一个热 key 的 reducer。输入为任意 Python generator 时，generator 仍在协调端运行。

每次 action 创建并关闭自己的池；context 负责绑定 executor，不跨 action 缓存计算
或维持常驻 worker。小任务可能因启动/溢写更慢，正式阶段表仍负责复用与恢复。
进程启动使用 spawn，Arrow CPU/I/O 线程数在每个子进程中设为 1，减少嵌套线程池。

脚本内在 `if __name__ == '__main__':` 保护的函数中进入 context；Jupyter 可以
直接进入 context，cloudpickle 传送用户函数。图的两侧都须在同一 context 内建立，
action 也必须在其中执行。不能覆盖已绑定的 Pipeline Driver 或嵌套 executor。
模型/async、全局 sort/random_shuffle/repartition 等未覆盖算子应使用既有执行路径；
本版明确拒绝它们，不悄悄降级。`map_batches(zero_copy_batch=True)` 也明确拒绝。

用户回调只能依赖当前输入和显式参数，回调实例按任务隔离。全局可变对象、驱动端
列表或计数器不会自动同步；多个 key 间的调用先后没有承诺。既有 pipeline 的进度
map 若依赖共享对象，应迁移到框架任务统计后再用于正式多进程运行。subset 隔离
回归验证了选样统计和所有输出字段一致；生产 notebook 目前仍用原执行模式。

`session.stats` 返回最近一次 action 的快照：状态、耗时、实际 worker 标识、在途
任务峰值，各阶段的已提交/完成任务数、输入/输出行数和 worker 累计工作秒数。
输入/输出数在任务结果被协调端收取后增长，不是逐行实时计数；各阶段包含重复处理
的数据，不能相加当作源总行数。`worker_seconds` 可大于墙钟时间，不代表单阶段延迟。
最终计时覆盖任务池回收；运行时的完成状态不可当作业务 writer 已成功提交。

失败传播原异常，不自动重试任意 UDF。进程模式失败/提前关闭时终止本 action 拥有
的子进程，再清理临时目录；线程模式等待运行中的回调返回，Python 不能安全强杀
线程。沿用正常 Lance writer 的提交边界，worker 异常不会发布一份部分输出。

## 性能测量

运行 `python benchmarks/local_kernel.py --mode process --workers 4 --rows 300000`。
每轮在临时目录创建合成元数据，固定版本读取，不访问公共数据、不下载图片。
`--mode legacy/thread/process` 运行相同 Dataset 图。另一个低命中率 join 场景：

```bash
python benchmarks/local_kernel.py --mode process --workers 4 --workload join_reduce --rows 1000000 --match-stride 37
```

本机 2026-09-27 测量每种模式重复三次，完整逐次结果及输出摘要保存于
[LOCAL_KERNEL_RESULTS.json](LOCAL_KERNEL_RESULTS.json)。耗时包含 action 内 worker
启动/退出、读取、计算、溢写和结果收集，排除合成数据生成及输出摘要计算。
每个场景的三种模式、所有重复必须得到相同 SHA256。共享机器上的合成测量不能
直接推算 1,800 万图片账本的生产加速；并行扫描收益也取决于 Lance 分片布局。

| 场景（3 次中位数） | 默认路径 | 4 线程 | 4 进程 |
| --- | ---: | ---: | ---: |
| 30 万行 Python CPU map（每行 200 次运算）→reduce | 8.811 s | 7.210 s | 4.401 s |
| 100 万行宽表 join→reduce，约 2.7% 命中 | 3.331 s | 0.791 s | 2.682 s |

CPU 场景用进程约 2.00 倍；低命中 join 用线程约 4.21 倍、进程约 1.24 倍。
后者同时受益于 Arrow 预过滤、融合和并行读取，不能把全部提升归因于多核；其计算
很轻，进程启动成本更明显。两组结果支持按负载显式选择模式，不据此自动切换用户代码。
环境为 Python 3.11.16，主机报告 192 CPU，cgroup CPU 配额 32 核；这些是本次
测量环境，非平台最低要求。

`parent_peak_rss_kib` 是父进程峰值，包含计时外的造数，不包括子进程，不能用它
比较总内存。本轮不宣称新版降低了峰值 RSS。

## Rust 接入边界

目前是 Python 参考实现，不含 Rust 内核。`KeyedSource/PlanInput` 描述数据图，
`TaskScheduler` 负责任务和生命周期，`_map_task/_shuffle_task/_key_task` 负责分区
计算；这些职责分开后，原生实现可替换分区函数，而业务 Dataset 编排保持不变。

优先替换稳定键编码、分区交换、哈希 join、排序/归并等原生数据处理；Python UDF
仍须显式调用 Python 或使用原生批算子。当前内部溢写使用 pickle，尚非跨语言 ABI；
接 Rust 前需将相关块协议升级为 Arrow IPC/C Data，固定 key/null/重复键/顺序语义，
用同一组语义测试对照，不能把更换语言当作任意 Python 回调自动多线程化。

## 验证

`tests/test_local_kernel.py` 覆盖真实线程/进程并发、有序非结合折叠、复合与异型键、
空值、多对多 join、组批、同 key 融合和失效、固定 Lance 版本/投影/多分片顺序、
Bloom 假阳性、提前停止、异常回收与 writer 不提交部分结果。subset 的隔离集成
测试比较新旧内核下的全部选样统计及两张表的完整原字段。

本轮平台全套 355 项、业务 subset 加统一 pipeline 布局检查 73 项通过。
另通过 `demiwtg` Jupyter kernel 实际执行 notebook 中定义的函数和 lambda，确认
两个 spawn worker 参与计算、结果正确并完成资源回收；未运行生产 notebook。
平台测试仍输出原有 store 测试的 Lance fork warning，以及另一个模型服务启动失败
测试的异步异常日志；进程分区内核使用 spawn，本次不改动模型服务/异步流实现。
