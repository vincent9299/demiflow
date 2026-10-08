# 本地关联与分组性能记录（2026-09-27）

本次只修改 demiflow 通用关系执行，subset 继续原 Dataset 编排。输入为流式
生成的键/载荷，执行 equijoin 后按键聚合计数、行号之和与桶字段，并逐行计算
SHA256。不是原始图片账本吞吐量，也不是全流程耗时预测。

| 输入行 / 右表行 | 实现 | local workers | 用时（秒） |
|---|---|---:|---:|
| 1,000,000 / 100,000 | 修改前 | 4（旧关系实现串行） | 38.70 |
| 1,000,000 / 100,000 | 去重排序/批量 IO/内存组 | 1 | 11.31 |
| 1,000,000 / 100,000 | 再启用排序进程 | 4 | 10.79 |
| 4,000,000 / 400,000 | 优化后 | 1 | 51.59 |
| 4,000,000 / 400,000 | 优化后 | 4 | 45.60 |

相同规模各配置输出摘要一致：

- 百万行：`28d73050f5a14df70f04b164138729cdbb1b4015e7aac53b9f4b5363523c31f0`
- 四百万行：`13a251aa77b95ff6b59cece5f1da00e7b31be2ebc755794e5c0c5145e3450914`

这些是单次测量，运行时同机有其他任务（同规模对照亦曾并发执行），不能作为
统计稳定的加速承诺。百万行约 3.6 倍改善主要来自执行策略；4 进程相对优化后
串行的吞吐额外收益约 13%（耗时缩短约 12%），没有线性提速。父进程峰值 RSS 由脚本报告，不包含
工作进程或整个容器的文件缓存，不能当作总内存。

```bash
../env/bin/python benchmarks/local_relational.py --rows 1000000 --groups 100000 --workers 4
../env/bin/python benchmarks/local_relational.py --rows 4000000 --groups 400000 --workers 1
../env/bin/python benchmarks/local_relational.py --rows 4000000 --groups 400000 --workers 4
# 需自行提供修改前源码，基准不会从网络拉取或恢复仓库文件。
../env/bin/python benchmarks/local_relational.py --baseline-file /path/to/old_local_relational.py
```

正确性验证：demiflow 317 项测试；subset 与布局 57 项测试。新增关系测试覆盖
哈希/落盘路径、复合与 null 键、多对多乘积、跨多轮归并稳定性、嵌套对象隔离、
同键排序复用、回调后排序失效、串并行一致性及失败清理。另在隔离 subset
fixture 上直接运行修改前/后的引擎，concepts 全字段、images 全字段、所选
图片顺序和全部 stats 均一致。生产全量结果仍需单独验收。

设计参考：

- [Spark SQL tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html)：依据大小选择关联策略、广播小表、处理倾斜。
- [DuckDB external sorting](https://duckdb.org/2021/08/27/external-sorting)：块排序、并行归并、编码键和载荷搬运。
- [DuckDB memory management](https://duckdb.org/2024/07/09/memory-management)：有界流处理与超内存落盘。

本次是现有 Python 执行器的局部优化，不等同于以上引擎的向量化、完整优化器
或自适应分区实现。若生产剖析显示键编码/归并仍占主要 CPU，可进一步仅把
这些通用批处理原语移入 Rust；保持 Dataset API、稳定排序和业务回调边界。

## Rust 范围判断

用户已允许必要时实现小范围 Rust 内核。生产进程的附加 profiler 被系统
ptrace 权限拒绝，未修改系统权限或打断任务；另对百万行串行基准使用 cProfile。
该剖析运行约 30.7 秒（有明显插桩开销，不与上表无插桩耗时比较），`key_of`
累计约 10.8 秒，其中 JSON 编码是主要开销；逐行观测 progress 累计约 2.9 秒。
这些是合成关系基准的热点，不能当作真实 metadata pipeline 的耗时占比。

目前没有新增 Rust 依赖。先完成真实任务重试；若继续下沉，应批量传递键与
编码载荷，在 demiflow 内实现键编码/排序归并等小原语。逐行 Python→Rust
往返、直接翻译业务 reducer 都不是已证明有效的优化。采样算法保持在 subset。
