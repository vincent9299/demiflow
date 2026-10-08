# 真实图片向量索引小试

入口 `benchmarks/vector_search.py`。复用固定版本向量，不运行编码器，不修改来源表。
这是有限样本的索引机制/接线实验；正式业务编码继续使用 image_embeddings pipeline。

## 复现

在工作区根目录执行，output 必须是尚不存在的目录：

```bash
PYTHONPATH=demiflow LANCE_CPU_THREADS=4 LANCE_IO_THREADS=2 \
OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 LANCE_LOG=error \
LANCE_DEFAULT_IO_BUFFER_SIZE=8388608 \
env/bin/python demiflow/benchmarks/vector_search.py \
  --source /absolute/path/embeddings_a.lance@1 \
  --source /absolute/path/embeddings_b.lance@1 \
  --output /absolute/path/new-benchmark-output \
  --queries 128 --threads 4 --repeats 2 --timeout 120
```

来源必须具有 `sha256`、`encoder_id`、固定维度 float32 `embedding`，并带有
`image_embeddings.contract` metadata；多来源须契约一致、ID 唯一、向量归一化。
不混合不同编码身份，也不复制或扰动向量来凑规模。
`--cases IVF_RQ IVF_HNSW_SQ` 可限定比较对象；精确搜索基准始终先运行。
`--stream-ef 500 --stream-refine-factor 5` 可额外验证流式算子的 HNSW 精排参数；
这些选项只修改流式检查，不改变下面的原生检索对比配置。

1. 固定源版本；随机种子固定为 20261002，留出 128 个图片向量作为查询，
   其余作为库。两组 SHA 严格不重叠。
2. `use_index=False` 求出每个查询的精确 top-100。
3. 每种索引用独立进程和临时 Lance 表，计时 `create_index`，另记数据写入时间。
4. 统一 cosine、16 个 IVF 分区，对比 nprobes=4、16；量化方案另测 refine_factor=5。
   RQ 为 5-bit、normal；PQ 为 512 个 8-bit 子向量；HNSW_SQ 为 m=16、
   ef_construction=100，查询 ef=max(200, 100×refine_factor)。
5. 串行请求每组查询两轮，记录 Recall@20/100、p50/p95、首次查询时间、索引大小及 RSS。
   top-20 取本次 top-100 返回值的前 20，不是另发一个 k=20 请求。
6. 再通过真实 `Dataset.search_vectors` 执行 128 行、concurrency=4 的接线检查，
   nprobes=16，默认不显式 refine/ef，可用上述两个选项覆盖；单独记录墙钟时间和召回。

`first_query_ms` 是新表句柄的第一次查询；其余计时先预热一次、再混合统计两轮。
不清理操作系统页缓存，因此它们不是严格的磁盘冷/热缓存实验。
串行时间包含 scanner 创建、原生检索和结果转换；不包含查询向量编码。
流式墙钟还包含 Dataset 输入建立、表打开、线程与清理，不能当成单请求响应时间。

## 2026-10-02 小试

使用 image_embeddings 的 perf1024 v1/v2/v3，各 1024 条，共 3072 条同契约
4096 维 WeMM 向量。留出 128 条后，实际检索库为 **2944 条**。
CPU 线程限制 4，I/O 线程 2，无 GPU，pylance=12.0.0+demiflow.arrowfix1。
源码没有固定原生聚类的随机性，单次构建不是多随机种子统计。

下面均为 nprobes=16（遍历全部 16 个分区）、不显式 refine。该设置方便
观察量化/图搜索误差，也说明这次规模不足以评价大库的分区裁剪收益。

| 方案 | 构建秒数 | 索引 MiB | Recall@100 | 串行 p50 ms | 串行 p95 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| 精确扫描 | — | — | 100% | 13.95 | 17.83 |
| IVF_FLAT | 0.80 | 46.26 | 100% | 4.40 | 6.51 |
| IVF_RQ，5-bit | 1.27 | 7.51 | 99.19% | 2.78 | 3.35 |
| IVF_HNSW_SQ | 1.08 | 12.14 | 99.32% | 4.05 | 5.59 |
| IVF_PQ，m=512 | 12.25 | 5.69 | 83.40% | 10.08 | 14.37 |

使用 nprobes=4 时，Recall@100 分别约为 80.18%、80.79%、80.76%、75.59%，
说明分区裁剪造成的漏召回不可忽略。RQ、HNSW_SQ、PQ 采用全分区和 refine=5
后均达到本样本 100% Recall@100，p50 分别增加到 13.42、15.86、20.56 ms。
本例 IVF_FLAT 的索引缓存路径快于直接列扫描，不代表精确计算量减少。

流式算子四并发的 128 行接线检查全部通过；本样本各索引的 Recall@100
与上述全分区、无显式 refine 配置一致。独立进程峰值 RSS 为约 478–988 MiB
（包括 Python、Arrow、平台导入与查询阶段，不能解释为建索引净内存）。

实际记录：

- [主要运行结果](../.validation/vector_search_smoke_20261002_b/results.json)
- [HNSW 补跑结果](../.validation/vector_search_smoke_20261002_c/results.json)
- [源版本、完整查询/库 ID 与划分摘要](../.validation/vector_search_smoke_20261002_b/manifest.json)

主要运行中的 HNSW 精排尝试因 ef=200 小于扩大的候选 k=500 失败；已修正为
ef>=k×refine_factor 后补跑。两个运行的源、划分和契约摘要一致，精确搜索
p50 为 13.95/13.91 ms。初版另有基准文件被摘要覆盖的问题，已通过分别使用
ground_truth.json 和 exact.json 修正；首轮失败记录不作为性能结论。

## 2026-10-03 HNSW 算子验收

本轮选择 `IVF_HNSW_SQ`，沿用上述来源、划分、4096 维向量和 CPU 限额。
运行 `--cases IVF_HNSW_SQ --stream-ef 500 --stream-refine-factor 5`，
流式算子使用索引所在的 version=2、top_k=100、nprobes=16、concurrency=4。
128 行查询全部成功，Recall@100=100%，墙钟 1.13 秒（约 113.5 查询/秒）。
临时索引构建 1.21 秒、大小 12.14 MiB；包含构建与查询的 worker 峰值 RSS
约 539 MiB。源表未修改，临时索引表已清理。

这验证了显式 ef、扩大候选后的精排及流式执行接线，不是百万级性能结论，
也不是在线文本查询的端到端耗时。另有 89 项相关自动测试通过，覆盖默认/显式
ef、候选预算、过滤、空结果、索引后追加、精确扫描和执行器生命周期。
见 [本轮结果](../.validation/hnsw_search_adaptation_20261003/results.json)。

## 资源与结论边界

输入限制为 16 个源、总计 10,000 行、8192 维及原向量 256 MiB；读取批次
128 行，转换前检查保留缓冲。准备阶段会保留拼接、划分等约四份原向量载荷，
单一完整矩阵仅在该声明规模内使用；不支持把本脚本直接用于百万级全量。
每种索引独立进程，线程/亲和性明确限制，索引表在自己的临时目录内，正常
退出或父进程清理时删除；仅保留来源/划分/结果 JSON 和日志。

父进程每 200 ms 监测 worker RSS（4 GiB）、工作目录占用（2 GiB）及 wall time
（默认 120 秒/worker），越界停止自己创建的进程组。这是采样保护，可能短暂
超出阈值，不是 cgroup 硬内存上限；主要保护仍是输入规模、批次、线程和预读预算。
中断、超时或索引失败保留诊断，不将失败当作空结果。GPU 对 worker 明确禁用。

当前先接入 **HNSW_SQ**，RQ 5-bit 保留为后续对照。这些结果不支持按 2944 行
线性推算 200 万行性能，不支持宣称已测得文本找图准确率。下一阶段应使用
10 万以上真实库、独立真实场景文本查询，固定召回目标后比较延迟与资源。

## 已发布素材表的流式验证

`published_vector_search.py` 直接只读检索指定版本的已有索引，不复制数据库，
不重建索引。示例：

```bash
CUDA_VISIBLE_DEVICES='' PYTHONPATH=demiflow \
env/bin/python demiflow/benchmarks/published_vector_search.py \
  --uri /absolute/path/edit_scene_images.lance --version 9 \
  --output /absolute/path/new-validation-output \
  --queries 64 --threads 8 --timeout 300
```

从表内均匀抽取 64 张图片向量作为查询；精确与 ANN 路径均用逐行 prefilter
排除查询图自己的 SHA。它们仍是库内图片查询，可能存在近重复图，不能替代
独立文本检索评测。来源版本、编码契约、抽样位置和 SHA 固定在 manifest。
全表精确搜索作为参考，HNSW 比较 nprobes=8/16/32/64，top_k=100、ef=500、
refine_factor=5。Recall@20 是这些 top-100 结果的前 20 项相交比例，不是另发
k=20 的请求。达到样本 Recall@100≥98% 的配置中选择末轮 p50 最低者再测
四并发；若都未达到，测最大 nprobes 的配置，但不宣称达标。这个阈值只是
首轮参数筛选目标，后续须用独立查询验证，不能当作统计置信保证。
另用选定配置测单/四并发的无过滤路径，避免把实验排除自身的 SHA prefilter
成本混入日常文本找图的速度。无过滤组仅测性能，不与排除自身的基准计算召回。

每组实际经过 `Dataset.search_vectors → map → run_stream`；保留查询标识，
核对输入/输出一一对应、每条候选数量、距离顺序、无重复命中、原生 ANN 执行计划
及 action 结束后的句柄释放。精确搜索跑一轮；ANN 在同一 action 内先跑一轮
查询，再跑两轮乱序重复查询。每轮单独报告耗时、QPS、p50/p95。四并发时相邻
轮次的少量在途请求可能重叠，所以末轮另列；不清空操作系统页缓存，不称为
严格冷启动实验。插桩仅位于基准 worker，不修改生产算子。

算子耗时自 worker 开始调用到候选返回，包含表打开/缓存、原生检索和结果转换，
不包含上游排队、文本编码、图片读取或 VLM 审核。完整 action 的墙钟与 QPS
另报；不能用并发吞吐倒数冒充单请求时延。每个 action 新开表句柄，当前
结果不意味着多个独立 HTTP 请求已经复用了常驻索引缓存。

输入上限 200 万行、8192 维，抽样查询 8–128 条；只将查询向量放入 Python，
每次 take 8 行并检查 16 MiB 保留缓冲。每组独立进程，CPU 限为 1–16 核，
GPU 禁用，索引缓存显式 1 GiB、元数据 32 MiB、每查询 I/O 16 MiB。父进程
每 200 ms 检查 8 GiB RSS、64 MiB 结果文件及至多 600 秒 worker 墙钟，超限
终止自己创建的进程组。该采样保护可能瞬间超限，不能视为 cgroup 硬上限。
原生全表精确计算可能有额外分配，不因使用流式接口而宣称其内存固定。

业务效果另用冻结的真实编辑原图需求，编码文本后取前 10 张实际看图，记录
可用、相关但不适用、不相关或无法判断。统计查询可用率（前 10 张至少 1 张
可用）、前 10 张可用比例及分类型失败原因。检索词描述编辑前的对象/场景，
例如“在桌上加猫”应寻找有可用桌面空间的原图；判断画面是否满足条件仍靠
下游看图。小样本试题缺乏全库人工标注，不计算语义召回率。

### 2026-10-03 编辑原图 @9 验证结果

固定 `curation/edit_scene_images/datasets/edit_scene_images__a170b8529adc30d8.lance@9`，
172,292 个 4096 维向量；IVF_HNSW_SQ/cosine，256 分区、m=16、
ef_construction=200。编码契约 `a170b8529adc30d8…`。64 条随机库内图片查询均
排除自身；8 核 CPU，索引缓存 1 GiB，无 GPU。最终记录在
[edit_scene_search_20261003_v3](../.validation/edit_scene_search_20261003_v3/results.json)。

| nprobes | Recall@20（top-100 前缀） | 平均 Recall@100 | 最低单查询 Recall@100 |
| ---: | ---: | ---: | ---: |
| 8 | 93.67% | 90.00% | 51% |
| 16 | 97.19% | 95.14% | 65% |
| 32 | 99.14% | 98.05% | 76% |
| 64 | 100.00% | 99.41% | 91% |

共同查询参数 top_k=100、ef=500、refine_factor=5。nprobes=64 时仍有 4/64
查询的 Recall@100 低于 98%，不能只看平均数。该轮选用 64 补测无过滤路径：

| 并发 | 末轮算子 p50 | 末轮算子 p95 | 末轮窗口 QPS | 含首轮的完整 action QPS |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 58.64 ms | 69.02 ms | 17.08 | 17.59 |
| 4 | 109.71 ms | 139.79 ms | 36.55 | 31.35 |

完整 action 为同 64 个查询的三轮共 192 次请求；首个调用分别为 187.64 / 433.52 ms。
峰值 RSS 约 1.12 / 1.17 GiB；精确搜索对照峰值约 5.04 GiB、单次 p50 1.71 秒。
共享机器上的时间有波动，不能从 nprobes=64 本轮更快推断其通常比 32 更快。
四并发提高吞吐，但单请求尾延迟增加；尚未进行持续负载或端到端 SLA 验收。

本轮发现原生精确检索可把 top-100 的尾部 36 项排在前部 64 项之前。候选集合
正确，但截取前 20 张会出错。平台已对预算内的 top-k 按 `_distance` 排序，
排序工作区也计入结果预算；新增真实结果分块乱序回归，91 项相关测试通过。
最终基准每次均检查距离有序、无重复、候选数量、输入完整性及句柄释放。
初版计时变量错误、v2 排序问题分别保留为诊断，不使用它们的错误延迟/前缀召回。
v3 才是修复后的完整记录，来源表和索引未改写。

[20 条文本查询及查看标准](../.validation/edit_scene_search_20261003_v3/text_query_pilot.json)
已准备，尚未编码或执行。覆盖添加、移除、替换、状态与关系编辑；接下来用
同编码契约的文本向量实际检索，看前 10 张中是否至少有一张符合原图需求，
再分别统计编码、检索、读图和审核时间。当前结果不构成文本找图质量结论。
