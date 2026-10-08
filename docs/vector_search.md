# 流式向量检索

`Dataset.search_vectors` 是逐行流式算子：每行提供一个查询向量，检索一次，
保留原行并写入候选列表。可以直接连接 `map_embeddings` 和下游审核、出题节点。
底层使用 Lance 原生 `scanner(nearest=...)`，执行器复用已有线程 worker、
有界队列和 actor 生命周期；没有嵌套 Dataset action。

## 使用

```python
from demiflow import data

# model 是与编辑原图素材表一致的 EmbeddingModel；published_version 是含索引的发布版本。
stream = (
    data.from_items([{'query_id': 'q1', 'text': 'a wooden table near a window'}])
    .map_embeddings(model=model, inputs={'text': 'text'}, output='query_embedding')
    .search_vectors(
        query='query_embedding', output='candidates',
        uri=scene_images_uri, version=published_version,
        vector_column='embedding', columns=['sha256', 'image_uri'],
        metric='cosine', top_k=20, concurrency=4, queue_depth=4,
        options={'use_index': True, 'nprobes': 16, 'ef': 200, 'refine_factor': 2},
    )
)
# 在 stream 后继续连接下游节点，最后用 run_stream() 执行。
# 需要普通 Dataset writer/关系运算时，先 materialize()。
results = stream.materialize()
```

输出一行形如：

```python
{
    'query_id': 'q1', 'text': 'a wooden table near a window',
    'query_embedding': [...],
    'candidates': [
        {'sha256': '...', 'image_uri': '...', '_distance': 0.13},
        # 最多 top_k 项，按 _distance 从小到大排序。
    ],
}
```

`query` 是输入行的列名，`vector_column` 是被检索表的向量字段名。
`columns` 必须明确指定非空投影，通常只返回标识和对象 URI；`_distance`
自动加入，含义由 metric 决定，是距离而非统一的相似度分数。候选列表按距离
排序；Lance 并行读取可能乱序交付结果批次，算子在有界 top-k 内恢复全局顺序。
同距离候选的先后顺序不保证固定。目标向量列必须
是固定维度浮点列表；输入长度、数值类型、有限性和可表示范围会校验。
使用 `cosine` 时拒绝零查询向量。模型、权重、归一化等语义契约仍由调用方
保持一致，维度一致本身不能证明来自同一个向量空间。

每个有效输入保留一行，重复查询不去重；无命中为 `[]`。
配置、输入、存储或检索错误终止 action，不转为空候选，也不静默丢行。
并发完成顺序可能不同于输入顺序，查询标识随原行保留。
当前支持 Local 流式执行，Ray 在声明阶段报错。

## 过滤和索引

`filter="status = 'accepted'"` 为固定 SQL 条件。
`filter_column='candidate_filter'` 则逐行读取 SQL 字符串或 `None`；
同时提供时用 `AND` 组合。条件以 Lance prefilter 执行，先限定搜索范围，
再选 top-k；应由可信程序构造 SQL 条件。普通下游 `.filter(...)` 过滤的是
查询结果行，含义不同。

`use_index=True` 允许 Lance 使用兼容的向量索引；没有索引时仍能精确扫描。
`False` 强制精确扫描，可用于评估 ANN 召回，此时忽略 nprobes/refine_factor/ef。
`nprobes` 控制 IVF 探测分区数，`refine_factor` 控制候选扩大与原向量精排；
HNSW 的 `ef` 控制图搜索候选池大小。这三个参数在索引搜索时直接传给 Lance。
索引度量应与查询 metric 一致；参数作用取决于实际索引类型。
算子不建索引、不修改数据，也不跳过新增的未索引数据。

### HNSW 接入

编辑原图检索使用 `curation/edit_scene_images` 产出的
`datasets/edit_scene_images__<encoder_id前16位>.lance`。该 pipeline 在素材向量
写入后构建索引，发布同一素材表的含索引版本；查询端使用这个 URI/version。

`IVF_HNSW_SQ` 是当前图片检索选定的索引类型。构建端的 `m`、
`ef_construction` 和 IVF 分区数决定索引结构；查询端使用 nprobes/ef，
无需在每行请求中重新指定索引类型。输入 URI/version 应来自构建端发布的
含索引快照；`use_index=True` 本身仍允许 Lance 在缺索引时回落精确扫描。

显式 `ef` 必须为正整数，且 `ef >= top_k * (refine_factor or 1)`。
例如 top_k=100、refine_factor=5 时需 ef>=500；不满足时在声明阶段报错，
不会自动改大用户的 ef。省略 ef 时沿用当前固定 pylance 12 的默认值，
即扩大后的 k 加上 k//2；默认值会随精排候选数增长。

`max_search_candidates` 默认 10000，声明时检查精排扩大的候选数及 ef。
未指定 ef 时按上述原生默认值做保守预算（非 HNSW 索引也按此预留）；
精确扫描只检查 top_k。它限制配置的候选池规模，不表示实际遍历的向量、
图节点数、索引加载或进程内存已被限制。超限明确报错，可以调小参数或
在核对资源后显式提高预算。

每次 action 第一次查询时打开表，后续查询共享同一个快照与缓存。
显式 `version` 固定发布版本；省略时固定第一次打开时的 head，action 中途
不会追随追加或索引更新。索引新建后需使用包含该索引的表版本。
指标 `stats.metrics['resources']['VectorSearch:0']` 记录实际版本、查询数和命中数。
空输入不打开表。action 结束释放引用，下次 action 重新打开；这不是跨请求的
常驻 HTTP 检索服务。旧 `data.vector_search_lance` 继续用于单次查询的数据源入口。

## 资源边界

`concurrency` 默认 4，限制同时执行的查询；`queue_depth` 默认等于并发数，
限制本节点输入队列。每个 action 拥有独立线程池和表句柄；不为所有输入
预先创建 future。不同 action 的预算独立，调用方仍须限制同时运行的 action 数。

`options` 使用以下闭合集合；未知键直接报错：

| 选项 | 默认值 | 作用 |
| --- | --- | --- |
| `use_index` | `True` | 允许使用 Lance 索引 |
| `nprobes` / `refine_factor` | `None` | 使用 Lance 默认检索参数 |
| `ef` | `None` | 使用 Lance 默认 HNSW 候选池大小；显式值须覆盖精排候选数 |
| `max_search_candidates` | 10000 | 精排候选数和 HNSW ef 的声明阶段准入上限 |
| `index_cache_size_bytes` | 256 MiB | 此表句柄的索引缓存预算 |
| `metadata_cache_size_bytes` | 32 MiB | 此表句柄的元数据缓存预算 |
| `io_buffer_size` | 16 MiB | 每个查询的原生扫描 I/O 缓冲预算 |
| `batch_size` | 64 | 每个返回批次的目标行数，同时不超过 top-k |
| `batch_size_bytes` | 1 MiB | 返回批次的目标字节数，同时不超过结果预算 |
| `max_result_bytes` | 4 MiB | 单次查询累计 Arrow 保留缓冲的检查上限 |
| `max_python_result_bytes` | 32 MiB | 转换前检查的 Python 结果估算预算 |
| `max_query_dimensions` | 65536 | 查询转换、表维度的准入上限 |

参数均须为正整数（`use_index` 除外；三个 ANN 参数允许 `None`）。
表字段先投影，扫描预读固定为各 1 批/片段。top-k 的行容器预算在声明时
检查；各返回批次在 `to_pylist()` 前检查 Arrow 缓冲和 Python 转换估算，
超限明确失败，不截断候选。估算包含行字典、标量、嵌套列表/结构体容器
以及文本展开余量，并预留每个候选 24 字节的排序工作空间；不支持的复杂
Arrow 类型拒绝转换。保留父缓冲和重复
计数可能导致保守拒绝，调用方可以缩小投影或调整预算。

同时驻留规模应计入：两类共享缓存、`concurrency ×` 原生扫描/计算工作集、
Arrow 到 Python 转换期间的双份结果，以及已完成 worker、下游队列和消费者
持有的候选列表；还包括原输入行和上游源的预读。节点队列只限制行数，
不能替任意输入字段或下游消费者提供字节上限。

这些参数不是进程 RSS 硬上限。Lance 的解码、索引分区加载、距离计算和
排序有额外分配，超大单元格可能在 Arrow 返回前分配；返回后的检查无法
阻止那次分配。当前线程执行器也不能强制终止正在执行的原生调用：取消
时停止新任务，等待已启动调用返回，再清理线程池及表引用。跨查询的
硬内存/时间隔离仍是平台缺口，见工作区 DF-016；需要硬边界的部署应将
整个 action 放进受内存和时间限制的独立服务进程，不能把本算子的预算
当成进程隔离保证。尚未完成百万级数据、远端 I/O 故障或 RSS 硬限压测。

验证覆盖临时真实 Lance 表的逐行检索、重复输入、空结果、SQL prefilter、
快照固定、IVF_FLAT/IVF_HNSW_SQ 索引与精确扫描、HNSW 精排和过滤、
索引后追加数据的召回、候选池与转换预算拒绝、并发上界、慢消费者
背压及失败后的在途调用清理；通用流式执行器测试覆盖取消与线程释放。

4096 维真实图片向量另已完成 2944 条入库、128 条独立查询的小样本验收：
IVF_HNSW_SQ、top_k=100、nprobes=16、ef=500、refine_factor=5 的流式
Recall@100 为 100%。这衡量对精确向量搜索的复现，不是语义找图准确率；
参数尚未经过百万级调优。复现方式和完整结果见
[真实向量验证记录](../benchmarks/VECTOR_SEARCH.md)。
