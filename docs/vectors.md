# 向量：编码、建索引、检索

三步组成一条向量检索链路：

1. `Dataset.map_embeddings` 把文本、图片或文档片段编码成向量；
2. `data.ensure_lance_vector_index` 给 Lance 表里的向量列建索引；
3. `Dataset.search_vectors` 让每一行查询去检索，把候选列表写回这一行。

编码通过 OpenAI 兼容的 `/v1/embeddings` 接口调用模型服务。安装时加上 `demiflow[embeddings]`；模型服务和权重需要自行准备。

## 快速开始

```python
import pyarrow as pa
from demiflow import data
from demiflow.embeddings import EmbeddingModel

model = EmbeddingModel(
    name="my-embedding-model", revision="2026-01", dimensions=1024,
    base_url="http://127.0.0.1:8002/v1",
)

# 1. 编码并写表。向量列必须是定长 float32 列表，所以显式给 schema。
schema = pa.schema([("id", pa.int64()), ("text", pa.string()),
                    ("embedding", pa.list_(pa.float32(), 1024))])
docs = data.read_lance("/data/docs_raw.lance").select_columns(["id", "text"])
receipt = (
    docs.map_embeddings(model=model, inputs={"text": "text"}, output="embedding",
                        batch_size=32, concurrency=4)
    .materialize()
    .write_lance("/data/docs.lance", mode="create", schema=schema, return_receipt=True)
)

# 2. 建索引。返回值里的 uri/version 就是带索引的固定版本。
index = data.ensure_lance_vector_index(
    "/data/docs.lance", expected_version=receipt.committed_version,
    vector_column="embedding",
)

# 3. 检索。
hits = (
    data.from_items([{"q": "a wooden table near a window"}])
    .map_embeddings(model=model, inputs={"text": "q"}, output="query_embedding")
    .search_vectors(query="query_embedding", output="candidates",
                    uri=index["uri"], version=index["version"],
                    vector_column="embedding", columns=["id", "text"],
                    metric="cosine", top_k=3)
    .materialize()
    .take_all()
)
# hits[0]["candidates"] == [{'id': ..., 'text': ..., '_distance': 0.08}, ...]
```

## 编码：`map_embeddings`

### 声明模型

`EmbeddingModel` 描述一个固定的模型版本和它的接口：

| 字段 | 含义 |
|---|---|
| `name`、`revision` | 模型名和不可变的权重版本 |
| `dimensions` | 向量维度，返回值不符时报错 |
| `base_url` | 服务地址 |
| `input_format` | `'text'`（默认）发送标准 `input: [str]`；`'chat'` 发送 vLLM 的批量 messages，图片模型必须用它 |
| `normalize` | 默认 `True`，输出前做 L2 归一化 |
| `api_key_env` | 存放密钥的环境变量名；密钥不会写进请求记录 |
| `request_options` | 透传给模型服务的处理参数 |
| `encoding_parameters` | 后端版本、精度、模板等会影响向量的设置，用于区分缓存 |

查询和入库必须用同一个模型声明。维度相同不代表向量空间相同。

### 输入

`inputs` 把一种输入类型映射到一列：

- `{"text": "列名"}`：非空字符串。
- `{"image": "列名"}`：图片对象引用 `{uri, sha256}`。读取时校验 SHA 并在本地完整解码。给一个列表表示同一张图的多个备用位置，不是多图输入。
- `{"document": "列名"}`：已保存的规范文档里的若干片段，用 `Dataset.document_embeddings(model=..., document="列名")` 更方便。

### 输出与失败

- `output` 列是 float32 向量，原有列保留。
- `call_output="列名"`：附带这次调用的信息（延迟、是否命中缓存、用量）。用量是整批的，不要按行累加。
- `error_output="列名"`：读不到或不合法的输入保留为一行，向量为 null，原因写在这一列。不设时这类输入会让整个动作失败。
- 配置错误、服务不可用、返回不合协议一律终止动作，不会产出空向量。
- 关系运算或普通写表之前先 `materialize()`；`run_stream()` 直接执行。目前只支持本地执行。

### 并发与批量

| 参数 | 默认 | 含义 |
|---|---|---|
| `batch_size` | 16 | 每个请求最多多少条输入 |
| `concurrency` | 1 | 同时进行的请求数 |
| `queue_depth` | — | 上游最多排多少行 |
| `prefetch_batches` | 0 | 请求进行时额外提前准备多少批输入 |
| `max_requests` | 不限 | 请求批数上限 |
| `request_policy` | — | 按延迟和错误自动调节并发，例如 `{"min_concurrency": 1, "initial_concurrency": 2, "max_concurrency": 4}` |

`options` 里常用的项：

| 选项 | 默认 | 含义 |
|---|---|---|
| `timeout_s` | 300 | 单个请求的总时限 |
| `max_image_bytes` | 64 MiB | 单张图片的大小上限 |
| `max_decode_pixels` | 1 亿 | 单张图片解码像素上限 |
| `max_request_bytes` / `max_response_bytes` | 96 / 64 MiB | 单个请求和响应的大小上限 |
| `batch_request_bytes` | 关闭 | 设置后按目标字节数把一批拆成多个请求 |
| `prepare_workers` / `response_workers` / `io_workers` | 各 2 | 图片准备、响应解析、请求记录的线程数 |

这些上限约束的是请求和单个对象，不是整个进程的内存。内存紧张时降低 `concurrency`、`batch_size` 和图片上限。

### 模型服务

- `service=None`（默认）：使用 `base_url` 指向的外部服务，平台不启动也不停止它。
- `service=VLLMService({...})`：由平台管理。第一个需要真正调用模型的请求到来时启动，动作结束、失败或取消时停止；全部命中缓存时不启动。

```python
from demiflow.services import VLLMService

service = VLLMService({"model_path": "models/my-model", "runner": "pooling",
                       "dtype": "bfloat16", "gpus": [0]}, root=workspace)
rows.map_embeddings(model=model, inputs={"image": "image"}, service=service)
```

两个模型要先后使用同一批 GPU 时，在它们之间 `materialize()`。

## 建索引：`ensure_lance_vector_index`

对 Lance 表的定长 float32 向量列建 `IVF_HNSW_SQ` 索引（余弦距离），在 CPU 上构建。

```python
index = data.ensure_lance_vector_index(
    "/data/docs.lance", expected_version=version, vector_column="embedding",
    options={"num_partitions": 256, "m": 16, "ef_construction": 200},
)
```

返回字典里的要点：

| 键 | 含义 |
|---|---|
| `uri`、`version` | 带索引的固定版本，检索时用它 |
| `status` | `ready` 表示索引完整覆盖这个版本；空表为 `empty` |
| `action` | `build`、`rebuild`、`reuse` 或 `empty` |

- 已有参数相同且覆盖完整的索引时直接复用，不产生新版本。
- 表里新增了向量或参数变了，会整体重建。目前没有增量更新。
- 构建失败、版本被别人改动或校验不通过时抛异常，这时不要把表当作已建索引。
- `num_partitions`（默认 256）不能超过表的行数，小表要显式调小。
- 资源选项：`threads`（默认 8）、`memory_bytes`（8 GiB）、`max_rss_bytes`（20 GiB）、`timeout_s`（3600 秒）。

## 检索：`search_vectors`

每行输入提供一个查询向量，检索一次，原行保留并写入候选列表。

| 参数 | 含义 |
|---|---|
| `query` | 输入行里查询向量所在的列 |
| `output` | 写入候选列表的列 |
| `uri`、`version` | 被检索的表及其固定版本；省略 `version` 时固定为第一次打开时的最新版本 |
| `vector_column` | 表里的向量列 |
| `columns` | 候选要带回的列，必须显式给出 |
| `metric` | 距离度量，例如 `cosine` |
| `top_k` | 每个查询最多返回多少候选，默认 20 |
| `concurrency` | 同时进行的查询数，默认 4 |

- 候选按 `_distance` 从小到大排序。它是距离，不是相似度分数。
- 每个输入保留一行；没有命中时候选为 `[]`。重复的查询不去重。
- 输入、存储或检索出错时终止动作，不会返回空候选来掩盖错误。
- 并发下输出顺序可能与输入不同。

### 过滤

- `filter="status = 'accepted'"`：固定的 SQL 条件。
- `filter_column="列名"`：逐行读取 SQL 条件或 `None`。两者同时给出时用 `AND` 组合。

过滤在检索之前生效：先限定范围，再取 top-k。SQL 条件应由可信的程序构造。

### 索引参数

通过 `options` 传入：

| 选项 | 默认 | 含义 |
|---|---|---|
| `use_index` | `True` | 使用索引；没有索引时自动精确扫描。设为 `False` 强制精确扫描，可用来评估召回 |
| `nprobes` | Lance 默认 | 探测的分区数 |
| `ef` | Lance 默认 | HNSW 搜索的候选池大小；显式给出时必须不小于 `top_k × refine_factor` |
| `refine_factor` | Lance 默认 | 候选扩大的倍数，用原始向量重新排序 |
| `max_search_candidates` | 10000 | 候选池规模的上限，超出时在声明阶段报错 |
| `max_result_bytes` | 4 MiB | 单次查询结果的大小上限，超出时报错而不是截断 |

增大 `nprobes`、`ef`、`refine_factor` 提高召回，也增加耗时。

### 只查一次

不在流里、只想对一个向量查一次时，用 `data.vector_search_lance(uri, vector, vector_column=..., top_k=...)`，它返回一个 Dataset。

## 需要知道的限制

- 编码、建索引和流式检索目前只支持本地执行。
- 各项预算限制的是请求、候选和单个对象的规模，不是进程内存的硬上限。需要硬隔离时，把整个任务放进有内存和时间限制的独立进程。
- 检索不会建索引，也不会修改数据；新写入的向量在重建索引前按精确扫描参与检索。
