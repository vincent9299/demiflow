# 获取、队列与文档

这一篇讲三件相关的事：

1. `Dataset.fetch`：把每行的一个地址完整下载为一个原始对象；
2. 任务队列：把“要下载什么”和“谁来下载”分开，多个进程分担；
3. 下载之后：解析文档、读取对象、处理图片。

获取只做下载和校验，不解析内容。解析、图片解码和业务筛选是之后各自独立的步骤。

## 下载：`fetch`

```python
from demiflow import data
from demiflow.collect import FetchPolicy
from demiflow.execution import RunContext
from demiflow.objects import LocalObjectStore

with RunContext.start("/data/runs/fetch-example"):
    rows = (
        data.from_items([
            {"id": "a", "request": {"uri": "https://example.org/page"}},
            {"id": "b", "request": {"uri": "https://example.org/missing"}},
        ])
        .fetch(store=LocalObjectStore("/data/raw"),
               policy=FetchPolicy(max_object_bytes=64 * 1024**2), concurrency=4)
        .materialize()
        .take_all()
    )

for row in rows:
    print(row["id"], row["result"]["status"], row["result"]["reason"])
# a ok None
# b failed not_found
```

每行的 `request` 列是一个请求，下载结果写进 `result` 列（列名可用 `request=`、`output=` 改）。输出顺序不保证。

### 请求

| 字段 | 含义 |
|---|---|
| `uri` | 地址：`http(s)://...` 或 `cos://bucket/path` |
| `expected_size` | 预期的完整字节数，同时是写入的硬上限 |
| `expected_sha256` | 预期的内容 SHA256，下载后核对 |
| `source_version` | 固定来源版本，例如 `{"kind": "http_etag", "value": "\"etag\""}` 或 `{"kind": "cos_version_id", "value": "..."}` |

### 结果

| 字段 | 含义 |
|---|---|
| `status` | `ok`、`failed`（确定的失败，如 404、被拒绝、大小或摘要不符）或 `retryable`（重试次数用完，仍可能成功） |
| `raw_ref` | `{uri, sha256}`，指向已保存的原始对象；只有 `ok` 时有 |
| `size_bytes` | 对象大小 |
| `reason` | 失败原因，例如 `not_found` |
| `attempts`、`http_requests` | 尝试次数和实际发出的请求数 |

- 只有收到 200、内容完整、校验通过并且已经保存，才是 `ok`。
- 确定的失败保留为一行，不会让动作失败；`fetch` 没有 `catch`，不会把下载失败的行丢掉。
- 配置无效、取消、保存结果不明等情况会终止动作。
- 原始内容不解压、不解析、不断点续传。

### 下载策略

`FetchPolicy` 的常用项：

| 项 | 默认 | 含义 |
|---|---|---|
| `max_object_bytes` | 1 GiB | 单个对象的大小上限 |
| `max_attempts` | 3 | 每个对象最多尝试几次 |
| `total_timeout_s` | 600 | 一个对象的总时限，所有尝试共用 |
| `redirects` | 5 | 最多跟随几次重定向 |
| `per_host_concurrency` | 2 | 对同一主机的并发 |

### 认证、代理与安全默认值

```python
from demiflow.collect import CosSource, FetchPolicy, HttpSource, Secret

policy = FetchPolicy(
    max_object_bytes=64 * 1024**2,
    http=HttpSource(headers={"authorization": Secret("SOURCE_AUTH")},
                    credential_origin="https://example.org"),
    cos=(CosSource(bucket="bucket-appid", region="ap-region",
                   secret_id=Secret("COS_SECRET_ID"),
                   secret_key=Secret("COS_SECRET_KEY")),),
)
```

- 凭据只通过环境变量提供：`Secret("环境变量名")`。
- 不会自动使用进程的代理环境变量；需要代理时声明 `HttpSource(proxy=Secret(...))`。
- 始终校验 TLS 证书；自有 CA 用 `ca_file` 声明。
- 默认只跟随同源重定向；其他 HTTPS 源在 `redirect_origins` 里列出。跨源时会去掉认证头和 Cookie。
- 默认拒绝访问内网和本机地址；确实需要时声明 `HttpSource(allow_private=True)`。
- 不支持把短期签名 URL 直接当作 `uri`；请使用稳定地址加上声明的认证。

## 任务队列：多个进程分担下载

队列把任务清单持久化：一个生产者写入任务，一个或多个消费者领取、下载并结算。消费者中断后，未结算的任务会被重新领取。

```python
from pathlib import Path
from demiflow import data
from demiflow.collect import FetchPolicy
from demiflow.execution import RunContext
from demiflow.objects import LocalObjectStore
from demiflow.queue import QueueChannel, QueueConfig, ProducerSourceSpec, QueueResultSink

config = QueueConfig(backend="sqlite", queue_id="raw-v1", path="/data/queues/raw-v1.sqlite")

# 生产者：从一份固定的清单写入任务
manifest = Path("/data/manifest.jsonl")   # 每行含 messages=[task_message(task_id, payload, pool="raw")]
channel = QueueChannel(config)
producer = channel.open("raw", producer_id="manifest-v1",
    source=ProducerSourceSpec(kind="manifest", source_id="manifest-v1",
                              uri=manifest.as_uri(), sha256=manifest_sha256))
with RunContext.start("/data/runs/producer"):
    data.read_json(str(manifest)).enqueue(config, producer=producer).run_stream()
channel.seal(producer)

# 消费者：可以启动多个进程，各自领取不同任务
with RunContext.start("/data/runs/worker-1"):
    (data.read_queue(config, pool="raw", max_tasks=1000, max_inflight=8)
        .fetch(store=LocalObjectStore("/data/worker-raw"),
               policy=FetchPolicy(max_object_bytes=64 * 1024**2), concurrency=4)
        .write_datasink(QueueResultSink(config, pool="raw")))
```

要点：

- 任务用 `task_message(task_id, payload, pool=...)` 构造；`task_id` 要稳定，同一个 `task_id` 配不同内容会被拒绝。
- 生产者的来源必须是固定的：字面量、带 SHA 的本地 JSONL，或固定版本的本地 Lance 表。生产只能用 `run_stream()` 执行。
- `channel.seal(producer)` 表示任务已全部写入。消费者可以在这之前就启动；队列暂时为空时它会继续等待。
- 消费链路的形状是固定的：`read_queue → 逐行处理 → 一个 fetch → 逐行处理 → QueueResultSink`。中间不能丢行、扩行或跨任务合批。
- 只有结果已经持久保存，任务才会被标记完成。失败、取消或结果不明的任务不会被确认。
- 业务上要拒绝某个结果时，保留原 `result`，另加一个字段标记，结算之后再过滤。

后端：

| 后端 | 配置 | 说明 |
|---|---|---|
| SQLite | `QueueConfig(backend="sqlite", queue_id=..., path=...)` | 文件必须在本机磁盘上，不能在网络挂载上 |
| PostgreSQL | `QueueConfig(backend="postgresql", queue_id=..., dsn=Secret("QUEUE_DSN"), namespace=...)` | 安装 `demiflow[queue-postgresql]`；多台机器可以共用 |

可以直接运行的完整例子在仓库的 `examples/` 目录：

```bash
python -B examples/raw_fetch_queue.py      # 一个生产者、两个消费者进程
python -B examples/fetch_manifest.py       # 固定清单 → 入队 → 下载
python -B examples/fetch_metadata_links.py # 先取元数据，解析出链接后再入队下载
python -B examples/fetch_keywords.py       # 检索 → 选候选 → 入队 → 下载
```

这些例子只用本机的测试服务，不访问外部网络。

## 下载之后

### 读取已保存的对象

```python
from demiflow.objects import ObjectRef

ref = row["result"]["raw_ref"]
body = ObjectRef(**ref).read(max_bytes=8 * 1024**2)   # 读取时校验 SHA256
```

在管线里逐行读取用 `Dataset.read_object(reference="列名", output="列名", root="/data/raw", max_bytes=...)`：只从声明的根目录读取，校验 SHA256，不访问网络。

### 把网页解析成文档

```python
from demiflow.collect.documents import parse_document

doc = parse_document(body, url=uri, final_url=uri, content_type="text/html")
doc["blocks"][:2]
# [{'kind': 'heading', 'text': 'Title', 'block_id': 'b000000', ...},
#  {'kind': 'paragraph', 'text': 'Public text of the page.', 'block_id': 'b000001', ...}]
```

- 解析只读你给它的字节，不会再去下载。
- 结果是按块组织的规范文档：标题、段落、表格各为一块，带稳定的 `block_id`。
- PDF 默认不解析；传入 `pdf_parser={...}` 显式开启。
- 解析只提取结构，不判断来源是否可信。

在管线里把已保存的规范文档字节解码成记录，用 `Dataset.decode_document(column="列名", output="列名")`。

### 为提示词挑选文档内容

`Dataset.read_documents(request="列名", output="列名", context=...)` 读取已保存的规范文档，按问题挑出相关的完整块，并保证拼进提示词后不超过预算；它不调用模型。同一个函数也可以作为工具开放给 [Agent 算子](agents.md)。

### 图片

- `demiflow.collect.image_decode.decode_image(uri, sha256, policy)`：校验 SHA 后在独立进程里解码，返回宽高等信息。`policy` 必须给出 `max_bytes`、`max_pixels`、`max_frames`、`decode_memory_bytes`、`decode_timeout_s`。
- `Dataset.encode_image(column="列名", output="列名", profile=..., max_side=1536)`：把已校验的图片字节缩放并编码成适合送给模型的 JPEG，不修改原对象。

下载的回执只说明“下载到了什么”。图片尺寸、内容是否合格由你在解码之后判断。

## 需要知道的限制

- `fetch` 的并发和队列限制的是行数；单个对象的大小由 `max_object_bytes` 限制。
- 自动结算的队列链路只接受上面那一种形状；其他形状会在领取任务前被拒绝。
- 队列不负责启动远程进程，消费者需要你自己在各台机器上启动。
