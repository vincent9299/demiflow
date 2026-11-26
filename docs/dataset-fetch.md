# 原始对象获取与原生任务队列

`Dataset.fetch` 每行获取一个完整原始对象，HTTP(S) 和 COS 使用相同的请求与回执。
获取、图片解码、文档解析、业务筛选分别执行。`Dataset.fetch_images`、
`Dataset.fetch_documents`、`Download`、`WebSession.fetch/fetch_image`、
`WebClient.fetch`、`ImageClient/ImageFetchPolicy` 及其专属下载池已移除；历史对象及独立的解析、读取、
DocumentLibrary 能力保留。迁移正在运行的程序时使用新版本入口，
原冻结程序及其历史回执继续按原版本读取。

## 单机获取

```python
from demiflow import data
from demiflow.collect import FetchPolicy
from demiflow.objects import LocalObjectStore
from demiflow.execution.run_context import RunContext

with RunContext.start('/local/runs/raw-example'):
    result = (data.from_items([
        {'id': 'part-0', 'request': {'uri': 'https://example.org/part-0.bin'}},
        {'id': 'part-1', 'request': {'uri': 'https://example.org/part-1.bin'}},
    ]).fetch(store=LocalObjectStore('/local/raw'),
             policy=FetchPolicy(max_object_bytes=64*1024**2),
             concurrency=4).materialize())
    rows = result.take_all()  # 此处输入明确只有两行；大结果用持久 sink。
```

接口为 `fetch(*, request='request', output='result', store, policy=None,
concurrency=4, queue_depth=None, label='fetch') -> Dataset`。构图不打开连接、
读取秘密或创建目录。输入为 JSON record，原列保留；输出列必须未使用。
行序不保证，重复 URI 的不同 occurrence 仍各自有一行。

请求只接受 `uri`、`expected_size`、`expected_sha256`、`source_version`。
`expected_size` 是完整表示的字节数，也是写入硬上限，0 表示空对象。
`source_version={'kind':'http_etag','value':'"strong-etag"'}` 发出 If-Match 并核对
返回 ETag；`cos_version_id` 固定并核对 COS 版本；`manifest_label` 仅作身份声明。
SHA256 核对原始表示字节，不把 ETag 当内容摘要。

回执包含 `status=ok|failed|retryable`、`terminal`、普通 `raw_ref={uri,sha256}`、
`size_bytes`、`partial_bytes`、`reason`、`checks`、`source`、`attempts`、
`http_requests`、`retrieved_at`、`acquisition_ref` 和 `storage_domain`。
只有 200、完整 EOF、验证通过并完成 acquisition commit 才返回 ok。
404、访问拒绝、大小/摘要/版本不符等已知失败保留行；重试耗尽为非终态 retryable。
无效声明、编程异常、回调 TimeoutError、取消与未知提交向上传播。
不提供 fetch 专属 catch，不把未确认的效果转换成成功行。

默认单对象 1 GiB、64 KiB 块、最多 3 次 attempt、5 次重定向、总时限 600 秒。
每个 GET hop 另计 requests。一个 attempt 从稳定 URI 开始，所有 attempt 和 owner
共享持久截止时间。原始响应不自动解压、不解析、不续传，不接受 206 作为完整对象。
对象过大、磁盘或请求预算不足时停止；不因 policy 变大而提高 run 默认预算。

## 认证、TLS 与 COS

```python
from demiflow.collect import CosSource, HttpSource, Secret

policy = FetchPolicy(
    max_object_bytes=64*1024**2,
    http=HttpSource(headers={'authorization': Secret('SOURCE_AUTH')},
                    credential_origin='https://example.org'),
    cos=(CosSource(bucket='bucket-appid', region='ap-region',
                   secret_id=Secret('COS_SECRET_ID'),
                   secret_key=Secret('COS_SECRET_KEY')),),
)
# 对应请求：{'uri':'cos://bucket-appid/path/object',
#           'source_version':{'kind':'cos_version_id','value':'fixed-version'}}
```

Secret 只保存环境变量名，生命周期启动时才解析。HTTP 不隐式继承环境代理；代理
需声明 `HttpSource(proxy=Secret(...))`。TLS 始终验证证书和主机，可声明绝对
`ca_file`，非法本地 CA 为配置错误，远端证书失败为技术失败回执。

默认仅同 origin 重定向；额外 HTTPS origin 需在 `redirect_origins` 声明。
跨 origin 删除原 Authorization、Cookie 及所有 Secret 声明头。
临时签名目标仅在 transport 内存使用，保留请求所需 path/query，不进入行、身份、
数据库或 HTTPX/HTTPcore 日志；回执只保存安全 origin，`final_uri=None`。
临时目标 401/403 最多从稳定入口刷新一次，占原 attempt 和时限额度。
已 prepared 后恢复只核对原对象，不刷新、不重新 GET。
直接把短期签名 URL 当 `request.uri` 的方式不支持；使用稳定入口或声明认证资源。

## 生产与消费

```python
from pathlib import Path
from demiflow.queue import QueueChannel, QueueConfig, ProducerSourceSpec, QueueResultSink

# manifest.jsonl 每行已含 messages=[task_message(task_id, payload, pool='raw')]。
# payload 含 request；task_id 来自固定清单的稳定条目标识。
manifest = Path('/local/fixed/manifest.jsonl')
config = QueueConfig(backend='sqlite', queue_id='raw-v2',
                     path='/local/queues/raw-v2.sqlite')
channel = QueueChannel(config)
producer = channel.open('raw', producer_id='manifest-v1',
    source=ProducerSourceSpec(kind='manifest', source_id='manifest-v1',
        uri=manifest.as_uri(), sha256=verified_manifest_sha256))
with RunContext.start('/local/runs/producer'):
    data.read_json(str(manifest)).enqueue(config, producer=producer).run_stream()
channel.seal(producer)  # 只有原生源 EOS 且无未确认发布才可 seal。

# 消费者可在 producer 尚 open 时另行启动；暂空持续等待，非完成。
with RunContext.start('/local/runs/worker'):
    (data.read_queue(config, pool='raw', max_tasks=1000, max_inflight=8)
        .fetch(store=LocalObjectStore('/local/worker-raw'),
               policy=FetchPolicy(max_object_bytes=64*1024**2), concurrency=4)
        .write_datasink(QueueResultSink(config, pool='raw')))
```

消息使用 `task_message(task_id, payload, pool=...)`；调用者不提供 fingerprint
或 owner token。稳定来源、转换身份、条目 occurrence、完整 payload 与 task_id
共同定义不可变任务，冲突拒绝。一个 generation 有一个 producer；重新 open 相同
声明增加 epoch，旧 producer 失效。sealed generation 不能重新打开。

enqueue 只通过 `run_stream()` 增量生产，其他 terminal 在发布前拒绝。
源为固定 literal、SHA 固定本地 JSONL 或明确版本的本地 Lance 直接列读取；
不支持 live iterator、Lance filter/projection 或未固定版本源。每个输入独立推进，
0 输出仍提交进度，展开最多 1024 行/16 MiB，单输出最多 32 消息。
每发布 chunk 最多 32 消息/512 KiB，prepared 最多 1 MiB，且服从更小的 run 行预算。
JSONL 保存字节游标，Lance 使用固定版本行 offset；恢复无需从整个源头重扫。
搜索候选先由原生 SearchWeb 固定该关键词有界响应，再展开入队；已固定响应恢复
不重复搜索，未知 HTTP 效果不获得免费重试。总任务数可在 open 时未知，由 EOS/seal 固定。
搜索应显式声明适合当前 run 的 `SearchConfig.max_bytes/max_results/workers`；多个来源与
查询的响应、解析和保留副本共同准入。较大的默认来源配置可能超过 256 MiB 受管预算，
此时在 HTTP 前拒绝；缩小明确配置或显式给出有限 run 预算，不自动扩额。

自动结算图为 `read_queue → 逐行 map/字段绑定 → 恰好一个 fetch → 逐行 map/投影 → QueueResultSink`。
sink 是 terminal，返回 None。普通函数及无实例状态 actor 可用；通用 actor 只接
object 默认构造、无继承/类数据/自定义描述器、方法不读取或捕获 receiver 的子集。
身份包含方法源码/默认值/闭包及原生调用参数，执行和清理沿用 StreamMapRuntime。
普通 Python 回调仍是声明的纯变换，不是副作用沙箱。未知构造/可变状态/资源 actor、
catch 丢行、filter/flat_map/limit、跨任务批处理、额外副作用和任意 sink 领取前拒绝。
复杂只读初始化资源 actor 需已支持的原生适配；snapshot_state={} 不能单独授予准入。

`map(fn, inputs={参数:行字段}, output=新增字段)` 默认可逐行接入流式图，
`select_columns/drop_columns/rename_columns(映射)` 保留 `_queue_delivery` 和 fetch 后的 result。
sidecar 独立透传，终端核对唯一交付和原 acquisition receipt。静态覆盖/删除这些字段
在领取前拒绝，运行时丢行/扩行/改回执不 ACK。
`add_column` 仍是批语义，不进入 task 图；逐行加字段用字段绑定 map，批处理在固定历史上做。
业务拒绝保留原 result，另加 `acceptance={accepted:false,reason:...}`，结算后再过滤。

显式模式为 `read_queue(settlement='explicit') → fetch → ack_queue(config) → run_stream`，
它与自动 sink 互斥，使用同一结果提交与原子完成协议。

raw acquisition 和最终结果分别沿用原生
`pending → prepared → data_committed → checkpoint_committed → acked`。
只有最终结果持久并核对 raw 成果后，合法 lease 下的同一 SQL 事务才能写 completion、
task done、final acked 并清除 token。未知、取消、sink 失败不 ACK。
retryable 耗尽保存 observation 和 exhausted，terminal 抛 QueueIncomplete。
worker 窗口完成仅表示其领取任务已结算；只有 sealed 且所有声明任务 done 才是 queue_drained。

本仓可运行 [双进程示例](../examples/raw_fetch_queue.py)：
`python -B examples/raw_fetch_queue.py` 启动有界本机 HTTP fixture、增量 producer
和两个独立 worker 进程，并核对 7 成功/1 已知失败及只读历史。
默认使用 `/tmp` 本机 SQLite；PostgreSQL 模式显式提供
`DEMIFLOW_EXAMPLE_QUEUE_DSN` 后加 `--backend postgresql`。示例不访问真实云对象或启动生产服务。


三类来源的独立可运行接线：

- `python -B examples/fetch_manifest.py --explicit`：固定JSONL清单→map→enqueue→fetch→显式ACK。
- `python -B examples/fetch_metadata_links.py`：先获取元数据→普通函数解析链接→固定JSONL→enqueue→fetch→自动结果sink。
- `python -B examples/fetch_keywords.py`：有界本地HTTP SearchConfig adapter→search_web→普通候选选择→enqueue→fetch。

均用有限本机fixture与新临时目录，每个对象获取一次，结算后通过固定snapshot历史执行普通map/filter。
它们没有对真实云、供应商搜索或多host能力作出验证声明。

## 后端与读取域

SQLite 要求本机盘，拒绝 symlink 和网络挂载。PostgreSQL 配置为
`QueueConfig(backend='postgresql', queue_id=..., dsn=Secret('QUEUE_DSN'), namespace=...)`，
安装 `demiflow[queue-postgresql]`。原生事务使用数据库时间、行锁和递增 epoch；
不是 COS 条件写或本地锁模拟的分布式队列。队列不启动远程 worker 或通用 Dataset 调度器。

同一个 PostgreSQL pool 可让各 host 领取不同新任务并写各自本地 store。
一旦 prepared 固定 artifact_domain，恢复只能在可读同一域的 worker 上进行。
`LocalObjectStore(..., read_domain='shared', domain_id='declared-domain')` 要求
成员实际读到同一 marker；路径名或 hostname 相同不足以证明同域。
共享域更换挂载根后，原 `result.raw_ref`、`storage_domain`、native receipt、SQL行和proof
保留原始URI。消费者显式声明当前store并调用 `store.resolve(raw_ref, domain=storage_domain)`，
取得相同SHA的当前ObjectRef。只读核验原native namespace/hash后缀及现存同marker，
host域还要求原主机和物理root相同；shared才可换根，不要求旧挂载仍存在。
该调用不创建目录/marker，不读全域注册表，不GET/搬运，不注册全局URI别名。

```python
from demiflow.objects import LocalObjectStore, read_local_object

store = LocalObjectStore('/current/mount', read_domain='shared', domain_id='team')
# 在普通逐行fn/actor里使用当前store，保留row['result']：
ref = store.resolve(row['result']['raw_ref'], domain=row['result']['storage_domain'])
raw = read_local_object(ref.uri, ref.sha256, root=store.directory, max_bytes=8*1024**2)
```

resolve只核对地址和域，不核验完整内容或completion，不改变metadata历史的not_checked。
`read_local_object` 在1..128MiB预算内持有逐级目录/文件描述符，拒绝symlink、变更和SHA不符。
普通 `ref.read(max_bytes=...)` / 流式 `ref.verify(max_bytes=...)` 仍沿用ObjectRef原有URI/别名
行为，不能仅凭resolve成功就宣称其全程no-follow。更大的对象不做无界完整buffer读取。
同task图中的fn/actor可通过fn_kwargs显式传同一个fetch store；consumer身份沿用该资源的逻辑
store声明，物理root仍按worker的RawNamespace核验注册。其他路径/其他store不会被隐式重绑定。

目标仅支持 POSIX 本地/明确共享文件系统，不支持 COS/S3 目标或自动跨机复制。
原域不可用保持未解决，其他任务仍可处理；不会重新下载来掩盖已提交对象丢失。
`state` 返回最多 64 个未完成 artifact 域的原生计数，不扫描完成历史。
全局 `artifact_availability='not_probed'`，不会凭旧注册推断远端主机仍在线；
worker 按自己的已验证读取域计算 `blocked_artifact_domain`，其他域仍有任务且
达到空闲期限时抛 ArtifactDomainUnavailable，保留任务和成果。

## 固定完成结果与迁移

```python
from demiflow.queue import HistoryReadLimits

snapshot = channel.snapshot('raw', kind='results')
history = data.read_queue_records(config, pool='raw', through=snapshot, results=True,
    limits=HistoryReadLimits(max_rows=128, max_hash_bytes=1024**3))
rows = history.take_all()  # 独立 run；大历史显式按结构化 next_cursor 续读。
```

Snapshot 固定 queue UUID、generation、kind 与事务发布前缀，不是 MAX(id) 或 sequence。
worker 启动不读已完成历史；仅核对自己至多 max_inflight 个未确认引用。
显式 history 默认核对 native 元数据和实际对象。`verify='metadata'` 明确返回
`verification.status='not_checked'`；损坏、缺失、域不可达均失败，不修复或 GET。
读取有行、单条/累计字节、驻留、hash、分页及时间预算；HistoryReadLimit 含绑定原
Snapshot 的 `next_cursor`，可作为 `after=` 续读，不能换 Snapshot。

旧 SQLiteQueue/SQLiteChannel/COSQueue/queue_runner 已移除。v1 不原地升级：
新建 v2 库并从固定来源显式生产任务。旧已关闭的一致 SQLite backup 可用
`data.read_legacy_queue_records(path, sha256=..., pool=..., through=..., results=...)`
只读查询，标记 `legacy_not_certified`；不能 claim、ACK 或伪造 v2 completion。
独立 COSIO 签名存取、解析、历史库读取保留，业务仓和在运冻结代码没有自动迁移。

## 资源与验证范围

默认 pool 限制：100000 tasks、32 worker 注册、256 active leases、每 task 16 次领取、
24 小时总期限、512 MiB 逻辑 control、2 GiB 最终结果、64 GiB raw/scratch、
192 GiB representation body、1000000 GET；各项独立，较小额度可先耗尽。
同一队列库的 control 也共用累计额度；PG 索引、MVCC、WAL 与日志的物理空间另需有限运维配额。
attempt/reservation 共用既有 100000 记录硬界，wire grant 累计更新有界 attempt 行，
不逐块 INSERT。未知旧 owner 的授权保守保留，只有排空且有合法 fence 才退未用量。
SIGKILL 可留下未 prepared 的 staging 和对应未知授权；lease 过期本身不足以证明旧 I/O
已排空，本版保守保留，并由 attempt/总磁盘额度限制增长。本版没有自动跨域 orphan GC，
不把任务已完成解释为未知旧 staging 已被清理。普通异常、可排空取消和存储短写会清理
本次未准备文件；已 prepared 的成果留给原生恢复核验。

同一 native run 的生产和多个消费窗口共用 commit/request/bytes 预算，不能换 worker
重置；一个 run 只委托一个 queue UUID，普通本地持久效果使用另一个有界 run。
默认 run 的 256 MiB 受管内存、4 GiB RSS/scratch、2 GiB output、600 秒保持。
没有预期大小时按对象上限保守预留，容量紧张会有界等待或拒绝。
传输按 64 KiB 窗口流写，超限哨兵不落盘；不聚合整个对象。CAS 无覆盖、fsync、
staging 与最终对象同文件系统 link；大 hash 在 SQL 锁外，短 guard 重验路径/inode/fence。
续租使用独立 I/O 池；SQL 连接每 worker 最多 2 个，语句/锁等待默认 5 秒。
受管载荷预算不是解释器、TLS、Arrow 或任意业务函数的 RSS 硬上限。

本版本有真实本机 HTTP/HTTPS、独立 COS 签名协议 fixture、SQLite 和私有 PostgreSQL、
多进程 SIGKILL/原 run 与新 worker 恢复验证。真实云 COS 凭据和两台独立主机的共享域
接管尚未验证；单机多 worker 或重命名挂载目录不替代该实证。

普通固定 typed 输入的 fetch 已接同版 Forge 只读检查与实际 CheckedExecution。
queue 的公开声明、source/operation facts、effects/resources 进入同版目录和原生检查；
queue payload 没有静态 schema，当前 Forge typed flow 明确拒绝这类源及其生产/ACK 接线，
不会猜字段或制造 ProducerRef。它们的公开运行由原生 topology/identity/commit 检查负责，
不把 API 发现、协议测试或静态拒绝冒充完整 Forge typed queue 执行验收。

完整M01–M09的API、入出、边界与源码/示例索引见 [完整能力映射](dataset-api-mapping.md)。
