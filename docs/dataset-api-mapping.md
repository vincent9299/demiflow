# 能力到 Dataset API 的完整映射

原始意图是 M01–M09 整张能力表的沉淀。获取、来源展开、搜索、任务分发与结果确认，
以及普通自定义处理各有边界；`fetch` 是其中一项。
以下接口与当前原生执行器一致，详细参数、预算和恢复协议见 [原始获取与队列](dataset-fetch.md)。

| 项 | 能力与 API | 输入 → 输出 | 使用边界 | 源码与可运行证据 |
| --- | --- | --- | --- | --- |
| M01 | 原始获取 `Dataset.fetch(request='request', output='result', store=..., policy=...)` | 一行一 URI，可声明 size/SHA/version → 原行＋完整原始获取 receipt | HTTP(S)/COS；流式有限预算；不解析/验证业务质量；prepared 不重复 GET | [raw_fetch](../demiflow/execution/raw_fetch.py)、[raw tests](../tests/test_raw_fetch.py)、[COS protocol](../tests/test_raw_cos_protocol.py) |
| M02 | 验证 `map / map_async / batch_map(fn/actor)` | raw receipt → 原 receipt＋独立 acceptance | task 图内只准入同步逐行 fn/静态无状态 actor；丢行、扩行、可变状态、额外效果不 ACK。批/异步处理在结算后的固定结果上做 | [task maps](../demiflow/execution/queue_execution.py)、[公开正反例](../tests/test_queue_postprocessing.py)、[独立解码](../tests/test_image_fetch.py) |
| M03 | 来源解析/候选选择 `map / flat_map / filter` | 已获取 metadata / 固定 records → URI 任务行＋provenance | 来源规则由消费者实现；获取 task 图内不丢/扩行，producer 和结算后图可以有界展开/过滤 | [元数据链接示例](../examples/fetch_metadata_links.py)、[producer](../demiflow/execution/queue_producer.py) |
| M04 | 关键词候选 `search_web(requests='queries', output='search', session=...)` | 有限 query 列表 → 候选＋来源回执 | 当前 native Dataset/producer 只接有界 HTTP SearchConfig；producer 先固定实际响应再展开发布。候选不是业务验收 | [关键词示例](../examples/fetch_keywords.py)、[搜索恢复](../tests/test_queue_producer_recovery.py) |
| M05 | 发布 `enqueue(config, producer=..., messages='messages').run_stream()` | 固定 manifest / metadata / keywords 来源，messages 含 stable task_id/payload → 原行＋queue_publish | open/增量发布/实际 EOS/seal；自动 fingerprint；0/多输出有输入 barrier；delivered 与 inserted 分开 | [固定清单示例](../examples/fetch_manifest.py)、[发布协议](../tests/test_queue_v2_protocol.py) |
| M06 | 领取/接管 `data.read_queue(config, pool=..., max_tasks=..., max_inflight=...)` | 队列/pool/generation＋有限窗口 → payload 行＋展示 `_queue_delivery` | 所有权来自 native sidecar/lease/fence；空且未 seal 不算完成。prepared 复用原成果，completed 不默认重放，历史另显式读 | [source](../demiflow/execution/queue_source.py)、[故障恢复](../tests/test_queue_v2_crash.py)、[双进程示例](../examples/raw_fetch_queue.py) |
| M07 | 标准提交 `write_datasink(QueueResultSink(...))` | 一行＋真实 acquisition receipt → terminal `None` | 原生 CommitCoordinator；completion/task done/final acked/清 token 同 SQL 事务。未知、失败或不守恒不能假成功 | [result sink](../demiflow/execution/queue_sinks.py)、[事务边界](../tests/test_queue_v2_transactions.py) |
| M08 | 显式结算 `ack_queue(config, result='result', keep_row=...).run_stream()` | source-owned delivery＋真实 receipt → 原行或 completion 摘要 | `settlement='explicit'`，只能图尾，与自动 sink 互斥；复用同一个最终提交/ACK 协议 | [ACK](../demiflow/execution/queue_ack.py)、[显式示例](../examples/fetch_manifest.py)、[正反例](../tests/test_queue_postprocessing.py) |
| M09 | 后处理 `read_queue_records(..., through=snapshot, results=True).map/…/filter/join` | 固定 publication 前缀 → 原持久行＋proof＋verification，再普通解析/清洗/关联 | history 无 claim/repair/ACK；有限页/行/字节/hash/时间，metadata 仍为 not_checked；按普通 Dataset 资源/状态契约运行 | [只读 history](../demiflow/execution/queue_history.py)、[共享读取验证](../tests/test_queue_object_reads.py)、[示例后处理](../examples/fetch_source_common.py) |

`add_column` 是批语义，不接 task 图。逐行加字段用普通
`map(fn, inputs={'parameter': 'column'}, output='new_column')`。
投影/删除/部分重命名须保留 fetch 后的 result 和 `_queue_delivery` 展示；
原生 sidecar 独立传递，终端仍核对真实 receipt 和恰好一行交付。
未知 actor 状态或构造资源不因 `snapshot_state={}` 获准，普通 Python 纯回调也不是副作用沙箱。

共享域换挂载后，原 result/raw_ref/storage_domain/proof 不变。
显式 `LocalObjectStore.resolve(raw_ref, domain=storage_domain)` 返回当前 URI 和同一 SHA，
无需旧挂载仍存在。需要域根/no-follow 保护的实际读取使用现有
`read_local_object(ref.uri, ref.sha256, root=store.directory, max_bytes=...)`。
resolve 只核对地址/域；普通 ObjectRef.read/verify 的既有 URI/别名行为不全局改变。
公开后处理和 history 消费、GET=1、原证据不变及失败边界见共享读取验证。

旧 `Dataset.fetch_images/fetch_documents`、`Download`、`WebSession.fetch/fetch_image`、
`WebClient.fetch`、`ImageClient/ImageFetchPolicy`、专属下载路由/会话池与旧 queue runner 已移除。
通用解析、显式导入、历史对象/回执读取，以及有实际消费者的独立运输/搜索原语保留。
历史规范/封存源码不重写，外部业务迁移单列，不能用旧入口消费者证明另一路组合需要保留。

typed Forge queue 当前明确不准入：缺少固定 schema 与 managed queue permission contract；
普通 typed fetch 的现有检查不等于完整 Forge queue 验证。
SQLite 使用本机安全盘，PostgreSQL 是真实事务后端；本机多进程/私有PG/HTTPS协议验证
不能证明真实多host、共享挂载或云COS可用。该候选尚须独立终审和主仓安全集成窗口。
