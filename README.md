# demiflow

声明式数据管线引擎（自 [Demiurge](https://github.com/vincent9299/Demiurge) 独立化，2026-09-04 起独立维护）。

新增、修改和评审代码须遵守 [开发约束：有限资源](AGENTS.md)。预算必须贯穿读取、转换、并发、临时结果和写入，不能默认输入字段很小；资源保证以各接口实际覆盖的分配范围为准。

普通行变换的本地流式并发使用 [`Dataset.map` 调度策略](docs/streaming-map.md)：
显式声明 `concurrency/queue_depth/execution/catch`，可用 callable 类及
`callable_scope='worker'` 隔离状态。默认 map 行为保持；普通惰性动作和 Ray
明确拒绝流式参数，Local `run_stream/materialize/checkpoint` 执行同一策略。

模型节点的流式 HTTP、连接池、超时与恢复契约见 [map_prompt_async HTTP 说明](docs/prompt_http.md)。
模型需要在当前行内与算子多轮交互时使用 [`agentmap_async` 算子环境](docs/operator_environment.md)，复用原生算子契约与同一节点的日志、预算及生命周期。
图片/文本向量使用原生 [`Dataset.map_embeddings`](docs/embeddings.md)，复用流式批处理、请求准入、服务生命周期和 SQLite HTTP 日志。
逐行向量检索使用 [`Dataset.search_vectors`](docs/vector_search.md)：每行输入一个查询向量，Lance 原生检索后将候选列表写回该行，可直接连接编码与下游任务节点。

## 定位

- **采集底座**（`demiflow.collect`，2026-09-04 起，机制归引擎、策略归消费方）：
  - `net`：按源限速闸门、分类重试、双池代理客户端、流式下载原语
    （限速表/代理名单/身份 UA 由消费方 `register_limits`/`PROXY_URL` 注册）；
  - `fetch.fetch_tiers`：多候选档位轮转 + 字节封顶 + 硬超时 + verify 内容钩子
    （verify 返回业务元数据随 Fetched.extra 透传）；
  - `store.AppendManifestStore`：内容寻址 blob 原子写 + jsonl 追加清单 +
    跨进程 fcntl 幂等去重（吸收式尾扫索引）；
  - `resume.scan_counts`：清单现算 done-set/计数（断点续跑依据，不落盘）；
  - `crawl.PageCrawler`：URL → 正文 Markdown（Crawl4AI 进程内封装，
    新旧版本兼容 shim + 惰性依赖，extras `[crawl]`）；
  - `images.verify_image`：字节 → 图像元数据（Pillow 全量解码 +
    mime/ext 规范表，fetch_tiers 的 verify 钩子，extras `[images]`）；
  - `search`：SearchEngine 协议 + 注册表 + `is_connect_failure`；
  - `llm`：AsyncLLMClient（单次 chat，重试归消费方口径）+ 端点资源注册表
    （`register_endpoint(base_url_env=...)` 配置驱动，env 覆盖跨机器零代码）；
  - `exec_curl`（2026-09-18，fleet 实战上移）：短命 curl 传输 + AIMD 节拍 +
    出口唯一身份（VM 长跑场景 asyncio 静默断连的解）；
  - `fleet`（2026-09-18）：worker_plan 分配 + systemd-run 托管发射 +
    幂等守卫 + 巡检（ssh argv 直传，无引号地狱）；
  - `cosio` / `cosqueue` / `queue_runner`（2026-09-20，第三代队列沉淀）：
    COS 签名对象存取（瞬态 403/429 退避重试）→ COS 任务队列（分批生产/
    认领校验/**成功才 complete**/超龄认领回收）→ 认领-算子-完成常驻循环
    （身份 KEY=VALUE env 注入，值可含括号；消费方只写批算子）；
  - [Dataset SQLiteQueue](docs/sqlite-dataset-queue.md)：`enqueue/read_queue/ack_queue`
    提供本机持久交接，`read_queue_records` 按固定序号独立归档到 Lance；
    `demiflow.queue` 管理有界通道、预算和只读进度，不需要独立队列服务进程；
  - 调度：`data.plan.StreamStage` 规范算子（策略字段随算子声明）+
    `Dataset.map_stage` + `execution.stream.run_stages`（stage 列表一步执行 +
    退出期平台资源统一收尾）；
- **惰性批式路径**：Ray Data 兼容超集的 Dataset API（`from_items/map/filter/take_all/write_*`），
  确定性物理计划 + 本地线程池执行器；Ray 为可选 extras；
- **流式路径**（2026-09-04 新增）：`map_async` + `run_stream`——常驻 worker 协程 +
  有界队列 + 无序发射 + sentinel 逐级排空，为长夜跑采集/富化管线设计
  （认缺分级、显式队列容量与资源预算、Ctrl-C 收尾钩子；队列项数本身不保证字节或 RSS 上界）；
- **IO**：json/parquet/csv sink；Lance 版本化读写为 extras（`pip install demiflow[lance]`）；
- **LLM 算子**：`map_prompt`（schema 校验 JSON + 重试 + 图片输入，extras `[llm]`）。

## 快速开始

```bash
pip install -e .              # 核心：pyarrow/PyYAML/click/packaging（net/fetch/store/resume 零额外依赖）
pip install -e .[dev]         # + pytest
pip install -e .[collect]     # + 采集栈：crawl4ai（crawl）+ pillow（images）
```

依赖口径：crawl4ai 等重依赖全部 extras 化、机制内惰性 import——核心安装零重物。
网页搜索、文档获取与阅读使用原生 `Dataset.search_web/fetch_documents/read_documents`；图片原始字节获取与共享 CAS 复用使用 [`Dataset.fetch_images`](docs/image-acquisition.md)；
`save_lance` 在流中小批提交，`run_stream` 负责资源生命周期及技术观测。
`admit_rows` 在同一流中逐行执行持久去重及多个按键配额，返回准入回执；
它不攒齐分组、不排序行、不识别业务含义，详见 [在线准入](docs/web_evidence.md#在线准入与持久配额)。
原生搜索安装 `demiflow[search]`，全部来源适配器及公共依赖随包交付；
无需 SearXNG 服务地址、端口、源码目录或独立解释器。旧显式 HTTP 接口仅作兼容保留。
完整接口、契约及服务配置见 [网页文档 Dataset API](docs/web_evidence.md)。

python smoke_standalone.py  # Dataset 惰性路径冒烟
python -m pytest tests/ -q  # streaming 路径 10 用例
```

```python
from demiflow import data


# 惰性路径
out = (data.from_items([{"x": i} for i in range(10)])
       .map(lambda r: {**r, "y": r["x"] * 2})
       .filter(lambda r: r["y"] > 5)
       .take_all())

# 流式路径：async 算子 + 认缺分级 + 有界背压
def build(ds):
    return (ds
            .map_async(fetch, concurrency=32, queue_depth=64,
                       catch=(TransientError,), label="fetch")
            .map_async(score, concurrency=48, label="score"))

stats = build(data.from_items(rows)).run_stream(
    on_progress=lambda s: print(s.summary()),
    on_drain=lambda s: cleanup(),
    log_every=100)
```

## Dataset 入口与模型算子配置

统一入口是 `from demiflow import data`，直接调用 `data.read_*`、`data.from_*`。
返回 `Dataset`，在其上连接处理算子及 writer。普通脚本默认本地执行，打包 Pipeline 继承
Driver 选择的 Local/Ray 执行器；`DataAPI` 仅为平台内部读接口实现，不再公开导出。
删除 `demiflow.standalone.local_data`，不提供旧名转调。prompt 配置、调用参数和预算均属于算子。

```python
from demiflow import data

(
    data.read_lance(input_uri, version=input_version)
    .map_prompt_async(
        'design', config='prompts/tasks.yaml',
        options={'timeout_s': 120, 'request_options': {'max_tokens': 4096}},
        max_requests=20, inputs={'concept': 'concept'}, output='question',
        concurrency=2,
    )
    .materialize()
    .write_lance(output_uri, mode='overwrite', schema=output_schema)
)
```

`map_prompt` 和 `map_prompt_async` 都接受 `config`、`options`、`max_requests`。
`config` 是已解析的 `PromptPack` 或真实 YAML 路径，不是注册别名；声明时加载并绑定。
普通脚本的相对路径基于当前工作目录，打包 Pipeline 的相对路径基于其资源目录。
`options` 在声明时复制，作用于当前节点；同步调用同样支持 timeout、请求参数、offline 和日志复用。

请求上限覆盖该节点的所有行、worker 和 schema 重试；同一节点重复执行继续累计，
声明另一个节点有独立预算。缓存响应和 offline 不消耗新请求。调用日志可复用，
但日志总行数不再作为多个节点的公共上限；重新建立节点不继承上次节点的内存计数。
执行器的调用量统计只用于监控，不合并预算。Local 和 Ray 的同步节点均执行此预算规则；
异步节点目前仍只支持 Local。打包 Pipeline 的静态资源检查仍要求 YAML 字面量路径。

本地 HTTP 调用可配置 `options={'sqlite_journal': {'path': 'calls.sqlite', 'timeout_s': 30}}`。
记录保留精确请求键、去除内联图片字节的元数据、原始响应及错误；图片由输入 Blob 继续提供。
缓存查询、请求哈希和持久化在专用执行器中完成，完成响应先提交再返回，命中缓存不加载模型。
新调用的 `call_output` 带 `request_id/journal_path`，不为产生调用位置再读回请求/响应。
无响应的占位继续报 `UncertainPromptCall`，不隐式重发；模型验证在并发首批中只执行一次。

`options={'sqlite_journal': {'path': 'calls.sqlite', 'read_only': True}}` 只读已有日志。
它不建库、不补写导入请求元数据、不预留新请求，也不恢复 uncertain；缓存缺失返回
`PromptReplayMissError`（与新增请求预算耗尽区分），在服务加载和 HTTP 模型验证之前停止该请求。
`max_requests=0` 仍可显式作为节点预算；只读模式本身也禁止新请求。
HTTP 节点支持 `options={'model_revision': 'weights-v2'}` 区分同地址、同模型名的部署；
标识只进入请求身份和原生日志，不作为 provider 参数发送。省略或 None 保持旧请求键，
不能用当前 revision 冒充历史部署。外部取消传播出流执行，调用方不会获得部分物化成功。

旧 Lance 调用表可在运行前用 `SQLitePromptJournal.import_lance(root=..., relative_uri=..., version=...)`
按固定版本导入新库。迁移只读取请求键/写入版本与响应正文，不读请求中的 base64，不修改旧表；
原请求身份保持，历史 RecordRef 作为来源元数据带入；它不要求永久保留旧日志的历史快照。
迁移事务完整提交后才生效，重复导入相同快照不读旧表。
这是历史调用日志的只读导入入口；HTTP、Codex 与 offline 已统一接入原生 SQLite 日志。
业务输入/输出仍用标准 Dataset reader/writer。完整迁移、恢复和队列用法见 [平台 0.2 升级指南](docs/platform-upgrade.md)。

### 模型服务声明与算子生命周期

本地模型服务与 prompt、调用选项一样，显式绑定在模型节点上；入口仍是
`data.read_* → 处理算子 → map_prompt_async → materialize → writer`。
服务声明不读数据、不启动进程，数据行也不携带进程句柄。

```python
from demiflow import data
from demiflow.services import VLLMService

(
    data.read_lance(input_uri, version=input_version)
    .map(prepare_input)
    .map_prompt_async(
        'design', config=prompt_pack,
        inputs={'concept': 'concept'}, output='question',
        options=call_options, max_requests=request_budget,
        service=VLLMService(service_config, root=workspace,
                            log_path=service_log, log=print),
        concurrency=concurrency,
    )
    .map(check_result)
    .materialize()
    .write_lance(output_uri, mode='overwrite', schema=output_schema)
)
```

- **业务选择**：prompt 中的模型名/端点，服务权重路径、GPU、DP/TP、容量和超时；
  notebook 或调用方提供实验值。模型名/端点只在 prompt 声明一次。
- **平台机制**：第一个需要实际请求的行才启动服务；同节点全部 worker 共用一次加载。
  就绪检查通过后才发送请求；正常结束、失败和协作式取消沿原生 actor `aclose` 回收。
  空输入、全部跳过、预算为零或完整响应复用不加载模型。
- **执行边界**：生命周期到当前流动作退出为止。同一流上的节点可能重叠；
  不同模型复用同组 GPU 时，在两段之间显式 `materialize()`，前段释放后再执行后段。
  不隐式顺序调度模型，不按行启动/释放，不提供全局服务注册表。
- **请求语义**：`service` 不进入请求载荷、缓存身份和模型调用日志；原生模型参数、
  schema、预算与完整响应复用机制保持。权重必须与声明的模型名一致。
- **适用范围**：当前仅 Local `map_prompt_async` / `map_embeddings`，使用独立安装的 vLLM CLI 和本机
  HTTP `/v1` 端点；支持任意显式本地端口。`service=None` 使用外部服务。
  offline/其他传输与托管服务组合在声明时拒绝；同步和 Ray 路径尚未提供托管服务。

`service_config` 为字典，`model_path` 必填。多模态模型可配置 `limit_mm_per_prompt={"image": 4}`，直接传入 vLLM；不包含业务图片选择策略。可用 `vllm_config(value)` 纯校验、补齐默认值；
`VLLMService` 同样校验并复制配置。默认 `gpus=[0]`、DP/TP=1、
`gpu_memory_utilization=0.9`、`max_model_len=8192`、`max_num_seqs=16`、
`max_num_batched_tokens=16384`、`api_server_count=1`、`enforce_eager=False`。
DP×TP 必须等于 GPU 数；`python` 默认当前解释器，`cuda_bin=/usr/local/cuda/bin`。
向量服务可声明 `runner='pooling'`、本地 `chat_template`、`dtype`；必要时显式设置 `trust_remote_code`。
两类模型算子均可接入 `AdaptiveRequestGate` 动态调整实际请求并发；GPU 数量、模型副本及显存预算仍由部署配置固定，不自动扩缩容。
启动/停止超时为 `startup_timeout_s=600` / `shutdown_timeout_s=30`；
就绪单次请求 `readiness_timeout_s=2`、轮询 `poll_interval_s=1`、等待日志间隔
`startup_log_interval_s=15`，单位均为秒。服务 stdout/stderr 追加到 `log_path`，
缺省 `_demiflow/services/vllm_<port>.log`；加载/就绪/释放事件交给 `log`，缺省 Python logging。

端口或 GPU 协作锁已占用时明确失败，不接管未知服务；所有共享 GPU 的任务应使用同一
workspace root。启动/就绪失败属于节点资源错误，即使设置 `error_output` 也终止执行，
不把整批行记成模型业务失败。只清理本次创建的进程组；强杀宿主不保证清理子进程。
平台不下载权重，也不改变业务表、阶段选择或已有调用记录。

## 双执行路径语义

### Lance 写入

`Dataset.write_lance()` 是同步 Dataset 的终结动作，默认 `mode='append'`。
`mode='overwrite'` 用本次全部数据提交一个替换版本，不逐批覆盖；旧版本仍可按版本号读取。
两种模式都可以创建尚不存在的目标。追加要求目标 schema 一致；覆盖可以使用新的 schema。

```python
import pyarrow as pa

schema = pa.schema([('id', pa.int64()), ('text', pa.string())])
data.from_items(rows).write_lance('results.lance', mode='overwrite', schema=schema)
data.from_items(more_rows).write_lance('results.lance', mode='append', schema=schema)
# 空输入必须显式给出 schema；覆盖时会得到有效的空表。
data.from_items([]).write_lance('results.lance', mode='overwrite', schema=schema)
```

可选 `expected_version=N` 约束提交基于指定的当前版本；版本过期或写入期间发生竞争提交时拒绝写入。
compare-and-append 仍要求至少一行；覆盖支持带 schema 的空输入。
写入默认返回 `None`，`return_receipt=True` 返回实际版本与行数回执；提交结果不确定时抛出带回执的 `LanceWriteError`，不自动重试。
平台不做业务去重、运行冻结或完成状态管理。`write_lance` 不隐式运行异步链；
需要固定异步结果再写表时，显式使用下面的 `materialize()`。

### 部分列更新与增列

已有表可按唯一非空键（或复合键）增量更新，不要求每次输出整行：

```python
receipt = data.from_items([{'id': 1, 'text': 'new'}]).write_lance(
    'results.lance', mode='merge', on='id', update_columns=['text'],
    when_not_matched='error', expected_version=3, return_receipt=True,
)
print(receipt.committed_version, receipt.merge_stats)

# 显式添加 nullable 顶层列，已有行初始值为 null；不重写旧字段。
schema_receipt = data.add_lance_columns(
    'results.lance', pa.schema([('quality', pa.string())]),
    expected_version=receipt.committed_version,
)
data.from_items([{'id': 1, 'quality': 'checked'}]).write_lance(
    'results.lance', mode='merge', on='id', update_columns=['quality'],
    expected_version=schema_receipt.committed_version,
)
```

- `merge` 要求目标已存在；仅更新输入的非键列。未提交的列保持原值，显式 null 则清空。`update_columns` 可限定确切列集合，多列/缺列都拒绝；省略时用输入列。嵌套字段按顶层列整体更新，列表内部合并属于业务逻辑。
- 源和目标键均须唯一、非空，支持整数、字符串、binary 键；重复目标不能靠 merge 自动清理。`when_not_matched` 默认 `error`（有缺失键则整批不提交），也可 `ignore` 或 `insert`；插入时未提供目标列为 null，仍须满足目标约束。
- 数据沿现有目标类型校验，不隐式加列、改类型。`add_lance_columns` 只添加新的 nullable 顶层列；已有字段重名、非 nullable 字段均拒绝。增列与之后填值是两个独立版本，不伪装成一个跨操作事务。
- 整个 merge 只提交一次（Ray 也使用单 writer），校验失败不发布部分数据。`expected_version` 过期或读后提交竞争抛 `LanceWriteConflict`；即使未显式传版本，也不自动将部分列计算结果重放到竞争版本。
- 本地固定 Lance 补丁的部分列更新由平台自动选择写入策略，已知较大范围更新可复用其他列的数据文件；不新增业务策略参数。条件、验证与内存边界见 [Lance 合并说明](docs/lance_merge.md)。
- 回执包含 `updated_rows/inserted_rows/ignored_rows`，`written_rows` 为前两项合计。无匹配且忽略、空 patch 等无数据更新情况返回当前版本；相同值的已匹配行仍由原生 Lance 决定是否提交，业务幂等应在调用前识别无变化。

### 固定异步结果

local Dataset 的 `materialize()` 同时支持同步与异步链：

```python
cached = (
    data.read_lance(source_uri, version=source_version)
    .map_async(enrich, concurrency=4)
    .map(project_result)
    .materialize()
)
cached.write_lance(target_uri, mode='overwrite', schema=schema)
# 再次读取缓存，不重新执行 enrich。
count = cached.count()
```

这是同步 action，应在活动事件循环之外调用（notebook 可放到线程）。异步计划复用
`run_stream()` 的有界队列、错误传播和 actor 关闭机制，只支持该执行器现有的流式算子；
不因此扩展关系算子的流式支持，也不保证异步输出顺序。含 join / flat_map 等同步关系处理的
前缀应先按现有方式物化，再接异步节点。

缓存采用本地块缓存及原有内存限额，超出后溢写到临时文件。全部成功才返回
`MaterializedDataset`；失败清理本次创建的溢写，其他缓存仍有效。执行器关闭时释放其缓存。
物化不是持久 checkpoint，也不包含任何业务评分、指纹、去重或提交规则。

物化缓存逐行编码一次，以编码字节及行数共同控制块大小；达到内存预算前先刷新已有块，
单个超限行立即溢写。内存块保存编码快照，溢写逐行恢复，不因读取一行解码整个块，
也不让输入/消费者对可变对象的后续修改改变缓存。此预算只约束缓存载荷，
不包含单行解码对象、上游扫描/预读、Arrow、在途任务和用户代码的内存，不能视为进程RSS硬上限。

宽行来源可用 `data.read_lance(..., batch_size=1, batch_readahead=1, fragment_readahead=1)`
显式控制原生Lance扫描；三个参数均为正整数，省略时保持原缺省行为。
应同时使用列投影和过滤，避免加载不需要的大字段。行数限制无法拆开一个巨大JSON字段，
Lance内部解码仍可能需要显著内存；巨型嵌套来源应由业务改用固定引用和必要字段。


### 本地关系执行与并行

默认 local 路径的 `join / reduce_by_key / group_batches` 统一交给平台内部 DataFusion
处理关联和稳定键排序。连续固定 Lance 关联尽可能组成同一列式计划，Python 回调
是明确边界。业务使用原有 Dataset 方法，空值不匹配、多对多乘积、组内次序、
缺失右字段和列名冲突规则保持。固定类型的关联结果可直接通过 Arrow 交给标准 writer。

`chunk_bytes` 保留调用兼容，但不再决定原生引擎内存；预算、并发和进程清理由平台统一管理。
Python 任意 reducer 按完整组的稳定顺序执行，不自动假定可拆分合并。原有 Python
外排内核的性能文件保留为历史参考；当前实际原生计划与统计见 action 执行元数据。
详细说明见 [Dataset 关系执行](docs/datafusion.md)。关系算子当前仍限本地后端。

### 单机 Python 任务并行（显式配置）

`data.local_execution(...)` 配置 Dataset 中 Python 读取与回调的任务并行；关联和键排序由
平台原生内核管理。安装 `demiflow[local,lance]` 后，reader、变换和 action 都放在 context 内：

```python
from demiflow import data

with data.local_execution(workers=4, worker_mode="process", partitions=16,
                          memory_bytes=256 * 1024**2) as session:
    images = data.read_lance(image_uri, version=image_version,
                             columns=["sha256", "size_bytes"])
    selected = data.read_lance(selected_uri, version=selected_version,
                               columns=["sha256"])
    result = images.join(selected, on="sha256", how="semi").count()
    print(session.stats)  # 实际任务数、输入/输出行数、worker、耗时及完成状态
```

`process` 使用 spawn + cloudpickle，适合纯 Python CPU 算子，支持 notebook 中
定义的函数；脚本入口需用 `if __name__ == "__main__":` 保护。`thread` 使用线程池，
适合 I/O 或释放 GIL 的原生计算。Python 窄变换使用有界任务池；关系计算由下文的
平台原生内核管理，不再经 Python shuffle。连续关联可融合，任意 Python reducer
按原组内顺序处理原生排序结果，不擅自拆分或重试用户回调。

现阶段支持同步 map/filter/flat_map、投影/重命名、map_batches、limit，以及
inner/left/semi/anti join、reduce_by_key、group_batches。聚合 action 的最终合并和
writer 仍由协调端执行；不支持的变换在扫描前报错。context 不覆盖已绑定的 Pipeline driver；普通脚本和本地 driver 的关系节点同样走
原生内核。跨机器 Ray 路径仍独立，不在本地引擎中执行。

回调必须只依赖当前行/批次/分组和显式参数。任务之间不共享 Python 对象；业务
map 中修改的驱动端计数器不能用作全局进度，应读取框架的 `session.stats`。
`memory_bytes` 是 Python 分区任务的工作预算，不含进程、Arrow、在途批次和
用户 reducer 状态；原生查询的预算由平台资源池单独管理。
异常或提前结束会回收本次 worker 和临时文件，不自动重试有副作用的用户函数。

执行边界、Rust 后续接入方式与可复现测量见 [单机内核说明](benchmarks/LOCAL_KERNEL.md)。

### Dataset 统一关系执行

本地 Dataset 的 `join/reduce_by_key/group_batches` 自动使用平台内部 DataFusion 关系内核，
业务继续通过 Dataset API 读表、关联、回调和写表，不写执行 SQL、不创建引擎会话。
固定 Lance 表的连续关联保持列式；Python 回调仍按既有语义执行，平台保留缺省字段、
空值、重复多重性和组内顺序。标准 writer 的提交及恢复协议保持。

原公开 DataFusion 会话和 Options 导出已删除，业务不导入 execution 内部模块。
原生进程、共享名额、资源和失败清理由平台管理。具体行为、资源边界和 Dataset 示例见
[关系执行说明](docs/datafusion.md)。旧 MapReduce 分区执行记录是历史测量，不代表当前关系内核。

### 执行方式

| | 惰性路径（sync 算子） | 流式路径（`map_async`） |
|---|---|---|
| 触发 | `take*/count/write_*` 动作 | `run_stream()`（同步入口，勿包进 asyncio.run） |
| 并发 | 物理计划按 stage 分配线程 | 每级 `concurrency` 个 worker 协程（单事件循环） |
| 顺序 | 保序（滑窗） | 无序（吞吐优先） |
| 背压 | 滑窗限制在途任务数 | 每级 `queue_depth` 限制队列项数；变长载荷还需字节准入，项数不是 RSS 上界 |
| 失败 | 异常上抛 | `catch` 白名单=认缺计数；白名单外经 watchdog 终止整链 |
| 中断 | — | Ctrl-C → `on_drain` 收尾钩子（同步落盘放最前） |

流式计划支持 `map_async`、`batch_map`、普通行 `map` 与 `filter`。
可用 `run_stream()` 执行，或 `materialize()` 固定结果后连接标准 writer；
其余同步关系操作需先物化上游，再继续连接。

阻塞式 Blob 读取、图片解码和编码可直接声明线程执行，无需业务包装
`asyncio.to_thread`：

```python
prepared = source.map_async(
    prepare_row, execution='thread', concurrency=32, queue_depth=16,
)
```

`execution='thread'` 每级使用独立线程池；实际在途调用不超过 concurrency，
与日志 I/O 的专用池及源读取池隔离。同步函数仍返回 row/None/list[row]，
异常仍按 catch 白名单处理，contextvars 传入工作线程。函数实例在该级共享，
须保证线程安全；纯 Python CPU 密集任务应使用进程内核，不承诺线程加速。
旧的默认 `execution='inline'` 不变，普通 `map` 也不隐式改成线程执行。

线程不能强杀：取消或其他节点失败时先停止投喂、等待在途线程退出，再调用
actor 的 aclose。线程模式拒绝异步函数和 hard_timeout；阻塞 I/O 必须自行
设置有限超时。模型服务、HTTP 请求和日志的持久化/不确定调用语义保持原样。

## 独立化沿革

原为 Demiurge 的 `demiurge.demiflow` 子包；独立化时内联了两个共享模块
（`demiflow/_compat/error_transport|observability`），补写了原仓快照缺失的
`planning/__init__.py`，斩断了 Candidate/wheelhouse 平台耦合（零配置入口
`demiflow.data`）。工程细节见各模块 docstring。

## Codex CLI 图像工具与文件交付

原生 `map_prompt_async` 可使用已有 `codex_exec` transport。每次请求独立执行
`codex exec --ephemeral --ignore-user-config`，主模型由 prompt pack 配置。

```python
options = {
    'sqlite_journal': {'path': '/absolute/data-root/calls.sqlite'},
    'timeout_s': 900,
    'codex_exec': {
        'bin': 'codex', 'reasoning_effort': 'high', 'web_search': 'live',
        'image_generation': True,
        'artifact_store': {'directory': '/absolute/data-root/objects'},
        'max_artifact_files': 8,
        'max_artifact_bytes': 64 * 1024 * 1024,
    },
}
```

`image_generation` 可显式启用/关闭 CLI 原生功能；True 要求配置产物存储。
省略时保持原 CLI 默认行为及原请求身份，已有 T2I 调用无需修改。
配置 artifact_store 后使用临时目录的 workspace-write；排除全局 `/tmp`、
`TMPDIR` 的额外写范围，shell 网络保持关闭。未配置产物时仍为 read-only。
原生 web/image 工具由 Codex 提供，不在 demiflow 内实现图像 API 或模型选择。

运行环境在请求末尾追加 `CODEX_EXEC_FILE_CONTEXT` JSON，包含实际
`input_images=[{image_number,path,mime}]`、`artifact_directory` 与文件限额。
消费方 prompt 应要求模型把交付文件复制到此目录，并在结构化答案中返回文件名。
接入层不解释答案里的业务字段，也不会按模型返回的任意路径取文件。

进程退出后、临时目录删除前，接入层收集该目录中的普通文件并通过
LocalObjectStore 保存独立文件，响应 metadata 的 `artifacts` 返回
`[{name,byte_size,object_ref}]`。仅支持平铺文件；符号链接、硬链接、目录、
特殊文件和超限产物使调用交付失败。文件类型/像素/业务判据由消费方检查。
这是通用文件交付能力，也可用于非图片文件。

产物读取和独立对象写入在平台 I/O 线程执行，取消时排空写入后才清理临时目录。
执行失败/超时的已导出文件可留作诊断，但不会把失败调用转换为成功。
调用日志保留原始 stdout/stderr、最终答案、实际文件上下文和产物引用。
复用完整响应时校验独立对象 URI 与 SHA256；文件丢失/损坏报错，不自动重新付费执行。
文件限额、存储位置、显式图像功能开关和文件协议进入缓存身份，随机临时路径不进入。

本接口验证文件实际交付，不证明模型确实使用过某个工具或文件内容正确。
具体图像模型型号、模型看图结论和工具尝试次数不由此接入层保证。
Codex 原生调用现已统一使用 SQLite journal，文件交付使用 ObjectRef(uri, sha256)。

## 独立对象与表内 Blob

`demiflow.objects.ObjectRef(uri, sha256)` 直接读取普通 file/HTTP/对象存储 URI 并验证 SHA；`read(max_bytes=N)` 最多读取 N+1 字节，超限拒绝，不先缓冲整个对象；不传上限时保持原行为。`verify()` 分块校验而不把完整对象驻留内存。`LocalObjectStore(directory).put/put_stream` 将内容写成普通文件，返回 `{uri, sha256}`，相同内容共享文件，并发发布不覆盖已有对象。directory 是持久目录，不是可随意清理的缓存；file URI 需要各机器可访问相同稳定挂载路径，跨机器对象存储部署应使用稳定对象 URI，不能把有时效的签名链接当长期引用。

仍保留 Lance 原生表内 Blob 读写。移除 LanceBlobStore 跨表引用写入和 BlobAssetReader 项目式 SHA 查表解读；`demiflow.lance.blobs.BlobRef` 仅只读历史数据，不用于新交付。ArtifactSet 新 schema 用 object_uri，旧 schema 可 read/verify，但要生成新引用须先显式迁移。源表快照 DatasetRef 与图片 ObjectRef 各有用途，后者不依赖 Lance、登记表或项目路径映射。普通 URI 读取没有消除图片解码的内存开销。

## 原生多源检索

`Dataset.search_web` 可通过 `WebSession(search=SearchConfig(...))` 使用随包交付的 SearXNG 适配器，无需部署搜索 HTTP 服务。安装 `demiflow[search]`；配置、逐请求语言、来源状态、回执、自定义来源及切换说明见 [原生检索文档](docs/native-search.md)。

## 流式按键组批与固定版本查找

`group_batches(on, max_rows=4, flush_interval=2, chunk_bytes=..., max_groups=64, buffer_bytes=...)` 显式选择在线分组，不等待全局排序。原生 `StreamGroupBatchesOp` 在每键达到条数、首行等待到期、上游结束或缓冲压力时提交 `{键字段, items: [...]}`；一条输出 row 就是一个组，可连续传给多个 `map_prompt_async`。不同键不混组，不宣称知道整键的 `group_last`。不设置 flush_interval 时保留原离线分组语义。

分组的 chunk_bytes/buffer_bytes 使用嵌套 Python 对象的保守尺寸估计，限制保留的行载荷；另有 max_groups、每行最多10万个对象、输入队列1及单 worker 上界，单行超限报错。缓冲不足会提前提交小组，不丢行、不无限等待稀疏键。这些约束不等于全进程RSS上限；输入队列、输出组、回调及下游并发仍须分别核算。

`lookup_lance({uri, version}, on='key', columns=..., output='matches', max_matches=1, max_bytes=...)` 为每个流中记录附加固定版本的准确匹配结果，无需物化输入。匹配数与返回的 Arrow/Python 载荷有上限，歧义和超限明确失败；单 actor 的结果缓存至多128键/8MiB，返回独立副本。Lance 内部扫描分配不受这个返回载荷上限替代约束。

`save_lance(..., key=['field1', 'field2'])` 支持复合键，校验和不确定提交核对均使用全部键字段。它可在保存组内模型结果后继续传递该组，或在逐图展开后按概念/SHA交付，不写业务规则。

`deduplicate(on, path=..., key='record_id', output='deduplication', when=None, keep_duplicates=False, ...)` 是独立的 `DeduplicateOp` 计划节点，由现有单 worker 流式 actor 执行。`on` 为 1–32 个去重字段，`key` 为稳定输入身份；逐条持久化首个到达者的所有权后交付，不排序、不等待同键输入齐全。默认过滤重复行；`keep_duplicates=True` 保留全部行及 admitted/duplicate/skipped 回执，供消费者记录业务状态。when 为 false 的行透传，不读取其去重键。

去重和准入共用 SQLite 键状态实现，去重节点不接受业务额度。默认至多10万个已接受身份、每条键16KiB、数据库主文件256MiB（回滚日志可能另占同等空间）；只存键摘要，不存图片和整行载荷，另有至多 max_entries 个32字节摘要及 Python 容器开销、2MiB SQLite 页缓存。超限明确失败，不清空状态、不把资源上界当业务淘汰。乱序重放仍保留首次获准的身份，每次 action 交付一次；下游外部请求须另用请求日志保证不重复副作用。无额度的既有 admit_rows 日志可原样接续。需要“去重＋额度”原子保留的场景继续用 admit_rows，避免两个节点之间中断导致限额所有权漂移。测试覆盖提前交付、复合键、乱序续跑、下游失败与资源上限。

`AdmissionQuotaReader(path, quotas=..., max_key_bytes=16384).remaining(row)` 可只读查询同一准入账本的剩余额度，供上游业务actor在已满额时停止无效工作。不会创建账本、预占或清空计数；声明须与writer的额度及键预算一致，最多16次索引查找、64 KiB配置和2 MiB页缓存，使用后close，单实例串行调用。正余额只是快照，真正准入仍由下游 `admit_rows` 原子决定；不据此跳过已预留、尚未交付的下游工作。

## 流式检查点与增量恢复

`from demiflow import StreamCheckpoint` 提供本地单写入者流图的提交版本清单。调用方在持有整轮 `run_lock` 时创建 `StreamCheckpoint(path, identity=..., initial=...)`，将其传给 `Dataset.run_stream(checkpoint=...)`；参与节点必须使用 `save_lance(mode='append')`。identity 声明固定输入、协议、业务配置和外部账本身份，initial 仅供首次显式导入固定阶段版本。后续恢复不任意选取最新表头，身份变化、缺失节点及不属于检查点的表头都会报错。

一次写入先持久化有界提交意图，再提交 Lance，原子发布全图版本清单后才交付下游。进程若在提交和发布之间退出，会按主键、内容摘要和版本核对那一次提交。提交锁只串行化存储提交，搜索和模型请求仍可并发。每个已发布版本清单是可恢复的提交边界；这不等于任意第三方调用具备分布式事务或恰好一次语义。

`save_lance(mode='append', when=..., max_rows=..., max_row_bytes=..., max_key_bytes=...)` 保留已提交行；同键同内容验证后不再追加，同键不同内容失败。when 未选中的行直接向下游传递。默认最多100万行、单行 Python 载荷16MiB、键4KiB；存储键索引只保留32字节摘要及 Python 容器开销，另计扫描预读、微批副本和 Arrow 内部分配。检查点清单默认最多1MiB，提交意图包含当前微批的键与摘要，不含完整载荷。

`live.prepend(pending_dataset, max_rows=..., max_row_bytes=...)` 将固定版本的待处理记录直接接入当前流节点。只接受没有异步 actor 的有限同步 Dataset；单行在线程读取，受下游背压约束，前缀结束后接续上游实时数据。前缀的投影、扫描预算及未完成关系由调用方声明，平台不推断业务终态。禁止借它藏入另一条异步 pipeline。

全链恢复还要求消费者声明无损交接关系：扇出完成标记须晚于所有子项提交，零输出也需确认；组批应在外部调用前保存组身份和独立对象引用，调用结果需在展开交付前持久化。内存队列通过上下游检查点的差集恢复。原生模型及网络账本、准入额度保持单调，不随表版本回滚，不自动重发结果不明的外部请求。仅使用追加 sink 而没有这些关系，不能宣称已实现完整 pipeline 恢复。
