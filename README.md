# demiflow

声明式数据管线引擎（自 [Demiurge](https://github.com/vincent9299/Demiurge) 独立化，2026-09-04 起独立维护）。

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
  - 调度：`data.plan.StreamStage` 规范算子（策略字段随算子声明）+
    `Dataset.map_stage` + `execution.stream.run_stages`（stage 列表一步执行 +
    退出期平台资源统一收尾）；
- **惰性批式路径**：Ray Data 兼容超集的 Dataset API（`from_items/map/filter/take_all/write_*`），
  确定性物理计划 + 本地线程池执行器；Ray 为可选 extras；
- **流式路径**（2026-09-04 新增）：`map_async` + `run_stream`——常驻 worker 协程 +
  有界队列 + 无序发射 + sentinel 逐级排空，为长夜跑采集/富化管线设计
  （认缺分级、字节级内存上界、Ctrl-C 收尾钩子）；
- **IO**：json/parquet/csv sink；Lance 版本化读写为 extras（`pip install demiflow[lance]`）；
- **LLM 算子**：`map_prompt`（schema 校验 JSON + 重试 + 图片输入，extras `[llm]`）。

## 快速开始

```bash
pip install -e .              # 核心：pyarrow/PyYAML/click/packaging（net/fetch/store/resume 零额外依赖）
pip install -e .[dev]         # + pytest
pip install -e .[collect]     # + 采集栈：crawl4ai（crawl）+ pillow（images）
```

依赖口径：crawl4ai 等重依赖全部 extras 化、机制内惰性 import——核心安装零重物。
SearXNG 类**服务**依赖不是 Python 包（PyPI 同名包为占位包），由消费方
自行部署（如 demiwtg-data 的 data_pipeline/webgate 模块），不进本库依赖。

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
写入返回 `None`；提交结果不确定时抛出带回执的 `LanceWriteError`，不自动重试。
平台不做主键合并、业务去重、运行冻结或完成状态管理。`write_lance` 不隐式运行异步链；
需要固定异步结果再写表时，显式使用下面的 `materialize()`。

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


### 执行方式

| | 惰性路径（sync 算子） | 流式路径（`map_async`） |
|---|---|---|
| 触发 | `take*/count/write_*` 动作 | `run_stream()`（同步入口，勿包进 asyncio.run） |
| 并发 | 物理计划按 stage 分配线程 | 每级 `concurrency` 个 worker 协程（单事件循环） |
| 顺序 | 保序（滑窗） | 无序（吞吐优先） |
| 背压 | 滑窗宽度 | 每级 `queue_depth`（载字节级深度即内存上界） |
| 失败 | 异常上抛 | `catch` 白名单=认缺计数；白名单外经 watchdog 终止整链 |
| 中断 | — | Ctrl-C → `on_drain` 收尾钩子（同步落盘放最前） |

混用约束：计划中含 `map_async` 时只能 `run_stream`；streaming 计划只接受
`map_async` 与 `filter`（折叠），其余 sync 算子显式拒绝。

## 独立化沿革

原为 Demiurge 的 `demiurge.demiflow` 子包；独立化时内联了两个共享模块
（`demiflow/_compat/error_transport|observability`），补写了原仓快照缺失的
`planning/__init__.py`，斩断了 Candidate/wheelhouse 平台耦合（零配置入口
`demiflow.data`）。工程细节见各模块 docstring。
