# Prompt journal 存储吞吐对比与真实 canary

入口仍是 `SQLitePromptJournal`（`demiflow/operator_llm/sqlite_journal.py`）。默认
`throughput=None` 保持原实现；只有显式传入 `throughput` 才启用分组提交、数据库租约
和可选分片。本文件记录可复核的有界压测与真实模型 canary，二者不能相互替代。

## 1. 存储 A/B：同一请求流、同一并发

```bash
cd /yzp/zhaozy/yangzepeng/0905/demiflow

# 本地低延迟文件系统（/tmp，xfs）
../env/bin/python -B benchmarks/journal_throughput.py \
  --mode legacy  --requests 1000 --concurrency 64 --response-bytes 4096 --json
../env/bin/python -B benchmarks/journal_throughput.py \
  --mode batched --requests 1000 --concurrency 64 --response-bytes 4096 --json
../env/bin/python -B benchmarks/journal_throughput.py \
  --mode sharded --shards 4 --requests 1000 --concurrency 64 --response-bytes 4096 --json

# 共享项目文件系统（/yzp，dpc），日志目录用完即删
../env/bin/python -B benchmarks/journal_throughput.py \
  --mode legacy  --requests 1000 --concurrency 64 --response-bytes 4096 \
  --path /yzp/zhaozy/yangzepeng/0905/_bench_journal/legacy.sqlite --json
../env/bin/python -B benchmarks/journal_throughput.py \
  --mode batched --requests 1000 --concurrency 64 --response-bytes 4096 \
  --path /yzp/zhaozy/yangzepeng/0905/_bench_journal/batched.sqlite --json
../env/bin/python -B benchmarks/journal_throughput.py \
  --mode sharded --shards 4 --requests 1000 --concurrency 64 --response-bytes 4096 \
  --path /yzp/zhaozy/yangzepeng/0905/_bench_journal/sharded.sqlite --json
```

每个请求执行与 prompt client 相同的顺序：`lookup` → `reserve` → 写入 4 KiB 完整响应。
每轮 1000 个请求、64 并发，单轮独立临时 journal；`response_bytes`、批次、队列和
并发都有硬上限，不随请求量增长内存。

### 本地低延迟文件系统（/tmp）

| 模式 | 请求/秒 | 端到端 P95 | reserve P95 | response P95 | 批大小峰值 | 每组提交均值 | 请求租约文件 | journal 体积 | 进程 RSS 增长 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 旧默认（单文件、逐请求 FULL、文件租约） | 978.6 | 127.9 ms | 25.6 ms | 46.5 ms | — | 每请求一次提交 | 1000 | 4.5 MiB | +9 MiB |
| 新单分片分组提交 | 2296.6 | 76.3 ms | 15.8 ms | 12.7 ms | 64 | 2.1 ms | 0 | 4.6 MiB | +8 MiB |
| 新 4 分片分组提交 | 1181.1 | 92.8 ms | 70.8 ms | 37.5 ms | 30 | 3.9 ms | 0 | 4.7 MiB | +12 MiB |

本地快盘上单分片最快。4 分片各写一个文件，在本机 64 并发下每个分片只有约 16 个
在途请求，批次更小、提交次数更多（132 组 vs 33 组），因此没有收益。交接评审
（2026-10-31）用同一脚本独立复测得到 legacy 968.6、单分片 2268.0、4 分片 1090.0
req/s，结论一致。

### 共享项目文件系统（/yzp，dpc）

| 模式 | 请求/秒 | 端到端 P95 | reserve P95 | response P95 | 批大小峰值 | 每组提交均值 | 请求租约文件 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 旧默认 | 31.3 | 2932 ms | 693 ms | 1591 ms | — | 每请求一次提交 | 1000 |
| 新单分片分组提交 | 423.6 | 333 ms | 104 ms | 113 ms | 64 | 28.1 ms | 0 |
| 新 4 分片分组提交 | 650.1 | 259 ms | 68 ms | 50 ms | 35 | 15.5 ms | 0 |

共享盘上逐次 FULL 提交约 28 ms，分组提交把 1000 次提交压到 37 组；4 个分片并行
fsync，再比单分片快约 1.5 倍，比旧默认快约 20.8 倍。这正是原方案要解决的“单
journal、逐请求 FULL、逐请求文件租约、共享慢盘”组合。

说明与边界：

- 两处测量都是同一台机器、同一请求流；共享盘结果受当前 dpc 客户端和并发负载影响，
  不是其它共享文件系统的保证。
- `journal_rss_growth_bytes` 在 /yzp 上为 0，但进程峰值 RSS约 3.8 GiB 来自工作区已有的
  784 MB `_demiflow/artifact_locations.json` 缓存（`resolve_local_artifact`），与
  journal 模式无关，也不是本表要比较的量。本地 /tmp 轮次的 RSS 增量才是 journal 自身。
- 队列/批次字节上限默认 64 MiB（单条约 8 MiB），批量压测中实际峰值 110–270 KiB，
  远低于上限；这不是“无上限内存换吞吐”。
- 所有新配置均为 `synchronous=FULL`，`journal_mode=DELETE`（`wal=True` 才显式 WAL）。

## 2. 真实模型 canary：1000 次请求与崩溃恢复

同一模型、同一批 1000 条文本、64 并发，journal 放在本地快盘以便测量模型与提交路径，
不走任何模拟后端：

```bash
# 干净跑：1000 个全新请求
../env/bin/python -B benchmarks/journal_canary.py \
  --url http://127.0.0.1:8090/v1 --model AutoTrust-JEV-27B-v1.1 \
  --requests 1000 --concurrency 64 --max-tokens 8 \
  --journal /tmp/canary-clean.sqlite --log /tmp/canary-clean.jsonl

# 崩溃恢复：提交 600 条后硬退出，再用同一 journal 重跑同一批 1000 条
../env/bin/python -B benchmarks/journal_canary.py ... --journal /tmp/canary.sqlite \
  --log /tmp/canary.jsonl --crash-after 600
../env/bin/python -B benchmarks/journal_canary.py ... --journal /tmp/canary.sqlite \
  --log /tmp/canary.jsonl
```

干净跑（2026-10-31，`AutoTrust-JEV-27B-v1.1`，本地 vLLM `127.0.0.1:8090`）：

```text
requests=1000  seconds=8.683  requests_per_second=115.2
row_p50=540.3 ms  row_p95=694.9 ms
http_ok=valid=committed=1000  reused=0  errors=0
journal={'requests':1000,'responses':1000,'uncertain':0}
batches=124  batch_rows_max=64  commit_mean=1.2 ms  queue_wait_max=78.8 ms
```

达到原目标 `>= 100 次/秒`，1000 次全部是真实 HTTP 调用，且都通过 2xx + 响应契约校验。
证据：`benchmarks/canary_runs/clean_20261031.{json,jsonl}`。

崩溃恢复（提交 600 条后 `os._exit(0)`，不执行 close，同一 journal 重开）：

```text
第一轮崩溃快照：已提交 600 个 id
第二轮：reused=614（未发 HTTP）、recovered=9（显式 requeue）、新提交 386
journal: requests=1000  responses=1000  request_count=1009
vLLM request_success_total: before=2047  after=3047 → 本次 1000 个请求恰好 1000 次 provider 调用
```

已提交响应绝不重调；崩溃时未提交的 9 个预留只有显式 `requeue_uncertain` 才重发。
这次 9 个首次请求都在 POST 之前被杀，所以 provider 计数仍是 1000。若某个预留已经
到达 provider 但响应未提交，显式恢复会再调用一次——这是 uncertain 语义，不是
“已提交响应被重复调用”。证据：
`benchmarks/canary_runs/{crash_20261031.jsonl,resume_20261031.json,crash_20261031_accounting.json}`。

### canary 成功判定（2026-10-31 review 修正）

早期脚本只要拿到 HTTP 响应就计成功，非 2xx 或结构错误的 200 也可能被当成模型答案。
现在只有 `2xx` 且 body 满足声明的 chat completion 契约（`choices[0].message.content`
是字符串）才计入 `valid/committed`；其他情况写入 `journal.failed` 并计入 `errors`，
脚本以非零码退出。用本地假服务验证（3 个请求：有效 200、无效 200、HTTP 400）：

```text
exit=1  committed=1  http_ok=1  valid=1  errors=2  journal={'requests':3,'responses':1,'uncertain':2}
```

`--summary` 和 `--log` 写入 `benchmarks/canary_runs/`，只保留 request id、HTTP 状态和计数，
不保留模型正文；`--crash-after` 只写一个崩溃快照，由显式 requeue 决定是否恢复未提交预留。

## 3. 2026-10-31 review 收尾

按交接文档第 9 节逐项修正，均只改平台代码/测试/文档，不动业务数据：

- **response 队列不再保留原 request**（9.3）：`_save` 只把 `(key, column, serialized)`
  放进队列，字节预算只计序列化文本；原请求（可能含大 data URI）不再被写线程持有。
  新增弱引用 + `queued_bytes_max` 测试（200 KB data URI 下排队字节仍 < 10 KB）。
- **分片 manifest 原子首建**（9.4）：先写完整临时文件（fsync），再用 `os.link` 排他
  落名；已存在的 manifest 必须与请求布局完全一致，否则在看到任何 shard DB 前报错。
  新增“不同 shard 数并发初始化”和“同一布局并发初始化”测试，确认只有一个布局生效、
  另一进程明确失败且不产生混合文件。
- **正式半写入用例**（9.5）：新增 fresh key（无 calls 行）+ 预置活跃 lease 的测试，
  验证 calls 行回滚、活跃 lease 保留、同批其他请求正常提交、future 各完成一次。
- **canary 成功判定与证据**（9.6）：见上一节。
- **文档边界**（9.7）：`docs/prompt_http.md` 注明 ordinal 只在单协调进程内保证严格
  新近序；多分片候选页的字节预算逐 shard、逐 chunk 执行后再对最终页限流。

## 4. 未验证项

- 没有在 `/yzp` 上做真实模型 canary：模型与 journal 都用本地快盘，测的是真实提交路径
  与真实模型；共享盘吞吐已由上表单独覆盖。
- 没有对现有约 400 MB 运行 journal 做迁移或多分片改造；多分片必须用新目录，legacy
  Lance 导入仅支持单分片。
- 单进程预算协调是在一个 Python 进程内成立的，不是跨进程分布式硬配额。
- 多分片 `requeue_uncertain`/`requeue_http_errors` 是逐分片事务，不是跨文件原子事务；
  失败后用同一 `operation_id` 重试补齐，见 `docs/prompt_http.md`。
