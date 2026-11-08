# 身份审核主任务与 SQLite Journal 改造：DeepSeek 完整交接

## 0. 执行要求

这份交接包含两个有先后关系的任务，不能只执行第二部分：

1. **身份审核主任务**：640,521 个概念的全量主处理已经结束，但还有 21 个技术失败。按同一 run、同一 journal 显式恢复并只补这 21 个请求，最终核对 `complete=true`；不得重跑已完成模型调用，不得自动发布概念主表。
2. **派生的 demiflow 平台任务**：完成 SQLite journal 分片、批量提交、数据库租约、恢复和性能验收。它是独立平台工作，不能借身份业务任务扩大修改范围。

继续完成实际工作，不要只做评审。开始先重新核对当前文件、进程和数据状态；本文中的计数是交接时实测快照，不能代替执行前复核。工作树可能已有其他执行者继续推进 journal 修改，必须以当前 `git status`、`git diff` 和测试为准，不能覆盖或回退他人的新修改。

目标仓库：

```text
/yzp/zhaozy/yangzepeng/0905/demiwtg   # 身份审核业务 pipeline
/yzp/zhaozy/yangzepeng/0905/demiflow  # 独立平台改造
```

开始前必须阅读并遵守：

```text
/yzp/zhaozy/yangzepeng/0905/AGENTS.md
/yzp/zhaozy/yangzepeng/0905/demiwtg/AGENTS.md
/yzp/zhaozy/yangzepeng/0905/demiwtg/PIPELINE_SPEC.md
/yzp/zhaozy/yangzepeng/0905/demiwtg/shared/concepts/identity/README.md
/yzp/zhaozy/yangzepeng/0905/demiflow/AGENTS.md
```

不要 `git reset`、`git checkout`、`git clean` 或覆盖现有脏改动。不要提交、推送、迁移现有 journal、停止其他任务、修改共享环境或启动新全量，除非用户另行明确要求。业务 pipeline 工作默认把 demiflow 当只读依赖；第二部分的独立平台工作只改已批准的平台文件。

任务来源会话链：

- 身份方案、开发、四模型测试：`session_90801547-1abf-445d-9a46-ec83ee6085ca`
- 正文修复、80/100 例复核、全量运行、journal 方案批准：`session_ea2ddaf7-756a-4136-9164-a0fcd10039d4`
- journal 最近实现与当时中断点：`session_ed350466-5f62-4731-b826-6461689473ae`

# 第一部分：身份审核主任务

## A. 用户确认的业务契约

唯一现役入口：

```text
demiwtg/shared/concepts/identity/concept_identity_pipeline.py
config(...) → run_pipeline(...)
```

唯一主链：

```text
shared_concepts@3 + base_documents@22
  → 筛选有 document_id 主文档声明的概念
  → 按 document_id 左关联固定文档
  → 按 zh、en 顺序读取 document_ref
  → 选择并清理一个导语 wikitext_source 正文块
  → has_lead_text 才调用 SystemOne noul
  → 覆盖写出本轮完整结果的新 Lance 版本和最新 summary
```

用户明确确认的规则：

- 只选一个能确认身份的导语正文块，保留清理后的完整自然语言文本；不抽句、不摘要、不翻译、不改写、不拼接其他块。
- 清除图片、文件、Infobox、消歧/导航/维护模板、引用和元数据；标题、URL、block_id 只用于追溯。
- `identity_description` 使用上述原文；模型只判断概念名称与正文是否同对象、同范围，不生成 description。
- 使用既有 `map_prompt_async` 和 `model.transport=systemone` 的 `noul` 概率，不新增另一套模型配置或业务直连 HTTP。
- 显式阈值 `identity_match_threshold=0.8`；达到阈值为 resolved，其余为 unresolved。低置信不等于明确 mismatch。
- 不保留 inventory、availability、adopt、revise、asset_patch、旧大 assessment 或 run 级多份业务表。
- 不增加公共语义字段，不直接改 `shared_concepts` 主表；后续采用和发布仍需单独显式授权。

输出契约：

```text
demiwtg/shared/concepts/identity/datasets/identity_assessments.lance
demiwtg/shared/concepts/identity/datasets/summary.lance
demiwtg/shared/concepts/identity/runs/<RUN>/review_calls.sqlite
```

结果每概念一行，只有 `concept_id`、`source_concept_revision`、`status`、`identity_description`、`review`、`assessment_call`。unresolved 的 description 为空；insufficient_evidence 是无导语文本等正常证据不足结果，不是技术失败。

## B. 已完成的质量验证与模型选择

四款决策模型已跑完同一批 100 例，共 400 条有效结果、调用失败 0：

- d3-turbo：相对构造标签 97%，中位延迟约 1.372 秒。
- Perplexity Decider：97%，约 0.796 秒。
- Amos 1 Large：96%，约 0.757 秒。
- AutoTrust-JEV：97%，约 0.220 秒。

展示证据：

```text
models/decision-deploy/results/identity_lead_eval_20261029_n100/identity_lead_model_review.ipynb
models/decision-deploy/results/identity_lead_eval_20261029_n100/identity_lead_model_review.executed.ipynb
```

注意：80 个自然配对最初被脚本默认标为正确，不能直接当人工事实标签或全量匹配率。随后已逐条复核这 80 个 case，并发现 MacBook Pro、Irene 的问题是导语前模板被误当正文，不是原始 Wikipedia 缺导语。

正文修复与复核已经完成：

- 修复 `Distinguish`、`For`、姓氏提示等模板参数泄漏，以及 `As of`、语言名称、IPA、Convert、NoteTA、coord、维护模板、PAGENAMEBASE、魔术字等清理问题。
- 原 80 例全部重跑并逐条复核。
- 固定种子 `20261029` 随机 100 个真实 case 全部运行并逐条审查；第一遍 14 个问题均回查原始 wikitext，修复后同批复跑，另修复了一个 `bd` 边界回归。
- 当时验证：文档提取回归 16 项、identity 完整测试 21 项、文档与采集测试 36 项通过。
- 证据：`tmp/identity_lead_template_20261029/random100/{selection.json,cases_after.json}`。

这些证据支持当前导语提取实现，但不等于全量绝不存在长尾模板问题，也不等于完成 DemiForge 认证。DemiForge 一直受既有 watched-source 256 MiB 上限阻塞，不得缩小监测范围规避。

全量模型最终选用 AutoTrust-JEV-27B-v1.1，原因是固定样本上质量与其他模型接近且实测延迟最低。用户随后明确授权用 GPU 0、1 跑全量；不要占用或干预其他任务使用的 GPU。

## C. 当前全量配置

配置文件：

```text
demiwtg/shared/concepts/identity/configs/autotrust_tp2_full_20261029.json
```

关键值：

```text
run = identity_full_autotrust_tp2_20261029
concept_source = shared_concepts.lance@3
document_source = base_documents.lance@22
model = AutoTrust-JEV-27B-v1.1
base_url = http://127.0.0.1:8104/v1
identity_match_threshold = 0.8
max_concepts = 640611
assess_max_calls = null
concurrency = queue_depth = model_limit = 64
timeout_s = 600
writer_batch_size = 32
writer_flush_s = 5.0
```

1000 次 canary 已完成并复用进同一 run。全量过程中发生并处理过：

- FlashInfer all-reduce 与 CUDA 12.4 不兼容，改用标准 NCCL 后服务正常。
- DataFusion 文档关联多次 resource admission timeout，后来用独立临时资源目录和单 native 槽位越过。
- 双/单槽位复用临时目录导致容量冲突，换独立目录解决。
- AutoTrust 服务因工具输出超过 16 MiB 被杀，后续 ConnectError 触发连续失败保护；改为日志落盘并恢复，已保存响应未重复提交。
- 中途实测约 24–25 请求/秒，模型平均请求延迟约 0.122 秒，但 vLLM 平均运行请求数只有约 1.5；证据指向逐请求 SQLite `DELETE + FULL` 和共享盘 journal I/O，由此派生第二部分平台任务。

## D. 交接时最新实测状态

全量 pipeline 主处理已结束，进程 `14706` 已退出；AutoTrust wrapper/vLLM 服务进程 `8086/8090` 仍存在。执行前重新 `ps` 和健康检查，不要仅凭本文判断服务状态，也不要擅自停止其他进程。

实测结果：

```text
identity_assessments.lance: version=43978, rows=640521
summary.lance:             version=12, complete=false, phase=assessed
input_count = 640521
assessed = 640422
failed = 21
insufficient_evidence = 78
resolved = 614768
unresolved = 25654
```

journal：

```text
路径 = demiwtg/shared/concepts/identity/runs/identity_full_autotrust_tp2_20261029/review_calls.sqlite
大小约 = 1.486 GB
calls = 640443
response_json = 640423
error_json = 20
pending = 0
recovery_events = 0
```

`.leases` 中有 640443 个旧式文件租约路径；这是旧实现的持久小文件开销，不能把这些文件数直接解释为仍有活跃 writer。恢复前必须以进程和实际锁状态为准，不能简单删除整个 lease 目录或 SQLite 文件。

21 个失败的准确分类：

- 18 个 `uncertain_call / UncertainPromptCall`：journal 中有预留或错误证据但没有完整响应。
- 2 个 `provider_error / ReadError`：传输读取错误，response 为空，需要显式 uncertain 恢复。
- 1 个 `provider_error / PromptResponseContractError / HTTP 502`：完整错误响应已保存，需要显式 HTTP 错误恢复。

当前不能宣称身份任务全部完成，因为 summary 明确为 `complete=false`。但 640,521 行完整范围已经交付，不需要也不允许重新调用全部 64 万请求。

## E. 身份审核剩余执行步骤

1. 重新读取 summary 和结果表，确认仍只有上述 21 个 failed；若状态已变化，以新状态为准，不重复执行旧恢复。
2. 确认全量 writer 已退出、AutoTrust 服务和模型请求正常，不使用新 run，不修改阈值、prompt、模型、输入版本或请求身份。
3. 从 `identity_assessments.lance` 仅取 `status='failed'` 的 21 行，解析 `assessment_call.request_id`，先核对分类和数量。
4. 对 18 个 UncertainPromptCall 加 2 个 ReadError，共 20 个 key，调用 `SQLitePromptJournal.requeue_uncertain`，提供非空 `actor`、`reason` 和固定 `operation_id`。
5. 对唯一 HTTP 502 key 调用 `requeue_http_errors`，传 `expected_statuses={502}`、非空 actor/reason 和固定 operation_id。
6. 不直接 DELETE calls，不删 journal，不修改 response_json，不绕过恢复接口。恢复接口会归档旧 attempt/错误证据并释放 key；预算不因恢复退返。
7. 使用原配置和原 CLI 恢复同一 pipeline。它会重扫本轮输入，但 640,422 个 assessed 请求应直接复用 journal，只有恢复后的 21 个请求允许产生新模型调用。
8. 若再次出现失败，分类并报告，不做无限自动重试，不把 failed 当 unresolved 或 insufficient_evidence。
9. 最终验收：
   - 结果仍为 640,521 个不同概念，每个概念一行。
   - `failed=0`、`assessed=640443`、`insufficient_evidence=78`。
   - summary 为 `complete=true`，并指向新提交的 assessments/summary 版本。
   - 21 个恢复 key 都有新响应和归档证据；其他已完成请求没有重复模型调用。
   - resolved/unresolved 按新结果重新统计，不能预先硬编码为旧值。
10. 主表发布不在本任务内。只有用户另行批准后，才能按 catalog 契约采用 resolved 的 description/review。

原 CLI：

```bash
cd /yzp/zhaozy/yangzepeng/0905/demiwtg
PYTHONPATH=.:../demiflow ../env/bin/python -B -u \
  -m shared.concepts.identity.concept_identity_pipeline \
  --config shared/concepts/identity/configs/autotrust_tp2_full_20261029.json
```

当前 `demiwtg/shared/concepts/identity/` 整个目录在 Git 中显示为 untracked。必须完整保留源码、配置、notebook、datasets、runs 和历史证据，不能因 untracked 就清理。

# 第二部分：SQLite Journal 平台改造

## 1. 用户原始目标与硬边界

用户批准的目标是：升级现有 `SQLitePromptJournal` 内部存储和提交机制，解决单 journal、逐请求 `FULL sync`、逐请求文件租约和共享慢盘造成的吞吐瓶颈，同时保留精确恢复、防重复调用和 checkpoint 业务恢复。

架构必须是：

> 可配置低延迟存储 + 原生分片 + 数据库租约 + 预留/响应组提交 + checkpoint 业务恢复。

硬性要求：

- 不新增第二套 journal API，不修改业务判断逻辑，不把队列、分片或线程模型泄漏给业务层。
- 默认 `throughput=None` 保持旧行为；只有显式传 `throughput` 才启用新机制。
- 预留未提交，不能发 HTTP。
- 响应未提交，不能发布业务行。
- 响应已提交但 checkpoint 未提交，重开必须复用响应，不能重调模型。
- 不使用 `synchronous=OFF`，不做“后台写完就算”的无等待异步写入。
- 本地低延迟文件系统可显式启用 WAL；共享文件系统不强上 WAL。`synchronous` 始终为 `FULL`。
- 每分片单写线程、单写连接、有界队列；同 key 固定进入同一分片。
- 新吞吐模式不再创建每请求 `.leases/<key>` 小文件。
- 保留请求、响应、错误、attempt、恢复证据和引用兼容性。
- 严格限制平台修改范围，不改 identity、其他 pipeline、现有运行数据或共享环境。

## 2. 当前实现状态

### 2.1 本任务已修改的文件

```text
demiflow/operator_llm/sqlite_journal.py
demiflow/operator_llm/call_ref.py
tests/test_sqlite_journal_throughput.py   # 新文件
```

本交接文档为新增文件：

```text
docs/sqlite_journal_throughput_handoff.md
```

### 2.2 已有但属于其他工作的脏改动

以下文件在本任务前就已有修改，必须保留，不要覆盖或混入无关重构：

```text
demiflow/execution/stream_lance.py
demiflow/operator_llm/systemone.py
docs/prompt_http.md
tests/test_prompt_systemone.py
```

`docs/prompt_http.md` 当前差异是 SystemOne 图片支持说明，不是 journal 吞吐说明。后续只能增量追加 journal 配置文档。

### 2.3 已实现的结构

`demiflow/operator_llm/sqlite_journal.py`：

- 原实现收为内部 `_SQLiteJournalShard`，公开 `SQLitePromptJournal` 负责路由、跨分片预算和聚合。
- `throughput=None`：旧单文件、逐请求文件租约路径。
- `throughput={}`：启用单分片批量提交和数据库租约。
- 分片数 `shards` 支持 1–8；多分片布局为：

```text
calls.sqlite.d/
  manifest.json
  000.sqlite
  001.sqlite
  ...
```

- 分片规则：`int(request_key[:16], 16) % shard_count`。
- 单分片配置默认值：
  - `batch_size=64`
  - `flush_interval_s=0.01`
  - `queue_size=256`
  - `wal=False`
- 批量队列覆盖 reserve 和 response/failed；调用方等待持久化 future 后才继续。
- `close()` 先排空队列再关闭连接。
- 批量循环已加入每操作 `SAVEPOINT batch_operation`，单项失败应回滚自己的部分写入。
- 新吞吐模式使用 `writer_leases`，不创建逐请求 lease 文件。
- 跨分片 `max_requests` 采用进程内预占，首次从各 shard 的 `journal_state.request_count` 恢复。
- `references()` 返回真实 shard 路径和真实 `PromptRecordRef`。
- 单分片保留外部现有行为：可修改 `journal.limit`，离线物化使用的 `_lock/_connect` 仍可用，分页仍直接走原实现。
- 多分片裸 `_connect()` 明确报错，不能悄悄固定写 shard 0。
- legacy Lance import 目前只允许单分片。

`demiflow/operator_llm/call_ref.py`：

- `_journal_paths()` 识别普通 SQLite 文件或 `.sqlite.d/manifest.json`。
- `journal_options()` 新增可选 `throughput` 透传。
- `call_snapshot()` 聚合各 shard 的 request/response/error。
- `journal_totals()` 聚合请求、响应、错误和 token 计数。
- `journal_observation()` 合并原始计时行后计算全局 mean/P50/P95，不是对各 shard P95 做错误平均。

`tests/test_sqlite_journal_throughput.py` 当前有 7 个测试，覆盖：

- 批量 reserve/response、WAL/FULL、数据库租约、无逐请求 lease 文件。
- 已保存响应去重。
- 并发预算。
- 活跃 writer 阻塞 requeue，owner close 后显式恢复。
- 默认模式仍使用文件租约。
- 三分片预算、真实引用回读、聚合读取、重开和只读回放。
- 空 `throughput` 启用批处理及 shards 参数校验。

## 3. 已验证结果与验证边界

最近会话曾得到：

- 新聚焦测试：`7 passed`。
- 相关旧回归：`53 passed`。
- 本地 HTTP 相关回归：补回环代理排除后 `16 passed`。
- `test_prompt_systemone.py`：`5 passed`。

本会话在最后中断修改之后只重新跑了：

```text
tests/test_sqlite_journal_throughput.py: 7 passed
```

因此，之前的 53/16 项结果不能代替当前最终回归。最后一次租约修改后还没有跑完整相关测试，也没有跑性能测试或真实模型 canary。

本地 HTTP 测试的环境问题已定位：进程有小写 `http_proxy/https_proxy`，但没有 `no_proxy`，导致 `127.0.0.1` 测试请求走代理并返回 503。测试命令需要仅为本次进程设置：

```bash
env no_proxy=127.0.0.1,localhost NO_PROXY=127.0.0.1,localhost ...
```

不要修改全局代理配置，也不要把 503 当作业务代码失败。

## 4. 上个会话中断时的问题清单（历史快照，先核对当前代码）

以下是 `session_ed350466...` 中断时的精确状态，用于防止遗漏，不代表当前工作树仍未修复。交接后已经观察到 `sqlite_journal.py` 被其他执行者继续推进，包括租约 owner 写入、字节预算、指标和恢复预检等变化。DeepSeek 必须先逐项对照当前 diff 和测试：已解决的验证后关闭，未解决的继续实现；不要按旧快照重复改造或覆盖新代码。

### 4.1 崩溃后的数据库租约清理尚未接通

最后正在给 `writer_leases` 增加：

- `owner_pid`
- `owner_start`

并已添加：

- `_process_start()`：读取 `/proc/<pid>/stat` 的启动标识。
- `_process_alive()`：活跃进程保守阻塞；进程消失或 PID 启动标识变化时判定旧租约可清理由显式恢复接口处理。
- `_insert_lease()`：唯一约束冲突时区分活跃与失效 owner。

但所有插入仍只写 `(request_key, owner_token)`，至少包括当前文件中 `_commit_batch`、`reserve_http_retry`、`requeue_uncertain`、`requeue_http_errors` 的四处调用。结果是 `owner_pid/owner_start` 为 NULL，失效 owner 仍会被当成活跃 owner，崩溃恢复目标没有实现。

下一步必须：

1. 让所有数据库租约插入同时保存 `lease.pid` 和 `lease.start`。
2. 保持普通 reserve 对 unanswered row 仍返回 `UncertainPromptCall`，不能因进程死亡而自动重发。
3. 只有显式 `requeue_uncertain`/相应恢复接口可以清理失效租约。
4. 增加“模拟死进程遗留租约”的测试，以及“同 key 活跃进程仍互斥”的测试。
5. 检查旧 `writer_leases` 表的 additive migration 和关闭清理。

### 4.2 批量事务的外层失败路径需要修正和故障测试

每操作 SAVEPOINT 已加入，7 个聚焦测试通过，但还没有覆盖：

- 同批一个 reserve 因活跃租约失败，不能遗留它刚插入的 calls row，也不能影响其他成功项。
- 整批 `BEGIN`/commit/连接失败时，所有 future 必须恰好完成一次。
- 外层失败不能出现先 `set_exception()`、随后又对同一 future `set_result()` 的 `InvalidStateError`。
- 外层事务回滚后，内存 `_leases` 必须与数据库租约状态一致；不能留下假 owner 或丢失关闭时需要清理的 token。
- `references()` 或提交后的完成逻辑异常不能让写线程退出并留下永远等待的 future。
- close、取消、队列满和写线程异常都要能排空或明确失败。

不要只测正常路径。应做可控故障注入，并验证请求数、响应数、租约和 future 终态。

### 4.3 字节预算和指标尚未实现

当前队列和批次只有条数上限，没有原方案要求的批次/队列字节上限。必须结合 `AGENTS.md` 的有限资源要求：

- 对排队的序列化载荷做有界字节核算和背压。
- 明确单条超限行为，不能无限放行或静默截断。
- 计入序列化副本、调用方等待载荷和当前批次峰值。
- 不用无限内存换吞吐。

原方案要求的分阶段指标也尚未落地：reserve 排队/提交、response 排队/提交、批大小、组提交耗时、端到端吞吐。至少要有可核对的有界压测记录，不能只报一次磁盘探针。

### 4.4 多分片契约仍需审计

重点核对：

- 跨 shard requeue 的幂等和部分失败语义。当前按 shard 分组执行，不提供跨 shard 全局原子事务；不得宣称全局原子。若旧公开契约要求跨 key 原子恢复，必须先解决，不能悄悄弱化。
- 多分片离线物化目前会因裸 `_connect()` 拒绝而不支持。不要路由到 shard 0 假装支持；明确测试并记录支持边界，或在既有架构内正确实现按键路由。
- `call_snapshot()` 是逐 shard 读取聚合，不是跨 shard 单点全局事务快照；文档不能过度承诺。
- 跨分片预算是单进程内协调，不是多进程全局硬配额；不得宣称分布式严格预算。
- `call_page` 多分片排序、分页边界、`registered_keys(max_keys=0)` 和异常参数需补边界测试。
- 单分片默认路径必须继续保持旧分页顺序、动态 `limit`、Lance 导入和离线物化兼容。

## 5. 建议执行顺序

1. 用 `git status --short`、`git diff` 阅读当前修改，保留全部已有脏改动。
2. 完成四处租约 owner 信息写入，补活跃/死进程、显式恢复和旧表迁移测试。
3. 修正批量事务外层失败、future 完成和内存租约回滚，补同批部分失败与整批失败测试。
4. 实现有界字节背压和必要的提交指标，避免改业务 API。
5. 审计并补齐多分片恢复、同 key 竞争、64 并发、只读和聚合边界。
6. 跑聚焦测试、全部相关旧回归和 `git diff --check`。
7. 做有界 A/B 存储压测：旧默认、新单分片、新 4 分片，使用相同请求和相同 64 并发，比较端到端请求/秒、P95、批大小/提交耗时和资源峰值。
8. 在独立测试数据上做响应已提交后 kill/reopen，证明恢复后零重复模型调用。
9. 真实 canary 使用同一模型、同一批文本、1000 次请求。原目标为 `>=100 次/秒` 且恢复后零重复调用；若环境或服务不可用，明确报告未验证，不能用模拟压测冒充。
10. 增量更新 `docs/prompt_http.md` 的 `throughput` 配置、WAL 适用边界、分片布局、恢复语义和已知限制。
11. 最后核对最终差异，只保留本任务和原有脏改动，不提交，不启动全量迁移。

## 6. 必须通过的 correctness 清单

- [ ] 默认 `throughput=None` 旧行为及相关旧测试不变。
- [ ] 预留未提交不发 HTTP。
- [ ] 响应未提交不发布业务行。
- [ ] 响应提交后 kill/reopen 直接复用，零重复调用。
- [ ] checkpoint 失败后可复用 journal 响应重新交付。
- [ ] 同 key 并发只有一次有效预留；多进程活跃 writer 严格互斥。
- [ ] unanswered 请求崩溃后保持 uncertain，只有显式恢复可重试。
- [ ] 原 attempt、响应和错误证据不被覆盖。
- [ ] 跨分片 `max_requests` 在声明的单进程边界内不能突破。
- [ ] 队列满有背压，批/队列字节有上限，单条超限明确失败。
- [ ] 单批部分失败不污染其他请求；整批失败所有 future 有明确终态。
- [ ] close 正常排空，异常和取消不留下永久等待。
- [ ] 新吞吐模式无逐请求 lease 文件，正常完成后数据库租约清空。
- [ ] 引用指向真实 shard，`PromptRecordRef` 可回读。
- [ ] 只读回放、监控、token、mean/P50/P95、快照和分页聚合正确。
- [ ] WAL 只在显式配置下启用，`synchronous=FULL`，无 OFF。
- [ ] 模拟 A/B、故障恢复和真实 1000 次 canary 分别记录结果，不能相互替代。

## 7. 验证命令

在仓库根目录执行：

```bash
cd /yzp/zhaozy/yangzepeng/0905/demiflow

../env/bin/python -B -m py_compile \
  demiflow/operator_llm/sqlite_journal.py \
  demiflow/operator_llm/call_ref.py

timeout 120 ../env/bin/python -B -m pytest \
  tests/test_sqlite_journal_throughput.py \
  -q -p no:cacheprovider

timeout 180 ../env/bin/python -B -m pytest \
  tests/test_platform_upgrade.py \
  tests/test_prompt_http_retry.py \
  tests/test_prompt_journal_inspection.py \
  tests/test_http_error_recovery.py \
  tests/test_prompt_systemone.py \
  -q -p no:cacheprovider

timeout 180 env no_proxy=127.0.0.1,localhost NO_PROXY=127.0.0.1,localhost \
  ../env/bin/python -B -m pytest \
  tests/test_sqlite_journal.py \
  tests/test_readonly_prompt_replay.py \
  -q -p no:cacheprovider

git diff --check
git status --short
```

如修改影响其他 prompt/journal 调用路径，再运行项目完整标准测试；不能自行缩小验收范围后宣称全部完成。

## 8. 配置示例与文档口径

本地低延迟文件系统、4 分片的显式新配置示例：

```python
options = {
    'path': '/local/fast/path/calls.sqlite',
    'throughput': {
        'shards': 4,
        'batch_size': 64,
        'flush_interval_s': 0.01,
        'queue_size': 256,
        'wal': True,
    },
}
```

共享文件系统不要默认建议 `wal=True`。旧单文件 journal 应可直接按原路径复用；多分片 legacy import 当前不支持，不能擅自迁移现有约 400MB 运行 journal。

最终汇报必须分别列出：实现完成项、测试实际通过数、性能实测、真实 canary 结果、未验证项和已知限制。不得只写“性能已极致优化”或以 7 个聚焦测试代替完整交付。

## 9. 平台最终实现 Review 与完善意见

### 9.1 总体结论

最终实现相较中断快照已经补齐数据库租约 owner、批量事务故障处理、队列字节预算、指标、分片恢复预检、崩溃测试、A/B 和真实 provider canary。整体架构符合原方案，核心持久化和恢复语义成立，测试与本地存储性能结果良好。

但当前不建议直接标记“平台任务全部完成”。仍有一个可复现的队列内存计量漏洞、一个分片 manifest 并发初始化风险，以及 canary 和故障测试证据缺口。先完成下列高优先级项，再做最终关闭。

### 9.2 本次实际核验

代码与文档：

- `py_compile` 通过，`git diff --check` 通过。
- 修改范围仍为 `sqlite_journal.py`、`call_ref.py`、对应测试/文档/基准；既有 SystemOne 和 `stream_lance.py` 脏改动未被本评审修改。
- 多分片离线不支持、跨分片恢复非全局原子、预算仅进程内协调等限制已写入文档，不再属于隐瞒行为。

测试：

- `tests/test_sqlite_journal_throughput.py`：**24 passed**。
- journal/prompt/SystemOne 相关旧回归：**67 passed**。
- artifact、embeddings、Codex、image、gateway 等扩展消费者回归：**231 passed**。
- 合计本评审实际运行 **322 项通过**；这不是整个仓库全量测试，最终汇报不得写成“全仓测试全部通过”。
- 额外做了“fresh calls row 已插入后发生活跃租约冲突”的最小复现：future 为 `UncertainPromptCall`，calls 回滚为 0，原活跃 lease 保留 1 行。说明当前 SAVEPOINT 实现本身能处理该场景，但正式测试尚未覆盖，见 9.5。

本地 `/tmp` 存储 A/B 复测，1000 请求、64 并发、4 KiB 响应：

- legacy：**968.6 req/s**，端到端 P95 130.164 ms。
- 新单分片：**2268.0 req/s**，端到端 P95 80.017 ms，33 组提交。
- 新 4 分片：**1090.0 req/s**，端到端 P95 113.849 ms，134 组提交。

这与报告中的本地快盘结论一致：单分片收益明显，4 分片在本地快盘上反而增加调度和提交开销。共享盘报告和真实 provider canary 未由本评审重跑。

### 9.3 高优先级：response 队列仍保留未计量的大 request

位置：

- `demiflow/operator_llm/sqlite_journal.py:460`
- `demiflow/operator_llm/sqlite_journal.py:660-665`

吞吐模式 `_save()` 将 `(key, column, payload_text, request)` 放进队列，但字节预算只计算 `payload_text + key`。`_apply_operation()` 解包出的 `request` 实际没有使用。若原请求带大 base64 图片，队列会额外保留完整原请求，且完全不受 `max_queue_bytes` 约束。

本次最小复现：

```text
accounted_bytes = 84
original_request_chars = 1000022
original_request_retained = True
```

这违反了原方案的内存硬边界，不是单纯“Python 对象开销无法精确估计”。完善要求：

1. 从 save 的 queued payload 删除未使用的 `request`，同步简化 `_apply_operation()` 解包。
2. 保留响应文本的精确 UTF-8 字节计量。
3. 增加大 data URI 测试，证明响应排队不会额外保留原 request，也不能绕过 `max_record_bytes/max_queue_bytes`。
4. 修复后重跑图片、离线、并发和字节预算回归。

### 9.4 高优先级：manifest 创建需要防并发布局竞争

位置：`demiflow/operator_llm/sqlite_journal.py:1051-1075`。

当前两个进程同时看到 manifest 不存在时，都会写临时文件并 `replace(manifest)`。若它们配置了不同 `shards`，后写者覆盖先写者，但两个对象都可能继续按各自布局写数据库，已有 manifest 校验无法阻止这次首建竞争。

完善要求：

- 首建使用原子排他创建（例如 `O_EXCL`）或明确的初始化锁；创建后重新读取并校验完整 manifest。
- 布局不一致的进程必须在任何 shard DB 写入前失败。
- 增加两个进程不同 shard 数同时初始化的测试，确认只有一个布局生效，另一个明确报错，不产生混合文件。

### 9.5 测试缺口：正式部分失败用例没有覆盖真正的半写入

位置：`tests/test_sqlite_journal_throughput.py:292-315`。

当前 `test_partial_batch_failure_keeps_other_rows_and_no_leak` 的失败 key 已经有 unanswered calls row，因此在 `_apply_operation()` 的早期 row 检查就失败，没有覆盖“新 calls row 插入后，`_insert_lease()` 才发现活跃冲突”的半写入路径。

虽然本评审的最小复现证明实现能正确回滚，仍应把该复现固化为正式测试：

- fresh key 无 calls row，但预置另一活跃 owner 的 writer lease。
- 同批加入其他正常请求。
- 验证失败项 calls 不残留、活跃 lease 不被删除、其他项正常提交、future 各自只完成一次。

### 9.6 canary 需要防止假成功，并补齐生产链口径

位置：`benchmarks/journal_canary.py:84-113`。

脚本对非 429/5xx 的响应直接保存并增加 `called`，没有要求 2xx，也没有验证 provider 响应结构。因此 HTTP 400 或结构错误的 200 也可能被计为成功且 `errors=0`。完善要求：

- 只有预期 2xx 且响应 body 符合声明契约时才计成功；其他情况保存错误并计失败。
- canary 报告区分 HTTP 成功、业务有效响应、journal committed 和 reused。
- 当前 canary 使用原始 `httpx /chat/completions`，能证明真实 provider + journal 序列，但不等于 `map_prompt_async + SystemOne + checkpoint` 的生产端到端验收。若要关闭原方案的全部验收项，还需用实际客户端和 SystemOne 跑有界 canary，并记录 vLLM running/waiting、HTTP、提交和 checkpoint 阶段指标。
- `/tmp/canary-clean.jsonl` 与 `/tmp/canary.jsonl` 在本次复核时已不存在，因此报告中的 123.2 req/s 和 600+400 恢复计数只能核对脚本与文字报告，不能独立重放原始日志。后续把有界 JSONL summary 保留到 `benchmarks/` 或明确运行回执目录，不保留大响应正文。

### 9.7 低优先级文档边界

- `ordinal` 由进程内计数器恢复和递增，多进程同时写入可能产生相同 ordinal；`call_page` 的“全局 recency”严格说只在当前协调边界内成立。可在文档注明，或另建持久全局顺序后再作更强承诺。
- 多分片候选页的字节预算是逐 shard、逐 chunk 执行，最终页会再次限流；文档不要把中间候选内存也说成仅一个 `max_page_bytes`。
- 多分片 legacy import、离线物化、跨文件原子恢复和分布式硬预算继续明确列为不支持/非目标，不为关闭任务而扩大实现范围。

### 9.8 建议关闭顺序

1. 删除 save 队列中的原 request 并补大图片字节测试。
2. 修复 manifest 原子首建并补不同布局并发测试。
3. 固化真正的半写入 SAVEPOINT 测试。
4. 修正 canary 成功判定，保存可复核日志；按原验收口径决定是否补 SystemOne/客户端/checkpoint canary。
5. 重跑本评审的 322 项相关测试及项目标准测试，再更新最终性能与限制说明。

完成上述高优先级项之前，可以评价为“核心实现和性能方案已验证，剩余少量正确性/证据收尾”，不要写成无任何未决项的最终完成。
