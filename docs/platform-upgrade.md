# 平台 0.2：存储、恢复与队列

本次按 `DEMIFLOW_PLATFORM_TODO.md` 合并实施日志和业务状态迁移。模型身份、prompt、业务通过标准不随存储迁移改变。业务仍通过 Dataset 读取固定版本、写入显式 Arrow schema。

当前工作区使用 `env/bin/python`，以 editable 方式安装 0.2.0。由于父目录中仓库目录也叫 `demiflow`，开发安装使用 `--config-settings editable_mode=compat`，使从工作区根目录运行时也能加载实际包的 `__init__.py`。本轮安装带 `--no-deps --no-build-isolation --no-index`，没有更新其他依赖或访问包索引。

## 存储边界与升级

| 内容 | 载体 | 入口 |
| --- | --- | --- |
| HTTP / Codex 调用、离线请求与响应、预算、恢复审计 | SQLite 专用 calls / journal_state / recovery_events | `SQLitePromptJournal`、`sqlite_offline` |
| 离线输入图片 | 内容寻址普通文件，SQLite 保存路径和 SHA256 | `externalize` / `restore` |
| 平台 manifest、阶段提交、完成状态、空运行重置 | SQLite 专用 run journal | `RunJournal` |
| 下线维护、文件交付、业务输入绑定、追加意图 | 普通不可变 JSON 文件 | `artifacts`、`JsonArtifactRef`、`DatasetCommit` |
| 业务 summary、阶段引用、发布结果、append 去重回执 | 业务自行声明 schema 的 Lance 表 | Dataset API |
| 原图、审核、题目、训练样本、评分 | 业务 Lance 表与固定 DatasetRef / BlobRef | 原 Dataset API |

`demiflow.lance.records` 和 `operator_llm.lance_journal` 已移除，没有通用 key/payload 写入兼容类。历史数据不删除；`lance.legacy` 仅允许读取，历史模型引用使用 `read_call(ref, root)`。

新配置：

```python
options = {'sqlite_journal': {'path': '/absolute/path/calls.sqlite'}}  # HTTP 或 Codex
options = {'offline_store': {'path': '/absolute/path/offline.sqlite'}}
```

旧调用表首次续跑：

```python
from demiflow.operator_llm.call_ref import journal_options
options = {'sqlite_journal': journal_options('/data', 'pipeline/calls__run.lance')}
```

这里 `.lance` 是旧表定位符，新日志写同名 `.sqlite`。存在旧表时先按固定快照事务导入请求键和完整响应；不中途发出新模型请求。它不复制旧请求里的 base64，也不删除旧表。已完成响应命中仍可复用；已有占位保持不确定；历史请求计入累计预算。业务已统一通过该入口转换既有调用定位符。

新调用引用为 `{journal_path, request_id, kind}`。响应不可变；`PromptRecordRef.read()` 和 `call_snapshot()` 以 SQLite 只读模式访问。HTTP 请求仅含紧凑元数据；离线请求读取可校验并重建图片上下文。已有绝对路径的原生日志引用依赖其存储位置；搬迁 SQLite 时也需保留/迁移对应 `.inputs`，不能仅复制数据库后假定图片随之移动。

离线回填：

```python
from demiflow.operator_llm.sqlite_offline import submit_response
submit_response(None, request_ref, {'result': result}, model=requested_model,
                metadata={'reviewer': 'operator', 'reviewer_kind': 'human'})
```

`request_ref` 来自本轮 offline 输出。接口验证请求哈希和模型；结果仍经过正常的响应解析和业务校验。等待、HTTP 技术失败、结构错误不会变成业务通过。

## 不确定请求恢复

默认绝不自动重发占位。每个在途请求持有 OS 文件锁，恢复会排斥仍存活的写入者；进程退出后锁由 OS 释放。旧 journal 实例丢失归属后不能提交迟到响应。

```bash
python -m demiflow.operator_llm.recover inspect --path /data/calls.sqlite
python -m demiflow.operator_llm.recover requeue --path /data/calls.sqlite \
  --key REQUEST_SHA256 --actor OPERATOR --reason '旧写入进程已退出，核查后允许重试' \
  --operation-id recovery_001
python -m demiflow.operator_llm.recover history --path /data/calls.sqlite
```

恢复只处理明确指定且无完整响应的 key；混有完成/缺失 key 时整批回滚。保存操作者、原因、时间、原请求、原错误和尝试次数，操作 ID 可幂等复用。恢复不会发起模型调用，也不会退还已消耗的持久预算；正常入口的下一次运行才能发送新请求。

旧 Lance 导入前必须先结束旧版本写入者：旧版本没有新 journal 的 OS 请求锁，无法由新恢复 API 证明它已退出。不要对仍在运行的旧写入者执行迁移/恢复。已启动旧进程保持原有状态；新启动入口使用新实现。

## 追加提交与空运行重置

业务 append 指纹仍由业务定义，并由显式提交表保存固定版本。`DatasetCommit` 是围绕标准 Dataset writer 的控制协议：

```python
from demiflow.execution.dataset_commit import DatasetCommit
with DatasetCommit(run_control / 'append_intents', batch_fingerprint, target_uri) as commit:
    if commit.output is None:
        receipt = rows.write_lance(target_uri, mode='append', schema=SCHEMA, return_receipt=True)
        commit.confirm(receipt)
    output = commit.output  # uri + 确认的版本，不再重新读取表头猜测
# 随后写业务提交表和 summary
```

已确认 writer 结果可跨业务回执写入失败复用。只有 intent、没有确认回执时抛 `UncertainDatasetCommit`，不重放 append。明确确认旧写入者退出后，可调用相同对象的 `abort_uncommitted(actor=..., reason=...)`；它仅在目标仍是写入前版本时允许归档该 intent 并开放下一次尝试。目标版本有变化、已有确认回执、无法证明未提交时拒绝恢复，要求核对实际数据。该意图协议目前限定本地文件系统 Lance 目标；未扩展远端追加的自动恢复。

`RunJournal.reset_empty(actor=..., reason=...)` 仅重置无 stage、activity、completion 和未知文件的 run，并保留旧 manifest 审计。业务材料运行使用同一业务运行锁与更完整的文件检查：

```python
from preparation.articles.operators.run_reset import reset_empty_run
receipt = reset_empty_run(run_path, actor='operator', reason='空 run 的源码已定稿')
```

业务接口会拒绝已有模型调用、请求绑定、阶段表、公共发布、其他未解释产物或活动写入者的运行。仅把允许的空脚手架归档到 `_demiflow/run_resets/...`，不清理有实际工作的 run；之后显式用新配置启动。

## 长流程内存和受限环境

```python
cached = flow.materialize()
try:
    result = cached.count()
finally:
    cached.release()

from demiflow.execution.isolation import release_unused_memory, run_isolated
release_unused_memory()
receipt = run_isolated(terminal_stage, fixed_source_ref, timeout_s=600)
```

`release()` 当前支持 Local：删除本物化实例的缓存与 spill，其他缓存不受影响；共享该实例的衍生计划随后执行会明确报错。Ray 沿用原有生命周期，显式 release 尚未提供。`release_unused_memory` 尝试 GC、Arrow 释放、Linux malloc_trim，无法回收仍被引用的对象，也不是 RSS 硬上限。图片审核入口在最终公共 merge 前调用该释放提示。

需要可靠清空一个重阶段的 Python/Arrow 原生分配时，使用 `run_isolated`，传固定路径/版本，返回小回执。子进程退出释放其内存；超时/异常不自动重放写入。该边界不会继承父进程的 monkeypatch/未持久配置，也不代替 cgroup 内存规划。

Local 的公开 Dataset 动作通过统一执行入口。并行度由运行策略、调度器和实际取得的资源决定。

宽行物化、按字节 spill、宽载荷偏移归并、Lance scanner 预读开关，以及部分列 merge/显式增列、Codex 文件交付保持已有语义。此次回归包含这些功能，但没有把既有实现重复记为新功能；真实 GPU、Codex 图像工具、生产峰值 RSS 仍需对应环境验收。

## 原生队列与下载迁移

当前获取与队列接口见 [Dataset.fetch 与原生任务队列](dataset-fetch.md)。
单机使用本地 SQLite v2，多 worker 控制后端使用 PostgreSQL；目标仍为声明的
POSIX host/shared 对象域，已 prepared 的任务按真实域限制恢复。
原 SQLiteQueue/SQLiteChannel/COSQueue、queue_runner 和组合获取 API 已移除。
COSIO 对象存取、解析/读取与历史库能力保留。

旧业务 collect/download 入口及其冻结依赖没有在此次平台任务中自动迁移，不能把
原文档中的旧命令当作新版本可运行入口。新建 v2 库，从固定清单显式生产并验收；
旧库仅从已关闭的一致 backup 按 SHA256 只读查询，不原地转换或伪造认证。
未迁移的在运作业继续固定原 revision，不切换代码、队列或服务。

`NetworkConfig.from_env` 等独立网络声明保留其原有限读取和无 shell 执行边界，
不等于新原生队列支持了旧 supervisor 或自动远程启动。

## 任意 HTTP 模型服务（DF-008）

`ManagedHTTPService` 补齐非 vLLM 服务的声明式启动、回环端口探活、GPU/端口协作锁和进程组清理。配置只描述命令，不加载模型。GPU 选择使用 `gpus=[...]`，显存/offload 策略由实际服务的命令参数和显式环境变量表达，不推测 diffusers 或其他后端的参数。

```python
from demiflow.services import ManagedHTTPService
service = ManagedHTTPService(
    ['/path/to/python', '/path/to/server.py', '--port', '8003', '--sequential-offload'],
    base_url='http://127.0.0.1:8003/v1', root='/shared/local/workspace',
    gpus=[0], ready_path='/health', expected_model='my-model',
    startup_timeout_s=600,
)
# OpenAI 兼容 chat prompt 节点：map_prompt_async(..., service=service)。
# 自定义图片/其他 HTTP 节点：在其 actor 生命周期使用下面的显式作用域。
async with service.bind() as owner:
    await owner.ensure_ready()
    # 在此执行自定义 HTTP 节点；作用域结束后关闭本服务。
```

prompt 缓存命中不启动服务；声明端点必须与 prompt 的实际端点一致。offline/Codex 不会自动启动 HTTP 服务，已有参数约束保留；需要多个独立节点共用同一服务时，用上述显式作用域或独立受管服务。

跨 run 存活使用独立监督 CLI，无须业务手写 nohup/PID 清理：

```bash
python -m demiflow.services.manage start --root /workspace --name image-model --config /path/service.json
python -m demiflow.services.manage status --root /workspace --name image-model
python -m demiflow.services.manage stop --root /workspace --name image-model
```

JSON 字段与 `ManagedHTTPService` 构造参数相同（root 由 CLI 统一传入）。start 等待 readiness 成功后返回，监督进程继续持有资源锁；客户端 pipeline 仍使用外部 endpoint。status 同时检查监督进程身份与 HTTP 健康；stop 校验主机、PID 和进程启动标识，只请求对应监督器清理其自身子进程，不接管或终止占用端口的未知进程。启动失败、等待超时、取消、子进程退出均记录状态并释放资源。没有后台自动重启已崩溃的模型；明确的失败让调用方决定是否重启，避免自动重放副作用。

zimage_probe 提供 `demiwtg/docs/model_services/z_image.example.json` 配置样板，保留“不随 pipeline 自动启动模型”的业务约定。本次仅测试本地 HTTP 模拟服务，未启动真实 Z-Image / vLLM / GPU 模型。
