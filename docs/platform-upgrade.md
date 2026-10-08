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

Local 可设 `LocalDatasetExecutor(sort_workers=1)` 或 `DEMIFLOW_LOCAL_SORT_WORKERS=1`。兼容 TODO 原拼写 `DEMIWFLOW_LOCAL_SORT_WORKERS`，标准拼写优先。外排进程池出现 `BrokenProcessPool` 后仅重做已编码块的纯排序/归并；不重新遍历上游、不重放模型/用户回调，后续任务转单进程。其他数据异常正常传播。

宽行物化、按字节 spill、宽载荷偏移归并、Lance scanner 预读开关，以及部分列 merge/显式增列、Codex 文件交付保持已有语义。此次回归包含这些功能，但没有把既有实现重复记为新功能；真实 GPU、Codex 图像工具、生产峰值 RSS 仍需对应环境验收。

## 单机队列与多机队列

| 场景 | 选择 |
| --- | --- |
| 单机、百万以内有限任务、逐任务预算/退避 | `SQLiteQueue` |
| 多机共享同一任务集，节点动态加入退出 | `COSQueue` + `queue_runner` |
| 多机但可以按桶/来源硬分区 | 每机各自 `SQLiteQueue` |
| 千万级或持续无限输入 | COS 分批/分片队列，或专门流式系统；不把无限输入塞进一个本地库 |

**SQLite WAL 队列必须放本地磁盘。** 构造时检查 Linux mountinfo，拒绝 NFS/CIFS/Ceph/Lustre/FUSE 等网络文件系统。容器中还需运维确认挂载底层没有伪装网络存储；mountinfo 无法识别 overlay 背后的任意存储实现。网络盘只存 `queue.backup(snapshot_path)` 生成的一致快照，不能把正在运行的 db/WAL 直接复制过去当共享队列。默认护航每 20 分钟执行 SQLite backup API，完成后原子替换快照。

最小入口：

```python
from demiflow.collect.sqlite_queue import SQLiteQueue
from demiflow.collect.embedded_worker import run_worker, NamedPoolSupervisor, PoolPolicy
queue = SQLiteQueue('/tmp/my-collection/tasks.sqlite')
queue.add({'task_id': str(i), 'payload': {'i': i}, 'pool': 'source_a'} for i in range(100))
run_worker(queue, consume, verify_artifact, pool='source_a', batch_size=8, exit_when_idle=True)
# 持续监督/分池爬山：每个 pool 使用独立的 PoolPolicy 对象。
watch = NamedPoolSupervisor(queue, consume, verify_artifact,
    {'source_a': PoolPolicy(maximum=40)}, snapshot_path='/persistent/task-snapshot.sqlite')
# 定期 watch.tick({'source_a': measured_rate})；退出 watch.close()
```

`consume(handle)` 返回产物描述，`verify_artifact(result)` 必须核对实际内容后才记 done。`existing(handle)` 可提供内容寻址实存快道；必须同样经过验证。worker 每次事务批量认领，默认 8 个；失败按退避再次 pending，达到 max_attempts 进入 failed 死信。完成状态与 completion 账本同事务写入；不确定提交保留 claim，不能当作未处理任务自动放回。

预算预留和已完成消耗在事务中计算。回退到其他来源类之前调用 `transfer_budget(handle, class, cost)`；超帽不发出请求。`actual_cost` 可按验证后的实际结果结算。成本单位和失败尝试费用由业务定义；下载样板保持成功字节口径，不把它当精确账单计费器。

worker 的 PID、进程启动标识、主机、心跳持久存在注册表；只有**心跳过期且原进程身份已失效**才回收，旧 claim token 无法写入新归属。监督器的消费者是线程，每个消费者必须有有限 I/O 超时；它替换退出线程、隔离命名池、定期快照，不强杀卡住的 Python 线程。线程内的提交不确定会保留 claim，需核查或等待其宿主退出后恢复，不能借线程重启重发副作用。

队列 `reconcile(verify_artifact)` 按任务状态、completion 流水、实际产物三方核对。COS 对应 `batches/claims/done`，只有成功 rc 才允许 done；损坏批次报错，禁止静默跳过坏行。COS 依赖对象存储条件 PUT 的原子性；显式回收旧 claim 属于至少一次处理语义，消费者仍须使用内容寻址/幂等提交。

`NetworkConfig.from_env(path)` 支持 direct/environment/explicit 代理模式和精确 host_map，配置只当数据读取，不执行 shell，也不修改进程全局环境。文件使用原始 `KEY=value`，不使用 shell 引号、变量插值或命令替换。

## 下载业务的新入口

`demiwtg/collect/download/platform_pipeline.py` 仅定义固定版本 Lance 任务源、COS 签名/源回退、SHA256+长度验证、原子产物落盘；队列、命名池、批量认领、心跳、回收、快照归平台。

```bash
# 在 demiwtg 项目目录，用已安装 demiflow[collect,lance,local] 的 Python。
python -m collect.download.platform_pipeline build --db /tmp/new-download/tasks.sqlite \
  --source /absolute/path/images.lance --version 1
python -m collect.download.platform_pipeline run --db /tmp/new-download/tasks.sqlite --max-workers 8
python -m collect.download.platform_pipeline status --db /tmp/new-download/tasks.sqlite
python -m collect.download.platform_pipeline reconcile --db /tmp/new-download/tasks.sqlite
```

配置可保存到 `.dl_env`：`DL_PLATFORM_QUEUE_DB`、`DL_PLATFORM_SNAPSHOT`、`DL_PLATFORM_BLOBS`、`DL_BUDGET_CN`、`DL_BUDGET_INTL`、`DEMIFLOW_PROXY_MODE`、`DEMIFLOW_PROXY_URL`、`DEMIFLOW_HOST_MAP`。兼容既有 `DL_PROXY_MODE=keep` 和 `DL_ACCELERATE_TAGS`；凭证仍从原 COS 凭证机制获取。

本次没有热替换正在运行的旧下载线。新 CLI 拒绝使用 `DL_QUEUE_DB` 指向的旧队列文件。已有旧进程、旧队列和脚本保留至本批结束；后续任务使用新入口。正式切换前停止原写入者，固定原任务源版本并核对旧完成账和产物；新队列重新声明任务时，经过 SHA 复验的已存产物走零下载快道。

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
