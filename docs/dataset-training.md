# 在 Dataset 图中训练与评测

训练数据和留出集可以各自经过 Dataset 变换。调用 `fit` 建立训练结果引用，`checkpoints.select` 指定要读的模型步数，`predict` 将模型引用与留出集连入同一张图并返回 Dataset。执行由已有的 Dataset action 触发。

```python
from demiflow import data

train = data.read_lance(train_uri, version=train_version).filter(accept_training_pair)
validation = data.read_lance(validation_uri, version=validation_version)
holdout = data.read_lance(holdout_uri, version=holdout_version).filter(is_scored)

fitted = train.fit(recipe=recipe, monitors={"validation": validation})
scores = (
    fitted.checkpoints.select(steps=[400, 800])
    .predict(on=holdout, inputs={"prompt": "question"})
    .map(score_sample)
)
scores.write_lance(scores_uri, mode="create")
```

上例中的 URI、数据版本、行函数及 recipe 由项目提供。每个选定模型都读完整的 holdout。训练内的 validation 会改变训练身份。只换下游 holdout、评分函数或选取步数时，已提交的训练结果可以复用。

## 项目作者提供什么

| 接口 | 合同 |
|---|---|
| `data.TrainingRecipe(...)` | 指定 `trainer`、`train_schema`、具名 `monitor_schemas`、有效参数及其 JSON Schema、基础模型与环境内容摘要、优化器更新预算、保存步数、数值拓扑、模型格式和默认 predictor。训练输入及监控输入在领取作业前固定到版本。只在明确声明的 `media_fields` 上验证外部文件内容，部署提供允许访问的根目录。 |
| `trainer(request, context) -> FitResult` | `request.inputs` 含固定的数据版本及证明。训练文件写到 `context.model_dir`，框架工作文件写到 `work_dir`。用 `report_snapshot` 汇报已经提交的步数，用 `check_cancelled` 响应停止。需要进程组时通过 `run_group` 启动部署允许的模块。可选的 `trainer.reconcile(request, *, model_dir, work_dir)` 用于找回框架已落盘、尚未上报的原生快照。 |
| `data.PredictorSpec(...)` | 声明 actor 工厂、具名输入到列名的映射、`input_schema`、输出 schema、兼容模型格式、参数、batch 和资源。actor 构造函数接受 `model`、`inputs`、`parameters`。在 worker 内加载清单所指的模型并实现 `__call__(batch)`。 |
| 预测批输出 | 每个输入按原顺序返回一行。设置 `batch_size > 1` 时，批输入还包含 `__demiflow_prediction_input_id`，actor 必须逐行原样带回，worker 校验后移除。多 seed 可每行返回结果列表，再用 Dataset.flat_map 展开。项目用明细行保留每个失败样本和原因。 |
| 模型产物 | `FitResult.snapshots` 包含各步的 `TrainingSnapshotRef`。推理文件和续训状态各自带 URI、SHA-256 与字节数。模型归项目持久存储管理。评分表用 Lance 的实际发布回执固定所用模型版本。 |

具体构造及可执行示例见[公开声明](../demiflow/data/training.py)、[作业合同](../demiflow/execution/contracts/training.py)与[通过公开 action 写表的测试](../tests/test_training_dataset_action.py)。

## 运行在哪里

| 部署 | 启动与接管 |
|---|---|
| `EngineManaged` | 引擎按 action 资源上限和设备池分配整组资源。单元就绪后，持有原 run 写入权的 driver 才能授权项目运行。内置实现支持单机 CPU 或一机多卡。部署提供每作业 CPU、内存、解释器和子进程模块白名单；数值拓扑另由 recipe 声明。 |
| `ProviderManaged` | 部署提供具固定 scope、实现摘要及每次调用 deadline 的 `submit / lookup / inspect / result / cancel`。第三方负责设备与进程。提交回执不明时以原请求键查询。同键重送需有该服务的幂等承诺、原请求确未受理的证据以及有效期。付费部署还须具备可执行的费用上限。 |

Python 可将 `TrainingDeployment(project_id, binding, provider)` 传入 `RunContext.start(..., training=...)`。工作台通过环境变量 `DEMIFLOW_TRAINING_DEPLOYMENT` 读取部署 JSON 的绝对路径，配置声明 `project_root`、`project_id` 和 `binding`，第三方可以指定受信的 `provider_factory`。项目索引使用同一控制主机上的 SQLite，当前模型导入使用本机可访问的项目持久目录。引擎托管作业的 allocated_gpu_seconds 来自原设备池的实际分配至确认释放时间，包含同一实例历次训练启动的占卡时间。详见[部署配置](../demiflow/execution/training/deployment.py)。

## 模型配额用满后

四项额度由部署指向的项目控制目录统一审批。共用同一 `fit.sqlite` 的多个 `project_id` 共用额度。需要隔离时，各自配置独立的控制目录和模型存储。

| 部署字段 | 计量 |
|---|---|
| `max_model_bytes`、`max_files` | 按内容去重后的模型文件和清单的总字节数、对象数，含正在导入的预留量。 |
| `max_instances` | 索引中所有状态的训练实例总数。复用同一实例无需新增名额。 |
| `max_references` | 评分表的待确认或已确认引用、父快照引用和仍在使用的引用总数。 |

管理员保留原受信部署文件，另存一份提高额度的新部署，给后续 run 使用。首次使用该部署的新 run 会在训练 action 登记时应用额度。管理员也可以提前应用，随后直接恢复符合条件的原 run。

```python
from demiflow.execution.training.deployment import load_training_deployment

deployment = load_training_deployment(deployment_v2_path, project_root=project_root)
deployment.apply_project_quotas()
```

四项额度在同一个项目索引事务中逐项比较，允许持平或提高。任一项下降，整次变更回滚。原控制主机、模型存储和已登记币种的费用上限继续严格核对。已有实例、权重、评分表和引用继续保留。

扩容后先检查原 run 和实例状态。工作台恢复仍校验原部署文件的路径和 SHA-256。

| 已记录的状态 | 后续操作 |
|---|---|
| 已登记 fit，原训练仍可接管，评分表允许恢复 | 恢复原 run，用原请求键、原训练和模型身份使用已批准额度。 |
| 原 run 在首次登记 fit 前中断 | 原部署四项额度须与已建立的项目索引完全一致。若已变化，用当前部署开启新 run。 |
| 训练实例已取消或失败，且没有已发布清单 | 如需重新训练，新 run 显式指定新的 `retrain_id`。 |
| 模型已发布，评分表因引用名额不足而失败 | 提高引用额度后，用相同训练键启动新的评分 action，复用原模型。评分目标仍须符合原有写表规则。 |

旧版可能已登记部署摘要，但在首次创建 `fit.sqlite` 前退出。仅当确认该控制目录从未建立索引、从未提交训练时，管理员可用原受信部署执行 `apply_project_quotas()` 首建后恢复。已使用的索引或权重丢失时，应先从备份恢复原库和原模型对象。

## 复用、重训与恢复

- 同一项目内，由固定训练/监控数据、配方代码、环境、基础模型、有效训练参数、预算、保存步数、数值拓扑以及显式父快照确定训练身份。并发消费者共用一个生产者，只有生产者申请训练资源。选取未保存步数会报错。显式重训使用稳定的 `retrain_id`，显式续训传 `resume_from` 并在配方中声明新的总更新预算。
- driver 在第一条预测行提交前退出时，从工作台恢复**原 run**。引擎沿原 action 接回已受理的 fit。显式 `create` 在原目标缺失已登记、恢复时仍可证明目标未被外部写入时，可以继续。尚无行提交的默认 `append` 或目标已有外来表时，引擎拒绝回放。可对已受理作业执行原 run 授权的对账；继续评测需满足原终端的恢复条件。
- 一旦有行恢复点，预测、普通行处理和写表共用已有的行恢复协议。权重引用须与该切点实际处理过的预测节点一致。actor 的可恢复性沿用普通批算子的状态检查。含模型张量或其他不可自动保存状态的 actor 应提供成对的 snapshot_state / restore_state；可重建的 Qwen actor 只保存并复核模型和 predictor 的身份。训练框架管理优化器/RNG 快照。旧训练组退出得到确认后，才可用已校验快照恢复，次数受部署上限约束。
- 训练预算可指定 `optimizer_updates` 和 `max_epochs`。声明 epoch 上限时同时提供 `topology.effective_batch`；默认以固定训练表行数估计每 epoch 可用样本，项目有经校验的遍历规则时可提供 `budget.examples_per_epoch`。不可达的更新数会在训练资源分配前拒绝。
