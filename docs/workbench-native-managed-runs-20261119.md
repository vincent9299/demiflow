# 原生 Dataset 受管提交

工作台通过 P4 的原始对象接入原生 Dataset 流程，沿用 RunJournal、RunContext、DataflowExecution、CommitCoordinator、部署准入和停止／恢复协议。没有新增运行账本、调度器或业务专属路由。

## 接口与边界

- `capture_managed_pipeline(entry, arguments, *, limits, on_compiled=None)`：一次性只读工作进程构造真实 Dataset，停在第一个终结动作；不伪造结果去探测后续 Python 分支。返回原 `CapturedAction`，附带有限的原生提交声明。
- `native_submission_view(captured)`：原生出现、边、写出目标和观测键的只读投影。节点来自真实编译器，不从 AST 重算 DAG。
- `NativeExecution(contract=..., ...)`：复用原 `CheckedExecution` 的观测、阶段提交投影和回执接线。实际驱动在启动数据读取前，逐字段比较完整编译产物和原生写出声明，然后封存同一身份桥；恢复仍读取原封存计划和原提交日志。
- `local_capacity_domain(directory)`：维护调用选择原部署容量目录的上下文。不同线程互不覆盖，不修改全局环境；原 domain/owner/租约校验保留。

本轮新增的受管原生范围是 Local、单个 `write_lance(mode='create', schema=...)` 终结动作及其全部上游图，包含原生 `save_lance` 阶段。阶段目标初始必须不存在；即使阶段声明 append，也只能基于原 run 的阶段回执继续，不能追加到未经准入的已有业务表。纯 `data.require` 使用原生断言动作记录，不占用 Dataset 终结动作的身份。第二个未封存 Dataset 动作明确拒绝。原有静态检查绑定、多动作受管路径保持。

工作台不猜 `output_directory` 等参数名，也不改写普通业务参数。原生写出使用编译器确认的实际目标；再次运行需提供新目标。现有显式 output 绑定仍保留原来的新运行目录功能。两类行为在提交确认中区分说明。

## 历史、资源与部署

源码导入闭包、构造时读取的项目资源、业务参数、平台环境与完整编译声明一并封存；业务源码和历史数据不改。历史图、源码、Prompt 和结果均读原封存依据。结果复用 P4 原提交验证和固定 Lance 版本，不读取当前表头冒充历史结果。

部署资源目录在准备时固定，传播给 supervisor、executor 和停止维护；先回收原引擎资源，再删除本次临时目录。新原生输出目录的文件产物也进入原 OutputWatch 与目录互斥范围。目录已有时只授权实际目标，不扩大到整个父目录。

需要外部 CLI 认证或项目运行环境时，部署可通过 `DEMIFLOW_WORKBENCH_RUN_ENVIRONMENT` 指向项目外的 JSON：

```json
{"format":"demiflow_managed_environment_v1","workspaces":{"/absolute/project":["MODEL_API_KEY","TOOL_SETTINGS"]}}
```

这里只登记环境变量名称，真实值从服务环境传递，不写入报告、manifest 或浏览器响应。没有按项目名称写死分支。HTTP 调用不能提交这份名单；HOME、PATH、TMPDIR、Python/平台控制变量等不可覆盖。

需要 Node 等外部工具时，同一声明可加 `"executable_dirs":{"/absolute/project":["/deployment/tools/bin"]}`。每项目最多 8 个规范绝对目录，必须位于项目外；封存目录列表并同时传给 supervisor 和 executor，不能通过 HTTP 任意覆盖 PATH。

运行图的业务名称、API 调用、代码和 Prompt 来自封存的编译材料，通过原身份桥关联真实执行节点。结果在运行页直接预览固定版本（最多 10 行、16 字段，复用原有有界读取），也可转入原生 Notebook 数据页；不要求先执行 Notebook 才能看运行结果。

组合验收发现并修复了首次对象协议登记与 supervisor 记账的竞争：只有无历史协议且取得原锁、目录、nonce 校验的首次登记可通过；既有协议替换、历史锁丢失和旧 attempt 仍拒绝。

## 验收记录

本文件是实现说明，最终发布状态以共享目录 `workbench-native-runs-20261119` 的验收证据为准。开发中已完成真实小流程的原生提交、固定结果读取和同 run 停止／恢复，以及 RefGen 完整业务代码配合可控外部模型进程的端到端执行；这些不替代真实模型服务和最终浏览器验收。所有验证都在隔离树和 `/tmp/demiflow-*` 私有目录进行，主仓未合入。
