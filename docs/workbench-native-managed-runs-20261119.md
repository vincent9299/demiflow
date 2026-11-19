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

supervisor 与 executor 的进程生命周期复用原 `RunJournal.connection_scope`，每进程至多保有原读／写连接，单次操作仍独立提交或回滚。在真实项目所在的共享文件系统上，避免反复初始化 SQLite 与首次对象登记争用；没有新建账本、关闭锁或扩大数据库等待时限。该原生方法列入 P4 的明确公开表面。

原生模型阶段因驻留预算而停驻、等待在途请求结束时，沿用既有 `checkpoint_wait` 消息区分业务等待和检查点协调。只有 driver 已授权停驻、且归属于该 worker 的原生 prompt／embedding 请求才能申报；串行请求只在实际调用期间计入。完成即撤销，boot／generation／partition 校验、其他 worker 的等待、停止／发布时限与累计运行时限保持。普通 UDF 或未停驻模型不能借此豁免检查点超时。

只读检查遇到不支持的原生提交形态，返回具体准入原因；不会启动执行，也不把明确的准入错误降成“工作进程失败”。前端读取共享有限请求队列，切换视图取消过期读取；只有服务器明确声明尚未执行的繁忙只读请求可以重试。

## 验收记录

本文件是实现说明，最终发布状态以共享目录 `workbench-native-runs-20261119` 的验收证据为准。

- 原始 RefGen `reference_delivery` 代码、算子和 Prompt 未改。真实项目运行 `rde7e5eda6c5d4a0b94deabf8` 调用已配置模型，以一个原始概念验收，三份固定 Lance 输出各一行；出题 ready、参考图 reused、定稿 not_required，业务错误为空。这次模型选择复用参考图，不声称覆盖真实生图分支。
- 可控外部模型进程覆盖复用与合成分支；完整原始 Dataset 接线、封存、提交与恢复均真实执行。浏览器运行 `r9218dc7182e94428922d2549` 停止后沿同一 run 恢复为 attempt 2，三份输出各四行；已完成作者请求未重复调用，attempt 1 保留。可控模型证据不冒充外部真实模型服务。
- 浏览器检查实际封存的 16 节点图、业务名称、源码／Prompt／接线、三份固定结果，覆盖刷新和 1440／1024／390 宽度。七条原始业务流程均通过只读编译和原生准入；不称七条都已调用真实模型执行。
- 专项覆盖容量域与回收、首次对象登记竞争、慢模型停驻等待、历史代码漂移拒绝、错误准入与重复请求，以及原有运行界面回归。最终命令、通过数、跳过项和发布后的合流检查另存机器可读证据，不声称全仓测试。

修改与测试运行在隔离工作树。合成数据／外部进程夹具使用 `/tmp/demiflow-*`；真实原项目的验收记录正常保存在其 `.forge/runs`，输出使用本轮独立 `/tmp/demiflow-*` 目标。只通过公共工作区接口调整 RefGen 运行预算及当前平台绑定，七条流程唯一业务参数、业务源码、Prompt、历史表不改。主仓未合入。
