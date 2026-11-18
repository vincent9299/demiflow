# 工作台与引擎边界：Claude 步骤 0–4 集成

2026-11-18。目标仍是让用户看懂真实 Dataset 计划、API、输入输出和自定义实现，同时由引擎唯一解释执行、提交和历史事实。工作台保留权限、展示、分页和交互政策，不新增运行账本或执行器。

## 位置与提交

- 隔离树：`/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_boundary_integration_20261118`
- 分支：`integration/workbench-engine-boundary-20261118`
- 起点：`7634eecadc02fd9db5768cfd97b9ecefa3195374`（原 GPT 候选树保持）。
- Claude 独立克隆的三项按序取入：`9cb5ac8 → bef442d`、`2eb6f3e → f0f5435`、`8e62440 → 0a319f5`。没有取其其他探针提交。
- 工作台消费者切换、读预算补齐和测试迁移：`f15c974`。
- 已授权的页面顶部紧凑化：`b9da45062f8d4b17d788c585996558e088cb1562`。以下代码验收绑定此提交。
- 主仓仍为 `e40a6dd`，未修改、未合入、未推送。原主仓 dirty 和文档逐字校验保持；冻结七件 SHA-256 与权威清单一致。

## 实际接线与清理

| 工作台/消费者 | 引擎唯一实现 | 保留在工作台的职责 |
|---|---|---|
| `runs/manager.py` 原 `_native_execution_receipt` | `publication_facts.inspect_write_receipt`，原名字直接引用同一函数 | 结果登记、权限与恢复操作准入 |
| `runs/native_stages.py::admit_prepared` | `publication_facts.admit_prepared_stage` | 封存报告的父 action 选择、两类原生错误映射，不伪造 ACK |
| `runs/storage.py` 所有权命名空间 | `publication_facts.object_namespace` | 项目可写位置与路径准入 |
| `execution/dataflow_sinks.py` v3 回执观察 | `observation_action / commit_write_context` | 引擎内也不再保留第四份观察上下文拼装 |
| `runs/view.py` 状态/事件/观测/上下文摘要 | `run_reader.read_run / RunReader` | HTTP 错误、游标绑定、分页与响应预算、输出状态呈现 |
| `runs/storage.py` 存储快照/manifest | `run_reader.storage_snapshot / document` | 有界文件遍历、历史容量政策 |
| `runs/domain.py::_owner_finished` | `RunReader.attempt_state` | 进程确已停止与清理完成后才释放容量 |
| `runs/requests.py::_incomplete_seal` | `tables / document / has_rows` | 准备请求身份、未知执行事实不准清理 |
| `runs/facts.py` F1/v3/身份桥 | `sealed_plan_facts` | 历史载体校验、报告与计划身份对应、诚实的不可用原因 |

工作台运行管理的五处 SQLite 直连、提交器 `_context` 和 journal `_connection` 私有访问已消除。旧 SQL 和提交检查实现已删除，原生检查逻辑不复制到新适配层。临时 `boundary_shadow.py`、六个 `test_boundary_equiv_*` 与 publication 对比夹具删除；它们复用的原始场景测试全部保留。publication 的独立只读/防伪造/冷导入测试保留为 `test_publication_facts.py`。

## 集成中补齐的细节

1. 新 reader 的状态、runtime summary、控制文档、事件在 SQL 返回前限长，避免先把整个超限值读入 Python 再拒绝。保留历史 attempt-state 的字符上限口径，runtime summary 则按交接约定统一为 64 KiB 字节上限。
2. `open_journal/read_run` 拒绝链接路径。`storage_snapshot` 接收历史扫描器已经安全打开的 `/proc/self/fd/...` 目录，保留文件类型、大小和前后 inode 校验；不能把受控 FD 路径误判为业务符号链接。文档明确二者的不同调用前提。
3. `RunReadError` 属引擎契约错误，工作台负责错误码映射。容量所有者无法读取或链接路径变更时返回“不足以证明已释放”，不释放他人资源。新增真实 SQLite 反例覆盖所有者和未完成准备。
4. 边界白名单按新基线逐项核对。公开 SDK 类型、原生 schema 解释、原生 stage 提交事实与封存 Lance spec 解码列为公共消费；没有将编译器 frame 观察转成公开接口。
5. 旧恢复测试的注入函数仍使用过期签名，原候选树独立复现后，补上实际 `publication_reserved_bytes/admit_publication` 参数。旧“中间 ACK 一律空”的假设改为核验暂存分区与回执的行/字节相符、没有 native receipt，最终仍严格验证唯一已提交版本 1、结果精确相等和原 dag_id。没有放宽发布或身份断言。

## 产品呈现

计划状态、流程、入参文件、Pipeline 源码、解析和运行同处一个紧凑栏；节点定位、缩放、全屏归属画布；“更多”变为纵向菜单，其他入参路径和诊断默认折叠。首个执行动作的范围在标题上明确保留，完整解释按需展开。

算子目录与图共用 `API接口 / 输入 / 输出 / 自定义算子代码 / 自定义 Prompt`；输入直接呈现参数（含默认）和上游 Dataset Schema。已移除“输入输出与执行详情”“原文信息”“来源摘要”的独立显示。没有业务 pipeline 名称或内容判断分支。

## 验收

最终验收绑定源码提交 `b9da45062f8d4b17d788c585996558e088cb1562`，全部通过：

| 验收范围 | 结果 | 原始证据 |
|---|---|---|
| 工作台/引擎边界、只读 journal、历史容量与权限、封存计划与身份桥、真实 Lance 提交/进程中断/恢复、保存产品链路 | **250 passed，0 failed / error / skipped**；739.05 秒 | `/tmp/demiflow-boundary-final2.xml`、同名前缀 `.log` |
| 紧凑导航、上下文切换、迟到编译响应隔离、配置变更清除旧预览 | **4 passed，0 failed / error / skipped** | `/tmp/demiflow-boundary-ui-final.xml` |
| 原端口的真实标杆页面 | **7 组检查通过**，47 个节点；浏览器错误与失败 HTTP 均为 0 | `/tmp/demiflow-combined-preview-20261117-q0st03t2/boundary-compact-live.json` |

现场覆盖 join 的七个参数（含四个默认值）、左右 Dataset Schema、Prompt/UDF/API/Pipeline 源码、目录与图往返、连续刷新、六种宽度（1700/1440/1024/780/390/320）、更多菜单、全屏及缩放。已亲看桌面图与窄屏菜单截图；截图同在上述预览目录，以 `boundary-compact-` 为前缀。此次浏览器检查以读取与编译呈现为范围，不代表所有产品功能重新验收。

机器可读证据：[boundary-integration-20261118-evidence.json](boundary-integration-20261118-evidence.json)，含代码提交与树摘要、包内逐文件 SHA-256、两份完整 XML 摘要、现场检查摘要、冻结七件与主仓/注册表/token 保持结果。测试过程中源码和测试文件均未改变。

验收纪律：探索阶段一次集合运行与 UI 编辑重叠，平台快照保护正确报出 `source_changed`；该次不作为交付验收。随后冻结 `b9da450` 的源码再验。第一轮冻结验收进程在写出最终 XML 前退出，原因未确定，保留其不完整日志、不计通过；最终以独立临时目录 `/tmp/demiflow-boundary-final2` 的完整运行替代。所有测试显式指定隔离树 `PYTHONPATH`，使用该树下的 `python -m pytest` 和私有运行域，不从主仓取包，不写主仓或业务数据。

预览服务沿用 `59866`，项目注册表、会话 token、Notebook 数据库及业务代码路径保持。进程中断后，只清理已确认无人监听的本预览 Unix socket 并恢复本服务；不改其他服务。现场检查只做读取和执行计划解析，不启动付费模型调用。

本次没有将“流程/入参文件”直接重命名。用户提出的信息层级问题已核对源码：项目登记多个入口，每个入口对应多个参数文件；目前没有“管道下面再登记多个流程”的独立对象。建议统一显示为“管道 / 参数”，把编译计划和运行状态作为结果信息，不新增一层实体。该建议仍与已交付的布局调整分开记录。

## 保留范围与未做项

- 完成的是设计步骤 0–4 加本轮被替代代码清理。步骤 5 的统一宿主启动入口、步骤 6 的其余架构整理没有借此宣称完成。
- `runs/worker.py` 的三项执行入口导入和三处 `_executor` 仍列为步骤 5 待迁移。
- 现有源码展示还有 `graph/plan_details.py` 五项模型/计划类型依赖，以及 `graph/plan_callsite.py` 的编译器 frame 观察接口；均保留精确 pending 清单。这是已存在的呈现接缝，不以本次读取边界集成掩盖它。
- 未跑整个仓库测试，也没有重新做跨机/GPU/性能验收；本次不改变调度、算子计算、提交协议或数据 API 语义。

## 分支与 worktree

创建前：133 个本地分支、117 个登记工作树，另有 Claude 一个独立克隆；无缺失路径可直接 prune。创建后盘点：134 分支、118 工作树。只读审计发现 9 棵 dirty、4 棵被进程 cwd 使用；48 棵满足“干净、HEAD 已被旧候选包含、未发现进程 cwd”的初步条件，仍须查归属和服务/路径引用，不能直接据此删除。

本轮删除工作树/分支数量为 0。清理不作为开发前置，新工作已在独立分支进行。审计快照在 `/tmp/demiflow-boundary-worktree-audit.json`。
