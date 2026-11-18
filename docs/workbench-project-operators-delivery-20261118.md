# 工作台：项目算子、Prompt 与多流程

本轮在 `workbench/project-operators-20261118` 隔离分支实施，基线 `f22980d`。主仓只读；原端口为 59866。

## 用户看见的变化

- 名称为“算子”。左侧可展开目录与搜索，右侧阅读真实函数/类或具名 Prompt；目录属于项目，不随当前流程的调用次数复制。
- 代码算子展示定义、参数、默认值与已声明返回类型；未知静态类型明确写未声明。Prompt 默认显示正文，另一页显示原定义版本、模型默认值和输出 Schema。
- 版本历史读取当前 Git 文件最近 16 次真实提交，选中后读取该提交中的同名定义。历史只读，不恢复或覆盖工作区；没有 Git 历史、历史中尚无该定义时明确说明。
- 管道只有流程选择，不再展示参数文件/配置方案选择器。业务参数在一个轻量表单中按需展开，默认值来自入口签名。项目可以有多条真正独立的流程。
- 每流程沿用唯一已登记参数文档。未绑定时只读默认值；存在多份旧绑定时明确冲突，不猜选。修改 A 不改 B；切换保留各自未保存草稿，草稿不参与解析或运行。
- 编译图继续来自引擎。节点 API、输入、输出、代码、Prompt 沿原快照阅读；从自定义代码/Prompt 可进入匹配的当前项目定义，明确区分当前与快照。
- 减少边框、常驻按钮和重复标题；目录与正文各自滚动，窄窗口可阅读。

## 代码与边界

`projects/discovery.py` 支持 v3 的明确 flows/operators/prompts 归属。`projects/definitions.py` 只读 AST、原生 Prompt pack 和 Git，不导入用户代码。

| 接口 | 输入 | 输出/约束 |
| --- | --- | --- |
| GET project-operators | 项目身份 | 稳定定义 ID、路径/符号、摘要、索引 revision；不含大段正文 |
| GET operator-definition | id, revision=current 或所列 Git 提交 | 精确代码/Prompt 内容、接口、版本依据 |
| GET operator-history | id | 最多 16 个真实文件提交，不伪造语义版本 |
| GET flow-arguments | entry | 唯一参数绑定、当前值、签名默认值、source_sha256、文件 revision |
| POST flow-arguments-save | entry, revision, source_sha256, arguments | 需 config_write；类型/必填校验；原子 CAS 写原文档；冲突保留草稿 |

最多 128 个定义文件、1024 个定义；单文件 512 KiB、总源码 4 MiB；AST、Prompt 深度/节点、输出大小有界。Git 命令固定且限时，不使用 shell。API 增加一个并发度为 1 的项目元数据通道：只处理这五类轻量源文件/参数请求，不执行候选代码，不占用运行停止的保留通道。HTTP 总请求上限保持。

多流程解析支持项目内 import 和仓库命名空间，仍落在同一个有界源码观察与只读解析保护内。没有新增执行器、运行账本、Prompt 账本或参数方案。

## 标杆

`examples/refgen_reference_case` 显式注册在 `examples/.demiforge/project.json`：

- `flows/reference.py:run`：完整参考图联合出题与对照评测，47 节点 / 52 边。
- `flows/quality.py:run`：独立图片质量审核，7 节点。
- 两条流程共享 `refgen_operators.py` 与 `prompts/quality.yaml`。保留实际文件名，不为目录标签复制算子；图像 Prompt 从内联定义移到 `prompts/image.yaml`。
- `settings/reference.json` / `settings/quality.json` 各一份业务值；`settings/runtime.json` 负责模型部署。隔离生成文件不提交。
- 原 `flow.py` 为原测试调用的兼容入口；完整接线只有 `flows/reference.py` 一份。

## 验证与明确限制

证据目录：`/tmp/demiflow-project-operators-20261118`。正式测试均从实际隔离树以显式 PYTHONPATH 调用；原生测试只写隔离临时目录。

- 目录、Git 历史、默认值、参数隔离、CAS、无权限拒绝和读取容量竞争有测试。
- 浏览器验证真实目录/搜索/历史/Prompt，独立 7 节点和完整 47 节点图、业务参数、刷新和窄窗口；页面错误为 0。另验证流程草稿往返、保存、重载和图到定义的跳转。
- 原生 Dataset 标杆实际执行与提交：质量审核 3 条；完整实验 36 条（24 条评分 + 12 条汇总）。只有外部模型 transport 是模拟；数据、图片对象、算子与提交是真实引擎。
- 本轮标杆原生运行使用 RunContext。该复杂标杆尚未通过工作台旧静态提交准入，页面不开放它的提交按钮；不能把真实编译预览或直接原生执行冒充已通过工作台 prepare/start。现有受管流程的提交路径保持。
- 当前定义与文件提交历史可读；不做共享算子发布仓、版本恢复按钮或全局算子注册中心。动态跨文件使用关系不凭近似字符串推断。
- 已存在多参数方案的老项目不静默迁移。未声明/无法静态提取的类型、动态默认值不冒充已知 Schema。
- 本轮未调用真实付费模型，不据模拟响应宣称业务质量验收。

最终测试、提交及主仓/冻结件保持证据以同目录交付证据为准。
