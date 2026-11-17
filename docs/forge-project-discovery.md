# 原项目识别与统一检查

登记一个原项目目录后，CLI、HTTP、独立 Web 和外部客户端读取相同的识别结果。平台不复制业务、不创建第二份接入配置、不执行项目代码或声明中的验证命令。业务改造仍需用户确认。

## 职责与数据流

`console.add` 只登记目录及必要平台管理信息。`project_discovery.discover` 只解释公开声明，产出事实、依据和缺口；`workspace_context` 直接携带这份结果。Web 负责呈现，不能推测协议或补造状态。

`project_check` 使用同一识别结果，复用 `SourceTree` 做实际原源码的前置检查，复用原有 Flow 检查器检查已明确绑定的原生入口与参数。未绑定参数的入口、旧入口协议和未覆盖代码均不计为通过。部分源码检查成功也不代表完整 Flow 检查成功。

数据查看复用现有 `workspace_worker` 和 Lance 受限预览，不新建数据登记、执行器或业务适配器。原生 prepare/start/resume 准入不变；识别不产生执行权限。

## 支持的依据与边界

| 依据 | 读取内容 | 明确不推断的内容 |
| --- | --- | --- |
| 原有 `workspace.json` 的入口和参数绑定 | 原生入口、配置与实际源码 | 不把公开 helper 当成新入口 |
| 最近的 `.demiforge/project.json` 公开 JSON（project-v1/v2；candidate.1–9） | 只选择 `path` 与注册目录准确匹配的 pipeline；旧协议入口、现代 `{path,symbol}` 和算子角色 | 不读取工具实现，不执行 `verify`，不合并兄弟项目，不改变原规范 |
| 原项目 `README.md` 中明确的本地 JSON/YAML 链接 | 配置原文、来源行号与内容摘要 | 文档链接不等于入口参数绑定；YAML 不求值、不展开 |
| 配置中的精确 `{uri, version}` 引用、README 的字面量 `.lance` 路径 | 数据位置、已声明版本、可访问性 | 不按业务字段名猜输入输出，不将外部 URI 当成读取授权 |
| 原有平台运行记录、README 中明确的运行产物路径 | 已支持的平台状态或原文引用 | `.sqlite/.jsonl/.log` 引用不等于可解析运行记录，更不能表示成功、恢复能力或完整历史 |

发现多个目录归属声明、入口协议冲突、不支持的版本、语法错误、缺失入口/配置/算子时分别列出诊断。没有支持的声明时显示“未识别”及原因，不将未识别数量解释为项目没有内容。目录名称只在已支持的历史规范规定角色时解释；任意项目不因目录命名被猜测或改造。

父声明最多向上查找 32 层，并在最近仓库边界停止；只读取公开项目 JSON。项目源码、配置和预览数据必须留在注册目录内，拒绝符号链接、路径越界及隐藏控制路径。项目外数据保留引用和原因，不新增授权；原生已经授权的固定表继续走原有接口。

识别每次最多读取 128 个文件、合计 4 MiB；单声明/源码和 README 各 1 MiB，配置各 64 KiB，配置引用最多 64 个、数据引用最多 256 个。README 展示与引用识别采用同一读取上限；超过 64 KiB 的文档复用现有分页原文阅读器，逐页完整查看，不扩大 Markdown 解析或单页 DOM 预算。JSON 遍历最多 4096 个节点、深度 16，拒绝重复字段。不会递归扫描运行历史、数据库、图片或整表。超限显示无法识别的具体部分；内容可见不代表全项目扫描完整。

## 查看数据与检查

声明已给定版本时，只查看该版本；文档未指定版本时，打开时读取一次当前版本，然后分页及 Case 始终绑定该版本、schema、行数和声明摘要。表头更新不使当前分页追随新版本。声明变化返回冲突，用户重新选择并核对。外部资源、图片和未知调用账本不会从行内容自动解引用。

源码前置检查使用现有 `SourceTree` 的规则，实际读取的文件及内容摘要写入报告，包括已读取但语法解析或节点预算检查失败的源码；可能在首个错误、不支持的导入或初始化处停止。读取失败仍报告未完成，不能以内容摘要表示未读取的文件。未读取的依赖、Flow 语义、算子行为、业务语义均不被这一步认证。完整原生 Flow 检查仍要求明确的受支持入口和参数绑定，遵守原有平台快照和资源边界。

报告保存识别摘要、读取源码摘要及本次检查器的平台摘要。当前内容依据覆盖声明的入口/参数和检查实际读取的依赖闭包；父声明、识别时已读取的入口/配置、报告内源码或检查器变化后，`check-status` 标记依据变化，不会刷新成通过。已声明但未进入实际读取闭包的算子仅检查角色路径存在（`operator_checks: declared_path_existence_only`），不检查其全部内容；仅修改这类算子的内容不保证使报告 stale。未能读取的目标没有内容摘要，修复后需重新检查，不能从旧诊断推断当前内容。原工作区执行快照的状态独立保留，不为只读识别自动重绑执行快照。页面 `changes.discovery` 触发重新读取识别结果；这不是后台全量源码或数据巡检，也不改变既有审计机制。项目检查更新当前派生摘要，历史 Flow 检查报告和运行不重写。

## 无 UI 使用

```bash
demiflow workbench console --registry /absolute/console.json add /absolute/original-project
demiflow workbench discover /absolute/original-project --json
demiflow workbench check /absolute/original-project
demiflow workbench check-status /absolute/original-project
```

查看配置使用 `workspace document ROOT PATH --expected SHA --json`。
查看声明数据先使用 `workspace data-preview ROOT ID --expected DISCOVERY_DIGEST --json` 获得元数据，再使用同一命令加 `--version VERSION --binding BINDING --limit 20` 读取固定版本页；支持 `--column` 和 `--row-id`。不要求创建 `tables/` 或重复绑定数据。

接入另一个项目使用相同流程。缺少支持的规范信息时只报告缺口；平台不替用户生成业务声明或改写代码。
