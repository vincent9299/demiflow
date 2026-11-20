# 原项目识别与统一检查

登记一个原项目目录后，CLI、HTTP、独立 Web 和外部客户端读取相同的识别结果。平台不复制业务、不创建第二份接入配置、不执行项目代码或声明中的验证命令。业务改造仍需用户确认。

## 职责与数据流

`console.add` 只登记目录及必要平台管理信息。`project_discovery.discover` 只解释公开声明，产出事实、依据和缺口；`workspace_context` 直接携带这份结果。Web 负责呈现，不能推测协议或补造状态。

`project_check` 复用原生 Flow 检查器检查登记入口与参数，并报告单一静态状态。

数据查看复用现有 `workspace_worker` 和 Lance 受限预览，不新建数据登记、执行器或业务适配器。原生 prepare/start/resume 准入不变；识别不产生执行权限。

## 支持的依据与边界

| 依据 | 读取内容 | 明确不推断的内容 |
| --- | --- | --- |
| 原有 `workspace.json` 的入口和参数绑定 | 原生入口、配置与实际源码 | 不把公开 helper 当成新入口 |
| 原项目 `README.md` 中明确的本地 JSON/YAML 链接 | 配置原文、来源行号与内容摘要 | 文档链接不等于入口参数绑定；YAML 不求值、不展开 |
| 配置中的精确 `{uri, version}` 引用、README 的字面量 `.lance` 路径 | 数据位置、已声明版本、可访问性 | 不按业务字段名猜输入输出，不将外部 URI 当成读取授权 |
| 原有平台运行记录、README 中明确的运行产物路径 | 已支持的平台状态或原文引用 | `.sqlite/.jsonl/.log` 引用不等于可解析运行记录，更不能表示成功、恢复能力或完整历史 |

当前项目的 `workspace.json` 提供入口登记。依据 `operators/` 与 `prompts/` 展示定义目录。没有登记时显示原因，目录发现不授予执行权限。

前台变更检测使用同一次发现得到的算子和 Prompt 目录，因此新建、编辑或删除定义会触发目录重读，即使任一条已登记的 flow 尚未引用它。

项目声明只从当前登记目录读取。项目源码、配置和预览数据必须留在注册目录内，拒绝符号链接、路径越界及隐藏控制路径。项目外数据保留引用和原因，不新增授权；原生已经授权的固定表继续走原有接口。

识别每次最多读取 128 个文件、合计 4 MiB；单声明/源码和 README 各 1 MiB，配置各 64 KiB，配置引用最多 64 个、数据引用最多 256 个。README 展示与引用识别采用同一读取上限；超过 64 KiB 的文档复用现有分页原文阅读器，逐页完整查看，不扩大 Markdown 解析或单页 DOM 预算。JSON 遍历最多 4096 个节点、深度 16，拒绝重复字段。不会递归扫描运行历史、数据库、图片或整表。超限显示无法识别的具体部分；内容可见不代表全项目扫描完整。

## 查看数据与检查

声明已给定版本时，只查看该版本；文档未指定版本时，打开时读取一次当前版本，然后分页及 Case 始终绑定该版本、schema、行数和声明摘要。表头更新不使当前分页追随新版本。声明变化返回冲突，用户重新选择并核对。外部资源、图片和未知调用账本不会从行内容自动解引用。

项目检查枚举已登记的入口与参数配置，并复用原生 FlowChecker。报告记录实际读取的源码摘要，包含读取成功后语法解析或预算检查失败的文件。检查只覆盖已登记且在本轮完成的变体。

报告记录工作区登记、源码、参数和检查器的平台摘要。状态读取核对工作区依据、各变体报告和源码摘要。README 引用用于发现与阅读，独立于静态检查的结果。旧项目检查格式需要显式重跑。运行快照及其历史身份由运行服务维护。

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
