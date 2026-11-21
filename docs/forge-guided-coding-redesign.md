# Forge 开发辅助改造

任务分支 `gpt/workbench/interaction-20261120`。工程基线 `a9641d2`，含 `main@5f3c077` 和本分支发送修复 `5ae98dc`。本方案按用户最后确认的全新 Forge 设计。

## 中粒度

AI 在首次写代码前收到本次实际使用的 DemiFlow 公共 API、项目目录约定和当前项目的源码位置。业务 flow 用原生 Dataset API 串联数据源、转换、模型与输出。普通行处理可以用函数或 Actor。flow、代码算子和 Prompt/Agent 配置分别落在 `flows/`、`operators/`、`prompts/`。

| 模块 | 权威与输出 | 依赖方向 |
|---|---|---|
| DemiFlow SDK | 原生 API、签名、语义和运行行为 | 保持 SDK 原有公开入口。Forge 读取它 |
| Forge | 生成写前参考，检查实际源码的原生 flow 和目录角色 | 复用公开目录及原生静态检查。输入采用当前项目根、变更和显式提供的参数/资源解析 |
| 工作台项目 | 选中项目、读取权限、源码目录、`workspace.json` 和参数来源 | 负责文件解析和参数/资源绑定，将有界事实传给 Forge |
| 工作台开发对话 | 授权、用户要求、真实模型传输、进程排空、改动和结果展示 | 每轮构造资料并把实际检查结果写入现有回合。模型适配器处理通信和能力约定 |
| 工作台运行与引擎 | 运行准入、资源授权、代码绑定、执行提交和封存事实 | 走现有原生链路。静态检查结果只用于开发反馈 |

当前项目的登记事实来自其 `workspace.json`。约定目录用于识别引用和提供源码位置。读取 README、规则或发现源码均不会登记该 flow 或授予权限。开发配置分为管道开发 `pipeline` 和明确授权的平台开发 `platform`。

全新 Forge 退役固定 v1 模板、父级 DemiForge 声明、任务 CLI、登记写回、模型独立审阅、自动修复与开发验收凭证。开发状态从独立新目录启动。工作台运行域也使用隔离环境验收。本次工作只提交源码，磁盘上的旧数据、预览和在线运行由其原环境持有。

### 工作台共享 Python 源码

部署者可通过 `DEMIFLOW_WORKBENCH_SHARED_SOURCES` 指向工作区之外的 JSON 文件，为已登记项目绑定共享模块。键是项目绝对路径和完整模块名，值是对应的绝对 Python 源文件路径。示例文件由工作台部署持有。

```json
{
  "format": "demiflow_workbench_shared_sources_v1",
  "workspaces": {
    "/srv/projects/shipping": {
      "shared.transforms": "/srv/library/shared/transforms.py"
    }
  }
}
```

需要执行的包初始化器可按包名绑定其 `__init__.py`。目录中已声明子模块的其他父包作为 Python 命名空间。单项目最多绑定 128 个模块、每个文件 1 MiB、合计 8 MiB。共享代码使用明确的运行参数或部署环境读取业务数据。部署文件只绑定 Python 源码。

预览与受管准备使用同一个模块解析规则并记录文件摘要。准备成功后将声明的源码封存到运行目录，包含运行期才导入的模块。启动时校验准备依据和封存副本，运行进程从封存副本加载。浏览器的解析与启动请求都沿用已登记的入口和参数。

## 源码级

### 写前资料

1. `forge/api_reference.py` 的 `build_reference()` 通过现有 `platform.discover_catalog` 读取 `demiflow.data`、`demiflow.operator_llm`、`demiflow.objects`、`demiflow.agent`。`sdk_identity(catalog)` 绑定 SDK 源码及语义文件。 `render_reference(reference)` 投影常用 API 的真实签名、文档节选、出处和最小示例，公开但尚未覆盖的 API 明确标注。目录上限 2 MiB，参考正文上限 20 KiB。
2. `workbench/projects/discovery.py:coding_view(root, *, exclude)` 读取当前根的控制文件和角色索引。它不打开平台快照、父项目声明、运行报告和数据文件。当前项目的 Pipeline 规范、README 和有范围的源码由 `conversations/context.py:assemble` 送进可信参考段。项目文本不能改变 grant。
3. `Service._launch_provider` 在首次需要时生成 API 目录，利用私有的匿名文件描述符复用。worker 读取并关闭描述符后启动 provider，并核验源码摘要。Codex 首次和续聊的开发指令包含所附资料，真人原文通过用户消息单独传递。DeepSeek ACP 在本轮 Prompt 中分别放入资料与原文。只读输入工具的能力由 Workbench 明确提供，适配器负责传递。
4. `Deployment` 校验当前 grant 和 provider 配置。全局配置一次，项目复用连接。当前项目根、写入范围、受保护文件、已授予的输入、机密和操作系统进程边界继续由现有服务和沙箱掌握。

### 写后检查

1. `conversations/worker.py:run` 用 `changes.capture`、`differences` 记录授权范围内的实际变更，范围可能包含显式 watch。已知保护内容不纳入改动采集。只有成功、已确认送达且排空的模型回合出现源码变更，才进入源码检查。普通问答不产生检查结论。
2. `workbench/projects/workspace.py:inspect_coding_changes` 确定受影响的已登记入口和每份参数。新的单入口 flow 可先做静态检查。资源继续用现有有界解析器。显式参数、控制文件、表绑定和检入的源码在结果发布前复核。同一项目共享定义改动检查已知调用者，预算或未知依赖造成的缺口单独记录。
3. `forge/pipeline_check.py:check_changes` 用 `FlowChecker` 检查最多 24 个入口/配置变体。检查从真实 flow 节点的算子、回调及 Prompt 资源定位文件角色。目录规则由 `project_rules.py` 持有。结果的 `native_flow`、`project_layout` 各自提供 `passed`、`failed`、`incomplete` 或 `not_applicable`、检查数和有限诊断。静态覆盖的上限跟随原生检查器，模糊调用和缺失事实不会产生原生流程通过结论。
4. `Store.finish_provider` 原子保存排空及检查结果。停止与发布并发时以已提交的停止为准。`development-chat.js` 只投影两项结果及诊断；无第二个 gate、任务表、验收状态或模型回合。

### 去除旧入口和接线

- `forge/` 删除 v1 固定模板执行、工具和合同。移除旧 `forge/development/`、`forge dev` 及专属打包数据。保留 API 目录、原生静态分析、已经有调用方的平台事实工具和 `demiflow run`。
- `workbench/projects/discovery.py:discover` 从本地项目登记发现 flow，公共角色目录概览与写前参考共享角色定义。`projects/definitions.py`、`graph/preview.py`、`runs/native_submission.py` 使用当前项目的相对路径和导入核验。移除子项目向上寻找父级声明及路径补前缀。
- `conversations` 删除旧任务登记、审阅、自动修复、gate worker、附加字段和专属启动分支。`runs/manager.py` 删除开发验收凭证依赖，继续以原有授权提交 guard 和原生运行预检为准。
- `Store` 使用当前有限 schema。有不同 schema 的状态目录被拒绝，无历史任务恢复或迁移分支。预览、引擎、算子、业务项目和发布目录没有随源码改造切换。

## 设计复核

| 接线 | 复核 |
|---|---|
| 模型实际收到了什么 | 使用首次和续聊的协议请求作证。内部 context 清单只作来源记录 |
| API、参数、资源有几个权威 | 签名来自 SDK，入口与参数来自工作台登记，运行事实来自运行服务和引擎 |
| 静态检查能否代替执行 | 检查没有运行准入接口。通过状态是静态结论，运行仍依赖每个操作自己的真实授权和封存 |
| 会不会检查错版本或报假绿 | SDK、项目配置与相关源码按实际检查身份核对。拒绝预算超限、未知事实和并发取消造成的假绿 |
| 删除后会不会触及在线状态 | 仅在隔离分支和私有测试目录工作。旧状态不会作为此分支验收的输入 |

## 落地复核

- 写前参考实测进入 Codex 和 DeepSeek 的首次及续聊协议。正常问答无源码变更，不生成两项检查。RefGen 只读实测从选中根识别到 7 个登记入口和本地定义目录。
- 浏览器逐轮写入并验证五种结果。原生 Prompt 流两项通过，手写替代流判失败，语义无法证明的写法判未完成，算子错置与跨 flow 引用分别由目录检查拒绝。页面刷新后仍读取同一回合的持久结果。
- 角色归属取实际依赖闭包中的路径，包级算子与谓词同样检查。参数变更定位到对应现行入口和变体。工作台将只读工具传给适配器，工具访问由单一的 `InputAccess` 绑定回合、截止时间和受保护路径。
- 旧状态使用测试数据库实测拒绝接入且保留内容字节。并发取消后晚到的静态结果被丢弃，已有排空事实仍保存。
- 工作台的 Host Surface 元数据仅删去已移除会话组件的引用。七个冻结文件、SDK、调度与计算实现没有改动。
- 本任务提交走隔离分支交付。预览换版与正式发版使用各自的既定流程。

## 旧接线收尾

整项目检查和开发对话的写后检查共享同一个 `FlowChecker`。项目检查枚举注册入口及参数并保留源码证据，展示单一静态结果。写后检查按实际改动选择范围，再独立检查目录职责。退役的 review/behavior 汇总字段、无参数的旧入口后备、特殊检查进程启动路径和旧登记样例已移除。运行准入和封存继续使用各自的权威记录。

写前资料包含 `data.range` 和 `Dataset.count`。静态检查读取 SDK 对 `range` 的原生声明及整型 `id` 字段。执行仍使用同一数据源实现。新建在其他目录的单入口 Python 文件，经同一检查器确认包含原生终端动作后，目录检查提示移到 `flows/`。单独构造 Dataset 的辅助函数不触发这条规则。

## 项目算子与数据工具的公开定义

一个 Python 模块可以同时容纳公开入口与实现细节。作者在模块顶层列出可复用的本地定义，工作台直接读取静态源码。声明与流程接线独立，尚未接线的公开算子同样进入「算子」目录。

| 用途 | 项目内位置 | 模块声明 | 工作台入口 |
| --- | --- | --- | --- |
| 处理一行或一批数据的函数、异步函数或可调用 Actor | `operators/*.py`，支持子目录 | `__demiflow_operators__` | 算子 → 代码算子 |
| 数据浏览、抽样、比较、可视化、人工审核与标注工具 | `data_tools/*.py`，支持子目录 | `__demiflow_data_tools__` | 数据 → 数据工具 |

声明只需一份字符串列表或元组，成员必须是本模块中唯一的公开定义。允许声明空列表。导入其他工具或定义本地辅助函数时无需将其加入列表。每个公开入口用 docstring 写清业务用途、输入粒度与输出；有写入时说明写到哪里、何时发生。函数签名承载参数与默认值，Actor 遵循现有调用与生命周期协议。

```python
# operators/quality.py
__demiflow_operators__ = ("has_review",)

def has_review(row: dict) -> bool:
    """判断一条候选记录是否已有人工审核决定。"""
    return bool(row.get("review_decision"))
```

```python
# data_tools/review.py
__demiflow_data_tools__ = ("review_page",)

def review_page(rows, offset=0, limit=20):
    """返回本页候选的图片标识与审核结论，供 Notebook 查看。"""
    return [(row["image_id"], row.get("review_decision"))
            for row in rows[offset:offset + limit]]
```

Notebook 可显式导入 `from data_tools.review import review_page`。工作台浏览源码不会触发工具的查询或写入。对分页媒体、人工审核等工具，分别提供有界查看入口和显式保存入口，由调用方传入数据版本、分页范围或标注目标。

旧模块暂未声明时，工作台在兼容视图标明登记流程源码中可直接定位的 Dataset 回调。这个引用只能说明该位置使用了定义，覆盖范围不包含动态组装。作者添加声明后，以声明为准。完整源码仍可在开发页的项目目录中查看。
