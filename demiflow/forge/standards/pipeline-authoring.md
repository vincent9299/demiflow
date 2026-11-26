# DemiFlow Pipeline 开发指南

## core

### DF-01

先确认任务范围、入口、已有定义和受影响的调用方，再编写代码。以所选项目中的源码、参数和测试以及当前 SDK 为依据。

### DF-02

项目标准角色目录为 `flows/`、`operators/`、`prompts/`、`configs/`、`data_tools/`、`notebooks/`、`workflows/`。每条 Flow 在 `flows/` 定义一个明确入口并在 `workspace.json` 登记；多管线 DAG 编排声明（`format: "demiflow_workflow_v1"`）放在 `workflows/*.json`（可选在 `workspace.json` 的 `workflows` 列表登记，引用其他项目时只能写已注册项目名或工作区 ID，禁止写绝对路径）；共享代码算子放在 `operators/`；版本化的 Prompt 或 Agent 定义放在 `prompts/`；Flow 运行参数与业务/部署配置统一放在 `configs/`；数据探索与分析 Notebook 放在 `notebooks/`（保留原有输出）。业务数据表与默认运行产物统一放在 `data/`（如 `data/<name>.lance`）；可读运行日志放在 `logs/`；Forge 平台内部状态（`.forge/state.json`、SDK 快照、检查报告、运行账本与锁）统一由 `.forge/` 管理（不进 Git，缺失时按当前环境自动重建）。`workspace.json` 仅保留项目可读清单（`name`、`entries` 及可选 `budgets`、`workflows`）。项目根目录保持整洁，只允许标准业务目录、`data/`、`logs/`、`docs/`、`tests/`、`runs/` 及隐藏目录，不散落非规范顶层目录、业务配置（`*.json`/`*.yaml`）、Notebook（`*.ipynb`）、运行脚本（`*.py`）或压缩归档包；`flows/`、`operators/`、`configs/`、`prompts/` 仅保留活跃入口、已声明复用模块或有效引用文件；废弃非数据文件应直接删除清理，切勿转存或打包（`.tar.gz`/`.zip`）到 `docs/`（`docs/` 仅存放可读文档与验收证据），而业务数据（`data/`、`runs/`）与 Notebook（`notebooks/*.ipynb`）始终保留。项目源码必须**自包含且仅依赖 DemiFlow 公开 SDK**：`flows/`、`operators/`、`data_tools/` 仅允许依赖 Python 标准库、已安装第三方包、DemiFlow 公开 API（如 `demiflow.data`、`demiflow.objects`、`demiflow.operator_llm`、`demiflow.agent`、`demiflow.execution` 公开门面）及本项目自身模块，**严禁导入 `demiflow.execution.<内部子模块>` 等平台内部实现，严禁跨项目 `import` 其他业务项目的源码目录**。

**项目依赖声明（`pyproject.toml` 与 `packages/`）**：项目以根目录 `pyproject.toml` 为唯一依赖声明——`[project].dependencies` 声明安装到项目显式 Python 的 A 类分发包，`[tool.demiflow.packages]` 声明 B 类只读外部源码包（`vendored` 置于 `packages/<包名>`，`local_ref` 指向宿主已授权的外部绝对包目录；仅用 A 类包的新项目可用空 `[tool.demiflow.packages]` 表启用）。B 类包必须具备合法顶层包名和 `__init__.py`，仅接纳合规 `.py` 源码（忽略正常 `__pycache__`，拒绝软链、权重、数据、归档、Notebook 及反向导入项目角色目录或 `demiflow`），受单文件 512 KiB、单包 256 文件 / 8 MiB、单项目 16 包 / 512 文件 / 16 MiB 上限约束。使用 `demiflow forge dep vendor / remove / list / verify / install` 管理依赖，`packages/` 目录由 Forge 守护只读，业务代码按普通 `import <包名>` 导入已声明包。

**参数文件写法（`configs/*.json`）**：直接采用扁平 JSON 键值——输入 Lance 表写 `{"uri": "data/in.lance", "version": 3}`（`version` 可选；项目内写相对路径，外部只读表写绝对路径）；输出 Lance 表写 `{"kind": "output", "uri": "data/out.lance"}`（落在项目 `data/` 下，`mode` 默认 `create`）；本地对象/图片目录直接传规范路径。

**项目运行环境变量自闭环（`.env` / `.env.example`）**：业务运行所需的外部服务密钥与环境变量归属项目空间本身——Prompt 中只通过 `api_key_env` / `base_url_env` 声明变量名；项目根目录维护入库模板 `.env.example`（完整列出所有声明变量名、值为空，严禁填写真实密钥）与本地实际配置文件 `.env`（由 `.gitignore` 排除、不进源码快照与评审材料、不对开发 Agent 开放写权限）。独立运行时按标准加载项目 `.env` 启动；工作台启动作业时按本规范从项目 `.env` 读取并按声明白名单注入执行子进程。

源码目录中的文件和项目声明中已登记的入口分别计数。准备交付的入口显式绑定实际使用的参数。

在模块顶层用 `__demiflow_operators__ = ["normalize"]` 声明可复用的算子函数或可调用的 Actor。尚未接入 Flow 的算子也可声明。用 docstring 说明用途、输入和返回值。

Notebook 使用的可复用代码放在 `data_tools/`（连同配套查看模板资源）。未声明 `__demiflow_data_tools__` 时，模块顶层的公开函数与类默认按 `tool` 自动发现；需要进一步标注细分类别时，可在模块顶层用 `__demiflow_data_tools__` 映射用途，例如 `{"summarize": "analysis", "preview": "view", "review": "annotation", "sample_card": "annotation_component"}`。分析和查看函数返回值、图表或控件。标注入口组合组件，接受用户判断后，按稳定样本标识和数据版本保存结果。目录发现只读取静态声明。项目自写与引入的第三方模块同在「工具」出现；平台发布的通用模块从 `demiflow.data_tools` 导入。

### DF-03

在 Flow 内用原生 Dataset API 串联数据的来源、转换、模型节点和输出。普通函数或 Actor 只承担本地业务逻辑。模型调用和表写入在入口显式编排。跨 Flow 共用逻辑提取为算子，独立入口之间用明确的持久交付引用衔接。

**算子自包含与语义充血规范（`DF-02` 静态门禁强制校验，MECE 三维七条）**：
1. **模块分层（Layering）**
   - **`OP-L1` 公开业务算子模块互不依赖**：`operators/<domain>.py` 之间严禁互相导入（含相对导入、项目前缀导入与函数体内延迟导入），杜绝“半垫片”多代套娃；若多条 Flow 共用同一业务算子，直接在 Flow 中引用该算子并通过 `fn_kwargs` 传参。
   - **`OP-L2` 私有底层工具模块纯叶节点**：无业务协议依赖的纯底层通用工具（编解码、序列化、hash、确定性随机、原子写等）按能力命名为 `operators/_<util>.py`，须同时满足：① 严禁与公开模块 1:1 同名影子镜像（存在 `foo.py` 时不得建 `_foo.py`）；② 私有模块为纯叶节点，严禁导入任何其他 `operators/` 模块（无论公开或私有）；③ Flow 严禁直接导入 `operators/_*.py`（需在 Flow 编排的节点必须定义在公开模块中）。
2. **入口归属（Ownership）**
   - **`OP-O1` 禁止无逻辑转发壳**：严禁编写单行转调函数（`def f(row): return other.f(row, k=1)`）或赋值别名重导出（`f = other.f`），无论目标为公开或私有模块。
   - **`OP-O2` 禁止导入重导出算子**：公开算子必须直接定义在公开模块中，严禁通过 `from ._<util> import fn` 将私有模块中的函数/类重导出作为公开算子入口（私有工具函数只允许在公开模块内部被调用）。
3. **语义自描述（Semantics）**
   - **`OP-S1` 强制首行语义描述**：每个公开代码算子（非 `_` 开头的顶层函数/Actor 类）与 Prompt 定义必须写清一行中文业务语义描述（首行 docstring / `description`），直接作为工作台执行图节点卡片说明展示。
   - **`OP-P1` 强制参数语义说明与一致性**：公开代码算子除首个数据行形参（如 `row`）外，每个业务参数（`fn_kwargs` / Actor `__init__` 参数）必须在 docstring 的 `Args:`（或 `参数:`）段逐个写清中文含义，且尽量使用扁平、表意的参数名（避免 `options`/`config`/`data` 等无语义黑盒字典）；参数增删改名后须同步更新，严禁残留已不存在的过期参数说明。
   - **`OP-E1` 强制最小输入输出变换示例与一致性**：每个公开代码算子（顶层函数或可调用 Actor 类）必须在 docstring 的 `Example:`（或 `示例:`）段给出 1 条最小「`输入: ...`」「`输出: ...`」对比样例（仅列出关键变化字段，兼容行变换字典、展开列表或过滤布尔值）；代码产出字段变更后须同步更新示例，严禁残留已删除的过期输出字段或与当前产出字段完全脱节。

Flow 显式选择本次使用的上游产物、Prompt 和业务要求的版本，将相应引用或要求传给算子。可复用算子声明自己支持的输入格式，校验调用方传入的要求。仅消费上游结果时，依据上游交付的数据和运行记录核对来源。

### DF-05

模型、模板输入和输出约束归原生 Prompt 或 Agent 定义所有。调用 Flow 显式引用具名定义。业务参数、算子参数和部署配置按其实际作用域传递。

### 建议与示例

最小接线示例：从 `demiflow.data` 导入 `from_items`，在 Flow 入口返回 `from_items([{'id': 1}]).count()`。需要筛选时把业务判断写成普通谓词，在 Flow 中用 Dataset.filter 接入。具体签名取自本次 SDK 查询。

现有项目先核对入口职责、参数、实际输入输出及相关测试。改动公开契约时同步维护已有说明。需求与平台约束冲突时说明冲突点。遇到当前 SDK 缺失的能力，记录所缺能力及证据。平台工程契约的 FE-01 至 FE-07 给出编排、职责、资源与交付依据。

## flow

### DF-04

标明输入的固定版本、业务范围、一行的含义、主键及输出。区分业务拒绝、合法空结果、技术失败和已提交的结果。运行用到的参数和数据来源必须能从原生入口及绑定追溯。

### DF-07

分别验证静态接线与业务行为。明确本次实际检查的入口、参数变体、源码和仍待核实的范围。只有取得实际执行回执后才能报告运行及输出版本。

### 建议与示例

先用很小的确定性样本写出预期结果。比如核对 10 行输入经筛选得到 5 行，并加上空输入用例。对共享算子同时检查使用它的其他入口。大数据路径优先让 Dataset 直接读写表，估算收集到驱动进程或重复物化的成本。引擎的读写及恢复保证以实际契约和回执为准。

多源关联时逐个绑定来源、版本和键。允许使用最新版本时，在本轮开始时解析一次并记录确定的引用。写出前核对 schema、允许空值、更新模式与冲突策略。合法空输出也有明确 schema。区分追加历史与本轮交付，在目标粒度验证重复键和一对多放大。

CLI、可选 Notebook 和后台使用同一入口、同一份实际参数。参数写清单位、默认值和上限。保留 Notebook 中已有的编辑和输出。面向观察取有限摘要，进度来自实际事件和提交，无需额外全表扫描。修改路径前检查已知调用者及存量运行。

复用时核对实际输入、影响内容的参数、代码、模型和部署版本。单纯调度变化与内容变更分别验证。历史记录缺必要信息时说明不能确认的范围。中断后根据原生 checkpoint 和已提交结果说明保留工作与重跑边界。用隔离数据复现原故障，分别报告静态检查、行为测试与实际运行证据。

## operator

### DF-06

异步并发、资源、失败与清理由原生节点协议表达。有状态算子的初始化、消费和关闭要符合 Actor 生命周期；错误处理保留真实失败原因。普通算子不自建执行器、调度器或表级提交协议。

### 建议与示例

共享纯函数可像 `def is_even(row): return row["id"] % 2 == 0` 一样只检查当前行。先核对输入缺失字段、空值、重复键等边界。状态需跨批次或重试保持时，先查询当前 SDK 支持的恢复保证，再用中断和重入样例验证。预算应考虑同时驻留的副本、队列和单行大小。用超大单行、热点组和高输出放大验证背压、取消、资源释放。必要的有效性检查放在真正使用输入的位置。

数据集的关联、汇总、全表写出由 Dataset 表达。以函数输入输出声明一行或一组的契约。使用运行时提供的进程、服务、并发及版本冲突处理。显式设置重试边界，技术错误保留原因。用于业务拒绝的判断与技术失败分别测试。

## prompt

### 建议与示例

应用核心条款 DF-05 时，核对实际引用的定义、必要字段、渲染输入与目标业务样例。输出字段更新时同步核对解析、存储和实际消费者。静态格式检查与真实模型效果分别验证。

对评测类 Prompt，可选一对容易混淆的正反例，说明证据不足时如何输出。只传判断所需的字段，避免把待评测的答案放入输入。结构化字段留给代码校验，模型专注于需要判断的部分。用小样本查看实际渲染消息和输出解析。真实模型调用按用户授权和预算执行。

固定说明与每条变化的输入分别组织，写明判断条件、使用的证据和输出边界。检查原生渲染产生的真实消息、变量缺失行为及后端长度限制。区分固定前缀缓存与响应复用，分别统计模型请求数、输出行数和节点预算。在无待办输入的样例中核对请求数为零。

## workflow

### 建议与示例

多管线编排使用 `workflows/<name>.json`。顶层必填 `format`、`name`、`nodes`，可选 `description`、`projects`、`inputs`、`concurrency_groups`、`policy`。每个节点填写唯一的 `id` 和已登记的 `entry`，还可以填写 `title`、`description`、`project`、`params`、`overrides`、`after` 和 `concurrency_group`。同一入口绑定多个参数文件时要写明选用的项目相对路径。两个节点可以复用同一入口。节点省略 `project` 或显式填写 `"project":"self"` 都引用本项目。`self` 是内置别名，`projects` 仅声明外部项目。

```json
{
  "format": "demiflow_workflow_v1",
  "name": "生成并评测",
  "projects": {"evaluation": "eval-project"},
  "inputs": {"scale": {"type": "integer", "default": 2}},
  "concurrency_groups": {"gpu": 1},
  "policy": {"max_parallel": 2, "on_failure": "fail_fast"},
  "nodes": [
    {"id": "generate", "entry": "flows/generate.py:run", "params": "configs/generate.json",
     "overrides": {"scale": {"$input": "scale"}}},
    {"id": "evaluate", "project": "evaluation", "entry": "flows/evaluate.py:run",
     "params": "configs/evaluate.json", "concurrency_group": "gpu",
     "overrides": {"input_table": {"$from": "generate.outputs.output_table"}}}
  ]
}
```

`projects` 将别名映射到宿主登记的逻辑项目名或工作区 ID。省略节点的 `project` 表示当前项目。跨项目前在工作台开发回合调用 `list_registered_projects`，核对目标和入口。独立 Forge 检查本地声明时会列出待宿主核实的外部项目。输入声明支持 `type`、`required`、`default`、`description`，类型可选 string、integer、number、boolean、object、array、any。用 `{"$input":"scale"}` 引用已声明输入，这个对象只放一个键。

`after` 是上游节点 ID 数组，用于表达控制顺序。`$from` 自动形成数据依赖。表引用为 `node.outputs.name` 或追加 `uri`、`version`、`row_count`、`schema_hash`、`dataset_id` 字段。模型文件引用为 `train.models.default.files.adapter.safetensors.uri`，指定步骤可写 `train.models.default.steps.1000.files.adapter.safetensors.sha256`，文件字段支持 `uri`、`sha256`、`size_bytes`。模型本身可取 `manifest_uri`、`manifest_digest`、`final_step`、`instance_id`、`fit_key`。并发组限制组内同时运行数，`policy.max_parallel` 限制整个编排。`on_failure` 取字符串 `fail_fast` 或 `continue_independent`。

| 规则 | 触发示例及核验阶段 |
|---|---|
| WF-01 | 声明字段或类型无效，例如把失败策略写成数组 |
| WF-02 | 节点依赖自身或形成环 |
| WF-03 | 引用了未声明输入、未知节点或项目别名 |
| WF-04 | 目标入口未登记，或参数文件未绑定到该入口 |
| WF-05 | 宿主实际提交前核验参与项目的运行权限 |
| WF-06 | 调度时解析上游已提交的表或模型产物，核实名称、版本和文件 |
| WF-07 | 声明路径或项目引用非法，例如用宿主绝对路径指代外部项目 |

使用 `demiflow forge check --root . --path workflows/demo.json` 检查。独立终端用 `demiflow forge add workflow --root . --register --path workflows/demo.json` 登记。工作台开发回合使用宿主的 `manage_project_layout`，操作为 `register_workflow`。
