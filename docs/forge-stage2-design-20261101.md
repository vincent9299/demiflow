# Forge 第二阶段架构与方案设计

日期：2026-11-01。状态：原提案已暂停，未实现。后续安排以 [工作区与数据加工阶段规划](forge-roadmap-20261101.md) 为准；当前 [第二阶段接线契约与工作区方案](forge-stage2-contract-design-20261102.md) 待 Review。

本文保留原提案与代码事实供追溯；“一个项目一个入口”的开发约束不再作为实施依据。新方向允许同一工作区容纳多张表、多种组合和多次实验，并将接线标准、基础连接图及 coding agent 使用纳入交付。

第二阶段让业务项目直接使用原生 Python 和 Dataset 组合流程，Forge 从实际代码派生项目清单。业务增加 map、join、分支、模型阶段或输出，不需要修改 Forge 的节点契约。

第一阶段已提供动态平台目录与内容快照。本阶段接通“业务需求 → 原生实现 → 派生 Manifest”，第三阶段再接通 Verify、Review、Freeze 和受管运行。

## 1 用户可用结果

| 用户操作 | 第二阶段结果 |
| --- | --- |
| 根据业务需求新建项目 | 获得原生 `pipeline/` 包、普通业务函数、配置与测试骨架，不生成固定三节点 Definition |
| 接入已有原生 Pipeline | 检查入口和目录，生成 Forge 控制文件；不改写业务流程 |
| 增加第二个 map、join、分支、展开或多个输出 | 修改原生代码，重新生成 Manifest；无需改 Forge 核心 |
| 使用 prompt / agent 节点 | 记录实际 API 引用、配置文件身份和检查缺口，保留原生格式 |
| 查看 Manifest | 看到入口、源码位置、平台 API、配置、需求与平台绑定、未解析项 |
| 请求新格式的受管执行 | 明确提示第三阶段尚未接入；不能借旧 v1 或普通 CLI 绕过 Forge 门禁 |

本阶段交付“可开发、可查看、可做结构检查”的原生项目。结构检查成功不代表业务正确、预算可执行或允许生产运行。

## 2 已核对的原生基础与缺口

| 当前代码 | 可以复用什么 | 必须正视的限制 |
| --- | --- | --- |
| `pipeline/operator.py` | 已有唯一 `PipelineProgram`、`run(self, ctx)`、执行声明、显式终结动作 | 不应再造 `ForgePipeline` 或第二套业务入口 |
| `pipeline/discovery.py` | 通过 AST 发现入口和 execution，候选代码不需要被 import | 当前读取和遍历需由有界输入副本约束；不把它当作资源沙箱 |
| `pipeline/core.py` | 原生 Pipeline 绑定 executor，调用 run，要求返回 None | 受管执行和权限接线留到第三阶段 |
| `execution/pipeline_sources.py` | 包内相对 import 的静态闭包 | 只跟踪部分静态 import，不能宣称覆盖动态 import、反射或所有调用关系 |
| `data/dataset.py` | map、flat_map、filter、join、union、checkpoint、模型算子和 sink | 异步模型节点之后的 checkpoint、后端和重复执行约束仍以原生契约为准 |
| `operator_llm/parser.py` | 原生 Prompt Pack 解析；配置资源按 pipeline 顶层 basename 解析 | 引用扫描目前识别特定模型方法，不能直接当作通用业务 Manifest |
| `agent.py`、`operator_api.py` | 完整 Agent 配置与普通 fn/actor 契约 | `load_agent_config` 会解析并 import 自定义工具，不能在只读业务检查中直接调用 |
| `execution/contracts.py` | bundle 内容身份与已有执行契约 | `PlatformRuntimeIdentity` 还存在 `distribution == 'demiurge'` 的历史约束，第三阶段必须清理实际独立运行入口；本阶段不加伪造身份或临时垫片 |

这些事实决定：复用原生入口与静态发现，Manifest 保持事实清单；完整配置语义、实际运行身份和执行授权不能提前宣称完成。

## 3 架构与职责

```text
用户确认的 request.json ────────────→ 需求身份
当前工程标准 + platform snapshot ─→ 平台与标准身份
pipeline/ + tests/ ─→ 有界内容副本 ─→ 原生入口发现
                                      ↓
                             静态引用与资源事实
                                      ↓
                            派生 Manifest + 诊断

业务执行：原生 Pipeline → 现有 Dataset executor（第三阶段接门禁）
```

| 内容 | 所有者 | 变更方式 |
| --- | --- | --- |
| 需求、分母、完成条件与验收标准 | 用户 / 需求方 | 单独确认需求变更；候选代码不能自行放宽验收条件 |
| 原生代码、配置、业务测试 | 业务开发者 | 正常修改，自动产生新的实现身份 |
| 工程标准、平台 API | 平台 | 独立版本；业务变化不修改平台方法清单 |
| project binding、平台快照、Manifest | Forge 生成 | 明确创建或重新绑定；不手改派生字段获得通过 |
| 测试、Review、Freeze 与运行证据 | 后续生命周期 | 第三阶段绑定以上全部身份 |

不新增 operator 注册装饰器、业务基类、执行节点表或策略插件框架。普通数据函数与 actor 保留现有接口。单一 PipelineProgram 是已有原生入口，不向每个业务算子传染继承要求。

## 4 项目结构与入口

```text
project.json                  # demiflow_forge_project_v2；格式和控制绑定
request.json                  # demiflow_forge_request_v2；用户需求
platform.snapshot.json        # 第一阶段格式的完整平台快照
manifest.json                 # demiflow_forge_manifest_v2；派生产物
pipeline/
  __init__.py
  main.py                     # 唯一原生 PipelineProgram
  operators/
    __init__.py
    transform.py              # 普通 fn / actor；按实际需要拆文件
  business.json               # 业务配置；资源使用现有原生读取机制
  generate.yaml               # 需要时新增原生 Prompt Pack
  review.yaml                 # 独立阶段使用自己的原生配置
tests/
  test_behavior.py
README.md
```

`pipeline/main.py` 为脚手架默认名称；入口由原生发现得出，允许其他合法模块名。project.json 不再维护另一份手填 entrypoint。

配置保持原生资源布局：Prompt Pack、Agent 配置和 `ctx.resources` 文件目前采用 `pipeline/` 顶层文件名。本阶段不增加 `prompts/` 寻址垫片。仅在实际需要时生成配置，不制造空目录与占位框架。

业务实现的示意形态：

```python
from demiflow import PipelineProgram, data
from .operators.transform import normalize, keep

class Main(PipelineProgram):
    execution = {"mode": "portable"}

    def run(self, ctx):
        source = data.read_lance("datasets/source.lance", version=1)
        metadata = data.read_lance("datasets/metadata.lance", version=1)
        joined = source.map(normalize).join(metadata, on="id")
        joined.filter(keep).write_lance("datasets/result.lance", mode="overwrite")
```

这是入口与组合形式示意，数据路径及版本需由实际项目需求确认，不是生产执行配置。`ctx.data` 的已有原生写法同样保留；Forge 只定位其公开 reader 对应关系，不封装或改写参数。

## 5 需求与实现分离

新 request 只描述业务：原始意图、目标、数据粒度、输入范围与固定身份要求、输出及分母、完成条件、验收条目、资源与外部交互约束、未决问题。输入和输出支持多个命名对象。

不再要求 `map.name`、单个 operator config 或预先选择 nodes。用户需要明确指定业务阶段时，可以写入验收要求；实现仍由 Python 表达。需求中的业务字段 schema 与原生平台 API 参数 schema 是不同对象，不复制平台签名为业务配置。

每项验收有稳定 ID，测试与 Review 后续引用它。Manifest 记录 request 摘要及验收条目引用，但不能从实现自动生成“已满足”的需求。request 改动返回 `requirements_changed`，不会仅因重算 Manifest 就获得需求批准。

## 6 派生 Manifest 的内容和保证

### 6.1 内容

| 字段组 | 来源 | 用途 |
| --- | --- | --- |
| format、binding | 项目版本、标准、平台快照、需求身份 | 确定本项目检查依据 |
| entrypoint、execution | 原生入口发现 | 唯一入口和 backend affinity；没有重复运行配置 |
| source_inventory | pipeline、配置及测试文件内容 | 任何受管实现变化产生新身份 |
| references | 有界 AST、公开平台目录、静态本地引用 | 接口 ID、调用位置、实参表达式、业务 callable 来源 |
| resources | 可静态定位的配置与资源文件 | 文件路径、内容摘要、声明格式、关联源码位置 |
| inspection | 已覆盖范围、未解析项、外部依赖及诊断 | 区分事实、声明与未知 |
| digests | 需求、实现、平台、标准和 Manifest | 第三阶段的证据绑定输入 |

不存储可供 Forge 执行的另一份 nodes/edges。源码中的分支、循环、helper 和多个 sink 保持 Python 语义；Manifest 不是执行计划。

### 6.2 静态识别边界

支持显式 import 与别名、包内相对 import、普通函数/actor 引用、可追踪 Dataset 变量及调用链、字面量或有明确来源的配置文件引用。允许多次调用同一接口，保留每个调用位置，不把它们按 API ID 合并成一个业务节点。

API 方法名与签名从第一阶段目录读取；新增原生公开方法无需改 Forge 的方法枚举。对返回类型不能确认的表达式、动态工厂、反射、动态 import 或条件下不同来源，记录候选与 unresolved，不猜测成 Dataset 调用或纯计算。

静态引用可说明“代码可能调用什么”，不能证明某个分支实际执行、循环次数、输出行数、全部副作用或业务完成。本阶段不尝试从任意 Python 推导完整 DAG。

包内全部受管源码参与摘要，而不只哈希已识别的可达文件。原生闭包函数的覆盖缺口需要诊断，不能据此漏掉身份。超出可解释范围的项目可导出带诊断的 Manifest，但结构检查必须明确 incomplete，不能把 unresolved 计作通过。

### 6.3 模型与配置

模型请求继续写成原生 `map_prompt*` / `agentmap_async`，保留单次完成与行内交互的区别。配置文件使用原生 `demiflow_prompt_pack_v2` / `demiflow_agent_v2`，不另外定义 Forge 模型节点 schema。

Manifest 阶段只读配置身份与声明，不 import 候选工具、不装配 actor、不调用模型。`load_agent_config` 的自定义工具解析会触发 import，不能直接当作只读检查器。需要完整静态配置语义时，由原生配置模块提供纯检查边界，第三阶段按具体设计补齐；不在 Forge 复制一套 Agent 校验器。

普通 operator 中隐藏模型调用仍违反工程标准。本阶段记录本地函数和引用，能直接识别的违规产生诊断；不能承诺静态扫描已证明所有 callback 无模型副作用。完整门禁、测试和 Review 在第三阶段完成。

## 7 命令和格式边界

| 命令 | 拟定行为 |
| --- | --- |
| `forge init REQUEST --out PROJECT` | 创建 native-v2 项目，生成新控制绑定与当前平台快照 |
| `forge adopt PROJECT --request REQUEST` | 检查已有原生项目；仅新建缺少的 Forge 控制文件，不覆盖代码或旧证据 |
| `forge manifest PROJECT` | 按项目格式明确分派；v2 派生事实清单及诊断 |
| `forge check PROJECT` | v2 检查布局、入口、身份、引用和 Manifest 新鲜度，输出 coverage |
| `forge platform ...` | 继续使用第一阶段命令 |

v1 `new` 和其完整生命周期保留原语义。v2 暂不接 `verify/review/freeze/run`，这些入口必须明确拒绝 v2 并说明尚未实现，不能回落到 v1 Runner。按已知项目格式做显式分派即可，不构建通用版本插件层。

旧项目迁移采用新目标目录：复用原 request 中业务需求与普通函数，人工把固定 Definition 转为原生入口，重新生成平台绑定与 Manifest。原来的 Review、Freeze、运行记录保留历史，不迁移为新版本认证。本阶段先给迁移说明与示例，不自动翻译任意旧业务或修改原项目。

## 8 有限资源与输入纯净性

在调用原生 AST 发现之前，先对项目做有界扫描和内容副本；原生函数只读取本次稳定副本。符号链接、特殊文件、路径逃逸和生成期间变化直接拒绝。候选源码不 import，不执行 `run`、函数默认值、装饰器或配置里的自定义工具。

拟定初值：源码及配置目录项最多 1,024；单文件 2 MiB（Agent 原生 1 MiB 上限仍适用）；总内容 32 MiB；目录深度 16；AST 每文件最多 50,000 节点、总计 200,000；静态模块/调用解析工作队列最多 4,096 项；Manifest 最多 8 MiB；检查进程 60 秒、并发 1。超限返回 incomplete，不扩大预算或截断后标成功。

AST 解析本身的分配发生在节点计数之前，因此还需要单文件准入及独立进程；这些预算不是 RSS 硬上限。格式、诊断和分页复用第一阶段思路，不通用化成额外资源框架。

## 9 实现分解与范围

| 顺序 | 工作 | 交付 |
| --- | --- | --- |
| 1 | 定义 v2 project/request/manifest 结构与职责 | 不含固定拓扑的 schema、错误与覆盖语义 |
| 2 | 有界项目内容扫描与原生入口复用 | 稳定输入、原生入口、内容身份 |
| 3 | 公开 API 与资源引用提取 | 可解释事实和 unresolved；不执行业务 |
| 4 | init/adopt、Manifest 生成、check 及显式版本分派 | 用户可用的工程闭环 |
| 5 | 多形态原生示例与迁移说明 | 组合能力和旧格式隔离的实际验收 |

主要改动放在 `forge/` 的新 v2 项目、检查和 Manifest 模块，以及现有 CLI 的小型格式分派。原生平台代码仅在证据确认必要时做单一职责修正，先列明影响；不改 Dataset 执行器、模型客户端、服务或规划器来迁就 Forge。

## 10 验收矩阵

1. 单 map、多 map、两输入 join、filter/flat_map、分支与多个 sink 使用相同 Forge 核心生成 Manifest。
2. 原生平台 fixture 新增公开 Dataset 方法后，业务引用可被识别，无需增加 Forge 方法清单。
3. 同名普通对象的方法不误识别成 Dataset API；别名、继承与多次调用保留准确来源位置。
4. 多个入口、越界资源、缺失引用、动态导入、不可解释来源及超限分别给出具体诊断，无法确认的不通过。
5. 带顶层副作用探针和自定义 Agent 工具的候选项目，Manifest 与 check 均不执行候选代码。
6. 需求、业务代码、测试、模型配置、资源、平台私有实现任一变化，都使相应绑定或 Manifest 失效；手改 Manifest 无效。
7. 模型多阶段示例使用假响应/离线 fixture；识别配置及缺口，不发送真实请求，不启动服务。
8. 纯本地原生示例通过现有 Dataset 测试入口跑出实际预期结果，作为组合能力证据；不声称已有 Forge v2 受管执行。
9. v1 回归继续通过；v2 进入旧生命周期明确拒绝；旧项目与旧证据内容不变。
10. 独立安装后的初始化、接入、检查、清单生成和错误退出码一致。

## 11 本次 Review 的关键决策

1. **复用已有 PipelineProgram，业务直接写原生 Python / Dataset。** 不再设计新的入口或执行 DSL。
2. **需求约定结果和验收，代码决定实现组合。** 固定三节点与唯一 operator 从 v2 业务契约中移除。
3. **Manifest 是代码事实清单。** 不代替源码、不作为执行器输入，未知必须可见。
4. **纯静态接入先完成。** Agent 解析会 import 工具、历史 runtime 仍绑定 demiurge 等真实缺口明确留给第三阶段，不垫适配层掩盖。
5. **完整运行门禁分阶段接通。** 第二阶段完成后再次提交第三阶段设计；本设计通过不直接授权模型或生产执行。
