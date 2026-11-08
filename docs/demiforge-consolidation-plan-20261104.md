# DemiForge 收敛到 demiflow：依赖清单与最小迁移方案

2026-11-04。状态：**供主会话 Review，未实施。** 只迁入当前在用的 DemiForge 开发工具与其真实依赖；不搬 DemiOrch、Candidate/Role/Profile、调度、沙箱或整套 Demiurge。此执行包不 push、不删除目录、不修改业务登记、Web、实现或现行标准。GitHub 归档及删除前置核验由其他会话负责。

## 1. 当前依赖结论与盘点边界

在已核对范围中，当前工作区对本地 `Demiurge/` 的明确活动依赖是 **DemiForge 开发入口、工具源码、标准与 Skill/文档**。没有发现活动业务源码从外部导入 DemiOrch/Candidate 实现的证据；这不是对所有未扫描目录和运行进程的全局证明。

盘点采用 Git 索引中的当前存在源文件，并补读 demiflow 当前未跟踪 Forge 源码/测试/文档、harness，以及两个新业务项目源目录。共读取 **3,898 个文件、37,927,236 字节**，单文件最多 1 MiB、总量上限 64 MiB；不读 Notebook 输出、表、运行历史、模型、共享环境内容。索引中 357 个不存在/符号链接路径未读取，12 个大文件未读取，不能当成额外活动消费者。训练目录的 Git 索引定点查询没有可读的现存 Python/shell 项，因此**训练当前未跟踪源码是否另有依赖仍未确认**。`.codex-worktrees/`、`.worktrees/` 及所有隐藏副本也未作全量扫描，属于删除前清单的剩余核验范围。

两次递归路径枚举在 35/30 秒预算内未完成，未作为完整扫描证据；最终使用上述索引与明确源目录清单。成功盘点的原始路径/哈希、未读项及逐行引用保存在 `/tmp/demiforge-consolidation-indexed-20261104-z19buuyg/` 的 `source-inventory.json`、`references.json`。以下结论按证据强度分开。

| 消费者/入口 | 已确认事实 | 迁移处理 |
| --- | --- | --- |
| `demiwtg/.demiforge/forge:7–11` | 加入 `config.tool_root` 后导入 `demiurge.demiforge.development.cli.main`。 | 改为正常导入新归属的同一实现；保留薄项目入口，不保留 Demiurge 软链接/副本。 |
| `demiwtg/.demiforge/project.json` | v2，`tool_root: ../Demiurge`；21 个 pipeline，其中 19 个 candidate.5、2 个 candidate.9；26 个 sources、165 个 watch 项。readonly 的 6 项均指向 demiflow。 | 后续授权的消费方包只更改工具定位及必要当前引用；schema、逐项标准、verify、watch、readonly 和任务基线保留。不能一并升级 19 个旧 pipeline。 |
| `demiwtg-bagel-review-20261016/.demiforge/{forge,project.json}` | 同一旧 import；v1，tool_root 指向 `../Demiurge`；23 个 candidate.5 登记，20 个 sources；readonly 也是 demiflow 6 项。 | 这是存在的可执行旧入口；该 worktree 是否继续使用未确认。删除前明确迁移或历史只读状态，不能只改主 checkout。 |
| 根 `AGENTS.md:7–12` | 指向 Demiurge canonical Skill 和 quickstart，并要求项目 launcher。 | 更新当前路由到 demiflow 的唯一权威位置；保留业务开发不授权平台修改的边界。 |
| `.codex-home/skills/demiforge` | 确认软链接到 `Demiurge/integrations/codex/demiforge`。 | 在新 Skill 就绪后改为新权威位置/受验证安装产物；删除旧仓前核对没有悬空链接。 |
| `demiwtg/AGENTS.md:53–63` | 当前开发入口、Skill、工具说明链接。 | 更新当前入口和 quickstart 路径；历史决定块、旧任务记录不批量替换。 |
| `demiwtg/PIPELINE_SPEC.md:65,110,336,445` | 当前规范分别链接 candidate.9 布局、candidate.8 性能、candidate.5 模型边界、candidate.3 结果浏览器。 | 仅把权威链接改到新归属的**同版本原文**，保留版本、章节与规则内容。 |
| `demiwtg/pipelines/common_concepts/README.md:57` | 当前 README 链接 candidate.8 原文；两个新项目 README 也描述 candidate.9 和旧范围阻塞。 | 当前链接更新；保留旧越界/未接受事实及 reviews 原证据。 |
| demiflow `project_discovery.py:174,184` 与针对性测试 | 读取 `.demiforge/project.json` 和 v1/v2 公共 JSON；没有导入 Demiurge 工具。 | 格式兼容照旧。开发命令虽迁入同一产品，识别器仍只解释公开声明，不从中执行命令或授予通过/运行权限。 |
| `demi-forge-harness/client.js:1549,3636` | `DemiForgePanel` 是客户端组件名称；未发现指向本地 Demiurge 的实现 import/路径。 | 不是迁入旧 Candidate 体系的理由；不为本迁移改 Web/harness。 |
| `_demiforge/orchestration_20261104` | 定点查看 start/verification 摘要，task 为 `common-concepts-performance-20261104`，是业务开发验证证据。 | 不是 DemiOrch 在用的证明；原样保留，不作为运行时包迁移。 |

实际项目配置中检查了所有字符串值，当前指向 Demiurge 的机器引用只有 `tool_root`，未发现 `verify argv/env` 使用 `{tool}` 或额外 Demiurge 源路径；一般工具的 `{tool}` 展开仍必须在迁移中正确处理。部分当前 `sources` 指向业务 docs/archive，这是仍被显式要求的上下文，不能因目录名 archive 就删掉、改内容或省略覆盖。

### 名称兼容与真实依赖分开

| 现有 demiflow 代码 | 实际语义 | 本迁移边界 |
| --- | --- | --- |
| `config.py:95–118`、`execution/contracts.py:85–95` | 校验历史 runtime wheel 的 distribution `demiurge` 和 wheel 内 `demiurge/demiflow/...` ABI 文件。 | 这是输入制品格式兼容，不是从本地 Demiurge 源目录 import。保留，不因统一产品就替换成新包名。若仍有现役这类运行，其**具体 wheel 文件**是否位于待删目录需另行定点确认。 |
| `execution/ray.py:335,368,385`、`package_loader.py:29` | 历史 runtime/bundle 名称及私有执行 namespace。 | 保留冻结制品与回放语义，不为迁移开发工具而重写 bundle 或启动 Ray。 |
| `pipeline/discovery.py:241` | AST 识别旧 `from demiurge.demiflow import PipelineProgram` 的声明。 | 静态旧协议识别，不是导入该旧外部包；保留。 |
| `_compat/observability`、`data/observability.py:8,13` | 实现已在 demiflow 内；`_demiurge_fields` 和 logger 名是既有日志约定。 | 无外部源码依赖；不改历史字段/日志身份。 |
| `.demiforge` 格式名、Notebook metadata、reviews/冻结记录中的旧路径 | 开发协议或历史事实。 | 不全局改名、不重写输出，不把字符串命中计为需要迁移整个旧框架。 |

## 2. 真实最小源码闭包

对 development 全部 Python 文件的 AST import 已核对：外部依赖都是 stdlib；唯一跨出 development 的实现依赖是 `demiforge/atomic_io.py`。没有到 Candidate、DemiOrch 或旧角色执行器的 import 边。

| 当前来源 | 新归属建议 | 说明 |
| --- | --- | --- |
| `development/{project,checks,tasks,cli,runner,notebook,__init__}.py`，7 文件 | `demiflow/forge/development/` | 保留现有任务、上下文、范围检查、注册 fixture 命令执行、Notebook 投影与验收规则；runner 仍只跑显式可信验证命令，不成为 pipeline 执行器。 |
| `demiforge/atomic_io.py`，917 字节 | 优先复用现有 `demiflow.forge.io.atomic_write_text` 的唯一落盘实现 | 旧 helper 的 JSON `default=str`/换行和新 helper 的 `allow_nan=False`/末尾换行不同。不能未经核对直接当成等价函数；可由 dev 层保留必要序列化策略，调用共用原子文本写入，不复制第二套 fsync/replace。 |
| `demiforge/__main__.py`、`bin/demiforge`、Skill `scripts/forge.py` | 新模块薄启动/公共 CLI 适配及项目薄 launcher | 三者只负责定位和传参；不各保留业务命令实现。旧 bin 与旧 Python 包不作为第二套受维护入口。 |
| `demiforge/standards/` 的 9 份 Markdown | 新 dev 包的 `standards/` | candidate.1 的原文件名是 `pipeline-v1.md`，其余 candidate.2–9；版本映射和文件全文逐字节保留。 |
| canonical `integrations/codex/demiforge/SKILL.md` 及必要启动脚本 | 新 dev 包的 `skill/`，作为唯一维护源 | 对应同一 dev 工具，不混入旧 Candidate/Role Skill。更新当前位置链接与命令说明，历史标准全文不改。 |
| `tests/test_development*.py` | demiflow 中明确命名的 dev 测试 | 当前 28+15+10，共 53 个测试方法；需适配 import/资源/launcher 路径，**本盘点没有执行并宣称其通过**。另加迁移、安装布局及轻依赖测试。 |
| quickstart、业务开发定位、项目配置示例、已接纳源码覆盖设计 | demiflow 当前开发文档/示例 | 迁入仍在用的说明，不复制旧框架 README、冻结规格、运行资产为新产品能力。 |

建议以已审查源码 SHA 为迁移输入（清单 `core-inputs.json`）；先保持行为及历史版本资源，再在**新归属**修 F-01/F-02。旧仓不再接收该修复实现。F-01/F-02 的设计与跨层审查作为迁移包的依据迁到 demiflow 文档，不能让修复只留在将删除目录中的一份文件。

## 3. 一个产品入口与轻依赖边界

建议公共入口为：

```text
demiflow forge dev --project /absolute/business start|list|context|check|verify|review|handoff ...
```

已用现有环境实际读取 Click 命令注册表，`forge` 有 18 个命令，**没有 `dev` 冲突**。原 `forge check/verify/review/...` 是旧 v1；`workspace/flow/run` 属平台检查/执行。dev 子命令避免覆盖它们，仍使用同一产品 CLI。适配层只把参数交给迁入后的现有 argparse/main/execute，复用一套任务用例和退出码；不重新维护 11 套业务命令的分支。

项目 `.demiforge/forge` 可保留：读取既有 tool_root、用正常 Python 包导入新 `demiflow.forge.development.cli.main`，只补 `--project` 并传递原参数。不得指向 Demiurge、创建旧目录软链、复制 core，或在失败时回退旧实现。

**轻依赖是实际迁移问题。** 现有 `/bin/python3` 为 3.10.12，无 pyarrow/Click；把 demiflow checkout 加到正常 sys.path 后 `import demiflow` 实测先因缺少 `yaml` 失败。`demiflow/__init__.py:3–34` 会提前导入 data/pipeline/execution。原 DemiForge launcher 的 stdlib 能力不能因移动文件便视为已保留。现有业务环境 `/yzp/zhaozy/yangzepeng/0905/env/bin/python` 可从 `/tmp` 正常定位 demiflow 源码，但该环境没有 `bin/demiflow` console script，也不能假设裸 `demiflow` 命令已经部署。

**推荐显式解决父包初始化：** 用正常 Python 的公开延迟导出机制，将 demiflow 根包的重型导入推迟到实际使用对应 API 时；保持既有 `__all__`、公开对象身份及可发现性并回归验证。dev core、资源读取、原子 I/O 保持 stdlib；`forge/__init__.py` 当前静态 definition/io/schema 闭包可保持轻依赖，但必须测。薄 launcher 因而可正常导入同一新模块，不需要伪造 `sys.modules`、绕过父包、spec-loader、临时 namespace 包或私有旧树。

公共 Click CLI 是已安装 demiflow 的产品入口；它自身的依赖与原项目 stdlib launcher 要分开说明。若要求公共 `forge dev` 也不加载 Arrow，需显式延迟 `demiflow/cli.py` 及 Forge 注册链中只为运行命令使用的 import，并测试；不要把该行为藏在 wrapper 中。主会话若不接受父包延迟导出的改动，另一选择是明确要求已有选定 demiflow 解释器运行，公开承认旧 stdlib launcher 契约改变。**不能静默选择这一兼容破坏，更不能安装/修改共享环境补缺依赖。** 推荐前一方案，具体源码范围由 Review 下发。

## 4. wheel、标准/Skill 与绑定资源

建议唯一权威位置都放进包内，源码与安装布局使用同一内容：

```text
demiflow/forge/development/
  project.py checks.py tasks.py cli.py runner.py notebook.py __init__.py
  standards/pipeline-v1.md, pipeline-v1.0.0-candidate.2.md ... candidate.9.md
  skill/SKILL.md
  skill/scripts/forge.py                 # 若保留则只有薄定位/传参
```

- `pyproject.toml` 当前 package-data 只有 Forge JSON/Web 资源；必须显式加入新子包的 `standards/*.md` 和 `skill/**`，并核对 sdist 的 MANIFEST。只把文件放到 `docs/` / `integrations/` 不能保证 wheel 可用。
- 通过 stdlib `importlib.resources` 定位包资源；不再用 `parents[3] / demiurge/...` 查标准/Skill。常规 wheel 解压安装须直接可用；不以临时解包路径参与 fingerprint，不宣称已支持 zipapp/直接 zipimport。
- `tool_root` 字段和 project-v1/v2 保留。新定位应明确支持 demiflow 源码 checkout 根、或实际已安装的 demiflow 包根；校验它与**实际加载的新实现**一致。不要把整个 site-packages 当作 tool_root、递归哈希共享环境，或关闭原 identity 校验来容忍任意路径。
- `Project.binding()` 当前写死旧标准路径、atomic_io 和 `__main__`（`project.py:337–343`）；应换为新 dev 源码及实际公共 helper/启动依赖的有限清单，按稳定逻辑名哈希完整内容。标准版本资源保留原 bytes。父包/辅助 I/O 的变化若影响实际工具行为也须进入绑定；不为每次调用扫描整个 demiflow/环境。
- `source_defs()` 的 standard 定位改到新资源，仍能分页读取同版本全文；runner 的 `{tool}` 使用同一个经核对的新工具位置，不再由 runner 自己推 `parents[3]`。本次已核对的两个项目未用 `{tool}`，测试仍应覆盖它。
- Skill 唯一正文置于新包的 `skill/SKILL.md`；仓库 integration 入口可指向/安装该资源，不维护第二份可独立修改的正文。源码安装链接可指新位置；wheel 安装则从包资源安装或导出可定位的完整 Skill，记录来源版本/哈希。安装副本是部署产物，不是第二权威源。
- 更新根 AGENTS、当前项目 AGENTS/PIPELINE_SPEC/README、demiflow 当前指南的 canonical 链接；版本化标准只换位置不换内容。旧归档/冻结记录中的原 URL、绝对路径和 receipt 不批量替换。

必须在**没有旧仓、没有源码 checkout 兜底**的自有临时安装目录验证 wheel：枚举并读取全部 9 个标准、Skill/脚本；执行 dev help/init/context/检查 fixture；核对工具 binding 与 `{tool}`；保证导入无需旧 `demiurge`。同时在无 Arrow/YAML/Click 的 stdlib 子进程验证项目薄 launcher 和 dev core。构建/安装只写自有临时目录，不安装到共享环境，不联网补包。

## 5. 旧证据与进行中任务

迁入新模块、工具依赖清单、standard 物理路径和 `project.json.tool_root` 都会改变原绑定。当前 `tasks.current:102–104` 本来就拒绝工具/配置变化，迁移必须保留这一事实：**旧通过证据不会在新工具下仍然有效。** 不能令新 binding 等于旧值，不能通过 refresh 更新工具绑定或重拍 baseline。

这不意味着旧记录丢失或必须另建一个当前基线：

1. 原 `.demiforge/tasks/`、baseline/readonly baseline、notes、日志、review 和 handoff 字节不变。主项目看到 144 个带 task.json 的目录，旧 worktree 1 个；只查了目录元数据和各自最多 3 个最近 handoff 摘要，未遍历所有历史 task 内容，也未由文件时间断言任务是否正在执行。
2. 新 dev 入口需要一个明确的**历史只读查看路径**（可为 `dev history`）。它先读取有界保存记录，不实例化需要现行 tool_root 验证的 Project，不经过会原子写回 task.json 的 `tasks.locked()`。显示“当时结果、旧绑定、当前未验证/绑定已变”，绝不把读取历史变成 current/refresh/accept。缺失原上下文显示缺失；相同版本标准可按 hash 比对后供查阅，但不补写旧回执。
3. 已知当前业务 README 记录了旧范围/并行改动阻塞，必须继续可见。旧任务没有 handoff 不等于已结束，也不等于可以删除。切换前由主会话确认使用者和命令是否仍在执行；不自动中断模型、训练或其他服务。
4. 对确需跨工具继续的任务，不能建议“重新 start 取当前基线就好了”。可先由主会话在旧工具仍可用时明确保存其未完成/阻塞交接；若需新工具续接，必须另行审查一种显式 continuation：保留原任务不动，新记录引用原任务并携带**原 baseline/readonly baseline 的原摘要与内容、原范围和未解决事实**，所有新验证门禁重置为未验证，逐项核对表示/标准兼容。无法可靠比较的项继续阻断。现行工具没有这个迁移能力，本方案不冒称已实现，也不把旧 baseline 换成当前树。
5. 若当前实施包不包含受审的 continuation，则未完成任务的处理是删除前未满足条件，不能靠归档旧仓就宣称已解决。主会话决定结项或另分配该能力；本包不改这些任务。

## 6. 可下发的最小实施包与删除门槛

建议分包交付，先切换工具归属再修当前缺陷，避免两个仓各维护一份新实现：

| 包 | 明确范围与验收 |
| --- | --- |
| A：dev core/入口/资源 | 新 `demiflow.forge.development`、现有公共 I/O 适配、有限资源绑定、`forge dev` 薄注册、标准/Skill 打包；必要根包轻依赖调整须显式列入范围。保持旧标准语义，验证源码与 wheel 两种布局、原 53 方法及新迁移边界。 |
| B：消费者切换 | 根 AGENTS、Skill 安装链接、两个已确认 launcher/config 及当前权威链接；主会话先决定旧 worktree 是否继续使用。仅改工具接线，不改业务入口/算子/运行参数，不自动升级标准，不变更平台执行快照。保留原记录和切换前哈希。 |
| C：旧任务查看/续接 | 至少完成无需旧仓的只读历史查看；正在进行任务逐项明确。若需 continuation，必须携原基线与事实另行 Review，不能重拍。 |
| D：F-01/F-02 | 在新归属落实已审查的缺失本地 import、源码覆盖修复；沿用 `demiforge-source-coverage-design-20261104.md` 的精确穿透、预算/控制保护和模糊范围非通过取舍。该设计迁入 demiflow 后由主会话确认，不回旧仓实施。 |

归属收敛不要求把不同证据域混为一体：dev 的工程规范/任务验收和原生 Flow/运行准入仍分别绑定各自事实；同一 demiflow 产品复用公共能力，不再依赖外部 Demiurge。迁移不需要改 Web 页面或重启现有服务。搬包及轻依赖初始化可能使现有平台快照发现源码变化，须如实标 stale；不能为“平滑迁移”自动重绑真实项目或封存运行。

**主会话控制删除门槛：** GitHub 备份覆盖实际 dirty/untracked 资产并验证可恢复；A/B/C 及必要 D 的 Review 和隔离测试就绪；所有已确认活动引用可在旧目录不可用时工作；未覆盖 worktree/训练源码、具体旧 runtime wheel 路径、进行中工具命令已核对或明确处置；历史记录仍可读、原基线与未解决事实保留；当前 Skill/资源没有悬空路径。不满足时保留本地旧仓。此表不是本执行包对备份完成或可删除的批准。

## 7. 本次交付与未验证项

临时证据目录：`/tmp/demiforge-consolidation-indexed-20261104-z19buuyg/`。主要文件为 `references.json`、`source-inventory.json`、`project-controls.json`、`core-inputs.json`、`source/`、`light-entry-probe.json`。只读 CLI 命令注册表确认 dev 可用；系统 Python 导入 probe 确认轻依赖问题。没有跑生产、模型或整库测试，没有构建 wheel、安装环境、push 或删除。

**仍未确认：** 两个明确消费者以外隐藏/未跟踪登记是否在用；训练未跟踪源码与其他 worktree；历史 runtime wheel 的实际保存位置与当前消费者；所有保存任务的实时活动状态；新 wheel/轻依赖/历史 reader/continuation 的实现行为。它们不得以本次静态零命中或旧测试通过替代核验。仓库唯一新增文件是本文；Demiurge F-01/F-02 实施保持停止。
