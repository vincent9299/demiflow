# Forge 跨层有界审查（2026-11-04）

本执行包确认 **3 个实现缺陷和 2 处当前指南偏差**，另列两项能力/范围边界供主会话判断。未发现需要复制业务、增加执行器、服务或客户端私有接线的依据。本文提供证据与最小修复范围，不代替主会话的整体架构、优先级及最终 Review。

## 审查范围与源码依据

已读取根及两个仓库的 AGENTS.md、Demiurge README/业务开发定位、candidate.9、canonical skill、quickstart，以及 demiflow 当前契约/生命周期/工作区/识别/客户端/HTTP 指南。旧 v1、candidate.7/.8 的明确历史约定不作为当前缺陷。

静态核对覆盖 `console/workspace/project_discovery/project_check/source/flow_check/operator_check/runs` 的相关路径、workspace CLI/worker/HTTP/Web，以及 Demiurge 的 `development/project.py`、`checks.py` 和相关针对性测试。算子检查器与运行模块按归属、依赖、证据及准入路径核对；没有重新审计所有算子语义和执行器内部。

本地源码快照时间为 **2026-11-04 16:05–16:13（Asia/Singapore，UTC+8）**。61 份审查输入共 868,495 字节已保存在自有临时目录；fixture 验证后逐文件比较，均未变化。两个仓库均有大量既有修改/未跟踪文件，不能仅用 Git HEAD 表示被审查实现。

| 审查输入 | SHA256 |
| --- | --- |
| demiflow `project_discovery.py` | `da632a2e793fb890e44f81013a0708d9a870521126ebc55be25e44ef71bf90e6` |
| demiflow `source.py` | `91c1efb180cc4794b59eaf532066b30193c7a1e06c149308724f86ff117eb352` |
| demiflow `project_check.py` | `2d2d70b67eb3c830e92c6a904dc6e3a7b000fd9301eb7be5c5e3b1b5dd3cccc8` |
| Demiurge `development/project.py` | `eab743bc67d561c891d28da88f9472690d3e0042a89ca826bf5680920ef6c961` |
| Demiurge `development/checks.py` | `415a8c775b29d8a8395ce4513db462bc6d4019004d57955cd5ea9da6ef00524e` |
| Demiurge candidate.9 | `dd8e310063075fb44a9f7dcc7f7c3182e99a5d041ed687ccfb8940e40132bb88` |
| canonical skill | `b1c45f89eef336153e25b1d9340ffdc5dd2d7b80eeb0fc3447af48e3a1acb1a8` |

完整清单、源码副本、输入复核和仓库状态保存在 `/tmp/forge-cross-layer-review-20261104-fpm6r4m3/` 的 `baseline.json`、`source/`、`after-fixtures.json`、`*-status-*.txt`。Git HEAD 仅作定位参考：demiflow 为 `49527158d9c4f37f57f264adc28adbc556cfffba`，Demiurge 为 `057bc69275c7bbe11ce27986f10091d00cdde2b6`。

## 具体发现

### F-01：candidate.9 对明确指向缺失模块的相对导入仍返回结构检查通过

**触发与影响。** 已登记的 `orders/recipe.py` 含 `from .missing import keep`，而 `orders/missing.py` 和对应包均不存在时，`structural(project, 'orders')` 返回 `[]`。`from .absent.processing import keep` 同样通过。此处是可确定的本地相对模块依赖，不涉及动态 import 或需要猜测的第三方包；机械检查遗漏了标准明确要求拒绝的缺失源码。这不表示完整平台检查、行为测试或整体验收也能通过。

**代码依据。** [checks.py:35](/yzp/zhaozy/yangzepeng/0905/Demiurge/demiurge/demiforge/development/checks.py:35) 的 `local_modules()` 仅返回存在的文件；相对 import 在 [checks.py:113](/yzp/zhaozy/yangzepeng/0905/Demiurge/demiurge/demiforge/development/checks.py:113) 被规范化，但 [checks.py:135](/yzp/zhaozy/yangzepeng/0905/Demiurge/demiurge/demiforge/development/checks.py:135) 对空解析结果不记录问题，也不加入待检查项。因此检查结束仍无 finding。与 [candidate.9:128](/yzp/zhaozy/yangzepeng/0905/Demiurge/demiurge/demiforge/standards/pipeline-v1.0.0-candidate.9.md:128) 的“missing sources cannot produce a passing result”不一致。

**验证。** `repro_demiurge.py` 的 `valid`、`missing_relative_module`、`missing_relative_parent` 三个小项目变体均返回 `problems: []`；结果见 `repro-demiurge.json`。只进行 JSON 登记与 AST 检查，没有导入或调用 fixture 业务入口。

**最小修复范围。** 在现有 candidate.9 静态导入解析中保留“这是明确的本地模块引用”信息，对缺失或无法核验的模块产生非通过结果；不能将 `from module import symbol` 的符号部分一概当成缺失子模块。补缺失相对模块/缺失父包边界用例，保留既有别名、相对 import、包初始化及共享算子行为。无需增加业务适配器或导入候选代码。

### F-02：candidate.9 仍按 `runs` / `datasets` 字面目录名拒绝源码并剪枝证据

**触发与影响。** 将现有算子显式登记为 `orders/runs/processing.py`，即使处于 owner 和 watch 内，当前 candidate.9 仍抛出 `Excluded or noncanonical source path`。同时，`watch: ['orders']` 下 `orders/runs/settings.json` 的内容变化不会改变源码树快照。这说明现代路径合法性和证据覆盖仍受旧运行目录名称控制，不能完全按已登记角色与依赖判断。后一结果证明指纹排除行为；本次没有执行该配置或宣称某个真实业务任务已绕过验收。

**代码依据。** [project.py:28](/yzp/zhaozy/yangzepeng/0905/Demiurge/demiurge/demiforge/development/project.py:28) 的全局 `SKIP` 同时包含技术控制目录与 `datasets/runs`；[project.py:96](/yzp/zhaozy/yangzepeng/0905/Demiurge/demiurge/demiforge/development/project.py:96) 在路径任何一段匹配时拒绝，`validate()` 对现代登记继续调用它。快照 [project.py:162](/yzp/zhaozy/yangzepeng/0905/Demiurge/demiurge/demiforge/development/project.py:162) 和现代结构遍历 [checks.py:68](/yzp/zhaozy/yangzepeng/0905/Demiurge/demiurge/demiforge/development/checks.py:68) 也无版本/角色区分地剪枝。这是 candidate.9 的当前共用代码，不是把 candidate.7/.8 的历史 `flows/operators` 要求误报成新缺陷。

**验证。** `repro-demiurge.json` 的 `named_operator_rejected` 记录上述异常；`named_directory_not_fingerprinted` 为 `fingerprints_equal: true, included_paths: []`。与 [candidate.9:43](/yzp/zhaozy/yangzepeng/0905/Demiurge/demiurge/demiforge/standards/pipeline-v1.0.0-candidate.9.md:43) 的“目录名不决定角色”和 canonical skill 的现代归属说明存在实际差距。

**最小修复范围。** 仅调整现代版本的路径合法性、快照和静态依赖遍历，使既有显式角色、依赖与 watch 能覆盖真实源码/配置；保留控制目录保护、运行数据读取边界和现有预算，旧版本保持原绑定。不能简单删除所有剪枝后扫描业务运行目录，也无需增加第二份配置。

### F-03：已读取但语法解析失败的依赖没有证据摘要，修复后旧项目检查不失效

**触发与影响。** 原入口 `recipe.py` 导入项目内 `processing.py`，后者包含语法错误。项目检查实际读取了该依赖，诊断也指出 `processing.py` 的语法错误，但报告 `sources` 只有 `recipe.py`。只修复 `processing.py` 后，公共 `project_check_status()` 仍返回旧 `incomplete`、`changes: []` 和原语法错误，未标为 `stale`。客户端重新读取同一公共结果也不能修正这一证据问题。没有观察到其升级为静态通过或授予执行权限。

**代码依据。** [source.py:124](/yzp/zhaozy/yangzepeng/0905/demiflow/demiflow/forge/source.py:124) 已读入源码，但直到 `ast.parse` 与 AST 预算检查之后的 [source.py:138](/yzp/zhaozy/yangzepeng/0905/demiflow/demiflow/forge/source.py:138) 才将摘要写入 `tree.files`。`project_check` 仅汇总该映射（[project_check.py:71](/yzp/zhaozy/yangzepeng/0905/demiflow/demiflow/forge/project_check.py:71)），状态查询也仅复核记录中的源文件（[project_check.py:163](/yzp/zhaozy/yangzepeng/0905/demiflow/demiflow/forge/project_check.py:163)）。识别器对原算子只检查路径存在，不能补上失败依赖的内容摘要。

**验证。** `repro-demiflow.json` 中 `syntax_error_check.sources` 只有入口；`after_fix_without_recheck` 保留旧错误且无变化原因。显式重新检查后，同一依赖进入 `sources`；再修改已成功解析的依赖，正常得到 `stale` 和 `source_changed:processing.py`，作为对照。使用真实 `ws.check_project` / `ws.project_check_status` 及 worker，无模拟检查器；自有项目无业务数据、未执行 Flow。

**最小修复范围。** 将“已读取的内容依据”与“成功解析/检查”分开记录，确保失败源码的变化也能使旧诊断过期；读不到的目标则保留可复核的失败/未覆盖状态。同步保持实际依赖路径的诊断位置，不把失败依赖算作检查通过。此项是 `SourceTree` 的证据失效问题，**不是已知 `project_discovery` 失败解析重复读取的性能项**。

### F-04：当前指南中的两条规范检查命令带有不存在的 `--json` 参数

**触发与影响。** [工作区指南:12](/yzp/zhaozy/yangzepeng/0905/demiflow/docs/forge-workspace.md:12) 给出的 `workspace check ROOT --json` 和 `check-status ROOT --json` 都在 Click 参数解析阶段返回退出码 2、`No such option '--json'`，不会执行检查。新会话直接按正式接入示例操作会在入口处失败。

**代码与验证。** [workspace_cli.py:85](/yzp/zhaozy/yangzepeng/0905/demiflow/demiflow/forge/workspace_cli.py:85) 和第 91 行两个命令未应用本文件已有的 `pretty_option`，而是直接固定输出 JSON。`repro-demiflow.json.documented_cli_flags` 保存两条真实 `CliRunner` 调用结果，均为退出码 2。

**最小修复范围。** 让 CLI 接受与同组命令一致的 `--json`，或修正当前指南为实际可用命令；该选择由主会话决定。无须改检查器、HTTP 或权限。

### F-05：当前工作区指南仍要求算子/模型配置位于固定目录

**触发与影响。** 同一份当前指南先声明目录名不决定职责，却在 [forge-workspace.md:160](/yzp/zhaozy/yangzepeng/0905/demiflow/docs/forge-workspace.md:160) 写“实际定义必须在 operators”，在 [forge-workspace.md:237](/yzp/zhaozy/yangzepeng/0905/demiflow/docs/forge-workspace.md:237) 写“模型配置文件属于 flows”。这两句没有限定为旧 v1/历史标准，容易让新会话继续移动原项目代码或新增接入布局。

**代码依据。** 当前 [OperatorChecker.describe:239](/yzp/zhaozy/yangzepeng/0905/demiflow/demiflow/forge/operator_check.py:239) 要求实际本地定义，不要求 `operators/` 名称；声明文件由 [resolve_flow_resources:1257](/yzp/zhaozy/yangzepeng/0905/demiflow/demiflow/forge/workspace.py:1257) 按项目内安全路径与预算绑定，不要求 `flows/`。Demiurge candidate.9 与 canonical skill 同样使用实际角色登记。现有 `test_forge_project_checks.py` 也明确以 `recipe.py` / `processing.py` 覆盖这种布局。

**最小修复范围。** 只修正当前指南这两处措辞，明确按实际定义、声明与项目归属维护；确需介绍旧约定时显式标历史版本。保留旧 v1 契约/skill 和 candidate.7/.8 原文。

## 已核对、未作为新缺陷的边界

| 层次 | 本次确认的实际行为与依据 |
| --- | --- |
| 新建/登记/移除 | `console.py:57–110` 复用原目录，只初始化平台控制信息；登记使用稳定 workspace ID，移除只删当前 registry 成员。未见业务白名单、复制业务或删除原项目的路径。 |
| 原项目识别 | `project_discovery.py:167–234` 只解释版本化公开 JSON，精确选择 owner；不导入 Demiurge、不执行其命令。读取当前项目源码/配置，不把外部引用变成授权。已给定的多入口读取优化 SHA 与审查源码一致；失败解析重复读取不重复登记。 |
| 检查与认证 | `project_check.py:92–124` 区分原生已绑定变体、源码前置检查及缺口；行为/review 仍为 missing，静态 checked 时整体仍为 incomplete。未知协议/未绑定参数不会自动成为原生可执行入口。 |
| 公共能力与客户端 | CLI、HTTP、Web 调用同一 `workspace`/`runs` 用例；Web 原项目、检查和数据视图读取公共事实，没有业务专属分支。`changes` 为刷新提示，不是授权证据。宿主插件说明明确尚无经过宿主验证的插件交付，本次也未声称验证了实际宿主插件。 |
| 原生准入 | `runs.py:184–199,252–254` 在 prepare 时做明确入口、参数、范围及新鲜原生检查；`623–633,698–707` 的 start/resume 依据复核不依赖观察缓存。HTTP 还检查 registry 的 allow_runs 与 server enable_runs（`workspace_view.py:155,470`）。未发现识别结果能直接替代这些准入的新增旁路。这里是静态调用链结论，没有启动真实运行。 |

**能力限制 L-01：共享源码与外部读取。** Demiurge 的 repository 范围内，共享算子可从另一已声明 owner 导入，当前针对性测试证明此路径受支持；但把其中一个 pipeline 目录登记为 demiflow workspace 后，`SourceTree.root` 就是该目录，原项目识别也不为兄弟目录源码或项目外数据授权读取。它可能被识别，但不能据此认为其整个依赖闭包已通过原生检查。该限制在原项目识别指南已有披露，应继续如实显示缺口；不能为接入而复制共享源码、注册第二份所有权或自行扩大读取权限。本次不建议新框架。

**范围事项 L-02：已声明但未进入入口依赖闭包的算子。** 当前识别结果明确报告 `operator_checks: declared_path_existence_only`，`project_check` 的源码检查也只沿入口依赖展开。因此声明的 `unused.py` 内容变化不会改变 discovery digest，也不会使项目检查过期；临时 fixture 已验证这一点。现有 [识别指南:35](/yzp/zhaozy/yangzepeng/0905/demiflow/docs/forge-project-discovery.md:35) 对“原算子变化后标记依据变化”的概括比实现更强。主会话需确定整项报告究竟承诺“实际读取闭包”还是“全部已声明算子”的内容覆盖，然后统一覆盖说明与证据绑定。这里没有将未调用算子的行为描述为已检查，也未将这项范围选择列作独立执行权限缺陷。

## 验证及操作边界

- 自有 fixture 脚本与结果：`repro_demiurge.py` / `repro-demiurge.json`；`repro_demiflow.py` / `repro-demiflow.json` / `repro-demiflow.log`，均在上述 `/tmp` 目录。首次执行使用 `python3 -B`，demiflow 使用已有 `/yzp/zhaozy/yangzepeng/0905/env/bin/python -B`。需要重跑时将脚本复制到新的自有 `/tmp` 目录，避免覆盖已有 fixture 控制文件。
- Demiurge 现有 4 个针对性用例通过：任意 owner 内模块名、既有共享算子 owner、helper 间接反向 import、未归属/未 watch 依赖。日志为 `demiurge-targeted-tests.log`。未重复运行整个测试库。
- demiflow fixture 创建一个空数据项目的控制信息，只执行识别、源码检查、状态查询和 CLI 参数解析；没有数据表、模型、网络请求、Flow 执行或服务启动。
- 没有修改实现、标准、Skill、README、业务仓、Notebook、training、运行数据、共享环境、管理登记、令牌或现有服务；没有重拍/重绑任何业务证据。仓库唯一新增文件为本报告，其余证据均在自有 `/tmp`。

本报告未完成动态 Python 安全性、全部运行故障、生产数据/业务质量或所有客户端竞争路径的证明，也不把已有测试通过提升为整体验收。
