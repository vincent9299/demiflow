# Forge 工作区、契约与本地实验

原 pipeline 目录就是项目；用户选择目录新建或原地注册，平台不复制、拆分或重排业务代码。统一规范约束实际定义和调用契约，不按业务目录名称划分职责。通用算子基础阶段已扩展原生契约、异步执行、节点观察及请求证据；当前边界见 [基础能力验收](forge-operator-foundation-acceptance-20261102.md)。第一阶段的平台发现继续使用；旧 Forge v1 项目的生命周期和三节点契约仍独立保留。

## 正式登记已有项目

操作台左侧提供“新建项目”和“注册已有项目”，填写服务所在机器的绝对目录。也可无 UI 使用：

```bash
demiflow workbench console --registry /absolute/console.json create /absolute/new-project --name 新项目
demiflow workbench console --registry /absolute/console.json add /absolute/existing-pipeline
demiflow workbench check /absolute/existing-pipeline
demiflow workbench check-status /absolute/existing-pipeline
```

左侧每个项目旁的“移除”用于取消本控制台登记。确认框显示项目名称和原目录；确认后不再展示，其他打开的页面也会同步清除该项目，移除最后一个项目后回到空清单。目录、业务代码、平台管理文件和历史结果都保留；已启动任务继续运行，关联巡检在重新读取清单后停止跟踪。之后可通过“注册已有项目”恢复访问。CLI 使用 `workbench console --registry REGISTRY remove WORKSPACE_ID --expected DIGEST`，摘要取自最新的 `console list`。登记清单变化时拒绝沿用旧确认，不提供删除项目文件的参数。

新建只接受不存在或为空的目录。注册接受非空原目录，不导入、执行、改写其中的业务代码。必要管理信息只有 `workspace.json` 和 `.forge/`；已有兼容管理信息直接复用，冲突拒绝覆盖。不创建 `flows/`、`operators/`、`pipelines/`、`tables/`、`data/`，不分配默认写入位置。登记失败后保留已初始化项目，可修复控制台登记问题后重试 add；不删除原项目回滚。

当前统一规范为 2.4.0。操作台“规范检查”与 CLI 调用同一受限检查器，覆盖原声明的入口和配置，显示实际诊断、源码、静态结果及缺失证据。不识别的声明、未声明代码、未完成检查均不计为通过；不自动推断任意脚本是 Flow，也不修正业务问题。修正业务代码需用户确认。

`workspace check` 不执行业务行为测试或代替语义审查；即使已声明入口的静态检查通过，整体仍会显示尚缺证据，CLI 返回非零。检查报告不是执行许可；prepare/start/resume 仍执行原有的新鲜度、原生契约和资源准入检查。任意绕过 Forge 的 Python 执行不受此机制约束。

新工作区采用 v3：沿用 v2 的稳定工作区 ID、明确入口/配置索引和数据位置，新增显式 audit 观察与修改范围控制。旧工作区不自动升级。
`--existing` 可初始化非空仓库，保留 README、代码、数据和 Notebook；已有控制文件冲突时拒绝，
失败仅清理本次创建且未提交的文件。旧 v1 工作区仍按原格式读取，不自动升级绑定或认证。

```bash
demiflow workbench init /absolute/project --name project --existing
demiflow workbench flow register /absolute/project flows/process.py:process --params flows/configs/default.json
demiflow workbench console --registry /absolute/console.json add /absolute/project
demiflow workbench console --registry /absolute/console.json list
demiflow workbench console --registry /absolute/console.json serve --port 59865
```

一个控制台可显式登记最多 128 个工作区。登记文件保存 root、显示名称和运行权限上限，
HTTP 的项目内容操作只按登记 ID 解析项目；新建/注册是独立的已认证控制台操作，显式接收服务器目录。项目切换使用请求局部 root；
缺失项目单独显示诊断。页面自动读取登记变化并按项目隔离未提交参数，切换使旧读取失效。
v2 定位链接使用稳定 ID，旧路径链接仅在对应 root 已登记时转换。已有 SSH 转发可以沿用。
运行按钮需要登记 `--allow-runs` 和服务 `--enable-runs` 同时开启，后续执行准入仍逐次核验。

操作台左侧列出已登记的工作区／项目；选择项目后，顶部切换“项目总览”、
“数据表 / Case”、“Flows / 检查”、“规范检查”和“运行记录”。总览可只读查看原项目 Python 源码。右上角显示当前项目的运行权限，
并提供“刷新当前视图”；左侧底部显示自动同步状态。窄屏时项目列表横向排列。
这些入口复用同一项目身份、URL 定位和读取接口，不建立第二份项目状态或聊天后端。

Flow 登记只检查入口存在和配置位置，不运行代码、不宣布契约通过。v2/v3 只列登记入口，
公开 helper 不自动成为 Flow；同一入口可登记最多 64 个配置。源码修改使符号失效时显示诊断。
先使用 `flow inspect` 核对契约，再按实际授权执行。平台可只读解释受支持的公开项目登记，
展示原入口及其依据，但不会导入开发工具或执行其中的验证命令。开发标准工具与运行工作区各自负责自己的绑定。
注册后的原项目识别、配置/数据查看和检查覆盖范围见 [原项目识别](forge-project-discovery.md)。

受管运行的幂等准备、部署域、停止证明、资源总量与指标边界见[运行管理 v1](forge-runtime-management.md)。旧 prepared seal 不会自动升级，新执行须重新 prepare。

## 从真实示例开始

使用已安装 demiflow 的 Python 环境；图像预览需要 `demiflow[images]`。示例只写入新建的自有目录，不调用业务 Flow、不访问模型或生产数据。

```bash
python examples/forge_workspace/make_demo.py /tmp/forge-stage2-demo
demiflow workbench view /tmp/forge-stage2-demo
```

打开命令输出的本地会话地址。界面可独立查看项目说明、数据位置、固定版本表、图文 case、业务键对照、Flow 源码和检查报告。candidates@v3 与 scores@v2 各有三条记录；comparison_fixture 是独立准备的对照数据，c02 有两个匹配、c03 无匹配，不能称为 Flow 的执行结果。

通过 SSH 连接服务器时，浏览器需要访问转发后的本机端口。查看服务允许 `localhost`、`127.0.0.1`、`[::1]` 的本机端口与服务器监听端口不同，API 仍要求会话 token，Origin 必须与浏览器请求的 Host 一致。例如服务监听服务器的 59865 端口时，在本机终端执行 `ssh -N -L 55971:127.0.0.1:59865 你的SSH主机别名`，再打开 `http://localhost:55971/#token=服务输出的token`。如果客户端已经自动转发，直接使用它给出的本机地址即可。

在 Flows 页选择 `flows/questions.py:select`，分别使用 `flows/select_a.json` / `flows/select_b.json`。同一份接线分别绑定阈值 0.80 / 0.90 与两个输出目标。两份报告各有 7 个节点、6 条边。输出节点显示计划写入，真实输出尚未创建。

## 已有项目的数据能力与示例布局

以下是既有示例自行选择的布局，不是新建或注册项目的目录模板。平台保留原声明和历史查看能力；注册不要求另做数据接入配置。

```
workspace.json                  # 稳定 ID、入口/配置、平台快照、位置和权限
README.md                       # 项目说明
flows/questions.py              # 平台 API 接线及 helper
flows/select_a.json              # 同一接线的配置
operators/question_rules.py     # 普通 fn/actor 的实际定义
tables/candidates.json          # alias + location_id + 原生 DatasetRef
data/                           # 示例显式声明的自有数据位置
.forge/platform/                # 不可覆盖的平台快照
.forge/inspections/              # 不可覆盖的派生检查报告
```

Notebook 可留在项目中，也可从其他工具使用这些代码和数据；它不承担平台接线或可视化的必需职责。平台检查不会执行、导入 Notebook 或候选 Python。

表别名不是实际身份。身份包括位置根、原生 DatasetRef、固定 Lance 版本和元数据；`--head` 只在 attach 时解析一次。后续读取不会自动跟随新版本，旧版本不存在时直接失败。外部位置默认只读；v2 只有显式 `workspace location ... --writable` 才声明写入权限，具体执行模式还需运行契约允许。detach 只删引用，不删表。

```bash
demiflow workbench init /tmp/experiment --name experiment
demiflow workbench location /tmp/experiment corpus /absolute/read-only-root
demiflow workbench table attach /tmp/experiment candidates --location-id corpus \
  --uri candidates.lance --version 3 \
  --dataset-id questions --schema-name questions --schema-version 1
demiflow workbench table list /tmp/experiment --json
demiflow workbench table show /tmp/experiment candidates --json
demiflow workbench table sample /tmp/experiment candidates --column case_id --column question --limit 20 --json
```

已有原生 Catalog 时复用其中的引用，身份歧义需明确 dataset_id；无登记项时必须明确业务标识和 schema 版本。`table replace/detach --expected DIGEST` 使用当前引用摘要做并发检查。

## 人和 coding agent 的相同入口

1. 阅读 README，运行 `workspace context`，了解表、入口、支持的原生 API 与标准。
2. 按原项目组织普通函数/actor 和平台 API 接线；入口例如 `recipe.py:run`，配置例如 `settings/default.json`，不要求固定顶层目录。使用 `flow register` 明确登记公开入口及命名配置。
3. 用 `workspace check` 检查全部已声明入口，或 `flow inspect` 检查指定入口；均不导入候选代码获取结果。报告 invalid/incomplete，业务修正由用户明确确认。
4. 用统一界面查看图、字段、参数、诊断位置和源码，按业务键查看真实 case。
5. 用第三阶段的 `workspace run prepare/start` 绑定源码、配置、固定数据及平台后运行；checked 本身不等于运行成功或结果回执。

```bash
demiflow workbench context /tmp/experiment --json
demiflow workbench flow inspect /tmp/experiment flows/questions.py:select --params flows/select_a.json --json
demiflow workbench flow view /tmp/experiment REPORT_DIGEST --json
demiflow workbench flow diff /tmp/experiment LEFT_DIGEST RIGHT_DIGEST --json
demiflow workbench flow view /tmp/experiment REPORT_DIGEST --out /tmp/flow.html
```

Python 入口位于 `demiflow.forge.workspace`：`init_workspace`、`attach_table`、`inspect_flow`、`describe_table`、`sample_table`、`report_status`、`diff_reports`。CLI 与 HTTP 查看接口调用这些函数，没有另一套业务 API 或执行器。离线 HTML 内嵌同一报告，不含样本，不接触当前工作区，也不表示当前依据已复核。

平台代码/依赖/规范变化后，显式 `workspace bind-platform ROOT --expected OLD_PLATFORM_DIGEST` 采纳新的快照，再检查 Flow；旧报告保留。移动工作区、修改相关源码/helper、参数或表引用都会使既有报告相对当前绑定 stale。未依赖的 Flow 文件修改不影响该报告。

## 在 Codex 开发，在操作台查看同一份事实

对话始终留在 Codex；操作台独立展示同一工作区。Codex 通过已有 CLI/Python 修改和检查项目、准备并启动实验；操作台不调用 Codex CLI，不保存聊天，不要求选中 Case 发送注释。

1. 启动一次 `demiflow workbench view ROOT`，打开它输出的会话地址。SSH 场景沿用已建立的端口转发。查看 CLI 创建的运行不需要 `--enable-runs`；只有要在操作台使用运行按钮时才添加该参数。
2. 在 Codex 中明确工作区、Flow、参数和目标，例如：“在这个工作区，将 select_a 的筛选阈值改成 0.90，检查后运行一次，保留旧实验。”Codex 保存配置，使用下文的 prepare/start；不需要通过页面转发指令。
3. 页面可见时，约每 3 秒检查一次已保存变化。项目说明、Flow/配置目录、检查报告、运行状态和结果列表按需更新。源码变化不会自动执行检查或启动实验；旧报告仍绑定原始依据，打开时会复核并标记变化。
4. 打开“运行记录”查看新实验，完成后点击“查看结果”查看 Case、图片与按业务键对照。正在查看旧实验 A 时，B 的出现不会把页面带走；A 仍读取其固定版本。
5. 在 Codex 继续提出修改，再检查和运行。操作台展示真实运行记录，不能根据聊天中的“完成”判断成功。

查看服务需要作为独立的长期进程运行，后续复用同一入口；不要把它的生命周期绑定到 Codex 的一轮临时工具调用。CLI 保持原有前台服务语义，没有增加另一套进程管理服务。

页面左侧显示自动同步状态和最近检查时间。隐藏页面暂停，重新可见检查一次；断线时保留旧内容并明确标示未更新，恢复后重试读取。手动“刷新当前视图”强制重新读取，与自动刷新合并调度，不重放写操作。Flow 表单有未提交编辑时保留表单并提示外部更新，提交或清空后再更新；草稿不跨整个浏览器重载持久化。

注册原目录无需为源码浏览补配数据位置。源码索引属于有界的展示与变化提示：单个作者目录最多扫描 1,024 项、深度 16，整个观察清单最多 4,096 个文件；总览最多展示 256 个 Python 文件。超限、符号链接或不可读目录会在 `source_diagnostics` 和页面源码区明确列出，其他范围继续展示和同步，不使整个项目打不开。未扫描范围不参与源码自动同步，不能将浏览清单当成完整监测或合规证据。规范检查、后台显式范围核验和执行前源码闭包检查仍按各自严格预算拒绝不完整证据；不按 `datasets`、`runs` 等业务目录名称自动授予豁免。

“复制定位链接”仍可保存表、Case、报告、节点、运行与 A/B 对比位置。链接不包含会话 token、Case 正文或草稿，需当前浏览器已有会话凭据。Case 引用含 `binding_digest` 和字符串 `row_id`；别名换绑后旧链接读取会拒绝，不切到 head。row_id 不能作为跨表业务键。

```bash
demiflow workbench context /tmp/experiment --json
demiflow workbench flow inspect /tmp/experiment flows/questions.py:select --params flows/select_a.json --json
demiflow workbench run prepare /tmp/experiment flows/questions.py:select --params flows/select_a.json --json
# 从 prepare 的返回中取同一 run_id 和 digest：
demiflow workbench run start /tmp/experiment RUN_ID --expected PREPARED_DIGEST --json
```

变化检测只读工作区控制、已声明入口/配置及近期报告依赖所在作者目录的文件元数据、已有表引用、派生索引及有界活动运行的日志元数据。依赖提示最多读取近期 32 份索引报告、合计 16 MiB；没有入口时使用有界作者目录清单，排除平台状态和已声明数据位置。超限会报告，不伪装完整。变化标记是刷新提示，不是全部历史依赖的完整性证明；显式任务范围与执行前的内容校验仍是强制边界。实际显示与执行仍通过身份、摘要和固定版本校验。无变化时不重复读取 Case 或图片，不重新计算源码内容哈希；列表只读取当前页的原始事实。

每个源码目录最多 1,024 项、深度 16；表引用最多 1,024；报告单项最多 8 MiB。报告和运行使用同一个可重建索引，最多 100 万条 / 256 MiB，每页默认 20、最多 100 条。索引不完整或损坏会明确提示；使用 `workbench index ROOT --synchronize` 或 `--rebuild` 显式修复，普通刷新不全扫历史、不静默丢项。源事实发布前保留有界待更新标记，索引失败不回滚已提交结果。单页一条同步链，上一轮结束后再等待 3 秒；实际延迟包含读取耗时，不承诺跨文件原子快照或毫秒级更新。

设计见[共享工作区方案](forge-shared-workspace-panel-design-20261103.md)，实际实验与验证边界见[同步验收](forge-workspace-sync-acceptance-20261103.md)。

## 静态检查的边界

FL-01—10 与 OP-01—08 的完整含义见 [已通过设计](forge-stage2-contract-design-20261102.md)。实现按共享事实判定，不为每个编号创建一层框架。

- 原生接口签名继续动态发现；支持接线的函数附带 `demiflow_api_flow_v2` 声明及所属原生模块的纯语义函数。声明不包装函数，不注册业务算子，不增加执行引擎。相同语义的新 API 无需增加 Forge 方法名分支；未知语义阻断。
- Flow 支持原生读取、map/filter、flat_map、map_async、batch_map、join/union、归约、投影、materialize、写表及当前原生提交/恢复。原生 count、require_columns、有限 collect_summary 和 require 表达前置条件；schema/receipt/stage 值只能由对应原生能力产生。有限配置分支、有限列表展开和同目录 helper 可用；拓扑不能由真实数据运行结果决定。
- 字段式 map 保留输入；整行 map 用声明输出替代；outputs 是返回键到目标字段的映射。TypedDict 必需键与 nullable 分开检查，left join 的右侧字段可能缺失；不推断排序、唯一性、实际行数或真实返回值。
- Operator 支持明确类型、局部别名/helper、可解析的纯计算 actor、条件、容器和有限支持的循环/推导式、常用标量内置函数与字符串方法。实际定义须在项目内可核验的本地源码中，按声明、归属和依赖解析，目录名称不决定算子角色；包初始化、默认值、类体、注解和调用闭包一并检查。旧 v1 及明确规定固定目录的历史版本仍按原规则读取。
- 普通算子可用受限 JSON 编解码、SHA256、标量数学、固定正则和 HTML 文本处理，以及有类型的排序和局部列表追加。JSON 解析结果不是任意类型的豁免：必须通过 builtin 类型判断后再读取；声明不能充当强制转换。浅拷贝可以修改自有外层容器，借用的子对象仍禁止修改。第三方依赖、泛化 callable、继承/装饰器、动态反射、任意 JSON hooks 和不确定调用仍拒绝；某些合法 Python 会得到 incomplete。
- 同步/流式、构造参数、scope、并发/队列和 catch 对齐已知原生规则。当前不证明实际 cleanup、内存峰值、线程安全、所有异常路径或业务失败语义；这些报告字段为 not_run。
- 受管 Flow 的声明检查覆盖原生文本/图片节点及资源绑定；实际执行支持面以当前基础适配与同版验收为准。新组合已为 collect document/image/wiki 和固定 LanceLookup 补齐具体原生状态适配并验证原成功及恢复路径；固定 batch_map 物化限单 worker、无定时 flush、无 group/prefix，整源排空后单次原生提交。业务导语选择、模板名单、评分规则不进入平台。schema 使用受限原生声明构造，完整绑定字段顺序、物理类型、nullable 和 metadata。agent、服务启动、远程对象存储、动态 backend_options、跨表事务仍无当前准入契约。静态检查不执行候选 Flow；无 schema 的空输出按原生行为失败。

静态状态 checked / invalid / incomplete 与运行证据分开。必要静态事实未知会阻断。CLI 对 invalid / incomplete / stale 返回 1；用旧 v1 命令不能让 workspace 获得通过。

## 资源与本地查看边界

源码有文件数、字节、AST、闭包深度/上下文、展开数、节点/边上限；报告最多 8 MiB，HTML 最多 16 MiB，历史不自动淘汰。检查与原生预览由固定 worker 协议执行，60 秒到期终止本任务进程组；Linux 父进程退出会终止子 worker。

每个原生预览 worker 在加载可信库后，设置至多新增 384 MiB 虚拟地址空间的 RLIMIT_AS，绝对上限 4 GiB；不足 128 MiB 余量则拒绝。它是分配限制，不是精确 RSS 或完整业务流水线预算。Lance 先投影，再按 1 行批次、64 KiB 批字节、1 批预读 / 1 fragment 预读、1 MiB I/O 缓冲读取。native 解码前存在进程分配保护；解码后再次检查 Arrow 缓冲和 Python/JSON 响应大小。单条不可拆对象超限失败，不通过截断“成功”。

隔离检查进程在启动前将 glibc malloc arena 数量限制为 2，避免 native 线程各自预留大块虚拟地址导致小 Case 偶发耗尽上述上限。恢复过程中启动的元数据检查还须遵守父进程传下来的软、硬地址空间上限，不尝试提额；不足余量时拒绝。线程、地址空间和响应预算仍各自生效；此设置不增加可读取范围，也不是 RSS 配额。

每页默认 20、最多 100 行和 16 列；页响应最多 256 KiB，单条详情 1 MiB。预览当前限制本地直连表：目录最多 65,536 项、深度 16、固定版本最多 1,024 个 fragment；拒绝符号链接、特殊文件、隐式位置重定向、外部 fragment 与未覆盖的 overlay/stable-row-id 元数据。限制是本地预览支持范围，不修改原生 Dataset 能力。

原生位置映射按完整键使用有界派生索引，不能为看一个 Case 全量加载共享 JSON。首次构建与失效重建有独立预算；解析失败不猜测其他表或最新版本。历史表读取和本轮执行证据分别展示。

图片只有用户明确点击才读取；旧表内图片预览仍限制在对应数据位置。新的表展示声明支持嵌套字段和列表，图片须同时声明 URI、SHA256 字段及资源位置；worker 重读固定 Case 后核对路径、大小和哈希。编码文件最多 8 MiB、16M 像素，缩略图最多 512 KiB。普通字符串不自动读取文件或 URL。浏览器图片缓存最多 32 MiB / 64 项。历史证据只从声明字段读取原生 PromptRecordRef，不展开离线二进制引用，按明确传输凭据键和 URL 参数脱敏；不声称能识别正文中所有秘密。

HTTP 仅绑定 127.0.0.1，使用会话 token、Host/Origin 校验与禁用外部资源的 CSP；2 个活动操作、8 个等待连接，超限 503，慢连接有超时。默认不开放执行写操作；显式 `--enable-runs` 才开放下面的准备、启动、取消、核对与结果引用登记。没有修改 case、审批写回或远程发布接口。源码/README/数据均按文本展示。

## 第三阶段：从接线运行到真实结果

```bash
demiflow workbench view /tmp/experiment --enable-runs
demiflow workbench run prepare /tmp/experiment flows/questions.py:select \
  --params flows/select_a.json --json
demiflow workbench run start /tmp/experiment RUN_ID --expected PREPARED_DIGEST --wait --json
demiflow workbench run list /tmp/experiment --json
demiflow workbench run show /tmp/experiment RUN_ID --json
demiflow workbench run logs /tmp/experiment RUN_ID --json
demiflow workbench run compare /tmp/experiment RUN_A RUN_B --json
demiflow workbench run cancel /tmp/experiment RUN_ID --json
demiflow workbench run reconcile /tmp/experiment RUN_ID --json
```

命令放在 `workbench run` 下；旧 `forge run PROJECT --config ...` 仍用于旧 v1 生命周期。两者没有隐式转换。Python 对应 `demiflow.workbench.runs.manager.prepare/start/show/list_runs/cancel/reconcile/compare/logs`。

在界面中：

1. **Flows / 检查**：选入口和配置，在“运行输入与预算”选固定输入、运行时间，可填输出业务身份；点击“准备运行”。
2. **核对后开始**：准备只保存检查结果和代码快照。查看本次实际输入、配置、输出路径及预算，再点击“开始运行”。默认给每次尝试分配独立的 `data/runs/<run_id>/<输出参数名>.lance`；原配置目标不被暗中覆盖。
3. **运行记录**：查看状态、动作图、已提交结果和有限日志；可以取消。关闭浏览器或 SSH 转发不结束已启动的实验。重新连接后从历史列表打开原 run。
4. **看 Case**：点“查看结果”或输出节点直接读该次真实提交的固定版本；点“与输入对照”，明确选择业务键。匹配缺失、多重匹配分别展示；行号不是跨表业务键。
5. **比较 A/B**：选择两次运行，比较当时源码、输入、配置、行数，再按业务键对照实际结果。平台不自动推断哪次业务质量更好。
6. **继续加工**：选择结果的“作为输入继续加工”，选择另一条 Flow 和参数文件，再明确选择要替换的输入槽。引用形式为 `run:<run_id>:<输出名>`，包含准确版本；不会自动选 latest，也不自动裁剪原图。需要长期别名时再点击“保留为输入表”。

“数据表 / Case”分别列出显式绑定表和运行产物；刷新页面后仍可找回已核验产物。运行历史的图和源码来自当次封存内容，当前代码修改不会改写历史。`materialize()` 是运行内临时复用，只有明确写出的结果表可以长期传给后续实验。

CLI 可用 `prepare --inputs '{"source":"run:RUN_ID:target"}'` 改变固定输入；`--outputs` 接受以输出参数名为键的 `dataset_id/schema_name/schema_version`；`--limits` 只能收紧工作区有效预算。`--exact-output` 使用配置中的目标：create 要求不存在，overwrite 要求明确旧版本和 CAS 所有权，不能静默重定向新表。准备后相关源文件、参数、输入引用或平台变化，需要重新准备。

## 运行状态、恢复和资源保证

真实 Python Flow 调用现有 Dataset 执行器，图只核对及展示；没有另一套图解释器。原生计划/实际 callable 参数和返回值在动作边界检查。普通原生 Dataset 使用不自动启用 Forge 校验。

状态分三层：运行是否结束、输出是否已经提交、结果是否登记并可读取。多输出中第一张成功而第二张失败时，整次失败，第一张保留。取消须等进程停止事实，不能只凭收到请求标 cancelled。进程消失后只读查询显示监督进程不可用；用户显式“核对提交与补登记”才检查原生 create 的唯一 operation_id 标记并修复引用，绝不重新调用 Flow。没有匹配证据时仍显示待核对。外部删除版本后读取失败，不能切换到 head。

异步前缀接 join/union/普通写表须先 materialize。新入口复用原生 RunContext/RunJournal 的提交、ACK 与恢复；显式 open_checkpoint/checkpoint 编排已退役，不再以 stage checkpoint 为新运行入口。显式 resume 只恢复同一封存绑定的已停止技术中断，保留 attempt 历史和累计预算，复用核验过的提交。业务 guard 失败、未知最终用量、剩余额度不足或绑定变化不能借恢复重置。CLI 使用 `run resume ... --expected DIGEST --expected-attempt N`；运行页可选择历史尝试并按该尝试定位节点和日志。图不调度任务，实际仍重执行原 Python。

默认每工作区 1 个活跃运行、无队列；600 秒执行、30 秒原生准入、10 秒取消宽限。原生池 256 MiB、进程树 RSS 监控 4 GiB；受管进程在可信依赖加载后有至多新增 1 GiB、绝对最多 8 GiB 的虚拟地址分配限制。默认算子批行额度为 256；原生 writer 的常规切批目标为 8 MiB。单行与批的硬字节额度根据有效原生内存预算计算，显式上限只能收紧；合法 Arrow slice 的逻辑字节与所保留父缓冲分别核算。流图还按并发、队列与预读解析可驻留额度，Python 值继续检查深度/项数/字节。详见 [当前资源合同](execution-payload-budget.md)。输入元数据合计最多 100,000 行、每个物化/写动作最多 200,000 行，超限失败、不截断提交。

上述为小实验默认预算；工作区可在通用部署上限内显式扩大，单次运行只能收紧。默认临时目录目标 4 GiB、结果增量 2 GiB、日志尾部 16 MiB；日志轮转后旧游标明确返回截断。结果计量采用有界文件通知及近期缓存，淘汰后可能保守重计；通知丢失使最终用量未知。它不是内核配额，也不宣称统计全部写盘流量。正常/失败/取消在停止并有界回收本次子进程后清理 scratch；未知状态不清理可能仍使用的目录，不删已提交表。列表索引容量独立于这些运行预算，不再使用 128 次运行的总量限制。

这些限制不是完整 OS 沙箱或生产资源隔离：RSS/磁盘轮询允许间隔内超调，Arrow 逻辑字节不等于 RSS；原生池只约束其管理的分配。分配保护、输入投影与预读、值/批检查和终止兜底共同作用，仍不宣称任意第三方代码有分配前精确上界。业务回调只执行受检查的计算闭包；网络及账本由声明过的原生资源节点拥有，不允许藏进普通 operator。不修改共享环境、不安装依赖。完整离线环境制品与正式生产放行属于后续阶段。

设计与验收见 [第三阶段设计](forge-stage3-execution-design-20261102.md) 和 [第三阶段验收](forge-stage3-acceptance-20261102.md)。

## 通用算子：节点观察与证据

历史示例 `python examples/forge_operator_foundation/make_demo.py /tmp/native-operator-demo` 的目标是：3 条输入展开成 6 条，经普通异步 actor 标注后写入新表。新基础要求 actor 提供有界 snapshot_state/restore_state，历史示例不能替代当前运行验收。接线、配置、算子均为可读源码，详见 [示例说明](../examples/forge_operator_foundation/README.md)。

点击运行图中的节点，可以按 action / occurrence 查看输入、输出、调用次数、可测错误、耗时和 actor 钩子完成情况。融合任务只能显示整组耗时，不可相加当成节点墙钟时间；未测量、未确认、观察不完整不会填成 0 或成功。相同上游被多个 action 消费时分别记录；materialize 复用与重新计算明确区分。

历史模型节点使用 `call_output` 时，Case 页面从原生引用打开准确 request / response / error。结构化字段默认折叠；重复引用合并为同一个按钮。读取仅接受本次运行声明的账本，每次最多 1 MiB；不会重发请求或凭日志推断响应。CLI 对应 `demiflow workbench run evidence ROOT RUN_ID --reference '<原生引用 JSON>' --json`，Python 对应 `runs.evidence`。

模型配置按原生节点的明确声明、项目归属和项目内安全路径绑定，不要求位于 flows 目录；单文件最多 1 MiB，封存时绑定内容；凭据只从声明环境变量传给 worker，不写报告。新运行不接受 `sqlite_journal` 或平行请求恢复账本；请求额度和原生提交/恢复由当前 RunContext 承担，重复 action/恢复不会重置额度。旧账本 Case 读取仅保留历史语义。问题数、字符/token、图片、请求/响应、账本磁盘和时间分别准入，取工作区及运行预算的交集。token 计量要求显式本地 BPE 文件、SHA256、pattern 与 special tokens，不按编码名联网下载；revision 为所有者声明，不等于已验证远端部署。取消宽限必须容纳实际 HTTP/账本排空预算。

历史阶段曾使用目录 v4、工程规范 2.3、检查报告 v3、运行 manifest v4、原生 journal v2；当前新准备为 manifest v5 / demiflow_forge_runtime_v1，原生事实仍由统一 RunJournal 承担。运行 v4 额外绑定修改范围及封存基线；旧 v3 在未升级工作区保留原规则，升级后须重新准备，历史事实不补写。平台变化后应显式绑定并重新检查。diagnostic.category 区分 unknown、unsupported、denied、invalid，仍保留 FL/OP 编号和源码位置。

## 内容观察与任务范围

`workspace audit` 将明确绑定的通过报告与任务开始时的内容基线分开维护。前者回答检查依据是否还对应当前内容，后者回答新增/删除/修改是否在允许路径内。重新检查、刷新、巡检都不会移动任务基线。

范围仅覆盖声明的作者根及工作区控制投影；默认 flows/operators，不扫描数据行、图片库、运行账本或 training。Notebook 若明确纳入，则完整哈希已保存输出。未启用/未覆盖、超限、读取失败必须可见；这不是业务语义验证或恶意代码沙箱。

CLI、独立 watcher、HTTP 和 Web 共用公共核验用例。prepare/start/resume 使用现场核验，观察缓存没有执行授权作用。具体命令、启动方式见[多端接入](forge-client-integration.md#工作区变化核验)。


## 固定历史表的读取边界

Lance 位置映射复用原生有界派生索引，按完整键解析；不得猜测路径或把旧引用换成最新版本。
控制台仍要求登记实际物理表路径。预览检查固定版本所引用的数据/删除文件和目录，
不遍历全部历史版本或无关文件；拒绝实际使用路径中的软链接及不支持的外部 fragment、overlay、稳定 row-id 元数据。
查询禁用未核验的外部标量索引。元数据、解码、投影、响应字节和 worker 耗时均保持显式预算。
读取历史表不产生受管 run，也不代表历史代码已经通过当前契约或业务质量审核。
