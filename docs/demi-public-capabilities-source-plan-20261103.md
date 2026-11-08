# Demi 公共能力与多端接入：源码级方案

状态：方案已获用户批准并完成 C1–C4 实施；以下保留设计与复审基线，实际功能 / 测试 / 部署结论见 [实施验收记录](demi-public-capabilities-implementation-20261103.md)。

配套：[中粒度方案](demi-public-capabilities-design-20261103.md)、[复审与验收](demi-public-capabilities-review-20261103.md)。所有平台路径相对本 `demiflow` 仓库。

## 1. 改造原则与调用顺序

公共入口仍是 `demiflow.forge.workspace`、`demiflow.forge.runs`，外加访问登记 `console`。CLI / HTTP 直接调用这些函数；函数继续使用现有 worker 隔离、原生执行和固定数据契约。

`请求解析 → 身份 / 权限 → 公共用例校验 → 原生检查或执行 → 持久事实 → 返回引用`。

不增加 facade、service locator、repository、事件总线、通用命令分发器或另一套 DatasetRef。HTTP handler 只分发有限端点，不做按字符串反射调用 Python 函数。

## 2. C1：公共用例与无 UI 入口

### 2.1 `demiflow/forge/workspace.py`

现有 `workspace_context/workspace_changes/inspect_flow/describe_table/sample_table/attach_table` 保留。拟补：

| 函数 | 改造 | 保证 |
| --- | --- | --- |
| `source_text(root, path, expected_sha256=None)` | 从 `workspace_view.py` 移入，增加返回实际源码 sha256 | 只读 flows/operators 内 Python、最多 1 MiB、可校验已观察摘要；不导入候选代码 |
| `read_parameters(root, path)` | 接管 handler 内参数读取，复用 `validate_parameter_path/read_control` | 只读 flows 下 JSON、最多 64 KiB；返回原参数文档，不重复解释参数业务含义 |
| `table_media(...)`、`table_evidence(...)` | 给当前 worker 操作提供公开的语义入口，保持 expected_binding / 字段声明 / row_id 约束 | 隔离 I/O 与资源限制仍由 workspace_worker / 原生读取实现；HTTP 不再依赖 worker 私有协议 |
| 控制编辑错误 | CAS 冲突、绑定变化、锁忙等携带稳定 code | 保留原摘要算法及锁；不以 changes 标记代替内容摘要 |

新增公开读取先通过 `load_workspace` 确认工作区，再使用规范化 root；不能因为 HTTP 之前已经解析过项目，就让 CLI 的同一入口缺少工作区核验。`attach_table` 拟增加可选 `expected_ref`：在目录发现、物理表核验后且写入别名之前，将最终 DatasetRef 与该引用完整比较；既有调用省略时语义不变。它与 `expected`（已有别名控制的 CAS 摘要）是两个不同的前置条件。

这是补齐入口边界；不另造一个与 `workspace_worker.operation` 平行的调度层。media 公共返回继续采用当前有界数据及 MIME / binding 信息，HTTP 负责二进制编码响应，CLI 可输出结构化结果。不能增加先全读文件再检查大小的路径。

`source_text` 旧位置仅有内部 / 测试引用，实施前搜索全部调用方并更新到新位置，不留只为转发的旧函数。`workspace.py` 不反向导入 `workspace_view.py`。

### 2.2 `demiflow/forge/runs.py`

保留 prepare / start / resume / show / list_runs 的既有执行语义，新增或收紧：

1. `cancel(root, run_id, *, expected_attempt=<省略>)`：显式值必须与读取状态匹配；随后使用 `RunJournal(target, attempt=observed_attempt)` 完成状态变更，利用事务中的 attempt 防护。省略值仅保留本地命令“取消调用时的当前尝试”语义：读取一次并绑定该次，不能在竞态中重新选择最新 attempt。HTTP 不允许省略。
2. `reconcile(..., expected_attempt=<省略>)`：在父入口先捕获一次目标 attempt，再将明确值传到隔离 worker；在现有 `execution_lock` 内校验同一 attempt，再核对并登记提交。HTTP 必须显式携带。
3. `result_table(..., attempt=None)`：支持明确读取历史 attempt；继续检查原生 receipt / checkpoint commit、准确版本和位置绑定。原来的不带 attempt 读取当前结果行为不变。
4. `attach_result(root, run_id, output, alias, *, attempt, expected_result)`：按指定 attempt 调用上项，比较 `content_digest(ref.to_dict())`，再向 `workspace.attach_table` 传入 `expected_ref=ref` 创建别名；不能改成登记“当前输出”。新别名 create-only，存在则冲突，覆盖继续走已有表控制 CAS 管理操作。
5. 输出描述增加 `result_digest`，有已登记固定 ref 才提供；它不是已提交的额外证明，提交判定仍来自原生事实。保留 `retained_from_attempt`，不把复用旧输出伪造成重新执行。

现代运行 attempt 为严格整数（不接受布尔值）；历史无 attempt 的运行只在其 journal 本身确无 attempt 格式时接受明确 null。不能用 null 绕过现代运行的检查，也不迁移旧记录来生成 attempt。这里的“省略”用一个模块内默认标记与显式 None 区分，不增加持久实体：省略仅触发一次当前状态捕获，None 只表示历史无 attempt，整数表示明确现代 attempt。取消已结束的同一次运行可返回其已知状态；对旧 attempt 的控制请求必须冲突，不能操作新一次。

start 的公共函数已有 `expected_attempt`，默认值同样改为区分省略与明确 None；CLI 增加可选参数，Web / HTTP 强制传当前已观察值；resume 继续要求 `expected` 与 `expected_attempt`。start 的公共入口在省略时先捕获一次，后续 preflight 和执行锁内都核对该值，不跨重试选择最新 attempt。CLI 仅传递用户显式提供的值，省略时交由公共入口处理，避免入口间分别实现捕获规则。

attach 的核验可以在锁外读取固定历史结果；创建别名仍在控制锁内且 create-only。运行继续推进不改变已核验的固定 DatasetRef。物理旧版本被删除或位置被重绑时必须失败，不通过重取最新版本补偿。

### 2.3 原生执行错误与 worker 传递

`demiflow/forge/schema.py::ForgeError` 增加可选 `code`，保留 ValueError 继承关系与 `str(exc)`。只有跨端消费需要识别的错误才赋稳定代码；不对所有既有异常做大范围重构。

建议最小码集：`invalid_request`、`not_found`、`conflict`、`busy`、`resource_limit`、`operation_rejected`、`operation_failed`。认证 / 远程权限的 `unauthorized/forbidden` 归 HTTP。不要把任意 OSError 都当作参数错误或不存在。

`execution/run_journal.py` 的事务期 attempt / revision 冲突需要可识别类型：在原生 `demiflow/errors.py` 增加一个明确的执行冲突异常（同时保留 ValueError 兼容与原生 DemiflowError 归属），再由 Forge 边界转换成 `ForgeError(code='conflict')`。原生执行不导入 Forge。只覆盖对应冲突分支，不改 journal 格式、状态机、锁或恢复算法。

`workspace_worker.py` 的错误 envelope 目前只传字符串，必须同时传经过白名单校验的 code，父进程重建 ForgeError 时保留它；否则子进程中识别出的 409 冲突会退化成普通 400。继续限制消息 2048 字符与整体响应预算。底层未分类异常统一 operation_failed，不能通过匹配错误文本猜分类。

具体要收紧的既有抛错点包括：`write_control/detach_table` 控制摘要冲突、`runs.start/_resume` 的 digest / attempt 冲突、`workspace_worker` 的 preview binding 改变，以及前述 RunJournal 事务冲突。输入解析阶段的已知错误归 invalid_request；其他未知 ValueError 不一律映射成 400，避免掩盖内部问题。

`platform_worker.py::_capture` 只补自身明确的超时 / 输出超限错误分类，保持进程清理与有限输出。无需把平台 catalog 协议改造成全局 RPC。

### 2.4 `workspace_cli.py`、`run_cli.py`、`platform_cli.py`

新增 / 补齐命令均直接调用公共函数：

| 入口（拟新增部分） | 调用 |
| --- | --- |
| `forge workspace changes ROOT --json` | workspace_changes |
| `forge workspace source ROOT PATH [--expected-sha256 ...] --json` | source_text |
| `forge workspace parameters ROOT PATH --json` | read_parameters |
| `forge table media/evidence ROOT ALIAS --row-id ... --field-path ... --expected-binding ... --json` | table_media / table_evidence |
| `forge workspace run attach ROOT RUN_ID OUTPUT ALIAS --attempt N --expected-result DIGEST --json` | attach_result |
| `run start/cancel/reconcile ... --expected-attempt N` | 公共函数 attempt 防护；resume 已有 |

历史无 attempt 的明确表示应使用独立的 `--legacy-attempt` 标志，与 `--attempt` 互斥，不使用 0 或猜测 latest。该标志只用于确无 attempt 契约的历史结果，不作为现代 run 控制后门。

保留当前 CLI JSON 成功结构和退出码。`_output` 和 run_cli 的 emit 各自增加相同的 code 字段，避免为几行输出代码引入一个新 CLI service；仍区分“检查 invalid / stale”与“命令执行失败”。用契约测试防止两处错误分类漂移。

## 3. C2：HTTP 正式化与可选 Web

### 3.1 `workspace_view.py` 保留为唯一服务实现

历史文件名暂不重命名，避免纯文件搬运及转导入。源文件查看移出后，handler 只做 HTTP 相关工作。

| 路由组 | 公开操作 / 限制 |
| --- | --- |
| 全局 | `/api/capabilities`、`/api/projects`；鉴权后返回契约 / 登记清单 |
| `/api/workspaces/{id}/context, changes` | 上下文、变化提示；无 root query |
| `parameters, source` | 受限参数 / 源码读取；源码可绑定 sha256 |
| `table, sample, media, table-evidence` | 固定绑定、投影、有限页 / 单 Case、声明字段 |
| `reports, report, diff, inspect` | 有界报告、摘要身份；inspect 是显式 POST |
| `runs, run, run-logs, run-source, run-evidence, run-compare` | 有界列表 / 日志，明确运行及可选历史 attempt |
| `run-prepare, run-start, run-resume, run-cancel, run-reconcile, run-attach` | 显式 POST、运行权限、对象与 attempt 前置条件 |

所有 query 和 body 拒绝未知、重复、缺失字段；在读取 / 解码 / 发起 worker 前限制路径、请求字节与字段数。列表沿用现有 cursor 与 epoch 失效规则，不宣称滚动翻页是整个项目的事务快照。

服务端公共源码 / 参数 / media / evidence / attach 路由调用 C1 函数。HTTP 不直接导入 DatasetRef 或拼接登记结果。

写请求具体约定：

| 操作 | 必填前置条件 | 不确定响应的处理 |
| --- | --- | --- |
| prepare | entry、params；现有输入 / 输出 / limits 的封闭可选集合 | 不自动重试；查运行列表核对；不能仅凭相同参数认定同一运行 |
| start / resume | run_id、expected manifest、expected_attempt | 查同一 run / attempt；已启动 / 已续接不能当作再次操作许可 |
| cancel / reconcile | run_id、expected_attempt | 冲突后重新展示，不能自动改成 latest |
| attach | run_id、attempt、output、alias、expected_result | 查询 alias 对应引用；相同引用可确认已登记，不重绑其他引用 |

`POST /inspect` 响应丢失也不自动重发；检查会保存报告，即使不执行模型也不是无副作用请求。

错误映射：输入非法 400、认证 / 权限 403、不存在 404、版本冲突 409、请求或已知数据体超限 413、业务准入拒绝 422、忙 / 临时容量不足 503、非预期内部失败 500。客户端只自动重试读取；服务端不返回“写操作可安全重试”的泛化布尔值。原始 traceback 不返回浏览器。

`process_request` 的 503 改为小型固定 JSON 错误，仍在并发准入失败时立即返回，不创建工作线程。前端也必须处理空体 / 非 JSON 的代理或网络错误，不能只靠后端永远返回 JSON。

`WorkspaceServer(..., api_only=False, token_file=None)` 与 `serve` 增加参数；API-only 下 `/` 和静态资产 404，API 不变。认证、loopback、Host / Origin、CSP 默认保持现有边界；不增加跨域开关。

### 3.2 `console.py`、`workspace_cli.py` 的启动参数

`forge console --registry PATH serve --port PORT [--enable-runs] [--api-only] [--token-file PATH]`。单工作区 `workspace view` 只同步需要的凭据选项，API-only 的正式推荐入口是 console serve，不新建 serve 命令组。

token_file 读取位于 HTTP 启动逻辑：O_NOFOLLOW / fstat 核验当前用户、普通文件、0600；先限制到 256 字节再读，接受 43–128 个 URL-safe 字符的随机凭据（字符和长度校验不能证明熵，文档要求由安全随机数生成）。禁止将文件中的内容打印进错误；该模式启动输出只给无凭据地址，原临时交互启动模式保留私有授权链接。它属于服务部署，不混入 registry / workspace 内容摘要。

访问登记仍限制项目数、逐项核验真实 ID，单项目故障隔离。端口冲突失败；不查找空闲端口并悄悄替换固定地址。

## 4. C3：Web 作为普通 HTTP 客户端

`web/app.js` 保留现有布局、渲染与同步链，仅做契约相关改动：

- 启动读取 capabilities，核对支持的 HTTP 契约；不匹配时明确提示更新客户端，不猜测操作参数。
- 每个请求绑定 workspace / 当前视图 generation；取消或忽略旧响应，保持已有 race 保护。
- 对 start / cancel / reconcile / attach 发送看到的 attempt / result_digest；冲突提示重读，不自动重提交。
- 请求失败能识别结构化 code，也能处理非 JSON、超时与断线；保留旧内容 / 草稿，写入结果不确定时提供读取现状。
- 继续单条、可暂停的变化轮询，不添加第二条插件专用轮询，不为每个隐藏 Tab 保持后台请求。
- 鉴权失败显示需要重新打开有效授权入口，不能无休止把 403 当临时离线重试。
- 页面查看历史 run / result 时，后台列表可更新但选择不自动跳到新对象。

`web/view.js` 保持 workspace / run / table / report 的中性引用，不增加 Codex thread、DeepSeek session 或 token 字段。外部插件的连接地址由插件配置管理；页面目前仍用自身 origin，不为了预留跨域而先建通用 JS 客户端框架。

`web/index.html/style.css` 非功能需要不改。`workspace_view.export_report` 与打包静态资源保持有效，离线报告不得意外尝试初始化远程 capabilities 或同步任务。

## 5. C4：外部接入资料与验收，不假装交付宿主插件

拟新增：

- `docs/forge-http-api.md`：正式 HTTP v1 端点、类型、字段、预算、错误、权限、重试、旧客户端迁移。
- `docs/forge-client-integration.md`：本地 CLI、远程 HTTP、Harness 插件的连接方式与边界；包含无 UI 操作流程。
- `integrations/coding-agent/demiflow-workspace/SKILL.md`：通用 workspace 使用说明，不绑定 Codex、不改历史 forge v1 skill、不覆盖 DemiForge 项目开发规范。

具体宿主插件在选定实际目标后放在 `integrations/<host>/`，只依赖 HTTP 文档与宿主 SDK；不是本轮预先建立的空壳目录。本设计不承诺未验证的 UI 插槽 API。

验收用标准 HTTP 客户端构造第二消费者即可证明协议不依赖本 Web。真实插件交付必须另补宿主加载、可信后端凭据保管、连接断开释放、同源 / 代理预算和项目切换测试；这些不能由通用 HTTP fixture 替代。

## 6. 测试落点及实施边界

扩展现有 `test_forge_workspace.py`、`test_forge_runs.py`、`test_forge_resume.py`、`test_forge_console.py`、`test_forge_workspace_sync.py`、`test_forge_workspace_sync_browser.py`、`test_forge_onboarding_browser.py`，并按需要新增一个 `test_forge_public_api.py` 集中测试入口一致性和正式协议。不按每层再造一套重复 fixture。

重点是竞态、版本身份、响应丢失、权限与资源边界，不写仅验证“函数确实转发了参数”的镜像测试。native attempt 冲突的异常改动需跑对应 RunJournal 回归；新增启动参数跑 CLI / 实际 HTTP 集成；前端跑真实浏览器和离线导出。

若现有已安装平台快照与正在修改的平台源码不一致，旧运行仍使用原封存 runtime。测试使用新隔离环境 / 临时 workspace；不能自动 bind-platform、覆盖历史报告或把旧检查升级成新通过。

本轮设计只增加三份方案文档。后续实施不触碰 `demiwtg/training/`、旧 pipeline、Notebook 输出及生产数据；现有服务更新是最终验证后的明确部署步骤，不能作为测试副作用。
