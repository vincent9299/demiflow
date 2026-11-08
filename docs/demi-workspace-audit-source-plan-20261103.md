# 工作区变化核验与后台巡检：源码级方案

状态：已获用户批准，A1–A4 已落地；本文保留设计时的职责和约束。准确的已实现接口见 [CLI/接入文档](forge-client-integration.md) 和 [HTTP 契约](forge-http-api.md)，实际测试、实现取舍见[实施记录](demi-workspace-audit-implementation-20261103.md)。路径相对本 demiflow 仓库。

## 1. 模块职责与依赖

```text
CLI / HTTP ───────────────→ workspace.py / runs.py
watch.py ────────────────→ workspace.py
                                │
                                ├─ audit.py：基线契约、文件清单、范围比较
                                ├─ workspace_worker：现有有界隔离入口
                                ├─ report_status / platform_snapshot：原检查依据
                                └─ 原生执行、RunJournal：原执行与结果事实

Web / Harness 插件 ──────→ 同一 HTTP 契约
```

只新增两个职责明确的实现文件：`audit.py` 与 `watch.py`。不创建 façade、通用 scheduler、repository、文件事件系统、Agent adapter 或另一个运行状态机。

| 文件 | 职责与拟改内容 | 不承担 |
| --- | --- | --- |
| `forge/audit.py`，新增 | 封闭基线格式、路径规则、流式清单、稳定读取、基线比较、范围判断 | 不 import workspace/runs/DemiForge，不调业务代码或写报告 |
| `forge/workspace.py` | v3 控制、基准绑定与 CAS、公共核验用例、缓存读取/发布、现有报告核验补充结构化证据 | 不启动永久循环，不读取上层 task.json |
| `forge/runs.py` | 准备/启动/恢复现场检查、运行准入绑定、版本兼容 | 不信任观察缓存，不自动放宽 writes |
| `forge/workspace_worker.py` | 增加有限 audit 操作分支，复用已有资源限制与错误 envelope | 不开放任意 Python 回调或命令执行 |
| `forge/watch.py`，新增 | 有界轮转、到期完整核验、变化合并、租约与退出清理 | 不执行 Flow，不创建通过报告 |
| `forge/console.py` | 新增 watch 命令入口，复用 registry 的解析、身份与数量限制 | 不在 HTTP serve 内复制巡检实现 |
| `forge/workspace_cli.py` | audit 命令解析及 JSON 输出；inspect 可显式 track | 不自行判断文件是否获准 |
| `forge/workspace_view.py` | 有限路由、能力声明、权限、字段/字节检查 | 不调私有任务格式，不提供远端随意改范围的权限 |
| `forge/web/app.js` 等 | 沿用单同步链，展示核验覆盖、时间、失效/越界路径 | 不替后台巡检调度、不计算判定 |

`workspace.py` 是既有公共用例归属，新增用例放在这里，避免再建一个只有转发作用的 service 层。`audit.py` 是下层算法模块：接收已验证的数据与 root，返回有界结果，不反向依赖调用者。

## 2. A1：契约与范围闭环

### 2.1 Workspace v3

新增 `FORMAT_V3 = demiflow_forge_workspace_v3`，v3 必须含 `audit`，其余沿用 v2 的 id、entries、locations、platform、budgets。结构示意：

```json
{
  "audit": {
    "reports": ["<inspection_sha256>"],
    "change_scope": {
      "baseline": "<baseline_sha256>",
      "writes": [
        {"path": "operators/normalize.py", "kind": "file"},
        {"path": "flows/experiment", "kind": "tree"}
      ]
    }
  }
}
```

`change_scope: null` 明确未启用；`reports: []` 明确未选巡检报告。未知/重复字段、布尔伪装整数、非法摘要、重叠歧义路径一律拒绝。最多 64 个报告、256 条 writes；相同 `(entry, parameter_path)` 不能绑定两个摘要。

`load_workspace` 只验证 audit 字段的封闭结构和路径/摘要形态，不在每次上下文读取时完整加载基线或哈希作者树。报告归属/完整性在绑定和核验时验证；基线不可读时，audit 返回不可核验，运行准入拒绝，但历史浏览和取消不被基线读取耦合阻断。入口取消登记后，原绑定报告显示登记依据已失效；不得为了让上下文可读自动删报告或选新基准。

显式 `audit enable` 将 v2 升为 v3，使用已有 `control_lock/write_control(expected=...)`；不触碰源码、数据或历史报告。v1 先走原有明确登记步骤，不在 enable 中猜入口。新工作区默认 v3，旧工作区不自动迁移。

必须逐处修改当前只识别 `FORMAT_V2` 的分支：

- `load_workspace` 的封闭字段校验；旧格式按原字段读取，v3 独立验证。
- `set_budgets/register_flow/unregister_flow/add_location/workspace_context` 的现代工作区判定。
- `runs._prepare` 的显式入口/参数登记检查。
- `init_workspace` 默认格式及 audit 初值。

不以“任何非 v1 都算现代”容忍未来未知格式。升级改变已绑定的 workspace format，旧报告可能 stale；明确重新检查，不静默改报告绑定。

### 2.2 内容基线格式

新增 `demiflow_forge_change_baseline_v1`：

```text
format, workspace_id, root,
watch_roots, inventory_rules_version,
limits,
entries: [{path, kind, sha256?, size?}],
workspace_policy_sha256,
created_at, digest
```

- entries 按规范路径排序、唯一，类型为 file/directory/missing；文件有完整 SHA256 和大小。missing 只允许显式观察根原本不存在，不把读取失败伪装成 missing。
- `watch_roots` 固定包含 flows、operators，可增加明确的作者文件根；workspace 控制投影单列，不当作普通字节哈希。
- 最多 64 个观察根，单路径 UTF-8 最多 4,096 字节，禁止根目录 `.` 和祖先/后代重复覆盖；可将 workspace.json 声明为可写控制，但不能把整个工作区当成允许任意变更的通配根。
- `workspace_policy_sha256` 对解析后的 workspace 值删除顶层 audit 后 canonical digest；必须验证完整工作区结构后再投影。
- `writes` 在 workspace 控制中独立保存。scope identity 为 `content_digest({baseline, writes})`，writes 排序/去重规范固定；替换观察报告不改变它。
- baseline 身份为整个文档除 digest 外的内容摘要。磁盘文件名必须一致；读取预算与 digest 均验证。保存采用同目录临时文件、fsync、create-only，现存不同内容拒绝。
- 基线读取上限 8 MiB；捕获在 worker 内完成，有界结果返回后父用例执行存储空间准入和短锁发布。跨进程大结果不塞入当前 1 MiB request；核验通过已绑定 digest 在本地读取。
- 捕获完成后发布前再确认作者清单、工作区控制与预计摘要没有变化；变化则拒绝发布。基线会包含开始任务时的既有修改，不作自动清理。

单次捕获受现有 worker 60 秒总限额；不能用两个各 60 秒的子步骤宣称总计仍受 60 秒约束。完整捕获/稳定性复核都在该预算内，发布的短锁/CAS 不重新做无界扫描。

基线描述的是捕获时的内容，不声称发布瞬间是文件系统事务点。发布前能观察到的输入变化须拒绝；未合作的外部写入若发生在最后观察之后，后续范围检查以这份已捕获基线比较，不能把新字节吸收到基线中。

`audit.py` 提供具体算法函数，建议职责划分：

| 函数 | 输入/输出与约束 |
| --- | --- |
| `validate_baseline(value)` | 封闭结构、路径/类型、预算、摘要；不访问业务环境 |
| `inventory(root, roots, limits)` | 稳定遍历与分块 SHA256，完整记录空目录/不存在观察根；有界计数，不读候选代码 |
| `compare(baseline, current, writes)` | 分类 added/deleted/modified/type_changed；逐条判断路径范围，保留总数/违规总数 |
| `scope_digest(scope)` | 稳定绑定原 baseline 与当前 writes；不含报告选择/心跳时间 |
| `metadata_hint(root, roots, limits)` | 只作触发提示；同样有限遍历，不返回 verified 结论 |

读取约束：逐目录有界枚举，不先 list 全树；拒绝 symlink/FIFO/socket/device、路径越界及登记数据根重叠。单文件 read 前检查额度，按块累计；打开前后/读取后核对 inode、类型、size、mtime、ctime；目录枚举前后校验。按完整快照前后清单检查发现竞态；不宣称能冻结不合作的写入者。

新建授权文件所必需的空父目录仅在其所有新增后代均获准时放行；不能用新增父目录掩盖其他越界后代。目录删除按被删除的全部后代逐项判断。预算不足不做局部“通过”。

### 2.3 基准控制用例

拟在 workspace 公共用例内提供：

- `enable_audit(root, expected)`：只做显式 v2 → v3 控制升级。
- `bind_audit_report(root, digest, expected)`：加载并核验报告，按 entry/参数替换对应项；保留其他项及当前 scope。
- `unbind_audit_report(root, digest, expected)`：显式移除一个观察目标，保留原报告和 scope；移除后显示未覆盖，不能留给修改 JSON 或删除报告文件完成。
- `begin_change_scope(root, watch_roots, writes, expected)`：只有 scope 为 null 时可建立；捕获原内容并绑定，不覆盖已有任务基线。
- `import_change_scope(root, baseline_path, writes, expected)`：本机显式导入中性基线，结构/身份/覆盖兼容后保存。不会要求当前字节等于旧基线；当前已有越界应在导入后如实报告。导入者须负责说明原基线来源，内容摘要本身不是来源认证。
- `amend_change_scope(root, writes, expected, reason)`：原 baseline 不变；reason 有界并在命令回执中返回，不声称已有审批系统。观察根或基线不能通过此操作重拍。
- `end_change_scope(root, expected, reason)`：显式清除当前约束，不删除基线/历史运行。绑定旧 scope 的准备/恢复结果随之冲突；已运行进程继续封存代码。

所有控制用例使用整个已观察 workspace digest 做 CAS。新建、修订、结束不由 watcher 调用。第一轮只通过本机 CLI 暴露；这不是远端令牌持有者的新权限。

`inspect --track --expected-workspace DIGEST`：显式检查成功后调用 bind；若控制冲突，报告仍保留且回执明确“报告已保存，巡检绑定未更新”，不将两个持久动作伪装成原子事务，也不自动重试绑定。

### 2.4 报告核验的结构化结果

`_report_status` 保留既有 status/report/changes/platform_checked 字段及历史语义，增加有界 `verification`：

```text
verification.complete
verification.checks[] = {kind, subject, outcome, reason_code}
```

outcome 区分 match/mismatch/unavailable；旧报告格式不支持为 unsupported。已确认内容变化与权限不足、表存储不可达、超时分别输出。不在 auditor 中通过匹配 changes 字符串猜错误类型，不把所有 OSError 都判为合法删除。

核验完整性与原报告检查结果分别判断：invalid 报告仍 invalid，旧格式不能通过观察重新获得执行资格；`platform_check=False` 的读取不能标为完整通过。第一版完整核验直接复用当前平台检查，不先引入跨轮全局哈希缓存。

## 3. A2：运行准入

### 3.1 现场范围验证函数

`workspace._verify_change_scope(root, expected_scope=...)` 在现有 worker 内调用 audit 算法：

1. 验证工作区与 scope/baseline 结构、身份、摘要、覆盖和预算。
2. 若调用方指定准备时的 scope digest，要求当前完全匹配；明确 null 不能在后续启用范围后继续按无约束运行。
3. 对基线作者根完整读取内容；比较 workspace 语义控制。
4. 完整且无越界才返回准入结果。范围不存在返回明确 unconfigured，不伪造范围通过。

所有准入都调用此函数，不调用 `audit_status`，不采用元数据相同作为免检理由；不要求 watcher 存活。

### 3.2 Run manifest v4 与封存

新 prepare 生成 `demiflow_forge_run_manifest_v4`，新增 `change_scope`：null 或 scope digest、baseline digest、writes 的精确绑定。活动基线在封存目录内保存一份受摘要约束的 JSON 清单，便于历史说明；不复制作者数据文件，也不覆盖现有封存目录。

`_load/_report/verify_seal` 明确处理支持版本；封存基线亦验证 digest。`run_worker` 继续通过 `verify_seal` 读取原生执行闭包，不自己解析开发任务，也不持续追踪工作树。

历史规则：

- 旧 manifest 原样可读，历史事实/表引用保持原身份。
- v3 旧准备/恢复在未升级的 v1/v2 工作区保留原准入语义。
- 工作区显式升到 v3 后，旧 v3 准备/恢复不能冒充包含新约束，须新 prepare；不得补写旧 manifest。
- 已启动旧进程保留封存环境；升级不杀进程。取消、提交核对及历史浏览继续可用。

格式判断需全文检索当前 `FORMAT`、`demiflow_forge_run_manifest_v3` 与各 seal 调用方，不能只改生成常量而遗漏读取/恢复。单纯增加观察报告不改变 scope digest，不应使已经准备好的 v4 运行失效。

### 3.3 准备、启动、恢复的落点

`runs._prepare`：在生成运行前验证当前 scope；保存有效参数检查报告、复制源码资源/平台 runtime 后，在发布 prepared 前重新核验相同 scope。manifest 绑定首次捕获的 scope，不悄悄采用期间改变的允许范围。失败只清理这次新建的未发布运行目录，保存过的检查报告如实保留，不删除别人的文件。

`runs.start`：当前代码在 execution_lock 之前做 preflight，锁内只重新核对 manifest/attempt/state。改为在拿到 execution_lock 并确认当前 attempt 后，在进入 starting 的最终准入区内执行完整只读 preflight。与 workspace 控制写入按固定 `execution_lock → control_lock` 顺序协调；bounded worker 不再次获取父进程已持有的控制锁。可将原锁外预检移入，避免无收益地重复两遍重检查。

进入 starting 后视为已准入运行，随后控制变化按运行中修改处理；监督者仍核验封存。不能将“几秒前通过”或 audit 缓存 digest 当作最终授权。锁等待非阻塞/有限，不改成无限等待；长核验期间其他控制操作可返回 busy。

`runs._preflight/_check_basis`：在既有报告、原参数、封存检查之外增加 scope 绑定与当前范围；表输出 CAS 与累计预算照旧。scope 不可读/超限/变化中均拒绝。

`runs._resume`：原有执行租约下，推进 attempt 前在相同控制边界核验；后续 start 再核验。不要为了复用新门禁阻断 cancel、reconcile、show、result_table，避免违规时连已提交结果都无法恢复登记。

原生 `Dataset`、算子、RunJournal 状态机与提交协议不增加 Forge scope 字段。准入在 Forge 层；旧业务脚本直接调用 Dataset 不因此自动受管。

## 4. A3：核验与巡检用例

### 4.1 公共用例与隔离操作

拟新增：

```text
audit_status(root)
audit_check(root, *, kind, report_digest=None, expected_policy)
```

kind 仅为 scope 或 report。一次检查一个对象，避免 HTTP 请求串行扫描 64 份报告超过现有 worker/服务时间上限。report 必须是当前明确绑定的 digest；scope 不接受客户端提供的临时 writes。

`audit_check` 复用 workspace_worker 的白名单操作。worker 执行完整只读核验，父用例在短锁内验证 policy 仍匹配后原子更新缓存；不改 inspection、scope 或 journal。检查与缓存发布是明确分开的职责，执行准入只用检查算法。

每工作区增加非阻塞审计操作租约，避免多个客户端/watcher 同时发起同类全量扫描；租约只约束审计，不成为执行准入的前置依赖。后台忙不能阻止运行的现场核验。锁顺序为审计操作租约在前，短 control_lock 在后；执行端不请求审计操作租约。

`audit_status` 只读取有限缓存及当前绑定，核对每项适用身份，并计算 age/overdue；不启动 worker、不扫描作者文件。缓存损坏/不存在/绑定不符为 unknown 或 pending，保留可解释原因。缓存格式采用 `demiflow_forge_audit_status_v1`，包含：

- workspace_id、policy_digest、revision；
- 每对象 kind/identity、scope/报告 digest、result、verified_at、due_at、覆盖与截断信息；
- 最近 lightweight_observed_at、pending 原因；
- watcher session/heartbeat（可缺失）与对象各自年龄。

手动 audit 不伪造 watcher 心跳；心跳不刷新 verified_at。时钟异常/重启后状态不确定时显示 unknown/overdue；进程调度使用 monotonic 时间，持久记录为 UTC，不能用负 age 把旧结果保鲜。

### 4.2 watcher

`watch.py::watch(registry, *, interval=30, full_interval=300)`：

1. 读取有界 registry，逐项目确认真实身份，取得该项目 watch 租约；单项目失败独立记录。
2. 对每个项目维护有限对象集合：scope + 明确绑定的报告；未绑定项只显示覆盖数量，不自动挑选历史。
3. 一次轻量观察得到文件/绑定/报告文件存在性提示；按已有报告 binding 选受影响对象。无法确定依赖时对有限的绑定集合做保守排队，不跳过。
4. 按项目/对象公平轮转，每次最多一个 worker；优先未核验/过期对象，但变化频繁对象不得垄断调度。
5. 即使轻量标记不变，到期也做真实完整内容核验。绑定报告文件直接核对 digest；不单靠 index 修订号。
6. 政策变更、项目删除、取消、预算或磁盘错误都不发布过期策略下的绿灯。移除 registry 项只停止观察，不删工作区。
7. 最多保留每对象一个 pending 位，失败有限退避，无永久线程池/无限消息队列。退出清理自己启动的进程，不接管既有运行。

轻量观察和完整核验都计入有限 CPU/时间/字节预算；只有轻量观察完成才更新其时间。单个报告/范围检查超时后轮转到其他项目，积压公开显示。后台不会遍历全部 .forge/runs、全部 inspections 或重新运行 tests。

### 4.3 CLI 与 HTTP

CLI 拟增加 `forge workspace audit` 下的 enable/bind/unbind/scope begin|import|amend|end/check/status；console 下增加 watch。scope 命令接收有界 JSON 声明或重复路径参数，由公共用例做最终校验。

`audit check --all` 如提供，只在 CLI 编排单对象核验，带整个命令 deadline；超时返回部分完成与未核验数量，不能回报整体通过。HTTP 不接受 `all` 或任意 root/path。

HTTP v1 增量端点：

| 路由 | 字段/权限 | 行为 |
| --- | --- | --- |
| `GET /api/workspaces/{id}/audit` | 无 query；现有读取权限 | 返回缓存、覆盖与鲜度；无扫描副作用 |
| `POST /api/workspaces/{id}/audit-check` | kind、expected_policy；report 类型另需 digest；使用明确的核验能力权限 | 检查一个当前绑定对象；只写观察缓存 |
| `/api/capabilities` | 增列操作与 `demiflow_forge_audit_status_v1` 能力 | 客户端据此判断支持，不猜字段 |
| `/changes` | 增加 audit 状态的轻量 revision 标记 | 不读取基线清单或执行 hash |

维持 64 KiB 请求体、现有响应上限、Host/Origin/token/登记范围及错误码白名单。状态内容限制 1 MiB。核验失败的业务结果采用正常有界结果体；请求非法、冲突、忙、超时/资源不足沿用稳定错误类别，不为每个失败再建一个错误类体系。

HTTP 不提供修改允许范围/关闭范围约束的端点。capabilities 必须区分读取与 audit-check；不能因为后台只读文件就宣称 POST 没有缓存写入。客户端不自动重放它。

## 5. A4：Web、插件与验收资料

`web/app.js`：

- 原 `syncStatus` 的“最近检查”改为“最近同步”，防止元数据请求被误读为完整检查。
- 沿用 `scheduleSync/syncView`；只在 audit revision 变化、首次打开或明确刷新时 GET audit，不新建一条定时循环。
- 显示“报告依据 / 修改范围 / 巡检鲜度”三项和最后实际核验时间，未配置/无法核验不呈绿色通过。
- 审计结果更新不触发无关 Case 图片、全表或源码重载；保留原 viewEpoch/selectionEpoch 与草稿保护。
- 手动“核验此项”只做 audit-check；“重新检查 Flow”仍走 inspect。刷新当前视图不产生新的报告或范围基线。
- 高亮越界路径、受影响入口/参数、明确的报告 digest；无选中 Case 对话或宿主专用状态。

不改工作面板布局、端口、SSH 映射和鉴权入口。Harness 插件只新增状态展示/有限核验调用，与独立 Web 平行，不添加插件私有指纹或私有 watcher。

拟更新文档：forge-workspace、forge-http-api、forge-client-integration、demi-harness-plugin-handoff，以及本设计对应的实际实施验收记录。任何 DeepSeek/Codex 接入文档都注明“宿主实现自行完成”，不把本次平台接口验收当成插件交付。

## 6. 测试落点与分阶段 Review

| 阶段 | 新增/扩展测试 | Review 必查 |
| --- | --- | --- |
| A1 | `test_forge_audit.py`；workspace/public API 测试 | dirty 基线、全部类型变化、目录边界、完整 Notebook、预算、投影自引用、CAS、原基线导入不抹除越界 |
| A2 | runs/run_boundaries/resume/run_faults | 不传 scope 仍检查；伪缓存无效；preflight 竞态；基线损坏；旧运行原样保留；cancel/reconcile 仍可用 |
| A3 | `test_forge_watch.py`；public_api/console | 无 UI 持续观察、双 watcher、报告篡改不改 index、mtime 不变仍完整检查、超时公平性、退出/磁盘失败 |
| A4 | `test_forge_audit_browser.py`；workspace_sync/public_browser | Web/插件协议模拟/CLI 读同一状态；关闭页面；草稿和历史选择；认证错误不重试；无真实模型/生产写入 |

测试使用隔离工作区、可控时钟与阻塞点注入，不以长时间 sleep 或真实模型调用验证调度。既有两条真实 pipeline 仅做读取/独立副本检查；第三种 Flow 用隔离数据完成实际准入。详细验收条件见配套矩阵。

每阶段先复审该阶段的实现与本方案差异，再运行相关测试；失败修正后复测。全量回归在模块边界/公共行为变化要求时执行，报告真实测试数与未覆盖项，不沿用历史测试数冒充本轮结果。
