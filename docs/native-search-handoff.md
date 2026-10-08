# 交接与切换单

原会话：`01a0ee22-9984-7502-9b1f-cf7c3293b135`（remote-ssh-discovered:devbox-codex）。本次已读取会话并核对实际代码。原窗口仍负责 demiwtg/preparation/concepts，本交付没有修改业务提示词、审定政策、结果或运行目录。

## 可审查交付

- 隔离仓库：`/yzp/zhaozy/yangzepeng/0905/.worktrees/demiflow-native-search-20261001`。
- 实际工作树基线提交：`ad46632fad8c3142326f086c72638759b9687bf4`；根目录 BASELINE.json 记录原工作树逐文件摘要及来源 HEAD。不是从干净旧 HEAD 重新实现。
- 查看任务差异：在隔离仓库执行 `git diff ad46632 HEAD -- demiflow pyproject.toml README.md docs tests tools`。
- 安装包位于 dist/，安装与自定义来源说明、完整 Dataset API 改动表见 `docs/native-search.md`，验收与限制见 `docs/native-search-validation.md`。

## 初次交付的协调方案（历史）

1. 在合入前重新运行 `python tools/check_native_search_cutover.py /yzp/zhaozy/yangzepeng/0905/demiflow`。检查结果只读；本次最终预检中 README.md、demiflow/collect/contracts.py、demiflow/collect/session.py、demiflow/collect/web.py 和 demiflow/data/dataset.py 均需三方合并。若原窗口又修改其他文件，以新的报告为准，不直接覆盖。
2. 在目标独立解释器准备本地 wheel 的 `[search]` 依赖。该 wheel 仍名为 0.2.0，未发布包索引；按交付文件及 SHA256 识别，不从索引猜版本。不要更换当前测试进程的环境。
3. 业务入口改为 `WebSession(search=SearchConfig(...))`，去掉该入口的 SearxNGService/search_url 和运行环境参数。声明与旧测试相同的来源及明确语言；代理凭据由目标环境重新提供 Secret 引用。本文不携带凭据，也不要求平台猜查询语言。
4. 使用更新后的 SEARCH_RESULT 保存新回执，核对 partial/unsupported_parameters/interrupted 的处理。保持概念行流式粒度，不增加业务子 pipeline；不改变提示词、政策或表内容。
5. 原窗口确认合适的切换时点后，合入经过三方检查的差异，先完成已授权的小批业务入口验收。没有授权本次运行 3,315 条全批或写公共表。
6. 发生回归时，让新入口回到明确声明的旧 service/search_url，使用旧独立环境。新旧缓存分开保存，不能通过改写历史回执伪装内核运行结果。
7. 确认所有旧消费者已退出后再单独退役其服务与运行依赖。历史查询、原始响应、SQLite、日志、网页对象和版本证据保留；不要按目录名批量删除 _demiflow 或旧服务。

## 初次交付的协调状态（已被下文接入取代）

本会话可读取原聊天，但没有可调用的跨聊天发送工具，因此未向原窗口发送完成通知，也未取得业务切换时点的确认。交付代码和安装包已准备好；业务接入仍待原窗口按此单协调，不应把本次完成实现理解为现役业务已切换。

## 用户授权后的实际接入 · 2026-10-01

用户随后明确要求改造该会话的 pipeline，统一接入新平台。本次已基于实际工作区再次快照、三方合并；原窗口新增的公共文档库、注册接口及其他未提交改动保留。改动先在隔离目录使用业务自身 `env/bin/python` 通过平台 83 项、业务 40 项回归，再按逐文件摘要保护合入。

- 实际平台目录：`/yzp/zhaozy/yangzepeng/0905/demiflow`；业务入口：`demiwtg/preparation/concepts/concepts_pipeline.py`。
- `Dataset.search_web` 调用保持，WebSession 改用原生 `search` 配置；首次与补查均逐请求传递 `search_language`，保存完整来源、参数、错误和缓存回执。业务不再声明旧服务或独立解释器。
- CLI/JSON/notebook 同用 `search` 映射，代理只保存 `secret_env`。公共文档库和原有取证/阅读额度继续生效。提示词、审定政策、模型协议和公共结果表没有修改。
- notebook 读取 `preparation/concepts/runs/native_platform_20261001/config.json`，保留四概念范围、语言、模型与调用额度，使用新 run 和数据根；`RUN_PIPELINE=False`、`through='plan'`。本次回归不发真实模型或外部来源请求。
- 主环境只补装原先缺失的 8 个依赖，不升级或降级已有依赖。lxml/typer 允许并验证当前环境已有的小版本。
- 接入前文件副本、变更清单及后续实际目录测试日志在 `_demiflow/native_search_integration_20261001`。旧实验配置、SQLite、网页对象、模型响应及正在运行的服务均未删除或停止。

“已接入”表示实际源码和新启动入口使用新平台，不表示已经启动新业务批次，也不表示运行中的 Python 进程已热更新。已导入旧模块的 notebook 应重新启动 kernel，再读新配置。撤回个别文件前必须重新比较摘要并合并后续改动，不能整目录覆盖备份。

## 2026-10-02：可选的查询级连续失败保护

`SearchConfig.query_failure_limit` 默认 `None`，启用正整数后，对整个原生检索会话统计连续不可用查询。来源均正常但结果为空时不算服务失败；部分来源故障且合并结果仍含结果、答案或信息框时也不算失败。所有来源失败、结果合并失败，或故障来源只剩正常空结果作为回退时计入失败。到达上限抛出平台 `ServiceStopped`，由 Dataset 流停止投喂并收尾在途模型请求；来源原始尝试在抛错前已经持久化。指标为 `query_consecutive_failures`、`query_stopped`。单源暂停策略和不自动重发验证码请求的行为不变。

3,315 概念实际启动暴露了必要性：Wikipedia 关键词源 429 后，summary 来源对长查询正常为空，不能把这种部分失败长期当作正常检索继续消耗模型。本轮配置上限为 5；41 个原生运行测试通过，包括正常空结果、部分可用、空回退、暂停状态、停止后无新请求及持久回执。此保护本身不保证检索覆盖或证据质量。


### 通用代理链、按域名路由与获取排除（2026-10-02）

SearchConfig.proxy 支持显式声明 `{"chain":["http://upstream.example:3128",{"secret_env":"SOURCE_PROXY"}],"allowed_domains":["source.example"]}`。Secret 在执行时解析，原生运行持有绑定 loopback 的认证 CONNECT 转发器；不终止目标 TLS，不把凭据或本地临时转发地址写入检索缓存身份。链路当前支持 2–4 跳 HTTP CONNECT 代理及目标 HTTPS/443，目的域名明确限定，连接随 Dataset 资源关闭。检索身份根据原声明及凭据摘要计算，重启后的临时监听端口不导致重复搜索。

WebSession 的 `fetch_proxy_routes` 为域名到同类代理声明的映射，匹配最长域名后缀；可用 `'*'` 声明未匹配域名的默认路由/代理池，具体域名规则优先。没有默认路由时继续使用 `fetch_proxy_url` 或原直连配置。每次重定向都重新选择路由。代理地址、源域名、业务站点类别及选择哪一跳完全由业务 run 配置提供，平台不内置特定公司或目标网站的策略，也不将域名后缀当作自动地理定位。

`search_fallback.session_pool` 可声明与主搜索相同契约的可更新会话池。备用搜索复用原生 `RenewableSearchRoutePool` 的有界创建、替换、连接复用、健康和关闭机制；其 `routes` 可留空，`route_attempts` 不得超过池的初始 size。旧未配置会话池的回执身份保持不变，重放已选结果不重新搜索。此能力不内置代理供应商、账号、地区或搜索引擎选择；这些由调用者的 factory 和配置给出。

WebSession 同时接受 `blocked_domains` 和有序 `fetch_url_rules=[{"name":"rule-id","action":"exclude|allow","path_pattern":"regex"}]`。域名排除优先，路径规则按首个匹配项执行；无匹配时保留。排除在下载/公共复用及重定向前生效，返回 `excluded` 与命中规则，直接排除的 URL 不发 HTTP、不消耗运输尝试、不删除历史成功文档或覆盖原回执。历史重定向目标也按当前规则检查；已发生的网络尝试记录仍保留。默认空规则不改变其他 pipeline。平台仅执行显式规则，不把“电商”“专业”做成隐藏语义分类。

## WebSession multi-route admission and completed receipt reuse (2026-10-01)

`WebSession(search=..., search_routes=[{name, proxy, interval_s}], search_request_interval_s=3, search_route_cooldown_s=1800, search_route_attempts=2)` provides generic route scheduling outside source adapters. Per-route concurrency is one; global HTTP admission remains bounded. Captcha/rate-limit/auth failures cool down that route; two consecutive ordinary failures do likewise. Cooldowns and query route events persist in `native_search_route_health/events`; all routes cooling causes ServiceStopped. No automatic captcha solving or unbounded retry.

`search_reuse_configs` explicitly allows completed ok/no_results source receipts from configurations differing only in proxy. Native source/runtime identity, all other config values and exact query parameters must match. Reuse keeps the original receipt ID and stores a `native_search_reuse` link rather than duplicating HTTP records. Errors and reservations retain normal identities. New modules `collect/search_routes.py` and `collect/search_reuse.py` do not change source-adapter/runtime request semantics. Actual route choices, Secrets and allowed destinations belong to caller config. Tests cover independent cooldown, persistence, bounded failure, global/per-route pacing and sibling reuse.

Cached route receipts do not constitute fresh route-health observations: replayed failures remain failures but do not extend cooldown, and old cached successes do not reset fresh failure counts. Route events prefix these observations with `reused:`. The pool owns the aggregate query breaker; native per-source pause remains active. A restart/expiry regression preserves old failed receipts with zero new HTTP and admits the next new query.


2026-10-01：HTML空表格产生仅换行块时，store_document读回验证现在在fetch的parse_error边界内拒绝无效文档，避免公共库登记异常中断整条流。有效parser v3字节与缓存身份不变；完整案例在concepts全量run的invalid_document_incident.json和invalid_document_local_validation.json。30项公库/文档测试通过。


2026-10-01 路线限速补充：SearchRoutePool 在fresh native call之前等待单出口next_ready，使30秒路线间隔不消耗30秒worker执行deadline；缓存查询不额外等待。HTTP准入gate仍校验最终间隔/并发，未修改native runtime/profile身份，已有成功回执继续复用。8项相关测试通过。


### 2026-10-02：按域名的下载代理池与稳定搜索回执

WebSession.fetch_proxy_routes 的域名值除单个代理声明外，可声明 `pool=[{name,proxy,interval_s}]`、`cooldown_s`、`failure_limit`、`reuse_completed`。每跳按最长域名后缀重新选择；成员并发1、独立限速，403/429立即冷却，连续网络/5xx按阈值冷却，健康与每跳结果存入原生SQLite。已有网络重试额度是唯一重试边界，全部出口冷却抛ServiceStopped，停止投喂并收尾。reuse_completed显式列旧代理，只复用相同获取参数、解析器和文档库身份的已完成回执；旧终结失败仍为失败，不因换代理自动重试。初次准入等待不算HTTP期限，重定向保留原期限。

搜索池保存已选中的完整查询结果，复用要求相同原生runtime、搜索配置（除代理）、查询参数，并且旧profile仍在现役或显式复用声明中。旧日志按最后成功选用的原生来源回执/复用绑定恢复，避免换出口时另选一份成功搜索而改变P2材料。失败回执和HTTP计数保持。新请求只走现役路线，旧出口只读复用。下载池扩展位于fetch_routes.py；CONNECT实现proxy.py原字节保留以维持现有native指纹。供应商、地区、会话串、目标域名仍是消费方配置。


## 2026-10-02：可选自动检索准入与通用模型准入

`WebSession(search_adaptive={...}, search_routes=[...], search_route_failure_limit=1)`启用原生池的有界调速，默认None保持已有静态行为。完整参数在`collect/search_admission.py:adaptive_policy`校验：min/initial/max_concurrency、initial/min/max_interval_s、window_s、min_samples、recovery_s、latency_p95_s与升/降失败比例。运行时的并发和间隔不进入source profile；既有SearchConfig请求语义与指纹不改。启用时外层控制器取代静态search_request_interval_s，并限制source执行准入，实际HTTP仍经过共享gate与单路线gate。

只按真实来源尝试观察健康，缓存回放不升降速。健康窗口增加一路并将总间隔乘0.8；来源拥塞减半并发、间隔乘1.5；持续网络失败比例或成功HTTP的P95恶化也减速。已有在途调用正常结束，后续入口才收缩。路线失败独立计数/冷却；每查询attempt上限和全池失败边界保持，不无限循环到成功。`native_search_admission`及`native_search_admission_events`保存状态和调速依据，同一声明重启恢复控制状态。供应商、目标域名、地区与会话生成不在平台内。

路线可显式声明`initial_cooldown_until`（UTC Unix时间），用于把已知预检故障带入首次调度；不会制造搜索请求回执或释放已有更长冷却。到期后走正常有界调度，无需编辑配置。业务应保存该值的实际预检证据。

`execution.adaptive_requests.AdaptiveRequestGate`可作为模型节点共用request_gate，声明初始与最大容量、增量、成功窗口、延迟阈值和恢复等待。只在原生模型实际执行分支记录时长，journal lookup/replay不进入门；HTTP/传输失败保留原有错误和熔断，不自动重试。每action从配置初值开始，可选state_path仅记录控制快照与事件，预算仍由原生SQLite累计限制。`RequestGate`静态行为保持。

可选 `coalesce_inflight_failures=true` 避免同一批在途失败反复减半：按调用实际耗时推算开始时刻，已在最近一次降档前发出的请求失败仍保留错误和熔断计数，但不再重复降低容量、延长恢复等待或清空成功窗口。降档后新发请求的失败仍可继续降档；缺失耗时不能证明属于旧在途批，维持原行为。默认 false，旧调用方不改变。仅增加一个时间标记和一个累计计数，不保留请求集合或载荷；不取消在途调用，不重试请求，也不关闭已有连续失败/鉴权保护。运行快照提供 `coalesced_transient_failures` 供核查。修改源码不表示已经切换正在运行的冻结进程。

可选 `transient_min_failures`、`transient_window_s`、`transient_failure_ratio` 为普通瞬时故障增加降档窗口：同时达到失败数量和比例才减半。默认 `1/60/0` 保持单次降档。窗口只存最近至多256个数值完成观测并按时间淘汰，计数上限也为256，不保存请求或载荷；低于门槛的故障不清空成功窗口、不延长恢复时间，但仍保留原始错误并计入独立的连续失败保护。`RequestGate.result(backpressure=True)` 是立即降档的通用压力信号，原生模型HTTP429及5xx保守使用此信号，明确服务压力不被普通网络噪声窗口延迟。旧在途失败合并规则仍适用。鉴权、预算、连续失败停止与在途收尾保持；此机制不重试未知结果。观测增加 `deferred_transient_failures` 和窗口计数。该候选能力尚未切换正在执行的concepts full_v10冻结运行。

### 2026-10-02：轮换连接与跨路线故障窗口

搜索路线可声明 `reuse_connections=False`，默认仍复用连接。该路线一次查询的全部 adapter 调用结束后关闭空闲 worker 及其 TCP 池，下次新查询重新建立连接；查询内跳转和来源语义不改，不关闭其他路线的在途工作。实际出口由代理供应商分配，平台不承诺新连接一定得到不同 IP。该调度选项不进入 native source 指纹；选定的旧查询仍直接返回同一回执。代价是每个新查询增加 worker 启动及连接握手。

`search_adaptive` 可选 `failure_window_limit`（默认 None 不启用）、`failure_window_s`、`failure_window_ratio`、`stop_s`。窗口内新失败达到数量与比例两个门槛时，先持久化来源回执和控制状态，再抛 `ServiceStopped` 停止新投喂；成功穿插不能清空整个故障窗口。当前已发请求保留正常收尾路径，后续执行在暂停期限内仍拒绝新请求；到期只解除准入阻止，不会自动重新启动已退出的 Dataset action 或重试付费模型。长时间停顿后的旧成功样本不能单凭一个新请求触发提速。

代理供应商、地区、是否用轮换地址以及逻辑调度通道数仍由业务配置决定。多个逻辑通道不能被报告为相同数量的独立出口 IP。


`decrease_on_single_congestion` 默认 True 保持原行为；显式 False 时，单个来源拥塞仍参与失败窗口及路线冷却，但全池调速依据累计失败比例或成功HTTP时延，避免把互相独立的单连接出口故障直接视为整个代理池过载。已有故障窗口停止阈值不被关闭。该选项不改变查询、源指纹、每查询attempt上限或故障回执；消费者必须根据真实出口模型选择。新增覆盖此分离行为的测试后，相关回归33项通过。


### 2026-10-02：批量导入与实时消费并行、失败查询冻结

DocumentLibrary 对已有完整索引只读取并校验 schema/object-directory 元数据，不再每次 lookup 执行初始化 INSERT；真正初始化才申请写锁。SQLite 等待使用声明的 lock_timeout_s（此前固定30秒），publish 在任何读取/写入前 BEGIN IMMEDIATE，避免延迟事务的读转写锁冲突。没有启用 WAL、改索引身份或重写对象。锁超时仍是技术故障，不能转为来源或概念不成立。

流执行器对内部阶段异常也发送有界收尾信号，使已登记模型请求按其原期限保存响应；向调用者仍抛原始异常。外部直接取消保持立即取消，硬收尾期限不取消。此前只有 ServiceStopped 触发这项保护，SQLite错误会取消付费请求。

SearchRoutePool 现在固定保存完整失败查询以及成功查询，同一 action 相同查询合并等待，换线路/重启不重新给已完成失败分配新尝试。旧查询回放不更新线路健康或绕过全池停止发送新查询。旧缺失聚合记录可显式 restore_results(entries, provenance=固定来源)：校验参数、声明profile与原生来源receipt_id/状态，已有选择不覆盖，不执行HTTP、不修改源回执。该维护入口不解释业务结论，不可用于重置预算。原生源文件及runtime_id保持。

### 无固定间隔与后台会话管理（2026-10-02）

`search_adaptive` 的 `min_interval_s`、`initial_interval_s`、`max_interval_s` 可同时为 0，表示仅用并发容量约束全池请求，不设置全池发车间隔。零间隔预约不会在来源 worker 启动期间串行持有发车锁；有间隔配置保持原有实际 HTTP 起始顺序。单路线间隔、请求次数上限、来源失败和认证保护仍独立生效。

`adjustment_scope='query'` 要求 `failure_window_scope='query'`：容量调整只观察经过有界换路后的查询结果，单路线失败但查询恢复不算全池失败。路线原始错误及隔离照常记录；缓存重放不作为新成功/失败样本。默认 attempt 的持久控制器身份保持不变，显式改变策略必须保留或审计迁移旧故障窗口及暂停期限，不以更换策略清空额度。

`search_session_pool` 可显式启用 `background_maintenance=True`，由当前 Dataset action 的 asyncio 任务维护和收尾，不要求业务启动独立守护进程。`worker_reserve`（默认0）与 `worker_prepare_concurrency` 控制本地 worker 预启动；当 SearchConfig 未固定 language 时必须声明 `worker_language`，与实际请求上下文匹配才可直接复用。预启动不发目标探测 HTTP、不建立目标 TCP/TLS、不把未测出口标成健康；`worker_ready`、`healthy`含义不同。仅为新查询启动管理任务，纯回执重放/检查不新建代际或 worker。坏会话先隔离，旧资源后台关闭，备用会话领取不等待旧连接关闭；shutdown 等待准备/清理任务退出。创建窗口、TTL、配置/认证失败保护持久化；供应商及 SID 生成仍由业务 factory 声明。

备用 worker 目标只统计当前可领取、未临近到期的空闲会话；使用中和仍在单路线间隔内的 worker 不占备用名额。`prefer_unused_fallback=True` 可让一次查询失败后的剩余有界尝试优先取尚未发生真实成功请求的代际，再按 worker 准备情况选取；不增加尝试次数、不保证新 SID 对应不同 IP。默认关闭以保持现有路由选择。该选项由业务决定，平台不写入 Google 或代理供应商专用规则。


## 2026-10-02T13:40:52.216709+00:00 Optional in-process search recovery

`search_adaptive.transient_failure_action="pause"` adds durable query-boundary pause/half-open recovery; default remains stop. `pause_trigger="route_shortage"` additionally requires a sustained inventory shortage (`pause_min_routes`, `pause_shortfall_s`). Available busy/paced leases count as viable; renewable inventory distinguishes healthy and untried. No provider/domain rule is embedded. Existing admitted queries drain under their original route allowance. Completed selected responses (including failures) bypass pause without network replay. Query-only feedback is required. Recovery probe and repeated-episode limits persist across restarts, authentication/configuration remains fatal, and the native request/admission identities are preserved. `native_search_recovery` / events expose observed state for read-only notebooks. Tests include continued failures with surplus routes, shortage, one probe, drain, cancellation, failed-result reuse, durable limits and renewable maintenance. Shared/frozen related suite: 99 passing tests.

`pause_route_inventory="healthy"` is an opt-in refinement for renewable pools. It counts routes with successful target observations; newly declared or locally prepared workers cannot mask a sustained shortage by continual refill. Busy or paced healthy routes still count. Pools without a separate health inventory retain their eligible-route count. The existing failure-pressure and sustained-shortfall conditions must both hold, so a cold start or a few failures alone do not pause the stream. Default `eligible` preserves prior behavior. This recovery policy is excluded from admission/request identity and adds no network probes, queues, or growing state. Three boundary tests cover cold refill, failure thresholds, busy/paced health, static compatibility, and unchanged admission identity; the related shared suite passed 80 tests on 2026-10-02. Production adoption and actual health remain consumer validation decisions.


### Source-scoped static route failure isolation (2026-10-07)

`WebSession(search_failure_scope="source", search_routes=...)` opts into per-source
route health. The default `"pool"` retains the existing fatal recovery policy.
Source mode requires one declared engine per request and a static route pool;
renewable session pools are rejected explicitly. Source + network + route
cooldowns persist in `native_search_source_route_health`; quarantine for one
source never quarantines that route for unrelated sources. Initial preflight
route quarantine, route attempt limits, pacing, HTTP concurrency and source
request budgets still apply. The existing adaptive controller may reduce
concurrency, but its aggregate failure window and legacy recovery fatal state
are not shutdown signals in source mode. Admission state uses a separate
control identity; native request identities and completed response selections
are unchanged, and legacy recovery history is preserved.

When no route can admit a request within its configured cooldown wait, the
result has `retryable: true`. Unsent suspended source receipts get the same
classification. They are not frozen as completed selections. `search_web`
leaves these task ledger entries retryable and emits explicit deferred evidence;
its next action restores the saved input even with empty upstream. This does
not create an unbounded in-action retry loop. Consumers must retain deferred
status when summarizing completion. Actual attempted failures remain completed
receipts under the existing route allowance; no paid request is silently reset.
Cancellation, real shared shutdown, programming/config validation errors still
propagate. Credential rejection returned by a source quarantines that source
route; it does not stop unrelated sources.

Health memory is bounded by declared routes × declared sources × declared
networks. Persistence reads known route profiles and only restores declared
keys, without loading historical profiles into memory. No per-source clients
or worker pools are created. Tests cover six failure classes, durable isolation,
legacy fatal-state migration, completed receipt reuse, deferred task recovery,
and compatibility with pool recovery and renewable lifecycle behavior.
