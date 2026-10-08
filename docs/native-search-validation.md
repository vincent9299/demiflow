# 原生检索验收记录 · 2026-10-01

初次交付来自实际未提交工作树的隔离副本；以下 69 项及 14 次真实来源验证属于该阶段。随后用户授权的实际业务接入另见文末记录。入口为正式 `Dataset.search_web`。没有启动隐藏的 SearXNG HTTP 服务，没有运行模型或全量概念批次，没有写公共概念表/图片表。

## 自动化验证

相关测试共 **69 项通过**，命令：

```bash
python -m pytest -q tests/test_native_search_runtime.py tests/test_native_search_adapters.py \
  tests/test_native_web_dataset.py tests/test_evidence_primitives.py tests/test_stream_cancel_propagation.py
```

| 验收边界 | 证据 |
| --- | --- |
| 请求和解析 | Google、wikisearch、wikipedia、Mwmbl 固定响应；中/英/日/挪威语言站点，页号、安全搜索、时间范围；历史响应仅离线回放。 |
| 处理器与完整结果 | online/offline/dictionary/currency/URL 五类处理器不需要 Flask 请求上下文；MainResult/Image/File/Code/Paper/Answer/KeyValue、日期、infobox、suggestion/correction、engine_data 传输与合并。 |
| 排序和归属 | 固定来源次序、上游权重和重复项合并，断言合并分值 8、两个来源归属、分页标记；缓存重跑保持结果相同。 |
| 结果状态 | 正常为空、部分成功、全失败、验证码、HTTP 429、401、503、解析错误、参数不支持、响应超限分别断言。 |
| 隔离与认证 | 多 session 的语言、配置、故障状态隔离；同 session 的多网络出口与缓存区分；两组认证代理、本地认证来源 URL、跨站重定向认证剥离；凭据不出现在普通回执/快照/缓存内容。 |
| 背压与资源 | 10,000 个输入只创建 3 个消费者；来源/实际 HTTP/host 并发和间隔；连接复用；辅助 multi_requests 进入同一额度；超时/取消释放连接、进程组、配额和私有目录。 |
| 缓存与额度 | 重跑持久复用、跨 session 文件互斥、共享等待者取消、最后等待者中止、取消后不重发、重试 0/1 精确发起 1/2 次断连请求；暂停期满只恢复未消耗尝试的 admission。 |
| 非 HTTP 来源 | 真实本地 SQLite 只读查询、第二页、MainResult 和 KeyValue；不消耗 HTTP 配额。其他数据库驱动/端点未在线验证。 |
| Dataset 兼容 | 行与 bindings 保留；Arrow SEARCH_RESULT 保存新回执；执行器关闭 session；旧 HTTP 搜索、文档获取、证据读取和流式取消回归通过。 |

最后一次 review 补齐了合并后的 paging 标记、超时/取消时已接纳 HTTP 的回执，以及认证 URL 的传输与脱敏。这些变化经过上述固定响应和运行时测试。不是仅凭接口存在判断完成。

## 全来源清单

完整保留 **257 个原始适配器模块、344 个原始命名来源**，再加入 demiflow 的 wikisearch，共 **345 个命名来源**。另有 **33 个未提供默认命名配置的模块**，也逐一登记和探测。模块文件存在、配置可加载、初始化成功、解析正确、在线可用分别记录。

无网络加载检查共 378 项：290 项加载成功，88 项配置未满足。单看 344 个默认命名来源：270 个可加载、72 个默认 inactive、2 个需要 Tor/相关配置（ahmia、torch）。未运行这些来源的远程初始化，不宣称 270 个均在线可用。

所有来源的语言、分页、安全搜索、时间范围、处理器类型、导入依赖、配置线索、固定响应及在线状态见 `native-search-inventory.json` 和 `native-search-sources.csv`。清单随 wheel 安装，可调用 `demiflow.collect.search_source_inventory()` 读取。SQL/Mongo 可选驱动、API key、自建服务、Tor 等依赖没有假装成已配置。

## 真实请求证据

总共发起 **14 次来源尝试 / 14 次 HTTP 请求**，没有自动重试。代理通过环境变量引用，报告只保存变量名。

1. 实现阶段，通过 Dataset API 对 `Crassula ovata`（en）、`天牛`（zh-CN）、`文氏图`（zh-CN）各调用 Google、wikisearch、wikipedia、Mwmbl。12 个来源回执全部为 ok；用时约 11.53 秒，实际 HTTP 并发峰值 2，各 host 峰值 1。原始结果与参数见 `native-search-live.json`。
2. 工作区外安装 wheel 后，通过 Dataset API 查询 `Ginkgo biloba`（en），调用 wikisearch 和 wikipedia。2 个来源均为 ok。见 `native-search-installed-live.json`。

上述在线证据形成于最终回执、分页标记修复之前，保留原样及其当时的 profile/metrics；早期顶层 search_requests 的计数问题已在最终版本修正，原始报告仍保留，实际请求数取当时的 native_search.http_requests。最终代码再次通过确定性测试及重新构建 wheel 的离线安装验证，没有为重复对照追加真实请求。结果相关性只作样本观察，未进行事实或业务通过判定。

## 安装与基线

最终 wheel 在仓库外的临时目录安装，以 `python -I` 运行正式 Dataset API：随包 demo_offline 返回有效非 HTTP 结果，HTTP 请求数为 0；断言所有 demiflow 模块来自安装目录，宿主解释器没有导入 searx。安装包包含 257 个原始适配器、完整对应源码归档、许可证、作者和修改记录。验证工具是 `tools/verify_native_search_install.py`，最终回执与包摘要在 `dist/`。

安装验证复用了本隔离解释器已安装的依赖，不宣称在任意全新系统上完成了所有可选数据库客户端的安装。`pip check` 与 wheel/source 构建通过。运行时路径只指向安装包及调用者指定的缓存/对象目录，不查找 demiwtg/collect 或 _demiflow 临时运行环境。

供给的 SearXNG 目录没有独立上游 Git 元数据，不能给出可信的上游 commit。精确内容基线为 `bf277e831957f44df001778742432e4155c50017229084f6414b38e385291fae`；保留原始源码归档和每文件摘要。原始文件唯一集成改动是搜索协调器入口，另新增固定版本文件；原始适配器字节未改。AGPL-3.0-or-later、版权和本地修改记录随包交付。

## 尚存边界

- 在线只验证四个来源，固定响应也只覆盖这四个适配器的代表结构；其余解析器仍需要有针对性的响应样本/端点验证。没有持续负载或全网可用性结论。
- 上游验证码、IP 限流、解析结构变化、来源相关性不会因内化自动消失。有些适配器可能把异常页面静默解析为空。
- 无会话共享站点 UI、浏览器偏好、上游 Web 服务。Python 3.11+ 和 POSIX 是原生后端要求；Windows 未支持。
- HTTP 限流/代理作用于 searx.network ABI；非 HTTP 来源受工作进程、来源配额及超时控制。自定义源码必须可信，执行工作进程不是不可信代码安全沙箱。
- 响应字节和结果数有界，但不是整个进程 RSS 的硬限制；被硬杀的自定义代码不保证执行 finally。崩溃留下的不确定请求不会自动补发。
- 实际业务接入已按后续用户要求完成三方合并；既有进程、服务及历史证据保留。新旧接口与接入范围见 `native-search-handoff.md`。

## concepts 入口接入回归

隔离合并使用主业务环境的解释器执行：平台 **83 项通过**（包括公共文档库及 JSON/Secret 配置往返），concepts 正式入口 **40 项通过**。业务测试通过本地 HTTP 端点驱动真实原生工作进程，模型与正文采用固定响应；包含首轮 P2、条件补查、概念流式交错、部分来源验证码、两次运行共享公共正文、完整回执持久化以及旧下游接口。

本次没有新增真实外部检索或模型请求；此前 14 次真实来源验证仍按原版本与时间保存，不能将本地端点测试当作上游持续可用性证据。实际工作区复核日志和合入摘要位于 `_demiflow/native_search_integration_20261001`。


## 2026-10-02 HTTP admission and source deadlines

Worker execution deadlines exclude local host/global HTTP admission and pacing waits. The remaining execution budget is preserved across HTTP hops, so multiple hops do not reset the source deadline. HTTP/adapter execution still times out; cancellation while queued releases any acquired permits and emits no fictitious HTTP attempt. `http_admission_wait_s` reports completed admission waits separately. Source fingerprints and stored receipt identities are unchanged.

Validation: `test_search_worker_admission_deadline.py`, `test_search_route_pool.py`, `test_search_session_pacing.py`: 11 passed; six native local-HTTP fixture tests covering connection limits, cancellation/reservation, timeout/response bound, redirects, actual HTTP pacing and adapter helper requests: 6 passed. No external or model calls in these tests. Operational incident/patch hashes are recorded in concepts run `taxonomy3315_p1p2_v8_20261001/search_admission_fix_20261002.json`; per-business proxy selection remains outside the platform.


### Correction: worker deadline patch deferred; routed execution mitigation active

The worker patch described immediately above changes `runtime_id()` because that identity hashes native-search Python source. It was rolled back after a controlled live check exposed changed cache/cooldown profiles. The patch and its tests are preserved in the concepts run `search_worker_deadline_deferred`; they are not the active native worker implementation.

Active mitigation is in `SearchRoutePool`: a shared execution semaphore bounds source calls before native worker deadlines begin, preventing a slow source from consuming sibling deadlines while they wait for HTTP capacity. Per-hop HTTP admission/pacing remains; this is not complete exclusion of every admission wait. Native source files were restored byte-for-byte, and four real configured profiles were recomputed and matched to prior persistent profiles without network requests. Tests for route scheduling/reuse, session pacing, completed receipt reuse and native local-HTTP fixtures: 49 passed, 1 Dataset entry test deselected. Operational audit: `search_admission_rollback_20261002.json`.


### 2026-10-02：域名代理池与迁移后续跑验收

隔离生产runtime完成43项代理池、搜索回执复用、代理链、域名排除与文档获取回归；29项对象/文档库/离线回执迁移及P1固定输入回放通过。覆盖每跳选路、代理池轮换/取消/限速、单出口403/429冷却、Retry-After、404不误封出口、网络重试不放大、冷却跨重启、仅声明的旧profile可复用、另一出口已有不同成功回执时保持原选择、旧下载终结失败原样保留。

用户授权的真实验收：3个US Sticky10分钟声明通过公司CONNECT链访问IP回显服务，出口散列互异；新增两会话各一次中/英文Google查询成功。5条静态代理各获取一篇已在授权检索候选中的Wikipedia正文，5次成功并登记原公共库。仅是接通与单次成功，不代表持续成功率或地区独立核验。运行在同一3315范围、P1v8/P2v9下恢复，后续持续窗口决定并发档位。回执在concepts run的domain_proxy_pool_probe、domain_pools_relocation_validation与domain_pool_cutover_receipt_20261002.json。

注意：验证中发现proxy.py也参与native runtime_id，最初下载池声明扩展将其指纹改变；在正式恢复前已移到fetch_routes.py，并还原proxy.py字节，5条旧生产profile逐一精确匹配。中途两条动态Google探测的原回执及当时profile保留，未启动生产模型，不能把探测回执跨runtime静默迁移。


## 2026-10-02 自动准入验收

87项相关回归通过：search_admission、adaptive_requests、search_route_pool、search_receipt_reuse、search_session_pacing、map_prompt_async、prompt_http_stream，以及消费者的progress_view。覆盖真正并发HTTP、失败降级、恢复窗口、在途收尾、等待取消、跨重启搜索控制状态、预检冷却不伪造尝试、旧资料精确复用和本地SSE完整usage/零HTTP回放。通用包测试未产生付费模型调用。

消费者另做32条真实中英文Google查询，峰值HTTP4：28ok、4SSLError（无HTTP状态/0字节）；不称全线路健康，未观测验证码/429。真实模型高并发上限与整批吞吐仍在验证，不能用回归通过或配置80并发宣称已实现1000概念/小时。

后续生产放量出现跨多个路线的302（adapter 标作 captcha）及 TLS 失败，已受控停止；原始回执没有 Location，不能据此独立确认每个302都是验证码。业务完成出口回显：32个Sticky会话中28个回显成功且散列互异，4连接失败；这不代表各Google请求的出口已经独立确认。

轮换连接与故障窗口补充：32项相关测试通过，覆盖真实本地HTTP的新TCP/复用TCP对照、策略切换后相同源指纹及零HTTP回放、穿插成功的多路线失败仍停流、故障回执先保存、状态跨重启、旧成功样本过期，以及notebook吞吐保护。测试未访问外网或调用模型。

用户授权的单独真实Rotating验收40查询来自固定P1计划，中英文混合，34成功、6网络SSLError，40实际HTTP、峰值4，每次查询新建worker/连接，无验证码或429；成功HTTP平均3.51秒、P95次序统计约7.7秒。此小样本只支持有界恢复观察，不能称持续稳定通过。详细回执在concepts run的 `rotating_pool_acceptance_20261002.json`。


### 2026-10-02 06:50 UTC：并行公共库与异常收尾修复验收

56项文档库、wikitext导入、SSE、取消/线程测试通过；31项检索池、失败冻结/固定回执恢复、准入、连接生命周期及复用测试通过。生产隔离runtime另通过46项文档库/SSE/取消测试、26项查询池/准入测试。覆盖真实SQLite写事务期间读命中、配置等待期限、内部sqlite异常不丢已发模型响应且仍抛原错误、外部取消仍生效、同查询并发合并、失败跨重启/新增路线仍零HTTP、恢复来源身份拒绝不匹配。无付费模型测试。

在独立Wiki全量导入进程运行中，9个中英文正文/转义/重定向URL通过新的native fetch日志全部公共库命中，真实HTTP0；共享源码读取0.52秒、幂等发布等待10.52秒，隔离runtime第二次读取0.58秒、发布0.22秒。只是并行互操作验收，不保证未来永无锁等待。固定报告在concepts run的library_lockfix_acceptance_20261002.json和library_lockfix_runtime_acceptance_20261002.json。


### 2026-10-02 查询级故障窗口

SearchAdmission可配置failure_window_scope=attempt|query，默认保留按尝试统计；query仅统计新查询完成有界切换后的结果，缓存回放不增样本。路线的每次成功/失败仍驱动容量和冷却，decrease_min_samples控制降速前样本下限。共享源码及生产隔离runtime分别37项相关测试通过：恢复成功不误计最终失败、非连续最终失败仍停流、查询失败回执先保存、暂停/重启回放不再请求、无效策略拒绝。无外网或模型调用。供应商/代理数量/阈值由消费者配置；本能力不自动重试付费模型或放宽每查询尝试上限。


短冷却等待补充：自适应策略route_cooldown_wait_s默认0，允许0..300秒；搜索取路在有明确近期开路时间时等待，重复延后共享有限等待额度。零HTTP等待、自动恢复、长期冷却退出、取消及全池熔断优先测试通过；共享及生产隔离runtime各44项相关搜索回归通过。路线等待次数/耗时作为原生指标报告。供应商与模式选择仍由业务配置。

### 2026-10-02 · Renewable sessions (consumer-selected Sticky, no Rotating fallback)

`WebSession(search_session_pool=...)` now delegates bounded lifecycle to `RenewableSearchRoutePool`. The importable consumer factory receives a non-secret generation token and TTL and returns a validated proxy declaration using Secret references. Slots are exclusively leased, healthy native workers retain TCP pools, expiry/failure drains and closes the old generation, and replenishment has a durable rolling creation quota. Authentication/configuration failures stop new admissions. Route cooldown and Retry-After survive replacement; the shared query allowance and adaptive circuit do not reset when generations change. The static source configuration remains the journal/identity anchor and never receives fresh requests in this mode. `search_routes`, when supplied, authorize historical reuse only.

Retired source profiles remain authorized in the same run's durable history. Completed failed aggregate queries replay too. Pool size and TTL adjustments preserve history; an existing generation retains its originally declared TTL. Source receipt fallback uses bounded indexed batches, retaining declared precedence and original receipts. Native adapter fingerprint remains `f7c4ec6a1d6af3671b91cb0535a631bf9767332450cd1d2b6046fc3af92b47b1`.

61 related tests passed, including actual local TCP reuse/renewal with the native worker, cancellation, expiry during a lease, auth stop, rolling creation limits across restart, failed-query replay after retirement/restart, and source receipt precedence across more than 800 keys. The 19 related existing pipeline/pacing tests also passed (offline business fixtures, not factual-quality evaluation). Production acceptance remains conditional: first real bounded probe hit its failure circuit (37 successful HTTP responses, 8 TLS/network failures, 5 adapter CAPTCHA/302 responses across both probe portions); after the saved pause, the remaining 10 queries using the production two-attempt bound all succeeded with 10 fresh requests. Total source attempts 53, real HTTP attempts 50; the original 64-source-attempt ceiling was retained. No model calls or document downloads. This is not proof of sustained Google availability or distinct exit IPs.


2026-10-02 elastic-pool follow-up: shared and isolated runtimes each passed 68 related tests. FIFO acquisition treats busy eligible leases as backpressure; only absence of eligible generations consumes the finite exhaustion wait. A delayed previous release cannot close a new owner's busy lease. Optional max_size/reserve derives bounded capacity from existing global HTTP pacing without increasing admission; surplus healthy leases drain at expiry. Generation initialization reuses already verified history and shares one immutable profile sequence. Production's earlier healthy-queue timeout is recorded as a scheduler defect, not network exhaustion. Two real consumer sessions spaced 60 seconds apart returned 6/6 Google queries; this small sample is not sustained acceptance. Scheduling changes preserve pool identity, history, failed queries, native runtime fingerprint and cumulative budget.


2026-10-02：renewable pool 将排队与领取租约分离，领取数不超过执行容量；空闲临期会话提前退役；零HTTP过期最多重新领取两次并单独计数；全局 circuit 不连带污染每条路线的失败记录。新增真实异步 fixture 验证并发背压、无限临期有界停止、故障窗口不连带退役、临期不发HTTP，连同路由/pacing/connection及缓存观测测试共享与冻结runtime均89 passed。生产保留原native指纹、源配置、准入状态和回执，恢复后持续稳定仍待观察。


2026-10-02 first-HTTP scheduling repair: RequestGate.reserve waits for capacity/pacing before the source worker deadline, commits spacing at actual HTTP start, and releases unused/cancelled reservations. Routed sources consume that permit for the first hop; additional hops still acquire their own permits under the existing overall source deadline. This prevents slow global pacing from spending the first-hop deadline in a local queue. Shared and isolated runtimes each passed 141 request reservation, search, stream, cancellation and journal tests. Native source fingerprint, selected receipts, production policy and durable admission history are unchanged; production sustained availability remains under observation.

2026-10-02 Google 调度验证：新增无固定间隔的并行准备与容量上限测试；查询换路恢复不触发全池调整，失败查询仍触发原保护；后台空闲补充、慢关闭不阻塞换路、重放不启动后台、预启动语言必须与请求上下文匹配，并验证原生 worker 队列实际复用预启动 worker。24查询真实 canary 30 HTTP/24成功/6 SSLError 后恢复，32.49秒、峰值12；此轮发现来源默认语言为空造成预启动上下文不匹配，单独修复并补真实验收，不能将旧canary当预启动收益证明。业务回执在 concepts 的 google_unpaced_acceptance_20261002 与 google_warmlang_acceptance_20261002；无模型探测、无公共表覆盖。网络长期稳定和P2语义验收不由这些调度测试证明。


## 2026-10-02T13:40:52.216709+00:00 Optional in-process search recovery

`search_adaptive.transient_failure_action="pause"` adds durable query-boundary pause/half-open recovery; default remains stop. `pause_trigger="route_shortage"` additionally requires a sustained inventory shortage (`pause_min_routes`, `pause_shortfall_s`). Available busy/paced leases count as viable; renewable inventory distinguishes healthy and untried. No provider/domain rule is embedded. Existing admitted queries drain under their original route allowance. Completed selected responses (including failures) bypass pause without network replay. Query-only feedback is required. Recovery probe and repeated-episode limits persist across restarts, authentication/configuration remains fatal, and the native request/admission identities are preserved. `native_search_recovery` / events expose observed state for read-only notebooks. Tests include continued failures with surplus routes, shortage, one probe, drain, cancellation, failed-result reuse, durable limits and renewable maintenance. Shared/frozen related suite: 99 passing tests.
