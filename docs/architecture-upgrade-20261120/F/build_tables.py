"""Join reviewed per-symbol decisions to the finite static source inventory.

Writes documentation only. No analyzed module is imported or executed.
The concrete decisions below are author judgments, not scanner conclusions.
"""
import collections
import argparse
import hashlib
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
FIVE = {'demiflow.forge.' + n for n in ('schema','io','platform','platform_environment','operation_budget')}

# Each tuple: category, exact behavior/output, errors/limits, recommendation.
F1 = {
'io.no_symlinks': ('neutral_candidate','Path -> 原 Path；逐父检查 symlink，无持久状态','ForgeError；并发替换 TOCTOU 不在保证内','留原公开工具；无当前底座消费者，不为目录下沉'),
'io.read_json': ('neutral_candidate','Path/maximum -> JSON 值；no_symlinks+stat 后读文件','ForgeError/Unicode/JSON 转换；OSError 传播；默认2MiB，stat/read竞态不是硬内存界','留原；不能替代 read_bounded 的稳定 fd/身份语义'),
'io.atomic_write_text': ('neutral_candidate','Path/str -> None；mkdir、唯一临时文件、flush/fsync、replace、目录fsync','OSError/编码异常；临时文件 finally 删除；值本身由调用方限额','留原；只承诺原持久顺序，不吞掉提交后fsync失败'),
'io.atomic_write_json': ('neutral_candidate','Path/value -> None；sort_keys/indent2/allow_nan=False 后调用 atomic_write_text','JSON TypeError/ValueError/OSError；编码前值须由调用方控规模','留原；不可换成不同 canonical 编码破坏历史摘要'),
'io.write_exclusive': ('neutral_candidate','Path/str -> None；O_EXCL 0600，一次写入并fsync','FileExistsError/OSError；异常删本次路径；无覆盖既有记录','留原；不是跨记录事务或统一CAS'),
'io.fsync_directory': ('neutral_candidate','Path -> None；打开目录fd，fsync后finally close','OSError 原样传播；仅一个fd','留原；由真实发布者决定何时调用'),
'schema.ForgeError': ('engineering_contract','message/code -> ForgeError(ValueError)，code限七个领域错误码','非法code抛ValueError；对象是HTTP/Forge错误分派所用的真实类型','保留 Forge 领域合同；不改名、不换异常基类、不下沉为万能平台错误'),
'schema.digest': ('neutral_candidate','严格JSON值 -> canonical UTF8 的SHA256 hex；无字节预算参数','TypeError/ValueError；编码时整值驻留，不等于 bounded_json','留原；与 platform.content_digest/artifacts.digest 的编码/限额不同'),
'schema.required_text': ('neutral_candidate','value/label/maximum -> 原 str；非空且UTF8字节限额，默认100000','ForgeError；strip仅用于验证，不改变返回文本；encode产生临时字节','留原；不顺手收紧历史文本或归一化业务值'),
'platform.bounded_json': ('neutral_candidate','JSON value/maximum -> sorted紧凑UTF8 bytes；逐编码片计量','ForgeError(code=resource_limit)/JSON错误；累计输出<=maximum，单个encoder片仍可能较大','留原；纯算法候选，不把现有JSONEncoder说成严格分配前硬内存界'),
'platform.content_digest': ('engineering_contract','JSON value -> SHA256(bounded_json(value, LIMITS.snapshot_bytes))','ForgeError/JSON错误；默认32MiB快照上限属于Forge当前证明合同','留 Forge 公共证明；不能用无界digest或不同限额替换'),
'platform.limits_with': ('engineering_contract','None/override map -> LIMITS副本，仅接受已知键且0<整数<=默认值','ForgeError；不扩额，不改变原LIMITS对象','留 Forge；不是所有领域通用资源政策'),
'platform_environment.read_bounded': ('neutral_candidate','Path/maximum -> bytes；NOFOLLOW/NONBLOCK+regular fd+前后身份复核','ForgeError/OSError；观测size+1读取，max超限/resource_limit，替换/conflict；fd按with关闭','留原；比read_json更强的固定读取语义不可退化'),
'platform_environment.platform_declaration': ('engineering_contract','package root/limits -> source或installed平台声明、依赖/extra与元数据摘要','ForgeError/TOML/metadata/OSError；限额沿read_bounded；不会授予运行许可','保留 Forge 平台证明 owner；不拆成公共配置读取器'),
'platform_environment.environment_inventory': ('engineering_contract','平台声明/limits/extras/distribution -> 版本/元数据指纹/diagnostics清单','ForgeError/metadata/requirement/OSError；512 distributions默认，边/元数据队列限额；metadata_only','保留 Forge；不是安装文件attestation，也不替代引擎平台身份'),
'operation_budget.OPERATION_SECONDS': ('application_control_protocol','60秒常量，供本次控制请求/清理软件期限取小','不是整个finally或不可抢占IO的硬墙钟保证','留原控制协议；不变成执行预算/宿主容量账本'),
'operation_budget.current_deadline': ('application_control_protocol','() -> ContextVar期限与_worker_bound取小，或None','只读进程/上下文状态；不建立run/attempt','留原；中立外观不代表可拆分共享ContextVar'),
'operation_budget.check': ('application_control_protocol','deadline|None -> None；现有/显式期限取小后检查单调钟','ForgeError(resource_limit)；不取消已提交事实、不释放外部资源','留原；原错误与嵌套期限不变'),
'operation_budget.scope': ('application_control_protocol','deadline|None -> context manager yield limit；finally reset token','ForgeError(resource_limit)；有限数值、父期限取小；无晚到提交回滚','留原；唯一ContextVar，不在不同应用复制一份'),
'operation_budget.controlled': ('application_control_protocol','function -> wraps装饰调用；每次进入scope后调原函数','保持原异常；scope结束恢复token，不伪造取消成功','留原；保留方法装饰与嵌套控制语义'),
'operation_budget.child_deadline': ('application_control_protocol','positive seconds -> min(60秒/父期限)绝对单调期限','ForgeError(resource_limit)，bool/非有限/<=0拒绝','留原；不逐子任务扩总时限'),
'operation_budget.clear_worker_environment': ('application_control_protocol','mutable environment mapping -> 原mapping；删除两个期限传输键','修改调用方传入映射；不接管服务环境；无持久状态','留原；常驻reader/原生worker仍必须清除陈旧transport'),
'operation_budget.bind_worker': ('application_control_protocol','() -> None；已验证父进程后从os.environ绑定一次_worker_bound并清transport','ForgeError(resource_limit)；无transport为no-op，重绑/扩额拒绝；进程局部状态','留原；不得在常驻进程或任意配置加载时调用'),
}

GROUPS = {
'contract': dict(category='contract',state_owner='被传入的不可变声明/值与原生合同定义；不拥有运行实例',side_effects='校验/编码/只读描述；具体文件读取与源码身份项另见逐符号覆盖',lifecycle='调用期间临时值；不创建worker/Journal/lease，不扩大预算',concurrency='纯值可并发；共享可变输入须由调用方保持不变',history='保留原module/qualname、编码和对象类型；不搬声明、不重签历史',decision='retain_original_contract'),
'neutral_io': dict(category='neutral_runtime_service',state_owner='调用方的文件/结果位置；helper只拥有本次fd/临时文件/缓存',side_effects='按实际符号只读、原子文件写或非阻塞文件锁；不代表Pipeline成功',lifecycle='关闭自身fd/锁/临时文件；调用方给有限输入与目标；不访问未授权根',concurrency='沿原单写者/唯一临时文件/缓存键；并发/超限错误原样传播',history='文件编码、定位与发布顺序保持；目录命名不是新的运行事实',decision='retain_original_neutral_service'),
'actor_resources': dict(category='neutral_runtime_service',state_owner='构造它的actor/client/session；原物理资源只一个owner',side_effects='有限并发/排空/度量/进程线程或本地资源准入；不自动创建Pipeline run',lifecycle='构造不偷启动远端服务；显式start/run/close；取消先排空或杀死并回收自己资源',concurrency='实例按既有event-loop/锁使用，禁止活跃实例跨loop重绑；不新建全局池',history='资源实例非业务语义状态；恢复保留原数据/请求事实，不复制计量',decision='retain_original_scoped_service'),
'native_state': dict(category='engine_state_to_inject',state_owner='原RunJournal/action/fence/occurrence/commit和实际执行owner唯一',side_effects='读取或写原生预算/记录/状态/对象，库不能自己解释db或创建第二账本',lifecycle='引擎在原bind_execution处给精确能力；随attempt/row作用域失效；取消排空后释放',concurrency='原fence/事务/顺序/expected检查在原生所有者处；不得以Python duck typing作为授权',history='继续全树精确拒绝/原版本恢复/历史只读；不改变封存表示或允许旧pickle跨路径加载',decision='design_narrow_injected_capability_keep_original_until_reviewed'),
'native_adapter': dict(category='existing_native_adapter',state_owner='原生引擎Journal/RunContext拥有执行事实；库中的专用装配沿P1既定合同',side_effects='特定注册/确认/作用域装配；不能泛化为随意运行SDK或绕过准入',lifecycle='原始锁/token/attempt顺序和finally，成功发布不因后续错误回滚',concurrency='原生单写/fence/CAS与注册锁保留，不接管其他运行',history='已有真实对象与原路径保留；P1明确保留依赖，不以导入清零替代身份设计',decision='retain_approved_native_assembly'),
'legacy_cleanup': dict(category='legacy_scoped_compatibility',state_owner='旧模块各自global资源；registry仅登记cleanup回调，不是新service locator',side_effects='导入时注册到现有字典；清理由原StreamResources调用',lifecycle='本批保留；后续在有实际生命周期消费者替换后移除旧登记',concurrency='当前同进程字典，无新并发/发现/动态插件保证',history='不删除外部消费者未知的兼容路径；与P8逐符号处置协调',decision='retain_until_scoped_retirement'),
}

# name -> (group, purpose/input/output, precise caveat, proposed slice)
F2 = {
'adaptive_requests.AdaptiveRequestGate':('actor_resources','policy/failures/state_path -> AIMD RequestGate；begin_action/snapshot/result/enter','有界策略max_concurrency<=1024、观察窗口；state_path可能写策略状态。prompt_facts目前精确type比较，不能换成任意假对象','retain; passive gate-facts only if G needs it'),
'artifacts.digest':('neutral_io','bytes或JSON value -> 原encoded(value) SHA256 hex','JSON编码为indent2+末尾换行；与Forge紧凑digest不同；当前未自行限JSON大小','retain'),
'artifacts.immutable':('neutral_io','path/value -> bool（本次新建true、已有相同false）；硬链接发布不可变JSON','现存不同ValueError；OSError/JSON错误传播；原调用方持锁、线程用唯一tmp；原字节绝不更新','retain'),
'artifacts.read':('neutral_io','path -> resolve_local_artifact之后的JSON值','原函数read_text无独立上界，调用方必须有文件规模；不是bounded读取证明','retain; bounded caller review only on affected slice'),
'artifacts.resolve_local_artifact':('neutral_io','显式原文件路径 -> exact relocation后的Path','只解析上层_demiflow/artifact_locations.json精确键，无前缀/链查找；ValueError/IO；LRU16、>8MiB用IndexedLocations','retain; G location contract'),
'artifacts.run_lock':('neutral_io','directory -> contextmanager；mkdir/.lock与LOCK_NB，退出解锁','RuntimeError忙、OSError；同名run只是目录锁，不是作业账本；services.manage/searxng实际用服务目录','retain'),
'block_sink.storage_usage':('native_state','context -> context.journal.storage_usage()投影','不是扫描目录；原Journal异常；media._store_image当前拿到store.run，不能移交raw db给库','F2-IMAGE'),
'blocking_io.BlockingIOPool':('actor_resources','workers/name -> run(fn,*args)、drain/aclose；原结果或原异常','先Semaphore再submit，线程数限制包含started jobs；caller等待载荷另控。重复取消先排空实际blocking工作才释放slot；close wait=True','retain'),
'capabilities.require_action':('contract','kind/arguments -> None或原能力拒绝','RecoveryContractError；纯retired-checkpoint能力判断，不新增运行或另一套checkpoint','retain'),
'commit.CommitCoordinator':('native_state','原RunAction -> commit/recover/inspect_completed；已确认commit事实','prepare/data/checkpoint/ACK顺序归原owner；UnresolvedCommit不重发搜索/外部请求','F2-SEARCH'),
'dataflow_images.RemoteImageStore':('native_state','journal/store/options -> store_image(bytes)->原image引用reply','字节<=media max_image_bytes，先store.put再原journal stage_image；worker无发布权；原DataflowError/ValueError','F2-IMAGE'),
'dataflow_managed_http.managed_http_digest':('contract','已声明ManagedHTTPService -> sealed passive declaration SHA256','freeze_managed_http仅允许精确字段和有限类型，document<=_MAX_BYTES；不启动owner；DataflowError/contract拒绝保持','retain; G passive managed-service facts'),
'dataflow_model_journal.RemotePromptJournal':('native_state','WorkerJournalPort/NativeActionView/Bounds -> lookup/reserve/response/failed/references等具名方法','必须原capability-verified action/occurrence/fence；writable/finite limit、store bytes和RPC上限；close只关本视图不释放driver owner','F2-PROMPT'),
'dataflow_offline.RemoteOfflinePromptClient':('native_state','model/options/verified journal -> lookup/execute -> OperatorLLMResponse','仅一个offline_store/offline_dir；pending抛PromptResponsePending，unknown DataflowError；不开paid/provider/private ledger；aclose不关run port','F2-PROMPT'),
'dataflow_prompt_journal.RemotePrivatePromptJournal':('native_state','services/action/node/declaration/route/maximum -> 原remote journal专用变体','继承RemotePromptJournal；declaration精确匹配、禁止private+native双写；reserve/retry预算在driver核验','F2-PROMPT'),
'dataflow_prompt_service.prompt_service_declaration':('contract','actor -> None或原passive managed service声明','检查actor._runtime._service与_sealed_service_declaration digest；DataflowError INVALID_CONTRACT，不造新owner','retain; G inspect_prompt_model passive facts'),
'datafusion.DataFusionOptions':('contract','dataclass配置 -> 有限Query/worker资源配置值','正整数/有限期限；memory_bytes < max_rss_bytes；不可把DataFusion pool当RSS硬隔离','retain independent query contract'),
'datafusion.DataFusionSession':('actor_resources','resource_directory/options/max_concurrent等 -> context session/query/borrow_resources 与受限Lance staging','独立SQL/index不建Pipeline run；P4保留域。lance.index还调用_prepare_lance_index两处，不能称已经全公共接口','F2-INDEX optional local boundary'),
'datafusion._save':('neutral_io','Path/value -> None；固定.tmp写JSON再replace','私有工具，无fsync/独立字节上限/并发锁，不能换强弱不同写入后称等价；当前lance.index真实一处','defer; actual owner index record helper if INDEX slice approved'),
'inference_options.CALL_OPTION_DEFAULTS':('contract','共享默认dict -> io/prepare/response worker和keepalive等默认值','实际是可变dict；消费者只读/合并不修改；collect_journal_totals含义保留','retain'),
'inference_options.normalize_call_options':('contract','protocol已验allowed keys的options -> 合并默认dict','正整数worker/finite非负keepalive/bool totals；ValueError；它不负责拒绝协议未知键','retain'),
'isolated_pool.IsolatedWorkerPool':('actor_resources','named pure functions/workers/message/max_tasks -> async run(name,args,timeout)结果、metrics','入场后序列化，64MiB默认frames/128 tasks；消息编码可能先分配后检查，不是RSS界。取消/超时killpg+reap，无自动重试','retain'),
'isolation.run_isolated':('actor_resources','module-level function/args/timeout -> 子进程原结果','cloudpickle请求/回复当前无独立字节界；调用者只传固定表ref与小回执；timeout stop_owned并回收；未知副作用不得自动重放','retain'),
'lance_action.bind_lance_action':('native_adapter','before_execute/max_rows_per_batch -> 进程内一次性binding context','P1批准；嵌套拒绝，消费前先清绑定，回调早于源/worker启动，finally reset；不进ActionSpec/序列化','retain exact P1 7.6'),
'lance_sink.spec_from_identity':('native_adapter','原identity document -> 原LanceWriteSpec（Arrow schema按原格式还原）','只接受原type path，RecoveryContractError/Arrow错误；确认既有注册receipt时用，不新建结果/类型','retain'),
'local_storage.process_identity':('neutral_io','pid -> /proc非僵尸starttime str，进程不存在None','FileNotFoundError转None，其它IO错误仍传播；调用services还核host；这不是boot+pidfd完整身份','retain'),
'location_index.IndexedLocations':('neutral_io','filename/source_identity/field/files|tables/content_digest -> get/get_readonly映射','原manifest<=1GiB、流式JSON、缓存SQLite+文件锁/身份复核；get可构建派生cache，get_readonly拒绝/不重建区别保留','retain; G location contract'),
'location_index.recent_digest':('neutral_io','filename/stat -> 近2秒ctime文件的SHA或None','扫描按size/time110秒界；身份变化ValueError；非语义运行状态、不保证所有旧cache永远无IO','retain; G location contract'),
'object_sink.check_image_replay':('native_state','action -> None；确认image请求水位或拒绝未定结果','action局部_checked标记、context lock、Journal commits/budget；UnresolvedCommit不得当cache miss再生成','F2-IMAGE'),
'object_sink.image_request_budget':('native_state','action -> 基于action_id的原预算key','不产生请求；key身份必须原样，不能让库伪造action_id或新分预算账本','F2-IMAGE'),
'payload_limits.arrow_batches':('neutral_io','借用Arrow batches+limits -> 懒生成有界RecordBatch','先核native type和父buffer memory_bytes，再bounded_record_batches；retained parent不冒充logical bytes；RecoveryContractError','retain'),
'pipeline_sources.reachable_pipeline_sources':('contract','固定bundle_root/entrypoint -> tuple[Path]静态相对import闭包','读项目源码/AST，不执行业务；helper本身无独立文件/字节总额，依赖封存包上界；路径/语法/OSError保持','retain with explicit caller package budget; G no semantic transplant'),
'queue_commit.producer_action':('native_state','parent/channel/producer/occurrence/contract/phase -> 原子作用域RunAction','queue producer身份/fence/预算归原channel owner；Search不得自己拼另一种scope或扩大phase','F2-SEARCH'),
'raw_budget.budget_delta':('native_state','受保护db/name/delta/ceiling/owner -> 新消耗数值','原native_budget_delta或同transaction fallback；ceiling不变/0<=value<=ceiling；RecoveryContractError；db不能出owner边界','F2-SEARCH'),
'raw_budget.record_admission':('native_state','受保护db/ceiling -> None；reservation_records +1','同预算事务，不能把record准入和实际HTTP计数拆开伪造成功','F2-SEARCH'),
'raw_fetch.RawFetch':('native_adapter','fetch声明/request/output/store/policy/concurrency/queue_depth -> 原actor对象','冻结raw_fetch.py；P1原生保留类，声明构造不当成纯预检；astart/stop/close/restore等保持原协议','retain frozen type and P1 exact consumers'),
'raw_fetch.raw_fetch_facts':('contract','原RawFetch actor -> 原fetch facts','同冻结文件；不搬/重写helper为删依赖，不放宽capture/actor状态检查','retain frozen facts'),
'recovery_contract.RecoveryContractError':('contract','message -> 原RuntimeContractError子类异常','类型身份参与catch/restore；不换成ForgeError/普通ValueError','retain'),
'recovery_contract.RunRef':('contract','directory/run_id -> 原frozen dataclass','记录引用不是执行权限；持久类型路径不能搬','retain'),
'recovery_contract.SourcePosition':('contract','source_id/next_ordinal -> frozen dataclass/to_dict','identifier与ordinal>=0检验；固定source occurrence，不从业务row值推导','retain'),
'recovery_contract.UnresolvedCommit':('contract','message -> 原RecoveryContractError子类','未知外部提交结果；不能重解释成普通row失败/自动重试许可','retain'),
'recovery_contract.document':('contract','strict JSON value/maximum默认1MiB -> canonical bytes','depth<=32/nodes<=65536、有限float/keys/count/encoding限制；异常原RecoveryContractError','retain exact encoding and bounds'),
'recovery_contract.identifier':('contract','str -> 原str，1..128的[A-Za-z0-9_-]','RecoveryContractError；不做别名/自动规范化','retain'),
'recovery_contract.identity':('contract','strict JSON value -> SHA256(document) hex','原最大/编码必须相同；不能与Forge或artifacts digest互换','retain'),
'recovery_identity.describe':('contract','value/depth -> 原恢复身份JSON投影','callable整source文件hash、class方法code/dataclass/type paths；可能只读源码/Arrow声明；不是无IO通用repr','retain sensitive identity owner; no smaller hash promise'),
'request_limits.LatencySummary':('actor_resources','seconds -> histogram/summary count,p50/p95上界','0.25s桶，>1天overflow，共<=345602桶；无持久执行真值，不保证线程安全','retain'),
'request_limits.RequestGate':('actor_resources','concurrency/failures/interval -> reserve/enter permit、result/check','并发/pace admission，取消预约不花attempt；ServiceStopped fatal不能转row verdict；active跨loop begin_action拒绝','retain original scoped resource'),
'request_limits.ServiceStopped':('contract','reason -> 原RuntimeError子类服务终止信号','停止dispatch，不是业务row判定；固定cooperative词汇决定排空，不任意异常授权drain','retain'),
'request_limits.drain_on_service_stop':('actor_resources','awaitable/enabled -> 原结果或原CancelledError','只为已准入且收到ServiceStopped取消信号的effect有限排空；普通cancel仍取消；deadline归effect owner','retain'),
'request_limits.is_cooperative_stop_error':('contract','error -> bool（类型+固定允许词汇）','不因任意字符串或旧snapshot认定获准排空','retain'),
'resource_registry.register_stream_cleanup':('legacy_cleanup','name/callback -> None；覆盖原_cleanups[name]','导入注册不是全局service发现；回调实现和跨进程清理不由registry保证','retain no new use; coordinate P8'),
'row_scope.current_row_scope':('native_state','() -> RowScope|None（含occurrence/delivery）','ContextVar只读当前实际侧车；不可从business row/token或新字段合成权限','F2-SEARCH/PROMPT'),
'run_context.RunAction':('native_state','context/action_id/declaration -> 原RunAction/record/complete','绑定同Journal/fence；不可让SDK请求传RunAction；Search原child scope需原生owner供给','F2-SEARCH'),
'run_context.RunContext':('native_adapter','start/resume/attach/original directory+limits -> 原执行scope','注册写入是特定原生装配；代码/平台/环境/预算原严格校验；token/lease退出与未知保护保持','retain registered_write assembly'),
'run_context.current_action':('native_state','() -> 当前RunAction|None','ambient getter不授予调用者创建/接管owner；后续局部注入时不复制ContextVar','F2-SEARCH/PROMPT/IMAGE'),
'run_context.current_run':('native_adapter','() -> 当前RunContext|None','registered_write用于选择原owning run；不新建当前运行中心','retain P1 registered_write assembly'),
'run_journal.ExecutionBudgetExceeded':('contract','budget/message -> 原RecoveryContractError，含budget字段','预算超限不是可自动重试错误；prompt journal沿原失败记录语义','retain'),
'run_journal.ExecutionConflict':('contract','原demiflow.errors.ExecutionConflict类型，通过run_journal重导出','本地AST无定义属真实re-export，不是缺失；错误路径/类型身份保持','retain original object; do not duplicate class'),
'run_journal.RunJournal':('native_adapter','directory/attempt -> 原Journal；注册确认/历史读取用受限原成员','构造/conn可写；不是纯reader。库adapter需维持原注册锁与原运行事实，禁止扩展为随意db访问','retain specialized native storage adapter'),
'state_artifacts.atomic_bytes':('neutral_io','path/bytes -> None；独占.staging+fsync+replace+目录fsync','原RecoveryContractError symlink/IO；同目标并发FileExistsError，调用方持注册锁且raw已有限；不能换成无fsync _save','retain'),
'stream_resources.CallMetrics':('actor_resources','reset/observe(call,error)/summary -> 原usage与latency观测','仅本actor度量，reused不计重复tokens；histogram有界，结果不当准入/提交或可靠usage真值','retain'),
}


def make_tables(scan_path=HERE/'scan.json'):
    s = json.loads(scan_path.read_text())
    decl = {(x['provider'],x['symbol']): x for x in s['target_declarations']}
    out = {'source_sha':s['source_sha'], 'method':'Static import inventory plus reviewed contracts; no runtime or external completeness claim.',
           'scan_sha256':hashlib.sha256(scan_path.read_bytes()).hexdigest(), 'F1':[], 'F2':[]}
    for section, decisions, prefix in [('F1',F1,'demiflow.forge.'),('F2',F2,'demiflow.execution.')]:
        for serial,(short,info) in enumerate(sorted(decisions.items()),1):
            module,name=short.rsplit('.',1); provider=prefix+module
            entry={'id':section+'-'+str(serial).zfill(2),'provider':provider,'symbol':name,'provider_declaration':decl[(provider,name)]['definitions']}
            if not entry['provider_declaration'] and short=='run_journal.ExecutionConflict':
                entry['provider_declaration']=[{'path':'demiflow/errors.py','line':11,'type':'ClassDef','reexported_via':'demiflow/execution/run_journal.py:16'}]
            source_hashes={r['path']:r['sha256'] for r in s['sources']}
            entry['provider_source_sha256']={d['path']:source_hashes[d['path']] for d in entry['provider_declaration']}
            key_match=lambda r:r.get('provider')==provider and (r.get('symbol')==name or r.get('symbol','').startswith(name+'.'))
            entry['focal_imports']=[r for r in s['f1_imports' if section=='F1' else 'f2_imports'] if key_match(r)]
            entry['focal_use_sites']=[r for r in s['uses'] if key_match(r) and (r['consumer'].startswith('demiflow.workbench.') if section=='F1' else r['consumer'].startswith('demiflow.') and r['consumer'].split('.')[1] in {'collect','embeddings','operator_llm','services','lance'})]
            entry['all_repository_import_consumers']=[r for r in s['imports'] if key_match(r)]
            entry['all_repository_lexical_use_sites']=[r for r in s['uses'] if key_match(r)]
            entry['relevant_module_bindings']=[r for r in s['imports'] if r['provider']==provider and r['symbol']=='<module>']
            entry['string_references']=[r for r in s['string_references'] if key_match(r)]
            entry['unknowns']='External/dynamic and assignment/reflection aliases are not fully resolved; import sites are complete only within declared static scope. Runtime reachability not tested.'
            if section=='F1':
                category,io,error,recommendation=info
                entry.update(category=category,input_output=io,errors_limits=error,recommendation=recommendation,
                             state_owner='Forge domain/type/attestation owner or original caller; operation_budget process/ContextVar identity remains unique.',
                             side_effects='Exact behavior in input_output; no permission or Pipeline execution granted by import.',
                             lifecycle_concurrency='Caller owns request/file/payload bounds; file handles/tokens follow original finally; no new global service or duplicate state.',
                             history_identity='No current source changes; future extraction changes platform/Forge identity and possibly class/callable paths. No automatic old seal adoption.',
                             user_decision='Recommend keep current five public modules. Any extraction requires a concrete selected symbol scope and separate approved design.')
                if module=='operation_budget':
                    entry['state_owner']='同一控制协议的唯一ContextVar/_worker_bound；确切读写见本符号输入输出，不拥有run/资源事实。'
                elif short=='schema.ForgeError':
                    entry['state_owner']='异常实例的code/message；唯一真实Forge错误类型，没有全局运行状态。'
                elif short in ('platform.limits_with','platform_environment.platform_declaration','platform_environment.environment_inventory'):
                    entry['state_owner']='调用方本次Forge证明输入/观测；函数不发布快照、不中断或采用业务运行。'
                else:
                    entry['state_owner']='无运行状态；参数/返回值归调用方，本次fd/临时文件归原helper；副作用以本符号实际行为为准。'
            else:
                group,io,caveat,slice_id=info
                entry.update(GROUPS[group],input_output=io,errors_limits=caveat,proposed_slice=slice_id)
                if group=='native_state':
                    entry['current_boundary_status']=('Already injected and capability-verified; retain real class checks and original owner.'
                        if module in ('dataflow_images','dataflow_model_journal','dataflow_offline','dataflow_prompt_journal') else
                        'Current ambient/direct native state use; preserve until the proposed complete consumer/owner slice is separately approved.')
                if short=='run_journal.RunJournal':
                    entry['focal_permitted_role']='lance.run initializes/stages/finishes its existing record; registered_write reads original identity/source/commit/action proofs and writes only through original RunContext binding. Method inventory is source evidence, not blanket permission.'
            out[section].append(entry)
    required1={(r['provider'],r['symbol']) for r in s['target_declarations'] if r['provider'] in FIVE}
    required2={(r['provider'],r['symbol']) for r in s['f2_imports'] if r['symbol']!='<module>'}
    for section,required in [('F1',required1),('F2',required2)]:
        actual={(r['provider'],r['symbol']) for r in out[section]}
        assert actual==required,(section,'missing',required-actual,'extra',actual-required)
    out['reconciliation']={'f1_import_sites':len(s['f1_imports']),'f1_module_bindings':sum(r['symbol']=='<module>' for r in s['f1_imports']),
                           'f1_used_symbols':len(out['F1']),'f2_import_sites':len(s['f2_imports']),'f2_used_symbols':len(out['F2']),
                           'f1_by_category':dict(collections.Counter(r['category'] for r in out['F1'])),
                           'f2_by_category':dict(collections.Counter(r['category'] for r in out['F2']))}
    return out


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scan',type=Path,default=HERE/'scan.json')
    parser.add_argument('--output-dir',type=Path,default=HERE)
    options=parser.parse_args()
    options.output_dir.mkdir(parents=True,exist_ok=True)
    table=make_tables(options.scan)
    for section in ('F1','F2'):
        with (options.output_dir/(section+'-symbols.json')).open('x') as f:
            json.dump({'format':'task_f_symbol_contracts_v1','source_sha':table['source_sha'],'reconciliation':table['reconciliation'],'symbols':table[section]},f,ensure_ascii=False,indent=2);f.write('\n')
        lines=[section+' 逐符号职责与契约（完整消费者和原方法签名见同名 JSON）', '固定源 '+table['source_sha'],'']
        for row in table[section]:
            lines.extend([row['id']+' '+row['provider']+':'+row['symbol'], '分类 '+row['category'], '输入/输出 '+row['input_output'], '错误/预算 '+row['errors_limits'],
                          '状态 '+row['state_owner'], '副作用 '+row['side_effects'], '生命周期/并发 '+row.get('lifecycle_concurrency',row.get('lifecycle','')+'；'+row.get('concurrency','')),
                          '历史/身份 '+row.get('history_identity',row.get('history','')), '建议 '+row.get('recommendation',row.get('decision','')+' / '+row.get('proposed_slice','')),
                          '目标导入 '+(', '.join(r['path']+':'+str(r['line']) for r in row['focal_imports']) or '（经模块属性绑定，见目标调用/引用）'),
                          '目标调用/引用 '+', '.join(r['path']+':'+str(r['line'])+' '+r['role'] for r in row['focal_use_sites']), ''])
        with (options.output_dir/(section+'-symbols.txt')).open('x') as f:f.write('\n'.join(lines).rstrip()+'\n')
    with (options.output_dir/'reconciliation.json').open('x') as f:json.dump(table['reconciliation'],f,ensure_ascii=False,indent=2);f.write('\n')
    print(json.dumps(table['reconciliation'],ensure_ascii=False))
