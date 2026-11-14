"""Second-review corrections and explicit leaf ownership; no production edits."""
import ast
import hashlib


def apply(tasks, add, add_js, params, root):
    params.update({
        'prefetch_batches':'int>=1；有界批预读窗口，不是额外内存额度',
        'batch_size':'int>0 或 None；原批语义，仍受字节上限',
        'batch_format':'既有批输出格式；不得因路由改变返回类型',
        'drop_last':'bool；是否保留末尾不足批，保持原语义',
        'fetch_if_missing':'bool；False 不因查询 schema 新读取业务数据；True 的缺失回退属于实际执行',
        'aggregates':'tuple[既有聚合表达式,...]；保留顺序及空输入语义',
        'datasink':'已有 Datasink；复用其提交/清理契约，不复制提交权威',
        'format_name':'现有 file sink 格式名称；仅允许当前实现支持的值',
        'path':'已检查的输出路径；不追随未经绑定的外部目标',
        'options':'该函数原生 options；不可跨类型互用；绑定执行只能收紧原选项',
        'native_options':'现有终端原生配置或 None；业务语义和资源边界均保持',
        'terminal_category':'既有 action/terminal 分类；用于真实计划事实',
        'terminal_native_options':'原生终端选项或 None；仅当前 action 配置',
        '_native':'bool；False 为已绑定的 Python 子阶段，不是再次自动选择后端的授权',
        'queues':'当前 stage 的有界队列序列；切 cut 控制帧仍由原驱动持有',
        'commit':'既有提交回调；不创建第二个 CommitCoordinator',
        'final':'bool；本 barrier 是否最终 cut，语义沿用原协议',
        'track':'原后台任务登记回调或 None；取消时不得遗留发布任务',
        'error':'原异常对象；取消/超时不变成成功或自动重试',
        'worker_slot':'稳定非负 worker slot 或 None；普通 buffered 必须提供，native 阶段单 token 保留 None',
        'cut_id':'当前 barrier 的稳定 checkpoint 身份；不得用 attempt/时钟；错误 cut 拒绝',
        'node_id':'原报告节点ID；与图 binding 校验，不能当作 logical ID',
        'kind':'既有 source projection implementation/callsite/configuration/prompt',
        'resource':'原可选受限源资源引用；不允许任意路径读取',
        'max_requests':'int>=0 或 None；本新预算 helper 明确 None=采用run额度、0=禁止fresh；Actor原构造器仍要求正数',
        'timeout_s':'有限正超时秒；使用已有运行/取消截止时间的剩余上限',
        'program_id':'稳定ProgramSlice ID或None；None只用于原单program兼容键',
        'inflight_slots':'已准入正整数槽额度或原非绑定路径的None，不得由credit floor扩容',
        'inflight_bytes':'绑定执行的正整数字节额度；不得用槽数替代字节额度',
        'max_pending_floor':'int>=1；原InputGate协议所需pending下限',
        'credit_budget':'{rows:int>=1,bytes:int>=1}；实际通道credit，不是额外grant',
        'credit_explicit':'bool；用户显式credit不可静默改写；默认credit允许在封存前收紧',
        'graph_binding':'原图身份文档；workspace/basis/action/attempt/摘要精确校验，不重算历史',
        'report':'已校验摘要的有界Forge报告载体；只读，不执行业务',
        'project':'原工作区project ID；仍严格校验原正则和项目范围',
        'selection':'{logical_node_id:str,occurrence_id:str|null}或null；同action/attempt内的详情选择',
        'preload_lance':'bool；原worker初始化选项，不扩大预算',
        'function':'原worker任务函数；不因重试再次执行不确定副作用',
        'arguments':'本函数原参数；TaskScheduler中为惰性任务参数迭代器，单项与pending合计有界',
    })
    byid={t['id']:t for t in tasks}
    def edit(tid, behavior=None, calls=None, **fields):
        t=byid[tid]
        if behavior is not None:t['behavior']=behavior
        if calls is not None:t['calls']=list(calls)
        t.update(fields)
        t['review_revision']='20261114-r2'
        return t
    def signature(t, value):
        t['signature']=value
        n=ast.parse(value+'\n    pass').body[0]
        t['input']={a.arg:params[a.arg] for a in [*n.args.posonlyargs,*n.args.args,*n.args.kwonlyargs,*([n.args.vararg] if n.args.vararg else []),*([n.args.kwarg] if n.args.kwarg else [])]}
    def existing(owner,file,symbol,output,behavior,gates,contract,calls=(),mode='implement',target=None,effects='runtime_state'):
        add(owner,file,symbol,output,behavior,gates,contract=contract,calls=calls,mode=mode,effects=effects)
        t=tasks[-1];t['review_revision']='20261114-r2'
        if target:signature(t,target)
        return t
    def declaration(owner,file,symbol,behavior,contract,gates):
        src=(root/file).read_text();tree=ast.parse(src);node=tree
        parts=symbol.split('.')
        for part in parts[:-1]:node=next(n for n in node.body if isinstance(n,ast.ClassDef) and n.name==part)
        n=next(n for n in node.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id==parts[-1] for t in n.targets))
        value=ast.get_source_segment(src,n)
        tid=f'{owner}{sum(t["owner"]==owner for t in tasks)+1:03}'
        tasks.append(dict(id=tid,owner=owner,file=file,symbol=symbol,mode='implement',signature=value,baseline_signature=value,
            language='python_declaration',baseline_line=n.lineno,baseline_function_sha256=hashlib.sha256(value.encode()).hexdigest(),
            input={},output='类/模块声明的窄修改；其余字段逐字保留',behavior=behavior,contract=contract,effects='declaration_only',
            calls=[],gates=gates,status='planned',review_revision='20261114-r2'))
        return tasks[-1]

    # A: public ownership and every real terminal consumer, not just take/write.
    for sym in ['take','take_all','iter_rows','iter_batches','schema']:
        existing('A','demiflow/data/dataset.py','Dataset.'+sym,'原公开返回类型保持',
            '公开终端只建一次checked.action；与当前count/materialize既有边界一致。惰性迭代器从首次消费到close/耗尽/取消都在scope内；take(0)不读源。schema(fetch_if_missing=False)仅可读已有元数据，缺失不暗跑；True真读取必须进action。内部helper不调用另一个公开终端。',
            ['ADMISSION','DELIVERY','HISTORY'],'C03',calls=['admit_action_plan','LocalDatasetExecutor.'+('take' if sym=='take_all' else sym)])
    existing('A','demiflow/execution/stream_delivery.py','iter_stream_rows','有界行迭代器；仅owner可complete',
        '显式action或current_action存在则借用；验证context/source/plan属于当前终端，不再run.action、不调用action.complete。仅无外层scope的原内部兼容入口自建delivery action并负责complete。借用路径的最终complete由外层checked.action执行；提前close先停止并join producer，再退出scope；线程未停保留隔离/失败，不释放资源。保留回执先于yield和unknown语义。',
        ['ADMISSION','DELIVERY','RECOVERY'],'C03',target='def iter_stream_rows(executor, source, plan, *, limit=None, action=None):')
    add('A','demiflow/execution/executors/local.py','LocalDatasetExecutor._execution_capabilities','tuple[str,...]；固定可用backend集合',
        '复用现有has_relational、_local_kernel、arrow_batch_sink.eligible和Lance fast-path条件；只检查声明/配置，不读数据、不构造actor。显式缺能力报错，不返回空集合后静默换后端。',
        ['ADMISSION','LAYOUT'],contract='C03',new_signature='def _execution_capabilities(self, source, plan, *, action_kind, sink_spec=None):',effects='read_only')
    existing('A','demiflow/execution/executors/local.py','LocalDatasetExecutor._apply_plan','原惰性行迭代器',
        '实际执行型调用必须携带已封存execution；验证backend/profile/program。_native=False只执行该binding指定的Python子阶段，不借此绕过准入或重选backend。纯元数据读取不调用本函数。无外层action的兼容调用按显式非绑定契约处理，不宣称已受统一布局支配。',
        ['ADMISSION','LAYOUT','RELATIONAL'],'C08',calls=['execute_native_plan'],target="def _apply_plan(self, source, plan, *, action_kind='iter_rows', terminal_category='action', terminal_native_options=None, physical_plan=None, _native=True, execution=None):")
    for sym in ['iter_batches','count','take_batch','aggregate','schema','size_bytes','materialize','write_datasink','write_file']:
        existing('A','demiflow/execution/executors/local.py','LocalDatasetExecutor.'+sym,'原公开终端返回值/类型',
            '在唯一action内调用真实能力生产者与准入决策，compile/load原封存execution，再沿本终端原调用链显式传到_apply_plan或native/stream消费者。所有分支保持原输出类型、空输入、取消和sink提交语义；不调用其他公开Dataset action。schema纯元数据分支verify_preserve，fetch回退真读取必须绑定。stream组合不支持时在读源前明确拒绝，不落到全局map选项误拒或legacy偷跑。',
            ['ADMISSION','LAYOUT','STREAM'],'C03',calls=['LocalDatasetExecutor._execution_capabilities','admit_action_plan','LocalDatasetExecutor._apply_plan','iter_stream_rows'])
    for tid in ['A001','A006','A007','A008']:
        t=byid[tid]
        if tid!='A001':t['calls']+=['LocalDatasetExecutor._execution_capabilities']
    edit('A004',byid['A004']['behavior']+' 当前源码已建action，保留该边界；修delivery借用，不外包第二个action。',calls=['admit_action_plan','LocalDatasetExecutor.count'])

    # M: real reserve boundary plus an explicit live-call owner; preserve journal atomicity.
    add('M','demiflow/operator_llm/stream_recovery.py','native_request_budget','ContextManager[str]；yield reservation_id',
        '仅生成有限charges并设置CALL_RESERVATION，finally reset token；不在事务外扣账。None与0分开，沿既有requests和node额度键。journal._enqueue在调用线程取值，_apply_operation从payload读；沿既有savepoint原子reserve+intent。同集合重复reserve已幂等；禁止用不同组件集合双重预留同一请求。',
        ['EMBED','PROMPT','BOUNDS'],contract='C07',new_signature='def native_request_budget(run, *, node_id, max_requests):')
    add('M','demiflow/embeddings/stream_recovery.py','NativeEmbeddingRecovery.drain','None（async）；超时抛TimeoutError且不关闭仍被用资源',
        'inflight为本adapter持有的reservation_id→真实执行Task映射，覆盖reserve到fresh响应/failed持久化完成。等待这些任务有界排空，不将journal unknown当仍在运行的任务；未知效果记录保留。不能等待自己；超时不清空inflight、不提前close client/journal，交原失败/隔离路径。',
        ['EMBED','UNKNOWN','BOUNDS'],contract='C07',new_signature='async def drain(self, *, timeout_s):')
    edit('M003',byid['M003']['behavior']+' 初始化inflight={}；复用run._native_prompt_journal的RunPromptJournal；不是新SQLitePromptJournal、不新建coordinator。')
    edit('M004','绑定借用的run-owned RunPromptJournal到actor；无native路径保留既有独立运行契约。native不得并存private SQLite预算权威；显式不兼容private journal先拒绝，不静默双写。校验响应上限，_calls在native仅作观测，不作第二额度守卫。inflight归adapter，aclose必须先drain。',effects='runtime_state')
    edit('M007','先lookup复用已存响应；在_request_slots/request_gate内、await io(journal.reserve,request)之前进入native_request_budget，并保持到fresh发送/response或failed持久化完成。inflight登记当前真实Task，finally只在实际工作结束后删除。BlockingIOPool.run已copy_context，复用它；_enqueue读ContextVar，writer读payload，不能反过来。非native路径保留原计数策略。取消不重发unknown，不提前释放client。',calls=['native_request_budget','NativeEmbeddingRecovery.scope_request','EmbeddingActor._fresh_exchange'],effects='durable_write')
    edit('M011','native路径先await adapter.drain(有效取消deadline)再关闭client/owner与原pool；超时不得宣称已收尾或清空仍运行的inflight。借用journal的close不得关闭run-owned journal。保留非native原关闭语义。',calls=['NativeEmbeddingRecovery.drain'],mode='implement')
    edit('M012',byid['M012']['behavior']+' 此函数没有reserve；不得在这里才首次设置CALL_RESERVATION。',mode='verify_preserve')
    existing('M','demiflow/operator_llm/http_options.py','validate_http_options','规范化HTTP options或原ValueError',
        '拒绝记录型request_options内的api_key/extra_headers，不再以credentials_env是否存在为前提；凭据继续用现有transport-only gateway_credentials。错误不回显值。显式迁移受影响用例，其他合法非秘密语义和既有兼容规则保持。',
        ['PROMPT','IDENTITY','HISTORY'],'C07',effects='pure')

    # R: real barrier owner and class assembly. Buffer payload remains explicitly unfrozen.
    for sym in ['__init__','tokens','arrive','fail']:
        t=existing('R','demiflow/execution/stream_barrier.py','_CheckpointBarrier.'+sym,'原barrier返回语义；align每stage每cut一次',
            '普通buffered按稳定worker_slot登记、去重和聚合全部pending；错cut/slot或重复arrive拒绝。expected slots来自真实活跃worker及EOF登记，不以整数重复计数替代。native row-local沿原单token+全阶段native_pending和双锁快照，不能改成全worker排空。fail必须唤醒所有等待者、保留原发布/取消/timeout路径。payload格式尚未冻结，禁止按旧groups字典猜写。',
            ['BUFFER','RECOVERY','UNKNOWN'],'C05',calls=['StreamStageProgress.align'] if sym=='arrive' else [])
        if sym=='arrive':signature(t,'async def arrive(self, stage, batch=None, *, worker_slot=None, cut_id=None):')
    existing('R','demiflow/execution/stream_barrier.py','CheckpointControl.run','原async协调循环',
        '保留source ordinal与(ordinal>=replay_cut OR full)，full含容量压力；不能用stage输出或batch ordinal替代源frontier。新增buffer只消费原cut和状态预算。',
        ['BUFFER','RECOVERY'],'C05',mode='verify_preserve')
    declaration('R','demiflow/execution/stream.py','_Stage.__slots__','仅添加buffer_adapter，与R010构造器同一装配提交；保留全部原slots及native锁/注册表。','C05',['BUFFER'])
    edit('R010',byid['R010']['behavior']+' 必须与_Stage.__slots__同提交装配。',calls=['_Stage.__slots__'])
    edit('R006','原F3守卫按kind保留；固定batch旧恢复字典{batch_ordinal,data}不改名。恢复batch_ordinal新增type is int且>=0校验（当前无该检查）；恢复rows>=max_batch与字节守卫保留。native全注册表恢复沿原协议；其余kind只有adapter完整+真实故障证据通过才放行。',calls=byid['R006']['calls'])
    edit('R008',byid['R008']['behavior']+' restore赋值和运行自增是同一batch序号的两个合法生命周期写点；均经验证，不声称只有一个写点。')
    edit('R015',byid['R015']['behavior']+' 本叶唯一持有整_make_worker，含grouped/batch/native嵌套分支；不另派重叠嵌套任务。每个非native参与slot显式arrive，grouped不能把barrier当业务行。',calls=byid['R015']['calls']+['_CheckpointBarrier.arrive'])

    # L: stable per-slice driver pin, source dependency roots, native fallback consumer.
    add('L','demiflow/execution/chain.py','source_callable_roots','tuple[CallableSpec或函数,...]；稳定顺序、有界',
        '按原生SourcePlan/PlanInput明确类型遍历reducer及其参数，保留input_path；不调用用户属性/函数。只提供现有_business_dependency_modules的额外根，复用原BFS上限，不复制依赖遍历权威。',
        ['HISTORY','RELATIONAL'],contract='C08',new_signature='def source_callable_roots(source):',effects='read_only')
    for sym in ['_business_dependency_modules','plan_code_pin','snapshot_code_sources']:
        sig={'_business_dependency_modules':'def _business_dependency_modules(plan, *, source=None):','plan_code_pin':'def plan_code_pin(plan, *, source=None):','snapshot_code_sources':'def snapshot_code_sources(action, plan, *, source=None):'}[sym]
        existing('L','demiflow/execution/chain.py',sym,'沿用原返回形状；新增source依赖的有界内容',
            '现状pin与归档已共用_business_dependency_modules，不重写已正确BFS。只扩展source_callable_roots并保证两次收集同集合、封存期漂移拒绝；旧无source调用保持旧字节。新增source保护不能反向声称旧记录已覆盖，缺保护的可恢复source组合明确拒绝。',
            ['HISTORY','IDENTITY','RELATIONAL'],'C08',calls=['source_callable_roots'] if sym=='_business_dependency_modules' else ['_business_dependency_modules'],target=sig,effects='durable_write' if sym=='snapshot_code_sources' else 'read_only')
    existing('L','demiflow/execution/chain.py','seal_write_plan_ref','ProgramRef五键不变；code-pin-only载体',
        'program_id=None保留原单program兼容键；新多slice必须显式program_id，pin_key=identity([action_id,"code-pin-write",program_id])，不能含attempt/pickle字节。source传给pin与归档同一集合。恢复读原记录核对，不重新封存覆盖；同键不同语义仍fail-closed。新旧schema缺少source保证须明确拒绝恢复，而非猜测通过。',
        ['HISTORY','WRITE','RELATIONAL'],'C08',calls=['plan_code_pin','snapshot_code_sources'],target='def seal_write_plan_ref(action, plan, *, source=None, program_id=None):',effects='durable_write')
    edit('L030','native_driver_v1每个ProgramSlice调用seal_write_plan_ref(action,slice.plan,source=slice.source,program_id=slice.program_id)，不cloudpickle已物化actor。独立worker路径继续原stage_plan_artifact，不由此函数偷偷改交付方式。ProgramRef五键保持；source和operator映射完整；恢复优先读取原引用。',calls=['seal_write_plan_ref'],effects='durable_write')
    edit('L031','按稳定input_path定位真实子程序，native driver核code-pin-only记录及带source的plan_code_pin，worker仍用verify_loaded_plan。载体种类由封存backend/placement确定，不按同样五键猜测。缺source保护、多slice错键/漏段均拒绝，不用当前代码重写旧档案。',calls=['plan_code_pin','verify_loaded_plan'],effects='read_only')
    existing('L','demiflow/execution/dataset_native.py','Engine.python_rows','原Python子阶段行迭代器',
        '这是_apply_plan第十个真实调用点；按本Engine的execution及对应input_path把子阶段binding传入_native=False调用，不能重新选backend或丢预算。保留原stats合并与finally。',
        ['LAYOUT','RELATIONAL','BOUNDS'],'C08',calls=['LocalDatasetExecutor._apply_plan'])
    edit('F002','安全的已支持事实保持稳定；model_map只投影非秘密业务语义，凭据和运行环境路径不进入永久事实。此前泄漏形态不能同时承诺逐字节不变：新运行按新平台revision封存，旧载体只读不改写，恢复遵循原版本拒绝。候选按函数吸收且先过合成秘密负例。',calls=byid['F002']['calls'])

    # Q: solve ceiling/floor before seal, not a new resource subsystem.
    existing('Q','demiflow/execution/reservation.py','derive_prefetch','原prefetch字典或ReservationError(INVALID_SPEC)',
        '正数inflight_slots路径验证max_pending_floor<=min(inflight_slots,legacy max_pending)，否则拒绝，不强行max越界。None的旧非绑定路径保留原legacy行为，不借此宣称资源准入已完成。调用者必须先联合求解credit，不能仅删除max后上线。',
        ['BOUNDS','GRAPH','RECOVERY'],'C06',effects='pure')
    add('Q','demiflow/execution/reservation.py','resolve_prefetch_credit','{prefetch:dict,credit_budget:{rows:int,bytes:int}}；全新有界值',
        '封存/launch前联合检查槽和字节grant与通道credit。显式credit不满足则在读源前拒绝；默认credit可收紧到grant并把同一结果传封存、router和InputGate。恢复仅验证原封存，不动态改credit。调用derive_prefetch，保证floor<=ceiling；不修改输入或申请第二份grant。',
        ['BOUNDS','GRAPH','RECOVERY'],contract='C06',new_signature='def resolve_prefetch_credit(inflight_slots, *, inflight_bytes, credit_budget, credit_explicit):',calls=['derive_prefetch'],effects='pure')
    for tid in ['L017','L018']:
        edit(tid,byid[tid]['behavior']+' 初跑在封存之前resolve_prefetch_credit；恢复验证原封存credit与grant，不重新求另一份布局。',calls=byid[tid]['calls']+['resolve_prefetch_credit'])

    # W: reuse the real resolver and extend the HTTP boundary and existing UI functions.
    declaration('W','demiflow/workbench/api.py','GET_FIELDS','只扩graph-source可选键logical_node_id/occurrence_id；保留graph_binding/node_id/kind必填和其他route精确键集。同步forge-http-api的graph-source字段说明，不改HTTP版本或权限。','C09',['WORKBENCH','HISTORY'])
    existing('W','demiflow/workbench/graph/source_view.py','get','原source-view响应；仍只读',
        '既有函数已接受logical_node_id/occurrence_id，主要verify_preserve转发与响应身份。logical/occurrence必须同绑定action/attempt，错配拒绝；无桥或不唯一不猜首个。response携带实际解析身份，旧node_id调用兼容。',
        ['WORKBENCH','HISTORY'],'C09',calls=['bound_source'],mode='verify_preserve',effects='read_only')
    existing('W','demiflow/workbench/graph/model.py','_resolve_bridge_node','原report_node_id或明确错误',
        '既有生产函数，非新建。验证同action/report的二元选择，不唯一或不匹配不得猜首个；保持原只读桥及历史载体验证。',
        ['WORKBENCH','HISTORY'],'C09',mode='verify_preserve',effects='read_only')
    for sym in ['chooseNode','loadSource']:
        add_js('demiflow/workbench/web/workbench.js',sym,sym+'(id)' if sym=='chooseNode' else sym+'()',
            '扩展现有函数，不新建解析体系。保留report_node_id标量state.node；额外逻辑选择存state.logical_selection，source请求携带logical/occurrence。捕获ticket和选择版本，迟到/跨attempt响应丢弃；历史载体只读，物理摘要用logical ID。')
    add_js('demiflow/workbench/web/workbench-state.js','create','create(project)',
        '保留所有既有状态API。value新增logical_selection=null；chooseNode(id,selection=null)校验并同时更新node及logical_selection、递增detail；transition清空二元选择；capture/valid仍拒跨run和旧详情。rememberView回调继续存原report_node_id，重开未恢复二元选择时诚实要求重新选择，不猜位置。')
    edit('W020','按logical_node_id绘制唯一顶点及原logical边；nodeMap仍按logical ID，不拆occurrence顶点。occurrence列表由详情面板承接；selectedId是logical ID或null。来源sealed不等于运行成功，原physical键域不变。')
    byid['W020']['input'].update(nodeMap='logical_node_id到已验证report节点的原映射；多义项不能猜首个',selectedId='logical_node_id或null；occurrence在详情Selection内')
    edit('W021','只高亮logical_node_id顶点；occurrence选择不增加图顶点/边；切action/换图清空无效选择。')
    byid['W021']['input']['id']='logical_node_id或null，不是occurrence_id'
    edit('W019','复用已有bridgeEntryFor和occurrence picker；一个logical顶点的详情可选择occurrence，传给chooseNode及source请求，物理摘要仍按logical ID。旧桥缺失/多义时显示不可用，不重算DAG。',calls=['chooseNode','bridgeEntryFor'])
    edit('W001','复用中立reader读取F1 sealed_plan_facts.v1；v1/v2区别仅属execution_bindings。历史schema校验不依赖当前业务代码或verify_seal；本轮dag.py冻结，长期历史格式策略仍待单独落实。')

    # V: attach semantic counterexamples to executable leaf contracts, not only prose.
    assertions={
      'V001':['公开take/take_all/iter/count/write各一次action，内部delivery借用；旧直接内部入口自建兼容不嵌套','生成器close/早停/取消先drain后退出，不遗留active action','篡改封存backend/profile/program在业务读取前拒绝；真实consumer记录绑定ID匹配，而非只数unified_chain_eligible'],
      'V007':['native两个fresh请求总额度=1只有一个transport；两个node共享run总额度','新budget helper ceiling=0拒绝；EmbeddingActor原构造器仍拒绝max_requests=0，不误报活缺陷','reserve事务失败不出现仅扣额度无intent；相同组件重复reserve幂等，组件改变不可双重扣账','经过真实BlockingIOPool到_enqueue的上下文到达writer payload；writer不直接读ContextVar；异常/取消后token复原','inflight覆盖reserve/HTTP/持久化；aclose先drain，超时不关闭仍使用资源；未知效果不重发'],
      'V008':['普通buffer c=2各有pending，全部进入同cut；重复slot/错cut拒绝，EOF参与者不死等','native row-local单token保存全注册表，原input/emit锁保护不被改变','group timeout/压力flush/空源prepend及快照前后崩溃保持多重性和顺序','restore batch_ordinal拒bool/负数/错类型；旧{batch_ordinal,data}可读，满批及字节守卫不削弱'],
      'V010':['native driver真实PromptActor带锁封存不pickle；同action双slice使用不同稳定pin key，恢复无attempt漂移','覆盖_apply_plan所有10个调用点含Engine.python_rows(_native=False)，绑定execution不得丢失'],
      'V013':['真实HTTP graph-source同时传logical_node_id/occurrence_id不被白名单误拒，错配明确拒绝','同函数两occurrence仅一logical顶点；各自源码/参数/观测准确；physical摘要仍可读；重绘和迟到响应不串位'],
      'V018':['纯derive_prefetch(1,max_pending_floor=2)拒绝；joint resolver默认credit收紧、显式不兼容credit先拒绝；router/InputGate/封存同值','Arrow真实TaskScheduler和临时文件必须受同一grant约束；只传execution对象不算通过；该grant装配仍是发布阻塞项'],
      'V020':['凭据轮换不改变安全模型语义身份；合成API key/Authorization值不得出现在事实/code-pin/历史源码公开投影','旧载体不改写，当前代码或版本不兼容只拒执行不拒合法历史阅读'],
      'V021':['只改source reducer导入helper后resume拒绝；operations原BFS保护保留；source相同input_path重复定位不混段'],
    }
    for tid,items in assertions.items():
        t=byid[tid];t['acceptance_assertions']=items;t['behavior']+=' 必须满足卡内全部验收断言，逐项保留正反例证据。';t['review_revision']='20261114-r2'
    for t in tasks:
        if t['symbol']=='chooseNode' and t['file'].endswith('workbench.js'):
            t['signature']='chooseNode(id, selection=null)';t['input']['selection']=params['selection']
        if t['symbol']=='LocalDatasetExecutor._execution_capabilities':
            t['calls']=[]

    for t in tasks:
        if t['symbol']=='native_request_budget':t['input']['node_id']='稳定的原生执行节点预算键；来自本action的node绑定，不是工作台report节点ID'

    t=edit('W015','保留原logical_node_id到唯一report_node_id映射，多义不猜测；不新增occurrence图键域。',mode='verify_preserve')
    signature(t,'def _bridge_node_map(entries):')
    edit('S009','统一policy：declaration factory的True代表平台deferred适配机制可用，不表示实例已验；instance完整hooks或可安全auto_json才True。不得构造class，不要求class必须自带两个钩子。',calls=['classify_operator_state'])

    for t in tasks:
        if t['id'] not in byid:t['review_revision']='20261114-r2'
    for tid in ['A001','L001','L002','L004','L007','L008']:
        byid[tid]['behavior']+=' recovery字段使用RecoveryGuarantee四值描述执行能力，非用户可选策略；不得与queue proof的final_commit字段互相转换。v1缺此字段不补造能力。'

    signature(byid['R015'],'def _make_worker(stage, q_in, q_out, stats, on_progress, log_every, consume=None, unit_driver=None, *, worker_slot=None):')
    byid['R012']['behavior']+=' _arun按实际worker创建循环的稳定索引传worker_slot给_make_worker，不能在arrive临时按到达次序分配slot。'
    byid['R012']['calls']+=['_make_worker']
    byid['R015']['input']['worker_slot']='_arun创建worker时给定的稳定0..concurrency-1索引；checkpoint路径不得None，native单token分支不以slot数量改变协议'

    params['aggs']='非空AggregateFnV2位置参数序列；名称唯一，原merge/空输入/返回值语义保持'
    for sym in ['take_batch','aggregate','size_bytes','_write_file']:
        target='write_file' if sym=='_write_file' else sym
        existing('A','demiflow/data/dataset.py','Dataset.'+sym,'原公开/内部终端返回值',
            '本轮独立源码追查：当前仅observe_action而无RunAction。补唯一checked.action边界并沿checked后的source/plan调用executor；不绕过前置校验、不调用第二公开终端。_write_file是parquet/json/csv三个薄包装的唯一汇合点；写出后原validate_write_result及observer保留。',
            ['ADMISSION','LAYOUT','WRITE'],'C03',calls=['LocalDatasetExecutor.'+target])
    byid['V001']['acceptance_assertions'].append('补验take_batch/aggregate/size_bytes与parquet-json-csv共用_write_file边界；每个实际执行终端只有一次action，纯元数据读取不造计算')
