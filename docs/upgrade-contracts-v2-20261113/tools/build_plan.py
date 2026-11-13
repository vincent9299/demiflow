import ast, hashlib, json, re, subprocess
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / 'docs/upgrade-contracts-v2-20261113'
OUT.mkdir(parents=True, exist_ok=True)
(OUT / 'tasks').mkdir(exist_ok=True)
TASKS = []
PARAMS = {
 'self':'所属类实例；内部字段见 class-state.json；只按本方法的 ownership 修改',
 'source':'data.sources.SourcePlan；逻辑源声明，不读取数据；执行入口收到已固定源',
 'plan':'data.plan.LogicalPlan；operations 为有序不可变算子序列，禁止就地改写',
 'operation':'data.plan.LogicalOp 的一个原生实例；不接受任意第三方伪装子类',
 'op':'同 operation：原生 LogicalOp', 'target':'CallableSpec.target；函数、类工厂或实例；编译阶段不可构造',
 'owner':'已构造 callable 实例或纯函数；资源句柄不得混同语义状态',
 'traits':'映射；state_ownership/cardinality/ordering/effect_kind/batching 使用既有枚举',
 'callable_spec':'data.plan.CallableSpec；target、constructor_args/kwargs、call_args/kwargs、runtime_contract',
 'run':'execution.run_context.RunContext；借用；不能另建 journal/allocator 或关闭调用者资源',
 'action':'execution.run_context.RunAction；action_id/context/fence 绑定原执行权威',
 'action_id':'非空稳定 action 标识；不得使用 attempt/PID/时钟生成替代身份',
 'run_id':'原 run 标识；只能读取/控制当前项目获准的运行',
 'root':'业务项目 Path；授权根，不能替换为任意用户路径',
 'directory':'已有 RunContext 根 Path；reader 不初始化或修复目录',
 'executor':'已有 LocalDatasetExecutor 或对应后端实例；资源归原所有者',
 'dataset':'已有 Dataset；_source/_plan/_executor 组合，不调用额外公开 action',
 'logical_dag':'已验证 LogicalDag v1 文档；source/node/edge/terminal 身份固定',
 'physical_plan':'PhysicalExecutionPlan v1 文档；stages/partitions/channels/recovery/worker_groups',
 'physical':'同 physical_plan；不丢弃后按当前配置重算',
 'bindings':'{ref:ArtifactRef,document:ExecutionBindingsV1|V2}；不能只传 document',
 'manifest_ref':'ManifestRef {manifest_id,sha256}；必须与 F1/bindings 一致',
 'plan_ref':'ProgramRef；载体、代码摘要与代码来源同一程序',
 'layout':'ExecutionLayout 递归只读值；C08 固定；不是可变调度器',
 'execution':'ExecutionPlacement 或 None；绑定调用不得为 None，预算和 backend 取该值',
 'execution_id':'layout.executions 中唯一标识；不能按函数名模糊匹配',
 'assignment':'已验证 WorkerAssignment；只属于选定 group/stage/partition',
 'authority':'已有 worker/run authority；含目录/fence/grant，不可复制成另一权威',
 'launch_spec':'原启动字典，含 execution_plan={action_id,group_id,bindings_ref}',
 'group_id':'现有 worker group 标识；driver placement 必须为 None',
 'source_reference':'已固定源 Artifact/identity 引用；不含临时 cursor',
 'programs':'v1 program 列表，字段见 C08/v1；v2 必须为空并使用 executions',
 'executions':'v2 execution 条目列表或 None；键集见 C08',
 'profiles':'profile_ref→ResourceProfile 文档；profile_ref 是文档 identity',
 'driver_stage_ids':'driver 所有的 stage ID 序列；与 worker operation stages 无重叠',
 'source_bindings':'[{stage_id,kind,ref}]；fixed_source 或 queue_declaration',
 'channel_options':'每条物理 channel 一条选项；logical_edge_ids 精确覆盖',
 'checkpoint_rows':'正整数；不得超过封存 unconfirmed_rows 或制造无法推进的 cut',
 'supported_slice':'v1 三切片，或 v2 native_driver_v1；不得静默扩大',
 'row':'有界 RuntimeValue 行；保留 dict/list/tuple/bytes/标量与数值编码语义',
 'value':'本函数契约定义的输入值；不改变调用者对象；具体 shape 见输出/语义栏',
 'facts':'operation_facts 结果序列；无客户端/凭证/不可序列化对象',
 'limits':'原 run 有限预算映射；任何有效值只能收紧，不能扩大上限',
 'max_bytes':'正整数字节上限；先检查/有界解析，不先全量分配',
 'depth':'非负递归深度；保持既有深度/节点/字节上限',
 'ref':'ArtifactRef；大小、摘要与路径必须三重校验',
 'sealed_plan':'load_sealed_plan 返回 {facts,logical_dag,physical_plan}',
 'sealed_operators':'[{logical_node_id,occurrence_id}]；顺序与实际 program 对应',
 'terminal_kind':'已支持 action/terminal kind；用于逻辑身份，不混用 UI 名称',
 'sink_ids':'稳定 sink 标识序列；同计划重复编译不变',
 'backend':'受支持后端枚举；不能仅凭名字声称本机已验多机/GPU/Ray',
 'phase':'declaration 或 instance；class 工厂只在前者允许 deferred',
 'ownership':'none/partitionable/shared_colocated；来自统一状态策略',
 'state':'有界恢复状态；schema/字段/类型先验通过才修改实际对象',
 'snapshot':'C05 StageBufferSnapshot 或明确支持的旧 fixed-batch shape',
 'barrier':'现有 cut/barrier 实例；snapshot_bytes/states 归唯一提交路径',
 'pending':'当前 cut 的未消费输入/有序分组引用；不含全历史',
 'stage':'stream._Stage；stage_budget/operation_index/native adapter 与封存位置一致',
 'stage_key':'现有 node-<index> 稳定状态键；不得随机生成',
 'restored':'已验证原状态字典或 None；None 表示首次执行',
 'key':'同一 stage 的稳定恢复键；共享 owner 仍只 snapshot 一次',
 'prefixes':'本次 cut 涉及的 stage key 前缀集合；不能扫所有历史状态',
 'budget':'原 barrier 或 run 聚合状态预算；只借用，不分配第二份额度',
 'stages':'由真实 plan lowering 得到的 _Stage 有序序列',
 'index':'实际 stage 整数索引；范围必须验证',
 'batch':'当前 buffer 的有界行序列；不得隐式物化全部流',
 'stage_limits':'按 operation_index 绑定的预算；真实 reader/queue/sink 必须消费',
 'checkpoint_policy':'CheckpointPolicy 或 None；绑定路径不得用 None 绕过封存策略',
 'client':'Actor 已拥有的 transport client；适配器借用，不创建第二连接所有者',
 'actor':'真实 PromptActor/EmbeddingActor/原生 Actor；禁止测试假类冒充生产适配',
 'node_id':'真实 occurrence/stage 对应键；不能用随机 constructor UUID',
 'request':'原生模型 request，含规范化 request_key、模型与输入语义',
 'encoded':'原请求有限编码；秘密不入身份/报告',
 'timings':'本次请求阶段计时映射；不是成功/恢复权威',
 'depth':'正整数队列深度或 describe 递归深度；具体边界按所在接口',
 'max_item_bytes':'单行字节上限，None 仅在未绑定路径从原 run 解析',
 'source_batch_size':'1..256 的预读行数；queue source 固定为 1',
 'retained_rows':'0..max_output_rows，交付端保留的行数',
 'spec':'LanceWriteSpec；URI/mode/schema/version/operation_id 保持原事务语义',
 'native_options':'既有 NativeOptions 或 None；不吞掉未知选项',
 'schema':'真实 Arrow schema；不能把已知字段类型改为字符串凑通过',
 'arrow':'bool；输出 Arrow batch 或 Python row 的原生分支',
 'sink_schema':'Arrow schema 或 None；只决定投影/编码，不改变业务值',
 'limit':'非负行数或 None；0 仍需静态检查但不读源',
 'graph_spec':'已有显式 source/ancestor/targets graph 规格；不从 UI 图重建',
 'ancestor':'实际 ancestor LogicalPlan；代码与参数纳入 pin',
 'targets':'真实 target plan 映射/序列；标签与程序顺序按原 graph contract',
 'bridge':'IdentityBridge v1；entries 是真实报告→执行节点映射',
 'observations':'既有分页 observation 文档；保留 cursor/revision/attempt',
 'attempt':'正整数 attempt；必须与请求上下文和 observation 页一致',
 'report_digest':'已经 verify_seal 验证的 manifest.report_digest；不是 prepare digest',
 'dag_id':'真实封存 DAG 摘要；不得根据静态报告重算',
 'declared_view':'当前声明图视图；覆盖时保持 declared/sealed/executed 的区别',
 'binding':'当前项目/运行/源码绑定；身份字段以实际 view contract 为准',
 'node_id':'报告节点或 occurrence 标识，按接口选择；禁止只凭显示名称匹配',
 'entry':'项目入口 module:symbol 或既有入口规格；从授权项目解析',
 'params':'声明式参数字典；schema/type/来源固定，不能直接 eval 任意字符串',
 'expected':'原内容/revision 摘要；写入/控制前核验，冲突不自动覆盖',
 'expected_attempt':'预期当前 attempt；旧 attempt 不得控制新进程',
 'recover':'bool；仅允许已支持原生恢复语义，不开全局忽略守卫开关',
 'include_report':'bool；控制有界读取报告，不触发重新检查/执行',
 'compact':'bool；保留原摘要返回格式',
 'model':'固定模型契约；凭证只引用，不写进文档',
 'rows':'有界行/batch 输入；保持顺序约定和重复多重性',
 'document':'对应领域已验证文档；与版本/CAS一起使用',
 'patch':'原 Notebook patch 文档；严格允许键集',
 'source_batch_size':'预读正整数；queue source=1，其余<=256',
}

PARAMS.update({
 'catch':'tuple[Exception class,...] 或 None；规范化保留 null/[] 差异',
 'action_kind':'str；公开 terminal/action 名，不能由用户任意注入私有kind',
 'on_progress':'Callable[[StreamStats],None|Awaitable] 或 None；原进度回调',
 'on_drain':'Callable[[StreamStats],None|Awaitable] 或 None；成功/失败/取消均有收尾语义',
 'log_every':'int>=0；进度采样间隔，不参与计划语义',
 'cancellation':'已有取消信号对象/None；不得另建互不传播的取消源',
 'queue_factory':'原测试 seam 或 None；生产必须符合 native bounded queue 契约',
 'stall_timeout':'float>0 或 None；原有限等待，不能静默放大',
 'stop_file':'Path/str或None；只读停止信号，非任意文件执行',
 'checkpoint':'原退休 facade 参数；非None按现有契约明确拒绝',
 'logical_node_id':'已封存 logical node ID，非显示名称',
 'occurrence_id':'同一 DAG 中实际出现位置ID，不能以逻辑节点代替 occurrence',
 'artifact_manifest_ref':'ManifestRef {manifest_id,sha256}',
 'module_key':'str，p_<24hex>，由实际artifact bundle提供',
 'bound_inputs':'dict[参数名,字段路径]或None；保持原输入绑定',
 'bound_outputs':'dict[返回键,字段路径]或None；保持原输出绑定',
 'profile_ref':'ResourceProfile 文档的identity字符串或原接口允许的None',
 'native_options_digest':'64hex或None；绑定规范化原后端选项',
 'batching':'{unit:record|batch,max_batch?:int,flush_interval_ms?:int}或None',
 'context':'原 CommitContext，含 commit_id/run/action/fence；不是新事务',
 'name':'str；stage显示/观测名称，不替代稳定stage ID',
 'fn':'已解析 callable/StreamMapRuntime；遵循原生命周期',
 'concurrency':'int>=1；真实并发上限且计入驻留预算',
 'queue_depth':'int>=1；真实队列槽上限，仍受字节上限',
 'pre_filters':'有序 callable 列表；状态/异常同原operator规则',
 'post_filters':'有序 callable 列表；不得吞掉取消',
 'hard_timeout':'float>0或None；执行超时不等同副作用未发生',
 'max_batch':'int>=1或None；保留原batch语义，不为验收强改1',
 'flush_interval':'float>0或None；窗口语义进入buffer恢复合同',
 'flat_map':'bool；保持展开与多重性约定',
 'source_iter':'Iterator[RuntimeRow]；借用，异常/取消正确关闭',
 'stats':'StreamStats实例；观测而非提交权威',
 'consume':'Callable[[RuntimeRow],Awaitable|None]或None；下游有背压',
 'progress':'现有StreamStageProgress/WorkerProgress对象或None；不得新建平行游标',
 'unit_driver':'已有UnitDriver或None；控制权仍属原运行',
 'on_stop':'已有停止回调或None；按原native语义传播',
 'declaration':'原原生action声明字典；source/operations/arguments等语义固定',
 'intent':'原write intent字典；含spec/request_hash/operation_id，逐字段验证',
 'blocks':'Iterator[RuntimeRow]或None；仅在本次prepare内有界消费',
 'owner_limits':'原owner budget映射或None；只能收紧run预算',
 'arguments':'原action参数映射或None；source_batch_size/_retained_rows等',
 'doc':'ExecutionBindings v1/v2文档；schema决定精确键集',
 'host_id':'str，当前资源provider主机标识；不参与逻辑DAG身份',
 'resource_root':'原ResourceManager登记根Path或None；不能创建第二allocator',
 'control_timeout_s':'float>0；control deadline，遵循原默认120秒',
 'source_plan':'SourcePlan；queue声明须匹配原task来源与grant',
 'sink':'已存在Datasink/LanceSink；提交权威由driver持有',
 'basis_kind':'source|report|run；互斥身份字段按graph.model.get校验',
 'node_offset':'int 0..65536；当前页起点，不能当全历史缺席证据',
 'include_parameters':'bool；是否读取已绑定参数，不执行配置代码',
 'outputs':'声明输出映射或None；路径、mode、版本与检查报告一致',
 'new_outputs':'bool；保留准备阶段的输出目标规则',
 'inputs':'固定输入引用映射或None；不能resume时追随新HEAD',
 'request_id':'调用幂等键str或None；不是run/attempt标识',
 'telemetry':'bool；观测开关，不改变业务结果与成功语义',
 'tmp_path':'pytest Path；调用必须指定 /tmp/demiflow-* 的basetemp',
})

PARAMS.update({
 'decision':'ActionDecision；准入后的route/backend/recovery不可在执行时重选',
 'available_backends':'tuple[str,...]；executor已拥有后端能力声明，不做运行时探测',
 'sink_spec':'LanceWriteSpec或None；只有write允许非None，Arrow必须含schema',
 'program_slices':'tuple[ProgramSlice,...]；完整真实PlanInput结构遍历输出，路径唯一',
 'q_in':'ObservedQueue或等价原native bounded queue，单stage输入且保留cut控制帧',
 'q_out':'已有下游有界队列；消费与背压由同一资源scope所有',
 'on_start':'Callable或None；原运行开始生命周期回调',
 'on_close':'Callable或None；原关闭回调，失败取消也必须执行',
 'topology':'已有封存拓扑tuple或None；不得重算历史身份',
 'native_layout':'ExecutionLayout或None；只有v2 driver绑定使用',
 'native_execution_id':'str或None；native_layout存在时必须在其executions中',
 'graph':'工作台graph响应；logical action列表、completeness和basis来自后端',
 'id':'occurrence_id显示键；不得以logical_node_id合并多次出现',
 'declared':'report_node_id字符串或null；历史映射不可猜测',
 'dag':'已封存或明确标声明的LogicalDag文档',
 'nodeMap':'occurrence_id到已验证report节点的映射；缺项保持不可用',
 'labels':'逻辑节点显示标签映射；文本渲染，不解析成HTML',
 'selectedId':'occurrence_id或null；与所选action/attempt绑定',
 'basis':'declared/sealed，消费后端事实；旧basis=executed仅指历史封存来源，不能当成功状态',
})

PARAMS.update({
 'next_ordinal':'int>=0；本cut之后的输入frontier，恢复不得从0重启',
 'sealed':'原已prepare的output descriptor，receipt前不可视为提交',
 'states':'本cut状态引用映射，由原StateArtifacts保存',
 'final':'bool；最后cut标志，不等于commit已经ACK',
 'contribution':'graph worker贡献帧；operation/sequence/channel身份保持原协议',
 'stack':'contextlib.ExitStack；Engine借用的资源收尾栈，不关闭外层run',
 'rel':'dataset_native.Relation；固定schema/keys/order及payload_row_bytes估计',
 'prefer_hash_join':'bool；已有DataFusion优化偏好，不替代资源准入',
 'lance_merge':'bool；已有writer/merge保留量模式，不改变输出事务语义',
 'entries':'IdentityBridge.entries；精确action/dag/report已经校验的映射记录',
 'key_kind':'logical或occurrence；默认logical兼容旧消费者，歧义键省略不猜',
 'result':'read_sealed_plan返回的available/facts/logical_dag/physical_plan载体，先校验再展示',
 'model':'EmbeddingModel或原ModelSpec；固定provider/base_url/model_id/维度等非秘密配置',
 'output':'str；写入向量的结果字段名，保持原API验证',
 'call_output':'str或None；请求元信息输出字段名',
 'error_output':'str或None；声明允许的错误记录字段，不吞取消',
 'label':'str或None；用户显示标签，不替代身份',
 'options':'规范化embedding options映射；超时、连接、IO、响应字节上限等',
 'max_requests':'int>=0或None；fresh请求预算，重用不重复计费',
 'service':'现有模型服务声明或None；生命周期归原owner',
 'request_gate':'既有request_policy准入对象；不新建第二份fresh预算',
})

def locate(path, symbol):
    tree=ast.parse((ROOT/path).read_text()); current=tree
    for part in symbol.split('.'):
        current=next((n for n in current.body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef,ast.ClassDef)) and n.name==part),None)
        if current is None: raise ValueError(f'Missing symbol {path}:{symbol}')
    return current

def add(owner,path,symbol,result,behavior,gates,*,contract='C10',calls=(),new_signature=None,mode='implement',effects='runtime_state'):
    number=sum(t['owner']==owner for t in TASKS)+1; tid=f'{owner}{number:03}'
    n=None if new_signature else locate(path,symbol)
    if n is not None:
        signature=('async ' if isinstance(n,ast.AsyncFunctionDef) else '')+symbol+'('+ast.unparse(n.args)+')'
        params=[a.arg for a in [*n.args.posonlyargs,*n.args.args,*n.args.kwonlyargs]]
        src=ast.get_source_segment((ROOT/path).read_text(),n)
    else:
        signature=new_signature; fn=ast.parse(new_signature.replace(symbol, symbol.split('.')[-1],1)+'\n    pass').body[0]
        params=[a.arg for a in [*fn.args.posonlyargs,*fn.args.args,*fn.args.kwonlyargs]];src=''
    task={'id':tid,'owner':owner,'file':path,'symbol':symbol,'mode':mode,'signature':signature,'baseline_signature':signature if n else None,'language':'python',
          'baseline_line':getattr(n,'lineno',None),'baseline_function_sha256':hashlib.sha256(src.encode()).hexdigest() if src else None,
          'input':{p:PARAMS[p] for p in params},
          'output':result,'behavior':behavior,'contract':contract,'effects':effects,'calls':list(calls),'gates':list(gates),'status':'planned'}
    TASKS.append(task);return tid

# F: actual semantics and source graph, no user code execution.
add('F','demiflow/execution/plan_facts.py','operation_capability','CapabilityFact，C02 基础键，不新增 admitted 字段','唯一原生 kind 权威；保留原生特化，拒绝伪装第三方；新旧事实兼容。',['FACTS','HISTORY'],contract='C02',effects='pure')
add('F','demiflow/execution/plan_facts.py','operation_facts','OperationFact：C02 arguments/native/reserved_bytes','按 kind 提取完整参数；13 类逐个验证，5 特化 domain 有界；已有支持输出逐字节不变。',['FACTS','VECTOR','EMBED','PREPEND','HISTORY'],contract='C02',calls=['operation_capability'],effects='pure')
add('F','demiflow/execution/plan_facts.py','_catch_facts','None 或 sorted list[str]；原空值 null/[] 不变','保留 catch 空值历史，异常类名确定；不把 cancellation 吃掉。',['FACTS','HISTORY'],contract='C02',mode='verify_preserve',effects='pure')
add('F','demiflow/planning/dataset_dag.py','compile_dataset_logical_dag','(LogicalDag v1, tuple[ProgramSlice,...])；路径完整且局部程序→全图stage映射唯一','递归展开实际 PlanInput/KeyedSource/UnionSource，循环/节点预算先验；复用 dag.py 公共 API；稳定 occurrence，不调用业务。',['LAYOUT','RELATIONAL','HISTORY'],contract='C08',new_signature='def compile_dataset_logical_dag(*, action_id, source, plan, terminal_kind, sink_ids):',effects='pure')
add('F','demiflow/planning/graph_dag.py','compile_graph_logical_dag','(LogicalDag v1, list[program mapping])；保留现有group_id/role/reader_id/operators键','保留现有静态 fan-out 的编译与身份，验证布局变化不会漂移 dag。',['GRAPH','HISTORY'],contract='C08',mode='verify_preserve',effects='pure')

# A: action selection, early validation, one local executor writer.
add('A','demiflow/execution/capabilities.py','admit_action_plan','ActionDecision(action_kind,backend,route,recovery)','只做静态准入和路线决定，保留合法 map(thread).take/write；不构造 actor、不读源。',['ADMISSION','DELIVERY'],contract='C03',new_signature="def admit_action_plan(source, plan, *, action_kind, backend='local', sink_spec=None, available_backends=()):",effects='pure')
add('A','demiflow/execution/capabilities.py','validate_stream_plan','None；无效抛原能力错误','调用统一 capability，整图校验在 payload facts/action/source 之前；无额外准入白名单。',['ADMISSION','FACTS'],contract='C03',calls=['operation_capability'],effects='pure')
add('A','demiflow/data/dataset.py','Dataset.run_stream','StreamStats；原回调/取消/资源语义','保留 e517110 的前置检查，公开入口执行同一 ActionDecision；callback/queue source 参数不得遗漏。',['ADMISSION','STREAM','QUEUE'],contract='C03',calls=['admit_action_plan','validate_stream_plan'])
add('A','demiflow/data/dataset.py','Dataset.count','int >= 0','流式 count 通过已拥有 action 的内部驱动计数，不调用第二公开 action；预算与取消有界。',['ADMISSION','STREAM'],contract='C03',calls=['admit_action_plan'])
add('A','demiflow/data/plan.py','reject_streaming_map_options','None 或 UnsupportedExecutionOptionError','限定为确实不能执行流式 options 的后端/关系边界；不得从流式 terminal 路由前全局拒绝合法调用。',['ADMISSION','DELIVERY'],contract='C03',effects='pure')
add('A','demiflow/execution/executors/local.py','LocalDatasetExecutor.take','list[RuntimeRow]，最多 limit 行','撤销 78d6492 的能力退步；由 decision 路由流式 delivery；take(0) 无 source/actor 副作用；早停保持租约。',['DELIVERY','ADMISSION'],contract='C03',calls=['admit_action_plan','iter_stream_rows','compile_native_topology','execute_native_plan'])
add('A','demiflow/execution/executors/local.py','LocalDatasetExecutor.iter_rows','Iterator[RuntimeRow]；关闭时原所有权收尾','合法流式进入有界 delivery；pull 保留惰性；finally 不提早释放仍在运行 worker。',['DELIVERY','STREAM'],contract='C03',calls=['admit_action_plan','iter_stream_rows','compile_native_topology','execute_native_plan'])
add('A','demiflow/execution/executors/local.py','LocalDatasetExecutor.write_lance','LanceWriteReceipt；uri/version/rows/operation_id等字段保持现有模型','所有 source/stream/relational/Arrow/pure-copy 路径先固定路线与封存 placement，再执行；唯一 LanceSink 提交；恢复复用旧绑定。',['WRITE','RELATIONAL','LAYOUT','HISTORY'],contract='C08',calls=['admit_action_plan','compile_native_topology','execute_native_plan','seal_write_topology'],effects='durable_write')

# S: all operator-state semantics belong together.
add('S','demiflow/execution/operator_policy.py','classify_operator_state','StatePolicy；C04 完整枚举与字段','factory 与 instance 分开；完整 hooks/有界 auto_json/unsupported 三分；没有 hooks 绝不直接等同无状态。',['STATE','IDENTITY'],contract='C04',new_signature="def classify_operator_state(target, *, phase, ownership='none'):",effects='pure')
add('S','demiflow/execution/operator_policy.py','snapshot_auto_state',"{schema:'demiflow.auto_state.v1',fields:dict}",'仅普通 __dict__ 严格 JSON；保留全部状态；先预算校验再独立复制；资源/slots/custom descriptors 明确拒绝。',['STATE','BOUNDS'],contract='C04',new_signature='def snapshot_auto_state(owner, *, max_bytes):',effects='read_only')
add('S','demiflow/execution/operator_policy.py','restore_auto_state','None；失败时 owner 不变','验证 schema、字段、预算后原子恢复字段；Counter 累计值恢复，不执行用户属性/setter。',['STATE','BOUNDS'],contract='C04',new_signature='def restore_auto_state(owner, state, *, max_bytes):')
add('S','demiflow/execution/operator_precheck.py','precheck_operator_state','bool：是否有可用 state adapter；factory 为延后实例检查','调用统一 policy，传 traits ownership；实例无钩子但可安全 auto_json 时用平台适配，不默认静默放过。',['STATE','VECTOR'],contract='C04',calls=['classify_operator_state'],effects='pure')
add('S','demiflow/execution/operator_precheck.py','precheck_plan_state','None；失败使用原 STATE_ADAPTER_MISSING','遍历真实 plan callable，用同一 policy；不建 actor，不做任何源/网络 IO。',['STATE','ADMISSION'],contract='C04',calls=['precheck_operator_state'],effects='pure')
add('S','demiflow/execution/operator_state.py','OperatorStates.validate','None；unsupported/半套 hooks 抛 RecoveryContractError','保留 class 构造前豁免；实例检查 hooks/auto_json；不重复维护另一套可用类型规则。',['STATE'],contract='C04',calls=['classify_operator_state'],effects='read_only')
add('S','demiflow/execution/operator_state.py','OperatorStates.register','None（async）；更新 owners/aliases/identities，必要时恢复','实际实例首行前登记；共享 owner 只存一份；hooks 或 auto_json 同一保存协议；不丢弃无 hook Counter。',['STATE','RECOVERY'],contract='C04',calls=['classify_operator_state','snapshot_auto_state','restore_auto_state'])
add('S','demiflow/execution/operator_state.py','OperatorStates.snapshot','dict[stage_key,JSONState]（async）；聚合受预算限制','统一导出 hooks 与 auto_json 状态，保留前缀对齐/共享 owner；失败不提交半份快照。',['STATE','RECOVERY','BOUNDS'],contract='C04',calls=['snapshot_auto_state'])
add('S','demiflow/execution/contracts/operator.py','_adapter_present','bool','统一 policy 的 adapter_available；不再只探两个方法而忽略 auto_json；不构造 class。',['STATE','LAYOUT'],contract='C04',calls=['classify_operator_state'],effects='pure')
add('S','demiflow/execution/contracts/operator.py','compile_operator_spec','validated OperatorSpec v1；已支持旧输入输出兼容','state.ownership 与 adapter_present 来自同一策略；factory 真正实例检查留运行期；保留 artifact/args digest。',['STATE','LAYOUT','HISTORY'],contract='C04',calls=['_adapter_present'],effects='pure')
add('S','demiflow/planning/traits.py','unified_operation_traits','原 OperationTraits dataclass 值','实例和类的 state ownership 同源；不因为不是 class 就把 native state adapter 丢掉。',['STATE','VECTOR'],contract='C04',calls=['classify_operator_state'],effects='pure')
add('S','demiflow/execution/stream_map.py','_MapWorker.prepare','None（async）；原 callable 生命周期','构造完成后、首行业务之前调用真实 validate/register；恢复后再发输入；取消失败必须关闭已经启动的实例。',['STATE','DELIVERY'],contract='C04',calls=['OperatorStates.validate','OperatorStates.register'])
add('S','demiflow/execution/recovery_identity.py','describe','JSON 语义描述；已有成功类型字节不变','不删除 callable 配置；不把 ContextVar 值抹为类型；资源声明有限且可重建；未知仍明确拒绝。',['IDENTITY','HISTORY','STATE'],contract='C04',effects='read_only')
add('S','demiflow/lance/search.py','VectorSearch.snapshot_state','有界 JSON：固定表版本/语义进度，不含锁/client','为 native VectorSearch 提供真实恢复状态；表头首次固定后不得 resume 时重新取 HEAD；只保存实际存在语义字段。',['VECTOR','STATE','RECOVERY'],contract='C04',new_signature='def VectorSearch.snapshot_state(self):',effects='read_only')
add('S','demiflow/lance/search.py','VectorSearch.restore_state','None','验证原 search/config identity，恢复固定版本；资源留 astart 重建；缺版本/外部变化明确拒绝。',['VECTOR','STATE','RECOVERY'],contract='C04',new_signature='def VectorSearch.restore_state(self, state):')

# R: one owner for native progress, buffers, run recovery.
add('R','demiflow/execution/stage_buffers.py','StageBufferAdapter.__init__','StageBufferAdapter：stage/action/stage_key、kind、受限 pending、ordinal、closed','只引用现有 stage/run；不发请求、不造 journal；按 C05 解析 fixed/concurrent/grouped/prepend。',['BUFFER'],contract='C05',new_signature='def StageBufferAdapter.__init__(self, stage, *, action, stage_key):')
add('R','demiflow/execution/stage_buffers.py','StageBufferAdapter.align','C05 StageBufferSnapshot（async）','停止跨 cut 输入，排空或固化 native 效果，封存待消费行/分组/前缀/ordinal；全部字节计入同一 barrier。',['BUFFER','RECOVERY','UNKNOWN'],contract='C05',new_signature='async def StageBufferAdapter.align(self, barrier, pending):',effects='durable_write')
add('R','demiflow/execution/stage_buffers.py','StageBufferAdapter.restore','None','先验证快照类型/身份/大小，再恢复 buffer 和编号；旧 fixed-batch 可读；先处理 pending 再读新输入。',['BUFFER','RECOVERY'],contract='C05',new_signature='def StageBufferAdapter.restore(self, snapshot):')
add('R','demiflow/execution/stage_buffers.py','StageBufferAdapter.close','None，幂等','释放本实例暂存/句柄，保留已提交引用；不删除别人的工件或已固定历史。',['BUFFER','BOUNDS'],contract='C05',new_signature='def StageBufferAdapter.close(self):')
add('R','demiflow/execution/stage_buffers.py','make_stage_buffer_adapter','StageBufferAdapter | None','无 buffer 返回 None；已知 native kind 建唯一适配器并恢复；未知 buffered shape 不绕过守卫。',['BUFFER'],contract='C05',new_signature='def make_stage_buffer_adapter(stage, *, action, stage_key, restored=None):',calls=['StageBufferAdapter.__init__','StageBufferAdapter.restore'])
add('R','demiflow/execution/stream_materialize.py','StreamStageProgress.bind_stages','None；设置 stages/group_ends/buffer adapter/direct_owners','以真实 stage position 接 C04/C05；用 adapter 替换全局 buffered 拒绝，保留未经适配种类拒绝；不改 batch/concurrency 凑绿。',['BUFFER','EMBED','VECTOR'],contract='C05',calls=['make_stage_buffer_adapter','OperatorStates.validate'])
add('R','demiflow/execution/stream_materialize.py','StreamStageProgress.align','None（async）；写本 cut 的 barrier.states','原 native row-local 快照与 buffer 快照及 Actor 状态同一 cut；不越界、不先发布下游；聚合状态有界。',['BUFFER','RECOVERY','UNKNOWN'],contract='C05',calls=['StageBufferAdapter.align','OperatorStates.snapshot'],effects='durable_write')
add('R','demiflow/execution/stream_materialize.py','StreamStageProgress.restore_stages','None（async）','恢复实例语义状态，然后 buffer/frontier；不在恢复前重新发模型请求。',['STATE','BUFFER','RECOVERY'],contract='C05',calls=['OperatorStates.register','StageBufferAdapter.restore'])
add('R','demiflow/execution/stream_materialize.py','prepare_stream_blocks','Iterator[Arrow/Python batch]；原最终写入输入','消费传入 checkpoint_policy/stage_limits；重放已提交 blocks；单 handoff 背压；异常/取消 join producer。',['WRITE','BUFFER','RECOVERY'],contract='C05',calls=['precheck_plan_state','StreamStageProgress.bind_stages','StreamStageProgress.align'])
add('R','demiflow/execution/stream.py','_Stage.__init__','完整 _Stage 实例；新增 buffer_adapter=None，既有 slots 保留','所有其他属性按基线构造；新 adapter 为本 stage 所有，不模块全局缓存。',['BUFFER'],contract='C05')
add('R','demiflow/execution/stream.py','_materialize','list[_Stage]，operation_index/budget/adapter 位置固定','每个实际 op lowering 一致；预算按布局绑定；不能为 grouping/prefix 换一套执行器。',['BUFFER','LAYOUT'],contract='C05',calls=['validate_stream_plan'])
add('R','demiflow/execution/stream.py','_arun','StreamStats；传入stats实例，rows_in/rows_out/errors/耗时按原结构更新','把 cut 安全点、pending flush 和 adapter align 接入实际循环；效果排空/超时/关闭与现有 native 协议一致。',['BUFFER','RECOVERY','UNKNOWN','BOUNDS'],contract='C05',calls=['StageBufferAdapter.align','StageBufferAdapter.close'])
add('R','demiflow/execution/run_context.py','RunContext.action','contextmanager[RunAction]','保留 nested/attempt/fence/unknown 拒绝与 create-target 精确准入；只修有证据的接入，不扩大恢复特例。',['WRITE','HISTORY','RECOVERY'],contract='C05',mode='verify_preserve',effects='durable_write')
add('R','demiflow/execution/run_context.py','create_target_absent_binding','dict{kind,action_id,operation_id,request_hash,uri,mode}或None；None不是准入','保留 intent/action/operation/request_hash/URI/mode 全字段绑定；未知或既有目标不准入。',['WRITE','HISTORY'],contract='C05',mode='verify_preserve',effects='pure')

# Q: no dependency on state implementation.
add('Q','demiflow/execution/block_sink.py','_canonical_row','值/类型/float bits/字典序保真的 RuntimeValue','使用 IEEE 字节签名区别 signed-zero/NaN；不误合并 bool/int/float；bounded DAG；set 跨进程边界显式。',['CODEC','BOUNDS'],contract='C06',effects='pure')
add('Q','demiflow/execution/block_sink.py','BlockSink.append','None；向本 sink 有界 buffer 追加','继续先 check_size 再编码后上限；编码不改变行语义；flush/rows/aggregate 保留。',['CODEC','BOUNDS'],contract='C06',calls=['_canonical_row'])
add('Q','demiflow/execution/block_sink.py','BlockSink.prepare','BlockDescriptor；原 commit-keyed 幂等预留','保持 commit/sink/component/ordinal reservation；同额复用、异额拒绝；不通过退款/换 ID 绕开恢复问题。',['CODEC','RECOVERY','BOUNDS'],contract='C06',mode='verify_preserve',effects='durable_write')
add('Q','demiflow/execution/payload_limits.py','stream_limits','dict 有效 limits；每项<=输入预算','区分固定驻留与动态槽位；同预算确定准入；低预算失败前不读源；保留显式 max_row ceiling。',['BOUNDS','STREAM'],contract='C06',effects='pure')
add('Q','demiflow/execution/payload_limits.py','payload_scope','contextmanager[dict limits]','读取 F 事实，借用封存/owner limits；finally 恢复 ContextVar；不吞 RuntimeContractError。',['BOUNDS','ADMISSION'],contract='C06',calls=['operation_facts','stream_limits'])
add('Q','demiflow/execution/stream_resources.py','StreamResources.make_queue','ObservedQueue 实例；depth/bytes/item 三重限制','维持 native 队列单实现；不接纳无字节协议 custom factory；为测试提供现有观测边界而非拆守卫。',['BOUNDS','STREAM'],contract='C06')

# M: request state is not actor state or buffer state.
add('M','demiflow/operator_llm/stream_recovery.py','NativePromptRecovery.bind_client','None；将既有 client 绑定 RunPromptJournal','保持已知响应重用、unknown 拒绝、fresh budget 唯一计费；actor 所有连接生命周期不变。',['PROMPT','UNKNOWN'],contract='C07',mode='verify_preserve')
add('M','demiflow/operator_llm/stream_recovery.py','RunPromptJournal.check_recovery','None 或 UncertainPromptCall','unknown 不自动重发；已收到/未发布各窗口保留真实证据；不另建 sqlite。',['PROMPT','UNKNOWN'],contract='C07',mode='verify_preserve',effects='read_only')
add('M','demiflow/embeddings/stream_recovery.py','NativeEmbeddingRecovery.__init__','适配器 actor/action/node_id；借用同一 run effect authority','不启动 client、不发请求；绑定模型与 batch lineage；不创建私有成功库。',['EMBED','UNKNOWN'],contract='C07',new_signature='def NativeEmbeddingRecovery.__init__(self, actor, action, node_id):')
add('M','demiflow/embeddings/stream_recovery.py','NativeEmbeddingRecovery.bind_actor','None','把借用包装绑定actor._journal；client为HTTPX对象，不套用PromptClient.record/execute；close不关闭run journal。',['EMBED','UNKNOWN'],contract='C07',new_signature='def NativeEmbeddingRecovery.bind_actor(self):')
add('M','demiflow/embeddings/runtime.py','EmbeddingActor.native_stream_recovery','NativeEmbeddingRecovery','实际 materialize/worker 路径能够获得适配器；不能只写无消费者 helper。',['EMBED','BUFFER'],contract='C07',new_signature='def EmbeddingActor.native_stream_recovery(self, action, node_id):',calls=['NativeEmbeddingRecovery.__init__'])
add('M','demiflow/embeddings/runtime.py','EmbeddingActor.astart','None（async）；原有客户端/pools 启动','native run 下使用 run-owned journal；明确外部旧 journal 模式边界；有限池只在首次合法动作启动。',['EMBED','BOUNDS'],contract='C07',calls=['NativeEmbeddingRecovery.bind_actor'])
add('M','demiflow/embeddings/runtime.py','EmbeddingActor._exchange','原 embedding call/result 结构（async），结果字段不变','同 request_key 查已知响应；发送前 reserve；响应持久化后返回；fresh/replay 不双计费。',['EMBED','UNKNOWN'],contract='C07',effects='durable_write')

# L: frozen interpreter and real execution bindings, never a second ledger.
add('L','demiflow/execution/plan_interpreter.py','_validate_bindings','解释上下文字典，仅内部消费','v1 严格原样；新增 v2 executions 精确键集/覆盖/driver-worker 互斥/backend/profile校验；不能改 v1 放宽。',['LAYOUT','HISTORY'],contract='C08',effects='pure')
add('L','demiflow/execution/plan_interpreter.py','interpret_physical_plan','ExecutionLayout，增加末尾 executions 默认 tuple','v1 派生 worker executions；v2 显式 driver/worker；workers 单向派生不二次决定；递归只读。',['LAYOUT','HISTORY'],contract='C08',calls=['_validate_bindings'],effects='pure')
add('L','demiflow/execution/plan_interpreter.py','validate_worker_assignment','WorkerPlacement；非法抛 invalid_assignment','严格验证 group/stage/partition/channel；driver execution 不可伪装 worker；运行前 fail-closed。',['LAYOUT','GRAPH','QUEUE'],contract='C08',effects='pure')
add('L','demiflow/execution/sealed_plans.py','seal_execution_bindings','ArtifactRef，journal 中唯一 execution-bindings 记录','加 executions=None keyword；v1 不变，v2用 executions；幂等一致才复用；不创建第二状态库。',['LAYOUT','HISTORY'],contract='C08',calls=['_validate_bindings'],effects='durable_write')
add('L','demiflow/execution/sealed_plans.py','load_execution_bindings','{ref:ArtifactRef,document:ExecutionBindingsV1|V2}','有界只读、schema 分派、固定 F1/manifest/action/run；错误不静默重算或修历史。',['LAYOUT','HISTORY'],contract='C08',effects='read_only')
add('L','demiflow/execution/sealed_plans.py','load_sealed_plan','{facts,logical_dag,physical_plan}','保持历史 F1 只读；无 bindings 与腐坏载体区别；不 import 用户代码。',['LAYOUT','HISTORY'],contract='C08',mode='verify_preserve',effects='read_only')
add('L','demiflow/execution/native_plan.py','compile_native_topology','(logical_dag,physical_plan,bindings_ref)','使用真实 Dataset DAG，driver placement 对应实际 backend，原 compiler 校验；source/terminal 预算同权威。',['LAYOUT','RELATIONAL'],contract='C08',new_signature='def compile_native_topology(action, source, plan, *, decision):',calls=['compile_dataset_logical_dag','stage_native_programs','seal_execution_bindings'],effects='durable_write')
add('L','demiflow/execution/native_plan.py','load_native_execution','(ExecutionLayout,ExecutionPlacement)','读同一封存输入并精确定位 execution_id；不根据当前 defaults/eligible 重选。',['LAYOUT','HISTORY'],contract='C08',new_signature='def load_native_execution(run, action_id, *, execution_id):',calls=['load_sealed_plan','load_execution_bindings','interpret_physical_plan'],effects='read_only')
add('L','demiflow/execution/native_plan.py','execute_native_plan','Iterator[RuntimeRow|ArrowRecordBatch]','核 program/code-pin/固定源；只执行封存 backend、stage预算、恢复策略；generator finally 释放本次资源。',['LAYOUT','RELATIONAL','WRITE'],contract='C08',new_signature='def execute_native_plan(executor, source, plan, *, layout, execution_id, arrow=False, sink_schema=None):',calls=['verify_native_programs'])
add('L','demiflow/execution/dataset_native.py','execute','原 Iterator[rows|ArrowRecordBatch]；增加 execution=None kw','绑定调用使用 placement/profile 的批次/内存/spill约束，禁止内部再自由选择公开 action；DataFusion内部优化不冒充平台节点调度。',['RELATIONAL','BOUNDS'],contract='C08')
add('L','demiflow/execution/arrow_batch_sink.py','execute','Iterator[ArrowRecordBatch]；增加 execution=None kw','绑定 Arrow stage 的真实预算/程序，保留 schema/类型；不走未封存 fast-path。',['RELATIONAL','WRITE','BOUNDS'],contract='C08')
add('L','demiflow/execution/chain.py','seal_write_topology','_WriteSeal；首次封存，恢复读取原绑定','保留已修恢复复用；按 native driver 事实选择 v2，旧 v1继续读；返回值仍供 verify/layout 使用。',['WRITE','LAYOUT','HISTORY'],contract='C08',calls=['compile_native_topology','load_native_execution'],effects='durable_write')
add('L','demiflow/execution/chain.py','_WriteSeal.layout','原 (checkpoint_policy,stage_limits)；由 placement 派生','driver route 不再 load 一个不存在的 worker；worker v1保持兼容；不再读当前run默认替代原预算。',['LAYOUT','WRITE'],contract='C08',calls=['load_native_execution'],effects='read_only')
add('L','demiflow/execution/chain.py','verify_loaded_plan','None 或原拒绝错误','代码pin、operators 全核；新 native driver引用也受检；不能改成信任driver提供的摘要。',['HISTORY','LAYOUT'],contract='C08',mode='verify_preserve',effects='read_only')
for sym,gates in [('run_local_chain',['LAYOUT','RECOVERY']),('run_ray_chain',['RAY','LAYOUT'])]:
 add('L','demiflow/execution/chain.py',sym,'_LocalMaterializedHandle；含_ChainBlocks、terminal.rows、recovery_reference(run_id/action_id/semantic_id)','从解释后的 layout 获取资源/通道/checkpoint/程序；唯一 driver commit；所有权与失败收尾保留。',gates,contract='C08',calls=['interpret_physical_plan'],mode='verify_preserve',effects='durable_write')
for sym,gates in [('run_local_graph',['GRAPH','RECOVERY']),('run_ray_graph',['RAY','GRAPH'])]:
 add('L','demiflow/execution/graph.py',sym,'dict{target_rows,contributions,egress_rows,workers,ancestor_contributions,final_arrival,checkpoints}；基线键集不变','真实 graph spec→同一封存 layout→worker；route ordinal/ACK/recovery恢复一致；不手工重建不同拓扑。',gates,contract='C08',calls=['interpret_physical_plan'],mode='verify_preserve',effects='durable_write')
add('L','demiflow/execution/queue_chain.py','run_local_queue_chain','WriteResult(written_rows,failed_rows,blocks_written,target,metadata)；原claim/finalize/ACK权威不变','worker suffix只消费解释后的配置，driver保持唯一claim/提交；不复制allocator与request journal。',['QUEUE','RECOVERY'],contract='C08',calls=['interpret_physical_plan'],mode='verify_preserve',effects='durable_write')
add('L','demiflow/execution/worker/layout.py','load_worker_layout','(WorkerPlacement,CheckpointPolicy,stage_limits)','核 launch_ref→F1→bindings→assignment；只读；v1/v2选择一致，driver条目不作为worker返回。',['LAYOUT','GRAPH','QUEUE'],contract='C08',calls=['load_execution_bindings','interpret_physical_plan','validate_worker_assignment'],effects='read_only')
for path,cls,gate in [('execution.py','WorkerChainExecutor','LAYOUT'),('graph_execution.py','GraphWorkerExecutor','GRAPH'),('queue_transform.py','QueueTransformExecutor','QUEUE')]:
 for sym in ['_load_plan','start','_run']:
  add('L','demiflow/execution/worker/'+path,cls+'.'+sym,({'_load_plan':'LogicalPlan；反序列化及code-pin/布局检查均通过后返回','start':'None；启动本实例后台执行线程，重复调用按原状态机拒绝','_run':'None；记录统计/错误并设置done，贡献只发送driver'}[sym]),'消费同一布局与code-pin；提交只请求driver；deadline/取消有界；不得在event loop无限future.result。',[gate,'HISTORY','RECOVERY'],contract='C08',calls=['load_worker_layout','verify_loaded_plan'],mode='verify_preserve')

# W: real historical facts and public control, no execution state duplication.
for sym,out,beh in [
 ('read_sealed_plan','原 available 分支文档','复用中立reader识别v1/v2；缺失、损坏、不支持分别显示'),
 ('read_identity_bridge','{available:True,ref,bridge} 或 {available:False,reason,detail}','action/dag/report三重绑定；不猜源码节点'),
 ('join_node_observations','tuple[entry + observation + availability]','按action/attempt/occurrence精确join，分页缺席not_in_page'),
 ('overlay_sealed','原 logical view；增加派生 binding_level','declared/sealed依据合法载体区分，运行状态独立取原观测，历史不重新compile')]:
 add('W','demiflow/workbench/runs/facts.py',sym,out,beh,['WORKBENCH','HISTORY'],contract='C09',effects='read_only')
for sym,out,beh in [('get','原 graph 响应','显示当前声明与历史封存的区别；运行图绑定真实plan/attempt；部分观测诚实标记'),('bound_source','原源码/参数响应','从原sealed carriers取历史源码/参数，当前编辑不能污染历史；节点绑定精确')]:
 add('W','demiflow/workbench/graph/model.py',sym,out,beh,['WORKBENCH','HISTORY'],contract='C09',calls=['read_identity_bridge','join_node_observations'],effects='read_only')
add('W','demiflow/workbench/runs/worker.py','execute','原 worker退出/执行结果；events归原run','传已验证 manifest.report_digest；按值封存实际代码；从公开程序跑真实引擎，错误原样记录。',['WORKBENCH','WRITE','HISTORY'],contract='C09',mode='verify_preserve',effects='durable_write')
for sym,beh in [('prepare','参数/检查/固定输入输出/代码摘要一致，错误不启动'),('start','只启动准备好的同一digest与expected_attempt；接入真实引擎'),('show','同一run/attempt的真实status/outputs/observations；无假成功'),('cancel','仅停止匹配的真实进程；迟到worker不先释放所有权'),('resume','原代码/输入/资源/提交恢复；冲突/unknown明确失败'),('register_results','原生receipt权威；absent-create特例严格；不补造commit')]:
 add('W','demiflow/workbench/runs/manager.py',sym,({'prepare':'RunView；固定准备摘要及未启动状态','start':'RunView；已启动attempt和真实进程状态','show':'RunView；只读指定attempt','cancel':'RunView；停止请求/实际状态，不把尚未退出称已停止','resume':'RunView；新增attempt和保留历史输出','register_results':'None；按原receipt原子更新结果登记'}[sym]),beh,['WORKBENCH','WRITE','HISTORY'],contract='C09',mode='verify_preserve',effects='read_only' if sym=='show' else 'durable_write')

# V: one executable test function per user-visible or cross-module promise.
SCENARIOS=[
 ('test_v2_supported_stream_terminals','ADMISSION','合法 thread map 的 take/take_all/count/write 各自结果正确；真正无效选项拒绝前events为空；take(0)不读源。'),
 ('test_v2_actor_counter_crash_resume','STATE','无钩子普通JSON Counter类及实例，checkpoint后真进程退出，resume累加状态不归零；不支持资源实例必须先拒绝。'),
 ('test_v2_identity_distinguishes_business_configuration','IDENTITY','threshold 0.5/0.9、ContextVar业务值不同、auto_json初始状态不同不能碰撞；等值跨进程身份稳定。'),
 ('test_v2_signed_zero_roundtrip','CODEC','真实BlockSink append/iter_rows，+0/-0 float bits保留；bool/int/float及tuple/bytes保真。'),
 ('test_v2_reprepare_budget_same_bytes','CODEC','同cut多次prepare+crash/resume字节与额度不漂移；真实改变值仍拒绝。'),
 ('test_v2_vector_search_real_state','VECTOR','固定Lance版本、查询/输出真实，Actor实例路径可用；resume不改表头且结果多重性一致。'),
 ('test_v2_embedding_batch_recovery','EMBED','本地HTTP embedding concurrency=2/max_batch=2，不强制降为1；跨cut恢复已知响应不重发，unknown拒绝。'),
 ('test_v2_grouped_and_prepend_buffers','BUFFER','group timeout/pressure/empty-upstream prepend，故障后pending/ordinal/分组边界不丢、不重复；小预算真触发。'),
 ('test_v2_prompt_policy_and_unknown','PROMPT','声明request_policy实限fresh调用；response已存/未存两个真crash窗口，unknown零重发、无私有journal。'),
 ('test_v2_write_routes_consume_layout','LAYOUT','stream/relational/Arrow/pure Lance copy四路真实输出；改变合法封存预算能影响执行，损坏或错assignment在执行前拒绝。'),
 ('test_v2_write_create_recovery','WRITE','工作台prepare/start真实write create→取消/进程丢失→resume，同dag及binding，目标正确，无双提交；外部目标反例不覆盖。'),
 ('test_v2_graph_queue_commit_authority','GRAPH','真实local graph/queue的路由、ACK、replay、worker贡献；worker不拥有CommitCoordinator，driver唯一提交；driver故意不ACK时event-loop heartbeat持续、deadline到期并保持unknown。'),
 ('test_v2_workbench_history_end_to_end','WORKBENCH','浏览器配置→check→run→节点真实source/参数/观测→结果→cancel/resume；编辑当前代码不改变旧run；无假契约通过标签。'),
 ('test_v2_notebook_sql_lifecycle','NOTEBOOK','保存/重开Notebook，Python及SQL执行，固定结果、取消/重启、旧generation拒绝；不创建Pipeline run。'),
 ('test_v2_case_media_review_roundtrip','CASE','固定数据比较、图片/媒体有界读取、Case/方案/字段评测、保存/下一条/草稿恢复、CAS冲突与历史；不重新执行生产者。'),
 ('test_v2_demo_navigation_and_layout','DEMO','D1.9侧栏和四tab、图为主体、窄宽窗口自适应、配置/错误可达；截图与既有demo按用户流程核。'),
 ('test_v2_development_context_isolation','DEVELOPMENT','开发对话/README保留，所属project权限与历史一致；取消不影响Pipeline/Notebook；Forge不import工作台。'),
 ('test_v2_resource_and_performance_envelope','BOUNDS','同预算慢消费者/宽行/倾斜/磁盘不足/取消，峰值与吞吐对比；不扩预算凑通过。'),
 ('test_v2_ray_backend_actual','RAY','真实已配置Ray环境中同接口执行与恢复；不可用则明确未验，不得skip计通过或宣布全局100%。'),
 ('test_v2_historical_read_and_code_drift','HISTORY','历史v1/v2载体只读，业务代码变化拒绝执行；读图不重算，不篡改记录；成功路径dag保持稳定。'),
]
for sym,gate,beh in SCENARIOS:
 add('V','tests/test_upgrade_v2_acceptance.py',sym,'None；断言通过才有验收证据',beh,[gate],contract='C10',new_signature=f'def {sym}(tmp_path):',effects='test_only')

# Functions found during connection review; one owner remains per file.
add('R','demiflow/execution/stream.py','_make_worker','Callable[[],Awaitable[None]]；真实stage worker协程工厂','真正的batch/group/prepend循环在此；把pending/ordinal/计时安全点接buffer_adapter，恢复先消费pending，cut前不越界。',['BUFFER','EMBED','RECOVERY'],contract='C05',calls=['StageBufferAdapter.align','StageBufferAdapter.restore'])
add('R','demiflow/execution/stream.py','run_stream','StreamStats；成功/失败回调与关闭顺序保持','把stage_limits一路传_materialize，progress实际绑定stages；禁止入口接受参数后丢弃。',['LAYOUT','BUFFER','BOUNDS'],contract='C05',calls=['_materialize','_arun'],mode='verify_preserve')
add('M','demiflow/embeddings/stream_recovery.py','NativeEmbeddingRecovery.scope_request','RequestRecord；绑定native_occurrence和响应上限','从current_row_scope取已固定batch occurrence；无scope拒绝；同一batch跨attempt键不变，不将HTTPX当PromptClient。',['EMBED','UNKNOWN'],contract='C07',new_signature='def NativeEmbeddingRecovery.scope_request(self, request):',effects='read_only')
add('M','demiflow/embeddings/runtime.py','EmbeddingActor.__init__','EmbeddingActor完整实例；新增_native_recovery=None','构造器不发请求；原配置与资源声明不改；字段初始化和方法补丁同一装配提交。',['EMBED'],contract='C07')
add('M','demiflow/embeddings/runtime.py','EmbeddingActor.cancel_drain_timeout_s','float>=0秒；native journal有有限正drain时间','原sqlite_journal或native journal存在均启用drain；无effect路径按原行为。',['EMBED','UNKNOWN'],contract='C07')
add('M','demiflow/embeddings/runtime.py','EmbeddingActor.aclose','None（async）；原资源全部收尾','native借用journal.close不关闭run-owned journal；迟到HTTP先drain，owner/client/pool仍由actor释放。',['EMBED','BOUNDS'],contract='C07',mode='verify_preserve')
add('M','demiflow/embeddings/runtime.py','EmbeddingActor._fresh_exchange','(response_record:dict,references:dict|None)（async）','原send/failed/response语义兼容RunPromptJournal；response持久化先于向调用者返回，unknown不自动retry。',['EMBED','UNKNOWN'],contract='C07',mode='verify_preserve')
add('L','demiflow/execution/native_plan.py','stage_native_programs','tuple[ProgramBinding,...]','每个ProgramSlice独立stage_plan_artifact；局部sealed_operators与全图bindings不混淆；source身份固定；无子计划遗漏。',['LAYOUT','HISTORY','RELATIONAL'],contract='C08',new_signature='def stage_native_programs(action, program_slices):',calls=['stage_plan_artifact'],effects='durable_write')
add('L','demiflow/execution/native_plan.py','verify_native_programs','None或RecoveryContractError','按稳定input_path定位全部真实子程序；核source身份、每段verify_loaded_plan和映射完整性；缺段错段拒绝。',['LAYOUT','HISTORY','RELATIONAL'],contract='C08',new_signature='def verify_native_programs(source, plan, *, programs, action_id):',calls=['verify_loaded_plan'],effects='read_only')
add('L','demiflow/execution/chain.py','_WriteSeal.__init__','_WriteSeal；原字段与native_layout/native_execution_id/source','只绑定已验证引用；v1默认字段为None，v2必须有真实placement；不隐式load或compile。',['LAYOUT','WRITE'],contract='C08')
add('L','demiflow/execution/chain.py','_WriteSeal.verify','None或原拒绝','v1走原verify_loaded_plan；v2调用verify_native_programs，核所有子计划，不能只核根plan。',['HISTORY','WRITE'],contract='C08',calls=['verify_loaded_plan','verify_native_programs'],effects='read_only')
add('W','demiflow/workbench/runs/facts.py','_sealed_entry','logical action entry；增加binding_level=sealed；原运行状态单独显示','F1校验成功标sealed；不得由F1存在推断已执行或成功；观测/receipt仍从原run读取，不新造执行证明账本。',['WORKBENCH','HISTORY'],contract='C09',effects='read_only')
add('V','tests/test_upgrade_v2_acceptance.py','test_v2_nested_program_code_pin','None；断言通过才有证据','join/union两侧不同UDF；只修改右子plan代码必须拒绝resume；完整图stage映射不碰撞；历史读取仍可。',['LAYOUT','HISTORY','RELATIONAL'],contract='C10',new_signature='def test_v2_nested_program_code_pin(tmp_path):',effects='test_only')

for file,cls,sym in [('execution.py','WorkerChainExecutor','_request_commit'),('graph_execution.py','GraphWorkerExecutor','_request_contribution')]:
 add('L','demiflow/execution/worker/'+file,cls+'.'+sym,'CommitReceipt（async）；错误/取消抛出原异常','等待driver ACK必须有限，async路径不得future.result阻塞event loop；deadline/取消保持unknown，不自动重发或提前释放。',['RECOVERY','GRAPH','BOUNDS'],contract='C08')

add('S','demiflow/execution/operator_state.py','OperatorStates.__init__','完整OperatorStates实例；增加policies映射','保留aliases/owners/identities与恢复输入；新增状态策略缓存不得形成第二份checkpoint权威。',['STATE','RECOVERY'],contract='C04')

add('L','demiflow/execution/dataset_native.py','Engine.__init__','Engine实例；新增execution引用','实例持有已解释placement；所有子query沿用同一引用和ExitStack，不修改共享executor配置。',['RELATIONAL','LAYOUT','BOUNDS'],contract='C08')
add('L','demiflow/execution/dataset_native.py','Engine.session','DataFusionSession；property惰性创建一次','把execution传dataset_session；原envelope和资源权威不变；缺失预算绑定失败不fallback。',['RELATIONAL','LAYOUT','BOUNDS'],contract='C08',calls=['dataset_session'])
add('L','demiflow/execution/dataset_native.py','Engine.batch_rows','int>=1；真实reader/hydration行窗口','session预算、封存inflight_rows/bytes、payload_row_bytes三者取可准入上限；单行超限拒绝，不能max(1)掩盖字节超限。',['RELATIONAL','BOUNDS'],contract='C08')
add('L','demiflow/execution/native_resources.py','dataset_session','DataFusionSession；原contextmanager资源对象','原native/run/envelope限额与execution.profile/stage_limits取交集，封存边界优先；实际DataFusionOptions及slot准入均消费，不新分配host总额度。',['RELATIONAL','BOUNDS','LAYOUT'],contract='C08')

add('W','demiflow/workbench/runs/facts.py','_bridge_node_map','dict[str,str]；选定key_kind到唯一report_node_id','logical默认兼容旧调用；occurrence模式按实际出现位置映射，歧义省略，_sealed_entry同时产两种map供新旧消费者。',['WORKBENCH','HISTORY'],contract='C09',effects='pure')

# Amend final signatures separately from baseline signature/hash.
FINAL_SIGNATURES={
 'WorkerChainExecutor._request_commit':'async def _request_commit(self, next_ordinal, sealed, states, final):',
 'GraphWorkerExecutor._request_contribution':'async def _request_contribution(self, contribution, sealed, states, final):',
 '_bridge_node_map':"def _bridge_node_map(entries, *, key_kind='logical'):",
 'Engine.__init__':'def __init__(self, executor, stack, directory, stats, *, execution=None):',
 'dataset_session':'def dataset_session(executor, directory, *, prefer_hash_join=False, lance_merge=False, execution=None):',
 'seal_execution_bindings':'def seal_execution_bindings(run, action, *, logical_dag, physical_plan, profiles, programs, driver_stage_ids, source_bindings, channel_options, checkpoint_rows, supported_slice, executions=None):',
 'seal_write_topology':"def seal_write_topology(action, source, plan, *, host_id='local', decision=None):",
 '_WriteSeal.__init__':'def __init__(self, action, plan_ref, topology, *, source=None, native_layout=None, native_execution_id=None):',
}
for t in TASKS:
 sig=FINAL_SIGNATURES.get(t['symbol'])
 if t['file']=='demiflow/execution/dataset_native.py' and t['symbol']=='execute':sig='def execute(executor, source, plan, *, arrow=False, sink_schema=None, execution=None):'
 if t['file']=='demiflow/execution/arrow_batch_sink.py' and t['symbol']=='execute':sig='def execute(executor, source, plan, schema, *, execution=None):'
 if sig:
  fn=ast.parse(sig+'\n    pass').body[0];t['signature']=sig;t['input']={a.arg:PARAMS[a.arg] for a in [*fn.args.posonlyargs,*fn.args.args,*fn.args.kwonlyargs]}

# JS leaf tasks: explicitly scoped function anchors, no module-global fake class.
def add_js(file,symbol,signature,behavior,calls=()):
 name=symbol.split('.')[-1];src=(ROOT/file).read_text();offset=src.index('function mountLogical(') if symbol.startswith('mountLogical.') else 0
 m=re.search(r'(?m)^( +)(?:async )?function '+re.escape(name)+r'\(',src[offset:]);assert m,(file,symbol)
 start=offset+m.start();indent=m.group(1);rest=src[start:];nxt=re.search(r'(?m)^'+indent+r'(?:async )?function \w+\(',rest[1:]);end=start+1+nxt.start() if nxt else len(src)
 number=sum(t['owner']=='W' for t in TASKS)+1
 args=re.search(r'\((.*)\)',signature).group(1).split(',');args=[a.strip() for a in args if a.strip()]
 TASKS.append(dict(id=f'W{number:03}',owner='W',file=file,symbol=symbol,mode='implement',signature=signature,baseline_signature=signature,language='javascript',baseline_line=src[:start].count('\n')+1,baseline_function_sha256=hashlib.sha256(src[start:end].encode()).hexdigest(),input={a:PARAMS[a] for a in args},output='string' if name=='logicalName' else 'LogicalActionEntry|null' if name=='logicalEntry' else 'undefined；更新当前视图DOM/选择状态',behavior=behavior,contract='C09',effects='runtime_state',calls=list(calls),gates=['WORKBENCH','DEMO','HISTORY'],status='planned'))
for sym,sig,beh in [
 ('logicalName','logicalName()','按当前action.binding_level区分声明图和运行封存计划；不可用不伪装检查通过。'),
 ('logicalEntry','logicalEntry()','只选择同run/attempt/action的entry；切run清空旧选择，不跨action沿用映射。'),
 ('renderLogical','renderLogical(graph)','显示来源等级/独立运行状态/不可用原因；occurence展示键及已封存预算；不因dag_id认定已经成功执行。'),
 ('chooseLogicalNode','chooseLogicalNode(id, declared)','精确occurrence定位原封存源码/参数和同attempt观测；缺页/缺映射分开提示，不展示另一个节点状态。')]:
 add_js('demiflow/workbench/web/workbench.js',sym,sig,beh)
add_js('demiflow/workbench/web/workbench-graph.js','mountLogical.update','update(dag, nodeMap, labels, selectedId, basis)','按occurrence_id绘制；相同logical id多次出现保留不同节点；选择/来源状态更新一致。')
add_js('demiflow/workbench/web/workbench-graph.js','mountLogical.select','select(id)','只高亮匹配occurrence，删除/换图清空旧高亮；可访问性aria与真实选择一致。')

for t in TASKS:
 if t['language']=='python':
  sig=t['signature']
  if not sig.startswith(('def ','async def ')):
   sig=('async def '+sig[6:]) if sig.startswith('async ') else ('def '+sig)
  sig=re.sub(r'((?:async )?def )([\w.]+)(\()',lambda m:m[1]+m[2].split('.')[-1]+m[3],sig)
  if not sig.endswith(':'):sig+=':'
  t['signature']=sig


EXTRA_CALLS={
 'OperatorStates.snapshot':['VectorSearch.snapshot_state'],
 'OperatorStates.register':['VectorSearch.restore_state'],
 'StreamStageProgress.bind_stages':['EmbeddingActor.native_stream_recovery'],
 'EmbeddingActor._exchange':['NativeEmbeddingRecovery.scope_request','EmbeddingActor._fresh_exchange'],
 'NativeEmbeddingRecovery.__init__':['RunPromptJournal.check_recovery'],
 '_sealed_entry':['_bridge_node_map'],
 'overlay_sealed':['read_sealed_plan','read_identity_bridge','_sealed_entry'],
 'renderLogical':['logicalName','logicalEntry','mountLogical.update'],
 'chooseLogicalNode':['mountLogical.select'],
 'execute_native_plan':['verify_native_programs','demiflow/execution/dataset_native.py:execute','demiflow/execution/arrow_batch_sink.py:execute'],
 'Engine.batch_rows':['Engine.session'],
}
for t in TASKS:
 t['calls']=list(dict.fromkeys(t['calls']+EXTRA_CALLS.get(t['symbol'],[])))


def render():
    for old in (OUT/'tasks').glob('*.md'): old.unlink()
    source_files=sorted({t['file'] for t in TASKS if (ROOT/t['file']).exists()})
    baseline={'base_commit':'bbd93ffcc136c361093c5a4b6d79d0352c4c2557',
              'captured_at':datetime.now().astimezone().isoformat(),'files':{p:hashlib.sha256((ROOT/p).read_bytes()).hexdigest() for p in source_files}}
    (OUT/'baseline.json').write_text(json.dumps(baseline,ensure_ascii=False,indent=2)+'\n')
    # Every source class's concrete declaration and initialization state is available to method agents.
    class_state={}
    for path in source_files:
        if not path.endswith('.py'):continue
        src=(ROOT/path).read_text();tree=ast.parse(src)
        for n in tree.body:
            if isinstance(n,ast.ClassDef):
                fields={}
                for sub in ast.walk(n):
                    if isinstance(sub,ast.Assign):
                        for dest in sub.targets:
                            if isinstance(dest,ast.Attribute) and isinstance(dest.value,ast.Name) and dest.value.id=='self':
                                fields.setdefault(dest.attr,ast.unparse(sub.value))
                    if isinstance(sub,ast.AnnAssign) and isinstance(sub.target,ast.Name):fields[sub.target.id]=ast.unparse(sub.annotation)
                if n.name in {'CheckpointPolicy','WorkerPlacement','ChannelBinding','ExecutionLayout'} or any(t['file']==path and t['symbol'].startswith(n.name+'.') for t in TASKS):
                    class_state[path+':'+n.name]={'fields':fields,'init_source':next((ast.get_source_segment(src,s) for s in n.body if isinstance(s,(ast.FunctionDef,ast.AsyncFunctionDef)) and s.name=='__init__'),None)}
    (OUT/'class-state.json').write_text(json.dumps(class_state,ensure_ascii=False,indent=2)+'\n')
    by_symbol={}
    for t in TASKS:
        by_symbol.setdefault(t['symbol'],[]).append(t['id'])
        by_symbol[t['file']+':'+t['symbol']]=[t['id']]
    for t in TASKS:
        t['callee_tasks']=[x for s in t['calls'] for x in by_symbol.get(s,[])]
        t['caller_tasks']=[p['id'] for p in TASKS if any(t['id'] in by_symbol.get(call,[]) for call in p['calls'])]
        t['errors']={'C02':['RuntimeContractError','RecoveryContractError'], 'C03':['UnsupportedExecutionOptionError','ValueError','RecoveryContractError'], 'C04':['ProtocolError(STATE_ADAPTER_MISSING/INVALID_SPEC)','RecoveryContractError'], 'C05':['RecoveryContractError','原取消/超时异常；unknown不转换成功'], 'C06':['RuntimeContractError','RecoveryContractError'], 'C07':['UncertainPromptCall','原transport异常；不隐式重试'], 'C08':['PlanContractError(.reason)','RecoveryContractError'], 'C09':['原 unavailable/冲突/权限错误'], 'C10':['pytest assertion failure / 原业务异常']}[t['contract']]
        lines=[f"# {t['id']} · {t['symbol']}",f"\n文件：`{t['file']}`；文件装配负责人：**{t['owner']}**；任务状态：planned。",f"\n动作：{t['mode']}。只修改这个函数/方法；不得同时改同文件其他函数。需要 import/类字段变更时列入单独装配补丁，由文件负责人应用，不改别人工作树。",'\n## 冻结签名',f"\n```{t['language']}\n{t['signature']}\n```",f"\n基线签名：`{t['baseline_signature']}`。\n基线行：{t['baseline_line']}；基线函数 SHA-256：`{t['baseline_function_sha256']}`。新增函数无基线摘要。",'\n## 输入', '\n| 参数 | 类型、约束与所有权 |','|---|---|']
        lines += [f'| `{k}` | {v} |' for k,v in t['input'].items()]
        lines += ['\n## 输出与行为',f"\n**返回：** {t['output']}",f"\n{t['behavior']}",f"\n数据/状态/预算细则：共享契约 **{t['contract']}**；不得自行增删返回字段。",'\n## 连接',f"\n已列 caller 任务：{', '.join(t['caller_tasks']) or '由原公开入口/现有调用者调用；见总链路'}。",f"\ncallee：{', '.join(t['calls']) or '沿用本函数现有依赖，不新增跨层执行权威'}。",f"\ncallee 任务：{', '.join(t['callee_tasks']) or '既有依赖或无新增依赖'}。",'\n## 异常与副作用',f"\n错误：{'；'.join(t['errors'])}。不得吞异常继续执行、补造成功或读损坏历史后重算。",f"\n副作用类别：`{t['effects']}`。pure/read_only 不允许构造用户 Actor/读源/写 journal；runtime_state 只操作本对象；durable_write 必须通过原 RunJournal/CommitCoordinator/StateArtifacts。test_only 产物仅 /tmp/demiflow-*。",'\n## 验收与独立实现',f"\n验收门：{', '.join(t['gates'])}（详见 INTEGRATION-ACCEPTANCE.md）。",'\n对稳定签名编码；同一函数的阳性、错误、资源边界测试先独立跑，组合门由集成树跑。可使用本包固定 schema 样例；不得写生产 fallback。mock interpreter/journal/commit 得到的绿色不算集成通过。', '\n提交前逐项：检查模块来源；只含本函数窄补丁和本任务独立测试；保留原已有断言；记录准确命令/退出码/commit/基线。不要 push 或合主仓。',f"\n回执：`DELIVERY_ROOT/{t['id']}.json`。需裁决填 needs_decision；不得把消息发往已停止的旧总控后继续等待。"]
        (OUT/'tasks'/f"{t['id']}.md").write_text('\n'.join(lines)+'\n')
    data={'schema':'demiflow.implementation_plan.v2','base_commit':baseline['base_commit'],'tasks':TASKS}
    (OUT/'plan.json').write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n')
    table=['# 函数级任务总表','\n一个叶任务=一个函数/方法。数据类声明、imports/slots 的装配按 FILE-OWNERSHIP.md 处理；不能让函数 agent 自由改变公共字段。','\n`verify_preserve` 表示有实现，先验证并保留，只有发现该合同的真实缺陷才修改该函数；不能重复实现。','\n| ID | 包 | 文件/函数 | 动作 | 输出 | 验收 |','|---|---|---|---|---|---|']
    for t in TASKS:table.append(f"| [{t['id']}](tasks/{t['id']}.md) | {t['owner']} | `{t['file']}` · `{t['symbol']}` | {t['mode']} | {t['output']} | {', '.join(t['gates'])} |")
    (OUT/'FUNCTION-TASKS.md').write_text('\n'.join(table)+'\n')
    own=['# 文件装配归属','\n每文件一个装配负责人；叶任务可由不同 agent 在独立工作树同时实现。禁止直接互相 cherry-pick 未验收实现作为隐含接口。','\n| 文件 | 唯一负责人 | 叶任务 |','|---|---|---|']
    for p in sorted({t['file'] for t in TASKS}):
        ts=[t for t in TASKS if t['file']==p];owners={t['owner'] for t in ts}
        assert len(owners)==1,(p,owners)
        own.append(f"| `{p}` | {next(iter(owners))} | {', '.join(t['id'] for t in ts)} |")
    own += ['\n## 非函数代码的唯一装配责任','\n- A：capabilities.py 的 ActionDecision 声明和 imports；所有字段见 C03。','- S：operator_policy.py 的 StatePolicy 声明；OperatorStates 只增加策略选择所需私有映射，不改变已有 owners/aliases/identities 语义。','- R：stage_buffers.py 的类声明；stream._Stage.__slots__ 加 buffer_adapter，初始化见 R 叶任务；不能让别的函数 agent单独发另一版本 slots。','- F：dataset_dag.py 的 ProgramSlice 声明，精确字段见 interfaces.pyi。', '- L：plan_interpreter.py 的 ExecutionPlacement 声明及 ExecutionLayout 末尾 executions 默认值；v1/v2常量/schema键集由 L 一次装配。','- W：前端六个函数已进入任务表；事实生产者 _sealed_entry 与消费者同步装配。','- V：共享验收文件的 imports/fixture；单函数 agent只能新增自己的测试函数。','\n构造器和 slots 是一个接口事务：字段声明与 __init__ 必须在同一文件装配提交中出现，不能先合一个会 AttributeError 的半成品。']
    (OUT/'FILE-OWNERSHIP.md').write_text('\n'.join(own)+'\n')
    print('tasks',len(TASKS),'files',len({t['file'] for t in TASKS}))

if __name__=='__main__':render()
