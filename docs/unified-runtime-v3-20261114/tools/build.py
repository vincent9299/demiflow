from __future__ import annotations
import ast, hashlib, importlib.util, json, re, subprocess
from pathlib import Path
ROOT=Path(__file__).resolve().parents[3]; OUT=Path(__file__).resolve().parents[1]
BASE='267b9144c6bf7447c6822aa716f6d5c0eda16990'
modspec=importlib.util.spec_from_file_location('v3_specs',OUT/'tools/specs.py'); specs=importlib.util.module_from_spec(modspec);modspec.loader.exec_module(specs)
sha=lambda b:hashlib.sha256(b).hexdigest()
# Repository actual symbols; AST source identity, not guessed line numbers.
cached_path=OUT/'evidence/source-inventory.json'
cached=json.loads(cached_path.read_text())['files'] if cached_path.exists() else {}
inventory={}
for p in sorted((ROOT/'demiflow').rglob('*.py')):
 key=str(p.relative_to(ROOT)); digest=sha(p.read_bytes())
 if key in cached and cached[key]['sha256']==digest:
  inventory[key]=cached[key];continue
 try:t=ast.parse(p.read_text())
 except (SyntaxError,UnicodeError):continue
 items={}
 def walk(nodes,prefix=''):
  for n in nodes:
   if isinstance(n,(ast.ClassDef,ast.FunctionDef,ast.AsyncFunctionDef)):
    name=prefix+n.name
    sig=('('+ast.unparse(n.args)+')'+(' -> '+ast.unparse(n.returns) if n.returns else '')) if not isinstance(n,ast.ClassDef) else 'class '+n.name
    items[name]={'line':n.lineno,'signature':sig,'kind':'class' if isinstance(n,ast.ClassDef) else 'function','sha256':sha(ast.get_source_segment(p.read_text(),n).encode())}
    if isinstance(n,ast.ClassDef):walk(n.body,name+'.')
 walk(t.body);inventory[str(p.relative_to(ROOT))]={'sha256':sha(p.read_bytes()),'symbols':items}
# JS function boundaries are recorded by name/line with whole-file hash, not claimed as Python AST.
for js, symbols in [('workbench.js',['logicalEntry','renderLogical','chooseLogicalNode']),('workbench-graph.js',['mountLogical'])]:
 path=ROOT/'demiflow/workbench/web'/js;body=path.read_text();key=str(path.relative_to(ROOT));items={}
 for sym in symbols:
  match=re.search(r'function '+re.escape(sym)+r'\(([^\n]*)',body)
  assert match,sym
  signature='('+match.group(1).split(')')[0]+')'
  items[sym]={'line':body[:match.start()].count('\n')+1,'signature':signature,'kind':'function','sha256':sha(path.read_bytes()),'evidence_kind':'whole_file_sha256_with_symbol_location'}
 inventory[key]={'sha256':sha(path.read_bytes()),'symbols':items}
(OUT/'evidence/source-inventory.json').write_text(json.dumps({'base_commit':BASE,'files':inventory},ensure_ascii=False,indent=2)+'\n')
frozen=json.loads((ROOT/'docs/upgrade-contracts-v2-20261113/review-20261114/frozen-files.json').read_text())['files']
for f,d in frozen.items():assert sha((ROOT/f).read_bytes())==d,f
(OUT/'evidence/frozen-files.json').write_text(json.dumps(frozen,indent=2)+'\n')
original=Path('/yzp/zhaozy/yangzepeng/0905/_demiflow/platform_coordination_20261104/takeover_01a1b5db_20261107/direct-human-dialogue-evidence.json')
if original.exists():
 ds=json.loads(original.read_text()); chosen=[s for s in ds['sources'] if any(x in s['text'] for x in ['ray','Ray','单机','交接','materialize','不要复用Forge','架构上几大模块'])]
 (OUT/'evidence/intent-sources.json').write_text(json.dumps({'source':str(original),'sha256':sha(original.read_bytes()),'direct_human_messages':chosen},ensure_ascii=False,indent=2)+'\n')
rows=list(specs.GROUPS)
def existing(lane,file,symbol,contract,intents,behavior,counterexample,mode='modify',calls=()):
 key='demiflow/'+file; s=inventory[key]['symbols'][symbol]
 rows.append(dict(lane=lane,file=key,symbol=symbol,signature=s['signature'],contract=contract,intents=intents.split(','),behavior=behavior,counterexample=counterexample,mode=mode,explicit_calls=list(calls)))
# Every public executor method gets an owned migration. Private Ray lowerers retire explicitly.
terminal_methods=['from_items','from_arrow','from_numpy','from_pandas','iter_rows','iter_batches','materialize','take','take_batch','count','aggregate','schema','columns','size_bytes','num_blocks','stats','execution_metadata','write_datasink','write_lance','write_file','close','plan','operator_llm_usage','materialize_stream','materialize_graph']
for cls,name in [('LocalDatasetExecutor','local'),('RayDatasetExecutor','ray')]:
 file=f'execution/executors/{name}.py'; syms=inventory['demiflow/'+file]['symbols']
 existing('F',file,cls+'.__init__','C11','I01,I02,I14','保留构造签名，provider映射到统一策略，旧unified开关只做兼容校验，不选择旧执行引擎','同参数Local/Ray只改变provider；unified_chain=False不能静默恢复旧语义',calls=['resolve_runtime_policy'])
 for method in terminal_methods:
  sym=cls+'.'+method
  if sym not in syms:continue
  behavior=f'保留 {sym} 原签名和返回类型；'
  if method.startswith('from_'):behavior+='只返回InputMaterializedHandle中立物化句柄，保持DataAPI返回MaterializedDataset；禁止ray.data创建；预算与类型保留'
  elif method in ['stats','execution_metadata','num_blocks','operator_llm_usage']:behavior+='读取统一已封存执行/结果事实，不触发重新计算'
  elif method=='close':behavior+='关闭本executor持有的统一ActionHandle/资源scope，不关闭其他run'
  elif method=='plan':behavior+='调用同一v3编译/准入，不再生成RayData lowering或固定CPU整链布局'
  elif method=='materialize_graph':behavior+='兼容显式graph_spec转标准v3图后同入口执行；不保留独立graph driver'
  else:behavior+='只用make_action_request/run_dataset_action；移除旧compile/stream/native直接执行分支'
  existing('F',file,sym,'C11','I01,I02,I14',behavior,f'{method} 必须经过v3运行证据；禁用ray.data/旧分支时公共用例仍通过；失败不fallback',calls=['make_action_request','run_dataset_action'])
# Private lowerers cannot remain callable in new execution. Separate symbol-level retire cards.
for sym in ['RayDatasetExecutor._compile','RayDatasetExecutor._build_source','RayDatasetExecutor._lower_stage_options','RayDatasetExecutor._chain_readback']:
 existing('F','execution/executors/ray.py',sym,'C11','I02,I14','删除该旧RayData执行函数及同文件调用；历史只读不依赖它；imports由文件装配owner清理','rg/AST可达性与ban-RayData进程共同证明无新运行调用',mode='retire')
# Local's old execution authority also exits; retaining a second Local driver is not unification.
for sym in ['LocalDatasetExecutor._require_ray_backend','LocalDatasetExecutor._apply_plan','LocalDatasetExecutor._execution_capabilities','LocalDatasetExecutor._bind_plan','LocalDatasetExecutor._execute_plan','LocalDatasetExecutor._write_file_body']:
 existing('L','execution/executors/local.py',sym,'C11','I01,I02,I14','删除旧Local执行选路/整链计算/独立write循环及调用；公开方法已全部迁到统一入口；有用的纯计算由已指定kernel/reader承担','全公共API及历史反例在禁用旧Local函数时仍通过；不能把函数改名隐藏第二执行器',mode='retire')
# Dataset public boundaries previously reject cross-provider/new compositions or build live native actors.
for method in ['join','union','reduce_by_key','group_batches','exclude_keys','sort','repartition','random_shuffle','randomize_block_order','map_prompt','map_prompt_async','agentmap_async','map_embeddings','document_embeddings','search_vectors','map_cached','lookup','lookup_lance','admit_rows','deduplicate','search_web','read_object','decode_document','encode_image','parse_wikitext','fetch','register_documents','read_documents','prepend','save_lance','run_stream','enqueue','ack_queue']:
 sym='Dataset.'+method
 if sym not in inventory['demiflow/data/dataset.py']['symbols']:continue
 existing('F','data/dataset.py',sym,'C02','I01,I03,I09',f'保留 {method} 公开参数、业务语义和旧错误约定；构造纯声明节点并交统一编译。删除仅由旧执行器限制导致的backend/streaming前置拒绝；真实能力由统一准入判断','链构造不做I/O或创建client/模型；组合在Local和Ray受相同语义规则控制，不能通过放宽effect/state守卫实现',calls=['make_native_operation','capture_graph'])
for sym in ['keyed_dataset','union_dataset']:
 if sym in inventory['demiflow/execution/local_kernel.py']['symbols']:
  existing('F','execution/local_kernel.py',sym,'C02','I01,I05','保持SourcePlan构造兼容，移除执行器专属拒绝，真实关系输入交统一端口图；保留null/多重性','异步前缀join/union无需手工materialize，shared prefix不重复效果',calls=['expand_relational_inputs'])
# Runtime protocol attachment: do not change frozen K3 wire meaning, use versioned payload reference.
for sym in ['WorkerRuntime.__init__','WorkerRuntime._verify','WorkerRuntime._restore','WorkerRuntime._activate','WorkerRuntime._execute','WorkerRuntime._drain','WorkerRuntime._cancel','WorkerRuntime.close']:
 existing('B','execution/worker/runtime.py',sym,'C07','I01,I07,I11','接入明确v3 assignment与DataflowWorkerExecutor/RuntimeServices；v3不直接打开RunJournal数据库，保留生命周期校验与fence','worker只有本地cache且无driver journal目录仍可执行；旧格式读兼容不得成为新run fallback',calls=['DataflowWorkerExecutor.load','make_runtime_services'])
for sym in ['main','worker_main','run_worker']:
 if sym in inventory['demiflow/execution/worker/bootstrap.py']['symbols']:
  existing('B','execution/worker/bootstrap.py',sym,'C07','I01,I11','按封存entrypoint装配统一worker，启动先验证artifact/协议，Local不拼第二运行时','错entrypoint/codepin启动前失败；真实子进程验证',calls=['make_runtime_services','DataflowWorkerExecutor.__init__'])
# Public terminal wrappers also own semantics (take_all currently collects itself).
for method in ['materialize','take','take_all','take_batch','iter_rows','iter_batches','count','aggregate','schema','columns','require_columns','size_bytes','stats','execution_metadata','collect_summary','write_datasink','write_lance','write_parquet','write_json','write_csv']:
 sym='Dataset.'+method
 existing('U','data/dataset.py',sym,'C11','I01,I02,I08,I09','保持原返回值与边界语义，统一构造完整kind/arguments请求；take_all(limit)是超总量报错，不能当take截断；迭代器生命周期贯穿action','禁止RayData和旧直接路径仍可执行；take0零读、summary有界、write真实receipt',calls=['make_action_request','run_dataset_action'])
for method in ['release','num_blocks']:
 existing('U','data/dataset.py','MaterializedDataset.'+method,'C11','I08,I09','保持原返回值；委托中立MaterializedHandle读取/释放引用，绝不重跑已物化图','release使同一handle派生视图不可再执行；独立handle及其他run结果不删除',calls=['StoredMaterializedHandle.release','StoredMaterializedHandle.num_blocks'])
# DataAPI's constructor contracts return MaterializedDataset, not a SourcePlan handle.
for method in ['from_items','from_arrow','from_numpy','from_pandas','read_records']:
 existing('F','data/api.py','DataAPI.'+method,'C11','I01,I02,I09','保持公开返回类型；from系列使用中立InputMaterializedHandle，不创建run；read_records取消仅Local类名守卫并由统一读域准入','无RayData时构造/num_blocks/read保持；不能把SourcePlan误塞MaterializedSource.handle',calls=['build_input_handle'])
# Existing state/metadata integration must be assigned, not left implicit.
for file,symbol,lane,contract,behavior,calls in [
 ('execution/driver.py','execute_driver','F','C11','Local与Ray的正式Pipeline入口装配同一v3executor策略，无未开启默认开关的旧路由',['run_dataset_action']),
 ('execution/plan_facts.py','operation_facts','G','C02','增加v3惰性原生操作事实投影；旧类型输出及历史digest逐字保留',['compile_kernel_spec']),
 ('execution/plan_facts.py','operation_capability','G','C02','能力来自唯一operator_registry，与编译和workbench同源',['operator_registry']),
 ('execution/checked.py','CheckedExecution.execution_bindings','W','C13','检查结果绑定v3真实实例、端口与source tokens，交同一identity bridge',['build_dataflow_identity_bridge']),
 ('execution/checked.py','CheckedExecution.action','W','C13','action入口检查与runtime选路一致，不用静态图冒充运行图',['explain_dataset']),
 ('execution/sealed_plans.py','load_sealed_plan','W','C12','显式v3版本分支调用load_dataflow；v1/v2校验和原摘要保持',['load_dataflow','load_execution_version']),
 ('execution/sealed_plans.py','load_execution_bindings','W','C12','v3读取唯一布局，不把v2 driver placement误当worker',['load_dataflow']),
 ('workbench/runs/facts.py','read_sealed_plan','W','C13','读取v3原生封存事实和历史版本，不重新编译当时计划',['load_execution_version','build_plan_view']),
 ('workbench/runs/facts.py','_physical_summary','W','C13','展示stage/profile/partition/exchange/recovery的真实说明，未知观测不推测',['build_plan_view']),
 ('workbench/runs/facts.py','_sealed_entry','W','C13','统一封存图、桥、运行限制与版本，历史只读无业务调用',['build_plan_view']),
 ('workbench/runs/facts.py','join_node_observations','W','C13','以action/attempt/occurrence/plan复合键join，不按函数名合并',['build_plan_view']),
 ('execution/run_context.py','RunContext.resume','W','C12','启动业务前统一判历史兼容、恢复保证、程序与source，保留原RunRef',['validate_resume_compatibility']),
 ('workbench/runs/manager.py','resume','W','C13','同引擎恢复准入，展示不兼容具体原因，不能重写源或创建替代成功run',['validate_resume_compatibility']),
]:
 if symbol in inventory.get('demiflow/'+file,{}).get('symbols',{}):existing(lane,file,symbol,contract,'I01,I07,I12,I13',behavior,'历史旧run读取与1025枚举身份保持；v3嵌套union/模型/写出/取消/恢复真实链路',calls=calls)
# Data pipeline wrappers retained but implementation driver logic exits.
for file,symbol in [('execution/chain.py','run_local_chain'),('execution/chain.py','run_ray_chain'),('execution/graph.py','run_local_graph'),('execution/graph.py','run_ray_graph'),('execution/queue_chain.py','run_local_queue_chain')]:
 existing('F',file,symbol,'C11','I01,I02,I14','保留已用签名作显式v3请求翻译，所有新执行交execute_action；删除函数中的独立资源/调度/提交循环','原链/graph/queue测试经同一runtime通过；不能保留静默pickle失败回退',calls=['execute_action'])
# Native journal must be bound before client retry-policy validation, not after construction.
for file,sym,behavior in [
 ('operator_llm/client.py','AsyncOperatorLLMClient.__init__','增加内部keyword-only native_journal=None注入，旧参数兼容；在http_error_retry校验前装配经能力验证的远程native journal；保留有限写权限/额度守卫，不能仅hasattr即放行'),
 ('operator_llm/client.py','create_async_operator_llm_client','增加内部keyword-only native_journal=None并传入HTTP client；offline/Codex路径按原能力声明处理；不可吞注入或构造worker私有SQLite'),
 ('operator_llm/runtime.py','AsyncOperatorLLMRuntime.call_with_trace','有native_recovery时先取得其journal传工厂，再bind_client；保持完整原call参数、prompt/trace/工具/媒体语义，不绕过request_policy')]:
 existing('M',file,sym,'C09','I07,I09,I10',behavior,'真实Prompt+http_error_retry在Local/Ray都通过；未知效果不能因新注入可重试；调用预算与intent同事务',calls=['RemotePromptJournal.__init__'])
for sym in ['get','bound_source','_join_sealed_observations']:
 existing('W','workbench/graph/model.py',sym,'C13','I12,I13','读取build_plan_view的v3标准投影和真实桥；保留静态预览的诚实标签、原源码路径与参数授权；不把v3字段当v1重算','节点选择、attempt切换、历史源码、嵌套materialize及同函数不同occurrence端到端验证',calls=['build_plan_view'])
for file,sym in [('workbench.js','logicalEntry'),('workbench.js','renderLogical'),('workbench.js','chooseLogicalNode'),('workbench-graph.js','mountLogical')]:
 existing('W','workbench/web/'+file,sym,'C13','I12,I13','直接消费build_plan_view兼容投影；逻辑图及物理stage/profile/exchange/恢复限制可见；旧字段兼容但不猜v3执行事实；新绑定先清旧选择和观测','真实浏览器验证资源组详情、代码参数定位、跨attempt迟到返回、窄屏图可用；静态图不可显示执行契约已通过',calls=['build_plan_view'])
 rows[-1]['language']='javascript'
for file,sym,behavior,mode in [
 ('execution/dataset_native.py','execute','旧native关系执行入口变同一v3请求适配，arrow/sink_schema通过终端契约保留；不再实例化旧Compiler或检查Ray-only算子','modify'),
 ('execution/dataset_native.py','Engine.python_rows','旧私有计算回调不得调用已退役_apply_plan；仅兼容调用时经公开统一iter_rows；新v3 kernel不调用旧Compiler，避免嵌套action','modify'),
 ('execution/stream.py','_make_worker','删除隐式执行prefix Dataset的分支；新公共prepend由expand_operation_inputs变显式端口，旧私有stage.prefix不再可执行；其他生命周期内核按原协议保留','modify'),
 ('execution/source_snapshot.py','prefix_rows','删除旧prefix隐式计算/capture路径；新prefix与主流同一图，旧snapshot只读函数保留，不能再调用已退役_apply_plan','retire')]:
 existing('F',file,sym,'C11','I01,I05,I09,I14',behavior,'公共prepend/关系接口完整输出与恢复等价；源码无对退役Local方法的未授权活调用；旧私有执行断言映射公共行为，不能删业务反例',mode=mode,calls=['run_dataset_action'] if sym=='execute' else [])
# Additive durable pin state lives in the existing RunJournal database only.
for symbol,sig,desc in [
 ('RunJournal.bind_artifact_owner','(self, fence: str, ref: ArtifactRef, *, owner: str, operation_id: str) -> None','现有journal内新增owner pin，幂等同operation/digest，校验fence与配额'),
 ('RunJournal.release_artifact_owner','(self, fence: str, ref: ArtifactRef, *, owner: str, operation_id: str) -> None','幂等解除指定owner，不能替GC判全对象可删'),
 ('RunJournal.artifact_owners','(self, ref: ArtifactRef, *, limit: int, cursor: str | None = None) -> tuple[tuple[str, ...], str | None]','有界分页读取pin，GC同时校验committed恢复引用；不全表扫描')]:
 rows.append(dict(lane='H',file='demiflow/execution/run_journal.py',symbol=symbol,signature=sig,contract='C10',intents=['I07','I08','I11'],behavior=desc,counterexample='crash/重试不重复计费；缺页不能推导无引用；旧run schema不重写',explicit_calls=[]))
existing('H','execution/run_journal.py','RunJournal._initialize_connection','C10','I07,I13','增加artifact_owner表及索引的幂等schema迁移；旧表/记录/identity不变','schema迁移中断重启可重做；不新建平行数据库')
# Private Ray Data adapters are explicitly removed, one method/function per leaf.
rayfile='demiflow/execution/executors/ray.py'
already={(x['file'],x['symbol']) for x in rows}
for sym,meta in inventory[rayfile]['symbols'].items():
 if meta['kind']!='function' or (rayfile,sym) in already:continue
 if sym.startswith(('_RayDatasourceAdapter.','_RayLanceDatasource.','_RayDatasinkAdapter.','_RayLanceFragmentWriteSink.','_RayDirectLanceDatasink.')) or sym in {'_ray_aggregate','RayDatasetExecutor._write_lance_direct'}:
  existing('F','execution/executors/ray.py',sym,'C11','I02,I14','删除旧RayData适配方法，移除引用与必要imports；业务sink/source由统一adapter承担','生产源码依赖扫描与禁止RayData运行门均不得再触达该符号',mode='retire')
# Contract declaration is one indivisible leaf; Protocol methods have no production implementation.
type_tree=ast.parse((OUT/'interfaces.pyi').read_text())
for n in type_tree.body:
 if isinstance(n,ast.ClassDef):
  rows.append(dict(lane=('G' if n.name=='NativeOperation' else 'C'),file=('demiflow/data/dataflow_operation.py' if n.name=='NativeOperation' else 'demiflow/execution/contracts/dataflow_errors.py' if n.name=='DataflowError' else 'demiflow/execution/contracts/dataflow.py'),symbol=n.name,signature='class '+n.name,contract='C01',intents=['I01','I10'],behavior=ast.get_source_segment((OUT/'interfaces.pyi').read_text(),n),counterexample='字段/枚举/内层不可变/未知字段/空值/上界必须按CONTRACTS，不得由消费者自行扩展',mode='declare'))
# Independent integration-test functions; test-only root entrypoints, no fake production implementations.
import scenarios
for name,intents,behavior,counterexample,environments in scenarios.SCENARIOS:
 rows.append(dict(lane='V',file='tests/test_runtime_v3_acceptance.py',symbol='test_'+name,signature='(tmp_path, monkeypatch, runtime_v3_fixture) -> None',contract='C15',intents=intents.split(','),behavior=behavior,counterexample=counterexample,mode='test',environments=environments.split(','),entrypoint=True))
rows.append(dict(lane='V',file='tests/conftest.py',symbol='runtime_v3_fixture',signature='(tmp_path, request) -> Iterator[RuntimeV3Fixture]',contract='C15',intents=['I01','I11','I14'],behavior='pytest fixture读取显式provider/environment标记，创建RuntimeV3Fixture并yield后finally.close；每次进程内路径断言；缺环境记not_run不得skip算pass',counterexample='未设worktree或同机冒充two_hosts必须失败；fixture泄漏也使门失败',entrypoint=True,explicit_calls=['RuntimeV3Fixture.__init__','RuntimeV3Fixture.close']))
# Split the former broad facade lane by file ownership to avoid another serial C bottleneck.
for row in rows:
 if row['file']=='demiflow/data/dataset.py':row['lane']='U'
 elif row['file']=='demiflow/execution/executors/local.py':row['lane']='L'
 elif row['file']=='demiflow/execution/executors/ray.py':row['lane']='Y'
for row in rows:
 if row['symbol'] in {'AsyncOperatorLLMClient.__init__','create_async_operator_llm_client'}:
  row['signature']=row['signature'].replace('max_requests=None)', 'max_requests=None, *, native_journal=None)')
# Stable IDs assigned by owner, independent of future appended rows.
counts={};tasks=[]
for row in rows:
 lane=row['lane'];counts[lane]=counts.get(lane,0)+1;tid=f'{lane}{counts[lane]:03d}'
 base=inventory.get(row['file'],{}).get('symbols',{}).get(row['symbol'])
 task={**row,'id':tid,'mode':row.get('mode','modify' if base else 'add'),'baseline':base,'calls':[], 'callers':[], 'status':'specified_not_implemented', 'gate':'G-'+lane,'test_nodeid':(f"{row['file']}::{row['symbol']}" if row.get('mode')=='test' else f'tests/test_runtime_v3_{lane.lower()}.py::test_{tid.lower()}'), 'errors':['INVALID_CONTRACT','BUDGET_EXCEEDED','FENCED','CANCELLED'], 'ownership':'Only this symbol and its necessary imports/type references; no adjacent implementation edits.'}
 tasks.append(task)
by_sym={t['symbol']:t for t in tasks};by_id={t['id']:t for t in tasks}
# Explicit composition edges; registry fan-out is code registration, not independent runtime implementations.
links={
 'run_dataset_action':['execute_action','resolve_runtime_policy'],
 'make_action_request':['declare_action_v3','make_native_operation'],
 'execute_action':['make_driver_services','DataflowExecution.__init__','DataflowExecution.prepare','DataflowExecution.start','DataflowExecution.pump','DataflowExecution.finish','DataflowExecution.cancel','DataflowExecution.close','open_terminal'],
 'DataflowExecution.__init__':['DataflowScheduler.__init__','DataflowCheckpointCoordinator.__init__'],
 'DataflowExecution.prepare':['lower_action','stage_dataflow_programs','resolve_execution_constants','snapshot_source','plan_source_splits','compile_physical_dataflow','seal_dataflow','stage_runtime_bundle'],
 'lower_action':['capture_graph','terminal_registry'],
 'capture_graph':['expand_operation_inputs','compute_graph_digest','bind_occurrences','compile_kernel_spec','expand_relational_inputs','validate_graph'],
 'compile_kernel_spec':['operator_registry','build_model_factory'],
 'compile_physical_dataflow':['compute_physical_digest','partition_stages','select_fusion_groups','plan_exchanges','solve_residency','validate_physical_dataflow','infer_recovery'],
 'DataflowExecution.start':['DataflowCheckpointCoordinator.recover','DataflowScheduler.place','make_host_endpoint'],
 'DataflowExecution.pump':['DataflowScheduler.enqueue','DataflowScheduler.choose','DataflowScheduler.complete','DataflowCheckpointCoordinator.begin','DataflowCheckpointCoordinator.contribute','DriverJournalService.submit'],
 'DataflowExecution.finish':['DataflowCheckpointCoordinator.commit','DataflowScheduler.drain'],
 'DataflowExecution.cancel':['DataflowCheckpointCoordinator.abort'],
 'DataflowExecution.close':['close_channels'],
 'open_source_reader':['source_registry','verify_source_snapshot','build_input_handle'],
 'DataflowWorkerExecutor.load':['verify_dataflow_program','load_dataflow','fetch_runtime_bundle','make_runtime_services'],
 'DataflowWorkerExecutor.start':['make_kernel','open_source_reader','open_channels'],
 'DataflowWorkerExecutor.step':['invoke_unary','invoke_batch','finish_kernel','route_record','reserve_credit'],
 'DataflowWorkerExecutor.restore':['restore_channels','restore_actor_state'],
 'DataflowWorkerExecutor.request_cut':['cut_to_barrier'],
 'DataflowWorkerExecutor.snapshot':['snapshot_channels','snapshot_actor_state','prepare_sink_partition'],
 'DataflowWorkerExecutor.close':['close_channels'],
 'make_kernel':['OperatorKernel.__init__','OperatorKernel.start','OperatorKernel.push','OperatorKernel.finish','OperatorKernel.snapshot','OperatorKernel.restore','OperatorKernel.close','invoke_unary','invoke_batch','join_partitions','reduce_partition','union_inputs','emit_groups','build_sort_runs','merge_sort_runs','shuffle_key','repartition_records','accumulate_partial','merge_partials','finalize_aggregate','bind_model_services','bind_cached_map'],
 'open_channels':['ChannelSetState.__init__','RoutingCounters.__init__','to_channel_plan_v1'],
 'route_record':['RoutingCounters.next_ordinal','record_to_envelope','encode_record'],
 'publish_segment':['RunArtifactStore.put','artifact_to_data_ref'],
 'snapshot_channels':['publish_segment','seal_scope'],
 'restore_channels':['resolve_record'],
 'reclaim_segments':['RunArtifactStore.release'],
 'close_channels':['ChannelSetState.close'],
 'DataflowCheckpointCoordinator.commit':['combine_prepared_sinks','source_vector_position','ack_source_claims','reclaim_segments'],
 'DataflowCheckpointCoordinator.recover':['load_execution_version','validate_resume_compatibility','translate_native_checkpoint'],
 'prepare_sink_partition':['make_sink_adapter'],
 'open_terminal':['terminal_registry','RuntimeActionHandle.__init__','collect_rows','iterate_rows','iterate_batches','materialized_handle','write_result','inspect_result'],
 'materialized_handle':['StoredMaterializedHandle.__init__'],
 'make_driver_services':['DriverJournalService.__init__','RunArtifactStore.__init__','verify_host_domain'],
 'make_runtime_services':['WorkerJournalClient.__init__','RunArtifactStore.__init__'],
 'DriverJournalService.dispatch':['DriverJournalService.effect','DriverJournalService.reserve','DriverJournalService.artifact'],
 'bind_model_services':['make_effect_transport','restore_actor_state'],
 'make_effect_transport':['WorkerJournalClient.effect'],
 'bind_cached_map':['embedding_cache_key'],
 'fetch_runtime_bundle':['RunArtifactStore.read'],
 'resolve_data_location':['RunArtifactStore.inspect'],
 'resolve_record':['resolve_data_location'],
 'seal_dataflow':['encode_wire','build_dataflow_identity_bridge'],
 'build_plan_view':[],
 'load_dataflow':['decode_wire','load_execution_version'],
 'make_native_operation':['build_model_factory'],
 'build_input_handle':['InputMaterializedHandle.__init__','InputMaterializedHandle.rows','InputMaterializedHandle.release','InputMaterializedHandle.num_blocks'],
 'explain_dataset':['lower_action','infer_recovery'],
}
links['make_driver_services']+=['DriverArtifactPort.__init__']
links['DriverArtifactPort.__init__']=['DriverArtifactPort.reserve','DriverArtifactPort.artifact']
links['DriverArtifactPort.artifact']=['apply_artifact_command']
links['DriverJournalService.artifact']=['apply_artifact_command']
links['apply_artifact_command']=['RunJournal.bind_artifact_owner','RunJournal.release_artifact_owner','RunJournal.artifact_owners']
links['DataflowExecution.prepare']+=['DriverJournalService.bind_queue']
links['make_runtime_services']+=['fetch_runtime_bundle','verify_dataflow_program']
links['DataflowExecution.prepare']+=['NativeAcquisitionService.__init__','NativeAcquisitionService.configure']
links['DataflowExecution.start']+=['NativeAcquisitionService.start']
links['DataflowExecution.finish']+=['NativeAcquisitionService.finish']
links['DataflowExecution.close']+=['NativeAcquisitionService.close']
links['DriverJournalService.dispatch']+=['DriverJournalService.queue']
links['DataflowExecution.pump']+=['DriverJournalService.submit','DriverJournalService.poll_replies','LocalHostEndpoint.deliver_reply','RayHostEndpoint.deliver_reply']
links['DriverJournalService.submit']=['DriverJournalService.dispatch']
links['DataflowExecution.close']+=['DriverJournalService.close']
links['RayHostEndpoint.deliver_reply']=['HostedDataflowWorker.deliver_control_reply']
links['HostedDataflowWorker.deliver_control_reply']=['ControlMailbox.deliver']
links['HostedDataflowWorker.__init__']=['ControlMailbox.__init__']
links['HostedDataflowWorker.poll']=['ControlMailbox.drain']
links['HostedDataflowWorker.close']=['ControlMailbox.close']
links['RayControlTransport.request']=['ControlMailbox.request']
links['DriverJournalService.queue']=['NativeAcquisitionService.poll','NativeAcquisitionService.acquire','NativeAcquisitionService.finalize']
links['QueueReader.next_record']=['queue_service_request']
links['invoke_unary']=['queue_service_request']
links['queue_service_request']=['WorkerJournalClient.queue']
links['DriverJournalService.artifact']=['apply_artifact_command']
links['DriverJournalService.dispatch']+=['DriverJournalService.native_request']
links['bind_model_services']+=['bind_prompt_adapter','bind_embedding_adapter','bind_agent_adapter','bind_media_adapter','make_native_action_view']
links['bind_prompt_adapter']=['RemotePromptRecovery.__init__']
links['bind_embedding_adapter']=['RemoteEmbeddingRecovery.__init__','RemoteEmbeddingRecovery.verify_request_membership']
links['RemotePromptRecovery.__init__']=['RemotePromptJournal.__init__']
links['RemoteEmbeddingRecovery.__init__']=['RemotePromptJournal.__init__']
for t in tasks:
 if t['symbol'].startswith('RemotePromptJournal.') and not t['symbol'].endswith('__init__'):
  links.setdefault('RemotePromptJournal.__init__',[]).append(t['symbol'])
  links[t['symbol']]=['WorkerJournalClient.native_request']
links['runtime_v3_fixture']=['RuntimeV3Fixture.__init__','RuntimeV3Fixture.close']
for t in tasks:
 if t['mode']=='test':
  links[t['symbol']]=['runtime_v3_fixture','RuntimeV3Fixture.run','RuntimeV3Fixture.crash','RuntimeV3Fixture.assert_semantics']
links['encode_wire']=['validate_dataflow_record']
links['validate_dataflow_record']=['DataflowError.__init__']
links['decode_wire']=['validate_dataflow_record']
links['make_host_endpoint']=['HostedDataflowWorker.__init__','HostedDataflowWorker.activate','HostedDataflowWorker.poll','HostedDataflowWorker.request_cut','HostedDataflowWorker.cancel','HostedDataflowWorker.closed','make_worker_control_assignment','LocalControlTransport.__init__','LocalControlTransport.request','RayControlTransport.__init__','RayControlTransport.request']
links['make_host_endpoint']+=['HostedDataflowWorker.identity','HostedDataflowWorker.handle','HostedDataflowWorker.pull_events','HostedDataflowWorker.deliver_receipt','HostedDataflowWorker.close','DataflowRayConnector.host_class','DataflowLocalConnector.spawn_payload']
links['RunArtifactStore.retain']=['WorkerJournalClient.artifact']
links['RunArtifactStore.release']=['WorkerJournalClient.artifact']
# Class protocol dispatch consumers; each implementing method is explicitly owned and reachable.
for t in tasks:
 sym=t['symbol']
 if t['lane']=='S' and '.' in sym and sym.split('.')[0].endswith('Reader'):
  links.setdefault('open_source_reader',[]).append(sym)
 if sym.startswith(('LocalHostEndpoint.','RayHostEndpoint.')):links.setdefault('make_host_endpoint',[]).append(sym)
 if sym.startswith('WorkerJournalClient.') and not sym.endswith('__init__'):links.setdefault('make_runtime_services',[]).append(sym)
 if sym.startswith(('RuntimeActionHandle.','StoredMaterializedHandle.')):links.setdefault('open_terminal',[]).append(sym)
 if sym.startswith('DataflowWorkerExecutor.'):links.setdefault('make_host_endpoint',[]).append(sym)
for a,bs in links.items():
 assert a in by_sym, 'missing caller '+a
 assert all(b in by_sym for b in bs), 'missing callee '+a+': '+repr([b for b in bs if b not in by_sym])
 if a in by_sym:
  for b in bs:
   if b in by_sym:by_sym[a]['calls'].append(by_sym[b]['id'])
for t in tasks:
 for callee in t.pop('explicit_calls',[]):
  assert callee in by_sym, 'missing explicit callee '+t['symbol']+': '+callee
  if callee in by_sym:t['calls'].append(by_sym[callee]['id'])
# Specific internal seams with previously missing consumers.
for a,b in [('StoredMaterializedHandle.__init__','RunArtifactStore.retain'),('open_source_reader','spool_record'),('DataflowExecution.prepare','normalize_native_options'),('seal_dataflow','RunArtifactStore.put')]:
 by_sym[a]['calls'].append(by_sym[b]['id'])
for t in tasks:
 t['calls']=sorted(set(t['calls']))
 for c in t['calls']:by_id[c]['callers'].append(t['id'])
# Declaration consumption is type-level, tracked separately from runtime call edges.
for t in tasks:
 t['type_consumers']=[]
 if t['mode']=='declare':
  t['type_consumers']=[x['id'] for x in tasks if x['id']!=t['id'] and re.search(r'\b'+re.escape(t['symbol'])+r'\b',x['signature']+' '+x['behavior'])]
 if t['symbol']=='NativeOperation': t['type_consumers']=[by_sym['make_native_operation']['id'],by_sym['compile_kernel_spec']['id']]
 if t['symbol']=='DataflowError': t['type_consumers']=[by_sym['DataflowError.__init__']['id'],by_sym['validate_dataflow_record']['id']]
 if t['symbol']=='FiniteScope': t['type_consumers']=[by_sym['plan_exchanges']['id'],by_sym['DataflowCheckpointCoordinator.begin']['id']]
 t['public_boundary']=t['mode']=='modify' and t['symbol'] not in {'RayDatasetExecutor._compile','RayDatasetExecutor._build_source'}
# API inventory includes preserved entry points, mapped to owning execution gate.
apis=[]
for f,cls in [('demiflow/data/dataset.py','Dataset'),('demiflow/data/read_api.py',None)]:
 for sym,s in inventory[f]['symbols'].items():
  if s['kind']!='function':continue
  if cls and not sym.startswith((cls+'.','MaterializedDataset.')):continue
  if not cls and '.' in sym:continue
  name=sym.split('.')[-1]
  if name.startswith('_'):continue
  direct=next((t['id'] for t in tasks if t['file']==f and t['symbol']==sym),None)
  retired=name in {'checkpoint','checkpoint_async','checkpoint_lance','checkpoint_lance_async'}
  apis.append({'file':f,'symbol':sym,'signature':s['signature'],'line':s['line'],'disposition':'retired_preserved_rejection' if retired else 'internal_diagnostic' if name=='execution_metadata' else 'migrate' if direct else 'preserve_surface_migrate_execution','task_id':direct,'execution_owner':'F','gate':'G-API','acceptance':'行为与返回类型按原签名；进入统一引擎；恢复保证真实披露'})
plan={'schema':'demiflow.runtime-design.v3','base_commit':BASE,'dispatch_ready':json.loads((OUT/'release.json').read_text())['dispatch_ready'] if (OUT/'release.json').exists() else False,'implementation_complete':False,'tasks':tasks,'api':apis}
(OUT/'plan.json').write_text(json.dumps(plan,ensure_ascii=False,indent=2)+'\n')
# Render individual leaf contracts; no card claims tests have run.
for t in tasks:
 doc=f'''# {t['id']} · {t['symbol']}

文件：`{t['file']}`；唯一文件装配 lane：**{t['lane']}**；动作：`{t['mode']}`。
基线：`{BASE}`；原意图：{', '.join(t['intents'])}；合同：[CONTRACTS.md {t['contract']}](../CONTRACTS.md)。

## 精确接口

```{t.get('language','python')}
{t['symbol']}{t['signature'] if not t['signature'].startswith('class ') else '  # declaration in interfaces.pyi'}
```

共享字段、枚举、类型和嵌套冻结见 [interfaces.pyi](../interfaces.pyi)。现有签名保持兼容；不得自行增添隐式默认值或字段。

## 输入、输出与必须实现的语义

{t['behavior']}

## 失败、资源与恢复

必须遵守 {t['contract']} 的完整状态机、数据所有权和 side-effect 顺序；通用错误为 {', '.join(t['errors'])}，领域 UNKNOWN_EFFECT/SOURCE_CHANGED 等按合同使用。不在此函数外另建持久状态，不将运行期对象写入恢复镜像。输出使用签名类型，函数失败不返回部分成功。

## 必需反例与验收

{t['counterexample']}

单元验收 nodeid：`{t['test_nodeid']}`（待实现）；所属门：`{t['gate']}`。测试须调用本真实符号；仅外部服务/对端测试域可使用冻结契约 fake。最终必须参与 [ACCEPTANCE.md](../ACCEPTANCE.md) 中真实组合场景，不能以本叶单测代替 root 通过。

## 接线与独立编码

调用任务：{', '.join(t['calls']) or '无新增任务函数调用；现有库按上述契约复用'}。
消费任务：{', '.join(t['callers']) or ('公共入口/既有调用者（源码基线保留，按任务说明接线）' if t['public_boundary'] else '见 registry/类型消费者；必须在设计校验中闭合')}。
类型消费者：{', '.join(t['type_consumers']) or '不适用'}。
依赖的实现可尚未落地，但签名、字段、错误、顺序已冻结；消费者测试允许测试域 spec fake，生产代码不加 ImportError fallback。集成依赖不等于编码等待。

## 修改范围与交付

只改该符号及其必要 imports/type references；同文件其他符号由对应任务负责，文件owner统一装配。不得改七份字节冻结文件、主仓、旧历史或在线服务。附 commit、精确diff、完整测试命令、进程内模块路径、源码摘要、pass/fail/not_run和未关闭反例。不得为转绿删守卫/改业务样本/缩预算覆盖。

原符号证据：`{json.dumps(t['baseline'],ensure_ascii=False)}`。
状态：指定待实现，未运行验收。
'''
 (OUT/'tasks'/f"{t['id']}.md").write_text(doc)
# Remove only obsolete generated cards from this package; never other documents.
for card in (OUT/'tasks').glob('*.md'):
 if card.stem not in by_id: card.unlink()
(OUT/'TASKS.md').write_text('# 函数任务总表\n\n每行一个叶任务；不以数量代表完成。完整接口/数据/错误/反例见任务卡。\n\n| ID | lane | 文件 | 符号 | 动作 | 调用者 |\n|---|---|---|---|---|---|\n'+''.join(f"| [{t['id']}](tasks/{t['id']}.md) | {t['lane']} | `{t['file']}` | `{t['symbol']}` | {t['mode']} | {', '.join(t['callers']) or 'API/type/legacy'} |\n" for t in tasks))
(OUT/'API-MATRIX.md').write_text('# 全部现有公开 API 的迁移归属\n\n按基线 AST 枚举，不能用新增能力清单漏掉旧入口。data 是来源 facade，Dataset 是计算对象，签名保持。四个checkpoint入口继续明确拒绝；不恢复已退役的协议。下面“保留表面”也必须通过G-API，不是保留旧执行路线。\n\n| API | 处置 | 直接叶任务 | 执行装配/验收 |\n|---|---|---|---|\n'+''.join(f"| `{a['symbol']}` | {a['disposition']} | {a['task_id'] or 'F 公共装配；原函数保留'} | {a['gate']} |\n" for a in apis)+'\n## 参数与返回契约\n\n完整原签名及行号在 plan.json.api 中。原 map/async/batch/关系/模型配置不重写；新增执行支持不能通过静默忽略参数实现。Ray native_options 由 normalize_native_options 唯一转换，逐参数测试验证num_cpus、num_gpus、memory、concurrency、partition等；ActorPoolStrategy等Ray Data执行对象须明确迁移诊断，不能继续实例化RayData。\n')
# Design signatures grouped per target file; existing unresolved names are imported from their original module.
parts=['# Design-only functions; shared record declarations in interfaces.pyi.\n']
for t in tasks:
 if t['mode']!='declare' and t['mode']!='retire' and t.get('language')!='javascript':
  name=t['symbol'].replace('.','__');sig=t['signature'];parts.append(f"# {t['id']} {t['file']}:{t['symbol']}\ndef {name}{sig}: ...\n")
(OUT/'function-signatures.pyi').write_text('\n'.join(parts))
print(json.dumps({'tasks':len(tasks),'files':len({t['file'] for t in tasks}),'api':len(apis),'lanes':counts},ensure_ascii=False))
