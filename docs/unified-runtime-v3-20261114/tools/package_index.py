"""Generate handoff indexes from the frozen design; never count these as runtime tests."""
from pathlib import Path
import ast, hashlib, json, collections
O=Path(__file__).resolve().parents[1]; R=O.parents[1]
p=json.loads((O/'plan.json').read_text()); tasks=p['tasks']; lanes=collections.defaultdict(list)
for t in tasks:lanes[t['lane']].append(t)
labels={'C':'共享类型、codec、身份、错误','G':'真实图捕获与算子注册','P':'物理计划与资源可行性','S':'增量来源与游标','K':'算子与关系/排序/聚合内核','X':'通道、路由、credit、GC','B':'同一worker运行时','E':'驱动编排和公平调度','D':'程序封存、cut和唯一提交','T':'终端、迭代器和物化句柄','M':'模型、状态、缓存和原生journal适配','H':'多机宿主、控制通道、工件和driver权限','Q':'原生获取/队列结算服务','U':'Dataset公开入口迁移','L':'Local执行器变薄','Y':'Ray Core入口及Ray Data退出','F':'Pipeline/data与旧调用兼容接线','W':'工作台真实图与历史','V':'集成验收与测试域基础'}
order=['C','G','P','S','K','X','B','E','D','T','M','H','Q','U','L','Y','F','W','V']
lines=['# 并行编码与文件所有权','',f'推荐 **{len(lanes)} 个实现会话，加 Qwen 总控和最终 GPT/Qwen Review**。这是 {len(tasks)} 个函数/方法/类型叶任务的文件装配分组，不是开 {len(tasks)} 个会话。每个叶可以独立编码；同文件只有一个装配 owner。','', '所有 lane 同时从设计最终提交建隔离分支，按冻结接口写代码，不能等待供应方内部实现后才设计消费者。单元测试的 spec fake 仅在测试域；完整集成才要求所有实现到位。', '', '| lane | 职责 | 叶数 | 文件数 | 完成交付 |','|---|---|---:|---:|---|']
for lane in order:
 ts=lanes[lane];lines.append(f'| {lane} | {labels[lane]} | {len(ts)} | {len(set(t["file"] for t in ts))} | 全部指定符号、反例、完整回执 |')
lines+=['','## 每个文件一个装配任务','', '下面文件任务包含其全部函数叶卡。同一文件的 imports/类声明骨架/注册表由该文件 owner 装配，不分给第二lane；不能用新增未登记helper绕过权限。测试文件由对应lane单独持有，V只拥有集成测试和root fixture。','', '| 文件 | owner | 叶任务 |','|---|---|---|']
for f in sorted(set(t['file'] for t in tasks)):
 ts=[t for t in tasks if t['file']==f];lines.append(f'| `{f}` | {ts[0]["lane"]} | '+', '.join(f'[{t["id"]}](tasks/{t["id"]}.md)' for t in ts)+' |')
lines+=['','## 编码、集成与回报','', '编码不设阶段Review、不逐函数打断。缺口只在具体反例证明冻结接口不成立时上报，附producer/consumer/数据流和最小修订；Qwen集中裁决，不让双方私自扩字段。不相关lane继续。', '', '集成顺序 C→G/P/S/K/X/M/H/Q→B/D/E/T→U/L/Y/F/W→V 是合并/启动顺序，不是开工顺序。每个lane交付完整commit与回执后，Qwen在新的integration树合并，GPT与Qwen各自按源码终审，GPT统一修复并完整验收。禁止合主仓。', '', '每lane新分支建议 `engine/runtime-v3-<lane>-20261114`，工作树位于 `_demiflow/worktrees/runtime_v3_<lane>_20261114`。基于设计最终commit，不从主仓未提交状态或其他lane HEAD起步。', '', '回报只交当前活的Qwen总控；回执中保存该总控会话ID与回报接收状态。不得复用已停止的57bfe8f5。会话状态与磁盘commit分别核验。', '', '推荐资源受限时保留19个逻辑owner、降低同时运行测试的数量；不要把所有执行接入又塞回单一C会话。函数编写可并行，重Ray/GPU/两host测试按独立测试资源锁排队。']
(O/'LANES.md').write_text('\n'.join(lines)+'\n')
# Full baseline test inventory, no name filter that drops *_test.py.
files=[]
for f in sorted((R/'tests').rglob('*.py')):
 if f.name.startswith('test_') or f.name.endswith('_test.py'):
  files.append({'file':str(f.relative_to(R)),'sha256':hashlib.sha256(f.read_bytes()).hexdigest(),'gate':'HISTORICAL_ALL','owner':'V','collection':'all collected nodeids, no implicit deselect','environment':'classify each collected nodeid at execution; missing env is not_run'})
(O/'evidence/regression-files.json').write_text(json.dumps({'base_commit':p['base_commit'],'files':files,'count':len(files),'runtime_collected':False},ensure_ascii=False,indent=2)+'\n')
known=json.loads((R/'docs/upgrade-contracts-v2-20261113/review-integration-20261114/qwen-repair-evidence/known-failures.json').read_text())
for i,node in enumerate(known['nodeids']):
 # Classification is historical evidence, never a waiver or current test result.
 pass
(O/'evidence/historical-dispositions.json').write_text(json.dumps({'source':'review-integration-20261114/qwen-repair-evidence/known-failures.json','cases':[{'nodeid':n,'historical_class':'preexisting_real_defect' if i>=37 else 'old_contract_expectation','required':'retain original and fix; no xfail/delete' if i>=37 else 'explicit old/new assertion map with rationale; no blanket deselect','owner':'M/T/V' if i>=37 else 'U/L/Y/V'} for i,n in enumerate(known['nodeids'])]},ensure_ascii=False,indent=2)+'\n')
gates=[]
for lane in order:
 ts=lanes[lane];gates.append({'id':'G-'+lane,'kind':'unit_contract','required_nodeids':sorted(set(t['test_nodeid'] for t in ts if t['mode']!='test')),'task_ids':[t['id'] for t in ts],'environments':['local'],'status':'not_run'})
for t in tasks:
 if t['mode']=='test':gates.append({'id':'V-'+t['symbol'][5:],'kind':'integration','required_nodeids':[t['test_nodeid']],'task_ids':[t['id']],'environments':t['environments'],'status':'not_run'})
gates.append({'id':'HISTORICAL_ALL','kind':'regression','files':[f['file'] for f in files],'required_nodeids':'all collected per exact snapshot','environments':['partition by concrete fixture requirements; every nodeid has exactly one primary environment'],'status':'not_run'})
(O/'gates.json').write_text(json.dumps({'schema':'demiflow.design_gates.v3','all_results':'not_run','rules':{'skip_is_pass':False,'xfail_is_pass':False,'retry_hides_failure':False,'uncollected_required_is_pass':False,'duplicate_nodeid_is_pass':False,'source_digest_required':True,'worktree_assertion_required':True},'gates':gates},ensure_ascii=False,indent=2)+'\n')
intent_map={f'I{i:02d}':{'tasks':[t['id'] for t in tasks if f'I{i:02d}' in t['intents']],'integration':[g['id'] for g in gates if g['kind']=='integration' and any(t['id'] in g['task_ids'] and f'I{i:02d}' in t['intents'] for t in tasks)]} for i in range(1,15)}
(O/'intent-coverage.json').write_text(json.dumps(intent_map,ensure_ascii=False,indent=2)+'\n')
# Source-declared operator fields, inherited fields resolved, no guessed names.
mods=['demiflow/data/plan.py','demiflow/data/sources.py','demiflow/execution/local_kernel.py']; classes={}
for f in mods:
 tree=ast.parse((R/f).read_text())
 for n in tree.body:
  if isinstance(n,ast.ClassDef):classes[n.name]=(f,n)
def fields(name):
 f,n=classes[name];result=[]
 for b in n.bases:
  if isinstance(b,ast.Name) and b.id in classes:result+=fields(b.id)
 for x in n.body:
  if isinstance(x,ast.AnnAssign) and isinstance(x.target,ast.Name):result.append(x.target.id+': '+ast.unparse(x.annotation))
 return result
ops=[]
for name,(f,n) in classes.items():
 if name.endswith('Op') and name!='LogicalOp':
  if name in {'SortOp','RandomShuffleOp','RandomizeBlockOrderOp'}:route='ordering / finite_complete；固定seed与全局范围；纯输入可重建'
  elif name=='RepartitionOp':route='repartition_records / incremental hash或rebalance；路由计数入cut'
  elif name in {'BatchMapOp','MapBatchesOp','EmbeddingMapOp','StreamGroupBatchesOp','SaveLanceOp','RegisterDocumentsBatchOp'}:route='invoke_batch或原生adapter / bounded batch；未满buffer/timer/输出cursor入cut'
  elif name in {'AsyncMapOp','AgentMapOp','OperatorLLMMapOp','VectorSearchOp','SearchWebOp','RegisterDocumentsOp','ReadDocumentsOp','StreamPrependOp','DeduplicateOp'}:route='invoke_unary或M原生adapter / incremental；副作用按原协议；不能推断pure'
  else:route='invoke_unary / incremental；保持原顺序、字段与调用语义；未知state拒绝伪恢复'
  ops.append({'kind':name,'file':f,'line':n.lineno,'fields':fields(name),'mapping':route})
(O/'operator-matrix.json').write_text(json.dumps(ops,ensure_ascii=False,indent=2)+'\n')
doc=['# 算子/来源/终端映射与参数冻结','','不是重新设计业务参数。下表字段来自基线AST（含继承），编译器必须逐字段保留；CallableSpec/类/函数/聚合放代码工件，资源参数放profile，其余逻辑配置按C02进入身份，不能直接json.dumps全部操作。','','| LogicalOp | 真实字段 | 执行与恢复 |','|---|---|---|']
for x in ops:doc.append('| `'+x['kind']+'` | '+'; '.join('`'+v.replace('|','&#124;')+'`' for v in x['fields'])+' | '+x['mapping']+' |')
doc+=['','## 多输入与来源','','KeyedSource持有PlanInput的source+plan，join/exclude_keys用left/right；reduce_by_key/group_batches用单端口但complete-input requirement不同；UnionSource.inputs按构造次序编号，不按schema合并。完整字段见下表。','','| Source/结构 | 字段 | 归属 |','|---|---|---|']
for name in ['ItemsSource','RangeSource','FileSource','SqlSource','LanceSource','MaterializedSource','IterableSource','PlanInput','KeyedSource','UnionSource']:
 if name in classes:doc.append('| `'+name+'` | '+'; '.join('`'+v.replace('|','&#124;')+'`' for v in fields(name))+' | S；多输入结构G展开，K计算 |')
doc+=['','File格式支持Parquet/CSV/JSON/JSONL/text/binary/images/records，Reader按format与原options选择；SQL固定snapshot/keyset或明确spool；Lance query保留投影/filter/search/limit/版本，global top_k由全局终端合并；自定义Datasource计划的ReadTask用TaskReader；QueueDatasource是C16服务专用，历史queue/doc读取用HistoryReader。', '', '## 终端与返回值','', '所有原签名在API-MATRIX/plan.api。take/take_all→list；take_batch→原block格式；iter_rows/iter_batches→持有action的迭代器；count→int；aggregate→原聚合输出；schema/columns→原schema/列；materialize→MaterializedDataset；write_datasink/write_lance保留原WriteResult/表语义；write_parquet/json/csv保留None返回与既有格式，不承诺后端目录布局字节相同。run_stream保留原显式恢复约束，checkpoint旧入口继续拒绝。metadata只读不能触发UDF。', '', 'native_options业务资源映射必须在P.normalize_native_options精确列出：num_cpus→cpu_millis（有限非负且粒度校验），num_gpus→GPU资源声明，memory→resident request，concurrency→replica上下限；业务batch_size仍归kernel；block/partition参数按原字段名翻译。Ray Data ActorPoolStrategy/内部TaskPool对象给明确迁移错误，不实例化Ray Data。未知参数不能静默忽略。']
(O/'OPERATOR-MATRIX.md').write_text('\n'.join(doc)+'\n')
print(json.dumps({'lanes':len(lanes),'test_files':len(files),'gates':len(gates),'operator_kinds':len(ops)}))
