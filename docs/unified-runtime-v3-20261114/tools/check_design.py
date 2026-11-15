"""Design structural audit only. Does not claim implementation/runtime acceptance."""
from __future__ import annotations
import ast, hashlib, json, re, sys
from pathlib import Path
O=Path(__file__).resolve().parents[1]; R=O.parents[1]
p=json.loads((O/'plan.json').read_text()); ts=p['tasks']; ids={t['id'] for t in ts}; errors=[]
from design_rules import manifest_errors
errors.extend(manifest_errors(ts))
check=lambda ok,msg: errors.append(msg) if not ok else None
check(len(ids)==len(ts),'duplicate task id')
check({x.stem for x in (O/'tasks').glob('*.md')}==ids,'obsolete or missing generated cards')
seen=set(); files={}
for t in ts:
 k=(t['file'],t['symbol']);check(k not in seen,'duplicate symbol '+str(k));seen.add(k)
 files.setdefault(t['file'],set()).add(t['lane'])
 check(bool(t['behavior'].strip()) and bool(t['counterexample'].strip()),'empty behavior/counterexample '+t['id'])
 check(re.fullmatch(r'C\d{2}',t['contract']) is not None,'bad contract '+t['id'])
 check(all(c in ids for c in t['calls']+t['callers']),'dangling edge '+t['id'])
 check((O/'tasks'/f"{t['id']}.md").exists(),'missing card '+t['id'])
 if t['mode']=='add':check(bool(t['callers']) or t.get('entrypoint',False),'new symbol without consumer '+t['id']+' '+t['symbol'])
 if t['mode']=='declare':check(bool(t['type_consumers']) or t.get('wire_only',False),'type without consumer '+t['id']+' '+t['symbol'])
 if t['mode'] in {'modify','retire'}:check(t['baseline'] is not None,'missing baseline '+t['id'])
for f,owners in files.items():check(len(owners)==1,'multiple owners '+f+':'+str(owners))
for t in ts:
 for c in t['callers']:
  caller=next(x for x in ts if x['id']==c)
  check(caller['mode'] not in {'verify_preserve','retire'},'caller cannot wire '+t['id']+' from '+c)
for f in ['interfaces.pyi','function-signatures.pyi']:
 try:ast.parse((O/f).read_text())
 except SyntaxError as e:errors.append(f'syntax {f}:{e.lineno} {e.msg}')
frozen=json.loads((O/'evidence/frozen-files.json').read_text())
for f,h in frozen.items():
 check(hashlib.sha256((R/f).read_bytes()).hexdigest()==h,'frozen bytes changed '+f)
 check(f not in files,'task modifies frozen '+f)
check(any(t['file']=='demiflow/execution/executors/ray.py' and t['mode']=='modify' for t in ts),'Ray executor migration absent')
check(any(t['file']=='demiflow/execution/executors/ray.py' and t['symbol'].endswith('_compile') and t['mode']=='retire' for t in ts),'RayData exit absent')
for i in range(1,15):check(any(f'I{i:02d}' in t['intents'] for t in ts),'unmapped intent I%02d'%i)
source=json.loads((O/'evidence/source-inventory.json').read_text())['files']
actual_api=[]
for file,cls in [('demiflow/data/dataset.py','Dataset.'),('demiflow/data/read_api.py','')]:
 for sym,v in source[file]['symbols'].items():
  if v['kind']=='function' and sym.startswith((cls, 'MaterializedDataset.')) and not sym.split('.')[-1].startswith('_') and (cls or '.' not in sym):actual_api.append((file,sym))
check(set(actual_api)=={(a['file'],a['symbol']) for a in p['api']},'public API dropped')
# exact rendered coverage prevents stale card after manifest edit
for t in ts:
 card=(O/'tasks'/f"{t['id']}.md").read_text()
 check(t['behavior'] in card and t['counterexample'] in card,'stale card '+t['id'])
# Gate coverage is exact, including tests for declarations/retire leaves and integration functions.
gates=json.loads((O/'gates.json').read_text())['gates']
required={n for g in gates if isinstance(g.get('required_nodeids'),list) for n in g['required_nodeids']}
check({t['test_nodeid'] for t in ts}.issubset(required),'task acceptance nodeid without required gate')
check(len({g['id'] for g in gates})==len(gates),'duplicate gate')
hist=json.loads((O/'evidence/regression-files.json').read_text())['files']
actual_tests={str(f.relative_to(R)) for f in (R/'tests').rglob('*.py') if f.name.startswith('test_') or f.name.endswith('_test.py')}
check({f['file'] for f in hist}==actual_tests,'historical test file omitted')
check({f['file'] for f in hist}==set(next(g for g in gates if g['id']=='HISTORICAL_ALL')['files']),'historical gate misses files')
for row in hist:check(hashlib.sha256((R/row['file']).read_bytes()).hexdigest()==row['sha256'],'historical source drift '+row['file'])
coverage=json.loads((O/'intent-coverage.json').read_text())
for key,row in coverage.items():
 check(bool(row['integration']),'intent without integration scenario '+key)
 check(set(row['tasks'])=={t['id'] for t in ts if key in t['intents']},'intent coverage drift '+key)
# New annotations must refer to declared/shared types or owned concrete classes.
tree=ast.parse((O/'interfaces.pyi').read_text());defined={'None','str','int','float','bool','bytes','tuple','dict','list','object','type','BaseException'}
for n in tree.body:
 if isinstance(n,ast.ClassDef):defined.add(n.name)
 if isinstance(n,ast.ImportFrom):defined.update(a.asname or a.name for a in n.names)
 if isinstance(n,ast.Assign):defined.update(a.id for a in n.targets if isinstance(a,ast.Name))
defined.update(t['symbol'].split('.')[0] for t in ts if '.' in t['symbol'])
for t in ts:
 if t['mode']!='add' or t.get('language')=='javascript':continue
 f=ast.parse('def f'+t['signature']+': pass').body[0]
 for an in [a.annotation for a in f.args.args+f.args.kwonlyargs]+[f.returns]:
  if an:
   unknown={n.id for n in ast.walk(an) if isinstance(n,ast.Name)}-defined
   check(not unknown,'undefined type '+t['id']+': '+repr(sorted(unknown)))
# Every explicitly owned existing source byte remains the recorded baseline during design.
for file,meta in source.items():
 check(hashlib.sha256((R/file).read_bytes()).hexdigest()==meta['sha256'],'baseline code changed '+file)
# Human-readable artifacts referenced by cards must exist.
for name in ['LANES.md','ACCEPTANCE.md','MIGRATION.md','USER-OUTCOME.md','IDENTITY.md','OPERATOR-MATRIX.md']:
 check((O/name).exists(),'missing deliverable '+name)
# Fixtures only check closed-field structure and stated simple invariants here; compiler semantics remain runtime gates.
classes={n.name:{x.target.id for x in n.body if isinstance(x,ast.AnnAssign) and isinstance(x.target,ast.Name)} for n in tree.body if isinstance(n,ast.ClassDef)}
for item in json.loads((O/'fixtures/index.json').read_text()):
 doc=json.loads((O/'fixtures'/item['file']).read_text());v=doc['payload'];kind=doc['type']
 check(set(v)==classes[kind],'fixture closed fields '+item['file'])
 valid=True
 if kind in {'Record','WorkerEvent'}:valid=(v.get('inline',v.get('payload')) is None)!=(v['payload_ref'] is None)
 if kind=='WorkerEvent':valid=valid and v['kind'] in {'ready','progress','cut','effect','artifact','reserve','read_action','read_configuration','check_grant','queue','native_request','failure','eoi','closed'}
 if kind=='SourceCursor':valid=v['next_sequence']>=0
 if kind=='SourcePoll':valid=(v['status']=='record' and v['record'] is not None and v['retry_after_ms']==0) or (v['status']=='pending' and v['record'] is None and v['retry_after_ms']>0) or (v['status']=='eoi' and v['record'] is None and v['retry_after_ms']==0)
 check(valid==item['expected_valid'],'fixture expected result '+item['file'])
report={'kind':'design_structure_only','base_commit':p['base_commit'],'tasks_checked':len(ts),'files_checked':len(files),'api_checked':len(actual_api),'errors':errors,'passed':not errors,'runtime_tests_run':False,'gates_checked':len(gates),'historical_test_files_checked':len(hist),'fixture_examples_checked':len(json.loads((O/'fixtures/index.json').read_text()))}
(O/'evidence/design-check.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
print(json.dumps(report,ensure_ascii=False,indent=2));sys.exit(bool(errors))
