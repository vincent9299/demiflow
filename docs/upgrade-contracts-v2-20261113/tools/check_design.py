"""Validate design structure and baseline compatibility, not future runtime behavior."""
import ast, hashlib, json, re, subprocess, sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[3];OUT=ROOT/'docs/upgrade-contracts-v2-20261113'
sys.path.insert(0,str(ROOT))
BASE='bbd93ffcc136c361093c5a4b6d79d0352c4c2557'
checks=[]
def check(name,fn):
    detail=fn();checks.append({'check':name,'status':'passed','detail':detail})
plan=json.loads((OUT/'plan.json').read_text());tasks=plan['tasks'];baseline=json.loads((OUT/'baseline.json').read_text())
def baseline_check():
    assert plan['base_commit']==baseline['base_commit']==BASE
    for p,digest in baseline['files'].items():
        current=(ROOT/p).read_bytes(); original=subprocess.check_output(['git','show',BASE+':'+p],cwd=ROOT)
        assert hashlib.sha256(current).hexdigest()==digest and current==original,p
    return {'unchanged_baseline_source_files':len(baseline['files'])}
check('source_baseline',baseline_check)
def ownership():
    assert len({t['id'] for t in tasks})==len(tasks)
    assert len({(t['file'],t['symbol']) for t in tasks})==len(tasks)
    for f in {t['file'] for t in tasks}: assert len({t['owner'] for t in tasks if t['file']==f})==1,f
    assert len(list((OUT/'tasks').glob('*.md')))==len(tasks)
    return {'tasks':len(tasks),'files':len({t['file'] for t in tasks})}
check('unique_function_and_file_owner',ownership)
def signatures():
    for t in tasks:
        assert (OUT/'tasks'/f"{t['id']}.md").exists()
        if t['language']=='python':
            n=ast.parse(t['signature']+'\n    pass').body[0]
            assert n.name==t['symbol'].split('.')[-1],t['id']
            args=[a.arg for a in [*n.args.posonlyargs,*n.args.args,*n.args.kwonlyargs]]
            assert set(args)==set(t['input']),t['id']
            assert all(t['input'].values())
            if t['baseline_line'] is not None:
                tree=ast.parse((ROOT/t['file']).read_text());node=tree
                for part in t['symbol'].split('.'):
                    node=next(x for x in node.body if isinstance(x,(ast.FunctionDef,ast.AsyncFunctionDef,ast.ClassDef)) and x.name==part)
                src=ast.get_source_segment((ROOT/t['file']).read_text(),node)
                assert hashlib.sha256(src.encode()).hexdigest()==t['baseline_function_sha256'],t['id']
        assert '沿用基线该参数' not in str(t['input']),t['id']
    ast.parse((OUT/'interfaces.pyi').read_text())
    return 'All Python final signatures parse; baseline functions/hashes and argument descriptions match.'
check('signatures_inputs_and_type_declarations',signatures)
def graph():
    ids={t['id'] for t in tasks};by={}
    for t in tasks:by.setdefault(t['symbol'],[]).append(t)
    for t in tasks:
        assert set(t['callee_tasks'])<=ids
        for s in t['calls']:
            if ':' not in s: assert len(by.get(s,[]))<=1,(t['id'],s,'ambiguous call')
        if t['baseline_line'] is None and t['owner']!='V':assert t['caller_tasks'],('no consumer',t['id'])
        if t['file'].startswith('demiflow/execution/'):
            assert not any(next(x for x in tasks if x['id']==cid)['file'].startswith('demiflow/workbench/') for cid in t['callee_tasks']),t['id']
    return 'All new implementation functions have task consumers; engine task graph has no workbench dependency.'
check('producer_consumer_graph',graph)
def gates():
    g=json.loads((OUT/'gates.json').read_text());r=json.loads((OUT/'root-map.json').read_text())
    assert {k for t in tasks for k in t['gates']}==set(g)
    for k,v in g.items():
        assert v['status']=='not_run'
        assert all((ROOT/p).is_file() for p in v['required_existing_files']),(k,'missing regression')
        assert set(v['leaf_tasks'])=={t['id'] for t in tasks if k in t['gates']}
    assert all(set(v)<=set(g) for v in r.values())
    assert len([k for k in r if k.startswith('R.')])==15
    return {'acceptance_gates':len(g),'intent_leaves':len(r),'runtime_acceptance':'not_run'}
check('root_coverage_and_test_targets',gates)
def fixtures():
    from demiflow.execution.plan_interpreter import interpret_physical_plan
    from demiflow.planning.dag import validate_logical_dag,validate_physical_execution_plan
    import demiflow
    assert Path(demiflow.__file__).resolve().is_relative_to(ROOT)
    for p in (OUT/'fixtures').glob('*-v1.json'):
        f=json.loads(p.read_text());interpret_physical_plan(f['logical_dag'],f['physical_plan'],bindings=f['bindings'],manifest_ref=f['manifest_ref'])
    f=json.loads((OUT/'fixtures/native-driver-v2.json').read_text())
    validate_logical_dag(f['logical_dag']);validate_physical_execution_plan(f['physical_plan'])
    d=f['bindings']['document'];assert d['schema']=='demiflow.execution_bindings.v2'
    assert 'executions' in d and 'programs' not in d
    assert set(d)=={'schema','run_id','action_id','dag_id','plan_id','artifact_manifest_ref','supported_slice','profiles','executions','driver_stage_ids','source_bindings','channel_options','checkpoint_rows'}
    assert f['fixture_kind']=='proposed_v2_not_runtime_verified'
    return 'Three v1 samples accepted by actual interpreter. v2 DAG/physical valid; v2 runtime explicitly unverified.'
check('wire_samples_and_v1_compatibility',fixtures)
def links():
    for p in OUT.rglob('*.md'):
        for target in re.findall(r'\]\(([^)]+)\)',p.read_text()):
            if '://' in target or target.startswith('#'):continue
            path=target.split('#')[0]
            assert (p.parent/path).exists(),(str(p.relative_to(OUT)),target)
    return 'All local Markdown links resolve.'
check('documents_and_dispatch_links',links)
def code_examples():
    text=(OUT/'USER-EXPERIENCE.md').read_text()
    snippets=re.findall(r'```python\n(.*?)```',text,re.S)
    for src in snippets:ast.parse(src)
    from demiflow.data.dataset import Dataset
    from demiflow.execution import RunContext
    import inspect
    for method in ('map','take','write_lance','join','map_embeddings','materialize','count'):
        assert hasattr(Dataset,method)
    inspect.signature(Dataset.map).bind(None,lambda row:row,execution='thread',concurrency=2,queue_depth=4)
    inspect.signature(Dataset.write_lance).bind(None,'/tmp/demiflow-example/x',mode='create',return_receipt=True)
    inspect.signature(RunContext).bind('/tmp/demiflow-example/run',entry_identity='example-v1')
    return 'User examples parse and public methods/keywords exist; no future behavior claimed as executed.'
check('product_and_public_api_examples',code_examples)
def preservation():
    tracked=subprocess.check_output(['git','diff','--name-only',BASE],cwd=ROOT,text=True).splitlines()
    untracked=subprocess.check_output(['git','ls-files','--others','--exclude-standard'],cwd=ROOT,text=True).splitlines()
    prefix='docs/upgrade-contracts-v2-20261113/'
    assert all(p.startswith(prefix) for p in tracked+untracked),tracked+untracked
    return 'Only this design directory differs. All production, memory and global architecture files untouched.'
check('workspace_and_protected_files',preservation)
def final():
    text=(OUT/'REVIEW-10-PASSES.md').read_text()
    assert all(f'| {i} |' in text for i in range(1,11))
    assert 'not_run' in (OUT/'INTEGRATION-ACCEPTANCE.md').read_text()
    assert not any('NativeEmbeddingRecovery.bind_client' in p.read_text() for p in (OUT/'tasks').glob('*.md'))
    assert 'stage_limits' in (OUT/'interfaces.pyi').read_text()
    return 'Ten dimension-specific review records present; this does not establish ten complete reviews. Runtime acceptance remains separate.'
check('final_consistency',final)
report={'kind':'design_validation_not_runtime_acceptance','base_commit':BASE,'checks':checks,'runtime_accepted':False,'limitations':['v2 bindings interpreter and implementation remain to be written','browser/Ray/performance/business acceptance not executed by this design task']}
(OUT/'design-validation.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
print(json.dumps({'design_checks_passed':len(checks),'tasks':len(tasks),'files':len({t['file'] for t in tasks}),'runtime_accepted':False},ensure_ascii=False))
