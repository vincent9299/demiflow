"""Read-only source inventory for G; write evidence only to the explicit output path."""
import ast
import hashlib
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[3]
BASE = '122edf4bd0c827338846abd0114f0b36cf8c1b6d'
PACKAGE = Path('/yzp/zhaozy/yangzepeng/0905/_demiflow/shared-review/architecture-upgrade-20261120')
F = ROOT.parent/'runtime-services-20261120/docs/architecture-upgrade-20261120/F'
OUT = Path(sys.argv[1])
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def git(*args):return subprocess.check_output(['git',*args],cwd=ROOT,text=True).strip()
def module(path):
    text=path.relative_to(ROOT).as_posix()[:-3].replace('/','.')
    return text.removesuffix('.__init__')
def imported(path,node):
    if not node.level:return node.module or ''
    parts=module(path).split('.')
    if path.name!='__init__.py':parts.pop()
    return '.'.join(parts[:len(parts)-node.level+1]+((node.module or '').split('.') if node.module else []))
def scoped(tree):
    result={}
    def walk(node,scope=()):
        if isinstance(node,(ast.ClassDef,ast.FunctionDef,ast.AsyncFunctionDef)):scope=(*scope,node.name)
        result[node]='.'.join(scope) or '<module>'
        for child in ast.iter_child_nodes(node):walk(child,scope)
    walk(tree);return result
f_input=json.loads((F/'G-input.json').read_text())
retained_paths={e['source_path'] for e in f_input['edges']} | {e['path'] for e in f_input['P1_retained_runtime_types']}
source_files={}; all_imports={}
for path in sorted((ROOT/'demiflow').rglob('*.py')):
    if '_vendor' in path.parts:continue
    tree=ast.parse(path.read_text());scope=scoped(tree)
    for node in ast.walk(tree):
        if isinstance(node,ast.ImportFrom):
            target=imported(path,node)
            all_imports.setdefault(target,[]).append(dict(path=str(path.relative_to(ROOT)),line=node.lineno,scope=scope[node],names=[n.name for n in node.names]))
    if str(path.relative_to(ROOT)) in retained_paths:source_files[path]=(tree,scope)
def import_sites(target,names=()):
    records=[]
    for row in all_imports.get(target,[]):
        selected=[n for n in row['names'] if not names or n in names]
        if selected:records.append(dict(row,names=selected))
    return records
edges=[]
for edge in f_input['edges']:
    path=ROOT/edge['source_path'];tree,scopes=source_files[path]
    hits=[n for n in ast.walk(tree) if isinstance(n,ast.ImportFrom) and n.lineno==edge['line'] and imported(path,n)==edge['target_module'] and sorted(a.name for a in n.names)==sorted(edge['imported_names'])]
    assert len(hits)==1 and sha(path)==edge['source_sha256'],edge['id']
    node=hits[0]
    owner=scopes[node]
    enclosing=next((n for n in ast.walk(tree) if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef)) and scopes[n]==owner),None)
    provider=ROOT/(edge['target_module'].replace('.','/')+'.py')
    if not provider.exists():provider=ROOT/edge['target_module'].replace('.','/')/'__init__.py'
    record={k:edge[k] for k in ('id','group','source_path','line','target_module','imported_names')}
    record.update(source_sha256=sha(path),owner=owner,owner_span=[enclosing.lineno,enclosing.end_lineno] if enclosing else None,statement=ast.unparse(node),provider_path=str(provider.relative_to(ROOT)),provider_sha256=sha(provider),all_direct_provider_import_sites=import_sites(edge['target_module'],edge['imported_names']))
    if enclosing:record['owner_direct_import_sites']=import_sites(edge['source_module'],[enclosing.name])
    edges.append(record)
retained=[]
for item in f_input['P1_retained_runtime_types']:
    path=ROOT/item['path'];tree,_=source_files[path];node=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name==item['class'])
    retained.append(dict(type=item['module']+'.'+item['class'],path=item['path'],line=node.lineno,end=node.end_lineno,source_sha256=sha(path),class_ast_sha256=hashlib.sha256(ast.dump(node,include_attributes=False).encode()).hexdigest(),imports=import_sites(item['module'],[item['class']]),SDK_consumer=item['SDK_consumer']))
mixed=[]
for name in ['demiflow.execution.local_kernel','demiflow.data.local_relational','demiflow.data.result_cache']:
    path=ROOT/(name.replace('.','/')+'.py');records=import_sites(name)
    engine=[r for r in records if r['path'].startswith('demiflow/execution/')]
    mixed.append(dict(module=name,path=str(path.relative_to(ROOT)),sha256=sha(path),direct_import_statements=len(records),engine_import_statements=len(engine),engine_imported_symbols=sum(len(r['names']) for r in engine),imports=records))
frozen=['demiflow/execution/'+n+'.py' for n in ['raw_fetch','queue_execution','queue_sinks','queue_source']]+['demiflow/planning/dag.py','demiflow/execution/contracts/exchange.py','demiflow/resources/contracts.py']
refs=[F/'G-input.json',F/'F2-SERVICE-CONTRACTS.txt',PACKAGE/'context-manifest.json',PACKAGE/'context-amendments-gpt-review-20261120.json',PACKAGE/'DISPATCH-G.txt',PACKAGE/'G-SCOPE-CORRECTION.txt',PACKAGE/'TASK-G.txt',PACKAGE/'USER-AUTHORIZATION.txt',PACKAGE/'CONTEXT.txt',PACKAGE/'REVIEW-HANDOFF.txt',PACKAGE/'reviews/C-gpt-design-v1/receipt.json',PACKAGE/'reviews/F-gpt-design-v1/receipt.json',ROOT/'AGENTS.md',ROOT/'docs/repository-rules.md',ROOT/'docs/p1-sdk-engine-boundary-20261119.md',ROOT/'tests/fixtures/p1-native-uses.json']
result=dict(format='G-source-inventory-v1',base_sha=BASE,branch=git('branch','--show-current'),head_at_scan=git('rev-parse','HEAD'),worktree=str(ROOT),product_diff=git('diff',BASE,'--','demiflow','tests'),edge_count=len(edges),groups={g:sum(e['group']==g for e in edges) for g in f_input['groups']},edges=edges,P1_retained_runtime_types=retained,mixed_locations=mixed,frozen=[dict(path=p,sha256=sha(ROOT/p)) for p in frozen],references=[dict(path=str(p),sha256=sha(p),bytes=p.stat().st_size) for p in refs],limits='AST ImportFrom inventory of repository product Python excluding vendor; not a complete dynamic call graph, SCC recomputation, runtime acceptance or external-consumer guarantee.')
assert result['groups']=={'top-seed':3,'lazy-into-anchor-scc':9,'lazy-outside-anchor-scc':6}
assert not result['product_diff']
adjacent_path=ROOT/'demiflow/operator_llm/parser.py'
adjacent_node=next(n for n in ast.walk(ast.parse(adjacent_path.read_text())) if isinstance(n,ast.ImportFrom) and n.lineno==151)
result['adjacent_findings']=[dict(id='G-ADJ-01',source_path=str(adjacent_path.relative_to(ROOT)),line=adjacent_node.lineno,statement=ast.unparse(adjacent_node),sha256=sha(adjacent_path),discovery='Full parser chain inspection, not an added original v3/F seed edge')]
OUT.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
print(json.dumps(dict(edges=len(edges),groups=result['groups'],retained_types=len(retained),mixed=[{k:r[k] for k in ['module','engine_import_statements','engine_imported_symbols']} for r in mixed],product_diff_empty=True),ensure_ascii=False))
