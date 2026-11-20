"""Read-only P12 design inventory; writes only the named evidence directory."""
import ast
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[3]
OUT = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).parent
BASE = '122edf4bd0c827338846abd0114f0b36cf8c1b6d'
OLD = 'e3b645ba97ddb57d9636d3d3f75fb1f6dff3d1f1'
CONTEXT = Path('/yzp/zhaozy/yangzepeng/0905/_demiflow/shared-review/architecture-upgrade-20261120')
def git(*args):
    return subprocess.check_output(['git', *args], cwd=ROOT, text=True).strip()
def sha(raw):
    return hashlib.sha256(raw).hexdigest()
def write(name, value):
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT/name).write_text(json.dumps(value, ensure_ascii=False, indent=2)+'\n')
manifest = json.loads((CONTEXT/'context-manifest.json').read_text())
checks = []
for record in manifest['files']:
    checks.append({**record, 'original_matches': sha(Path(record['original_path']).read_bytes()) == record['sha256'],
                   'snapshot_matches': sha(Path(record['snapshot_path']).read_bytes()) == record['sha256']})
assert all(r['original_matches'] and r['snapshot_matches'] for r in checks)
write('references.json', dict(manifest_path=str(CONTEXT/'context-manifest.json'),
    manifest_sha256=sha((CONTEXT/'context-manifest.json').read_bytes()), files=checks,
    read_in_full=['USER-AUTHORIZATION.txt','CONTEXT.txt','REVIEW-HANDOFF.txt','TASK-B.txt',
                 'context-manifest.json','01-intent-message-block.md','03-architecture-master-intent-v1.md',
                 '04-demiflow-forge-global-architecture-20261108.md','08-AGENTS.md',
                 '09-demiflow-architecture-20261119.md','10-design.json',
                 '11-CLAUDE-REVIEW-p12-project-run-boundary-v2.1-20261119.md','12-repository-rules.md'],
    note='All manifest hashes checked. read_in_full is an author reading record; other files are reference/history, not a claim of full reading.'))
old = json.loads((ROOT/'docs/p12-project-run-boundary-20261119/source-inventory.json').read_text())
paths = [x['path'] for x in old['sources']]
paths += ['demiflow/workbench/analysis/source_query.py','demiflow/workbench/analysis/catalog_profile.py',
          'demiflow/workbench/notebooks/data.py','tests/test_forge_index.py','tests/test_forge_index_runs.py',
          'tests/test_forge_workspace_sync.py','tests/test_workbench_engine_boundary.py','tests/merge_gate.py']
paths = sorted(set(paths))
sources=[]
for rel in paths:
    file=ROOT/rel
    raw=file.read_bytes()
    symbols=[]
    if file.suffix=='.py':
        tree=ast.parse(raw)
        for node in tree.body:
            if isinstance(node,(ast.FunctionDef,ast.AsyncFunctionDef,ast.ClassDef)):
                symbols.append(dict(name=node.name,line=node.lineno,end=node.end_lineno))
    oldraw=subprocess.check_output(['git','show',OLD+':'+rel],cwd=ROOT)
    sources.append(dict(path=rel,absolute_path=str(file),sha256=sha(raw),bytes=len(raw),
                        same_at_old_design_base=raw==oldraw,symbols=symbols))
tracked=git('ls-files','*.py').splitlines()
proxies=[]
tree=ast.parse((ROOT/'demiflow/workbench/projects/workspace.py').read_text())
for node in tree.body:
    if isinstance(node,ast.FunctionDef) and any(isinstance(n,ast.ImportFrom) and n.module=='demiflow.workbench.worker' for n in ast.walk(node)):
        proxies.append(dict(name=node.name,line=node.lineno,end=node.end_lineno))
assert len(proxies)==18
names=[v['name'] for v in proxies]+['bind_audit_report','workspace_changes','verify_run_basis','attach_result','table_binding',
       'resolve_arguments','_inspect_flow','_report_status','_audit_check','fixed_input','resolve_version']
pattern=re.compile(r'\b(?:'+ '|'.join(names)+r')\b|runs\.limits|index\.(?:begin|publish|synchronize|rebuild)')
hits=[]
resolved_calls=[]
candidate_files=[]
for rel in tracked:
    if rel.startswith(('demiflow/_vendor/','docs/')):continue
    text=(ROOT/rel).read_text()
    tree=ast.parse(text)
    functions=[n for n in ast.walk(tree) if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef))]
    def owner(line):
        inside=[n for n in functions if n.lineno<=line<=n.end_lineno]
        return min(inside,key=lambda n:n.end_lineno-n.lineno).name if inside else '<module>'
    bindings={}
    for n in ast.walk(tree):
        if isinstance(n,ast.ImportFrom) and n.module:
            for alias in n.names:bindings[alias.asname or alias.name]=n.module+'.'+alias.name
        elif isinstance(n,ast.Import):
            for alias in n.names:bindings[alias.asname or alias.name.split('.')[0]]=alias.name if alias.asname else alias.name.split('.')[0]
    def resolve(n):
        if isinstance(n,ast.Name):return bindings.get(n.id,n.id)
        if isinstance(n,ast.Attribute):return resolve(n.value)+'.'+n.attr
        return ''
    for n in ast.walk(tree):
        if not isinstance(n,ast.Call):continue
        target=resolve(n.func)
        if target.startswith(('demiflow.workbench.projects.workspace.','demiflow.workbench.runs.manager.','demiflow.workbench.projects.index.','demiflow.workbench.runs.limits.')):
            if target.rsplit('.',1)[-1] in names+['begin','publish','synchronize','rebuild','limits_with']:
                resolved_calls.append(dict(path=rel,line=n.lineno,owner=owner(n.lineno),target=target))
    matched=False
    for n,line in enumerate(text.splitlines(),1):
        if pattern.search(line):
            hits.append(dict(path=rel,line=n,owner=owner(n),text=line.strip()))
            matched=True
    if matched:candidate_files.append(dict(path=rel,sha256=sha((ROOT/rel).read_bytes())))
write('callsite-candidates.json',dict(source_sha=BASE,proxies=proxies,hits=hits,resolved_calls=resolved_calls,candidate_files=candidate_files,
 note='Lexical candidate ledger over tracked Python, including from-import aliases, calls and strings. Resolve same-name false positives and test monkeypatch/embedded source strings; this is not a complete dynamic call graph.'))
surface=ast.literal_eval(ast.parse((ROOT/'demiflow/execution/host/_surface.py').read_text()).body[1].value)
workspaces=['demiflow/workbench/projects/workspace.py','demiflow/workbench/runs/manager.py']
p4={m:{n:fs for n,fs in ns.items() if any(f in workspaces for f in fs)} for m,ns in surface['independent_imports'].items()}
p4={m:ns for m,ns in p4.items() if ns}
write('source-inventory.json',dict(base_sha=BASE,old_design_base_sha=OLD,current_head=git('rev-parse','HEAD'),
 branch=git('branch','--show-current'),worktree=str(ROOT),initial_status_from_opening_probe='',status_at_inventory_generation=git('status','--short'),sources=sources,
 old_to_new_diff=git('diff','--name-status',OLD,BASE).splitlines(),
 current_main=git('rev-parse','main'),current_integration=git('rev-parse','integration'),
 project_modules=sorted(p.name for p in (ROOT/'demiflow/workbench/projects').glob('*.py')),
 run_modules=sorted(p.name for p in (ROOT/'demiflow/workbench/runs').glob('*.py')),
 p4_independent_consumers=p4,
 p4_parameter_origins=surface['parameter_origins'],p4_reflection_sites=surface['reflection_sites'],
 p4_surface_sha256=sha((ROOT/'demiflow/execution/host/_surface.py').read_bytes()),
 p4_document_sha256=sha((ROOT/'docs/workbench-engine-surface-v3.json').read_bytes())))
print(json.dumps(dict(sources=len(sources),proxies=len(proxies),callsite_candidates=len(hits),
    changed_sources=[r['path'] for r in sources if not r['same_at_old_design_base']],
    all_references_verified=True,project_modules=11,run_modules=18),ensure_ascii=False))
