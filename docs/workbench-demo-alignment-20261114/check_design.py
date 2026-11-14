"""Read-only design consistency check. Does not execute product code or pytest."""
from pathlib import Path
import ast
import hashlib
import json
import re
import subprocess

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
manifest = json.loads((HERE / 'tasks.json').read_text())
tasks = manifest['tasks']
by_id = {item['id']: item for item in tasks}
assert len(tasks) == len(by_id), 'duplicate task ID'
checks = []

def done(name):
    checks.append(name)

assert manifest['source_head'] == '267b9144c6bf7447c6822aa716f6d5c0eda16990'
done('baseline is explicit')
owners = {}
for item in tasks:
    assert item['lane'] in 'ABCDEFV'
    assert item['id'].startswith(item['lane'])
    assert item['file'] not in owners or owners[item['file']] == item['lane'], item['file']
    owners[item['file']] = item['lane']
    assert all(item[key] for key in ['signature', 'inputs', 'outputs', 'behavior', 'contract', 'callers', 'acceptance'])
    assert all(x in by_id for x in item['callers'] + item['acceptance']), item['id']
done('each file has one writing lane and each task has concrete IO')
gates = {item['id']:item for item in tasks if item['action'] == 'test'}
assert set(gates) == {f'V{i:02}' for i in range(1, 10)}
for item in tasks:
    assert all(gate in gates for gate in item['acceptance']), item['id']
    if item['action'] == 'add':
        callers = [by_id[key] for key in item['callers']]
        allowed = {'modify', 'add', 'assembly'} | ({'test'} if item['lane']=='V' else set())
        assert any(c['action'] in allowed for c in callers), item['id']
done('new symbols have consumers authorized to implement; every task has a gate')
acceptance = (HERE/'04-acceptance.md').read_text()
for item in gates.values():
    nodeid = f"{item['file']}::{item['symbol']}"
    assert nodeid in acceptance and ('  '+nodeid+' \\') in acceptance, nodeid
done('all nine new test functions are named in the executable command')

# Existing-symbol checks prevent stale/nonexistent task-book function names.
for item in tasks:
    if item['action'] not in {'modify', 'remove'}:
        continue
    path = ROOT/item['file']
    source = path.read_text()
    name = item['symbol'].split('.')[-1]
    if path.suffix == '.py':
        assert name in {n.name for n in ast.walk(ast.parse(source)) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))}, item['id']
    elif path.suffix == '.js':
        assert re.search(r'\b(?:function\s+'+re.escape(name)+r'\s*\(|'+re.escape(name)+r'\s*[:=])', source), item['id']
done('existing function and method names exist in the source')

planned_files=set(owners)
for relative in set(re.findall(r'tests/test_[A-Za-z0-9_]+\.py', acceptance)):
    assert (ROOT/relative).is_file() or relative in planned_files, relative
done('regression paths exist; future tests have explicit owners')

cards = '\n'.join(path.read_text() for path in (HERE/'tasks').glob('*.md'))
for item in tasks:
    assert cards.count('## '+item['id']+' · ') == 1, item['id']
done('all task cards exist exactly once')
for path in HERE.rglob('*.md'):
    for link in re.findall(r'\]\(([^)]+)\)', path.read_text()):
        if '://' in link or link.startswith('#'):
            continue
        assert (path.parent/link.split('#')[0]).exists(), (str(path),link)
done('document and image links resolve')

proof=json.loads((HERE/'evidence/provenance.json').read_text())
for item in proof['inputs']:
    path=ROOT/item['path']
    assert hashlib.sha256(path.read_bytes()).hexdigest() == item['sha256'],item['path']
    committed=subprocess.check_output(['git','show',manifest['source_head']+':'+item['path']],cwd=ROOT)
    assert hashlib.sha256(committed).hexdigest() == item['sha256'],item['path']
for item in proof['artifacts']:
    path=HERE/item['path']
    assert hashlib.sha256(path.read_bytes()).hexdigest()==item['sha256'],item['path']
done('reviewed inputs match baseline commit; evidence hashes match')

frozen={'demiflow/execution/'+p for p in ['raw_fetch.py','queue_execution.py','queue_sinks.py','queue_source.py','contracts/exchange.py']}
frozen.update({'demiflow/planning/dag.py','demiflow/resources/contracts.py'})
assert not frozen.intersection(planned_files)
done('load-bearing files have no implementation tasks')
print(json.dumps(dict(status='design_consistent',product_acceptance='not_run',
    tasks=len(tasks),files=len(owners),lanes=len(set(owners.values())),checks=checks),ensure_ascii=False,indent=2))
