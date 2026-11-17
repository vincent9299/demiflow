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
external_tests=set(manifest.get('integration_test_compatibility',{}).get('files',[]))
for relative in set(re.findall(r'tests/test_[A-Za-z0-9_]+\.py', acceptance)):
    assert (ROOT/relative).is_file() or relative in planned_files or relative in external_tests, relative
if external_tests:
    compatibility=manifest['integration_test_compatibility']
    assert compatibility['owner']=='V'
    assert compatibility['phase']=='after_runtime_W_delivery_only'
done('regression paths exist or have explicit local/external delivery owners')

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

# Cross-package wiring is explicit; inspect pinned design bytes, not live lane edits.
wiring=json.loads((HERE/'runtime-wiring.json').read_text())
receipt=json.loads((HERE/wiring['source_receipt']).read_text())
revision=wiring['runtime_design_commit']
assert manifest['design']==wiring['ui_design']=='UI-D1.9/2'
assert revision==receipt['runtime_design_head']
for item in receipt['inputs']:
    raw=subprocess.check_output(['git','show',revision+':'+item['path']],cwd=ROOT)
    assert hashlib.sha256(raw).hexdigest()==item['sha256'],item['path']
upstream=json.loads(subprocess.check_output(['git','show',revision+':'+wiring['runtime_plan_path']],cwd=ROOT))
up_by_id={item['id']:item for item in upstream['tasks']}
overlaps=planned_files & {item['file'] for item in upstream['tasks']}
assert overlaps=={item['file'] for item in wiring['handoffs']},overlaps
for handoff in wiring['handoffs']:
    path=handoff['file']
    assert handoff['final_owner']=='ui:'+owners[path]
    assert handoff['upstream_owner']=='runtime-v3:W'
    assert handoff['order']=='runtime_W_delivery_then_ui_semantic_migration'
    assert {i for i in up_by_id if up_by_id[i]['file']==path}==set(handoff['upstream_tasks'])
    assert all(up_by_id[i]['lane']=='W' for i in handoff['upstream_tasks'])
    assert all(by_id[i]['file']==path for i in handoff['ui_tasks'])
    assert all(by_id[i]['action'] in {'add','modify'} for i in handoff['replacement_tasks'])
for requirement in wiring['requirements']:
    assert all(i in up_by_id for i in requirement['producer_tasks'])
    assert all(i in by_id for i in requirement['ui_tasks'])
    assert all(i in gates for i in requirement['acceptance'])
assert external_tests=={'tests/test_runtime_v3_w.py'}
assert wiring['provider_status']=='required_not_verified'
assert wiring['integration_acceptance']=='not_run'
done('runtime source pin, all shared files, producers and authorized consumers have handoffs')

# These are synthetic consumer-contract examples, not executable engine artifacts.
sample=json.loads((HERE/'samples/runtime-v3-wire.json').read_text())
legacy=json.loads((HERE/'samples/legacy-wire.json').read_text())
for item in (sample,legacy):
    assert item['not_an_engine_artifact'] and item['not_runtime_evidence']
entry=sample['entry'];nodes=entry['dag']['nodes'];edges=entry['dag']['edges']
index={n['occurrence_id']:n for n in nodes}
assert len(index)==len(nodes)==sample['expected']['visual_nodes']
assert len({n['logical_node_id'] for n in nodes})<len(nodes)
assert len(edges)==sample['expected']['edges']
assert len({e['edge_id'] for e in edges})==len(edges)
for edge in edges:
    for side,ports in [('from','output_ports'),('to','input_ports')]:
        endpoint=edge[side];node=index[endpoint['occurrence_id']]
        assert endpoint['logical_node_id']==node['logical_node_id']
        assert endpoint['port'] in node[ports]
    assert edge['from']['occurrence_id']!=edge['to']['occurrence_id']
assert any(e['from']['logical_node_id']==e['to']['logical_node_id'] for e in edges)
assert set(entry['physical']['stages'])==set(index)
assert {e['edge_id'] for e in entry['physical']['exchanges']}=={e['edge_id'] for e in edges}
assert len({n['logical_node_id'] for n in legacy['entry']['dag']['nodes']})==legacy['expected']['visual_nodes']
assert all('occurrence_id' not in e['from'] for e in legacy['entry']['dag']['edges'])
assert by_id['C09']['callers']==['C03','C04']
assert by_id['B18']['action']=='remove' and by_id['B18']['symbol']=='logicalEntry'
done('versioned example topology preserves repeated occurrences and exact port edges')
print(json.dumps(dict(status='design_consistent',product_acceptance='not_run',
    tasks=len(tasks),files=len(owners),lanes=len(set(owners.values())),checks=checks),ensure_ascii=False,indent=2))
