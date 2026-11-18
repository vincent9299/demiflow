"""API calls are attached to actual compiled occurrences, not AST guesses."""
import importlib.util
import json
import hashlib
import sys
from pathlib import Path

from demiflow.execution.plan_capture import capture_pipeline
from demiflow.workbench.graph.plan_callsite import PlanCallsites
from demiflow.workbench.graph.plan_details import build_plan_details


def compile_code(tmp_path, text):
    path=tmp_path/'flow.py';path.write_text(text)
    spec=importlib.util.spec_from_file_location('operator_contract_probe',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    sources={'flow.py':hashlib.sha256(path.read_bytes()).hexdigest()}
    with PlanCallsites(tmp_path,sources) as calls:
        captured=capture_pipeline(module.run,{},on_compiled=calls.bind_compilation)
    details=build_plan_details(captured.compiled, sources=sources, root=tmp_path, callsites=calls, bounds=captured.bounds)
    return captured,details,module


def test_chained_join_filter_union_and_action_are_distinct_with_real_api_arguments(tmp_path):
    text='''from demiflow import data

def predicate(row): raise AssertionError("row execution prohibited")
def unused(row): return row

def run():
    left = data.from_items([{'image_id': 1, 'concept_id': 2}])
    images = data.from_items([{'image_id': 1}])
    concepts = data.from_items([{'concept_id': 2}])
    ignored = left.map(unused)
    options = {'on': 'image_id'}
    joined = (left.filter(predicate)
        .join(images, **options)
        .join(concepts, on='concept_id'))
    return joined.union(joined).take_all()
'''
    captured,details,module=compile_code(tmp_path,text)
    assert len(details)==8
    calls=[details[n.occurrence_id]['callsite'] for n in captured.compiled.graph.nodes]
    assert all(d['availability']=='available' for d in calls),calls
    joins=[d for d in calls if d['metadata']['api']=='Dataset.join']
    assert [d['text'] for d in joins]==[".join(images, **options)",".join(concepts, on='concept_id')"]
    assert joins[0]['metadata']['expansions']==['options']
    for expected,detail in zip(['image_id','concept_id'],joins):
        params={p['name']:p for p in detail['metadata']['arguments']}
        assert params['on']['value']==expected
        assert params['how']['value']=='inner'
        assert params['other']['value']['type']=='Dataset'
        assert 'max_rows' not in params and 'output' not in params # keyed implementation defaults aren't join API arguments
    assert calls[-1]['metadata']['api']=='Dataset.take_all'
    assert calls[-1]['text']=='.take_all()'
    assert not any('unused' in d['text'] for d in calls)
    # Observing calls has no influence on graph/semantic identity.
    again=capture_pipeline(module.run,{})
    assert again.compiled.graph==captured.compiled.graph
    assert sys.getprofile() is None


def test_repeated_helper_calls_shared_prefix_and_schema_unknown_are_honest(tmp_path):
    captured,details,_=compile_code(tmp_path,'''from demiflow import data
import pyarrow as pa

def unknown(row): raise AssertionError('do not invoke UDF')
def branch(ds, name): return ds.select_columns([name]).map(unknown)
def run():
    base = data.from_arrow(pa.table({'a': [1], 'b': ['x']}))
    first = branch(base, 'a'); second = branch(base, 'b')
    return first.union(second).take_all()
''')
    nodes=captured.compiled.graph.nodes
    assert len([n for n in nodes if n.kind=='source'])==1
    maps=[n for n in nodes if n.kernel.kind=='MapOp']
    assert len(maps)==2 and maps[0].occurrence_id!=maps[1].occurrence_id
    for node in maps:
        detail=details[node.occurrence_id]
        assert detail['callsite']['text']=='.map(unknown)'
        schema=detail['schema']['metadata']
        assert schema['output']['status']=='unknown'
        assert schema['inputs'][0]['schema']['status']=='declared'
    source=next(n for n in nodes if n.kind=='source')
    assert [f['name'] for f in details[source.occurrence_id]['schema']['metadata']['output']['fields']]==['a','b']
    selected=[details[n.occurrence_id]['schema']['metadata']['output']['fields'][0]['name'] for n in nodes if n.kernel.kind=='SelectColumnsOp']
    assert selected==['a','b']


def test_argument_projection_does_not_call_repr_or_read_arbitrary_objects(tmp_path):
    calls=PlanCallsites(tmp_path,{})
    class Opaque:
        def __repr__(self): raise AssertionError('repr is user code')
        def __iter__(self): raise AssertionError('iteration is user code')
    assert calls._value(Opaque())['type']=='Opaque'
    assert calls._value(list(range(1000)))['length']==1000
    assert calls._value(float('nan'))['type']=='float'
    assert len(json.dumps(calls._value({'nested': ['x']*1000})))<256


def test_explicit_arrow_output_schema_and_expanded_secret_arguments(tmp_path):
    captured,details,_=compile_code(tmp_path,'''from demiflow import data
import pyarrow as pa

def fn(row, **kwargs): raise AssertionError('must not execute')
def run():
    options = {'fn_kwargs': {'token': 'HIDDEN_SECRET', 'url': 'https://user:pass@example.test/x?token=bad'}}
    rows = data.from_arrow(pa.table({'id': [1]})).map(fn, **options)
    return rows.write_lance('/tmp/demiflow-never-write-operator-probe.lance', schema=pa.schema([('id', pa.int64())]), mode='create')
''')
    transform=next(n for n in captured.compiled.graph.nodes if n.kernel.kind=='MapOp')
    params=json.dumps(details[transform.occurrence_id]['configuration'],ensure_ascii=False)
    assert 'HIDDEN_SECRET' not in params and 'user:pass' not in params and 'token=bad' not in params
    assert '不展示凭据' in params and 'https://example.test/x' in params
    terminal=next(n for n in captured.compiled.graph.nodes if n.kind=='terminal')
    declared=details[terminal.occurrence_id]['schema']['metadata']['declarations']
    assert declared[0]['parameter']=='schema'
    assert declared[0]['fields']==[{'name':'id','type':'int64','nullable':True}]
    assert declared[0]['expression']=="pa.schema([('id', pa.int64())])"


import pytest

@pytest.mark.parametrize('operation',[
    'map(fn)', 'flat_map(fn)', 'map_async(fn)', 'map_batches(fn, batch_size=2)',
    'filter(fn)', 'limit(1)', "select_columns(['id'])", "drop_columns(['value'])",
    "rename_columns({'value':'renamed'})", "add_column('extra', fn)",
    'random_sample(0.5)', "sort('id')", 'repartition(2)',
    "group_batches('id', max_rows=2)", "reduce_by_key('id', fold, initial=0)",
])
def test_native_operator_families_keep_public_api_call_and_arguments(tmp_path, operation):
    captured,details,_=compile_code(tmp_path,"""from demiflow import data
import pyarrow as pa

def fn(row): raise AssertionError('not a data execution')
def fold(state, row): raise AssertionError('not a data execution')
def run():
    rows = data.from_arrow(pa.table({'id': [1], 'value': [2]}))
    return rows.OPERATION.take_all()
""".replace('OPERATION',operation))
    nodes=captured.compiled.graph.nodes
    assert len(nodes)==3
    detail=details[nodes[1].occurrence_id]['callsite']
    assert detail['availability']=='available',detail
    assert detail['text']=='rows.'+operation
    assert detail['metadata']['api']=='Dataset.'+operation.split('(')[0]
    assert details[nodes[1].occurrence_id]['configuration']['metadata']['arguments']
    assert details[nodes[1].occurrence_id]['schema']['metadata']['inputs']


def test_parenthesized_unicode_chain_keeps_exact_link_and_context(tmp_path):
    captured,details,_=compile_code(tmp_path,'''from demiflow import data
def fn(row): raise AssertionError('never execute')
def run():
    return ((data.from_items([{'文本': '中文'}]).map(fn, label='中文'))
        .filter(fn)).take_all()
''')
    filters=[n for n in captured.compiled.graph.nodes if n.kernel.kind=='FilterOp']
    detail=details[filters[0].occurrence_id]['callsite']
    assert detail['text']=='.filter(fn)'
    assert "label='中文'" in detail['metadata']['context']
    terminal=details[captured.compiled.graph.terminals[0]]['callsite']
    assert terminal['text']=='.take_all()'
