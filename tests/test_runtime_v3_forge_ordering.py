"""Checked authoring and real managed execution use the same pure operators."""
import importlib.util
import json
from pathlib import Path
import sys

import lance
import pytest
from demiflow import data
from demiflow.execution.checked import CheckedExecution
from demiflow.execution.run_context import RunContext
from demiflow.workbench.runs.manager import DEFAULT_LIMITS
from test_forge_native_values import catalog, fixture


OPERATIONS = [
    ("sort('x', descending=True)", [{'x': 2}, {'x': 1}]),
    ("repartition(2)", [{'x': 1}, {'x': 2}]),
    ("random_shuffle(seed=3, num_blocks=2).sort('x')", [{'x': 1}, {'x': 2}]),
    ("randomize_block_order(seed=3).sort('x')", [{'x': 1}, {'x': 2}]),
    ("random_sample(1.0, seed=3)", [{'x': 1}, {'x': 2}]),
]


@pytest.mark.parametrize('expression,expected', OPERATIONS)
def test_ordering_checked_and_actual_commit(tmp_path,catalog,expression,expected):
    path,report,args=fixture(tmp_path,catalog,body=
        f'    return rows.{expression}.write_lance(target, mode="create", return_receipt=True)\n')
    assert report['status']=='checked', report['diagnostics']
    spec=importlib.util.spec_from_file_location('flows.values',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    checked=CheckedExecution(**{k:report[k] for k in ('nodes','edges','actions','operators')},
        source_root=tmp_path,limits=DEFAULT_LIMITS,record=lambda *args:None,check_stop=lambda:None)
    with RunContext.start(tmp_path/'run') as run, data.local_execution(workers=4), checked.bind_context():
        receipt=module.run(**args)
        assert run.journal.execution_commits(report['actions'][0])[-1]['status']=='acked'
    assert lance.dataset(receipt.uri).to_table().to_pylist()==expected


@pytest.mark.parametrize('expression', ["sort('missing')", "sort('x', descending=[True, False])",
    "sort('x', boundaries=['bad'])", "repartition(0)", "repartition(2, 3)",
    "repartition(2, keys=['missing'])", "random_shuffle(seed=True)",
    "random_sample(2.0)", "unknown_transform()"])
def test_ordering_invalid_declarations_stay_rejected(tmp_path,catalog,expression):
    _,report,_=fixture(tmp_path,catalog,body=
        f'    return rows.{expression}.write_lance(target, mode="create", return_receipt=True)\n')
    assert report['status']!='checked'
    assert not (tmp_path/'output.lance').exists()


def test_checked_registry_accepts_mixed_finite_and_stream_without_second_engine():
    from demiflow.planning.traits import check_composition
    assert check_composition([{'kind':'async_map','driver':'stream'},
        {'kind':'sort','driver':'pull'},{'kind':'limit','driver':'pull'}])['supported']
    assert not check_composition([{'kind':'invented','driver':'pull'}])['supported']
    assert not check_composition([{'kind':'map','driver':'pull','native_options':True}])['supported']


def test_async_join_sort_is_one_checked_pipeline(tmp_path,catalog,monkeypatch):
    body = ('    enriched = rows.map_async(identity, concurrency=1)\n'
            '    return enriched.join(rows, on="x").sort("x").write_lance(target, mode="create", return_receipt=True)\n')
    path,_,args=fixture(tmp_path,catalog,body=body)
    (tmp_path/'operators/identity.py').write_text('from typing import TypedDict\n'
        'class Row(TypedDict):\n    x: int | None\n'
        'async def identity(row: Row) -> Row:\n    return row\n')
    path.write_text(path.read_text().replace('from demiflow import data',
        'from demiflow import data\nfrom operators.identity import identity'))
    from demiflow.forge.flow_check import FlowChecker
    from demiflow.data.flow_contract import arrow_fields
    import pyarrow as pa
    report=FlowChecker(tmp_path,catalog,{'input':{'uri':args['source'],'version':1,
        'fields':arrow_fields(pa.schema([('x',pa.int64())]))}},[args['target']]).inspect('flows/values.py:run',args)
    assert report['status']=='checked',report['diagnostics']
    monkeypatch.syspath_prepend(str(tmp_path))
    from types import ModuleType
    namespace=ModuleType('operators');namespace.__path__=[str(tmp_path/'operators')]
    monkeypatch.setitem(sys.modules,'operators',namespace)
    spec=importlib.util.spec_from_file_location('flows.values',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    checked=CheckedExecution(**{k:report[k] for k in ('nodes','edges','actions','operators')},
        source_root=tmp_path,limits=DEFAULT_LIMITS,record=lambda *args:None,check_stop=lambda:None)
    try:
        with RunContext.start(tmp_path/'run') as run,data.local_execution(workers=8),checked.bind_context():
            receipt=module.run(**args)
            assert run.journal.execution_commits(report['actions'][0])[-1]['status']=='acked'
        assert lance.dataset(receipt.uri).to_table().to_pylist()==[{'x':1},{'x':2}]
    finally:
        sys.modules.pop('operators.identity',None)


def test_ordering_fact_registration_does_not_rewrite_historical_identity():
    from demiflow.execution.recovery_identity import describe, operation_identity
    from demiflow.planning.dataflow_compile import _operation_semantic_facts
    for expression,_ in OPERATIONS:
        # Invoke the public builders only: their normalized plan is the same
        # object used before checked authoring gained these declarations.
        ds=eval('data.from_items([]).'+expression,{'data':data})
        for op in ds._plan.operations:
            assert operation_identity(op)==describe(op)
            assert _operation_semantic_facts(op)['type']==type(op).__module__+'.'+type(op).__qualname__
