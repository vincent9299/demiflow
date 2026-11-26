"""Real finite/timed grouping and prefix inputs through checked Dataset plans."""
import importlib.util
import lance
import pytest
from demiflow import data
from demiflow.execution.pipeline.checked import CheckedExecution
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.forge.runs.manager import DEFAULT_LIMITS
from test_forge_native_values import catalog,fixture


@pytest.mark.parametrize('expression,expected',[
    ("rows.group_batches('x', max_rows=2)",[
        {'x':1,'items':[{'x':1}],'group_index':0,'group_last':True},
        {'x':2,'items':[{'x':2}],'group_index':0,'group_last':True}]),
    ("rows.group_batches('x', max_rows=2, flush_interval=.04)",[
        {'x':1,'items':[{'x':1}]},{'x':2,'items':[{'x':2}]}]),
    ("rows.prepend(rows.limit(1), max_rows=1)",[{'x':1},{'x':1},{'x':2}]),
])
def test_group_and_prefix_checked_actual_receipt(tmp_path,catalog,expression,expected):
    path,report,args=fixture(tmp_path,catalog,body=
        f'    return {expression}.write_lance(target, mode="create", return_receipt=True)\n')
    assert report['status']=='checked',report['diagnostics']
    spec=importlib.util.spec_from_file_location('flows.values',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    observations=[]
    checked=CheckedExecution(**{k:report[k] for k in ('nodes','edges','actions','operators')},
        source_root=tmp_path,limits=DEFAULT_LIMITS,record=lambda *args:None,
        observe=lambda *args,**kwargs:observations.append((args,kwargs)),check_stop=lambda:None,
        report_digest=report['digest'])
    with RunContext.start(tmp_path/'run') as run,data.local_execution(workers=8),checked.bind_context():
        receipt=module.run(**args)
        assert run.journal.execution_commits(report['actions'][0])[-1]['status']=='acked'
    assert lance.dataset(receipt.uri).to_table().to_pylist()==expected
    assert observations


@pytest.mark.parametrize('expression',[
    "rows.group_batches('missing')", "rows.group_batches('x', output='x')",
    "rows.group_batches('x', flush_interval=0)","rows.group_batches('x',max_rows=0)",
    "rows.prepend(rows.limit(1),max_rows=-1)",
    "rows.prepend(rows.rename_columns({'x':'different'}),max_rows=1)",
])
def test_invalid_group_and_prefix_are_rejected_without_execution(tmp_path,catalog,expression):
    _,report,_=fixture(tmp_path,catalog,body=
        f'    return {expression}.write_lance(target, mode="create", return_receipt=True)\n')
    assert report['status']!='checked'
    assert not (tmp_path/'output.lance').exists()


def test_prepend_actual_prefix_is_recursively_checked(tmp_path,catalog):
    path,report,args=fixture(tmp_path,catalog,body=
        '    return rows.prepend(rows.limit(1), max_rows=2).write_lance(target, mode="create", return_receipt=True)\n')
    assert report['status']=='checked',report['diagnostics']
    # Runtime wiring changes the prefix only; main, declared bounds and sink
    # remain identical. This must fail before publishing the target.
    path.write_text(path.read_text().replace('rows.limit(1)','rows.limit(2)'))
    spec=importlib.util.spec_from_file_location('flows.values',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    checked=CheckedExecution(**{k:report[k] for k in ('nodes','edges','actions','operators')},
        source_root=tmp_path,limits=DEFAULT_LIMITS,record=lambda *args:None,check_stop=lambda:None)
    from demiflow.data.runtime_contract import RuntimeContractError
    with RunContext.start(tmp_path/'run'),data.local_execution(workers=8),pytest.raises(RuntimeContractError,match='limit'):
        with checked.bind_context():module.run(**args)
    assert not (tmp_path/'output.lance').exists()
