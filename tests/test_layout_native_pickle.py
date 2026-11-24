"""Ordinary current business functions are loaded without SDK substitutions."""
from demiflow.execution.seal.program_pickle import load_payload

def test_moduleless_business_callable_keeps_original_public_execution(tmp_path,monkeypatch):
    import cloudpickle
    from demiflow import data
    from demiflow.execution.pipeline.run_context import RunContext
    fn=eval('lambda row: {"id": row["id"] + 1}', {})
    assert fn.__module__ is None
    restored=load_payload([cloudpickle.dumps(fn)])
    assert restored.__module__ is None and restored({'id':3})=={'id':4}
    monkeypatch.setenv('DEMIFLOW_RESOURCE_ROOT',str(tmp_path/'resources'))
    with RunContext.start(tmp_path/'run'),data.local_execution(workers=1,worker_mode='process'):
        assert data.from_items([{'id':3}]).map(fn).take_all()==[{'id':4}]


def test_large_business_helper_keeps_original_public_execution(tmp_path,monkeypatch):
    import cloudpickle
    from demiflow import data
    from demiflow.execution.pipeline.run_context import RunContext
    # A normal generated lookup can exceed the separate SDK identity budget.
    # Even an SDK-like name in a business module must stay ordinary cloudpickle.
    context={'__name__':'business_lookup'}
    table='A'*(1024*1024+5)
    helper=eval('lambda i: '+repr(table)+'[i]',context)
    helper.__name__=helper.__qualname__='finish_typed_batch'
    context['helper']=helper
    map_row=eval("lambda row: {'id':row['id'],'code':helper(row['id'])}",context)
    restored=load_payload([cloudpickle.dumps(map_row)])
    expected=[{'id':3,'code':'A'}]
    assert restored({'id':3})==expected[0]
    monkeypatch.setenv('DEMIFLOW_RESOURCE_ROOT',str(tmp_path/'resources'))
    with RunContext.start(tmp_path/'run'),data.local_execution(workers=1,worker_mode='process'):
        assert data.from_items([{'id':3}]).map(map_row).take_all()==expected



def test_business_closure_and_current_sql_column_identity():
    import cloudpickle
    from demiflow.operators.relational_sql import Column
    from demiflow.execution.seal.program_identity import describe
    def close_over(n):
        return lambda x: x + n
    fn, values = load_payload([cloudpickle.dumps((close_over(7), {'unchanged': [1, 2]}))])
    assert fn(4) == 11 and values == {'unchanged': [1, 2]}
    assert describe(Column)['module'] == Column.__module__ == 'demiflow.operators.relational_sql'
