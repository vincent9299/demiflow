"""Original SDK by-value records retain computation without retired path aliases."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.seal.program_pickle import load_payload
from runtime_v3_history_scenarios import _extract_original


@pytest.fixture(scope='module')
def original_callbacks(tmp_path_factory):
    root = tmp_path_factory.mktemp('native-value-original')
    old, _ = _extract_original(root, '3d7b4e8c')
    producer = r'''
import cloudpickle, functools, io, json, sys
from pathlib import Path
from types import FunctionType
from demiflow.execution import dataset_native as native
from demiflow.execution.recovery_identity import describe
from demiflow.data.plan import CallableSpec
from demiflow.execution.seal.plan import _code_identity_from_value

out=Path(sys.argv[1]);cloudpickle.register_pickle_by_value(native)
def save(name,value): (out/(name+'.pickle')).write_bytes(cloudpickle.dumps(value))
for name in ('finish_typed_batch','_cast_sink_batch'):
    fn=getattr(native,name)
    save(name,fn)
    save(name+'-references',(fn,fn,functools.partial(fn)))
    clone=FunctionType(fn.__code__,fn.__globals__,fn.__name__)
    clone.__module__=fn.__module__;clone.__qualname__=fn.__qualname__
    save(name+'-two-factories',(fn,clone))
    spec=CallableSpec.create(fn)
    save(name+'-spec',spec)
    (out/(name+'-pin.json')).write_text(json.dumps(_code_identity_from_value('callable',spec)))
    clone.__code__=clone.__code__.replace(co_consts=(*clone.__code__.co_consts,'changed'))
    save(name+'-bad-code',clone)
    for label,key,value in [('defaults','__defaults__',(1,)),('kwdefaults','__kwdefaults__',{'x':1}),
            ('globals','__globals__',{'unexpected':1}),('name','__name__','other'),
            ('qualname','__qualname__','other'),('module','__module__','other'),
            ('annotation','__annotations__',{'x':'int'}),('doc','__doc__','changed')]:
        original=cloudpickle.cloudpickle._function_getstate
        def altered(f):
            attrs,slots=original(f)
            if f is fn: slots[key]=value
            return attrs,slots
        cloudpickle.cloudpickle._function_getstate=altered
        try: save(name+'-bad-'+label,fn)
        finally: cloudpickle.cloudpickle._function_getstate=original
    fn.extra='changed'
    save(name+'-bad-attribute',fn)
    del fn.extra
    factory=cloudpickle.CloudPickler(io.BytesIO())._dynamic_function_reduce(fn)
    class NoState:
        def __reduce__(self): return factory[:2]
    save(name+'-missing-state',NoState())
    for label,index,value in [('factory-name',2,'other'),('factory-defaults',3,(1,)),
                              ('factory-closure',4,((lambda x: lambda: x)(1).__closure__[0],))]:
        args=list(factory[1]);args[index]=value
        class BadFactory:
            def __reduce__(self): return factory[0],tuple(args)
        save(name+'-bad-'+label,BadFactory())

import pyarrow as pa
from demiflow.data.plan import MapOp
def plus(row): return {'id':row['id']+3}
args={'columns':[native.Column('id','c0','p0',pa.int64())],
      'encoded_operations':cloudpickle.dumps((MapOp(CallableSpec.create(plus)),)),
      'schema':pa.schema([('id',pa.int64())])}
batch=pa.record_batch({'c0':[1,2,2],'p0':[True,True,True]})
save('finish-arguments',(batch,args))
(out/'finish-result.json').write_text(json.dumps(native.finish_typed_batch(batch,**args).to_pylist()))

def close_over(n):
    return lambda x: x+n
save('business',(close_over(7),{'unchanged':[1,2]}))
'''
    command = [sys.executable, *(['-'+'O'*sys.flags.optimize] if sys.flags.optimize else []),
               '-B', '-c', producer, str(root)]
    result = subprocess.run(command, cwd=old, env={**os.environ, 'PYTHONPATH':str(old)},
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    return root


def owner(name):
    if name == 'finish_typed_batch':
        from demiflow.operators.arrow_batches import finish_typed_batch
        return finish_typed_batch
    from demiflow.execution.outputs.batch_conversion import _cast_sink_batch
    return _cast_sink_batch


def snapshot(fn):
    # Actual identities, not equality with potentially mutable payload values.
    return (fn.__code__, fn.__defaults__, fn.__kwdefaults__, fn.__closure__, dict(vars(fn)),
            {k:id(v) for k,v in fn.__globals__.items()})


@pytest.mark.parametrize('name', ['finish_typed_batch','_cast_sink_batch'])
def test_original_value_memo_partial_and_seal_keep_same_owner(original_callbacks, name):
    from demiflow.execution.seal.plan import _matches_original_program
    fn=owner(name)
    # Warm only real imports used by validation, then observe no old state writes.
    value=load_payload([(original_callbacks/(name+'.pickle')).read_bytes()])
    assert value is fn
    before=snapshot(fn)
    for suffix in ('-references','-two-factories'):
        raw=(original_callbacks/(name+suffix+'.pickle')).read_bytes()
        digest=hashlib.sha256(raw).hexdigest()
        values=load_payload([raw])
        assert values[0] is values[1] is fn
        if suffix=='-references': assert values[2].func is fn
        assert hashlib.sha256(raw).hexdigest()==digest
        assert snapshot(fn)==before
    spec=load_payload([(original_callbacks/(name+'-spec.pickle')).read_bytes()])
    pin=json.loads((original_callbacks/(name+'-pin.json')).read_text())
    assert _matches_original_program('callable',spec,pin)
    assert snapshot(fn)==before


@pytest.mark.parametrize('name', ['finish_typed_batch','_cast_sink_batch'])
@pytest.mark.parametrize('change', ['code','defaults','kwdefaults','globals','name','qualname','module','annotation','doc','attribute','factory-name','factory-defaults','factory-closure'])
def test_changed_original_callback_state_rejects_without_mutating_owner(original_callbacks,name,change):
    fn=owner(name);before=snapshot(fn)
    with pytest.raises(DataflowError,match='Program payload failed to load'):
        load_payload([(original_callbacks/(name+'-bad-'+change+'.pickle')).read_bytes()])
    assert snapshot(fn)==before


@pytest.mark.parametrize('name', ['finish_typed_batch','_cast_sink_batch'])
def test_missing_setter_cannot_deliver_unverified_owner(original_callbacks,name):
    fn=owner(name);before=snapshot(fn)
    with pytest.raises(DataflowError,match='state was not verified'):
        load_payload([(original_callbacks/(name+'-missing-state.pickle')).read_bytes()])
    assert snapshot(fn)==before


def test_business_closure_and_current_sql_column_identity_are_unchanged(original_callbacks):
    from demiflow.operators.relational_sql import Column
    from demiflow.execution.recovery_identity import describe
    fn,values=load_payload([(original_callbacks/'business.pickle').read_bytes()])
    assert fn(4)==11 and values=={'unchanged':[1,2]}
    assert describe(Column)['module']=='demiflow.operators.relational_sql'
    root=Path(__file__).parents[1]/'demiflow/execution'
    assert all(not (root/name).exists() for name in ('dataset_native.py','local_tasks.py','arrow_batch_sink.py'))


def test_original_by_value_finish_executes_the_original_callback(original_callbacks):
    fn=load_payload([(original_callbacks/'finish_typed_batch.pickle').read_bytes()])
    batch,args=load_payload([(original_callbacks/'finish-arguments.pickle').read_bytes()])
    expected=json.loads((original_callbacks/'finish-result.json').read_text())
    assert fn(batch,**args).to_pylist()==expected==[{'id':4},{'id':5},{'id':5}]


@pytest.mark.parametrize('name', ['finish_typed_batch','_cast_sink_batch'])
def test_modified_current_algorithm_is_not_certified_as_historical(original_callbacks,name,monkeypatch):
    if name=='finish_typed_batch':
        from demiflow.operators import arrow_batches
        monkeypatch.setattr(arrow_batches,'_apply',lambda rows,operations: rows)
    else:
        from demiflow.execution.contracts import record_codec
        monkeypatch.setattr(record_codec.cast_allocation,'__defaults__',(0,))
    fn=owner(name);before=snapshot(fn)
    with pytest.raises(DataflowError,match='owner differs'):
        load_payload([(original_callbacks/(name+'.pickle')).read_bytes()])
    assert snapshot(fn)==before


def test_moduleless_business_callable_keeps_original_public_execution(tmp_path,monkeypatch):
    import cloudpickle
    from demiflow import data
    from demiflow.execution.run_context import RunContext
    fn=eval('lambda row: {"id": row["id"] + 1}', {})
    assert fn.__module__ is None
    restored=load_payload([cloudpickle.dumps(fn)])
    assert restored.__module__ is None and restored({'id':3})=={'id':4}
    monkeypatch.setenv('DEMIFLOW_RESOURCE_ROOT',str(tmp_path/'resources'))
    with RunContext.start(tmp_path/'run'),data.local_execution(workers=1,worker_mode='process'):
        assert data.from_items([{'id':3}]).map(fn).take_all()==[{'id':4}]
