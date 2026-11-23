"""The eleven qualified rules, actual compiler boundaries, and original replay owners."""
from dataclasses import replace, make_dataclass
import json
import logging
import os
from pathlib import Path
import subprocess
import sys

import pytest

from demiflow import data
from demiflow.execution.contracts.dataflow import ActionRequest
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.planning import dataflow_compile as compiler, dataflow_configuration as schema
from configuration_schema_gate import reports_on_purpose
import compile_contract_corpus as corpus


def identity(row):
    return row


def request():
    ds = data.from_items([{'id': 1}]).map(identity)
    return ActionRequest(kind='take', source=ds._source, plan=ds._plan, arguments={'limit': 1},
                         sink=None, programs=(), action=type('Action', (), {'action_id': 'private-action'})(), provider='local')


def change(nodes, kind, values):
    return [replace(n, kernel=replace(n.kernel, configuration={**compiler._plain(n.kernel.configuration), **values}))
            if n.kernel.kind == kind else n for n in nodes]


def corrupt_capture(monkeypatch, kind='MapOp', values=None):
    original = compiler._Capture.capture_terminal
    def terminal(capture, producer):
        result = original(capture, producer)
        capture.nodes[:] = change(capture.nodes, kind, values or {'task_origin': {}})
        return result
    monkeypatch.setattr(compiler._Capture, 'capture_terminal', terminal)


CASES = [('occurrence_binding', 'MapOp', 'occurrence_id', 'f'*64),
         ('program_binding', 'MapOp', 'program', 'f'*64), ('terminal_kind', 'take', 'kind', 'wrong')]
CASES += [('compiler_key_shape', 'take' if k in ('terminal_memory','publication_workspace') else 'MapOp', k, {})
          for k in ('actor_binding_version','occurrence_id','program','publication_workspace',
                    'task_batch_rows','task_group','task_origin','terminal_memory')]


@reports_on_purpose
@pytest.mark.parametrize('code,kind,key,value', CASES)
def test_exact_qualified_rule_rejects_real_capture_before_callback(monkeypatch, code, kind, key, value):
    corrupt_capture(monkeypatch, kind, {key: value})
    callback = []
    with pytest.raises(DataflowError, match=code) as error:
        compiler.lower_action(request(), bounds=corpus.bounds(), on_compiled=callback.append)
    assert error.value.code == 'INVALID_CONTRACT' and error.value.retryable is False
    assert error.value.context['stage'] == 'capture' and error.value.context['finding_count'] == 1
    assert callback == []


@reports_on_purpose
@pytest.mark.parametrize('stage', schema.STAGES)
def test_each_mature_stage_rejects_before_the_next_phase(monkeypatch, stage):
    if stage == 'capture':
        corrupt_capture(monkeypatch)
    elif stage == 'graph':
        from demiflow.planning import dataflow_graph_adapter as adapter
        original = adapter.decorate_graph_capture
        def decorate(capture, *args, **kwargs):
            result = original(capture, *args, **kwargs)
            capture.nodes[:] = change(capture.nodes, 'MapOp', {'task_origin': {}})
            return result
        monkeypatch.setattr(adapter, 'decorate_graph_capture', decorate)
    elif stage == 'budget':
        original = compiler.declare_codec_windows
        def fit(nodes, *args, **kwargs):
            result = original(nodes, *args, **kwargs)
            nodes[:] = change(nodes, 'MapOp', {'task_origin': {}})
            return result
        monkeypatch.setattr(compiler, 'declare_codec_windows', fit)
    else:
        from demiflow.planning import dataflow_shared as shared
        original = shared.bind_shared_actors
        def bind(*args, **kwargs):
            return tuple(change(original(*args, **kwargs), 'MapOp', {'task_origin': {}}))
        monkeypatch.setattr(shared, 'bind_shared_actors', bind)
    with pytest.raises(DataflowError) as error:
        compiler.lower_action(request(), bounds=corpus.bounds(), on_compiled=lambda _: pytest.fail('callback ran'))
    assert error.value.context['stage'] == stage


@reports_on_purpose
def test_unqualified_shapes_unknown_and_task_semantics_still_report(monkeypatch):
    corrupt_capture(monkeypatch, values={'native_operation': {}, 'native_key': 'x',
        'default_batch_size': 1, 'unregistered-private-key': 'private value', '_configuration_purpose': 'history'})
    compiled = compiler.lower_action(request(), bounds=corpus.bounds())
    codes = {f.code for f in schema._graph_findings(compiled.graph)}
    assert {'compiler_key_shape','unknown_key'} <= codes
    # A missing task key is a semantic finding, not an approved shape refusal.
    with monkeypatch.context() as patch:
        original = compiler._Capture.capture_terminal
        def omit(capture, producer):
            result = original(capture, producer)
            capture.nodes[:] = [replace(n, kernel=replace(n.kernel, configuration={k:v for k,v in compiler._plain(n.kernel.configuration).items() if k!='task_group'})) if n.kernel.kind=='MapOp' else n for n in capture.nodes]
            return result
        patch.setattr(compiler._Capture, 'capture_terminal', omit)
        result = compiler.lower_action(request(), bounds=corpus.bounds())
        assert any(f.code=='task_complete' for f in schema._graph_findings(result.graph))


@reports_on_purpose
def test_typed_field_shape_priority_is_preserved(monkeypatch):
    from demiflow.data import plan
    declared = make_dataclass('ConfigurationDeclaredFixture', [('task_origin', object)], bases=(plan.LogicalOp,))
    monkeypatch.setattr(plan, 'ConfigurationDeclaredFixture', declared, raising=False)
    graph = compiler.lower_action(request(), bounds=corpus.bounds()).graph
    nodes = [replace(n, kernel=replace(n.kernel, kind=declared.__name__, configuration={
        'occurrence_id':n.occurrence_id, 'program':n.occurrence_id, 'task_origin': {}}))
        if n.kernel.kind=='MapOp' else n for n in graph.nodes]
    schema.report_configuration_stage(nodes, ((e.producer,e.consumer) for e in graph.edges),
        action_id=graph.action_id, stage='final', edge_count=len(graph.edges))
    assert not any(f.code=='compiler_key_shape' for f in schema._scan(nodes, (), stage='final'))


@pytest.mark.parametrize('purpose', ['new','probe','history_revalidation', {}, None])
def test_purpose_cannot_be_supplied_as_persistent_values(purpose):
    with pytest.raises(DataflowError, match='Unknown internal'):
        compiler.lower_action(None, bounds=None, _configuration_purpose=purpose)


@reports_on_purpose
def test_partial_checker_failure_keeps_qualified_rejection_and_bounded_diagnostics(monkeypatch):
    original = schema._node_findings
    def failing(node, by_occurrence, *, stage):
        yield from original(node, by_occurrence, stage=stage)
        if node.kernel.kind == 'MapOp':
            for _ in range(30):
                yield schema._finding(node,'program_binding','program does not name this node',stage=stage,key='program')
            raise RuntimeError('secret exception body')
    monkeypatch.setattr(schema, '_node_findings', failing)
    with pytest.raises(DataflowError) as error:
        compiler.lower_action(request(), bounds=corpus.bounds())
    assert len(error.value._message) <= 1024 and len(error.value.context['locations']) == 4
    assert error.value.context['finding_count'] == 30
    assert error.value.context['inspection_completed'] is False
    assert 'secret exception body' not in str(error.value)


@reports_on_purpose
def test_checker_error_alone_does_not_add_a_production_refusal(monkeypatch):
    original = schema._node_findings
    def broken(node, *args, **kwargs):
        if node.kernel.kind=='MapOp':
            raise RuntimeError('private checker message')
        yield from original(node,*args,**kwargs)
    monkeypatch.setattr(schema, '_node_findings', broken)
    assert compiler.lower_action(request(), bounds=corpus.bounds()).graph.nodes


@reports_on_purpose
def test_actual_new_execution_rejects_before_staging_source_seal_worker_or_sink(tmp_path, monkeypatch):
    from demiflow.execution.action_driver import dataflow as engine
    import pyarrow as pa
    touched=[]
    def forbidden(name):
        def call(*args,**kwargs):
            touched.append(name)
            raise AssertionError(name+' reached after rejected compilation')
        return call
    for name in ('stage_dataflow_programs','snapshot_source','seal_dataflow','stage_runtime_bundle'):
        monkeypatch.setattr(engine,name,forbidden(name))
    monkeypatch.setattr(engine.DataflowExecution,'_prepare_launch',forbidden('worker'))
    def source():
        touched.append('source_read')
        yield {'id':1}
    corrupt_capture(monkeypatch)
    target=tmp_path/'never.lance'
    with pytest.raises(DataflowError,match='task_origin'):
        with RunContext.start(tmp_path/'run', limits={'max_row_bytes':1024**2,'max_batch_bytes':1024**2,'max_state_bytes':1024**2}) as run, data.local_execution(worker_mode='thread',workers=2):
            data.from_iter(source).map(identity).write_lance(str(target),schema=pa.schema([('id',pa.int64())]))
    assert touched==[] and not target.exists()
    assert not run.journal.execution_commits('action-1')


@reports_on_purpose
def test_real_literal_schema_replay_reports_new_bad_shape_but_preserves_old_schema_check(tmp_path,monkeypatch):
    import pyarrow as pa
    from demiflow.execution.seal.sealed_plans import load_sealed_plan
    original=pa.schema([('id',pa.int32())]);changed=pa.schema([('id',pa.large_string())])
    def pipeline(types):
        return data.from_arrow(pa.Table.from_batches([],schema=types)).map(identity).take_all()
    class Finished(Exception):pass
    with pytest.raises(Finished):
        with RunContext.start(tmp_path/'run') as run,data.local_execution(worker_mode='thread',workers=2):
            ref=run.ref
            assert pipeline(original)==[]
            commits=run.journal.execution_commits('action-1')
            raise Finished()
    sealed=load_sealed_plan(ref.directory,'action-1')['graph']
    purposes=[];report=schema.report_configuration_stage
    def observed(*args,**kwargs):
        purposes.append(kwargs['_purpose'])
        return report(*args,**kwargs)
    monkeypatch.setattr(schema,'report_configuration_stage',observed)
    corrupt_capture(monkeypatch)
    with pytest.raises(DataflowError) as failure:
        with RunContext.resume(ref),data.local_execution(worker_mode='thread',workers=2):pipeline(changed)
    assert failure.value.code=='SOURCE_CHANGED'
    with RunContext.resume(ref) as resumed,data.local_execution(worker_mode='thread',workers=2):
        assert pipeline(original)==[]
        assert resumed.journal.execution_commits('action-1')==commits
    assert schema._HISTORY_REVALIDATION in purposes and schema._NEW not in purposes
    assert load_sealed_plan(ref.directory,'action-1')['graph']==sealed


@reports_on_purpose
def test_real_completed_live_revalidation_warns_without_reexecuting_callback(tmp_path,monkeypatch):
    from demiflow.execution.seal.sealed_plans import load_sealed_plan
    seen=[]
    def pipeline(observations):
        def callback(row):
            observations.append(row['id'])
            return row
        return data.from_items([{'id':1}]).map(callback,execution='thread',concurrency=1).take_all()
    class Finished(Exception):pass
    with pytest.raises(Finished):
        with RunContext.start(tmp_path/'run') as run,data.local_execution(worker_mode='thread',workers=2):
            ref=run.ref
            assert pipeline(seen)==[{'id':1}]
            commits=run.journal.execution_commits('action-1')
            raise Finished()
    sealed=load_sealed_plan(ref.directory,'action-1')['graph']
    assert any(n.kernel.configuration.get('thread_attempt') for n in sealed.nodes)
    purposes=[];report=schema.report_configuration_stage
    def observed(*args,**kwargs):
        purposes.append(kwargs['_purpose'])
        return report(*args,**kwargs)
    monkeypatch.setattr(schema,'report_configuration_stage',observed)
    corrupt_capture(monkeypatch)
    replay_seen=[]
    with RunContext.resume(ref) as resumed,data.local_execution(worker_mode='thread',workers=2):
        assert pipeline(replay_seen)==[{'id':1}]
        assert resumed.journal.execution_commits('action-1')==commits
    assert seen==[1] and replay_seen==[] and schema._HISTORY_REVALIDATION in purposes and schema._NEW not in purposes
    assert load_sealed_plan(ref.directory,'action-1')['graph']==sealed


@reports_on_purpose
def test_public_capture_rejects_on_first_lowering(monkeypatch):
    from demiflow.execution.plan_capture import capture_pipeline
    purposes=[]
    original=schema.report_configuration_stage
    def observed(*args, **kwargs):
        purposes.append(kwargs['_purpose'])
        return original(*args, **kwargs)
    monkeypatch.setattr(schema, 'report_configuration_stage', observed)
    corrupt_capture(monkeypatch)
    with pytest.raises(DataflowError, match='task_origin'):
        capture_pipeline(lambda: data.from_items([{'id':1}]).map(identity).take_all(), {},
                         on_compiled=lambda _:pytest.fail('callback ran'))
    assert purposes == [schema._NEW]


@reports_on_purpose
def test_actual_compiler_child_rejects_qualified_shape_and_completes_receipt(tmp_path):
    target=tmp_path/'child.json'
    process=subprocess.Popen([sys.executable,'-B',__file__,'--child',str(target)],
        cwd=Path(__file__).resolve().parents[1], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    stdout,stderr=process.communicate(timeout=30)
    assert process.returncode==0,stderr
    receipt=json.loads(target.read_text())
    assert receipt['pid']==process.pid and receipt['pid']!=os.getpid()
    assert receipt['completed'] is True and receipt['ok'] is False
    assert receipt['problems']==['schema_findings']
    case=receipt['cases']['qualified_shape']
    assert case['outcome']=='DataflowError:INVALID_CONTRACT'
    assert case['stages']=={'capture':1,'graph':0,'budget':0,'final':0}
    assert case['codes']=={'compiler_key_shape':1} and case['findings']==1 and case['check_errors']==0
    assert json.loads(stdout)=={'code':'INVALID_CONTRACT','stage':'capture'}


if __name__=='__main__':
    from configuration_schema_evidence import ProcessEvidence, source_binding
    observer=ProcessEvidence(source_binding(Path(__file__).resolve().parents[1]),
                             {'qualified_shape':'DataflowError:INVALID_CONTRACT'})
    with observer, observer.case('qualified_shape') as case, pytest.MonkeyPatch.context() as patch:
        corrupt_capture(patch)
        try:compiler.lower_action(request(),bounds=corpus.bounds())
        except DataflowError as exc:
            case['outcome']='DataflowError:'+exc.code
            print(json.dumps({'code':exc.code,'stage':exc.context['stage']}))
        else:raise AssertionError('bad new graph accepted in child')
    observer.write(sys.argv[2])
