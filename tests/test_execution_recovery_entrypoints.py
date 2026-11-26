"""Platform entry scopes and the formal sink SPI, without business adapters."""
import json
from pathlib import Path

import pytest
from demiflow import data
from demiflow.data.datasink import Datasink
from demiflow.execution import RunContext, RecoveryContractError, UnresolvedCommit, current_run
from demiflow.execution.pipeline.run_context import current_action
import demiflow
from pathlib import Path

# This fixture is also imported by the independently archived historical writer.
if (Path(demiflow.__file__).parent / 'execution/outputs/block_sink.py').is_file():
    from demiflow.execution.outputs.block_sink import BlockSink
else:
    from demiflow.execution.block_sink import BlockSink
from demiflow.execution.journal.run_journal import RunJournal
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.pipeline.core import Pipeline, PipelineExecution


class FormalSink(Datasink):
    sink_id = 'output'
    def recovery_identity(self):
        return {'fixture': 'formal-sink-v1'}
    def prepare(self, blocks, context):
        self.native = BlockSink(current_action())
        return self.native.prepare((row for block in blocks for row in block.to_pylist()), context)
    def _sink(self):
        return BlockSink(current_action())
    def _receipt(self, receipt):
        return {**receipt, 'data': {'written_rows': receipt['data']['rows'], 'target': 'fixture'}}
    def inspect(self, prepared, context):
        receipt = self._sink().inspect(prepared, context)
        return None if receipt is None else self._receipt(receipt)
    def commit(self, prepared, context):
        return self._receipt(self._sink().commit(prepared, context))
    def abort(self, prepared, context):
        self._sink().abort(prepared, context)


def test_formal_sink_reuses_receipt_without_legacy_callbacks_or_recomputation(tmp_path):
    calls=tmp_path/'source-calls'
    def observed(row):
        with calls.open('a') as stream: stream.write('row\n')
        return row
    def source(): return data.from_items([{'x':1},{'x':1}]).map(observed)
    with pytest.raises(RuntimeError, match='later'):
        with RunContext.start(tmp_path / 'run') as run:
            reference = run.ref
            assert source().write_datasink(FormalSink()) is None
            raise RuntimeError('later')
    class NeverPrepare(FormalSink):
        def prepare(self, blocks, context):
            pytest.fail('a committed native sink must not reconsume its input')
    with RunContext.resume(reference):
        source().write_datasink(NeverPrepare())
    fact = RunJournal(reference.directory).execution_commits('action-1')[0]
    assert fact['status'] == 'acked'
    receipt=fact['receipts']['write_datasink']['data']['native_receipt']
    assert receipt['sink_id']=='output' and receipt['commit_id']==fact['commit_id']
    assert receipt['status']=='committed' and receipt['data']['written_rows']==2
    assert calls.read_text().splitlines()==['row','row']


def test_lying_commit_without_inspect_proof_cannot_advance(tmp_path):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    calls=tmp_path/'commits'; inputs=tmp_path/'inputs'
    def observed(row):
        with inputs.open('a') as stream: stream.write('row\n')
        return row
    class LyingSink(FormalSink):
        def commit(self, prepared, context):
            with calls.open('a') as stream: stream.write('commit\n')
            return {'commit_id': context.commit_id, 'sink_id': 'output', 'status': 'committed',
                    'data': {'written_rows': 1}}
    def execute(): data.from_items([{'x':1}]).map(observed).write_datasink(LyingSink())
    with pytest.raises(UnresolvedCommit, match='inspect evidence') as first:
        with RunContext.start(tmp_path / 'run') as run:
            reference=run.ref
            execute()
    assert isinstance(first.value,DataflowError)
    assert first.value.code=='UNKNOWN_EFFECT' and not first.value.retryable
    fact = RunJournal(tmp_path / 'run').execution_commits('action-1')[0]
    assert fact['status'] == 'prepared' and not fact['receipts']
    with pytest.raises(UnresolvedCommit,match='inspect evidence') as resumed:
        with RunContext.resume(reference): execute()
    assert isinstance(resumed.value,DataflowError)
    assert resumed.value.code=='UNKNOWN_EFFECT' and not resumed.value.retryable
    assert RunJournal(tmp_path/'run').execution_commits('action-1')[0]==fact
    assert calls.read_text().splitlines()==['commit']
    assert inputs.read_text().splitlines()==['row']


def test_pipeline_reuses_launcher_scope_and_bad_return_cannot_succeed(tmp_path):
    executor = LocalDatasetExecutor()
    class Program:
        def run(self, ctx):
            assert current_run().ref == reference
            assert data.range(3).materialize().count() == 3
            return 'invalid program return'
    pipeline = Pipeline('fixture', 'fixture', Program(), PipelineExecution('portable'), resource_root=tmp_path)
    try:
        with pytest.raises(TypeError, match='must return None'):
            with RunContext.start(tmp_path / 'run') as run:
                reference = run.ref
                pipeline.run(dataset_executor=executor)
        assert RunJournal(reference.directory).execution_status()['status'] == 'failed'
    finally:
        executor.close()


def test_local_session_and_plain_python_use_same_context(tmp_path):
    with RunContext.start(tmp_path / 'run') as run:
        with data.local_execution(workers=1, worker_mode='thread') as session:
            assert session.run_ref == run.ref
            assert data.range(3).materialize().take_all() == [{'id': 0}, {'id': 1}, {'id': 2}]
    assert RunJournal(tmp_path / 'run').execution_status()['status'] == 'succeeded'


def test_checker_rejects_the_same_retired_checkpoint_before_execution(tmp_path):
    from demiflow.forge.platform import inspect_modules, LIMITS
    from demiflow.forge.flow_check import FlowChecker
    from test_forge_checkpoint import inspect as inspect_checkpoint
    catalog = inspect_modules(['demiflow.data'], Path(data.__file__).resolve().parents[1], LIMITS)
    _, report, arguments = inspect_checkpoint(tmp_path, catalog)
    assert report['status'] == 'invalid'
    assert any('retired' in str(item) for item in report['diagnostics'])
    assert not Path(arguments['stage']).exists()
    assert not Path(arguments['target']).exists()


def test_plain_python_stream_admission_precedes_source_and_actor_calls(tmp_path):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    calls=tmp_path/'worker-calls'
    def source():
        with calls.open('a') as stream: stream.write('source\n')
        yield {'id': 1}
    class Convert:
        def __init__(self):
            with calls.open('a') as stream: stream.write('construct\n')
        async def __call__(self,row):
            with calls.open('a') as stream: stream.write('actor\n')
            return row
    with pytest.raises(DataflowError,match='RESOURCE_INFEASIBLE'):
        # The single-pass source, one call of the actor (3 rows and its state)
        # and the terminal must run together; this run cannot hold them.
        with RunContext.start(tmp_path/'run',limits={'memory_bytes':32*1024,
                'max_batch_bytes':8192,'max_row_bytes':4096,'max_state_bytes':8192}):
            data.from_iter(source).map_async(Convert,concurrency=8,queue_depth=1).materialize()
    assert not calls.exists()
    assert RunJournal(tmp_path/'run').execution_commits('action-1')==[]
    assert not list((tmp_path / 'run').rglob('*.pkl'))


def test_byte_limit_refuses_oversize_input_before_any_callback(tmp_path):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    calls = tmp_path / 'calls'

    def seen(row):
        calls.write_text('called')
        return row

    with pytest.raises(DataflowError, match='BUDGET_EXCEEDED'):
        with RunContext.start(tmp_path / 'run', limits={'max_row_bytes': 64, 'max_batch_bytes': 1024}):
            data.from_items([{'value': 'x' * 256}]).map(seen).take_all()
    assert not calls.exists()
