"""Platform entry scopes and the formal sink SPI, without business adapters."""
import json
from pathlib import Path

import pytest
from demiflow import data
from demiflow.data.datasink import Datasink
from demiflow.execution import RunContext, RecoveryContractError, UnresolvedCommit, current_run
from demiflow.execution.run_context import current_action
from demiflow.execution.block_sink import BlockSink
from demiflow.execution.run_journal import RunJournal
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
    with pytest.raises(RuntimeError, match='later'):
        with RunContext.start(tmp_path / 'run') as run:
            reference = run.ref
            assert data.from_items([{'x': 1}, {'x': 1}]).write_datasink(FormalSink()) is None
            raise RuntimeError('later')
    class NeverPrepare(FormalSink):
        def prepare(self, blocks, context):
            pytest.fail('a committed native sink must not reconsume its input')
    with RunContext.resume(reference):
        data.from_items([{'x': 1}, {'x': 1}]).write_datasink(NeverPrepare())
    fact = RunJournal(reference.directory).execution_commits('action-1')[0]
    assert fact['status'] == 'acked'
    assert fact['receipts']['output']['data']['written_rows'] == 2


def test_lying_commit_without_inspect_proof_cannot_advance(tmp_path):
    class LyingSink(FormalSink):
        def commit(self, prepared, context):
            return {'commit_id': context.commit_id, 'sink_id': 'output', 'status': 'committed',
                    'data': {'written_rows': 1}}
    with pytest.raises(UnresolvedCommit, match='inspect evidence'):
        with RunContext.start(tmp_path / 'run'):
            data.from_items([{'x': 1}]).write_datasink(LyingSink())
    fact = RunJournal(tmp_path / 'run').execution_commits('action-1')[0]
    assert fact['status'] == 'prepared' and not fact['receipts']


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
    calls = []
    def source():
        calls.append('source')
        yield {'id': 1}
    async def convert(row):
        calls.append('actor')
        return row
    with pytest.raises(RecoveryContractError, match='admission limit'):
        with RunContext.start(tmp_path / 'run'):
            data.from_iter(source).map_async(convert, concurrency=65).materialize()
    assert calls == []
    assert not list((tmp_path / 'run').rglob('*.pkl'))


def test_native_queue_byte_budget_applies_without_checked_context(tmp_path):
    import asyncio
    from demiflow.execution.stream_resources import StreamResources
    with RunContext.start(tmp_path / 'run', limits={'max_row_bytes': 64, 'max_batch_bytes': 1024}) as run:
        queue = StreamResources([]).make_queue(2)
        assert queue.max_bytes == 128
        with pytest.raises(Exception, match='byte budget|bytes|size'):
            asyncio.run(queue.put({'value': 'x' * 256}))
        assert queue.qsize() == 0
