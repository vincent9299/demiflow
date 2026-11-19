"""Run explicitly with env-ray: real Ray Core actors, no Ray Data."""
import pytest
import tempfile

from demiflow import data
from demiflow.data.dataset import Dataset
from demiflow.execution.executors.ray import RayDatasetExecutor
from demiflow.execution.run_context import RunContext
from demiflow.operator_llm.parser import parse_prompt_pack
from test_runtime_v3_native_execution import PACK, native_server


def test_ray_prompt_and_relational_chain(tmp_path, native_server, monkeypatch):
    ray = pytest.importorskip('ray')
    import ray.data
    def no_ray_data(*args, **kwargs):
        raise AssertionError('Unified execution must not call Ray Data')
    for name in ('from_items', 'from_arrow', 'read_datasource'):
        monkeypatch.setattr(ray.data, name, no_ray_data)
    try:
        ray.init(num_cpus=6, include_dashboard=False, object_store_memory=100 * 1024**2,
                 _temp_dir=tempfile.mkdtemp(prefix='demiflow-ray-', dir='/tmp'))
        with RunContext.start(tmp_path / 'run') as run:
            source = data.from_items([{'item': 'b'}, {'item': 'a'}])
            ds = Dataset(source._source, source._plan, RayDatasetExecutor())
            result = ds.map_prompt_async('enrich', config=parse_prompt_pack(PACK),
                inputs={'payload': 'item'}, output='answer', concurrency=1, max_requests=2,
                options={'max_response_bytes': 65536, 'max_event_bytes': 65536}).sort('item').take_all()
            assert result == [{'item': 'a', 'answer': 'ok'}, {'item': 'b', 'answer': 'ok'}]
            assert len(native_server) == 2
            assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
            with run.journal._connection() as db:
                assert db.execute("SELECT consumed FROM runtime_budgets WHERE name='requests'").fetchone()[0] == 2
    finally:
        ray.shutdown()


def test_ray_native_producer_fetch_settlement_and_history(tmp_path, monkeypatch):
    ray = pytest.importorskip('ray')
    import ray.data
    from demiflow.execution.queue import QueueChannel, QueueConfig, ProducerSourceSpec, task_message
    from demiflow.execution.queue_sinks import QueueResultSink
    from demiflow.collect import FetchPolicy, HttpSource
    from demiflow.objects import LocalObjectStore
    from test_raw_fetch import origin_server

    def forbidden(*args, **kwargs):
        raise AssertionError('Ray Data is not the execution engine')
    for name in ('from_items', 'from_arrow', 'read_datasource'):
        monkeypatch.setattr(ray.data, name, forbidden)
    def hosted(ds):
        return Dataset(ds._source, ds._plan, RayDatasetExecutor())

    channel = QueueChannel(QueueConfig(backend='sqlite', queue_id='ray-proof', path=str(tmp_path/'queue.sqlite')))
    producer = channel.open('p', producer_id='ray-producer', source=ProducerSourceSpec(
        kind='records', source_id='fixed-input', uri='memory:records', version=1))
    try:
        ray.init(num_cpus=8, include_dashboard=False, object_store_memory=100 * 1024**2,
                 _temp_dir=tempfile.mkdtemp(prefix='demiflow-ray-', dir='/tmp'))
        with origin_server() as (url, calls):
            with RunContext.start(tmp_path/'producer'):
                rows = [{'messages': [task_message(str(i), {'request': {'uri': url+'/file'}}, pool='p')]}
                        for i in range(2)]
                stats = hosted(data.from_items(rows)).enqueue(channel.config, producer=producer).run_stream()
                assert stats.emitted == 2
            assert channel.seal(producer)['actual_tasks'] == 2
            with RunContext.start(tmp_path/'consumer'):
                ds = hosted(data.read_queue(channel.config, pool='p', max_tasks=2, max_inflight=1,
                                            idle_timeout_s=1)).fetch(store=LocalObjectStore(tmp_path/'objects'),
                    policy=FetchPolicy(max_object_bytes=128*1024, http=HttpSource(allow_private=True)), concurrency=1)
                ds.write_datasink(QueueResultSink(channel.config, pool='p'))
                assert ds.execution_metadata().diagnostics['queue']['settled'] == 2
            assert channel.state('p')['queue_drained'] and calls['/file'] == 2
            historical = hosted(data.read_queue_records(channel.config, pool='p',
                through=channel.snapshot('p'), results=True)).take_all()
            assert len(historical) == 2
            assert all(row['verification']['status'] == 'verified' for row in historical)
    finally:
        ray.shutdown()
