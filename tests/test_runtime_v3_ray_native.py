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
                 _temp_dir=tempfile.mkdtemp(prefix='demiflow-ray-'))
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
