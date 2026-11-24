"""Metadata peek never executes a lazy Dataset; real fetching uses native execution."""
from pathlib import Path
from types import SimpleNamespace

import pytest

from demiflow import data
from demiflow.data.dataset import Dataset
from demiflow.data.plan import LogicalPlan
from demiflow.data.sources import ItemsSource
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.operator_llm.parser import parse_prompt_pack
from test_runtime_v3_native_execution import PACK, native_server


def _record_read(row, *, path):
    with Path(path).open('a') as stream:
        stream.write('read\n')
    return row


@pytest.mark.parametrize('method', ['schema', 'columns'])
def test_metadata_peek_does_not_read_call_model_or_commit(tmp_path, native_server, method):
    marker = tmp_path / 'read-marker'
    with RunContext.start(tmp_path / 'run') as run, data.local_execution():
        dataset = data.from_items([{'item': 'a'}]).map(_record_read,
            fn_kwargs={'path': str(marker)}).map_prompt_async('enrich',
            config=parse_prompt_pack(PACK), inputs={'payload': 'item'}, output='answer',
            concurrency=1, max_requests=1,
            request_policy={'initial_concurrency': 1, 'max_concurrency': 1})
        result = getattr(dataset, method)(fetch_if_missing=False)
        with run.journal._connection() as db:
            actions = db.execute('SELECT count(*) FROM runtime_actions').fetchone()[0]
            commits = db.execute('SELECT count(*) FROM runtime_commits').fetchone()[0]
        assert (result is None, marker.exists(), len(native_server), actions, commits) == (
            True, False, 0, 0, 0)
        # The same declaration is executable. True performs exactly one native
        # request and produces the real schema/columns through its original ACK.
        fetched = getattr(dataset, method)(fetch_if_missing=True)
        assert set(fetched.names if method == 'schema' else fetched) == {'item', 'answer'}
        assert marker.read_text() == 'read\n' and len(native_server) == 1
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'


@pytest.mark.parametrize('method', ['schema', 'columns'])
def test_metadata_peek_preserves_existing_executor_metadata(method):
    # Interface-level evidence: an executor's existing known metadata is not
    # replaced with an unconditional None or an inferred schema cache.
    import pyarrow as pa
    schema = pa.schema([('value', pa.int64())])
    expected = schema if method == 'schema' else ['value']
    calls = []
    def known(source, plan, *, fetch_if_missing):
        assert fetch_if_missing is False
        calls.append((source, plan))
        return expected
    executor = SimpleNamespace(**{method: known})
    dataset = Dataset(ItemsSource(()), LogicalPlan(), executor)
    assert getattr(dataset, method)(fetch_if_missing=False) is expected
    assert calls == [(dataset._source, dataset._plan)]
