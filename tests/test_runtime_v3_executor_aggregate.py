"""Actual executor compatibility preserves business aggregate names on both hosts."""
import pytest

from demiflow.data.aggregate import Count, Sum
from demiflow.execution.run_context import RunContext
from _runtime_v3_executor_scope import call
from test_runtime_v3_chain_compatibility import _entry


@pytest.mark.parametrize('items,expected', [
    ([{'id': 1}, {'id': 2}], {'sum(id)': 3, 'count()': 2, 'total id (sum)': 3}),
    ([], {'sum(id)': None, 'count()': 0, 'total id (sum)': None}),
])
def test_real_executor_aggregate_preserves_names_and_original_commit(tmp_path, items, expected):
    with RunContext.start(tmp_path / 'run', limits={'memory_bytes': 32 * 2**20}) as run:
        with _entry(tmp_path) as (api, executor, _):
            dataset = api.from_items(items)
            aggregates = (Sum('id'), Count(), Sum('id', alias_name='total id (sum)'))
            result = call(executor, 'aggregate', dataset._source, dataset._plan, aggregates)
            assert result == expected
            assert run.position == 1
            assert run.journal.execution_action('action-2') is None
            commits = run.journal.execution_commits('action-1')
            assert commits and all(fact['status'] == 'acked' for fact in commits)
