"""The search operator's private checkpoint is retired; run recovery is the one protocol."""
import pytest

from demiflow.collect.operators import SearchWeb
from demiflow.execution.contracts.recovery_contract import RecoveryContractError


class _Session:
    async def search(self, query, **kwargs):
        pytest.fail('a retired checkpoint option must reject before any search')


@pytest.mark.parametrize('options', [
    {'checkpoint': 'operator.sqlite'}, {'checkpoint_operator': 'private'}, {'checkpoint_lease_s': 1.0}])
def test_search_web_rejects_retired_checkpoint_options(tmp_path, options):
    if 'checkpoint' in options:
        options = {'checkpoint': tmp_path / options['checkpoint']}
    with pytest.raises(RecoveryContractError, match='Explicit search checkpoint orchestration is retired'):
        SearchWeb('requests', 'results', _Session(), None, 5, 2, **options)
    assert not list(tmp_path.iterdir())


def test_search_web_without_checkpoint_options_is_accepted():
    actor = SearchWeb('requests', 'results', _Session(), None, 5, 2)
    assert (actor.requests, actor.output, actor.max_candidates, actor.request_concurrency) == ('requests', 'results', 5, 2)
