"""Lance request and budget failures reach the caller of an action with their own type."""
import json

import pytest

from demiflow.errors import InvalidLanceRequest
from demiflow.execution.contracts.dataflow_failures import failure_payload, restore_failure


@pytest.mark.parametrize('kind', [InvalidLanceRequest, MemoryError])
def test_lance_failure_type_and_message_survive_the_worker_boundary(kind):
    try:
        raise kind('query exceeds max_query_dimensions')
    except kind as error:
        payload = json.loads(json.dumps(failure_payload(error)))
    restored = restore_failure(payload)
    assert type(restored) is kind and str(restored) == 'query exceeds max_query_dimensions'
