"""Model request reservations and original response/replay projection."""
import json
import contextlib
import uuid
from contextvars import ContextVar

from demiflow.execution.contracts.recovery_contract import RecoveryContractError, identity


CALL_RESERVATION = ContextVar('native_prompt_budget_reservation', default=None)


@contextlib.contextmanager
def native_request_budget(run, *, node_id, max_requests):
    # The reservation is only staged here; RunPromptJournal applies it in the
    # same savepoint as the request intent. None inherits the run ceiling while
    # an explicit 0 forbids fresh dispatch, so the two must not collapse.
    if max_requests is not None and (type(max_requests) is not int or max_requests < 0):
        raise RecoveryContractError('Native request budget ceiling must be a non-negative integer or None')
    run_ceiling = run.limits['max_requests']
    node_ceiling = run_ceiling if max_requests is None else max_requests
    reservation = 'operator-llm-' + uuid.uuid4().hex
    charges = {'requests': (1, run_ceiling),
               'requests-' + identity(node_id): (1, node_ceiling)}
    run.check_stop()
    token = CALL_RESERVATION.set((reservation, charges))
    try:
        yield reservation
    finally:
        CALL_RESERVATION.reset(token)


def _stable_metadata(value):
    """JSON journal key ordering must not change an inferred Arrow struct."""
    if type(value) is dict:
        return {key: _stable_metadata(value[key]) for key in sorted(value)}
    if type(value) is list:
        return [_stable_metadata(item) for item in value]
    return value


def native_delivery_trace(trace):
    """Project native replay flags without changing provider or user metadata."""
    def original(call):
        return {**call, 'reused': False} if 'reused' in call else dict(call)
    result = original(trace)
    if 'attempts' in trace:
        result['attempts'] = [original(call) for call in trace['attempts']]
    return result

def native_response(response, *, provider):
    """Keep original image-reference and metadata representation after decoding."""
    from dataclasses import replace
    content = response.content
    if provider and type(content) is dict and content.get('image') is not None:
        from demiflow.objects import ObjectRef
        content = {**content, 'image': ObjectRef(**content['image']).to_dict()}
    return replace(response, content=content, metadata=_stable_metadata(response.metadata))
