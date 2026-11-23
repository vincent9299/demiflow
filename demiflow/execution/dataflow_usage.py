"""Observed worker prompt usage, separate from authoritative request budgets."""
from ..operator_llm.model import OperatorLLMUsage
from .contracts.dataflow_errors import DataflowError


def empty_usage():
    return OperatorLLMUsage().to_dict()


def worker_prompt_usage(kernel, *, final=False):
    from ..operator_llm.runtime import PromptActor
    from ..operator_llm.routed import RoutedPromptActor
    from ..data.result_cache import CacheCall
    if getattr(kernel, '_closed', False) and not final:
        return getattr(kernel, '_prompt_usage', None)
    totals, observed = empty_usage(), False
    for instance in getattr(kernel, '_instances', ()):
        owner = kernel._state_owner(instance)
        if isinstance(owner, CacheCall):
            owner = owner.fn
        owners = owner.actors.values() if isinstance(owner, RoutedPromptActor) else (owner,)
        for child in owners:
            if not isinstance(child, PromptActor):
                continue
            observed = True
            for key, value in child._runtime.coordinator.usage().to_dict().items():
                totals[key] += value
    if observed:
        kernel._prompt_usage = totals
        return totals
    return None


def observe_prompt_usage(execution, event, payload):
    usage = payload.get('prompt_usage')
    if usage is None:
        return
    fields = empty_usage()
    if set(usage) != set(fields) or any(type(v) is not int or v < 0 for v in usage.values()):
        raise DataflowError('INVALID_CONTRACT', 'Prompt usage must contain nonnegative integer counters')
    slots = getattr(execution, '_prompt_usage_slots', None)
    if slots is None:
        execution._prompt_usage_slots = slots = {}
        execution._observed_prompt_usage = empty_usage()
    key = (payload['occurrence_id'], payload['partition_id'])
    boot, previous = slots.get(key, (event.boot_id, empty_usage()))
    if boot != event.boot_id:
        previous = empty_usage()  # A new actor reports only its new work.
    if any(usage[name] < previous[name] for name in fields):
        raise DataflowError('FENCED', 'Prompt counters moved backwards within a worker boot')
    for name in fields:
        execution._observed_prompt_usage[name] += usage[name] - previous[name]
    slots[key] = (event.boot_id, dict(usage))


def publish_prompt_usage(executor, execution):
    current = getattr(execution, '_observed_prompt_usage', empty_usage())
    previous = getattr(execution, '_published_prompt_usage', empty_usage())
    if not hasattr(executor, '_dataflow_prompt_usage'):
        executor._dataflow_prompt_usage = empty_usage()
    for key in current:
        executor._dataflow_prompt_usage[key] += current[key] - previous[key]
    execution._published_prompt_usage = dict(current)


def executor_prompt_usage(executor):
    return dict(getattr(executor, '_dataflow_prompt_usage', empty_usage()))
