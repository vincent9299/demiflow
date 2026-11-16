"""Use the real Dataset owner boundary when exercising a thin adapter directly."""
import inspect

from demiflow.execution.checked import action


def call(executor, method, source, plan, *args, **kwargs):
    function = getattr(executor, method)
    bound = inspect.signature(function).bind(source, plan, *args, **kwargs)
    bound.apply_defaults()
    arguments = {key: value for key, value in bound.arguments.items()
                 if key not in {'source', 'plan'}}
    with action(method, source, plan, arguments=arguments) as checked:
        result = function(checked.source, checked.plan, *args, **kwargs)
        # These tests have at most five rows. Exhaust the original iterator
        # before its owning action exits, just as Dataset's public adapters do.
        return list(result) if method in {'iter_rows', 'iter_batches'} else result
