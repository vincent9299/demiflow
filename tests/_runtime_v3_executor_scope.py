"""Use the real Dataset owner boundary when exercising a thin adapter directly."""
import inspect

from demiflow.execution.pipeline.checked import action


def call(executor, method, source, plan, *args, **kwargs):
    function = getattr(executor, method)
    bound = inspect.signature(function).bind(source, plan, *args, **kwargs)
    bound.apply_defaults()
    arguments = {key: value for key, value in bound.arguments.items()
                 if key not in {'source', 'plan'}}
    if method == 'aggregate':
        # Match Dataset.aggregate's declaration; instances are executable
        # ActionPrograms, not values for the historical describe() identity.
        arguments = {'aggregates': [item.name for item in arguments['aggregates']]}
    with action(method, source, plan, arguments=arguments) as checked:
        result = function(checked.source, checked.plan, *args, **kwargs)
        # These tests have at most five rows. Exhaust the original iterator
        # before its owning action exits, just as Dataset's public adapters do.
        return list(result) if method in {'iter_rows', 'iter_batches'} else result


def observed_source(tmp_path):
    """A real lazy source records its first execution across worker processes."""
    from demiflow import data
    marker = tmp_path / 'source-read'
    def rows():
        marker.write_text('read\n')
        yield {'id': 0, 'name': 'x'}
    return data.from_iter(rows)._source, marker
