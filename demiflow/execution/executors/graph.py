"""Bind a routed Dataset graph to its existing action and provider."""


def graph_action_request(graph_spec, executor):
    from demiflow.execution.executors.dataflow_api import make_action_request
    from ...data.plan import LogicalPlan
    from ...planning.dataflow_graph_adapter import translate_graph
    source = translate_graph(graph_spec)
    return make_action_request(executor, source, LogicalPlan(), kind='materialize',
                               arguments={'graph': source.declaration})


def _validate_graph_location(executor, *, host_id, resource_root):
    """Legacy location keywords cannot create a second placement authority."""
    from pathlib import Path
    from ..contracts.dataflow_errors import DataflowError
    if host_id != 'local':
        raise DataflowError('UNSUPPORTED_CAPABILITY',
                            'graph host placement must use the current execution provider')
    if resource_root is not None and Path(resource_root).resolve() != executor.resource_root:
        raise DataflowError('INVALID_CONTRACT',
                            'graph resource_root differs from the current executor domain')


def materialize_graph(executor, graph_spec, *, host_id='local', resource_root=None, control_timeout_s=120):
    from ..action_driver.dataflow import run_graph_action
    from demiflow.execution.pipeline.run_context import current_action
    _validate_graph_location(executor, host_id=host_id, resource_root=resource_root)
    if current_action() is None:
        raise RuntimeError('Graph materialization requires the native action context')
    return run_graph_action(graph_action_request(graph_spec, executor), control_timeout_s=control_timeout_s)
