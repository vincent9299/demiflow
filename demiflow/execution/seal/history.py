"""History implementation."""
from __future__ import annotations


def offline_model_declaration(directory, action_id, node_id, *, route, store, bounds):
    """Verify the original sealed factory and return its offline input declaration."""
    from demiflow.execution.seal.plan import load_dataflow, load_sealed_dataflow, verify_dataflow_program
    from demiflow.execution.operator_bindings.dataflow_model_factory import PromptFactory, RoutedPromptFactory
    from demiflow.data.plan import CallableSpec
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    graph, _, sealed = load_dataflow(directory, action_id,
                                    store=store, bounds=bounds)
    node = next((node for node in graph.nodes if node.occurrence_id == node_id), None)
    if node is None or node.kernel.kind not in {'prompt', 'agent'} or node.program_ref is None:
        raise DataflowError('INVALID_CONTRACT', 'Offline lookup has no sealed model occurrence')
    _, _, pin = load_sealed_dataflow(sealed, action_id=action_id, store=store, bounds=bounds)
    program = verify_dataflow_program(node.program_ref,
        expected_pin=pin['programs'][node_id], store=store, bounds=bounds)
    spec = program.value
    if route is not None:
        if (not isinstance(spec, CallableSpec) or spec.target is not RoutedPromptFactory
                or len(spec.constructor_args) != 5 or route not in spec.constructor_args[0]):
            raise DataflowError('INVALID_CONTRACT', 'Offline route is absent from the sealed prompt factory')
        spec = spec.constructor_args[0][route]
    if (not isinstance(spec, CallableSpec) or spec.target is not PromptFactory
            or len(spec.constructor_args) != 2):
        raise DataflowError('INVALID_CONTRACT', 'Offline lookup requires the original prompt factory')
    operation, _ = spec.constructor_args
    options = dict(operation.options or {})
    if set(options) not in ({'offline_dir'}, {'offline_store'}):
        raise DataflowError('INVALID_CONTRACT', 'Occurrence declares no offline input store')
    declaration = options, operation.config.prompt_definitions[operation.prompt_name].model
    maximum = sum(len(node.kernel.configuration.get('routes', {})) or 1 for node in graph.nodes)
    return declaration, maximum
