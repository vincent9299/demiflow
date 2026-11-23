"""Publication facts implementation."""
from __future__ import annotations


def inspect_native_write_receipt(journal, action_id):
    """Project a v3 native publication without starting code or changing history."""
    from demiflow.execution.publication_facts import observation_action, commit_write_context
    from demiflow.execution.seal.sealed_plans import sealed_read_bounds
    from demiflow.execution.storage.objects import readonly_run_store
    from demiflow.execution.seal.plan import load_dataflow
    binding = journal.recovery_identity()
    action = journal.execution_action(action_id)
    facts = journal.execution_commits(action_id)
    if binding is None or action is None or not facts:
        return None
    fact = facts[-1]
    if fact['prepared'] is None or fact['intent']['sinks'] != ['write_lance']:
        return None
    seal = journal.source_binding(action_id + '-dataflow-sealed')
    if seal is None:
        return None
    bounds = sealed_read_bounds()
    store = readonly_run_store(journal.directory, store_id=seal['physical_ref']['store_id'], bounds=bounds)
    graph, physical, _ = load_dataflow(journal.directory, action_id, store=store, bounds=bounds, _reader=journal)
    native_action = observation_action(journal, action_id, action['declaration'], binding)
    prepared = fact['prepared']['sinks']['write_lance']
    from demiflow.execution.outputs.sinks import inspect_native_publication
    return inspect_native_publication(native_action, prepared, store=store, bounds=physical.bounds,
        make_context=lambda: commit_write_context(native_action, fact['commit_id']))
