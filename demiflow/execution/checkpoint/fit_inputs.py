"""Fit inputs implementation."""
from __future__ import annotations

import json
from demiflow.execution.contracts.recovery_contract import identity
from demiflow.execution.outputs.sinks import publication_blocks


def publish_training_input(occurrence, proof, *, action, image, services, check_stop):
    if image is None or occurrence not in image.confirmed_sinks:
        raise ValueError("Training input requires a confirmed row publication")
    ref = image.confirmed_sinks[occurrence]
    raw = b''.join(services.store.read(ref, max_bytes=services.bounds.max_control_bytes))
    publication = json.loads(raw)
    if publication['sink_id'] != occurrence:
        raise ValueError("Training input publication belongs to a different occurrence")
    root = action.context.directory / 'materializations' / 'fixed-datasets'
    root.mkdir(parents=True, exist_ok=True)
    destination = root / occurrence
    from demiflow.execution.checkpoint.commit import CommitCoordinator
    from demiflow.execution.contracts.recovery_contract import SourcePosition
    from demiflow.execution.pipeline.run_context import RunAction
    from demiflow.execution.outputs.lance_sink import LanceSink
    from demiflow.lance.model import LanceWriteSpec
    scope = identity(['training_input', action.action_id, occurrence])
    # The row receipt can be carried into later cuts. Its ordered partition
    # contents and source versions identify the original publication.
    position = SourcePosition(identity([proof, publication['manifest']['partitions']]), 1)
    declaration = {'kind': 'fixed_dataset', 'input': occurrence, 'target': str(destination),
                   'source': position.source_id}
    run = action.context
    run.journal.bind_commit_scope(run.fence, scope, action.action_id, declaration)
    child = RunAction(run, scope, declaration)
    sink = LanceSink(child, LanceWriteSpec(str(destination), mode='create'))
    coordinator = CommitCoordinator(child)
    fact = run.journal.latest_execution_commit(scope)
    if fact and fact['status'] != 'pending':
        coordinator.recover({'lance': sink})
    else:
        def blocks():
            for block in publication_blocks(publication['manifest'], store=services.store,
                        bounds=services.bounds, batch_rows=1, publication_workspace=True,
                        publication_metadata=True):
                check_stop()
                yield block
        coordinator.commit(1, outputs={'lance': (sink, blocks())}, position=position)
    fact = run.journal.latest_execution_commit(scope)
    if fact['status'] != 'acked' or fact['intent']['position'] != position.to_dict():
        raise ValueError("Fixed training input lacks its original commit receipt")
    receipt = fact['receipts']['lance']['data']
    if receipt['uri'] != str(destination):
        raise ValueError("Fixed training input receipt belongs to another table")
    return str(destination), receipt['committed_version']
