"""Projection of authenticated worker observations and committed graph metrics."""
import time

from ..contracts.dataflow_errors import DataflowError


def observe_graph_progress(observations, graph, bounds, payload):
    """Read-only first completion evidence after original worker authentication."""
    if payload.get('node_finished') is not True:
        return
    occurrence, partition = payload['occurrence_id'], payload['partition_id']
    node = next(node for node in graph.nodes if node.occurrence_id == occurrence)
    if not node.kernel.configuration.get('graph_observe'):
        return
    # Missing historical or budget-truncated times remain unavailable.
    if len(observations) < bounds.max_control_bytes // 512:
        observations.setdefault((occurrence, partition), time.monotonic())


def committed_graph_metrics(graph, physical, image, store, *, max_state_bytes):
    """Read counters from authenticated final contributions, including resume."""
    import json
    from ..recovery_contract import identity
    from ..contracts.dataflow_codec import compute_participant_id
    if image is None or not image.cut.final:
        raise DataflowError('INVALID_CONTRACT', 'graph result has no original final cut')
    owners = {compute_participant_id(stage.stage_id, partition): (stage, partition)
              for stage in physical.stages for partition in stage.partitions}
    wanted = {node.occurrence_id for node in graph.nodes if node.kernel.configuration.get('graph_observe')}
    result = {}
    for contribution in image.contributions:
        if contribution.participant_id not in owners:
            raise DataflowError('CORRUPT_ARTIFACT', 'unknown participant in graph final cut')
        stage, partition = owners[contribution.participant_id]
        if not wanted.intersection(stage.occurrences):
            continue
        ref = contribution.operator_states.get(identity(['worker-frontier', partition]))
        if ref is None:
            raise DataflowError('CORRUPT_ARTIFACT', 'graph contribution lacks its worker frontier')
        frontier = json.loads(b''.join(store.read(ref, max_bytes=max_state_bytes)))
        if frontier.get('schema') != 'demiflow.worker-frontier.v3':
            raise DataflowError('CORRUPT_ARTIFACT', 'graph contribution has an unknown frontier')
        for occurrence in wanted.intersection(stage.occurrences):
            values = frontier.get('metrics', {}).get(occurrence)
            if (type(values) is not dict or set(values) != {'rows_input', 'rows_output'}
                    or any(type(value) is not int or value < 0 for value in values.values())
                    or occurrence not in frontier['nodes']):
                raise DataflowError('CORRUPT_ARTIFACT', 'graph node has no committed completion counters')
            result[(occurrence, partition)] = values
    return result
