"""Project committed terminal results for callers."""


def graph_result(graph, physical, committed, observations, facts, result_ref, *, workers):
    """Project native counters, first EOI observations and original commit facts."""
    configuration = next(node.kernel.configuration['graph_result'] for node in graph.nodes
                         if 'graph_result' in node.kernel.configuration)
    def rows(occurrence, partition=None):
        values = [value['rows_output'] for (occ, part), value in committed.items()
                  if occ == occurrence and (partition is None or part == partition)]
        return sum(values) if values else None
    def finished(occurrence, partition=None):
        owners = [stage for stage in physical.stages if occurrence in stage.occurrences]
        members = (partition,) if partition is not None else owners[0].partitions
        values = [observations.get((occurrence, member)) for member in members]
        return max(values) if values and all(value is not None for value in values) else None
    targets = configuration['targets']
    return {'workers': workers,
            'egress_rows': rows(configuration['ancestor']),
            'target_rows': {reader: rows(binding['occurrence_id'], binding['partition_id']) for reader, binding in targets.items()},
            'final_arrival': {'ancestor': finished(configuration['ancestor']),
                **{reader: finished(binding['occurrence_id'], binding['partition_id']) for reader, binding in targets.items()}},
            'checkpoints': {fact['commit_id']: {'commit': fact} for fact in facts if fact['status'] == 'acked'},
            'result_ref': result_ref,
            'target_bindings': {reader: dict(binding) for reader, binding in targets.items()},
            'dag_id': graph.dag_id, 'plan_id': physical.plan_id}
