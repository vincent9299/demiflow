"""New-plan driver publication memory inside the original residency envelope.

The reservation coexists with live workers through final ACK. It is not another
allocator; the original scheduler subtracts it through control_reserved_bytes.
Old sealed kernels without this declaration keep their original budget behavior.
"""
from collections.abc import Mapping

from .contracts.dataflow_errors import DataflowError

FIELD = 'publication_workspace'
SCHEMA = 'demiflow.native_publication_workspace.v1'
PHASE_SCHEMA = 'demiflow.native_publication_workspace.v2'
REF_CARRIER = 'stored_typed_row.v1'
TERMINAL_WORKSPACE = 'demiflow.terminal_workspace.v1'


def workspace_bytes(configuration):
    if FIELD not in configuration:
        return 0
    value = configuration[FIELD]
    if (not isinstance(value, Mapping) or set(value) != {'schema', 'resident_bytes'}
            or value['schema'] not in (SCHEMA, PHASE_SCHEMA) or type(value['resident_bytes']) is not int
            or value['resident_bytes'] <= 0):
        raise DataflowError('INVALID_CONTRACT', 'invalid native publication workspace')
    return value['resident_bytes']


def with_publication_workspace(kernel, publication_bytes):
    """Bind the terminal's selected workspace for serial publication."""
    from dataclasses import replace
    if FIELD not in kernel.configuration:
        return kernel
    from .contracts.dataflow import thaw_json
    config = thaw_json(kernel.configuration)
    maximum = workspace_bytes(config)
    if not config.get('publication_min_bytes', maximum) <= publication_bytes <= maximum:
        raise DataflowError('INVALID_CONTRACT', 'physical publication workspace exceeds declared range')
    config[FIELD]['resident_bytes'] = publication_bytes
    return replace(kernel, configuration=config)


def phased(configuration):
    workspace_bytes(configuration)
    return configuration.get(FIELD, {}).get('schema') == PHASE_SCHEMA


class PublicationReservation:
    """Pure interpretation of one sealed reservation; owns no allocation.

    v2 reserves native conversion beside every live terminal stage. Earlier
    finite stages may use that space only while no terminal is resident. The
    scheduler retains those stages until its original physical-exit proof.
    """
    def __init__(self, graph, stages, bounds, *, declared=None):
        terminals = [n for n in graph.nodes if n.kind == 'terminal']
        self.bytes = max((workspace_bytes(n.kernel.configuration) for n in terminals), default=0)
        self.scoped = bool(self.bytes) and all(phased(n.kernel.configuration) for n in terminals)
        occurrences = {n.occurrence_id for n in terminals}
        self.stages = frozenset(s.stage_id for s in stages if occurrences.intersection(s.occurrences))
        base = max(1, min(bounds.max_control_bytes, bounds.max_resident_bytes // 8))
        if declared is not None and self.bytes:
            # The selected layout seals its driver grant inside control_reserved_bytes.
            self.bytes = declared - base
            for node in terminals:
                with_publication_workspace(node.kernel, self.bytes)
        self.maximum = base + self.bytes if declared is None else declared
        if self.bytes and self.maximum < base + self.bytes:
            raise DataflowError('INVALID_CONTRACT', 'publication workspace lacks its original reservation')
        self.base = self.maximum - self.bytes

    def reserved(self, active, *, publishing=False):
        return self.base + (self.bytes if publishing or not self.scoped
                            or self.stages.intersection(active) else 0)

    def admit(self, active, resident, maximum, *, publishing=False):
        if resident + self.reserved(active, publishing=publishing) > maximum:
            raise DataflowError('BUDGET_EXCEEDED', 'publication and physically resident workers exceed the original memory budget')


def graph_workspace_bytes(graph):
    values = [workspace_bytes(node.kernel.configuration) for node in graph.nodes
              if node.kind == 'terminal']
    # The original CommitCoordinator prepares native sinks serially. No new
    # parallel publication executor may multiply this one reservation.
    return max(values, default=0)
