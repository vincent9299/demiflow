"""New-plan driver publication memory inside the original residency envelope.

The reservation coexists with live workers through final ACK. It is not another
allocator; the original scheduler subtracts it through control_reserved_bytes.
Old sealed kernels without this declaration keep their original budget behavior.
"""
from collections.abc import Mapping

from .contracts.dataflow_errors import DataflowError

FIELD = 'publication_workspace'
SCHEMA = 'demiflow.native_publication_workspace.v1'


def workspace_bytes(configuration):
    if FIELD not in configuration:
        return 0
    value = configuration[FIELD]
    if (not isinstance(value, Mapping) or set(value) != {'schema', 'resident_bytes'}
            or value['schema'] != SCHEMA or type(value['resident_bytes']) is not int
            or value['resident_bytes'] <= 0):
        raise DataflowError('INVALID_CONTRACT', 'invalid native publication workspace')
    return value['resident_bytes']


def graph_workspace_bytes(graph):
    values = [workspace_bytes(node.kernel.configuration) for node in graph.nodes
              if node.kind == 'terminal']
    # The original CommitCoordinator prepares native sinks serially. No new
    # parallel publication executor may multiply this one reservation.
    return max(values, default=0)
