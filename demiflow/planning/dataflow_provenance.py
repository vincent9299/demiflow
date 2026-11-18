"""Bounded, process-local provenance from the final real compilation.

These values are an optional presentation side channel, never a plan, report,
code identity or persistence format. Object IDs are valid only while the caller
retains its original source/operation weak references in this capture process.
"""
from dataclasses import dataclass
from typing import Literal

MAX_NODES = 256
MAX_BINDINGS = 1024


@dataclass(frozen=True)
class OriginBinding:
    kind: Literal['source', 'operation']
    object_id: int
    occurrence_id: str


@dataclass(frozen=True)
class CompilationProvenance:
    action_id: str
    dag_id: str
    bindings: tuple[OriginBinding, ...]
    terminals: tuple[str, ...]
    unavailable: str | None = None


def compiled_provenance(graph, *, sources, operations):
    """Project compiler-owned indices only after graph decoration and validation.

    Check sizes before iterating or copying aliases. This function is not called
    without an observer. Preserve multiple occurrences of one operation object.
    Oversized presentation data never truncates or rejects a legal graph.
    """
    def unavailable(reason):
        return CompilationProvenance(graph.action_id, graph.dag_id, (), (), reason)

    if len(graph.nodes) > MAX_NODES or len(sources) + len(operations) > MAX_BINDINGS:
        return unavailable('来源关系超过展示预算。')
    bindings = []
    for kind, aliases in (('source', sources), ('operation', operations)):
        for key, index in aliases.items():
            if type(index) is not int or not 0 <= index < len(graph.nodes):
                return unavailable('来源关系没有对应的最终编译节点。')
            object_id = key if kind == 'source' else key[1]
            bindings.append(OriginBinding(kind, object_id, graph.nodes[index].occurrence_id))
    return CompilationProvenance(graph.action_id, graph.dag_id, tuple(bindings), graph.terminals)
