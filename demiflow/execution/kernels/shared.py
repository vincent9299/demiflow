"""Worker-local shared actor ownership authenticated by the original graph."""
from demiflow.execution.contracts.dataflow import shared_binding
from demiflow.data.plan import actor_owner


from collections.abc import Mapping
from demiflow.execution.contracts.dataflow_errors import DataflowError


class SharedActor:
    """One real lifecycle/event loop, distinct per-occurrence call adapters."""
    def __init__(self, binding, owner):
        self.binding, self.owner = binding, owner
        self.base = None
        self.active = set()
        self.restored = set()

    def acquire(self, occurrence, target, services, *, timeout, await_calls, lifecycle):
        from demiflow.execution.kernels.async_instance import AsyncInstance
        if occurrence in self.active:
            raise DataflowError('INVALID_CONTRACT', 'Shared actor occurrence acquired twice')
        if self.base is None:
            self.base = AsyncInstance(self.owner, services, owner=self.owner, lifecycle=lifecycle)
        self.active.add(occurrence)
        view = AsyncInstance(target, services, owner=self.owner, timeout=timeout, await_calls=await_calls)
        view._shared = self
        view._shared_occurrence = occurrence
        view._stopping = self.base._stopping
        return view

    def release(self, occurrence):
        self.active.discard(occurrence)
        if not self.active and self.base is not None:
            self.base.close()

    def restoring(self, occurrence, state):
        if occurrence in self.restored or not isinstance(state, Mapping):
            raise DataflowError('INCOMPATIBLE_HISTORY', 'Shared actor restore membership is missing or duplicated')
        if occurrence == self.binding['state_owner']:
            if state.get('mode') == 'shared_owner':
                raise DataflowError('INCOMPATIBLE_HISTORY', 'Shared actor authority image is absent')
        elif state != {'mode': 'shared_owner', 'owner': self.binding['state_owner']}:
            raise DataflowError('INCOMPATIBLE_HISTORY', 'Shared actor alias differs from its sealed authority')
        self.restored.add(occurrence)
        return occurrence == self.binding['state_owner']


def shared_actors(specs, programs, assignment, *, graph=None):
    groups = {}
    for occ, spec in specs.items():
        binding = shared_binding(spec.configuration, occ)
        if binding is None:
            continue
        if not set(binding['members']) <= set(specs) or len(assignment.lease.partition_ids) != 1:
            raise DataflowError('INVALID_CONTRACT', 'Shared actor group is split between worker owners')
        owner = actor_owner(programs[occ].value.target)
        group = groups.setdefault(binding['owner'], SharedActor(binding, owner))
        if group.binding != binding or owner is None or group.owner is not owner:
            raise DataflowError('CORRUPT_ARTIFACT', 'Shared program lost its actual actor identity')
    for group in groups.values():
        selected = set(group.binding['members'])
        if graph is not None:
            forward, backward = set(selected), set(selected)
            for seen, reverse in ((forward, False), (backward, True)):
                changed = True
                while changed:
                    changed = False
                    for edge in graph.edges:
                        left, right = (edge.consumer, edge.producer) if reverse else (edge.producer, edge.consumer)
                        if left in seen and right not in seen:
                            seen.add(right); changed = True
            selected = forward & backward
        group.live_members = frozenset(selected)
    return {occ: group for group in groups.values() for occ in group.binding['members']}
