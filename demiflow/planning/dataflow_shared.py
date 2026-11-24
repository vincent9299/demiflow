"""Explicit same-instance ownership; no changes to historical unshared plans."""
from dataclasses import replace
from ..execution.contracts.dataflow_errors import DataflowError
from demiflow.data.plan import actor_owner
from demiflow.execution.contracts.dataflow import shared_binding


def bind_shared_actors(nodes, edges, declarations):
    from .dataflow_compile import _plain
    owners = {}
    recovery_owners = set()
    by_occ = {n.occurrence_id: n for n in nodes}
    for declaration in declarations:
        spec = declaration.callable_spec
        owner = actor_owner(spec.target) if spec is not None else None
        if owner is not None:
            owners.setdefault(id(owner), []).append(declaration.occurrence_id)
            import inspect
            if inspect.getattr_static(owner, 'recovery_rows', None) is not None:
                recovery_owners.add(id(owner))
    groups = [members for members in owners.values() if len(members) > 1]
    if not owners:
        return nodes
    from .dataflow_place import _topo_order
    from types import SimpleNamespace
    order = _topo_order(SimpleNamespace(nodes=nodes, edges=edges))
    rank = {occ: i for i, occ in enumerate(order)}
    bindings = {}
    for members in groups:
        members.sort(key=rank.__getitem__)
        for occ in members:
            if by_occ[occ].kernel.configuration.get('callable_scope') == 'worker':
                raise DataflowError('UNSUPPORTED_CAPABILITY', 'A shared actor cannot request independent worker owners')
        binding = {'owner': members[0], 'members': members, 'state_owner': members[-1]}
        bindings.update((occ, binding) for occ in members)
    marked = {occ for members in owners.values() for occ in members}
    recovery = {occ for key, members in owners.items() if key in recovery_owners for occ in members}
    return tuple(replace(n, kernel=replace(n.kernel, configuration={
        **_plain(n.kernel.configuration), 'actor_binding_version': 1,
        **({'actor_recovery_rows': True} if n.occurrence_id in recovery else {}),
        **({'shared_actor': bindings[n.occurrence_id]} if n.occurrence_id in bindings else {})}))
        if n.occurrence_id in marked else n for n in nodes)


def colocate_shared_actors(graph, stages, *, bounds, hosts):
    """Contract each topologically convex closure, never just its endpoints.

    Mandatory ownership is independent of optional fusion. Keep every original
    resource request and state allowance; incompatible profiles fail admission.
    """
    from ..resources.contracts import ResourceProfile
    groups = {b['owner']: set(b['members']) for n in graph.nodes
              if (b := shared_binding(n.kernel.configuration, n.occurrence_id))}
    if not groups:
        return stages
    children = {n.occurrence_id: set() for n in graph.nodes}
    parents = {n.occurrence_id: set() for n in graph.nodes}
    for edge in graph.edges:
        children[edge.producer].add(edge.consumer)
        parents[edge.consumer].add(edge.producer)
    def reach(seed, adjacency):
        result, pending = set(seed), list(seed)
        while pending:
            for nxt in adjacency[pending.pop()] - result:
                result.add(nxt); pending.append(nxt)
        return result
    def closure(selected):
        return reach(selected, children) & reach(selected, parents)
    closures = [closure(members) for members in groups.values()]
    changed = True
    while changed:
        changed = False
        for i in range(len(closures)):
            for j in range(i + 1, len(closures)):
                if closures[i] & closures[j]:
                    closures[i] = closure(closures[i] | closures.pop(j))
                    changed = True
                    break
            if changed:
                break
    # Two disjoint convex contractions can together create a cycle (crossed
    # branches). Compute SCCs of the quotient, then close/merge those owners too.
    while True:
        owner = {o: i for i, selected in enumerate(closures) for o in selected}
        for occ in children:
            if occ not in owner:
                owner[occ] = len(closures)
                closures.append({occ})
        adjacency = {i: set() for i in range(len(closures))}
        reverse = {i: set() for i in adjacency}
        for left, rights in children.items():
            for right in rights:
                a, b = owner[left], owner[right]
                if a != b:
                    adjacency[a].add(b); reverse[b].add(a)
        visited, order = set(), []
        for node in adjacency:
            stack = [(node, False)]
            while stack:
                current, finishing = stack.pop()
                if finishing:
                    order.append(current)
                elif current not in visited:
                    visited.add(current)
                    stack.append((current, True))
                    stack.extend((v, False) for v in adjacency[current] if v not in visited)
        visited, components = set(), []
        for node in reversed(order):
            if node in visited:
                continue
            component, pending = set(), [node]
            while pending:
                current = pending.pop()
                if current in visited:
                    continue
                visited.add(current); component.add(current)
                pending.extend(reverse[current] - visited)
            components.append(component)
        if all(len(c) == 1 for c in components):
            break
        closures = [set().union(*(closures[i] for i in c)) for c in components]
    closures = [selected for selected in closures if len(selected) > 1]
    by_occ = {n.occurrence_id: n for n in graph.nodes}
    replacements, consumed = {}, set()
    for selected in closures:
        if (any(by_occ[o].kernel.configuration.get('actor_recovery_rows') for o in selected)
                and any(port.requirement == 'finite_complete' for o in selected for port in by_occ[o].kernel.inputs)):
            raise DataflowError('UNSUPPORTED_CAPABILITY', 'Shared recovery_rows cannot cross a finite-input stage')
        members = [s for s in stages if selected.intersection(s.occurrences)]
        if any(s.replica_min != 1 or not set(s.occurrences) <= selected for s in members):
            raise DataflowError('UNSUPPORTED_CAPABILITY', 'Shared actor closure requires one partition per original stage')
        # Limits/affinity are independent service contracts, not quantities to
        # discard or reinterpret as summed grants. Explicitly refuse an unsafe
        # placement rather than silently cloning the actor or broadening limits.
        if any(s.profile.limits or s.profile.requirements or
               set(s.profile.requests) - {'cpu_millis', 'memory_bytes'} for s in members):
            raise DataflowError('UNSUPPORTED_CAPABILITY', 'Shared actor closure has incompatible explicit service profiles')
        cpu = sum(s.profile.requests.get('cpu_millis', 0) for s in members)
        resident = sum(max(s.resident_bytes, s.profile.requests.get('memory_bytes', 0)) for s in members)
        state_bytes = sum(s.state_bytes for s in members)
        # The shared original holds one actor and emits a single state image.
        # Remove its duplicated default allowance only; explicit worker
        # requests, per-occurrence invocation slots and fixed costs still add.
        for stage in members:
            for occ in stage.occurrences:
                config = by_occ[occ].kernel.configuration
                binding = shared_binding(config, occ)
                if binding and occ != binding['state_owner'] and 'codec_min_bytes' in config:
                    resident -= stage.state_bytes
                    state_bytes -= stage.state_bytes
        if resident > bounds.max_resident_bytes - max(1, min(bounds.max_control_bytes, bounds.max_resident_bytes // 8)) or not any(
                cpu <= h.logical_resources.get('cpu_millis', h.logical_resources.get('cpu', 0)*1000)
                and resident <= h.logical_resources.get('memory_bytes', 0) for h in hosts):
            raise DataflowError('BUDGET_EXCEEDED', 'Shared actor closure does not fit one original host grant')
        first = members[0]
        replacements[first.stage_id] = replace(first,
            occurrences=tuple(o for s in members for o in s.occurrences),
            partitions=(first.partitions[0],),
            profile=ResourceProfile(requests={'cpu_millis': cpu, 'memory_bytes': resident}),
            replica_min=1, replica_target=1, replica_max=1, resident_bytes=resident,
            output_bytes=sum(s.output_bytes for s in members),
            state_bytes=state_bytes,
            kernel_refs=tuple(dict.fromkeys(r for s in members for r in s.kernel_refs)))
        consumed.update(s.stage_id for s in members)
    return tuple(replacements[s.stage_id] if s.stage_id in replacements else s
                 for s in stages if s.stage_id in replacements or s.stage_id not in consumed)
