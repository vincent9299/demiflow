"""Memory budgets of a newly compiled plan.

What each node may keep resident, and how optional codec room is shared when
the nodes that run together would otherwise not fit. This is arithmetic over
``Bounds`` and already built kernels; it never executes a source, a UDF or an
actor, and it is applied only while compiling a new plan. A sealed plan keeps
the profiles it was sealed with and is never passed through this module.

The capture in ``dataflow_compile`` decides the shape of the graph. This module
decides the sizes:

* ``payload_workspace`` / ``default_profile``  the default working set of a node;
* ``relational_profile``                       join copy and reducer working sets;
* ``terminal_budget``                          the terminal index, payload, native
                                               publication and carrier windows;
* ``codec_window`` / ``declare_codec_windows``     the proven working range of a kernel;
* ``allocate_workspace_slack``                    grants within each selected physical group.
"""
from __future__ import annotations

from dataclasses import replace
from typing import Mapping, NamedTuple

from ..data.plan import LogicalOp
from ..execution.contracts.dataflow import Bounds
from ..execution.contracts.dataflow_errors import DataflowError
from ..resources.contracts import ResourceProfile


def _error(code: str, message: str) -> DataflowError:
    return DataflowError(code, message, context={})


class TerminalBudget(NamedTuple):
    configuration: dict
    profile: ResourceProfile
    phased_publication: bool
    publication_window: tuple[int, int] | None    # (minimum, target) of the native publication workspace
    codec_window: tuple[int, int] | None          # (minimum, target) of the terminal's own optional room


def payload_workspace(bounds: Bounds, *, metadata_bytes=0) -> int:
    """Provision codec room in a new plan without enlarging the run budget.

    The old batch + row window can be smaller than one near-limit row's
    typed-document/JSONL decode. Small nested tuple/list rows also retain
    Python containers far larger than their encoded bytes: give their codec
    a 128-KiB default target, in addition to the scalar/bytes copy window.
    The runtime still measures actual
    structure and rejects anything that cannot fit the sealed allocation.
    Small runs retain their original batch/row minimum and may use a smaller
    codec window, rather than losing all small-row execution capability.
    """
    payload = bounds.max_batch_bytes + bounds.max_row_bytes
    control = max(1, min(bounds.max_control_bytes, bounds.max_resident_bytes // 8))
    available = max(0, bounds.max_resident_bytes - control - metadata_bytes)
    codec = max(128 * 1024, 6 * bounds.max_row_bytes + 4096)
    return max(payload, min(codec, available))


def default_profile(operation: LogicalOp | None, *, bounds: Bounds, state="none",
                     codec_workspace=True, concurrency=None) -> ResourceProfile:
    # The run memory budget covers admitted data, separately from process RSS.
    # Reserve the same working set used by physical residency, not a fixed
    # per-stage 64 MiB allowance multiplied by pipeline length.
    minimum = bounds.max_batch_bytes + bounds.max_row_bytes
    working_set = payload_workspace(bounds) if codec_workspace else minimum
    options = getattr(operation, 'stream_options', None)
    if concurrency is None:
        concurrency = getattr(options, 'concurrency', getattr(operation, 'concurrency', 1))
    working_set *= concurrency
    minimum *= concurrency
    if state != "none":
        owners = concurrency if getattr(options, "callable_scope", None) == "worker" else 1
        working_set += bounds.max_state_bytes * owners
        minimum += bounds.max_state_bytes * owners
    from ..execution.dataflow_native import collect_fixed_residency
    fixed = collect_fixed_residency(operation, concurrency=concurrency)
    if fixed > bounds.max_resident_bytes:
        from ..execution.contracts.dataflow_errors import DataflowRecoveryError
        raise DataflowRecoveryError("RESOURCE_INFEASIBLE",
                                    "Stream fixed residency exceeds its run memory budget")
    # Fixed decoder/object workspaces coexist with payload and state. They are
    # part of both codec-window endpoints, never optional slack to redistribute.
    working_set += fixed
    minimum += fixed
    requests = {"cpu_millis": 1000, "memory_bytes": working_set}
    native = getattr(operation, "native_options", None) if operation is not None else None
    resources = getattr(native, "worker_resources", None) or {}
    if isinstance(resources, Mapping):
        import math
        if "memory_bytes" in resources:
            # An explicit worker limit remains authoritative. It must retain
            # the original payload/state minimum; actual codec admission may
            # reject a row that cannot fit it, without silently increasing it.
            if resources["memory_bytes"] < minimum:
                raise _error("RESOURCE_INFEASIBLE", "worker memory request is below the admitted working set")
            requests["memory_bytes"] = resources["memory_bytes"]
        if "cpu" in resources:
            if resources["cpu"] <= 0:
                raise _error("RESOURCE_INFEASIBLE", "workers require a positive CPU reservation")
            requests["cpu_millis"] = math.ceil(resources["cpu"] * 1000)
        if resources.get("gpu", 0):
            gpu = resources["gpu"]
            if not float(gpu).is_integer():
                raise _error("UNSUPPORTED_CAPABILITY", "fractional GPU sharing is not declared by the device authority")
            requests["accelerators"] = [{"type": "gpu", "count": int(gpu)}]
        if resources.get("custom_resources"):
            requests["custom"] = {key: {"amount": math.ceil(value * 1000), "unit": "milli"}
                                  for key, value in resources["custom_resources"].items()}
    return ResourceProfile(requests=requests)


def relational_profile(kind, source, bounds):
    """Profile of a relational node, and for a reducer its (field, declaration) memory contract."""
    profile = default_profile(None, bounds=bounds)
    if kind in ('join', 'exclude_keys'):
        requests = dict(profile.requests)
        requests['memory_bytes'] = bounds.max_batch_bytes + 6 * bounds.max_row_bytes
        profile = ResourceProfile(requests=requests, limits=profile.limits, requirements=profile.requirements)
    reducer_memory = None
    if kind == 'reduce':
        from ..execution.kernels.reducer_memory import declare, resident_bytes, FIELD
        declared = declare(source.initial, bounds)
        profile = ResourceProfile(requests={**profile.requests,
            'memory_bytes':resident_bytes(declared,bounds)},
            limits=profile.limits,requirements=profile.requirements)
        reducer_memory = (FIELD, declared)
    return profile, reducer_memory


def terminal_budget(request, nodes, producer, *, bounds, semantic):
    """Memory contract of the terminal that consumes ``nodes[producer]``.

    Returns the terminal configuration and profile, whether its input is finite and
    phased, and the two optional windows the fitter may shrink. When the compact
    reference carrier is selected the producer node in ``nodes`` is rewritten in place.
    """
    from .dataflow_compile import _plain
    # The first terminal block and its first forest root occupy 3 KiB
    # while the input/output payload is still resident. Seal both windows
    # into the original profile; do not borrow half of that payload grant.
    payload = bounds.max_batch_bytes + bounds.max_row_bytes
    # A larger index is optional. Keep the exact control reservation used
    # by solve_residency and the whole live payload; cap only this new-plan
    # metadata window by the remaining single-worker space. The original
    # solver/provider still admit the resulting profile and historical
    # sealed profiles are never recalculated here.
    control = max(1, min(bounds.max_control_bytes, bounds.max_resident_bytes // 8))
    minimum_metadata = 3072
    metadata = min(bounds.max_state_bytes, max(minimum_metadata, payload // 2),
                   max(minimum_metadata, bounds.max_resident_bytes - control - payload))
    if metadata < minimum_metadata:
        raise DataflowError("RESOURCE_INFEASIBLE", "terminal state budget cannot hold one block and root "
            f"(max_state_bytes required={minimum_metadata}, available={metadata})",
            context={'resource': 'max_state_bytes', 'required': minimum_metadata, 'available': metadata})
    # Retain the already promised index capacity. Codec room uses only
    # remaining capacity, never space borrowed from that index or control.
    payload = payload_workspace(bounds, metadata_bytes=metadata)
    profile = default_profile(None, bounds=bounds)
    profile = replace(profile, requests={**profile.requests, "memory_bytes": payload + metadata})
    configuration = {**semantic, "terminal_memory": {"payload_bytes": payload, "metadata_bytes": metadata}}
    native_publication = request.kind in {'write_lance', 'write_file', 'write_datasink'}
    if request.kind == 'write_datasink':
        from ..execution.dataflow_native import publishes_natively
        # Queue tasks were settled by their original authority already;
        # this terminal only publishes the result manifest, without Arrow.
        native_publication = publishes_natively(request.sink)
    if native_publication:
        from ..execution.dataflow_publication_budget import FIELD, SCHEMA
        # A native consumer can still hold its previous yielded Arrow
        # batch when asking this generator to decode the next record.
        configuration[FIELD] = {'schema': SCHEMA,
            'resident_bytes': payload_workspace(bounds) + bounds.max_batch_bytes}
    finite_producer = nodes[producer].kernel
    phased_publication = (native_publication and finite_producer.effect == 'pure'
        and bool(finite_producer.inputs)
        and all(p.requirement == 'finite_complete' for p in finite_producer.inputs))
    if phased_publication:
        from ..execution.dataflow_publication_budget import PHASE_SCHEMA
        configuration[FIELD]['schema'] = PHASE_SCHEMA
    # A pure completed join can publish immutable typed row artifacts.
    # Its terminal handles only the original bounded Record references;
    # native hydration occurs later, under the single driver reservation.
    # No user UDF/queue callback is placed across this carrier boundary.
    compact = (request.kind == 'write_lance'
        and nodes[producer].kernel.kind == 'join'
        # A fused multi-join/reducer region owns a different output
        # iterator. It retains its current carrier/profile until that
        # producer explicitly implements this sealed representation.
        and sum(n.kind == 'relational' for n in nodes) == 1
        and nodes[producer].kernel.profile.requests['memory_bytes']
            + bounds.max_batch_bytes + bounds.max_row_bytes + control
            > bounds.max_resident_bytes
        and all(n.kernel.kind in {'items', 'range', 'materialized'}
                for n in nodes if n.kind == 'source'))
    if compact:
        from ..execution.dataflow_publication_budget import PHASE_SCHEMA, REF_CARRIER
        metadata = min(bounds.max_state_bytes, max(3072, bounds.max_resident_bytes // 128))
        payload = min(128 * 1024, max(4096, bounds.max_resident_bytes // 64))
        configuration['terminal_memory'] = {'payload_bytes': payload, 'metadata_bytes': metadata,
                                             'carrier': REF_CARRIER}
        configuration['input_carrier'] = REF_CARRIER
        configuration[FIELD]['schema'] = PHASE_SCHEMA
        profile = replace(profile, requests={**profile.requests, 'memory_bytes': payload + metadata})
        source_node = nodes[producer]
        source_kernel = source_node.kernel
        memory = min(source_kernel.profile.requests['memory_bytes'], bounds.max_resident_bytes - control)
        nodes[producer] = replace(source_node, kernel=replace(source_kernel,
            configuration={**_plain(source_kernel.configuration), 'output_carrier': REF_CARRIER},
            profile=replace(source_kernel.profile, requests={**source_kernel.profile.requests,
                'memory_bytes': memory})))
    # A small native terminal can stream ordinary Records with a smaller
    # resident codec window than the *business* R/B ceilings. This versioned
    # window accounts actual codec, index and flush peaks before allocation;
    # it does not promise every R-sized object fits an arbitrarily small M.
    # The existing durable residency protocol parks the upstream worker,
    # while the native reservation remains beside the live terminal.
    bounded_workspace = False
    if native_publication and not compact:
        # Default working sets are separate from the public R/B ceilings.
        # In a constrained run leave two shares for worker codec/index and
        # three for native input/array/writer copies. Both original runtime
        # peak guards still reject an indivisible row that cannot fit.
        publication_min = min(bounds.max_batch_bytes + bounds.max_row_bytes,
                              (bounds.max_resident_bytes - control) * 3 // 5)
        terminal_space = bounds.max_resident_bytes - control - publication_min
        legacy_floor = 2 * (bounds.max_batch_bytes + bounds.max_row_bytes) + metadata
        if (legacy_floor > bounds.max_resident_bytes - control
                and 8192 <= terminal_space < profile.requests['memory_bytes']):
            from ..execution.dataflow_publication_budget import PHASE_SCHEMA, TERMINAL_WORKSPACE
            bounded_workspace = True
            configuration[FIELD]['schema'] = PHASE_SCHEMA
            configuration['terminal_memory'] = {'schema': TERMINAL_WORKSPACE,
                                                   'resident_bytes': terminal_space}
            profile = replace(profile, requests={**profile.requests, 'memory_bytes': terminal_space})
    elif not native_publication and payload + metadata > bounds.max_resident_bytes - control:
        # Keep the original row contract. The terminal already measures codec,
        # index and flush peaks against one worker grant on each real append.
        from ..execution.dataflow_publication_budget import TERMINAL_WORKSPACE
        available = bounds.max_resident_bytes - control
        if available < 8192:
            raise _error('RESOURCE_INFEASIBLE', 'terminal cannot fit its bounded workspace')
        configuration['terminal_memory'] = {'schema': TERMINAL_WORKSPACE, 'resident_bytes': available}
        profile = replace(profile, requests={**profile.requests, 'memory_bytes': available})
        bounded_workspace = True
    publication_window = None
    if native_publication:
        publication_window = (publication_min if bounded_workspace else
            bounds.max_batch_bytes + bounds.max_row_bytes,
            payload_workspace(bounds) + bounds.max_batch_bytes)
    minimum = (payload if compact else bounds.max_batch_bytes + bounds.max_row_bytes) + metadata
    codec = (minimum, payload + metadata) if not bounded_workspace and payload + metadata > minimum else None
    return TerminalBudget(configuration, profile, phased_publication, publication_window, codec)


def codec_window(kernel, operation, bounds):
    """The optional codec slack of one node as (minimum, target); None when it has none to share."""
    resources = getattr(getattr(operation, 'native_options', None), 'worker_resources', None) or {}
    if 'memory_bytes' in resources:
        return None  # An explicit user grant is never redistributed.
    if kernel.kind in {'join', 'exclude_keys', 'reduce'}:
        return None  # These kernels own explicit copy/reducer working sets.
    if kernel.kind in {'prompt', 'embedding', 'agent', 'fetch', 'document'}:
        from ..execution.dataflow_models import _model_profile
        concurrency = kernel.configuration.get('concurrency', 1)
        expected = _model_profile(kernel.resource_role, bounds, concurrency,
                                  kernel.state, operation=operation)
        minimum = _model_profile(kernel.resource_role, bounds, concurrency,
            kernel.state, operation=operation, codec_workspace=False).requests['memory_bytes']
        # Managed-service declarations may refine the accelerator profile;
        # this planner only redistributes proven default memory slack.
        matches = kernel.profile.requests['memory_bytes'] == expected.requests['memory_bytes']
    else:
        expected = default_profile(operation, bounds=bounds, state=kernel.state)
        minimum = default_profile(operation, bounds=bounds, state=kernel.state,
                                   codec_workspace=False).requests['memory_bytes']
        matches = kernel.profile == expected
        # The native literal/range reader and synchronous row map measure their
        # actual input and output codec peaks. When even one hypothetical
        # maximum-sized pair cannot fit, give them the full available worker
        # and admit each real value there, retaining the original row bound.
        if (kernel.kind in {'items', 'range', 'materialized', 'literal_empty', 'MapOp', 'BoundMapOp'}
                and minimum == bounds.max_batch_bytes + bounds.max_row_bytes
                and kernel.state == 'none' and not kernel.configuration.get('streaming_map')
                and kernel.configuration.get('concurrency', 1) == 1):
            control = max(1, min(bounds.max_control_bytes, bounds.max_resident_bytes // 8))
            minimum = min(minimum, bounds.max_resident_bytes - control)
        calls = kernel.configuration.get('concurrency', 1)
        if kernel.kind == 'AsyncMapOp' and calls > 1:
            # Unary calls keep one full output per slot. Their input credit
            # can shrink to a single row; another row may already be in push
            # waiting for that credit. State and fixed costs still coexist.
            minimum -= calls * bounds.max_batch_bytes - 2 * bounds.max_row_bytes
    if not matches:
        return None  # Native models, reducers and other specialized grants retain their authority.
    target = kernel.profile.requests['memory_bytes']
    return (minimum, target) if target > minimum else None


def declare_codec_windows(nodes, codec_windows, publication_windows):
    """Capture proven elastic parts of the compiler's preferred kernel grants.

    An explicit or specialized worker grant has no optional codec component.
    Decorating a node may have replaced its profile; only the original target
    can carry the corresponding minimum into physical planning.
    """
    from ..execution.contracts.dataflow_codec import _thaw_json
    from ..execution.dataflow_publication_budget import FIELD, workspace_bytes
    for index, node in enumerate(nodes):
        fields = {}
        codec = codec_windows.get(index)
        if codec and codec[1] == node.kernel.profile.requests['memory_bytes']:
            fields['codec_min_bytes'] = codec[0]
        publication = publication_windows.get(index)
        if publication and FIELD in node.kernel.configuration and workspace_bytes(node.kernel.configuration) == publication[1]:
            fields['publication_min_bytes'] = publication[0]
        if fields:
            config = {**_thaw_json(node.kernel.configuration), **fields}
            nodes[index] = replace(node, kernel=replace(node.kernel, configuration=config))


def minimum_workspace_graph(graph):
    """Use real, indivisible demands when testing a physical layout."""
    from ..execution.dataflow_publication_budget import with_publication_workspace
    nodes = []
    for node in graph.nodes:
        config = node.kernel.configuration
        kernel = node.kernel
        if 'codec_min_bytes' in config:
            kernel = kernel.with_memory_bytes(config['codec_min_bytes'])
        if 'publication_min_bytes' in config:
            kernel = with_publication_workspace(kernel, config['publication_min_bytes'])
        nodes.append(replace(node, kernel=kernel))
    return replace(graph, nodes=tuple(nodes))


def allocate_workspace_slack(graph, floor_graph, stages, residency, *, hosts, bounds):
    """Expand optional codec capacity inside each selected co-resident group.

    A worker in a phased group need not share its slack with unrelated phases.
    Publication is a single driver grant, shared across the phases that use it.
    """
    from fractions import Fraction
    from ..execution.dataflow_publication_budget import PublicationReservation, graph_workspace_bytes
    reservation = PublicationReservation(floor_graph, stages, bounds)
    publication_extra = graph_workspace_bytes(graph) - reservation.bytes
    nodes = {n.occurrence_id: n.kernel for n in graph.nodes}
    by_id = {stage.stage_id: stage for stage in stages}
    extras = {}
    for stage in stages:
        values = {}
        for occ in stage.occurrences:
            kernel = nodes[occ]
            target = kernel.profile.requests['memory_bytes']
            minimum = kernel.configuration.get('codec_min_bytes', target)
            if target > minimum:
                values[occ] = target - minimum
        extras[stage.stage_id] = values

    smallest_host = min(int(host.logical_resources.get('memory_bytes', 0)) for host in hosts)
    shares = {}
    publication_share = Fraction(1)
    for ids in residency.simultaneous_groups:
        group = [by_id[name] for name in ids]
        def replicas(stage):
            return min(len(stage.partitions), stage.replica_target) if len(group) == 1 else len(stage.partitions)
        footprint = sum(max(s.resident_bytes, s.profile.requests['memory_bytes']) * replicas(s) for s in group)
        # Aggregate cluster headroom may strand an indivisible worker. Only
        # grow optional windows from room safe under any existing placement.
        capacity = min(bounds.max_resident_bytes - reservation.reserved(ids), smallest_host)
        remaining = max(0, capacity - footprint)
        optional = sum(sum(extras[s.stage_id].values()) * replicas(s) for s in group)
        publishing = not reservation.scoped or bool(reservation.stages.intersection(ids))
        if publishing:
            optional += publication_extra
        share = min(Fraction(1), Fraction(remaining, optional)) if optional else Fraction(1)
        if publishing:
            publication_share = min(publication_share, share)
        for stage in group:
            demand = sum(extras[stage.stage_id].values())
            if demand:
                shares[stage.stage_id] = share

    selected = []
    for stage in stages:
        share = shares.get(stage.stage_id, 1)
        overrides = {}
        growth = 0
        for occ, extra in extras[stage.stage_id].items():
            increment = int(share * extra)
            if increment < extra:
                overrides[occ] = nodes[occ].configuration['codec_min_bytes'] + increment
            growth += increment
        if growth:
            # Store the exact grant consumed by the existing worker codec guard.
            total = max(stage.resident_bytes, stage.profile.requests['memory_bytes']) + growth
            stage = replace(stage, resident_bytes=total, profile=replace(stage.profile,
                requests={**stage.profile.requests, 'memory_bytes': total}))
        if overrides:
            stage = replace(stage, kernel_memory_bytes=overrides)
        selected.append(stage)
    publication = reservation.bytes + int(publication_share * publication_extra)
    return tuple(selected), reservation.base + publication
