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
* ``codec_window`` / ``fit_codec_windows``     the optional slack of each node and
                                               its redistribution inside each
                                               incremental component.
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
    metadata = min(bounds.max_state_bytes, max(3072, payload // 2),
                   bounds.max_resident_bytes - control - payload)
    if metadata < 3072:
        raise _error("RESOURCE_INFEASIBLE", "terminal state budget cannot hold one block and root")
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
    if request.kind == 'write_lance' and not request.plan.operations:
        from ..data.sources import LanceSource
        from ..lance.model import LanceScanSpec, LanceWriteSpec
        source = request.source
        spec = next((p.value for p in request.programs if p.name == 'write_lance'), None)
        if (type(source) is LanceSource and type(source.query) is LanceScanSpec
                and source.native_options is None and source.query.version is not None
                and not source.query.filter and not source.query.projection and source.query.limit is None
                and type(spec) is LanceWriteSpec and spec.mode == 'merge'):
            configuration['input_carrier'] = 'fixed_scan_batches.v1'
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
    if not matches:
        return None  # Native models, reducers and other specialized grants retain their authority.
    target = kernel.profile.requests['memory_bytes']
    return (minimum, target) if target > minimum else None


def incremental_components(nodes, edges):
    """Node-index groups that must coexist; finite inputs split natural phases."""
    parents = list(range(len(nodes)))
    def owner(index):
        while parents[index] != index:
            parents[index] = parents[parents[index]]
            index = parents[index]
        return index
    for producer, _, consumer, port in edges:
        spec = next(p for p in nodes[consumer].kernel.inputs if p.name == port)
        if spec.requirement == 'incremental':
            parents[owner(consumer)] = owner(producer)
    groups = {}
    for index in range(len(nodes)):
        groups.setdefault(owner(index), []).append(index)
    return groups


def fit_codec_windows(nodes, edges, codec_windows, publication_windows, bounds):
    """Share only optional codec slack inside each incremental component.

    Input/output/state and terminal index minima remain fully additive.
    Finite-complete edges separate natural phases; no source is upgraded to
    checkpointable and no existing sealed graph is passed through this code.
    The physical solver still accounts for replicas, hosts and real grants;
    actual typed codec allocations must still pass the runtime peak guard.
    """
    from .dataflow_compile import _plain
    groups = incremental_components(nodes, edges)
    control = max(1, min(bounds.max_control_bytes, bounds.max_resident_bytes // 8))
    available = bounds.max_resident_bytes - control
    phased_publication = False
    if publication_windows:
        from fractions import Fraction
        from ..execution.dataflow_publication_budget import FIELD
        publication_min = max(v[0] for v in publication_windows.values())
        publication_target = max(v[1] for v in publication_windows.values())
        from ..execution.dataflow_publication_budget import phased
        phased_publication = all(phased(nodes[i].kernel.configuration)
                                 for i in publication_windows)
        factor = Fraction(1)
        for members in groups.values():
            if phased_publication and not set(members).intersection(publication_windows):
                continue
            targets = {i: nodes[i].kernel.profile.requests['memory_bytes'] for i in members}
            minima = {i: codec_windows.get(i, (target, target))[0]
                      for i, target in targets.items()}
            demand = sum(targets.values()) - sum(minima.values()) + publication_target - publication_min
            if demand:
                factor = min(factor, Fraction(max(0, available - sum(minima.values()) - publication_min), demand))
        publication = publication_min + int((publication_target - publication_min) * factor)
        for index in publication_windows:
            node = nodes[index]
            configuration = _plain(node.kernel.configuration)
            configuration[FIELD]['resident_bytes'] = publication
            nodes[index] = replace(node, kernel=replace(node.kernel, configuration=configuration))
    for members in groups.values():
        group_available = available - (publication if publication_windows and
            (not phased_publication or set(members).intersection(publication_windows)) else 0)
        targets = {i: nodes[i].kernel.profile.requests['memory_bytes'] for i in members}
        adjustable = {i: codec_windows[i] for i in members if i in codec_windows
                      and codec_windows[i][1] == targets[i]}
        minima = {i: adjustable[i][0] if i in adjustable else targets[i] for i in members}
        floor, desired = sum(minima.values()), sum(targets.values())
        if not adjustable or desired <= group_available or floor > group_available:
            continue
        slack, demand = group_available - floor, desired - floor
        for index, (minimum, target) in adjustable.items():
            memory = minimum + (target - minimum) * slack // demand
            node = nodes[index]
            kernel = node.kernel
            configuration = _plain(kernel.configuration)
            if node.kind == 'terminal' and 'terminal_memory' in configuration:
                contract = configuration['terminal_memory']
                contract['payload_bytes'] = memory - contract['metadata_bytes']
            nodes[index] = replace(node, kernel=replace(kernel,
                configuration=configuration,
                profile=replace(kernel.profile, requests={**kernel.profile.requests,
                    'memory_bytes': memory})))
