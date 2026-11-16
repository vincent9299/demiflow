"""Reducer-owned heap/copy windows within the original resident grant.

The serialized-state ceiling keeps its existing meaning. This module accounts
for the known retained objects and platform copies separately; user callback
and custom copy/reduce hooks remain user allocations, not a promised RSS cap.
"""
from dataclasses import dataclass
import gc
import pickle
import sys
import types

from ..contracts.dataflow_errors import DataflowError

FIELD = 'reducer_memory_v1'


def _error(code, message):
    return DataflowError(code, message)


@dataclass(frozen=True)
class TreeSize:
    retained: int
    copy_heap: int
    copy_workspace: int
    nodes: int


def measure_tree(value, *, max_bytes, max_nodes, max_copy_bytes=None):
    """Inspect a bounded object graph without running user sizing methods.

    Aliases count once. Known builtin container copies include conservative
    allocation growth and deepcopy memo entries. Python instance referents
    are read through GC's native traversal, never user properties/__sizeof__.
    """
    seen = set()
    retained = copied = mutable = deepest = 0
    immutable = (type(None), bool, int, float, complex, str, bytes)
    shared = (type, types.FunctionType, types.CodeType, types.ModuleType,
              types.BuiltinFunctionType)

    def visit(item, depth):
        nonlocal retained, copied, mutable, deepest
        if depth > 64:
            raise _error('BUDGET_EXCEEDED', 'Reducer object depth exceeds its inspection window')
        identifier = id(item)
        if identifier in seen or type(item) in shared:
            return
        if len(seen) >= max_nodes:
            raise _error('BUDGET_EXCEEDED', 'Reducer object metadata exceeds its inspection window')
        seen.add(identifier)
        deepest = max(deepest, depth)
        kind = type(item)
        if kind in immutable:
            # None/bool are process constants; no accumulator-owned allocation.
            retained += 0 if kind in (type(None), bool) else sys.getsizeof(item)
        elif kind in (list, tuple, dict, set, frozenset, bytearray):
            size = sys.getsizeof(item)
            retained += size
            # deepcopy may overallocate list/dict capacity, and tuple/set copy
            # construction may hold its temporary sequence at the same time.
            copied += 2 * size
            mutable += 1
            if kind is dict:
                for key, child in item.items():
                    visit(key, depth + 1)
                    visit(child, depth + 1)
            elif kind is not bytearray:
                for child in item:
                    visit(child, depth + 1)
        elif kind.__module__ == 'numpy' and kind.__name__ == 'ndarray':
            if item.dtype.hasobject:
                raise _error('UNSUPPORTED_CAPABILITY', 'Reducer object arrays need a bounded state adapter')
            size = object.__sizeof__(item) + int(item.nbytes)
            retained += size
            copied += size
            mutable += 1
        else:
            # Native GC traversal exposes Python slots/dicts without invoking
            # descriptors. A user __deepcopy__/__reduce__ is a user callback;
            # its returned state is inspected before platform serialization.
            size = object.__sizeof__(item)
            retained += size
            copied += 2 * size
            mutable += 1
            for child in gc.get_referents(item):
                visit(child, depth + 1)
        if retained > max_bytes or (max_copy_bytes is not None and copied > max_copy_bytes):
            raise _error('BUDGET_EXCEEDED', 'Reducer retained objects exceed the resident window')

    visit(value, 0)
    return TreeSize(retained, copied, 512 * mutable + 256 * deepest, len(seen))


def declare(initial, bounds):
    inspection = min(4 * 1024**2, max(65536, bounds.max_resident_bytes // 16))
    nodes = max(1, inspection // 128)
    size = measure_tree(initial, max_bytes=bounds.max_resident_bytes, max_nodes=nodes,
                        max_copy_bytes=bounds.max_resident_bytes)
    # Each state retains its own objects; this is not the serialized byte limit.
    # The staged pickle may rebuild builtin list/dict capacity differently.
    # Retain the measured immutable payload and a conservative container-growth
    # allowance; None remains exactly zero accumulator-owned bytes.
    initial_window = size.retained + size.copy_heap // 8 + size.copy_workspace
    copy_window = size.copy_heap + size.copy_heap // 4 + size.copy_workspace
    state = max(bounds.max_state_bytes, copy_window)
    return {'version': 1, 'initial_bytes': initial_window,
            'initial_copy_bytes': copy_window,
            'copy_workspace_bytes': size.copy_workspace,
            'state_resident_bytes': state, 'inspection_bytes': inspection,
            'inspection_nodes': nodes}


def contract(spec):
    value = spec.configuration.get(FIELD)
    if value is None:
        return None
    expected = {'version', 'initial_bytes', 'initial_copy_bytes', 'copy_workspace_bytes',
                'state_resident_bytes', 'inspection_bytes', 'inspection_nodes'}
    if (set(value) != expected or value['version'] != 1
            or any(type(v) is not int or v < 0 for v in value.values())
            or min(value['state_resident_bytes'],value['inspection_bytes'],value['inspection_nodes']) < 1):
        raise _error('INCOMPATIBLE_HISTORY', 'Invalid sealed reducer resident contract')
    return value


def resident_bytes(value, bounds, *, join_depth=None):
    """Maximum of disjoint build/hydrate/fold/copy/output phases, not their sum."""
    row, batch = bounds.max_row_bytes, bounds.max_batch_bytes
    initial, state = value['initial_bytes'], value['state_resident_bytes']
    hydrate = batch + ((join_depth + 1) if join_depth is not None else 2) * row
    build = 8 * row + hydrate
    fold = max(hydrate + row + state,            # next row + saved group key/state
               3 * row + 2 * state,             # row/two group keys + old/new state
               3 * row + state + max(state, bounds.max_state_bytes),
               3 * row + value['initial_copy_bytes'] + value['copy_workspace_bytes'],
               batch + 4 * row + state)         # next group row + final output/codec
    return initial + value['inspection_bytes'] + max(build, 7 * row + fold)


def sort_window(bounds, *, key_only):
    from .ordering import SortWindow
    row = bounds.max_row_bytes
    chunk = min(65536, max(1, row // 64))
    return SortWindow(resident_bytes=8 * row, run_bytes=2 * row,
        frame_bytes=max(1,(7 * row - 2 * chunk)//8), read_chunk_bytes=chunk,
        merge_fan_in=2, key_only=key_only, merge_resident_bytes=7 * row)


def check_initial(initial, value):
    measured = measure_tree(initial, max_bytes=max(1,value['initial_bytes']),
                            max_nodes=value['inspection_nodes'],max_copy_bytes=value['initial_copy_bytes'])
    if (measured.retained > value['initial_bytes']
            or measured.copy_heap > value['initial_copy_bytes']
            or measured.copy_workspace > value['copy_workspace_bytes']):
        raise _error('RESOURCE_INFEASIBLE', 'Reducer initial copy exceeds its sealed resident window')


def check_state(state, value, *, serialized_limit):
    # Must precede pickle's frame/UTF-8/blob allocations. The caller reserves a
    # second state-sized window for the known encoding peak.
    measure_tree(state, max_bytes=value['state_resident_bytes'],max_nodes=value['inspection_nodes'])

    class Counter:
        def __init__(self):
            self.count = 0
        def write(self, raw):
            if self.count + len(raw) > serialized_limit:
                raise _error('BUDGET_EXCEEDED', 'reducer state exceeds max_state_bytes')
            self.count += len(raw)
            return len(raw)
    # Preserve the exact pickle protocol and one serialization invocation, but
    # don't retain a growing BytesIO/full encoded copy merely to count bytes.
    pickle.Pickler(Counter(),protocol=pickle.HIGHEST_PROTOCOL).dump(state)


def check_grant(value, bounds, assignment, *, join_depth=None):
    required = resident_bytes(value,bounds,join_depth=join_depth)
    stages = getattr(assignment,'stages',())
    available = sum(min(stage.resident_bytes,stage.profile.requests['memory_bytes']) for stage in stages)
    if stages and (available < required or required > bounds.max_resident_bytes):
        raise _error('RESOURCE_INFEASIBLE','Reducer known copies exceed the admitted resident grant')
