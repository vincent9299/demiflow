"""Reducer computation's original bounded object/copy/encoding accounting.

No worker assignment, runtime service, or Record is inspected here. The caller
supplies the already admitted limits and roots owned by its output codec.
"""
from dataclasses import dataclass
import gc
import pickle
import sys
import types
from demiflow.execution.contracts.dataflow_errors import DataflowError


def _error(code, message):
    return DataflowError(code, message)


@dataclass(frozen=True)
class TreeSize:
    retained: int
    copy_heap: int
    copy_workspace: int
    nodes: int


def measure_tree(value, *, max_bytes, max_nodes, max_copy_bytes=None,
                 additional_roots=(), excluded_roots=(), max_encoded_atom_bytes=None):
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
    numpy = sys.modules.get('numpy')
    array_type = None if numpy is None else numpy.ndarray

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
        if max_encoded_atom_bytes is not None and kind in (str,bytes,bytearray):
            # Pickle may allocate UTF-8/bytearray data before calling write().
            # Check that known scalar copy before serialization, without
            # constructing the very encoding whose size is being admitted.
            if kind is str and not item.isascii():
                encoded = 0
                for char in item:
                    point=ord(char)
                    encoded += 1 if point<128 else 2 if point<2048 else 3 if point<65536 else 4
                    if encoded > max_encoded_atom_bytes:
                        break
            else:
                encoded=len(item)
            if encoded > max_encoded_atom_bytes:
                raise _error('BUDGET_EXCEEDED','reducer state exceeds max_state_bytes before encoding')
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
        elif array_type is not None and isinstance(item,array_type):
            import numpy as np
            if kind is not np.ndarray:
                raise _error('UNSUPPORTED_CAPABILITY', 'Reducer array subclass needs a bounded state adapter')
            if item.dtype.hasobject:
                raise _error('UNSUPPORTED_CAPABILITY', 'Reducer object arrays need a bounded state adapter')
            # ndarray.__sizeof__ includes owned storage, but a view's nbytes
            # does not include the owner kept alive by .base. Follow that
            # original ownership chain using the same alias set as the tree.
            retained += sys.getsizeof(item)
            copied += object.__sizeof__(item) + 16 + int(item.nbytes)
            mutable += 1
            if item.base is not None:
                if type(item.base) not in (np.ndarray,memoryview,bytes,bytearray):
                    raise _error('UNSUPPORTED_CAPABILITY','Reducer foreign array owner needs a bounded state adapter')
                visit(item.base,depth+1)
        elif kind is memoryview:
            retained += sys.getsizeof(item)
            copied += sys.getsizeof(item) + item.nbytes
            mutable += 1
            owner=item.obj
            import numpy as np
            if type(owner) not in (np.ndarray,memoryview,bytes,bytearray):
                raise _error('UNSUPPORTED_CAPABILITY','Reducer foreign buffer owner needs a bounded state adapter')
            visit(owner,depth+1)
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

    # Output codecs account for the current Record. Seed its reachable objects
    # before measuring peers so shared immutable payloads are not counted twice.
    for excluded in excluded_roots:
        visit(excluded, 0)
    retained = copied = mutable = deepest = 0
    visit(value, 0)
    for root in additional_roots:
        visit(root, 0)
    return TreeSize(retained, copied, 512 * mutable + 256 * deepest, len(seen))


def yielded_peers(initial, state, excluded_roots, value, bounds, *, pending=()):
    """Known peers at a suspended yield, excluding the codec-owned Record.

    There is one shared traversal for initial/current/next row/group keys. The
    explicit merge subwindow remains live only while a next group is pending.
    This observes the original grant; it neither reserves nor persists bytes.
    """
    measured = measure_tree(initial,
        max_bytes=value['initial_bytes'] + value['state_resident_bytes'] + 3 * bounds.max_row_bytes,
        max_nodes=value['inspection_nodes'], additional_roots=(state, *pending),
        excluded_roots=excluded_roots)
    return measured.retained + (7 * bounds.max_row_bytes if pending else 0)


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
    measure_tree(state, max_bytes=value['state_resident_bytes'],max_nodes=value['inspection_nodes'],
                 max_encoded_atom_bytes=serialized_limit)

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
