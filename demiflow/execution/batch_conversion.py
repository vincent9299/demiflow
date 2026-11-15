"""Pure pre-allocation admission for terminal row-to-column conversions.

This bounds managed payload buffers and conversion workspace, not process RSS.
It neither allocates arrays nor owns a resource grant. The caller keeps its
original Bounds, schema, batch size and third-party conversion semantics.
"""
from dataclasses import dataclass
import sys

from demiflow.data.runtime_contract import check_size
from .contracts.dataflow_errors import DataflowError


def _error(message):
    return DataflowError('BUDGET_EXCEEDED', 'batch conversion admission: ' + message)


def _builtin_storage(dtype):
    """Known native storage layouts; never invoke a user extension hook."""
    import pyarrow as pa
    known = tuple(getattr(pa, name) for name in ('JsonType', 'UuidType', 'Bool8Type')
                  if hasattr(pa, name))
    return dtype.storage_type if type(dtype) in known else None


def _binary_size(value):
    if type(value) is bytes or type(value) is str and value.isascii():
        return len(value)
    if type(value) is str:
        # Exact UTF-8 length without allocating a temporary encoded payload.
        return sum(1 if ord(char) < 128 else 2 if ord(char) < 2048 else
                   3 if ord(char) < 65536 else 4 for char in value)
    raise TypeError('Arrow string/binary conversion requires str or bytes')


@dataclass(frozen=True)
class _Column:
    count: int
    values: object  # Re-iterable factory over existing values; no flattened copy.
    implicit_nulls: int = 0


class _Admission:
    def __init__(self, rows, bounds):
        self.maximum = min(bounds.max_batch_bytes, bounds.max_resident_bytes)
        self.depth = min(16, bounds.max_depth)
        self.max_types = bounds.max_nodes
        self.types = self.visits = 0
        self.retained = sys.getsizeof(rows)
        for row in rows:
            self.retained += check_size(row, bounds.max_row_bytes)
            self.check(0)
        self.check(0)
        self.buffers = self.builder = 0

    def check(self, extra):
        if self.retained + extra > self.maximum:
            raise _error('input and conversion workspace exceed byte budget')

    def visit(self, depth):
        self.visits += 1
        if depth > self.depth or self.visits > max(1, self.maximum // 8):
            raise _error('value traversal exceeds structural budget')

    def buffer(self, size):
        if size < 0 or size > self.maximum:
            raise _error('declared buffer exceeds byte budget')
        if size:
            # Arrow builders grow geometrically, with 64-byte minimum alignment.
            # Reserve the old allocation, its replacement and final compaction
            # per buffer, including fixed-width allocations for null values.
            capacity = 1 << (max(64, size) - 1).bit_length()
            self.buffers += capacity
            self.builder += capacity * 3
        self.check(self.builder)

    def arrow(self, column, dtype, depth=0):
        import pyarrow as pa
        self.types += 1
        if depth > self.depth or self.types > self.max_types:
            raise _error('Arrow type traversal exceeds structural budget')
        self.builder += 256  # bounded builder/type bookkeeping per array
        self.check(self.builder)
        n = column.count
        if pa.types.is_null(dtype):
            return
        if isinstance(dtype, pa.BaseExtensionType):
            storage = _builtin_storage(dtype)
            if storage is not None:
                # Keep native JSON/UUID/Bool8 declarations, including empty
                # and null-only typed columns, on the real constructor.
                self.arrow(column, storage, depth + 1)
                return
            raise DataflowError('UNSUPPORTED_CAPABILITY',
                'batch conversion admission cannot bound Arrow extension type ' + str(dtype))
        self.buffer((n + 7) // 8)
        if pa.types.is_run_end_encoded(dtype):
            # No compression-rate assumption: at most one run per logical row.
            self.arrow(_Column(n, lambda: iter(()), n), dtype.run_end_type, depth + 1)
            self.arrow(column, dtype.value_type, depth + 1)
            return
        if pa.types.is_string_view(dtype) or pa.types.is_binary_view(dtype):
            self.buffer((max(32, n) if n else 0) * 16)
            # StringHeapBuilder packs non-inline (>12-byte) values into 32KiB
            # blocks, or one larger block for an oversized value. Account for
            # each old block and its compacted replacement before the next one.
            held = allocated = span = used = peak = blocks = 0
            for value in column.values():
                self.visit(depth)
                if value is None:
                    continue
                size = _binary_size(value)
                if size <= 12:
                    continue
                if size > span - used:
                    if allocated and used < span:
                        compact = ((used + 63) // 64) * 64
                        peak = max(peak, held + compact)
                        held += compact - allocated
                    span, used = max(32768, size), 0
                    allocated = ((span + 63) // 64) * 64
                    held += allocated
                    blocks += 1
                    peak = max(peak, held)
                used += size
                self.check(self.builder + peak + blocks * 64)
            if allocated and used < span:
                peak = max(peak, held + ((used + 63) // 64) * 64)
            self.builder += peak + blocks * 64
            self.check(self.builder)
            return
        if pa.types.is_dictionary(dtype):
            self.buffer((max(32, n) if n else 0) * dtype.index_type.bit_width // 8)
            self.buffer(n * 64)  # dictionary memo/index; never assume duplicates
            self.arrow(column, dtype.value_type, depth + 1)
            return
        try:
            bits = dtype.bit_width
        except (ValueError, AttributeError):
            bits = None
        if bits is not None:
            # Primitive builders have kMinBuilderCapacity=32. Fixed binary
            # builders reserve the byte width directly (not 32 huge values).
            capacity = n if pa.types.is_fixed_size_binary(dtype) else max(32, n) if n else 0
            self.buffer((capacity * bits + 7) // 8)
            return
        if any(test(dtype) for test in (pa.types.is_string, pa.types.is_large_string,
                                       pa.types.is_binary, pa.types.is_large_binary)):
            width = 8 if pa.types.is_large_string(dtype) or pa.types.is_large_binary(dtype) else 4
            self.buffer((n + 1) * width)
            size = 0
            for value in column.values():
                self.visit(depth)
                if value is None:
                    continue
                size += _binary_size(value)
                self.check(self.builder + size)
            self.buffer(size)
            return
        if pa.types.is_struct(dtype):
            for index, field in enumerate(dtype):
                def values(index=index, name=field.name):
                    for value in column.values():
                        if value is None:
                            yield None
                        elif type(value) is dict:
                            yield value.get(name)
                        elif type(value) in (tuple, list):
                            yield value[index] if index < len(value) else None
                        else:
                            raise TypeError('Arrow struct conversion requires dict or tuple')
                self.arrow(_Column(n, values, column.implicit_nulls), field.type, depth + 1)
            return
        if pa.types.is_map(dtype):
            self.buffer((n + 1) * 4)
            def pairs():
                for value in column.values():
                    if value is not None:
                        yield from value.items() if type(value) is dict else value
            count = 0
            for _ in pairs():
                self.visit(depth)
                count += 1
            self.arrow(_Column(count, lambda: (pair[0] for pair in pairs())), dtype.key_type, depth + 1)
            self.arrow(_Column(count, lambda: (pair[1] for pair in pairs())), dtype.item_type, depth + 1)
            return
        if (pa.types.is_list(dtype) or pa.types.is_large_list(dtype) or pa.types.is_fixed_size_list(dtype)
                or pa.types.is_list_view(dtype) or pa.types.is_large_list_view(dtype)):
            fixed = pa.types.is_fixed_size_list(dtype)
            if pa.types.is_list_view(dtype) or pa.types.is_large_list_view(dtype):
                width = 8 if pa.types.is_large_list_view(dtype) else 4
                self.buffer(n * width)  # offsets
                self.buffer(n * width)  # sizes (views have no trailing offset)
            elif not fixed:
                self.buffer((n + 1) * (8 if pa.types.is_large_list(dtype) else 4))
            count = implicit = 0
            if fixed:
                implicit = column.implicit_nulls * dtype.list_size
            for value in column.values():
                self.visit(depth)
                if value is None:
                    if fixed:
                        implicit += dtype.list_size
                elif type(value) in (tuple, list):
                    count += len(value)
                else:
                    raise TypeError('Arrow list conversion requires list or tuple')
            def children():
                for value in column.values():
                    if value is not None:
                        yield from value
            if fixed:
                # Even a malformed short explicit value must not make a large
                # declared child allocation invisible before Arrow rejects it.
                implicit += max(0, n * dtype.list_size - count - implicit)
            self.arrow(_Column(count + implicit, children, implicit), dtype.value_type, depth + 1)
            return
        raise DataflowError('UNSUPPORTED_CAPABILITY',
            'batch conversion admission cannot bound Arrow layout ' + str(dtype))

    def boxed(self, value, depth=0):
        """Conservative Python object output, including Pandas nested objects."""
        self.visit(depth)
        if value is None:
            return 16
        if type(value) in (bool, int, float):
            return max(128, sys.getsizeof(value))
        if type(value) in (str, bytes):
            return sys.getsizeof(value) + 64
        if type(value) in (list, tuple, set, frozenset):
            size = 128 + 8 * len(value)
            for child in value:
                size += self.boxed(child, depth + 1)
                self.check(size)
            return size
        if type(value) is dict:
            size = 256 + 64 * len(value)
            for key, child in value.items():
                size += self.boxed(key, depth + 1) + self.boxed(child, depth + 1)
                self.check(size)
            return size
        # check_size already admitted the closed temporal/Decimal scalar domain.
        return 256 + check_size(value, self.maximum)

    def numpy(self, column):
        import numpy as np
        def value_shape(value, depth=0):
            self.visit(depth)
            if type(value) in (list, tuple):
                shape, dtype = common(iter(value), depth + 1)
                return (len(value), *shape), dtype
            if value is None or type(value) in (dict, set, frozenset):
                return (), np.dtype('object')
            if type(value) is bool:
                return (), np.dtype('bool')
            if type(value) is int:
                kind = 'int64' if -(2**63) <= value < 2**63 else 'uint64' if 0 <= value < 2**64 else 'object'
                return (), np.dtype(kind)
            if type(value) is float:
                return (), np.dtype('float64')
            if type(value) is str:
                return (), np.dtype('U' + str(max(1, len(value))))
            if type(value) is bytes:
                return (), np.dtype('S' + str(max(1, len(value))))
            return (), np.dtype('object')
        def common(values, depth):
            shape = dtype = None
            for value in values:
                child_shape, child_dtype = value_shape(value, depth)
                if shape is not None and child_shape != shape:
                    raise ValueError('setting an array element with a sequence: inhomogeneous shape')
                shape = child_shape
                dtype = child_dtype if dtype is None else np.promote_types(dtype, child_dtype)
            return shape or (), dtype if dtype is not None else np.dtype('float64')
        shape, dtype = common(column.values(), 0)
        count = column.count
        for dimension in shape:
            count *= dimension
        size = count * dtype.itemsize
        # Source column references, final ndarray and a conversion/cast buffer.
        # String promotion is included in dtype.itemsize, not average row width.
        return 256 + 8 * column.count + 2 * size

    def typed_boxed(self, value, dtype, depth=0):
        """Pandas objects after declared Arrow coercions, including nested ones."""
        import pyarrow as pa
        self.visit(depth)
        if value is None:
            return 16
        storage = _builtin_storage(dtype)
        if storage is not None:
            return self.typed_boxed(value, storage, depth + 1)
        if pa.types.is_dictionary(dtype) or pa.types.is_run_end_encoded(dtype):
            return self.typed_boxed(value, dtype.value_type, depth + 1)
        if pa.types.is_string(dtype) or pa.types.is_large_string(dtype) or pa.types.is_string_view(dtype):
            return 80 + 4 * len(value) if type(value) is bytes else self.boxed(value, depth)
        if (pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype)
                or pa.types.is_fixed_size_binary(dtype) or pa.types.is_binary_view(dtype)):
            return 48 + 4 * len(value) if type(value) is str else self.boxed(value, depth)
        if pa.types.is_struct(dtype):
            size = 256 + len(dtype) * 64
            for index, field in enumerate(dtype):
                child = value.get(field.name) if type(value) is dict else value[index]
                size += self.boxed(field.name, depth + 1) + self.typed_boxed(child, field.type, depth + 1)
                self.check(size)
            return size
        if (pa.types.is_list(dtype) or pa.types.is_large_list(dtype) or pa.types.is_fixed_size_list(dtype)
                or pa.types.is_list_view(dtype) or pa.types.is_large_list_view(dtype)):
            size = 128 + 8 * len(value)
            for child in value:
                size += self.typed_boxed(child, dtype.value_type, depth + 1)
                self.check(size)
            return size
        if pa.types.is_map(dtype):
            size = 128 + 64 * len(value)
            for key, child in value.items() if type(value) is dict else value:
                size += self.typed_boxed(key, dtype.key_type, depth + 1)
                size += self.typed_boxed(child, dtype.item_type, depth + 1)
                self.check(size)
            return size
        return self.boxed(value, depth)

    def inference_guard(self, values, workspace):
        # Bound inferred type metadata before Arrow inspects it. The set is
        # limited by original max_nodes, not by unbounded business row count.
        seen = set()
        def visit(value, path=(), depth=0):
            self.visit(depth)
            if type(value) is dict:
                for name, child in value.items():
                    key = (*path, name)
                    seen.add(key)
                    if len(seen) > self.max_types:
                        raise _error('inferred Arrow schema exceeds structural budget')
                    self.check(workspace + len(seen) * 256)
                    visit(child, key, depth + 1)
            elif type(value) in (list, tuple):
                for child in value:
                    visit(child, (*path, None), depth + 1)
        for value in values:
            visit(value)


def admit_batch_conversion(rows, batch_format, *, schema, bounds):
    """Return the existing window after proving constructor workspace fits.

    Type inference (only when no schema was declared) creates type metadata from
    an admitted pointer list, never an Arrow/NumPy payload array. Known schemas
    remain unchanged and are passed directly to the actual constructor.
    """
    admission = _Admission(rows, bounds)
    n = len(rows)
    fmt = 'numpy' if batch_format == 'default' else batch_format
    if fmt == 'list':
        return admission.retained, admission.maximum
    if fmt == 'numpy':
        # NumPy's existing terminal ignores Arrow schema and exposes the actual
        # row columns. A schema must not hide an additional allocating column.
        names = tuple(rows[0]) if rows else ()
    elif fmt == 'pandas' and schema is None:
        names = set()
        for row in rows:
            names.update(row)
            admission.check(len(names) * 256)
        names = tuple(names)
    else:
        names = tuple(schema.names) if schema is not None else tuple(rows[0]) if rows else ()
    if len(names) > bounds.max_nodes:
        raise _error('column count exceeds structural budget')
    if fmt == 'numpy':
        working = 0
        for name in names:
            working += admission.numpy(_Column(n, lambda name=name: (row[name] for row in rows)))
            admission.check(working)
        return admission.retained, admission.maximum
    if fmt not in ('pyarrow', 'pandas'):
        raise DataflowError('INVALID_CONTRACT', 'unsupported batch format')
    if fmt == 'pyarrow' or schema is not None:
        import pyarrow as pa
        for index, name in enumerate(names):
            column = _Column(n, lambda name=name: (row.get(name) for row in rows))
            if schema is not None:
                dtype = schema.field(index).type
            else:
                # This pointer-only inference workspace is gone before builders
                # start. No result array is created merely to estimate its size.
                admission.check(64 + 8 * n)
                values = list(column.values())
                admission.inference_guard(values, 64 + 8 * n)
                dtype = pa.infer_type(values)
                del values
            admission.arrow(column, dtype)
    if fmt == 'pandas':
        # Object staging, inferred/native column arrays, block consolidation and
        # index/column metadata remain live during construction. Typed conversion
        # additionally retains every Arrow buffer admitted above.
        working = 512 + len(names) * 256 + n * len(names) * 40
        for row in rows:
            for index, name in enumerate(names):
                value = row.get(name)
                working += (admission.boxed(value) if schema is None else
                            admission.typed_boxed(value, schema.field(index).type))
                admission.check(admission.builder + working)
        admission.check(admission.builder + working)
    else:
        admission.check(admission.builder)
    return admission.retained, admission.maximum
