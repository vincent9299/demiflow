"""Join key membership, matching, row combination and native offset queries."""
from __future__ import annotations

from demiflow.execution.contracts.dataflow_errors import DataflowError

import json
import copy
import pickle
from demiflow.execution.contracts.record_codec import canonical_record_json as _canonical
from demiflow.execution.contracts.dataflow_errors import DataflowError

from collections.abc import Mapping
import itertools
from typing import Any


JOIN_PARTITION_BITS = 4


def key_membership(column, values):
    """Exact typed membership for canonical join keys; a null never joins a null."""
    import pyarrow.compute as pc
    return pc.is_in(column, value_set=values, skip_nulls=True)


def matching_indices(column, values):
    """Return exact positive probe positions; the caller owns reading and credit."""
    import pyarrow.compute as pc
    return pc.indices_nonzero(key_membership(column, values))


def probe_values(records, field, dtype, *, read_row, check_stop):
    """Compute a same-type exact build set without retaining business payloads."""
    import pyarrow as pa

    values = set()
    try:
        for record in records:
            check_stop()
            value = read_row(record)[field]
            if value is None:
                continue
            if type(value) is not int or value.bit_length() > dtype.bit_width:
                return None
            values.add(value)
    finally:
        records.close()
    try:
        return pa.array(values, type=dtype)
    except (pa.ArrowInvalid, OverflowError):
        return None


def right_index(rows, fields, chunk_bytes, *, key_of, encode, decode):
    """Build the original bounded small-right index, retaining overflow input.

    The algorithm is from data/local_relational._right_index. Only key and
    record encoding are supplied by the partition binding: they preserve the
    current typed equality and record identity instead of introducing a second
    row codec. Neither a source nor an action is opened here.
    """
    index = {}
    buffered = []
    size = 0
    for row in rows:
        key = key_of(row, fields)
        payload = encode(row)
        buffered.append(payload)
        size += len(payload) + 4 * len(key) + 256
        if size > chunk_bytes:
            return None, itertools.chain((decode(p) for p in buffered), rows)
        if key:  # The partition's key codec represents a null join key as b''.
            index.setdefault(key, []).append(payload)
    return index, ()


def probe_right_index(left, index, how, *, key_of, load_left, decode):
    """Original inner/left/semi/anti probe over an entirely admitted right side."""
    for row in left:
        key = key_of(row)
        matches = index.get(key, ())
        if how == 'semi':
            if matches:
                yield load_left(row, key), None
        elif how == 'anti':
            if not matches:
                yield load_left(row, key), None
        elif matches:
            for payload in matches:
                # Decode anew for every pair, preserving nested-value isolation.
                yield load_left(row, key), decode(payload)
        elif how == 'left':
            yield load_left(row, key), None


class JoinKeyFilter:
    """Negative membership on an already-evaluated inner/semi build side.

    The bitmap is valid for every canonical join key. A small homogeneous
    scalar set can also reject typed Arrow rows without decoding their payload.
    Positive probes always proceed to the join's exact equality comparison.
    """
    def __init__(self, budget, *, single_key):
        # Share the join's reserved index space. Leave room for the Arrow
        # hash table built by is_in, value array construction and a batch mask.
        self.bitmap = bytearray(min(64 * 1024, budget // 8))
        self.exact_limit = budget // 8
        self.keys = set() if single_key else None
        self.key_type = None
        self.key_bytes = 0
        self.arrow_values = self.batch_token = self.mask = None

    def _bits(self, hashed):
        count = len(self.bitmap) * 8
        step = (((hashed >> 32) | (hashed << 32)) & ((1 << 64) - 1)) | 1
        for index in range(3):
            yield (hashed + index * step) % count

    def add(self, hashed, value, key_size):
        for bit in self._bits(hashed):
            self.bitmap[bit >> 3] |= 1 << (bit & 7)
        if self.keys is None:
            return
        if type(value) not in (str, int, bool) or self.key_type not in (None, type(value)):
            self.keys = None
        elif value not in self.keys:
            charge = 192 + 4 * key_size
            if self.key_bytes + charge > self.exact_limit:
                self.keys = None
            else:
                self.keys.add(value)
                self.key_type = type(value)
                self.key_bytes += charge

    def finish(self):
        if self.keys:
            import pyarrow as pa
            try:
                self.arrow_values = pa.array(self.keys)
            except (pa.ArrowException, OverflowError):
                pass
        self.keys = None

    def may_match(self, hashed):
        return all(self.bitmap[bit >> 3] & (1 << (bit & 7)) for bit in self._bits(hashed))

    def arrow_rejects(self, column, index, token):
        if (self.arrow_values is None or column.type != self.arrow_values.type
                or len(column) > self.exact_limit):
            return False
        if token != self.batch_token:
            self.mask = None
            self.mask = key_membership(column, self.arrow_values)
            self.batch_token = token
        return not self.mask[index].as_py()


def merge_join_rows(
    left: Mapping[str, Any],
    right: Mapping[str, Any],
    lk: tuple[str, ...],
    rk: tuple[str, ...],
    suffix: str,
) -> dict[str, Any]:
    out = dict(left)
    for key, value in right.items():
        if key in lk and key in rk and key in out and out[key] == value:
            continue
        dest = key if key not in out else key + suffix
        if dest in out:
            raise ValueError('join column collision: ' + dest)
        out[dest] = value
    return out


def match_probe(spool, key, matches, how, load_left, *left_position):
    """Apply inner/left/semi/anti multiplicity before hydrating either payload."""
    left = None
    try:
        for offset, header in matches:
            if how == 'anti':
                return
            if left is None:
                left = load_left(*left_position)
            if how == 'semi':
                yield left, None
                return
            yield left, spool.matched_record(key, offset, header)
        if left is None and how in ('left', 'anti'):
            yield load_left(*left_position), None
    finally:
        spool.release_probe()


def hash_join_pairs(spool, left, right, *, how, shift=JOIN_PARTITION_BITS):
    """Match two finite spool chains, reading payloads only when the join emits rows."""
    if not left.count:
        return
    if not right.count:
        if how in ('left', 'anti'):
            for offset, header in spool.entries(left):
                yield spool.record(offset, header), None
        return
    index = spool.index(right)
    if index is None and shift < 64:
        first_hash = None
        for _, header in spool.entries(right):
            if first_hash is None:
                first_hash = header[1]
            elif first_hash != header[1]:
                break
        else:
            shift = 64  # A single hot digest cannot benefit from another pass.
        if shift < 64:
            left_children, right_children = spool.partition(left, shift), spool.partition(right, shift)
            for lpart, rpart in zip(left_children, right_children):
                yield from hash_join_pairs(spool, lpart, rpart, how=how, shift=shift + JOIN_PARTITION_BITS)
            return
    for loffset, lheader in spool.entries(left):
        key = spool.probe_key(loffset, lheader)
        if not key:
            matches = ()
        elif index is not None:
            matches = index.get(key, ())
        else:
            # Hash prefix collisions or a single hot key cannot be subdivided.
            # Only headers and the current key/row are resident during this scan.
            matches = ((offset, header) for offset, header in spool.entries(right)
                if header[1] == lheader[1] and spool.key(offset, header) == key)
        yield from match_probe(spool, key, matches, how, spool.hold_probe_record, loffset, lheader)


def project_columns(columns, kind, configuration):
    """Resolve row-preserving column operations without decoding business values."""
    from collections.abc import Mapping
    if kind == 'SelectColumnsOp':
        names = tuple(configuration['columns'])
        if any(name not in columns for name in names) or len(names) != len(set(names)):
            return None  # Let the original row path decide its error/collision semantics.
        return tuple(columns.index(name) for name in names), names
    if kind == 'DropColumnsOp':
        removed = configuration['columns']
        if any(name not in columns for name in removed):
            return None
        indices = tuple(i for i, name in enumerate(columns) if name not in removed)
        return indices, tuple(columns[i] for i in indices)
    if kind == 'RenameColumnsOp':
        names = configuration['names']
        names = tuple(names.get(name, name) for name in columns) if isinstance(names, Mapping) else tuple(names)
        if len(names) != len(columns) or len(names) != len(set(names)):
            return None
        return tuple(range(len(columns))), names
    return None


def key_token(value: Any) -> Any:
    """Type-tagged encoding keeping -0.0/bytes/tuple/IEEE distinct (C01/C06)."""
    if value is None:
        return ["n"]
    if isinstance(value, bool):
        return ["b", value]
    if isinstance(value, int):
        return ["i", value]
    if isinstance(value, float):
        return ["f", repr(value)]
    if isinstance(value, str):
        return ["s", value]
    if isinstance(value, bytes):
        return ["y", value.hex()]
    if isinstance(value, tuple):
        return ["t", [key_token(item) for item in value]]
    if isinstance(value, list):
        return ["l", [key_token(item) for item in value]]
    if isinstance(value, Mapping):
        items = sorted(value.items(), key=lambda kv: _canonical(key_token(kv[0])))
        return ["d", [[key_token(k), key_token(v)] for k, v in items]]
    raise TypeError(f'Object of type {type(value).__name__} is not JSON serializable')

def group_key(row: Mapping[str, Any], fields: tuple[str, ...]) -> str:
    return _canonical([key_token(row[field]) for field in fields])

def group_key_values(raw):
    """Recover the typed sort columns from a canonical finite join key."""
    def value(token):
        kind = token[0]
        if kind == 'n':
            return None
        payload = token[1]
        if kind in ('s', 'i', 'b'):
            return payload
        if kind == 'f':
            return float(payload)
        if kind == 'y':
            return bytes.fromhex(payload)
        if kind == 'l':
            return [value(item) for item in payload]
        if kind == 't':
            return tuple(value(item) for item in payload)
        if kind == 'd':
            return {value(k): value(v) for k, v in payload}
        raise DataflowError('CORRUPT_ARTIFACT', 'unknown tag in a finite join key')
    return tuple(value(token) for token in json.loads(raw))


def reduce_ordered_groups(ordered, *, reducer, initial, fn_kwargs, validation,
                          memory, bounds, output, output_roots, check_stop, retain=None):
    """Original sequential key-group fold over rows and explicit bound ports.

    Group order, initial copying, state validation and suspended-yield accounting
    remain here. The partition binding owns its output record and runtime budget.
    """
    from demiflow.operators.reducer_memory import check_initial, check_state, yielded_peers
    max_state = bounds.max_state_bytes
    sequence = 0

    missing = object()
    current_key, state = missing, None
    iterator = iter(ordered)
    try:
        # Do not let itertools.groupby keep the previous row alive while the
        # producer hydrates the next one. Only the group key and state survive.
        for key,row in iterator:
            check_stop()
            if current_key is missing or key != current_key:
                if current_key is not missing and state is not None:
                    out=output(state,current_key,sequence)
                    sequence+=1
                    if memory is not None and retain is not None:
                        retain(yielded_peers(initial,state,output_roots(out),memory,bounds,
                                            pending=(row,current_key,key)))
                    try:
                        yield out
                    finally:
                        if retain is not None:
                            retain(0)
                    del out
                state=None  # previous group cannot coexist with the next deepcopy
                current_key=key
                if memory is not None:
                    check_initial(initial,memory)
                state=copy.deepcopy(initial)
            if validation is not None:
                validation.inputs((state,row),fn_kwargs)
            state=reducer(state,row,**fn_kwargs)
            if validation is not None:
                state=validation.output(state)
            if memory is not None:
                check_state(state,memory,serialized_limit=max_state)
            elif max_state>0 and len(pickle.dumps(state,protocol=pickle.HIGHEST_PROTOCOL))>max_state:
                raise DataflowError('BUDGET_EXCEEDED','reducer state exceeds max_state_bytes')
            del row
        if current_key is not missing and state is not None:
            out=output(state,current_key,sequence)
            if memory is not None and retain is not None:
                retain(yielded_peers(initial,state,output_roots(out),memory,bounds))
            yield out
    finally:
        if retain is not None:
            retain(0)
        close=getattr(iterator,'close',None)
        if close is not None:
            close()


def select_columns(row, columns):
    """Original Python row projection; a missing column raises its KeyError."""
    return {column: row[column] for column in columns}

def drop_columns(row, columns):
    """Original checked row projection, independent of record identity/schema."""
    missing = [column for column in columns if column not in row]
    if missing:
        raise DataflowError("INVALID_CONTRACT", "drop column missing", context={"fields": missing})
    return {key: value for key, value in row.items() if key not in columns}

def rename_columns(row, names):
    """Original mapping/positional rename; retain their distinct validation."""
    if isinstance(names, Mapping):
        targets = [names.get(key, key) for key in row]
        if len(targets) != len(set(targets)):
            raise DataflowError("INVALID_CONTRACT", "rename produces duplicate column names")
        return {names.get(key, key): value for key, value in row.items()}
    else:
        targets = tuple(names)
        if len(row) != len(targets):
            raise DataflowError("INVALID_CONTRACT", "rename list length must match schema")
        return dict(zip(targets, row.values()))


def add_column_row(row, column, values):
    if len(values) != 1:
        raise DataflowError('INVALID_CONTRACT', 'add_column result length must match batch')
    return {**row, column: values[0]}


def add_column_rows(rows, column, values, *, schema, on_schema):
    if len(values) != len(rows):
        raise DataflowError('INVALID_CONTRACT', 'add_column result length must match batch')
    if schema is not None:
        import pyarrow as pa
        fields = [field for field in schema if field.name != column]
        fields.append(pa.field(column, pa.array(values).type))
        on_schema(pa.schema(fields, metadata=schema.metadata))
    return [{**row, column: value} for row, value in zip(rows, values)]
