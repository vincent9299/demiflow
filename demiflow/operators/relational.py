"""Join key membership, matching, row combination and native offset queries."""
from __future__ import annotations

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
