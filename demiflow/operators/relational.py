"""Join key membership, matching, row combination and native offset queries."""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any


JOIN_PARTITION_BITS = 4


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
            import pyarrow.compute as pc
            self.mask = None
            self.mask = pc.is_in(column, value_set=self.arrow_values)
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



# Tables contain the same canonical binary keys used by hash matching. A SQL
# NULL on either side never matches. Left duplicates have distinct offsets.
EXCLUDE_OFFSETS_SQL = """
    SELECT l.off FROM lkeys l LEFT ANTI JOIN rkeys r ON l.k = r.k
"""


def offset_join_query(first, second, maximum):
    """One bounded query for a left-deep three-way join, including logical intermediate pairs."""
    join = {'left': 'LEFT JOIN', 'inner': 'INNER JOIN'}
    if type(maximum) is not int or maximum < 1:
        raise ValueError('offset query requires a positive result ceiling')
    return f"""
      WITH middle AS (
          SELECT a.k, a.off AS ao, b.off AS bo, a.ord AS ai, b.ord AS bi
          FROM akeys a {join[first]} bkeys b ON a.k = b.k
      ), result AS (
          SELECT CAST(0 AS BIGINT) AS phase, ao, bo, CAST(NULL AS BIGINT) AS co,
                 ai, bi, CAST(NULL AS BIGINT) AS ci FROM middle
          UNION ALL
          SELECT CAST(1 AS BIGINT), m.ao, m.bo, c.off, m.ai, m.bi, c.ord
          FROM middle m {join[second]} ckeys c ON m.k = c.k
      )
      SELECT phase, ao, bo, co FROM result ORDER BY phase, ai, bi, ci LIMIT {maximum + 1}
    """


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
