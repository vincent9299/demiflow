"""Relational region implementation."""
from __future__ import annotations

from dataclasses import dataclass
import struct
from demiflow.operators.join_spool import JoinSpool, SPOOL_HEADER, RECORD_ID_BYTES, NO_OFFSET
from demiflow.operators.relational import hash_join_pairs, merge_join_rows
from demiflow.operators.relational_sql import offset_join_query
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.record_codec import row_bytes as _row_bytes

_REF = struct.Struct('!qqIQ')


@dataclass(frozen=True)
class RowReference:
    offset: int
    header: tuple

class RegionSpool(JoinSpool):
    def __init__(self, file, *, configurations, counters, hydrate_record, reference_id, **bindings):
        super().__init__(file, **bindings)
        self.configurations, self.counters = configurations, counters
        self.hydrate_record, self.reference_id = hydrate_record, reference_id
        self.max_row_bytes = bindings['max_row_bytes']
        # No second independently sized hash cache. Only one hash join probe is
        # live; this bounded index shares the explicitly admitted region grant.
        self.index_limit = max(1, bindings['max_row_bytes'] // 4)

    def append(self, record, fields, **filters):
        entry = super().append(record, fields, **filters)
        self.counters['leaf_key_rows'] += 1
        if entry is not None:
            self.counters['leaf_payload_bytes'] += self.header(entry[0])[3]
        return entry

    def record(self, offset, header):
        return RowReference(offset, header)

    def hydrate(self, ref):
        return self.hydrate_with_schema(ref)[0]

    def hydrate_with_schema(self, ref, depth=0):
        if depth > 2:
            raise DataflowError('CORRUPT_ARTIFACT', 'region reference exceeds sealed join depth')
        start = ref.offset + SPOOL_HEADER.size + RECORD_ID_BYTES + ref.header[2]
        tag = self.file.read_at(start, 1)
        if tag != b'@':
            record = super().record(ref.offset, ref.header)
            return self.hydrate_record(record)
        if ref.header[3] != 1 + _REF.size:
            raise DataflowError('CORRUPT_ARTIFACT', 'invalid region reference frame')
        left, right, index, size = _REF.unpack(self.file.read_at(start + 1, _REF.size))
        if not 0 <= left < ref.offset or right != NO_OFFSET and not 0 <= right < ref.offset or index >= len(self.configurations):
            raise DataflowError('CORRUPT_ARTIFACT', 'invalid region reference ancestry')
        lrow, schema = self.hydrate_with_schema(RowReference(left, self.header(left)), depth + 1)
        configuration = self.configurations[index]
        if right == NO_OFFSET:
            return lrow, schema if configuration.get('how', 'inner') in ('semi', 'anti') else None
        rrow, _ = self.hydrate_with_schema(RowReference(right, self.header(right)), depth + 1)
        return merge_join_rows(lrow, rrow, tuple(configuration['on']),
                          tuple(configuration.get('right_on') or configuration['on']),
                          configuration.get('suffix', '_right')), None

    def append_reference(self, left, right, *, index, sequence, admit_size):
        configuration = self.configurations[index]
        # Preserve validation at the original logical output boundary. This
        # bounded transient size encoding is counted separately; no encoded
        # intermediate payload is retained or sent through another finite port.
        lrow = self.hydrate(left)
        if right is None:
            row = lrow
        else:
            rrow = self.hydrate(right)
            row = merge_join_rows(lrow, rrow, tuple(configuration['on']),
                             tuple(configuration.get('right_on') or configuration['on']),
                             configuration.get('suffix', '_right'))
        size = _row_bytes(row, max_bytes=self.max_row_bytes)
        del row, lrow
        if right is not None:
            del rrow
        admit_size(size)
        self.counters['intermediate_validation_bytes'] += size
        key = self.key(left.offset, left.header)
        hashed = left.header[1]  # Inherit the exact original key partition.
        doc = b'@' + _REF.pack(left.offset, right.offset if right is not None else NO_OFFSET, index, size)
        record_id = self.reference_id(index, sequence)
        fields = (NO_OFFSET, hashed, len(key), len(doc), sequence)
        raw = SPOOL_HEADER.pack(*fields) + record_id.encode('ascii') + key + doc
        offset = self.file.append(raw)
        self.counters['intermediate_reference_bytes'] += len(raw)
        return RowReference(offset, fields)

def offset_query(arena, relations, configurations, *, occurrences, query, check_stop, counters):
    """Read only retained keys/offsets. Never run an unbounded join or sort."""
    from demiflow.execution.contracts.relational_region import SQL_CHILD_RSS, SQL_PARENT_WINDOW
    first, second = occurrences
    a, b, c = relations[(first, 'left')], relations[(first, 'right')], relations[(second, 'right')]
    kinds = tuple(configuration.get('how', 'inner') for configuration in configurations)
    # Same digest bucket is necessary for a match. This conservative bound
    # covers the complete un-limited joins *and* ordering, including nulls
    # and left preservation. A single hot key also stays bounded.
    upper = 0
    for ai, bi, ci in zip(a, b, c):
        middle = ai.count * max(bi.count, int(kinds[0] == 'left'))
        upper += middle + middle * max(ci.count, int(kinds[1] == 'left'))
    if not 0 < upper <= SQL_PARENT_WINDOW // 2048:
        return None
    tables, leaf_refs, retained = {}, {}, 0
    for name, chains in (('akeys', a), ('bkeys', b), ('ckeys', c)):
        table = []
        for chain in chains:
            index_bytes = 0
            for offset, header in arena.entries(chain):
                size = header[2]
                # Same-partition traversal order as the bounded hash path.
                # A hash partition that would split must keep its own order.
                if size and name != 'akeys':
                    index_bytes += 512 + size
                    if index_bytes > arena.index_limit:
                        return None
                # Bound retained header/reference, hash-table entry, encoded
                # key and the host's simultaneous serialization copy.
                retained += 768 + 4 * size
                if retained > SQL_PARENT_WINDOW // 8:
                    return None
                table.append((arena.key(offset, header) if size else None, offset))
                leaf_refs[offset] = RowReference(offset, header)
        tables[name] = table
    if query is None:
        raise DataflowError('UNSUPPORTED_CAPABILITY', 'sealed native offset query needs its worker host')
    pairs = query(offset_join_query(*kinds, upper), tables=tables,
        max_pairs=upper, rss_bytes=SQL_CHILD_RSS, check_stop=check_stop)
    counters['offset_sql_queries'] += 1
    counters['offset_sql_inputs'] += sum(len(table) for table in tables.values())
    counters['offset_sql_pairs'] += len(pairs)
    return pairs, leaf_refs

def matching_references(arena, left, right, configuration, native, intermediates, index):
    if native is None:
        for lpart, rpart in zip(left, right):
            yield from hash_join_pairs(arena, lpart, rpart, how=configuration.get('how', 'inner'))
        return
    # Both SQL joins share one process and one bounded response. First
    # validate *every* logical intermediate under its original node's row
    # and output constraints, even when the final join discards that row.
    pairs, leaf_refs = native
    for phase, ao, bo, co in pairs:
        if phase != index:
            continue
        if index == 0:
            yield leaf_refs[ao], None if bo is None else leaf_refs[bo]
        else:
            yield intermediates[(ao, bo)], None if co is None else leaf_refs[co]
