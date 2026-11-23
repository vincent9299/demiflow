"""One finite same-key computation over an original worker/store grant.

Leaf documents are written once. Intermediate frames contain only offsets,
original occurrence and encoded-size evidence; they never persist another copy
of a wide row. Public Record/transport and original ACK formats are unchanged.
"""
from dataclasses import dataclass
import struct

from ...operators.relational import hash_join_pairs, merge_join_rows
from ...operators.relational_sql import offset_join_query
from .join_spool import _Spool, _Chain, _HEADER, _ID_BYTES, _NONE, _FANOUT
from .dataflow import _check_stop, _err, _row_bytes, _output_record, _hash_id, RECORD_DOMAIN, _OutputBudget
from .dataflow import _materialize_row as materialize_row

_REF = struct.Struct('!qqIQ')


@dataclass(frozen=True)
class _RowReference:
    offset: int
    header: tuple


class _RegionSpool(_Spool):
    def __init__(self, file, services, specs, counters):
        super().__init__(file, services)
        self.specs, self.counters = specs, counters
        # No second independently sized hash cache. Only one hash join probe is
        # live; this bounded index shares the explicitly admitted region grant.
        self.index_limit = max(1, services.bounds.max_row_bytes // 4)

    def append(self, record, fields, **filters):
        entry = super().append(record, fields, **filters)
        self.counters['leaf_key_rows'] += 1
        if entry is not None:
            self.counters['leaf_payload_bytes'] += self.header(entry[0])[3]
        return entry

    def record(self, offset, header):
        return _RowReference(offset, header)

    def hydrate(self, ref):
        return self.hydrate_with_schema(ref)[0]

    def hydrate_with_schema(self, ref, depth=0):
        if depth > 2:
            raise _err('CORRUPT_ARTIFACT', 'region reference exceeds sealed join depth')
        start = ref.offset + _HEADER.size + _ID_BYTES + ref.header[2]
        tag = self.file.read_at(start, 1)
        if tag != b'@':
            record = super().record(ref.offset, ref.header)
            return materialize_row(record, self.services), record.schema_ref
        if ref.header[3] != 1 + _REF.size:
            raise _err('CORRUPT_ARTIFACT', 'invalid region reference frame')
        left, right, index, size = _REF.unpack(self.file.read_at(start + 1, _REF.size))
        if not 0 <= left < ref.offset or right != _NONE and not 0 <= right < ref.offset or index >= len(self.specs):
            raise _err('CORRUPT_ARTIFACT', 'invalid region reference ancestry')
        lrow, schema = self.hydrate_with_schema(_RowReference(left, self.header(left)), depth + 1)
        spec = self.specs[index]
        if right == _NONE:
            return lrow, schema if spec.configuration.get('how', 'inner') in ('semi', 'anti') else None
        rrow, _ = self.hydrate_with_schema(_RowReference(right, self.header(right)), depth + 1)
        return merge_join_rows(lrow, rrow, tuple(spec.configuration['on']),
                          tuple(spec.configuration.get('right_on') or spec.configuration['on']),
                          spec.configuration.get('suffix', '_right')), None

    def append_reference(self, left, right, *, index, sequence, budget):
        spec = self.specs[index]
        # Preserve validation at the original logical output boundary. This
        # bounded transient size encoding is counted separately; no encoded
        # intermediate payload is retained or sent through another finite port.
        lrow = self.hydrate(left)
        if right is None:
            row = lrow
        else:
            rrow = self.hydrate(right)
            row = merge_join_rows(lrow, rrow, tuple(spec.configuration['on']),
                             tuple(spec.configuration.get('right_on') or spec.configuration['on']),
                             spec.configuration.get('suffix', '_right'))
        size = _row_bytes(row, max_bytes=self.services.bounds.max_row_bytes)
        del row, lrow
        if right is not None:
            del rrow
        class Sized:
            encoded_bytes = size
        budget.charge(Sized())
        self.counters['intermediate_validation_bytes'] += size
        key = self.key(left.offset, left.header)
        hashed = left.header[1]  # Inherit the exact original key partition.
        doc = b'@' + _REF.pack(left.offset, right.offset if right is not None else _NONE, index, size)
        record_id = _hash_id([RECORD_DOMAIN, spec.configuration['occurrence_id'], 'join', sequence])
        fields = (_NONE, hashed, len(key), len(doc), sequence)
        raw = _HEADER.pack(*fields) + record_id.encode('ascii') + key + doc
        offset = self.file.append(raw)
        self.counters['intermediate_reference_bytes'] += len(raw)
        return _RowReference(offset, fields)


class RelationalRegionRuntime:
    def __init__(self, plan, *, kernels, services, edges):
        self.plan, self.kernels, self.services = plan, kernels, services
        self.internal = {}
        for edge in edges:
            if edge.edge_id in plan.internal_edges:
                self.internal.setdefault(edge.producer, []).append((edge.consumer, edge.input_port))
        self.root = kernels[plan.exit_occurrence]
        self.counters = dict(leaf_key_rows=0, leaf_payload_bytes=0, intermediate_payload_bytes=0,
                             intermediate_reference_bytes=0, intermediate_validation_bytes=0,
                             partition_builds=0, intermediate_key_rebuilds=0,
                             sort_runs=0, sort_window_bytes=0, sort_run_bytes=0,
                             sort_frame_bytes=0, sort_read_chunk_bytes=0, sort_fan_in_limit=0)
        if plan.sql_offsets:
            self.counters.update(offset_sql_queries=0, offset_sql_inputs=0, offset_sql_pairs=0)
        self.completed = None
        self.on_complete_member = None
        self.on_input_member = None
        self.active = False
        from ..contracts.relational_region import region_resident_bytes
        stage = next(s for s in services.assignment.stages if s.stage_id == plan.region_id)
        if min(stage.resident_bytes, stage.profile.requests['memory_bytes']) < region_resident_bytes(plan, services.bounds):
            raise _err('RESOURCE_INFEASIBLE', 'region lacks its admitted live-object window')

        previous = self.root.checkpoint_region_state()
        if previous is not None:
            self.restore(previous)

    def restore(self, previous):
        if (type(previous) is not dict or set(previous) != {'region_id', 'counters'}
                or previous['region_id'] != self.plan.region_id
                or set(previous['counters']) != set(self.counters)
                or any(type(v) is not int or v < 0 for v in previous['counters'].values())):
            raise _err('INCOMPATIBLE_HISTORY', 'region image does not match sealed compute region')
        self.counters.update(previous['counters'])

    def snapshot(self):
        return {'region_id': self.plan.region_id, 'counters': dict(self.counters)}

    def _offset_query(self, arena, relations, specs):
        """Read only retained keys/offsets. Never run an unbounded join or sort."""
        from ..contracts.relational_region import SQL_CHILD_RSS, SQL_PARENT_WINDOW
        first, second = self.plan.occurrences
        a, b, c = relations[(first, 'left')], relations[(first, 'right')], relations[(second, 'right')]
        kinds = tuple(spec.configuration.get('how', 'inner') for spec in specs)
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
                    leaf_refs[offset] = _RowReference(offset, header)
            tables[name] = table
        if self.services.query_region_offsets is None:
            raise _err('UNSUPPORTED_CAPABILITY', 'sealed native offset query needs its worker host')
        pairs = self.services.query_region_offsets(offset_join_query(*kinds, upper), tables=tables,
            max_pairs=upper, rss_bytes=SQL_CHILD_RSS, check_stop=self.services.check_stop)
        self.counters['offset_sql_queries'] += 1
        self.counters['offset_sql_inputs'] += sum(len(table) for table in tables.values())
        self.counters['offset_sql_pairs'] += len(pairs)
        return pairs, leaf_refs

    def _matches(self, arena, left, right, spec, native, intermediates, index):
        if native is None:
            for lpart, rpart in zip(left, right):
                yield from hash_join_pairs(arena, lpart, rpart, how=spec.configuration.get('how', 'inner'))
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

    def records(self):
        bounds = self.services.bounds
        store = self.services.store
        from ..storage.objects import RunArtifactStore
        if isinstance(store, RunArtifactStore):
            store.require_file_slots(store.SCRATCH_FILES + 2 * store.READ_FILES,
                                     maximum=bounds.max_open_files)
        elif bounds.max_open_files < 2:
            raise _err('RESOURCE_INFEASIBLE', 'relational region needs a scratch file and owner lease')
        self.active = True
        self.counters = {key: 0 for key in self.counters}
        specs = [self.kernels[o].spec for o in self.plan.occurrences]
        with store.scratch(max_bytes=bounds.max_spill_bytes,
                           max_io_bytes=max(1, bounds.max_row_bytes * 2)) as file:
            arena = _RegionSpool(file, self.services, specs, self.counters)
            relations = {}
            leaf_sizes = {}
            entries = {(entry.consumer, entry.port): entry for entry in self.plan.inputs}
            def read_leaf(entry, **filters):
                member = (entry.consumer, entry.port)
                records = self.kernels[entry.consumer].finite_join_records(entry.port, entry.fields)
                self.counters['partition_builds'] += 1
                try:
                    relations[member], leaf_sizes[member] = arena.partition_input(records, entry.fields, **filters)
                finally:
                    records.close()
            def input_size(occurrence, port):
                member = (occurrence, port)
                if member in leaf_sizes:
                    return leaf_sizes[member]
                return sum(chain.count for chain in relations[member])
            try:
                for entry in self.plan.inputs:
                    if (entry.consumer, entry.port) in relations:
                        continue
                    right = entries.get((entry.consumer, 'right'))
                    left = entries.get((entry.consumer, 'left'))
                    spec = self.kernels[entry.consumer].spec
                    # Only complete external inputs can build this filter. An
                    # internal producer's references are built later in order.
                    membership = (arena.negative_filter(right.fields, spec.configuration.get('how', 'inner'))
                                  if left and right and spec.kind == 'join' else None)
                    if membership is None:
                        read_leaf(entry)
                    else:
                        read_leaf(right, build_filter=membership)
                        membership.finish()
                        read_leaf(left, probe_filter=membership)
                        del membership
                native = self._offset_query(arena, relations, specs) if self.plan.sql_offsets else None
                intermediates = {}
                for index, occurrence in enumerate(self.plan.occurrences):
                    spec = specs[index]
                    if self.on_input_member is not None:
                        self.on_input_member(occurrence, sum(input_size(occurrence, port.name) for port in spec.inputs))
                    if spec.kind in ('reduce', 'groups'):
                        source = relations[(occurrence, 'left')]
                        yield from self._ordered_groups(arena, source, spec)
                        break
                    left, right = relations[(occurrence, 'left')], relations[(occurrence, 'right')]
                    outputs = [_Chain() for _ in range(_FANOUT)]
                    budget = _OutputBudget(spec, self.services, streaming=True)
                    sequence = 0
                    for lref, rref in self._matches(arena, left, right, spec, native, intermediates, index):
                        _check_stop(self.services)
                        ref = arena.append_reference(lref, rref, index=index, sequence=sequence, budget=budget)
                        outputs[ref.header[1] & (_FANOUT - 1)].link(arena, ref.offset)
                        if native is not None and index == 0:
                            intermediates[(lref.offset, rref.offset if rref else None)] = ref
                        sequence += 1
                    for consumer, port in self.internal.get(occurrence, ()):
                        relations[(consumer, port)] = outputs
                    kernel = self.kernels[occurrence]
                    if occurrence != self.plan.exit_occurrence:
                        kernel.complete_region_member(sequence)
                        if self.on_complete_member is not None:
                            self.on_complete_member(occurrence, sequence,
                                input_size(occurrence, 'left') + input_size(occurrence, 'right'))
                    else:
                        for chain in outputs:
                            for offset, header in arena.entries(chain):
                                ref = _RowReference(offset, header)
                                row, schema = arena.hydrate_with_schema(ref)
                                rid = _hash_id([RECORD_DOMAIN, occurrence, 'join', header[4]])
                                yield _output_record(rid, {'join_index': header[4]}, row, header[4], self.services,
                                                     schema_ref=schema)
            finally:
                self.active = False

    def _ordered_groups(self, arena, chains, spec):
        from .relational import _group_sort_spec, reduce_ordered_groups, batch_ordered_groups, admit_reducer
        from .ordering import build_sort_runs, merge_sort_runs
        from .reducer_memory import sort_window
        from .dataflow import _group_key as group_key, group_key_values
        keys = tuple(spec.configuration['on'])
        if spec.kind == 'reduce':
            admit_reducer(spec,self.services,join_depth=self.plan.join_depth)
        # Sort only a bounded key/offset record; wide payload remains in arena.
        def references():
            for chain in chains:
                for offset, header in arena.entries(chain):
                    token = arena.key(offset, header)
                    if token:
                        keyrow = dict(zip(keys, group_key_values(token), strict=True))
                    elif len(keys) == 1:
                        keyrow = {keys[0]: None}
                    else:
                        # SQL null never matches. A null-containing composite
                        # key has no join token; its other fields still sort.
                        row = arena.hydrate(_RowReference(offset, header))
                        keyrow = {key: row[key] for key in keys}
                        del row
                    rid = _hash_id(['region-sort-v1', self.plan.region_id, offset])
                    yield _output_record(rid, {'join_index': header[4]}, keyrow, offset, self.services)
        sort_spec = _group_sort_spec(spec, keys)
        window = sort_window(self.services.bounds,key_only=True)
        runs = build_sort_runs(references(), spec=sort_spec, services=self.services, window=window)
        self.counters.update(sort_runs=len(runs), sort_window_bytes=window.resident_bytes,
            sort_run_bytes=window.run_bytes, sort_frame_bytes=window.frame_bytes,
            sort_read_chunk_bytes=window.read_chunk_bytes, sort_fan_in_limit=window.merge_fan_in)
        null_key = group_key(dict.fromkeys(keys), keys) if len(keys) == 1 else None
        def ordered():
            for record in merge_sort_runs(runs, spec=sort_spec, services=self.services, window=window):
                offset = record.sequence
                header = arena.header(offset)
                token = arena.key(offset, header)
                row = arena.hydrate(_RowReference(offset, header))
                # A preserved nonnull key is already the exact equality token,
                # including scalar types. Group output still needs its row.
                group = token.decode('utf-8') if token else null_key or group_key(row, keys)
                yield group, row, record
                del row,record
        rows = ordered()
        try:
            if spec.kind == 'groups':
                yield from batch_ordered_groups(rows, spec=spec, services=self.services,
                    retained_limit=self.plan.group_batch_bytes(self.services.bounds))
            else:
                yield from reduce_ordered_groups(rows, spec=spec, services=self.services,
                    memory_join_depth=self.plan.join_depth,retain=self.root.retain_reducer_peers)
        finally:
            rows.close()
