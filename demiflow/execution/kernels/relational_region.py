"""One finite same-key computation over an original worker/store grant.

Leaf documents are written once. Intermediate frames contain only offsets,
original occurrence and encoded-size evidence; they never persist another copy
of a wide row. Public Record/transport and original ACK formats are unchanged.
"""
from dataclasses import dataclass
import struct

from .join_spool import _Spool, _Chain, _pairs, _HEADER, _ID_BYTES, _NONE, _FANOUT
from .dataflow import (_check_stop, _err, _materialize_row, _row_bytes, _output_record,
                       _hash_id, RECORD_DOMAIN, _OutputBudget)

_REF = struct.Struct('!qqIQ')


@dataclass(frozen=True)
class _RowReference:
    offset: int
    header: tuple


class _RegionSpool(_Spool):
    def __init__(self, file, services, specs, counters):
        super().__init__(file, services)
        self.specs, self.counters = specs, counters
        # No second independently sized hash cache. Only one _pairs call is
        # live; this bounded index shares the explicitly admitted region grant.
        self.index_limit = max(1, services.bounds.max_row_bytes // 4)

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
            return _materialize_row(record, self.services), record.schema_ref
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
        from .relational import _merge_row
        return _merge_row(lrow, rrow, tuple(spec.configuration['on']),
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
            from .relational import _merge_row
            rrow = self.hydrate(right)
            row = _merge_row(lrow, rrow, tuple(spec.configuration['on']),
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
        header = _HEADER.pack(_NONE, hashed, len(key), len(doc), sequence)
        raw = header + record_id.encode('ascii') + key + doc
        offset = self.file.append(raw)
        self.counters['intermediate_reference_bytes'] += len(raw)
        return offset, hashed


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
        self.completed = None
        self.on_complete_member = None
        self.on_input_member = None
        self.active = False
        from ...planning.dataflow_relational_regions import region_resident_bytes
        stage = next(s for s in services.assignment.stages if s.stage_id == plan.region_id)
        if min(stage.resident_bytes, stage.profile.requests['memory_bytes']) < region_resident_bytes(plan, services.bounds):
            raise _err('RESOURCE_INFEASIBLE', 'region lacks its admitted live-object window')
        previous = (self.root._image or {}).get('relational_region')
        if previous is not None:
            if (type(previous) is not dict or set(previous) != {'region_id', 'counters'}
                    or previous['region_id'] != plan.region_id
                    or set(previous['counters']) != set(self.counters)
                    or any(type(v) is not int or v < 0 for v in previous['counters'].values())):
                raise _err('INCOMPATIBLE_HISTORY', 'region image does not match sealed compute region')
            self.counters.update(previous['counters'])

    def snapshot(self):
        return {'region_id': self.plan.region_id, 'counters': dict(self.counters)}

    def records(self):
        bounds = self.services.bounds
        store = self.services.store
        from ..dataflow_store import RunArtifactStore
        if isinstance(store, RunArtifactStore):
            store.require_file_slots(store.SCRATCH_FILES + 2 * store.READ_FILES,
                                     maximum=bounds.max_open_files)
        elif bounds.max_open_files < 2:
            raise _err('RESOURCE_INFEASIBLE', 'relational region needs a scratch file and owner lease')
        self.active = True
        self.counters = {key: 0 for key in self.counters}
        specs = [self.kernels[o]._spec for o in self.plan.occurrences]
        with store.scratch(max_bytes=bounds.max_spill_bytes,
                           max_io_bytes=max(1, bounds.max_row_bytes * 2)) as file:
            arena = _RegionSpool(file, self.services, specs, self.counters)
            relations = {}
            try:
                for entry in self.plan.inputs:
                    kernel = self.kernels[entry.consumer]
                    records = kernel._ports[entry.port].records().for_join(entry.fields)
                    chains = [_Chain() for _ in range(_FANOUT)]
                    self.counters['partition_builds'] += 1
                    try:
                        for record in records:
                            offset, hashed = arena.append(record, entry.fields)
                            chains[hashed & (_FANOUT - 1)].link(arena, offset)
                            self.counters['leaf_key_rows'] += 1
                            self.counters['leaf_payload_bytes'] += arena.header(offset)[3]
                    finally:
                        records.close()
                    relations[(entry.consumer, entry.port)] = chains
                for index, occurrence in enumerate(self.plan.occurrences):
                    spec = specs[index]
                    if self.on_input_member is not None:
                        self.on_input_member(occurrence, sum(chain.count
                            for port in self.kernels[occurrence]._port_order
                            for chain in relations[(occurrence, port)]))
                    if spec.kind == 'reduce':
                        source = relations[(occurrence, 'left')]
                        yield from self._reduce(arena, source, spec)
                        break
                    left, right = relations[(occurrence, 'left')], relations[(occurrence, 'right')]
                    outputs = [_Chain() for _ in range(_FANOUT)]
                    budget = _OutputBudget(spec, self.services, streaming=True)
                    sequence = 0
                    for lpart, rpart in zip(left, right):
                        for lref, rref in _pairs(arena, lpart, rpart,
                                how=spec.configuration.get('how', 'inner'), shift=4):
                            _check_stop(self.services)
                            offset, hashed = arena.append_reference(lref, rref,
                                index=index, sequence=sequence, budget=budget)
                            outputs[hashed & (_FANOUT - 1)].link(arena, offset)
                            sequence += 1
                    for consumer, port in self.internal.get(occurrence, ()):
                        relations[(consumer, port)] = outputs
                    kernel = self.kernels[occurrence]
                    if occurrence != self.plan.exit_occurrence:
                        kernel._emitted = sequence
                        if self.on_complete_member is not None:
                            self.on_complete_member(occurrence, sequence,
                                sum(chain.count for chain in left) + sum(chain.count for chain in right))
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

    def _reduce(self, arena, chains, spec):
        from .relational import _group_sort_spec, reduce_ordered_groups, admit_reducer
        from .ordering import build_sort_runs, merge_sort_runs
        from .reducer_memory import sort_window
        from .dataflow import _group_key
        keys = tuple(spec.configuration['on'])
        admit_reducer(spec,self.services,join_depth=self.plan.join_depth)
        # Sort only a bounded key/offset record; wide payload remains in arena.
        def references():
            for chain in chains:
                for offset, header in arena.entries(chain):
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
        def ordered():
            for record in merge_sort_runs(runs, spec=sort_spec, services=self.services, window=window):
                offset = record.sequence
                row = arena.hydrate(_RowReference(offset, arena.header(offset)))
                yield _group_key(row, keys), row, record
                del row,record
        yield from reduce_ordered_groups(ordered(), spec=spec, services=self.services,
            memory_join_depth=self.plan.join_depth,retain=self.root._retain_reducer_peers)
