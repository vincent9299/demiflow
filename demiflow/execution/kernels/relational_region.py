"""One finite same-key computation over an original worker/store grant.

Leaf documents are written once. Intermediate frames contain only offsets,
original occurrence and encoded-size evidence; they never persist another copy
of a wide row. Public Record/transport and original ACK formats are unchanged.
"""

from .join_spool import bind_spool
from demiflow.operators.join_spool import JoinChain, PARTITIONS
from demiflow.operators.relational_region import RegionSpool, RowReference, offset_query, matching_references
from demiflow.execution.kernels.dataflow import _check_stop, _err, _output_record, RECORD_DOMAIN, _OutputBudget
from demiflow.execution.contracts.record_codec import record_identity as _hash_id
from demiflow.execution.storage.streams import materialize_row as materialize_row


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
            def hydrate_record(record):
                return materialize_row(record, store=store, bounds=bounds,
                    assignment=getattr(self.services, 'assignment', None)), record.schema_ref
            configurations = [spec.configuration for spec in specs]
            arena = bind_spool(file, self.services, spool_type=RegionSpool,
                configurations=configurations, counters=self.counters, hydrate_record=hydrate_record,
                reference_id=lambda index, sequence: _hash_id([
                    RECORD_DOMAIN, specs[index].configuration['occurrence_id'], 'join', sequence]))
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
                native = offset_query(arena, relations, configurations, occurrences=self.plan.occurrences,
                    query=self.services.query_region_offsets, check_stop=self.services.check_stop, counters=self.counters) if self.plan.sql_offsets else None
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
                    outputs = [JoinChain() for _ in range(PARTITIONS)]
                    budget = _OutputBudget(spec, self.services, streaming=True)
                    def admit_size(size):
                        class Sized:
                            encoded_bytes = size
                        budget.charge(Sized())
                    sequence = 0
                    for lref, rref in matching_references(arena, left, right, spec.configuration, native, intermediates, index):
                        _check_stop(self.services)
                        ref = arena.append_reference(lref, rref, index=index, sequence=sequence, admit_size=admit_size)
                        outputs[ref.header[1] & (PARTITIONS - 1)].link(arena, ref.offset)
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
                                ref = RowReference(offset, header)
                                row, schema = arena.hydrate_with_schema(ref)
                                rid = _hash_id([RECORD_DOMAIN, occurrence, 'join', header[4]])
                                yield _output_record(rid, {'join_index': header[4]}, row, header[4], self.services,
                                                     schema_ref=schema)
            finally:
                self.active = False

    def _ordered_groups(self, arena, chains, spec):
        from .relational import _group_sort_spec, reduce_ordered_groups, batch_ordered_groups, admit_reducer
        from .ordering import build_sort_runs, merge_sort_runs
        from demiflow.operators.ordering import reducer_sort_window as sort_window
        from demiflow.operators.relational import group_key as group_key, group_key_values
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
                        row = arena.hydrate(RowReference(offset, header))
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
                row = arena.hydrate(RowReference(offset, header))
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
