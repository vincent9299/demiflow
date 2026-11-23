"""Business-thread execution of the assigned portion of a sealed graph.

Both hosts use this runner. Only edges in the physical plan may cross workers;
internal edges run once and preserve their declared input ports. The driver sees
bounded byte locators and progress/cut metadata, never calls an operator.
"""
from __future__ import annotations

import json
import tempfile
import threading
from contextvars import copy_context
import time
from collections import deque
from collections.abc import Mapping
from itertools import islice
from dataclasses import asdict

from ..contracts.dataflow import FIXED_SCAN_BATCHES, ArtifactRef, CutContribution, WorkerStep
from ..contracts.dataflow_errors import DataflowError
from ..exchange.dataflow_bridge import _encode_record_document, _decode_record_document
from ..exchange.dataflow_channels import reserve_credit, _route_record_document, snapshot_channels
from ..exchange.dataflow_transport import PeerBuffers, read_peer, read_peer_many, MAX_PEER_BATCH, peer_batch_limit
from ..dataflow_sinks import (_artifact_json, _encode_record, _manifest_non_mapping_values,
    RECORDS_MEDIA, MANIFEST_MEDIA)
from ..recovery_contract import document
from ..dataflow_record_frames import (encode_document, decode_document, document_limit,
    frame_parts, decode_frame, line_limit, frame_stream, read_chunk_bytes, admit_receive, read_document)


class WorkerGraphRunner:
    _fixed_key_exclusion = None

    def __init__(self, executor):
        self.e = executor
        self.services = executor._services
        self.bounds = self.services.bounds
        self.partition = executor.assignment.lease.partition_ids[0]
        self._queue = deque()
        # At most the sole driver's receipt and sealed-frontier recut can be pending.
        # Keep these bounded control messages reachable even when the next
        # generation fills the business queue behind a sealed frontier.
        self._cut_controls = deque()
        self._sealed_cut = None
        self._condition = threading.Condition()
        self._wake_generation = 0
        self._activity_generation = 0
        self._thread = None
        self._paused = bool(executor.assignment.source_splits)
        self._source_credit = None
        self._unconfirmed_rows = 0
        self._unconfirmed_bytes = 0
        self._cut_credit = {}
        self._publication_overlap = False
        self._park_requested = False
        self._park_finish_pending = False
        self._idle_sent = False
        self._done_sent = False
        self._finished = set()
        self._metrics = {occ: {'rows_input': 0, 'rows_output': 0} for occ in executor._node_specs}
        self._dirty_metrics = set()
        self._metrics_at = time.monotonic()
        self._observation_rows = 0
        self._source_rows_since_cut = 0
        self._finished_splits = set()
        self._finished_inputs = set()
        self._finished_ports = set()
        self._active_finishes = {}
        self._checkpoint_finishes = set()
        self._checkpoint_wait_requested = False
        self._checkpoint_wait_signature = None
        self._checkpoint_wait_sequence = 0
        self._park_wait_authorized = False
        self._checkpoint_model_calls = set()
        self._closed_inputs = set()
        self._closed_outputs = set()
        self._emitted_eoi = set()
        self._credits = {}
        self._credit_payloads = {}
        self._consumed_deliveries = set()
        self._memory_frontiers = {}
        physical = executor._physical
        from ..arrow_payload import batch_carrier
        carrier = batch_carrier(executor._graph, physical)
        for reader in executor._readers.values():
            reader.batch_carrier = carrier
            reader._shared_frame_call = self._frame_call
        self._park_mode = (any(e.mode == 'durable_incremental' for e in physical.exchanges)
            and sum(s.resident_bytes * s.replica_target for s in physical.stages)
                + physical.residency.control_reserved_bytes > self.bounds.max_resident_bytes)
        # Pause before reading a record that cannot be retained while the next
        # resident group is absent. Expansion inside one invocation is separately
        # rejected if it cannot fit the actual channel credit window.
        self._credit_limit = min(self.bounds.max_pending_events, 64)
        from ..dataflow_native_limits import sealed_execution_ceilings
        self._credit_limit = min(self._credit_limit,
            sealed_execution_ceilings(executor._graph).get('inflight_rows', self._credit_limit))
        self._terminals = {}
        self._finite = {}
        self._cut = None
        self._outgoing = {}
        self._incoming = {}
        self._exchange = {x.edge_id: x for x in executor._physical.exchanges}
        for edge in executor._graph.edges:
            spec = self._exchange[edge.edge_id]
            if (edge.producer not in executor._node_specs or self.partition in spec.producer_partitions):
                self._outgoing.setdefault(edge.producer, []).append(edge)
            if (edge.consumer not in executor._node_specs or self.partition in spec.consumer_partitions):
                self._incoming.setdefault(edge.consumer, []).append(edge)
        from .finite_input import FiniteInputDrain
        self._finite_input = FiniteInputDrain(self, services=executor._finite_services)
        self._relational_regions = getattr(executor, '_relational_regions', {})
        self._region_members = {occ for region in self._relational_regions.values()
                                for occ in region.plan.occurrences}
        for region in self._relational_regions.values():
            region.on_complete_member = self._region_member_completed
            region.on_input_member = self._region_member_inputs
        # Every source row can occupy several delivery slots at once. Each
        # channel fitting its own window does not prove the aggregate fits the
        # same worker's bounded outstanding-delivery table. Stop between source
        # records, before consuming a new source position or routing sequence.
        width = self._source_delivery_width()
        aggregate_window = max(1, self._credit_limit // width)
        windows = self._source_durable_windows()
        self._source_quantum = (min([physical.policy.checkpoint_rows, aggregate_window, *windows])
                                if windows else physical.policy.checkpoint_rows)
        self._source_park_quantum = self._source_quantum
        self._source_live_quantum = (min(physical.policy.checkpoint_rows, aggregate_window)
                                    if windows else physical.policy.checkpoint_rows)
        host = '127.0.0.1'
        if executor._physical.policy.provider == 'ray_core':
            import ray
            host = ray.util.get_node_ip_address()
        self._peer = PeerBuffers(bounds=self.bounds, host=host, store=self.services.store)
        self._read_batch = None
        from ...resources.capacity import track_owned_thread
        track_owned_thread(self._peer._thread)
        for thread in self._peer._threads:
            track_owned_thread(thread)
        self._queue_finalizer = None
        self._terminal_rows = 0
        self._producer_expansion = {}
        self._terminal_bytes = 0
        self._terminal_columns = None
        self._terminal_schema_ref = None
        self._terminal_non_mapping_values = False
        self._restored_terminal = False
        self._terminal_spec = next((n.kernel for n in executor._node_specs.values()
                                    if n.kind == 'terminal'), None)
        from .terminal_blocks import TerminalBlocks, declared_blocks
        occurrence = next((occ for occ, node in executor._node_specs.items() if node.kind == 'terminal'), None)
        declaration = declared_blocks(executor._graph, occurrence, self.bounds) if occurrence else None
        scalar = self._terminal_spec is not None and self._terminal_spec.kind in (
            'count', 'schema', 'columns', 'size_bytes', 'aggregate', 'collect_summary')
        self._terminal_blocks = TerminalBlocks(self.services, self.partition,
            declaration=declaration, partitioned=not scalar,
            budget=self._terminal_spec.configuration.get('terminal_memory') if self._terminal_spec else None,
            frame_call=self._frame_call, coalesce_allowed=lambda: not self._finite)
        self._batch_source = None
        if (self._terminal_spec and self._terminal_spec.kind == 'write_lance'
                and self._terminal_spec.configuration.get('input_carrier') == FIXED_SCAN_BATCHES
                and len(executor._node_specs) == len(executor._graph.nodes) == 2
                and len(executor._readers) == len(executor._physical.splits) == 1):
            reader = next(iter(executor._readers.values()))
            if reader.supports_native_batches():
                self._batch_source = reader
        from .key_exclusion import FixedKeyExclusion
        self._fixed_key_exclusion = FixedKeyExclusion.open(self)
        args = self._terminal_spec.configuration.get('arguments', {}) if self._terminal_spec else {}
        self._limit = (args.get('limit') if self._terminal_spec and self._terminal_spec.kind == 'take'
                       else args.get('batch_size', 20) if self._terminal_spec and self._terminal_spec.kind == 'take_batch'
                       else None)
        # A direct collection has no filtering/expansion or other consumer.
        # Its source can prove overflow before filling the asynchronous channel
        # with more input. Keep the driver's global check for multiple splits;
        # never apply an output limit to inputs of a transforming graph.
        self._direct_collection = None
        graph = executor._graph
        if len(graph.nodes) == 2 and len(graph.edges) == 1:
            source = next((node for node in graph.nodes if node.kind == 'source'), None)
            terminal = next((node for node in graph.nodes if node.kind == 'terminal'), None)
            if (source is not None and terminal is not None
                    and graph.edges[0].producer == source.occurrence_id
                    and graph.edges[0].consumer == terminal.occurrence_id
                    and terminal.kernel.kind == 'take_all'):
                maximum = terminal.kernel.configuration.get('arguments', {}).get('limit')
                if maximum is not None:
                    self._direct_collection = (source.occurrence_id, maximum)

    def _restore_terminal(self):
        image = self.e._restore_image
        if image is None or self._terminal_spec is None:
            return
        from ..dataflow_sinks import _artifact_from_json
        for contribution in image.contributions:
            if contribution.participant_id not in self.e._participant_ids():
                continue
            ref = contribution.sinks.get(self._terminal_spec.configuration.get('sink_id', self._terminal_spec.kind))
            if ref is None:
                continue
            blocks = self._terminal_blocks
            if blocks._workspace_limit is not None:
                from ..dataflow_record_frames import decode_metadata
                raw = self._frame_call(read_document, ref, self.services.store, self.bounds,
                                       _maximum=self.bounds.max_control_bytes)
                manifest = self._frame_call(decode_metadata, raw, self.bounds)
                del raw
            else:
                manifest = json.loads(b"".join(self.services.store.read(ref, max_bytes=self.bounds.max_control_bytes)))
            with blocks.hold_metadata(manifest):
                non_mapping_values = _manifest_non_mapping_values(manifest)
                blocks.restore(manifest)
                self._terminal_rows, self._terminal_bytes = manifest["rows"], manifest["bytes"]
                blocks.retain_columns(manifest['columns'])
                self._terminal_columns = manifest["columns"]
                self._terminal_non_mapping_values = non_mapping_values
                from ..record_schema import RecordSchemas
                self._terminal_schema_ref = (_artifact_from_json(manifest['schema_ref'])
                                            if manifest.get('schema_ref') is not None else None)
                RecordSchemas(self.services).read(self._terminal_schema_ref)
            del manifest
            self._restored_terminal = True

    def _set_terminal_columns(self, values, *, row=None, sort=False, schema=False):
        blocks = self._terminal_blocks
        if blocks._workspace_limit is not None:
            from ..dataflow_record_frames import _typed_shape
            # The first row still owns the aliased key strings. The new list
            # and sorting scratch must fit before allocating columns.
            resident = _typed_shape(row, self.bounds.max_row_bytes)[1] if row is not None else 0
            if schema:
                import sys
                # Field names are inspected one at a time before Arrow creates
                # its full names list; the list and optional sorted copy coexist.
                resident += sum(sys.getsizeof(field.name) for field in values)
            blocks._check_workspace(resident + 64 + 16 * len(values))
        if schema:
            values = values.names
        columns = sorted(values) if sort else list(values)
        blocks.retain_columns(columns)
        self._terminal_columns = columns

    def _source_durable_windows(self):
        """Bound source admission by reachable original durable grants.

        A source's immediate memory peer may share its resident group while
        that peer's downstream durable consumer is parked. Traverse the sealed
        DAG (once per node/edge), including those downstream boundaries, rather
        than assuming a released memory credit replenished durable capacity.
        This tightens the source admission quantum, not checkpoint policy.
        """
        pending = [occurrence for occurrence, node in self.e._node_specs.items()
                   if node.kind == 'source']
        visited, windows = set(), []
        while pending:
            occurrence = pending.pop()
            if occurrence in visited:
                continue
            visited.add(occurrence)
            for edge in self._outgoing.get(occurrence, ()):
                spec = self._exchange[edge.edge_id]
                if spec.mode == 'durable_incremental':
                    windows.append(min(spec.credit_rows,
                        max(1, spec.credit_bytes // self.bounds.max_row_bytes)))
                pending.append(edge.consumer)
        return windows

    def _source_delivery_width(self):
        """Static delivery multiplicity for one source record, without UDFs.

        Sum paths through co-resident incremental nodes (including a fork and
        subsequent union), not all output specs of unrelated source owners.
        A broadcast reserves one slot per actual reader; finite spools reserve
        storage and no live delivery slots. Dynamic UDF expansion is still
        checked by the original per-invocation publication gate.
        """
        owners = self.e._node_specs
        memo = {}
        sources = {split.source_id for split in self.e.assignment.source_splits}
        for source in sources:
            pending = [(source, False)]
            while pending:
                occurrence, expanded = pending.pop()
                if occurrence in memo:
                    continue
                edges = self._outgoing.get(occurrence, ())
                children = []
                external = int(owners[occurrence].kind == 'terminal')
                for edge in edges:
                    if edge.consumer in owners:
                        ports = owners[edge.consumer].kernel.inputs
                        if any(p.name == edge.input_port and p.requirement == 'finite_complete' for p in ports):
                            continue
                        children.append(edge.consumer)
                    else:
                        spec = self._exchange[edge.edge_id]
                        if spec.mode != 'finite_sealed':
                            external += (len(spec.consumer_partitions) if spec.routing == 'broadcast' else 1)
                if not expanded:
                    pending.append((occurrence, True))
                    pending.extend((child, False) for child in children if child not in memo)
                    continue
                memo[occurrence] = min(self._credit_limit + 1,
                    external + sum(memo[child] for child in children))
        # Different source splits are polled one record at a time against the
        # same source-row counter, so the largest per-record width is enough.
        return max([1, *(memo[source] for source in sources)])

    def _park_can_output(self, occurrence):
        """Check one bounded output against the original live credit tables.

        This is only a stop-before-next-step check, never a reservation. Actual
        publication still reserves atomically. Unknown UDF expansion retains
        the original per-invocation rejection; it is not assigned a made-up
        multiplier. Traversal sums every static fork path and broadcast reader.
        """
        with self._condition:
            if not self._credits:
                # Even a window smaller than max_row_bytes can admit its first
                # actual small row. Original reserve is the final authority.
                return True
            edges, terminals = {}, 0
            pending = [occurrence]
            owners = self.e._node_specs
            while pending:
                current = pending.pop()
                if owners[current].kind == 'terminal':
                    terminals += 1
                for edge in self._outgoing.get(current, ()):
                    if edge.edge_id in self._closed_outputs:
                        continue
                    if edge.consumer in owners:
                        ports = owners[edge.consumer].kernel.inputs
                        if not any(p.name == edge.input_port and p.requirement == 'finite_complete' for p in ports):
                            pending.append(edge.consumer)
                    else:
                        spec = self._exchange[edge.edge_id]
                        if spec.mode != 'finite_sealed':
                            edges[edge.edge_id] = edges.get(edge.edge_id, 0) + 1
                # Do not traverse exponentially many fork paths once even the
                # minimum remaining static output cannot fit the fixed table.
                if len(pending) + terminals + sum(edges.values()) > self._credit_limit:
                    return False
            slots, channel_bytes = terminals, 0
            for edge_id, count in edges.items():
                spec = self._exchange[edge_id]
                recipients = len(spec.consumer_partitions) if spec.routing == 'broadcast' else 1
                slots += count * recipients
                channel_bytes += count * recipients * self.bounds.max_row_bytes
                # Hash/target/rebalance may choose any declared reader; require
                # enough room in every possible destination before advancing.
                if spec.mode != 'memory_incremental':
                    for reader in spec.consumer_partitions:
                        credit = self.e._channels.reader_credit(spec.channel_id, reader)
                        if credit['rows_left'] < count or credit['bytes_left'] < count * self.bounds.max_row_bytes:
                            return False
            if len(self._credits) + slots > self._credit_limit:
                return False
            leases = self.e._channels.outstanding_leases()
            return (self.e._channels.resident_bytes + sum(x.size_bytes for x in leases)
                    + channel_bytes <= self.bounds.max_resident_bytes)

    def start(self):
        if self._thread is not None:
            return
        self._restore_terminal()
        if self._terminal_spec is not None and not self._restored_terminal:
            from ..dataflow_schema import terminal_schema
            from ..record_schema import RecordSchemas
            occurrence = next(occ for occ, node in self.e._node_specs.items() if node.kind == 'terminal')
            schema = terminal_schema(self.e._graph, self.e._physical,
                terminal=occurrence, bounds=self.bounds)
            if schema is not None:
                self._terminal_schema_ref = RecordSchemas(self.services).put(schema)
                self._set_terminal_columns(schema, sort=True, schema=True)
        from ..recovery_contract import identity
        frontier_ref = self.e._restored_states.get(identity(['worker-frontier', self.partition]))
        if self.e._restore_image is not None:
            if frontier_ref is None:
                raise DataflowError('INCOMPATIBLE_HISTORY', 'partial recovery lacks worker completion frontier')
            frontier = json.loads(b''.join(self.services.store.read(frontier_ref,
                max_bytes=self.bounds.max_state_bytes)))
            if frontier.get('schema') != 'demiflow.worker-frontier.v3':
                raise DataflowError('INCOMPATIBLE_HISTORY', 'unknown worker completion frontier')
            if bool(frontier.get('fixed_key_batches')) != (self._fixed_key_exclusion is not None):
                raise DataflowError('INCOMPATIBLE_HISTORY', 'fixed key input strategy differs from the saved cut')
            self._finished = set(frontier['nodes'])
            self._finished_splits = set(frontier['splits'])
            self._finished_inputs = {tuple(x) for x in frontier['inputs']}
            # Older workers synchronously applied port EOI before a cut could
            # be observed. New workers retain deferred completion separately
            # so a resumed native final batch is never invoked a second time.
            ready_ports = {edge.edge_id for edge in self.e._graph.edges
                if edge.consumer in self.e._node_specs and all(
                    (edge.edge_id, p) in self._finished_inputs for p in
                    (self._exchange[edge.edge_id].producer_partitions
                     if edge.edge_id in self._exchange else (self.partition,)))}
            completed_ports = frontier.get('finished_ports', sorted(ready_ports))
            if (type(completed_ports) is not list or any(type(x) is not str for x in completed_ports)
                    or len(completed_ports) != len(set(completed_ports))
                    or not set(completed_ports) <= ready_ports):
                raise DataflowError('FENCED', 'recovery completion contains an unclosed input port')
            self._finished_ports = set(completed_ports)
            self._finite_input.restore(frontier.get('finite_reads', {}))
            self._closed_inputs = set(frontier.get('closed_inputs', ()))
            self._closed_outputs = set(frontier.get('closed_outputs', ()))
            for (occurrence, _), kernel in self.e._kernels.items():
                cursor = kernel._finish_cursor
                if cursor is None:
                    continue
                # Consumer satisfaction may close a finite iterator before its
                # suffix is emitted. The original closed-output frontier is the
                # proof to discard that suffix, including after a partial cut.
                pruned = (occurrence in self._finished and self._outgoing.get(occurrence)
                          and all(edge.edge_id in self._closed_outputs
                                  for edge in self._outgoing[occurrence]))
                if not pruned and (occurrence in self._finished or any(
                        edge.edge_id in self._finished_ports for edge in self._incoming.get(occurrence, ())
                        if edge.input_port == cursor['port'])):
                    raise DataflowError('FENCED', 'active finite output frontier was already completed')
            self._emitted_eoi = set(frontier.get('emitted_eoi', ()))
            metrics = frontier.get('metrics', {})
            if metrics:
                if set(metrics) != set(self._metrics):
                    raise DataflowError('FENCED', 'restored metric ownership differs')
                self._metrics = metrics
            self.e._event_sequence = max(self.e._event_sequence, frontier.get('event_sequence', 0))
            self._consumed_deliveries = set(frontier.get('consumed_deliveries', ()))
            memory = frontier.get('memory_frontiers', {})
            if type(memory) is not dict:
                raise DataflowError('FENCED', 'invalid memory input frontier')
            for edge_id, values in memory.items():
                spec = self._exchange.get(edge_id)
                if (spec is None or spec.mode != 'memory_incremental'
                        or self.partition not in spec.consumer_partitions or type(values) is not dict
                        or not set(values) <= set(spec.producer_partitions)
                        or any(type(seq) is not int or seq < 0 for seq in values.values())):
                    raise DataflowError('FENCED', 'memory input frontier differs from assigned channel')
                self._memory_frontiers[edge_id] = dict(values)
            self._credit_payloads = dict(frontier.get('durable_deliveries', {}))
            leases = {lease.operation_id: lease for lease in self.e._channels.outstanding_leases()}
            if not set(self._credit_payloads) <= set(leases):
                raise DataflowError('CORRUPT_ARTIFACT', 'parked producer delivery lacks original credit')
            self._credits = {key: (leases[key], None) for key in self._credit_payloads}
            if self._credits:
                self._paused = True  # Wait for original committed consumption ACKs.
            valid_inputs = {(edge.edge_id, p) for edge in self.e._graph.edges
                            if edge.consumer in self.e._node_specs for p in
                            (self._exchange[edge.edge_id].producer_partitions
                             if edge.edge_id in self._exchange else (self.partition,))}
            if (not self._finished <= set(self.e._node_specs)
                    or not self._finished_splits <= set(self.e._readers)
                    or not self._finished_inputs <= valid_inputs
                    or not self._closed_inputs <= {edge for edge, _ in valid_inputs}
                    or not self._closed_outputs <= {edge.edge_id for edge in self.e._graph.edges
                                                    if edge.producer in self.e._node_specs}):
                raise DataflowError('FENCED', 'recovery frontier contains unassigned work')
        for spec in self.e._output_specs:
            ref = self.e._restored_states.get(identity(['finite-spool', spec.channel_id]))
            if ref is not None:
                value = json.loads(b''.join(self.services.store.read(ref, max_bytes=self.bounds.max_control_bytes)))
                data = ArtifactRef(**value['ref'])
                from ..dataflow_segments import block_totals
                rows, size = block_totals(self.services.store, data, self.bounds)
                if (rows is not None and rows != value['count']) or size > spec.storage_bytes:
                    raise DataflowError('CORRUPT_ARTIFACT', 'finite spool restore counts differ')
                if value.get('bytes', size) != size:
                    raise DataflowError('CORRUPT_ARTIFACT', 'finite spool restore size differs')
                self._finite_buffer(spec).restore({'rows': value['count'], 'bytes': size,
                    'partitions': [{'data_ref': value['ref'], 'rows': value['count'], 'bytes': size}]})
        for edge_id in self._closed_inputs:
            edge = next(e for e in self.e._graph.edges if e.edge_id == edge_id)
            if edge.producer not in self.e._node_specs:
                self._send('eoi', {'scope': 'input_satisfied', 'edge_id': edge_id,
                                   'consumer_partition': self.partition})
        self._thread = threading.Thread(target=copy_context().run, args=(self._run,), name='dataflow-business', daemon=True)
        self._thread.start()
        from ...resources.capacity import track_owned_thread
        track_owned_thread(self._thread)

    def wake(self):
        with self._condition:
            self._wake_generation += 1
            self._condition.notify_all()

    def _send(self, kind, payload=None, ref=None):
        self.e._mailbox.send(self.e._event(kind, payload=payload, payload_ref=ref))

    def _send_cut(self, request):
        from ..contracts.dataflow_codec import encode_wire
        from ..recovery_contract import MAX_DOCUMENT_BYTES
        raw = encode_wire(self.snapshot(request), type_name='CutContribution', bounds=self.bounds)
        # A cut contribution contains bounded control metadata and references.
        # Its durable owner is the coordinator. Leave ample room for escaping
        # and the host envelope; the full event still goes through the wire codec.
        limit = min(self.bounds.max_control_bytes, MAX_DOCUMENT_BYTES)
        if len(raw) <= limit // 4:
            event = self.e._event('cut', payload={'contribution': raw.decode('utf-8')})
            try:
                encoded = encode_wire(event, type_name='WorkerEvent', bounds=self.bounds)
            except DataflowError as exc:
                if exc.code != 'BUDGET_EXCEEDED':
                    raise
            else:
                if len(encoded) <= limit:
                    self.e._mailbox.send(event)
                    return
        ref = self.services.store.put(iter([raw]), max_bytes=len(raw), media_type='application/json')
        self._send('cut', ref=ref)

    def _checkpoint_wait_notification(self):
        # Called under the same lock as pause/activity admission. This is live
        # control evidence only: no state, cursor or recovery promise is made.
        if not getattr(self, '_checkpoint_wait_requested', False):
            return None
        occurrence = min(getattr(self, '_checkpoint_finishes', ()), default=None)
        if occurrence is None and getattr(self, '_park_wait_authorized', False):
            # Parking retires this worker, so even journal-backed native calls
            # must finish before its live owner can be released. This is
            # business request time, not a stalled cut. Ordinary non-parking
            # model calls still snapshot pending requests without this claim.
            live = set(getattr(self, '_checkpoint_model_calls', ()))
            live.update(key[0] for key, kernel in self.e._kernels.items()
                if kernel.pending_calls and kernel._spec.kind in {'prompt', 'embedding'})
            occurrence = min(live, default=None)
        signature = (getattr(self, '_activity_generation', 0), occurrence)
        previous = getattr(self, '_checkpoint_wait_signature', None)
        if signature == previous or occurrence is None and (previous is None or previous[1] is None):
            return None
        self._checkpoint_wait_signature = signature
        self._checkpoint_wait_sequence += 1
        if self._checkpoint_wait_sequence >= 2**63:
            raise DataflowError('BUDGET_EXCEEDED', 'finite checkpoint wait sequence exhausted')
        return {'activity_generation': signature[0], 'sequence': self._checkpoint_wait_sequence,
                'partition_id': self.partition, 'occurrence_id': occurrence}

    def _model_results(self, occurrence, kernel, results):
        """Expose serial native request time to the independent control thread.

        Track only next(), never downstream output/ACK waiting. Concurrent
        calls use the original pending-call evidence after next() returns.
        """
        if kernel._spec.kind not in {'prompt', 'embedding'}:
            yield from results
            return
        iterator = iter(results)
        while True:
            with self._condition:
                self._checkpoint_model_calls.add(occurrence)
                notice = self._checkpoint_wait_notification()
            failure = None
            try:
                if notice is not None:
                    self._send('checkpoint_wait', notice)
                result = next(iterator)
            except StopIteration:
                return
            except BaseException as exc:
                failure = exc
                raise
            finally:
                with self._condition:
                    self._checkpoint_model_calls.discard(occurrence)
                    notice = self._checkpoint_wait_notification()
                if notice is not None:
                    try:
                        self._send('checkpoint_wait', notice)
                    except BaseException as exc:
                        if failure is None:
                            raise
                        failure.add_note('Native model wait cleanup failed: ' + str(exc))
            yield result

    def _finite_finish_iterator(self, occurrence, kernel, port):
        iterator = iter(kernel.finish(port, wait_for_calls=False))
        if kernel._spec.kind != 'reduce' or kernel.finish_replayable:
            return iterator

        def observed():
            # The caller has observed all original input EOIs before entering
            # this sealed finite reducer. Keep the evidence across yielded
            # output: its live fold iterator still cannot be snapshotted.
            with self._condition:
                self._checkpoint_finishes.add(occurrence)
                notice = self._checkpoint_wait_notification()
            failure = None
            try:
                if notice is not None:
                    self._send('checkpoint_wait', notice)
                yield from iterator
            except BaseException as exc:
                failure = exc
                raise
            finally:
                with self._condition:
                    self._checkpoint_finishes.discard(occurrence)
                    notice = self._checkpoint_wait_notification()
                if notice is not None:
                    try:
                        self._send('checkpoint_wait', notice)
                    except BaseException as notification_failure:
                        if failure is None:
                            raise
                        failure.add_note('Finite checkpoint wait cleanup also failed: ' + str(notification_failure))

        return observed()

    def receive(self, command):
        kind = command['kind']
        with self._condition:
            if 'activity_generation' in command:
                generation = command['activity_generation']
                if (type(generation) is not int or not 0 < generation < 2**63
                        or generation <= getattr(self, '_activity_generation', 0)):
                    raise DataflowError('FENCED', 'worker activity generation is stale or invalid')
                self._activity_generation = generation
            if kind == 'ack':
                entry = self._credits.get(command['delivery_id'])
                if entry is None:
                    raise DataflowError('INVALID_CONTRACT', 'ack does not name an outstanding delivery')
                lease, key = entry
                if key is not None:
                    self._peer.release(key)  # Sender references must die before credit release.
                if lease is not None:
                    self.e._channels.release(lease)
                self._credits.pop(command['delivery_id'])
                self._credit_payloads.pop(command['delivery_id'], None)
            elif kind == 'park_policy':
                self._park_wait_authorized = True
                self._park_mode = True
                self._source_quantum = self._source_park_quantum
                self._consumed_deliveries.intersection_update(command.get('retain_consumed', ()))
                self._idle_sent = False
            elif kind in ('pause', 'park'):
                self._park_requested = kind == 'park'
                self._paused = True
                self._checkpoint_wait_requested = True
                self._idle_sent = False
            elif kind == 'resume':
                if 'durable_peers_live' in command:
                    live = command['durable_peers_live']
                    if type(live) is not bool or live and self._park_mode:
                        raise DataflowError('INVALID_CONTRACT', 'invalid durable peer residency')
                    # The sole driver sends this only after activating every
                    # stage in the one admitted cohort. Live peers replenish
                    # the original channel/aggregate credits through real ACKs;
                    # no worst-case parked byte window needs a forced cut.
                    # Original credit checks and unconfirmed source bounds are
                    # unchanged. Absent/parked peers keep the conservative cap.
                    self._source_quantum = (self._source_live_quantum
                        if live else self._source_park_quantum)
                observation_rows = command.get('observation_rows', 0)
                if type(observation_rows) is not int or observation_rows < 0:
                    raise DataflowError('INVALID_CONTRACT', 'invalid progress observation interval')
                self._observation_rows = observation_rows
                credit = command.get('source_credit')
                if credit is not None:
                    if (type(credit) is not dict or set(credit) != {'rows', 'bytes'}
                            or any(type(v) is not int or v < 0 for v in credit.values())):
                        raise DataflowError('INVALID_CONTRACT', 'invalid unconfirmed source credit')
                    self._source_credit = credit
                self._publication_overlap = command.get('publishing', False)
                self._park_requested = False
                self._source_rows_since_cut = 0
                self._consumed_deliveries.clear()
                self._paused = False
                self._checkpoint_wait_requested = False
                self._checkpoint_wait_signature = None
                self._idle_sent = False
                self._cut = None
                self._sealed_cut = None
            elif kind == 'receipt' or kind == 'cut' and self._sealed_cut is not None:
                if len(self._cut_controls) >= 2:
                    raise DataflowError('BUDGET_EXCEEDED', 'worker cut control queue exceeds its bound')
                # An active cut stays in the original FIFO so earlier data
                # drains first. Only a recut of an already sealed frontier
                # bypasses next-generation input. Receipts are control only.
                self._cut_controls.append(command)
                self._idle_sent = False
            else:
                if len(self._queue) >= self.bounds.max_pending_events:
                    raise DataflowError('BUDGET_EXCEEDED', 'worker input queue exceeds credit')
                if kind in ('close_output', 'satisfy'):
                    self._queue.appendleft(command)
                else:
                    self._queue.append(command)
                self._idle_sent = False
            self._wake_generation += 1
            self._condition.notify_all()
            notice = self._checkpoint_wait_notification()
        # A blocked business fold cannot stop its control thread acknowledging
        # pause. Conversely, mailbox backpressure must not hold this lock.
        if notice is not None:
            self._send('checkpoint_wait', notice)

    def _run(self):
        try:
            import os
            self._send('ready', {'worker_process_id': os.getpid(),
                                 'runner_thread_id': threading.get_native_id()})
            while not self.e._cancel_event.is_set():
                with self._condition:
                    generation = self._wake_generation
                processed = self._step_ready()
                if not processed:
                    with self._condition:
                        # A command/credit/native completion can notify after
                        # step saw no work but before this wait acquires the
                        # lock. Remember that wake instead of sleeping over it.
                        if (generation == self._wake_generation
                                and not self.e._cancel_event.is_set()):
                            self._condition.wait(.005)
        except BaseException as exc:
            if not self.e._cancel_event.is_set():
                from ..dataflow_failures import failure_payload
                self._send('failure', failure_payload(exc))

    def _ready_peer_commands(self):
        """Metadata-only selection; never dequeue or read future payloads."""
        result, owner, size, limit = [], None, 0, MAX_PEER_BATCH
        with self._condition:
            if self._sealed_cut is not None or self._cut_controls:
                return result
            for command in islice(self._queue, MAX_PEER_BATCH):
                peer = command.get('peer')
                if command.get('kind') != 'data' or type(peer) is not dict:
                    break
                limit = min(limit, peer_batch_limit(peer))
                if len(result) >= limit:
                    break
                spec = self._exchange.get(command.get('edge_id'))
                if spec is None or spec.mode != 'memory_incremental':
                    break
                current = (command.get('edge_id'), command.get('producer_partition'),
                    command.get('route_epoch'), peer.get('host'), peer.get('port'), peer.get('token'))
                count = peer.get('bytes')
                if type(count) is not int or count < 0 or count > document_limit(self.bounds):
                    break
                if ((owner is not None and current != owner)
                        or len(result) >= min(spec.credit_rows, self.bounds.max_pending_events)
                        or size + count > min(spec.credit_bytes, self.bounds.max_resident_bytes)):
                    break
                owner = current
                result.append(command)
                size += count
        return result

    def _step_ready(self):
        commands = self._ready_peer_commands()
        if len(commands) < 2:
            return self.step(max_records=1, deadline_ms=50).processed
        frames = read_peer_many(tuple(command['peer'] for command in commands), bounds=self.bounds)
        self._read_batch = [commands, frames, 0]
        processed, deadline = 0, time.monotonic() + .05
        try:
            for command in commands:
                if self.e._cancel_event.is_set() or time.monotonic() >= deadline:
                    break
                with self._condition:
                    if not self._queue or self._queue[0] is not command:
                        break  # A priority close/satisfy command must run first.
                before = self._read_batch[2]
                processed += self.step(max_records=1, deadline_ms=50).processed
                if self._read_batch[2] == before:
                    break  # Control arrived between the head check and dequeue.
        finally:
            self._read_batch = None
            frames.close()
        return processed

    def _advance_batch(self, source, split_id, credit):
        """Transfer a co-located source batch within the same cut and credit window."""
        rows, size = self.bounds.max_output_rows, self.bounds.max_batch_bytes
        if credit is not None:
            rows = min(rows, credit['rows'] - self._unconfirmed_rows)
            size = min(size, credit['bytes'] - self._unconfirmed_bytes)
        if not self._publication_overlap:
            rows = min(rows, self._source_quantum - self._source_rows_since_cut)
        batch = self._batch_source.next_arrow_batch(rows, size)
        if batch is None:
            self._finish_source_split(split_id, source)
            return
        if self._terminal_schema_ref is None:
            from ..record_schema import RecordSchemas
            self._terminal_schema_ref = RecordSchemas(self.services).put(batch.schema)
            self._set_terminal_columns(batch.schema, schema=True)
        self._terminal_blocks.append_arrow_batch(batch)
        self._terminal_rows, self._terminal_bytes = self._terminal_blocks.rows, self._terminal_blocks.bytes
        count = batch.num_rows
        terminal = self.e._graph.terminals[0]
        self._metrics[source]['rows_output'] += count
        self._metrics[terminal]['rows_input'] += count
        self._metrics[terminal]['rows_output'] += count
        self._dirty_metrics.update((source, terminal))
        self._source_rows_since_cut += count
        self._unconfirmed_rows += count
        self._unconfirmed_bytes += batch.nbytes
        if not self._publication_overlap and self._source_rows_since_cut >= self._source_quantum:
            self._paused = True
            self._flush_metrics()

    def _accept_arrow_result(self, terminal_occurrence, batch, *, positions):
        """Seal each ordered output batch once and advertise that same leaf under ACK credit."""
        import pyarrow as pa
        from ..dataflow_native_limits import sealed_execution_ceilings
        from ..dataflow_record_frames import admit_workspace
        from ..outputs.arrow_rows import pack_result, validate_row_sizes
        from ..record_schema import RecordSchemas
        operator = next(node for node in self.e._node_specs.values() if node.kernel.kind == 'exclude_keys')
        source_id = operator.kernel.configuration['lance_key_source']
        split, = [s for s in self.e._physical.splits if s.source_id == source_id]
        snapshot = next(s for s in self.e._physical.sources if s.source_id == source_id)
        caps = sealed_execution_ceilings(self.e._graph)
        row_budget = min(self.bounds.max_row_bytes, operator.kernel.output_bound_bytes or self.bounds.max_row_bytes)
        validate_row_sizes(batch, row_budget)
        if not batch.num_rows:
            return
        if self._terminal_schema_ref is None:
            self._terminal_schema_ref = RecordSchemas(self.services).put(batch.schema)
            self._set_terminal_columns(batch.schema, schema=True)
        size_limit = min(self.bounds.max_batch_bytes, caps.get('inflight_bytes', self.bounds.max_batch_bytes))
        start = 0
        while start < batch.num_rows:
            count = min(batch.num_rows - start, caps.get('inflight_rows', batch.num_rows))
            while True:
                packed = pack_result(batch.slice(start, count), positions.slice(start, count), source_id=source_id,
                    split_id=split.split_id, version=snapshot.immutable_identity['version'],
                    occurrence=operator.occurrence_id, sequence=self._terminal_rows, bounds=self.bounds)
                size = pa.ipc.get_record_batch_size(packed) + len(packed.schema.serialize()) + 8
                if size <= size_limit:
                    break
                if count == 1:
                    raise DataflowError('BUDGET_EXCEEDED', 'terminal Arrow row exceeds admitted transfer bytes')
                count //= 2
            # Only one leased IPC leaf may await acknowledgement. Cursor movement
            # and cuts outside this call observe the complete processed source prefix.
            deadline = time.monotonic() + self.bounds.timeout_ms / 1000
            with self._condition:
                while self._credits:
                    if self.e._cancel_event.is_set():
                        raise DataflowError('CANCELLED', 'business execution cancelled')
                    if time.monotonic() >= deadline:
                        raise DataflowError('DEADLINE_EXCEEDED', 'terminal Arrow delivery credit deadline')
                    self._condition.wait(.01)
            self.services.check_stop()
            self._frame_call(admit_workspace, packed.get_total_buffer_size() + 2 * size + 65536, self.bounds)
            first = self._terminal_rows
            ref = self._terminal_blocks.append_arrow_batch(packed)
            self._terminal_rows, self._terminal_bytes = self._terminal_blocks.rows, self._terminal_blocks.bytes
            delivery = self.e._next_operation_id('terminal')
            with self._condition:
                self._credits[delivery] = (None, None)
            self._send('terminal', {'delivery_id': delivery, 'producer_partition': self.partition,
                'result_batch': _artifact_json(ref), 'rows': count, 'sequence': first,
                'schema_ref': _artifact_json(self._terminal_schema_ref)})
            self._metrics[terminal_occurrence]['rows_input'] += count
            self._metrics[terminal_occurrence]['rows_output'] += count
            self._dirty_metrics.add(terminal_occurrence)
            start += count

    def step(self, *, max_records, deadline_ms):
        if max_records <= 0 or deadline_ms <= 0:
            raise DataflowError('INVALID_CONTRACT', 'step budgets must be positive')
        processed = 0
        # Once idle was reported, freeze the observed output frontier until
        # receipt/resume. Native futures may complete, but their input remains
        # in the cut and no unobserved output may cross it.
        with self._condition:
            # Cut/receipt invalidate the notification so the driver gets a
            # fresh idle response, but are not permission to run business.
            # Polling a timer here can emit to a peer that already sealed this
            # cut, then wait forever for an ACK that requires resume. Earlier
            # business stays ahead of an active cut in the original FIFO.
            control_ready = bool(self._cut_controls or
                self._queue and self._queue[0]['kind'] == 'cut')
            poll_ready = (self._sealed_cut is None and not control_ready
                          and not (self._paused and self._idle_sent))
        if poll_ready:
            for (occurrence, _), kernel in self.e._kernels.items():
                for record in kernel.poll():
                    self._output(occurrence, record)
                    processed += 1
        with self._condition:
            command = (self._cut_controls.popleft() if self._cut_controls else
                       self._queue.popleft() if self._sealed_cut is None and self._queue else None)
            if command is not None:
                # receive() may enqueue while the previous step is publishing
                # its idle notification. Consuming new work invalidates that
                # notification again, before any output crosses the frontier.
                self._idle_sent = False
        if command:
            if command['kind'] == 'data':
                self._receive_data(command)
            elif command['kind'] == 'finite_input':
                self._receive_finite(command)
            elif command['kind'] == 'edge_eoi':
                self._edge_eoi(command['edge_id'], command['producer_partition'])
            elif command['kind'] == 'close_output':
                self._close_output(command['edge_id'])
            elif command['kind'] == 'satisfy' and self._fixed_key_exclusion is not None:
                # The key scans may already be exhausted while selected output
                # remains. Keep that unfinished node resumable from its published prefix.
                self._paused = True
            elif command['kind'] == 'satisfy':
                for occ, node in self.e._node_specs.items():
                    if node.kind == 'terminal':
                        self._finish_node(occ)
                        self._close_inputs(occ)
            elif command['kind'] == 'cut':
                # An idle event may have raced with resume. The cut itself
                # freezes admission before waiting for actual local obligations.
                self._paused = True
                self._park_requested = False
                self._idle_sent = False
                self._cut = command['request']
            elif command['kind'] == 'receipt':
                self.e.accept_receipt(command['commit_id'], cut_id=command['cut_id'])
                confirmed = self._cut_credit.pop(command['cut_id'], None)
                if confirmed is not None:
                    self._unconfirmed_rows -= confirmed[0]
                    self._unconfirmed_bytes -= confirmed[1]
            else:
                raise DataflowError('INVALID_CONTRACT', 'unknown business command')
            processed += 1
        if self._sealed_cut is None and not self._paused and not self._cut:
            if self._fixed_key_exclusion is not None:
                processed += self._fixed_key_exclusion.step(self)
            for split_id, reader in (() if self._fixed_key_exclusion else self.e._readers.items()):
                if split_id in self._finished_splits:
                    continue
                self.services.check_stop()
                credit = self._source_credit
                if credit is not None and (
                        self._unconfirmed_rows >= credit['rows'] or not credit['bytes']
                        or self._unconfirmed_bytes and
                        self._unconfirmed_bytes + self.bounds.max_row_bytes > credit['bytes']):
                    self._paused = True
                    self._flush_metrics()
                    break
                source = next(s.source_id for s in self.e.assignment.source_splits if s.split_id == split_id)
                if self._park_mode and not self._park_can_output(source):
                    self._paused = True
                    break
                from ..dataflow_source_context import source_run_scope
                with source_run_scope(self.e._source_run):
                    if reader is self._batch_source:
                        self._advance_batch(source, split_id, credit)
                        processed += 1
                        break
                    poll = reader.next_record()
                if poll.status == 'record':
                    if credit is not None and self._unconfirmed_bytes + poll.record.encoded_bytes > credit['bytes']:
                        raise DataflowError('BUDGET_EXCEEDED', 'source row exceeds checkpoint_unconfirmed_bytes')
                    self._unconfirmed_rows += 1
                    self._unconfirmed_bytes += poll.record.encoded_bytes
                    source = next(s.source_id for s in self.e.assignment.source_splits if s.split_id == split_id)
                    self._output(source, poll.record)
                    self._source_rows_since_cut += 1
                    if reader._eoi and not reader._replay_pending:
                        self._finish_source_split(split_id, source)
                    if not self._publication_overlap and self._source_rows_since_cut >= self._source_quantum:
                        self._paused = True
                        self._flush_metrics()
                    processed += 1
                elif poll.status == 'eoi':
                    source = next(s.source_id for s in self.e.assignment.source_splits if s.split_id == split_id)
                    self._finish_source_split(split_id, source)
                    processed += 1
                break
            # A partition can own no splits, e.g. more partitions than source blocks.
            for occ, node in self.e._node_specs.items():
                if node.kind == 'source' and not any(s.source_id == occ for s in self.e.assignment.source_splits):
                    self._finish_node(occ)
        if (self._sealed_cut is None and not (self._paused and self._idle_sent)
                and (command is None or command['kind'] not in ('cut', 'receipt'))):
            if self._fixed_key_exclusion is None:
                processed += self._advance_input_completions(max_records=max_records, deadline_ms=deadline_ms)
        if time.monotonic() - self._metrics_at >= .5:
            self._flush_metrics()
        done = set(self.e._node_specs).issubset(self._finished)
        with self._condition:
            # Late business belongs to the next generation. It neither
            # changes the sealed frontier nor blocks its receipt/final recut.
            quiet = ((self._sealed_cut is not None or not self._queue)
                     and not self._cut_controls
                     and (not self._credits or self._parkable_credits())
                     and not (getattr(self, '_park_requested', False)
                              and getattr(self, '_park_finish_pending', False))
                     and all(k.checkpoint_ready for k in self.e._kernels.values())
                     and (not self._park_mode or not any(k.pending_calls for k in self.e._kernels.values())))
            notify_done = (done and quiet and not self._done_sent
                           and not any(k.pending_calls for k in self.e._kernels.values()))
            notify_idle = quiet and (self._paused or done) and not self._idle_sent
            # Reserve this frontier before publishing. A pause/resume/input
            # arriving during mailbox backpressure invalidates it under this
            # same lock; never overwrite that invalidation after send returns.
            # Keep the actual sends outside the lock so control stays live.
            if notify_done:
                self._done_sent = True
            if notify_idle:
                self._idle_sent = True
            idle_generation = getattr(self, '_activity_generation', 0)
            wait_notice = self._checkpoint_wait_notification()
            snapshot_request = self._cut if quiet and (self._paused or done) else None
            if snapshot_request is not None:
                # Freeze under the same lock that admits new input, BEFORE
                # snapshot I/O or a mailbox send can release this thread.
                self._sealed_cut = snapshot_request.cut_id
        if wait_notice is not None:
            self._send('checkpoint_wait', wait_notice)
        if notify_done:
            from ..checkpoint.coordinator import _cursor_json
            self._send('eoi', {'scope': 'worker', 'cursors': [_cursor_json(r.snapshot())
                                                           for r in self.e._readers.values()],
                'pruned_splits': [s.split_id for s in self.e.assignment.source_splits
                    if s.split_id in self._finished_splits and not self.e._readers[s.split_id]._eoi]})
        if notify_idle:
            self._flush_metrics()
            self._send('idle', {'done': done, 'activity_generation': idle_generation,
                'source_done': set(self.e._readers) <= self._finished_splits})
        if snapshot_request is not None and self._cut is snapshot_request:
            self._send_cut(snapshot_request)
            self._cut = None
        return WorkerStep(processed=processed, blocked=not bool(processed), end_of_input=done, events=())

    def _finish_source_split(self, split_id, source):
        self._finished_splits.add(split_id)
        if all(s.split_id in self._finished_splits for s in self.e.assignment.source_splits
               if s.source_id == source):
            self._finish_node(source)

    def _metrics_payload(self, occ):
        from ..dataflow_usage import worker_prompt_usage
        kernel = self.e._kernels.get((occ, self.partition))
        payload = {'occurrence_id': occ, 'partition_id': self.partition,
                   'metrics': dict(self._metrics[occ])}
        from ..dataflow_model_observations import worker_model_observations
        models = worker_model_observations(kernel)
        if models is not None:
            payload['model_observations'] = models
        usage = worker_prompt_usage(kernel)
        if usage is not None:
            payload['prompt_usage'] = usage
        if getattr(kernel, 'observations', None) is not None:
            observed = kernel.observations.snapshot()
            if observed['count']:
                payload['callback_observations'] = observed
            handoff = kernel.observations.output_handoff()
            if handoff['peak']:
                payload['output_handoff'] = handoff
            lifecycle = kernel.observations.lifecycle_snapshot()
            if lifecycle['created']:
                payload['actor_lifecycle'] = lifecycle
        return payload

    def _flush_metrics(self):
        for occ in sorted(self._dirty_metrics):
            self._send('progress', self._metrics_payload(occ))
        self._dirty_metrics.clear()
        self._metrics_at = time.monotonic()

    def _output(self, occurrence, record):
        if (self._direct_collection is not None
                and occurrence == self._direct_collection[0]
                and record.sequence >= self._direct_collection[1]):
            maximum = self._direct_collection[1]
            raise ValueError(f'Dataset contains more than the take_all limit of {maximum} rows')
        if record.source_position.get('queue_delivery_kind') == 'producer':
            unit = record.source_position['producer_unit']
            old_unit, count, size = self._producer_expansion.get(occurrence, (unit, 0, 0))
            if old_unit != unit:
                count, size = 0, 0
            count += 1
            size += record.encoded_bytes
            if count > 1024 or size > 16 * 1024 ** 2:
                from ..recovery_contract import RecoveryContractError
                raise RecoveryContractError('Producer input expansion exceeds 1024 records or 16 MiB at a stage')
            self._producer_expansion[occurrence] = (unit, count, size)
        self._metrics[occurrence]['rows_output'] += 1
        self._dirty_metrics.add(occurrence)
        for edge in self._outgoing.get(occurrence, ()):
            if edge.edge_id in self._closed_outputs:
                continue
            if edge.consumer in self.e._node_specs:
                self._push(edge.consumer, edge.input_port, record)
                continue
            spec = self._exchange[edge.edge_id]
            routed_input = record
            if spec.routing in ('hash', 'target') and record.inline is None:
                from ..dataflow_records import clone_record
                from ..kernels.dataflow import _materialize_row, _row_bytes
                row = _materialize_row(record, self.services)
                routed_input = clone_record(record, inline=row, payload_ref=None,
                    encoded_bytes=_row_bytes(row, max_bytes=self.bounds.max_row_bytes))
            route_sequence = self.e._channels.routing.values().get(spec.channel_id, 0)
            routes, raw = self._frame_call(_route_record_document, routed_input, spec=spec,
                state=self.e._channels.routing, bounds=self.bounds)
            for routed in routes:
                from ..recovery_contract import identity
                self._publish(routed.record, edge=edge, spec=spec, target=routed.reader_id,
                              delivery=identity(["producer-delivery-v3", self.partition, routed.delivery_id]),
                              route_sequence=route_sequence, validated_raw=raw)

    def _frame_call(self, function, *args, _frame_retained=0, _frame_reader_growth=False,
                    _frame_source_reader_growth=None, _frame_check_stop=None, **kwargs):
        """Original peer ACKs release codec room; never add another grant."""
        deadline = time.monotonic() + self.bounds.timeout_ms / 1000
        while True:
            (self.services.check_stop if _frame_check_stop is None else _frame_check_stop)()
            with self._condition:
                generation = self._wake_generation
                peer_bytes = self._peer._bytes
            kernel_bytes = sum(getattr(kernel, 'retained_resident_bytes', 0)
                               for kernel in self.e._kernels.values())
            reader = getattr(self, '_finite_input', None)
            reader_bytes = reader.retained_bytes if reader is not None else 0
            if _frame_reader_growth and reader is not None:
                # _ReplayLines' proposed peak already includes its old buffer,
                # new storage, held chunk and extracted line. Count it once.
                reader_bytes -= reader.buffered_bytes
            source_bytes = sum(getattr(source, 'retained_bytes', 0)
                               for source in getattr(self.e, '_readers', {}).values())
            # A frame codec counts its own input Record. Other source splits'
            # suspended pending records remain distinct retained owners.
            for source in getattr(self.e, '_readers', {}).values():
                pending = getattr(source, '_pending_record', None)
                if pending is not None and any(value is pending for value in args):
                    source_bytes -= getattr(source, 'pending_inline_bytes', 0)
            if _frame_source_reader_growth is not None:
                source_bytes -= _frame_source_reader_growth.buffered_bytes
            retained = peer_bytes + kernel_bytes + reader_bytes + source_bytes + _frame_retained
            terminal = getattr(self, '_terminal_blocks', None)
            if terminal is not None and terminal._workspace_limit is not None:
                from ..dataflow_record_frames import metadata_bytes
                with self._condition:
                    queued = sum(metadata_bytes(command) for command in self._queue)
                retained += terminal.retained_bytes + queued
            try:
                return function(*args, services=self.services, retained=retained, **kwargs)
            except DataflowError as exc:
                peak = exc.context.get('codec_workspace_bytes')
                limit = exc.context.get('resident_limit_bytes')
                if (exc.code != 'BUDGET_EXCEEDED' or peak is None or not peer_bytes
                        or peak - peer_bytes > limit):
                    raise
                if time.monotonic() >= deadline:
                    raise DataflowError('DEADLINE_EXCEEDED', 'record codec credit deadline') from exc
                with self._condition:
                    if generation == self._wake_generation:
                        self._condition.wait(.01)

    def _publish(self, record, *, edge=None, spec=None, target=None, delivery=None,
                 route_sequence=None, validated_raw=None):
        # Routing and the terminal spool may share the already validated,
        # immutable document for this invocation. No record bytes are cached.
        raw = (self._frame_call(encode_document, record, self.bounds)
               if validated_raw is None else validated_raw)
        if type(raw) is not bytes or len(raw) > document_limit(self.bounds):
            raise DataflowError('INVALID_CONTRACT', 'invalid prevalidated record document')
        if spec is None and validated_raw is not None:
            from ..dataflow_record_frames import admit_encoded_record
            # Appending may have grown the terminal index. Re-admit the live
            # row/document and bounded send metadata against current owners;
            # the earlier encoding grant cannot stand in for this check.
            self._frame_call(admit_encoded_record, record, raw, self.bounds)
        if spec is not None and spec.mode == 'finite_sealed':
            stream = self._finite_buffer(spec)
            size, chunks = self._frame_call(frame_parts, record, self.bounds, target=target, raw=raw)
            if stream.bytes + size > spec.storage_bytes:
                raise DataflowError('BUDGET_EXCEEDED', 'finite producer exceeds reserved storage')
            stream.append_parts(record, chunks, size=size)
            return
        delivery = delivery or self.e._next_operation_id('terminal')
        deadline = time.monotonic() + self.bounds.timeout_ms / 1000
        while True:
            self.services.check_stop()
            if self.e._cancel_event.is_set():
                raise DataflowError('CANCELLED', 'business execution cancelled')
            with self._condition:
                generation = self._wake_generation
            try:
                if len(self._credits) >= self._credit_limit:
                    raise DataflowError('CAPACITY_PENDING', 'data credit exhausted', retryable=True)
                lease = (reserve_credit(spec.channel_id, target, rows=1, size_bytes=record.encoded_bytes,
                           operation_id=delivery, state=self.e._channels) if spec is not None else None)
                break
            except DataflowError as exc:
                if not exc.retryable:
                    raise
                if (self._park_mode and not (spec is not None
                        and spec.mode == 'memory_incremental'
                        and len(self._credits) < self._credit_limit)):
                    raise DataflowError('RESOURCE_INFEASIBLE',
                        'one operator invocation exceeds the parked durable credit window; co-resident execution required') from exc
                if time.monotonic() >= deadline:
                    raise DataflowError('DEADLINE_EXCEEDED', 'data delivery credit deadline') from exc
                with self._condition:
                    if (generation == self._wake_generation
                            and not self.e._cancel_event.is_set()):
                        self._condition.wait(.01)
        payload = {'delivery_id': delivery, 'producer_partition': self.partition,
                   'target_partition': target, 'edge_id': edge.edge_id if edge else None,
                   'record_id': record.record_id, 'sequence': record.sequence}
        if spec is not None:
            payload.update(route_epoch=spec.route_epoch, route_sequence=route_sequence)
        key = None
        if spec is None or spec.mode == 'memory_incremental':
            payload['peer'] = self._peer.put(raw)
            key = payload['peer']['key']
        else:
            ref = self.services.store.put(iter([raw]), max_bytes=len(raw),
                                          media_type='application/x-demiflow-record-v3')
            payload['ref'] = _artifact_json(ref)
        with self._condition:
            self._credits[delivery] = (lease, key)
            if spec is not None and spec.mode == 'durable_incremental':
                self._credit_payloads[delivery] = payload
        self._send('terminal' if edge is None else 'data', payload)

    def _receive_data(self, command):
        edge_id = command['edge_id']
        edge = next((e for e in self.e._graph.edges if e.edge_id == edge_id), None)
        spec = self._exchange.get(edge_id)
        if (edge is None or spec is None or edge.consumer not in self.e._node_specs
                or command['producer_partition'] not in spec.producer_partitions
                or self.partition not in spec.consumer_partitions):
            raise DataflowError('FENCED', 'delivery is outside assigned physical edge')
        memory = spec.mode == 'memory_incremental'
        if memory:
            sequence = command.get('route_sequence')
            if (type(sequence) is not int or sequence < 0
                    or type(command.get('route_epoch')) is not int or command['route_epoch'] != spec.route_epoch):
                raise DataflowError('FENCED', 'memory delivery lacks its original routing frontier')
            previous = self._memory_frontiers.get(edge_id, {}).get(command['producer_partition'], -1)
            if sequence <= previous:
                # A memory channel is ordered and has no replay owner. Reject
                # duplicate/reordered transport before re-reading a released
                # peer buffer or executing the business callback a second time.
                raise DataflowError('INVALID_CONTRACT', 'duplicate or reordered memory delivery')
        if 'peer' in command:
            self._frame_call(admit_receive, command['peer']['bytes'], self.bounds)
            batch = self._read_batch
            if batch is not None and batch[2] < len(batch[0]) and batch[0][batch[2]] is command:
                raw = next(batch[1])
                batch[2] += 1
            else:
                raw = read_peer(command['peer'], bounds=self.bounds)
        else:
            ref = ArtifactRef(**command['ref'])
            raw = self._frame_call(read_document, ref, self.services.store, self.bounds)
        record = self._frame_call(decode_document, raw, record_id=command['record_id'], sequence=command['sequence'],
                                 bounds=self.bounds)
        del raw  # Downstream encoding owns the decoded Record, not another raw copy.
        if memory or command['delivery_id'] not in self._consumed_deliveries:
            self._push(edge.consumer, edge.input_port, record)
            if memory:
                self._memory_frontiers.setdefault(edge_id, {})[command['producer_partition']] = sequence
            else:
                if len(self._consumed_deliveries) >= self.bounds.max_pending_events:
                    raise DataflowError('BUDGET_EXCEEDED', 'consumer delivery frontier exceeds bounded window')
                self._consumed_deliveries.add(command['delivery_id'])
        self._send('ack', {'delivery_id': command['delivery_id'],
                           'producer_partition': command['producer_partition']})

    def _push(self, occ, port, record):
        if occ in self._finished:
            return
        self._metrics[occ]['rows_input'] += 1
        self._dirty_metrics.add(occ)
        if (self._observation_rows and self.e._node_specs[occ].kind == 'transform'
                and self._metrics[occ]['rows_input'] % self._observation_rows == 0):
            self._send('progress', self._metrics_payload(occ))
        if self.e._node_specs[occ].kind == 'terminal':
            if self._terminal_spec.kind == 'take_all':
                maximum = self._terminal_spec.configuration['arguments'].get('limit')
                if maximum is not None and self._terminal_rows >= maximum:
                    # This partition alone proves global overflow. Keep the
                    # restored prefix; do not decode/encode/spool the probe.
                    # Cross-partition totals remain the driver's responsibility.
                    raise ValueError(f'Dataset contains more than the take_all limit of {maximum} rows')
            from ..arrow_payload import is_arrow_record, batch_cache
            if is_arrow_record(record):
                # Actual worker delivery uses the transfer codec directly.
                # Authenticate and bind the shared payload before accepting its
                # row bytes/schema into terminal state, even for co-location.
                batch_cache(self.services.store, self.bounds, services=self.services).locate(record)
            elif record.inline is None and self._terminal_spec.configuration.get('input_carrier') == 'stored_typed_row.v1':
                # Only the sealed pure-join carrier may defer hydration to the
                # native driver. Authenticate the complete original payload
                # before this worker ACKs its compact Record metadata.
                ref = record.payload_ref
                if (ref.media_type != 'application/octet-stream'
                        or ref.size_bytes != record.encoded_bytes
                        or not 0 <= ref.size_bytes <= self.bounds.max_row_bytes):
                    raise DataflowError('CORRUPT_ARTIFACT', 'stored terminal row differs from its sealed payload bound')
                chunks = self.services.store.read(ref, max_bytes=self.bounds.max_row_bytes, chunk_bytes=4096)
                chunk = None
                try:
                    for chunk in chunks:
                        self.services.check_stop()
                finally:
                    chunks.close()
                del chunk
            elif record.inline is None:
                from ..dataflow_records import clone_record
                from ..kernels.dataflow import _materialize_row, _row_bytes
                row = _materialize_row(record, self.services)
                record = clone_record(record, inline=row, payload_ref=None,
                    encoded_bytes=_row_bytes(row, max_bytes=self.bounds.max_row_bytes))
            if self._terminal_spec.kind == 'write_datasink':
                from ..contracts.dataflow import thaw_record_value
                facts = self.services.journal.read_configuration()
                binding = facts.get('queue_binding')
                if binding and binding['mode'] == 'sink':
                    from ..dataflow_acquisition_call import AcquisitionCall
                    from ..row_scope import RowScope, set_row_scope
                    from ..dataflow_records import clone_record
                    from ..kernels.dataflow import _row_bytes
                    if self._queue_finalizer is None:
                        self._queue_finalizer = AcquisitionCall('finalize')
                        self._queue_finalizer.bind(self.services, occ)
                    delivery = ArtifactRef(**json.loads(record.source_position['queue_delivery_ref']))
                    set_row_scope(RowScope(record.record_id, delivery))
                    try:
                        row = self._queue_finalizer(thaw_record_value(record.inline))
                    finally:
                        set_row_scope(None)
                    record = clone_record(record, inline=row, payload_ref=None,
                        encoded_bytes=_row_bytes(row, max_bytes=self.bounds.max_row_bytes))
            if self._limit is not None and self._terminal_rows >= self._limit:
                return
            if record.inline is not None and not isinstance(record.inline, Mapping):
                # Unknown columns alone do not prove a value collection: a
                # historical payload_ref can also defer table metadata.
                self._terminal_non_mapping_values = True
                self._terminal_columns = self._terminal_schema_ref = None
                self._terminal_blocks.retain_columns(None)
            elif self._terminal_columns is None and not self._terminal_non_mapping_values:
                if isinstance(record.inline, Mapping):
                    self._set_terminal_columns(record.inline, row=record.inline, sort=True)
                elif record.schema_ref is not None:
                    from ..record_schema import RecordSchemas
                    self._set_terminal_columns(RecordSchemas(self.services).read(record.schema_ref), schema=True)
            raw = self._frame_call(encode_document, record, self.bounds)
            size, chunks = self._frame_call(frame_parts, record, self.bounds, raw=raw)
            if self._terminal_bytes + size > self.bounds.max_spill_bytes:
                raise DataflowError('BUDGET_EXCEEDED', 'terminal durable spool exceeds disk bound')
            self._terminal_blocks.append_parts(record, chunks, size=size)
            self._terminal_bytes += size
            self._terminal_rows += 1
            self._metrics[occ]['rows_output'] += 1
            if self._terminal_spec.kind in ('take', 'take_all', 'take_batch', 'iter_rows', 'iter_batches'):
                self._publish(record, validated_raw=raw)
            del raw
            if self._limit is not None and self._terminal_rows >= self._limit:
                self._finish_node(occ)
                self._close_inputs(occ)
            return
        kernel = self.e._kernels[(occ, self.partition)]
        for out in self._model_results(occ, kernel, kernel.push(port, record)):
            self._output(occ, out)
        if kernel._spec.kind == 'LimitOp' and kernel._emitted >= kernel._spec.configuration['limit']:
            self._finish_node(occ)
            self._close_inputs(occ)

    def _close_inputs(self, occurrence):
        for edge in self._incoming.get(occurrence, ()):
            if edge.edge_id in self._closed_inputs:
                continue
            self._closed_inputs.add(edge.edge_id)
            if edge.producer in self.e._node_specs:
                self._close_output(edge.edge_id)
            else:
                self._send('eoi', {'scope': 'input_satisfied', 'edge_id': edge.edge_id,
                                   'consumer_partition': self.partition})

    def _close_output(self, edge_id):
        edge = next(e for e in self.e._graph.edges if e.edge_id == edge_id)
        if edge.producer not in self.e._node_specs:
            raise DataflowError('FENCED', 'closed output is not assigned to this worker')
        self._closed_outputs.add(edge_id)
        producer = edge.producer
        if all(e.edge_id in self._closed_outputs for e in self._outgoing[producer]):
            for split in self.e.assignment.source_splits:
                if split.source_id == producer:
                    self._finished_splits.add(split.split_id)
            self._finish_node(producer)
            self._close_inputs(producer)

    def _edge_eoi(self, edge_id, producer_partition):
        edge = next(e for e in self.e._graph.edges if e.edge_id == edge_id)
        spec = self._exchange.get(edge_id)
        producers = spec.producer_partitions if spec is not None else (self.partition,)
        if producer_partition not in producers:
            raise DataflowError('FENCED', 'EOI from an undeclared edge producer')
        self._finished_inputs.add((edge_id, producer_partition))
        # EOI is an input fact, not permission to block this runner while a
        # callback waits for another co-resident node's timer or output.

    def _region_member_inputs(self, occurrence, inputs):
        self._metrics[occurrence]['rows_input'] = inputs
        self._dirty_metrics.add(occurrence)

    def _region_member_completed(self, occurrence, outputs, inputs):
        self._metrics[occurrence]['rows_input'] = inputs
        self._metrics[occurrence]['rows_output'] = outputs
        self._dirty_metrics.add(occurrence)
        # A reference relation has actually been calculated, including its
        # output-budget/collision validation, before this original EOI appears.
        self._finish_node(occurrence)

    def _advance_regions(self, *, max_records, deadline_ms):
        processed = 0
        deadline = time.monotonic() + deadline_ms / 1000
        for region in self._relational_regions.values():
            occurrence = region.plan.exit_occurrence
            if occurrence in self._finished:
                continue
            if any(not all((entry.edge_id, p) in self._finished_inputs
                           for p in self._exchange[entry.edge_id].producer_partitions)
                   for entry in region.plan.inputs):
                continue
            kernel = region.root
            if kernel.finish_replayable and self._paused and (not self._park_requested or self._cut is not None):
                continue
            if self._park_mode and self._park_requested and not self._park_can_output(occurrence):
                self._paused = True
                continue
            key = region.plan.region_id
            iterator = self._active_finishes.get(key)
            if iterator is None:
                # External ports own their original verified finite artifacts;
                # internal ports carry no duplicate payload or invented seal.
                for member in region.kernels.values():
                    for port in member._ports.values():
                        port.finish()
                port = (kernel._finish_cursor['port'] if kernel._finish_cursor is not None
                        else kernel._port_order[0])
                iterator = self._finite_finish_iterator(occurrence, kernel, port)
                self._active_finishes[key] = iterator
            while True:
                try:
                    record = next(iterator)
                except StopIteration:
                    self._active_finishes.pop(key)
                    for member in region.plan.occurrences:
                        for edge in self._incoming.get(member, ()):
                            self._finished_ports.add(edge.edge_id)
                        self._finish_node(member)
                    break
                self._output(occurrence, record)
                processed += 1
                if self._park_requested and kernel.finish_replayable:
                    self._park_finish_pending = self._park_can_output(occurrence)
                if processed >= max_records or time.monotonic() >= deadline:
                    return processed
        return processed

    def _advance_input_completions(self, *, max_records, deadline_ms):
        self._park_finish_pending = False
        processed = self._finite_input.step(max_records=max_records, deadline_ms=deadline_ms)
        if processed >= max_records:
            return processed
        processed += self._advance_regions(max_records=max_records - processed, deadline_ms=deadline_ms)
        if processed >= max_records:
            return processed
        outputs = processed
        deadline = time.monotonic() + deadline_ms / 1000
        # Graph membership bounds this state. A completed node may close its
        # local outgoing edges, so visit until no further synchronous EOI can
        # advance; pending callbacks return to the ordinary bounded poll loop.
        while True:
            advanced = False
            for occurrence, edges in self._incoming.items():
                if (occurrence not in self.e._node_specs or occurrence in self._finished
                        or occurrence in self._region_members):
                    continue
                node = self.e._node_specs[occurrence]
                kernel = self.e._kernels[(occurrence, self.partition)] if node.kind != 'terminal' else None
                if kernel is not None and kernel.pending_calls:
                    continue
                for edge in edges:
                    if edge.edge_id in self._finished_ports:
                        continue
                    producers = (self._exchange[edge.edge_id].producer_partitions
                                 if edge.edge_id in self._exchange else (self.partition,))
                    if not all((edge.edge_id, p) in self._finished_inputs for p in producers):
                        continue
                    if kernel is not None and getattr(kernel, 'recovery_waits_for_live_drain', False):
                        members = kernel._shared_actor.live_members
                        if (self._queue or any(k.pending_calls for (o, _), k in self.e._kernels.items() if o in members)
                                or any((incoming.edge_id, p) not in self._finished_inputs
                                    for o in members for incoming in self._incoming.get(o, ())
                                    if incoming.producer not in members
                                    for p in (self._exchange[incoming.edge_id].producer_partitions
                                        if incoming.edge_id in self._exchange else (self.partition,)))):
                            continue
                    if kernel is not None and kernel.cooperative_finish:
                        if kernel.finish_replayable:
                            if self._paused and (not self._park_requested or self._cut is not None):
                                continue
                            if self._park_mode and not self._park_can_output(occurrence):
                                # Original ACK may resume a restored producer
                                # before the next park command arrives. Bound
                                # that interval too; never depend on timing.
                                self._paused = True
                                continue
                        iterator = self._active_finishes.get(edge.edge_id)
                        if iterator is None:
                            iterator = self._finite_finish_iterator(occurrence, kernel, edge.input_port)
                            self._active_finishes[edge.edge_id] = iterator
                        try:
                            out = next(iterator)
                        except StopIteration:
                            self._active_finishes.pop(edge.edge_id)
                        else:
                            self._output(occurrence, out)
                            if self._park_requested and kernel.finish_replayable:
                                self._park_finish_pending = self._park_can_output(occurrence)
                            processed += 1
                            outputs += 1
                            advanced = True
                            if outputs >= max_records or time.monotonic() >= deadline:
                                return processed
                            continue
                    elif kernel is not None:
                        for out in self._model_results(occurrence, kernel,
                                kernel.finish(edge.input_port, wait_for_calls=False)):
                            self._output(occurrence, out)
                    self._finished_ports.add(edge.edge_id)
                    processed += 1
                    advanced = True
                    # A final partial batch can launch an asynchronous call.
                    # Its output and cleanup must drain before downstream EOI.
                    if kernel is not None and kernel.pending_calls:
                        break
                if (all(edge.edge_id in self._finished_ports for edge in edges)
                        and (kernel is None or not kernel.pending_calls)):
                    self._finish_node(occurrence)
                    processed += 1
                    advanced = True
            if not advanced:
                return processed

    def _finish_node(self, occurrence):
        if occurrence in self._finished:
            return
        self._finished.add(occurrence)
        self._dirty_metrics.add(occurrence)
        self._flush_metrics()
        if self.e._node_specs[occurrence].kernel.configuration.get('graph_observe'):
            payload = self._metrics_payload(occurrence)
            payload['node_finished'] = True
            self._send('progress', payload)
        for edge in self._outgoing.get(occurrence, ()):
            if edge.edge_id in self._closed_outputs:
                continue
            if edge.consumer in self.e._node_specs:
                self._edge_eoi(edge.edge_id, self.partition)
            elif self._exchange[edge.edge_id].mode == 'finite_sealed':
                spec = self._exchange[edge.edge_id]
                ref, count = self._seal_finite_producer(spec)
                self._send('eoi', {'scope': 'finite', 'edge_id': edge.edge_id,
                    'producer_partition': self.partition, 'manifest_ref': _artifact_json(ref),
                    'last_sequence': count - 1})
            else:
                self._emitted_eoi.add(edge.edge_id)
                self._send('eoi', {'scope': 'edge', 'edge_id': edge.edge_id,
                                   'producer_partition': self.partition})

    def _finite_buffer(self, spec):
        if spec.channel_id not in self._finite:
            if (len(self._finite) + 2) * 2 >= self.bounds.max_open_files:
                raise DataflowError('BUDGET_EXCEEDED', 'finite spool exceeds open file budget')
            from .terminal_blocks import TerminalBlocks
            self._finite[spec.channel_id] = TerminalBlocks(self.services, self.partition, partitioned=False,
                frame_call=self._frame_call,
                coalesce_allowed=lambda: len(self._finite) <= 1 and not self._terminal_blocks.blocks)
        return self._finite[spec.channel_id]

    def _seal_finite_producer(self, spec):
        stream = self._finite_buffer(spec)
        part = stream.manifest_partitions()[0]
        return ArtifactRef(**part['data_ref']), part['rows']

    def _finite_frame_call(self, function, *args, **kwargs):
        return self._frame_call(function, *args,
                                _frame_check_stop=self.e._finite_services.check_stop, **kwargs)

    def _receive_finite(self, command):
        self.e._finite_services.check_stop()
        spec = self._exchange[command['edge_id']]
        ref = ArtifactRef(**command['seal_ref'])
        edge = next(e for e in self.e._graph.edges if e.edge_id == spec.edge_id)
        kernel = self.e._kernels.get((edge.consumer, self.partition))
        if kernel is not None and kernel.accepts_finite_reference(edge.input_port):
            from ..dataflow_finite_input import validate_finite_members
            # All members are authenticated before mutating the kernel port.
            entries = validate_finite_members(self.e._finite_services, ref, spec, target=self.partition,
                                             frame_call=self._finite_frame_call)
            self.e._finite_services.check_stop()
            for entry in entries:
                self.e._finite_services.check_stop()
                if (spec.edge_id, entry['producer']) in self._finished_inputs:
                    continue
                if edge.consumer not in self._finished:
                    kernel.adopt_finite_reference(edge.input_port, entry)
                    self._metrics[edge.consumer]['rows_input'] += entry['rows']
                    self._dirty_metrics.add(edge.consumer)
                self._edge_eoi(spec.edge_id, entry['producer'])
            self.e._finite_services.check_stop()
            self._send('ack', {'delivery_id': command['delivery_id'], 'producer_partition': None})
            return
        # ACK adopts immutable, fully authenticated references. Consumption
        # proceeds in bounded steps; its cursor shares the original worker cut.
        self._finite_input.adopt(spec.edge_id, ref)
        self.e._finite_services.check_stop()
        self._send('ack', {'delivery_id': command['delivery_id'], 'producer_partition': None})

    def _parkable_credits(self):
        return self._park_mode and set(self._credits) == set(self._credit_payloads)

    def _final_scalar(self, blocks, partitions):
        from .dataflow_terminal import aggregate_scalar, terminal_scalar
        if self._terminal_spec.kind != 'aggregate':
            return terminal_scalar(blocks.stream(), spec=self._terminal_spec, services=self.services,
                                   schema_ref=self._terminal_schema_ref)
        from functools import partial
        from ..kernels.aggregate import aggregate_records
        occurrence = next(occ for occ, node in self.e._node_specs.items() if node.kind == 'terminal')
        program = self.services.programs.get(occurrence)
        if program is None or program.kind != 'aggregate':
            raise DataflowError('INVALID_CONTRACT', 'terminal aggregate programs are not sealed')
        # The index keeps its sealed metadata reservation. Subtract the other
        # live worker owners even when stages share a larger assignment.
        memory = self._terminal_spec.configuration.get('terminal_memory', {})
        workspace = self._frame_call(blocks.scalar_workspace)
        workspace = min(workspace, memory.get('payload_bytes', workspace))
        values = aggregate_records(program.value, partial(blocks.records, partitions),
                                   services=self.services, workspace=workspace,
            batch_eligible=self._terminal_spec.configuration.get("integer_aggregate_blocks") is True)
        return aggregate_scalar(values, max_bytes=self.bounds.max_state_bytes)

    def snapshot(self, request):
        if ((self._queue and self._sealed_cut != request.cut_id)
                or (self._credits and not self._parkable_credits())):
            raise DataflowError('INVALID_CONTRACT', 'cut requires drained inputs and acknowledged deliveries')
        for reader in self.e._readers.values():
            reader.acknowledge_output()
        from ..dataflow_kernel_pack import snapshot_kernel_images
        states = snapshot_kernel_images(self.e._kernels, store=self.services.store, bounds=self.bounds)
        from ..recovery_contract import identity
        for spec in self.e._output_specs:
            if spec.mode == 'finite_sealed':
                ref, count = self._seal_finite_producer(spec)
                raw = document({'ref': _artifact_json(ref), 'count': count,
                                'bytes': self._finite_buffer(spec).bytes})
                states[identity(['finite-spool', spec.channel_id])] = self.services.store.put(
                    iter([raw]), max_bytes=len(raw), media_type='application/json')
        frontier = document({'schema': 'demiflow.worker-frontier.v3',
            **({'fixed_key_batches': True} if self._fixed_key_exclusion is not None else {}),
            'nodes': sorted(self._finished), 'splits': sorted(self._finished_splits),
            'inputs': [list(x) for x in sorted(self._finished_inputs)],
            'finished_ports': sorted(self._finished_ports),
            'closed_inputs': sorted(self._closed_inputs), 'closed_outputs': sorted(self._closed_outputs),
            'durable_deliveries': self._credit_payloads, 'emitted_eoi': sorted(self._emitted_eoi),
            'metrics': self._metrics, 'event_sequence': self.e._event_sequence,
            'consumed_deliveries': sorted(self._consumed_deliveries),
            'memory_frontiers': self._memory_frontiers,
            'finite_reads': self._finite_input.snapshot()}, self.bounds.max_state_bytes)
        states[identity(['worker-frontier', self.partition])] = self.services.store.put(
            iter([frontier]), max_bytes=len(frontier), media_type='application/json')
        del frontier
        refs = snapshot_channels(self.e._channels, cut=request, store=self.services.store)
        sinks = {}
        if self._terminal_spec is not None:
            blocks = self._terminal_blocks
            with blocks.hold_metadata((states, refs, sinks)):
                partitions = blocks.manifest_partitions()
                with blocks.hold_metadata(partitions):
                    blocks._check_workspace(2048)
                    sink_id = self._terminal_spec.configuration.get('sink_id', self._terminal_spec.kind)
                    manifest = {'schema': 'demiflow.dataflow_sink_manifest.v3', 'sink_id': sink_id,
                                'partition_id': self.partition, 'rows': self._terminal_rows,
                                'bytes': self._terminal_bytes, 'columns': self._terminal_columns,
                                'partitions': partitions}
                    if self._terminal_schema_ref is not None:
                        manifest['schema_ref'] = _artifact_json(self._terminal_schema_ref)
                    if self._terminal_non_mapping_values:
                        manifest['non_mapping_values'] = True
                    with blocks.hold_metadata(manifest):
                        if request.final:
                            manifest['scalar'] = self._final_scalar(blocks, partitions)
                        size, chunks = blocks.manifest_chunks(manifest)
                        try:
                            sinks[sink_id] = self.services.store.put(chunks, max_bytes=size, media_type=MANIFEST_MEDIA)
                        finally:
                            chunks.close() if hasattr(chunks, 'close') else None
        result = CutContribution(cut_id=request.cut_id, participant_id=self.e._participant_ids()[0],
            boot_id=self.e.assignment.lease.boot_id,
            sources=tuple(r.snapshot() for r in self.e._readers.values()),
            operator_states=states, channels={k: _artifact_json(v) for k, v in refs.items()},
            buffers=tuple(refs.values()), effects=tuple(self.e._effect_refs), sinks=sinks, final=request.final)
        self.e._snapshot_contributions[request.cut_id] = result
        self._cut_credit[request.cut_id] = (self._unconfirmed_rows, self._unconfirmed_bytes)
        return result

    def close(self):
        self.e._cancel_event.set()
        from ..request_limits import cooperative_stop_error
        if cooperative_stop_error(self.e._cancel_reason) is None:
            self.e._mailbox.cancel_pending()
        self.wake()
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(timeout=self.bounds.timeout_ms / 1000)
            if self._thread.is_alive():
                raise DataflowError('DEADLINE_EXCEEDED', 'Business thread still owns its scratch buffers')
        for iterator in self._active_finishes.values():
            iterator.close()
        self._active_finishes.clear()
        if self._fixed_key_exclusion is not None:
            self._fixed_key_exclusion.close()
        self._finite_input.close()
        self._peer.close()
        self._terminal_blocks.close()
        for stream in self._finite.values():
            stream.close()
