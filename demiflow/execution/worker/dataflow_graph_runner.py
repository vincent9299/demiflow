"""Business-thread execution of the assigned portion of a sealed graph.

Both hosts use this runner. Only edges in the physical plan may cross workers;
internal edges run once and preserve their declared input ports. The driver sees
bounded byte locators and progress/cut metadata, never calls an operator.
"""
from __future__ import annotations

import json
import tempfile
import threading
import time
from collections import deque
from dataclasses import asdict

from ..contracts.dataflow import ArtifactRef, CutContribution, WorkerStep
from ..contracts.dataflow_errors import DataflowError
from ..exchange.dataflow_bridge import _encode_record_document, _decode_record_document
from ..exchange.dataflow_channels import reserve_credit, route_record, snapshot_channels
from ..exchange.dataflow_transport import PeerBuffers, read_peer
from ..dataflow_sinks import _artifact_json, _encode_record, RECORDS_MEDIA, MANIFEST_MEDIA
from ..recovery_contract import document


class WorkerGraphRunner:
    def __init__(self, executor):
        self.e = executor
        self.services = executor._services
        self.bounds = self.services.bounds
        self.partition = executor.assignment.lease.partition_ids[0]
        self._queue = deque()
        self._condition = threading.Condition()
        self._thread = None
        self._paused = False
        self._idle_sent = False
        self._done_sent = False
        self._finished = set()
        self._metrics = {occ: {'rows_input': 0, 'rows_output': 0} for occ in executor._node_specs}
        self._dirty_metrics = set()
        self._metrics_at = time.monotonic()
        self._source_rows_since_cut = 0
        self._finished_splits = set()
        self._finished_inputs = set()
        self._closed_inputs = set()
        self._closed_outputs = set()
        self._credits = {}
        self._terminals = {}
        self._finite = {}
        self._cut = None
        self._outgoing = {}
        self._incoming = {}
        self._exchange = {x.edge_id: x for x in executor._physical.exchanges}
        for edge in executor._graph.edges:
            self._outgoing.setdefault(edge.producer, []).append(edge)
            self._incoming.setdefault(edge.consumer, []).append(edge)
        host = '127.0.0.1'
        if executor._physical.policy.provider == 'ray_core':
            import ray
            host = ray.util.get_node_ip_address()
        self._peer = PeerBuffers(bounds=self.bounds, host=host)
        self._terminal_rows = 0
        self._terminal_bytes = 0
        self._terminal_columns = None
        self._terminal_file = tempfile.TemporaryFile()
        self._terminal_spec = next((n.kernel for n in executor._node_specs.values()
                                    if n.kind == 'terminal'), None)
        args = self._terminal_spec.configuration.get('arguments', {}) if self._terminal_spec else {}
        self._limit = (args.get('limit') if self._terminal_spec and self._terminal_spec.kind == 'take'
                       else args.get('batch_size', 20) if self._terminal_spec and self._terminal_spec.kind == 'take_batch'
                       else None)

    def _restore_terminal(self):
        image = self.e._restore_image
        if image is None or self._terminal_spec is None:
            return
        from ..dataflow_sinks import _artifact_from_json
        for contribution in image.contributions:
            if contribution.participant_id not in self.e._participant_ids():
                continue
            ref = contribution.sinks.get(self._terminal_spec.kind)
            if ref is None:
                continue
            manifest = json.loads(b"".join(self.services.store.read(ref, max_bytes=self.bounds.max_control_bytes)))
            data = _artifact_from_json(manifest["data_ref"])
            for chunk in self.services.store.read(data, max_bytes=max(1, data.size_bytes)):
                self._terminal_file.write(chunk)
            self._terminal_rows, self._terminal_bytes = manifest["rows"], manifest["bytes"]
            self._terminal_columns = manifest["columns"]

    def start(self):
        if self._thread is not None:
            return
        self._restore_terminal()
        from ..recovery_contract import identity
        frontier_ref = self.e._restored_states.get(identity(['worker-frontier', self.partition]))
        if self.e._restore_image is not None:
            if frontier_ref is None:
                raise DataflowError('INCOMPATIBLE_HISTORY', 'partial recovery lacks worker completion frontier')
            frontier = json.loads(b''.join(self.services.store.read(frontier_ref,
                max_bytes=self.bounds.max_state_bytes)))
            if frontier.get('schema') != 'demiflow.worker-frontier.v3':
                raise DataflowError('INCOMPATIBLE_HISTORY', 'unknown worker completion frontier')
            self._finished = set(frontier['nodes'])
            self._finished_splits = set(frontier['splits'])
            self._finished_inputs = {tuple(x) for x in frontier['inputs']}
            self._closed_inputs = set(frontier.get('closed_inputs', ()))
            self._closed_outputs = set(frontier.get('closed_outputs', ()))
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
                stream, _, _ = self._finite_buffer(spec)
                for chunk in self.services.store.read(data, max_bytes=max(1, data.size_bytes)):
                    stream.write(chunk)
                self._finite[spec.channel_id] = (stream, data.size_bytes, value['count'])
        for edge_id in self._closed_inputs:
            edge = next(e for e in self.e._graph.edges if e.edge_id == edge_id)
            if edge.producer not in self.e._node_specs:
                self._send('eoi', {'scope': 'input_satisfied', 'edge_id': edge_id,
                                   'consumer_partition': self.partition})
        self._thread = threading.Thread(target=self._run, name='dataflow-business', daemon=True)
        self._thread.start()

    def wake(self):
        with self._condition:
            self._condition.notify_all()

    def _send(self, kind, payload=None, ref=None):
        self.e._mailbox.send(self.e._event(kind, payload=payload, payload_ref=ref))

    def receive(self, command):
        kind = command['kind']
        with self._condition:
            if kind == 'ack':
                entry = self._credits.pop(command['delivery_id'], None)
                if entry is None:
                    raise DataflowError('INVALID_CONTRACT', 'ack does not name an outstanding delivery')
                lease, key = entry
                if lease is not None:
                    self.e._channels.release(lease)
                if key is not None:
                    self._peer.release(key)
            elif kind == 'pause':
                self._paused = True
                self._idle_sent = False
            elif kind == 'resume':
                self._source_rows_since_cut = 0
                self._paused = False
                self._idle_sent = False
                self._cut = None
            else:
                if len(self._queue) >= self.bounds.max_pending_events:
                    raise DataflowError('BUDGET_EXCEEDED', 'worker input queue exceeds credit')
                self._queue.append(command)
                self._idle_sent = False
            self._condition.notify_all()

    def _run(self):
        try:
            while not self.e._cancel_event.is_set():
                step = self.step(max_records=1, deadline_ms=50)
                if not step.processed:
                    with self._condition:
                        self._condition.wait(.005)
        except BaseException as exc:
            if not self.e._cancel_event.is_set():
                import traceback
                self._send('failure', {'code': getattr(exc, 'code', 'INTERNAL_PROTOCOL'),
                                      'message': str(exc), 'traceback': traceback.format_exc()[-8000:]})

    def step(self, *, max_records, deadline_ms):
        if max_records <= 0 or deadline_ms <= 0:
            raise DataflowError('INVALID_CONTRACT', 'step budgets must be positive')
        processed = 0
        with self._condition:
            command = self._queue.popleft() if self._queue else None
        if command:
            if command['kind'] == 'data':
                self._receive_data(command)
            elif command['kind'] == 'finite_input':
                self._receive_finite(command)
            elif command['kind'] == 'edge_eoi':
                self._edge_eoi(command['edge_id'], command['producer_partition'])
            elif command['kind'] == 'close_output':
                self._close_output(command['edge_id'])
            elif command['kind'] == 'cut':
                self._cut = command['request']
            elif command['kind'] == 'receipt':
                self.e.accept_receipt(command['commit_id'], cut_id=command['cut_id'])
            else:
                raise DataflowError('INVALID_CONTRACT', 'unknown business command')
            processed += 1
        if not self._paused and not self._cut:
            for split_id, reader in self.e._readers.items():
                if split_id in self._finished_splits:
                    continue
                self.services.check_stop()
                poll = reader.next_record()
                if poll.status == 'record':
                    source = next(s.source_id for s in self.e.assignment.source_splits if s.split_id == split_id)
                    self._output(source, poll.record)
                    self._source_rows_since_cut += 1
                    if self._source_rows_since_cut >= self.e._physical.policy.checkpoint_rows:
                        self._paused = True
                        self._flush_metrics()
                    processed += 1
                elif poll.status == 'eoi':
                    self._finished_splits.add(split_id)
                    source = next(s.source_id for s in self.e.assignment.source_splits if s.split_id == split_id)
                    if all(s.split_id in self._finished_splits for s in self.e.assignment.source_splits
                           if s.source_id == source):
                        self._finish_node(source)
                    processed += 1
                break
            # A partition can own no splits, e.g. more partitions than source blocks.
            for occ, node in self.e._node_specs.items():
                if node.kind == 'source' and not any(s.source_id == occ for s in self.e.assignment.source_splits):
                    self._finish_node(occ)
        if time.monotonic() - self._metrics_at >= .5:
            self._flush_metrics()
        done = set(self.e._node_specs).issubset(self._finished)
        with self._condition:
            quiet = not self._queue and not self._credits
        if done and quiet and not self._done_sent:
            from ..dataflow_checkpoint import _cursor_json
            self._send('eoi', {'scope': 'worker', 'cursors': [_cursor_json(r.snapshot())
                                                           for r in self.e._readers.values()],
                'pruned_splits': [s.split_id for s in self.e.assignment.source_splits
                    if s.split_id in self._finished_splits and not self.e._readers[s.split_id]._eoi]})
            self._done_sent = True
        if quiet and (self._paused or done) and not self._idle_sent:
            self._flush_metrics()
            self._send('idle', {'done': done})
            self._idle_sent = True
        if self._cut is not None and quiet and (self._paused or done):
            from ..contracts.dataflow_codec import encode_wire
            contribution = self.snapshot(self._cut)
            raw = encode_wire(contribution, type_name='CutContribution', bounds=self.bounds)
            ref = self.services.store.put(iter([raw]), max_bytes=len(raw), media_type='application/json')
            self._send('cut', ref=ref)
            self._cut = None
        return WorkerStep(processed=processed, blocked=not bool(processed), end_of_input=done, events=())

    def _flush_metrics(self):
        for occ in sorted(self._dirty_metrics):
            self._send('progress', {'occurrence_id': occ, 'partition_id': self.partition,
                                   'metrics': dict(self._metrics[occ])})
        self._dirty_metrics.clear()
        self._metrics_at = time.monotonic()

    def _output(self, occurrence, record):
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
                from dataclasses import replace
                from ..kernels.dataflow import _materialize_row, _row_bytes
                row = _materialize_row(record, self.services)
                routed_input = replace(record, inline=row, payload_ref=None, encoded_bytes=_row_bytes(row))
            for routed in route_record(routed_input, spec=spec, state=self.e._channels.routing):
                from ..recovery_contract import identity
                self._publish(routed.record, edge=edge, spec=spec, target=routed.reader_id,
                              delivery=identity(["producer-delivery-v3", self.partition, routed.delivery_id]))

    def _publish(self, record, *, edge=None, spec=None, target=None, delivery=None):
        raw = _encode_record_document(record, max_bytes=self.bounds.max_row_bytes)
        if spec is not None and spec.mode == 'finite_sealed':
            stream, total, count = self._finite_buffer(spec)
            line = document({'record_id': record.record_id, 'sequence': record.sequence,
                             'target': target, 'document': raw.decode()}) + b'\n'
            if total + len(line) > spec.storage_bytes:
                raise DataflowError('BUDGET_EXCEEDED', 'finite producer exceeds reserved storage')
            stream.write(line)
            self._finite[spec.channel_id] = (stream, total + len(line), count + 1)
            return
        delivery = delivery or self.e._next_operation_id('terminal')
        deadline = time.monotonic() + self.bounds.timeout_ms / 1000
        while True:
            self.services.check_stop()
            if self.e._cancel_event.is_set():
                raise DataflowError('CANCELLED', 'business execution cancelled')
            try:
                if len(self._credits) >= min(self.bounds.max_pending_events, 64):
                    raise DataflowError('CAPACITY_PENDING', 'data credit exhausted', retryable=True)
                lease = (reserve_credit(spec.channel_id, target, rows=1, size_bytes=record.encoded_bytes,
                           operation_id=delivery, state=self.e._channels) if spec is not None else None)
                break
            except DataflowError as exc:
                if not exc.retryable:
                    raise
                if time.monotonic() >= deadline:
                    raise DataflowError('DEADLINE_EXCEEDED', 'data delivery credit deadline') from exc
                with self._condition:
                    self._condition.wait(.01)
        payload = {'delivery_id': delivery, 'producer_partition': self.partition,
                   'target_partition': target, 'edge_id': edge.edge_id if edge else None,
                   'record_id': record.record_id, 'sequence': record.sequence}
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
        self._send('terminal' if edge is None else 'data', payload)

    def _receive_data(self, command):
        edge_id = command['edge_id']
        edge = next((e for e in self.e._graph.edges if e.edge_id == edge_id), None)
        spec = self._exchange.get(edge_id)
        if (edge is None or spec is None or edge.consumer not in self.e._node_specs
                or command['producer_partition'] not in spec.producer_partitions
                or self.partition not in spec.consumer_partitions):
            raise DataflowError('FENCED', 'delivery is outside assigned physical edge')
        if 'peer' in command:
            raw = read_peer(command['peer'], bounds=self.bounds)
        else:
            ref = ArtifactRef(**command['ref'])
            raw = b''.join(self.services.store.read(ref, max_bytes=self.bounds.max_row_bytes))
        record = _decode_record_document(raw, record_id=command['record_id'], sequence=command['sequence'],
                                         max_bytes=self.bounds.max_row_bytes)
        self._push(edge.consumer, edge.input_port, record)
        self._send('ack', {'delivery_id': command['delivery_id'],
                           'producer_partition': command['producer_partition']})

    def _push(self, occ, port, record):
        if occ in self._finished:
            return
        self._metrics[occ]['rows_input'] += 1
        self._dirty_metrics.add(occ)
        if self.e._node_specs[occ].kind == 'terminal':
            if record.inline is None:
                from dataclasses import replace
                from ..kernels.dataflow import _materialize_row, _row_bytes
                row = _materialize_row(record, self.services)
                record = replace(record, inline=row, payload_ref=None, encoded_bytes=_row_bytes(row))
            if self._limit is not None and self._terminal_rows >= self._limit:
                return
            raw = _encode_record(record, self.bounds) + b'\n'
            if self._terminal_bytes + len(raw) > self.bounds.max_spill_bytes:
                raise DataflowError('BUDGET_EXCEEDED', 'terminal durable spool exceeds disk bound')
            self._terminal_file.write(raw)
            self._terminal_bytes += len(raw)
            self._terminal_rows += 1
            self._metrics[occ]['rows_output'] += 1
            if self._terminal_columns is None and record.inline is not None:
                self._terminal_columns = sorted(record.inline)
            if self._terminal_spec.kind in ('take', 'take_all', 'take_batch', 'iter_rows', 'iter_batches'):
                self._publish(record)
            return
        kernel = self.e._kernels[(occ, self.partition)]
        for out in kernel.push(port, record):
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
        if edge.consumer in self._finished:
            return
        if all((edge_id, p) in self._finished_inputs for p in producers):
            node = self.e._node_specs[edge.consumer]
            if node.kind != 'terminal':
                kernel = self.e._kernels[(edge.consumer, self.partition)]
                for out in kernel.finish(edge.input_port):
                    self._output(edge.consumer, out)
            if all(all((e.edge_id, p) in self._finished_inputs for p in
                       (self._exchange[e.edge_id].producer_partitions if e.edge_id in self._exchange
                        else (self.partition,))) for e in self._incoming[edge.consumer]):
                self._finish_node(edge.consumer)

    def _finish_node(self, occurrence):
        if occurrence in self._finished:
            return
        self._finished.add(occurrence)
        self._dirty_metrics.add(occurrence)
        self._flush_metrics()
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
                self._send('eoi', {'scope': 'edge', 'edge_id': edge.edge_id,
                                   'producer_partition': self.partition})

    def _finite_buffer(self, spec):
        if spec.channel_id not in self._finite:
            if len(self._finite) + 2 >= self.bounds.max_open_files:
                raise DataflowError('BUDGET_EXCEEDED', 'finite spool exceeds open file budget')
            stream = tempfile.TemporaryFile()
            self._finite[spec.channel_id] = (stream, 0, 0)
        return self._finite[spec.channel_id]

    def _seal_finite_producer(self, spec):
        stream, size, count = self._finite_buffer(spec)
        stream.flush()
        stream.seek(0)
        def chunks():
            while True:
                chunk = stream.read(65536)
                if not chunk:
                    return
                yield chunk
        ref = self.services.store.put(chunks(), max_bytes=max(1, size),
                                      media_type='application/x-demiflow-finite-records-v3')
        stream.seek(0, 2)
        return ref, count

    def _receive_finite(self, command):
        from ..dataflow_handles import _ChunkStream
        spec = self._exchange[command['edge_id']]
        ref = ArtifactRef(**command['seal_ref'])
        seal = json.loads(b''.join(self.services.store.read(ref, max_bytes=self.bounds.max_control_bytes)))
        if (spec.mode != 'finite_sealed' or seal['scope_id'] != spec.scope_id
                or set(seal['membership']) != set(spec.producer_partitions)
                or seal['status'] != 'success' or seal['generation'] != spec.route_epoch):
            raise DataflowError('FENCED', 'finite scope seal differs from physical membership')
        edge = next(e for e in self.e._graph.edges if e.edge_id == spec.edge_id)
        for member in seal['producers']:
            data = ArtifactRef(**member['manifest_ref'])
            if data.size_bytes > spec.storage_bytes:
                raise DataflowError('BUDGET_EXCEEDED', 'finite producer exceeds storage reservation')
            stream = _ChunkStream(self.services.store.read(data, max_bytes=max(1, data.size_bytes)))
            count = 0
            while True:
                line = stream.readline(self.bounds.max_row_bytes + 1)
                if not line:
                    break
                item = json.loads(line)
                count += 1
                if item['target'] == self.partition:
                    record = _decode_record_document(item['document'].encode(), record_id=item['record_id'],
                        sequence=item['sequence'], max_bytes=self.bounds.max_row_bytes)
                    self._push(edge.consumer, edge.input_port, record)
            if count - 1 != member['last_sequence']:
                raise DataflowError('CORRUPT_ARTIFACT', 'finite producer row count differs from seal')
            self._edge_eoi(spec.edge_id, member['partition_id'])
        self._send('ack', {'delivery_id': command['delivery_id'], 'producer_partition': None})

    def snapshot(self, request):
        if self._queue or self._credits:
            raise DataflowError('INVALID_CONTRACT', 'cut requires drained inputs and acknowledged deliveries')
        for reader in self.e._readers.values():
            reader.acknowledge_output()
        states = {}
        for (occ, partition), kernel in self.e._kernels.items():
            ref = kernel.snapshot()
            if ref is not None:
                states[occ] = ref
        from ..recovery_contract import identity
        for spec in self.e._output_specs:
            if spec.mode == 'finite_sealed':
                ref, count = self._seal_finite_producer(spec)
                raw = document({'ref': _artifact_json(ref), 'count': count})
                states[identity(['finite-spool', spec.channel_id])] = self.services.store.put(
                    iter([raw]), max_bytes=len(raw), media_type='application/json')
        frontier = document({'schema': 'demiflow.worker-frontier.v3',
            'nodes': sorted(self._finished), 'splits': sorted(self._finished_splits),
            'inputs': [list(x) for x in sorted(self._finished_inputs)],
            'closed_inputs': sorted(self._closed_inputs), 'closed_outputs': sorted(self._closed_outputs)}, self.bounds.max_state_bytes)
        states[identity(['worker-frontier', self.partition])] = self.services.store.put(
            iter([frontier]), max_bytes=len(frontier), media_type='application/json')
        refs = snapshot_channels(self.e._channels, cut=request, store=self.services.store)
        sinks = {}
        if self._terminal_spec is not None:
            self._terminal_file.flush()
            self._terminal_file.seek(0)
            def chunks():
                while True:
                    chunk = self._terminal_file.read(65536)
                    if not chunk:
                        return
                    yield chunk
            data_ref = self.services.store.put(chunks(), max_bytes=max(1, self._terminal_bytes),
                                                media_type=RECORDS_MEDIA)
            sink_id = self._terminal_spec.kind
            manifest = {'schema': 'demiflow.dataflow_sink_manifest.v3', 'sink_id': sink_id,
                        'partition_id': self.partition, 'rows': self._terminal_rows,
                        'bytes': self._terminal_bytes, 'columns': self._terminal_columns,
                        'data_ref': _artifact_json(data_ref)}
            if request.final:
                from .dataflow_terminal import terminal_scalar
                occurrence = next(occ for occ, node in self.e._node_specs.items() if node.kind == 'terminal')
                manifest['scalar'] = terminal_scalar(self._terminal_file, spec=self._terminal_spec,
                    services=self.services, occurrence_id=occurrence)
            raw = document(manifest)
            sinks[sink_id] = self.services.store.put(iter([raw]), max_bytes=len(raw), media_type=MANIFEST_MEDIA)
            self._terminal_file.seek(0, 2)
        result = CutContribution(cut_id=request.cut_id, participant_id=self.e._participant_ids()[0],
            boot_id=self.e.assignment.lease.boot_id,
            sources=tuple(r.snapshot() for r in self.e._readers.values()),
            operator_states=states, channels={k: _artifact_json(v) for k, v in refs.items()},
            buffers=tuple(refs.values()), effects=tuple(self.e._effect_refs), sinks=sinks, final=request.final)
        self.e._snapshot_contributions[request.cut_id] = result
        return result

    def close(self):
        self.e._cancel_event.set()
        self.wake()
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(timeout=self.bounds.timeout_ms / 1000)
        self._peer.close()
        self._terminal_file.close()
        for stream, _, _ in self._finite.values():
            stream.close()
